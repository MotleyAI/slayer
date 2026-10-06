"""SearchService: facade over the registered retrievers, RRF-fused into one flat hit list.

A search validates inputs, resolves entities leniently (failures warn), applies the optional
``cypher_filter`` allowlist to every channel before the ``max_results`` cap, surfaces user-named
entities as themselves, fans out to retrievers in parallel and refreshes surfaced column samples.
Retriever rankings are never truncated, so changing ``max_results`` never reorders hits. Writes fan
out to every retriever with failures isolated as warnings; deletes stay with ``StorageBackend``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from pydantic import BaseModel, Field

from slayer.core.errors import AmbiguousModelError, EntityResolutionError
from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.profiling import ensure_samples_fresh, refresh_table_backed_model_sampled
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.memories.models import MEMORY_CANONICAL_PREFIX as _MEMORY_PREFIX
from slayer.memories.models import Memory
from slayer.memories.resolver import (
    canonical_id_rooted_at,
    extract_entities_from_query,
    resolve_entity,
)
from slayer.search import graph as _search_graph
from slayer.search.cypher_naive import parse_naive_label_filter as _parse_naive_cypher
from slayer.search.index import Corpus, build_in_memory_corpus
from slayer.search.render import (
    collect_model_entity_pairs,
    compact_description_from_learning,
    render_column_text,
    render_datasource_pair,
)
from slayer.search.retriever import RetrievalResult, Retriever
from slayer.search.retrievers import (
    BM25Retriever,
    EmbeddingRetriever,
    TantivyRetriever,
)
from slayer.search.rrf import rrf_fuse
from slayer.storage.base import StorageBackend
from slayer.storage.document_loading import DocumentLoadFailures


logger = logging.getLogger(__name__)


_RRF_K = 60


# --- Hit & response models ---


class SearchHit(BaseModel):
    """A unified search result. ``kind`` is ``"memory"`` for
    memories, or the entity kind string (``"datasource"``, ``"model"``,
    ``"column"``, ``"measure"``, ``"aggregation"``) for entity hits.

    ``id`` is the raw storage id for memories (suitable for
    ``forget_memory(id=hit.id)``) and the canonical entity string for
    entity hits. ``score`` is always the Reciprocal-Rank-Fusion score
    (``Σ 1 / (k + rank)``, ``k=60``); even single-channel searches go
    through RRF, so the value is comparable across channels but is not
    directly the raw BM25 / tantivy / cosine score.

    ``matched_entities`` and ``query`` are populated for memory hits
    only; entity hits carry empty / ``None`` defaults.

    ``description`` carries a compact preview. For memory
    hits in compact mode it is ``Memory.description`` (or a
    first-paragraph fallback computed from ``learning``); for entity
    hits in any mode it is the entity's structured ``description``
    field. Under compact mode ``text`` is left empty for both kinds.
    """

    kind: str
    id: str
    score: float
    text: str
    description: str | None = None
    matched_entities: list[str] = Field(default_factory=list)
    query: SlayerQuery | None = None


# --- Named-entity lookup results ---


class LookupFound(BaseModel):
    """``_lookup_named_entity`` succeeded; carries ``(kind, text,
    description)``. ``description`` is the entity's
    structured description field (``None`` when absent), surfaced as
    ``SearchHit.description`` under compact mode."""

    kind: str
    text: str
    description: str | None = None


class LookupHidden(BaseModel):
    """The canonical resolved but is gated by a ``hidden`` flag (on the
    model or on the column). ``reason`` is a short human-readable hint
    used to compose the caller-facing warning."""

    reason: str


class LookupMissing(BaseModel):
    """The canonical resolved at ``_resolve_inputs`` time but the
    underlying datasource / model / leaf is no longer present at lookup
    time (race between resolve and lookup, or the entity was deleted)."""

    pass


LookupResult = LookupFound | LookupHidden | LookupMissing


class SearchResponse(BaseModel):
    """Unified search response. ``results`` is a single flat
    list ranked by RRF score; consumers partition by ``kind`` (or by
    ``query is None`` for the memory subset) at the call site."""

    results: list[SearchHit] = Field(default_factory=list)
    resolved_input_entities: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


# --- Helpers ---


def _coerce_query(query: SlayerQuery | dict) -> SlayerQuery:
    if isinstance(query, SlayerQuery):
        return query
    if isinstance(query, dict):
        return SlayerQuery.model_validate(query)
    raise ValueError(
        f"query must be a SlayerQuery or dict; got {type(query).__name__}."
    )


def _dedup(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _filter_memories_by_datasource(
    *, memories: list[Memory], datasource: str | None,
) -> list[Memory]:
    """Keep memories with an entity rooted at ``datasource``; ``None`` is a no-op."""
    if datasource is None:
        return memories
    return [
        m for m in memories
        if any(
            canonical_id_rooted_at(canonical_id=e, datasource=datasource)
            for e in m.entities
        )
    ]


def _collect_memory_canonicals(memories: list[Memory]) -> set:
    return {f"{_MEMORY_PREFIX}{m.id}" for m in memories}


def _backfill_memory_by_id(
    *,
    memory_by_id: dict,
    all_memories_by_id: "dict[str, Memory]",
    mem_ids,
) -> None:
    """Add each ``mem_ids`` entry missing from ``memory_by_id`` (mutated) from ``all_memories_by_id``."""
    for mem_id in mem_ids:
        if mem_id in memory_by_id:
            continue
        mem = all_memories_by_id.get(mem_id)
        if mem is not None:
            memory_by_id[mem_id] = mem


def _build_memory_hit(
    *,
    mem: Memory,
    memory_id: str,
    score: float,
    text_by_id: dict[str, str],
    canonical_input_entities: list[str],
    valid_canonicals: set | None = None,
    compact: bool = True,
) -> SearchHit:
    """Build a memory SearchHit.

    ``matched_entities`` uses only live canonicals plus the implicit ``memory:<self_id>`` ref.
    Compact mode falls back to the learning's first paragraph for ``description`` and leaves ``text`` empty.
    """
    if valid_canonicals is not None:
        live_entities = [e for e in mem.entities if e in valid_canonicals]
    else:
        live_entities = list(mem.entities)
    self_ref = f"{_MEMORY_PREFIX}{memory_id}"
    if self_ref not in live_entities:
        live_entities.append(self_ref)
    wanted_set = set(canonical_input_entities)
    matched = sorted(wanted_set & set(live_entities)) if wanted_set else []
    if compact:
        description = (
            mem.description
            if mem.description
            else compact_description_from_learning(mem.learning)
        )
        text = ""
    else:
        description = mem.description
        text = text_by_id.get(memory_id) or mem.learning
    return SearchHit(
        kind="memory",
        id=memory_id,
        score=score,
        text=text,
        description=description,
        matched_entities=matched,
        query=mem.query,
    )


def _resolve_entity_hit_kind_text(
    *,
    canonical: str,
    corpus: Corpus | None,
    named_kind_text: dict[str, tuple[str, str, str | None]] | None,
) -> tuple[str, str, str | None] | None:
    """Resolve a canonical's ``(kind, text, description)`` from the corpus, else ``named_kind_text``."""
    if corpus is not None:
        kind = corpus.canonical_to_kind.get(canonical)
        text = corpus.canonical_to_text.get(canonical)
        if kind is not None and text is not None:
            description = corpus.canonical_to_description.get(canonical)
            return kind, text, description
    if named_kind_text is not None:
        triple = named_kind_text.get(canonical)
        if triple is not None:
            return triple
    return None


def _build_hit_from_fused_key(
    *,
    key: str,
    score: float,
    memory_by_id: dict,
    text_by_id: dict[str, str],
    canonical_input_entities: list[str],
    corpus: Corpus | None,
    named_kind_text: dict[str, tuple[str, str, str | None]] | None,
    valid_canonicals: set | None,
    candidate_ids: frozenset[str] | None,
    kind_filter: set[str] | None,
    compact: bool = True,
) -> SearchHit | None:
    """Build one SearchHit from a fused key, or ``None`` when the cypher_filter allowlists exclude it."""
    if key.startswith(_MEMORY_PREFIX):
        memory_id = key[len(_MEMORY_PREFIX):]
        if candidate_ids is not None and key not in candidate_ids:
            return None
        if kind_filter is not None and "memory" not in kind_filter:
            return None
        mem = memory_by_id.get(memory_id)
        if mem is None:
            return None
        return _build_memory_hit(
            mem=mem,
            memory_id=memory_id,
            score=score,
            text_by_id=text_by_id,
            canonical_input_entities=canonical_input_entities,
            valid_canonicals=valid_canonicals,
            compact=compact,
        )
    if candidate_ids is not None and key not in candidate_ids:
        return None
    resolved = _resolve_entity_hit_kind_text(
        canonical=key,
        corpus=corpus,
        named_kind_text=named_kind_text,
    )
    if resolved is None:
        return None
    kind, text, description = resolved
    if kind_filter is not None and kind not in kind_filter:
        return None
    return SearchHit(
        id=key,
        kind=kind,
        score=score,
        text="" if compact else text,
        description=description,
    )


def _fuse_all_hits(
    *,
    memory_rankings: list[list[str]],
    entity_rankings: list[list[str]],
    memory_by_id: dict,
    text_by_id: dict[str, str],
    canonical_input_entities: list[str],
    corpus: Corpus | None,
    named_kind_text: dict[str, tuple[str, str, str | None]] | None,
    max_results: int,
    valid_canonicals: set | None = None,
    candidate_ids: frozenset[str] | None = None,
    kind_filter: set[str] | None = None,
    compact: bool = True,
) -> list[SearchHit]:
    """RRF-fuse memory and entity rankings into one flat list.

    Allowlists apply before the ``max_results`` cap, so the cap counts surviving hits only.
    """
    prefixed_memory_rankings = [
        [f"{_MEMORY_PREFIX}{mid}" for mid in ranking]
        for ranking in memory_rankings
    ]
    all_rankings = prefixed_memory_rankings + entity_rankings
    non_empty = [r for r in all_rankings if r]
    fused = rrf_fuse(rankings=non_empty, k=_RRF_K) if non_empty else {}
    fused_sorted = sorted(fused.items(), key=lambda kv: kv[1], reverse=True)

    results: list[SearchHit] = []
    for key, score in fused_sorted:
        hit = _build_hit_from_fused_key(
            key=key,
            score=score,
            memory_by_id=memory_by_id,
            text_by_id=text_by_id,
            canonical_input_entities=canonical_input_entities,
            corpus=corpus,
            named_kind_text=named_kind_text,
            valid_canonicals=valid_canonicals,
            candidate_ids=candidate_ids,
            kind_filter=kind_filter,
            compact=compact,
        )
        if hit is not None:
            results.append(hit)
            if len(results) >= max_results:
                break
    return results


def _merge_text_by_id_in_declaration_order(
    results: list[RetrievalResult],
) -> dict[str, str]:
    """Merge ``text_by_id``; the first non-empty text in declaration order wins."""
    merged: dict[str, str] = {}
    for result in results:
        for mem_id, text in result.text_by_id.items():
            if mem_id not in merged and text:
                merged[mem_id] = text
    return merged


# --- Named-entity surfacing helpers ---


def _memory_id_off_datasource_warnings(
    *,
    canonical_input_entities: list[str],
    live_memory_ids: set[str],
    datasource: str | None,
) -> list[str]:
    """Warn per named ``memory:<id>`` ref dropped by the datasource pre-filter."""
    if datasource is None:
        return []
    out: list[str] = []
    for canonical in canonical_input_entities:
        if not canonical.startswith(_MEMORY_PREFIX):
            continue
        memory_id = canonical[len(_MEMORY_PREFIX):]
        if memory_id and memory_id not in live_memory_ids:
            out.append(
                f"{canonical} is not rooted at datasource "
                f"{datasource!r}; dropped."
            )
    return out


def _memory_id_cypher_filter_warnings(
    *,
    canonical_input_entities: list[str],
    candidate_ids: frozenset[str],
) -> list[str]:
    """Warn per named ``memory:<id>`` ref excluded by the cypher_filter allowlist."""
    return [
        f"{c!r} excluded by cypher_filter."
        for c in canonical_input_entities
        if c.startswith(_MEMORY_PREFIX) and c not in candidate_ids
    ]


async def _lookup_bare_datasource_canonical(
    *, ds: str, storage: StorageBackend, failures: DocumentLoadFailures,
) -> LookupResult:
    """Render a bare ``<ds>`` canonical, re-checking it still exists."""
    known = await storage.list_datasources()
    if ds not in known:
        return LookupMissing()
    models = failures.skip(await storage.load_models(data_source=ds))
    cfg = await storage.get_datasource(ds)
    ds_description = cfg.description if cfg is not None else None
    pair = render_datasource_pair(
        name=ds, models=models, description=ds_description,
    )
    return LookupFound(
        kind=pair.kind, text=pair.text, description=pair.description,
    )


async def _lookup_model_or_leaf_canonical(
    *,
    canonical: str,
    ds: str,
    model_name: str,
    leaf: str | None,
    storage: StorageBackend,
) -> LookupResult:
    """Render a ``<ds>.<model>[.<leaf>]`` canonical, or report it hidden / missing."""
    model = await storage.get_model_or_builtin(model_name, data_source=ds)
    if model is None:
        return LookupMissing()
    if model.hidden:
        return LookupHidden(reason="hidden model")
    for re in collect_model_entity_pairs(model=model):
        if re.canonical_id == canonical:
            return LookupFound(
                kind=re.kind, text=re.text, description=re.description,
            )
    if leaf is not None:
        for column in model.columns:
            if column.name == leaf and column.hidden:
                return LookupHidden(reason="hidden column")
    return LookupMissing()


async def _lookup_named_entity(
    *,
    canonical: str,
    storage: StorageBackend,
    corpus: Corpus | None,
    failures: DocumentLoadFailures | None = None,
) -> LookupResult:
    """Resolve a canonical to its ``(kind, text, description)`` for named-entity surfacing."""
    if corpus is not None:
        kind = corpus.canonical_to_kind.get(canonical)
        text = corpus.canonical_to_text.get(canonical)
        if kind is not None and text is not None:
            return LookupFound(
                kind=kind, text=text,
                description=corpus.canonical_to_description.get(canonical),
            )
    segments = canonical.split(".")
    if len(segments) == 1:
        return await _lookup_bare_datasource_canonical(
            ds=segments[0], storage=storage, failures=failures or DocumentLoadFailures(),
        )
    return await _lookup_model_or_leaf_canonical(
        canonical=canonical,
        ds=segments[0],
        model_name=segments[1],
        leaf=segments[2] if len(segments) >= 3 else None,
        storage=storage,
    )


# --- Column-hit refresh helpers ---


def _group_column_hits(
    results: list[SearchHit],
) -> dict[tuple[str, str], list[tuple[int, SearchHit, str]]]:
    """Group column hits by ``(data_source, model)`` as ``(result_index, hit, column_name)`` tuples."""
    groups: dict[tuple[str, str], list[tuple[int, SearchHit, str]]] = {}
    for idx, hit in enumerate(results):
        if hit.kind != "column":
            continue
        segments = hit.id.split(".")
        if len(segments) != 3:
            continue
        data_source, model_name, column_name = segments
        groups.setdefault((data_source, model_name), []).append(
            (idx, hit, column_name)
        )
    return groups


# --- Service ---


class SearchService:
    """Orchestrates the registered retrievers + RRF fusion."""

    def __init__(
        self,
        *,
        storage: StorageBackend,
        engine: SlayerQueryEngine | None = None,
        retrievers: list[Retriever] | None = None,
    ) -> None:
        """``engine`` is optional; without one the post-fusion column-hit refresh is a no-op."""
        self._storage = storage
        self._engine = engine
        self._retrievers: list[Retriever] = (
            list(retrievers) if retrievers is not None
            else self._default_retrievers(storage)
        )

    @staticmethod
    def _default_retrievers(storage: StorageBackend) -> list[Retriever]:
        return [
            BM25Retriever(),
            TantivyRetriever(),
            EmbeddingRetriever(storage=storage),
        ]

    @property
    def retrievers(self) -> list[Retriever]:
        return self._retrievers

    async def _refresh_stale_column_hits(
        self,
        *,
        results: list[SearchHit],
        compact: bool = True,
    ) -> list[SearchHit]:
        """Refresh column hits per ``(data_source, model)`` group, models in parallel.

        Under ``compact=True`` only ``description`` is refreshed; ``text`` stays ``""``.
        """
        assert self._engine is not None  # caller-guarded
        groups = _group_column_hits(results)
        if not groups:
            return results
        refreshed_by_idx: dict[int, SearchHit] = {}
        await asyncio.gather(*[
            self._refresh_group_worker(
                ds_name=ds, model_name=model_name,
                members=members, refreshed_by_idx=refreshed_by_idx,
                compact=compact,
            )
            for (ds, model_name), members in groups.items()
        ])
        if not refreshed_by_idx:
            return results
        return [
            refreshed_by_idx.get(i, h) for i, h in enumerate(results)
        ]

    async def _refresh_group_worker(
        self,
        *,
        ds_name: str,
        model_name: str,
        members: list[tuple[int, SearchHit, str]],
        refreshed_by_idx: dict[int, SearchHit],
        compact: bool = True,
    ) -> None:
        """Profile one model's column hits in a single owner call; write changed hits into ``refreshed_by_idx``."""
        assert self._engine is not None  # caller-guarded
        try:
            model = await self._storage.get_model(
                model_name, data_source=ds_name,
            )
        except Exception as exc:  # NOSONAR(S112) — best-effort
            logger.warning(
                "search refresh: failed to load model %s.%s: %s",
                ds_name, model_name, exc,
            )
            return
        if model is None:
            return
        hits = [
            (idx, hit, col) for idx, hit, column_name in members
            if (col := model.get_column(column_name)) is not None
        ]
        if not hits:
            return
        outcome = await ensure_samples_fresh(
            model=model, columns=[col for _, _, col in hits],
            engine=self._engine, storage=self._storage,
        )
        scoped = self._engine.policy is not None
        for (idx, hit, col), fresh in zip(hits, outcome.columns, strict=True):
            if fresh is col and not scoped:
                continue
            update: dict[str, Any] = {"description": fresh.description}
            if not compact:
                update["text"] = render_column_text(model=model, column=fresh)
            refreshed_by_idx[idx] = hit.model_copy(update=update)

    # --- Read side ---

    async def search(
        self,
        *,
        entities: list[str] | None = None,
        query: SlayerQuery | dict | None = None,
        question: str | None = None,
        datasource: str | None = None,
        cypher_filter: str | None = None,
        max_results: int = 10,
        compact: bool = True,
    ) -> SearchResponse:
        failures = DocumentLoadFailures()
        response = await self._search(
            entities=entities, query=query, question=question, datasource=datasource,
            cypher_filter=cypher_filter, max_results=max_results, compact=compact, failures=failures,
        )
        skipped = [w.human_message() for w in failures.warnings]
        return response.model_copy(update={"warnings": _dedup([*response.warnings, *skipped])}) if skipped else response

    async def _search(  # NOSONAR(S3776) — single orchestrator entry point; stages are linear and named
        self,
        *,
        entities: list[str] | None,
        query: SlayerQuery | dict | None,
        question: str | None,
        datasource: str | None,
        cypher_filter: str | None,
        max_results: int,
        compact: bool,
        failures: DocumentLoadFailures,
    ) -> SearchResponse:
        if max_results < 1:
            raise ValueError(
                f"max_results must be >= 1; got {max_results}."
            )
        await self._validate_datasource_known(datasource)

        canonical_input_entities, warnings = await self._resolve_inputs(
            entities=entities, query=query,
        )
        channel_1_active = (
            (entities is not None and len(entities) > 0) or query is not None
        )
        question_active = bool(question and question.strip())

        candidate_ids, kind_filter, early = await self._apply_cypher_filter(
            cypher_filter=cypher_filter,
            canonical_input_entities=canonical_input_entities,
            warnings=warnings,
            failures=failures,
        )
        if early is not None:
            return early
        if kind_filter is not None and "memory" not in kind_filter:
            for canonical in canonical_input_entities:
                if canonical.startswith(_MEMORY_PREFIX):
                    warnings.append(
                        f"{canonical} excluded by cypher_filter kind filter "
                        f"(allowed kinds: {sorted(kind_filter)!r})."
                    )

        if not channel_1_active and not question_active:
            return await self._recency_fallback(
                datasource=datasource,
                candidate_ids=candidate_ids,
                kind_filter=kind_filter,
                max_results=max_results,
                warnings=warnings,
                compact=compact,
                failures=failures,
            )

        # Datasource filter precedes cypher narrowing so each dropped ref gets the right warning.
        datasource_filtered_memories: list[Memory] = (
            _filter_memories_by_datasource(
                memories=await self._storage.list_memories(entities=None, failures=failures),
                datasource=datasource,
            )
        )
        warnings = _dedup(
            warnings + _memory_id_off_datasource_warnings(
                canonical_input_entities=canonical_input_entities,
                live_memory_ids={m.id for m in datasource_filtered_memories},
                datasource=datasource,
            )
        )
        if candidate_ids is not None:
            all_memories: list[Memory] = [
                m for m in datasource_filtered_memories
                if f"{_MEMORY_PREFIX}{m.id}" in candidate_ids
            ]
        else:
            all_memories = datasource_filtered_memories

        valid_canonicals = await self._valid_canonical_set(
            all_memories=all_memories, datasource=datasource, failures=failures,
        )

        corpus: Corpus | None = None
        if question_active:
            all_models, datasources, datasource_descriptions = (
                await self._collect_index_corpus(datasource=datasource, failures=failures)
            )
            corpus = build_in_memory_corpus(
                memories=all_memories,
                models=all_models,
                datasources=datasources,
                datasource_descriptions=datasource_descriptions,
            )

        if candidate_ids is not None:
            warnings = _dedup(
                warnings + _memory_id_cypher_filter_warnings(
                    canonical_input_entities=canonical_input_entities,
                    candidate_ids=candidate_ids,
                )
            )
        (
            channel_1_entity_ranking,
            named_kind_text,
            entity_surfacing_warnings,
        ) = await self._build_channel_1_entity_ranking(
            canonical_input_entities=canonical_input_entities,
            datasource=datasource,
            corpus=corpus,
            failures=failures,
            candidate_ids=candidate_ids,
        )
        warnings = _dedup(warnings + entity_surfacing_warnings)

        # A failing retriever becomes a warning (declaration order), never a crash.
        raw_results = await asyncio.gather(
            *(
                r.retrieve(
                    query_entities=canonical_input_entities,
                    question=question,
                    all_memories=all_memories,
                    valid_canonicals=valid_canonicals,
                    corpus=corpus,
                    datasource=datasource,
                )
                for r in self._retrievers
            ),
            return_exceptions=True,
        )
        results: list[RetrievalResult] = []
        for r, raw in zip(self._retrievers, raw_results):
            if isinstance(raw, BaseException):
                warnings.append(
                    f"retriever {r.name!r} retrieve raised: {raw}"
                )
                results.append(RetrievalResult())
            else:
                results.append(raw)
                warnings.extend(raw.warnings)
        warnings = _dedup(warnings)

        text_by_id = _merge_text_by_id_in_declaration_order(results)

        all_memories_by_id = {m.id: m for m in all_memories}
        memory_by_id: dict[str, Memory] = {}
        for result in results:
            _backfill_memory_by_id(
                memory_by_id=memory_by_id,
                all_memories_by_id=all_memories_by_id,
                mem_ids=result.memory_ranking,
            )

        all_hits = _fuse_all_hits(
            memory_rankings=[r.memory_ranking for r in results],
            entity_rankings=(
                [channel_1_entity_ranking]
                + [r.entity_ranking for r in results]
            ),
            memory_by_id=memory_by_id,
            text_by_id=text_by_id,
            canonical_input_entities=canonical_input_entities,
            corpus=corpus,
            named_kind_text=named_kind_text,
            max_results=max_results,
            valid_canonicals=valid_canonicals,
            candidate_ids=candidate_ids,
            kind_filter=kind_filter,
            compact=compact,
        )

        # Named memory refs get stale-query warnings even when the cap dropped their hit.
        query_bearing_hits = [
            h for h in all_hits
            if h.kind == "memory" and h.query is not None
        ]
        warnings = _dedup(
            warnings + await self._stale_query_warnings(
                query_bearing_hits=query_bearing_hits,
                memory_by_id=memory_by_id,
            ) + await self._stale_query_warnings_for_named_memory_refs(
                canonical_input_entities=canonical_input_entities,
                all_memories=all_memories,
                already_warned_ids={h.id for h in query_bearing_hits},
            )
        )

        if self._engine is not None:
            all_hits = await self._refresh_stale_column_hits(
                results=all_hits, compact=compact,
            )
        return SearchResponse(
            results=all_hits,
            resolved_input_entities=canonical_input_entities,
            warnings=warnings,
        )

    async def _apply_cypher_filter(
        self,
        *,
        cypher_filter: str | None,
        canonical_input_entities: list[str],
        warnings: list[str],
        failures: DocumentLoadFailures,
    ) -> tuple[
        frozenset[str] | None,
        set[str] | None,
        SearchResponse | None,
    ]:
        """Resolve ``cypher_filter`` into ``(candidate_ids, kind_filter, early)``.

        ``candidate_ids`` is the graph-path allowlist; ``kind_filter`` the naive fallback's kinds (graph
        extra absent); ``early`` an empty response when the graph path matched nothing.
        """
        if cypher_filter is None:
            return None, None, None
        if _search_graph.is_available():
            candidate_ids = await _search_graph.get_filtered_ids(
                cypher=cypher_filter, storage=self._storage, failures=failures,
            )
            if not candidate_ids:
                early_warnings = _dedup(
                    warnings + [
                        "cypher_filter returned no matching nodes; "
                        "search returned no results."
                    ]
                )
                return candidate_ids, None, SearchResponse(
                    results=[],
                    resolved_input_entities=canonical_input_entities,
                    warnings=early_warnings,
                )
            return candidate_ids, None, None
        return None, _parse_naive_cypher(cypher_filter), None

    async def _build_channel_1_entity_ranking(
        self,
        *,
        canonical_input_entities: list[str],
        datasource: str | None,
        corpus: Corpus | None,
        failures: DocumentLoadFailures,
        candidate_ids: frozenset[str] | None = None,
    ) -> tuple[list[str], dict[str, tuple[str, str, str | None]], list[str]]:
        """Surface each user-named canonical as itself: ``(entity_ranking, named_kind_text, warnings)``.

        ``named_kind_text`` backs canonicals the corpus lacks; every dropped ref is warned.
        """
        entity_ranking: list[str] = []
        named_kind_text: dict[str, tuple[str, str, str | None]] = {}
        warnings: list[str] = []
        for canonical in canonical_input_entities:
            if canonical.startswith(_MEMORY_PREFIX):
                # memory:<id> refs participate in the memory ranking only.
                continue
            if candidate_ids is not None and canonical not in candidate_ids:
                warnings.append(
                    f"entity {canonical!r} excluded by cypher_filter."
                )
                continue
            if datasource is not None and not canonical_id_rooted_at(
                canonical_id=canonical, datasource=datasource,
            ):
                warnings.append(
                    f"entity {canonical!r} is not rooted at datasource "
                    f"{datasource!r}; dropped from entities bucket."
                )
                continue
            result = await _lookup_named_entity(
                canonical=canonical, storage=self._storage, corpus=corpus, failures=failures,
            )
            if isinstance(result, LookupHidden):
                warnings.append(
                    f"entity {canonical!r} is on a hidden "
                    f"{result.reason.removeprefix('hidden ')}; "
                    f"dropped from entities bucket."
                )
                continue
            if isinstance(result, LookupMissing):
                warnings.append(
                    f"entity {canonical!r} resolved but is no longer "
                    f"present in storage; dropped from entities bucket."
                )
                continue
            entity_ranking.append(canonical)
            named_kind_text[canonical] = (
                result.kind, result.text, result.description,
            )
        return entity_ranking, named_kind_text, warnings

    # --- Write side ---

    async def upsert_memory(self, memory: Memory) -> list[str]:
        return await self._fan_out_with_isolation(
            hook_name="upsert_memory",
            invoke=lambda r: r.upsert_memory(memory),
        )

    async def refresh_model_subtree(
        self, model: SlayerModel,
    ) -> list[str]:
        return await self._fan_out_with_isolation(
            hook_name="refresh_model_subtree",
            invoke=lambda r: r.refresh_model_subtree(model),
        )

    async def refresh_datasource(
        self,
        *,
        name: str,
        models: list[SlayerModel],
        description: str | None = None,
    ) -> list[str]:
        return await self._fan_out_with_isolation(
            hook_name="refresh_datasource",
            invoke=lambda r: r.refresh_datasource(
                name=name, models=models, description=description,
            ),
        )

    async def _fan_out_with_isolation(
        self, *, hook_name: str, invoke,
    ) -> list[str]:
        """Call ``invoke`` on each retriever in order; failures become prefixed warnings."""
        warnings: list[str] = []
        for r in self._retrievers:
            try:
                warnings.extend(await invoke(r))
            except Exception as exc:  # NOSONAR(S112) — best-effort fan-out
                warnings.append(
                    f"retriever {r.name!r} {hook_name} raised: {exc}"
                )
        return _dedup(warnings)

    # --- Input resolution / corpus collection ---

    async def _validate_datasource_known(
        self, datasource: str | None,
    ) -> None:
        """Reject an unknown ``datasource`` before any corpus walk."""
        if datasource is None:
            return
        known = sorted(await self._storage.list_datasources())
        if datasource not in known:
            raise ValueError(
                f"datasource {datasource!r} not found; known: {known}."
            )

    async def _resolve_inputs(
        self,
        *,
        entities: list[str] | None,
        query: SlayerQuery | dict | None,
    ) -> tuple[list[str], list[str]]:
        """Resolve ``entities`` + ``query`` into deduped canonicals and warnings; per-token failures warn."""
        canonical: list[str] = []
        warnings: list[str] = []
        if entities:
            for raw in entities:
                if not isinstance(raw, str):
                    raise ValueError(
                        f"entities list items must be strings; got "
                        f"{type(raw).__name__}."
                    )
                try:
                    result = await resolve_entity(
                        raw=raw, storage=self._storage,
                    )
                except (EntityResolutionError, AmbiguousModelError) as exc:
                    warnings.append(f"entity {raw!r} dropped: {exc}")
                    continue
                canonical.extend(result.canonical_forms)
                warnings.extend(result.warnings)
        if query is not None:
            try:
                extraction = await extract_entities_from_query(
                    query=_coerce_query(query), storage=self._storage,
                )
            except (EntityResolutionError, AmbiguousModelError) as exc:
                warnings.append(f"query input dropped: {exc}")
            else:
                canonical.extend(extraction.canonical_forms)
                warnings.extend(extraction.warnings)
        return _dedup(canonical), _dedup(warnings)

    async def _recency_fallback(
        self,
        *,
        max_results: int,
        warnings: list[str],
        datasource: str | None = None,
        candidate_ids: frozenset[str] | None = None,
        failures: DocumentLoadFailures,
        kind_filter: set[str] | None = None,
        compact: bool = True,
    ) -> SearchResponse:
        """Empty-input branch: newest memories under the same datasource / cypher_filter narrowing; no retrievers."""
        warnings.append(
            "no entities, query, or question supplied; returning "
            "newest memories by recency."
        )
        recency_memories = _filter_memories_by_datasource(
            memories=await self._storage.list_memories(entities=None, failures=failures),
            datasource=datasource,
        )
        had_candidates_pre_filter = bool(recency_memories)
        if candidate_ids is not None:
            recency_memories = [
                m for m in recency_memories
                if f"{_MEMORY_PREFIX}{m.id}" in candidate_ids
            ]
        if kind_filter is not None and "memory" not in kind_filter:
            recency_memories = []
        # Distinguish "the filter emptied the pool" from an empty corpus.
        filters_excluded_all = (
            had_candidates_pre_filter
            and not recency_memories
            and (candidate_ids is not None or kind_filter is not None)
        )
        if filters_excluded_all:
            warnings.append(
                "cypher_filter excluded all memory candidates for the "
                "empty-input recency fallback; no results."
            )
        recency_memories.sort(key=lambda m: m.created_at, reverse=True)
        valid_canonicals = await self._valid_canonical_set(
            all_memories=recency_memories, datasource=datasource, failures=failures,
        )
        hits: list[SearchHit] = []
        for m in recency_memories:
            if len(hits) >= max_results:
                break
            hits.append(_build_memory_hit(
                mem=m,
                memory_id=m.id,
                score=0.0,
                text_by_id={},
                canonical_input_entities=[],
                valid_canonicals=valid_canonicals,
                compact=compact,
            ))
        memory_by_id = {m.id: m for m in recency_memories}
        query_bearing = [h for h in hits if h.query is not None]
        warnings = _dedup(
            warnings + await self._stale_query_warnings(
                query_bearing_hits=query_bearing,
                memory_by_id=memory_by_id,
            )
        )
        return SearchResponse(
            results=hits,
            resolved_input_entities=[],
            warnings=warnings,
        )

    async def _valid_canonical_set(
        self,
        *,
        all_memories: list[Memory],
        datasource: str | None,
        failures: DocumentLoadFailures,
    ) -> set:
        canonicals: set = set()
        canonicals.update(
            await self._collect_datasource_canonicals(datasource=datasource)
        )
        canonicals.update(
            await self._collect_model_subtree_canonicals(
                datasource=datasource, failures=failures,
            )
        )
        canonicals.update(_collect_memory_canonicals(all_memories))
        return canonicals

    async def _collect_datasource_canonicals(
        self, *, datasource: str | None,
    ) -> set:
        names = await self._storage.list_datasources()
        if datasource is not None:
            names = [d for d in names if d == datasource]
        return set(names)

    async def _collect_model_subtree_canonicals(
        self, *, datasource: str | None, failures: DocumentLoadFailures,
    ) -> set:
        out: set = {
            f"{ds}.{name}" for ds, name in await self._storage._list_all_model_identities()
            if datasource is None or ds == datasource
        }
        for model in failures.skip(await self._storage.load_models(data_source=datasource)):
            ds, name = model.data_source, model.name
            for column in model.columns:
                out.add(f"{ds}.{name}.{column.name}")
            for measure in model.measures:
                if measure.name is None:
                    continue
                out.add(f"{ds}.{name}.{measure.name}")
            for agg in model.aggregations:
                out.add(f"{ds}.{name}.{agg.name}")
        return out

    async def _stale_query_warnings(
        self,
        *,
        query_bearing_hits: list[SearchHit],
        memory_by_id: dict[str, Memory],
    ) -> list[str]:
        """Warn per surfaced query-bearing hit whose attached query no longer resolves."""
        out: list[str] = []
        for hit in query_bearing_hits:
            mem = memory_by_id.get(hit.id)
            if mem is None or mem.query is None:
                continue
            try:
                await extract_entities_from_query(
                    query=mem.query, storage=self._storage,
                )
            except (EntityResolutionError, AmbiguousModelError) as exc:
                out.append(
                    f"example_query {_MEMORY_PREFIX}{hit.id}: attached "
                    f"query has stale references ({exc}); re-save to clean."
                )
        return out

    async def _stale_query_warnings_for_named_memory_refs(
        self,
        *,
        canonical_input_entities: list[str],
        all_memories: list[Memory],
        already_warned_ids: set[str],
    ) -> list[str]:
        """Stale-query warnings for named ``memory:<id>`` refs, even when the cap dropped the hit."""
        memories_by_id = {m.id: m for m in all_memories}
        out: list[str] = []
        for canonical in canonical_input_entities:
            if not canonical.startswith(_MEMORY_PREFIX):
                continue
            memory_id = canonical[len(_MEMORY_PREFIX):]
            if not memory_id or memory_id in already_warned_ids:
                continue
            mem = memories_by_id.get(memory_id)
            if mem is None or mem.query is None:
                continue
            try:
                await extract_entities_from_query(
                    query=mem.query, storage=self._storage,
                )
            except (EntityResolutionError, AmbiguousModelError) as exc:
                out.append(
                    f"example_query {_MEMORY_PREFIX}{memory_id}: attached "
                    f"query has stale references ({exc}); re-save to clean."
                )
        return out

    async def _collect_index_corpus(
        self,
        *,
        failures: DocumentLoadFailures,
        datasource: str | None = None,
    ) -> tuple[list[SlayerModel], list[str], dict[str, str | None]]:
        """Return ``(models, datasources, {ds_name: description})`` for the corpus build."""
        datasources = await self._storage.list_datasources()
        if datasource is not None:
            datasources = [d for d in datasources if d == datasource]
        models = failures.skip(await self._storage.load_models(data_source=datasource))
        for ds_name in datasources:
            models.extend(await self._storage.builtin_models(ds_name))
        descriptions: dict[str, str | None] = {}
        for ds_name in datasources:
            cfg = await self._storage.get_datasource(ds_name)
            descriptions[ds_name] = cfg.description if cfg is not None else None
        return models, datasources, descriptions


async def handle_edit_refresh(
    *,
    engine: SlayerQueryEngine,
    storage: StorageBackend,
    data_source: str,
    model_name: str,
    changed_columns: set[str],
    model_level_change: bool,
) -> list[str]:
    """``edit_model`` refresh: re-sample changed columns (all on a model-level change), then re-embed; failures warn."""
    model = await storage.get_model(name=model_name, data_source=data_source)
    if model is None:
        return [f"model {model_name!r} not found in datasource {data_source!r}"]
    only = None if model_level_change else changed_columns
    warnings = await refresh_table_backed_model_sampled(
        model=model, engine=engine, storage=storage, only_columns=only,
    )
    # Reload so the embedding text matches the refreshed model's content_hash.
    reloaded = await storage.get_model(name=model_name, data_source=data_source)
    if reloaded is not None:
        warnings.extend(await SearchService(storage=storage).refresh_model_subtree(reloaded))
    return warnings


__all__ = [
    "LookupFound",
    "LookupHidden",
    "LookupMissing",
    "SearchHit",
    "SearchResponse",
    "SearchService",
    "handle_edit_refresh",
]
