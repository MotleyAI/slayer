"""SearchService — facade orchestrator over a list of
:class:`~slayer.search.retriever.Retriever` instances (DEV-1514).

The orchestrator owns:

* Input validation (``max_*`` non-negative, ``datasource`` known).
* Lenient input-entity resolution (per-token failures → warnings).
* Recency fallback when no channel is active.
* One-shot ``all_memories`` fetch (datasource-filtered).
* One-shot ``valid_canonicals`` set build (datasources + persisted
  model identities + memory canonical ids).
* One-shot ``corpus`` build when ``question`` is active.
* Parallel fan-out across retrievers via ``asyncio.gather``.
* RRF fusion (``k=60``) over memory rankings and entity rankings.
* Bucket partitioning (``MemoryHit`` vs ``ExampleQueryHit``,
  each capped independently — per-bucket invariance, DEV-1414).
* Stale-``Memory.query`` warnings.

Each registered retriever runs ONCE per search call, returning a
combined :class:`RetrievalResult` with both memory and entity rankings.
The default retriever list is ``[BM25Retriever, TantivyRetriever,
EmbeddingRetriever]``; callers may inject any list via the
``retrievers=`` kwarg.

Write-side (``upsert_memory`` / ``refresh_model_subtree`` /
``refresh_datasource``): fans the call out to every registered
retriever, isolating per-retriever exceptions as prefixed warnings so
the fan-out always reaches the last retriever (Codex Finding 4).
Warning aggregation is deterministic — declared retriever order, not
gather completion order (Codex Finding 5).

This PR deliberately does NOT expose ``delete_*`` public methods:
:class:`StorageBackend` owns embedding-row cascade transactionally
with the row delete; adding retriever fan-out would create a second
deletion path on top. The :class:`Retriever` ABC defines the delete
hooks for future use (persistent tantivy will override them).
"""

from __future__ import annotations

import asyncio
from typing import Dict, List, Optional, Set, Tuple, Union

from pydantic import BaseModel, Field

from slayer.core.errors import AmbiguousModelError, EntityResolutionError
from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.memories.models import MEMORY_CANONICAL_PREFIX as _MEMORY_PREFIX
from slayer.memories.models import Memory
from slayer.memories.resolver import (
    canonical_id_rooted_at,
    extract_entities_from_query,
    resolve_entity,
)
from slayer.search.index import Corpus, build_in_memory_corpus
from slayer.search.retriever import RetrievalResult, Retriever
from slayer.search.retrievers import (
    BM25Retriever,
    EmbeddingRetriever,
    TantivyRetriever,
)
from slayer.search.rrf import rrf_fuse
from slayer.storage.base import StorageBackend


_RRF_K = 60


# ---------------------------------------------------------------------------
# Hit & response models (public — unchanged shape)
# ---------------------------------------------------------------------------


class MemoryHit(BaseModel):
    """A learning-only memory result (``Memory.query is None``). ``id``
    is the string memory id (suitable for ``forget_memory(id=hit.id)``).
    ``score`` is always the Reciprocal-Rank-Fusion score
    (``Σ 1 / (k + rank)``, ``k=60``); even single-channel searches go
    through RRF, so the value is comparable across channels but is not
    directly the raw BM25 / tantivy / cosine score."""

    id: str
    score: float
    text: str
    matched_entities: List[str] = Field(default_factory=list)


class ExampleQueryHit(BaseModel):
    """A query-bearing memory result (``Memory.query`` is set). Same id
    / score / text shape as ``MemoryHit`` but always carries the attached
    ``SlayerQuery``. Surfaces in ``SearchResponse.example_queries`` —
    bulky reference material, capped independently from learning-only
    memories so it cannot crowd them out."""

    id: str
    score: float
    text: str
    matched_entities: List[str] = Field(default_factory=list)
    query: SlayerQuery


class EntityHit(BaseModel):
    """An entity result. ``id`` is the canonical entity string
    (``"<ds>"``, ``"<ds>.<model>"``, or ``"<ds>.<model>.<leaf>"``).
    ``score`` is the RRF-fused score across retrievers that contributed
    an entity ranking."""

    id: str
    kind: str  # "datasource" | "model" | "column" | "measure" | "aggregation"
    score: float
    text: str


class SearchResponse(BaseModel):
    memories: List[MemoryHit] = Field(default_factory=list)
    example_queries: List[ExampleQueryHit] = Field(default_factory=list)
    entities: List[EntityHit] = Field(default_factory=list)
    resolved_input_entities: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _coerce_query(query: Union[SlayerQuery, dict]) -> SlayerQuery:
    if isinstance(query, SlayerQuery):
        return query
    if isinstance(query, dict):
        return SlayerQuery.model_validate(query)
    raise ValueError(
        f"query must be a SlayerQuery or dict; got {type(query).__name__}."
    )


def _dedup(items: List[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _filter_memories_by_datasource(
    memories: List[Memory], datasource: Optional[str],
) -> List[Memory]:
    """DEV-1409: keep memories with at least one entity rooted at
    ``datasource``. ``datasource=None`` is a no-op identity filter so
    callers can call this unconditionally."""
    if datasource is None:
        return memories
    return [
        m for m in memories
        if any(
            canonical_id_rooted_at(canonical_id=e, datasource=datasource)
            for e in m.entities
        )
    ]


def _collect_memory_canonicals(memories: List[Memory]) -> set:
    return {f"{_MEMORY_PREFIX}{m.id}" for m in memories}


def _backfill_memory_by_id(
    *,
    memory_by_id: dict,
    all_memories_by_id: "dict[str, Memory]",
    mem_ids,
) -> None:
    """For each id in ``mem_ids`` not already in ``memory_by_id``,
    look it up in ``all_memories_by_id`` and insert it. Mutates
    ``memory_by_id``. Takes a precomputed id→Memory dict (not the raw
    list) so per-call backfill stays O(N) (DEV-1414)."""
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
    text_by_id: Dict[str, str],
    canonical_input_entities: List[str],
    valid_canonicals: Optional[set] = None,
) -> Union[MemoryHit, ExampleQueryHit]:
    """Build the appropriate hit type for ``mem``: ``MemoryHit`` for
    learning-only memories (``query is None``), ``ExampleQueryHit`` for
    query-bearing ones. ``text`` falls back to ``mem.learning`` when no
    retriever supplied a hit text for this memory.

    DEV-1428: ``matched_entities`` is computed against the LIVE
    canonical set when ``valid_canonicals`` is supplied, so stale tags
    do not surface to the agent."""
    if valid_canonicals is not None:
        live_entities = [e for e in mem.entities if e in valid_canonicals]
    else:
        live_entities = list(mem.entities)
    wanted_set = set(canonical_input_entities)
    matched = sorted(wanted_set & set(live_entities)) if wanted_set else []
    text = text_by_id.get(memory_id) or mem.learning
    if mem.query is None:
        return MemoryHit(
            id=memory_id, score=score, text=text,
            matched_entities=matched,
        )
    return ExampleQueryHit(
        id=memory_id, score=score, text=text,
        matched_entities=matched, query=mem.query,
    )


def _fuse_memory_hits(
    *,
    rankings: List[List[str]],
    memory_by_id: dict,
    text_by_id: Dict[str, str],
    canonical_input_entities: List[str],
    max_memories: int,
    max_example_queries: int,
    valid_canonicals: Optional[set] = None,
) -> Tuple[List[MemoryHit], List[ExampleQueryHit]]:
    """RRF-fuse the supplied memory rankings and partition into
    learning-only (``MemoryHit``) vs query-bearing (``ExampleQueryHit``)
    lists, each capped independently. Empty inner rankings are filtered
    out so single-channel results still flow through RRF
    normalisation."""
    non_empty = [r for r in rankings if r]
    fused = rrf_fuse(rankings=non_empty, k=_RRF_K) if non_empty else {}
    fused_sorted = sorted(fused.items(), key=lambda kv: kv[1], reverse=True)

    learnings: List[MemoryHit] = []
    examples: List[ExampleQueryHit] = []
    for memory_id, score in fused_sorted:
        mem = memory_by_id.get(memory_id)
        if mem is None:
            continue
        hit = _build_memory_hit(
            mem=mem,
            memory_id=memory_id,
            score=score,
            text_by_id=text_by_id,
            canonical_input_entities=canonical_input_entities,
            valid_canonicals=valid_canonicals,
        )
        if isinstance(hit, MemoryHit) and len(learnings) < max_memories:
            learnings.append(hit)
        elif (
            isinstance(hit, ExampleQueryHit)
            and len(examples) < max_example_queries
        ):
            examples.append(hit)
        if (
            len(learnings) >= max_memories
            and len(examples) >= max_example_queries
        ):
            break
    return learnings, examples


def _fuse_entity_hits(
    *,
    rankings: List[List[str]],
    corpus: Optional[Corpus],
    max_entities: int,
) -> List[EntityHit]:
    """RRF-fuse the entity rankings and look text/kind up from the
    corpus map. Returns at most ``max_entities`` hits."""
    if corpus is None:
        return []
    non_empty = [r for r in rankings if r]
    fused = rrf_fuse(rankings=non_empty, k=_RRF_K) if non_empty else {}
    fused_sorted = sorted(fused.items(), key=lambda kv: kv[1], reverse=True)
    out: List[EntityHit] = []
    for canonical, score in fused_sorted:
        if len(out) >= max_entities:
            break
        kind = corpus.canonical_to_kind.get(canonical)
        text = corpus.canonical_to_text.get(canonical)
        if kind is None or text is None:
            continue
        out.append(EntityHit(
            id=canonical, kind=kind, score=score, text=text,
        ))
    return out


def _merge_text_by_id_in_declaration_order(
    results: List[RetrievalResult],
) -> Dict[str, str]:
    """Merge ``text_by_id`` across retriever results. First-non-empty
    in retriever declaration order wins per memory id (Codex Finding 6).
    """
    merged: Dict[str, str] = {}
    for result in results:
        for mem_id, text in result.text_by_id.items():
            if mem_id not in merged and text:
                merged[mem_id] = text
    return merged


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class SearchService:
    """Orchestrates the registered retrievers + RRF fusion."""

    def __init__(
        self,
        *,
        storage: StorageBackend,
        retrievers: Optional[List[Retriever]] = None,
    ) -> None:
        self._storage = storage
        self._retrievers: List[Retriever] = (
            list(retrievers) if retrievers is not None
            else self._default_retrievers(storage)
        )

    @staticmethod
    def _default_retrievers(storage: StorageBackend) -> List[Retriever]:
        return [
            BM25Retriever(),
            TantivyRetriever(),
            EmbeddingRetriever(storage=storage),
        ]

    @property
    def retrievers(self) -> List[Retriever]:
        return self._retrievers

    # ------------------------------------------------------------------
    # Read side — search()
    # ------------------------------------------------------------------

    async def search(
        self,
        *,
        entities: Optional[List[str]] = None,
        query: Optional[Union[SlayerQuery, dict]] = None,
        question: Optional[str] = None,
        datasource: Optional[str] = None,
        max_memories: int = 5,
        max_example_queries: int = 2,
        max_entities: int = 5,
    ) -> SearchResponse:
        if max_memories < 0:
            raise ValueError(
                f"max_memories must be >= 0; got {max_memories}."
            )
        if max_example_queries < 0:
            raise ValueError(
                f"max_example_queries must be >= 0; got "
                f"{max_example_queries}."
            )
        if max_entities < 0:
            raise ValueError(
                f"max_entities must be >= 0; got {max_entities}."
            )
        await self._validate_datasource_known(datasource)

        canonical_input_entities, warnings = await self._resolve_inputs(
            entities=entities, query=query,
        )
        channel_1_active = (
            (entities is not None and len(entities) > 0) or query is not None
        )
        question_active = bool(question and question.strip())

        if not channel_1_active and not question_active:
            return await self._recency_fallback(
                datasource=datasource,
                max_memories=max_memories,
                max_example_queries=max_example_queries,
                warnings=warnings,
            )

        all_memories: List[Memory] = _filter_memories_by_datasource(
            await self._storage.list_memories(entities=None),
            datasource,
        )

        valid_canonicals = await self._valid_canonical_set(
            all_memories=all_memories, datasource=datasource,
        )

        corpus: Optional[Corpus] = None
        if question_active:
            all_models, datasources = await self._collect_index_corpus(
                datasource=datasource,
            )
            corpus = build_in_memory_corpus(
                memories=all_memories,
                models=all_models,
                datasources=datasources,
            )

        # Fan out to every retriever in parallel. Per-retriever
        # exceptions are isolated and converted to prefixed warnings
        # in declaration order so a single failure can't crash the
        # whole search.
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
        results: List[RetrievalResult] = []
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

        # Merge text_by_id with first-non-empty-wins precedence.
        text_by_id = _merge_text_by_id_in_declaration_order(results)

        # Build memory_by_id from all retrievers' memory rankings.
        all_memories_by_id = {m.id: m for m in all_memories}
        memory_by_id: dict[str, Memory] = {}
        for result in results:
            _backfill_memory_by_id(
                memory_by_id=memory_by_id,
                all_memories_by_id=all_memories_by_id,
                mem_ids=result.memory_ranking,
            )

        memory_hits, example_query_hits = _fuse_memory_hits(
            rankings=[r.memory_ranking for r in results],
            memory_by_id=memory_by_id,
            text_by_id=text_by_id,
            canonical_input_entities=canonical_input_entities,
            max_memories=max_memories,
            max_example_queries=max_example_queries,
            valid_canonicals=valid_canonicals,
        )
        warnings = _dedup(
            warnings + await self._stale_query_warnings(
                example_query_hits=example_query_hits,
                memory_by_id=memory_by_id,
            )
        )
        entity_hits = _fuse_entity_hits(
            rankings=[r.entity_ranking for r in results],
            corpus=corpus,
            max_entities=max_entities,
        )
        return SearchResponse(
            memories=memory_hits,
            example_queries=example_query_hits,
            entities=entity_hits,
            resolved_input_entities=canonical_input_entities,
            warnings=warnings,
        )

    # ------------------------------------------------------------------
    # Write side — fan-out to retrievers
    # ------------------------------------------------------------------

    async def upsert_memory(self, memory: Memory) -> List[str]:
        return await self._fan_out_with_isolation(
            hook_name="upsert_memory",
            invoke=lambda r: r.upsert_memory(memory),
        )

    async def refresh_model_subtree(
        self, model: SlayerModel,
    ) -> List[str]:
        return await self._fan_out_with_isolation(
            hook_name="refresh_model_subtree",
            invoke=lambda r: r.refresh_model_subtree(model),
        )

    async def refresh_datasource(
        self, *, name: str, models: List[SlayerModel],
    ) -> List[str]:
        return await self._fan_out_with_isolation(
            hook_name="refresh_datasource",
            invoke=lambda r: r.refresh_datasource(name=name, models=models),
        )

    async def _fan_out_with_isolation(
        self, *, hook_name: str, invoke,
    ) -> List[str]:
        """Call ``invoke(retriever)`` on every registered retriever in
        declaration order, isolating per-retriever exceptions as
        prefixed warnings so subsequent retrievers still run. Returns
        the deduped warning list."""
        warnings: List[str] = []
        for r in self._retrievers:
            try:
                warnings.extend(await invoke(r))
            except Exception as exc:  # NOSONAR(S112) — best-effort fan-out
                warnings.append(
                    f"retriever {r.name!r} {hook_name} raised: {exc}"
                )
        return _dedup(warnings)

    # ------------------------------------------------------------------
    # Internal — input resolution / corpus collection
    # ------------------------------------------------------------------

    async def _validate_datasource_known(
        self, datasource: Optional[str],
    ) -> None:
        """DEV-1409: reject typos in ``datasource`` before any corpus
        walk."""
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
        entities: Optional[List[str]],
        query: Optional[Union[SlayerQuery, dict]],
    ) -> Tuple[List[str], List[str]]:
        """Walk ``entities`` + ``query`` into a deduped canonical-entity
        list plus a deduped warning list. DEV-1428: lenient —
        per-token failures become warnings."""
        canonical: List[str] = []
        warnings: List[str] = []
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
        max_memories: int,
        max_example_queries: int,
        warnings: List[str],
        datasource: Optional[str] = None,
    ) -> SearchResponse:
        """Empty-input branch: partition all memories by recency into
        the learning-only bucket and the query-bearing bucket. No
        retriever is invoked on this path."""
        warnings.append(
            "no entities, query, or question supplied; returning "
            "newest memories by recency."
        )
        recency_memories = _filter_memories_by_datasource(
            await self._storage.list_memories(entities=None),
            datasource,
        )
        recency_memories.sort(key=lambda m: m.created_at, reverse=True)
        valid_canonicals = await self._valid_canonical_set(
            all_memories=recency_memories, datasource=datasource,
        )
        memory_hits: List[MemoryHit] = []
        example_query_hits: List[ExampleQueryHit] = []
        for m in recency_memories:
            hit = _build_memory_hit(
                mem=m,
                memory_id=m.id,
                score=0.0,
                text_by_id={},
                canonical_input_entities=[],
                valid_canonicals=valid_canonicals,
            )
            if (
                isinstance(hit, MemoryHit)
                and len(memory_hits) < max_memories
            ):
                memory_hits.append(hit)
            elif (
                isinstance(hit, ExampleQueryHit)
                and len(example_query_hits) < max_example_queries
            ):
                example_query_hits.append(hit)
            if (
                len(memory_hits) >= max_memories
                and len(example_query_hits) >= max_example_queries
            ):
                break
        memory_by_id = {m.id: m for m in recency_memories}
        warnings = _dedup(
            warnings + await self._stale_query_warnings(
                example_query_hits=example_query_hits,
                memory_by_id=memory_by_id,
            )
        )
        return SearchResponse(
            memories=memory_hits,
            example_queries=example_query_hits,
            entities=[],
            resolved_input_entities=[],
            warnings=warnings,
        )

    async def _valid_canonical_set(
        self,
        *,
        all_memories: List[Memory],
        datasource: Optional[str],
    ) -> set:
        canonicals: set = set()
        canonicals.update(
            await self._collect_datasource_canonicals(datasource=datasource)
        )
        canonicals.update(
            await self._collect_model_subtree_canonicals(
                datasource=datasource,
            )
        )
        canonicals.update(_collect_memory_canonicals(all_memories))
        return canonicals

    async def _collect_datasource_canonicals(
        self, *, datasource: Optional[str],
    ) -> set:
        names = await self._storage.list_datasources()
        if datasource is not None:
            names = [d for d in names if d == datasource]
        return set(names)

    async def _collect_model_subtree_canonicals(
        self, *, datasource: Optional[str],
    ) -> set:
        out: set = set()
        identities = await self._storage._list_all_model_identities()
        for ds, name in identities:
            if datasource is not None and ds != datasource:
                continue
            out.add(f"{ds}.{name}")
            model = await self._storage.get_model(name, data_source=ds)
            if model is None:
                continue
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
        example_query_hits: List[ExampleQueryHit],
        memory_by_id: Dict[str, Memory],
    ) -> List[str]:
        out: List[str] = []
        for hit in example_query_hits:
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

    async def _collect_index_corpus(
        self,
        *,
        datasource: Optional[str] = None,
    ) -> Tuple[List[SlayerModel], List[str]]:
        datasources = await self._storage.list_datasources()
        if datasource is not None:
            datasources = [d for d in datasources if d == datasource]
        models: List[SlayerModel] = []
        identities = await self._storage._list_all_model_identities()
        for ds, name in identities:
            if datasource is not None and ds != datasource:
                continue
            m = await self._storage.get_model(name, data_source=ds)
            if m is not None:
                models.append(m)
        return models, datasources


__all__ = [
    "EntityHit",
    "ExampleQueryHit",
    "MemoryHit",
    "SearchResponse",
    "SearchService",
]
