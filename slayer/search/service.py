"""SearchService — facade orchestrator over a list of
:class:`~slayer.search.retriever.Retriever` instances (DEV-1514) with a
unified flat-results interface (DEV-1532) and an optional graph-backed
Cypher pre-filter (DEV-1464).

The orchestrator owns:

* Input validation (``max_results`` >= 1, ``datasource`` known).
* Lenient input-entity resolution (per-token failures → warnings).
* Optional ``cypher_filter`` pre-filter — when set, the result of
  the openCypher / naive-fallback query becomes a hard allowlist
  applied across every channel (DEV-1464).
* Recency fallback when no channel is active.
* One-shot ``all_memories`` fetch (datasource-filtered, then
  cypher_filter-narrowed when applicable).
* One-shot ``valid_canonicals`` set build (datasources + persisted
  model identities + memory canonical ids).
* One-shot ``corpus`` build when ``question`` is active.
* Parallel fan-out across retrievers via ``asyncio.gather``.
* Channel-1 named-entity surfacing (DEV-1513): every user-supplied
  canonical entity ref is contributed to the entity ranking as itself
  (subject to datasource / hidden / missing / cypher_filter checks),
  so an explicit ``entities=["<ds>.<model>"]`` surfaces that entity
  at the top of the results even without a fuzzy ``question``.
* RRF fusion (``k=60``) over memory + entity rankings, collapsed into
  a single flat ``results: List[SearchHit]`` list capped at
  ``max_results`` (DEV-1532). ``kind`` distinguishes memories from
  entity hits; ``query`` is populated for query-bearing memory hits.
* Post-fusion cypher_filter / kind_filter narrowing (DEV-1464) —
  candidates outside the allowlist (full graph path) or outside the
  naive kind list (fallback path) are dropped before the
  ``max_results`` cap, so the cap always counts surviving items only.
* Post-fusion column-hit refresh (DEV-1516) — categorical column
  hits with stale ``sampled_values`` are re-profiled inline via
  :func:`slayer.engine.profiling.ensure_column_sample_fresh` so the
  surfaced text reflects live values. Per-model writes serialise
  (storage's ``update_column_sampled`` is a model-level
  read-modify-write); cross-model writes parallelise via
  ``asyncio.gather``. Silently no-op when ``engine`` is None.
* Stale-``Memory.query`` warnings.

Each registered retriever runs ONCE per search call, returning a
combined :class:`RetrievalResult` with both memory and entity rankings.
The default retriever list is ``[BM25Retriever, TantivyRetriever,
EmbeddingRetriever]``; callers may inject any list via the
``retrievers=`` kwarg.

Ranking stability (DEV-1414): because each retriever produces a full
per-kind ranking — never truncated by a shared candidate-pool budget —
the relative order of any subset of the flat list is stable. Changing
only ``max_results`` never reorders existing entries nor causes an
entry to appear or disappear unless the cap boundary moves past it.

Write-side (``upsert_memory`` / ``refresh_model_subtree`` /
``refresh_datasource``): fans the call out to every registered
retriever, isolating per-retriever exceptions as prefixed warnings so
the fan-out always reaches the last retriever. Warning aggregation is
deterministic — declared retriever order, not gather completion order.

This module deliberately does NOT expose ``delete_*`` public methods:
:class:`StorageBackend` owns embedding-row cascade transactionally
with the row delete; adding retriever fan-out would create a second
deletion path on top. The :class:`Retriever` ABC defines the delete
hooks for future use (persistent tantivy will override them).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from pydantic import BaseModel, Field

from slayer.core.errors import AmbiguousModelError, EntityResolutionError
from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.profiling import ensure_column_sample_fresh
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


logger = logging.getLogger(__name__)


_RRF_K = 60


# ---------------------------------------------------------------------------
# Hit & response models
# ---------------------------------------------------------------------------


class SearchHit(BaseModel):
    """A unified search result (DEV-1532). ``kind`` is ``"memory"`` for
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

    DEV-1549: ``description`` carries a compact preview. For memory
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


# ---------------------------------------------------------------------------
# Lookup result for named-entity surfacing (DEV-1513)
# ---------------------------------------------------------------------------


class LookupFound(BaseModel):
    """``_lookup_named_entity`` succeeded; carries ``(kind, text,
    description)``. DEV-1549: ``description`` is the entity's
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
    """Unified search response (DEV-1532). ``results`` is a single flat
    list ranked by RRF score; consumers partition by ``kind`` (or by
    ``query is None`` for the memory subset) at the call site."""

    results: list[SearchHit] = Field(default_factory=list)
    resolved_input_entities: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def _collect_memory_canonicals(memories: list[Memory]) -> set:
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
    list) so per-call backfill stays O(N) instead of O(N²) when every
    retriever returns the full memory corpus (DEV-1414)."""
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
    """Build a SearchHit for a memory (DEV-1532 unified shape).

    ``text`` falls back to ``mem.learning`` when no retriever supplied
    a hit text for this memory.

    DEV-1428: ``matched_entities`` is computed against the LIVE
    canonical set when ``valid_canonicals`` is supplied, so stale tags
    do not surface to the agent.

    DEV-1513: every memory has an implicit ``memory:<self_id>``
    self-reference; it appears in ``matched_entities`` only when the
    user explicitly named that ref (so the surfaced memory honestly
    shows the reason it was returned).

    DEV-1549 (compact):
    * ``compact=True``  → ``description`` = ``mem.description`` if set,
      else the first-paragraph fallback; ``text = ""``.
    * ``compact=False`` → ``description`` = ``mem.description`` (or
      ``None``; no fallback); ``text`` = full learning rendering.
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
    """DEV-1513 / DEV-1549: resolve one canonical's
    ``(kind, text, description)`` triple for an entity hit. Prefers the
    corpus (channels 2/3 already built it); falls back to the channel-1
    ``named_kind_text`` lookup (used on pure-named calls with no
    corpus). Returns ``None`` when neither source carries the canonical.
    """
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
    """Build one SearchHit from a fused (key, score) pair, or return None
    to skip. Applies the DEV-1464 cypher_filter (candidate_ids allowlist
    for the full graph path; kind_filter for the naive fallback) BEFORE
    materialising the hit, so the upstream cap counts surviving items
    only.

    DEV-1549: ``compact`` flips memory + entity hit rendering between
    description-only and description+full-text shapes.
    """
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
    # Entity key.
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
    """RRF-fuse memory and entity rankings into a single flat list
    (DEV-1532). Memory IDs are prefixed with the canonical memory prefix
    so the unified pool contains no key collisions.

    DEV-1464: ``candidate_ids`` (full-graph allowlist) and
    ``kind_filter`` (naive-fallback kind allowlist) are applied BEFORE
    the ``max_results`` cap so the cap always counts surviving items
    only — a wrong implementation that filters AFTER capping would
    silently drop matching results when an unrelated hit happens to
    out-rank them."""
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
    """Merge ``text_by_id`` across retriever results. First-non-empty
    in retriever declaration order wins per memory id."""
    merged: dict[str, str] = {}
    for result in results:
        for mem_id, text in result.text_by_id.items():
            if mem_id not in merged and text:
                merged[mem_id] = text
    return merged


# ---------------------------------------------------------------------------
# DEV-1513 / DEV-1464: named-entity surfacing helpers
# ---------------------------------------------------------------------------


def _memory_id_off_datasource_warnings(
    *,
    canonical_input_entities: list[str],
    live_memory_ids: set[str],
    datasource: str | None,
) -> list[str]:
    """DEV-1513: emit one warning per user-supplied ``memory:<id>`` ref
    whose memory was dropped by the datasource pre-filter (the memory
    has no entities rooted at ``datasource``). Mirrors the entity-side
    off-ds drop on the memory side.

    No-op when ``datasource`` is None (nothing was filtered out)."""
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
    """DEV-1464: emit one warning per user-supplied ``memory:<id>`` ref
    that was excluded by the cypher_filter allowlist (the graph query
    did not return that memory's canonical id)."""
    return [
        f"{c!r} excluded by cypher_filter."
        for c in canonical_input_entities
        if c.startswith(_MEMORY_PREFIX) and c not in candidate_ids
    ]


async def _lookup_bare_datasource_canonical(
    *, ds: str, storage: StorageBackend,
) -> LookupResult:
    """DEV-1513: bare ``<ds>`` branch of ``_lookup_named_entity``.
    Re-verifies the datasource still exists (it may have been deleted
    between resolve and lookup) before rendering."""
    known = await storage.list_datasources()
    if ds not in known:
        return LookupMissing()
    identities = await storage._list_all_model_identities()
    models: list[SlayerModel] = []
    for ident_ds, name in identities:
        if ident_ds != ds:
            continue
        m = await storage.get_model(name, data_source=ident_ds)
        if m is not None:
            models.append(m)
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
    """DEV-1513: ``<ds>.<model>`` and ``<ds>.<model>.<leaf>`` branches of
    ``_lookup_named_entity``. Returns ``Hidden`` for hidden model /
    hidden column, ``Missing`` for "no such entity" (race between resolve
    and lookup)."""
    model = await storage.get_model(model_name, data_source=ds)
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
) -> LookupResult:
    """Resolve a canonical id to its ``(kind, text, description)`` triple
    for channel-1 named-entity surfacing (DEV-1513 / DEV-1549)."""
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
            ds=segments[0], storage=storage,
        )
    return await _lookup_model_or_leaf_canonical(
        canonical=canonical,
        ds=segments[0],
        model_name=segments[1],
        leaf=segments[2] if len(segments) >= 3 else None,
        storage=storage,
    )


# ---------------------------------------------------------------------------
# DEV-1516 column-hit refresh helpers (adapted to flat SearchHit)
# ---------------------------------------------------------------------------


def _group_column_hits(
    results: list[SearchHit],
) -> dict[tuple[str, str], list[tuple[int, SearchHit, str]]]:
    """DEV-1516 helper: split a fused result list into per-model buckets
    for the search-side sample-refresh hook.

    Walks ``results``, keeps only ``kind == "column"`` hits whose
    canonical id parses as ``<data_source>.<model>.<column>`` (3
    segments), and groups them by ``(data_source, model_name)`` so the
    caller can serialise writes within a model and parallelise across
    models. Each member tuple is ``(original_hit_index, hit,
    column_name)`` — the index is preserved so caller can splice
    refreshed text back into the original list in place."""
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


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class SearchService:
    """Orchestrates the registered retrievers + RRF fusion."""

    def __init__(
        self,
        *,
        storage: StorageBackend,
        engine: SlayerQueryEngine | None = None,
        retrievers: list[Retriever] | None = None,
    ) -> None:
        """DEV-1516: ``engine`` is optional so storage-only test contexts
        keep working unchanged. When supplied, the post-fusion column-hit
        hook auto-refreshes stale categorical columns via
        :func:`ensure_column_sample_fresh` before rendering ``SearchHit.text``.
        Without an engine the hook is a silent no-op."""
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
        """DEV-1516 post-fusion column-hit refresh.

        Groups column hits by ``(data_source, model_name)`` and dispatches
        each group to :meth:`_refresh_group_worker`. Per-model writes
        serialise (storage's ``update_column_sampled`` is a model-level
        read-modify-write); cross-model writes parallelise via
        ``asyncio.gather``. Returns ``results`` with refreshed text
        spliced in for each column hit whose helper call returned a
        materially-updated column.

        DEV-1549 (Codex#3): under ``compact=True`` the refresh leaves
        ``text=""`` and refreshes ``description`` so the column hit can
        never resurrect the full render mid-search.
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
        """Refresh every column hit on one ``(data_source, model_name)``
        group sequentially (per-model serialisation). Loads the model
        once, walks members, and writes refreshed hits into the shared
        ``refreshed_by_idx`` buffer keyed by original hit index.

        DEV-1549: under ``compact=True`` only refresh
        ``SearchHit.description``; leave ``text=""``.
        """
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
        for idx, hit, column_name in members:
            col = model.get_column(column_name)
            if col is None:
                continue
            refreshed_col = await ensure_column_sample_fresh(
                model=model,
                column=col,
                engine=self._engine,  # type: ignore[arg-type]
                storage=self._storage,
            )
            if refreshed_col is col:
                # Helper returned the input — cache hit, ineligible, or
                # any failure. Leave the hit text as-is.
                continue
            update: dict[str, Any] = {
                "description": refreshed_col.description,
            }
            if not compact:
                update["text"] = render_column_text(
                    model=model, column=refreshed_col,
                )
            refreshed_by_idx[idx] = hit.model_copy(update=update)

    # ------------------------------------------------------------------
    # Read side — search()
    # ------------------------------------------------------------------

    async def search(  # NOSONAR(S3776) — single orchestrator entry point; stages are linear and named
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

        # DEV-1464: optional cypher_filter pre-filter. When the graph
        # path runs and returns no ids, short-circuit to an empty result
        # with a warning — every channel would otherwise return zero
        # surviving hits and we'd burn a corpus build for nothing.
        candidate_ids, kind_filter, early = await self._apply_cypher_filter(
            cypher_filter=cypher_filter,
            canonical_input_entities=canonical_input_entities,
            warnings=warnings,
        )
        if early is not None:
            return early
        # Naive kind_filter parity with graph path: warn when a named
        # memory:<id> ref would be excluded by the kind filter so the
        # caller knows why it doesn't appear in results.
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
            )

        # Datasource filter runs first so the off-datasource warning
        # reflects "memory dropped because of datasource", not
        # "memory dropped because of cypher_filter".
        datasource_filtered_memories: list[Memory] = (
            _filter_memories_by_datasource(
                memories=await self._storage.list_memories(entities=None),
                datasource=datasource,
            )
        )
        # DEV-1513: detect named ``memory:<id>`` refs whose memory was
        # filtered out by the datasource pre-filter — emit BEFORE
        # cypher_filter narrowing so a memory that IS rooted at the
        # datasource but is excluded by cypher_filter doesn't get a
        # spurious "not rooted at datasource" warning on top of the
        # cypher_filter warning.
        warnings = _dedup(
            warnings + _memory_id_off_datasource_warnings(
                canonical_input_entities=canonical_input_entities,
                live_memory_ids={m.id for m in datasource_filtered_memories},
                datasource=datasource,
            )
        )
        # DEV-1464: now narrow by the cypher_filter allowlist for the
        # retrieval path — BM25 / tantivy / embeddings rank only the
        # surviving memories.
        if candidate_ids is not None:
            all_memories: list[Memory] = [
                m for m in datasource_filtered_memories
                if f"{_MEMORY_PREFIX}{m.id}" in candidate_ids
            ]
        else:
            all_memories = datasource_filtered_memories

        valid_canonicals = await self._valid_canonical_set(
            all_memories=all_memories, datasource=datasource,
        )

        corpus: Corpus | None = None
        if question_active:
            all_models, datasources, datasource_descriptions = (
                await self._collect_index_corpus(datasource=datasource)
            )
            corpus = build_in_memory_corpus(
                memories=all_memories,
                models=all_models,
                datasources=datasources,
                datasource_descriptions=datasource_descriptions,
            )

        # DEV-1464: surface the reason a named memory:<id> ref didn't
        # appear in results when cypher_filter excluded it.
        if candidate_ids is not None:
            warnings = _dedup(
                warnings + _memory_id_cypher_filter_warnings(
                    canonical_input_entities=canonical_input_entities,
                    candidate_ids=candidate_ids,
                )
            )
        # DEV-1513: channel-1 named-entity surfacing.
        (
            channel_1_entity_ranking,
            named_kind_text,
            entity_surfacing_warnings,
        ) = await self._build_channel_1_entity_ranking(
            canonical_input_entities=canonical_input_entities,
            datasource=datasource,
            corpus=corpus,
            candidate_ids=candidate_ids,
        )
        warnings = _dedup(warnings + entity_surfacing_warnings)

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

        # DEV-1428 + DEV-1513: stale-Memory.query warnings for surfaced
        # query-bearing hits AND for explicitly-named ``memory:<id>``
        # refs whose attached query has stale references.
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

        # DEV-1516: refresh stale categorical column hits in-place before
        # returning. Per-model writes serialise; cross-model writes run
        # concurrently. Silently no-op when engine is None.
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
    ) -> tuple[
        frozenset[str] | None,
        set[str] | None,
        SearchResponse | None,
    ]:
        """DEV-1464: resolve the optional ``cypher_filter`` into
        ``(candidate_ids, kind_filter, early)``.

        * ``candidate_ids`` is non-None when the full graph path ran
          (advanced_search extra installed). The set is the allowlist
          every channel is narrowed against.
        * ``kind_filter`` is non-None when the naive fallback ran
          (graph extra absent). The set is the entity kinds the result
          is filtered down to.
        * ``early`` is a short-circuit ``SearchResponse`` when the graph
          path returned no ids — we skip the corpus build entirely.
        """
        if cypher_filter is None:
            return None, None, None
        if _search_graph.is_available():
            candidate_ids = await _search_graph.get_filtered_ids(
                cypher=cypher_filter, storage=self._storage,
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
        candidate_ids: frozenset[str] | None = None,
    ) -> tuple[list[str], dict[str, tuple[str, str, str | None]], list[str]]:
        """DEV-1513: produce channel-1's contribution to the entity
        ranking by surfacing each user-named canonical ref as itself.

        Returns ``(entity_ranking, named_kind_text, warnings)``:

        * ``entity_ranking`` — surviving canonicals in user-supplied
          order; this is the channel-1 input to the entity-side of
          ``_fuse_all_hits``.
        * ``named_kind_text`` — ``{canonical: (kind, text)}`` lookup
          consumed by ``_fuse_all_hits`` as a fallback when the corpus
          doesn't carry the canonical (pure-named call with no corpus,
          or hidden-from-corpus refs).
        * ``warnings`` — drop reasons per filter (off-datasource,
          hidden, missing, cypher_filter exclusion).

        DEV-1464: when ``candidate_ids`` is supplied, entities outside
        the allowlist are dropped (with a warning) BEFORE the
        rendering / lookup work so we don't waste a storage round-trip.
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
                canonical=canonical, storage=self._storage, corpus=corpus,
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

    # ------------------------------------------------------------------
    # Write side — fan-out to retrievers
    # ------------------------------------------------------------------

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
        """Call ``invoke(retriever)`` on every registered retriever in
        declaration order, isolating per-retriever exceptions as
        prefixed warnings so subsequent retrievers still run. Returns
        the deduped warning list."""
        warnings: list[str] = []
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
        self, datasource: str | None,
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
        entities: list[str] | None,
        query: SlayerQuery | dict | None,
    ) -> tuple[list[str], list[str]]:
        """Walk ``entities`` + ``query`` into a deduped canonical-entity
        list plus a deduped warning list. DEV-1428: lenient —
        per-token failures become warnings."""
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
        kind_filter: set[str] | None = None,
        compact: bool = True,
    ) -> SearchResponse:
        """Empty-input branch: return the newest memories (both
        learning-only and query-bearing) as a flat list, capped by
        ``max_results``. No retriever is invoked on this path.

        DEV-1409: when ``datasource`` is set, the same memory pre-filter
        used by the main search path applies.

        DEV-1464: when ``candidate_ids`` is set, only memories whose
        canonical id appears in the allowlist survive; when
        ``kind_filter`` is set and doesn't include ``"memory"``, the
        recency bucket is empty (no entity recency on the fallback
        path)."""
        warnings.append(
            "no entities, query, or question supplied; returning "
            "newest memories by recency."
        )
        recency_memories = _filter_memories_by_datasource(
            memories=await self._storage.list_memories(entities=None),
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
        # DEV-1464: when cypher_filter (or its naive kind-filter
        # fallback) zeroed out an otherwise-populated recency pool,
        # surface that explicitly — the generic "returning newest"
        # warning would otherwise read as "system is healthy, the
        # corpus is just empty," masking that the filter was the cause.
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
            all_memories=recency_memories, datasource=datasource,
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
        # DEV-1428: emit stale-Memory.query warnings on the recency path too.
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
        self, *, datasource: str | None,
    ) -> set:
        names = await self._storage.list_datasources()
        if datasource is not None:
            names = [d for d in names if d == datasource]
        return set(names)

    async def _collect_model_subtree_canonicals(
        self, *, datasource: str | None,
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
        query_bearing_hits: list[SearchHit],
        memory_by_id: dict[str, Memory],
    ) -> list[str]:
        """Emit one warning per surfaced query-bearing hit whose
        attached ``SlayerQuery`` no longer resolves (entities pointing
        at deleted/renamed models or columns). DEV-1428."""
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
        """DEV-1513: emit the stale-query warning for any explicitly-named
        ``memory:<id>`` ref pointing at a query-bearing memory with
        stale refs, regardless of whether the ``max_results`` cap
        suppressed the hit. The user explicitly named the memory; they
        deserve to know the attached query is broken."""
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
        datasource: str | None = None,
    ) -> tuple[list[SlayerModel], list[str], dict[str, str | None]]:
        """DEV-1549: also returns ``{ds_name → description}`` so the
        corpus builder can populate ``canonical_to_description`` for
        datasource hits without re-loading the configs."""
        datasources = await self._storage.list_datasources()
        if datasource is not None:
            datasources = [d for d in datasources if d == datasource]
        models: list[SlayerModel] = []
        identities = await self._storage._list_all_model_identities()
        for ds, name in identities:
            if datasource is not None and ds != datasource:
                continue
            m = await self._storage.get_model(name, data_source=ds)
            if m is not None:
                models.append(m)
        descriptions: dict[str, str | None] = {}
        for ds_name in datasources:
            cfg = await self._storage.get_datasource(ds_name)
            descriptions[ds_name] = cfg.description if cfg is not None else None
        return models, datasources, descriptions


__all__ = [
    "LookupFound",
    "LookupHidden",
    "LookupMissing",
    "SearchHit",
    "SearchResponse",
    "SearchService",
]
