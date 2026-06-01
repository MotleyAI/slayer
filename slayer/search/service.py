"""SearchService — three-channel + RRF orchestrator (DEV-1375 / DEV-1386).

* **Channel 1** — entity-overlap BM25 with implicit self-references
  (``slayer.memories.ranker.bm25_rank``). Skipped when neither
  ``entities`` nor ``query`` is supplied. Contributes to BOTH the
  memory ranking AND the entity ranking (DEV-1513): every doc is
  treated as carrying an implicit reference to itself, so a memory
  ``M``'s effective tag list is ``M.entities ∪ {memory:<M.id>}`` (a
  ``memory:<id>`` ref surfaces the named memory at the top of the
  memory ranking) and a user-supplied canonical entity ref surfaces
  the named entity at the top of the entity ranking.
* **Channel 2** — tantivy full-text. Skipped when ``question`` is
  empty. Runs as TWO kind-filtered queries per call (DEV-1414): one
  with ``kind_filter="memory"`` for the memory ranking, one with
  ``exclude_kind="memory"`` for the entity ranking. Each query ranks
  the full per-kind subset of the corpus — no over-fetch truncation.
* **Channel 3** — dense embedding similarity (DEV-1386). Skipped when
  ``question`` is empty, when the ``embedding_search`` extra is not
  installed, when the query embedding call fails, or when there are
  no embedding rows for the active model name. The persisted embedding
  rows are partitioned by ``entity_kind`` (DEV-1414): memory rows feed
  the memory ranking, non-memory rows feed the entity ranking. Each
  partition is ranked in full.

Memory and entity rankings from every active channel are fused via RRF
(``k = 60``). Channel 1's entity ranking is the surviving canonical
inputs in supplied order (DEV-1513); channels 2 and 3 contribute fuzzy
hits.

Per-bucket invariance (DEV-1414): because each channel produces a full
per-kind ranking — never truncated by a shared candidate-pool budget —
the membership and order of every output bucket (``memories``,
``example_queries``, ``entities``) is a pure function of the corpus,
the question, the datasource filter, and that bucket's own cap. Varying
the other two caps cannot move ids in or out of the returned list nor
reorder it.

Empty input (no entities, no query, no question) falls back to recency:
newest ``max_memories`` learning-only memories + newest
``max_example_queries`` query-bearing memories, with a warning.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional, Set, Tuple, Union

from pydantic import BaseModel, Field

from slayer.core.errors import AmbiguousModelError, EntityResolutionError
from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.embeddings import client as embedding_client
from slayer.embeddings.models import Embedding
from slayer.engine.profiling import ensure_column_sample_fresh
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.memories.models import MEMORY_CANONICAL_PREFIX as _MEMORY_PREFIX
from slayer.memories.models import Memory
from slayer.memories.ranker import bm25_rank
from slayer.memories.resolver import (
    canonical_id_rooted_at,
    extract_entities_from_query,
    resolve_entity,
)
from slayer.search.index import (
    Corpus,
    IndexHit,
    build_in_memory_corpus,
    search_index,
)
from slayer.search.render import (
    collect_model_entity_pairs,
    render_column_text,
    render_datasource_pair,
)
from slayer.search.rrf import rrf_fuse
from slayer.storage.base import StorageBackend


logger = logging.getLogger(__name__)


_RRF_K = 60


# ---------------------------------------------------------------------------
# Hit & response models
# ---------------------------------------------------------------------------


class MemoryHit(BaseModel):
    """A learning-only memory result (``Memory.query is None``). ``id`` is
    the string memory id (suitable for ``forget_memory(id=hit.id)``).
    ``score`` is always the Reciprocal-Rank-Fusion score
    (``Σ 1 / (k + rank)``, ``k=60``); even single-channel searches go
    through RRF, so the value is comparable across channels but is not
    directly the raw BM25 / tantivy / cosine score."""

    id: str
    score: float
    text: str
    matched_entities: List[str] = Field(default_factory=list)


class ExampleQueryHit(BaseModel):
    """A query-bearing memory result (``Memory.query`` is set). Same id /
    score / text shape as ``MemoryHit`` but always carries the attached
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
    ``score`` is the RRF-fused score across channels 1, 2, and 3 (or the
    single-channel raw score when only one channel contributed).

    DEV-1513: channel 1 contributes named-entity surfacing via the
    implicit self-reference model — each entity is conceptually tagged
    with itself, so a user-supplied ref in ``entities=`` ranks at the
    top of the entities bucket alongside any fuzzy hits."""

    id: str
    kind: str  # "datasource" | "model" | "column" | "measure" | "aggregation"
    score: float
    text: str


# ---------------------------------------------------------------------------
# Lookup result for named-entity surfacing (DEV-1513)
# ---------------------------------------------------------------------------


class LookupFound(BaseModel):
    """``_lookup_named_entity`` succeeded; carries ``(kind, text)``."""

    kind: str
    text: str


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


LookupResult = Union[LookupFound, LookupHidden, LookupMissing]


class SearchResponse(BaseModel):
    memories: List[MemoryHit] = Field(default_factory=list)
    example_queries: List[ExampleQueryHit] = Field(default_factory=list)
    entities: List[EntityHit] = Field(default_factory=list)
    resolved_input_entities: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Service
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


def _backfill_memory_by_id(
    *,
    memory_by_id: dict,
    all_memories_by_id: "dict[str, Memory]",
    mem_ids,
) -> None:
    """For each id in ``mem_ids`` not already in ``memory_by_id``, look it
    up in ``all_memories_by_id`` and insert it. Mutates ``memory_by_id``.

    Takes a precomputed id→Memory dict (not the raw list) so per-call
    backfill stays O(N) instead of O(N²) when every channel returns the
    full memory corpus (DEV-1414).
    """
    for mem_id in mem_ids:
        if mem_id in memory_by_id:
            continue
        mem = all_memories_by_id.get(mem_id)
        if mem is not None:
            memory_by_id[mem_id] = mem


def _build_memory_hit(
    *,
    mem: "Memory",
    memory_id: str,
    score: float,
    index_hits_by_memory_id: dict,
    canonical_input_entities: List[str],
    valid_canonicals: Optional[set] = None,
) -> Union["MemoryHit", "ExampleQueryHit"]:
    """Build the appropriate hit type for ``mem``: ``MemoryHit`` for
    learning-only memories (``query is None``), ``ExampleQueryHit`` for
    query-bearing ones. ``text`` falls back to ``mem.learning`` when the
    memory wasn't reached via tantivy.

    DEV-1428: ``matched_entities`` is computed against the LIVE
    canonical set when ``valid_canonicals`` is supplied, so stale tags
    do not surface to the agent.

    DEV-1513: every memory has an implicit ``memory:<self_id>``
    self-reference; it appears in ``matched_entities`` only when the
    user explicitly named that ref (so the surfaced memory honestly
    shows the reason it was returned)."""
    if valid_canonicals is not None:
        live_entities = [e for e in mem.entities if e in valid_canonicals]
    else:
        live_entities = list(mem.entities)
    self_ref = f"{_MEMORY_PREFIX}{memory_id}"
    if self_ref not in live_entities:
        live_entities.append(self_ref)
    wanted_set = set(canonical_input_entities)
    matched = sorted(wanted_set & set(live_entities)) if wanted_set else []
    text = (
        index_hits_by_memory_id[memory_id].text
        if memory_id in index_hits_by_memory_id
        else mem.learning
    )
    if mem.query is None:
        return MemoryHit(
            id=memory_id, score=score, text=text, matched_entities=matched,
        )
    return ExampleQueryHit(
        id=memory_id, score=score, text=text,
        matched_entities=matched, query=mem.query,
    )


def _filter_memories_entities(
    memories: List["Memory"], valid_canonicals: set,
) -> List["Memory"]:
    """Return shallow copies of ``memories`` whose ``entities`` lists are
    filtered down to ``valid_canonicals`` only. Used to feed BM25 a
    stale-free corpus without writing back to storage (DEV-1428)."""
    out: List[Memory] = []
    for m in memories:
        live = [e for e in m.entities if e in valid_canonicals]
        if live == m.entities:
            out.append(m)
        else:
            out.append(m.model_copy(update={"entities": live}))
    return out


def _fuse_memory_hits(
    *,
    rankings: List[List[str]],
    memory_by_id: dict,
    index_hits_by_memory_id: dict,
    canonical_input_entities: List[str],
    max_memories: int,
    max_example_queries: int,
    valid_canonicals: Optional[set] = None,
) -> Tuple[List["MemoryHit"], List["ExampleQueryHit"]]:
    """RRF-fuse the supplied memory rankings and partition into
    learning-only (``MemoryHit``) vs query-bearing (``ExampleQueryHit``)
    lists, each capped independently. Empty inner rankings are filtered
    out so single-channel results still flow through RRF normalisation."""
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
            index_hits_by_memory_id=index_hits_by_memory_id,
            canonical_input_entities=canonical_input_entities,
            valid_canonicals=valid_canonicals,
        )
        if isinstance(hit, MemoryHit) and len(learnings) < max_memories:
            learnings.append(hit)
        elif isinstance(hit, ExampleQueryHit) and len(examples) < max_example_queries:
            examples.append(hit)
        if (
            len(learnings) >= max_memories
            and len(examples) >= max_example_queries
        ):
            break
    return learnings, examples


def _filter_memories_by_datasource(
    memories: List["Memory"], datasource: Optional[str],
) -> List["Memory"]:
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


def _filter_embedding_corpus_by_datasource(
    rows: List["Embedding"],
    *,
    datasource: str,
    eligible_memory_canonicals: Set[str],
) -> List["Embedding"]:
    """DEV-1409: narrow the embedding corpus to rows that survive a
    datasource filter. Memory rows (``entity_kind == 'memory'``) must
    appear in the supplied ``eligible_memory_canonicals`` set (already
    datasource-filtered upstream); entity rows must be rooted at
    ``datasource`` per the dotted-namespace rule."""
    return [
        r for r in rows
        if (
            (r.entity_kind == "memory"
                and r.canonical_id in eligible_memory_canonicals)
            or (r.entity_kind != "memory"
                and canonical_id_rooted_at(
                    canonical_id=r.canonical_id, datasource=datasource,
                ))
        )
    ]


def _count_corpus_kinds(corpus: Corpus) -> Tuple[int, int]:
    """Return ``(memory_count, entity_count)`` for a built corpus. Used
    by channel 2 to pass ``limit = full per-kind corpus size`` to each
    kind-filtered tantivy query so neither kind's ranking is truncated
    (DEV-1414)."""
    memory_count = 0
    entity_count = 0
    for kind in corpus.canonical_to_kind.values():
        if kind == "memory":
            memory_count += 1
        else:
            entity_count += 1
    return memory_count, entity_count


def _collect_memory_canonicals(memories: List["Memory"]) -> set:
    """Return ``{"memory:<id>" for m in memories}`` — pulled out so
    ``_valid_canonical_set`` stays under the cognitive-complexity gate."""
    return {f"{_MEMORY_PREFIX}{m.id}" for m in memories}


def _memory_id_from_canonical(canonical_id: str) -> Optional[str]:
    """Parse a memory row's canonical id back into the str memory id.

    Returns ``None`` when the input is not a memory canonical id — i.e.
    not exactly of the shape ``memory:<non-empty-id>``. DEV-1428 review:
    a corrupted / stale embedding row carrying ``foo:bar`` would
    otherwise be mis-mapped to a memory hit; the prefix gate keeps the
    memory channel honest.
    """
    if not canonical_id.startswith(_MEMORY_PREFIX):
        return None
    memory_id = canonical_id[len(_MEMORY_PREFIX):]
    return memory_id or None


def _rank_embedding_kind(
    *,
    rows: List["Embedding"],
    normalised_query,
    np,
    normalise_matrix,
    top_k_cosine,
) -> List[str]:
    """Rank one kind of embedding rows by cosine similarity to the
    pre-normalised query vector. Returns the rows' ``canonical_id``
    strings in descending similarity order. Empty input → empty list.

    Pulls the per-kind matrix build + cosine call out of
    ``SearchService._run_channel_3`` so each kind's ranking is a single
    line in the caller (DEV-1414 — keeps channel 3 below the
    cognitive-complexity gate)."""
    if not rows:
        return []
    matrix = np.array([r.embedding for r in rows], dtype=np.float32)
    pairs = top_k_cosine(
        query=normalised_query,
        matrix=normalise_matrix(matrix),
        k=len(rows),
    )
    return [rows[idx].canonical_id for idx, _score in pairs]


def _resolve_entity_hit_kind_text(
    *,
    canonical: str,
    corpus: Optional[Corpus],
    named_kind_text: Optional[Dict[str, Tuple[str, str]]],
) -> Optional[Tuple[str, str]]:
    """DEV-1513: resolve one canonical's ``(kind, text)`` for an entity
    hit. Prefers the corpus (channels 2/3 already built it); falls back
    to the channel-1 ``named_kind_text`` lookup (used on pure-named
    calls with no corpus). Returns ``None`` when neither source carries
    the canonical."""
    if corpus is not None:
        kind = corpus.canonical_to_kind.get(canonical)
        text = corpus.canonical_to_text.get(canonical)
        if kind is not None and text is not None:
            return kind, text
    if named_kind_text is not None:
        pair = named_kind_text.get(canonical)
        if pair is not None:
            return pair
    return None


def _fuse_entity_hits(
    *,
    rankings: List[List[str]],
    corpus: Optional[Corpus],
    named_kind_text: Optional[Dict[str, Tuple[str, str]]],
    max_entities: int,
) -> List[EntityHit]:
    """RRF-fuse the entity rankings and look text/kind up via
    ``_resolve_entity_hit_kind_text`` (corpus first, then channel-1
    named-entity fallback, DEV-1513). Returns at most ``max_entities``
    hits."""
    non_empty = [r for r in rankings if r]
    fused = rrf_fuse(rankings=non_empty, k=_RRF_K) if non_empty else {}
    fused_sorted = sorted(fused.items(), key=lambda kv: kv[1], reverse=True)
    out: List[EntityHit] = []
    for canonical, score in fused_sorted:
        if len(out) >= max_entities:
            break
        resolved = _resolve_entity_hit_kind_text(
            canonical=canonical,
            corpus=corpus,
            named_kind_text=named_kind_text,
        )
        if resolved is None:
            continue
        kind, text = resolved
        out.append(EntityHit(
            id=canonical, kind=kind, score=score, text=text,
        ))
    return out


# ---------------------------------------------------------------------------
# DEV-1513: implicit-self-reference helpers
# ---------------------------------------------------------------------------


def _memory_id_off_datasource_warnings(
    *,
    canonical_input_entities: List[str],
    live_memory_ids: Set[str],
    datasource: Optional[str],
) -> List[str]:
    """DEV-1513: emit one warning per user-supplied ``memory:<id>`` ref
    whose memory was dropped by the datasource pre-filter (the memory
    has no entities rooted at ``datasource``). Mirrors the entity-side
    off-ds drop on the memory side.

    No-op when ``datasource`` is None (nothing was filtered out)."""
    if datasource is None:
        return []
    out: List[str] = []
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


def _augment_with_self_refs(memories: List[Memory]) -> List[Memory]:
    """Return shallow copies of ``memories`` with each memory's
    ``entities`` augmented by ``memory:<self_id>`` (the implicit
    self-reference, DEV-1513). Idempotent: if the synthetic ref is
    already present, the copy is identical."""
    out: List[Memory] = []
    for m in memories:
        self_ref = f"{_MEMORY_PREFIX}{m.id}"
        if self_ref in m.entities:
            out.append(m)
        else:
            out.append(m.model_copy(
                update={"entities": [self_ref, *m.entities]},
            ))
    return out


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
    models: List[SlayerModel] = []
    for ident_ds, name in identities:
        if ident_ds != ds:
            continue
        m = await storage.get_model(name, data_source=ident_ds)
        if m is not None:
            models.append(m)
    pair = render_datasource_pair(name=ds, models=models)
    return LookupFound(kind=pair.kind, text=pair.text)


async def _lookup_model_or_leaf_canonical(
    *,
    canonical: str,
    ds: str,
    model_name: str,
    leaf: Optional[str],
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
            return LookupFound(kind=re.kind, text=re.text)
    # Not in the visible set. For a leaf canonical, distinguish "exists
    # on model but is hidden" from "doesn't exist on model at all".
    if leaf is not None:
        for column in model.columns:
            if column.name == leaf and column.hidden:
                return LookupHidden(reason="hidden column")
    return LookupMissing()


async def _lookup_named_entity(
    *,
    canonical: str,
    storage: StorageBackend,
    corpus: Optional[Corpus],
) -> LookupResult:
    """Resolve a canonical id to its ``(kind, text)`` pair for channel-1
    named-entity surfacing (DEV-1513).

    Reuses ``corpus.canonical_to_*`` when the canonical is already in the
    in-memory corpus (channel 2/3 active) — no re-render, no risk of
    corpus / live-storage drift mid-call. Otherwise dispatches to the
    bare-datasource or model/leaf helper, which direct-render via the
    unified ``render_datasource_pair`` / ``collect_model_entity_pairs``
    helpers in ``slayer.search.render``.

    Returns ``LookupHidden`` when the resolved canonical lives behind a
    ``hidden`` flag (model or column), and ``LookupMissing`` when the
    canonical resolved at ``_resolve_inputs`` time but is no longer
    present at lookup time (race between resolve and lookup)."""
    if corpus is not None:
        kind = corpus.canonical_to_kind.get(canonical)
        text = corpus.canonical_to_text.get(canonical)
        if kind is not None and text is not None:
            return LookupFound(kind=kind, text=text)
    segments = canonical.split(".")
    if len(segments) == 1:
        return await _lookup_bare_datasource_canonical(
            ds=segments[0], storage=storage,
        )
    if len(segments) >= 2:
        return await _lookup_model_or_leaf_canonical(
            canonical=canonical,
            ds=segments[0],
            model_name=segments[1],
            leaf=segments[2] if len(segments) >= 3 else None,
            storage=storage,
        )
    return LookupMissing()


def _group_column_hits(
    entity_hits: List["EntityHit"],
) -> Dict[Tuple[str, str], List[Tuple[int, "EntityHit", str]]]:
    """DEV-1516 helper: split a fused entity-hit list into per-model
    buckets for the search-side sample-refresh hook.

    Walks ``entity_hits``, keeps only ``kind == "column"`` hits whose
    canonical id parses as ``<data_source>.<model>.<column>`` (3
    segments), and groups them by ``(data_source, model_name)`` so the
    caller can serialise writes within a model and parallelise across
    models. Each member tuple is ``(original_hit_index, hit,
    column_name)`` — the index is preserved so caller can splice
    refreshed text back into the original list in place."""
    groups: Dict[Tuple[str, str], List[Tuple[int, EntityHit, str]]] = {}
    for idx, hit in enumerate(entity_hits):
        if hit.kind != "column":
            continue
        segments = hit.id.split(".")
        if len(segments) != 3:
            # Bare datasource / model canonicals are handled by other
            # kinds; columns are always 3-segment.
            continue
        data_source, model_name, column_name = segments
        groups.setdefault((data_source, model_name), []).append(
            (idx, hit, column_name)
        )
    return groups


class SearchService:
    """Orchestrates the three retrieval channels + RRF fusion."""

    def __init__(
        self,
        *,
        storage: StorageBackend,
        engine: Optional[SlayerQueryEngine] = None,
    ) -> None:
        """DEV-1516: ``engine`` is optional so storage-only test contexts
        keep working unchanged. When supplied, the post-fusion column-hit
        hook auto-refreshes stale categorical columns via
        :func:`ensure_column_sample_fresh` before rendering ``EntityHit.text``.
        Without an engine the hook is a silent no-op."""
        self._storage = storage
        self._engine = engine

    async def _validate_datasource_known(
        self, datasource: Optional[str],
    ) -> None:
        """DEV-1409: reject typos in ``datasource`` before any corpus
        walk. One ``list_datasources()`` round-trip; both backends back
        this with an indexed query so the cost is bounded."""
        if datasource is None:
            return
        known = sorted(await self._storage.list_datasources())
        if datasource not in known:
            raise ValueError(
                f"datasource {datasource!r} not found; known: {known}."
            )

    async def _refresh_stale_column_hits(
        self,
        *,
        entity_hits: List[EntityHit],
    ) -> List[EntityHit]:
        """DEV-1516 post-fusion column-hit refresh.

        Groups column hits by ``(data_source, model_name)`` and dispatches
        each group to :meth:`_refresh_group_worker`. Per-model writes
        serialise (storage's ``update_column_sampled`` is a model-level
        read-modify-write); cross-model writes parallelise via
        ``asyncio.gather``. Returns ``entity_hits`` with refreshed text
        spliced in for each column hit whose helper call returned a
        materially-updated column."""
        assert self._engine is not None  # caller-guarded
        groups = _group_column_hits(entity_hits)
        if not groups:
            return entity_hits
        refreshed_by_idx: Dict[int, EntityHit] = {}
        await asyncio.gather(*[
            self._refresh_group_worker(
                ds_name=ds, model_name=model_name,
                members=members, refreshed_by_idx=refreshed_by_idx,
            )
            for (ds, model_name), members in groups.items()
        ])
        if not refreshed_by_idx:
            return entity_hits
        return [
            refreshed_by_idx.get(i, h) for i, h in enumerate(entity_hits)
        ]

    async def _refresh_group_worker(
        self,
        *,
        ds_name: str,
        model_name: str,
        members: List[Tuple[int, EntityHit, str]],
        refreshed_by_idx: Dict[int, EntityHit],
    ) -> None:
        """Refresh every column hit on one ``(data_source, model_name)``
        group sequentially (per-model serialisation). Loads the model
        once, walks members, and writes refreshed hits into the shared
        ``refreshed_by_idx`` buffer keyed by original hit index."""
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
            refreshed_by_idx[idx] = hit.model_copy(update={
                "text": render_column_text(
                    model=model, column=refreshed_col,
                ),
            })

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
            raise ValueError(f"max_memories must be >= 0; got {max_memories}.")
        if max_example_queries < 0:
            raise ValueError(
                f"max_example_queries must be >= 0; got {max_example_queries}."
            )
        if max_entities < 0:
            raise ValueError(f"max_entities must be >= 0; got {max_entities}.")
        await self._validate_datasource_known(datasource)

        canonical_input_entities, warnings = await self._resolve_inputs(
            entities=entities, query=query,
        )
        channel_1_active = (entities is not None and len(entities) > 0) or query is not None
        question_active = bool(question and question.strip())

        # Recency fallback for the all-empty case.
        if not channel_1_active and not question_active:
            return await self._recency_fallback(
                datasource=datasource,
                max_memories=max_memories,
                max_example_queries=max_example_queries,
                warnings=warnings,
            )

        # ``valid_canonicals`` filled in below using the corpus we just
        # fetched (after datasource filter) so the lazy GC and recency
        # fallback both apply the same predicate.

        # Single memory-corpus fetch shared by all channels. Pre-filtered
        # by ``datasource`` so BM25 (channel 1) and the embedding cosine
        # (channel 3) consume the narrowed list — IDF / matrix shape
        # reflect the filtered subset (DEV-1409).
        all_memories: List[Memory] = []
        if channel_1_active or question_active:
            all_memories = _filter_memories_by_datasource(
                await self._storage.list_memories(entities=None),
                datasource,
            )

        # DEV-1428: build the live canonical set so stale entity tags
        # are excluded from BM25 ranking AND from any surfaced
        # ``matched_entities`` list. Built once per call; reused across
        # the BM25 path and the recency fallback.
        valid_canonicals = await self._valid_canonical_set(
            all_memories=all_memories, datasource=datasource,
        )

        # Build the in-memory corpus once when question is active — both
        # channels 2 and 3 read from it (channel 2 for tantivy search,
        # channel 3 to recover hit text by canonical_id).
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

        channel_1_memory_ranking, memory_by_id = self._run_channel_1(
            canonical_input_entities=canonical_input_entities,
            all_memories=all_memories,
            channel_1_active=channel_1_active,
            valid_canonicals=valid_canonicals,
        )
        # DEV-1513: detect named ``memory:<id>`` refs whose memory was
        # filtered out by the datasource pre-filter — emit a warning
        # symmetric with the entity-side off-ds drop.
        warnings = _dedup(
            warnings + _memory_id_off_datasource_warnings(
                canonical_input_entities=canonical_input_entities,
                live_memory_ids={m.id for m in all_memories},
                datasource=datasource,
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
        )
        warnings = _dedup(warnings + entity_surfacing_warnings)
        (
            channel_2_memory_ranking,
            channel_2_entity_ranking,
            index_hits_by_memory_id,
        ) = self._run_channel_2(
            corpus=corpus,
            question=question,
        )
        (
            channel_3_memory_ranking,
            channel_3_entity_ranking,
            channel_3_warnings,
        ) = await self._run_channel_3(
            question=question,
            corpus=corpus,
            question_active=question_active,
            datasource=datasource,
            eligible_memory_canonicals={
                f"{_MEMORY_PREFIX}{m.id}" for m in all_memories
            },
        )
        warnings = _dedup(warnings + channel_3_warnings)

        # Backfill memory_by_id from every channel so RRF can resolve
        # any memory hit downstream. Build the id→Memory dict once so
        # the three backfills stay O(N) overall (DEV-1414).
        all_memories_by_id = {m.id: m for m in all_memories}
        _backfill_memory_by_id(
            memory_by_id=memory_by_id,
            all_memories_by_id=all_memories_by_id,
            mem_ids=channel_1_memory_ranking,
        )
        _backfill_memory_by_id(
            memory_by_id=memory_by_id,
            all_memories_by_id=all_memories_by_id,
            mem_ids=index_hits_by_memory_id.keys(),
        )
        _backfill_memory_by_id(
            memory_by_id=memory_by_id,
            all_memories_by_id=all_memories_by_id,
            mem_ids=channel_3_memory_ranking,
        )

        memory_hits, example_query_hits = _fuse_memory_hits(
            rankings=[
                channel_1_memory_ranking,
                channel_2_memory_ranking,
                channel_3_memory_ranking,
            ],
            memory_by_id=memory_by_id,
            index_hits_by_memory_id=index_hits_by_memory_id,
            canonical_input_entities=canonical_input_entities,
            max_memories=max_memories,
            max_example_queries=max_example_queries,
            valid_canonicals=valid_canonicals,
        )
        # DEV-1428: stale Memory.query warnings — surface example_queries
        # whose attached query references entities that no longer resolve.
        # DEV-1513: ALSO emit the warning for any explicitly-named
        # ``memory:<id>`` ref whose memory carries a stale query, even
        # when ``max_example_queries`` suppressed the hit — the user
        # explicitly asked for that memory.
        warnings = _dedup(
            warnings + await self._stale_query_warnings(
                example_query_hits=example_query_hits,
                memory_by_id=memory_by_id,
            ) + await self._stale_query_warnings_for_named_memory_refs(
                canonical_input_entities=canonical_input_entities,
                all_memories=all_memories,
                already_warned_ids={h.id for h in example_query_hits},
            )
        )
        entity_hits = _fuse_entity_hits(
            rankings=[
                channel_1_entity_ranking,
                channel_2_entity_ranking,
                channel_3_entity_ranking,
            ],
            corpus=corpus,
            named_kind_text=named_kind_text,
            max_entities=max_entities,
        )
        # DEV-1516: refresh stale categorical column hits in-place before
        # returning. Group by (data_source, model_name) so persists for
        # different columns of the SAME model are serialized (the storage
        # write is a model-level read-modify-write — concurrent updates
        # would lose data); persists across DIFFERENT models run
        # concurrently via ``asyncio.gather``. Silently no-op when engine
        # is None.
        if self._engine is not None:
            entity_hits = await self._refresh_stale_column_hits(
                entity_hits=entity_hits,
            )
        return SearchResponse(
            memories=memory_hits,
            example_queries=example_query_hits,
            entities=entity_hits,
            resolved_input_entities=canonical_input_entities,
            warnings=warnings,
        )

    async def _resolve_inputs(
        self,
        *,
        entities: Optional[List[str]],
        query: Optional[Union[SlayerQuery, dict]],
    ) -> Tuple[List[str], List[str]]:
        """Walk ``entities`` + ``query`` into a deduped canonical-entity list
        plus a deduped warning list.

        DEV-1428: search is lenient. Per-token resolution failures and
        ambiguity errors become warnings (the token is dropped from the
        canonical set). Unrelated ``ValueError``s (typing issues) still
        raise — those are programmer errors, not data drift.
        """
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
                    warnings.append(
                        f"entity {raw!r} dropped: {exc}"
                    )
                    continue
                canonical.extend(result.canonical_forms)
                warnings.extend(result.warnings)
        if query is not None:
            try:
                extraction = await extract_entities_from_query(
                    query=_coerce_query(query), storage=self._storage,
                )
            except (EntityResolutionError, AmbiguousModelError) as exc:
                warnings.append(
                    f"query input dropped: {exc}"
                )
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
        """Empty-input branch: partition all memories by recency into the
        learning-only bucket (``memories``, capped by ``max_memories``)
        and the query-bearing bucket (``example_queries``, capped by
        ``max_example_queries``).

        DEV-1409: when ``datasource`` is set, the same memory pre-filter
        used by the main search path applies — only memories with at
        least one entity rooted at the requested datasource are eligible.
        """
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
                index_hits_by_memory_id={},
                canonical_input_entities=[],
                valid_canonicals=valid_canonicals,
            )
            if isinstance(hit, MemoryHit) and len(memory_hits) < max_memories:
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
        # DEV-1428: emit stale-Memory.query warnings on the recency path
        # too; otherwise an empty-input search would silently return
        # example_queries whose attached queries no longer resolve.
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

    def _run_channel_1(
        self,
        *,
        canonical_input_entities: List[str],
        all_memories: List[Memory],
        channel_1_active: bool,
        valid_canonicals: Optional[set] = None,
    ) -> Tuple[List[str], dict[str, Memory]]:
        """Entity-overlap BM25 channel. Ranks the full memory corpus —
        no candidate-pool truncation (DEV-1414).

        DEV-1428: each memory's ``entities`` list is pre-filtered
        against ``valid_canonicals`` so stale tags neither contribute
        to BM25 scoring nor surface as ``matched_entities``.

        DEV-1513: AFTER the stale-tag filter, each memory's effective
        tag list is augmented with the synthetic ``memory:<self_id>``
        ref so a user-supplied ``memory:<id>`` ref surfaces the named
        memory at the top of the BM25 ranking. Augmentation runs after
        the filter so the self-ref cannot be stripped even if
        ``valid_canonicals`` ever drifted."""
        channel_1_memory_ranking: List[str] = []
        memory_by_id: dict[str, Memory] = {}
        if channel_1_active and canonical_input_entities:
            filtered_memories = (
                _filter_memories_entities(
                    all_memories, valid_canonicals,
                )
                if valid_canonicals is not None
                else all_memories
            )
            augmented_memories = _augment_with_self_refs(filtered_memories)
            ranked = bm25_rank(
                memories=augmented_memories,
                query_entities=canonical_input_entities,
            )
            # Use the original memory rows (with stored entity lists) for
            # the returned mapping so callers see the un-filtered shape;
            # only the ranking input was filtered + augmented.
            originals_by_id = {m.id: m for m in all_memories}
            for memory, _score in ranked:
                original = originals_by_id.get(memory.id, memory)
                memory_by_id[memory.id] = original
                channel_1_memory_ranking.append(memory.id)
        return channel_1_memory_ranking, memory_by_id

    async def _build_channel_1_entity_ranking(
        self,
        *,
        canonical_input_entities: List[str],
        datasource: Optional[str],
        corpus: Optional[Corpus],
    ) -> Tuple[List[str], Dict[str, Tuple[str, str]], List[str]]:
        """DEV-1513: produce channel-1's contribution to the entity
        ranking by surfacing each user-named canonical ref as itself.

        Returns ``(entity_ranking, named_kind_text, warnings)``:

        * ``entity_ranking`` — surviving canonicals in user-supplied
          order; this is the channel-1 input to ``_fuse_entity_hits``.
        * ``named_kind_text`` — ``{canonical: (kind, text)}`` lookup
          consumed by ``_fuse_entity_hits`` as a fallback when the corpus
          doesn't carry the canonical (pure-named call with no corpus,
          or hidden-from-corpus refs).
        * ``warnings`` — drop reasons per filter (off-datasource,
          hidden, missing).
        """
        entity_ranking: List[str] = []
        named_kind_text: Dict[str, Tuple[str, str]] = {}
        warnings: List[str] = []
        for canonical in canonical_input_entities:
            if canonical.startswith(_MEMORY_PREFIX):
                # memory:<id> refs participate in the memory ranking only.
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
            named_kind_text[canonical] = (result.kind, result.text)
        return entity_ranking, named_kind_text, warnings

    def _run_channel_2(
        self,
        *,
        corpus: Optional[Corpus],
        question: Optional[str],
    ) -> Tuple[List[str], List[str], dict[str, IndexHit]]:
        """Tantivy full-text channel.

        DEV-1414: runs as TWO kind-filtered queries — one over memory
        docs only, one over entity docs only — so the per-kind ranking
        is a pure function of the corpus + question, never affected by
        the other kind's cap. The ``limit`` for each call is the size of
        the corresponding kind in the corpus, so each query returns the
        complete per-kind ranking.

        Returns ``(memory_ranking, entity_ranking_canonicals,
        by_memory_id_hits)``. Empty when ``corpus`` or ``question`` is
        missing.
        """
        if corpus is None or not question or not question.strip():
            return [], [], {}
        memory_count, entity_count = _count_corpus_kinds(corpus)
        memory_hits = (
            search_index(
                index=corpus.index,
                question=question,
                limit=memory_count,
                kind_filter="memory",
            )
            if memory_count > 0
            else []
        )
        entity_hits = (
            search_index(
                index=corpus.index,
                question=question,
                limit=entity_count,
                exclude_kind="memory",
            )
            if entity_count > 0
            else []
        )
        memory_ranking: List[str] = []
        by_memory_id: dict[str, IndexHit] = {}
        for hit in memory_hits:
            if hit.memory_id is None:
                continue
            memory_ranking.append(hit.memory_id)
            by_memory_id[hit.memory_id] = hit
        entity_ranking = [h.id for h in entity_hits]
        return memory_ranking, entity_ranking, by_memory_id

    async def _run_channel_3(
        self,
        *,
        question: Optional[str],
        corpus: Optional[Corpus],
        question_active: bool,
        datasource: Optional[str] = None,
        eligible_memory_canonicals: Optional[Set[str]] = None,
    ) -> Tuple[List[str], List[str], List[str]]:
        """Embedding-similarity channel (DEV-1386). Returns
        ``(memory_ranking, entity_ranking_canonicals, warnings)``.

        DEV-1414: the corpus is partitioned by ``entity_kind`` and each
        kind is ranked in full via two cosine calls. The per-kind
        ranking is a pure function of the corpus + question.

        Skipped (with a warning) when:

        * ``question`` is empty,
        * the ``embedding_search`` extra is not installed,
        * the active model has no embedding rows in storage,
        * the query embedding call fails.

        DEV-1409: when ``datasource`` is set, the corpus is pre-filtered
        before the matrix build so cosine similarity is computed only
        against:

        * entity rows (``entity_kind != 'memory'``) rooted at the
          requested datasource (exact match or dotted-path descendant),
        * memory rows whose ``canonical_id`` appears in the supplied
          ``eligible_memory_canonicals`` set (already datasource-filtered
          upstream).

        DEV-1414: rows whose ``canonical_id`` is not in the live tantivy
        corpus (stale memory ids, hidden / deleted entities) are dropped
        before the matrix build. Otherwise stale rows would consume
        cosine rank positions and degrade live docs' RRF scores —
        invariant under cap changes (so the per-bucket contract still
        holds) but surprising and lossy. The filter keeps the channel's
        candidate set aligned with channel 2's tantivy corpus.
        """
        if not question_active or corpus is None:
            return [], [], []
        if not embedding_client.is_available():
            return [], [], [
                "embedding channel skipped: `embedding_search` extra not "
                "installed or no API key configured for the active "
                "embedding model.",
            ]

        # Local import to break the ``slayer.search`` ↔ ``slayer.embeddings``
        # cycle (the embedding service imports render helpers from
        # ``slayer.search.render``).
        from slayer.embeddings.service import EmbeddingService

        service = EmbeddingService(storage=self._storage)
        rows = await service.fetch_corpus()
        if datasource is not None:
            rows = _filter_embedding_corpus_by_datasource(
                rows,
                datasource=datasource,
                eligible_memory_canonicals=eligible_memory_canonicals or set(),
            )
        # Drop sidecar rows that don't correspond to anything in the
        # live tantivy corpus (DEV-1414). Memory rows are keyed
        # ``memory:<int>`` in storage and as the corpus's
        # ``canonical_to_kind`` key; entity rows share the canonical
        # string directly. Both shapes match by single dict lookup.
        live_canonicals = corpus.canonical_to_kind.keys()
        rows = [r for r in rows if r.canonical_id in live_canonicals]
        if not rows:
            return [], [], [
                f"embedding channel skipped: no embedding rows for model "
                f"{service.model_name!r}. Run `slayer ingest` to populate.",
            ]
        try:
            import numpy as np
            from slayer.embeddings.ranker import (
                normalise,
                normalise_matrix,
                top_k_cosine,
            )
        except ImportError:
            return [], [], [
                "embedding channel skipped: numpy not installed "
                "(reinstall with the `embedding_search` extra).",
            ]
        query_vec = await service.embed_question(question or "")
        if query_vec is None:
            return [], [], [
                "embedding channel skipped: query embedding failed.",
            ]
        # All persisted rows share the active model's dim; sample any
        # row to detect a stale-dim corpus before partitioning.
        if len(rows[0].embedding) != len(query_vec):
            return [], [], [
                f"embedding channel skipped: dim mismatch "
                f"(query={len(query_vec)}, corpus={len(rows[0].embedding)}). "
                f"Re-run `slayer ingest` to refresh embeddings against "
                f"the current model.",
            ]

        memory_rows = [r for r in rows if r.entity_kind == "memory"]
        entity_rows = [r for r in rows if r.entity_kind != "memory"]
        normalised_query = normalise(query_vec)
        ranked_memory_canonicals = _rank_embedding_kind(
            rows=memory_rows,
            normalised_query=normalised_query,
            np=np,
            normalise_matrix=normalise_matrix,
            top_k_cosine=top_k_cosine,
        )
        memory_ranking: List[str] = []
        for canonical in ranked_memory_canonicals:
            memory_id = _memory_id_from_canonical(canonical)
            if memory_id is not None:
                memory_ranking.append(memory_id)
        entity_ranking = _rank_embedding_kind(
            rows=entity_rows,
            normalised_query=normalised_query,
            np=np,
            normalise_matrix=normalise_matrix,
            top_k_cosine=top_k_cosine,
        )
        return memory_ranking, entity_ranking, []

    async def _valid_canonical_set(
        self,
        *,
        all_memories: List[Memory],
        datasource: Optional[str],
    ) -> set:
        """DEV-1428: live canonical set used to filter stale entity tags
        out of memory ``entities`` lists before BM25 ranking and before
        ``matched_entities`` is surfaced.

        Walks datasources + every persisted model identity (cheap
        per-storage call) + ``memory:<id>`` for every memory in scope.
        Datasource-filtered when ``datasource`` is set.
        """
        canonicals: set = set()
        canonicals.update(
            await self._collect_datasource_canonicals(datasource=datasource)
        )
        canonicals.update(
            await self._collect_model_subtree_canonicals(datasource=datasource)
        )
        canonicals.update(_collect_memory_canonicals(all_memories))
        return canonicals

    async def _collect_datasource_canonicals(
        self, *, datasource: Optional[str],
    ) -> set:
        """Set of bare datasource canonical ids, narrowed by ``datasource``."""
        names = await self._storage.list_datasources()
        if datasource is not None:
            names = [d for d in names if d == datasource]
        return set(names)

    async def _collect_model_subtree_canonicals(
        self, *, datasource: Optional[str],
    ) -> set:
        """Set of `<ds>.<model>[.<leaf>]` canonical ids across every
        persisted model identity, narrowed by ``datasource`` when set."""
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
        example_query_hits: List["ExampleQueryHit"],
        memory_by_id: Dict[str, Memory],
    ) -> List[str]:
        """DEV-1428: emit one warning per example_queries hit whose
        attached ``Memory.query`` references entities that no longer
        resolve. The query is NOT rewritten — agents who notice the
        warning can re-save the memory to clean it."""
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

    async def _stale_query_warnings_for_named_memory_refs(
        self,
        *,
        canonical_input_entities: List[str],
        all_memories: List[Memory],
        already_warned_ids: Set[str],
    ) -> List[str]:
        """DEV-1513: emit the stale-query warning for any explicitly-named
        ``memory:<id>`` ref pointing at a query-bearing memory with
        stale refs, regardless of whether the example_queries cap
        suppressed the hit. The user explicitly named the memory; they
        deserve to know the attached query is broken."""
        memories_by_id = {m.id: m for m in all_memories}
        out: List[str] = []
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
        datasource: Optional[str] = None,
    ) -> Tuple[List[SlayerModel], List[str]]:
        """Walk datasources + models into the in-memory corpus.

        DEV-1409: when ``datasource`` is set, only models in that one
        datasource are walked, and only that datasource's doc lands in
        the returned list. Validation that ``datasource`` is known
        happens upstream in ``SearchService.search`` so this method
        stays cheap.
        """
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
