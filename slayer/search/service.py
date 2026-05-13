"""SearchService — three-channel + RRF orchestrator (DEV-1375 / DEV-1386).

* **Channel 1** — entity-overlap BM25 over memories
  (``slayer.memories.ranker.bm25_rank``). Skipped when neither
  ``entities`` nor ``query`` is supplied. Contributes only to the memory
  ranking.
* **Channel 2** — tantivy full-text over memories ∪ entities. Skipped
  when ``question`` is empty. Contributes to both the memory ranking
  and the entity ranking.
* **Channel 3** — dense embedding similarity over memories ∪ entities
  (DEV-1386). Skipped when ``question`` is empty, when the
  ``embedding_search`` extra is not installed, when the query
  embedding call fails, or when there are no embedding rows for the
  active model name. Contributes to both the memory ranking and the
  entity ranking.

Memory rankings from every active channel are fused via RRF
(``k = 60``). Entity rankings from channels 2 and 3 are fused the same
way. Channel 1 does not contribute to entity ranking (it operates on
memory entity tags, not on entity docs).

Empty input (no entities, no query, no question) falls back to recency:
newest ``max_memories`` learning-only memories + newest
``max_example_queries`` query-bearing memories, with a warning.
"""

from __future__ import annotations

from typing import List, Optional, Set, Tuple, Union

from pydantic import BaseModel, Field

from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.embeddings import client as embedding_client
from slayer.embeddings.models import Embedding
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
from slayer.search.rrf import rrf_fuse
from slayer.storage.base import StorageBackend


_RRF_K = 60
_OVER_FETCH_MULTIPLIER = 5


# ---------------------------------------------------------------------------
# Hit & response models
# ---------------------------------------------------------------------------


class MemoryHit(BaseModel):
    """A learning-only memory result (``Memory.query is None``). ``id`` is
    the integer memory id (suitable for ``forget_memory(id=hit.id)``).
    ``score`` is always the Reciprocal-Rank-Fusion score
    (``Σ 1 / (k + rank)``, ``k=60``); even single-channel searches go
    through RRF, so the value is comparable across channels but is not
    directly the raw BM25 / tantivy / cosine score."""

    id: int
    score: float
    text: str
    matched_entities: List[str] = Field(default_factory=list)


class ExampleQueryHit(BaseModel):
    """A query-bearing memory result (``Memory.query`` is set). Same id /
    score / text shape as ``MemoryHit`` but always carries the attached
    ``SlayerQuery``. Surfaces in ``SearchResponse.example_queries`` —
    bulky reference material, capped independently from learning-only
    memories so it cannot crowd them out."""

    id: int
    score: float
    text: str
    matched_entities: List[str] = Field(default_factory=list)
    query: SlayerQuery


class EntityHit(BaseModel):
    """An entity result. ``id`` is the canonical entity string
    (``"<ds>"``, ``"<ds>.<model>"``, or ``"<ds>.<model>.<leaf>"``).
    ``score`` is the RRF-fused score across channels 2 and 3 (or the
    single-channel raw score when only one channel contributed)."""

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


def _split_tantivy_hits(
    hits: List[IndexHit],
) -> Tuple[List[int], List[IndexHit], dict[int, IndexHit]]:
    """Sort tantivy hits into the memory ranking, the entity-hit list, and
    a memory-id→hit lookup."""
    memory_ranking: List[int] = []
    entity_hits: List[IndexHit] = []
    by_memory_id: dict[int, IndexHit] = {}
    for hit in hits:
        if hit.kind == "memory" and hit.memory_id is not None:
            memory_ranking.append(hit.memory_id)
            by_memory_id[hit.memory_id] = hit
        else:
            entity_hits.append(hit)
    return memory_ranking, entity_hits, by_memory_id


def _backfill_memory_by_id(
    *,
    memory_by_id: dict,
    all_memories: List["Memory"],
    mem_ids,
) -> None:
    """For each id in ``mem_ids`` not already in ``memory_by_id``, look it
    up in ``all_memories`` and insert it. Mutates ``memory_by_id``."""
    for mem_id in mem_ids:
        if mem_id in memory_by_id:
            continue
        mem = next((m for m in all_memories if m.id == mem_id), None)
        if mem is not None:
            memory_by_id[mem_id] = mem


def _build_memory_hit(
    *,
    mem: "Memory",
    memory_id: int,
    score: float,
    index_hits_by_memory_id: dict,
    canonical_input_entities: List[str],
) -> Union["MemoryHit", "ExampleQueryHit"]:
    """Build the appropriate hit type for ``mem``: ``MemoryHit`` for
    learning-only memories (``query is None``), ``ExampleQueryHit`` for
    query-bearing ones. ``text`` falls back to ``mem.learning`` when the
    memory wasn't reached via tantivy."""
    wanted_set = set(canonical_input_entities)
    matched = sorted(wanted_set & set(mem.entities)) if wanted_set else []
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


def _fuse_memory_hits(
    *,
    rankings: List[List[int]],
    memory_by_id: dict,
    index_hits_by_memory_id: dict,
    canonical_input_entities: List[str],
    max_memories: int,
    max_example_queries: int,
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


def _split_embedding_pairs(
    pairs: List[Tuple[int, float]],
    rows: List["Embedding"],
) -> Tuple[List[int], List[str]]:
    """Split ``(row_idx, score)`` pairs from cosine top-k into
    ``(memory_ranking, entity_ranking)``. Skips memory rows whose
    ``canonical_id`` doesn't parse back to a valid int — those would
    only appear if a backend smuggled in a malformed row."""
    memory_ranking: List[int] = []
    entity_ranking: List[str] = []
    for idx, _score in pairs:
        row = rows[idx]
        if row.entity_kind == "memory":
            try:
                memory_id = int(row.canonical_id.split(":", 1)[1])
            except (IndexError, ValueError):
                continue
            memory_ranking.append(memory_id)
        else:
            entity_ranking.append(row.canonical_id)
    return memory_ranking, entity_ranking


def _fuse_entity_hits(
    *,
    rankings: List[List[str]],
    corpus: Optional[Corpus],
    max_entities: int,
) -> List[EntityHit]:
    """RRF-fuse the entity rankings and look text/kind up from the corpus
    map. Returns at most ``max_entities`` hits."""
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


class SearchService:
    """Orchestrates the three retrieval channels + RRF fusion."""

    def __init__(self, *, storage: StorageBackend) -> None:
        self._storage = storage

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

        # Over-fetch must cover the worst case where every fused hit lands
        # in the same bucket (all learnings, or all example queries).
        over_fetch_budget = max(
            max_memories + max_example_queries, max_entities,
        ) * _OVER_FETCH_MULTIPLIER

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
            over_fetch=over_fetch_budget,
            channel_1_active=channel_1_active,
        )
        (
            channel_2_memory_ranking,
            channel_2_entity_ranking,
            index_hits_by_memory_id,
        ) = self._run_channel_2(
            corpus=corpus,
            question=question,
            over_fetch=over_fetch_budget,
        )
        (
            channel_3_memory_ranking,
            channel_3_entity_ranking,
            channel_3_warnings,
        ) = await self._run_channel_3(
            question=question,
            corpus=corpus,
            over_fetch=over_fetch_budget,
            question_active=question_active,
            datasource=datasource,
            eligible_memory_canonicals={f"memory:{m.id}" for m in all_memories},
        )
        warnings = _dedup(warnings + channel_3_warnings)

        # Backfill memory_by_id from every channel so RRF can resolve
        # any memory hit downstream.
        _backfill_memory_by_id(
            memory_by_id=memory_by_id,
            all_memories=all_memories,
            mem_ids=channel_1_memory_ranking,
        )
        _backfill_memory_by_id(
            memory_by_id=memory_by_id,
            all_memories=all_memories,
            mem_ids=index_hits_by_memory_id.keys(),
        )
        _backfill_memory_by_id(
            memory_by_id=memory_by_id,
            all_memories=all_memories,
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
        )
        entity_hits = _fuse_entity_hits(
            rankings=[channel_2_entity_ranking, channel_3_entity_ranking],
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

    async def _resolve_inputs(
        self,
        *,
        entities: Optional[List[str]],
        query: Optional[Union[SlayerQuery, dict]],
    ) -> Tuple[List[str], List[str]]:
        """Walk ``entities`` + ``query`` into a deduped canonical-entity list
        plus a deduped warning list."""
        canonical: List[str] = []
        warnings: List[str] = []
        if entities:
            for raw in entities:
                if not isinstance(raw, str):
                    raise ValueError(
                        f"entities list items must be strings; got "
                        f"{type(raw).__name__}."
                    )
                result = await resolve_entity(raw=raw, storage=self._storage)
                canonical.extend(result.canonical_forms)
                warnings.extend(result.warnings)
        if query is not None:
            extraction = await extract_entities_from_query(
                query=_coerce_query(query), storage=self._storage,
            )
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
        memory_hits: List[MemoryHit] = []
        example_query_hits: List[ExampleQueryHit] = []
        for m in recency_memories:
            hit = _build_memory_hit(
                mem=m,
                memory_id=m.id,
                score=0.0,
                index_hits_by_memory_id={},
                canonical_input_entities=[],
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
        over_fetch: int,
        channel_1_active: bool,
    ) -> Tuple[List[int], dict[int, Memory]]:
        """Entity-overlap BM25 channel."""
        channel_1_memory_ranking: List[int] = []
        memory_by_id: dict[int, Memory] = {}
        if channel_1_active and canonical_input_entities:
            ranked = bm25_rank(
                memories=all_memories,
                query_entities=canonical_input_entities,
            )
            for memory, _score in ranked[:over_fetch]:
                memory_by_id[memory.id] = memory
                channel_1_memory_ranking.append(memory.id)
        return channel_1_memory_ranking, memory_by_id

    def _run_channel_2(
        self,
        *,
        corpus: Optional[Corpus],
        question: Optional[str],
        over_fetch: int,
    ) -> Tuple[List[int], List[str], dict[int, IndexHit]]:
        """Tantivy full-text channel. Returns
        ``(memory_ranking, entity_ranking_canonicals, by_memory_id_hits)``.
        Empty when ``corpus`` or ``question`` is missing."""
        if corpus is None or not question or not question.strip():
            return [], [], {}
        tantivy_hits = search_index(
            index=corpus.index, question=question, limit=over_fetch,
        )
        (
            memory_ranking,
            entity_hits,
            by_memory_id,
        ) = _split_tantivy_hits(tantivy_hits)
        entity_ranking = [h.id for h in entity_hits]
        return memory_ranking, entity_ranking, by_memory_id

    async def _run_channel_3(
        self,
        *,
        question: Optional[str],
        corpus: Optional[Corpus],
        over_fetch: int,
        question_active: bool,
        datasource: Optional[str] = None,
        eligible_memory_canonicals: Optional[Set[str]] = None,
    ) -> Tuple[List[int], List[str], List[str]]:
        """Embedding-similarity channel (DEV-1386). Returns
        ``(memory_ranking, entity_ranking_canonicals, warnings)``.

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

        Both `rows[idx]` indexing and the matrix stay aligned because
        the filter happens before `np.array(...)`.
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
        matrix = np.array([r.embedding for r in rows], dtype=np.float32)
        if matrix.shape[1] != len(query_vec):
            return [], [], [
                f"embedding channel skipped: dim mismatch "
                f"(query={len(query_vec)}, corpus={matrix.shape[1]}). "
                f"Re-run `slayer ingest` to refresh embeddings against "
                f"the current model.",
            ]
        pairs = top_k_cosine(
            query=normalise(query_vec),
            matrix=normalise_matrix(matrix),
            k=over_fetch,
        )
        memory_ranking, entity_ranking = _split_embedding_pairs(pairs, rows)
        return memory_ranking, entity_ranking, []

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
