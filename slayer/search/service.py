"""SearchService — two-channel + RRF orchestrator (DEV-1375).

* Channel 1: entity-overlap BM25 over memories (existing
  ``slayer.memories.ranker.bm25_rank``). Skipped when neither
  ``entities`` nor ``query`` is supplied.
* Channel 2: tantivy full-text over memories ∪ entities. Skipped when
  ``question`` is empty.

Memory hits from both channels are fused via RRF; entity hits come from
channel 2 only and surface their raw tantivy BM25 score.

Empty input (no entities, no query, no question) falls back to recency:
newest ``max_memories`` memories with an explanatory warning.
"""

from __future__ import annotations

from typing import List, Optional, Set, Union

from pydantic import BaseModel, Field

from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.memories.models import Memory
from slayer.memories.ranker import bm25_rank
from slayer.memories.resolver import (
    extract_entities_from_query,
    resolve_entity,
)
from slayer.search.index import IndexHit, build_in_memory_index, search_index
from slayer.search.rrf import rrf_fuse
from slayer.storage.base import StorageBackend


_RRF_K = 60
_OVER_FETCH_MULTIPLIER = 5


# ---------------------------------------------------------------------------
# Hit & response models
# ---------------------------------------------------------------------------


class MemoryHit(BaseModel):
    """A memory result. ``id`` is the integer memory id (suitable for
    ``forget_memory(id=hit.id)``). ``score`` is the RRF-fused score
    when both channels contributed; otherwise the channel's raw score."""

    id: int
    score: float
    text: str
    matched_entities: List[str] = Field(default_factory=list)
    query: Optional[SlayerQuery] = None


class EntityHit(BaseModel):
    """An entity result. ``id`` is the canonical entity string
    (``"<ds>"``, ``"<ds>.<model>"``, or ``"<ds>.<model>.<leaf>"``)."""

    id: str
    kind: str  # "datasource" | "model" | "column" | "measure" | "aggregation"
    score: float
    text: str


class SearchResponse(BaseModel):
    memories: List[MemoryHit] = Field(default_factory=list)
    entities: List[EntityHit] = Field(default_factory=list)
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


class SearchService:
    """Orchestrates the two retrieval channels + RRF fusion."""

    def __init__(self, *, storage: StorageBackend) -> None:
        self._storage = storage

    async def search(
        self,
        *,
        entities: Optional[List[str]] = None,
        query: Optional[Union[SlayerQuery, dict]] = None,
        question: Optional[str] = None,
        max_memories: int = 5,
        max_entities: int = 5,
    ) -> SearchResponse:
        if max_memories < 0:
            raise ValueError(f"max_memories must be >= 0; got {max_memories}.")
        if max_entities < 0:
            raise ValueError(f"max_entities must be >= 0; got {max_entities}.")

        warnings: List[str] = []
        canonical_input_entities: List[str] = []

        # Channel-1 input: union of resolved entity-list + entities
        # extracted from query.
        channel_1_active = (entities is not None and len(entities) > 0) or query is not None
        if entities:
            for raw in entities:
                if not isinstance(raw, str):
                    raise ValueError(
                        f"entities list items must be strings; got "
                        f"{type(raw).__name__}."
                    )
                result = await resolve_entity(raw, storage=self._storage)
                canonical_input_entities.extend(result.canonical_forms)
                warnings.extend(result.warnings)
        if query is not None:
            extraction = await extract_entities_from_query(
                _coerce_query(query), storage=self._storage,
            )
            canonical_input_entities.extend(extraction.canonical_forms)
            warnings.extend(extraction.warnings)

        canonical_input_entities = _dedup(canonical_input_entities)
        warnings = _dedup(warnings)

        question_active = bool(question and question.strip())

        # Recency fallback for the all-empty case (mirrors recall_memories).
        if not channel_1_active and not question_active:
            warnings.append(
                "no entities, query, or question supplied; returning "
                "newest memories by recency."
            )
            recency_memories = await self._storage.list_memories(entities=None)
            recency_memories.sort(key=lambda m: m.created_at, reverse=True)
            recency_memories = recency_memories[:max_memories]
            return SearchResponse(
                memories=[
                    MemoryHit(
                        id=m.id,
                        score=0.0,
                        text=m.learning,
                        matched_entities=[],
                        query=m.query,
                    )
                    for m in recency_memories
                ],
                entities=[],
                warnings=warnings,
            )

        over_fetch = max(max_memories, max_entities) * _OVER_FETCH_MULTIPLIER

        # Single memory-corpus fetch shared by both channels — avoids two
        # full scans when entities/query AND question are both supplied.
        all_memories: List[Memory] = []
        if channel_1_active or question_active:
            all_memories = await self._storage.list_memories(entities=None)

        # ---- Channel 1: entity-overlap BM25 over memories ------------------
        channel_1_memory_ranking: List[int] = []
        memory_by_id: dict[int, Memory] = {}
        if channel_1_active and canonical_input_entities:
            ranked = bm25_rank(all_memories, canonical_input_entities)
            for memory, _score in ranked[:over_fetch]:
                memory_by_id[memory.id] = memory
                channel_1_memory_ranking.append(memory.id)

        # ---- Channel 2: tantivy full-text over memories ∪ entities ---------
        channel_2_memory_ranking: List[int] = []
        channel_2_entity_hits: List[IndexHit] = []
        index_hits_by_memory_id: dict[int, IndexHit] = {}

        if question_active:
            all_models, datasources = await self._collect_index_corpus()
            index = build_in_memory_index(
                memories=all_memories,
                models=all_models,
                datasources=datasources,
            )
            tantivy_hits = search_index(
                index=index,
                question=question,
                limit=over_fetch,
            )
            for hit in tantivy_hits:
                if hit.kind == "memory" and hit.memory_id is not None:
                    channel_2_memory_ranking.append(hit.memory_id)
                    index_hits_by_memory_id[hit.memory_id] = hit
                else:
                    channel_2_entity_hits.append(hit)
            # Make sure channel-1's memories surface as MemoryHits even
            # when channel 2 didn't visit them.
            for mem_id in channel_1_memory_ranking:
                if mem_id not in memory_by_id:
                    mem = next((m for m in all_memories if m.id == mem_id), None)
                    if mem is not None:
                        memory_by_id[mem_id] = mem
            # And the inverse: channel-2 memories not in channel-1.
            for mem_id, hit in index_hits_by_memory_id.items():
                if mem_id not in memory_by_id:
                    mem = next((m for m in all_memories if m.id == mem_id), None)
                    if mem is not None:
                        memory_by_id[mem_id] = mem

        # ---- RRF fusion of memory rankings ---------------------------------
        rankings: List[List[int]] = []
        if channel_1_memory_ranking:
            rankings.append(channel_1_memory_ranking)
        if channel_2_memory_ranking:
            rankings.append(channel_2_memory_ranking)
        fused = rrf_fuse(rankings=rankings, k=_RRF_K) if rankings else {}
        fused_sorted = sorted(fused.items(), key=lambda kv: kv[1], reverse=True)

        memory_hits: List[MemoryHit] = []
        wanted_set = set(canonical_input_entities)
        for memory_id, score in fused_sorted[:max_memories]:
            mem = memory_by_id.get(memory_id)
            if mem is None:
                continue
            matched = sorted(wanted_set & set(mem.entities)) if wanted_set else []
            memory_hits.append(MemoryHit(
                id=memory_id,
                score=score,
                text=(
                    index_hits_by_memory_id[memory_id].text
                    if memory_id in index_hits_by_memory_id
                    else mem.learning
                ),
                matched_entities=matched,
                query=mem.query,
            ))

        # ---- Entity hits (channel 2 only) ----------------------------------
        entity_hits = [
            EntityHit(
                id=hit.id,
                kind=hit.kind,
                score=hit.score,
                text=hit.text,
            )
            for hit in channel_2_entity_hits[:max_entities]
        ]

        return SearchResponse(
            memories=memory_hits,
            entities=entity_hits,
            warnings=warnings,
        )

    async def _collect_index_corpus(
        self,
    ) -> tuple[List[SlayerModel], List[str]]:
        """Walk every datasource + every model into the in-memory corpus."""
        datasources = await self._storage.list_datasources()
        models: List[SlayerModel] = []
        identities = await self._storage._list_all_model_identities()
        for ds, name in identities:
            m = await self._storage.get_model(name, data_source=ds)
            if m is not None:
                models.append(m)
        return models, datasources
