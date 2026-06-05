"""Tantivy retriever — in-memory full-text channel (DEV-1514).

Ports the body of the former ``SearchService._run_channel_2`` into a
standalone :class:`~slayer.search.retriever.Retriever`. The two
kind-filtered queries (memory + entity) run SEQUENTIALLY inside one
:meth:`retrieve` call (Codex Finding 2) so neither competes with the
other against the same in-memory ``tantivy.Index``.

This PR keeps the index in-memory (rebuilt per search call by
:class:`SearchService`). A future PR can override the write hooks to
persist segments on disk and mutate them in step with the embedding
retriever — that change requires no facade modification.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from slayer.memories.models import Memory
from slayer.search.index import Corpus, IndexHit, search_index
from slayer.search.retriever import RetrievalResult, Retriever


def _count_corpus_kinds(corpus: Corpus) -> Tuple[int, int]:
    """Return ``(memory_count, entity_count)`` for a built corpus.

    Used to pass ``limit = full per-kind corpus size`` to each
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


class TantivyRetriever(Retriever):
    """Two kind-filtered tantivy queries per retrieve call. Returns
    both memory + entity rankings and populates ``text_by_id`` from
    the memory hits."""

    name = "tantivy"

    async def retrieve(
        self,
        *,
        query_entities: List[str],
        question: Optional[str],
        all_memories: List[Memory],
        valid_canonicals: set,
        corpus: Optional[Corpus],
        datasource: Optional[str],
    ) -> RetrievalResult:
        if corpus is None or not question or not question.strip():
            return RetrievalResult()

        memory_count, entity_count = _count_corpus_kinds(corpus)
        memory_hits: List[IndexHit] = (
            search_index(
                index=corpus.index,
                question=question,
                limit=memory_count,
                kind_filter="memory",
            )
            if memory_count > 0
            else []
        )
        entity_hits: List[IndexHit] = (
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
        text_by_id: Dict[str, str] = {}
        for hit in memory_hits:
            if hit.memory_id is None:
                continue
            memory_ranking.append(hit.memory_id)
            text_by_id[hit.memory_id] = hit.text
        entity_ranking = [h.id for h in entity_hits]

        return RetrievalResult(
            memory_ranking=memory_ranking,
            text_by_id=text_by_id,
            entity_ranking=entity_ranking,
        )
