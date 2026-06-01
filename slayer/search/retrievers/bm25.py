"""BM25 retriever — entity-overlap BM25 over memory tags (DEV-1514).

Ports the body of the former ``SearchService._run_channel_1`` into a
standalone :class:`~slayer.search.retriever.Retriever`. Stateless: no
persistence, no write hooks (defaults to ABC no-op).
"""

from __future__ import annotations

from typing import List, Optional

from slayer.memories.models import Memory
from slayer.memories.ranker import bm25_rank
from slayer.search.index import Corpus
from slayer.search.retriever import RetrievalResult, Retriever


def _filter_memories_entities(
    memories: List[Memory], *, valid_canonicals: set,
) -> List[Memory]:
    """Return shallow copies of ``memories`` whose ``entities`` lists
    are filtered down to ``valid_canonicals`` only (DEV-1428). Used to
    feed BM25 a stale-free corpus without writing back to storage."""
    out: List[Memory] = []
    for m in memories:
        live = [e for e in m.entities if e in valid_canonicals]
        if live == m.entities:
            out.append(m)
        else:
            out.append(m.model_copy(update={"entities": live}))
    return out


class BM25Retriever(Retriever):
    """BM25 over memory entity tags. Contributes only to memory
    ranking — has nothing to say about entity documents."""

    name = "bm25"

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
        if not query_entities:
            return RetrievalResult()
        filtered = (
            _filter_memories_entities(
                all_memories, valid_canonicals=valid_canonicals,
            )
            if valid_canonicals
            else all_memories
        )
        ranked = bm25_rank(
            memories=filtered, query_entities=query_entities,
        )
        return RetrievalResult(
            memory_ranking=[mem.id for mem, _ in ranked],
        )
