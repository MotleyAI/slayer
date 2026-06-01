"""BM25 retriever — entity-overlap BM25 over memory tags (DEV-1514).

Ports the body of the former ``SearchService._run_channel_1`` into a
standalone :class:`~slayer.search.retriever.Retriever`. Stateless: no
persistence, no write hooks (defaults to ABC no-op).

DEV-1513: every memory's effective tag list is augmented with
``memory:<self_id>`` before BM25 ranking, so a user-supplied
``memory:<id>`` ref surfaces the named memory at the top of the BM25
ranking. Augmentation runs after the stale-tag filter so the self-ref
cannot be stripped even if ``valid_canonicals`` ever drifted.
"""

from __future__ import annotations

from typing import List, Optional

from slayer.memories.models import MEMORY_CANONICAL_PREFIX as _MEMORY_PREFIX
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


def _augment_with_self_refs(memories: List[Memory]) -> List[Memory]:
    """DEV-1513: augment each memory's ``entities`` with
    ``memory:<self_id>`` so a user-supplied ``memory:<id>`` ref surfaces
    the named memory at the top of the BM25 ranking. Idempotent."""
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
        # ``valid_canonicals`` is always supplied (non-Optional in the
        # ABC); empty set means "no entities are live — drop every
        # stale tag", which makes ``_filter_memories_entities`` strip
        # everything and BM25 returns empty. Don't truthy-check it.
        filtered = _filter_memories_entities(
            all_memories, valid_canonicals=valid_canonicals,
        )
        # DEV-1513: self-ref augmentation runs AFTER the stale-tag
        # filter so the synthetic ref always survives.
        augmented = _augment_with_self_refs(filtered)
        ranked = bm25_rank(
            memories=augmented, query_entities=query_entities,
        )
        return RetrievalResult(
            memory_ranking=[mem.id for mem, _ in ranked],
        )
