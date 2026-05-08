"""BM25 ranking for memory retrieval (DEV-1365).

Replaces the naive `match_count = |wanted ∩ memory.entities|` ranker
that previously drove `MemoryService.recall_memories`. The old ranker
trivially favoured memories with large entity sets — a memory tagged
with 50 entities would out-overlap a precisely-tagged one of 2
regardless of relevance. BM25's length normalisation is the explicit
fix.

We use ``BM25Plus`` (not ``BM25Okapi``) because the typical memory
corpus is small (tens of memories), and ``BM25Okapi``'s IDF goes
negative when a term appears in even a moderate fraction of the
corpus. With negative IDF, BM25's length normalisation inverts —
broad memories get higher scores than narrow ones, which is the
exact bug DEV-1365 is trying to fix. ``BM25Plus`` uses
``IDF = log((N+1)/df)``, always positive, so length normalisation
behaves as intended. ``k1=1.5``/``b=0.75``/``delta=1`` are the
library defaults.

Tokenisation is identity: each canonical entity string (e.g.
``mydb.orders.amount``) is one atomic token. Memory entity lists are
deduped defensively so a malformed row cannot game ranking via
repetition. The "must overlap on ≥1 entity" rule is enforced by an
explicit set-intersection pre-filter; BM25 is used purely to rank the
eligible set. (``BM25Plus`` adds a constant ``delta`` per query term
to *every* score, including memories with zero overlap — without the
pre-filter, every memory in storage would be returned.)
"""

from __future__ import annotations

from typing import List, Tuple

from rank_bm25 import BM25Plus

from slayer.memories.models import Memory


def bm25_rank(
    memories: List[Memory],
    query_entities: List[str],
) -> List[Tuple[Memory, float]]:
    """Rank ``memories`` against ``query_entities`` using BM25Plus.

    Returns ``(memory, score)`` pairs sorted by score descending.
    Memories with no entity overlap with ``query_entities`` are
    excluded.

    BM25 statistics (IDF, avgdl) are computed over the **full**
    ``memories`` corpus so length normalisation reflects the real
    distribution. The eligible-subset filter is applied after
    scoring.

    Empty corpus or empty query both return ``[]``.
    """
    if not memories or not query_entities:
        return []

    query_set = set(query_entities)
    eligible = [
        i for i, memory in enumerate(memories)
        if set(memory.entities) & query_set
    ]
    if not eligible:
        return []

    tokenised = [list(set(memory.entities)) for memory in memories]
    bm25 = BM25Plus(tokenised)
    scores = bm25.get_scores(list(query_set))

    paired: List[Tuple[Memory, float]] = [
        (memories[i], float(scores[i])) for i in eligible
    ]
    paired.sort(key=lambda pair: pair[1], reverse=True)
    return paired


__all__ = ["bm25_rank"]
