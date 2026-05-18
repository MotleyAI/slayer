"""Reciprocal Rank Fusion (DEV-1375).

Cormack et al. 2009 standard formula::

    score(d) = Σ_r 1 / (k + rank_r(d))

with ranks 1-indexed (top of each list = rank 1) and ``k = 60`` by
convention. Documents present in only some rankings get a partial score
from those rankings; documents in no ranking are absent from the output.

Hand-rolled (~10 lines) to avoid pulling in a heavy fusion library.
"""

from __future__ import annotations

from typing import Hashable, List, TypeVar

K = TypeVar("K", bound=Hashable)


def rrf_fuse(
    *,
    rankings: List[List[K]],
    k: int = 60,
) -> dict[K, float]:
    """Fuse multiple ranked lists into one score map.

    Args:
        rankings: A list of ranked-document lists, one per ranker. Each
            inner list is in best-first order (index 0 = rank 1).
        k: The RRF constant. Cormack 2009 standard is 60.

    Returns:
        ``{document_id: fused_score}`` — sort by score descending to get
        the fused ranking.
    """
    fused: dict[K, float] = {}
    for ranking in rankings:
        for index, doc_id in enumerate(ranking):
            rank = index + 1
            fused[doc_id] = fused.get(doc_id, 0.0) + 1.0 / (k + rank)
    return fused
