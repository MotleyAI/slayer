"""Numpy-based top-k cosine similarity for embedding retrieval (DEV-1386).

Pure numpy on the assumption that the embedding corpus stays in the
low-thousands of docs (memories + every non-hidden datasource / model /
column / measure / aggregation). Loading the matrix into RAM per search
call costs O(N*dim) memory + one matmul; both are negligible at this
scale and avoid the operational burden of a persistent ANN index.

Imports numpy at module top — this module is only imported behind the
``advanced_search`` extra's gate, so a missing numpy is a programming
error here, not a runtime fallback.
"""

from __future__ import annotations

from typing import List, Tuple

import numpy as np


def normalise(vector: List[float]) -> np.ndarray:
    """Return a unit-L2 numpy view of ``vector``. Zero vectors come back
    unchanged so we never divide by zero."""
    arr = np.asarray(vector, dtype=np.float32)
    norm = float(np.linalg.norm(arr))
    if norm <= 0.0:
        return arr
    return arr / norm


def normalise_matrix(matrix: np.ndarray) -> np.ndarray:
    """Row-normalise a 2D matrix. Zero rows stay zero (similarity of 0)."""
    if matrix.size == 0:
        return matrix
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    safe = np.where(norms > 0.0, norms, 1.0)
    return matrix / safe


def top_k_cosine(
    *,
    query: np.ndarray,
    matrix: np.ndarray,
    k: int,
) -> List[Tuple[int, float]]:
    """Return the top-``k`` ``(row_index, cosine_similarity)`` pairs.

    Assumes ``query`` is already unit-normalised (1D shape ``(dim,)``) and
    ``matrix`` is already row-normalised (2D shape ``(N, dim)``). Caller
    is responsible for that: it lets us skip redundant normalisation on
    every call against the same corpus matrix.

    Returns at most ``k`` pairs, sorted by similarity descending. ``k``
    clamps to ``matrix.shape[0]`` automatically.
    """
    if k <= 0 or matrix.size == 0:
        return []
    if matrix.ndim != 2 or query.ndim != 1:
        raise ValueError(
            f"top_k_cosine expects query shape (dim,) and matrix shape "
            f"(N, dim); got query={query.shape}, matrix={matrix.shape}."
        )
    if matrix.shape[1] != query.shape[0]:
        raise ValueError(
            f"Dimension mismatch: query.dim={query.shape[0]}, "
            f"matrix.dim={matrix.shape[1]}."
        )
    scores = matrix @ query
    n = scores.shape[0]
    take = min(k, n)
    # argpartition for the top `take`, then sort those by score desc.
    if take >= n:
        order = np.argsort(-scores)
    else:
        partition = np.argpartition(-scores, take - 1)[:take]
        order = partition[np.argsort(-scores[partition])]
    return [(int(idx), float(scores[idx])) for idx in order]
