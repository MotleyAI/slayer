"""Reciprocal Rank Fusion (DEV-1375).

Pins the standard k=60 formula:
    score(d) = Σ_r 1 / (k + rank_r(d))
where rank is 1-indexed (top of the list = rank 1).
"""

from __future__ import annotations

import math

import pytest

from slayer.search.rrf import rrf_fuse


def test_empty_input_returns_empty() -> None:
    assert rrf_fuse(rankings=[]) == {}


def test_all_empty_rankers_returns_empty() -> None:
    assert rrf_fuse(rankings=[[], [], []]) == {}


def test_single_ranker_degenerates_to_inverse_rank_order() -> None:
    fused = rrf_fuse(rankings=[["a", "b", "c"]], k=60)
    # Standard k=60: rank 1 → 1/61, rank 2 → 1/62, rank 3 → 1/63
    assert fused["a"] == pytest.approx(1.0 / 61)
    assert fused["b"] == pytest.approx(1.0 / 62)
    assert fused["c"] == pytest.approx(1.0 / 63)
    # Sorted-desc check
    sorted_keys = sorted(fused, key=fused.get, reverse=True)
    assert sorted_keys == ["a", "b", "c"]


def test_two_rankers_fuse_correctly() -> None:
    # a is rank 1 in both → 2/61
    # b is rank 2 in both → 2/62
    # c is in neither → not present
    fused = rrf_fuse(rankings=[["a", "b"], ["a", "b"]], k=60)
    assert fused["a"] == pytest.approx(2.0 / 61)
    assert fused["b"] == pytest.approx(2.0 / 62)
    assert "c" not in fused


def test_documents_in_only_one_ranking_get_partial_score() -> None:
    fused = rrf_fuse(rankings=[["a", "b"], ["c", "a"]], k=60)
    # a: rank 1 in r1, rank 2 in r2 → 1/61 + 1/62
    # b: rank 2 in r1 only → 1/62
    # c: rank 1 in r2 only → 1/61
    assert fused["a"] == pytest.approx(1.0 / 61 + 1.0 / 62)
    assert fused["b"] == pytest.approx(1.0 / 62)
    assert fused["c"] == pytest.approx(1.0 / 61)


def test_high_overlap_high_rank_beats_partial_overlap_low_rank() -> None:
    fused = rrf_fuse(rankings=[["x", "y"], ["x", "y"]], k=60)
    # x: 2 * 1/61
    # y: 2 * 1/62
    # The top item from one ranker beats a partial overlap from both.
    fused_singleton = rrf_fuse(rankings=[["a"]], k=60)
    assert fused["x"] > fused_singleton["a"]


def test_int_keys_supported() -> None:
    """Memory IDs are ints — fusion must work with non-string keys."""
    fused = rrf_fuse(rankings=[[1, 2, 3], [3, 2, 1]], k=60)
    # 1: rank 1 + rank 3 → 1/61 + 1/63
    # 2: rank 2 + rank 2 → 2/62
    # 3: rank 3 + rank 1 → 1/63 + 1/61 (same as 1)
    assert fused[1] == pytest.approx(1.0 / 61 + 1.0 / 63)
    assert fused[3] == pytest.approx(1.0 / 61 + 1.0 / 63)
    assert fused[2] == pytest.approx(2.0 / 62)


def test_custom_k_value_changes_scores_predictably() -> None:
    fused_60 = rrf_fuse(rankings=[["a"]], k=60)
    fused_10 = rrf_fuse(rankings=[["a"]], k=10)
    # Smaller k → larger inverse → larger score for a top-ranked item.
    assert fused_10["a"] > fused_60["a"]
    assert fused_10["a"] == pytest.approx(1.0 / 11)


def test_default_k_is_60() -> None:
    """Cormack et al. 2009 standard."""
    fused = rrf_fuse(rankings=[["only"]])
    assert fused["only"] == pytest.approx(1.0 / 61)


def test_fused_score_is_finite_for_long_rankings() -> None:
    long_ranking = [str(i) for i in range(1000)]
    fused = rrf_fuse(rankings=[long_ranking], k=60)
    for score in fused.values():
        assert math.isfinite(score)
        assert score > 0
