"""DEV-1777 sub-item 3(b): the ``declared_measures`` prefix partition is
structural. ``partition_declared_measures`` single-sources the dim / time-dim /
aggregate slice arithmetic that ``stage_planner`` and ``cross_model_planner``
used to inline as ``[:n_dims]`` / ``[n_dims:n_dims+n_tds]`` / ``[n_dims+n_tds:]``;
``PreboundQuery.grain_declared_measures`` exposes the grain prefix over it.
"""

from __future__ import annotations

import pytest

from slayer.engine.prebound import PreboundQuery, partition_declared_measures

# Sentinel elements — partition_declared_measures is pure list slicing, so the
# element type is irrelevant; strings make the boundaries readable.
_DMS = ["d0", "d1", "t0", "a0", "a1"]


@pytest.mark.parametrize(
    "n_dims,n_tds,dims,tds,aggs",
    [
        (2, 1, ["d0", "d1"], ["t0"], ["a0", "a1"]),
        (0, 0, [], [], ["d0", "d1", "t0", "a0", "a1"]),
        (5, 0, ["d0", "d1", "t0", "a0", "a1"], [], []),
        (0, 2, [], ["d0", "d1"], ["t0", "a0", "a1"]),
        (3, 2, ["d0", "d1", "t0"], ["a0", "a1"], []),
    ],
)
def test_partition_matches_manual_slicing(n_dims, n_tds, dims, tds, aggs) -> None:
    got_dims, got_tds, got_aggs = partition_declared_measures(
        declared_measures=_DMS, n_dims=n_dims, n_time_dimensions=n_tds,
    )
    assert (got_dims, got_tds, got_aggs) == (dims, tds, aggs)
    # Byte-for-byte the slices the planners used to inline.
    grain = n_dims + n_tds
    assert got_dims == _DMS[:n_dims]
    assert got_tds == _DMS[n_dims:grain]
    assert got_aggs == _DMS[grain:]


def test_partition_empty_list() -> None:
    assert partition_declared_measures(
        declared_measures=[], n_dims=0, n_time_dimensions=0,
    ) == ([], [], [])


def test_grain_accessor_is_dims_plus_time_dims() -> None:
    # model_construct skips validation so the accessor's slicing can be pinned
    # without hand-building heavy DeclaredMeasure / BoundExpr fixtures.
    pq = PreboundQuery.model_construct(
        declared_measures=_DMS, n_dims=2, n_time_dimensions=1,
    )
    assert pq.grain_declared_measures == ["d0", "d1", "t0"]
    assert pq.grain_declared_measures == _DMS[: 2 + 1]


def test_grain_accessor_empty_grain() -> None:
    pq = PreboundQuery.model_construct(
        declared_measures=_DMS, n_dims=0, n_time_dimensions=0,
    )
    assert pq.grain_declared_measures == []
