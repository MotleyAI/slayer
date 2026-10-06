"""Window transforms over a bare literal execute at the query grain."""

from __future__ import annotations

import pytest

from tests._dev2062_fixtures import (
    FEB,
    JAN,
    MAR,
    REGIONS,
    approx_map,
    by_month,
    m,
    make_exec_engine,
    month_td,
    monthly_q,
    orders_q,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


_TIME_ORDERED = {
    "cumsum(1)": {JAN: 1, FEB: 2, MAR: 3},
    "lag(1)": {JAN: None, FEB: 1, MAR: 1},
    "lead(1)": {JAN: 1, FEB: 1, MAR: None},
    "change(1)": {JAN: None, FEB: 0, MAR: 0},
    "change_pct(1)": {JAN: None, FEB: 0, MAR: 0},
    "time_shift(1, -1)": {JAN: None, FEB: 1, MAR: 1},
    "consecutive_periods(1 > 0)": {JAN: 1, FEB: 2, MAR: 3},
}


@pytest.mark.parametrize("formula", list(_TIME_ORDERED))
async def test_time_ordered_transform_over_literal(exec_engine, formula) -> None:
    resp = await exec_engine.execute(monthly_q(m("sum(amount)", "s"), m(formula, "v")))
    assert by_month(resp, "s") == {JAN: 40, FEB: 10, MAR: 90}
    assert by_month(resp, "v") == approx_map(_TIME_ORDERED[formula])


_RANK_FAMILY = {
    "rank(1, direction='desc')": 1,
    "dense_rank(1, direction='desc')": 1,
    "percent_rank(1)": 0,
}


@pytest.mark.parametrize("formula", list(_RANK_FAMILY))
async def test_rank_family_over_literal(exec_engine, formula) -> None:
    resp = await exec_engine.execute(
        orders_q(dimensions=["region"], measures=[m("sum(amount)", "s"), m(formula, "v")]),
    )
    assert sorted(r["orders.region"] for r in resp.data) == sorted(REGIONS)
    assert [r["orders.v"] for r in resp.data] == [pytest.approx(_RANK_FAMILY[formula])] * len(REGIONS)


@pytest.mark.parametrize("formula", list(_RANK_FAMILY))
async def test_partitioned_rank_family_over_literal(exec_engine, formula) -> None:
    partitioned = f"{formula[:-1]}, partition_by=region)"
    resp = await exec_engine.execute(orders_q(
        dimensions=["region"], time_dimensions=month_td(),
        measures=[m("sum(amount)", "s"), m(partitioned, "v")],
    ))
    assert len(resp.data) == 6, resp.data
    assert [r["orders.v"] for r in resp.data] == [pytest.approx(_RANK_FAMILY[formula])] * 6


async def test_ntile_over_literal_splits_rows_into_balanced_buckets(exec_engine) -> None:
    resp = await exec_engine.execute(
        orders_q(dimensions=["region"], measures=[m("sum(amount)", "s"), m("ntile(1, n=2)", "v")]),
    )
    assert sorted(r["orders.v"] for r in resp.data) == [1, 1, 2]
