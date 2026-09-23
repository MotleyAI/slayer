"""Shifted partitioned composite over a stage time dimension with a stage-level
``date_range`` (queries/transforms, "Shifted composite over a stage time
dimension"), SQLite + DuckDB."""

from __future__ import annotations

import pytest

from tests._dev1832_fixtures import (
    ColumnRef,
    ModelMeasure,
    SlayerQuery,
    TimeDimension,
    TimeGranularity,
    make_exec_engine,
    month_key,
    month_td,
    monthly_q,
)

EXPECTED = {("North", "2024-02"): 10 / 15, ("South", "2024-02"): 5 / 15,
            ("North", "2024-03"): 20 / 35, ("West", "2024-02"): None}


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


async def test_shifted_share_over_stage_time_dimension(exec_engine) -> None:
    inner = monthly_q(name="s1", dimensions=["region"], time_dimensions=month_td(),
                      measures=[ModelMeasure(formula="amount:sum", name="rev")])
    outer = SlayerQuery.model_validate({
        "source_model": "s1", "dimensions": ["region"],
        "time_dimensions": [TimeDimension(dimension=ColumnRef(name="ordered_at"),
                                          granularity=TimeGranularity.MONTH,
                                          date_range=["2024-02-01", "2024-03-31"])],
        "measures": [ModelMeasure(
            formula="time_shift(rev:sum / rev:sum(partition_by=[ordered_at]), -1)",
            name="t")]})
    resp = await exec_engine.execute([inner, outer])
    got = {(r["s1.region"], month_key(r["s1.ordered_at"])): r["s1.t"] for r in resp.data}
    assert set(got) == set(EXPECTED)
    for cell, want in EXPECTED.items():
        if want is None:
            assert got[cell] is None, cell
        else:
            assert got[cell] is not None, cell
            assert float(got[cell]) == pytest.approx(want), (cell, got[cell])
