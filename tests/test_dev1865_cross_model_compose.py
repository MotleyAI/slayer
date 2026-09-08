"""DEV-1865 — cross-model aggregates compose in expressions and dimensions.

Pins the modified cross-model requirement's composition scenarios that
DEV-1865 restates but does not change (the filter/order scenarios live in
test_dev1865_newly_legal / test_dev1865_cross_model_keyless). Run on
SQLite + DuckDB.
"""

from __future__ import annotations

import pytest

from tests._dev1865_fixtures import ModelMeasure, make_exec_engine, q

CM_BAND = (
    "CASE WHEN customers.spend:sum(partition_by=customers.tier) > 175 "
    "THEN 'hi' ELSE 'lo' END"
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


async def test_local_and_cross_model_aggregates_in_one_expression(exec_engine) -> None:
    resp = await exec_engine.execute(q(
        dimensions=["customers.tier"],
        measures=[
            ModelMeasure(formula="amount:sum", name="a"),
            ModelMeasure(formula="customers.spend:sum", name="sp"),
            ModelMeasure(formula="amount:sum / customers.spend:sum", name="r"),
        ],
    ))
    assert resp.data
    for row in resp.data:
        assert float(row["orders.r"]) == pytest.approx(
            float(row["orders.a"]) / float(row["orders.sp"])
        )


async def test_cross_model_source_inside_a_computed_dimension(exec_engine) -> None:
    resp = await exec_engine.execute(q(
        dimensions=[{"expression": CM_BAND, "name": "band"}, "customers.tier"],
        measures=[ModelMeasure(formula="customers.spend:sum", name="sp")],
    ))
    assert resp.data
    assert all(r["orders.band"] in {"hi", "lo"} for r in resp.data)


async def test_computed_dimension_coexists_with_cross_model_measure(exec_engine) -> None:
    resp = await exec_engine.execute(q(
        dimensions=[{"expression": CM_BAND, "name": "band"}, "customers.regions.name"],
        measures=[ModelMeasure(formula="customers.spend:sum", name="sp")],
    ))
    assert resp.data
    assert "orders.band" in resp.columns
    assert any(c.endswith("sp") for c in resp.columns)
