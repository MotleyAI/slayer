"""DEV-1865 — cross-model aggregates compose in expressions and dimensions.

Pins the modified cross-model requirement's composition scenarios that
DEV-1865 restates but does not change (the filter/order scenarios live in
test_dev1865_newly_legal / test_dev1865_cross_model_keyless). Run on
SQLite + DuckDB.
"""

from __future__ import annotations

import pytest

from slayer.core.query import QueryMetric

from tests._dev1865_fixtures import q
from slayer.engine.query_engine import SlayerQueryEngine

CM_BAND = (
    "CASE WHEN sum(customers.spend, partition_by=customers.tier) > 175 "
    "THEN 'hi' ELSE 'lo' END"
)


async def test_local_and_cross_model_aggregates_in_one_expression(exec_engine) -> None:
    resp = await exec_engine.execute(q(
        dimensions=["customers.tier"],
        metrics=[
            QueryMetric(metric="sum(orders.amount)", alias="a"),
            QueryMetric(metric="sum(customers.spend)", alias="sp"),
            QueryMetric(metric="sum(orders.amount) / sum(customers.spend)", alias="r"),
        ],
    ))
    assert resp.data
    for row in resp.data:
        assert float(row["r"]) == pytest.approx(
            float(row["a"]) / float(row["sp"])
        )


async def test_cross_model_source_inside_a_computed_dimension(exec_engine) -> None:
    resp = await exec_engine.execute(q(
        dimensions=[{"field": CM_BAND, "alias": "band"}, "customers.tier"],
        metrics=[QueryMetric(metric="sum(customers.spend)", alias="sp")],
    ))
    assert resp.data
    assert all(r["band"] in {"hi", "lo"} for r in resp.data)


async def test_computed_dimension_coexists_with_cross_model_measure(exec_engine: SlayerQueryEngine) -> None:
    resp = await exec_engine.execute(q(
        dimensions=[{"field": CM_BAND, "alias": "band"}, "customers.regions.name"],
        metrics=[QueryMetric(metric="sum(customers.spend)", alias="sp")],
    ))
    assert resp.data
    assert "band" in resp.columns
    assert any(c.endswith("sp") for c in resp.columns)
