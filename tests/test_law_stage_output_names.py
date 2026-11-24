"""Law: a stage's static output names equal the field names of its compiled stage schema."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.extensions import stage_output_names
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev2187_fixtures import BACKENDS, orders_engine


def m(expression: str, alias: str) -> dict[str, str]:
    return {"metric": expression, "alias": alias}


def _orders(**body: Any) -> dict[str, Any]:
    return {"population": "orders", **body}


S1 = {"name": "s1", "population": "orders", "dimensions": ["orders.status"], "metrics": [m("sum(orders.amount)", "rev")]}

#: case → the stages of a query-backed dataset; the law reads its last stage.
CASES: dict[str, list[dict[str, Any]]] = {
    "relationship-path": [_orders(dimensions=["orders.customers.name"], metrics=["count(orders.*)"])],
    "computed-dimension": [_orders(dimensions=["upper(orders.status)"], metrics=["sum(orders.amount)"])],
    "time-dimension-granularity": [_orders(
        time_dimensions=[{"field": "orders.created_at", "granularity": "month"}], metrics=["orders.revenue"])],
    "time-dimension-two-granularities": [_orders(
        time_dimensions=[{"field": "orders.created_at", "granularity": "month"},
                         {"field": "orders.created_at", "granularity": "year"}], metrics=["count(orders.*)"])],
    "time-dimension-aliased-sibling": [_orders(
        time_dimensions=[{"field": "orders.created_at", "granularity": "month", "alias": "m"},
                         {"field": "orders.created_at", "granularity": "year"}], metrics=["count(orders.*)"])],
    "time-dimension-expression": [_orders(
        time_dimensions=["date_trunc('month', orders.created_at)"], metrics=["count(orders.*)"])],
    "simple-metric": [_orders(dimensions=["orders.status"], metrics=["sum(orders.amount)", "count(orders.*)"])],
    "composite-metric": [_orders(dimensions=["orders.status"], metrics=["sum(orders.amount) / count(orders.*)"])],
    "explicit-aliases": [_orders(dimensions=[{"field": "orders.status", "alias": "st"}],
                                 metrics=[m("sum(orders.amount)", "rev")])],
    "deduplicated": [_orders(dimensions=["orders.status", "orders.status"],
                             metrics=["sum(orders.amount)", "sum(orders.amount)"])],
    "anchorless-injected-alias": [_orders(
        dimensions=["orders.status"], metrics=[m("sum(orders.amount) / count(customers.*)", "amount_sum_customers_count")])],
    "anchorless-unaliased": [_orders(dimensions=["orders.status"], metrics=["sum(orders.amount) / count(customers.*)"])],
    "stage-over-stage": [S1, {"population": "s1", "dimensions": ["s1.status"], "metrics": [m("sum(s1.rev)", "rev")]}],
}


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with orders_engine(request.param) as eng:
        yield eng


@pytest.mark.parametrize("case", sorted(CASES))
async def test_static_output_names_equal_the_compiled_stage_schema(engine: SlayerQueryEngine, case: str) -> None:
    stages = [SlayerQuery.model_validate(s) for s in CASES[case]]
    expanded = await engine.expand_query_backed_dataset(
        SlayerDataset(name=f"law_{case.replace('-', '_')}", data_source="test", source_queries=stages))
    assert set(stage_output_names(stages[-1])) == {f.name for f in expanded.fields}
