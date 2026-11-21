"""Aggregates over all-NULL inputs take the empty value in every location (semantics Axiom 4).

Spec: aggregations/boolean-inputs › "Aggregates over all-NULL inputs take the empty value".
West's ``amount`` values are all NULL, and every ``customers.score`` is NULL.
"""

from __future__ import annotations

from typing import Callable, Dict, List

import pytest

from slayer.core.query import QueryMetric, SlayerQuery

from tests._dev2046_fixtures import m, make_exec_engine, month_td, orders_q

# aggregation → empty value
EMPTY = {
    "sum": None, "avg": None, "min": None, "max": None, "median": None,
    "count": 0, "count_distinct": 0,
}
WEST = ["orders.region == 'west'"]


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


def _metrics(source: str, *, suffix: str = "") -> List[QueryMetric]:
    """Every aggregation in ``EMPTY`` over ``source`` plus the boolean ``sum`` / ``max``, named by aggregation."""
    measures = [m(f"{agg}({source}{suffix})", agg) for agg in EMPTY]
    return measures + [m(f"sum({source} > 15{suffix})", "bsum"), m(f"max({source} > 15{suffix})", "bmax")]


EXPECTED = {**EMPTY, "bsum": None, "bmax": None}

LOCATIONS: Dict[str, Callable[[], object]] = {
    "local": lambda: orders_q(dimensions=["orders.region"], metrics=_metrics("orders.amount"), filter=WEST),
    "partition": lambda: orders_q(
        dimensions=["orders.region", "orders.status"], metrics=_metrics("orders.amount", suffix=", partition_by=orders.region"), filter=WEST,
    ),
    "window": lambda: orders_q(
        time_dimensions=month_td(), metrics=_metrics("orders.amount", suffix=", window='30d'"), filter=WEST,
    ),
    "cross_model": lambda: orders_q(metrics=_metrics("customers.score")),
    "stage": lambda: [
        {"name": "s1", "population": "orders", "dimensions": ["orders.region", "orders.status"],
         "metrics": [{"metric": "sum(orders.amount)", "alias": "x"}]},
        {"population": "s1", "dimensions": ["s1.region"], "metrics": _metrics("s1.x"), "filter": ["s1.region == 'west'"]},
    ],
    "association": lambda: SlayerQuery.model_validate({
        "population": "orders", "dimensions": ["orders.region"], "metrics": _metrics("customers.score"),
        "to_many_handling": "associate",
    }),
}


@pytest.mark.parametrize("location", list(LOCATIONS))
async def test_empty_value_in_every_location(engine, location: str) -> None:
    resp = await engine.execute(LOCATIONS[location]())
    assert resp.data
    for row in resp.data:
        got = {name: row[name] for name in EXPECTED}
        assert got == EXPECTED, (location, row)
