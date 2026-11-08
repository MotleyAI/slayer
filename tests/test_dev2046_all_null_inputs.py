"""Aggregates over all-NULL inputs take the empty value in every location (semantics Axiom 4).

Spec: aggregations/boolean-inputs › "Aggregates over all-NULL inputs take the empty value".
West's ``amount`` values are all NULL, and every ``customers.score`` is NULL.
"""

from __future__ import annotations

from typing import Callable, Dict, List

import pytest

from slayer.core.models import ModelMeasure
from slayer.core.query import SlayerQuery

from tests._dev2046_fixtures import m, make_exec_engine, month_td, orders_q

# aggregation → empty value
EMPTY = {
    "sum": None, "avg": None, "min": None, "max": None, "median": None,
    "count": 0, "count_distinct": 0,
}
WEST = ["region == 'west'"]


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


def _measures(source: str, *, suffix: str = "") -> List[ModelMeasure]:
    """Every aggregation in ``EMPTY`` over ``source`` plus the boolean ``sum`` / ``max``, named by aggregation."""
    measures = [m(f"{agg}({source}{suffix})", agg) for agg in EMPTY]
    return measures + [m(f"sum({source} > 15{suffix})", "bsum"), m(f"max({source} > 15{suffix})", "bmax")]


EXPECTED = {**EMPTY, "bsum": None, "bmax": None}

LOCATIONS: Dict[str, Callable[[], object]] = {
    "local": lambda: orders_q(dimensions=["region"], measures=_measures("amount"), filters=WEST),
    "partition": lambda: orders_q(
        dimensions=["region", "status"], measures=_measures("amount", suffix=", partition_by=region"), filters=WEST,
    ),
    "window": lambda: orders_q(
        time_dimensions=month_td(), measures=_measures("amount", suffix=", window='30d'"), filters=WEST,
    ),
    "cross_model": lambda: orders_q(measures=_measures("customers.score")),
    "stage": lambda: [
        {"name": "s1", "source_model": "orders", "dimensions": ["region", "status"],
         "measures": [{"formula": "sum(amount)", "name": "x"}]},
        {"source_model": "s1", "dimensions": ["region"], "measures": _measures("x"), "filters": WEST},
    ],
    "association": lambda: SlayerQuery.model_validate({
        "source_model": "orders", "dimensions": ["region"], "measures": _measures("customers.score"),
        "to_many_handling": "associate",
    }),
}


@pytest.mark.parametrize("location", list(LOCATIONS))
async def test_empty_value_in_every_location(engine, location: str) -> None:
    resp = await engine.execute(LOCATIONS[location]())
    assert resp.data
    model = "s1" if location == "stage" else "orders"
    for row in resp.data:
        got = {name: row[f"{model}.{name}"] for name in EXPECTED}
        assert got == EXPECTED, (location, row)
