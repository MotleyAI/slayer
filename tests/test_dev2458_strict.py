"""Strict admits exactly Ossie's DISTINCT and percentile aggregate spellings; native answers every spelling.

Spec: queries/ossie-l3-binding › The filter is an Ossie expression.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest

from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2217_fixtures import strict_error
from tests._dev2446_strict_fixtures import counting_engine

#: Ossie's spellings: an aggregate in a strict filter.
OSSIE = [
    "SUM(DISTINCT orders.qty)", "sum(distinct orders.qty)", "Sum( Distinct /* c */ orders.qty )",
    "COUNT(DISTINCT orders.qty)",
    "PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY orders.amount)",
    "percentile_disc(0.5) within group (order by orders.amount asc)",
    "APPROX_PERCENTILE(orders.amount, 0.5)", "approx_percentile(orders.amount, 0.5)",
]
#: SLayer's own spellings and DISTINCT outside COUNT / SUM: not Ossie.
NOT_OSSIE = [
    "AVG(DISTINCT orders.amount)", "avg(distinct orders.amount)", "MIN(DISTINCT orders.amount)",
    "max( DISTINCT /* x */ orders.amount)", "sum_distinct(orders.qty)", "SUM_DISTINCT(orders.qty)",
    "avg_distinct(orders.qty)", "percentile_disc(orders.amount, 0.5)", "percentile_disc(orders.amount, p=0.5)",
    "percentile_approx(orders.amount, 0.5)", "PERCENTILE_APPROX(orders.amount, 0.5)",
]
SHAPES = ["{a} > 1", "{a} FILTER (WHERE orders.region = 'EU') > 1", "ABS({a}) > 1", "({a} + 1) > 1 AND orders.id > 0"]


@pytest.fixture(scope="module")
async def engine() -> AsyncGenerator[SlayerQueryEngine]:
    async with counting_engine("duckdb") as e:
        yield e


def _query(filter_text: str) -> dict:
    return {"metrics": ["orders.n"], "filter": filter_text}


async def _native_count(engine: SlayerQueryEngine, filter_text: str) -> list:
    resp = await engine.execute(_query(filter_text))
    return [list(r.values()) for r in resp.data]


@pytest.mark.parametrize("shape", SHAPES)
@pytest.mark.parametrize("aggregate", OSSIE)
async def test_ossie_spellings_are_aggregates(engine: SlayerQueryEngine, aggregate: str, shape: str) -> None:
    """Scenario: Ossie's aggregates are aggregates in the filter — and answer under ``slayer``."""
    text = shape.format(a=aggregate)
    await strict_error(engine, _query(text), "E_AGGREGATE_IN_WHERE")
    assert await _native_count(engine, text) == [[6]]


@pytest.mark.parametrize("shape", SHAPES)
@pytest.mark.parametrize("aggregate", NOT_OSSIE)
async def test_other_spellings_are_refused(engine: SlayerQueryEngine, aggregate: str, shape: str) -> None:
    """Scenario: Aggregates SLayer cannot evaluate are refused — and answer under ``slayer``."""
    text = shape.format(a=aggregate)
    await strict_error(engine, _query(text), "E_INVALID_QUERY")
    assert await _native_count(engine, text) == [[6]]


@pytest.mark.parametrize(("aggregate", "code"), [
    ("SUM(DISTINCT orders.qty)", "E_WINDOW_IN_WHERE"),
    ("PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY orders.amount)", "E_WINDOW_IN_WHERE"),
    ("AVG(DISTINCT orders.amount)", "E_INVALID_QUERY"),
    ("percentile_disc(orders.amount, 0.5)", "E_INVALID_QUERY"),
])
async def test_windowed_judged_alike(engine: SlayerQueryEngine, aggregate: str, code: str) -> None:
    await strict_error(engine, {"dimensions": ["orders.id"], "filter": f"{aggregate} OVER () > 1"}, code)
