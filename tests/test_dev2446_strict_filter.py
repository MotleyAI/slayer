"""Spec: queries/ossie-l3-binding › The filter is an Ossie expression (argument counts, casts, aggregates)."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path

import pytest

from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2217_fixtures import rows, strict, strict_error
from tests._dev2446_strict_fixtures import orders_engine, revenue_query

#: Admitted filter → total revenue it keeps, under strict and native alike.
ADMITTED = {
    "LOG(10, orders.amount) > 2": 500,
    "orders.order_date > CAST('2025-01-01 00:00:00' AS TIMESTAMP_NTZ)": 350,
    "COALESCE(orders.status, NULL, 'none') = 'none'": 300,
    "CONCAT(orders.status, '-', 'x') = 'complete-x'": 150,
    "GREATEST(orders.amount, 150, 250) = 250": 350,
}

#: Refused under strict (an argument count Ossie does not define) → total revenue native keeps.
OFF_COUNT = {
    "LOG(orders.amount) > 2": 500,
    "ROUND(orders.amount / 120.0) = 1": 100,
    "TRUNC(orders.amount / 120.0) = 0": 150,
    "SUBSTRING(orders.status, 1) = 'complete'": 150,
    "REGEXP_LIKE(orders.status, '^C', 'i')": 350,
    "REGEXP_REPLACE(orders.status, 'E$', '', 'i') = 'complet'": 150,
    "COALESCE(orders.status) = 'complete'": 150,
    "CONCAT(orders.status) = 'complete'": 150,
    '"LOG"(orders.amount) > 2': 500,
    "\"regexp_extract\"(orders.status, '^(c[a-z])', 1) = 'co'": 150,
}

#: Aggregate filter → strict code.
AGGREGATES = {
    "APPROX_PERCENTILE(orders.amount, 0.5) > 1": "E_AGGREGATE_IN_WHERE",
    "percentile_disc(orders.amount, 0.5) > 1": "E_INVALID_QUERY",
    "PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY orders.amount) > 1": "E_AGGREGATE_IN_WHERE",
    "SUM(orders.amount, 1) > 1": "E_INVALID_QUERY",
    "MEDIAN(orders.amount, 1) > 1": "E_INVALID_QUERY",
    "MEDIAN(orders.amount) > 1": "E_AGGREGATE_IN_WHERE",
    "STDDEV(orders.amount) > 1": "E_AGGREGATE_IN_WHERE",
    "VARIANCE(orders.amount) > 1": "E_AGGREGATE_IN_WHERE",
    "APPROX_COUNT_DISTINCT(orders.status) > 1": "E_AGGREGATE_IN_WHERE",
    "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY orders.amount) > 1": "E_AGGREGATE_IN_WHERE",
    "SUM(orders.amount) FILTER (WHERE orders.status = 'complete') > 1": "E_AGGREGATE_IN_WHERE",
    "COUNT(DISTINCT orders.status) FILTER (WHERE orders.amount > 1) > 1": "E_AGGREGATE_IN_WHERE",
}

#: A count SLayer does not accept.
WRONG_COUNT = ["ABS(orders.amount, 1) > 0", "UPPER(orders.status, 1) = 'X'", "TRUNC(orders.amount, 1) > 0"]


@pytest.fixture
async def engine(tmp_path: Path) -> AsyncGenerator[SlayerQueryEngine]:
    async with orders_engine(tmp_path) as engine:
        yield engine


@pytest.mark.parametrize("filter_text", sorted(ADMITTED))
async def test_ossie_argument_counts_and_casts_are_admitted(engine: SlayerQueryEngine, filter_text: str) -> None:
    expected = [(float(ADMITTED[filter_text]),)]
    assert rows(await strict(engine, revenue_query(filter_text))) == expected
    assert rows(await engine.execute(revenue_query(filter_text)), "orders.total_revenue") == expected


@pytest.mark.parametrize("filter_text", sorted(OFF_COUNT))
async def test_other_argument_counts_are_refused(engine: SlayerQueryEngine, filter_text: str) -> None:
    await strict_error(engine, revenue_query(filter_text), "E_INVALID_QUERY")
    native = await engine.execute(revenue_query(filter_text))
    assert rows(native, "orders.total_revenue") == [(float(OFF_COUNT[filter_text]),)]


@pytest.mark.parametrize("filter_text", sorted(AGGREGATES))
async def test_an_aggregate_is_ossie_only_in_a_shape_slayer_evaluates(engine: SlayerQueryEngine,
                                                                     filter_text: str) -> None:
    await strict_error(engine, revenue_query(filter_text), AGGREGATES[filter_text])


@pytest.mark.parametrize("filter_text", WRONG_COUNT)
async def test_a_wrong_argument_count_is_a_coded_refusal(engine: SlayerQueryEngine, filter_text: str) -> None:
    await strict_error(engine, revenue_query(filter_text), "E_INVALID_QUERY")
