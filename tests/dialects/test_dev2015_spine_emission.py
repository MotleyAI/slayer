"""The spine renders as an in-SQL generated series on every Tier-1 dialect, never a stored table."""

from __future__ import annotations

import re

import pytest

from tests._dev2015_fixtures import DS_GRANULARITIES, TWO_FACTS, cg_orders_model, m, spine_models, spine_td
from tests._engine_helpers import _engine_generate
from tests._time_points_fixtures import PinnedClock

TIER1 = ["sqlite", "postgres", "duckdb", "mysql", "clickhouse", "tsql", "snowflake", "bigquery"]
# Each dialect's integer-sequence primitive.
SEQUENCE = {
    "sqlite": r"WITH\s+RECURSIVE",
    "postgres": r"GENERATE_SERIES\s*\(",
    "duckdb": r"\b(RANGE|GENERATE_SERIES)\s*\(",
    "mysql": r"cte_max_recursion_depth",
    "clickhouse": r"\bnumbers\s*\(",
    "tsql": r"GENERATE_SERIES\s*\(",
    "snowflake": r"GENERATOR\s*\(",
    "bigquery": r"GENERATE_ARRAY\s*\(",
}


async def _sql(dialect: str, query: dict) -> str:
    models = [cg_orders_model()] if query.get("source_model") == "orders" else spine_models()
    return await _engine_generate(
        query=query, model=models[0], extra_models=models[1:], dialect=dialect, validate=False,
        clock=PinnedClock(), datasource_fields=DS_GRANULARITIES,
    )


@pytest.mark.parametrize("dialect", TIER1)
@pytest.mark.parametrize("granularity", ["month", "quarter_hour"])
async def test_spine_is_a_generated_series(dialect, granularity) -> None:
    sql = await _sql(dialect, {"measures": TWO_FACTS, "time_dimensions": [spine_td(granularity=granularity)]})
    assert re.search(SEQUENCE[dialect], sql, flags=re.IGNORECASE), sql
    assert not re.search(r"\b(FROM|JOIN)\s+[`\"\[]?time_spine\b", sql, flags=re.IGNORECASE), sql


@pytest.mark.parametrize("dialect", TIER1)
async def test_custom_bucketing_renders(dialect) -> None:
    sql = await _sql(dialect, {
        "source_model": "orders", "measures": [m("sum(amount)", "s")],
        "time_dimensions": [{"dimension": "order_date", "granularity": "fiscal_year"}],
    })
    assert "2000" in sql  # the fiscal year's origin anchors the bucket arithmetic
