"""Only Ossie's query shape (wide_table §3.7) is admitted under the strict binding.

Spec: queries/ossie-l3-binding › Only Ossie's query shape is admitted.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest

from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2217_fixtures import fixture_engine, strict, strict_error

REVENUE = "orders.total_revenue"
BASE: dict[str, Any] = {"metrics": [REVENUE]}

#: Every SLayer-only part, with a value native evaluation would accept.
NATIVE_ONLY_PARTS: dict[str, Any] = {
    "population": "orders",
    "time_dimensions": [{"dimension": "orders.order_id", "granularity": "month"}],
    "order": [{"field": REVENUE, "direction": "desc"}],
    "limit": 10,
    "offset": 1,
    "extensions": [{"dataset": "orders", "metrics": [{"name": "n", "expression": "count(*)"}]}],
    "variables": {"r": "EAST"},
    "to_many_handling": "error",
    "distinct_dimension_values": True,
    "whole_periods_only": False,
    "main_time_dimension": "orders.order_id",
    "name": "q",
    "filters": ["orders.amount > 100"],
}

MALFORMED: dict[str, Any] = {
    "null filter": {**BASE, "filter": None},
    "null metrics": {"metrics": None},
    "null dimensions": {**BASE, "dimensions": None},
    "null entry": {"metrics": [None]},
    "null alias": {"metrics": [{"metric": REVENUE, "alias": None}]},
    "dimensions not a list": {**BASE, "dimensions": "customers.region"},
    "metrics not a list": {"metrics": REVENUE},
    "metric entry an object of the wrong kind": {"metrics": [{"field": REVENUE}]},
    "dimension entry an object of the wrong kind": {"dimensions": [{"metric": "customers.region"}]},
    "filter not a string": {**BASE, "filter": 5},
    "filter a list": {**BASE, "filter": ["orders.amount > 100"]},
    "dimension entry with another property": {"dimensions": [{"field": "customers.region", "alias": "r", "label": "R"}]},
    "metric entry with another property": {"metrics": [{"metric": REVENUE, "label": "Revenue"}]},
    "metric entry without its reference": {"metrics": [{"alias": "r"}]},
    "aggregate call in metric position": {"metrics": ["sum(orders.amount)"]},
    "arithmetic in metric position": {"metrics": ["orders.total_revenue * 2"]},
    "count star in metric position": {"metrics": ["count(*)"]},
    "string literal in dimension position": {"dimensions": ["'EAST'"]},
    "number in dimension position": {"dimensions": ["1"]},
    "alias with a space": {"metrics": [{"metric": REVENUE, "alias": "order amount"}]},
    "alias starting with a digit": {"metrics": [{"metric": REVENUE, "alias": "1st"}]},
    "empty alias": {"metrics": [{"metric": REVENUE, "alias": ""}]},
    "alias not a string": {"metrics": [{"metric": REVENUE, "alias": 1}]},
    "filter that does not parse": {**BASE, "filter": "orders.amount >"},
    # Raw inputs native conversion would rewrite or reject with a Pydantic error.
    "legacy measures key": {"measures": [REVENUE]},
    "legacy source_model key": {**BASE, "source_model": "orders"},
    "legacy formula entry": {"metrics": [{"formula": REVENUE, "name": "r"}]},
    "legacy name on a metric entry": {"metrics": [{"metric": REVENUE, "name": "r"}]},
    "unknown key": {**BASE, "dimension": ["customers.region"]},
}


@pytest.fixture
async def engine(tmp_path: Path) -> AsyncGenerator[SlayerQueryEngine]:
    async with fixture_engine(tmp_path, "retail") as engine:
        yield engine


async def test_an_unsupported_part_is_malformed(engine: SlayerQueryEngine) -> None:
    """An unsupported part is malformed."""
    query = {**BASE, "limit": 10}
    await strict_error(engine, query, "E_INVALID_QUERY")
    assert (await engine.execute(query)).data


@pytest.mark.parametrize("part", sorted(NATIVE_ONLY_PARTS))
async def test_every_native_only_part_is_malformed(engine: SlayerQueryEngine, part: str) -> None:
    await strict_error(engine, {**BASE, part: NATIVE_ONLY_PARTS[part]}, "E_INVALID_QUERY")


async def test_a_null_value_is_malformed(engine: SlayerQueryEngine) -> None:
    """A null value is malformed."""
    await strict_error(engine, {**BASE, "filter": None}, "E_INVALID_QUERY")


async def test_an_expression_in_metric_position_is_malformed(engine: SlayerQueryEngine) -> None:
    """An expression in metric position is malformed."""
    await strict_error(engine, {"metrics": ["sum(orders.amount)"]}, "E_INVALID_QUERY")


async def test_a_list_of_queries_or_of_filters_is_malformed(engine: SlayerQueryEngine) -> None:
    """A list of queries or of filters is malformed."""
    await strict_error(engine, [{"name": "s", **BASE}, BASE], "E_INVALID_QUERY")
    await strict_error(engine, {**BASE, "filter": ["orders.amount > 100"]}, "E_INVALID_QUERY")


async def test_an_alias_must_be_an_identifier(engine: SlayerQueryEngine) -> None:
    """An alias must be an identifier."""
    await strict_error(engine, {"metrics": [{"metric": REVENUE, "alias": "order amount"}]}, "E_INVALID_QUERY")
    response = await strict(engine, {"metrics": [{"metric": REVENUE, "alias": '"order amount"'}]})
    assert response.columns == ["order amount"]
    assert response.data == [{"order amount": 650}]


@pytest.mark.parametrize("name", sorted(MALFORMED))
async def test_malformed_inputs(engine: SlayerQueryEngine, name: str) -> None:
    await strict_error(engine, MALFORMED[name], "E_INVALID_QUERY")


async def test_a_python_query_object_is_judged_by_the_parts_it_sets(engine: SlayerQueryEngine) -> None:
    await strict_error(engine, SlayerQuery.model_validate({**BASE, "limit": 10}), "E_INVALID_QUERY")
    response = await strict(engine, SlayerQuery.model_validate(BASE))
    assert response.data == [{"total_revenue": 650}]


async def test_object_entries_with_and_without_alias(engine: SlayerQueryEngine) -> None:
    response = await strict(engine, {"dimensions": [{"field": "customers.region"}], "metrics": [{"metric": REVENUE}]})
    assert response.columns == ["region", "total_revenue"]


@pytest.mark.parametrize("query", [
    pytest.param({}, id="no parts"),
    pytest.param({"filter": "TRUE"}, id="filter only"),
    pytest.param({"dimensions": [], "metrics": []}, id="empty lists"),
    pytest.param({"dimensions": []}, id="empty dimensions"),
])
async def test_empty_lists_count_as_absent(engine: SlayerQueryEngine, query: dict[str, Any]) -> None:
    await strict_error(engine, query, "E_EMPTY_AGGREGATION_QUERY")
