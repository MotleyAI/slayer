"""A window in a strict filter: Ossie's §8 rows before row 7, Ossie's aggregate arguments, native unchanged.

Spec: queries/ossie-l3-binding › The filter is an Ossie expression.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest

from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2217_fixtures import sql_engine, strict_error
from tests.test_dev2217_filter import DATASETS, SQL

ORDERED = "OVER (ORDER BY orders.amount)"

#: A malformed window, a function that is not Ossie's, wrong arity, a nested window, a frame out of ANSI order.
MALFORMED = [
    "ROW_NUMBER() OVER (",
    "FOO() OVER ()",
    "ROW_NUMBER(orders.amount) OVER ()",
    "SUM(ROW_NUMBER() OVER ()) OVER ()",
    "ROW_NUMBER() OVER (ORDER BY orders.amount ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) = 1",
    "ROW_NUMBER() OVER (ORDER BY orders.amount GROUPS UNBOUNDED PRECEDING) = 1",
    "ROW_NUMBER() OVER (ORDER BY orders.amount ROWS UNBOUNDED FOLLOWING) = 1",
    "ROW_NUMBER() OVER (ORDER BY orders.amount ROWS 1 FOLLOWING) = 1",
    "ROW_NUMBER() OVER (ORDER BY orders.amount ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) = 1",
    "ROW_NUMBER() OVER (ORDER BY orders.amount ROWS BETWEEN 2 FOLLOWING AND 1 FOLLOWING) = 1",
    "ROW_NUMBER() OVER (ORDER BY orders.amount ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) = 1",
    "ROW_NUMBER() OVER (ORDER BY orders.amount ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING) = 1",
    "ROW_NUMBER() OVER (ORDER BY orders.amount ROWS 1.5 PRECEDING) = 1",
    "ROW_NUMBER() OVER (ORDER BY orders.amount ROWS BETWEEN 1.5 PRECEDING AND CURRENT ROW) = 1",
    "ROW_NUMBER() OVER (ORDER BY orders.amount ROWS BETWEEN -1 PRECEDING AND CURRENT ROW) = 1",
    "ABS(orders.amount) OVER () > 1",
    "SUM(DISTINCT orders.amount) OVER () > 1",
    "ROW_NUMBER() OVER (PARTITION BY ROW_NUMBER() OVER ()) = 1",
    f"ROW_NUMBER() OVER (ORDER BY RANK() {ORDERED}) = 1",
    "ROW_NUMBER() OVER (PARTITION BY orders.status ~ '^c') = 1",
    "FIRST_VALUE(*) OVER () > 1",
    "LAG(*) OVER () > 1",
    "ROW_NUMBER() OVER (ORDER BY orders.amount ROWS BETWEEN 10000000000000000000000000000 PRECEDING "
    "AND 10000000000000000000000000001 PRECEDING) = 1",
    "APPROX_PERCENTILE(orders.amount, 0.5) OVER () > 1",  # SLayer cannot evaluate it yet (DEV-2282)
]

#: Complete windows over known fields.
COMPLETE = [
    "SUM(orders.amount) OVER (PARTITION BY orders.status) > 10",
    "AVG(orders.amount) OVER (ORDER BY orders.amount DESC NULLS LAST ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) > 1",
    "COUNT(*) OVER () > 1",
    "SUM(orders.amount) FILTER (WHERE orders.amount > 1) OVER () > 1",
    "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY orders.amount) OVER () > 1",
    "ROW_NUMBER() OVER (ORDER BY orders.amount ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) = 1",
    "ROW_NUMBER() OVER (ORDER BY orders.amount ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) = 1",
    "ROW_NUMBER() OVER (ORDER BY orders.amount RANGE UNBOUNDED PRECEDING) = 1",
    "ROW_NUMBER() OVER (ORDER BY orders.amount ROWS 2 PRECEDING) = 1",
    "ROW_NUMBER() OVER (ORDER BY orders.amount ROWS BETWEEN 1e30 PRECEDING AND 2 PRECEDING) = 1",
    "ROW_NUMBER() OVER (ORDER BY orders.amount ROWS BETWEEN 1e1000000000 PRECEDING AND CURRENT ROW) = 1",
]


#: (function, arguments, code): each Ossie window function at and beyond its arity bounds.
ARITIES = [
    *[(name, args, "E_WINDOW_IN_WHERE" if not args else "E_INVALID_QUERY")
      for name in ("ROW_NUMBER", "RANK", "DENSE_RANK", "PERCENT_RANK", "CUME_DIST") for args in ("", "orders.amount")],
    ("NTILE", "", "E_INVALID_QUERY"), ("NTILE", "4", "E_WINDOW_IN_WHERE"), ("NTILE", "4, 2", "E_INVALID_QUERY"),
    *[(name, args, code) for name in ("LAG", "LEAD") for args, code in (
        ("", "E_INVALID_QUERY"), ("orders.amount", "E_WINDOW_IN_WHERE"), ("orders.amount, 1", "E_WINDOW_IN_WHERE"),
        ("orders.amount, 1, 0", "E_WINDOW_IN_WHERE"), ("orders.amount, 1, 0, 5", "E_INVALID_QUERY"))],
    *[(name, args, code) for name in ("FIRST_VALUE", "LAST_VALUE") for args, code in (
        ("", "E_INVALID_QUERY"), ("orders.amount", "E_WINDOW_IN_WHERE"), ("orders.amount, 1", "E_INVALID_QUERY"))],
    ("NTH_VALUE", "orders.amount", "E_INVALID_QUERY"), ("NTH_VALUE", "orders.amount, 2", "E_WINDOW_IN_WHERE"),
    ("NTH_VALUE", "orders.amount, 2, 3", "E_INVALID_QUERY"),
]

#: (aggregate, code when plain): each judged alike when windowed, row 7 replacing row 6.
AGGREGATE_SHAPES = [
    ("SUM(orders.amount, orders.amount)", "E_INVALID_QUERY"),
    ("SUM()", "E_INVALID_QUERY"),
    ("AVG(orders.amount, orders.amount)", "E_INVALID_QUERY"),
    ("COUNT()", "E_INVALID_QUERY"),
    ("COUNT(orders.amount, orders.amount)", "E_INVALID_QUERY"),
    ("APPROX_PERCENTILE(orders.amount)", "E_INVALID_QUERY"),
    ("APPROX_PERCENTILE(orders.amount, 0.5, 1)", "E_INVALID_QUERY"),
    ("PERCENTILE_CONT(0.5, 0.1) WITHIN GROUP (ORDER BY orders.amount)", "E_INVALID_QUERY"),
    ("COUNT(DISTINCT orders.amount, unexpected=1)", "E_INVALID_QUERY"),
    ("COUNT(DISTINCT orders.amount)", "E_AGGREGATE_IN_WHERE"),
    ("PERCENTILE_CONT(orders.amount)", "E_INVALID_QUERY"),
    ("PERCENTILE_DISC(orders.amount)", "E_INVALID_QUERY"),
    ("COUNT(*)", "E_AGGREGATE_IN_WHERE"),
    ("COUNT(orders.amount)", "E_AGGREGATE_IN_WHERE"),
    ("APPROX_PERCENTILE(orders.amount, 0.5)", "E_INVALID_QUERY"),
    ("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY orders.amount)", "E_AGGREGATE_IN_WHERE"),
    ("SUM(orders.amount)", "E_AGGREGATE_IN_WHERE"),
]


@pytest.fixture
async def engine(tmp_path: Path) -> AsyncGenerator[SlayerQueryEngine]:
    async with sql_engine(tmp_path, sql=SQL, datasets=DATASETS) as engine:
        yield engine


def _query(filter_text: str) -> dict[str, Any]:
    return {"dimensions": ["orders.order_id"], "filter": filter_text}


@pytest.mark.parametrize("filter_text", MALFORMED)
async def test_a_malformed_window_is_malformed(engine: SlayerQueryEngine, filter_text: str) -> None:
    await strict_error(engine, _query(filter_text), "E_INVALID_QUERY")


@pytest.mark.parametrize(("filter_text", "code"), [
    ("ROW_NUMBER() OVER (ORDER BY orders.no_such_field) = 1", "E_NAME_NOT_FOUND"),
    ("ROW_NUMBER() OVER (PARTITION BY orders.no_such_field) = 1", "E_NAME_NOT_FOUND"),
    ("SUM(orders.no_such_field) OVER () > 1", "E_NAME_NOT_FOUND"),
    ("SUM(orders.amount) OVER (PARTITION BY orders.total_revenue) > 10", "E_AGGREGATE_IN_WHERE"),
    ("ROW_NUMBER() OVER (ORDER BY SUM(orders.amount)) = 1", "E_AGGREGATE_IN_WHERE"),
])
async def test_names_inside_a_window_resolve_before_the_window(engine: SlayerQueryEngine, filter_text: str,
                                                               code: str) -> None:
    await strict_error(engine, _query(filter_text), code)


@pytest.mark.parametrize("filter_text", [
    f"orders.status ~ '^c' AND ROW_NUMBER() {ORDERED} = 1",
    "orders.status ~ '^c' AND ROW_NUMBER() OVER (ORDER BY orders.no_such_field) = 1",
    f"orders.amount == 1 AND ROW_NUMBER() {ORDERED} = 1",
    f"contains(orders.tags, 'x') AND ROW_NUMBER() {ORDERED} = 1",
])
async def test_the_rest_of_a_windowed_filter_is_judged(engine: SlayerQueryEngine, filter_text: str) -> None:
    await strict_error(engine, _query(filter_text), "E_INVALID_QUERY")


@pytest.mark.parametrize("filter_text", COMPLETE)
async def test_a_complete_window_is_a_window_in_the_filter(engine: SlayerQueryEngine, filter_text: str) -> None:
    await strict_error(engine, _query(filter_text), "E_WINDOW_IN_WHERE")


@pytest.mark.parametrize(("filter_text", "variables"), [
    (f"ROW_NUMBER() {ORDERED} = {{n}}", {"n": 1}),
    (f"LAG(orders.amount, {{k}}) {ORDERED} > 1", {"k": 1}),
    (f"orders.status = '{{s}}' AND ROW_NUMBER() {ORDERED} = 1", {"s": "complete"}),
])
async def test_a_bound_variable_in_or_beside_a_window(engine: SlayerQueryEngine, filter_text: str,
                                                      variables: dict[str, Any]) -> None:
    await strict_error(engine, _query(filter_text), "E_WINDOW_IN_WHERE", variables=variables)


@pytest.mark.parametrize(("filter_text", "variables"), [
    pytest.param("ROW_NUMBER() OVER (ORDER BY orders.amount = {n}", {"n": 1}, id="malformed-window"),
    pytest.param(f"orders.amount > {{v}} AND ROW_NUMBER() {ORDERED} = 1", {"v": float("nan")}, id="bad-slot"),
])
async def test_a_binding_failure_beside_a_window_is_malformed(engine: SlayerQueryEngine, filter_text: str,
                                                              variables: dict[str, Any]) -> None:
    await strict_error(engine, _query(filter_text), "E_INVALID_QUERY", variables=variables)


@pytest.mark.parametrize(("name", "args", "code"), ARITIES)
async def test_a_window_function_takes_ossies_arguments(engine: SlayerQueryEngine, name: str, args: str,
                                                        code: str) -> None:
    await strict_error(engine, _query(f"{name}({args}) {ORDERED} > 0"), code)


@pytest.mark.parametrize(("aggregate", "code"), AGGREGATE_SHAPES)
async def test_an_aggregate_takes_ossies_arguments(engine: SlayerQueryEngine, aggregate: str, code: str) -> None:
    await strict_error(engine, _query(f"{aggregate} > 1"), code)
    windowed = "E_WINDOW_IN_WHERE" if code == "E_AGGREGATE_IN_WHERE" else code
    await strict_error(engine, _query(f"{aggregate} OVER () > 1"), windowed)


async def test_native_evaluates_a_window_in_the_filter_before_and_after_strict(engine: SlayerQueryEngine) -> None:
    query = _query(f"ROW_NUMBER() {ORDERED} = 1")
    before = await engine.execute(query)
    await strict_error(engine, query, "E_WINDOW_IN_WHERE")
    after = await engine.execute(query)
    assert [r["orders.order_id"] for r in before.data] == ["O3"]
    assert [r["orders.order_id"] for r in after.data] == ["O3"]
