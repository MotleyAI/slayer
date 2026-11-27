"""Golden SQL for moved-frame shifted shapes on the five golden dialects (spec: queries/transforms)."""

from __future__ import annotations

from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

from slayer.sql.scope_check import assert_dependency_ordered_ctes, assert_scope_closed

from tests._dev2429_fixtures import AMOUNT, generate, query
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev2429_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]
ALLOWED_DELTAS: dict[str, str] = {}
PREDICATE = "predicate"


def _cases() -> dict:
    return {
        PREDICATE: f"time_shift({AMOUNT} > 35, -1)",
        "nested_transform": f"time_shift(cumsum({AMOUNT}), -1)",
        "cross_model_composite": f"time_shift({AMOUNT} + sum(customers.spend), -1)",
    }


async def _generate_one(case: str, dialect: str):
    try:
        return await generate(query(case), dialect=dialect)
    except Exception as exc:  # noqa: BLE001 — the raise itself is the contract
        return record_raise(exc)


bind_golden_tests(
    namespace=globals(),
    golden_path=GOLDEN_PATH,
    cases=_cases,
    dialects=DIALECTS,
    allowed=ALLOWED_DELTAS,
    generate_one=_generate_one,
)


@pytest.mark.parametrize("case_id", sorted(_cases()))
@pytest.mark.parametrize("dialect", DIALECTS)
async def test_scope_closed_and_dependency_ordered(case_id: str, dialect: str) -> None:
    sql = await _generate_one(_cases()[case_id], dialect)
    assert isinstance(sql, str), sql
    assert_dependency_ordered_ctes(sql, dialect=dialect)
    assert_scope_closed(sql, dialect=dialect)


async def test_tsql_projects_the_predicate_answer_as_a_value() -> None:
    """T-SQL has no boolean value: no projection may be a bare condition."""
    sql = await _generate_one(_cases()[PREDICATE], "tsql")
    assert isinstance(sql, str), sql
    for select in sqlglot.parse_one(sql, read="tsql").find_all(exp.Select):
        for column in select.expressions:
            assert not isinstance(column.unalias(), (exp.Predicate, exp.Connector, exp.Not)), column.sql("tsql")
