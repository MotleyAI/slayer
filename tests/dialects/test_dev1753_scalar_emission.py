"""Exact per-dialect SQL for ceiling, sign, ltrim, rtrim and substring on every Tier-1 dialect."""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.sql.dialects import get_dialect
from slayer.sql.render.value_expr import render_scalar_call

TIER1 = ["sqlite", "postgres", "duckdb", "mysql", "clickhouse", "tsql", "snowflake", "bigquery"]


def _same(sql: str) -> dict[str, str]:
    return dict.fromkeys(TIER1, sql)


EXPECTED: dict[tuple[str, int], dict[str, str]] = {
    ("ceiling", 1): {**_same("CEIL(a)"), "tsql": "CEILING(a)"},
    ("sign", 1): _same("SIGN(a)"),
    ("ltrim", 1): _same("LTRIM(a)"),
    ("rtrim", 1): _same("RTRIM(a)"),
    # T-SQL SUBSTRING requires a length; LEN(a) reaches the end of the string.
    ("substring", 2): {**_same("SUBSTRING(a, b)"), "postgres": "SUBSTRING(a FROM b)", "tsql": "SUBSTRING(a, b, LEN(a))"},
    ("substring", 3): {**_same("SUBSTRING(a, b, c)"), "postgres": "SUBSTRING(a FROM b FOR c)"},
}

CASES = [(name, argc, d) for (name, argc) in EXPECTED for d in TIER1]


def _emit(name: str, argc: int, dialect: str) -> str:
    d = get_dialect(dialect)
    args: list[Expression] = [exp.column(c) for c in "abc"[:argc]]
    return render_scalar_call(name=name, args=args, dialect=d).sql(dialect=d.sqlglot_name)


@pytest.mark.parametrize("name,argc,dialect", CASES)
def test_exact_emission(name: str, argc: int, dialect: str) -> None:
    assert _emit(name, argc, dialect) == EXPECTED[(name, argc)][dialect]


@pytest.mark.parametrize("name,argc,dialect", CASES)
def test_emission_reparses_in_its_dialect(name: str, argc: int, dialect: str) -> None:
    sqlglot.parse_one(_emit(name, argc, dialect), read=get_dialect(dialect).sqlglot_name)


def test_tsql_two_arg_substr_gets_a_length() -> None:
    assert _emit("substr", 2, "tsql") == "SUBSTRING(a, b, LEN(a))"
