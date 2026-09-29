"""Tests for SqliteDialect."""

from __future__ import annotations

import sqlglot
from sqlglot import exp

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.sql.dialects import sqlite as sqlite_mod
from slayer.sql.dialects.sqlite import (
    SqliteDialect,
    register_sqlite_udfs,
    rewrite_sqlite_json_extract,
)

from slayer.storage.sqlite_conn import transaction


# Fields


def test_sqlite_sqlglot_name() -> None:
    assert SqliteDialect().sqlglot_name == "sqlite"


def test_sqlite_explain_prefix() -> None:
    assert SqliteDialect().explain_prefix == "EXPLAIN QUERY PLAN"


def test_sqlite_log10_and_log2_native() -> None:
    """SQLite gets log10 / log2 via registered UDFs — both must be native."""
    d = SqliteDialect()
    assert d.should_use_native_log(10) is True
    assert d.should_use_native_log(2) is True


# build_date_trunc — STRFTIME forms


@pytest.mark.parametrize(
    "granularity,expected_fmt",
    [
        (TimeGranularity.YEAR, "%Y-01-01"),
        (TimeGranularity.MONTH, "%Y-%m-01"),
        (TimeGranularity.DAY, "%Y-%m-%d"),
        (TimeGranularity.HOUR, "%Y-%m-%d %H:00:00"),
        (TimeGranularity.MINUTE, "%Y-%m-%d %H:%M:00"),
        (TimeGranularity.SECOND, "%Y-%m-%d %H:%M:%S"),
    ],
)
def test_sqlite_build_date_trunc_strftime_forms(
    granularity: TimeGranularity, expected_fmt: str
) -> None:
    d = SqliteDialect()
    col = sqlglot.parse_one("created_at", dialect="sqlite")
    out = d.build_date_trunc(col, granularity)
    sql = out.sql(dialect="sqlite")
    assert "STRFTIME" in sql.upper()
    assert expected_fmt in sql


def test_sqlite_build_date_trunc_week_uses_weekday_modifier() -> None:
    """Week truncation uses DATE(col, 'weekday 0', '-6 days')."""
    d = SqliteDialect()
    col = sqlglot.parse_one("created_at", dialect="sqlite")
    out = d.build_date_trunc(col, TimeGranularity.WEEK)
    sql = out.sql(dialect="sqlite")
    assert "weekday 0" in sql
    assert "-6 days" in sql


def test_sqlite_build_date_trunc_week_sunday_emission() -> None:
    """WEEK_SUNDAY composes the date-add primitive around SQLite's Monday-week truncation."""
    d = SqliteDialect()
    col = sqlglot.parse_one("ordered_at", dialect="sqlite")
    out = d.build_date_trunc(col, TimeGranularity.WEEK_SUNDAY)
    sql = out.sql(dialect="sqlite")
    assert sql == (
        "SLAYER_DATE_ADD(DATE(SLAYER_DATE_ADD(ordered_at, 1, 'day'), 'weekday 0', '-6 days'), -1, 'day')"
    )


@pytest.mark.parametrize(
    "input_date,expected_sunday",
    [
        ("2024-01-07", "2024-01-07"),  # Sunday   -> itself
        ("2024-01-08", "2024-01-07"),  # Monday
        ("2024-01-09", "2024-01-07"),  # Tuesday
        ("2024-01-10", "2024-01-07"),  # Wednesday
        ("2024-01-11", "2024-01-07"),  # Thursday
        ("2024-01-12", "2024-01-07"),  # Friday
        ("2024-01-13", "2024-01-07"),  # Saturday
        ("2024-01-01", "2023-12-31"),  # Monday, crosses the year boundary
    ],
)
def test_sqlite_build_date_trunc_week_sunday_executes_to_sunday(
    input_date: str, expected_sunday: str
) -> None:
    """The emitted WEEK_SUNDAY expression buckets to the exact Sunday on real SQLite."""
    d = SqliteDialect()
    col = sqlglot.parse_one("ts", dialect="sqlite")
    expr = d.build_date_trunc(
        col, TimeGranularity.WEEK_SUNDAY
    ).sql(dialect="sqlite")

    with transaction(":memory:") as con:
        d.register_udfs(con)
        con.execute("CREATE TABLE t(ts TEXT)")
        con.execute("INSERT INTO t VALUES (?)", (input_date,))
        (got,) = con.execute(f"SELECT {expr} FROM t").fetchone()
    assert got == expected_sunday


def test_sqlite_build_date_trunc_quarter_uses_case_when() -> None:
    """Quarter truncation uses STRFTIME + CASE WHEN to map month→quarter start."""
    d = SqliteDialect()
    col = sqlglot.parse_one("created_at", dialect="sqlite")
    out = d.build_date_trunc(col, TimeGranularity.QUARTER)
    sql = out.sql(dialect="sqlite").upper()
    assert "CASE" in sql
    assert "STRFTIME" in sql
    # The four quarter-start dates
    assert "01-01" in sql
    assert "04-01" in sql
    assert "07-01" in sql
    assert "10-01" in sql


# build_date_add — the slayer_date_add UDF (SQLite modifiers overflow past month-end)


@pytest.mark.parametrize("count,unit", [(3, "day"), (-1, "week"), (1, "quarter")])
def test_sqlite_build_date_add_calls_the_udf(count: int, unit: str) -> None:
    d = SqliteDialect()
    col = exp.column("created_at")
    out = d.build_date_add(
        expr=col, count=exp.Literal.number(count), unit=TimeGranularity(unit), operand=DataType.DATE,
    )
    assert out.sql(dialect="sqlite") == f"SLAYER_DATE_ADD(created_at, {count}, '{unit}')"


# build_percentile — scientific notation must survive (Codex finding #3)


def test_sqlite_build_percentile_preserves_scientific_notation() -> None:
    """``5e-2`` must NOT be normalized to ``0.05`` — the original spelling travels through the dialect intact."""
    d = SqliteDialect()
    out = d.build_percentile(p=exp.Literal.number("5e-2"), col_expr=exp.column("amount"))
    assert "5e-2" in out.sql(dialect="sqlite")


# build_median / build_percentile — SQLite UDF forms


def test_sqlite_build_median_emits_percentile_cont_pair_form() -> None:
    """``median(x)`` transpiles to the UDF pair form ``PERCENTILE_CONT(x, 0.5)``."""
    d = SqliteDialect()
    inner = sqlglot.parse_one("amount", dialect="sqlite")
    out = d.build_median(inner)
    sql = out.sql(dialect="sqlite")
    assert "PERCENTILE_CONT(" in sql.upper()
    assert "0.5" in sql
    # SQLite must NOT emit the WITHIN GROUP form — its UDF takes (value, p)
    assert "WITHIN GROUP" not in sql.upper()


def test_sqlite_build_percentile_uses_percentile_cont_udf() -> None:
    """SQLite's UDF is ``percentile_cont(value, p)`` — args in that order."""
    d = SqliteDialect()
    out = d.build_percentile(p=exp.Literal.number("0.95"), col_expr=exp.column("amount"))
    sql = out.sql(dialect="sqlite")
    assert "percentile_cont(" in sql.lower()
    assert "0.95" in sql


def test_sqlite_build_percentile_preserves_literal_string() -> None:
    d = SqliteDialect()
    out = d.build_percentile(p=exp.Literal.number("0.50"), col_expr=exp.column("amount"))
    assert "0.50" in out.sql(dialect="sqlite")


# Module-level helpers (folded in from the deleted sqlite_dialect.py / sqlite_udfs.py)


def test_rewrite_sqlite_json_extract_is_module_level() -> None:
    """The JSON rewrite helper is a module-level function in the sqlite dialect module."""
    assert callable(rewrite_sqlite_json_extract)


def test_register_sqlite_udfs_is_module_level() -> None:
    """``register_sqlite_udfs`` is a module-level helper."""
    assert callable(register_sqlite_udfs)


def test_sqlite_module_exposes_udf_aggregate_classes() -> None:
    """The UDF aggregate classes are module-level in ``slayer.sql.dialects.sqlite``."""
    expected = [
        "_CorrAgg",
        "_CovarPopAgg",
        "_CovarSampAgg",
        "_MedianAgg",
        "_PercentileContAgg",
        "_PercentileDiscAgg",
        "_StddevPopAgg",
        "_StddevSampAgg",
        "_VarPopAgg",
        "_VarSampAgg",
    ]
    for name in expected:
        assert hasattr(sqlite_mod, name), f"missing module-level class: {name}"

    # Smoke-instantiate one to confirm it's a real working class
    agg = sqlite_mod._MedianAgg()
    agg.step(1)
    agg.step(2)
    agg.step(3)
    assert agg.finalize() == 2
