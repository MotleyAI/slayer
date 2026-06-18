"""DEV-1542: tests for SqliteDialect.

SQLite is the most-overridden dialect: STRFTIME date_trunc, DATETIME-modifier
time arithmetic, Python UDF aggregates, JSON-extract AST rewrite.

``rewrite_sqlite_json_extract``, ``register_sqlite_udfs``, and the UDF
aggregate classes are module-level helpers defined in
``slayer/sql/dialects/sqlite.py`` (not nested inside ``SqliteDialect``).
The dialect class's ``rewrite_parsed_ast`` / ``register_udfs`` methods
delegate to these helpers in one line. Tests import the helpers
directly — this is where they live now, no shim layer.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

import pytest

from slayer.core.enums import TimeGranularity
from slayer.sql.dialects import sqlite as sqlite_mod
from slayer.sql.dialects.sqlite import (
    SqliteDialect,
    register_sqlite_udfs,
    rewrite_sqlite_json_extract,
)


def _parse_sqlite(sql: str) -> exp.Expression:
    """SQLite-style parse: parses then runs the JSON-extract rewrite."""
    tree = sqlglot.parse_one(sql, dialect="sqlite")
    return rewrite_sqlite_json_extract(tree)


# ---------------------------------------------------------------------------
# Fields
# ---------------------------------------------------------------------------


def test_sqlite_sqlglot_name() -> None:
    assert SqliteDialect().sqlglot_name == "sqlite"


def test_sqlite_explain_prefix() -> None:
    assert SqliteDialect().explain_prefix == "EXPLAIN QUERY PLAN"


def test_sqlite_log10_and_log2_native() -> None:
    """SQLite gets log10 / log2 via registered UDFs — both must be native."""
    d = SqliteDialect()
    assert d.should_use_native_log(10) is True
    assert d.should_use_native_log(2) is True


# ---------------------------------------------------------------------------
# build_date_trunc — STRFTIME forms
# ---------------------------------------------------------------------------


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
    out = d.build_date_trunc(col, granularity, parse=_parse_sqlite)
    sql = out.sql(dialect="sqlite")
    assert "STRFTIME" in sql.upper()
    assert expected_fmt in sql


def test_sqlite_build_date_trunc_week_uses_weekday_modifier() -> None:
    """Week truncation uses DATE(col, 'weekday 0', '-6 days')."""
    d = SqliteDialect()
    col = sqlglot.parse_one("created_at", dialect="sqlite")
    out = d.build_date_trunc(col, TimeGranularity.WEEK, parse=_parse_sqlite)
    sql = out.sql(dialect="sqlite")
    assert "weekday 0" in sql
    assert "-6 days" in sql


def test_sqlite_build_date_trunc_week_sunday_emission() -> None:
    """DEV-1572: WEEK_SUNDAY delegates to the generic shift, which composes
    SQLite's day-offset (+1d / -1d) around SQLite's Monday-week truncation."""
    d = SqliteDialect()
    col = sqlglot.parse_one("ordered_at", dialect="sqlite")
    out = d.build_date_trunc(col, TimeGranularity.WEEK_SUNDAY, parse=_parse_sqlite)
    sql = out.sql(dialect="sqlite")
    assert sql == (
        "DATE(DATE(DATE(ordered_at, '1 days'), 'weekday 0', '-6 days'), '-1 days')"
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
    """Execute the emitted SQLite WEEK_SUNDAY expression against a real
    in-memory SQLite connection and assert the bucket lands on the exact
    Sunday (not merely 'a Sunday', and not a week early). Native DATE()
    math — no UDFs required.
    """
    import sqlite3

    d = SqliteDialect()
    col = sqlglot.parse_one("ts", dialect="sqlite")
    expr = d.build_date_trunc(
        col, TimeGranularity.WEEK_SUNDAY, parse=_parse_sqlite
    ).sql(dialect="sqlite")

    con = sqlite3.connect(":memory:")
    try:
        con.execute("CREATE TABLE t(ts TEXT)")
        con.execute("INSERT INTO t VALUES (?)", (input_date,))
        (got,) = con.execute(f"SELECT {expr} FROM t").fetchone()
    finally:
        con.close()
    assert got == expected_sunday


def test_sqlite_build_date_trunc_quarter_uses_case_when() -> None:
    """Quarter truncation uses STRFTIME + CASE WHEN to map month→quarter start."""
    d = SqliteDialect()
    col = sqlglot.parse_one("created_at", dialect="sqlite")
    out = d.build_date_trunc(col, TimeGranularity.QUARTER, parse=_parse_sqlite)
    sql = out.sql(dialect="sqlite").upper()
    assert "CASE" in sql
    assert "STRFTIME" in sql
    # The four quarter-start dates
    assert "01-01" in sql
    assert "04-01" in sql
    assert "07-01" in sql
    assert "10-01" in sql


# ---------------------------------------------------------------------------
# build_time_offset_expr — DATE(col, 'N units')
# ---------------------------------------------------------------------------


def test_sqlite_build_time_offset_expr_positive_day() -> None:
    d = SqliteDialect()
    col = sqlglot.parse_one("created_at", dialect="sqlite")
    out = d.build_time_offset_expr(col, offset=3, granularity="day")
    sql = out.sql(dialect="sqlite")
    assert "DATE(" in sql.upper()
    assert "'3 days'" in sql


def test_sqlite_build_time_offset_expr_negative_week_normalizes_to_days() -> None:
    """SQLite has no week unit — week→7*days."""
    d = SqliteDialect()
    col = sqlglot.parse_one("created_at", dialect="sqlite")
    out = d.build_time_offset_expr(col, offset=-1, granularity="week")
    sql = out.sql(dialect="sqlite")
    assert "'-7 days'" in sql


def test_sqlite_build_time_offset_expr_quarter_normalizes_to_months() -> None:
    """Quarter→3*month."""
    d = SqliteDialect()
    col = sqlglot.parse_one("created_at", dialect="sqlite")
    out = d.build_time_offset_expr(col, offset=1, granularity="quarter")
    sql = out.sql(dialect="sqlite")
    assert "'3 months'" in sql


# ---------------------------------------------------------------------------
# duration_interval_exprs / add_intervals_expr — SQLite uses DATETIME-modifier strings
# ---------------------------------------------------------------------------


def test_sqlite_duration_interval_exprs_emits_modifier_strings() -> None:
    """SQLite has no INTERVAL syntax — duration parts become DATETIME-modifier
    string literals like ``'+2 days'`` / ``'+3 hours'`` with the sign baked in."""
    d = SqliteDialect()
    out = d.duration_interval_exprs([(2, "d"), (3, "h")], sign=1)
    assert len(out) == 2
    # Every entry is a string literal, NOT an exp.Interval
    assert all(isinstance(n, exp.Literal) for n in out)
    sql_parts = [n.sql(dialect="sqlite") for n in out]
    assert "'+2 days'" in sql_parts
    assert "'+3 hours'" in sql_parts


def test_sqlite_duration_interval_exprs_negative_sign_baked_in() -> None:
    d = SqliteDialect()
    out = d.duration_interval_exprs([(1, "d")], sign=-1)
    assert out[0].sql(dialect="sqlite") == "'-1 days'"


def test_sqlite_duration_interval_exprs_week_normalizes_to_days() -> None:
    """SQLite has no week unit — weeks→N*7 days, sign baked in."""
    d = SqliteDialect()
    out = d.duration_interval_exprs([(2, "w")], sign=1)
    assert out[0].sql(dialect="sqlite") == "'+14 days'"


def test_sqlite_add_intervals_expr_wraps_in_datetime_call() -> None:
    """SQLite wraps as ``DATETIME(expr, mod1, mod2, ...)`` — the sign is
    already baked into each modifier by ``duration_interval_exprs``, so
    the ``sign`` arg is intentionally ignored on SQLite."""
    d = SqliteDialect()
    col = sqlglot.parse_one("created_at", dialect="sqlite")
    modifiers = d.duration_interval_exprs([(1, "d")], sign=-1)
    out = d.add_intervals_expr(col, modifiers, sign=-1)
    sql = out.sql(dialect="sqlite")
    assert "DATETIME(" in sql.upper()
    assert "'-1 days'" in sql


# ---------------------------------------------------------------------------
# build_percentile — scientific notation must survive (Codex finding #3)
# ---------------------------------------------------------------------------


def test_sqlite_build_percentile_preserves_scientific_notation() -> None:
    """``5e-2`` must NOT be normalized to ``0.05`` — the original spelling
    travels through the dialect intact."""
    d = SqliteDialect()
    out = d.build_percentile("5e-2", "amount", parse=_parse_sqlite)
    assert "5e-2" in out.sql(dialect="sqlite")


# ---------------------------------------------------------------------------
# build_median / build_percentile — SQLite UDF forms
# ---------------------------------------------------------------------------


def test_sqlite_build_median_emits_percentile_cont_pair_form() -> None:
    """``build_median`` parses ``median(x)`` into ``exp.Median``; sqlglot's
    SQLite generator then transpiles that to ``PERCENTILE_CONT(x, 0.5)``
    (the pair form the registered UDF expects — value first, p second).
    Crucially NOT the WITHIN GROUP form that Postgres/DuckDB use."""
    d = SqliteDialect()
    inner = sqlglot.parse_one("amount", dialect="sqlite")
    out = d.build_median(inner, parse=_parse_sqlite)
    sql = out.sql(dialect="sqlite")
    assert "PERCENTILE_CONT(" in sql.upper()
    assert "0.5" in sql
    # SQLite must NOT emit the WITHIN GROUP form — its UDF takes (value, p)
    assert "WITHIN GROUP" not in sql.upper()


def test_sqlite_build_percentile_uses_percentile_cont_udf() -> None:
    """SQLite's UDF is ``percentile_cont(value, p)`` — args in that order."""
    d = SqliteDialect()
    out = d.build_percentile("0.95", "amount", parse=_parse_sqlite)
    sql = out.sql(dialect="sqlite")
    assert "percentile_cont(" in sql.lower()
    assert "0.95" in sql


def test_sqlite_build_percentile_preserves_literal_string() -> None:
    d = SqliteDialect()
    out = d.build_percentile("0.50", "amount", parse=_parse_sqlite)
    assert "0.50" in out.sql(dialect="sqlite")


# ---------------------------------------------------------------------------
# Module-level helpers (folded in from the deleted sqlite_dialect.py / sqlite_udfs.py)
# ---------------------------------------------------------------------------


def test_rewrite_sqlite_json_extract_is_module_level() -> None:
    """The rewrite helper lives as a module-level function in
    ``slayer.sql.dialects.sqlite`` — directly importable, used by
    ``SqliteDialect.rewrite_parsed_ast`` AND by callers that need the
    rewrite without going through the dialect class
    (e.g. ``tests/test_sqlite_json_extract.py``)."""
    assert callable(rewrite_sqlite_json_extract)


def test_register_sqlite_udfs_is_module_level() -> None:
    """``register_sqlite_udfs`` is a module-level helper. ``SqliteDialect.register_udfs``
    delegates to it; ``tests/test_sqlite_udfs.py`` imports it directly."""
    assert callable(register_sqlite_udfs)


def test_sqlite_module_exposes_udf_aggregate_classes() -> None:
    """The UDF aggregate classes are module-level in
    ``slayer.sql.dialects.sqlite``. ``tests/test_sqlite_udfs.py``
    imports them directly to drive step/finalize."""
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
