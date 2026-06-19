"""DEV-1542: tests for PostgresDialect.

Postgres is the base-class shape made explicit. Native DATE_TRUNC,
PERCENTILE_CONT, CORR, COVAR_SAMP, COVAR_POP, and native log10/log2 via
sqlglot's anonymous-rewrite path.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from slayer.core.enums import TimeGranularity
from slayer.sql.dialects.postgres import PostgresDialect


def _parse_pg(sql: str) -> exp.Expression:
    return sqlglot.parse_one(sql, dialect="postgres")


def test_postgres_sqlglot_name() -> None:
    assert PostgresDialect().sqlglot_name == "postgres"


def test_postgres_explain_prefix() -> None:
    assert PostgresDialect().explain_prefix == "EXPLAIN ANALYZE"


def test_postgres_log_native_flags() -> None:
    d = PostgresDialect()
    assert d.should_use_native_log(10) is True
    assert d.should_use_native_log(2) is True


# ---------------------------------------------------------------------------
# build_date_trunc — native DATE_TRUNC
# ---------------------------------------------------------------------------


def test_postgres_build_date_trunc_month() -> None:
    d = PostgresDialect()
    col = sqlglot.parse_one("created_at", dialect="postgres")
    out = d.build_date_trunc(col, TimeGranularity.MONTH, parse=_parse_pg)
    sql = out.sql(dialect="postgres")
    assert "DATE_TRUNC" in sql.upper()
    assert "'month'" in sql.lower()


def test_postgres_build_date_trunc_quarter() -> None:
    """Postgres has a native QUARTER unit for DATE_TRUNC — emitted as-is."""
    d = PostgresDialect()
    col = sqlglot.parse_one("created_at", dialect="postgres")
    out = d.build_date_trunc(col, TimeGranularity.QUARTER, parse=_parse_pg)
    sql = out.sql(dialect="postgres").lower()
    assert "date_trunc" in sql
    assert "quarter" in sql


def test_postgres_build_date_trunc_casts_literal_to_timestamp() -> None:
    d = PostgresDialect()
    lit = sqlglot.parse_one("'2025-01-01'", dialect="postgres")
    out = d.build_date_trunc(lit, TimeGranularity.MONTH, parse=_parse_pg)
    assert "CAST" in out.sql(dialect="postgres").upper()


def test_postgres_build_date_trunc_week_sunday_shift() -> None:
    """DEV-1572: WEEK_SUNDAY = Monday-week of (col + 1 day) minus 1 day.

    The generic shift reuses Postgres' native (Monday-based) DATE_TRUNC('week').
    Both day-shift legs must be present so the bucket lands on Sunday.
    """
    d = PostgresDialect()
    col = sqlglot.parse_one("ordered_at", dialect="postgres")
    out = d.build_date_trunc(col, TimeGranularity.WEEK_SUNDAY, parse=_parse_pg)
    sql = out.sql(dialect="postgres")
    up = sql.upper()
    assert "DATE_TRUNC('WEEK'" in up
    # Inner +1 day and outer -1 day legs.
    assert "+ INTERVAL '1 DAY'" in up
    assert "- INTERVAL '1 DAY'" in up


# ---------------------------------------------------------------------------
# build_time_offset_expr — INTERVAL N UNIT
# ---------------------------------------------------------------------------


def test_postgres_build_time_offset_expr_day() -> None:
    d = PostgresDialect()
    col = sqlglot.parse_one("created_at", dialect="postgres")
    out = d.build_time_offset_expr(col, offset=3, granularity="day")
    sql = out.sql(dialect="postgres").upper()
    assert "INTERVAL" in sql
    assert "DAY" in sql


def test_postgres_build_time_offset_expr_quarter_normalizes_to_3_month() -> None:
    """Postgres uses ``INTERVAL '3 month'`` for quarter — preserves today's
    ``val * 3`` multiplication in ``generator.py:_build_time_offset_expr``."""
    d = PostgresDialect()
    col = sqlglot.parse_one("created_at", dialect="postgres")
    out = d.build_time_offset_expr(col, offset=1, granularity="quarter")
    sql = out.sql(dialect="postgres").upper()
    assert "MONTH" in sql
    assert "3" in sql


# ---------------------------------------------------------------------------
# build_median / build_percentile — PERCENTILE_CONT
# ---------------------------------------------------------------------------


def test_postgres_build_median() -> None:
    d = PostgresDialect()
    inner = sqlglot.parse_one("amount", dialect="postgres")
    out = d.build_median(inner, parse=_parse_pg)
    sql = out.sql(dialect="postgres").upper()
    assert "PERCENTILE_CONT" in sql
    assert "WITHIN GROUP" in sql
    assert "0.5" in sql


def test_postgres_build_percentile_native() -> None:
    d = PostgresDialect()
    out = d.build_percentile("0.9", "amount", parse=_parse_pg)
    sql = out.sql(dialect="postgres").upper()
    assert "PERCENTILE_CONT" in sql
    assert "WITHIN GROUP" in sql
    assert "0.9" in sql


def test_postgres_build_percentile_preserves_literal_50() -> None:
    """``0.50`` stays ``0.50``, not normalized to ``0.5``."""
    d = PostgresDialect()
    out = d.build_percentile("0.50", "amount", parse=_parse_pg)
    assert "0.50" in out.sql(dialect="postgres")


# ---------------------------------------------------------------------------
# build_stat_agg_1arg / build_covar_2arg — native CORR / COVAR
# ---------------------------------------------------------------------------


def test_postgres_build_stat_agg_1arg_stddev_samp() -> None:
    d = PostgresDialect()
    out = d.build_stat_agg_1arg("stddev_samp", "amount", parse=_parse_pg)
    sql = out.sql(dialect="postgres").upper()
    assert "STDDEV_SAMP" in sql or "STDDEV(" in sql


def test_postgres_build_covar_2arg_corr_native() -> None:
    d = PostgresDialect()
    out = d.build_covar_2arg("corr", "amount", "quantity", parse=_parse_pg)
    sql = out.sql(dialect="postgres").upper()
    assert "CORR(" in sql


def test_postgres_build_covar_2arg_covar_samp_native() -> None:
    d = PostgresDialect()
    out = d.build_covar_2arg("covar_samp", "amount", "quantity", parse=_parse_pg)
    sql = out.sql(dialect="postgres").upper()
    assert "COVAR_SAMP" in sql


def test_postgres_build_covar_2arg_covar_pop_native() -> None:
    d = PostgresDialect()
    out = d.build_covar_2arg("covar_pop", "amount", "quantity", parse=_parse_pg)
    sql = out.sql(dialect="postgres").upper()
    assert "COVAR_POP" in sql


# ---------------------------------------------------------------------------
# rewrite_parsed_ast / register_udfs — defaults from base
# ---------------------------------------------------------------------------


def test_postgres_rewrite_parsed_ast_is_identity() -> None:
    d = PostgresDialect()
    tree = sqlglot.parse_one("SELECT json_extract(j, '$.k') FROM t", dialect="postgres")
    before = tree.sql(dialect="postgres")
    after = d.rewrite_parsed_ast(tree).sql(dialect="postgres")
    assert before == after


def test_postgres_register_udfs_is_noop() -> None:
    """Default no-op — no Python UDFs needed on Postgres."""
    PostgresDialect().register_udfs(None)  # accepts any arg without side effects


# ---------------------------------------------------------------------------
# build_explain_sql
# ---------------------------------------------------------------------------


def test_postgres_build_explain_sql() -> None:
    assert PostgresDialect().build_explain_sql("SELECT 1") == "EXPLAIN ANALYZE SELECT 1"


# ---------------------------------------------------------------------------
# DEV-1576: rewrite_target_ast — numeric cast for 2-arg ROUND
#
# Postgres has no ``round(double precision, integer)`` — only
# ``round(numeric, integer)``. The target-keyed hook wraps the first arg of a
# 2-arg ROUND in a numeric CAST so 2-arg round over a DOUBLE measure executes.
# 1-arg round and abs are untouched.
# ---------------------------------------------------------------------------


def test_postgres_rewrite_target_ast_casts_two_arg_round() -> None:
    d = PostgresDialect()
    tree = sqlglot.parse_one("ROUND(x, 2)", dialect="postgres")
    out = d.rewrite_target_ast(tree).sql(dialect="postgres").upper()
    assert "ROUND(CAST(" in out
    assert "AS DECIMAL" in out or "AS NUMERIC" in out


def test_postgres_rewrite_target_ast_one_arg_round_unchanged() -> None:
    d = PostgresDialect()
    tree = sqlglot.parse_one("ROUND(x)", dialect="postgres")
    out = d.rewrite_target_ast(tree).sql(dialect="postgres").upper()
    assert "CAST(" not in out


def test_postgres_rewrite_target_ast_abs_unchanged() -> None:
    d = PostgresDialect()
    tree = sqlglot.parse_one("ABS(x)", dialect="postgres")
    out = d.rewrite_target_ast(tree).sql(dialect="postgres").upper()
    assert "CAST(" not in out


def test_postgres_rewrite_target_ast_idempotent() -> None:
    d = PostgresDialect()
    once = d.rewrite_target_ast(sqlglot.parse_one("ROUND(x, 2)", dialect="postgres"))
    twice = d.rewrite_target_ast(once)
    assert once.sql(dialect="postgres") == twice.sql(dialect="postgres")


def test_postgres_rewrite_target_ast_casts_round_over_expression() -> None:
    d = PostgresDialect()
    tree = sqlglot.parse_one("ROUND(SUM(amount) / 7, 2)", dialect="postgres")
    out = d.rewrite_target_ast(tree).sql(dialect="postgres").upper()
    assert "ROUND(CAST(" in out


def test_postgres_rewrite_target_ast_preserves_json_extract() -> None:
    # The new hook must only touch ROUND — leave everything else alone.
    d = PostgresDialect()
    tree = sqlglot.parse_one("SELECT json_extract(j, '$.k') FROM t", dialect="postgres")
    before = tree.sql(dialect="postgres")
    after = d.rewrite_target_ast(tree).sql(dialect="postgres")
    assert before == after
