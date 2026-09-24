"""Tests for ClickhouseDialect."""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from slayer.core.enums import TimeGranularity
from slayer.sql.dialects.clickhouse import ClickhouseDialect


def test_clickhouse_sqlglot_name() -> None:
    assert ClickhouseDialect().sqlglot_name == "clickhouse"


def test_clickhouse_explain_prefix() -> None:
    assert ClickhouseDialect().explain_prefix == "EXPLAIN"


def test_clickhouse_log_native_flags() -> None:
    d = ClickhouseDialect()
    assert d.should_use_native_log(10) is True
    assert d.should_use_native_log(2) is True


# Median / percentile — ClickHouse native forms


def test_clickhouse_build_median_emits_quantile_05_form() -> None:
    """``median(x)`` transpiles to ClickHouse's parametric ``quantile(0.5)(x)``."""
    d = ClickhouseDialect()
    inner = sqlglot.parse_one("amount", dialect="clickhouse")
    out = d.build_median(inner)
    sql = out.sql(dialect="clickhouse")
    assert "quantile(0.5)" in sql.lower()
    assert "PERCENTILE_CONT" not in sql.upper()
    assert "WITHIN GROUP" not in sql.upper()


def test_clickhouse_build_percentile_uses_parametric_quantile() -> None:
    """ClickHouse uses ``quantile(p)(x)`` — parametric aggregate syntax."""
    d = ClickhouseDialect()
    out = d.build_percentile(p=exp.Literal.number("0.9"), col_expr=exp.column("amount"))
    sql = out.sql(dialect="clickhouse")
    assert "quantile(" in sql.lower()
    assert "0.9" in sql
    # The parametric form is quantile(p)(x), not quantile(x, p)
    assert "quantile(0.9)" in sql.lower()


def test_clickhouse_build_percentile_preserves_literal() -> None:
    d = ClickhouseDialect()
    out = d.build_percentile(p=exp.Literal.number("0.50"), col_expr=exp.column("amount"))
    assert "0.50" in out.sql(dialect="clickhouse")


# Stat aggs — native


def test_clickhouse_build_covar_2arg_corr_native() -> None:
    d = ClickhouseDialect()
    out = d.build_covar_2arg(agg_name="corr", col_expr=exp.column("amount"), other_expr=exp.column("quantity"))
    sql = out.sql(dialect="clickhouse").upper()
    assert "CORR" in sql


def test_clickhouse_build_covar_2arg_covar_samp_native() -> None:
    d = ClickhouseDialect()
    out = d.build_covar_2arg(agg_name="covar_samp", col_expr=exp.column("amount"), other_expr=exp.column("quantity"))
    sql = out.sql(dialect="clickhouse").upper()
    assert "COVAR_SAMP" in sql or "COVAR" in sql


def test_clickhouse_build_stat_agg_1arg_stddev_samp() -> None:
    d = ClickhouseDialect()
    out = d.build_stat_agg_1arg(agg_name="stddev_samp", col_expr=exp.column("amount"))
    sql = out.sql(dialect="clickhouse").upper()
    assert "STDDEV" in sql


# Time arithmetic — INTERVAL-based (sqlglot transpiles to ClickHouse syntax)


def test_clickhouse_build_time_offset_expr_day() -> None:
    d = ClickhouseDialect()
    col = sqlglot.parse_one("created_at", dialect="clickhouse")
    out = d.build_time_offset_expr(col, offset=3, granularity="day")
    sql = out.sql(dialect="clickhouse").upper()
    assert "INTERVAL" in sql
    assert "DAY" in sql


def test_clickhouse_build_date_trunc_month() -> None:
    d = ClickhouseDialect()
    col = sqlglot.parse_one("created_at", dialect="clickhouse")
    out = d.build_date_trunc(col, TimeGranularity.MONTH)
    sql = out.sql(dialect="clickhouse").upper()
    # sqlglot transpiles to ClickHouse-appropriate date function
    assert "MONTH" in sql or "DATE_TRUNC" in sql


def test_clickhouse_build_date_trunc_week_sunday_shift() -> None:
    """WEEK_SUNDAY reuses ClickHouse's native (Monday) week truncation with the +1d / -1d shift."""
    d = ClickhouseDialect()
    col = sqlglot.parse_one("ordered_at", dialect="clickhouse")
    out = d.build_date_trunc(col, TimeGranularity.WEEK_SUNDAY)
    up = out.sql(dialect="clickhouse").upper()
    # sqlglot emits the canonical ClickHouse spelling ``dateTrunc``
    # (upper-cased here to ``DATETRUNC``). ClickHouse accepts both
    # ``DATE_TRUNC`` and ``dateTrunc`` so either is correct on the wire,
    # but sqlglot only emits one form — pin it.
    assert "DATETRUNC('WEEK'" in up
    assert "+ INTERVAL 1 DAY" in up
    assert "- INTERVAL 1 DAY" in up
