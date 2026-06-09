"""DEV-1542: tests for ClickhouseDialect.

ClickHouse uses native ``median(x)`` (not PERCENTILE_CONT) and the
parametric ``quantile(p)(x)`` form for percentile. CORR / COVAR_SAMP /
COVAR_POP are native; log10 is native, log2 is also native.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from slayer.core.enums import TimeGranularity
from slayer.sql.dialects.clickhouse import ClickhouseDialect


def _parse_ch(sql: str) -> exp.Expression:
    return sqlglot.parse_one(sql, dialect="clickhouse")


def test_clickhouse_sqlglot_name() -> None:
    assert ClickhouseDialect().sqlglot_name == "clickhouse"


def test_clickhouse_explain_prefix() -> None:
    assert ClickhouseDialect().explain_prefix == "EXPLAIN"


def test_clickhouse_log_native_flags() -> None:
    d = ClickhouseDialect()
    assert d.should_use_native_log(10) is True
    assert d.should_use_native_log(2) is True


# ---------------------------------------------------------------------------
# Median / percentile — ClickHouse native forms
# ---------------------------------------------------------------------------


def test_clickhouse_build_median_emits_quantile_05_form() -> None:
    """``build_median`` parses ``median(x)`` into ``exp.Median``; sqlglot's
    ClickHouse generator transpiles that to ``quantile(0.5)(x)`` —
    parametric aggregate syntax."""
    d = ClickhouseDialect()
    inner = sqlglot.parse_one("amount", dialect="clickhouse")
    out = d.build_median(inner, parse=_parse_ch)
    sql = out.sql(dialect="clickhouse")
    assert "quantile(0.5)" in sql.lower()
    assert "PERCENTILE_CONT" not in sql.upper()
    assert "WITHIN GROUP" not in sql.upper()


def test_clickhouse_build_percentile_uses_parametric_quantile() -> None:
    """ClickHouse uses ``quantile(p)(x)`` — parametric aggregate syntax."""
    d = ClickhouseDialect()
    out = d.build_percentile("0.9", "amount", parse=_parse_ch)
    sql = out.sql(dialect="clickhouse")
    assert "quantile(" in sql.lower()
    assert "0.9" in sql
    # The parametric form is quantile(p)(x), not quantile(x, p)
    assert "quantile(0.9)" in sql.lower()


def test_clickhouse_build_percentile_preserves_literal() -> None:
    d = ClickhouseDialect()
    out = d.build_percentile("0.50", "amount", parse=_parse_ch)
    assert "0.50" in out.sql(dialect="clickhouse")


# ---------------------------------------------------------------------------
# Stat aggs — native
# ---------------------------------------------------------------------------


def test_clickhouse_build_covar_2arg_corr_native() -> None:
    d = ClickhouseDialect()
    out = d.build_covar_2arg("corr", "amount", "quantity", parse=_parse_ch)
    sql = out.sql(dialect="clickhouse").upper()
    assert "CORR" in sql


def test_clickhouse_build_covar_2arg_covar_samp_native() -> None:
    d = ClickhouseDialect()
    out = d.build_covar_2arg("covar_samp", "amount", "quantity", parse=_parse_ch)
    sql = out.sql(dialect="clickhouse").upper()
    assert "COVAR_SAMP" in sql or "COVAR" in sql


def test_clickhouse_build_stat_agg_1arg_stddev_samp() -> None:
    d = ClickhouseDialect()
    out = d.build_stat_agg_1arg("stddev_samp", "amount", parse=_parse_ch)
    sql = out.sql(dialect="clickhouse").upper()
    assert "STDDEV" in sql


# ---------------------------------------------------------------------------
# Time arithmetic — INTERVAL-based (sqlglot transpiles to ClickHouse syntax)
# ---------------------------------------------------------------------------


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
    out = d.build_date_trunc(col, TimeGranularity.MONTH, parse=_parse_ch)
    sql = out.sql(dialect="clickhouse").upper()
    # sqlglot transpiles to ClickHouse-appropriate date function
    assert "MONTH" in sql or "DATE_TRUNC" in sql
