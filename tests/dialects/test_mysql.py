"""DEV-1542: tests for MysqlDialect.

MySQL has no native PERCENTILE_CONT (raises ``NotImplementedError`` on
both ``build_median`` and ``build_percentile``) and no native CORR /
COVAR_SAMP / COVAR_POP (uses the variance-decomposition formula). It
also needs the ``exp.Anonymous`` workaround for ``var_samp`` / ``var_pop``
because sqlglot mis-renames those to ``VARIANCE``.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

import pytest

from slayer.core.enums import TimeGranularity
from slayer.sql.dialects.mysql import MysqlDialect


def _parse_mysql(sql: str) -> exp.Expression:
    return sqlglot.parse_one(sql, dialect="mysql")


def test_mysql_sqlglot_name() -> None:
    assert MysqlDialect().sqlglot_name == "mysql"


def test_mysql_explain_prefix() -> None:
    assert MysqlDialect().explain_prefix == "EXPLAIN FORMAT=JSON"


def test_mysql_log_native_flags() -> None:
    d = MysqlDialect()
    assert d.should_use_native_log(10) is True
    assert d.should_use_native_log(2) is True


# ---------------------------------------------------------------------------
# Median / percentile — not supported on MySQL
# ---------------------------------------------------------------------------


def test_mysql_build_median_raises_not_implemented() -> None:
    d = MysqlDialect()
    inner = sqlglot.parse_one("amount", dialect="mysql")
    with pytest.raises(NotImplementedError, match="median.*MySQL"):
        d.build_median(inner, parse=_parse_mysql)


def test_mysql_build_percentile_raises_not_implemented() -> None:
    d = MysqlDialect()
    with pytest.raises(NotImplementedError, match="percentile.*MySQL"):
        d.build_percentile("0.5", "amount", parse=_parse_mysql)


# ---------------------------------------------------------------------------
# stat aggs: var_samp / var_pop via Anonymous (sqlglot mis-renames otherwise)
# ---------------------------------------------------------------------------


def test_mysql_build_stat_agg_1arg_var_samp_uses_anonymous() -> None:
    """sqlglot's MySQL transpiler rewrites VAR_SAMP → VARIANCE (which on
    MySQL is actually VAR_POP — silently wrong). The dialect override
    emits the canonical MySQL name via ``exp.Anonymous`` to bypass
    sqlglot's rewrite."""
    d = MysqlDialect()
    out = d.build_stat_agg_1arg("var_samp", "amount", parse=_parse_mysql)
    sql = out.sql(dialect="mysql").upper()
    assert "VAR_SAMP" in sql
    assert "VARIANCE" not in sql


def test_mysql_build_stat_agg_1arg_var_pop_uses_anonymous() -> None:
    d = MysqlDialect()
    out = d.build_stat_agg_1arg("var_pop", "amount", parse=_parse_mysql)
    sql = out.sql(dialect="mysql").upper()
    assert "VAR_POP" in sql
    assert "VARIANCE" not in sql


def test_mysql_build_stat_agg_1arg_stddev_samp_native() -> None:
    """STDDEV_SAMP is native on MySQL — no Anonymous workaround needed."""
    d = MysqlDialect()
    out = d.build_stat_agg_1arg("stddev_samp", "amount", parse=_parse_mysql)
    sql = out.sql(dialect="mysql").upper()
    assert "STDDEV_SAMP" in sql or "STDDEV(" in sql


# ---------------------------------------------------------------------------
# Covariance via variance-decomposition formula
# ---------------------------------------------------------------------------


def test_mysql_build_covar_2arg_corr_uses_decomposition_formula() -> None:
    d = MysqlDialect()
    out = d.build_covar_2arg("corr", "amount", "quantity", parse=_parse_mysql)
    sql = out.sql(dialect="mysql").upper()
    # Variance-decomposition uses VAR_SAMP for corr/covar_samp
    assert "VAR_SAMP" in sql
    # Plus a NULLIF guard against zero denominator (correlation only)
    assert "NULLIF" in sql
    # Plus STDDEV (denominator of correlation)
    assert "STDDEV" in sql


def test_mysql_build_covar_2arg_covar_samp_uses_decomposition() -> None:
    d = MysqlDialect()
    out = d.build_covar_2arg("covar_samp", "amount", "quantity", parse=_parse_mysql)
    sql = out.sql(dialect="mysql").upper()
    assert "VAR_SAMP" in sql
    # covariance doesn't divide by stddev product — no NULLIF needed
    assert "NULLIF" not in sql


def test_mysql_build_covar_2arg_covar_pop_uses_pop_variance() -> None:
    d = MysqlDialect()
    out = d.build_covar_2arg("covar_pop", "amount", "quantity", parse=_parse_mysql)
    sql = out.sql(dialect="mysql").upper()
    assert "VAR_POP" in sql


def test_mysql_build_covar_2arg_excludes_null_pairs() -> None:
    """The formula must NULL-guard each column against the other so rows
    where either is NULL are excluded uniformly (matches today's
    ``_build_covar_formula`` in ``generator.py``).

    sqlglot's MySQL emit spells this as ``NOT (col) IS NULL`` rather than
    ``col IS NOT NULL`` — both forms are semantically identical.
    """
    d = MysqlDialect()
    out = d.build_covar_2arg("corr", "amount", "quantity", parse=_parse_mysql)
    sql = out.sql(dialect="mysql").upper()
    assert "IS NULL" in sql
    assert "NOT" in sql
    assert "CASE" in sql


# ---------------------------------------------------------------------------
# Date / time — MySQL uses Postgres-shaped INTERVAL
# ---------------------------------------------------------------------------


def test_mysql_build_date_trunc_emits_date_trunc() -> None:
    d = MysqlDialect()
    col = sqlglot.parse_one("created_at", dialect="mysql")
    out = d.build_date_trunc(col, TimeGranularity.MONTH, parse=_parse_mysql)
    # sqlglot translates DATE_TRUNC for MySQL; we just need to confirm
    # the AST builds via DateTrunc, not a hand-rolled STRFTIME path
    assert isinstance(out, (exp.DateTrunc, exp.Func))


def test_mysql_build_date_trunc_week_sunday_shift() -> None:
    """DEV-1572: WEEK_SUNDAY reuses MySQL's Monday-based week truncation
    (sqlglot emits ``WEEK(x, 1)`` / ``%u``) with the +1d / -1d shift."""
    d = MysqlDialect()
    col = sqlglot.parse_one("ordered_at", dialect="mysql")
    out = d.build_date_trunc(col, TimeGranularity.WEEK_SUNDAY, parse=_parse_mysql)
    up = out.sql(dialect="mysql").upper()
    assert "+ INTERVAL 1 DAY" in up
    assert "- INTERVAL 1 DAY" in up
    # Monday-based inner week truncation (mode 1 / ISO %u).
    assert "WEEK(" in up


def test_mysql_build_time_offset_expr_quarter_normalizes_to_3_month() -> None:
    d = MysqlDialect()
    col = sqlglot.parse_one("created_at", dialect="mysql")
    out = d.build_time_offset_expr(col, offset=1, granularity="quarter")
    sql = out.sql(dialect="mysql").upper()
    assert "INTERVAL" in sql
    assert "MONTH" in sql
    assert "3" in sql
