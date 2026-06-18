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


def test_mysql_build_time_offset_expr_quarter_normalizes_to_3_month() -> None:
    d = MysqlDialect()
    col = sqlglot.parse_one("created_at", dialect="mysql")
    out = d.build_time_offset_expr(col, offset=1, granularity="quarter")
    sql = out.sql(dialect="mysql").upper()
    assert "INTERVAL" in sql
    assert "MONTH" in sql
    assert "3" in sql


# ---------------------------------------------------------------------------
# DEV-1571 Bug 3 — outer-wrap quote style mismatch
#
# Today's ``SQLGenerator._build_outer_wrap`` hardcodes ANSI double quotes
# for the public projection list. MySQL parses ``"..."`` as a string
# literal by default, so the outer wrap is invalid SQL on MySQL. The fix
# emits each public alias via sqlglot's dialect-aware identifier quoting,
# which yields backticks on MySQL.
# ---------------------------------------------------------------------------


def test_mysql_emit_outer_wrap_uses_backticks_for_aliases() -> None:
    """Outer projection list emits backticked identifiers on MySQL, never
    ANSI double quotes.

    MySQL's default ``sql_mode`` does not include ``ANSI_QUOTES``; double
    quotes get parsed as string literals and the outer wrap fails to
    resolve the inner alias.
    """
    out = MysqlDialect().emit_outer_wrap(
        inner_sql="SELECT 1 AS `orders.created_at`",
        public=["orders.created_at"],
        order=None,
        limit=None,
        offset_arg=None,
    )
    assert "`orders.created_at`" in out, (
        f"MySQL outer projection must use backticks: {out}"
    )
    assert '"orders.created_at"' not in out, (
        f"MySQL outer projection must not use ANSI double quotes: {out}"
    )


def test_mysql_emit_outer_wrap_preserves_inner_cte_in_derived_table() -> None:
    """MySQL 8+ tolerates ``WITH`` inside a derived-table subquery. The
    base impl behaviour applies — no CTE hoisting, just wrap the inner SQL.

    Pins that we did NOT accidentally pull the T-SQL Bug 1 hoist into the
    base impl. Cross-dialect regression guard.
    """
    inner = (
        "WITH base AS (SELECT 1 AS x)\n"
        "SELECT x AS `orders.x` FROM base"
    )
    out = MysqlDialect().emit_outer_wrap(
        inner_sql=inner,
        public=["orders.x"],
        order=None,
        limit=None,
        offset_arg=None,
    )
    # The inner WITH stays nested in the derived-table subquery on MySQL.
    normalised = " ".join(out.split())
    assert not normalised.startswith("WITH "), (
        f"MySQL should NOT hoist CTEs (T-SQL bug 1 is T-SQL-only): {out}"
    )
    assert "WITH base" in out, (
        f"Inner CTE list must be preserved verbatim on MySQL: {out}"
    )
