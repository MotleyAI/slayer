"""Tests for MysqlDialect."""

from __future__ import annotations

import sqlglot
from sqlglot import exp

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.sql.dialects.mysql import MysqlDialect

from tests._dev1965_fixtures import assert_single_top_level_with, cumsum_chain, gen, parse
from tests._engine_helpers import _engine_generate


def test_mysql_sqlglot_name() -> None:
    assert MysqlDialect().sqlglot_name == "mysql"


def test_mysql_explain_prefix() -> None:
    assert MysqlDialect().explain_prefix == "EXPLAIN FORMAT=JSON"


def test_mysql_log_native_flags() -> None:
    d = MysqlDialect()
    assert d.should_use_native_log(10) is True
    assert d.should_use_native_log(2) is True


# Median / percentile — not supported on MySQL


def test_mysql_build_median_raises_not_implemented() -> None:
    d = MysqlDialect()
    inner = sqlglot.parse_one("amount", dialect="mysql")
    with pytest.raises(NotImplementedError, match="median.*MySQL"):
        d.build_median(inner)


def test_mysql_build_percentile_raises_not_implemented() -> None:
    d = MysqlDialect()
    p = exp.Literal.number("0.5")
    col_expr = exp.column("amount")
    with pytest.raises(NotImplementedError, match="percentile.*MySQL"):
        d.build_percentile(p=p, col_expr=col_expr)


# stat aggs: var_samp / var_pop via Anonymous (sqlglot mis-renames otherwise)


def test_mysql_build_stat_agg_1arg_var_samp_uses_anonymous() -> None:
    """sqlglot rewrites VAR_SAMP to VARIANCE (VAR_POP on MySQL), so Anonymous is used."""
    d = MysqlDialect()
    out = d.build_stat_agg_1arg(agg_name="var_samp", col_expr=exp.column("amount"))
    sql = out.sql(dialect="mysql").upper()
    assert "VAR_SAMP" in sql
    assert "VARIANCE" not in sql


def test_mysql_build_stat_agg_1arg_var_pop_uses_anonymous() -> None:
    d = MysqlDialect()
    out = d.build_stat_agg_1arg(agg_name="var_pop", col_expr=exp.column("amount"))
    sql = out.sql(dialect="mysql").upper()
    assert "VAR_POP" in sql
    assert "VARIANCE" not in sql


def test_mysql_build_stat_agg_1arg_stddev_samp_native() -> None:
    """STDDEV_SAMP is native on MySQL — no Anonymous workaround needed."""
    d = MysqlDialect()
    out = d.build_stat_agg_1arg(agg_name="stddev_samp", col_expr=exp.column("amount"))
    sql = out.sql(dialect="mysql").upper()
    assert "STDDEV_SAMP" in sql or "STDDEV(" in sql


# Covariance via variance-decomposition formula


def test_mysql_build_covar_2arg_corr_uses_decomposition_formula() -> None:
    d = MysqlDialect()
    out = d.build_covar_2arg(agg_name="corr", col_expr=exp.column("amount"), other_expr=exp.column("quantity"))
    sql = out.sql(dialect="mysql").upper()
    # Variance-decomposition uses VAR_SAMP for corr/covar_samp
    assert "VAR_SAMP" in sql
    # Plus a NULLIF guard against zero denominator (correlation only)
    assert "NULLIF" in sql
    # Plus STDDEV (denominator of correlation)
    assert "STDDEV" in sql


def test_mysql_build_covar_2arg_covar_samp_uses_decomposition() -> None:
    d = MysqlDialect()
    out = d.build_covar_2arg(agg_name="covar_samp", col_expr=exp.column("amount"), other_expr=exp.column("quantity"))
    sql = out.sql(dialect="mysql").upper()
    assert "VAR_SAMP" in sql
    # covariance doesn't divide by stddev product — no NULLIF needed
    assert "NULLIF" not in sql


def test_mysql_build_covar_2arg_covar_pop_uses_pop_variance() -> None:
    d = MysqlDialect()
    out = d.build_covar_2arg(agg_name="covar_pop", col_expr=exp.column("amount"), other_expr=exp.column("quantity"))
    sql = out.sql(dialect="mysql").upper()
    assert "VAR_POP" in sql


def test_mysql_build_covar_2arg_excludes_null_pairs() -> None:
    """The formula NULL-guards each column against the other."""
    d = MysqlDialect()
    out = d.build_covar_2arg(agg_name="corr", col_expr=exp.column("amount"), other_expr=exp.column("quantity"))
    sql = out.sql(dialect="mysql").upper()
    assert "IS NULL" in sql
    assert "NOT" in sql
    assert "CASE" in sql


# Date / time — MySQL uses Postgres-shaped INTERVAL


def test_mysql_build_date_trunc_emits_date_trunc() -> None:
    d = MysqlDialect()
    col = sqlglot.parse_one("created_at", dialect="mysql")
    out = d.build_date_trunc(col, TimeGranularity.MONTH)
    # sqlglot translates DATE_TRUNC for MySQL; we just need to confirm
    # the AST builds via DateTrunc, not a hand-rolled STRFTIME path
    assert isinstance(out, (exp.DateTrunc, exp.Func))


def test_mysql_build_date_trunc_week_sunday_shift() -> None:
    """WEEK_SUNDAY shifts MySQL's Monday-week truncation by +1d / -1d."""
    d = MysqlDialect()
    col = sqlglot.parse_one("ordered_at", dialect="mysql")
    out = d.build_date_trunc(col, TimeGranularity.WEEK_SUNDAY)
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


# Outer wrap: MySQL parses ``"..."`` as a string literal, so public aliases are backticked.


async def test_mysql_outer_wrap_uses_backticks_for_aliases() -> None:
    """Outer projection list emits backticked identifiers on MySQL, never ANSI double quotes."""
    top = parse(await gen(cumsum_chain(), dialect="mysql"), "mysql")
    projected = [e.sql(dialect="mysql") for e in top.expressions]
    assert "`orders.created_at`" in projected, projected
    assert not [p for p in projected if '"' in p], projected


async def test_mysql_outer_wrap_hoists_the_chain_with() -> None:
    """One top-level WITH on every dialect, MySQL included."""
    assert_single_top_level_with(await gen(cumsum_chain(), dialect="mysql"), "mysql")


# Inner CTE assembly emits dialect-aware quotes.
#
# The original Bug 3 description only mentioned the outer wrap. Reality:
# `_assemble_combined_sql`, `_generate_with_computed`, and the time-shift
# self-join CTE builders ALSO hardcoded ANSI double quotes for identifier
# references. On MySQL those parse as string literals and crash, then
# sqlglot canonicalises the broken result. End-to-end regression coverage
# lives in tests/integration/test_integration_mysql.py; these are the
# fast unit-level pins.


async def _mysql_generate(query: SlayerQuery, model: SlayerModel) -> str:
    """Render ``query`` for MySQL and return the full emitted SQL."""
    return await _engine_generate(query=query, model=model, dialect="mysql")


def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="total", sql="amount", type=DataType.DOUBLE),
        ],
    )


async def test_mysql_time_shift_inner_cte_uses_backticks_not_ansi_quotes() -> None:
    """``change_pct(total:sum)`` CTEs must not embed ANSI double-quoted identifiers."""
    q = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
            date_range=["2024-03-01", "2024-03-31"],
        )],
        measures=[
            ModelMeasure(formula="total:sum"),
            ModelMeasure(formula="change_pct(total:sum)", name="pct"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    sql = await _mysql_generate(q, _orders_model())
    # No ANSI-quoted identifiers ANYWHERE — those would be MySQL string
    # literals and either fail SQL parsing or silently corrupt results.
    assert '"orders.' not in sql, (
        f'MySQL emission must not contain ANSI-quoted identifiers '
        f'(MySQL would parse them as string literals):\n{sql}'
    )
    # The computed change_pct expression's column refs must be backticked.
    # (The typed pipeline names the shifted intermediate
    # ``orders._time_shift_inner``; the legacy stack spelled it
    # ``orders._ts_pct``. Same slot, same assertion.)
    assert "`orders._time_shift_inner`" in sql, (
        f"Inner computed expression should reference the time-shift "
        f"intermediate via backticks:\n{sql}"
    )
    # The self-join CTE's ON clause must use backticks on both sides.
    assert (
        "base.`orders.created_at` - INTERVAL 1 MONTH <=> "
        "shifted__time_shift_inner.`orders.created_at`"
    ) in sql, (
        f"Self-join ON clause must use backticked identifiers:\n{sql}"
    )
    # The outer ORDER BY must reference a backticked identifier, not a
    # single-quoted string literal (which is what sqlglot emits when it
    # re-parses an ANSI-quoted alias under MySQL dialect).
    #
    # Asserted over the ORDER BY *clause* rather than the text immediately
    # after the keyword: MySQL has no NULLS syntax, so the term is preceded by
    # sqlglot's ``CASE WHEN <col> IS NULL …`` emulation of the nulls-last
    # ordering every dialect gets. Which term comes first is not this test's
    # subject — how the alias is quoted is.
    order_clause = sql[sql.rindex("ORDER BY"):]
    assert "`orders.created_at`" in order_clause, (
        f"ORDER BY must reference a backticked alias, not a string literal:\n{sql}"
    )
    assert "'orders.created_at'" not in order_clause, (
        f"sqlglot re-parsed an ANSI-quoted identifier as a string literal:\n{sql}"
    )
