"""Tests for MysqlDialect."""

from __future__ import annotations

from sqlglot import exp

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.query import OrderItem, QueryMetric, SlayerQuery, TimeDimension
from slayer.sql.dialects.mysql import MysqlDialect
from slayer.sql.dialects.postgres import PostgresDialect
from slayer.sql.render.parse import parse_expression
from slayer.sql.reserved_keywords import prequote_reserved_identifiers_mapped

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


# Median / percentile: the ordered-set aggregate, split into a window subquery at render


def test_mysql_build_median_is_percentile_cont_at_half() -> None:
    median = MysqlDialect().build_median(exp.column("amount"))
    assert isinstance(median, exp.WithinGroup)
    assert median.this.this.name == "0.5"


def test_mysql_render_reads_no_ordered_set_aggregate() -> None:
    d = MysqlDialect()
    select = exp.select(d.build_percentile(p=exp.Literal.number("0.9"), col_expr=exp.column("amount"))).from_("t")
    assert not list(d.rewrite_statement(select).find_all(exp.WithinGroup))


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
    col = exp.column("created_at")
    out = d.build_date_trunc(col_expr=col, granularity=TimeGranularity.MONTH, operand=DataType.DATETIME)
    # sqlglot translates DATE_TRUNC for MySQL; we just need to confirm
    # the AST builds via DateTrunc, not a hand-rolled STRFTIME path
    assert isinstance(out, (exp.DateTrunc, exp.Func))


@pytest.mark.parametrize("granularity,fmt", [
    (TimeGranularity.HOUR, "%Y-%m-%d %H:00:00"),
    (TimeGranularity.MINUTE, "%Y-%m-%d %H:%i:00"),
    (TimeGranularity.SECOND, "%Y-%m-%d %H:%i:%s"),
])
def test_mysql_build_date_trunc_sub_day_keeps_time(granularity: TimeGranularity, fmt: str) -> None:
    """Sub-day buckets keep the time of day (sqlglot alone collapses them to ``DATE(t)``)."""
    out = MysqlDialect().build_date_trunc(col_expr=exp.column("ts"), granularity=granularity, operand=DataType.DATETIME)
    assert out.sql(dialect="mysql") == f"CAST(DATE_FORMAT(ts, '{fmt}') AS DATETIME)"


def test_mysql_build_date_trunc_week_sunday_shift() -> None:
    """WEEK_SUNDAY shifts MySQL's Monday-week truncation by +1d / -1d."""
    d = MysqlDialect()
    col = exp.column("ordered_at")
    out = d.build_date_trunc(col_expr=col, granularity=TimeGranularity.WEEK_SUNDAY, operand=DataType.DATETIME)
    up = out.sql(dialect="mysql").upper()
    assert "DATE_ADD(ORDERED_AT, INTERVAL 1 DAY)" in up
    assert "INTERVAL -1 DAY)" in up
    # Monday-based inner week truncation (mode 1 / ISO %u).
    assert "WEEK(" in up


def test_mysql_build_date_add_quarter_is_native() -> None:
    d = MysqlDialect()
    col = exp.column("created_at")
    out = d.build_date_add(
        expr=col, count=exp.Literal.number(1), unit=TimeGranularity.QUARTER, operand=DataType.DATETIME,
    )
    assert out.sql(dialect="mysql").upper() == "DATE_ADD(CREATED_AT, INTERVAL 1 QUARTER)"


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


async def _mysql_generate(query: SlayerQuery, dataset: SlayerDataset) -> str:
    """Render ``query`` for MySQL and return the full emitted SQL."""
    return await _engine_generate(query=query, dataset=dataset, dialect="mysql")


def _orders_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="orders", sql_table="orders", data_source="test",
        default_time_dimension="created_at",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="created_at", expression="created_at", datatype=DataType.DATETIME),
            DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
            DatasetField(name="total", expression="amount", datatype=DataType.FLOAT),
        ],
    )


async def test_mysql_time_shift_inner_cte_uses_backticks_not_ansi_quotes() -> None:
    """``change_pct(sum(total))`` CTEs must not embed ANSI double-quoted identifiers."""
    q = SlayerQuery(
        population="orders",
        time_dimensions=[TimeDimension(
            field="orders.created_at",
            granularity=TimeGranularity.MONTH,
            date_range=["2024-03-01", "2024-03-31"],
        )],
        metrics=[
            QueryMetric(metric="sum(orders.total)"),
            QueryMetric(metric="change_pct(sum(orders.total))", alias="pct"),
        ],
        order=[OrderItem(field="orders.created_at", direction="asc")],
    )
    sql = await _mysql_generate(q, _orders_dataset())
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
    assert "`_time_shift_inner`" in sql, (
        f"Inner computed expression should reference the time-shift "
        f"intermediate via backticks:\n{sql}"
    )
    # The self-join CTE's ON clause must use backticks on both sides.
    assert (
        "DATE_ADD(base.`orders.created_at`, INTERVAL -1 MONTH) <=> "
        "shifted__time_shift_inner.`created_at`"
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


def _monthly_orders_query() -> SlayerQuery:
    return SlayerQuery(
        population="orders",
        time_dimensions=[TimeDimension(field="orders.created_at", granularity=TimeGranularity.MONTH)],
        metrics=[QueryMetric(metric="count(orders.*)")],
        order=[OrderItem.model_validate({"field": "date_trunc('month', orders.created_at)"})],
    )


async def test_mysql_nulls_last_emulation_orders_by_the_alias() -> None:
    """``ONLY_FULL_GROUP_BY`` fails to match some inlined projections (``DATE_ADD … INTERVAL``)."""
    model = SlayerDataset(
        name="orders", sql_table="orders", data_source="test",
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                 DatasetField(name="created_at", datatype=DataType.DATETIME)],
    )
    sql = await _engine_generate(query=_monthly_orders_query(), dataset=model, dialect="mysql")
    order_clause = sql[sql.rindex("ORDER BY"):]
    assert "CASE WHEN `orders.created_at` IS NULL THEN 1 ELSE 0 END ASC" in order_clause
    assert "STR_TO_DATE" not in order_clause


def test_mysql_nulls_first_descending_is_emulated_on_the_term() -> None:
    statement = exp.select(exp.column("x").as_("a")).from_("t").order_by(
        exp.Ordered(this=exp.column("a", quoted=True), desc=True, nulls_first=True),
    )
    sql = MysqlDialect().rewrite_statement(statement).sql(dialect="mysql")
    assert sql.endswith("ORDER BY CASE WHEN `a` IS NULL THEN 1 ELSE 0 END DESC, `a` DESC")


def test_mysql_native_null_placement_is_left_alone() -> None:
    statement = exp.select(exp.column("x").as_("a")).from_("t").order_by(
        exp.Ordered(this=exp.column("a", quoted=True), desc=True, nulls_first=False),
    )
    assert MysqlDialect().rewrite_statement(statement).sql(dialect="mysql").endswith("ORDER BY `a` DESC")


# A Mode B division is true division: MySQL's ``/`` keeps only ``div_precision_increment`` extra digits.


def test_mysql_true_division_reads_the_dividend_as_a_double() -> None:
    quotient = exp.Div(this=exp.Div(this=exp.column("a"), expression=exp.column("b")), expression=exp.column("c"))
    sql = MysqlDialect().true_division(quotient).sql(dialect="mysql")
    assert sql == "CAST(a AS DOUBLE) / b / c"


def test_mysql_true_division_keeps_a_double_dividend() -> None:
    quotient = exp.Div(this=exp.Cast(this=exp.column("a"), to=exp.DataType.build("DOUBLE")), expression=exp.column("b"))
    assert MysqlDialect().true_division(quotient).sql(dialect="mysql") == "CAST(a AS DOUBLE) / b"


async def test_mysql_query_division_is_true_division() -> None:
    query = SlayerQuery.model_validate({"population": "orders", "distinct_dimension_values": False,
                                        "dimensions": ["orders.id", "orders.amount / orders.id"]})
    orders = SlayerDataset(name="orders", sql_table="orders", data_source="test", fields=[
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="amount", datatype=DataType.INTEGER)])
    sql = await _engine_generate(query=query, dataset=orders, dialect="mysql")
    assert "CAST(orders.amount AS DOUBLE) / " in sql


def test_mysql_parsed_variances_keep_mysqls_names() -> None:
    """sqlglot writes VAR_SAMP as VARIANCE, MySQL's population variance, and VAR_POP as VARIANCE_POP, no function."""
    tree = parse("SELECT VAR_SAMP(a), VAR_POP(a) FROM t", dialect="mysql")
    assert MysqlDialect().rewrite_target_ast(tree).sql(dialect="mysql") == "SELECT VAR_SAMP(a), VAR_POP(a) FROM t"


def test_mysql_variance_rename_rewrites_its_argument_too() -> None:
    tree = parse("SELECT VAR_SAMP(TRUNC(a)) FROM t", dialect="postgres")
    assert MysqlDialect().rewrite_target_ast(tree).sql(dialect="mysql") == "SELECT VAR_SAMP(TRUNCATE(a, 0)) FROM t"


@pytest.mark.parametrize(("text", "emitted"), [
    ("VARIANCE(a)", "VAR_POP(a)"), ("VAR_SAMP(a)", "VAR_SAMP(a)"), ("VAR_POP(a)", "VAR_POP(a)"),
    ("STD(a)", "STDDEV_POP(a)"), ("STDDEV(a)", "STDDEV_POP(a)"), ("STDDEV_SAMP(a)", "STDDEV_SAMP(a)"),
    ("t.variance + variance(x)", "t.variance + VAR_POP(x)"), ("'VARIANCE(x)'", "'VARIANCE(x)'"),
])
def test_mysql_text_keeps_its_statistics_meaning(text: str, emitted: str) -> None:
    mysql = MysqlDialect()
    assert parse_expression(sql=text, target_dialect=mysql, parse_dialect=mysql).sql(dialect="mysql") == emitted


@pytest.mark.parametrize(("text", "emitted"), [
    ("VARIANCE(a)", "VAR_SAMP(a)"), ("STDDEV(a)", "STDDEV_SAMP(a)"), ("VAR_POP(a)", "VAR_POP(a)"),
    ("STDDEV_POP(a)", "STDDEV_POP(a)"),
])
def test_portable_statistics_keep_their_meaning_on_mysql(text: str, emitted: str) -> None:
    tree = parse_expression(sql=text, target_dialect=MysqlDialect(), parse_dialect=PostgresDialect())
    assert tree.sql(dialect="mysql") == emitted


def test_a_respelled_statistic_maps_back_to_its_source() -> None:
    text, origin = prequote_reserved_identifiers_mapped("1 + std(a)", dialect="mysql")
    assert text == "1 + STDDEV_POP(a)"
    assert len(origin) == len(text)
    assert [origin[i] for i in (0, 4, text.index("("), text.index("a"))] == [0, 4, 7, 8]
