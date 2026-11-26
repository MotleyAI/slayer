"""Tests for ClickhouseDialect."""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.query import QueryMetric, SlayerQuery, TimeDimension
from slayer.sql.dialects.base import cast_target
from slayer.sql.dialects.clickhouse import ClickhouseDialect

from tests._engine_helpers import _engine_generate


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


def test_clickhouse_build_date_add_day() -> None:
    d = ClickhouseDialect()
    col = exp.column("created_at")
    out = d.build_date_add(
        expr=col, count=exp.Literal.number(3), unit=TimeGranularity.DAY, operand=DataType.DATETIME,
    )
    assert out.sql(dialect="clickhouse") == "addDays(created_at, 3)"


def test_clickhouse_build_date_trunc_month() -> None:
    d = ClickhouseDialect()
    col = exp.column("created_at")
    out = d.build_date_trunc(col_expr=col, granularity=TimeGranularity.MONTH, operand=DataType.DATETIME)
    sql = out.sql(dialect="clickhouse").upper()
    # sqlglot transpiles to ClickHouse-appropriate date function
    assert "MONTH" in sql or "DATE_TRUNC" in sql


def test_clickhouse_build_date_trunc_week_sunday_shift() -> None:
    """WEEK_SUNDAY reuses ClickHouse's native (Monday) week truncation with the +1d / -1d shift."""
    d = ClickhouseDialect()
    col = exp.column("ordered_at")
    out = d.build_date_trunc(col_expr=col, granularity=TimeGranularity.WEEK_SUNDAY, operand=DataType.DATETIME)
    up = out.sql(dialect="clickhouse").upper()
    # sqlglot emits the canonical ClickHouse spelling ``dateTrunc``
    # (upper-cased here to ``DATETRUNC``). ClickHouse accepts both
    # ``DATE_TRUNC`` and ``dateTrunc`` so either is correct on the wire,
    # but sqlglot only emits one form — pin it.
    assert "DATETRUNC('WEEK'" in up
    assert "ADDDAYS(ORDERED_AT, 1)" in up
    assert up.endswith(", -1)")


async def test_clickhouse_orders_by_the_projection_not_its_dotted_alias() -> None:
    """ClickHouse resolves the dotted alias ``orders.created_at`` to the source column, not the bucket."""
    model = SlayerDataset(
        name="orders", sql_table="orders", data_source="test",
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                 DatasetField(name="created_at", datatype=DataType.DATETIME)],
    )
    query = SlayerQuery(
        population="orders",
        time_dimensions=[TimeDimension(field="orders.created_at", granularity=TimeGranularity.MONTH)],
        metrics=[QueryMetric(metric="count(orders.*)")],
    )
    sql = await _engine_generate(query=query, dataset=model, dialect="clickhouse")
    assert sql[sql.rindex("ORDER BY"):].split("\n", 1)[1].strip() == (
        "toDateTime64(dateTrunc('MONTH', toDateTime64(orders.created_at, 6)), 6) ASC"
    )


def test_clickhouse_windowed_projection_keeps_its_alias() -> None:
    window = exp.Window(this=exp.func("SUM", exp.column("x")), order=exp.Order(expressions=[exp.column("x")]))
    statement = exp.select(window.as_("w")).from_("t").order_by(exp.column("w", quoted=True))
    assert ClickhouseDialect().rewrite_statement(statement).sql(dialect="clickhouse").endswith('ORDER BY "w"')


# Layer 1 meanings on ClickHouse


@pytest.mark.parametrize("author", ["TIME", "TIME WITH TIME ZONE"])
def test_clickhouse_refuses_a_time_of_day_cast(author: str) -> None:
    """No time-of-day type: the cast would read NULL."""
    dialect = ClickhouseDialect()
    with pytest.raises(ValueError, match="cannot be expressed on clickhouse"):
        cast_target(author, dialect=dialect)


def _parsed(sql: str) -> Expression:
    tree = sqlglot.parse_one(sql)
    assert isinstance(tree, Expression)
    return tree


@pytest.mark.parametrize(("value", "digits", "sql"), [
    ("2.5", 0, "ROUND(toDecimal128OrNull('2.5', 1), 0)"),
    ("-2.5", 0, "ROUND(toDecimal128OrNull('-2.5', 1), 0)"),
    ("0.125", 2, "ROUND(toDecimal128OrNull('0.125', 3), 2)"),
])
def test_clickhouse_rounds_a_number_literal_tie_away_from_zero(value: str, digits: int, sql: str) -> None:
    """A number literal is exact; read as a Float64, ``round`` would break its tie to even."""
    rounded = ClickhouseDialect().build_round(_parsed(value), exp.Literal.number(digits))
    assert rounded is not None
    assert rounded.sql(dialect="clickhouse") == sql


@pytest.mark.parametrize("digits", ["-1", "orders.d"])
def test_clickhouse_keeps_its_own_round_otherwise(digits: str) -> None:
    assert ClickhouseDialect().build_round(_parsed("2.5"), _parsed(digits)) is None


@pytest.mark.parametrize(("value", "digits"), [("1", 37), ("1", 38), ("123.5", 35)])
def test_clickhouse_keeps_its_own_round_past_decimal128(value: str, digits: int) -> None:
    """The guard digit needs ``integer digits + digits + 1`` ≤ 38: past it the Decimal128 read is NULL."""
    assert ClickhouseDialect().build_round(_parsed(value), exp.Literal.number(digits)) is None


@pytest.mark.parametrize(("value", "digits"), [("1", 36), ("0.5", 37), ("123.5", 34)])
def test_clickhouse_rounds_exactly_up_to_decimal128(value: str, digits: int) -> None:
    assert ClickhouseDialect().build_round(_parsed(value), exp.Literal.number(digits)) is not None


def test_clickhouse_number_cast_to_boolean_is_a_bool() -> None:
    """A comparison yields UInt8; a boolean cast reads Bool, like ClickHouse's own TRUE."""
    sql = ClickhouseDialect().number_to_boolean(exp.column("qty")).sql(dialect="clickhouse")
    assert sql == "CAST(qty <> 0 AS Nullable(Bool))"
