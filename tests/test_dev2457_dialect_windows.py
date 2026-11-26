"""Per-database window rewrites, by emission (the executed matrix runs in the ClickHouse and SQL Server workflows).

Spec: queries/window-functions › Every database runs the required windows or refuses with a typed error.
"""

from __future__ import annotations

import math

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.errors import QueryTypeError
from slayer.core.query import SlayerQuery
from slayer.sql.dialects.sqlite import _StddevPopAgg, _VarPopAgg
from tests._dev2101_fixtures import emit
from tests._dev2457_fixtures import col, customers_dataset, orders_dataset, raw, window_datasets


async def _windows(dialect: str, expression: str) -> list[exp.Window]:
    sql = await emit(SlayerQuery.model_validate(raw("orders", expression)), dialect=dialect, datasets=window_datasets())
    return list(sqlglot.parse_one(sql, dialect=dialect).find_all(exp.Window))


async def test_clickhouse_cume_dist_is_two_counts() -> None:
    windows = await _windows("clickhouse", "CUME_DIST() OVER (ORDER BY orders.amount)")
    assert not any(isinstance(w.this, exp.CumeDist) for w in windows)
    counts = [w for w in windows if isinstance(w.this, exp.Count)]
    assert {w.args["spec"].args["kind"] if w.args.get("spec") else None for w in counts} == {"RANGE", None}


async def test_clickhouse_count_of_a_column_sums_its_non_null_flags() -> None:
    (window,) = await _windows("clickhouse", "COUNT(orders.amount) OVER (PARTITION BY orders.region)")
    assert isinstance(window.this, exp.Sum)


async def test_clickhouse_sample_statistic_over_one_row_is_null() -> None:
    sql = await emit(SlayerQuery.model_validate(raw("orders", "STDDEV(orders.amount) OVER (PARTITION BY orders.region)")),
                     dialect="clickhouse", datasets=window_datasets())
    assert "isNaN" in sql


async def test_clickhouse_offsets_read_the_whole_partition() -> None:
    (window,) = await _windows("clickhouse", "LEAD(orders.amount, 1, 0) OVER (ORDER BY orders.amount)")
    spec = window.args["spec"]
    assert (spec.args["kind"], spec.args["start"], spec.args["end"], spec.args["end_side"]) == (
        "ROWS", "UNBOUNDED", "UNBOUNDED", "FOLLOWING")


@pytest.mark.parametrize("frame", ["", " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                                   " RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                                   " ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"],
                         ids=["default", "rows-running", "range-running", "centred"])
async def test_sql_server_nth_value_is_rewritten(frame: str) -> None:
    windows = await _windows("tsql", f"NTH_VALUE(orders.amount, 2) OVER (ORDER BY orders.amount{frame})")
    assert windows
    assert not any(isinstance(w.this, exp.NthValue) for w in windows)


async def test_sql_server_nth_value_over_another_frame_is_refused() -> None:
    expression = "NTH_VALUE(orders.amount, 2) OVER (ORDER BY orders.id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"
    with pytest.raises(QueryTypeError, match="SQL Server"):
        await _windows("tsql", expression)


async def test_sql_server_window_avg_reads_its_input_as_float() -> None:
    (window,) = await _windows("tsql", "AVG(orders.amount) OVER (PARTITION BY orders.region)")
    assert isinstance(window.this, exp.Avg)
    assert isinstance(window.this.this, exp.Cast)
    assert window.this.this.to.is_type(exp.DataType.Type.FLOAT)


async def test_sql_server_window_avg_keeps_a_decimal_input_exact() -> None:
    datasets = [orders_dataset(fields=[col("price", DataType.DECIMAL, expression="amount")]), customers_dataset()]
    sql = await emit(SlayerQuery.model_validate(raw("orders", "AVG(orders.price) OVER (PARTITION BY orders.region)")),
                     dialect="tsql", datasets=datasets)
    (window,) = [w for w in sqlglot.parse_one(sql, dialect="tsql").find_all(exp.Window) if isinstance(w.this, exp.Avg)]
    assert not isinstance(window.this.this, exp.Cast), sql


@pytest.mark.parametrize("body", [raw("orders", "AVG(orders.amount) OVER (PARTITION BY orders.region)"),
                                  {"population": "orders", "metrics": [{"metric": "avg(orders.amount)"}]}],
                         ids=["window", "aggregate"])
async def test_mysql_avg_reads_an_integer_input_as_double(body: dict) -> None:
    # MySQL cuts an integer AVG to div_precision_increment (4) decimals.
    sql = await emit(SlayerQuery.model_validate(body), dialect="mysql", datasets=window_datasets())
    (avg,) = sqlglot.parse_one(sql, dialect="mysql").find_all(exp.Avg)
    assert isinstance(avg.this, exp.Cast), sql
    assert avg.this.to.is_type(exp.DataType.Type.DOUBLE), sql


def test_sqlite_sliding_deviation_survives_round_off() -> None:
    """Removing rows from a frame of near-equal large values must not drive the variance state below zero."""
    agg = _StddevPopAgg()
    values = [1e12, 1e12 + 0.0002, 1e12, 1e12 + 0.0001, 1e12 + 0.0001]
    for value in values:
        agg.step(value)
    for value in values[:3]:
        agg.inverse(value)
    assert agg.value() is not None


@pytest.mark.parametrize("expression", [
    "MEDIAN(orders.amount) OVER (ORDER BY orders.id)",
    "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY orders.amount) OVER (PARTITION BY orders.region "
    "ORDER BY orders.id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
], ids=["ordered_median", "framed_percentile"])
async def test_snowflake_holistic_window_takes_only_partition_by(expression: str) -> None:
    with pytest.raises(QueryTypeError, match="Snowflake"):
        await _windows("snowflake", expression)


async def test_snowflake_partitioned_median_is_kept() -> None:
    (window,) = await _windows("snowflake", "MEDIAN(orders.amount) OVER (PARTITION BY orders.region)")
    assert window.args.get("spec") is None


@pytest.mark.parametrize("expression", ["LAST_VALUE(orders.amount) OVER (ORDER BY orders.amount)",
                                        "NTH_VALUE(orders.amount, 2) OVER (ORDER BY orders.amount)"])
async def test_snowflake_ordered_value_window_states_the_running_frame(expression: str) -> None:
    (window,) = await _windows("snowflake", expression)
    spec = window.args["spec"]
    assert (spec.args["kind"], spec.args["start"], spec.args["start_side"], spec.args["end"]) == (
        "RANGE", "UNBOUNDED", "PRECEDING", "CURRENT ROW")


def test_sqlite_sliding_variance_after_a_large_value_leaves_the_frame() -> None:
    agg = _VarPopAgg()
    for value in (1e16, 1, 2):
        agg.step(value)
    agg.inverse(1e16)
    assert agg.value() == pytest.approx(0.25)


def test_sqlite_variance_beside_an_infinite_row_is_nan() -> None:
    agg = _VarPopAgg()
    for value in (1.0, float("inf"), 2.0):
        agg.step(value)
    assert math.isnan(agg.value() or 0.0)
    agg.inverse(1.0)
    agg.inverse(float("inf"))
    assert agg.value() == 0


@pytest.mark.parametrize("frame", ["", " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                                   " ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"],
                         ids=["default", "rows-running", "centred"])
async def test_sql_server_nth_value_orders_positional_windows_by_one_row_number(frame: str) -> None:
    sql = await emit(SlayerQuery.model_validate(raw(
        "orders", f"NTH_VALUE(orders.amount, 2) OVER (PARTITION BY orders.region ORDER BY orders.ordered_at{frame})")),
        dialect="tsql", datasets=window_datasets())
    tree = sqlglot.parse_one(sql, dialect="tsql")
    (numbering,) = [w.parent for w in tree.find_all(exp.Window) if isinstance(w.this, exp.RowNumber)]
    assert isinstance(numbering, exp.Alias), sql
    positional = [w for w in tree.find_all(exp.Window) if isinstance(w.this, (exp.Lead, exp.Lag, exp.FirstValue))]
    assert positional, sql
    for window in positional:
        (item,) = window.args["order"].expressions
        assert item.this.unnest().name == numbering.alias, sql
