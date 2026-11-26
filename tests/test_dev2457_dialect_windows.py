"""Per-database window rewrites, by emission (the executed matrix runs in the ClickHouse and SQL Server workflows).

Spec: queries/window-functions › Every database runs the required windows or refuses with a typed error.
"""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.errors import QueryTypeError
from slayer.core.query import SlayerQuery
from slayer.sql.dialects.sqlite import _StddevPopAgg
from tests._dev2101_fixtures import emit
from tests._dev2457_fixtures import raw, window_datasets


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


def test_sqlite_sliding_deviation_survives_round_off() -> None:
    """Removing rows from a frame of near-equal large values must not drive the variance state below zero."""
    agg = _StddevPopAgg()
    values = [1e12, 1e12 + 0.0002, 1e12, 1e12 + 0.0001, 1e12 + 0.0001]
    for value in values:
        agg.step(value)
    for value in values[:3]:
        agg.inverse(value)
    assert agg.value() is not None
