"""Time-point bounds, date_range bounds and granularity calls render on every Tier-1 dialect."""

from __future__ import annotations

import re
from datetime import date, datetime

import pytest

from sqlglot import exp

from slayer.core.enums import DataType, TimeGranularity
from slayer.sql.dialects import get_dialect

from tests._engine_helpers import _engine_generate
from tests._time_points_fixtures import PinnedClock, customers_model, ev_model

TIER1 = ["sqlite", "postgres", "duckdb", "mysql", "clickhouse", "tsql", "snowflake", "bigquery"]
TYPED = [d for d in TIER1 if d not in ("sqlite", "bigquery")]
STRFTIME = "STRFTIME('%Y-%m-%d %H:%M:%f', "


async def _sql(
    dialect: str, *, filters: list[str], date_range=None, granularity: str = "month", column: str = "ts",
    measures: list | None = None,
) -> str:
    td: dict = {"dimension": column, "granularity": granularity}
    if date_range is not None:
        td["date_range"] = date_range
    query = {
        "source_model": "ev", "measures": measures or [{"formula": "count(*)", "name": "n"}],
        "time_dimensions": [td], "filters": filters,
    }
    return await _engine_generate(
        query=[query], model=ev_model(), extra_models=[customers_model()], dialect=dialect, clock=PinnedClock(),
    )


def _literal(dialect: str, value: date) -> str:
    dt = DataType.TIMESTAMP if isinstance(value, datetime) else DataType.DATE
    d = get_dialect(dialect)
    return d.build_temporal_literal(value=value, dt=dt).sql(dialect=d.sqlglot_name)


def _flat(sql: str) -> str:
    return re.sub(r"\s+", " ", sql)


@pytest.mark.parametrize("dialect", TIER1)
async def test_lowered_bounds_render(dialect) -> None:
    sql = await _sql(dialect, date_range="2025-Q1", filters=["ts > 'last 6 hours'", "month(ts) = month(shipped_at)"])
    assert "2025-01-01" in sql, sql
    assert "2025-04-01" in sql, sql
    assert "2026-09-29 12:00:00" in sql, sql  # > last 6 hours ⇔ >= next_start
    assert "last 6 hours" not in sql, sql
    assert "2025-Q1" not in sql, sql
    assert "BETWEEN" not in sql.upper(), sql
    assert "shipped_at" in sql, sql


@pytest.mark.parametrize("dialect", TYPED)
async def test_typed_dialects_compare_against_build_temporal_literal(dialect) -> None:
    sql = _flat(await _sql(dialect, date_range="2025-Q1", filters=["ts > 'last 6 hours'"]))
    for value in (datetime(2025, 1, 1), datetime(2025, 4, 1), datetime(2026, 9, 29, 12)):
        assert _flat(_literal(dialect, value)) in sql, (value, sql)


@pytest.mark.parametrize("dialect", TYPED)
async def test_date_operand_gets_a_date_literal(dialect) -> None:
    sql = _flat(await _sql(dialect, column="d", granularity="day", date_range="2025-Q1", filters=[]))
    assert _flat(_literal(dialect, date(2025, 1, 1))) in sql, sql
    assert _flat(_literal(dialect, date(2025, 4, 1))) in sql, sql


@pytest.mark.parametrize("dialect", TIER1)
async def test_one_sided_range_renders_a_single_bound(dialect) -> None:
    sql = await _sql(dialect, date_range=["2024-01-01", None], filters=[])
    assert "2024-01-01" in sql, sql
    assert "NULL" not in sql.upper(), sql


@pytest.mark.parametrize("dialect", TIER1)
async def test_granularity_call_non_literal_comparison_truncates_both_sides(dialect) -> None:
    sql = _flat(await _sql(dialect, filters=["month(ts) = month(shipped_at)"], granularity="day"))
    d = get_dialect(dialect)
    for col in ("ts", "shipped_at"):
        trunc = d.build_date_trunc(exp.column(col, table="ev"), TimeGranularity.MONTH).sql(dialect=d.sqlglot_name)
        assert _flat(trunc) in sql, (col, sql)


@pytest.mark.parametrize("dialect", TIER1)
async def test_granularity_call_literal_comparison_is_a_raw_column_bound(dialect) -> None:
    sql = await _sql(dialect, filters=["month(ts) >= '2024-03-15'"], granularity="day")
    assert "2024-04-01" in sql, sql
    assert "2024-03-15" not in sql, sql


@pytest.mark.parametrize(("filt", "measures"), [
    ("ts >= '2025-Q1'", None),
    ("date_add(ts, 1, 'day') >= '2025-Q1'", None),
    ("date_add(d, 1, 'day') >= '2025-Q1'", None),
    ("date_add(d, 2, 'hour') >= '2025-Q1'", None),
    ("month(ts) >= '2025-Q1'", None),
    ("now() >= '2025-Q1'", None),
    ("max(ts) >= '2025-Q1'", [{"formula": "count(*)", "name": "n"}]),
], ids=["column", "timestamp_add", "date_add", "datetime_add", "trunc", "now", "aggregate"])
async def test_bigquery_bounds_are_plain_iso_text(filt, measures) -> None:
    sql = _flat(await _sql("bigquery", filters=[filt], measures=measures))
    assert re.search(r">=\s*'2025-01-01( 00:00:00)?'", sql), sql
    assert "CAST('2025-01-01" not in sql, sql
    assert "TIMESTAMP('2025-01-01" not in sql, sql


async def test_sqlite_midnight_half_open_bounds_stay_plain_text() -> None:
    sql = _flat(await _sql("sqlite", date_range="2025-Q1", filters=["ts < '2025-03'"]))
    assert re.search(r"\bts\s*>=\s*'2025-01-01'", sql), sql
    assert re.search(r"\bts\s*<\s*'2025-04-01'", sql), sql
    assert re.search(r"\bts\s*<\s*'2025-03-01'", sql), sql
    assert "STRFTIME('%Y-%m-%d %H:%M:%f', '2025-" not in sql, sql


@pytest.mark.parametrize(("filt", "op", "value"), [
    ("ts >= '2025-03-01 10:00:00'", ">=", "2025-03-01 10:00:00"),
    ("'2025-03-01 10:00:00' <= ts", ">=", "2025-03-01 10:00:00"),
    ("ts > 'last 6 hours'", ">=", "2026-09-29 12:00:00"),
    ("ts <= '2025-03-01 10:00:00'", "<=", "2025-03-01 10:00:00"),
    ("ts = '2024-06-01 00:00:00'", "=", "2024-06-01 00:00:00"),
], ids=["sub-day", "literal-left", "relative", "inclusive-instant", "instant-equality"])
async def test_sqlite_other_comparisons_normalise_both_sides(filt, op, value) -> None:
    sql = _flat(await _sql("sqlite", filters=[filt]))
    pattern = re.escape(STRFTIME) + r"[^()]*\bts\)\s*" + re.escape(op) + r"\s*" + re.escape(f"{STRFTIME}'{value}')")
    assert re.search(pattern, sql), sql
