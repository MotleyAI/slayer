"""The SQLite ``slayer_date_add(ts, n, unit)`` UDF: calendar clamping, shapes, and NULL on bad input."""

from __future__ import annotations

from datetime import datetime

import pytest

from slayer.sql.dialects.sqlite import SqliteDialect, register_sqlite_udfs
from slayer.storage.sqlite_conn import open_connection


@pytest.fixture
def conn():
    with open_connection(":memory:") as c:
        register_sqlite_udfs(c)
        yield c


def _add(conn, ts, n, unit):
    return conn.execute("SELECT slayer_date_add(?, ?, ?)", (ts, n, unit)).fetchone()[0]


@pytest.mark.parametrize("ts,n,unit,expected", [
    ("2024-01-31", 1, "month", "2024-02-29"),
    ("2023-01-31", 1, "month", "2023-02-28"),
    ("2024-03-31", -1, "month", "2024-02-29"),
    ("2024-05-31", 1, "month", "2024-06-30"),
    ("2024-12-31", 2, "month", "2025-02-28"),
    ("2024-01-31", -13, "month", "2022-12-31"),
    ("2024-02-29", 1, "year", "2025-02-28"),
    ("2024-02-29", -4, "year", "2020-02-29"),
    ("2024-03-01", -1, "day", "2024-02-29"),
    ("2024-12-31", 1, "day", "2025-01-01"),
    ("2024-01-31 10:15:00", 1, "month", "2024-02-29 10:15:00"),
    ("2024-03-01 23:30:00", 1, "hour", "2024-03-02 00:30:00"),
    ("2024-03-01 00:00:30", -1, "minute", "2024-02-29 23:59:30"),
    ("2024-12-31 23:59:59", 1, "second", "2025-01-01 00:00:00"),
    ("2024-03-01", 2, "hour", "2024-03-01 02:00:00"),
    ("2024-03-01", -1, "second", "2024-02-29 23:59:59"),
    ("2024-01-31T10:15:00", 1, "month", "2024-02-29 10:15:00"),
])
def test_exact_text(conn, ts, n, unit, expected) -> None:
    assert _add(conn, ts, n, unit) == expected


def test_date_only_stays_date_only_for_day_or_coarser(conn) -> None:
    for unit in ("day", "month", "year"):
        assert len(_add(conn, "2024-01-15", 1, unit)) == 10, unit


def test_fractional_seconds_kept(conn) -> None:
    out = _add(conn, "2024-01-31 10:15:00.250000", 1, "month")
    assert datetime.fromisoformat(out) == datetime(2024, 2, 29, 10, 15, 0, 250000)
    out = _add(conn, "2024-03-01 10:00:00.75", 1, "hour")
    assert datetime.fromisoformat(out) == datetime(2024, 3, 1, 11, 0, 0, 750000)


@pytest.mark.parametrize("ts,n", [
    (None, 1), ("2024-01-01", None), ("not a date", 1), ("2024-02-30", 1),
    ("2024-13-01", 1), ("", 1), ("2024-01-01 25:00:00", 1), (20240101, 1),
])
def test_null_or_malformed_is_null(conn, ts, n) -> None:
    assert _add(conn, ts, n, "month") is None


def test_zero_count(conn) -> None:
    assert _add(conn, "2024-01-31", 0, "month") == "2024-01-31"


def test_registered_by_the_dialect_hook() -> None:
    with open_connection(":memory:") as fresh:
        SqliteDialect().register_udfs(fresh)
        assert _add(fresh, "2024-01-31", 1, "month") == "2024-02-29"
