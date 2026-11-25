"""SQLite ``slayer_convert_tz``: naive or offset text in, fixed-width wall clock out, the Postgres DST rule
(spec: queries/time-zones › DST resolves by the Postgres rule; Zone conversion renders on every Tier-1 dialect)."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from typing import Any, Optional

import pytest

from slayer.sql.dialects import dialect_for_ds_type
from slayer.storage.sqlite_conn import open_connection

from tests._dev2000_fixtures import NY, TOKYO


@pytest.fixture
def con() -> Iterator[sqlite3.Connection]:
    with open_connection(":memory:") as conn:
        dialect_for_ds_type("sqlite").register_udfs(conn)
        yield conn


def _convert(con: sqlite3.Connection, value: Any, source: Optional[str], target: str) -> Any:
    return con.execute("SELECT slayer_convert_tz(?, ?, ?)", (value, source, target)).fetchone()[0]


def test_registered() -> None:
    assert "slayer_convert_tz" in dialect_for_ds_type("sqlite").slayer_registered_functions


@pytest.mark.parametrize(("value", "source", "target", "expected"), [
    ("2025-01-01 03:30:00", "UTC", NY, "2024-12-31 22:30:00.000000"),
    ("2025-01-31 23:30:00", "UTC", TOKYO, "2025-02-01 08:30:00.000000"),
    ("2025-01-01T03:30:00.25", "UTC", NY, "2024-12-31 22:30:00.250000"),
    ("2025-01-01", "UTC", NY, "2024-12-31 19:00:00.000000"),
    ("2025-01-01 12:30:00", TOKYO, "UTC", "2025-01-01 03:30:00.000000"),
    ("2025-01-01 03:30:00", "UTC", "UTC", "2025-01-01 03:30:00.000000"),
], ids=["utc_to_ny", "utc_to_tokyo", "t_separator_fraction", "date_only_midnight", "tokyo_to_utc", "identity"])
def test_naive_text(con: sqlite3.Connection, value: str, source: str, target: str, expected: str) -> None:
    assert _convert(con, value, source, target) == expected


@pytest.mark.parametrize(("value", "expected"), [
    ("2025-01-01 03:30:00+00:00", "2024-12-31 22:30:00.000000"),
    ("2025-01-01 12:30:00+09:00", "2024-12-31 22:30:00.000000"),
    ("2025-01-01T03:30:00Z", "2024-12-31 22:30:00.000000"),
], ids=["utc_offset", "tokyo_offset", "zulu"])
def test_offset_text_is_an_instant(con: sqlite3.Connection, value: str, expected: str) -> None:
    assert _convert(con, value, "UTC", NY) == expected


@pytest.mark.parametrize("value", ["x", "2025-13-01 00:00:00", "", "2025-01-01 25:00:00"])
def test_malformed_text_is_null(con: sqlite3.Connection, value: str) -> None:
    assert _convert(con, value, "UTC", NY) is None


def test_null_is_null(con: sqlite3.Connection) -> None:
    assert _convert(con, None, "UTC", NY) is None


def test_skipped_time_takes_the_pre_transition_offset(con: sqlite3.Connection) -> None:
    assert _convert(con, "2025-03-09 02:30:00", NY, "UTC") == "2025-03-09 07:30:00.000000"


def test_repeated_time_is_the_later_occurrence(con: sqlite3.Connection) -> None:
    assert _convert(con, "2024-11-03 01:30:00", NY, "UTC") == "2024-11-03 06:30:00.000000"


def test_into_a_repeated_hour(con: sqlite3.Connection) -> None:
    assert [_convert(con, v, "UTC", NY) for v in ("2024-11-03 05:30:00", "2024-11-03 06:30:00")] == [
        "2024-11-03 01:30:00.000000", "2024-11-03 01:30:00.000000"]
