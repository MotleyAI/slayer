"""Result fetch reads each cell the driver returns as text by its dialect (ClickHouse ``Bool``, Snowflake ``ARRAY``)."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from slayer.sql.client import _fetch_rows
from slayer.sql.dialects import get_dialect


class _Cursor:
    def __init__(self, description: list[tuple[Any, ...]]) -> None:
        self.description = description


class _Result:
    def __init__(self, columns: list[str], types: list[Any], rows: list[tuple[Any, ...]]) -> None:
        self.cursor = _Cursor([(c, t, None, None, None, None, True) for c, t in zip(columns, types)])
        self._columns = columns
        self._rows = rows

    def keys(self) -> list[str]:
        return self._columns

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


@pytest.mark.parametrize("type_code", ["Bool", "Nullable(Bool)"])
def test_clickhouse_bool_text_reads_as_a_boolean(type_code: str) -> None:
    result = _Result(["b", "n"], [type_code, "UInt8"], [("true", 1), ("false", 0), (None, None)])
    rows = _fetch_rows(result, dialect=get_dialect("clickhouse"))
    assert [r["b"] for r in rows] == [True, False, None]
    assert [r["n"] for r in rows] == [1, 0, None]


def test_snowflake_array_text_reads_as_a_list() -> None:
    result = _Result(["l", "t"], [10, 2], [('["go", null]', '["x"]'), (None, "y")])
    rows = _fetch_rows(result, dialect=get_dialect("snowflake"))
    assert rows == [{"l": ["go", None], "t": '["x"]'}, {"l": None, "t": "y"}]


def test_a_driver_tuple_reads_as_a_list() -> None:
    result = _Result(["l"], [None], [(("go", "sql"),)])
    assert _fetch_rows(result, dialect=get_dialect("duckdb")) == [{"l": ["go", "sql"]}]


@pytest.mark.parametrize(("type_code", "text", "expected"), [
    ("Array(String)", "['Oslo','Rome']", ["Oslo", "Rome"]),
    ("Array(Nullable(String))", "['it\\'s','a\\\\b',NULL]", ["it's", "a\\b", None]),
    ("Array(LowCardinality(String))", "['go, sql']", ["go, sql"]),
    ("Array(Int64)", "[1,-2,3]", [1, -2, 3]),
    ("Array(Nullable(Float64))", "[1.5,NULL]", [1.5, None]),
    ("Array(Date)", "['2024-01-05']", [date(2024, 1, 5)]),
    ("Array(Bool)", "[true,false]", [True, False]),
    ("Array(String)", "[]", []),
])
def test_clickhouse_array_text_reads_as_a_typed_list(type_code: str, text: str, expected: list[Any]) -> None:
    result = _Result(["l"], [type_code], [(text,)])
    assert _fetch_rows(result, dialect=get_dialect("clickhouse")) == [{"l": expected}]


def test_clickhouse_nested_arrays_stay_text() -> None:
    result = _Result(["l"], ["Array(Array(Int64))"], [("[[1],[2]]",)])
    assert _fetch_rows(result, dialect=get_dialect("clickhouse")) == [{"l": "[[1],[2]]"}]
