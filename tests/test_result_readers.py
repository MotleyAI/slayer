"""Result fetch reads each cell the driver returns as text by its dialect (ClickHouse ``Bool``, Snowflake ``ARRAY``),
and cursor type codes classify lists."""

from __future__ import annotations

from datetime import date
from typing import Any, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column
from slayer.sql.client import _fetch_rows, _types_from_cursor
from slayer.sql.dialects import get_dialect
from slayer.sql.dialects.base import describe_type_name_code
from slayer.sql.dialects.reflection import ResultColumnType, is_exact_numeric_db_type
from slayer.sql.render.value_expr import keeps_native_type
from slayer.sql.dialects._clickhouse_cells import array_reader


class _Cursor:
    def __init__(self, description: list[tuple[Any, ...]]) -> None:
        self.description = description


class _Result:
    returns_rows = True

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


# --------------------------------------------------------------------------- #
# ClickHouse array cells keep every escape
# --------------------------------------------------------------------------- #
_CH_ESCAPES = {"\\": "\\\\", "'": "\\'", "\n": "\\n", "\t": "\\t", "\r": "\\r", "\0": "\\0", "\b": "\\b",
               "\f": "\\f"}


def _clickhouse_cell(values: list[Optional[str]]) -> str:
    """``values`` as the ClickHouse HTTP driver returns an ``Array(Nullable(String))`` cell."""
    def quoted(value: str) -> str:
        return "'" + "".join(_CH_ESCAPES.get(ch, ch) for ch in value) + "'"
    return "[" + ",".join("NULL" if v is None else quoted(v) for v in values) + "]"


_STRINGS = array_reader("Array(Nullable(String))")


@pytest.mark.parametrize("values", [
    ["back\\slash", "quo'te", "new\nline", "tab\tbed", "carriage\rreturn", "nul\0byte", "bell\bspace", "form\ffeed"],
    ["c:\\path\\to\\file.txt", "\\\\server\\share"],
    ["\\\\", "trailing\\", "\\x41", "\\N", "a,b", "[x]", "é€\U0001F600"],
    ["esc\x1bape", "vt\vab", "al\aert", "\x01\x7f"],
    [None, "", "x", None],
])
def test_clickhouse_cell_round_trip(values: list[Optional[str]]) -> None:
    assert _STRINGS is not None
    assert _STRINGS(_clickhouse_cell(values)) == values


@pytest.mark.parametrize(("cell", "expected"), [
    (r"['\x41\x62']", ["Ab"]),
    (r"['\a\v\e']", ["\a\v\x1b"]),
    (r"['say \"hi\"']", ['say "hi"']),
    # An unknown escape keeps its backslash, as ClickHouse's own parser does.
    (r"['a\qb']", ["a\\qb"]),
    (r"['a\xZ1b']", ["a\\xZ1b"]),
    (r"['a\x']", ["a\\x"]),
    (r"['a\x4']", ["a\\x4"]),
])
def test_clickhouse_cell_escapes(cell: str, expected: list[str]) -> None:
    assert _STRINGS is not None
    assert _STRINGS(cell) == expected


# --------------------------------------------------------------------------- #
# Cursor type codes
# --------------------------------------------------------------------------- #
def _row(type_code: Any) -> tuple[Any, ...]:
    return ("c", type_code, None, None, None, None, None)


@pytest.mark.parametrize("type_code", ["INTEGER[]", "VARCHAR[]", "LIST(VARCHAR)", "ARRAY(INTEGER)", "ARRAY<STRING>"])
def test_list_type_names_are_lists(type_code: str) -> None:
    typed = describe_type_name_code(_row(type_code))
    assert typed is not None
    assert typed.type is DataType.ARRAY


def test_an_untyped_array_name_is_a_list() -> None:
    typed = describe_type_name_code(_row("ARRAY"))
    assert typed is not None
    assert typed.type is DataType.ARRAY


@pytest.mark.parametrize(("oid", "pg_type"), [
    (1000, "bool[]"), (1005, "int2[]"), (1007, "int4[]"), (1009, "text[]"), (1015, "varchar[]"), (1016, "int8[]"),
    (1021, "float4[]"), (1022, "float8[]"), (1182, "date[]"), (1115, "timestamp[]"), (1231, "numeric[]"),
    (1185, "timestamptz[]"), (1014, "bpchar[]"), (2951, "uuid[]"), (199, "json[]"), (3807, "jsonb[]"),
    (1001, "bytea[]"),
])
def test_postgres_array_type_codes_are_lists(oid: int, pg_type: str) -> None:
    typed = get_dialect("postgres").describe_cursor_column(_row(oid))
    assert typed is not None
    assert typed.type is DataType.ARRAY, pg_type


def test_an_untyped_cursor_column_holding_lists_is_a_list() -> None:
    result = _Result(["l", "n"], [None, None], [(["go", "sql"], 1)])
    types = _types_from_cursor(result, dialect=get_dialect("sqlite"))
    assert types["l"].type is DataType.ARRAY
    assert types["n"].type is DataType.DOUBLE


def test_a_probed_list_keeps_its_type_name() -> None:
    assert ResultColumnType(type=DataType.ARRAY, db_type="INTEGER[]").lossy_db_type == "INTEGER[]"


@pytest.mark.parametrize("name", ["NUMERIC[]", "DECIMAL(10,2)[]", "ARRAY(DECIMAL(10,2))", "Array(Decimal(9,2))"])
def test_a_numeric_list_is_not_exact_numeric(name: str) -> None:
    assert not is_exact_numeric_db_type(name)


def test_a_numeric_list_column_is_cast_like_any_list() -> None:
    col = Column(name="prices", type=DataType.ARRAY, element_type=DataType.DOUBLE, db_type="NUMERIC[]")
    assert not keeps_native_type(col=col, dialect=get_dialect("postgres"))
