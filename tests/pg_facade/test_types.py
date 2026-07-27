"""Tests for slayer.pg_facade.types — type mapping + value (de)serialisation."""

from __future__ import annotations

import datetime as dt
import struct
from decimal import Decimal

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.facade.catalog import build_catalog
from slayer.facade.translator import QueryResult, translate
from slayer.pg_facade import types as t
from slayer.pg_facade.protocol import (
    OID_BOOL,
    OID_DATE,
    OID_FLOAT8,
    OID_INT8,
    OID_TEXT,
    OID_TIMESTAMP,
)


# --- datatype_to_oid ---------------------------------------------------------


@pytest.mark.parametrize(
    "dt_,oid",
    [
        (DataType.TEXT, OID_TEXT),
        (DataType.INT, OID_INT8),
        (DataType.DOUBLE, OID_FLOAT8),
        (DataType.BOOLEAN, OID_BOOL),
        (DataType.DATE, OID_DATE),
        (DataType.TIMESTAMP, OID_TIMESTAMP),
    ],
)
def test_datatype_to_oid(dt_, oid) -> None:
    assert t.datatype_to_oid(dt_) == oid


def test_datatype_to_oid_none_falls_back_to_text() -> None:
    assert t.datatype_to_oid(None) == OID_TEXT


def test_query_result_oid_overridden_by_cast() -> None:
    """DEV-1566: end-to-end — a CAST(<DATE col> AS TIMESTAMP) translation
    yields a projection_types entry that maps to OID_TIMESTAMP, not the
    column's declared OID_DATE."""
    orders = SlayerModel(
        name="orders", data_source="jaffle", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="delivered_at", type=DataType.DATE),
        ],
    )
    catalog = build_catalog(models_by_datasource={"jaffle": [orders]})

    # Baseline: bare projection → OID_DATE.
    bare = translate(sql="SELECT delivered_at FROM orders", catalog=catalog)
    assert isinstance(bare, QueryResult)
    assert t.datatype_to_oid(bare.projection_types[0]) == OID_DATE

    # CAST projection → OID_TIMESTAMP (wire layer rewritten).
    casted = translate(
        sql="SELECT CAST(delivered_at AS TIMESTAMP) FROM orders", catalog=catalog,
    )
    assert isinstance(casted, QueryResult)
    assert t.datatype_to_oid(casted.projection_types[0]) == OID_TIMESTAMP


# --- value_to_text -----------------------------------------------------------


def test_value_to_text_none_is_sql_null() -> None:
    assert t.value_to_text(None) is None


def test_value_to_text_bool() -> None:
    # DEV-1566: the default oid is OID_TEXT, which is the Postgres text shape
    # for booleans (``true``/``false``). The BOOL-wire ``t``/``f`` shape is
    # covered separately by test_value_to_text_bool_in_bool_column.
    assert t.value_to_text(True) == b"true"
    assert t.value_to_text(False) == b"false"


def test_value_to_text_bool_in_bool_column() -> None:
    """OID_BOOL keeps the wire-format ``t``/``f`` shape — that's what the
    Postgres binary protocol carries on the text-format path for BOOL."""
    assert t.value_to_text(True, OID_BOOL) == b"t"
    assert t.value_to_text(False, OID_BOOL) == b"f"


def test_value_to_text_int_and_decimal() -> None:
    assert t.value_to_text(42) == b"42"
    assert t.value_to_text(Decimal("3.14")) == b"3.14"


def test_value_to_text_float_finite() -> None:
    assert t.value_to_text(1.5) == b"1.5"


def test_value_to_text_float_non_finite() -> None:
    assert t.value_to_text(float("nan")) == b"NaN"
    assert t.value_to_text(float("inf")) == b"Infinity"
    assert t.value_to_text(float("-inf")) == b"-Infinity"


def test_value_to_text_timestamp_uses_space_separator() -> None:
    ts = dt.datetime(2026, 5, 27, 12, 0, 0)
    assert t.value_to_text(ts) == b"2026-05-27 12:00:00"


def test_value_to_text_timestamp_with_micros() -> None:
    ts = dt.datetime(2026, 5, 27, 12, 0, 0, 123456)
    assert t.value_to_text(ts) == b"2026-05-27 12:00:00.123456"


def test_value_to_text_date() -> None:
    assert t.value_to_text(dt.date(2026, 5, 27)) == b"2026-05-27"


def test_value_to_text_datetime_in_date_column_is_narrowed() -> None:
    """Round-20c live Metabase repro: DuckDB returns
    ``CAST(date_trunc('month', ordered_at) AS DATE)`` as a
    ``datetime.datetime``. With the column declared OID DATE, the wire
    payload must be ``"2024-06-01"`` (date-only), not the timestamp
    string — pgjdbc's ``TimestampUtils.toLocalDate`` rejects the latter
    with ``DateTimeException`` for a DATE-typed column."""
    ts = dt.datetime(2024, 6, 1, 0, 0, 0)
    assert t.value_to_text(ts, OID_DATE) == b"2024-06-01"
    # Default (no oid / OID_TEXT) preserves the timestamp shape — the
    # caller decides what the column is.
    assert t.value_to_text(ts) == b"2024-06-01 00:00:00"


def test_value_to_text_datetime_in_timestamp_column_is_preserved() -> None:
    ts = dt.datetime(2024, 6, 1, 12, 30, 45)
    assert t.value_to_text(ts, OID_TIMESTAMP) == b"2024-06-01 12:30:45"


def test_value_to_text_date_in_timestamp_column_is_widened() -> None:
    """DEV-1566: symmetric widening to the existing OID_DATE-driven narrowing.
    CAST(<DATE col> AS TIMESTAMP) needs the wire text payload to look like a
    Postgres TIMESTAMP literal (``YYYY-MM-DD HH:MM:SS``) — without the widen
    step pgjdbc/psycopg2 mis-parse a bare ``YYYY-MM-DD`` value for a column
    declared with OID 1114."""
    d = dt.date(2024, 6, 1)
    assert t.value_to_text(d, OID_TIMESTAMP) == b"2024-06-01 00:00:00"
    # Default (no oid / OID_TEXT) preserves the bare date shape.
    assert t.value_to_text(d) == b"2024-06-01"


def test_value_to_text_bool_in_text_column_uses_true_false() -> None:
    """DEV-1566: CAST(<bool> AS TEXT) must surface Postgres-shaped boolean
    text (``true``/``false``), not the BOOL wire shape (``t``/``f``).
    Default oid == OID_TEXT so the explicit and default forms agree;
    BOOL-wire ``t``/``f`` is reachable only via explicit OID_BOOL."""
    assert t.value_to_text(True, OID_TEXT) == b"true"
    assert t.value_to_text(False, OID_TEXT) == b"false"


def test_value_to_text_str_and_bytes() -> None:
    assert t.value_to_text("hello") == b"hello"
    assert t.value_to_text(b"raw") == b"raw"


# --- binary roundtrip --------------------------------------------------------


def test_binary_int8_roundtrip() -> None:
    assert t.value_to_binary(123456789, OID_INT8) == struct.pack(">q", 123456789)
    assert t.value_from_binary(struct.pack(">q", -42), OID_INT8) == -42


def test_binary_float8_roundtrip() -> None:
    encoded = t.value_to_binary(1.5, OID_FLOAT8)
    assert encoded == struct.pack(">d", 1.5)
    assert t.value_from_binary(encoded, OID_FLOAT8) == 1.5  # NOSONAR(S1244) — exact binary roundtrip


def test_binary_bool_roundtrip() -> None:
    assert t.value_to_binary(True, OID_BOOL) == b"\x01"
    assert t.value_to_binary(False, OID_BOOL) == b"\x00"
    assert t.value_from_binary(b"\x01", OID_BOOL) is True
    assert t.value_from_binary(b"\x00", OID_BOOL) is False


def test_binary_text_roundtrip() -> None:
    assert t.value_to_binary("café", OID_TEXT) == "café".encode("utf-8")
    assert t.value_from_binary("café".encode("utf-8"), OID_TEXT) == "café"


def test_binary_date_roundtrip() -> None:
    d = dt.date(2026, 5, 27)
    encoded = t.value_to_binary(d, OID_DATE)
    assert t.value_from_binary(encoded, OID_DATE) == d
    # Epoch is 2000-01-01.
    assert t.value_to_binary(dt.date(2000, 1, 1), OID_DATE) == struct.pack(">i", 0)


def test_binary_timestamp_roundtrip() -> None:
    ts = dt.datetime(2026, 5, 27, 12, 34, 56, 789000)
    encoded = t.value_to_binary(ts, OID_TIMESTAMP)
    assert t.value_from_binary(encoded, OID_TIMESTAMP) == ts
    # Epoch.
    assert t.value_to_binary(dt.datetime(2000, 1, 1), OID_TIMESTAMP) == struct.pack(">q", 0)


def test_binary_none_is_null() -> None:
    assert t.value_to_binary(None, OID_INT8) is None


# --- value_from_text (param decoding) ---------------------------------------


def test_value_from_text_per_oid() -> None:
    assert t.value_from_text(b"42", OID_INT8) == 42
    assert t.value_from_text(b"1.5", OID_FLOAT8) == 1.5  # NOSONAR(S1244) — exact representable value
    assert t.value_from_text(b"t", OID_BOOL) is True
    assert t.value_from_text(b"false", OID_BOOL) is False
    assert t.value_from_text(b"2026-05-27", OID_DATE) == dt.date(2026, 5, 27)
    assert t.value_from_text(b"2026-05-27 12:00:00", OID_TIMESTAMP) == dt.datetime(2026, 5, 27, 12, 0, 0)
    assert t.value_from_text(b"hello", OID_TEXT) == "hello"


# --- literal_for_substitution -----------------------------------------------


def test_literal_none_is_sql_null() -> None:
    assert t.literal_for_substitution(None) == "NULL"


def test_literal_bool() -> None:
    assert t.literal_for_substitution(True) == "TRUE"
    assert t.literal_for_substitution(False) == "FALSE"


def test_literal_numbers() -> None:
    assert t.literal_for_substitution(42) == "42"
    assert t.literal_for_substitution(Decimal("3.14")) == "3.14"
    assert t.literal_for_substitution(1.5) == "1.5"


def test_literal_non_finite_float_rejected() -> None:
    with pytest.raises(ValueError):
        t.literal_for_substitution(float("nan"))


def test_literal_string_is_quoted_and_escaped() -> None:
    assert t.literal_for_substitution("hello") == "'hello'"
    # Single-quote escaping protects against breaking out of the literal.
    assert t.literal_for_substitution("O'Brien") == "'O''Brien'"
    assert t.literal_for_substitution("'; DROP TABLE x; --") == "'''; DROP TABLE x; --'"


def test_literal_date_and_timestamp_quoted() -> None:
    assert t.literal_for_substitution(dt.date(2026, 5, 27)) == "'2026-05-27'"
    assert (
        t.literal_for_substitution(dt.datetime(2026, 5, 27, 12, 0, 0))
        == "'2026-05-27 12:00:00'"
    )
