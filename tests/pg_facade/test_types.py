"""Tests for slayer.pg_facade.types — type mapping + value (de)serialisation."""

from __future__ import annotations

import datetime as dt
import struct
from decimal import Decimal

import pytest

from slayer.core.enums import DataType
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


# --- value_to_text -----------------------------------------------------------


def test_value_to_text_none_is_sql_null() -> None:
    assert t.value_to_text(None) is None


def test_value_to_text_bool() -> None:
    assert t.value_to_text(True) == b"t"
    assert t.value_to_text(False) == b"f"


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
    assert t.value_from_binary(encoded, OID_FLOAT8) == 1.5


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
    assert t.value_from_text(b"1.5", OID_FLOAT8) == 1.5
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
