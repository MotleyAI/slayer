"""Type mapping + value (de)serialisation for the Postgres facade (DEV-1486).

Three concerns:

1. ``DATATYPE_TO_OID`` / ``datatype_to_oid`` — SLayer ``DataType`` → Postgres
   type OID. Only the six built-in OIDs are ever emitted (unknown → text), so
   asyncpg never has to run its ``pg_type`` introspection path.
2. ``value_to_text`` / ``value_to_binary`` — engine value → wire bytes for a
   ``DataRow``, in the per-column format the client requested in ``Bind``.
   Simple-query always uses text; extended-query honours the format codes.
3. ``value_from_text`` / ``value_from_binary`` + ``literal_for_substitution`` —
   decode a bound parameter value and render it as a safe SQL literal for
   ``$N`` substitution before translation.

Binary formats follow Postgres with ``integer_datetimes=on``: date is int32
days since 2000-01-01; timestamp is int64 microseconds since 2000-01-01
00:00:00. Text timestamps use a space separator (``YYYY-MM-DD HH:MM:SS``).
"""

from __future__ import annotations

import datetime as _dt
import math
import struct
from decimal import Decimal
from typing import Any, Dict, Optional

from slayer.core.enums import DataType
from slayer.pg_facade.protocol import (
    OID_BOOL,
    OID_DATE,
    OID_FLOAT8,
    OID_INT8,
    OID_TEXT,
    OID_TIMESTAMP,
)

DATATYPE_TO_OID: Dict[DataType, int] = {
    DataType.TEXT: OID_TEXT,
    DataType.INT: OID_INT8,
    DataType.DOUBLE: OID_FLOAT8,
    DataType.BOOLEAN: OID_BOOL,
    DataType.DATE: OID_DATE,
    DataType.TIMESTAMP: OID_TIMESTAMP,
}

# Postgres binary date/timestamp epoch.
_PG_EPOCH_DATE = _dt.date(2000, 1, 1)
_PG_EPOCH_DATETIME = _dt.datetime(2000, 1, 1)


def datatype_to_oid(dt: Optional[DataType]) -> int:
    """Map a SLayer ``DataType`` to a Postgres OID; unknown / None → text."""
    if dt is None:
        return OID_TEXT
    return DATATYPE_TO_OID.get(dt, OID_TEXT)


# --- text-format output ------------------------------------------------------


def value_to_text(  # NOSONAR(S3776) — flat per-Python-type dispatch
    value: Any, oid: int = OID_TEXT,
) -> Optional[bytes]:
    """Encode an engine value as Postgres text-format bytes (``None`` → SQL NULL).

    ``oid`` lets the encoder coerce values to match the declared column type.
    Notably, DuckDB returns ``DATE``-typed columns as ``datetime.datetime``
    when the value is produced by ``date_trunc(...) :: date``; without
    coercion the text-format wire payload becomes ``"2024-06-01 00:00:00"``
    which pgjdbc's ``TimestampUtils.toLocalDate`` mis-parses for a column
    declared with OID 1082 (DATE).
    """
    if value is None:
        return None
    # OID-driven coercion runs first so a datetime in a DATE column is
    # narrowed to a date before the per-Python-type dispatch below.
    if oid == OID_DATE and isinstance(value, _dt.datetime):
        value = value.date()
    if isinstance(value, bool):
        return b"t" if value else b"f"
    if isinstance(value, float):
        if math.isnan(value):
            return b"NaN"
        if math.isinf(value):
            return b"Infinity" if value > 0 else b"-Infinity"
        return repr(value).encode("utf-8")
    if isinstance(value, Decimal):
        return str(value).encode("utf-8")
    if isinstance(value, int):
        return str(value).encode("utf-8")
    if isinstance(value, _dt.datetime):
        return _format_timestamp(value).encode("utf-8")
    if isinstance(value, _dt.date):
        return value.isoformat().encode("utf-8")
    if isinstance(value, bytes):
        # Defensive: re-encode through UTF-8 so the wire payload is valid text.
        return value.decode("utf-8", errors="replace").encode("utf-8")
    return str(value).encode("utf-8")


def _format_timestamp(value: _dt.datetime) -> str:
    """Postgres text timestamp — space separator, microsecond precision."""
    if value.tzinfo is not None:
        value = value.astimezone(_dt.timezone.utc).replace(tzinfo=None)
    return value.isoformat(sep=" ")


# --- binary-format output ----------------------------------------------------


def value_to_binary(value: Any, oid: int) -> Optional[bytes]:
    """Encode an engine value as Postgres binary-format bytes for ``oid``
    (``None`` → SQL NULL)."""
    if value is None:
        return None
    if oid == OID_INT8:
        return struct.pack(">q", int(value))
    if oid == OID_FLOAT8:
        return struct.pack(">d", float(value))
    if oid == OID_BOOL:
        return b"\x01" if value else b"\x00"
    if oid == OID_DATE:
        return struct.pack(">i", (_as_date(value) - _PG_EPOCH_DATE).days)
    if oid == OID_TIMESTAMP:
        delta = _as_naive_datetime(value) - _PG_EPOCH_DATETIME
        micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
        return struct.pack(">q", micros)
    # OID_TEXT and any fallback → UTF-8 text bytes (same as text format).
    text = value_to_text(value)
    return text if text is not None else b""


def _as_date(value: Any) -> _dt.date:
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    return _dt.date.fromisoformat(str(value))


def _as_naive_datetime(value: Any) -> _dt.datetime:
    if isinstance(value, _dt.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(_dt.timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, _dt.date):
        return _dt.datetime(value.year, value.month, value.day)
    return _dt.datetime.fromisoformat(str(value).replace(" ", "T"))


# --- parameter decoding (Bind) ----------------------------------------------


def value_from_text(buf: bytes, oid: int) -> Any:
    """Decode a text-format bound parameter value for ``oid``."""
    s = buf.decode("utf-8")
    if oid == OID_INT8:
        return int(s)
    if oid == OID_FLOAT8:
        return float(s)
    if oid == OID_BOOL:
        return s.strip().lower() in ("t", "true", "1", "y", "yes", "on")
    if oid == OID_DATE:
        return _dt.date.fromisoformat(s)
    if oid == OID_TIMESTAMP:
        return _dt.datetime.fromisoformat(s.replace(" ", "T"))
    return s


def value_from_binary(buf: bytes, oid: int) -> Any:
    """Decode a binary-format bound parameter value for ``oid``."""
    if oid == OID_INT8:
        return struct.unpack(">q", buf)[0]
    if oid == OID_FLOAT8:
        return struct.unpack(">d", buf)[0]
    if oid == OID_BOOL:
        return buf != b"\x00"
    if oid == OID_DATE:
        days = struct.unpack(">i", buf)[0]
        return _PG_EPOCH_DATE + _dt.timedelta(days=days)
    if oid == OID_TIMESTAMP:
        micros = struct.unpack(">q", buf)[0]
        return _PG_EPOCH_DATETIME + _dt.timedelta(microseconds=micros)
    return buf.decode("utf-8")


# --- literal substitution ----------------------------------------------------


def literal_for_substitution(value: Any) -> str:
    """Render a decoded bound value as a safe SQL literal for ``$N`` substitution.

    Strings/dates/timestamps are single-quoted with ``'`` doubled; numbers and
    booleans are emitted bare; ``None`` → ``NULL``. Non-finite floats are
    rejected (no portable SQL literal).
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite float cannot be used as a bound parameter")
        return repr(value)
    if isinstance(value, (int, Decimal)):
        return str(value)
    if isinstance(value, _dt.datetime):
        return _quote(_format_timestamp(value))
    if isinstance(value, _dt.date):
        return _quote(value.isoformat())
    if isinstance(value, bytes):
        return _quote(value.decode("utf-8", errors="replace"))
    return _quote(str(value))


def _quote(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"
