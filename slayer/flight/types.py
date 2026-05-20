"""Type-mapping tables for the Flight SQL facade (DEV-1390 §5.3).

Three concentric type systems converge here:

* SLayer's ``DataType`` (``slayer.core.enums``) — six canonical values:
  ``TEXT``, ``INT``, ``DOUBLE``, ``BOOLEAN``, ``DATE``, ``TIMESTAMP``.
* Apache Arrow ``DataType`` — the wire encoding the Flight SQL gRPC
  server emits to clients.
* JDBC type-name strings — what `INFORMATION_SCHEMA.{COLUMNS,METRICS,
  DIMENSIONS}.data_type` rows display, matching what the dbt-SL JDBC
  driver renders for BI tools.

The forward direction (``DataType → Arrow`` and ``DataType → JDBC``) is
total over the six supported values. The reverse direction
(``Arrow → DataType``) collapses Arrow's much wider type space onto
the six SLayer types: any signed-integer width → ``INT``, any float /
decimal → ``DOUBLE``, any timestamp unit → ``TIMESTAMP``, etc.
``arrow_to_datatype`` returns ``None`` for genuinely unmappable Arrow
types (e.g. ``list_``, ``struct_``); callers decide how to handle.
"""

from __future__ import annotations

from typing import Optional

import pyarrow as pa

from slayer.core.enums import DataType

_DATATYPE_TO_ARROW: dict[DataType, pa.DataType] = {
    DataType.TEXT: pa.utf8(),
    DataType.INT: pa.int64(),
    DataType.DOUBLE: pa.float64(),
    DataType.BOOLEAN: pa.bool_(),
    DataType.DATE: pa.date32(),
    DataType.TIMESTAMP: pa.timestamp("us"),
}

_DATATYPE_TO_JDBC: dict[DataType, str] = {
    DataType.TEXT: "VARCHAR",
    DataType.INT: "BIGINT",
    DataType.DOUBLE: "DOUBLE",
    DataType.BOOLEAN: "BOOLEAN",
    DataType.DATE: "DATE",
    DataType.TIMESTAMP: "TIMESTAMP",
}


def datatype_to_arrow(dt: DataType) -> pa.DataType:
    """Return the canonical Arrow type for a SLayer ``DataType``."""
    return _DATATYPE_TO_ARROW[dt]


def datatype_to_jdbc(dt: DataType) -> str:
    """Return the JDBC type-name string for a SLayer ``DataType``."""
    return _DATATYPE_TO_JDBC[dt]


def arrow_to_datatype(at: pa.DataType) -> Optional[DataType]:
    """Best-effort reverse map.

    Returns ``None`` if ``at`` cannot be coerced into one of the six
    SLayer types (e.g. list, struct, decimal-with-precision-loss).
    Callers typically use this to reconcile a ``LIMIT 0``-derived
    Arrow schema against a catalog-declared ``DataType``; on mismatch
    the wire schema wins (§5.3).
    """
    if pa.types.is_string(at) or pa.types.is_large_string(at):
        return DataType.TEXT
    if pa.types.is_integer(at):
        return DataType.INT
    if pa.types.is_floating(at) or pa.types.is_decimal(at):
        return DataType.DOUBLE
    if pa.types.is_boolean(at):
        return DataType.BOOLEAN
    if pa.types.is_date(at):
        return DataType.DATE
    if pa.types.is_timestamp(at):
        return DataType.TIMESTAMP
    return None


SUPPORTED_DATATYPES: tuple[DataType, ...] = tuple(_DATATYPE_TO_ARROW.keys())
