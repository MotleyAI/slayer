"""Arrow type-mapping for the Flight SQL facade (DEV-1390 §5.3).

The pyarrow-bound half of the facade type system. The pyarrow-free half
(``SUPPORTED_DATATYPES`` + ``datatype_to_jdbc``) lives in
``slayer.facade.datatypes`` and is re-exported here for backward compat.

* SLayer's ``DataType`` (``slayer.core.enums``) — six canonical values.
* Apache Arrow ``DataType`` — the wire encoding the Flight SQL gRPC
  server emits to clients.

The forward direction (``DataType → Arrow``) is total over the six
supported values. The reverse (``Arrow → DataType``) collapses Arrow's
wider type space onto the six SLayer types; ``arrow_to_datatype``
returns ``None`` for genuinely unmappable Arrow types.
"""

from __future__ import annotations

from typing import Optional

import pyarrow as pa

from slayer.core.enums import DataType
from slayer.facade.datatypes import SUPPORTED_DATATYPES, datatype_to_jdbc
from slayer.facade.rows import RowBatch

_DATATYPE_TO_ARROW: dict[DataType, pa.DataType] = {
    DataType.TEXT: pa.utf8(),
    DataType.INT: pa.int64(),
    DataType.DOUBLE: pa.float64(),
    DataType.BOOLEAN: pa.bool_(),
    DataType.DATE: pa.date32(),
    DataType.TIMESTAMP: pa.timestamp("us"),
}


def datatype_to_arrow(dt: DataType) -> pa.DataType:
    """Return the canonical Arrow type for a SLayer ``DataType``."""
    return _DATATYPE_TO_ARROW[dt]


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


def row_batch_to_arrow(batch: RowBatch) -> pa.Table:
    """Convert a facade-neutral ``RowBatch`` into a ``pyarrow.Table``.

    The conversion boundary between the shared (pyarrow-free) facade layer
    and the Flight facade's Arrow wire format.
    """
    fields = [
        pa.field(col.name, datatype_to_arrow(col.type)) for col in batch.columns
    ]
    return pa.Table.from_pylist(batch.rows, schema=pa.schema(fields))


__all__ = [
    "SUPPORTED_DATATYPES",
    "arrow_to_datatype",
    "datatype_to_arrow",
    "datatype_to_jdbc",
    "row_batch_to_arrow",
]
