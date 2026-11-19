"""Arrow type-mapping for the Flight SQL facade (DEV-1390 §5.3).

The pyarrow-bound half of the facade type system. The pyarrow-free half
(``SUPPORTED_DATATYPES`` + ``datatype_to_jdbc``) lives in
``slayer.facade.datatypes`` and is re-exported here for backward compat.

* SLayer's ``DataType`` (``slayer.core.enums``) — six operable values plus
  the opaque ``UNKNOWN`` marker.
* Apache Arrow ``DataType`` — the wire encoding the Flight SQL gRPC
  server emits to clients.

The forward direction (``DataType → Arrow``) is total over every value.
The reverse (``Arrow → DataType``) collapses Arrow's wider type space onto
the six operable SLayer types; ``arrow_to_datatype`` returns ``None`` for
genuinely unmappable Arrow types. The round trip is lossy for ``UNKNOWN``
(it goes out as ``utf8`` and comes back as ``TEXT``) — Arrow has no
"unclassified" type to preserve it.
"""

from __future__ import annotations


import pyarrow as pa

from slayer.core.enums import DataType, TypeKind
from slayer.facade.datatypes import SUPPORTED_DATATYPES, datatype_to_jdbc
from slayer.facade.rows import RowBatch

_KIND_TO_ARROW: dict[TypeKind, pa.DataType] = {
    TypeKind.TEXT: pa.utf8(),
    TypeKind.INT: pa.int64(),
    TypeKind.DOUBLE: pa.float64(),
    TypeKind.BOOLEAN: pa.bool_(),
    TypeKind.DATE: pa.date32(),
    TypeKind.TIMESTAMP: pa.timestamp("us"),
    # Opaque columns travel as Arrow strings — see DataType.is_opaque. The
    # forward map is intentionally lossy here: UNKNOWN -> utf8 -> TEXT does
    # not round-trip back to UNKNOWN.
    TypeKind.UNKNOWN: pa.utf8(),
    # Lists travel as their JSON array text.
    TypeKind.ARRAY: pa.utf8(),
}
_DATATYPE_TO_ARROW: dict[DataType, pa.DataType] = {t: _KIND_TO_ARROW[t.kind] for t in DataType}


def datatype_to_arrow(dt: DataType) -> pa.DataType:
    """Return the canonical Arrow type for a SLayer ``DataType``."""
    return _DATATYPE_TO_ARROW[dt]


def arrow_to_datatype(at: pa.DataType) -> DataType | None:
    """Best-effort reverse map.

    Returns ``None`` if ``at`` cannot be coerced into one of the six
    SLayer types (e.g. list, struct, decimal-with-precision-loss).
    Callers typically use this to reconcile a ``LIMIT 0``-derived
    Arrow schema against a catalog-declared ``DataType``; on mismatch
    the wire schema wins (§5.3).
    """
    if pa.types.is_string(at) or pa.types.is_large_string(at):
        return DataType.STRING
    if pa.types.is_integer(at):
        return DataType.INTEGER
    if pa.types.is_floating(at) or pa.types.is_decimal(at):
        return DataType.FLOAT
    if pa.types.is_boolean(at):
        return DataType.BOOLEAN
    if pa.types.is_date(at):
        return DataType.DATE
    if pa.types.is_timestamp(at):
        return DataType.DATETIME
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
