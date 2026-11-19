"""SLayer ``DataType`` → JDBC type-name mapping + the supported-type set.

The pyarrow-free half of the old ``slayer/flight/types.py``. Lives in the
shared facade layer so both facades (and the catalog / INFORMATION_SCHEMA
builders) can map types without importing pyarrow. The Arrow-specific half
(``datatype_to_arrow`` / ``arrow_to_datatype``) stays in
``slayer/flight/types.py``.
"""

from __future__ import annotations

from slayer.core.enums import DataType, TypeKind

_KIND_TO_JDBC: dict[TypeKind, str] = {
    TypeKind.TEXT: "VARCHAR",
    TypeKind.INT: "BIGINT",
    TypeKind.DOUBLE: "DOUBLE",
    TypeKind.BOOLEAN: "BOOLEAN",
    TypeKind.DATE: "DATE",
    TypeKind.TIMESTAMP: "TIMESTAMP",
    # Opaque columns travel as text over the wire — the value is whatever the
    # driver stringified, and no client-side type claim would be honest.
    TypeKind.UNKNOWN: "VARCHAR",
    # Lists travel as their JSON array text.
    TypeKind.ARRAY: "VARCHAR",
}
_DATATYPE_TO_JDBC: dict[DataType, str] = {t: _KIND_TO_JDBC[t.kind] for t in DataType}


def datatype_to_jdbc(dt: DataType) -> str:
    """Return the JDBC type-name string for a SLayer ``DataType``."""
    return _DATATYPE_TO_JDBC[dt]


SUPPORTED_DATATYPES: tuple[DataType, ...] = tuple(_DATATYPE_TO_JDBC.keys())
