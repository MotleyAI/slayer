"""SLayer ``DataType`` → JDBC type-name mapping + the supported-type set.

The pyarrow-free half of the old ``slayer/flight/types.py``. Lives in the
shared facade layer so both facades (and the catalog / INFORMATION_SCHEMA
builders) can map types without importing pyarrow. The Arrow-specific half
(``datatype_to_arrow`` / ``arrow_to_datatype``) stays in
``slayer/flight/types.py``.
"""

from __future__ import annotations

from slayer.core.enums import DataType

_DATATYPE_TO_JDBC: dict[DataType, str] = {
    DataType.TEXT: "VARCHAR",
    DataType.INT: "BIGINT",
    DataType.DOUBLE: "DOUBLE",
    DataType.BOOLEAN: "BOOLEAN",
    DataType.DATE: "DATE",
    DataType.TIMESTAMP: "TIMESTAMP",
}


def datatype_to_jdbc(dt: DataType) -> str:
    """Return the JDBC type-name string for a SLayer ``DataType``."""
    return _DATATYPE_TO_JDBC[dt]


SUPPORTED_DATATYPES: tuple[DataType, ...] = tuple(_DATATYPE_TO_JDBC.keys())
