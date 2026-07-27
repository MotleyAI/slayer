"""Dependency-free column-introspection helpers.

Extracted from ``slayer/engine/ingestion.py`` (DEV-1578) so the
forced-filter column-presence probe in ``slayer/engine/query_engine.py``
can reuse ``_safe_get_columns`` without importing ``ingestion`` (which
imports ``query_engine`` — a cycle). ``ingestion`` and ``schema_drift``
import these from here; ``ingestion`` also re-exports them for back-compat.

``_safe_get_columns`` tries SQLAlchemy's ``Inspector.get_columns`` first and
falls back to a parameterized ``INFORMATION_SCHEMA.columns`` query when
reflection raises — see ``docs`` / the ingestion module for rationale.
"""

from __future__ import annotations

from typing import Dict, List, Optional

import sqlalchemy as sa

from slayer.core.enums import DataType

# Float-like INFORMATION_SCHEMA type names
_FLOAT_LIKE_INFO_SCHEMA_TYPES = frozenset(
    {
        "FLOAT",
        "DOUBLE",
        "REAL",
    }
)

# Map INFORMATION_SCHEMA type names to SLayer DataTypes (for DuckDB fallback).
# DEV-1361: integer family → INT, floating family → DOUBLE.
_INFO_SCHEMA_TYPE_MAP = {
    # Integer family
    "INTEGER": DataType.INT,
    "BIGINT": DataType.INT,
    "SMALLINT": DataType.INT,
    "TINYINT": DataType.INT,
    "HUGEINT": DataType.INT,
    # Floating family
    "FLOAT": DataType.DOUBLE,
    "DOUBLE": DataType.DOUBLE,
    "REAL": DataType.DOUBLE,
    # Strings / boolean / temporal
    "VARCHAR": DataType.TEXT,
    "CHAR": DataType.TEXT,
    "TEXT": DataType.TEXT,
    "BOOLEAN": DataType.BOOLEAN,
    "TIMESTAMP": DataType.TIMESTAMP,
    "TIMESTAMP WITH TIME ZONE": DataType.TIMESTAMP,
    "DATETIME": DataType.TIMESTAMP,
    "DATE": DataType.DATE,
    "TIME": DataType.TIMESTAMP,
}


def _parse_info_schema_is_float(data_type_str: str) -> bool:
    """Determine if a NUMERIC/DECIMAL info-schema type string is float-like.

    Parses scale from strings like "DECIMAL(10,2)" or "NUMERIC(10,0)".
    Scale > 0 means float-like; scale == 0 means integer-like; no scale
    info defaults to float-like.
    """
    if "(" in data_type_str and "," in data_type_str:
        try:
            scale_str = data_type_str.split(",")[-1].rstrip(")").strip()
            return int(scale_str) > 0
        except (ValueError, IndexError):
            return True  # Can't parse scale, default to float
    return True  # No precision/scale info, default to float


def _get_columns_fallback(
    sa_engine: sa.Engine,
    table_name: str,
    schema: Optional[str],
) -> List[Dict]:
    """Get columns via INFORMATION_SCHEMA when Inspector.get_columns() fails."""
    if schema:
        sql = (
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = :table_name "
            "AND table_schema = :schema "
            "ORDER BY ordinal_position"
        )
        params = {"table_name": table_name, "schema": schema}
    else:
        sql = (
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = :table_name "
            "ORDER BY ordinal_position"
        )
        params = {"table_name": table_name}
    with sa_engine.connect() as conn:
        rows = conn.execute(sa.text(sql), params).fetchall()
    result = []
    for col_name, data_type_str in rows:
        # Strip precision info (e.g. "DECIMAL(10,2)" → "DECIMAL")
        base_type = data_type_str.split("(")[0].upper().strip()
        sa_type = _INFO_SCHEMA_TYPE_MAP.get(base_type)
        is_float = base_type in _FLOAT_LIKE_INFO_SCHEMA_TYPES
        # NUMERIC/DECIMAL: check scale to decide float vs integer
        if base_type in ("NUMERIC", "DECIMAL") or (
            sa_type is None and ("DECIMAL" in base_type or "NUMERIC" in base_type)
        ):
            sa_type = sa_type or DataType.DOUBLE
            is_float = _parse_info_schema_is_float(data_type_str)
        elif sa_type is None and "INT" in base_type:
            # DEV-1361: integer-shaped types should narrow to INT, not the
            # coarse DOUBLE fallback (e.g. MEDIUMINT, TINYINT variants not
            # otherwise mapped).
            sa_type = DataType.INT
        elif sa_type is None and ("CHAR" in base_type or "TEXT" in base_type):
            sa_type = DataType.TEXT
        result.append({"name": col_name, "type": sa_type or DataType.TEXT, "is_float": is_float})
    return result


def _safe_get_columns(
    inspector: sa.engine.Inspector,
    sa_engine: sa.Engine,
    table_name: str,
    schema: Optional[str],
) -> List[Dict]:
    """Get columns, falling back to INFORMATION_SCHEMA on failure."""
    try:
        return inspector.get_columns(table_name, schema=schema)
    except Exception:
        return _get_columns_fallback(sa_engine, table_name, schema)
