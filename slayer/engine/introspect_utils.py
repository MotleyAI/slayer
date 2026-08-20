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
from sqlglot import exp

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


def _clean_comment(value: Optional[str]) -> Optional[str]:
    """Normalize a DB comment: strip whitespace, empty → None."""
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


# Per-dialect column-comment queries for the fallback path. INFORMATION_SCHEMA
# has no standard comment column, so each dialect needs its own source
# (DEV-1809). SQL Server / BigQuery are omitted: their Inspector paths already
# surface comments, and their fallback equivalents are disproportionately
# complex (sys.extended_properties / region-qualified INFORMATION_SCHEMA).
_COMMENT_FALLBACK_SQL = {
    "mysql": (
        "SELECT column_name, column_comment FROM information_schema.columns "
        "WHERE table_name = :table_name{schema_clause}",
        " AND table_schema = :schema",
        " AND table_schema = DATABASE()",
    ),
    "snowflake": (
        "SELECT column_name, comment FROM information_schema.columns "
        "WHERE table_name = :table_name{schema_clause}",
        " AND table_schema = :schema",
        " AND table_schema = CURRENT_SCHEMA()",
    ),
    "clickhouse": (
        "SELECT name, comment FROM system.columns "
        "WHERE table = :table_name AND database = {schema_clause}",
        ":schema",
        "currentDatabase()",
    ),
    "duckdb": (
        "SELECT column_name, comment FROM duckdb_columns() "
        "WHERE table_name = :table_name{schema_clause}",
        " AND schema_name = :schema",
        " AND schema_name = current_schema()",
    ),
    "postgresql": (
        "SELECT a.attname, col_description(c.oid, a.attnum) "
        "FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid "
        "WHERE c.relname = :table_name AND a.attnum > 0 "
        "AND NOT a.attisdropped{schema_clause}",
        " AND n.nspname = :schema",
        " AND pg_catalog.pg_table_is_visible(c.oid)",
    ),
}
_COMMENT_FALLBACK_SQL["mariadb"] = _COMMENT_FALLBACK_SQL["mysql"]


def _get_column_comments_fallback(
    sa_engine: sa.Engine,
    table_name: str,
    schema: Optional[str],
) -> Dict[str, str]:
    """Column comments for the INFORMATION_SCHEMA fallback path.

    Without an explicit ``schema`` the query is scoped to the connection's
    default/current schema, so a same-named table in another schema can't
    cross-assign its comments. Best-effort: unknown dialects and any query
    failure return ``{}``.
    """
    try:
        dialect_name = getattr(sa_engine.dialect, "name", None)
        entry = _COMMENT_FALLBACK_SQL.get(dialect_name)
        if entry is None:
            return {}
        template, schema_clause, default_clause = entry
        clause = schema_clause if schema else default_clause
        sql = template.format(schema_clause=clause)
        params = {"table_name": table_name}
        if schema:
            params["schema"] = schema
        with sa_engine.connect() as conn:
            rows = conn.execute(sa.text(sql), params).fetchall()
        out: Dict[str, str] = {}
        for name, comment in rows:
            cleaned = _clean_comment(comment)
            if cleaned:
                out[name] = cleaned
        return out
    except Exception:
        return {}


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


def _info_schema_columns_query(
    *,
    sa_engine: sa.Engine,
    table_name: str,
    schema: Optional[str],
) -> tuple[str, Dict]:
    """Build the parameterized INFORMATION_SCHEMA.columns query for one table."""
    source = "information_schema.columns"
    if getattr(getattr(sa_engine, "dialect", None), "name", "") == "bigquery":
        # BigQuery only exposes INFORMATION_SCHEMA per dataset; the bare name
        # resolves to a project-level view a dataset-scoped account cannot read.
        dataset = schema
        if "." in table_name:
            dataset, table_name = table_name.rsplit(".", 1)
        if dataset:
            # sqlglot quotes/escapes the dataset, which is config-supplied.
            source = exp.Table(
                this=exp.to_identifier("COLUMNS"),
                db=exp.to_identifier("INFORMATION_SCHEMA"),
                catalog=exp.to_identifier(dataset, quoted=True),
            ).sql(dialect="bigquery")
            schema = None
    sql = (
        "SELECT column_name, data_type "
        f"FROM {source} "
        "WHERE table_name = :table_name "
    )
    params = {"table_name": table_name}
    if schema:
        sql += "AND table_schema = :schema "
        params["schema"] = schema
    return sql + "ORDER BY ordinal_position", params


def _get_columns_fallback(
    sa_engine: sa.Engine,
    table_name: str,
    schema: Optional[str],
) -> List[Dict]:
    """Get columns via INFORMATION_SCHEMA when Inspector.get_columns() fails."""
    sql, params = _info_schema_columns_query(
        sa_engine=sa_engine, table_name=table_name, schema=schema,
    )
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
    comments = _get_column_comments_fallback(sa_engine, table_name, schema)
    for col in result:
        col["comment"] = comments.get(col["name"])
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
