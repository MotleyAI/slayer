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
from slayer.engine.schema_scope import (
    SchemaRef,
    _dialect_qualifies_tokens,
    default_schema_ref_for_engine,
)

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
# (DEV-1809). Each entry is ``(template, schema_clause, default_clause,
# catalog_clause)``; ``catalog_clause`` is appended (DEV-1758) only when the
# ref carries a catalog, so a same-named table in an attached DuckDB catalog
# can't cross-assign its comments. SQL Server / BigQuery are omitted: their
# Inspector paths already surface comments, and their fallback equivalents are
# disproportionately complex (sys.extended_properties / region-qualified
# INFORMATION_SCHEMA).
_COMMENT_FALLBACK_SQL = {
    "mysql": (
        "SELECT column_name, column_comment FROM information_schema.columns "
        "WHERE table_name = :table_name{schema_clause}{catalog_clause}",
        " AND table_schema = :schema",
        " AND table_schema = DATABASE()",
        " AND table_catalog = :catalog",
    ),
    "snowflake": (
        "SELECT column_name, comment FROM information_schema.columns "
        "WHERE table_name = :table_name{schema_clause}{catalog_clause}",
        " AND table_schema = :schema",
        " AND table_schema = CURRENT_SCHEMA()",
        " AND table_catalog = :catalog",
    ),
    "clickhouse": (
        "SELECT name, comment FROM system.columns "
        "WHERE table = :table_name AND database = {schema_clause}{catalog_clause}",
        ":schema",
        "currentDatabase()",
        "",
    ),
    "duckdb": (
        "SELECT column_name, comment FROM duckdb_columns() "
        "WHERE table_name = :table_name{schema_clause}{catalog_clause}",
        " AND schema_name = :schema",
        " AND schema_name = current_schema()",
        " AND database_name = :catalog",
    ),
    "postgresql": (
        "SELECT a.attname, col_description(c.oid, a.attnum) "
        "FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid "
        "WHERE c.relname = :table_name AND a.attnum > 0 "
        "AND NOT a.attisdropped{schema_clause}{catalog_clause}",
        " AND n.nspname = :schema",
        " AND pg_catalog.pg_table_is_visible(c.oid)",
        "",
    ),
}
_COMMENT_FALLBACK_SQL["mariadb"] = _COMMENT_FALLBACK_SQL["mysql"]


def _resolve_fallback_ref(
    sa_engine: sa.Engine, ref: Optional[SchemaRef]
) -> Optional[SchemaRef]:
    """Resolve a ``ref=None`` request to the connection default on dialects
    whose bare listing would union across attached catalogs (DuckDB). On every
    other dialect ``None`` passes through unchanged (single catalog, no sweep).
    """
    if ref is not None:
        return ref
    dialect_name = getattr(getattr(sa_engine, "dialect", None), "name", None)
    if _dialect_qualifies_tokens(dialect_name):
        try:
            return default_schema_ref_for_engine(sa_engine)
        except Exception:  # noqa: BLE001 — can't resolve (e.g. mock) → no filter
            return None
    return None


def _get_column_comments_fallback(
    sa_engine: sa.Engine,
    table_name: str,
    ref: Optional[SchemaRef],
) -> Dict[str, str]:
    """Column comments for the INFORMATION_SCHEMA fallback path.

    Scoped to ``ref``'s schema (and catalog, where the ref carries one), so a
    same-named table in another schema / attached catalog can't cross-assign
    its comments. Best-effort: unknown dialects and any query failure return
    ``{}``.

    Unlike the column fallback, a ``ref=None`` request is NOT resolved to a
    live default here — the per-dialect ``default_clause`` (``current_schema()``
    / ``DATABASE()``) already scopes to the connection default without a query.
    The real ingestion path always calls this with the resolved ref anyway
    (from ``_get_columns_fallback``), so the catalog predicate still applies.
    """
    try:
        schema = ref.name if ref else None
        catalog = ref.catalog if ref else None
        dialect_name = getattr(sa_engine.dialect, "name", None)
        entry = _COMMENT_FALLBACK_SQL.get(dialect_name)
        if entry is None:
            return {}
        template, schema_clause, default_clause, catalog_clause = entry
        clause = schema_clause if schema else default_clause
        cat_clause = catalog_clause if catalog else ""
        sql = template.format(schema_clause=clause, catalog_clause=cat_clause)
        params = {"table_name": table_name}
        if schema:
            params["schema"] = schema
        if catalog:
            params["catalog"] = catalog
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
    catalog: Optional[str] = None,
) -> tuple[str, Dict]:
    """Build the parameterized INFORMATION_SCHEMA.columns query for one table.

    The ``table_catalog`` predicate is added (DEV-1758) only when ``catalog``
    is supplied — a same-named table in an attached DuckDB catalog must not
    union its columns into this one.
    """
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
            catalog = None
    sql = (
        "SELECT column_name, data_type "
        f"FROM {source} "
        "WHERE table_name = :table_name "
    )
    params = {"table_name": table_name}
    if schema:
        sql += "AND table_schema = :schema "
        params["schema"] = schema
    if catalog:
        sql += "AND table_catalog = :catalog "
        params["catalog"] = catalog
    return sql + "ORDER BY ordinal_position", params


def _info_schema_type(data_type_str: str) -> tuple[DataType, bool]:
    """Map an INFORMATION_SCHEMA type string to ``(DataType, is_float)``.

    NUMERIC/DECIMAL resolve float-vs-integer by scale (DEV-1361). Substring
    branches are ordered widest-match-last: ``FLOATING POINT`` and ``INTERVAL``
    both contain ``INT`` but must not read as INT.
    """
    # Strip precision info (e.g. "DECIMAL(10,2)" → "DECIMAL").
    base = data_type_str.split("(")[0].upper().strip()
    mapped = _INFO_SCHEMA_TYPE_MAP.get(base)
    if mapped is not None:
        return mapped, base in _FLOAT_LIKE_INFO_SCHEMA_TYPES
    if "DECIMAL" in base or "NUMERIC" in base:
        return DataType.DOUBLE, _parse_info_schema_is_float(data_type_str)
    if "DOUBLE" in base or "FLOAT" in base or "REAL" in base:
        # e.g. Postgres "DOUBLE PRECISION".
        return DataType.DOUBLE, True
    if "TIMESTAMP" in base or "DATETIME" in base:
        return DataType.TIMESTAMP, False
    if "INTERVAL" in base:
        return DataType.TEXT, False  # no interval DataType; groupable as TEXT
    if "INT" in base:
        return DataType.INT, False
    if "CHAR" in base or "TEXT" in base:
        return DataType.TEXT, False
    return DataType.TEXT, False


def _get_columns_fallback(
    sa_engine: sa.Engine,
    table_name: str,
    ref: Optional[SchemaRef],
) -> List[Dict]:
    """Get columns via INFORMATION_SCHEMA when Inspector.get_columns() fails.

    Scoped to ``ref``'s schema and (where present) catalog, so an attached
    catalog's same-named table never unions in. A ``ref=None`` request is
    resolved to the connection default on catalog-qualifying dialects, so the
    bare listing never sweeps another catalog.
    """
    ref = _resolve_fallback_ref(sa_engine, ref)
    schema = ref.name if ref else None
    catalog = ref.catalog if ref else None
    sql, params = _info_schema_columns_query(
        sa_engine=sa_engine, table_name=table_name, schema=schema, catalog=catalog,
    )
    with sa_engine.connect() as conn:
        rows = conn.execute(sa.text(sql), params).fetchall()
    result = []
    for col_name, data_type_str in rows:
        sa_type, is_float = _info_schema_type(data_type_str)
        base_type = data_type_str.split("(")[0].upper().strip()
        db_type = data_type_str if "DECIMAL" in base_type or "NUMERIC" in base_type else None
        result.append({"name": col_name, "type": sa_type, "is_float": is_float, "db_type": db_type})
    comments = _get_column_comments_fallback(
        sa_engine=sa_engine, table_name=table_name, ref=ref,
    )
    for col in result:
        col["comment"] = comments.get(col["name"])
    return result


def _safe_get_columns(
    inspector: sa.engine.Inspector,
    sa_engine: sa.Engine,
    table_name: str,
    ref: Optional[SchemaRef],
) -> List[Dict]:
    """Get columns, falling back to INFORMATION_SCHEMA on failure.

    ``ref`` carries the catalog-qualified schema identity: the Inspector call
    uses its token, and the fallback scopes to its schema + catalog so an
    attached catalog's same-named table never unions in.
    """
    try:
        return inspector.get_columns(table_name, schema=ref.token if ref else None)
    except Exception:
        return _get_columns_fallback(
            sa_engine=sa_engine, table_name=table_name, ref=ref,
        )
