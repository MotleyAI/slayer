"""INFORMATION_SCHEMA.* responses built from a FlightCatalog (DEV-1390 §6.3).

Five tables are served:

* ``INFORMATION_SCHEMA.METRICS`` — modelled on dbt-SL's metric registry,
  one row per (catalog, schema, table, metric).
* ``INFORMATION_SCHEMA.DIMENSIONS`` — one row per (catalog, schema, table,
  dimension), with the SLayer-specific ``is_time`` flag.
* ``INFORMATION_SCHEMA.SCHEMATA`` — one row per registered datasource.
* ``INFORMATION_SCHEMA.TABLES`` — Postgres-shaped (essential columns only).
* ``INFORMATION_SCHEMA.COLUMNS`` — Postgres-shaped, flattens both metrics
  and dimensions into "columns" since that's the schema-y view a BI tool
  introspecting via the dbt-SL JDBC driver sees.

Phase 1 does not apply ``WHERE`` predicates server-side, nor does it
slice the canned table by the ``SELECT`` projection — the full table
is returned and BI tools / clients filter client-side. Tracked in
DEV-1425.
"""

from __future__ import annotations

from typing import List, Optional

import pyarrow as pa
import sqlglot.expressions as exp

from slayer.flight.catalog import CATALOG_NAME, FlightCatalog
from slayer.flight.types import datatype_to_jdbc

SUPPORTED_INFO_SCHEMA_TABLES = frozenset({
    "METRICS",
    "DIMENSIONS",
    "SCHEMATA",
    "TABLES",
    "COLUMNS",
})


def _is_information_schema_from(node: exp.Expression) -> Optional[str]:
    """If ``node`` is ``SELECT ... FROM information_schema.<TABLE>``,
    return the uppercased table name; else ``None``.

    Matches:
    * bare: ``FROM INFORMATION_SCHEMA.METRICS``
    * catalog-qualified: ``FROM slayer.INFORMATION_SCHEMA.METRICS``
    * case-insensitive on schema and table names.
    """
    if not isinstance(node, exp.Select):
        return None
    from_clause = node.args.get("from_")
    if from_clause is None:
        return None
    table = from_clause.this
    if not isinstance(table, exp.Table):
        return None
    # `db` is the schema portion in sqlglot's Table representation.
    schema_part = table.args.get("db")
    if schema_part is None:
        return None
    schema_name = str(schema_part.this) if hasattr(schema_part, "this") else str(schema_part)
    if schema_name.lower() != "information_schema":
        return None
    # Catalog-qualified form must name the SLayer catalog. Anything else is a
    # user mistake; return None so a typo'd catalog raises "Unknown catalog"
    # in the regular table-resolution path rather than silently returning
    # SLayer metadata under a foreign-catalog query. Matched case-insensitively
    # to stay consistent with the schema / table comparisons above.
    catalog_part = table.args.get("catalog")
    if catalog_part is not None:
        catalog_name = (
            str(catalog_part.this) if hasattr(catalog_part, "this") else str(catalog_part)
        )
        if catalog_name.lower() != CATALOG_NAME.lower():
            return None
    table_name = str(table.this.this) if hasattr(table.this, "this") else str(table.this)
    table_name_upper = table_name.upper()
    if table_name_upper not in SUPPORTED_INFO_SCHEMA_TABLES:
        return None
    return table_name_upper


def match_info_schema(
    *, parsed: exp.Expression, catalog: FlightCatalog,
) -> Optional[pa.Table]:
    """Return the canned ``INFORMATION_SCHEMA.<table>`` answer or ``None``."""
    table_name = _is_information_schema_from(parsed)
    if table_name is None:
        return None
    return _serve(table=table_name, catalog=catalog)


def _serve(*, table: str, catalog: FlightCatalog) -> pa.Table:
    if table == "METRICS":
        return _serve_metrics(catalog=catalog)
    if table == "DIMENSIONS":
        return _serve_dimensions(catalog=catalog)
    if table == "SCHEMATA":
        return _serve_schemata(catalog=catalog)
    if table == "TABLES":
        return _serve_tables(catalog=catalog)
    if table == "COLUMNS":
        return _serve_columns(catalog=catalog)
    raise KeyError(f"Unsupported INFORMATION_SCHEMA table: {table!r}")


def _serve_metrics(*, catalog: FlightCatalog) -> pa.Table:
    schema = pa.schema([
        pa.field("catalog_name", pa.utf8()),
        pa.field("schema_name", pa.utf8()),
        pa.field("table_name", pa.utf8()),
        pa.field("metric_name", pa.utf8()),
        pa.field("description", pa.utf8()),
        pa.field("data_type", pa.utf8()),
        pa.field("label", pa.utf8()),
    ])
    rows: List[dict] = []
    for sch in catalog.schemas:
        for tbl in sch.tables:
            for m in tbl.metrics:
                rows.append({
                    "catalog_name": catalog.catalog_name,
                    "schema_name": sch.name,
                    "table_name": tbl.name,
                    "metric_name": m.name,
                    "description": m.description,
                    "data_type": datatype_to_jdbc(m.data_type) if m.data_type else None,
                    "label": m.label,
                })
    return pa.Table.from_pylist(rows, schema=schema)


def _serve_dimensions(*, catalog: FlightCatalog) -> pa.Table:
    schema = pa.schema([
        pa.field("catalog_name", pa.utf8()),
        pa.field("schema_name", pa.utf8()),
        pa.field("table_name", pa.utf8()),
        pa.field("dimension_name", pa.utf8()),
        pa.field("description", pa.utf8()),
        pa.field("data_type", pa.utf8()),
        pa.field("label", pa.utf8()),
        pa.field("is_time", pa.bool_()),
    ])
    rows: List[dict] = []
    for sch in catalog.schemas:
        for tbl in sch.tables:
            for d in tbl.dimensions:
                rows.append({
                    "catalog_name": catalog.catalog_name,
                    "schema_name": sch.name,
                    "table_name": tbl.name,
                    "dimension_name": d.name,
                    "description": d.description,
                    "data_type": datatype_to_jdbc(d.data_type),
                    "label": d.label,
                    "is_time": d.is_time,
                })
    return pa.Table.from_pylist(rows, schema=schema)


def _serve_schemata(*, catalog: FlightCatalog) -> pa.Table:
    schema = pa.schema([
        pa.field("catalog_name", pa.utf8()),
        pa.field("schema_name", pa.utf8()),
    ])
    rows = [
        {"catalog_name": catalog.catalog_name, "schema_name": sch.name}
        for sch in catalog.schemas
    ]
    return pa.Table.from_pylist(rows, schema=schema)


def _serve_tables(*, catalog: FlightCatalog) -> pa.Table:
    schema = pa.schema([
        pa.field("table_catalog", pa.utf8()),
        pa.field("table_schema", pa.utf8()),
        pa.field("table_name", pa.utf8()),
        pa.field("table_type", pa.utf8()),
    ])
    rows: List[dict] = []
    for sch in catalog.schemas:
        for tbl in sch.tables:
            rows.append({
                "table_catalog": catalog.catalog_name,
                "table_schema": sch.name,
                "table_name": tbl.name,
                "table_type": tbl.table_type,
            })
    return pa.Table.from_pylist(rows, schema=schema)


def _serve_columns(*, catalog: FlightCatalog) -> pa.Table:
    """One row per metric AND per dimension on every table, flattened
    into the JDBC ``COLUMNS`` shape. BI tools introspecting a "table"
    via the JDBC driver see this as the column list of the underlying
    semantic model.
    """
    schema = pa.schema([
        pa.field("table_catalog", pa.utf8()),
        pa.field("table_schema", pa.utf8()),
        pa.field("table_name", pa.utf8()),
        pa.field("column_name", pa.utf8()),
        pa.field("ordinal_position", pa.int64()),
        pa.field("data_type", pa.utf8()),
        pa.field("is_nullable", pa.utf8()),  # Postgres uses YES/NO strings here
        pa.field("column_kind", pa.utf8()),  # SLayer extension: METRIC / DIMENSION
    ])
    rows: List[dict] = []
    for sch in catalog.schemas:
        for tbl in sch.tables:
            position = 1
            for d in tbl.dimensions:
                rows.append({
                    "table_catalog": catalog.catalog_name,
                    "table_schema": sch.name,
                    "table_name": tbl.name,
                    "column_name": d.name,
                    "ordinal_position": position,
                    "data_type": datatype_to_jdbc(d.data_type),
                    "is_nullable": "YES",
                    "column_kind": "DIMENSION",
                })
                position += 1
            for m in tbl.metrics:
                rows.append({
                    "table_catalog": catalog.catalog_name,
                    "table_schema": sch.name,
                    "table_name": tbl.name,
                    "column_name": m.name,
                    "ordinal_position": position,
                    "data_type": (
                        datatype_to_jdbc(m.data_type) if m.data_type else None
                    ),
                    "is_nullable": "YES",
                    "column_kind": "METRIC",
                })
                position += 1
    return pa.Table.from_pylist(rows, schema=schema)


# Silence pyflakes — re-export of CATALOG_NAME from catalog is documented.
__all__ = [
    "CATALOG_NAME",
    "SUPPORTED_INFO_SCHEMA_TABLES",
    "match_info_schema",
]
