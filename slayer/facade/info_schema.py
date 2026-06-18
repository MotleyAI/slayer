"""INFORMATION_SCHEMA.* responses built from a FacadeCatalog (DEV-1390 §6.3).

Five tables are served:

* ``INFORMATION_SCHEMA.METRICS`` — modelled on dbt-SL's metric registry,
  one row per (catalog, schema, table, metric).
* ``INFORMATION_SCHEMA.DIMENSIONS`` — one row per (catalog, schema, table,
  dimension), with the SLayer-specific ``is_time`` flag.
* ``INFORMATION_SCHEMA.SCHEMATA`` — one row per registered datasource.
* ``INFORMATION_SCHEMA.TABLES`` — Postgres-shaped (essential columns only).
* ``INFORMATION_SCHEMA.COLUMNS`` — Postgres-shaped, flattens both metrics
  and dimensions into "columns" since that's the schema-y view a BI tool
  introspecting via a wire-facade driver sees.

Phase 1 does not apply ``WHERE`` predicates server-side, nor does it
slice the canned table by the ``SELECT`` projection — the full table
is returned and BI tools / clients filter client-side. Tracked in
DEV-1425.

Returns a ``RowBatch`` (pyarrow-free); each facade renders it into its
own wire format.
"""

from __future__ import annotations

from typing import List, Optional

import sqlglot.expressions as exp

from slayer.core.enums import DataType
from slayer.facade.catalog import (
    CATALOG_NAME,
    FacadeCatalog,
    local_dimensions,
    local_metrics,
)
from slayer.facade.datatypes import datatype_to_jdbc
from slayer.facade.rows import FacadeColumn, RowBatch

SUPPORTED_INFO_SCHEMA_TABLES = frozenset({
    "METRICS",
    "DIMENSIONS",
    "SCHEMATA",
    "TABLES",
    "COLUMNS",
})

# Pre-lowered for the case-insensitive catalog qualifier compare on every parse.
_CATALOG_NAME_LOWER = CATALOG_NAME.lower()


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
        if catalog_name.lower() != _CATALOG_NAME_LOWER:
            return None
    table_name = str(table.this.this) if hasattr(table.this, "this") else str(table.this)
    table_name_upper = table_name.upper()
    if table_name_upper not in SUPPORTED_INFO_SCHEMA_TABLES:
        return None
    return table_name_upper


def match_info_schema(
    *, parsed: exp.Expression, catalog: FacadeCatalog,
) -> Optional[RowBatch]:
    """Return the canned ``INFORMATION_SCHEMA.<table>`` answer or ``None``."""
    table_name = _is_information_schema_from(parsed)
    if table_name is None:
        return None
    return _serve(table=table_name, catalog=catalog)


def _serve(*, table: str, catalog: FacadeCatalog) -> RowBatch:
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


def _serve_metrics(*, catalog: FacadeCatalog) -> RowBatch:
    columns = [
        FacadeColumn(name="catalog_name", type=DataType.TEXT),
        FacadeColumn(name="schema_name", type=DataType.TEXT),
        FacadeColumn(name="table_name", type=DataType.TEXT),
        FacadeColumn(name="metric_name", type=DataType.TEXT),
        FacadeColumn(name="description", type=DataType.TEXT),
        FacadeColumn(name="data_type", type=DataType.TEXT),
        FacadeColumn(name="label", type=DataType.TEXT),
    ]
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
    return RowBatch(columns=columns, rows=rows)


def _serve_dimensions(*, catalog: FacadeCatalog) -> RowBatch:
    columns = [
        FacadeColumn(name="catalog_name", type=DataType.TEXT),
        FacadeColumn(name="schema_name", type=DataType.TEXT),
        FacadeColumn(name="table_name", type=DataType.TEXT),
        FacadeColumn(name="dimension_name", type=DataType.TEXT),
        FacadeColumn(name="description", type=DataType.TEXT),
        FacadeColumn(name="data_type", type=DataType.TEXT),
        FacadeColumn(name="label", type=DataType.TEXT),
        FacadeColumn(name="is_time", type=DataType.BOOLEAN),
    ]
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
    return RowBatch(columns=columns, rows=rows)


def _serve_schemata(*, catalog: FacadeCatalog) -> RowBatch:
    columns = [
        FacadeColumn(name="catalog_name", type=DataType.TEXT),
        FacadeColumn(name="schema_name", type=DataType.TEXT),
    ]
    rows = [
        {"catalog_name": catalog.catalog_name, "schema_name": sch.name}
        for sch in catalog.schemas
    ]
    return RowBatch(columns=columns, rows=rows)


def _serve_tables(*, catalog: FacadeCatalog) -> RowBatch:
    columns = [
        FacadeColumn(name="table_catalog", type=DataType.TEXT),
        FacadeColumn(name="table_schema", type=DataType.TEXT),
        FacadeColumn(name="table_name", type=DataType.TEXT),
        FacadeColumn(name="table_type", type=DataType.TEXT),
    ]
    rows: List[dict] = []
    for sch in catalog.schemas:
        for tbl in sch.tables:
            rows.append({
                "table_catalog": catalog.catalog_name,
                "table_schema": sch.name,
                "table_name": tbl.name,
                "table_type": tbl.table_type,
            })
    return RowBatch(columns=columns, rows=rows)


def _serve_columns(*, catalog: FacadeCatalog) -> RowBatch:
    """One row per metric AND per dimension on every table, flattened
    into the JDBC ``COLUMNS`` shape. BI tools introspecting a "table"
    via the wire-facade driver see this as the column list of the
    underlying semantic model.

    DEV-1567: cross-model entries are excluded — they leak as dotted
    "columns" that Metabase / dbt fingerprint scans then project, landing
    a dotted name in ``SlayerQuery.measures[*].name`` (Pydantic rejects
    the dot). Catalog-namespaced surfaces (``INFORMATION_SCHEMA.METRICS``
    / ``.DIMENSIONS``) and the catalog SQL fingerprint hash continue to
    use the raw ``tbl.metrics`` / ``tbl.dimensions``.
    """
    columns = [
        FacadeColumn(name="table_catalog", type=DataType.TEXT),
        FacadeColumn(name="table_schema", type=DataType.TEXT),
        FacadeColumn(name="table_name", type=DataType.TEXT),
        FacadeColumn(name="column_name", type=DataType.TEXT),
        FacadeColumn(name="ordinal_position", type=DataType.INT),
        FacadeColumn(name="data_type", type=DataType.TEXT),
        FacadeColumn(name="is_nullable", type=DataType.TEXT),  # Postgres YES/NO
        FacadeColumn(name="column_kind", type=DataType.TEXT),  # METRIC / DIMENSION
    ]
    rows: List[dict] = []
    for sch in catalog.schemas:
        for tbl in sch.tables:
            position = 1
            for d in local_dimensions(tbl):
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
            for m in local_metrics(tbl):
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
    return RowBatch(columns=columns, rows=rows)


__all__ = [
    "CATALOG_NAME",
    "SUPPORTED_INFO_SCHEMA_TABLES",
    "match_info_schema",
]
