"""pg_catalog.* responses for the Postgres facade (DEV-1486).

Phase 1 implements the minimum-viable set BI tools query while enumerating
schemas / tables / columns / types: ``pg_namespace``, ``pg_class``,
``pg_attribute``, ``pg_type``, ``pg_proc``, ``pg_settings``. Both
``pg_catalog.<table>`` and the bare ``<table>`` (search_path) forms resolve.

Phase 1 ignores ``WHERE`` (returns every row; the client filters in memory)
and ignores the SELECT projection (returns every column), mirroring the
shared INFORMATION_SCHEMA approach. Only the six built-in type OIDs are ever
emitted, so a client never has to introspect an unknown type.

OIDs are deterministic — ``zlib.crc32`` over a namespaced ``<ds>.<model>`` /
``<ds>.<model>.<column>`` string — so they're stable across server restarts
(unlike the per-process-salted builtin ``hash``). A collision check runs at
build time.
"""

from __future__ import annotations

import zlib
from typing import Dict, Optional, Tuple

import sqlglot.expressions as exp

from slayer.core.enums import DataType
from slayer.facade.catalog import FacadeCatalog, FacadeTable
from slayer.facade.rows import FacadeColumn, RowBatch
from slayer.pg_facade.identity import PG_SERVER_VERSION
from slayer.pg_facade.types import datatype_to_oid
from slayer.pg_facade.protocol import (
    OID_BOOL,
    OID_DATE,
    OID_FLOAT8,
    OID_INT8,
    OID_TEXT,
    OID_TIMESTAMP,
)

# The fixed OID for the single exposed namespace, matching Postgres's
# well-known `public` schema OID (2200) closely enough for BI introspection.
PUBLIC_NAMESPACE_OID = 2200
PG_CATALOG_NAMESPACE_OID = 11
DEFAULT_OWNER_OID = 10

SUPPORTED_PG_CATALOG_TABLES = frozenset({
    "pg_namespace",
    "pg_class",
    "pg_attribute",
    "pg_type",
    "pg_proc",
    "pg_settings",
})

# Per-OID metadata for pg_type / pg_attribute: (typname, typlen, typcategory).
_TYPE_META: Dict[int, Tuple[str, int, str]] = {
    OID_BOOL: ("bool", 1, "B"),
    OID_INT8: ("int8", 8, "N"),
    OID_TEXT: ("text", -1, "S"),
    OID_FLOAT8: ("float8", 8, "N"),
    OID_DATE: ("date", 4, "D"),
    OID_TIMESTAMP: ("timestamp", 8, "D"),
}


def stable_oid(*parts: str) -> int:
    """Deterministic positive 31-bit OID from a namespaced identifier."""
    key = ".".join(parts).encode("utf-8")
    return zlib.crc32(key) & 0x7FFFFFFF


def _pg_catalog_table(node: exp.Expression) -> Optional[str]:
    """If ``node`` is ``SELECT ... FROM [pg_catalog.]<supported>``, return the
    lowercased table name; else ``None``."""
    if not isinstance(node, exp.Select):
        return None
    from_clause = node.args.get("from_")
    if from_clause is None:
        return None
    table = from_clause.this
    if not isinstance(table, exp.Table):
        return None
    schema_part = table.args.get("db")
    if schema_part is not None:
        schema_name = (
            str(schema_part.this) if hasattr(schema_part, "this") else str(schema_part)
        )
        if schema_name.lower() != "pg_catalog":
            return None
    name = str(table.this.this) if hasattr(table.this, "this") else str(table.this)
    name_lower = name.lower()
    if name_lower not in SUPPORTED_PG_CATALOG_TABLES:
        return None
    return name_lower


def match_pg_catalog(parsed: exp.Expression, catalog: FacadeCatalog) -> Optional[RowBatch]:
    """Return the canned ``pg_catalog.<table>`` answer or ``None``.

    Signature matches the translator's ``CatalogMatcher`` protocol so it can be
    injected via ``translate(..., catalog_matchers=[match_pg_catalog])``.
    """
    table_name = _pg_catalog_table(parsed)
    if table_name is None:
        return None
    builders = {
        "pg_namespace": _serve_pg_namespace,
        "pg_class": _serve_pg_class,
        "pg_attribute": _serve_pg_attribute,
        "pg_type": _serve_pg_type,
        "pg_proc": _serve_pg_proc,
        "pg_settings": _serve_pg_settings,
    }
    return builders[table_name](catalog)


def _all_tables(catalog: FacadeCatalog):
    """Yield ``(datasource, FacadeTable)`` for every table in the catalog."""
    for sch in catalog.schemas:
        for tbl in sch.tables:
            yield sch.name, tbl


def _table_oid(datasource: str, table: FacadeTable) -> int:
    return stable_oid(datasource, table.name)


def _column_specs(table: FacadeTable):
    """Yield ``(name, DataType)`` for every projectable column (dims + metrics)."""
    for d in table.dimensions:
        yield d.name, d.data_type
    for m in table.metrics:
        yield m.name, m.data_type if m.data_type is not None else DataType.TEXT


def _serve_pg_namespace(catalog: FacadeCatalog) -> RowBatch:  # noqa: ARG001
    columns = [
        FacadeColumn(name="oid", type=DataType.INT),
        FacadeColumn(name="nspname", type=DataType.TEXT),
        FacadeColumn(name="nspowner", type=DataType.INT),
        FacadeColumn(name="nspacl", type=DataType.TEXT),
    ]
    rows = [
        {
            "oid": PUBLIC_NAMESPACE_OID,
            "nspname": "public",
            "nspowner": DEFAULT_OWNER_OID,
            "nspacl": None,
        },
        {
            # Builtin types live here (pg_type.typnamespace == 11); without
            # this row a join from pg_type back to pg_namespace dangles.
            "oid": PG_CATALOG_NAMESPACE_OID,
            "nspname": "pg_catalog",
            "nspowner": DEFAULT_OWNER_OID,
            "nspacl": None,
        },
    ]
    return RowBatch(columns=columns, rows=rows)


def _serve_pg_class(catalog: FacadeCatalog) -> RowBatch:
    columns = [
        FacadeColumn(name="oid", type=DataType.INT),
        FacadeColumn(name="relname", type=DataType.TEXT),
        FacadeColumn(name="relnamespace", type=DataType.INT),
        FacadeColumn(name="reltype", type=DataType.INT),
        FacadeColumn(name="relowner", type=DataType.INT),
        FacadeColumn(name="relkind", type=DataType.TEXT),
        FacadeColumn(name="relnatts", type=DataType.INT),
        FacadeColumn(name="relhasindex", type=DataType.BOOLEAN),
        FacadeColumn(name="relpersistence", type=DataType.TEXT),
        FacadeColumn(name="relpages", type=DataType.INT),
        FacadeColumn(name="reltuples", type=DataType.DOUBLE),
        FacadeColumn(name="relhasrules", type=DataType.BOOLEAN),
        FacadeColumn(name="relhastriggers", type=DataType.BOOLEAN),
        FacadeColumn(name="relrowsecurity", type=DataType.BOOLEAN),
        FacadeColumn(name="relispartition", type=DataType.BOOLEAN),
    ]
    rows = []
    seen_oids: Dict[int, str] = {}
    for ds, tbl in _all_tables(catalog):
        oid = _table_oid(ds, tbl)
        _check_collision(seen_oids, oid, f"{ds}.{tbl.name}")
        natts = sum(1 for _ in _column_specs(tbl))
        rows.append({
            "oid": oid,
            "relname": tbl.name,
            "relnamespace": PUBLIC_NAMESPACE_OID,
            "reltype": 0,
            "relowner": DEFAULT_OWNER_OID,
            "relkind": "r",
            "relnatts": natts,
            "relhasindex": False,
            "relpersistence": "p",
            "relpages": 0,
            "reltuples": -1.0,
            "relhasrules": False,
            "relhastriggers": False,
            "relrowsecurity": False,
            "relispartition": False,
        })
    return RowBatch(columns=columns, rows=rows)


def _serve_pg_attribute(catalog: FacadeCatalog) -> RowBatch:
    columns = [
        FacadeColumn(name="attrelid", type=DataType.INT),
        FacadeColumn(name="attname", type=DataType.TEXT),
        FacadeColumn(name="atttypid", type=DataType.INT),
        FacadeColumn(name="attnum", type=DataType.INT),
        FacadeColumn(name="attlen", type=DataType.INT),
        FacadeColumn(name="atttypmod", type=DataType.INT),
        FacadeColumn(name="attnotnull", type=DataType.BOOLEAN),
        FacadeColumn(name="atthasdef", type=DataType.BOOLEAN),
        FacadeColumn(name="attisdropped", type=DataType.BOOLEAN),
        FacadeColumn(name="attidentity", type=DataType.TEXT),
        FacadeColumn(name="attgenerated", type=DataType.TEXT),
    ]
    rows = []
    for ds, tbl in _all_tables(catalog):
        attrelid = _table_oid(ds, tbl)
        attnum = 1
        for name, data_type in _column_specs(tbl):
            oid = datatype_to_oid(data_type)
            rows.append({
                "attrelid": attrelid,
                "attname": name,
                "atttypid": oid,
                "attnum": attnum,
                "attlen": _TYPE_META[oid][1],
                "atttypmod": -1,
                "attnotnull": False,
                "atthasdef": False,
                "attisdropped": False,
                "attidentity": "",
                "attgenerated": "",
            })
            attnum += 1
    return RowBatch(columns=columns, rows=rows)


def _serve_pg_type(catalog: FacadeCatalog) -> RowBatch:  # noqa: ARG001
    columns = [
        FacadeColumn(name="oid", type=DataType.INT),
        FacadeColumn(name="typname", type=DataType.TEXT),
        FacadeColumn(name="typnamespace", type=DataType.INT),
        FacadeColumn(name="typlen", type=DataType.INT),
        FacadeColumn(name="typtype", type=DataType.TEXT),
        FacadeColumn(name="typcategory", type=DataType.TEXT),
        FacadeColumn(name="typisdefined", type=DataType.BOOLEAN),
        FacadeColumn(name="typdelim", type=DataType.TEXT),
        FacadeColumn(name="typrelid", type=DataType.INT),
        FacadeColumn(name="typelem", type=DataType.INT),
        FacadeColumn(name="typarray", type=DataType.INT),
    ]
    rows = []
    for oid, (typname, typlen, typcategory) in _TYPE_META.items():
        rows.append({
            "oid": oid,
            "typname": typname,
            "typnamespace": PG_CATALOG_NAMESPACE_OID,
            "typlen": typlen,
            "typtype": "b",
            "typcategory": typcategory,
            "typisdefined": True,
            "typdelim": ",",
            "typrelid": 0,
            "typelem": 0,
            "typarray": 0,
        })
    return RowBatch(columns=columns, rows=rows)


def _serve_pg_proc(catalog: FacadeCatalog) -> RowBatch:  # noqa: ARG001
    columns = [
        FacadeColumn(name="oid", type=DataType.INT),
        FacadeColumn(name="proname", type=DataType.TEXT),
        FacadeColumn(name="pronamespace", type=DataType.INT),
        FacadeColumn(name="prorettype", type=DataType.INT),
    ]
    return RowBatch(columns=columns, rows=[])


def _serve_pg_settings(catalog: FacadeCatalog) -> RowBatch:  # noqa: ARG001
    columns = [
        FacadeColumn(name="name", type=DataType.TEXT),
        FacadeColumn(name="setting", type=DataType.TEXT),
        FacadeColumn(name="category", type=DataType.TEXT),
        FacadeColumn(name="unit", type=DataType.TEXT),
        FacadeColumn(name="source", type=DataType.TEXT),
        FacadeColumn(name="vartype", type=DataType.TEXT),
        FacadeColumn(name="context", type=DataType.TEXT),
        FacadeColumn(name="min_val", type=DataType.TEXT),
        FacadeColumn(name="max_val", type=DataType.TEXT),
    ]
    settings = [
        ("server_version", PG_SERVER_VERSION),
        ("client_encoding", "UTF8"),
        ("server_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("IntervalStyle", "postgres"),
        ("TimeZone", "UTC"),
        ("standard_conforming_strings", "on"),
        ("integer_datetimes", "on"),
        ("max_index_keys", "32"),
        ("block_size", "8192"),
    ]
    rows = [{
        "name": name,
        "setting": value,
        "category": "Preset Options",
        "unit": None,
        "source": "default",
        "vartype": "string",
        "context": "user",
        "min_val": None,
        "max_val": None,
    } for name, value in settings]
    return RowBatch(columns=columns, rows=rows)


def _check_collision(seen: Dict[int, str], oid: int, key: str) -> None:
    prior = seen.get(oid)
    if prior is not None and prior != key:
        raise ValueError(
            f"pg_catalog OID collision: {key!r} and {prior!r} both hash to {oid}"
        )
    seen[oid] = key


__all__ = [
    "SUPPORTED_PG_CATALOG_TABLES",
    "match_pg_catalog",
    "stable_oid",
]
