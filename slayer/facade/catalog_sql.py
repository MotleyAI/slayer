"""DuckDB-backed catalog SQL executor.

Materialises the catalog corpus as in-memory DuckDB tables and runs catalog
queries against it: pre-rewrite the AST (strip catalog qualifiers, resolve
``::regclass``/``::regtype``, substitute session functions, rename stub
functions to ``_slayer_*`` so DuckDB built-ins can't shadow them), transpile
postgres → duckdb, execute, map to ``RowBatch``. ``executor_for`` caches
executors by catalog fingerprint (FIFO, 4 entries; no lock — single-threaded).
"""

from __future__ import annotations

import collections
import hashlib
import json
import logging
import zlib
from collections.abc import Iterable
from typing import Any

import duckdb
import sqlglot
import sqlglot.expressions as exp
from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType, TypeKind
from slayer.facade.catalog import (
    CATALOG_NAME,
    FacadeCatalog,
    FacadeTable,
    advertised_dimensions,
    local_dimensions,
    local_metrics,
)
from slayer.facade.datatypes import datatype_to_jdbc
from slayer.facade.rows import FacadeColumn, RowBatch
from slayer.pg_facade.identity import PG_SERVER_VERSION
from slayer.pg_facade.protocol import OID_BOOL, OID_DATE, OID_FLOAT8, OID_INT8, OID_TEXT, OID_TIMESTAMP
from slayer.pg_facade.types import datatype_to_oid

logger = logging.getLogger(__name__)


PUBLIC_NAMESPACE_OID = 2200
PG_CATALOG_NAMESPACE_OID = 11
DEFAULT_OWNER_OID = 10

# Real Postgres OIDs, so e.g. ``'pg_class'::regclass`` matches ``pg_description.classoid`` (Metabase).
KNOWN_SYSTEM_OIDS: dict[str, int] = {
    "pg_class": 1259,
    "pg_namespace": 2615,
    "pg_attribute": 1249,
    "pg_type": 1247,
    "pg_proc": 1255,
    "pg_description": 2609,
    "pg_constraint": 2606,
    "pg_index": 2610,
    "pg_attrdef": 2604,
    "pg_am": 2601,
    "pg_database": 1262,
    "pg_authid": 1260,
}

PG_AM_HEAP_OID = 2  # real Postgres heap access-method OID
# Shared-token auth → exactly one principal.
PG_SLAYER_ROLE_OID = 10
PG_SLAYER_ROLE_NAME = "slayer"
PG_ENCODING_UTF8 = 6

# OID → (typname, typlen, typcategory) for the six wire types.
_TYPE_META: dict[int, tuple[str, int, str]] = {
    OID_BOOL: ("bool", 1, "B"),
    OID_INT8: ("int8", 8, "N"),
    OID_TEXT: ("text", -1, "S"),
    OID_FLOAT8: ("float8", 8, "N"),
    OID_DATE: ("date", 4, "D"),
    OID_TIMESTAMP: ("timestamp", 8, "D"),
}

# typname → OID, for resolving ``::regtype`` casts.
_KNOWN_TYPE_OIDS: dict[str, int] = {
    typname: oid for oid, (typname, _len, _cat) in _TYPE_META.items()
}


_UDT_NAME_BY_KIND: dict[TypeKind, str] = {
    TypeKind.BOOLEAN: "bool",
    TypeKind.INT: "int8",
    TypeKind.TEXT: "text",
    TypeKind.DOUBLE: "float8",
    TypeKind.DATE: "date",
    TypeKind.TIMESTAMP: "timestamp",
}
_UDT_NAME_BY_DATATYPE: dict[DataType, str] = {t: _UDT_NAME_BY_KIND[t.kind] for t in DataType if t.kind in _UDT_NAME_BY_KIND}


def stable_oid(*parts: str) -> int:
    """Deterministic positive 31-bit OID from a namespaced identifier."""
    key = ".".join(parts).encode("utf-8")
    return zlib.crc32(key) & 0x7FFFFFFF


class CatalogRelation(BaseModel):
    """One catalog table's content; ``rows`` are ``{column_name: value}`` dicts."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    columns: list[FacadeColumn]
    rows: list[dict[str, Any]]


def build_catalog_relations(
    catalog: FacadeCatalog,
    datasource: str | None = None,
    *,
    extra_relations: Iterable[CatalogRelation] | None = None,
) -> list[CatalogRelation]:
    """Build every catalog table from ``catalog``.

    ``datasource`` (default: the catalog name) fills ``catalog_name``/``table_catalog``
    to match the ``current_database()`` substitution. ``extra_relations`` replace
    same-named default relations in place, or are appended.
    """
    ds = datasource or catalog.catalog_name
    out: list[CatalogRelation] = []
    out.append(_build_pg_namespace(catalog))
    out.append(_build_pg_class(catalog))
    out.append(_build_pg_attribute(catalog))
    out.append(_build_pg_type())
    out.append(_build_pg_proc())
    out.append(_build_pg_settings())
    out.append(_build_pg_description(catalog))
    out.append(_build_pg_stat_user_tables(catalog))
    out.append(_build_pg_enum())
    out.append(_build_pg_tables(catalog))
    out.append(_build_pg_views(catalog))
    out.append(_build_pg_matviews())
    out.append(_build_pg_constraint())
    out.append(_build_pg_index())
    out.append(_build_pg_attrdef())
    out.append(_build_pg_am())
    out.append(_build_pg_database(catalog, ds))
    out.append(_build_pg_roles())
    out.append(_build_pg_collation())
    out.append(_build_pg_policy())
    out.append(_build_pg_trigger())
    out.append(_build_pg_inherits())
    out.append(_build_pg_publication())
    out.append(_build_pg_publication_rel())
    out.append(_build_pg_publication_tables())
    out.append(_build_pg_partitioned_table())
    out.append(_build_pg_rewrite())
    out.append(_build_pg_extension())
    out.append(_build_is_columns(catalog, ds))
    out.append(_build_is_table_constraints())
    out.append(_build_is_key_column_usage())
    out.append(_build_is_schemata(catalog, ds))
    out.append(_build_is_tables(catalog, ds))
    out.append(_build_is_metrics(catalog, ds))
    out.append(_build_is_dimensions(catalog, ds))
    if extra_relations is not None:
        extras = {r.name: r for r in extra_relations}
        out = [extras.pop(r.name, r) for r in out]
        out.extend(extras.values())
    return out


def _all_tables(catalog: FacadeCatalog):
    """Yield ``(datasource, FacadeTable)`` for every table in the catalog."""
    for sch in catalog.schemas:
        for tbl in sch.tables:
            yield sch.name, tbl


def _table_oid(datasource: str, table: FacadeTable) -> int:
    return stable_oid(datasource, table.name)


def _column_specs(table: FacadeTable):
    """Yield ``(name, DataType)`` for every local (same-dataset) dimension and metric."""
    for d in local_dimensions(table):
        yield d.name, d.data_type
    for m in local_metrics(table):
        yield m.name, m.data_type if m.data_type is not None else DataType.STRING


def _namespace_oid(schema: str) -> int:
    """Stable namespace OID; ``public`` keeps Postgres's well-known 2200."""
    if schema == "public":
        return PUBLIC_NAMESPACE_OID
    return stable_oid("namespace", schema)


def _user_schema_names(catalog: FacadeCatalog) -> list[str]:
    """Distinct facade schema names in catalog order (no pg_catalog / info)."""
    seen: dict[str, None] = {}
    for sch in catalog.schemas:
        seen.setdefault(sch.name, None)
    seen.setdefault("public", None)  # always advertise public, even if empty
    return list(seen)


def _build_pg_namespace(catalog: FacadeCatalog) -> CatalogRelation:
    columns = [
        FacadeColumn(name="oid", type=DataType.INTEGER),
        FacadeColumn(name="nspname", type=DataType.STRING),
        FacadeColumn(name="nspowner", type=DataType.INTEGER),
        FacadeColumn(name="nspacl", type=DataType.STRING),
    ]
    rows = [
        {"oid": _namespace_oid(name), "nspname": name,
         "nspowner": DEFAULT_OWNER_OID, "nspacl": None}
        for name in _user_schema_names(catalog)
    ]
    rows.append(
        {"oid": PG_CATALOG_NAMESPACE_OID, "nspname": "pg_catalog",
         "nspowner": DEFAULT_OWNER_OID, "nspacl": None}
    )
    return CatalogRelation(name="pg_namespace", columns=columns, rows=rows)


def _build_pg_class(catalog: FacadeCatalog) -> CatalogRelation:
    columns = [
        FacadeColumn(name="oid", type=DataType.INTEGER),
        FacadeColumn(name="relname", type=DataType.STRING),
        FacadeColumn(name="relnamespace", type=DataType.INTEGER),
        FacadeColumn(name="reltype", type=DataType.INTEGER),
        FacadeColumn(name="relowner", type=DataType.INTEGER),
        FacadeColumn(name="relkind", type=DataType.STRING),
        FacadeColumn(name="relnatts", type=DataType.INTEGER),
        FacadeColumn(name="relhasindex", type=DataType.BOOLEAN),
        FacadeColumn(name="relpersistence", type=DataType.STRING),
        FacadeColumn(name="relpages", type=DataType.INTEGER),
        FacadeColumn(name="reltuples", type=DataType.FLOAT),
        FacadeColumn(name="relhasrules", type=DataType.BOOLEAN),
        FacadeColumn(name="relhastriggers", type=DataType.BOOLEAN),
        FacadeColumn(name="relrowsecurity", type=DataType.BOOLEAN),
        FacadeColumn(name="relispartition", type=DataType.BOOLEAN),
        FacadeColumn(name="relam", type=DataType.INTEGER),  # → pg_am.oid (heap)
        FacadeColumn(name="relchecks", type=DataType.INTEGER),
        FacadeColumn(name="relforcerowsecurity", type=DataType.BOOLEAN),
        FacadeColumn(name="reloftype", type=DataType.INTEGER),
        FacadeColumn(name="reltablespace", type=DataType.INTEGER),
        FacadeColumn(name="reltoastrelid", type=DataType.INTEGER),
        FacadeColumn(name="relreplident", type=DataType.STRING),
    ]
    rows = []
    seen_oids: dict[int, str] = {}
    for ds, tbl in _all_tables(catalog):
        oid = _table_oid(ds, tbl)
        _check_collision(seen_oids, oid, f"{ds}.{tbl.name}")
        natts = sum(1 for _ in _column_specs(tbl))
        relkind = "v" if tbl.table_type == "VIEW" else "r"
        rows.append({
            "oid": oid, "relname": tbl.name,
            "relnamespace": _namespace_oid(ds), "reltype": 0,
            "relowner": DEFAULT_OWNER_OID, "relkind": relkind,
            "relnatts": natts, "relhasindex": False, "relpersistence": "p",
            "relpages": 0, "reltuples": -1.0,
            "relhasrules": False, "relhastriggers": False,
            "relrowsecurity": False, "relispartition": False,
            "relam": PG_AM_HEAP_OID,
            "relchecks": 0, "relforcerowsecurity": False,
            "reloftype": 0, "reltablespace": 0, "reltoastrelid": 0,
            "relreplident": "d",  # 'd' = default replica identity
        })
    return CatalogRelation(name="pg_class", columns=columns, rows=rows)


def _build_pg_attribute(catalog: FacadeCatalog) -> CatalogRelation:
    columns = [
        FacadeColumn(name="attrelid", type=DataType.INTEGER),
        FacadeColumn(name="attname", type=DataType.STRING),
        FacadeColumn(name="atttypid", type=DataType.INTEGER),
        FacadeColumn(name="attnum", type=DataType.INTEGER),
        FacadeColumn(name="attlen", type=DataType.INTEGER),
        FacadeColumn(name="atttypmod", type=DataType.INTEGER),
        FacadeColumn(name="attnotnull", type=DataType.BOOLEAN),
        FacadeColumn(name="atthasdef", type=DataType.BOOLEAN),
        FacadeColumn(name="attisdropped", type=DataType.BOOLEAN),
        FacadeColumn(name="attidentity", type=DataType.STRING),
        FacadeColumn(name="attgenerated", type=DataType.STRING),
        # 0 = default collation: psql's ``\d`` collation sub-select yields NULL.
        FacadeColumn(name="attcollation", type=DataType.INTEGER),
    ]
    rows = []
    for ds, tbl in _all_tables(catalog):
        attrelid = _table_oid(ds, tbl)
        attnum = 1
        for name, data_type in _column_specs(tbl):
            oid = datatype_to_oid(data_type)
            rows.append({
                "attrelid": attrelid, "attname": name, "atttypid": oid,
                "attnum": attnum, "attlen": _TYPE_META[oid][1],
                "atttypmod": -1, "attnotnull": False, "atthasdef": False,
                "attisdropped": False, "attidentity": "", "attgenerated": "",
                "attcollation": 0,
            })
            attnum += 1
    return CatalogRelation(name="pg_attribute", columns=columns, rows=rows)


def _build_pg_type() -> CatalogRelation:
    columns = [
        FacadeColumn(name="oid", type=DataType.INTEGER),
        FacadeColumn(name="typname", type=DataType.STRING),
        FacadeColumn(name="typnamespace", type=DataType.INTEGER),
        FacadeColumn(name="typlen", type=DataType.INTEGER),
        FacadeColumn(name="typtype", type=DataType.STRING),
        FacadeColumn(name="typcategory", type=DataType.STRING),
        FacadeColumn(name="typisdefined", type=DataType.BOOLEAN),
        FacadeColumn(name="typdelim", type=DataType.STRING),
        FacadeColumn(name="typrelid", type=DataType.INTEGER),
        FacadeColumn(name="typelem", type=DataType.INTEGER),
        FacadeColumn(name="typarray", type=DataType.INTEGER),
        FacadeColumn(name="typnotnull", type=DataType.BOOLEAN),
        FacadeColumn(name="typbasetype", type=DataType.INTEGER),
        FacadeColumn(name="typtypmod", type=DataType.INTEGER),
        FacadeColumn(name="typcollation", type=DataType.INTEGER),
    ]
    rows = []
    for oid, (typname, typlen, typcategory) in _TYPE_META.items():
        rows.append({
            "oid": oid, "typname": typname,
            "typnamespace": PG_CATALOG_NAMESPACE_OID,
            "typlen": typlen, "typtype": "b", "typcategory": typcategory,
            "typisdefined": True, "typdelim": ",", "typrelid": 0,
            "typelem": 0, "typarray": 0, "typnotnull": False,
            "typbasetype": 0, "typtypmod": -1,
            "typcollation": 0,
        })
    return CatalogRelation(name="pg_type", columns=columns, rows=rows)


def _build_pg_proc() -> CatalogRelation:
    return CatalogRelation(name="pg_proc", columns=[
        FacadeColumn(name="oid", type=DataType.INTEGER),
        FacadeColumn(name="proname", type=DataType.STRING),
        FacadeColumn(name="pronamespace", type=DataType.INTEGER),
        FacadeColumn(name="prorettype", type=DataType.INTEGER),
    ], rows=[])


def _build_pg_settings() -> CatalogRelation:
    columns = [
        FacadeColumn(name="name", type=DataType.STRING),
        FacadeColumn(name="setting", type=DataType.STRING),
        FacadeColumn(name="category", type=DataType.STRING),
        FacadeColumn(name="unit", type=DataType.STRING),
        FacadeColumn(name="source", type=DataType.STRING),
        FacadeColumn(name="vartype", type=DataType.STRING),
        FacadeColumn(name="context", type=DataType.STRING),
        FacadeColumn(name="min_val", type=DataType.STRING),
        FacadeColumn(name="max_val", type=DataType.STRING),
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
    rows = [{"name": name, "setting": value, "category": "Preset Options",
             "unit": None, "source": "default", "vartype": "string",
             "context": "user", "min_val": None, "max_val": None}
            for name, value in settings]
    return CatalogRelation(name="pg_settings", columns=columns, rows=rows)


def _build_pg_description(catalog: FacadeCatalog) -> CatalogRelation:
    columns = [
        FacadeColumn(name="objoid", type=DataType.INTEGER),
        FacadeColumn(name="classoid", type=DataType.INTEGER),
        FacadeColumn(name="objsubid", type=DataType.INTEGER),
        FacadeColumn(name="description", type=DataType.STRING),
    ]
    rows: list[dict[str, Any]] = []
    for ds, tbl in _all_tables(catalog):
        oid = _table_oid(ds, tbl)
        if tbl.description:
            rows.append({"objoid": oid, "classoid": KNOWN_SYSTEM_OIDS["pg_class"],
                         "objsubid": 0, "description": tbl.description})
        attnum = 1  # mirrors pg_attribute's attnum space
        for d in local_dimensions(tbl):
            if d.description:
                rows.append({"objoid": oid, "classoid": KNOWN_SYSTEM_OIDS["pg_class"],
                             "objsubid": attnum, "description": d.description})
            attnum += 1
        for m in local_metrics(tbl):
            if m.description:
                rows.append({"objoid": oid, "classoid": KNOWN_SYSTEM_OIDS["pg_class"],
                             "objsubid": attnum, "description": m.description})
            attnum += 1
    return CatalogRelation(name="pg_description", columns=columns, rows=rows)


def _build_pg_stat_user_tables(catalog: FacadeCatalog) -> CatalogRelation:
    columns = [
        FacadeColumn(name="schemaname", type=DataType.STRING),
        FacadeColumn(name="relname", type=DataType.STRING),
        FacadeColumn(name="n_live_tup", type=DataType.INTEGER),
    ]
    rows = [{"schemaname": ds, "relname": tbl.name, "n_live_tup": None}
            for ds, tbl in _all_tables(catalog)]
    return CatalogRelation(name="pg_stat_user_tables", columns=columns, rows=rows)


def _build_pg_enum() -> CatalogRelation:
    return CatalogRelation(name="pg_enum", columns=[
        FacadeColumn(name="oid", type=DataType.INTEGER),
        FacadeColumn(name="enumtypid", type=DataType.INTEGER),
        FacadeColumn(name="enumlabel", type=DataType.STRING),
    ], rows=[])


def _build_pg_tables(catalog: FacadeCatalog) -> CatalogRelation:
    columns = [
        FacadeColumn(name="schemaname", type=DataType.STRING),
        FacadeColumn(name="tablename", type=DataType.STRING),
        FacadeColumn(name="tableowner", type=DataType.STRING),
        FacadeColumn(name="tablespace", type=DataType.STRING),
        FacadeColumn(name="hasindexes", type=DataType.BOOLEAN),
        FacadeColumn(name="hasrules", type=DataType.BOOLEAN),
        FacadeColumn(name="hastriggers", type=DataType.BOOLEAN),
        FacadeColumn(name="rowsecurity", type=DataType.BOOLEAN),
    ]
    rows = []
    for ds, tbl in _all_tables(catalog):
        if tbl.table_type != "TABLE":
            continue
        rows.append({
            "schemaname": ds, "tablename": tbl.name,
            "tableowner": "slayer", "tablespace": None,
            "hasindexes": False, "hasrules": False, "hastriggers": False,
            "rowsecurity": False,
        })
    return CatalogRelation(name="pg_tables", columns=columns, rows=rows)


def _build_pg_views(catalog: FacadeCatalog) -> CatalogRelation:
    columns = [
        FacadeColumn(name="schemaname", type=DataType.STRING),
        FacadeColumn(name="viewname", type=DataType.STRING),
        FacadeColumn(name="viewowner", type=DataType.STRING),
        FacadeColumn(name="definition", type=DataType.STRING),
    ]
    # ``definition`` stays NULL: never leak datasource SQL through the facade.
    rows = []
    for ds, tbl in _all_tables(catalog):
        if tbl.table_type != "VIEW":
            continue
        rows.append({
            "schemaname": ds,
            "viewname": tbl.name,
            "viewowner": "slayer",
            "definition": None,
        })
    return CatalogRelation(name="pg_views", columns=columns, rows=rows)


def _build_pg_matviews() -> CatalogRelation:
    return CatalogRelation(name="pg_matviews", columns=[
        FacadeColumn(name="schemaname", type=DataType.STRING),
        FacadeColumn(name="matviewname", type=DataType.STRING),
        FacadeColumn(name="matviewowner", type=DataType.STRING),
        FacadeColumn(name="tablespace", type=DataType.STRING),
        FacadeColumn(name="hasindexes", type=DataType.BOOLEAN),
        FacadeColumn(name="ispopulated", type=DataType.BOOLEAN),
        FacadeColumn(name="definition", type=DataType.STRING),
    ], rows=[])


def _build_pg_constraint() -> CatalogRelation:
    return CatalogRelation(name="pg_constraint", columns=[
        FacadeColumn(name="oid", type=DataType.INTEGER),
        FacadeColumn(name="conname", type=DataType.STRING),
        FacadeColumn(name="contype", type=DataType.STRING),
        FacadeColumn(name="conrelid", type=DataType.INTEGER),
        FacadeColumn(name="confrelid", type=DataType.INTEGER),
        FacadeColumn(name="connamespace", type=DataType.INTEGER),
        FacadeColumn(name="conkey", type=DataType.STRING),
        FacadeColumn(name="confkey", type=DataType.STRING),
    ], rows=[])


def _build_pg_index() -> CatalogRelation:
    return CatalogRelation(name="pg_index", columns=[
        FacadeColumn(name="indexrelid", type=DataType.INTEGER),
        FacadeColumn(name="indrelid", type=DataType.INTEGER),
        FacadeColumn(name="indnatts", type=DataType.INTEGER),
        FacadeColumn(name="indnkeyatts", type=DataType.INTEGER),
        FacadeColumn(name="indisunique", type=DataType.BOOLEAN),
        FacadeColumn(name="indisprimary", type=DataType.BOOLEAN),
        FacadeColumn(name="indkey", type=DataType.STRING),
    ], rows=[])


def _build_pg_attrdef() -> CatalogRelation:
    return CatalogRelation(name="pg_attrdef", columns=[
        FacadeColumn(name="oid", type=DataType.INTEGER),
        FacadeColumn(name="adrelid", type=DataType.INTEGER),
        FacadeColumn(name="adnum", type=DataType.INTEGER),
        FacadeColumn(name="adbin", type=DataType.STRING),
    ], rows=[])


def _build_pg_am() -> CatalogRelation:
    """Access methods: only ``heap``, which every ``pg_class.relam`` points at."""
    return CatalogRelation(
        name="pg_am",
        columns=[
            FacadeColumn(name="oid", type=DataType.INTEGER),
            FacadeColumn(name="amname", type=DataType.STRING),
            FacadeColumn(name="amhandler", type=DataType.INTEGER),
            FacadeColumn(name="amtype", type=DataType.STRING),
        ],
        rows=[
            {"oid": PG_AM_HEAP_OID, "amname": "heap",
             "amhandler": 0, "amtype": "t"},
        ],
    )


def _build_pg_policy() -> CatalogRelation:
    """RLS policies — empty (psql ``\\d`` "Policies:" section)."""
    return CatalogRelation(
        name="pg_policy",
        columns=[
            FacadeColumn(name="oid", type=DataType.INTEGER),
            FacadeColumn(name="polname", type=DataType.STRING),
            FacadeColumn(name="polrelid", type=DataType.INTEGER),
            FacadeColumn(name="polcmd", type=DataType.STRING),
            FacadeColumn(name="polpermissive", type=DataType.BOOLEAN),
            FacadeColumn(name="polroles", type=DataType.STRING),
            FacadeColumn(name="polqual", type=DataType.STRING),
            FacadeColumn(name="polwithcheck", type=DataType.STRING),
        ],
        rows=[],
    )


# Empty stubs for tables psql's ``\d`` touches; each declares the full column set psql references.


def _build_pg_trigger() -> CatalogRelation:
    """Triggers — empty."""
    return CatalogRelation(
        name="pg_trigger",
        columns=[
            FacadeColumn(name="oid", type=DataType.INTEGER),
            FacadeColumn(name="tgrelid", type=DataType.INTEGER),
            FacadeColumn(name="tgparentid", type=DataType.INTEGER),
            FacadeColumn(name="tgname", type=DataType.STRING),
            FacadeColumn(name="tgfoid", type=DataType.INTEGER),
            FacadeColumn(name="tgtype", type=DataType.INTEGER),
            FacadeColumn(name="tgenabled", type=DataType.STRING),
            FacadeColumn(name="tgisinternal", type=DataType.BOOLEAN),
            FacadeColumn(name="tgconstrrelid", type=DataType.INTEGER),
            FacadeColumn(name="tgconstrindid", type=DataType.INTEGER),
            FacadeColumn(name="tgconstraint", type=DataType.INTEGER),
            FacadeColumn(name="tgdeferrable", type=DataType.BOOLEAN),
            FacadeColumn(name="tginitdeferred", type=DataType.BOOLEAN),
            FacadeColumn(name="tgnargs", type=DataType.INTEGER),
            FacadeColumn(name="tgargs", type=DataType.STRING),
            FacadeColumn(name="tgqual", type=DataType.STRING),
            FacadeColumn(name="tgoldtable", type=DataType.STRING),
            FacadeColumn(name="tgnewtable", type=DataType.STRING),
        ],
        rows=[],
    )


def _build_pg_inherits() -> CatalogRelation:
    """Inheritance — empty; datasets are flat."""
    return CatalogRelation(
        name="pg_inherits",
        columns=[
            FacadeColumn(name="inhrelid", type=DataType.INTEGER),
            FacadeColumn(name="inhparent", type=DataType.INTEGER),
            FacadeColumn(name="inhseqno", type=DataType.INTEGER),
            FacadeColumn(name="inhdetachpending", type=DataType.BOOLEAN),
        ],
        rows=[],
    )


def _build_pg_publication() -> CatalogRelation:
    """Logical-replication publications — empty. ``\\dRp`` and ``\\d``
    query this."""
    return CatalogRelation(
        name="pg_publication",
        columns=[
            FacadeColumn(name="oid", type=DataType.INTEGER),
            FacadeColumn(name="pubname", type=DataType.STRING),
            FacadeColumn(name="pubowner", type=DataType.INTEGER),
            FacadeColumn(name="puballtables", type=DataType.BOOLEAN),
            FacadeColumn(name="pubinsert", type=DataType.BOOLEAN),
            FacadeColumn(name="pubupdate", type=DataType.BOOLEAN),
            FacadeColumn(name="pubdelete", type=DataType.BOOLEAN),
            FacadeColumn(name="pubtruncate", type=DataType.BOOLEAN),
            FacadeColumn(name="pubviaroot", type=DataType.BOOLEAN),
        ],
        rows=[],
    )


def _build_pg_publication_rel() -> CatalogRelation:
    """Publication membership — empty."""
    return CatalogRelation(
        name="pg_publication_rel",
        columns=[
            FacadeColumn(name="oid", type=DataType.INTEGER),
            FacadeColumn(name="prpubid", type=DataType.INTEGER),
            FacadeColumn(name="prrelid", type=DataType.INTEGER),
        ],
        rows=[],
    )


def _build_pg_publication_tables() -> CatalogRelation:
    """Publication → table denormalized view — empty."""
    return CatalogRelation(
        name="pg_publication_tables",
        columns=[
            FacadeColumn(name="pubname", type=DataType.STRING),
            FacadeColumn(name="schemaname", type=DataType.STRING),
            FacadeColumn(name="tablename", type=DataType.STRING),
        ],
        rows=[],
    )


def _build_pg_partitioned_table() -> CatalogRelation:
    """Partitioning metadata — empty. SLayer has no partitioned datasets."""
    return CatalogRelation(
        name="pg_partitioned_table",
        columns=[
            FacadeColumn(name="partrelid", type=DataType.INTEGER),
            FacadeColumn(name="partstrat", type=DataType.STRING),
            FacadeColumn(name="partnatts", type=DataType.INTEGER),
            FacadeColumn(name="partdefid", type=DataType.INTEGER),
            FacadeColumn(name="partattrs", type=DataType.STRING),
            FacadeColumn(name="partclass", type=DataType.STRING),
            FacadeColumn(name="partcollation", type=DataType.STRING),
            FacadeColumn(name="partexprs", type=DataType.STRING),
        ],
        rows=[],
    )


def _build_pg_rewrite() -> CatalogRelation:
    """Rewrite rules — empty; views surface only via ``relkind = 'v'``."""
    return CatalogRelation(
        name="pg_rewrite",
        columns=[
            FacadeColumn(name="oid", type=DataType.INTEGER),
            FacadeColumn(name="rulename", type=DataType.STRING),
            FacadeColumn(name="ev_class", type=DataType.INTEGER),
            FacadeColumn(name="ev_type", type=DataType.STRING),
            FacadeColumn(name="ev_enabled", type=DataType.STRING),
            FacadeColumn(name="is_instead", type=DataType.BOOLEAN),
            FacadeColumn(name="ev_qual", type=DataType.STRING),
            FacadeColumn(name="ev_action", type=DataType.STRING),
        ],
        rows=[],
    )


def _build_pg_extension() -> CatalogRelation:
    """Installed extensions — empty. SLayer doesn't advertise any."""
    return CatalogRelation(
        name="pg_extension",
        columns=[
            FacadeColumn(name="oid", type=DataType.INTEGER),
            FacadeColumn(name="extname", type=DataType.STRING),
            FacadeColumn(name="extowner", type=DataType.INTEGER),
            FacadeColumn(name="extnamespace", type=DataType.INTEGER),
            FacadeColumn(name="extrelocatable", type=DataType.BOOLEAN),
            FacadeColumn(name="extversion", type=DataType.STRING),
            FacadeColumn(name="extconfig", type=DataType.STRING),
            FacadeColumn(name="extcondition", type=DataType.STRING),
        ],
        rows=[],
    )


def _build_pg_collation() -> CatalogRelation:
    """Collations — empty; every ``attcollation`` is 0 (default)."""
    return CatalogRelation(
        name="pg_collation",
        columns=[
            FacadeColumn(name="oid", type=DataType.INTEGER),
            FacadeColumn(name="collname", type=DataType.STRING),
            FacadeColumn(name="collnamespace", type=DataType.INTEGER),
            FacadeColumn(name="collencoding", type=DataType.INTEGER),
            FacadeColumn(name="collcollate", type=DataType.STRING),
            FacadeColumn(name="collctype", type=DataType.STRING),
        ],
        rows=[],
    )


def _build_pg_roles() -> CatalogRelation:
    """Roles: one synthetic principal (override via ``extra_relations``)."""
    return CatalogRelation(
        name="pg_roles",
        columns=[
            FacadeColumn(name="oid", type=DataType.INTEGER),
            FacadeColumn(name="rolname", type=DataType.STRING),
            FacadeColumn(name="rolsuper", type=DataType.BOOLEAN),
            FacadeColumn(name="rolinherit", type=DataType.BOOLEAN),
            FacadeColumn(name="rolcreaterole", type=DataType.BOOLEAN),
            FacadeColumn(name="rolcreatedb", type=DataType.BOOLEAN),
            FacadeColumn(name="rolcanlogin", type=DataType.BOOLEAN),
            FacadeColumn(name="rolreplication", type=DataType.BOOLEAN),
            FacadeColumn(name="rolconnlimit", type=DataType.INTEGER),
            FacadeColumn(name="rolvaliduntil", type=DataType.DATETIME),
            FacadeColumn(name="rolbypassrls", type=DataType.BOOLEAN),
        ],
        rows=[{
            "oid": PG_SLAYER_ROLE_OID, "rolname": PG_SLAYER_ROLE_NAME,
            "rolsuper": False, "rolinherit": True,
            "rolcreaterole": False, "rolcreatedb": False,
            "rolcanlogin": True, "rolreplication": False,
            "rolconnlimit": -1, "rolvaliduntil": None, "rolbypassrls": False,
        }],
    )


def _build_pg_database(catalog: FacadeCatalog, datasource: str) -> CatalogRelation:
    """Databases: one row, the connected datasource."""
    return CatalogRelation(
        name="pg_database",
        columns=[
            FacadeColumn(name="oid", type=DataType.INTEGER),
            FacadeColumn(name="datname", type=DataType.STRING),
            FacadeColumn(name="datdba", type=DataType.INTEGER),
            FacadeColumn(name="encoding", type=DataType.INTEGER),
            FacadeColumn(name="datcollate", type=DataType.STRING),
            FacadeColumn(name="datctype", type=DataType.STRING),
            FacadeColumn(name="datistemplate", type=DataType.BOOLEAN),
            FacadeColumn(name="datallowconn", type=DataType.BOOLEAN),
            FacadeColumn(name="datconnlimit", type=DataType.INTEGER),
            FacadeColumn(name="dattablespace", type=DataType.INTEGER),
            FacadeColumn(name="datacl", type=DataType.STRING),
        ],
        rows=[{
            "oid": stable_oid("database", datasource),
            "datname": datasource,
            "datdba": PG_SLAYER_ROLE_OID,
            "encoding": PG_ENCODING_UTF8,
            "datcollate": "en_US.UTF-8",
            "datctype": "en_US.UTF-8",
            "datistemplate": False, "datallowconn": True,
            "datconnlimit": -1, "dattablespace": 1663,
            "datacl": None,
        }],
    )


def _build_is_columns(catalog: FacadeCatalog, datasource: str) -> CatalogRelation:
    """``information_schema.columns`` (as ``_is_columns``) plus SLayer extension fields."""
    columns = [
        FacadeColumn(name="table_catalog", type=DataType.STRING),
        FacadeColumn(name="table_schema", type=DataType.STRING),
        FacadeColumn(name="table_name", type=DataType.STRING),
        FacadeColumn(name="column_name", type=DataType.STRING),
        FacadeColumn(name="ordinal_position", type=DataType.INTEGER),
        FacadeColumn(name="column_default", type=DataType.STRING),
        FacadeColumn(name="is_nullable", type=DataType.STRING),
        FacadeColumn(name="data_type", type=DataType.STRING),
        FacadeColumn(name="udt_schema", type=DataType.STRING),
        FacadeColumn(name="udt_name", type=DataType.STRING),
        FacadeColumn(name="is_identity", type=DataType.STRING),
        FacadeColumn(name="is_generated", type=DataType.STRING),
        FacadeColumn(name="column_kind", type=DataType.STRING),
        FacadeColumn(name="description", type=DataType.STRING),
        FacadeColumn(name="label", type=DataType.STRING),
    ]
    rows: list[dict[str, Any]] = []
    for ds, tbl in _all_tables(catalog):
        position = 1
        for d in local_dimensions(tbl):
            udt = _UDT_NAME_BY_DATATYPE.get(d.data_type, "text")
            rows.append({
                "table_catalog": datasource, "table_schema": ds,
                "table_name": tbl.name, "column_name": d.name,
                "ordinal_position": position, "column_default": None,
                "is_nullable": "YES", "data_type": udt,
                "udt_schema": "pg_catalog", "udt_name": udt,
                "is_identity": "NO", "is_generated": "NEVER",
                "column_kind": "DIMENSION", "description": d.description,
                "label": d.label,
            })
            position += 1
        for m in local_metrics(tbl):
            udt = _UDT_NAME_BY_DATATYPE.get(m.data_type, "text") if m.data_type else "text"
            rows.append({
                "table_catalog": datasource, "table_schema": ds,
                "table_name": tbl.name, "column_name": m.name,
                "ordinal_position": position, "column_default": None,
                "is_nullable": "YES", "data_type": udt,
                "udt_schema": "pg_catalog", "udt_name": udt,
                "is_identity": "NO", "is_generated": "NEVER",
                "column_kind": "METRIC", "description": m.description,
                "label": m.label,
            })
            position += 1
    return CatalogRelation(name="_is_columns", columns=columns, rows=rows)


def _build_is_table_constraints() -> CatalogRelation:
    return CatalogRelation(name="_is_table_constraints", columns=[
        FacadeColumn(name="constraint_catalog", type=DataType.STRING),
        FacadeColumn(name="constraint_schema", type=DataType.STRING),
        FacadeColumn(name="constraint_name", type=DataType.STRING),
        FacadeColumn(name="table_catalog", type=DataType.STRING),
        FacadeColumn(name="table_schema", type=DataType.STRING),
        FacadeColumn(name="table_name", type=DataType.STRING),
        FacadeColumn(name="constraint_type", type=DataType.STRING),
        FacadeColumn(name="is_deferrable", type=DataType.STRING),
        FacadeColumn(name="initially_deferred", type=DataType.STRING),
    ], rows=[])


def _build_is_key_column_usage() -> CatalogRelation:
    return CatalogRelation(name="_is_key_column_usage", columns=[
        FacadeColumn(name="constraint_catalog", type=DataType.STRING),
        FacadeColumn(name="constraint_schema", type=DataType.STRING),
        FacadeColumn(name="constraint_name", type=DataType.STRING),
        FacadeColumn(name="table_catalog", type=DataType.STRING),
        FacadeColumn(name="table_schema", type=DataType.STRING),
        FacadeColumn(name="table_name", type=DataType.STRING),
        FacadeColumn(name="column_name", type=DataType.STRING),
        FacadeColumn(name="ordinal_position", type=DataType.INTEGER),
    ], rows=[])


def _build_is_schemata(
    catalog: FacadeCatalog, datasource: str,
) -> CatalogRelation:
    """INFORMATION_SCHEMA.SCHEMATA — one row per distinct facade schema."""
    columns = [
        FacadeColumn(name="catalog_name", type=DataType.STRING),
        FacadeColumn(name="schema_name", type=DataType.STRING),
    ]
    rows = [
        {"catalog_name": datasource, "schema_name": name}
        for name in _user_schema_names(catalog)
    ]
    return CatalogRelation(name="_is_schemata", columns=columns, rows=rows)


def _build_is_tables(
    catalog: FacadeCatalog, datasource: str,
) -> CatalogRelation:
    columns = [
        FacadeColumn(name="table_catalog", type=DataType.STRING),
        FacadeColumn(name="table_schema", type=DataType.STRING),
        FacadeColumn(name="table_name", type=DataType.STRING),
        FacadeColumn(name="table_type", type=DataType.STRING),
    ]
    rows = [{"table_catalog": datasource, "table_schema": sch.name,
             "table_name": tbl.name, "table_type": tbl.table_type}
            for sch in catalog.schemas for tbl in sch.tables]
    return CatalogRelation(name="_is_tables", columns=columns, rows=rows)


def _build_is_metrics(
    catalog: FacadeCatalog, datasource: str,
) -> CatalogRelation:
    """SLayer's INFORMATION_SCHEMA.METRICS extension, with JDBC-style type names."""
    columns = [
        FacadeColumn(name="catalog_name", type=DataType.STRING),
        FacadeColumn(name="schema_name", type=DataType.STRING),
        FacadeColumn(name="table_name", type=DataType.STRING),
        FacadeColumn(name="metric_name", type=DataType.STRING),
        FacadeColumn(name="description", type=DataType.STRING),
        FacadeColumn(name="data_type", type=DataType.STRING),
        FacadeColumn(name="label", type=DataType.STRING),
    ]
    rows: list[dict[str, Any]] = []
    for sch in catalog.schemas:
        for tbl in sch.tables:
            for m in tbl.metrics:
                rows.append({
                    "catalog_name": datasource,
                    "schema_name": sch.name, "table_name": tbl.name,
                    "metric_name": m.name, "description": m.description,
                    "data_type": (
                        datatype_to_jdbc(m.data_type) if m.data_type else None
                    ),
                    "label": m.label,
                })
    return CatalogRelation(name="_is_metrics", columns=columns, rows=rows)


def _build_is_dimensions(
    catalog: FacadeCatalog, datasource: str,
) -> CatalogRelation:
    """SLayer's INFORMATION_SCHEMA.DIMENSIONS extension, with JDBC-style type names."""
    columns = [
        FacadeColumn(name="catalog_name", type=DataType.STRING),
        FacadeColumn(name="schema_name", type=DataType.STRING),
        FacadeColumn(name="table_name", type=DataType.STRING),
        FacadeColumn(name="dimension_name", type=DataType.STRING),
        FacadeColumn(name="description", type=DataType.STRING),
        FacadeColumn(name="data_type", type=DataType.STRING),
        FacadeColumn(name="label", type=DataType.STRING),
        FacadeColumn(name="is_time", type=DataType.BOOLEAN),
    ]
    rows: list[dict[str, Any]] = []
    for sch in catalog.schemas:
        for tbl in sch.tables:
            for d in advertised_dimensions(tbl):
                rows.append({
                    "catalog_name": datasource,
                    "schema_name": sch.name, "table_name": tbl.name,
                    "dimension_name": d.name, "description": d.description,
                    "data_type": datatype_to_jdbc(d.data_type),
                    "label": d.label, "is_time": d.is_time,
                })
    return CatalogRelation(name="_is_dimensions", columns=columns, rows=rows)


def _check_collision(seen: dict[int, str], oid: int, key: str) -> None:
    prior = seen.get(oid)
    if prior is not None and prior != key:
        raise ValueError(
            f"pg_catalog OID collision: {key!r} and {prior!r} both hash to {oid}"
        )
    seen[oid] = key


# Must list every ``_build_pg_*`` relation, or bare refs misroute as user tables.
_PG_CATALOG_NAMES = frozenset({
    "pg_namespace", "pg_class", "pg_attribute", "pg_type", "pg_proc",
    "pg_settings", "pg_description", "pg_stat_user_tables", "pg_enum",
    "pg_tables", "pg_views", "pg_matviews", "pg_constraint", "pg_index",
    "pg_attrdef",
    "pg_am", "pg_roles", "pg_database", "pg_collation", "pg_policy",
    "pg_trigger", "pg_inherits",
    "pg_publication", "pg_publication_rel", "pg_publication_tables",
    "pg_partitioned_table", "pg_rewrite", "pg_extension",
})

_INFO_SCHEMA_NAMES = frozenset({
    "columns", "table_constraints", "key_column_usage",
    "schemata", "tables", "metrics", "dimensions",
})


def _is_known_catalog_table(tbl: exp.Table) -> bool:
    """True if ``tbl`` is any ``pg_catalog``/``information_schema`` table, or a known bare pg_catalog name.

    Unknown qualified tables are synthesized empty at execute time; bare names need
    an allowlist match so they never shadow same-named user datasets.
    """
    name = str(tbl.name).lower()
    schema_part = tbl.args.get("db")
    schema = None
    if schema_part is not None:
        schema = (str(schema_part.this) if hasattr(schema_part, "this") else str(schema_part)).lower()
    catalog_part = tbl.args.get("catalog")
    catalog = None
    if catalog_part is not None:
        catalog = (
            str(catalog_part.this) if hasattr(catalog_part, "this") else str(catalog_part)
        ).lower()
        if catalog != CATALOG_NAME.lower():
            return False
    if schema == "information_schema":
        return True
    if schema == "pg_catalog":
        return True
    if schema is None:
        return name in _PG_CATALOG_NAMES
    return False


def is_catalog_only(parsed: exp.Expression) -> bool:
    """True iff every table in ``parsed`` is a catalog relation or CTE, and it references at least one.

    A tableless SELECT qualifies only if it references a catalog function; otherwise
    routing it through DuckDB would widen the accepted SQL surface.
    """
    cte_names = {
        str(cte.alias).lower() for cte in parsed.find_all(exp.CTE)
        if cte.alias
    }
    saw_catalog_table = False
    for tbl in parsed.find_all(exp.Table):
        name = str(tbl.name).lower()
        if name in cte_names:
            continue
        if not _is_known_catalog_table(tbl):
            return False
        saw_catalog_table = True
    if saw_catalog_table:
        return True
    return _references_catalog_function(parsed)


_CATALOG_FUNCTION_NAMES = frozenset({
    # Underscored spellings (Anonymous + bare-word Column refs).
    "current_database", "current_catalog", "current_user", "session_user",
    "current_role", "current_schemas",
    "format_type", "obj_description", "col_description", "pg_get_userbyid",
    "pg_table_is_visible", "pg_get_expr", "pg_total_relation_size",
    "pg_encoding_to_char",
    "has_table_privilege", "has_any_column_privilege", "has_schema_privilege",
    "_pg_expandarray",
    "pg_get_constraintdef", "pg_get_indexdef", "pg_get_triggerdef",
    "pg_get_viewdef", "pg_get_partkeydef", "pg_get_ruledef",
    "pg_get_functiondef", "pg_get_statisticsobjdef",
    "pg_get_statisticsobjdef_columns", "pg_get_statisticsobjdef_expressions",
    "pg_get_serial_sequence", "pg_column_is_updatable",
    "pg_get_object_address", "pg_identify_object", "pg_size_pretty",
    "pg_relation_size", "pg_indexes_size", "pg_table_size",
    "pg_database_size", "pg_tablespace_size",
    # sqlglot Func class keys (no underscore).
    "currentdatabase", "currentcatalog", "currentuser", "sessionuser",
    "currentrole", "currentschema", "currentschemas",
})


def _references_catalog_function(parsed: exp.Expression) -> bool:
    """True iff ``parsed`` has a ``::reg*`` cast, a catalog-qualified call, or a known stub function."""
    return (
        _has_catalog_cast(parsed)
        or _has_catalog_qualified_dot(parsed)
        or _has_catalog_function_call(parsed)
    )


_CATALOG_CAST_KINDS = frozenset({"regclass", "regproc", "regtype"})
_CATALOG_DOT_SCHEMAS = frozenset({"pg_catalog", "information_schema"})


def _has_catalog_cast(parsed: exp.Expression) -> bool:
    for cast in parsed.find_all(exp.Cast):
        to = cast.args.get("to")
        kind = getattr(to, "this", None) if to is not None else None
        if kind is not None and str(kind).lower() in _CATALOG_CAST_KINDS:
            return True
    return False


def _has_catalog_qualified_dot(parsed: exp.Expression) -> bool:
    for dot in parsed.find_all(exp.Dot):
        lhs = dot.this
        if isinstance(lhs, exp.Identifier) and str(lhs.this).lower() in _CATALOG_DOT_SCHEMAS:
            return True
    return False


def _has_catalog_function_call(parsed: exp.Expression) -> bool:
    for node in parsed.walk():
        if _function_name_lower(node) in _CATALOG_FUNCTION_NAMES:
            return True
        # sqlglot parses bareword niladic functions (``current_role``) as an unqualified Column.
        if isinstance(node, exp.Column) and not node.table:
            ident = node.this
            if isinstance(ident, exp.Identifier):
                name = str(ident.this).lower()
                if name in _CATALOG_FUNCTION_NAMES:
                    return True
    return False


# Rewritten to literals, not macros: DuckDB macros can't overload arity (has_*_privilege has 2 and 3).
_CONSTANT_STUB_LITERALS: dict[str, Any] = {
    "has_table_privilege": True,
    "has_any_column_privilege": True,
    "has_schema_privilege": True,
    "pg_get_userbyid": "slayer",
    "pg_table_is_visible": True,
    "pg_total_relation_size": 0,
    "pg_get_expr": None,
    "pg_encoding_to_char": "UTF8",
    # NULL = absent, as real Postgres answers for objects that don't exist.
    "pg_get_constraintdef": None,
    "pg_get_indexdef": None,
    "pg_get_triggerdef": None,
    "pg_get_viewdef": None,
    "pg_get_partkeydef": None,
    "pg_get_ruledef": None,
    "pg_get_functiondef": None,
    "pg_get_statisticsobjdef": None,
    "pg_get_statisticsobjdef_columns": None,
    "pg_get_statisticsobjdef_expressions": None,
    "pg_get_serial_sequence": None,
    "pg_column_is_updatable": False,
    "pg_get_object_address": None,
    "pg_identify_object": None,
    "pg_size_pretty": "0 bytes",
    "pg_relation_size": 0,
    "pg_indexes_size": 0,
    "pg_table_size": 0,
    "pg_database_size": 0,
    "pg_tablespace_size": 0,
}

# Renamed to single-arity private macros; 2-arg ``obj_description`` drops its second arg.
_LOOKUP_STUB_NAMES: dict[str, str] = {
    "format_type": "_slayer_format_type",
    "obj_description": "_slayer_obj_description",
    "col_description": "_slayer_col_description",
    "_pg_expandarray": "_slayer_pg_expandarray",
}


def _function_name_lower(node: exp.Expression) -> str | None:
    """Lowercased function name of ``node``, or None if it isn't a function call."""
    if isinstance(node, exp.Anonymous):
        n = node.args.get("this")
        if n is not None:
            return str(n).lower()
    if isinstance(node, exp.Func):
        return type(node).key.lower()
    return None


def _to_literal(value: Any) -> exp.Expression:
    """Build a sqlglot Literal for ``value``."""
    if value is None:
        return exp.Null()
    if isinstance(value, bool):
        return exp.Boolean(this=value)
    if isinstance(value, int):
        return exp.Literal.number(value)
    return exp.Literal.string(str(value))


def _unwrap_qualified_stub_call(node: exp.Expression) -> exp.Expression | None:
    """Rewrite a ``pg_catalog.``/``information_schema.``-qualified call (a sqlglot Dot) as a bare call, or None."""
    if not isinstance(node, exp.Dot):
        return None
    lhs = node.this
    rhs = node.expression
    if not isinstance(lhs, exp.Identifier):
        return None
    schema = str(lhs.this).lower()
    if schema not in {"information_schema", "pg_catalog"}:
        return None
    name = _function_name_lower(rhs)
    if name is None:
        return None
    if name in _CONSTANT_STUB_LITERALS:
        return _to_literal(_CONSTANT_STUB_LITERALS[name])
    if name in _LOOKUP_STUB_NAMES:
        private = _LOOKUP_STUB_NAMES[name]
        args = list(rhs.args.get("expressions") or [])
        if name == "obj_description" and len(args) > 1:
            args = args[:1]
        return exp.Anonymous(this=private, expressions=args)
    # Unknown fn: bare it — many exist in DuckDB; a left-in Dot reads as a missing column.
    args = list(rhs.args.get("expressions") or [])
    return exp.Anonymous(this=name, expressions=args)


class _AstRewriter:
    """Postgres → DuckDB-ready AST pre-rewrite (qualifiers, ``::reg*`` casts, session functions,
    stub renames, regex and ``OPERATOR(pg_catalog.x)`` operators, ``COLLATE pg_catalog.default``)."""

    def __init__(self, *, datasource: str,
                 regclass_map: dict[str, int] | None = None) -> None:
        self.datasource = datasource
        self.regclass_map: dict[str, int] = regclass_map or {}

    def rewrite(self, parsed: exp.Expression) -> exp.Expression:
        parsed = parsed.transform(self._strip_schema_qualifiers)
        parsed = parsed.transform(self._strip_column_schema_qualifiers)
        parsed = parsed.transform(self._rewrite_current_schemas_indexed)
        parsed = parsed.transform(self._rewrite_current_schemas_bare)
        parsed = parsed.transform(self._rewrite_pg_format_quoted_ident)
        # Before the reg* passes, so ``pg_catalog.regclass`` reaches them as bare ``regclass``.
        parsed = parsed.transform(self._rewrite_schema_qualified_cast)
        parsed = parsed.transform(self._rewrite_regclass_casts)
        parsed = parsed.transform(self._rewrite_regproc_regtype_casts)
        parsed = parsed.transform(self._substitute_context_functions)
        parsed = parsed.transform(self._rename_stub_functions)
        parsed = parsed.transform(self._rewrite_regex_operators)
        parsed = parsed.transform(self._rewrite_schema_qualified_operator)
        parsed = parsed.transform(self._strip_collate_clause)
        parsed = parsed.transform(self._rewrite_pg_any_array)
        # Last, so known stubs claim their calls first.
        parsed = parsed.transform(self._stub_unknown_pg_functions)
        return parsed

    @staticmethod
    def _strip_schema_qualifiers(node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.Table):
            return node
        schema_part = node.args.get("db")
        if schema_part is None:
            return node
        schema_name = (
            str(schema_part.this) if hasattr(schema_part, "this") else str(schema_part)
        ).lower()
        if schema_name == "pg_catalog":
            new = node.copy()
            new.set("db", None)
            new.set("catalog", None)
            return new
        if schema_name == "information_schema":
            new = node.copy()
            new.set("db", None)
            new.set("catalog", None)
            inner_name = str(new.this.this) if hasattr(new.this, "this") else str(new.this)
            new.set("this", exp.Identifier(this=f"_is_{inner_name.lower()}", quoted=False))
            return new
        return node

    @staticmethod
    def _rewrite_pg_format_quoted_ident(node: exp.Expression) -> exp.Expression:
        """``FORMAT('%I.%I', a, b)`` → ``CONCAT(a, '.', b)`` and ``FORMAT('%I', a)`` → ``a``.

        DuckDB's printf-style ``FORMAT`` doesn't know ``%I``.
        """
        if not isinstance(node, exp.Format):
            return node
        fmt = node.this
        if not (isinstance(fmt, exp.Literal) and fmt.is_string):
            return node
        spec = str(fmt.this)
        rest = list(node.args.get("expressions") or [])
        if spec == "%I.%I" and len(rest) == 2:
            return exp.Anonymous(
                this="concat",
                expressions=[rest[0], exp.Literal.string("."), rest[1]],
            )
        if spec == "%I" and len(rest) == 1:
            return rest[0]
        return node

    @staticmethod
    def _strip_column_schema_qualifiers(node: exp.Expression) -> exp.Expression:
        """Column-side mirror of ``_strip_schema_qualifiers`` (``information_schema.X.c`` → ``_is_X.c``)."""
        if not isinstance(node, exp.Column):
            return node
        db_part = node.args.get("db")
        table_part = node.args.get("table")
        if db_part is None:
            return node
        schema = (
            str(db_part.this) if hasattr(db_part, "this") else str(db_part)
        ).lower()
        if schema not in {"pg_catalog", "information_schema"}:
            return node
        new = node.copy()
        new.set("db", None)
        new.set("catalog", None)  # else a stale ``slayer.pg_class.oid`` fails to bind
        if schema == "information_schema" and table_part is not None:
            tbl_name = (
                str(table_part.this) if hasattr(table_part, "this") else str(table_part)
            ).lower()
            new.set("table", exp.Identifier(this=f"_is_{tbl_name}", quoted=False))
        return new

    @staticmethod
    def _rewrite_current_schemas_bare(node: exp.Expression) -> exp.Expression:
        """Unindexed ``current_schemas(...)`` → ``['public']``, not DuckDB's own schema list."""
        if _AstRewriter._is_current_schemas(node):
            return exp.Anonymous(
                this="list_value", expressions=[exp.Literal.string("public")],
            )
        return node

    @staticmethod
    def _is_current_schemas(node: exp.Expression) -> bool:
        if isinstance(node, exp.CurrentSchemas):
            return True
        if _function_name_lower(node) == "current_schemas":
            return True
        if isinstance(node, exp.Dot):
            lhs = node.this
            rhs = node.expression
            if isinstance(lhs, exp.Identifier) and str(lhs.this).lower() == "pg_catalog":
                if isinstance(rhs, exp.CurrentSchemas):
                    return True
                if _function_name_lower(rhs) == "current_schemas":
                    return True
        return False

    @staticmethod
    def _rewrite_current_schemas_indexed(node: exp.Expression) -> exp.Expression:
        # Only ``current_schemas(...)[1]`` (sqlglot: 0-based ``Literal(0)``) collapses to 'public'.
        if not isinstance(node, exp.Bracket):
            return node
        inner = node.this
        if isinstance(inner, exp.Paren):
            inner = inner.this
        if not _AstRewriter._is_current_schemas(inner):
            return node
        indices = node.args.get("expressions") or []
        if len(indices) != 1:
            return node
        index = indices[0]
        if not (isinstance(index, exp.Literal) and not index.is_string):
            return node
        try:
            if int(str(index.this)) != 0:  # 0-based — user's [1]
                return node
        except ValueError:
            return node
        return exp.Literal.string("public")

    def _rewrite_regclass_casts(self, node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.Cast):
            return node
        target_kind = self._cast_target_kind(node)
        if target_kind != "regclass":
            return node
        inner = node.this
        if isinstance(inner, exp.Literal) and inner.is_string:
            text = inner.this  # raw string value (no quotes)
            return exp.Literal.number(self._lookup_static_regclass(text))
        # Numeric inners (``c.oid``, ``0``) are already OIDs; the UDF is VARCHAR-only.
        if isinstance(inner, exp.Column):
            return inner
        if isinstance(inner, exp.Literal) and not inner.is_string:
            return inner
        # Dynamic names, e.g. Metabase's ``CAST(FORMAT('%I.%I', s, t) AS regclass)``.
        return exp.Anonymous(this="slayer_regclass_oid", expressions=[inner])

    @staticmethod
    def _cast_target_kind(node: exp.Cast) -> str | None:
        to = node.args.get("to")
        if to is None:
            return None
        kind_attr = getattr(to, "this", None)
        if kind_attr is not None:
            return str(kind_attr).lower()
        return None

    def _lookup_static_regclass(self, text: str) -> int:
        return (
            self.regclass_map.get(text)
            or self.regclass_map.get(text.lower())
            or self.regclass_map.get(text.split(".")[-1].lower(), 0)
        )

    def _rewrite_regproc_regtype_casts(self, node: exp.Expression) -> exp.Expression:
        """``::regproc``-family casts → ``0``; ``::regtype`` → the known ``pg_type.oid``, else ``0``."""
        if not isinstance(node, exp.Cast):
            return node
        target_kind = self._cast_target_kind(node)
        if target_kind in {"regproc", "regoper", "regoperator", "regprocedure"}:
            return exp.Literal.number(0)
        if target_kind == "regtype":
            inner = node.this
            if isinstance(inner, exp.Literal) and inner.is_string:
                return exp.Literal.number(_KNOWN_TYPE_OIDS.get(
                    str(inner.this).lower(), 0,
                ))
            return exp.Literal.number(0)
        return node

    def _substitute_context_functions(self, node: exp.Expression) -> exp.Expression:
        substituted = (
            self._substitute_qualified_context_call(node)
            or self._substitute_qualified_context_column(node)
            or self._substitute_dedicated_func(node)
            or self._substitute_bareword_column(node)
            or self._substitute_anonymous_function(node)
        )
        return substituted if substituted is not None else node

    def _substitute_qualified_context_call(
        self, node: exp.Expression,
    ) -> exp.Expression | None:
        """Replace the whole ``pg_catalog.<ctx-fn>()`` Dot, so it can't end up wrapping a literal."""
        if not isinstance(node, exp.Dot):
            return None
        lhs = node.this
        if not isinstance(lhs, exp.Identifier):
            return None
        if str(lhs.this).lower() != "pg_catalog":
            return None
        rhs = node.expression
        return (
            self._substitute_dedicated_func(rhs)
            or self._substitute_bareword_column(rhs)
            or self._substitute_anonymous_function(rhs)
        )

    def _substitute_qualified_context_column(
        self, node: exp.Expression,
    ) -> exp.Expression | None:
        """Replace no-parens ``pg_catalog.current_user`` (a Column with table ``pg_catalog``)."""
        if not isinstance(node, exp.Column):
            return None
        table = node.args.get("table")
        if table is None:
            return None
        table_name = (
            str(table.this) if hasattr(table, "this") else str(table)
        ).lower()
        if table_name != "pg_catalog":
            return None
        ident = node.this
        if not isinstance(ident, exp.Identifier):
            return None
        return self._literal_for_context_name(str(ident.this).lower())

    def _substitute_dedicated_func(self, node: exp.Expression) -> exp.Expression | None:
        """Dedicated sqlglot Func subclasses (typed nodes for niladic ctx fns)."""
        if isinstance(node, (exp.CurrentDatabase, getattr(exp, "CurrentCatalog", exp.CurrentDatabase))):
            return exp.Literal.string(self.datasource)
        if isinstance(node, (exp.CurrentUser, exp.SessionUser)):
            return exp.Literal.string("slayer")
        if isinstance(node, exp.CurrentSchema):
            return exp.Literal.string("public")
        return None

    def _substitute_bareword_column(self, node: exp.Expression) -> exp.Expression | None:
        """Bare ``current_role`` parses as an unqualified Column (``node.table == ""``)."""
        if not (isinstance(node, exp.Column) and not node.table):
            return None
        ident = node.this
        if not isinstance(ident, exp.Identifier):
            return None
        return self._literal_for_context_name(str(ident.this).lower())

    def _substitute_anonymous_function(self, node: exp.Expression) -> exp.Expression | None:
        """Less-common Anonymous function spellings — fallback path."""
        name = _function_name_lower(node)
        if name is None:
            return None
        return self._literal_for_context_name(name)

    def _literal_for_context_name(self, name: str) -> exp.Expression | None:
        if name in {"current_database", "current_catalog",
                    "currentdatabase", "currentcatalog"}:
            return exp.Literal.string(self.datasource)
        if name in {"current_user", "session_user", "current_role",
                    "currentuser", "sessionuser", "currentrole"}:
            return exp.Literal.string("slayer")
        # DuckDB's own current_schema returns 'main'.
        if name in {"current_schema", "currentschema"}:
            return exp.Literal.string("public")
        return None

    @staticmethod
    def _rename_stub_functions(node: exp.Expression) -> exp.Expression:
        # e.g. ``(information_schema._pg_expandarray(i.indkey)).n``: rename inner, keep outer field access.
        unwrapped = _unwrap_qualified_stub_call(node)
        if unwrapped is not None:
            return unwrapped
        name = _function_name_lower(node)
        if name is None:
            return node
        if name in _CONSTANT_STUB_LITERALS:
            return _to_literal(_CONSTANT_STUB_LITERALS[name])
        if name in _LOOKUP_STUB_NAMES:
            private = _LOOKUP_STUB_NAMES[name]
            args = list(node.args.get("expressions") or [])
            if name == "obj_description" and len(args) > 1:
                args = args[:1]
            return exp.Anonymous(this=private, expressions=args)
        return node

    @staticmethod
    def _rewrite_pg_any_array(node: exp.Expression) -> exp.Expression:
        """``<x> = ANY(<col>)`` → ``FALSE``: array columns are untyped here, and their relations are empty."""
        if not isinstance(node, exp.EQ):
            return node
        rhs = node.expression
        if not isinstance(rhs, exp.Any):
            return node
        inner = rhs.this
        if isinstance(inner, exp.Paren):
            inner = inner.this
        if isinstance(inner, exp.Column):
            return exp.Boolean(this=False)
        return node

    # DuckDB-native ``pg_*`` names we must NOT rewrite.
    _DUCKDB_NATIVE_PG_FUNCTIONS: frozenset[str] = frozenset()

    @classmethod
    def _stub_unknown_pg_functions(cls, node: exp.Expression) -> exp.Expression:
        """Unknown ``pg_*`` calls → NULL with a warning; non-``pg_`` names stay so typos still fail."""
        if not isinstance(node, exp.Anonymous):
            return node
        name = str(node.args.get("this", "")).lower()
        if not name.startswith("pg_"):
            return node
        if name in _CATALOG_FUNCTION_NAMES:
            return node
        if name in cls._DUCKDB_NATIVE_PG_FUNCTIONS:
            return node
        logger.warning(
            "pg-facade: stubbed unknown Postgres helper function %s(...) → NULL — "
            "consider adding it to _CONSTANT_STUB_LITERALS if this recurs.",
            name,
        )
        return exp.Null()

    @staticmethod
    def _rewrite_regex_operators(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.RegexpLike):
            return exp.Anonymous(
                this="regexp_matches",
                expressions=[node.this, node.expression],
            )
        if isinstance(node, exp.RegexpILike):
            return exp.Anonymous(
                this="regexp_matches",
                expressions=[node.this, node.expression, exp.Literal.string("i")],
            )
        return node

    # psql emits ``x OPERATOR(pg_catalog.~) y`` (``\du <pattern>``); DuckDB has no OPERATOR() syntax.
    _PG_REGEX_OPS = {"~", "~*", "!~", "!~*"}

    @staticmethod
    def _rewrite_schema_qualified_operator(node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.Operator):
            return node
        raw = str(node.args.get("operator", ""))
        op = raw.rsplit(".", 1)[-1]
        lhs = node.this
        rhs = node.expression
        if op == "~":
            return exp.Anonymous(this="regexp_matches", expressions=[lhs, rhs])
        if op == "~*":
            return exp.Anonymous(
                this="regexp_matches",
                expressions=[lhs, rhs, exp.Literal.string("i")],
            )
        if op == "!~":
            return exp.Not(this=exp.Anonymous(
                this="regexp_matches", expressions=[lhs, rhs],
            ))
        if op == "!~*":
            return exp.Not(this=exp.Anonymous(
                this="regexp_matches",
                expressions=[lhs, rhs, exp.Literal.string("i")],
            ))
        return exp.condition(f"({lhs.sql()}) {op} ({rhs.sql()})")

    # psql's ``COLLATE pg_catalog."default"`` has no DuckDB equivalent; byte-wise suits ASCII names.
    @staticmethod
    def _strip_collate_clause(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Collate):
            return node.this
        return node

    # psql's ``CAST(x AS pg_catalog.text)`` → DuckDB type; unknown → VARCHAR.
    _PG_TO_DUCKDB_TYPES: dict[str, str] = {
        "text": "VARCHAR", "varchar": "VARCHAR", "char": "VARCHAR",
        "name": "VARCHAR",
        "int2": "SMALLINT", "int4": "INTEGER", "int8": "BIGINT",
        "oid": "BIGINT",
        "float4": "REAL", "float8": "DOUBLE", "numeric": "DECIMAL",
        "bool": "BOOLEAN", "date": "DATE",
        "timestamp": "TIMESTAMP", "timestamptz": "TIMESTAMP",
        "bytea": "BLOB",
    }
    # Kept as bare reg* types so the OID-lookup passes handle them.
    _PG_REG_TYPES = frozenset({
        "regclass", "regproc", "regtype",
        "regoper", "regoperator", "regprocedure",
    })

    @classmethod
    def _rewrite_schema_qualified_cast(cls, node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.DataType):
            return node
        # sqlglot: ``pg_catalog.<type>`` = USERDEFINED with a dotted ``kind``.
        if node.args.get("this") != exp.DataType.Type.USERDEFINED:
            return node
        kind = node.args.get("kind")
        if not isinstance(kind, exp.Dot):
            return node
        lhs = kind.this
        rhs = kind.expression
        if not (isinstance(lhs, exp.Identifier) and str(lhs.this).lower() == "pg_catalog"):
            return node
        if not isinstance(rhs, exp.Identifier):
            return node
        pg_type = str(rhs.this).lower()
        if pg_type in cls._PG_REG_TYPES:
            return exp.DataType.build(pg_type.upper(), dialect="postgres")
        duckdb_type = cls._PG_TO_DUCKDB_TYPES.get(pg_type, "VARCHAR")
        return exp.DataType.build(duckdb_type)


_DUCKDB_TO_DATATYPE: dict[str, DataType] = {
    "TINYINT": DataType.INTEGER, "SMALLINT": DataType.INTEGER,
    "INTEGER": DataType.INTEGER, "BIGINT": DataType.INTEGER, "HUGEINT": DataType.INTEGER,
    "UTINYINT": DataType.INTEGER, "USMALLINT": DataType.INTEGER,
    "UINTEGER": DataType.INTEGER, "UBIGINT": DataType.INTEGER,
    "INT1": DataType.INTEGER, "INT2": DataType.INTEGER, "INT4": DataType.INTEGER,
    "INT8": DataType.INTEGER,
    "BIGINT[]": DataType.STRING,  # arrays fall back to TEXT
    "REAL": DataType.FLOAT, "FLOAT": DataType.FLOAT,
    "DOUBLE": DataType.FLOAT, "DECIMAL": DataType.FLOAT,
    "NUMERIC": DataType.FLOAT, "FLOAT4": DataType.FLOAT,
    "FLOAT8": DataType.FLOAT,
    "BOOLEAN": DataType.BOOLEAN, "BOOL": DataType.BOOLEAN,
    "DATE": DataType.DATE,
    "TIMESTAMP": DataType.DATETIME,
    "TIMESTAMP_NS": DataType.DATETIME, "TIMESTAMP_MS": DataType.DATETIME,
    "TIMESTAMP_S": DataType.DATETIME,
    "DATETIME": DataType.DATETIME,
    "VARCHAR": DataType.STRING, "TEXT": DataType.STRING, "STRING": DataType.STRING,
    "CHAR": DataType.STRING, "BPCHAR": DataType.STRING,
}


def _duckdb_typename_to_datatype(typename: str) -> DataType:
    """Map a DuckDB type name to a coarse ``DataType``; unmapped → STRING."""
    base = str(typename).split("(")[0].split("[")[0].strip().upper()
    return _DUCKDB_TO_DATATYPE.get(base, DataType.STRING)


_KIND_TO_DUCKDB_CREATE: dict[TypeKind, str] = {
    TypeKind.INT: "BIGINT",
    TypeKind.DOUBLE: "DOUBLE",
    TypeKind.TEXT: "VARCHAR",
    TypeKind.BOOLEAN: "BOOLEAN",
    TypeKind.DATE: "DATE",
    TypeKind.TIMESTAMP: "TIMESTAMP",
}
_DATATYPE_TO_DUCKDB_CREATE: dict[DataType, str] = {
    t: _KIND_TO_DUCKDB_CREATE[t.kind] for t in DataType if t.kind in _KIND_TO_DUCKDB_CREATE}


class CatalogSqlExecutor:
    """In-memory DuckDB with the catalog materialised; expensive — cache via ``executor_for``."""

    def __init__(
        self,
        *,
        catalog: FacadeCatalog,
        datasource: str,
        extra_relations: Iterable[CatalogRelation] | None = None,
    ) -> None:
        self._datasource = datasource
        self._conn = duckdb.connect(":memory:")
        # Block file/HTTP readers (``read_text('/etc/hostname')``) and lock config against re-enabling.
        self._conn.execute("SET enable_external_access = false")
        try:
            self._conn.execute("SET lock_configuration = true")
        except duckdb.Error:
            pass  # older DuckDB lacks lock_configuration
        # regclass names: ``<schema>.<t>`` (else COL_DESCRIPTION gets OID 0), ``public.<t>`` alias, bare ``<t>``.
        self._regclass_map: dict[str, int] = dict(KNOWN_SYSTEM_OIDS)
        for ds, tbl in _all_tables(catalog):
            oid = _table_oid(ds, tbl)
            self._regclass_map[f"{ds}.{tbl.name}"] = oid
            self._regclass_map[f"public.{tbl.name}"] = oid
            self._regclass_map[tbl.name] = oid
        self._rewriter = _AstRewriter(
            datasource=datasource, regclass_map=self._regclass_map,
        )
        relations = build_catalog_relations(
            catalog, datasource, extra_relations=extra_relations,
        )
        self._registered_tables: set[str] = set()
        for relation in relations:
            self._register_relation(relation)
        self._register_stubs()

    def _register_relation(self, relation: CatalogRelation) -> None:
        table = exp.table_(relation.name, quoted=True)
        column_defs = [
            exp.ColumnDef(
                this=exp.to_identifier(c.name, quoted=True),
                kind=exp.DataType.build(_DATATYPE_TO_DUCKDB_CREATE[c.type], dialect="duckdb"),
            )
            for c in relation.columns
        ]
        create = exp.Create(this=exp.Schema(this=table, expressions=column_defs), kind="TABLE")
        self._conn.execute(create.sql(dialect="duckdb"))
        self._registered_tables.add(relation.name.lower())
        if not relation.rows:
            return
        col_names = [c.name for c in relation.columns]
        params = [tuple(row.get(name) for name in col_names) for row in relation.rows]
        insert = exp.insert(
            expression=exp.Values(expressions=[exp.Tuple(expressions=[exp.Placeholder() for _ in col_names])]),
            into=table.copy(),
            columns=[exp.to_identifier(n, quoted=True) for n in col_names],
        )
        self._conn.executemany(insert.sql(dialect="duckdb"), params)

    def _register_stubs(self) -> None:
        macros = [
            "CREATE MACRO _slayer_format_type(p_oid, p_typmod) AS "
            "    COALESCE((SELECT typname FROM pg_type WHERE oid = p_oid LIMIT 1), 'text')",
            "CREATE MACRO _slayer_obj_description(p_oid) AS "
            "    (SELECT description FROM pg_description "
            "     WHERE objoid = p_oid AND objsubid = 0 LIMIT 1)",
            "CREATE MACRO _slayer_col_description(p_oid, p_attnum) AS "
            "    (SELECT description FROM pg_description "
            "     WHERE objoid = p_oid AND objsubid = p_attnum LIMIT 1)",
            # Struct so ``(stub).x`` / ``(stub).n`` field access binds.
            "CREATE MACRO _slayer_pg_expandarray(p_arr) AS "
            "    STRUCT_PACK(x := CAST(NULL AS INTEGER), n := CAST(NULL AS INTEGER))",
        ]
        for sql in macros:
            self._conn.execute(sql)
        self._conn.create_function(
            "slayer_regclass_oid", self._regclass_oid,
            ["VARCHAR"], "INTEGER",
        )

    def _regclass_oid(self, text: str | None) -> int:
        if text is None:
            return 0
        return self._regclass_map.get(text, self._regclass_map.get(text.lower(), 0))

    _SYNTHETIC_SCHEMAS: frozenset[str] = frozenset({
        "pg_catalog", "information_schema",
    })

    def _synthesize_missing_catalog_tables(
        self, root: exp.Expression,
    ) -> exp.Expression:
        """Replace unregistered ``pg_catalog.<x>``/``information_schema.<x>`` refs with an empty subquery.

        Columns are those the query references off the table; bare (user-schema)
        names never get synthesized, so typos stay loud.
        """
        replacements: list[tuple[exp.Table, exp.Subquery]] = []
        for tbl in root.find_all(exp.Table):
            db_part = tbl.args.get("db")
            if db_part is None:
                continue
            schema = (
                str(db_part.this) if hasattr(db_part, "this") else str(db_part)
            ).lower()
            if schema not in self._SYNTHETIC_SCHEMAS:
                continue
            name = str(tbl.name).lower()
            if name in self._registered_tables:
                continue
            if schema == "information_schema" and f"_is_{name}" in self._registered_tables:
                continue
            handle = tbl.alias or tbl.name
            columns = self._discover_referenced_columns(root, handle)
            if not columns:
                columns = ["oid"]  # e.g. ``SELECT *``: DuckDB needs something to project
            # Unaliased info-schema refs must be ``_is_<name>``, matching the column-qualifier rewrite.
            output_alias = handle
            if schema == "information_schema" and not tbl.alias:
                output_alias = f"_is_{name}"
            subq = self._build_empty_subquery(columns, alias=output_alias)
            replacements.append((tbl, subq))
            logger.warning(
                "pg-facade: synthesized empty relation for unknown catalog "
                "table %r (columns: %s) — consider adding a stub in "
                "build_catalog_relations if this recurs.",
                (schema or "pg_catalog") + "." + name, ", ".join(columns),
            )
        for old, new in replacements:
            old.replace(new)
        return root

    @staticmethod
    def _discover_referenced_columns(
        root: exp.Expression, handle: str,
    ) -> list[str]:
        """Distinct column names referenced as ``handle.col`` or bare ``col``, first-seen order.

        Over-including bare columns is harmless: the synthetic relation is empty.
        """
        seen: dict[str, None] = {}
        handle_lower = handle.lower()
        for col in root.find_all(exp.Column):
            tbl_ident = col.args.get("table")
            if tbl_ident is not None:
                tbl_name = (
                    str(tbl_ident.this) if hasattr(tbl_ident, "this")
                    else str(tbl_ident)
                ).lower()
                if tbl_name != handle_lower:
                    continue
            colname = str(col.name)
            if colname and colname not in seen:
                seen[colname] = None
        return list(seen.keys())

    @staticmethod
    def _build_empty_subquery(
        columns: list[str], *, alias: str,
    ) -> exp.Subquery:
        """Emit ``(SELECT NULL::VARCHAR AS c1, ... WHERE FALSE) AS <alias>``."""
        varchar = exp.DataType.build("VARCHAR", dialect="duckdb")
        parsed_sub = exp.select(
            *(exp.alias_(exp.cast(exp.null(), to=varchar), c, quoted=True) for c in columns)
        ).where(exp.false())
        return exp.Subquery(
            this=parsed_sub,
            alias=exp.TableAlias(this=exp.to_identifier(alias)),
        )

    def execute(self, *, parsed: exp.Expression, sql: str) -> RowBatch:
        # Before the rewriter strips the qualifiers that mark a ref as catalog-qualified.
        parsed = self._synthesize_missing_catalog_tables(parsed.copy())
        rewritten = self._rewriter.rewrite(parsed)
        try:
            duckdb_sql = rewritten.sql(dialect="duckdb")
            cursor = self._conn.execute(duckdb_sql)
            description = cursor.description
            data_rows = cursor.fetchall()
        except Exception as exc:  # noqa: BLE001 — every DuckDB failure surfaces
            from slayer.facade.translator import TranslationError
            logger.warning("catalog-sql exec failed: %s\nSQL: %s", exc, sql)
            raise TranslationError(str(exc)) from exc
        if description is None:
            return RowBatch(columns=[], rows=[])
        columns: list[FacadeColumn] = []
        col_keys: list[str] = []
        seen_keys: dict[str, int] = {}
        for col in description:
            name = col[0]
            typename = col[1] if len(col) > 1 else "VARCHAR"
            # Duplicate output names keep their wire name; dict keys get ``__<n>`` suffixes.
            base_key = name
            n = seen_keys.get(base_key, 0)
            seen_keys[base_key] = n + 1
            key = base_key if n == 0 else f"{base_key}__{n + 1}"
            columns.append(FacadeColumn(
                name=name, type=_duckdb_typename_to_datatype(str(typename)),
            ))
            col_keys.append(key)
        rows = [
            {col_keys[i]: value for i, value in enumerate(row)}
            for row in data_rows
        ]
        batch = RowBatch(columns=columns, rows=rows)
        # Non-schema attribute: positional row keys for wire emitters when names duplicate.
        object.__setattr__(batch, "_row_keys", col_keys)
        return batch


_EXECUTOR_CACHE_LIMIT = 4
_EXECUTOR_CACHE: "collections.OrderedDict[str, CatalogSqlExecutor]" = collections.OrderedDict()


def _fingerprint(catalog: FacadeCatalog, datasource: str) -> str:
    """Stable executor cache key; tables sorted, columns not (their order drives ``attnum``)."""
    summary = [
        catalog.catalog_name,
        datasource,
        sorted([(
            sch.name, tbl.name, tbl.table_type, tbl.description,
            [
                (d.name, d.data_type.value, d.description, d.label, d.is_time, d.row_preserving)
                for d in tbl.dimensions
            ],
            [
                (m.name, m.data_type.value if m.data_type else None,
                 m.description, m.label)
                for m in tbl.metrics
            ],
        ) for sch in catalog.schemas for tbl in sch.tables]),
    ]
    payload = json.dumps(summary, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def executor_for(
    catalog: FacadeCatalog,
    datasource: str | None = None,
    *,
    extra_relations: Iterable[CatalogRelation] | None = None,
) -> CatalogSqlExecutor:
    """Return a process-cached ``CatalogSqlExecutor`` for ``catalog``.

    ``datasource`` (default: first schema name) is what ``current_database()`` returns.
    Passing ``extra_relations`` bypasses the cache — hold the returned executor.
    """
    if datasource is None:
        datasource = (
            catalog.schemas[0].name if catalog.schemas else catalog.catalog_name
        )
    if extra_relations is not None:
        return CatalogSqlExecutor(
            catalog=catalog, datasource=datasource,
            extra_relations=extra_relations,
        )
    fp = _fingerprint(catalog, datasource)
    cached = _EXECUTOR_CACHE.get(fp)
    if cached is not None:
        return cached
    executor = CatalogSqlExecutor(catalog=catalog, datasource=datasource)
    _EXECUTOR_CACHE[fp] = executor
    if len(_EXECUTOR_CACHE) > _EXECUTOR_CACHE_LIMIT:
        _EXECUTOR_CACHE.popitem(last=False)
    return executor


__all__ = [
    "CatalogRelation",
    "CatalogSqlExecutor",
    "KNOWN_SYSTEM_OIDS",
    "build_catalog_relations",
    "executor_for",
    "is_catalog_only",
    "stable_oid",
]


_ = sqlglot  # quiet linters about the conditional import
