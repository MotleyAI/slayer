"""DuckDB-backed catalog SQL executor (DEV-1558).

Replaces the canned-row ``match_pg_catalog`` and (for the Postgres facade)
``match_info_schema`` matching with arbitrary SQL execution against an
in-memory DuckDB that materialises the catalog corpus as flat tables under
the ``main`` schema. The translator routes catalog-shaped queries here when
``catalog_sql_executor`` is provided; Flight's path keeps the canned
``match_info_schema`` answer unchanged.

The pipeline:

1. Pre-rewrite the parsed AST: strip ``pg_catalog.`` and
   ``information_schema.`` qualifiers (info-schema tables rewrite to
   ``_is_<X>``); rewrite ``::regclass`` casts (literal → OID, dynamic →
   ``slayer_regclass_oid(...)`` UDF); rewrite ``::regproc``/``::regtype`` to
   ``0``; substitute ``current_database``/``current_catalog``/
   ``current_user``/``session_user``/``current_role`` with stored literals;
   short-circuit ``current_schemas(true)[1]`` to ``'public'``; rewrite
   Postgres regex operators (``~``/``!~``/``~*``/``!~*``) to
   ``regexp_matches``; AST-rename every stub function (``format_type``,
   ``obj_description`` …) to a private ``_slayer_*`` name so the macros
   can't be shadowed by DuckDB built-ins.
2. Transpile postgres → duckdb via sqlglot.
3. Execute synchronously.
4. Map DuckDB cursor description → ``RowBatch`` with the six coarse
   ``DataType``s.

Errors → ``TranslationError`` after a WARNING log carrying the offending
SQL.

Caching: ``executor_for(catalog)`` returns a process-cached executor keyed
by a SHA-256 fingerprint of a compact ``FacadeCatalog`` summary; FIFO
eviction at 4 entries. No lock — single-threaded asyncio + sync execute.
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

from slayer.core.enums import DataType
from slayer.facade.catalog import (
    CATALOG_NAME,
    FacadeCatalog,
    FacadeTable,
    local_dimensions,
    local_metrics,
)
from slayer.facade.rows import FacadeColumn, RowBatch
from slayer.pg_facade.types import datatype_to_oid

logger = logging.getLogger(__name__)


# --- OID constants (moved from slayer/pg_facade/pg_catalog.py) ---------------

PUBLIC_NAMESPACE_OID = 2200
PG_CATALOG_NAMESPACE_OID = 11
DEFAULT_OWNER_OID = 10

# Well-known Postgres system catalog OIDs. Hardcoded so ``'pg_class'::regclass``
# resolves to ``1259`` (matching ``pg_description.classoid``) and Metabase's
# get-tables JOIN works end-to-end.
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
    # psql backslash-command coverage stubs.
    "pg_am": 2601,
    "pg_database": 1262,
    "pg_authid": 1260,
}

# Postgres heap access-method OID. Hardcoded to match real Postgres so
# ``pg_class.relam = 2`` round-trips against any tool that knows the
# canonical value.
PG_AM_HEAP_OID = 2
# Single synthetic role for ``\du`` / ``pg_get_userbyid``. The facade's
# auth model is shared-token, so there's exactly one principal.
PG_SLAYER_ROLE_OID = 10
PG_SLAYER_ROLE_NAME = "slayer"
# UTF8 is the only encoding the facade ever advertises.
PG_ENCODING_UTF8 = 6

# Per-OID metadata (typname, typlen, typcategory) for the six wire types.
from slayer.pg_facade.protocol import (  # noqa: E402 — wire OIDs co-located here
    OID_BOOL,
    OID_DATE,
    OID_FLOAT8,
    OID_INT8,
    OID_TEXT,
    OID_TIMESTAMP,
)

_TYPE_META: dict[int, tuple[str, int, str]] = {
    OID_BOOL: ("bool", 1, "B"),
    OID_INT8: ("int8", 8, "N"),
    OID_TEXT: ("text", -1, "S"),
    OID_FLOAT8: ("float8", 8, "N"),
    OID_DATE: ("date", 4, "D"),
    OID_TIMESTAMP: ("timestamp", 8, "D"),
}

# Inverse of _TYPE_META: type name → OID. Used to resolve ``::regtype``
# casts to the underlying ``pg_type.oid`` so catalog queries like
# ``WHERE oid = 'int8'::regtype`` work.
_KNOWN_TYPE_OIDS: dict[str, int] = {
    typname: oid for oid, (typname, _len, _cat) in _TYPE_META.items()
}


_UDT_NAME_BY_DATATYPE: dict[DataType, str] = {
    DataType.BOOLEAN: "bool",
    DataType.INT: "int8",
    DataType.TEXT: "text",
    DataType.DOUBLE: "float8",
    DataType.DATE: "date",
    DataType.TIMESTAMP: "timestamp",
}


def stable_oid(*parts: str) -> int:
    """Deterministic positive 31-bit OID from a namespaced identifier."""
    key = ".".join(parts).encode("utf-8")
    return zlib.crc32(key) & 0x7FFFFFFF


# --- CatalogRelation Pydantic ------------------------------------------------


class CatalogRelation(BaseModel):
    """One catalog table's content — facade-neutral.

    ``columns`` is a list of typed FacadeColumns; ``rows`` is a list of
    ``{column_name: value}`` dicts.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    columns: list[FacadeColumn]
    rows: list[dict[str, Any]]


# --- corpus builder ---------------------------------------------------------


def build_catalog_relations(
    catalog: FacadeCatalog,
    datasource: str | None = None,
    *,
    extra_relations: Iterable[CatalogRelation] | None = None,
) -> list[CatalogRelation]:
    """Build every catalog table from ``catalog``.

    ``datasource`` is used as the ``catalog_name`` / ``table_catalog``
    value in the ``information_schema.*`` relations so queries that
    filter by ``current_database()`` (which the AST rewrite substitutes
    with the connection's datasource) see consistent rows. When omitted,
    falls back to the catalog's static name (``slayer``) for backward
    compatibility.

    ``extra_relations`` is the extensibility hook for embedders: each
    ``CatalogRelation`` provided either **replaces** the default builder's
    output for that table name (override case — e.g. project real per-tenant
    rows into ``pg_roles``) or **adds** a new relation the default doesn't
    know about. Override is by table-name match; order of the returned
    list places overrides where the default originally appeared, with
    additions appended at the end.
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
    """Yield ``(name, DataType)`` for every projectable column (dims +
    metrics).

    DEV-1567: cross-model entries are excluded so ``pg_attribute`` /
    ``pg_class.relnatts`` advertise only the same-model column list. BI
    tools that flatten the catalog into a column view (Metabase, dbt
    schema scan) discover only base columns and don't issue fingerprint
    queries that would lead to dotted ``SlayerQuery.measures[*].name``.
    """
    for d in local_dimensions(table):
        yield d.name, d.data_type
    for m in local_metrics(table):
        yield m.name, m.data_type if m.data_type is not None else DataType.TEXT


def _namespace_oid(schema: str) -> int:
    """Stable namespace OID for a facade schema. ``public`` keeps Postgres's
    well-known 2200 so clients that hardcode it keep working; other
    ``postgres_schema`` values get a deterministic derived OID."""
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
        FacadeColumn(name="oid", type=DataType.INT),
        FacadeColumn(name="nspname", type=DataType.TEXT),
        FacadeColumn(name="nspowner", type=DataType.INT),
        FacadeColumn(name="nspacl", type=DataType.TEXT),
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
        # Access-method OID — points at ``pg_am.oid``. psql's ``\d`` LEFT
        # JOINs on this; ``heap`` is the only access method we advertise.
        FacadeColumn(name="relam", type=DataType.INT),
    ]
    rows = []
    seen_oids: dict[int, str] = {}
    for ds, tbl in _all_tables(catalog):
        oid = _table_oid(ds, tbl)
        _check_collision(seen_oids, oid, f"{ds}.{tbl.name}")
        natts = sum(1 for _ in _column_specs(tbl))
        # SQL-backed models are advertised as views (relkind='v') so
        # ``pg_views`` discovery and JDBC view-specific paths see them;
        # ``sql_table``-mode models stay as regular tables ('r').
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
        })
    return CatalogRelation(name="pg_class", columns=columns, rows=rows)


def _build_pg_attribute(catalog: FacadeCatalog) -> CatalogRelation:
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
                "attrelid": attrelid, "attname": name, "atttypid": oid,
                "attnum": attnum, "attlen": _TYPE_META[oid][1],
                "atttypmod": -1, "attnotnull": False, "atthasdef": False,
                "attisdropped": False, "attidentity": "", "attgenerated": "",
            })
            attnum += 1
    return CatalogRelation(name="pg_attribute", columns=columns, rows=rows)


def _build_pg_type() -> CatalogRelation:
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
        FacadeColumn(name="typnotnull", type=DataType.BOOLEAN),
        FacadeColumn(name="typbasetype", type=DataType.INT),
        FacadeColumn(name="typtypmod", type=DataType.INT),
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
        })
    return CatalogRelation(name="pg_type", columns=columns, rows=rows)


def _build_pg_proc() -> CatalogRelation:
    return CatalogRelation(name="pg_proc", columns=[
        FacadeColumn(name="oid", type=DataType.INT),
        FacadeColumn(name="proname", type=DataType.TEXT),
        FacadeColumn(name="pronamespace", type=DataType.INT),
        FacadeColumn(name="prorettype", type=DataType.INT),
    ], rows=[])


def _build_pg_settings() -> CatalogRelation:
    from slayer.pg_facade.identity import PG_SERVER_VERSION
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
    rows = [{"name": name, "setting": value, "category": "Preset Options",
             "unit": None, "source": "default", "vartype": "string",
             "context": "user", "min_val": None, "max_val": None}
            for name, value in settings]
    return CatalogRelation(name="pg_settings", columns=columns, rows=rows)


def _build_pg_description(catalog: FacadeCatalog) -> CatalogRelation:
    columns = [
        FacadeColumn(name="objoid", type=DataType.INT),
        FacadeColumn(name="classoid", type=DataType.INT),
        FacadeColumn(name="objsubid", type=DataType.INT),
        FacadeColumn(name="description", type=DataType.TEXT),
    ]
    rows: list[dict[str, Any]] = []
    for ds, tbl in _all_tables(catalog):
        oid = _table_oid(ds, tbl)
        if tbl.description:
            rows.append({"objoid": oid, "classoid": KNOWN_SYSTEM_OIDS["pg_class"],
                         "objsubid": 0, "description": tbl.description})
        attnum = 1
        # DEV-1567: stay within the (filtered) pg_attribute attnum space so
        # ``objsubid`` always matches a real pg_attribute row.
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
        FacadeColumn(name="schemaname", type=DataType.TEXT),
        FacadeColumn(name="relname", type=DataType.TEXT),
        FacadeColumn(name="n_live_tup", type=DataType.INT),
    ]
    rows = [{"schemaname": ds, "relname": tbl.name, "n_live_tup": None}
            for ds, tbl in _all_tables(catalog)]
    return CatalogRelation(name="pg_stat_user_tables", columns=columns, rows=rows)


def _build_pg_enum() -> CatalogRelation:
    return CatalogRelation(name="pg_enum", columns=[
        FacadeColumn(name="oid", type=DataType.INT),
        FacadeColumn(name="enumtypid", type=DataType.INT),
        FacadeColumn(name="enumlabel", type=DataType.TEXT),
    ], rows=[])


def _build_pg_tables(catalog: FacadeCatalog) -> CatalogRelation:
    columns = [
        FacadeColumn(name="schemaname", type=DataType.TEXT),
        FacadeColumn(name="tablename", type=DataType.TEXT),
        FacadeColumn(name="tableowner", type=DataType.TEXT),
        FacadeColumn(name="tablespace", type=DataType.TEXT),
        FacadeColumn(name="hasindexes", type=DataType.BOOLEAN),
        FacadeColumn(name="hasrules", type=DataType.BOOLEAN),
        FacadeColumn(name="hastriggers", type=DataType.BOOLEAN),
        FacadeColumn(name="rowsecurity", type=DataType.BOOLEAN),
    ]
    rows = []
    for ds, tbl in _all_tables(catalog):
        # M1 — VIEW-typed models are excluded from pg_tables.
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
        FacadeColumn(name="schemaname", type=DataType.TEXT),
        FacadeColumn(name="viewname", type=DataType.TEXT),
        FacadeColumn(name="viewowner", type=DataType.TEXT),
        FacadeColumn(name="definition", type=DataType.TEXT),
    ]
    # SQL-backed models surface here so view-aware clients
    # (pgAdmin, dbeaver view category, JDBC ``getTables`` with
    # ``types=['VIEW']``) discover them as views. We do NOT include the
    # underlying SQL definition — it's a SLayer abstraction detail and
    # exposing it would leak datasource SQL through the facade.
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
        FacadeColumn(name="schemaname", type=DataType.TEXT),
        FacadeColumn(name="matviewname", type=DataType.TEXT),
        FacadeColumn(name="matviewowner", type=DataType.TEXT),
        FacadeColumn(name="tablespace", type=DataType.TEXT),
        FacadeColumn(name="hasindexes", type=DataType.BOOLEAN),
        FacadeColumn(name="ispopulated", type=DataType.BOOLEAN),
        FacadeColumn(name="definition", type=DataType.TEXT),
    ], rows=[])


def _build_pg_constraint() -> CatalogRelation:
    return CatalogRelation(name="pg_constraint", columns=[
        FacadeColumn(name="oid", type=DataType.INT),
        FacadeColumn(name="conname", type=DataType.TEXT),
        FacadeColumn(name="contype", type=DataType.TEXT),
        FacadeColumn(name="conrelid", type=DataType.INT),
        FacadeColumn(name="confrelid", type=DataType.INT),
        FacadeColumn(name="connamespace", type=DataType.INT),
        FacadeColumn(name="conkey", type=DataType.TEXT),
        FacadeColumn(name="confkey", type=DataType.TEXT),
    ], rows=[])


def _build_pg_index() -> CatalogRelation:
    return CatalogRelation(name="pg_index", columns=[
        FacadeColumn(name="indexrelid", type=DataType.INT),
        FacadeColumn(name="indrelid", type=DataType.INT),
        FacadeColumn(name="indnatts", type=DataType.INT),
        FacadeColumn(name="indnkeyatts", type=DataType.INT),
        FacadeColumn(name="indisunique", type=DataType.BOOLEAN),
        FacadeColumn(name="indisprimary", type=DataType.BOOLEAN),
        FacadeColumn(name="indkey", type=DataType.TEXT),
    ], rows=[])


def _build_pg_attrdef() -> CatalogRelation:
    return CatalogRelation(name="pg_attrdef", columns=[
        FacadeColumn(name="oid", type=DataType.INT),
        FacadeColumn(name="adrelid", type=DataType.INT),
        FacadeColumn(name="adnum", type=DataType.INT),
        FacadeColumn(name="adbin", type=DataType.TEXT),
    ], rows=[])


def _build_pg_am() -> CatalogRelation:
    """Stub access-method table. Real Postgres exposes heap/btree/hash/gist/gin/
    brin; for the facade only ``heap`` matters (every ``pg_class.relam`` we emit
    points at it). psql's ``\\d`` LEFT JOINs ``pg_class.relam = pg_am.oid``;
    without ``heap`` the JOIN would yield NULL for every relation."""
    return CatalogRelation(
        name="pg_am",
        columns=[
            FacadeColumn(name="oid", type=DataType.INT),
            FacadeColumn(name="amname", type=DataType.TEXT),
            FacadeColumn(name="amhandler", type=DataType.INT),
            FacadeColumn(name="amtype", type=DataType.TEXT),
        ],
        rows=[
            {"oid": PG_AM_HEAP_OID, "amname": "heap",
             "amhandler": 0, "amtype": "t"},
        ],
    )


def _build_pg_roles() -> CatalogRelation:
    """Stub roles table. SLayer's auth is shared-token (one principal at a
    time); ``\\du`` gets one synthetic row so the listing renders. Embedders
    can override via the ``extra_relations`` hook on ``build_catalog_relations``
    to project real per-tenant principals."""
    return CatalogRelation(
        name="pg_roles",
        columns=[
            FacadeColumn(name="oid", type=DataType.INT),
            FacadeColumn(name="rolname", type=DataType.TEXT),
            FacadeColumn(name="rolsuper", type=DataType.BOOLEAN),
            FacadeColumn(name="rolinherit", type=DataType.BOOLEAN),
            FacadeColumn(name="rolcreaterole", type=DataType.BOOLEAN),
            FacadeColumn(name="rolcreatedb", type=DataType.BOOLEAN),
            FacadeColumn(name="rolcanlogin", type=DataType.BOOLEAN),
            FacadeColumn(name="rolreplication", type=DataType.BOOLEAN),
            FacadeColumn(name="rolconnlimit", type=DataType.INT),
            FacadeColumn(name="rolvaliduntil", type=DataType.TIMESTAMP),
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
    """Stub databases table. The facade scopes one connection to one SLayer
    datasource (= one Postgres ``database``); ``\\l`` returns a single row for
    the connected datasource. Embedders that want to enumerate every available
    datasource (multi-tenant management UIs, etc.) override this builder."""
    return CatalogRelation(
        name="pg_database",
        columns=[
            FacadeColumn(name="oid", type=DataType.INT),
            FacadeColumn(name="datname", type=DataType.TEXT),
            FacadeColumn(name="datdba", type=DataType.INT),
            FacadeColumn(name="encoding", type=DataType.INT),
            FacadeColumn(name="datcollate", type=DataType.TEXT),
            FacadeColumn(name="datctype", type=DataType.TEXT),
            FacadeColumn(name="datistemplate", type=DataType.BOOLEAN),
            FacadeColumn(name="datallowconn", type=DataType.BOOLEAN),
            FacadeColumn(name="datconnlimit", type=DataType.INT),
            FacadeColumn(name="dattablespace", type=DataType.INT),
            FacadeColumn(name="datacl", type=DataType.TEXT),
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
    """Postgres-shape information_schema.columns with SLayer extension fields.

    Materialised as ``_is_columns`` in DuckDB; the AST rewrite pass strips
    the ``information_schema.`` qualifier and rewrites the table name.
    """
    columns = [
        FacadeColumn(name="table_catalog", type=DataType.TEXT),
        FacadeColumn(name="table_schema", type=DataType.TEXT),
        FacadeColumn(name="table_name", type=DataType.TEXT),
        FacadeColumn(name="column_name", type=DataType.TEXT),
        FacadeColumn(name="ordinal_position", type=DataType.INT),
        FacadeColumn(name="column_default", type=DataType.TEXT),
        FacadeColumn(name="is_nullable", type=DataType.TEXT),
        FacadeColumn(name="data_type", type=DataType.TEXT),
        FacadeColumn(name="udt_schema", type=DataType.TEXT),
        FacadeColumn(name="udt_name", type=DataType.TEXT),
        FacadeColumn(name="is_identity", type=DataType.TEXT),
        FacadeColumn(name="is_generated", type=DataType.TEXT),
        # SLayer extension columns (Q3 sub-b-ii).
        FacadeColumn(name="column_kind", type=DataType.TEXT),
        FacadeColumn(name="description", type=DataType.TEXT),
        FacadeColumn(name="label", type=DataType.TEXT),
    ]
    rows: list[dict[str, Any]] = []
    # DEV-1567: exclude cross-model entries — they leak as dotted "columns"
    # that Metabase fingerprint scans then project (see local_metrics
    # docstring in slayer/facade/catalog.py).
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
        FacadeColumn(name="constraint_catalog", type=DataType.TEXT),
        FacadeColumn(name="constraint_schema", type=DataType.TEXT),
        FacadeColumn(name="constraint_name", type=DataType.TEXT),
        FacadeColumn(name="table_catalog", type=DataType.TEXT),
        FacadeColumn(name="table_schema", type=DataType.TEXT),
        FacadeColumn(name="table_name", type=DataType.TEXT),
        FacadeColumn(name="constraint_type", type=DataType.TEXT),
        FacadeColumn(name="is_deferrable", type=DataType.TEXT),
        FacadeColumn(name="initially_deferred", type=DataType.TEXT),
    ], rows=[])


def _build_is_key_column_usage() -> CatalogRelation:
    return CatalogRelation(name="_is_key_column_usage", columns=[
        FacadeColumn(name="constraint_catalog", type=DataType.TEXT),
        FacadeColumn(name="constraint_schema", type=DataType.TEXT),
        FacadeColumn(name="constraint_name", type=DataType.TEXT),
        FacadeColumn(name="table_catalog", type=DataType.TEXT),
        FacadeColumn(name="table_schema", type=DataType.TEXT),
        FacadeColumn(name="table_name", type=DataType.TEXT),
        FacadeColumn(name="column_name", type=DataType.TEXT),
        FacadeColumn(name="ordinal_position", type=DataType.INT),
    ], rows=[])


def _build_is_schemata(
    catalog: FacadeCatalog, datasource: str,
) -> CatalogRelation:
    """INFORMATION_SCHEMA.SCHEMATA — one row per facade schema. Datasources
    map to schemas via ``postgres_schema`` (default ``public``); multiple
    datasources sharing a schema collapse to one row."""
    columns = [
        FacadeColumn(name="catalog_name", type=DataType.TEXT),
        FacadeColumn(name="schema_name", type=DataType.TEXT),
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
        FacadeColumn(name="table_catalog", type=DataType.TEXT),
        FacadeColumn(name="table_schema", type=DataType.TEXT),
        FacadeColumn(name="table_name", type=DataType.TEXT),
        FacadeColumn(name="table_type", type=DataType.TEXT),
    ]
    rows = [{"table_catalog": datasource, "table_schema": sch.name,
             "table_name": tbl.name, "table_type": tbl.table_type}
            for sch in catalog.schemas for tbl in sch.tables]
    return CatalogRelation(name="_is_tables", columns=columns, rows=rows)


def _build_is_metrics(
    catalog: FacadeCatalog, datasource: str,
) -> CatalogRelation:
    """SLayer's INFORMATION_SCHEMA.METRICS extension — JDBC-style type
    names (``DOUBLE`` / ``BIGINT`` / ``TIMESTAMP``) to match the contract
    the canned ``match_info_schema._serve_metrics`` previously emitted."""
    from slayer.facade.datatypes import datatype_to_jdbc
    columns = [
        FacadeColumn(name="catalog_name", type=DataType.TEXT),
        FacadeColumn(name="schema_name", type=DataType.TEXT),
        FacadeColumn(name="table_name", type=DataType.TEXT),
        FacadeColumn(name="metric_name", type=DataType.TEXT),
        FacadeColumn(name="description", type=DataType.TEXT),
        FacadeColumn(name="data_type", type=DataType.TEXT),
        FacadeColumn(name="label", type=DataType.TEXT),
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
    """SLayer's INFORMATION_SCHEMA.DIMENSIONS extension — JDBC-style type
    names to match the contract the canned ``_serve_dimensions``
    previously emitted."""
    from slayer.facade.datatypes import datatype_to_jdbc
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
    rows: list[dict[str, Any]] = []
    for sch in catalog.schemas:
        for tbl in sch.tables:
            for d in tbl.dimensions:
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


# --- is_catalog_only --------------------------------------------------------

# The set of known catalog relation names (bare). Both `pg_catalog.X` and
# `information_schema.X` schema qualifiers are stripped before lookup; the
# pre-rewrite pass aliases information_schema names to `_is_<X>` so those go
# under their alias forms too.
#
# Must stay in lockstep with the relations built by ``build_catalog_relations``
# above — every ``out.append(_build_pg_<name>(...))`` needs a corresponding
# entry here, or the routing decision misclassifies the catalog query as a
# user-table reference and falls through to "Unknown schema: 'pg_catalog'".
_PG_CATALOG_NAMES = frozenset({
    "pg_namespace", "pg_class", "pg_attribute", "pg_type", "pg_proc",
    "pg_settings", "pg_description", "pg_stat_user_tables", "pg_enum",
    "pg_tables", "pg_views", "pg_matviews", "pg_constraint", "pg_index",
    "pg_attrdef",
    # psql backslash-command coverage stubs.
    "pg_am", "pg_roles", "pg_database",
})

_INFO_SCHEMA_NAMES = frozenset({
    "columns", "table_constraints", "key_column_usage",
    "schemata", "tables", "metrics", "dimensions",
})


def _is_known_catalog_table(tbl: exp.Table) -> bool:
    """True if ``tbl`` resolves to a known catalog relation.

    Bare names resolve ONLY to ``pg_catalog`` relations (whose ``pg_``
    prefix disambiguates them from user models). Bare
    ``information_schema`` names like ``columns`` / ``tables`` would
    otherwise hijack user models with the same name, so they require the
    explicit ``information_schema.`` qualifier.
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
        # Three-part catalog-qualified refs must name the SLayer catalog.
        # Anything else (a foreign catalog) is never our catalog SQL.
        if catalog != CATALOG_NAME.lower():
            return False
    if schema == "information_schema":
        return name in _INFO_SCHEMA_NAMES
    if schema == "pg_catalog":
        return name in _PG_CATALOG_NAMES
    if schema is None:
        # Bare names — pg_catalog only (bare info-schema names like
        # ``columns`` / ``tables`` are too generic and would shadow user
        # models with those names).
        return name in _PG_CATALOG_NAMES
    return False


def is_catalog_only(parsed: exp.Expression) -> bool:
    """True iff ``parsed`` references at least one known catalog relation
    or qualified catalog function, AND every Table node it walks resolves
    to a known catalog relation (or a same-statement CTE).

    Tableless SELECTs are NOT auto-routed to the executor (CR review
    feedback): the probe matcher already handles the standard tableless
    probes (``SELECT 1``, ``SELECT current_database()``, ``SHOW …``),
    and routing unknown tableless SQL through DuckDB would expand the
    facade's accepted SQL surface with DuckDB semantics. Only tableless
    SELECTs that explicitly reference catalog functions
    (``::regclass``, ``pg_catalog.<fn>``, ``information_schema.<fn>``)
    are accepted here.
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
    # Tableless: only catalog-only if the statement references a catalog
    # function explicitly (regclass cast, qualified pg_catalog/info_schema
    # function call, or a known stub function name).
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
    # sqlglot's class-key forms for the dedicated Func subclasses
    # (``type(node).key`` returns e.g. ``currentdatabase`` without the
    # underscore — they appear here via ``_function_name_lower``).
    "currentdatabase", "currentcatalog", "currentuser", "sessionuser",
    "currentrole", "currentschema", "currentschemas",
})


def _references_catalog_function(parsed: exp.Expression) -> bool:
    """True iff ``parsed`` contains a ``::regclass``/``::regproc``/
    ``::regtype`` cast, a ``pg_catalog.<fn>`` or
    ``information_schema.<fn>`` qualified function call, or a bare known
    stub function name. Used by ``is_catalog_only`` to admit tableless
    SELECTs only when they explicitly target catalog metadata."""
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
        # sqlglot parses bareword niladic context functions
        # (``current_role``, ``current_user``, ``current_catalog`` …) as
        # an unqualified ``Column``. Treat those as catalog-only too so
        # ``SELECT current_role`` routes to the executor instead of
        # falling through to the SLayer model-query path (which then
        # errors with "no FROM clause"). CR/Codex review.
        if isinstance(node, exp.Column) and not node.table:
            ident = node.this
            if isinstance(ident, exp.Identifier):
                name = str(ident.this).lower()
                if name in _CATALOG_FUNCTION_NAMES:
                    return True
    return False


# --- AST pre-rewrite pass ---------------------------------------------------


# Constant-return stubs are AST-rewritten directly to literals (no DuckDB
# macros). This sidesteps DuckDB's lack of macro arity overloading — the
# corpus has 2-arg and 3-arg variants of has_*_privilege.
_CONSTANT_STUB_LITERALS: dict[str, Any] = {
    "has_table_privilege": True,
    "has_any_column_privilege": True,
    "has_schema_privilege": True,
    "pg_get_userbyid": "slayer",
    "pg_table_is_visible": True,
    "pg_total_relation_size": 0,
    "pg_get_expr": None,
    # SLayer always emits UTF8; ``\l`` calls this with ``d.encoding``.
    "pg_encoding_to_char": "UTF8",
}

# Data-lookup stubs are AST-renamed to private names; the macros are
# registered with a single arity per name. ``obj_description`` has two
# Postgres arities; the 2-arg form is normalised to the 1-arg by dropping
# the second argument at rewrite time.
_LOOKUP_STUB_NAMES: dict[str, str] = {
    "format_type": "_slayer_format_type",
    "obj_description": "_slayer_obj_description",
    "col_description": "_slayer_col_description",
    "_pg_expandarray": "_slayer_pg_expandarray",
}


def _function_name_lower(node: exp.Expression) -> str | None:
    """Return the lowercased function name for any kind of function node,
    or None if ``node`` isn't a function call."""
    if isinstance(node, exp.Anonymous):
        n = node.args.get("this")
        if n is not None:
            return str(n).lower()
    if isinstance(node, exp.Func):
        # exp.Func sub-classes carry their name in `sql_name()` (CamelCase
        # class -> snake_case). We use the class's `key` attribute.
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
    """Detect ``information_schema.<stub>(args)`` (and the same for
    ``pg_catalog.``) and rewrite the qualified call as a bare private-name
    Anonymous so DuckDB resolves the macro.

    sqlglot parses ``information_schema._pg_expandarray(arr)`` as
    ``Dot(this=Identifier("information_schema"), expression=Anonymous("_pg_expandarray", [arr]))``.
    Returns the rewritten Anonymous, or None if no match.
    """
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
    # Unknown ``pg_catalog.<fn>`` / ``information_schema.<fn>`` — strip the
    # schema qualifier and emit a bare call. Many such names (``array_to_string``,
    # ``string_agg``, …) are real functions in DuckDB and resolve cleanly
    # without the qualifier. Leaving the Dot node in place makes DuckDB
    # interpret ``pg_catalog`` as a column reference and surface a
    # misleading "column not found" Binder error; baring the call yields
    # either a working resolution or a sensible "function not found".
    args = list(rhs.args.get("expressions") or [])
    return exp.Anonymous(this=name, expressions=args)


class _AstRewriter:
    """Encapsulates the pre-rewrite pass.

    The pass walks the AST top-down and applies, in order:

    1. Schema-qualifier strip on every Table node:
       ``pg_catalog.X`` → ``X``; ``information_schema.X`` → ``_is_X``.
    2. ``current_schemas(...)[1]`` → ``'public'`` (short-circuit).
    3. ``::regclass`` casts (literal + dynamic).
    4. ``::regproc`` / ``::regtype`` → ``0``.
    5. Substitute zero-arg ``current_database``/``current_catalog``/
       ``current_user``/``session_user``/``current_role`` with stored
       literals.
    6. Rename Postgres-only stubs (``format_type``, ``obj_description``…)
       to private ``_slayer_*`` names.
    7. Rewrite Postgres regex operators (``~``/``!~``/``~*``/``!~*``)
       to ``regexp_matches``/``NOT regexp_matches`` with the case flag.
    """

    def __init__(self, *, datasource: str,
                 regclass_map: dict[str, int] | None = None) -> None:
        self.datasource = datasource
        self.regclass_map: dict[str, int] = regclass_map or {}

    def rewrite(self, parsed: exp.Expression) -> exp.Expression:
        # Strip schema qualifiers AND rewrite information_schema names first
        # so subsequent passes see the canonical bare names.
        parsed = parsed.transform(self._strip_schema_qualifiers)
        parsed = parsed.transform(self._strip_column_schema_qualifiers)
        parsed = parsed.transform(self._rewrite_current_schemas_indexed)
        parsed = parsed.transform(self._rewrite_current_schemas_bare)
        parsed = parsed.transform(self._rewrite_pg_format_quoted_ident)
        parsed = parsed.transform(self._rewrite_regclass_casts)
        parsed = parsed.transform(self._rewrite_regproc_regtype_casts)
        parsed = parsed.transform(self._substitute_context_functions)
        parsed = parsed.transform(self._rename_stub_functions)
        parsed = parsed.transform(self._rewrite_regex_operators)
        parsed = parsed.transform(self._rewrite_pg_any_array)
        return parsed

    # ----- 1. schema qualifier strip ----------------------------------------

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
            # Drop any outer catalog qualifier too (e.g.
            # ``slayer.pg_catalog.pg_class`` → ``pg_class``).
            new.set("catalog", None)
            return new
        if schema_name == "information_schema":
            # Rewrite the table name itself: information_schema.X → _is_X.
            new = node.copy()
            new.set("db", None)
            new.set("catalog", None)
            inner_name = str(new.this.this) if hasattr(new.this, "this") else str(new.this)
            new.set("this", exp.Identifier(this=f"_is_{inner_name.lower()}", quoted=False))
            return new
        return node

    # ----- 1b. FORMAT('%I.%I', a, b) → CONCAT(a, '.', b) --------------------

    @staticmethod
    def _rewrite_pg_format_quoted_ident(node: exp.Expression) -> exp.Expression:
        """Postgres' ``FORMAT('%I.%I', a, b)`` quotes both arguments and
        returns ``"a"."b"``. DuckDB's ``FORMAT`` is printf-style and treats
        ``%I`` literally. Rewrite the schema-qualified ident pattern to a
        plain ``CONCAT(a, '.', b)`` so the regclass UDF sees ``public.orders``
        instead of an unfilled format string. Same shape applies to the
        single-argument ``FORMAT('%I', a)`` → ``a``.
        """
        if not isinstance(node, exp.Format):
            return node
        # sqlglot puts the spec at .this and the args under .expressions.
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

    # ----- 2. current_schemas(...)[1] → 'public' ----------------------------

    @staticmethod
    def _strip_column_schema_qualifiers(node: exp.Expression) -> exp.Expression:
        """Rewrite ``pg_catalog.<X>.<col>`` / ``information_schema.<X>.<col>``
        column refs so they match the underlying DuckDB table names
        after the FROM-side strip (Codex review).

        sqlglot's ``exp.Column`` represents the dotted qualifiers as
        ``this`` (leaf), ``table`` (the table-qualifier), ``db`` (the
        schema-qualifier), ``catalog`` (the catalog-qualifier — only
        for 4-part refs).

        * For ``pg_catalog`` the underlying table keeps the same name
          (``pg_namespace.nspname``); we drop ``db`` so the column
          resolves as a 2-part ``pg_namespace.nspname`` ref.
        * For ``information_schema`` the table is renamed to ``_is_<X>``
          by ``_strip_schema_qualifiers``; here we apply the same
          rename to the column's ``table`` qualifier so
          ``information_schema.columns.column_name`` resolves to
          ``_is_columns.column_name``.
        """
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
        # Drop any outer catalog qualifier too (e.g.
        # ``slayer.pg_catalog.pg_class.oid``) — symmetric with the
        # FROM-side strip in ``_strip_schema_qualifiers``. Otherwise the
        # 4-part column ref keeps a stale ``slayer.pg_class.oid`` form
        # that fails to bind against DuckDB's ``main.pg_class`` (Codex
        # round-20 follow-up).
        new.set("catalog", None)
        if schema == "information_schema" and table_part is not None:
            tbl_name = (
                str(table_part.this) if hasattr(table_part, "this") else str(table_part)
            ).lower()
            new.set("table", exp.Identifier(this=f"_is_{tbl_name}", quoted=False))
        return new

    @staticmethod
    def _rewrite_current_schemas_bare(node: exp.Expression) -> exp.Expression:
        """Rewrite a bare ``current_schemas(...)`` call (no bracket index)
        to ``['public']`` — the facade only advertises one schema, so
        the unindexed call must return the single-element list rather
        than fall through to DuckDB's internal schema list (CR/Codex
        review). Handles bare and ``pg_catalog.current_schemas(...)``
        qualified forms; the indexed form is rewritten separately by
        ``_rewrite_current_schemas_indexed``."""
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
        # Qualified ``pg_catalog.current_schemas(...)`` form.
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
        # Match Bracket(this=Paren?(CurrentSchemas|Anonymous("current_schemas")))
        # AND a single literal index ``[1]`` — other indices are not
        # safely collapsible to 'public' because the facade only advertises
        # one schema (cf. CR review feedback). sqlglot normalises 1-based
        # SQL array indices to 0-based internally, so user-level ``[1]``
        # arrives as ``Literal(0)``. Handles bare and
        # ``pg_catalog.current_schemas(...)`` qualified forms.
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

    # ----- 3. regclass casts ------------------------------------------------

    def _rewrite_regclass_casts(self, node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.Cast):
            return node
        target_kind = self._cast_target_kind(node)
        if target_kind != "regclass":
            return node
        inner = node.this
        # Static: CAST('foo' AS REGCLASS) → integer OID literal.
        if isinstance(inner, exp.Literal) and inner.is_string:
            text = inner.this  # raw string value (no quotes)
            return exp.Literal.number(self._lookup_static_regclass(text))
        # Dynamic: CAST(<expr> AS REGCLASS) → slayer_regclass_oid(<expr>).
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
        # Check the full schema-qualified name, then the bare leaf name.
        # The map carries both forms for user tables ('public.orders' AND
        # 'orders') plus the well-known system OIDs.
        return (
            self.regclass_map.get(text)
            or self.regclass_map.get(text.lower())
            or self.regclass_map.get(text.split(".")[-1].lower(), 0)
        )

    # ----- 4. regproc / regtype casts ---------------------------------------

    def _rewrite_regproc_regtype_casts(self, node: exp.Expression) -> exp.Expression:
        """``::regproc`` → ``0`` (pg_proc is empty in this facade).
        ``::regtype`` → ``pg_type.oid`` lookup for known type names, ``0``
        otherwise — so ``WHERE oid = 'int8'::regtype`` matches the int8
        row in pg_type (Codex review)."""
        if not isinstance(node, exp.Cast):
            return node
        target_kind = self._cast_target_kind(node)
        if target_kind == "regproc":
            return exp.Literal.number(0)
        if target_kind == "regtype":
            inner = node.this
            if isinstance(inner, exp.Literal) and inner.is_string:
                return exp.Literal.number(_KNOWN_TYPE_OIDS.get(
                    str(inner.this).lower(), 0,
                ))
            return exp.Literal.number(0)
        return node

    # ----- 5. context function substitution ---------------------------------

    def _substitute_context_functions(self, node: exp.Expression) -> exp.Expression:
        # Try each substitution branch in order; first hit wins.
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
        """Replace ``pg_catalog.<ctx-fn>`` (and ``pg_catalog.<ctx-fn>()``)
        as a whole so the outer ``Dot`` doesn't end up wrapping a string
        literal (``pg_catalog.'jaffle'`` — invalid SQL).

        sqlglot parses ``pg_catalog.current_database()`` as
        ``Dot(Identifier('pg_catalog'), CurrentDatabase(...))``; without
        this branch the inner-node rewrite would leave the Dot intact.
        """
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
        """Replace ``pg_catalog.<bareword-ctx-fn>`` where sqlglot parses the
        whole thing as ``Column(this=<ctx-fn>, table='pg_catalog')`` — the
        no-parens shape (``pg_catalog.current_user``,
        ``pg_catalog.current_catalog``). The Dot-shaped variant
        (``pg_catalog.current_database()``) is handled by
        ``_substitute_qualified_context_call``.
        """
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
        """sqlglot parses ``current_role`` (no parens) as a Column reference.
        Treat single-token unqualified Column refs naming a known niladic ctx
        function as that function. ``node.table`` is ``""`` (not None) for an
        unqualified column."""
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
        # current_schema() / current_schema → 'public' (the single schema
        # the pg facade advertises). DuckDB has its own current_schema
        # (returning 'main'), so we must rewrite explicitly even inside
        # catalog SQL.
        if name in {"current_schema", "currentschema"}:
            return exp.Literal.string("public")
        return None

    # ----- 6. rename stub functions to private names ------------------------

    @staticmethod
    def _rename_stub_functions(node: exp.Expression) -> exp.Expression:
        # Strip qualified function calls of the form
        # ``Dot(this=Paren?(Dot(Identifier(<pg_catalog|information_schema>),
        # expression=Anonymous(<stub_name>(args)))), expression=<field>)``.
        # The full pattern arises from corpus #14's
        # ``(information_schema._pg_expandarray(i.indkey)).n`` — we want to
        # rewrite the inner Anonymous to the private name in place and let
        # the outer Dot continue to extract the field.
        unwrapped = _unwrap_qualified_stub_call(node)
        if unwrapped is not None:
            return unwrapped
        name = _function_name_lower(node)
        if name is None:
            return node
        # Constant-return stubs collapse straight to a literal regardless of
        # arity (sidesteps DuckDB's no-overload-by-arity macro limitation).
        if name in _CONSTANT_STUB_LITERALS:
            return _to_literal(_CONSTANT_STUB_LITERALS[name])
        if name in _LOOKUP_STUB_NAMES:
            private = _LOOKUP_STUB_NAMES[name]
            args = list(node.args.get("expressions") or [])
            # Postgres obj_description has 2-arg form (oid, catname); the
            # macro is single-arg, so drop the second.
            if name == "obj_description" and len(args) > 1:
                args = args[:1]
            return exp.Anonymous(this=private, expressions=args)
        return node

    # ----- 8. <expr> = ANY(<col>) → FALSE -----------------------------------

    @staticmethod
    def _rewrite_pg_any_array(node: exp.Expression) -> exp.Expression:
        """Postgres' ``<x> = ANY(<arr-col>)`` performs array-membership testing.
        DuckDB needs typed array columns to evaluate this; declaring
        ``pg_constraint.conkey`` as ``BIGINT[]`` would work but couples
        the wire-type abstraction. Since the relations that carry array
        columns (``pg_constraint``, ``pg_index``) are always empty, the
        WHERE result is always false anyway — rewrite the comparison to a
        literal ``FALSE`` so the bind step succeeds.
        """
        if not isinstance(node, exp.EQ):
            return node
        rhs = node.expression
        if not isinstance(rhs, exp.Any):
            return node
        # ``ANY(<expr>)`` wraps the inner in a Paren; unwrap before the
        # column check.
        inner = rhs.this
        if isinstance(inner, exp.Paren):
            inner = inner.this
        if isinstance(inner, exp.Column):
            return exp.Boolean(this=False)
        return node

    # ----- 7. regex operator rewrites ---------------------------------------

    @staticmethod
    def _rewrite_regex_operators(node: exp.Expression) -> exp.Expression:
        # Postgres parses these as exp.Binary subclasses. sqlglot maps:
        #   x ~ y   → exp.RegexpLike(this=x, expression=y)
        #   x ~* y  → exp.RegexpILike (or RegexpLike with flag=i)
        #   x !~ y  → exp.Not(this=RegexpLike(...))
        # The cleanest approach: rebuild as Anonymous regexp_matches calls.
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


# --- DuckDB type mapping ----------------------------------------------------


_DUCKDB_TO_DATATYPE: dict[str, DataType] = {
    # ints
    "TINYINT": DataType.INT, "SMALLINT": DataType.INT,
    "INTEGER": DataType.INT, "BIGINT": DataType.INT, "HUGEINT": DataType.INT,
    "UTINYINT": DataType.INT, "USMALLINT": DataType.INT,
    "UINTEGER": DataType.INT, "UBIGINT": DataType.INT,
    "INT1": DataType.INT, "INT2": DataType.INT, "INT4": DataType.INT,
    "INT8": DataType.INT,
    "BIGINT[]": DataType.TEXT,  # arrays fall back to TEXT
    # floats
    "REAL": DataType.DOUBLE, "FLOAT": DataType.DOUBLE,
    "DOUBLE": DataType.DOUBLE, "DECIMAL": DataType.DOUBLE,
    "NUMERIC": DataType.DOUBLE, "FLOAT4": DataType.DOUBLE,
    "FLOAT8": DataType.DOUBLE,
    # bool / date / timestamp / text
    "BOOLEAN": DataType.BOOLEAN, "BOOL": DataType.BOOLEAN,
    "DATE": DataType.DATE,
    "TIMESTAMP": DataType.TIMESTAMP,
    "TIMESTAMP_NS": DataType.TIMESTAMP, "TIMESTAMP_MS": DataType.TIMESTAMP,
    "TIMESTAMP_S": DataType.TIMESTAMP,
    "DATETIME": DataType.TIMESTAMP,
    "VARCHAR": DataType.TEXT, "TEXT": DataType.TEXT, "STRING": DataType.TEXT,
    "CHAR": DataType.TEXT, "BPCHAR": DataType.TEXT,
}


def _duckdb_typename_to_datatype(typename: str) -> DataType:
    """Map a DuckDB column type name to one of the six coarse SLayer
    ``DataType``s. Anything unmapped falls back to ``TEXT`` so wire encoding
    has a safe path."""
    base = str(typename).split("(")[0].split("[")[0].strip().upper()
    return _DUCKDB_TO_DATATYPE.get(base, DataType.TEXT)


# --- DuckDB type mapping for table columns at creation time ----------------


_DATATYPE_TO_DUCKDB_CREATE: dict[DataType, str] = {
    DataType.INT: "BIGINT",
    DataType.DOUBLE: "DOUBLE",
    DataType.TEXT: "VARCHAR",
    DataType.BOOLEAN: "BOOLEAN",
    DataType.DATE: "DATE",
    DataType.TIMESTAMP: "TIMESTAMP",
}


# --- CatalogSqlExecutor -----------------------------------------------------


class CatalogSqlExecutor:
    """Owns one in-memory DuckDB connection with the catalog materialised.

    Construction is expensive (table creates + bulk inserts + macro defs);
    cache via ``executor_for(catalog)``.
    """

    def __init__(
        self,
        *,
        catalog: FacadeCatalog,
        datasource: str,
        extra_relations: Iterable[CatalogRelation] | None = None,
    ) -> None:
        self._datasource = datasource
        self._conn = duckdb.connect(":memory:")
        # DEV-1558 security hardening (Codex round 9): lock down the
        # DuckDB instance so a catalog-shaped query can't pivot through
        # built-ins like ``read_text('/etc/hostname')`` to exfiltrate
        # local files. ``enable_external_access=false`` is DuckDB's
        # one-shot kill switch — it blocks all filesystem / HTTP / S3
        # readers at bind time (BinderException). The setting cannot be
        # re-enabled within the same connection by an authenticated
        # client. ``lock_configuration`` (DuckDB 1.x) further prevents
        # any later SET from re-opening the door if a future code path
        # registers something risky.
        self._conn.execute("SET enable_external_access = false")
        try:
            self._conn.execute("SET lock_configuration = true")
        except duckdb.Error:
            # Older DuckDB versions don't have lock_configuration;
            # enable_external_access alone is still binding.
            pass
        # OID lookup for the regclass UDF: maps both schema-qualified and
        # bare names to OIDs (system catalogs + user tables).
        self._regclass_map: dict[str, int] = dict(KNOWN_SYSTEM_OIDS)
        for ds, tbl in _all_tables(catalog):
            oid = _table_oid(ds, tbl)
            self._regclass_map[f"public.{tbl.name}"] = oid
            self._regclass_map[tbl.name] = oid
        self._rewriter = _AstRewriter(
            datasource=datasource, regclass_map=self._regclass_map,
        )
        relations = build_catalog_relations(
            catalog, datasource, extra_relations=extra_relations,
        )
        for relation in relations:
            self._register_relation(relation)
        self._register_stubs()

    def _register_relation(self, relation: CatalogRelation) -> None:
        cols_ddl = ", ".join(
            f'"{c.name}" {_DATATYPE_TO_DUCKDB_CREATE[c.type]}'
            for c in relation.columns
        )
        self._conn.execute(f'CREATE TABLE "{relation.name}" ({cols_ddl})')
        if not relation.rows:
            return
        # Bulk insert via a single statement with a row-list expansion.
        col_names = [c.name for c in relation.columns]
        placeholders = "(" + ", ".join("?" for _ in col_names) + ")"
        params = [tuple(row.get(name) for name in col_names) for row in relation.rows]
        sql = (
            f'INSERT INTO "{relation.name}" '
            f'({", ".join(f"{chr(34)}{n}{chr(34)}" for n in col_names)}) VALUES {placeholders}'
        )
        self._conn.executemany(sql, params)

    def _register_stubs(self) -> None:
        # Only the data-aware stubs need DuckDB macros — constant-return
        # stubs collapse to literals in the AST rewrite pass.
        macros = [
            "CREATE MACRO _slayer_format_type(p_oid, p_typmod) AS "
            "    COALESCE((SELECT typname FROM pg_type WHERE oid = p_oid LIMIT 1), 'text')",
            "CREATE MACRO _slayer_obj_description(p_oid) AS "
            "    (SELECT description FROM pg_description "
            "     WHERE objoid = p_oid AND objsubid = 0 LIMIT 1)",
            "CREATE MACRO _slayer_col_description(p_oid, p_attnum) AS "
            "    (SELECT description FROM pg_description "
            "     WHERE objoid = p_oid AND objsubid = p_attnum LIMIT 1)",
            # _pg_expandarray returns a struct with x and n fields so
            # `(stub).x` / `(stub).n` field-access parses; combined with
            # empty pg_index, corpus #14 yields zero rows.
            "CREATE MACRO _slayer_pg_expandarray(p_arr) AS "
            "    STRUCT_PACK(x := CAST(NULL AS INTEGER), n := CAST(NULL AS INTEGER))",
        ]
        for sql in macros:
            self._conn.execute(sql)
        # Python UDF for the dynamic-regclass path.
        self._conn.create_function(
            "slayer_regclass_oid", self._regclass_oid,
            ["VARCHAR"], "INTEGER",
        )

    def _regclass_oid(self, text: str | None) -> int:
        if text is None:
            return 0
        # Both schema-qualified and bare lookups go through the same map.
        return self._regclass_map.get(text, self._regclass_map.get(text.lower(), 0))

    def execute(self, *, parsed: exp.Expression, sql: str) -> RowBatch:
        rewritten = self._rewriter.rewrite(parsed.copy())
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
            # CR/Codex review: Postgres allows duplicate output names
            # (``SELECT oid AS x, relname AS x``); the row-as-dict shape
            # would collapse them. Keep the user-visible ``name`` on the
            # FacadeColumn (so the wire RowDescription still reports the
            # duplicate name) but disambiguate the per-row dict key by
            # appending ``__<n>`` to the second and later occurrences.
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
        # Stash the row-key list on the batch so wire emitters can look
        # up values by position-aware key even when names duplicate.
        batch = RowBatch(columns=columns, rows=rows)
        # Pydantic v2 model_extra lets us attach a non-schema attribute;
        # consumers that don't care about duplicates still read via name.
        object.__setattr__(batch, "_row_keys", col_keys)
        return batch


# --- caching ----------------------------------------------------------------


_EXECUTOR_CACHE_LIMIT = 4
_EXECUTOR_CACHE: "collections.OrderedDict[str, CatalogSqlExecutor]" = collections.OrderedDict()


def _fingerprint(catalog: FacadeCatalog, datasource: str) -> str:
    """Stable cache key for an executor.

    Tables are sorted for cross-build determinism (table order doesn't
    affect any catalog row content). Dimensions and metrics WITHIN each
    table are NOT sorted because their position drives ``attnum``,
    ``ordinal_position``, and ``pg_description.objsubid`` — reordering
    columns under the same name set is a real catalog change that the
    fingerprint must distinguish.
    """
    summary = [
        catalog.catalog_name,
        datasource,
        sorted([(
            sch.name, tbl.name, tbl.table_type, tbl.description,
            [
                (d.name, d.data_type.value, d.description, d.label, d.is_time)
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

    ``datasource`` scopes ``current_database()`` / ``current_catalog`` /
    ``current_user`` literal substitution. When omitted, falls back to the
    catalog's first schema name (or the catalog name for empty catalogs).
    The pg facade always passes the real datasource explicitly because its
    catalog schema is the literal ``public``.

    ``extra_relations`` is the per-call extensibility hook. Passing it
    BYPASSES the cache (the cache key would otherwise need to digest the
    relations' row data, which is the expensive part we're trying to skip).
    Embedders that want fast repeat introspection should call this once at
    setup and hold the returned executor.

    Cache (default path) is keyed by a stable SHA-256 of (catalog, datasource).
    FIFO eviction at 4 entries. Single-threaded asyncio + sync execute, so no
    lock is needed.
    """
    if datasource is None:
        datasource = (
            catalog.schemas[0].name if catalog.schemas else catalog.catalog_name
        )
    if extra_relations is not None:
        # Hot path is the default-relations cache; embedders with extras
        # opt out of caching to avoid digesting row payloads in the key.
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
        # FIFO eviction.
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


# Trigger Pydantic to fully construct the forward-ref classes (so the
# translator's imports don't run into a stale reference when this module
# is the entry point).
_ = sqlglot  # quiet linters about the conditional import
