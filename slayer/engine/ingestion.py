"""Auto-ingestion: introspect a database and generate SlayerModels with FK joins."""

import asyncio
import logging
import sys
from collections import defaultdict
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TextIO

import sqlalchemy as sa
import sqlalchemy.dialects.mssql as _sqla_mssql
from pydantic import BaseModel, Field

from slayer.core.enums import DataType, ObjectKind
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    SlayerModel,
    is_key_set_unique,
    sanitize_model_name,
)
from slayer.core.join_edges import edge_reference
from slayer.engine.cardinality import infer_structural_cardinality
from slayer.engine.fk_edges import FkEdgePlan, plan_fk_edges
from slayer.engine.internal_tables import internal_table_rule
from slayer.engine.introspect_utils import (  # noqa: F401  (re-exported for back-compat)
    _CLICKHOUSE_WRAPPER_MAX_DEPTH,
    _CLICKHOUSE_WRAPPER_NAMES,
    _FLOAT_LIKE_INFO_SCHEMA_TYPES,
    _INFO_SCHEMA_TYPE_MAP,
    _clean_comment,
    _get_columns_fallback,
    _parse_info_schema_is_float,
    _resolve_fallback_ref,
    _safe_get_columns,
    is_exact_numeric_db_type,
)
from slayer.sql import engine_factory, sqlite_introspect
from slayer.engine.schema_scope import (
    SchemaRef,
    SkippedSchema,
    default_schema_ref,
    resolve_ingest_scope,
    schema_ref_from_token,
    split_sql_table,
    validate_scope_args,
)
from slayer.core.errors import AmbiguousModelError, EntityResolutionError, StoredDocumentLoadError
from slayer.memories.models import MEMORY_CANONICAL_PREFIX as _MEMORY_PREFIX
from slayer.memories.resolver import (
    canonical_id_rooted_at,
    extract_entities_from_query,
)
from slayer.storage.base import StorageBackend
from slayer.storage.document_loading import DocumentLoadFailures

if TYPE_CHECKING:
    # Runtime import is lazy: keeps the optional search extra off cold start.
    from slayer.search.service import SearchService


logger = logging.getLogger(__name__)


class IntrospectedColumn(BaseModel):
    """One column as read from the live database during introspection."""

    name: str
    type: DataType
    primary_key: bool = False
    is_float: bool = False
    db_type: str | None = None
    comment: str | None = None


# Dedup set for unrecognized SA type warnings, keyed by upper-cased class name.
_logged_unmapped_sa_types: set[str] = set()

# Inspectors whose empty PK means "no PK" (not "ask INFORMATION_SCHEMA").
_PK_AUTHORITATIVE_DIALECTS = frozenset({"sqlite", "bigquery"})

# Types with no default btree/hash opclass (no GROUP BY / DISTINCT) → UNKNOWN.
# A small allow-list on purpose: unmapped-but-comparable types stay TEXT.
_OPAQUE_SA_TYPE_NAMES = frozenset({
    "JSON",  # ``jsonb`` is groupable and deliberately absent
    "XML",
    "TXID_SNAPSHOT",
    # Geometric / spatial — none have a default btree/hash opclass
    "POINT", "LINE", "LSEG", "BOX", "PATH", "POLYGON", "CIRCLE",
    "GEOMETRY", "GEOGRAPHY", "RASTER",
    # Range types
    "INT4RANGE", "INT8RANGE", "NUMRANGE", "TSRANGE", "TSTZRANGE", "DATERANGE",
})

_SA_TYPE_MAP = {
    "INTEGER": DataType.INT,
    "BIGINT": DataType.INT,
    "SMALLINT": DataType.INT,
    "SERIAL": DataType.INT,
    "BIGSERIAL": DataType.INT,
    "FLOAT": DataType.DOUBLE,
    "REAL": DataType.DOUBLE,
    "DOUBLE": DataType.DOUBLE,
    "DOUBLE_PRECISION": DataType.DOUBLE,
    # Refined by scale in _sa_type_to_data_type; DOUBLE when scale is unknown.
    "NUMERIC": DataType.DOUBLE,
    "DECIMAL": DataType.DOUBLE,
    "VARCHAR": DataType.TEXT,
    "CHAR": DataType.TEXT,
    "TEXT": DataType.TEXT,
    "STRING": DataType.TEXT,
    "BOOLEAN": DataType.BOOLEAN,
    "BOOL": DataType.BOOLEAN,
    "BIT": DataType.BOOLEAN,  # T-SQL (SQL Server) boolean type
    "TIMESTAMP": DataType.TIMESTAMP,
    "DATETIME": DataType.TIMESTAMP,
    "TIMESTAMP WITHOUT TIME ZONE": DataType.TIMESTAMP,
    "TIMESTAMP WITH TIME ZONE": DataType.TIMESTAMP,
    "TIMESTAMP_NTZ": DataType.TIMESTAMP,
    "TIMESTAMP_LTZ": DataType.TIMESTAMP,
    "TIMESTAMP_TZ": DataType.TIMESTAMP,
    "DATE": DataType.DATE,
    "TIME": DataType.TIMESTAMP,
    # ClickHouse
    "INT8": DataType.INT,
    "INT16": DataType.INT,
    "INT32": DataType.INT,
    "INT64": DataType.INT,
    "INT128": DataType.INT,
    "INT256": DataType.INT,
    "UINT8": DataType.INT,
    "UINT16": DataType.INT,
    "UINT32": DataType.INT,
    "UINT64": DataType.INT,
    "UINT128": DataType.INT,
    "UINT256": DataType.INT,
    "FLOAT32": DataType.DOUBLE,
    "FLOAT64": DataType.DOUBLE,
    "DATETIME64": DataType.TIMESTAMP,
    "DATE32": DataType.DATE,
    # T-SQL (SQL Server) types; TINYINT also covers MySQL/MariaDB
    "TINYINT": DataType.INT,
    "DATETIME2": DataType.TIMESTAMP,
    "SMALLDATETIME": DataType.TIMESTAMP,
    "DATETIMEOFFSET": DataType.TIMESTAMP,
    "NVARCHAR": DataType.TEXT,
    "NCHAR": DataType.TEXT,
    "NTEXT": DataType.TEXT,
    "UUID": DataType.TEXT,  # ClickHouse (not an sa.Uuid)
    "JSONB": DataType.TEXT,  # comparable, unlike generic JSON
    "MONEY": DataType.DOUBLE,
    "SMALLMONEY": DataType.DOUBLE,
    # SQL Server rowversion — 8-byte binary counter, not temporal
    "ROWVERSION": DataType.TEXT,
}

_NUMERIC_TYPES = {DataType.INT, DataType.DOUBLE}
_ID_SUFFIXES = ("_id", "_key", "_pk", "_fk")

# NUMERIC/DECIMAL are float-like only by scale (see _sa_type_is_float).
_FLOAT_LIKE_SA_TYPES = frozenset(
    {
        "FLOAT",
        "REAL",
        "DOUBLE",
        "DOUBLE_PRECISION",
        "FLOAT32",
        "FLOAT64",
        "MONEY",
        "SMALLMONEY",
    }
)


def _is_id_column(name: str) -> bool:
    """Check if a column name looks like an ID/key rather than a quantity."""
    lower = name.lower()
    return lower == "id" or lower.endswith(_ID_SUFFIXES)


def _unwrap_clickhouse_wrappers(sa_type: sa.types.TypeEngine) -> sa.types.TypeEngine:
    """Peel nested ClickHouse Nullable/LowCardinality wrappers (as-is if no ``nested_type``)."""
    current = sa_type
    for _ in range(_CLICKHOUSE_WRAPPER_MAX_DEPTH):
        if type(current).__name__.upper() not in _CLICKHOUSE_WRAPPER_NAMES:
            return current
        inner = getattr(current, "nested_type", None)
        if inner is None:
            return current
        current = inner
    return current


def _sa_type_to_data_type(sa_type: sa.types.TypeEngine) -> DataType:
    sa_type = _unwrap_clickhouse_wrappers(sa_type)
    # mssql.TIMESTAMP is rowversion (binary), and its name collides with sa.TIMESTAMP.
    if isinstance(sa_type, _sqla_mssql.TIMESTAMP):
        return DataType.TEXT
    # Any dialect's UUID, e.g. SQL Server UNIQUEIDENTIFIER.
    if isinstance(sa_type, sa.Uuid):
        return DataType.TEXT
    type_name = type(sa_type).__name__.upper()
    type_str = str(sa_type).split("(")[0].upper().strip()
    if type_name in _OPAQUE_SA_TYPE_NAMES or type_str in _OPAQUE_SA_TYPE_NAMES:
        return DataType.UNKNOWN
    if is_exact_numeric_db_type(type_name) or is_exact_numeric_db_type(type_str):
        return DataType.DOUBLE if _sa_type_is_float(sa_type) else DataType.INT
    if type_name in _SA_TYPE_MAP:
        return _SA_TYPE_MAP[type_name]
    if type_str in _SA_TYPE_MAP:
        return _SA_TYPE_MAP[type_str]
    if type_name not in _logged_unmapped_sa_types:
        _logged_unmapped_sa_types.add(type_name)
        logger.warning(
            "Unrecognized SQLAlchemy type %r (str=%r); falling back to "
            "DataType.TEXT. Most unmapped types (bytea, arrays) "
            "are comparable and work as TEXT; add genuinely non-comparable "
            "ones to _OPAQUE_SA_TYPE_NAMES and the rest to _SA_TYPE_MAP.",
            type_name,
            str(sa_type),
        )
    return DataType.TEXT


def _raw_db_type_str(sa_type: sa.types.TypeEngine) -> str | None:
    """Best-effort raw database type string for ``Column.db_type`` (never raises)."""
    try:
        text = str(sa_type).strip()
    except Exception:
        text = ""
    if text:
        return text
    name = type(sa_type).__name__
    return name or None


def _sa_type_is_exact_numeric(sa_type: sa.types.TypeEngine) -> bool:
    """Whether ``sa_type`` is an exact NUMERIC/DECIMAL database type."""
    sa_type = _unwrap_clickhouse_wrappers(sa_type)
    return is_exact_numeric_db_type(type(sa_type).__name__) or is_exact_numeric_db_type(
        str(sa_type)
    )


def _sa_type_is_float(sa_type: sa.types.TypeEngine) -> bool:
    """Whether the type is float-like; NUMERIC/DECIMAL only when scale > 0 or unknown."""
    sa_type = _unwrap_clickhouse_wrappers(sa_type)
    type_name = type(sa_type).__name__.upper()
    if type_name in _FLOAT_LIKE_SA_TYPES:
        return True
    if is_exact_numeric_db_type(type_name):
        scale = getattr(sa_type, "scale", None)
        return scale is None or scale > 0
    type_str = str(sa_type).split("(")[0].upper().strip()
    if type_str in _FLOAT_LIKE_SA_TYPES:
        return True
    if is_exact_numeric_db_type(type_str):
        scale = getattr(sa_type, "scale", None)
        return scale is None or scale > 0
    return False


# --- Join generation from FK relationships ---


def _is_cross_schema_fk(
    fk: dict, schema: str | None, default_schema: str | None = None,
) -> bool:
    """Does this FK point outside the ingested schema (it would mis-bind by bare name)?"""
    referred_schema = fk.get("referred_schema")
    if referred_schema is None:  # same-schema FK; always None on SQLite
        return False
    effective_schema = schema if schema is not None else default_schema
    # Unknown ingested schema: skip rather than risk binding to the wrong table.
    if effective_schema is None:
        return True
    return referred_schema != effective_schema


def _get_fk_constraint_groups(
    inspector: sa.engine.Inspector,
    table_name: str,
    schema: str | None,
    table_set: set[str],
    schema_name: str | None = None,
) -> list[tuple[str, list[tuple[str, str]]]]:
    """``[(referred_table, [(src_col, tgt_col), ...])]``, one entry per FK constraint; ``[]`` on error."""
    try:
        fks = inspector.get_foreign_keys(table_name, schema=schema)
    except Exception as exc:  # noqa: BLE001 — FK metadata is optional
        logger.debug("get_foreign_keys failed for %r: %s", table_name, exc)
        return []
    result: list[tuple[str, list[tuple[str, str]]]] = []
    for fk in fks:
        referred_table = fk["referred_table"]
        if referred_table not in table_set or referred_table == table_name:
            continue
        if _is_cross_schema_fk(
            fk=fk,
            schema=schema_name if schema_name is not None else schema,
            default_schema=getattr(inspector, "default_schema_name", None),
        ):
            continue
        pairs = list(zip(fk["constrained_columns"], fk["referred_columns"]))
        if pairs:
            result.append((referred_table, pairs))
    return result


def _safe_introspect(fn) -> list:
    """Run a best-effort introspection call, yielding ``[]`` on failure."""
    try:
        return list(fn())
    except Exception as exc:  # noqa: BLE001 — degrade to "no evidence"
        logger.debug(
            "Constraint/index reflection unavailable (%s); "
            "treating as no uniqueness evidence.", exc,
        )
        return []


def _ref_from_token(sa_engine: sa.Engine | None, token: str | None) -> SchemaRef | None:
    """Rebuild a ``SchemaRef`` from a (possibly catalog-qualified) schema token."""
    if token is None:
        return None
    dialect_name = getattr(getattr(sa_engine, "dialect", None), "name", None)
    return schema_ref_from_token(token, dialect_name=dialect_name)


def _pk_key_sets(
    inspector: sa.engine.Inspector,
    table_name: str,
    schema: str | None,
    sa_engine: sa.Engine | None,
) -> list[list[str]]:
    """The table's primary key as a single key-set (or none)."""
    try:
        if sa_engine is not None:
            pk = _safe_get_pk_constraint(
                inspector=inspector,
                sa_engine=sa_engine,
                table_name=table_name,
                ref=_ref_from_token(sa_engine, schema),
            )
        else:
            pk = inspector.get_pk_constraint(table_name=table_name, schema=schema)
            if not isinstance(pk, dict):
                return []
    except Exception:
        return []
    cols = pk.get("constrained_columns")
    # A bare string would ``list()`` into characters — a bogus key-set.
    if not isinstance(cols, list):
        return []
    return [list(cols)] if cols else []


def _unique_constraint_key_sets(
    inspector: sa.engine.Inspector, table_name: str, schema: str | None,
) -> list[list[str]]:
    """Key-sets from declared UNIQUE constraints."""
    out: list[list[str]] = []
    for uc in _safe_introspect(
        lambda: inspector.get_unique_constraints(table_name, schema=schema)
    ):
        cols = uc.get("column_names") or []
        if cols and all(cols):
            out.append(list(cols))
    return out


def _is_partial_index(idx: dict) -> bool:
    """Does this index carry a filter predicate (a PARTIAL index)?"""
    opts = idx.get("dialect_options") or {}
    for key, value in opts.items():
        if not key.endswith("_where") or value is None:
            continue
        if isinstance(value, str):
            if value.strip():
                return True
            continue
        # Never bool() a non-string: ColumnElement.__bool__ raises.
        return True
    return False


def _unique_index_key_sets(
    inspector: sa.engine.Inspector, table_name: str, schema: str | None,
) -> list[list[str]]:
    """Key-sets from unique indexes that constrain the WHOLE table."""
    out: list[list[str]] = []
    for idx in _safe_introspect(
        lambda: inspector.get_indexes(table_name, schema=schema)
    ):
        if not idx.get("unique") or _is_partial_index(idx):
            continue
        cols = idx.get("column_names") or []
        # Expression members reflect as None; any falsy member rejects the set.
        if cols and all(cols):
            out.append(list(cols))
    return out


def _get_unique_key_sets(
    inspector: sa.engine.Inspector,
    table_name: str,
    schema: str | None,
    sa_engine: sa.Engine | None = None,
) -> list[list[str]]:
    """All PK + UNIQUE key-sets for a table."""
    return (
        _pk_key_sets(
            inspector=inspector, table_name=table_name,
            schema=schema, sa_engine=sa_engine,
        )
        + _unique_constraint_key_sets(
            inspector=inspector, table_name=table_name, schema=schema,
        )
        + _unique_index_key_sets(
            inspector=inspector, table_name=table_name, schema=schema,
        )
    )


def _solo_unique_columns_for_table(
    *,
    inspector: sa.engine.Inspector,
    sa_engine: sa.Engine,
    table_name: str,
    schema: str | None,
) -> set[str]:
    """``_get_single_column_unique_names`` with the table's PK resolved for it."""
    pk = _safe_get_pk_constraint(
        inspector=inspector, sa_engine=sa_engine,
        table_name=table_name, ref=_ref_from_token(sa_engine, schema),
    )
    return _get_single_column_unique_names(
        inspector=inspector, table_name=table_name, schema=schema,
        pk_cols=set(pk.get("constrained_columns", [])),
    )


def _get_single_column_unique_names(
    inspector: sa.engine.Inspector,
    table_name: str,
    schema: str | None,
    *,
    pk_cols: set[str],
) -> set[str]:
    """Non-PK columns that ALONE form a UNIQUE constraint / unique index."""
    key_sets = (
        _unique_constraint_key_sets(
            inspector=inspector, table_name=table_name, schema=schema,
        )
        + _unique_index_key_sets(
            inspector=inspector, table_name=table_name, schema=schema,
        )
    )
    names = {ks[0] for ks in key_sets if len(ks) == 1}
    return names - set(pk_cols)


def _logical_column_name(live_name: str) -> str:
    """A live column's model name: ``_count`` would shadow the COUNT(*) alias."""
    return "count_col" if live_name == "_count" else live_name


def _generate_joins(
    inspector: sa.engine.Inspector,
    source_table: str,
    schema: str | None,
    table_set: set[str],
    sa_engine: sa.Engine | None = None,
    model_name_by_table: dict[str, str] | None = None,
    schema_name: str | None = None,
) -> list[ModelJoin]:
    """Direct ModelJoins from the table's own FKs; one grouped join per composite FK.

    Joins name the MODEL (via ``model_name_by_table``); a target with no model is dropped.
    """
    groups = _get_fk_constraint_groups(
        inspector=inspector,
        table_name=source_table,
        schema=schema,
        table_set=table_set,
        schema_name=schema_name,
    )
    if not groups:
        return []
    source_uniques = _get_unique_key_sets(
        inspector=inspector, table_name=source_table,
        schema=schema, sa_engine=sa_engine,
    )

    joins = []
    seen_signatures: set[tuple] = set()
    for ref_table, pairs in groups:
        # Model names strip `__`, so the live name is not always the model name.
        target_name = (
            ref_table if model_name_by_table is None
            else model_name_by_table.get(ref_table)
        )
        if target_name is None:
            continue  # target collided on sanitization and was never ingested
        signature = (ref_table, tuple(pairs))
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)

        source_cols = [s for s, _ in pairs]
        target_cols = [t for _, t in pairs]
        target_uniques = _get_unique_key_sets(
            inspector=inspector, table_name=ref_table,
            schema=schema, sa_engine=sa_engine,
        )
        cardinality = infer_structural_cardinality(
            source_unique=is_key_set_unique(
                key_columns=source_cols, unique_key_sets=source_uniques
            ),
            target_verified_unique=is_key_set_unique(
                key_columns=target_cols, unique_key_sets=target_uniques
            ),
        )
        joins.append(
            ModelJoin(
                target_model=target_name,
                join_pairs=[[_logical_column_name(s), _logical_column_name(t)]
                            for s, t in pairs],
                cardinality=cardinality,
            )
        )

    return joins


# --- INFORMATION_SCHEMA fallbacks (e.g. DuckDB) ---


def _get_pk_constraint_fallback(
    sa_engine: sa.Engine,
    table_name: str,
    ref: SchemaRef | None,
) -> dict:
    """PK constraint via INFORMATION_SCHEMA when the Inspector fails.

    Joins on ``table_catalog`` too: DuckDB reuses constraint names across attached catalogs.
    """
    ref = _resolve_fallback_ref(sa_engine, ref)
    schema = ref.name if ref else None
    catalog = ref.catalog if ref else None
    join = (
        "  ON tc.constraint_name = kcu.constraint_name "
        "  AND tc.table_schema = kcu.table_schema "
    )
    where = (
        "WHERE tc.table_name = :table_name "
        "  AND tc.constraint_type = 'PRIMARY KEY'"
    )
    params: dict[str, str] = {"table_name": table_name}
    if catalog:
        join += "  AND tc.table_catalog = kcu.table_catalog "
        where += " AND tc.table_catalog = :catalog"
        params["catalog"] = catalog
    if schema:
        where += " AND tc.table_schema = :schema"
        params["schema"] = schema
    sql = (
        "SELECT kcu.column_name "
        "FROM information_schema.table_constraints tc "
        "JOIN information_schema.key_column_usage kcu "
        + join
        + where
    )
    with sa_engine.connect() as conn:
        rows = conn.execute(sa.text(sql), params).fetchall()
    return {"constrained_columns": [row[0] for row in rows]}


def _normalized_pk(result: object) -> dict | None:
    """The inspector's mapping if it carries a list of column names, else None."""
    if not isinstance(result, dict):
        return None
    columns = result.get("constrained_columns")
    if isinstance(columns, list) and all(isinstance(c, str) for c in columns):
        return result
    return None


def _safe_get_pk_constraint(
    inspector: sa.engine.Inspector,
    sa_engine: sa.Engine,
    table_name: str,
    ref: SchemaRef | None,
) -> dict:
    """PK constraint with INFORMATION_SCHEMA fallback; ALWAYS a mapping (no columns on failure)."""
    token = ref.token if ref else None
    if sa_engine.dialect.name in _PK_AUTHORITATIVE_DIALECTS:
        try:
            result = inspector.get_pk_constraint(table_name=table_name, schema=token)
        except Exception:
            return {"constrained_columns": []}
        return _normalized_pk(result) or {"constrained_columns": []}
    try:
        normalized = _normalized_pk(inspector.get_pk_constraint(table_name, schema=token))
        if normalized and normalized["constrained_columns"]:
            return normalized
    except Exception:
        logger.debug("Inspector PK lookup failed for %r", table_name, exc_info=True)
    # DuckDB's inspector returns empty PK — try INFORMATION_SCHEMA.
    try:
        return _get_pk_constraint_fallback(sa_engine, table_name, ref)
    except Exception:
        logger.debug("PK fallback failed for %r", table_name, exc_info=True)
        return {"constrained_columns": []}


def _safe_get_table_comment(
    inspector: sa.engine.Inspector,
    table_name: str,
    schema: str | None,
) -> str | None:
    """Table comment via Inspector; None when unsupported or failing."""
    try:
        return _clean_comment(
            inspector.get_table_comment(table_name, schema=schema).get("text")
        )
    except Exception:
        return None


def _introspected_column(col: dict[str, Any], *, name: str, primary_key: bool) -> IntrospectedColumn:
    """One inspector column; ``db_type`` only where ``DataType`` is lossy (opaque, exact NUMERIC/DECIMAL)."""
    col_type = col["type"]
    db_type: str | None = None
    if isinstance(col_type, DataType):
        data_type = col_type
        is_float = col.get("is_float", False)
        db_type = col.get("db_type")
    else:
        data_type = _sa_type_to_data_type(col_type)
        is_float = _sa_type_is_float(col_type)
        if data_type.is_opaque or _sa_type_is_exact_numeric(col_type):
            db_type = _raw_db_type_str(_unwrap_clickhouse_wrappers(col_type))
    return IntrospectedColumn(
        name=name, type=data_type, primary_key=primary_key, is_float=is_float,
        db_type=db_type, comment=_clean_comment(col.get("comment")),
    )


def _introspect_query_columns_via_inspector(
    sa_engine: sa.Engine,
    inspector: sa.engine.Inspector,
    table_name: str,
    ref: SchemaRef | None,
) -> list[IntrospectedColumn]:
    """Introspect the table's own columns."""
    pk_constraint = _safe_get_pk_constraint(inspector, sa_engine, table_name, ref)
    pk_columns = set(pk_constraint.get("constrained_columns", []))
    return [
        _introspected_column(col, name=col["name"], primary_key=col["name"] in pk_columns)
        for col in _safe_get_columns(inspector, sa_engine, table_name, ref)
    ]


# --- Model generation from introspected columns ---


def _columns_to_model(
    name: str,
    columns: list[IntrospectedColumn],
    data_source: str,
    sql_table: str | None = None,
    joins: list[ModelJoin] | None = None,
    unique_columns: set[str] | None = None,
    source_kind: ObjectKind | None = None,
    hidden: bool = False,
    meta: dict[str, Any] | None = None,
    description: str | None = None,
) -> SlayerModel:
    """Generate a SlayerModel with one Column per introspected column."""
    cols: list[Column] = []
    unique_set = unique_columns or set()

    _INT_FORMAT = NumberFormat(type=NumberFormatType.INTEGER)
    _FLOAT_FORMAT = NumberFormat(type=NumberFormatType.FLOAT)

    for col in columns:
        column_name = _logical_column_name(col.name)

        if col.is_float:
            fmt = _FLOAT_FORMAT
        elif col.type in _NUMERIC_TYPES:
            fmt = _INT_FORMAT
        else:
            fmt = None

        cols.append(
            Column(
                name=column_name,
                sql=col.name,
                type=col.type,
                db_type=col.db_type,
                primary_key=col.primary_key,
                unique=(col.name in unique_set),
                format=fmt,
                description=col.comment,
            )
        )

    return SlayerModel(
        name=name,
        sql_table=sql_table,
        data_source=data_source,
        columns=cols,
        joins=joins or [],
        source_kind=source_kind,
        hidden=hidden,
        meta=meta,
        description=description,
    )


def _sqlite_probe_integer_columns(
    *,
    sa_engine: sa.Engine,
    sql_table: str,
    columns: list[IntrospectedColumn],
) -> list[IntrospectedColumn]:
    """SQLite only: widen declared-INT base columns whose stored values aren't integers.

    A ``None`` probe verdict keeps the declared INT.
    """
    if sa_engine.dialect.name != "sqlite":
        return columns

    schema, table = _parse_qualified_sql_table(sql_table)
    out: list[IntrospectedColumn] = []
    with sa_engine.connect() as conn:
        for col in columns:
            if col.type is not DataType.INT or "." in col.name:
                out.append(col)
                continue
            try:
                verdict = sqlite_introspect.probe_sqlite_integer_column(
                    conn=conn,
                    table=table,
                    column=col.name,
                    schema=schema,
                )
            except Exception as exc:
                logger.warning(
                    "probe call raised for %s.%s; keeping declared INT: %s",
                    sql_table,
                    col.name,
                    exc,
                )
                verdict = None
            if verdict is None or verdict is DataType.INT:
                out.append(col)
                continue
            out.append(col.model_copy(update={
                "type": verdict,
                "is_float": verdict is DataType.DOUBLE,
            }))
    return out


def _parse_qualified_sql_table(sql_table: str) -> tuple[str | None, str]:
    """Split ``"schema.table"`` on the first dot into ``(schema, table)``."""
    if "." in sql_table:
        schema, _, table = sql_table.partition(".")
        return schema or None, table
    return None, sql_table


def introspect_table_to_model(
    *,
    sa_engine: sa.Engine,
    inspector: sa.engine.Inspector,
    table_name: str,
    schema: str | None,
    data_source: str,
    model_name: str | None = None,
    source_kind: ObjectKind | None = None,
) -> SlayerModel:
    """Introspect a single table (no joins) into a SlayerModel.

    ``schema=None`` probes the catalog-qualified default (a bare token lets DuckDB
    sweep an attached twin) while ``sql_table`` stays bare.
    """
    ref = (
        schema_ref_from_token(
            schema, dialect_name=sa_engine.dialect.name, requested=schema
        )
        if schema
        else default_schema_ref(inspector, sa_engine)
    )
    columns = _introspect_query_columns_via_inspector(
        sa_engine=sa_engine,
        inspector=inspector,
        table_name=table_name,
        ref=ref,
    )
    sql_table = ref.qualify(table_name)
    columns = _sqlite_probe_integer_columns(
        sa_engine=sa_engine,
        sql_table=sql_table,
        columns=columns,
    )
    unique_columns = _solo_unique_columns_for_table(
        inspector=inspector, sa_engine=sa_engine,
        table_name=table_name, schema=ref.token,
    )
    return _columns_to_model(
        name=model_name or table_name,
        columns=columns,
        data_source=data_source,
        sql_table=sql_table,
        unique_columns=unique_columns,
        source_kind=source_kind,
        description=_safe_get_table_comment(inspector, table_name, ref.token),
    )


# --- Object discovery ---


class IngestableObject(BaseModel):
    """One database object discovered by :func:`list_ingestable_objects`."""

    name: str
    kind: ObjectKind


class SkippedTable(BaseModel):
    """A live object that could not be turned into a model.

    Distinct from ``IngestionError`` ("this model failed to persist"): separate
    cause, separate fix, reported separately.
    """

    table_name: str
    reason: str
    kind: ObjectKind | None = None


class InternalTable(BaseModel):
    """A live object recognised as ELT/migration bookkeeping.

    Unlike ``SkippedTable`` the model exists and stays queryable; ``hidden``
    only keeps it off the listing surfaces (False only when surfaced). Both
    names are kept because ``__``-sanitization makes them differ (live
    ``_dlt_loads__x`` → model ``_dlt_loads_x``): the report needs the table name
    to locate the object and the model name to un-hide it, and carrying both
    spares consumers re-deriving the live name from ``sql_table`` (lossy for a
    dotted ``--schema``).
    """

    table_name: str
    model_name: str
    tool: str
    kind: ObjectKind | None = None
    hidden: bool = True


class IngestionScanReport(BaseModel):
    """Full result of one introspection pass over a datasource."""

    models: list[SlayerModel] = Field(default_factory=list)
    skipped: list[SkippedTable] = Field(default_factory=list)
    # Every recognised internal that produced a model, regardless of ``surface_internals``.
    internal_tables: list[InternalTable] = Field(default_factory=list)
    # Every object discovered, modelled or not.
    objects: list[IngestableObject] = Field(default_factory=list)
    # BigQuery only: the dataset description, when the datasource has none.
    schema_description: str | None = None
    # Own-catalog schemas out of scope (single-schema, non-``all_schemas`` passes only).
    other_schemas: list[str] = Field(default_factory=list)
    # Requested schemas dropped from scope, with a reason.
    skipped_schemas: list[SkippedSchema] = Field(default_factory=list)

    @property
    def hidden_internals(self) -> list[InternalTable]:
        """The subset this scan hid (the idempotent path uses ``_effective_hidden_internals``)."""
        return [t for t in self.internal_tables if t.hidden]


def _safe_object_names(
    *,
    accessor_name: str,
    inspector: sa.engine.Inspector,
    schema: str | None,
) -> list[str]:
    """Call an ``Inspector.get_*_names`` accessor; ``[]`` if missing, unimplemented or failing."""
    accessor = getattr(inspector, accessor_name, None)
    if accessor is None:
        return []
    try:
        return list(accessor(schema=schema) or [])
    except NotImplementedError:
        logger.debug("%s not implemented for this dialect", accessor_name)
        return []
    except Exception as exc:  # noqa: BLE001 — discovery is best-effort
        logger.debug("%s failed: %s", accessor_name, exc)
        return []


def list_ingestable_objects(
    *,
    inspector: sa.engine.Inspector,
    ref: SchemaRef | None = None,
    schema: str | None = None,
    include_views: bool = True,
) -> list[IngestableObject]:
    """Discover every ingestable object in ``ref``'s schema (default: connection default).

    Deduped across accessors, the most specific kind winning (matview > view > table).
    """
    if ref is None and schema is not None:
        bind = getattr(inspector, "bind", None)
        dialect_name = getattr(getattr(bind, "dialect", None), "name", None)
        ref = schema_ref_from_token(
            schema, dialect_name=dialect_name, requested=schema
        )
    if ref is None:
        ref = default_schema_ref(inspector)
    token = ref.token

    view_list: list[str] = []
    matview_list: list[str] = []
    if include_views:
        view_list = _safe_object_names(
            accessor_name="get_view_names", inspector=inspector, schema=token
        )
        matview_list = _safe_object_names(
            accessor_name="get_materialized_view_names",
            inspector=inspector,
            schema=token,
        )
    view_names, matview_names = set(view_list), set(matview_list)

    def _resolve_kind(name: str, default: ObjectKind) -> ObjectKind:
        if name in matview_names:
            return "materialized_view"
        if name in view_names:
            return "view"
        return default

    objects: list[IngestableObject] = []
    seen: set[str] = set()

    def _add(names: list[str], kind: ObjectKind) -> None:
        for name in names:
            if name in seen:
                continue
            seen.add(name)
            objects.append(
                IngestableObject(name=name, kind=_resolve_kind(name, kind))
            )

    _add(list(inspector.get_table_names(schema=token) or []), "table")
    if include_views:
        _add(view_list, "view")
        _add(matview_list, "materialized_view")
    return objects


def _collision_sort_key(ref: SchemaRef, obj: IngestableObject) -> tuple:
    """Order-independent winner order: unsanitized name, default schema, schema, object name."""
    is_sanitized = sanitize_model_name(obj.name) != obj.name
    return (is_sanitized, not ref.is_default, ref.name or "", obj.name)


def _resolve_scanned_collisions(
    scanned: list[tuple[SchemaRef, IngestableObject]],
    *,
    multi_schema: bool,
) -> tuple[
    dict[str, tuple[SchemaRef, IngestableObject]], list[SkippedTable]
]:
    """Pick one winning ``(ref, object)`` per live name across schemas; losers are skipped."""
    groups: dict[str, list[tuple[SchemaRef, IngestableObject]]] = defaultdict(list)
    for ref, obj in scanned:
        groups[obj.name].append((ref, obj))

    winners: dict[str, tuple[SchemaRef, IngestableObject]] = {}
    skipped: list[SkippedTable] = []
    for model_name, members in groups.items():
        ordered = sorted(members, key=lambda ro: _collision_sort_key(*ro))
        win_ref, win_obj = ordered[0]
        winners[model_name] = (win_ref, win_obj)
        winner_label = (
            win_ref.qualify(win_obj.name) if multi_schema else win_obj.name
        )
        for ref, obj in ordered[1:]:
            label = (
                f"{ref.name}.{obj.name}" if (multi_schema and ref.name) else obj.name
            )
            skipped.append(
                SkippedTable(
                    table_name=label,
                    kind=obj.kind,
                    reason=(
                        f"name collision: resolves to model '{model_name}', "
                        f"already taken by '{winner_label}'"
                    ),
                )
            )
    return winners, skipped


# --- Main ingestion ---


def _build_one_model(
    *,
    sa_engine: sa.Engine,
    inspector: sa.engine.Inspector,
    obj: IngestableObject,
    model_name: str,
    ref: SchemaRef,
    data_source: str,
    table_set: set[str],
    model_name_by_table: dict[str, str] | None = None,
    internal_tool: str | None = None,
) -> SlayerModel:
    """Introspect one live object into a model; raises on failure.

    A set ``internal_tool`` builds it ``hidden`` with a ``meta.internal_table`` breadcrumb.
    """
    schema_token = ref.token
    sql_table = ref.qualify(obj.name)
    model_joins = _generate_joins(
        inspector=inspector,
        source_table=obj.name,
        schema=schema_token,
        table_set=table_set,
        sa_engine=sa_engine,
        model_name_by_table=model_name_by_table,
        schema_name=ref.name,
    )
    columns = _introspect_query_columns_via_inspector(
        sa_engine=sa_engine,
        inspector=inspector,
        table_name=obj.name,
        ref=ref,
    )
    columns = _sqlite_probe_integer_columns(
        sa_engine=sa_engine,
        sql_table=sql_table,
        columns=columns,
    )
    meta = {"internal_table": internal_tool} if internal_tool else None
    return _columns_to_model(
        name=model_name,
        columns=columns,
        data_source=data_source,
        sql_table=sql_table,
        joins=model_joins,
        unique_columns=_solo_unique_columns_for_table(
            inspector=inspector, sa_engine=sa_engine,
            table_name=obj.name, schema=schema_token,
        ),
        source_kind=obj.kind,
        hidden=internal_tool is not None,
        meta=meta,
        description=_safe_get_table_comment(inspector, obj.name, schema_token),
    )


def _fetch_bigquery_dataset_description(
    *,
    sa_engine: sa.Engine,
    datasource: DatasourceConfig,
    schema: str | None,
) -> str | None:
    """BigQuery only: the dataset description, or None."""
    try:
        if getattr(sa_engine.dialect, "name", None) != "bigquery":
            return None
        dataset = (
            schema
            or getattr(sa_engine.dialect, "dataset_id", None)
            or datasource.schema_name
        )
        if not dataset:
            return None
        with sa_engine.connect() as conn:
            # Private handle sqlalchemy-bigquery itself uses; no public accessor.
            client = conn.connection._client
            return _clean_comment(client.get_dataset(dataset).description)
    except Exception:
        return None


def _requested_list(
    schema: str | None, schemas: list[str] | None
) -> list[str] | None:
    """Merge legacy ``schema`` and ``schemas`` into one requested list (None = default)."""
    if schemas is not None:
        return list(schemas)
    if schema is not None:
        return [schema]
    return None


def _scan_one_schema(
    *,
    sa_engine: sa.Engine,
    inspector: sa.engine.Inspector,
    ref: SchemaRef,
    entries: list[tuple[str, IngestableObject]],
    data_source: str,
    surface_internals: bool,
) -> tuple[list[SlayerModel], list[SkippedTable], list[InternalTable]]:
    """Build every winning model for one schema; joins resolve within this schema only."""
    table_set = {obj.name for _, obj in entries}
    name_by_object = {obj.name: mn for mn, obj in entries}

    models: list[SlayerModel] = []
    skipped: list[SkippedTable] = []
    internal_tables: list[InternalTable] = []
    for model_name, obj in entries:
        # Classified on the live name; still recorded under ``surface_internals``.
        tool = internal_table_rule(obj.name)
        try:
            models.append(
                _build_one_model(
                    sa_engine=sa_engine,
                    inspector=inspector,
                    obj=obj,
                    model_name=model_name,
                    ref=ref,
                    data_source=data_source,
                    table_set=table_set,
                    model_name_by_table=name_by_object,
                    internal_tool=None if surface_internals else tool,
                )
            )
        except Exception as exc:  # noqa: BLE001 — per-object isolation
            logger.warning(
                "Skipping %s %r in datasource %r: %s",
                obj.kind, obj.name, data_source, exc,
            )
            skipped.append(
                SkippedTable(table_name=obj.name, kind=obj.kind, reason=str(exc))
            )
            continue
        # Only after success: never in both ``skipped`` and ``internal_tables``.
        if tool is not None:
            internal_tables.append(
                InternalTable(
                    table_name=obj.name,
                    model_name=model_name,
                    tool=tool,
                    kind=obj.kind,
                    hidden=not surface_internals,
                )
            )
    return models, skipped, internal_tables


def _plan_scanned_edges(models: list[SlayerModel]) -> list[SlayerModel]:
    """The scanned models with their FK joins planned as a first ingest (no stored models)."""
    plan = plan_fk_edges(
        models={m.name: m.model_copy(update={"joins": []}) for m in models},
        candidates={m.name: m.joins for m in models},
    )
    return [plan.models[m.name] for m in models]


def ingest_datasource_report(
    datasource: DatasourceConfig,
    include_tables: list[str] | None = None,
    exclude_tables: list[str] | None = None,
    schema: str | None = None,
    schemas: list[str] | None = None,
    all_schemas: bool = False,
    include_views: bool = True,
    surface_internals: bool = False,
) -> IngestionScanReport:
    """Introspect ``datasource``, returning models plus everything skipped.

    Scope: ``all_schemas`` → ``schemas``/``schema`` → ``datasource.schema_name`` → default.
    ELT bookkeeping is modelled ``hidden`` unless ``surface_internals``, even if in ``include_tables``.
    """
    validate_scope_args(schema=schema, schemas=schemas, all_schemas=all_schemas)
    requested = _requested_list(schema, schemas)

    sa_engine = engine_factory.get_engine(datasource.resolve_env_vars())
    try:
        inspector = sa.inspect(sa_engine)
        scope = resolve_ingest_scope(
            inspector=inspector,
            sa_engine=sa_engine,
            requested=requested,
            all_schemas=all_schemas,
            datasource_schema=datasource.schema_name,
        )
        multi_schema = len(scope.schemas) > 1

        scanned: list[tuple[SchemaRef, IngestableObject]] = []
        for ref in scope.schemas:
            objs = list_ingestable_objects(
                inspector=inspector, ref=ref, include_views=include_views
            )
            if include_tables:
                objs = [o for o in objs if o.name in include_tables]
            if exclude_tables:
                objs = [o for o in objs if o.name not in exclude_tables]
            for o in objs:
                scanned.append((ref, o))

        all_objects = [obj for _, obj in scanned]
        winners, skipped = _resolve_scanned_collisions(
            scanned, multi_schema=multi_schema
        )

        by_schema: dict[str | None, tuple[SchemaRef, list[tuple[str, IngestableObject]]]] = {}
        for model_name, (ref, obj) in winners.items():
            key = ref.token
            if key not in by_schema:
                by_schema[key] = (ref, [])
            by_schema[key][1].append((model_name, obj))

        models: list[SlayerModel] = []
        internal_tables: list[InternalTable] = []
        for ref, entries in by_schema.values():
            m, s, i = _scan_one_schema(
                sa_engine=sa_engine,
                inspector=inspector,
                ref=ref,
                entries=entries,
                data_source=datasource.name,
                surface_internals=surface_internals,
            )
            models.extend(m)
            skipped.extend(s)
            internal_tables.extend(i)

        schema_description = None
        if not datasource.description:
            first_schema = scope.schemas[0].name if scope.schemas else None
            schema_description = _fetch_bigquery_dataset_description(
                sa_engine=sa_engine, datasource=datasource, schema=first_schema,
            )

        return IngestionScanReport(
            models=_plan_scanned_edges(models),
            skipped=skipped,
            objects=all_objects,
            internal_tables=internal_tables,
            schema_description=schema_description,
            other_schemas=scope.other_schemas,
            skipped_schemas=scope.skipped,
        )
    finally:
        # Releases DuckDB file handles; the factory keeps owning the engine.
        engine_factory.release_idle(sa_engine)


def ingest_datasource(
    datasource: DatasourceConfig,
    include_tables: list[str] | None = None,
    exclude_tables: list[str] | None = None,
    schema: str | None = None,
    schemas: list[str] | None = None,
    all_schemas: bool = False,
    include_views: bool = True,
    surface_internals: bool = False,
) -> list[SlayerModel]:
    """Models only, for callers that don't need the skip report."""
    return ingest_datasource_report(
        datasource=datasource,
        include_tables=include_tables,
        exclude_tables=exclude_tables,
        schema=schema,
        schemas=schemas,
        all_schemas=all_schemas,
        include_views=include_views,
        surface_internals=surface_internals,
    ).models


# --- Idempotent re-ingestion ---


def _is_auto_default_integer_format(fmt: NumberFormat | None) -> bool:
    """Whether ``fmt`` is the auto-ingested plain INTEGER format (no precision/symbol)."""
    if fmt is None:
        return False
    if fmt.type != NumberFormatType.INTEGER:
        return False
    return fmt.precision is None and fmt.symbol is None


def _format_for_widened_type(verdict: DataType) -> NumberFormat | None:
    """Return the auto-default format for a probed widening verdict."""
    if verdict is DataType.DOUBLE:
        return NumberFormat(type=NumberFormatType.FLOAT)
    return None  # TEXT clears format


def _merge_persisted_column_with_probe(
    *,
    persisted_col: Column,
    fresh_col: Column | None,
    model_name: str,
    sqlite_widen_enabled: bool,
) -> tuple[Column, bool]:
    """``(merged_column, did_widen)``: widen a persisted INT to a probed DOUBLE/TEXT (SQLite only)."""
    if not (
        sqlite_widen_enabled
        and fresh_col is not None
        and persisted_col.type is DataType.INT
        and fresh_col.type in (DataType.DOUBLE, DataType.TEXT)
    ):
        return persisted_col, False

    updates: dict[str, Any] = {"type": fresh_col.type}
    if _is_auto_default_integer_format(persisted_col.format):
        updates["format"] = _format_for_widened_type(fresh_col.type)
    else:
        logger.info(
            "Custom format on %s.%s preserved on SQLite probe widening "
            "(persisted INT -> %s). Review whether the format still applies.",
            model_name,
            persisted_col.name,
            fresh_col.type.value,
        )
    return persisted_col.model_copy(update=updates), True


def _join_sig(j: ModelJoin) -> tuple:
    return (j.target_model, tuple(sorted((p[0], p[1]) for p in j.join_pairs)))


def _repair_legacy_join_targets(
    persisted: SlayerModel, fresh: SlayerModel,
) -> tuple[SlayerModel, bool]:
    """Rewrite legacy join targets that name the live object instead of the model.

    Requires a full signature match against a fresh join, so hand-authored joins never move.
    """
    fresh_sigs = {_join_sig(j) for j in fresh.joins}
    claimed = {
        j.target_model for j in persisted.joins
        if sanitize_model_name(j.target_model) == j.target_model
    }
    repaired = False
    joins: list[ModelJoin] = []
    for j in persisted.joins:
        candidate = sanitize_model_name(j.target_model)
        renamed = j.model_copy(update={"target_model": candidate})
        if (
            candidate != j.target_model
            and candidate not in claimed
            and _join_sig(renamed) in fresh_sigs
        ):
            joins.append(renamed)
            claimed.add(candidate)
            repaired = True
        else:
            joins.append(j)
    if not repaired:
        return persisted, False
    return persisted.model_copy(update={"joins": joins}), True


def _merge_stored_joins(
    persisted: SlayerModel, fresh: SlayerModel,
) -> tuple[list[ModelJoin], bool]:
    """Stored joins with legacy targets repaired and unset ``cardinality`` filled from the matching fresh join."""
    persisted, changed = _repair_legacy_join_targets(persisted=persisted, fresh=fresh)
    fresh_by_sig = {_join_sig(j): j for j in fresh.joins}
    joins: list[ModelJoin] = []
    for pj in persisted.joins:
        fj = fresh_by_sig.get(_join_sig(pj))
        if pj.cardinality is None and fj is not None and fj.cardinality is not None:
            joins.append(pj.model_copy(update={"cardinality": fj.cardinality}))
            changed = True
        else:
            joins.append(pj)
    return joins, changed


class AdditiveMergeResult(BaseModel):
    """Outcome of :func:`_additive_merge_existing`."""

    merged: SlayerModel
    new_columns: list[str] = Field(default_factory=list)
    widened_columns: list[str] = Field(default_factory=list)
    kind_changed: bool = False
    #: A metadata-only fill (join cardinality / column unique) that still
    #: has to be saved even when no column or join was added.
    metadata_changed: bool = False
    #: Existing columns whose empty description was filled from a
    #: DB comment, and whether the model-level description was filled.
    described_columns: list[str] = Field(default_factory=list)
    model_described: bool = False

    @property
    def has_changes(self) -> bool:
        return bool(
            self.new_columns or self.widened_columns or self.kind_changed or self.metadata_changed
            or self.described_columns or self.model_described
        )


def _merge_one_column(
    *,
    persisted_col: Column,
    fresh_col: Column | None,
    model_name: str,
    sqlite_widen_enabled: bool,
) -> tuple[Column, bool, bool, bool]:
    """Merge one persisted column against its fresh one → ``(merged, did_widen, unique_filled, described)``."""
    merged_col, did_widen = _merge_persisted_column_with_probe(
        persisted_col=persisted_col,
        fresh_col=fresh_col,
        model_name=model_name,
        sqlite_widen_enabled=sqlite_widen_enabled,
    )
    unique_filled = False
    described = False
    if fresh_col is not None:
        if fresh_col.unique and not merged_col.unique:
            merged_col = merged_col.model_copy(update={"unique": True})
            unique_filled = True
        if fresh_col.description and not merged_col.description:
            merged_col = merged_col.model_copy(
                update={"description": fresh_col.description}
            )
            described = True
    return merged_col, did_widen, unique_filled, described


def _additive_merge_existing(
    *,
    persisted: SlayerModel,
    fresh: SlayerModel,
    sqlite_widen_enabled: bool = False,
) -> AdditiveMergeResult:
    """Merge ``fresh`` into ``persisted`` additively: existing entries are never overwritten.

    Exceptions: SQLite INT→DOUBLE/TEXT widening, empty-field gap-fills, and a
    non-None ``source_kind`` refresh.
    """
    fresh_by_name: dict[str, Column] = {c.name: c for c in fresh.columns}
    persisted_names = {c.name for c in persisted.columns}
    # A live column is the persisted column it is the physical spelling of, else the one named like it.
    live_name = {
        c.name: c.physical_name if c.is_base and c.physical_name not in persisted_names - {c.name} else c.name
        for c in persisted.columns
    }

    widened_column_names: list[str] = []
    described_column_names: list[str] = []
    merged_columns: list[Column] = []
    metadata_changed = False
    for persisted_col in persisted.columns:
        merged_col, did_widen, unique_filled, described = _merge_one_column(
            persisted_col=persisted_col,
            fresh_col=fresh_by_name.get(live_name[persisted_col.name]),
            model_name=persisted.name,
            sqlite_widen_enabled=sqlite_widen_enabled,
        )
        metadata_changed = metadata_changed or unique_filled
        if described:
            described_column_names.append(persisted_col.name)
        merged_columns.append(merged_col)
        if did_widen:
            widened_column_names.append(persisted_col.name)

    new_cols = [c for c in fresh.columns if c.name not in persisted_names | set(live_name.values())]
    merged_columns.extend(new_cols)
    new_column_names = [c.name for c in new_cols]

    merged_joins, joins_metadata_changed = _merge_stored_joins(
        persisted=persisted, fresh=fresh
    )
    metadata_changed = metadata_changed or joins_metadata_changed

    kind_changed = (
        fresh.source_kind is not None
        and fresh.source_kind != persisted.source_kind
    )

    model_described = bool(fresh.description) and not persisted.description

    if not (
        new_column_names
        or widened_column_names
        or kind_changed
        or metadata_changed
        or described_column_names
        or model_described
    ):
        return AdditiveMergeResult(merged=persisted)

    update: dict[str, Any] = {"columns": merged_columns, "joins": merged_joins}
    if kind_changed:
        update["source_kind"] = fresh.source_kind
    if model_described:
        update["description"] = fresh.description

    return AdditiveMergeResult(
        merged=persisted.model_copy(update=update),
        new_columns=new_column_names,
        widened_columns=widened_column_names,
        kind_changed=kind_changed,
        metadata_changed=metadata_changed,
        described_columns=described_column_names,
        model_described=model_described,
    )


class _TableMerge(BaseModel):
    """One fresh table merged into its stored model, before FK edges are planned."""

    table_name: str
    fresh: SlayerModel
    base: SlayerModel  # stored joins only; the edge plan adds the fresh FKs
    created: bool = False
    merge: AdditiveMergeResult | None = None
    sql_table_change: str | None = None
    kind_change: str | None = None

    @property
    def changed(self) -> bool:
        return self.created or self.sql_table_change is not None or (self.merge is not None and self.merge.has_changes)


def _qualifier_repair_allowed(
    *,
    bare_name: str,
    fresh_schema: str | None,
    default_schema_name: str | None,
    default_objects: set[str] | None,
) -> bool:
    """May a persisted BARE model be healed to a fresh QUALIFIED ``sql_table``?

    Always for a default-schema twin; for another schema only if the bare name
    isn't a default-schema table. Unknown membership fails closed.
    """
    if fresh_schema is not None and fresh_schema == default_schema_name:
        return True
    if default_objects is None:
        return False
    return bare_name not in default_objects


def _merge_one_table(
    *,
    table_name: str,
    fresh: SlayerModel,
    persisted: SlayerModel | None,
    datasource: DatasourceConfig,
    default_schema_name: str | None = None,
    default_objects: set[str] | None = None,
) -> _TableMerge | SkippedTable | None:
    """Merge one fresh model into its stored twin (``None``: a sql/query-backed model, left alone).

    A BARE persisted ``sql_table`` is healed to the qualified one when safe; a
    qualified one is never rewritten.
    """
    if persisted is None:
        return _TableMerge(
            table_name=table_name, fresh=fresh, base=fresh.model_copy(update={"joins": []}), created=True,
        )
    if persisted.sql or persisted.source_queries:
        return None

    sql_table_change: str | None = None
    if (
        persisted.sql_table
        and fresh.sql_table
        and persisted.sql_table != fresh.sql_table
    ):
        persisted_schema, persisted_obj = split_sql_table(persisted.sql_table)
        fresh_schema, _ = split_sql_table(fresh.sql_table)
        if persisted_schema is not None:
            # A differently qualified twin is a different physical table.
            return SkippedTable(
                table_name=persisted.sql_table,
                reason=(
                    f"kept qualified {persisted.sql_table!r}: the fresh "
                    f"{fresh.sql_table!r} is a different physical table"
                ),
            )
        if not _qualifier_repair_allowed(
            bare_name=persisted_obj,
            fresh_schema=fresh_schema,
            default_schema_name=default_schema_name,
            default_objects=default_objects,
        ):
            return SkippedTable(
                table_name=persisted.sql_table,
                reason=(
                    f"kept unqualified {persisted.sql_table!r}: the fresh "
                    f"{fresh.sql_table!r} is a different physical table"
                ),
            )
        sql_table_change = f"{persisted.sql_table} → {fresh.sql_table}"
        persisted = persisted.model_copy(update={"sql_table": fresh.sql_table})

    outcome = _additive_merge_existing(
        persisted=persisted,
        fresh=fresh,
        sqlite_widen_enabled=(datasource.type or "").lower() == "sqlite",
    )
    kind_change = None
    if outcome.kind_changed:
        kind_change = f"{persisted.source_kind or 'unknown'} → {fresh.source_kind}"
    return _TableMerge(
        table_name=table_name, fresh=fresh, base=outcome.merged, merge=outcome,
        sql_table_change=sql_table_change, kind_change=kind_change,
    )


def _bare_table_name(sql_table: str) -> str:
    """Strip an optional schema prefix from a ``schema.table`` reference."""
    return sql_table.split(".", 1)[1] if "." in sql_table else sql_table


def _sql_table_identity(
    sql_table: str | None, *, default_schema: str | None,
) -> tuple[str | None, str] | None:
    """``(schema, object)`` identity of a ``sql_table``, unqualified → ``default_schema``."""
    if not sql_table:
        return None
    schema, obj = split_sql_table(sql_table)
    return (schema if schema is not None else default_schema, obj)


async def _stored_sanitized_identity_map(
    *,
    storage: StorageBackend,
    datasource: DatasourceConfig,
    default_schema: str | None,
    failures: DocumentLoadFailures,
) -> tuple[set[str], dict[str, tuple[str | None, str] | None]]:
    """Stored model names for ``datasource`` plus each loadable one's live-object identity."""
    identities = await storage._list_all_model_identities()
    stored_names = {n for d, n in identities if d == datasource.name}
    stored_identity: dict[str, tuple[str | None, str] | None] = {
        m.name: _sql_table_identity(m.sql_table, default_schema=default_schema)
        for m in failures.skip(await storage.load_models(data_source=datasource.name))
    }
    return stored_names, stored_identity


def _sanitized_rename_map(
    *,
    fresh_by_name: dict[str, "SlayerModel"],
    stored_names: set[str],
    stored_identity: dict[str, tuple[str | None, str] | None],
    default_schema: str | None,
) -> dict[str, str]:
    """Map each fresh name onto a stored sanitized spelling of the SAME live object."""
    rename_map: dict[str, str] = {}
    for name, fresh in fresh_by_name.items():
        if name in stored_names:
            continue  # exact stored model — additive pass matches it directly
        sanitized = sanitize_model_name(name)
        if sanitized == name or sanitized not in stored_names:
            continue
        if sanitized in fresh_by_name:
            continue  # sanitized spelling is a distinct live object — don't collapse
        fresh_identity = _sql_table_identity(
            fresh.sql_table, default_schema=default_schema,
        )
        if fresh_identity is not None and stored_identity.get(sanitized) == fresh_identity:
            rename_map[name] = sanitized
    return rename_map


async def _adopt_stored_sanitized_names(
    *,
    fresh_by_name: dict[str, "SlayerModel"],
    storage: StorageBackend,
    datasource: DatasourceConfig,
    default_schema: str | None,
    failures: DocumentLoadFailures,
) -> tuple[dict[str, "SlayerModel"], dict[str, str]]:
    """Rename fresh ``a__b`` models onto a stored sanitized ``a_b`` of the same object.

    Renames cascade into join targets; returns ``(adopted_models, rename_map)``.
    """
    stored_names, stored_identity = await _stored_sanitized_identity_map(
        storage=storage, datasource=datasource, default_schema=default_schema, failures=failures,
    )
    rename_map = _sanitized_rename_map(
        fresh_by_name=fresh_by_name, stored_names=stored_names,
        stored_identity=stored_identity, default_schema=default_schema,
    )
    if not rename_map:
        return fresh_by_name, {}

    adopted: dict[str, "SlayerModel"] = {}
    for name, fresh in fresh_by_name.items():
        new_name = rename_map.get(name, name)
        joins = [
            j.model_copy(update={"target_model": rename_map[j.target_model]})
            if j.target_model in rename_map else j
            for j in fresh.joins
        ]
        updates: dict = {}
        if new_name != name:
            updates["name"] = new_name
        if joins != fresh.joins:
            updates["joins"] = joins
        adopted[new_name] = fresh.model_copy(update=updates) if updates else fresh
    return adopted, rename_map


async def _scoped_models_for_validation(
    *,
    storage: StorageBackend,
    datasource: DatasourceConfig,
    in_scope_table_names: set[str],
    failures: DocumentLoadFailures,
) -> list[SlayerModel]:
    """Persisted models to validate: in-scope ``sql_table`` models plus all sql/query-backed ones."""
    return [
        m for m in failures.skip(await storage.load_models(data_source=datasource.name))
        if not m.sql_table or _bare_table_name(m.sql_table) in in_scope_table_names
    ]


async def _effective_hidden_internals(
    *,
    candidates: list[InternalTable],
    datasource: DatasourceConfig,
    storage: StorageBackend,
    failures: DocumentLoadFailures,
) -> list[InternalTable]:
    """Narrow scan-time internal classifications to models actually hidden in storage."""
    effective: list[InternalTable] = []
    for entry in candidates:
        try:
            persisted = await storage.get_model(
                entry.model_name, data_source=datasource.name
            )
        except StoredDocumentLoadError as exc:
            failures.record(exc)
            continue
        except Exception as exc:  # noqa: BLE001 — reporting must not fail ingest
            logger.debug(
                "hidden-internal re-check failed for %r: %s", entry.model_name, exc
            )
            continue
        if persisted is not None and persisted.hidden:
            effective.append(entry)
    return effective


def _default_schema_membership(
    datasource: DatasourceConfig,
) -> tuple[str | None, set[str] | None]:
    """The default schema's name and its object names (``None`` if listing failed)."""
    sa_engine = engine_factory.get_engine(datasource.resolve_env_vars())
    try:
        inspector = sa.inspect(sa_engine)
        ref = default_schema_ref(inspector, sa_engine)
        try:
            objs = list_ingestable_objects(
                inspector=inspector, ref=ref, include_views=True
            )
            names: set[str] | None = {o.name for o in objs}
        except Exception:  # noqa: BLE001 — unknown membership → fail closed
            names = None
        return ref.name, names
    finally:
        engine_factory.release_idle(sa_engine)


def _schema_hint_message(other_schemas: list[str]) -> str | None:
    """Hint offering schemas discovered but not ingested this pass."""
    if not other_schemas:
        return None
    return (
        "Other schemas are available and were not ingested: "
        + ", ".join(other_schemas)
        + ". Re-run with --schema <name> or --all-schemas to include them."
    )


def _edge_name_collision(*, table_name: str, fresh: SlayerModel, edge_owner: dict[str, str]) -> SkippedTable:
    return SkippedTable(
        table_name=_bare_table_name(fresh.sql_table) if fresh.sql_table else table_name,
        kind=fresh.source_kind,
        reason=(
            f"model name '{table_name}' collides with the edge '{table_name}' declared on "
            f"model '{edge_owner[table_name]}'"
        ),
    )


def _merge_fresh_tables(
    *,
    fresh_by_name: dict[str, SlayerModel],
    stored: dict[str, SlayerModel],
    unloadable: set[str],
    datasource: DatasourceConfig,
    default_schema_name: str | None,
    default_objects: set[str] | None,
) -> tuple[dict[str, _TableMerge], list[SkippedTable]]:
    """Merge every fresh model into storage's view; a table named like a stored edge is skipped."""
    edge_owner = {j.name: m.name for m in stored.values() for j in m.joins if j.name}
    merges: dict[str, _TableMerge] = {}
    skipped: list[SkippedTable] = []
    for table_name, fresh in fresh_by_name.items():
        if table_name in unloadable:
            continue
        if table_name not in stored and table_name in edge_owner:
            skipped.append(_edge_name_collision(table_name=table_name, fresh=fresh, edge_owner=edge_owner))
            continue
        outcome = _merge_one_table(
            table_name=table_name, fresh=fresh, persisted=stored.get(table_name), datasource=datasource,
            default_schema_name=default_schema_name, default_objects=default_objects,
        )
        if isinstance(outcome, SkippedTable):
            skipped.append(outcome)
        elif outcome is not None:
            merges[table_name] = outcome
    return merges, skipped


def _addition_fields(*, merge: _TableMerge, final: SlayerModel, plan: FkEdgePlan) -> dict[str, Any]:
    """``ModelAddition`` fields for one merged table."""
    added = final.joins if merge.created else plan.added.get(final.name, [])
    fields: dict[str, Any] = {
        "model_name": merge.table_name,
        "new_joins": [edge_reference(model=final, join=j) for j in added],
        "named_joins": plan.named.get(final.name, []),
        "source_kind": final.source_kind,
    }
    if merge.created:
        return fields | {
            "created": True,
            "new_columns": [c.name for c in final.columns],
            "described_columns": [c.name for c in final.columns if c.description],
            "model_described": bool(final.description),
        }
    assert merge.merge is not None
    return fields | {
        "new_columns": merge.merge.new_columns,
        "widened_columns": merge.merge.widened_columns,
        "kind_change": merge.kind_change,
        "described_columns": merge.merge.described_columns,
        "model_described": merge.merge.model_described,
        "sql_table_change": merge.sql_table_change,
    }


async def _run_additive_pass(
    *,
    fresh_by_name: dict[str, SlayerModel],
    datasource: DatasourceConfig,
    storage: StorageBackend,
    default_schema_name: str | None,
    default_objects: set[str] | None,
    failures: DocumentLoadFailures,
):
    """Save / merge every fresh model, FK edges planned datasource-wide → ``(additions, errors, merge_skipped)``.

    Every name is computed before the first save, and name-only updates save first.
    """
    from slayer.engine.schema_drift import IngestionError, ModelAddition  # ALLOW(import-not-top): circular — schema_drift imports ingestion

    loaded, unloaded = await storage.load_models(data_source=datasource.name)
    stored = {m.name: m for m in failures.skip((loaded, unloaded))}
    merges, merge_skipped = _merge_fresh_tables(
        fresh_by_name=fresh_by_name, stored=stored, unloadable={e.name for e in unloaded},
        datasource=datasource, default_schema_name=default_schema_name, default_objects=default_objects,
    )
    plan = plan_fk_edges(
        models={**stored, **{n: m.base for n, m in merges.items()}},
        candidates={n: m.fresh.joins for n, m in merges.items()},
    )

    name_only = [n for n, v in plan.named.items() if not plan.added.get(n) and not (n in merges and merges[n].changed)]
    rest = [n for n in merges if n not in name_only and (merges[n].changed or plan.added.get(n) or plan.named.get(n))]
    errors: list[IngestionError] = []
    for name in [*name_only, *rest]:
        try:
            await storage.save_model(plan.models[name], failures=failures)
        except Exception as exc:  # noqa: BLE001 — best-effort per-model isolation
            errors.append(IngestionError(model_name=name, data_source=datasource.name, error=str(exc)))
    failed = {e.model_name for e in errors}

    additions = [
        ModelAddition(data_source=datasource.name, **_addition_fields(merge=m, final=plan.models[n], plan=plan))
        for n, m in merges.items() if n not in failed
    ]
    additions.extend(
        ModelAddition(model_name=n, data_source=datasource.name, named_joins=plan.named[n])
        for n in name_only if n not in merges and n not in failed
    )
    return additions, errors, merge_skipped


async def ingest_datasource_idempotent(
    *,
    datasource: DatasourceConfig,
    storage: StorageBackend,
    include_tables: list[str] | None = None,
    exclude_tables: list[str] | None = None,
    schema: str | None = None,
    schemas: list[str] | None = None,
    all_schemas: bool = False,
    include_views: bool = True,
    surface_internals: bool = False,
):
    """Idempotent re-ingestion: create missing models, additively merge existing ones.

    User-authored sql/query-backed models are skipped. Then validates the same
    scope so drift and dropped tables show up in ``to_delete``.
    """
    validate_scope_args(schema=schema, schemas=schemas, all_schemas=all_schemas)

    from slayer.engine.schema_drift import (  # ALLOW(import-not-top): circular — schema_drift imports ingestion
        IdempotentIngestResult,
        IngestionError,
        validate_datasource,
    )

    scan = await asyncio.to_thread(
        ingest_datasource_report,
        datasource=datasource,
        include_tables=include_tables,
        exclude_tables=exclude_tables,
        schema=schema,
        schemas=schemas,
        all_schemas=all_schemas,
        include_views=include_views,
        surface_internals=surface_internals,
    )
    default_schema_name, default_objects = await asyncio.to_thread(
        _default_schema_membership, datasource
    )
    failures = DocumentLoadFailures()
    fresh_models = scan.models
    fresh_by_name = {m.name: m for m in fresh_models}
    fresh_by_name, adopted_renames = await _adopt_stored_sanitized_names(
        fresh_by_name=fresh_by_name,
        storage=storage,
        datasource=datasource,
        default_schema=default_schema_name,
        failures=failures,
    )
    fresh_models = list(fresh_by_name.values())
    # Re-point internal-table entries at adopted names, else the re-check drops them.
    if adopted_renames:
        scan.internal_tables = [
            e.model_copy(update={"model_name": adopted_renames[e.model_name]})
            if e.model_name in adopted_renames else e
            for e in scan.internal_tables
        ]
    # Live object names, not model names (the two can differ).
    in_scope_table_names: set[str] = {
        _bare_table_name(m.sql_table) for m in fresh_models if m.sql_table
    }

    additions, errors, merge_skipped = await _run_additive_pass(
        fresh_by_name=fresh_by_name,
        datasource=datasource,
        storage=storage,
        default_schema_name=default_schema_name,
        default_objects=default_objects,
        failures=failures,
    )

    # Fill-if-empty, checked against the freshly-loaded STORED config, not the caller's copy.
    datasource_described = False
    if scan.schema_description and not datasource.description:
        try:
            stored = await storage.get_datasource(datasource.name) or datasource
            if not stored.description:
                await storage.save_datasource(
                    stored.model_copy(update={"description": scan.schema_description})
                )
                datasource_described = True
        except Exception as exc:  # noqa: BLE001 — best-effort isolation
            errors.append(IngestionError(
                model_name="",
                data_source=datasource.name,
                error=f"datasource description save: {exc}",
            ))

    scoped_models = await _scoped_models_for_validation(
        storage=storage,
        datasource=datasource,
        in_scope_table_names=in_scope_table_names,
        failures=failures,
    )
    to_delete = await validate_datasource(
        datasource=datasource, models=scoped_models
    )

    # No sample profiling here (a full scan per column); read paths refresh on cache miss.

    embedding_errors = await _refresh_datasource_embeddings(
        datasource_name=datasource.name, storage=storage, failures=failures,
    )
    for model_name, err in embedding_errors:
        errors.append(IngestionError(
            model_name=model_name,
            data_source=datasource.name,
            error=f"embedding refresh: {err}",
        ))

    # Effective state, not the scan's verdict — see ``_effective_hidden_internals``.
    hidden_internals = await _effective_hidden_internals(
        candidates=scan.internal_tables,
        datasource=datasource,
        storage=storage,
        failures=failures,
    )
    errors.extend(
        IngestionError(model_name=e.name, data_source=e.data_source, error=str(e))
        for e in failures.errors if e.data_source is not None
    )

    return IdempotentIngestResult(
        additions=additions,
        to_delete=list(to_delete),
        errors=errors,
        skipped=list(scan.skipped) + merge_skipped,
        objects=scan.objects,
        hidden_internals=hidden_internals,
        datasource_described=datasource_described,
        schema_hint=_schema_hint_message(scan.other_schemas),
        skipped_schemas=list(scan.skipped_schemas),
    )


# --- Friendly-error helper (shared with the MCP server) ---


def _friendly_db_error(exc: Exception) -> str:
    """Convert a database exception into a user-friendly message with hints."""
    msg = str(exc)
    if hasattr(exc, "orig") and exc.orig:
        msg = str(exc.orig)

    hints = []
    msg_lower = msg.lower()
    if "no password supplied" in msg_lower or "password authentication failed" in msg_lower:
        hints.append("Check that username and password are correct.")
    elif "does not exist" in msg_lower and "database" in msg_lower:
        hints.append("Verify the database name is correct.")
    elif "could not translate host" in msg_lower or "name or service not known" in msg_lower:
        hints.append("Check that the host address is correct.")
    elif "connection refused" in msg_lower:
        hints.append("Check that the database server is running and the port is correct.")
    elif "timeout" in msg_lower:
        hints.append("The database server is not responding. Check host/port and network access.")

    result = f"Database error: {msg}"
    if hints:
        result += "\nHint: " + " ".join(hints)
    return result


# --- Renderers (shared by `slayer ingest` and the boot-time orchestrator) ---


def _get_schemas(ds: DatasourceConfig) -> list[str]:
    """List a datasource's schemas. Best-effort; empty means no hint."""
    try:
        engine = engine_factory.get_engine(ds.resolve_env_vars())
        inspector = sa.inspect(engine)
        return inspector.get_schema_names()
    except Exception:  # noqa: BLE001 — hint-only and never fatal
        return []


def _empty_ingest_message(
    *,
    schema_name: str,
    ds: DatasourceConfig,
    retry_hint: str | None = None,
) -> str:
    """Explain an empty ingest and point at the likely fix."""
    schema_label = f" in schema '{schema_name}'" if schema_name else ""
    lines = [f"No tables or views found{schema_label}."]
    schemas = _get_schemas(ds)
    if schemas:
        lines.append(f"Available schemas: {', '.join(schemas)}")
        if retry_hint:
            lines.append(retry_hint)
    return "\n".join(lines)


_KIND_LABELS = {"view": " [view]", "materialized_view": " [materialized view]"}


def _updated_detail_lines(addition) -> list[str]:
    """The ``Updated: <model> (...)`` detail fragments, in display order."""
    described = getattr(addition, "described_columns", []) or []
    widened = getattr(addition, "widened_columns", []) or []
    named = getattr(addition, "named_joins", []) or []
    entries = [
        f"sql_table: {addition.sql_table_change}"
        if getattr(addition, "sql_table_change", None) else None,
        f"+columns: {', '.join(addition.new_columns)}"
        if addition.new_columns else None,
        f"+joins: {', '.join(addition.new_joins)}"
        if addition.new_joins else None,
        f"named joins: {', '.join(named)}" if named else None,
        f"widened: {', '.join(widened)}" if widened else None,
        f"source_kind: {addition.kind_change}"
        if getattr(addition, "kind_change", None) else None,
        f"+descriptions: {', '.join(described)}" if described else None,
        "+model description" if getattr(addition, "model_described", False) else None,
    ]
    return [e for e in entries if e]


def _print_ingest_addition(
    addition, *, file: TextIO | None = None
) -> None:
    out = file if file is not None else sys.stdout
    # Label non-table objects — a view-backed model has no PK and no joins.
    label = _KIND_LABELS.get(getattr(addition, "source_kind", None) or "", "")
    if addition.created:
        described = getattr(addition, "described_columns", []) or []
        suffix = f", {len(described)} described" if described else ""
        print(
            f"Created: {addition.model_name} "
            f"({len(addition.new_columns)} columns{suffix}){label}",
            file=out,
        )
        return
    details = _updated_detail_lines(addition)
    if details:
        print(f"Updated: {addition.model_name} ({'; '.join(details)})", file=out)


def _print_report_section(
    *,
    entries: list,
    header: str,
    line: Callable[[Any], str],
    out: TextIO,
    footer: str | None = None,
) -> None:
    """Print one ``header`` + indented-bullet section, or nothing when empty."""
    if not entries:
        return
    print(header, file=out)
    for entry in entries:
        print(f"  - {line(entry)}", file=out)
    if footer is not None:
        print(footer, file=out)


def _hidden_internal_line(entry) -> str:
    """``<table>: <tool>``, appending the model name (needed to un-hide) when it differs."""
    target = entry.table_name
    if entry.model_name != entry.table_name:
        target = f"{entry.table_name} (model: {entry.model_name})"
    return f"{target}: {entry.tool}"


def _unhide_hint(data_source: str | None = None) -> str:
    """The ``edit_model`` call that un-hides an internal (qualified: internals collide across datasources)."""
    if data_source:
        return f'edit_model("<model>", data_source="{data_source}", hidden=false)'
    return 'edit_model("<model>", hidden=false)'


def _print_ingest_drift_and_errors(
    result, *, file: TextIO | None = None, data_source: str | None = None
) -> None:
    """Render the non-addition sections of an ingest.

    ``getattr`` because this also takes a bare ``IngestionScanReport``.
    """
    out = file if file is not None else sys.stdout
    if getattr(result, "datasource_described", False):
        print("Datasource description imported.", file=out)
    _print_report_section(
        entries=getattr(result, "to_delete", None) or [],
        header="\nPending drift (run `slayer validate-models` to inspect):",
        line=lambda e: f"{e.tool}: {e.model_name}",
        out=out,
    )
    skipped = getattr(result, "skipped", None) or []
    _print_report_section(
        entries=skipped,
        header=(
            f"\nSkipped ({len(skipped)}) — not modellable; "
            f"re-run with --exclude to silence:"
        ),
        line=lambda e: f"{e.table_name}: {e.reason}",
        out=out,
    )
    skipped_schemas = getattr(result, "skipped_schemas", None) or []
    _print_report_section(
        entries=skipped_schemas,
        header=f"\nSkipped schemas ({len(skipped_schemas)}):",
        line=lambda e: f"{e.token}: {e.reason}",
        out=out,
    )
    # Hidden internals never affect the exit code: nothing was declined.
    hidden_internals = getattr(result, "hidden_internals", None) or []
    _print_report_section(
        entries=hidden_internals,
        header=(
            f"\nHidden ({len(hidden_internals)}) — recognised ELT/migration "
            f"internals (excluded from models_summary; still queryable by "
            f"name):"
        ),
        line=_hidden_internal_line,
        # The flag only governs models this run creates, so mention un-hiding.
        footer=(
            "  --surface-internals ingests NEW internals visible; use "
            f"{_unhide_hint(data_source)} to unhide an existing one."
        ),
        out=out,
    )
    errors = getattr(result, "errors", None) or []
    _print_report_section(
        entries=errors,
        header=f"\nErrors ({len(errors)}):",
        line=lambda e: f"{e.model_name}: {e.error}",
        out=out,
    )


# --- Boot-time orchestrator ---


class StartupIngestFailure(BaseModel):
    """One per-datasource failure surfaced by the startup orchestrator."""

    name: str
    error: str


class StartupIngestSummary(BaseModel):
    """Outcome of :func:`ingest_all_datasources_idempotent`.

    ``drift_pending`` accumulates ``ToDeleteEntry`` objects from
    :mod:`slayer.engine.schema_drift` across every per-datasource result.
    It is typed as ``List[Any]`` to avoid a circular import with
    ``schema_drift``; runtime entries are
    ``EditModelDelete | WholeModelDelete``.
    """

    succeeded: list[str] = Field(default_factory=list)
    failures: list[StartupIngestFailure] = Field(default_factory=list)
    drift_pending: list[Any] = Field(default_factory=list)


async def ingest_all_datasources_idempotent(
    *,
    storage: StorageBackend,
    stream: TextIO | None = None,
) -> StartupIngestSummary:
    """Run idempotent auto-ingestion across every configured datasource.

    Per-datasource failures are accumulated, never raised; output goes to ``stream``
    (default stderr, keeping MCP stdio clean). Drift is reported, never applied.
    """
    out = stream if stream is not None else sys.stderr
    summary = StartupIngestSummary()

    names = await storage.list_datasources()
    if not names:
        print("Ingest-on-startup: no datasources configured", file=out)
        return summary

    for name in names:
        print(f"Ingesting datasource '{name}'…", file=out)
        try:
            ds = await storage.get_datasource(name)
        except Exception as exc:  # noqa: BLE001 — per-datasource isolation
            friendly = _friendly_db_error(exc)
            summary.failures.append(StartupIngestFailure(name=name, error=friendly))
            print(f"Datasource '{name}': failed — {friendly}", file=out)
            continue
        if ds is None:
            err = "datasource config disappeared between listing and load"
            summary.failures.append(StartupIngestFailure(name=name, error=err))
            print(f"Datasource '{name}': failed — {err}", file=out)
            continue
        try:
            result = await ingest_datasource_idempotent(
                datasource=ds,
                storage=storage,
                schema=None,
                include_tables=None,
                exclude_tables=None,
            )
        except Exception as exc:  # noqa: BLE001 — per-datasource isolation
            friendly = _friendly_db_error(exc)
            summary.failures.append(StartupIngestFailure(name=name, error=friendly))
            print(f"Datasource '{name}': failed — {friendly}", file=out)
            continue

        for addition in result.additions:
            _print_ingest_addition(addition, file=out)
        _print_ingest_drift_and_errors(result, file=out, data_source=name)
        summary.succeeded.append(name)
        summary.drift_pending.extend(result.to_delete)
        print(f"Datasource '{name}': ingested", file=out)

    total = len(summary.succeeded) + len(summary.failures)
    base = f"Ingest-on-startup: {len(summary.succeeded)}/{total} datasources ingested"
    if summary.failures:
        names_failed = ", ".join(f.name for f in summary.failures)
        base += f" ({len(summary.failures)} failed: {names_failed})"
    print(base, file=out)
    return summary


async def _refresh_models_for_datasource(
    *,
    datasource_name: str,
    storage: StorageBackend,
    search: "SearchService",
    failures: DocumentLoadFailures,
) -> tuple[list[tuple[str, str]], list[SlayerModel]]:
    """Refresh every loadable model's embeddings → ``(warnings tagged <ds>.<name>, models_in_ds)``."""
    warnings: list[tuple[str, str]] = []
    try:
        models_in_ds = failures.skip(await storage.load_models(data_source=datasource_name))
    except Exception as exc:  # noqa: BLE001 — defensive
        return [("", f"{datasource_name}: {exc}")], []
    for m in models_in_ds:
        tag = f"{m.data_source}.{m.name}"
        try:
            subtree_warnings = await search.refresh_model_subtree(m)
        except Exception as exc:  # noqa: BLE001 — defensive per-model
            subtree_warnings = [str(exc)]
        for w in subtree_warnings:
            warnings.append((tag, w))
    return warnings, models_in_ds


async def _refresh_datasource_doc(
    *,
    datasource_name: str,
    models: list[SlayerModel],
    search: "SearchService",
    storage: StorageBackend,
) -> list[tuple[str, str]]:
    """Refresh the datasource doc embedding; warnings are tagged ``""``."""
    cfg = await storage.get_datasource(datasource_name)
    description = cfg.description if cfg is not None else None
    try:
        doc_warnings = await search.refresh_datasource(
            name=datasource_name, models=models, description=description,
        )
    except Exception as exc:  # noqa: BLE001 — defensive
        return [("", f"{datasource_name} (datasource doc): {exc}")]
    return [("", w) for w in doc_warnings]


async def _entity_ref_exists(
    *, entity: str, storage: StorageBackend,
) -> bool | None:
    """Does the canonical ref still resolve? ``None`` on a transient lookup failure."""
    if entity.startswith(_MEMORY_PREFIX):
        memory_id = entity[len(_MEMORY_PREFIX):]
        try:
            row = await storage.get_memory_row(memory_id)
        except Exception:  # noqa: BLE001 — transient
            return None
        return row is not None
    # ``<ds>[.<model>[.<leaf>]]``
    try:
        datasources = set(await storage.list_datasources())
    except Exception:  # noqa: BLE001 — transient
        return None
    parts = entity.split(".")
    head = parts[0]
    if head not in datasources:
        return False
    if len(parts) == 1:
        return True
    model_name = parts[1]
    try:
        model = await storage.get_model(model_name, data_source=head)
    except Exception:  # noqa: BLE001 — transient
        return None
    if model is None:
        return False
    if len(parts) == 2:
        return True
    leaf = parts[-1]
    if model.get_column(leaf) is not None:
        return True
    if model.get_measure(leaf) is not None:
        return True
    if model.get_aggregation(leaf) is not None:
        return True
    return False


async def _refresh_memories_for_datasource(  # NOSONAR(S3776) — straight-line per-memory walk over the existing-refresh edge plus a stale-ref-cleanup edge; splitting the two phases would force a second iteration over the same memory corpus
    *,
    datasource_name: str,
    storage: StorageBackend,
    search: "SearchService",
    failures: DocumentLoadFailures,
) -> list[tuple[str, str]]:
    """Re-embed memories rooted at this datasource and strip their definitively stale refs.

    Warnings are tagged ``memory:<id>``; a stale ``Memory.query`` is reported, not rewritten.
    """
    try:
        memories = await storage.list_memories(failures=failures)
    except Exception as exc:  # noqa: BLE001 — defensive
        return [("", f"{datasource_name} (memories): {exc}")]
    warnings: list[tuple[str, str]] = []
    for memory in memories:
        rooted_at_ds = any(
            canonical_id_rooted_at(e, datasource_name)
            for e in memory.entities
        )
        # ``memory:<id>`` refs are datasource-agnostic: cleaned here, not re-embedded.
        has_memory_refs = any(
            e.startswith(_MEMORY_PREFIX) for e in memory.entities
        )
        if not rooted_at_ds and not has_memory_refs:
            continue
        tag = f"{_MEMORY_PREFIX}{memory.id}"
        if rooted_at_ds:
            try:
                memory_warnings = await search.upsert_memory(memory)
            except Exception as exc:  # noqa: BLE001 — defensive per-memory
                memory_warnings = [str(exc)]
            for w in memory_warnings:
                warnings.append((tag, w))
        # Drop definitively missing refs; keep ones whose lookup raised.
        cleaned: list[str] = []
        changed = False
        for entity in memory.entities:
            exists = await _entity_ref_exists(
                entity=entity, storage=storage,
            )
            if exists is False:
                changed = True
                continue
            cleaned.append(entity)
        if changed:
            try:
                rewritten = memory.model_copy(update={"entities": cleaned})
                await storage._save_memory_row(rewritten)
            except Exception as exc:  # noqa: BLE001 — defensive
                warnings.append((tag, f"cleanup failed: {exc}"))
        if memory.query is not None and rooted_at_ds:
            try:
                await extract_entities_from_query(
                    query=memory.query, storage=storage,
                )
            except (EntityResolutionError, AmbiguousModelError) as exc:
                warnings.append(
                    (tag, f"attached query has stale references: {exc}"),
                )
    return warnings


async def _refresh_datasource_embeddings(
    *, datasource_name: str, storage: StorageBackend, failures: DocumentLoadFailures | None = None,
) -> list[tuple[str, str]]:
    """Refresh embeddings for the datasource's models, doc and memories.

    Never raises; returns ``(entity_tag, error_text)`` tuples.
    """
    from slayer.search.service import SearchService  # ALLOW(import-not-top): optional embedding extra off the cold-start path

    search = SearchService(storage=storage)
    failures = failures or DocumentLoadFailures()
    model_warnings, models_in_ds = await _refresh_models_for_datasource(
        datasource_name=datasource_name, storage=storage, search=search, failures=failures,
    )
    doc_warnings = await _refresh_datasource_doc(
        datasource_name=datasource_name,
        models=models_in_ds,
        search=search,
        storage=storage,
    )
    memory_warnings = await _refresh_memories_for_datasource(
        datasource_name=datasource_name, storage=storage, search=search, failures=failures,
    )
    return model_warnings + doc_warnings + memory_warnings
