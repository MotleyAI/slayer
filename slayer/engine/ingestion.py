"""Auto-ingestion: introspect a database and generate SlayerModels with rollup-style joins.

Flow:
1. Get table names, build FK graph, check for cycles
2. For each table, build rollup SQL (with LEFT JOINs for referenced tables)
3. Introspect the rollup query's result columns for types
4. Generate one Column per non-joined column (v2 unified-columns shape)
"""

import asyncio
import logging
import sys
from collections import defaultdict, deque
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
    sanitize_model_name,
)
from slayer.engine.cardinality import (
    infer_structural_cardinality,
    is_key_set_unique,
)
from slayer.engine.internal_tables import internal_table_rule
from slayer.engine.introspect_utils import (  # noqa: F401  (re-exported for back-compat)
    _FLOAT_LIKE_INFO_SCHEMA_TYPES,
    _INFO_SCHEMA_TYPE_MAP,
    _get_columns_fallback,
    _parse_info_schema_is_float,
    _safe_get_columns,
)
from slayer.core.errors import AmbiguousModelError, EntityResolutionError
from slayer.memories.models import MEMORY_CANONICAL_PREFIX as _MEMORY_PREFIX
from slayer.memories.resolver import (
    canonical_id_rooted_at,
    extract_entities_from_query,
)
from slayer.storage.base import StorageBackend

if TYPE_CHECKING:
    # The runtime import lives inside ``_refresh_datasource_embeddings``
    # so the search module stays off the cold-start import graph
    # when the optional embedding extra isn't installed.
    from slayer.search.service import SearchService


logger = logging.getLogger(__name__)

# Module-level dedup set for unrecognized SA type warnings (see
# _sa_type_to_data_type). Keyed by upper-cased class name.
_logged_unmapped_sa_types: set[str] = set()

# Database types with no usable equality operator — grouping, DISTINCT or
# aggregating them fails at the database ("could not identify an equality
# operator for type point"). These map to ``DataType.UNKNOWN``: stored and
# displayed, never operated on. To query inside one, define a derived Column
# whose ``sql`` is a dialect-specific expression — e.g.
# ``Column(name="status", sql="payload->>'status'", type=TEXT)`` — which is
# emitted into the generated SQL and groups/filters like any other column.
#
# Deliberately a small allow-list of known-bad types rather than "everything
# unrecognized": comparable-but-unmapped types (uuid, jsonb, bytea, arrays,
# inet, citext, ...) are common and must keep working as TEXT. Marking one of
# those opaque would tell an agent that a perfectly groupable column is
# unusable — a worse failure than the query-time error opacity exists to
# prevent, which the Data Profile fallback already degrades gracefully.
# Membership verified against the Postgres catalog: a type is groupable iff it
# has a *default* btree/hash operator class (that is exactly what GROUP BY and
# DISTINCT require) —
#   SELECT EXISTS (SELECT 1 FROM pg_opclass oc JOIN pg_am am ON am.oid = oc.opcmethod
#                  WHERE oc.opcintype = t.oid AND am.amname IN ('btree','hash')
#                    AND oc.opcdefault)
# Note ``tsvector`` / ``tsquery`` ARE groupable and must not be listed here,
# and ``jsonb`` is groupable while ``json`` is not.
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

# Map SQLAlchemy types to SLayer DataTypes.
# DEV-1361: integer family → INT, floating family → DOUBLE, NUMERIC/DECIMAL
# resolved via _sa_type_is_float (scale>0 → DOUBLE, scale=0 → INT).
_SA_TYPE_MAP = {
    # Integer family → INT
    "INTEGER": DataType.INT,
    "BIGINT": DataType.INT,
    "SMALLINT": DataType.INT,
    "SERIAL": DataType.INT,
    "BIGSERIAL": DataType.INT,
    # Floating family → DOUBLE
    "FLOAT": DataType.DOUBLE,
    "REAL": DataType.DOUBLE,
    "DOUBLE": DataType.DOUBLE,
    "DOUBLE_PRECISION": DataType.DOUBLE,
    # NUMERIC/DECIMAL — refined via _sa_type_is_float in
    # _sa_type_to_data_type. Default-mapped to DOUBLE here for the rare path
    # where scale info is unavailable.
    "NUMERIC": DataType.DOUBLE,
    "DECIMAL": DataType.DOUBLE,
    # Strings
    "VARCHAR": DataType.TEXT,
    "CHAR": DataType.TEXT,
    "TEXT": DataType.TEXT,
    "STRING": DataType.TEXT,
    # Boolean
    "BOOLEAN": DataType.BOOLEAN,
    "BOOL": DataType.BOOLEAN,
    "BIT": DataType.BOOLEAN,  # T-SQL (SQL Server) boolean type
    # Temporal
    "TIMESTAMP": DataType.TIMESTAMP,
    "DATETIME": DataType.TIMESTAMP,
    "TIMESTAMP WITHOUT TIME ZONE": DataType.TIMESTAMP,
    "TIMESTAMP WITH TIME ZONE": DataType.TIMESTAMP,
    # Snowflake (DEV-1551) — three timestamp variants by timezone semantics.
    "TIMESTAMP_NTZ": DataType.TIMESTAMP,
    "TIMESTAMP_LTZ": DataType.TIMESTAMP,
    "TIMESTAMP_TZ": DataType.TIMESTAMP,
    "DATE": DataType.DATE,
    "TIME": DataType.TIMESTAMP,
    # ClickHouse adapter integer types → INT
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
    # ClickHouse adapter float types → DOUBLE
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
    "MONEY": DataType.DOUBLE,
    "SMALLMONEY": DataType.DOUBLE,
    # SQL Server rowversion — 8-byte binary counter, not temporal
    "ROWVERSION": DataType.TEXT,
}

_NUMERIC_TYPES = {DataType.INT, DataType.DOUBLE}
_ID_SUFFIXES = ("_id", "_key", "_pk", "_fk")

# Float-like SA type names — these columns get a FLOAT NumberFormat on the emitted Column.
# NUMERIC/DECIMAL are handled separately via scale inspection in _sa_type_is_float.
_FLOAT_LIKE_SA_TYPES = frozenset(
    {
        "FLOAT",
        "REAL",
        "DOUBLE",
        "DOUBLE_PRECISION",
        # ClickHouse adapter (clickhouse-sqlalchemy)
        "FLOAT32",
        "FLOAT64",
        # T-SQL monetary types (fixed-precision decimal, no integer rounding)
        "MONEY",
        "SMALLMONEY",
    }
)

# ClickHouse SA wrapper class names — peeled before type lookup.
# clickhouse-sqlalchemy exposes the inner type via .nested_type on both.
_CLICKHOUSE_WRAPPER_NAMES = frozenset({"NULLABLE", "LOWCARDINALITY"})
_CLICKHOUSE_WRAPPER_MAX_DEPTH = 8

# NUMERIC/DECIMAL type names — float-like only when scale > 0
_NUMERIC_DECIMAL_TYPES = frozenset({"NUMERIC", "DECIMAL"})

# INFORMATION_SCHEMA type maps + ``_safe_get_columns`` / ``_get_columns_fallback``
# now live in the dependency-free ``introspect_utils`` leaf module (DEV-1578);
# imported + re-exported at the top of this file for back-compat.


def _is_id_column(name: str) -> bool:
    """Check if a column name looks like an ID/key rather than a quantity."""
    lower = name.lower()
    return lower == "id" or lower.endswith(_ID_SUFFIXES)


def _unwrap_clickhouse_wrappers(sa_type: sa.types.TypeEngine) -> sa.types.TypeEngine:
    """Recursively peel ClickHouse Nullable(...) / LowCardinality(...) wrappers.

    Returns the innermost non-wrapper type. Handles arbitrary nesting order
    (e.g. LowCardinality(Nullable(String))). If the wrapper's `.nested_type`
    attribute is missing (e.g. an upstream rename), returns the wrapper as-is
    so the caller's normal fallback path runs.
    """
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
    # mssql.TIMESTAMP is SQL Server's rowversion (8-byte binary counter), not
    # a temporal type. Its class name collides with sa.TIMESTAMP, so we must
    # check isinstance before the generic name-based _SA_TYPE_MAP lookup.
    if isinstance(sa_type, _sqla_mssql.TIMESTAMP):
        return DataType.TEXT
    type_name = type(sa_type).__name__.upper()
    type_str = str(sa_type).split("(")[0].upper().strip()
    # Types with no equality operator are opaque: querying them fails at the
    # database, so declare that explicitly instead of pretending they're TEXT.
    if type_name in _OPAQUE_SA_TYPE_NAMES or type_str in _OPAQUE_SA_TYPE_NAMES:
        return DataType.UNKNOWN
    # DEV-1361: NUMERIC/DECIMAL with scale=0 are integer-shaped → INT.
    # Anything float-like (scale>0 or unknown) → DOUBLE.
    if type_name in _NUMERIC_DECIMAL_TYPES or type_str in _NUMERIC_DECIMAL_TYPES:
        return DataType.DOUBLE if _sa_type_is_float(sa_type) else DataType.INT
    if type_name in _SA_TYPE_MAP:
        return _SA_TYPE_MAP[type_name]
    if type_str in _SA_TYPE_MAP:
        return _SA_TYPE_MAP[type_str]
    if type_name not in _logged_unmapped_sa_types:
        _logged_unmapped_sa_types.add(type_name)
        logger.warning(
            "Unrecognized SQLAlchemy type %r (str=%r); falling back to "
            "DataType.TEXT. Most unmapped types (uuid, jsonb, bytea, arrays) "
            "are comparable and work as TEXT; add genuinely non-comparable "
            "ones to _OPAQUE_SA_TYPE_NAMES and the rest to _SA_TYPE_MAP.",
            type_name,
            str(sa_type),
        )
    return DataType.TEXT


def _raw_db_type_str(sa_type: sa.types.TypeEngine) -> str | None:
    """Best-effort raw database type string for ``Column.db_type``.

    ``str(sa_type)`` renders the dialect-level spelling (``"point"``,
    ``"jsonb"``, ``"geometry(Point,4326)"``). Some third-party types raise
    when compiled without a dialect, so fall back to the SA class name and
    finally to ``None`` — ``db_type`` is metadata, never worth aborting an
    ingest over.
    """
    try:
        text = str(sa_type).strip()
    except Exception:
        text = ""
    if text:
        return text
    name = type(sa_type).__name__
    return name or None


def _sa_type_is_float(sa_type: sa.types.TypeEngine) -> bool:
    """Return True if the SQLAlchemy type is float-like.

    FLOAT/REAL/DOUBLE are always float-like. NUMERIC/DECIMAL are float-like
    only when their scale is > 0 (or unknown), so NUMERIC(10,0) is treated as
    integer-like.
    """
    sa_type = _unwrap_clickhouse_wrappers(sa_type)
    type_name = type(sa_type).__name__.upper()
    if type_name in _FLOAT_LIKE_SA_TYPES:
        return True
    if type_name in _NUMERIC_DECIMAL_TYPES:
        scale = getattr(sa_type, "scale", None)
        return scale is None or scale > 0
    type_str = str(sa_type).split("(")[0].upper().strip()
    if type_str in _FLOAT_LIKE_SA_TYPES:
        return True
    if type_str in _NUMERIC_DECIMAL_TYPES:
        scale = getattr(sa_type, "scale", None)
        return scale is None or scale > 0
    return False


class RollupGraphError(Exception):
    """Raised when the FK reference graph contains cycles."""

    pass


# ---------------------------------------------------------------------------
# FK graph utilities
# ---------------------------------------------------------------------------


def _is_cross_schema_fk(
    fk: dict, schema: str | None, default_schema: str | None = None,
) -> bool:
    """Does this FK point at a table outside the schema being ingested?

    Models are keyed by bare table name, so a cross-schema FK has no model to
    bind to and would otherwise bind to a same-named local table.
    """
    referred_schema = fk.get("referred_schema")
    if referred_schema is None:  # same-schema FK; always None on SQLite
        return False
    # Ingesting the default schema passes schema=None, so fall back to it.
    effective_schema = schema if schema is not None else default_schema
    # Unknown ingested schema: skip rather than risk binding to the wrong table.
    if effective_schema is None:
        return True
    return referred_schema != effective_schema


def _get_fk_relationships(
    inspector: sa.engine.Inspector,
    table_name: str,
    schema: str | None,
    table_set: set[str],
) -> list[tuple]:
    """Get FK relationships for a table, filtered to tables in table_set.

    Returns list of (source_column, target_table, target_column).

    FK lookup is guarded: views carry no FKs and some dialects raise instead of
    returning ``[]``; this feeds ``_build_fk_graph``, so a raise would abort the
    whole ingest.
    """
    try:
        fks = inspector.get_foreign_keys(table_name, schema=schema)
    except Exception as exc:  # noqa: BLE001 — FK metadata is optional
        logger.debug("get_foreign_keys failed for %r: %s", table_name, exc)
        return []
    result = []
    for fk in fks:
        referred_table = fk["referred_table"]
        if referred_table not in table_set or referred_table == table_name:
            continue
        if _is_cross_schema_fk(
            fk=fk,
            schema=schema,
            default_schema=getattr(inspector, "default_schema_name", None),
        ):
            continue
        constrained = fk["constrained_columns"]
        referred = fk["referred_columns"]
        for src_col, tgt_col in zip(constrained, referred):
            result.append((src_col, referred_table, tgt_col))
    return result


def _build_fk_graph(
    inspector: sa.engine.Inspector,
    table_names: list[str],
    schema: str | None,
) -> dict[str, set[str]]:
    """Build directed graph: graph[table] = set of tables it references via FK."""
    table_set = set(table_names)
    graph: dict[str, set[str]] = defaultdict(set)
    for table_name in table_names:
        for _, ref_table, _ in _get_fk_relationships(
            inspector=inspector,
            table_name=table_name,
            schema=schema,
            table_set=table_set,
        ):
            graph[table_name].add(ref_table)
    return dict(graph)


def _check_acyclic(graph: dict[str, set[str]]) -> None:
    """Check that FK graph is a DAG. Raises RollupGraphError if cycles found."""
    visited: set[str] = set()
    rec_stack: set[str] = set()

    def dfs(node: str, path: list[str]) -> None:
        visited.add(node)
        rec_stack.add(node)
        path.append(node)
        for neighbor in graph.get(node, set()):
            if neighbor not in visited:
                dfs(neighbor, path)
            elif neighbor in rec_stack:
                cycle_start = path.index(neighbor)
                cycle = path[cycle_start:] + [neighbor]
                raise RollupGraphError(f"Foreign key graph contains a cycle: {' -> '.join(cycle)}")
        path.pop()
        rec_stack.remove(node)

    all_nodes: set[str] = set(graph.keys())
    for neighbors in graph.values():
        all_nodes.update(neighbors)
    for node in all_nodes:
        if node not in visited:
            dfs(node, [])


def _compute_transitive_closure(graph: dict[str, set[str]], source: str) -> set[str]:
    """BFS to find all tables transitively reachable from source (excluding source)."""
    reachable: set[str] = set()
    queue = deque([source])
    visited = {source}
    while queue:
        current = queue.popleft()
        for neighbor in graph.get(current, set()):
            if neighbor not in visited:
                visited.add(neighbor)
                reachable.add(neighbor)
                queue.append(neighbor)
    return reachable


# ---------------------------------------------------------------------------
# Join generation from FK relationships
# ---------------------------------------------------------------------------


def _get_fk_constraint_groups(
    inspector: sa.engine.Inspector,
    table_name: str,
    schema: str | None,
    table_set: set[str],
) -> list[tuple[str, list[tuple[str, str]]]]:
    """``[(referred_table, [(src_col, tgt_col), ...]), ...]``, one entry per FK
    constraint — so a composite FK stays one grouped join.
    """
    fks = inspector.get_foreign_keys(table_name, schema=schema)
    result: list[tuple[str, list[tuple[str, str]]]] = []
    for fk in fks:
        referred_table = fk["referred_table"]
        if referred_table not in table_set or referred_table == table_name:
            continue
        if _is_cross_schema_fk(
            fk=fk,
            schema=schema,
            default_schema=getattr(inspector, "default_schema_name", None),
        ):
            continue
        pairs = list(zip(fk["constrained_columns"], fk["referred_columns"]))
        if pairs:
            result.append((referred_table, pairs))
    return result


def _safe_introspect(fn) -> list:
    """Run a best-effort introspection call, yielding ``[]`` on failure.

    Constraint/index reflection is unsupported or partial on several backends,
    so a raising call degrades to "no uniqueness evidence" rather than aborting.
    """
    try:
        return list(fn())
    except Exception as exc:  # noqa: BLE001 — degrade to "no evidence"
        logger.debug(
            "Constraint/index reflection unavailable (%s); "
            "treating as no uniqueness evidence.", exc,
        )
        return []


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
                schema=schema,
            )
        else:
            # Only the bare-inspector path needs normalizing;
            # _safe_get_pk_constraint already guarantees a mapping.
            pk = inspector.get_pk_constraint(table_name=table_name, schema=schema)
            if not isinstance(pk, dict):
                return []
    except Exception:
        return []
    cols = pk.get("constrained_columns")
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
    """Does this index carry a filter predicate (a PARTIAL index)?

    A partial unique index constrains only the rows matching its predicate, so
    it is no evidence of whole-table uniqueness.
    """
    opts = idx.get("dialect_options") or {}
    for key, value in opts.items():
        # SQLAlchemy names the predicate per dialect: postgresql_where, ...
        if not key.endswith("_where") or value is None:
            continue
        if isinstance(value, str):
            if value.strip():
                return True
            continue
        # Never bool() a non-string: ColumnElement.__bool__ raises, and this
        # runs outside _safe_introspect. Presence alone means "predicate".
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
        # Expression members reflect as None, so any falsy member rejects the
        # whole set — else unique(email, lower(name)) claims email alone.
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
        table_name=table_name, schema=schema,
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
    """Names of columns that ALONE form a UNIQUE constraint / unique index.

    PK columns are excluded (``primary_key`` is the canonical marker), and so
    are composite key-sets — unique ``(a, b)`` says nothing about ``a`` alone.
    """
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


def _generate_joins(
    inspector: sa.engine.Inspector,
    source_table: str,
    referenced_tables: set[str],
    schema: str | None,
    table_set: set[str],
    sa_engine: sa.Engine | None = None,
    model_name_by_table: dict[str, str] | None = None,
) -> list[ModelJoin]:
    """Direct ModelJoins from the source table's own FKs (multi-hop is resolved
    at query time). A composite FK becomes one grouped join, and cardinality is
    inferred from key constraints alone.

    ``model_name_by_table`` maps live object names to the model names they were
    ingested under; a join names the MODEL, and a target with no model is
    dropped rather than left dangling.
    """
    groups = _get_fk_constraint_groups(
        inspector=inspector,
        table_name=source_table,
        schema=schema,
        table_set=table_set,
    )
    source_uniques = _get_unique_key_sets(
        inspector=inspector, table_name=source_table,
        schema=schema, sa_engine=sa_engine,
    )

    joins = []
    seen_signatures: set[tuple] = set()
    for ref_table, pairs in groups:
        if ref_table not in referenced_tables:
            continue
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
                join_pairs=[[s, t] for s, t in pairs],
                cardinality=cardinality,
            )
        )

    return joins


# ---------------------------------------------------------------------------
# INFORMATION_SCHEMA fallbacks (for databases like DuckDB where
# the SQLAlchemy Inspector's pg_catalog queries may not be supported)
# ---------------------------------------------------------------------------


def _get_pk_constraint_fallback(
    sa_engine: sa.Engine,
    table_name: str,
    schema: str | None,
) -> dict:
    """Get PK constraint via INFORMATION_SCHEMA when Inspector.get_pk_constraint() fails."""
    if schema:
        sql = (
            "SELECT kcu.column_name "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name = kcu.constraint_name "
            "  AND tc.table_schema = kcu.table_schema "
            "WHERE tc.table_name = :table_name "
            "  AND tc.constraint_type = 'PRIMARY KEY' "
            "  AND tc.table_schema = :schema"
        )
        params = {"table_name": table_name, "schema": schema}
    else:
        sql = (
            "SELECT kcu.column_name "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name = kcu.constraint_name "
            "  AND tc.table_schema = kcu.table_schema "
            "WHERE tc.table_name = :table_name "
            "  AND tc.constraint_type = 'PRIMARY KEY'"
        )
        params = {"table_name": table_name}
    with sa_engine.connect() as conn:
        rows = conn.execute(sa.text(sql), params).fetchall()
    return {"constrained_columns": [row[0] for row in rows]}


def _safe_get_pk_constraint(
    inspector: sa.engine.Inspector,
    sa_engine: sa.Engine,
    table_name: str,
    schema: str | None,
) -> dict:
    """Get PK constraint, falling back to INFORMATION_SCHEMA on failure.

    ALWAYS returns a mapping — the one place normalizing an inspector that may
    hand back ``None``. SQLite's PRAGMA reflection is authoritative.
    """
    if sa_engine.dialect.name == "sqlite":
        try:
            result = inspector.get_pk_constraint(table_name=table_name, schema=schema)
        except Exception:
            return {"constrained_columns": []}
        return result if isinstance(result, dict) else {"constrained_columns": []}
    try:
        result = inspector.get_pk_constraint(table_name, schema=schema)
        if result.get("constrained_columns"):
            return result
        # DuckDB's inspector returns empty PK — try INFORMATION_SCHEMA
        return _get_pk_constraint_fallback(sa_engine, table_name, schema)
    except Exception:
        return _get_pk_constraint_fallback(sa_engine, table_name, schema)


def _introspect_query_columns_via_inspector(
    sa_engine: sa.Engine,
    inspector: sa.engine.Inspector,
    table_name: str,
    schema: str | None,
    rollup_sql: str | None,
    referenced_tables: set[str],
    fk_columns_by_table: dict[str, set[str]],
    joins: list[ModelJoin] | None = None,
    live_name_by_model: dict[str, str] | None = None,
) -> list[tuple]:
    """Introspect columns from a rollup query or plain table.

    Returns list of ``(column_name, DataType, is_primary_key, is_float,
    db_type)`` tuples. ``db_type`` is the raw database type string and is only
    populated when ``DataType`` came out opaque (``UNKNOWN``) — for mapped
    types the declared ``DataType`` already carries everything, so leaving it
    ``None`` keeps stored models and golden tests clean.

    For rollup queries, uses per-table inspector data since LIMIT 0
    type inference can be unreliable across databases.
    """
    results = []

    # Source table columns
    columns = _safe_get_columns(inspector, sa_engine, table_name, schema)
    pk_constraint = _safe_get_pk_constraint(inspector, sa_engine, table_name, schema)
    pk_columns = set(pk_constraint.get("constrained_columns", []))

    for col in columns:
        col_name = col["name"]
        col_type = col["type"]
        db_type: str | None = None
        if isinstance(col_type, DataType):
            data_type = col_type
            is_float = col.get("is_float", False)
        else:
            data_type = _sa_type_to_data_type(col_type)
            is_float = _sa_type_is_float(col_type)
            if data_type.is_opaque:
                db_type = _raw_db_type_str(col_type)
        is_pk = col_name in pk_columns
        results.append((col_name, data_type, is_pk, is_float, db_type))

    # Build list of (ref_table, dotted_path) from joins — supports diamond joins
    # where the same table appears via multiple paths
    table_path_pairs: list[tuple] = []
    if joins:
        lookup = live_name_by_model or {}
        for mj in joins:
            if mj.join_pairs and "." in mj.join_pairs[0][0]:
                prefix = mj.join_pairs[0][0].split(".")[0]
                path = f"{prefix}.{mj.target_model}"
            else:
                path = mj.target_model
            # The path alias is the MODEL name; introspection needs the live
            # object name, and sanitization can make the two differ.
            table_path_pairs.append(
                (lookup.get(mj.target_model, mj.target_model), path)
            )
    else:
        # Fallback: one entry per referenced table
        for ref_table in referenced_tables:
            table_path_pairs.append((ref_table, ref_table))

    # Referenced table columns — emit once per join path
    for ref_table, path in table_path_pairs:
        ref_cols = _safe_get_columns(inspector, sa_engine, ref_table, schema)
        ref_pk = _safe_get_pk_constraint(inspector, sa_engine, ref_table, schema)
        ref_pk_cols = set(ref_pk.get("constrained_columns", []))
        ref_fk_cols = fk_columns_by_table.get(ref_table, set())

        for col in ref_cols:
            if col["name"] in ref_fk_cols:
                continue
            alias = f"{path}.{col['name']}"
            col_type = col["type"]
            ref_db_type: str | None = None
            if isinstance(col_type, DataType):
                data_type = col_type
                is_float = col.get("is_float", False)
            else:
                data_type = _sa_type_to_data_type(col_type)
                is_float = _sa_type_is_float(col_type)
                if data_type.is_opaque:
                    ref_db_type = _raw_db_type_str(col_type)
            is_pk = col["name"] in ref_pk_cols
            results.append((alias, data_type, is_pk, is_float, ref_db_type))

    return results


# ---------------------------------------------------------------------------
# Model generation from introspected columns
# ---------------------------------------------------------------------------


def _columns_to_model(
    name: str,
    columns: list[tuple],
    data_source: str,
    sql_table: str | None = None,
    joins: list[ModelJoin] | None = None,
    unique_columns: set[str] | None = None,
    source_kind: ObjectKind | None = None,
    hidden: bool = False,
    meta: dict[str, Any] | None = None,
) -> SlayerModel:
    """Generate a SlayerModel from introspected ``(column_name, DataType,
    is_pk, is_float, db_type)`` tuples.

    In v2 every Column is potentially both a dimension and a measure — what it's
    used as is decided per query. This function emits one Column per non-joined
    column, with format inferred from the column's data type. ``db_type`` is
    carried through verbatim (set only for opaque ``UNKNOWN`` columns).
    """
    cols: list[Column] = []
    unique_set = unique_columns or set()

    _INT_FORMAT = NumberFormat(type=NumberFormatType.INTEGER)
    _FLOAT_FORMAT = NumberFormat(type=NumberFormatType.FLOAT)

    for col_name, data_type, is_pk, is_float, db_type in columns:
        # Skip joined columns — they live on the target model and are
        # resolved via the join graph at query time.
        if "." in col_name:
            continue

        # Avoid name collision with the magic "*:count" / "_count" alias used
        # for COUNT(*) by renaming a literal "_count" column.
        column_name = "count_col" if col_name == "_count" else col_name

        if is_float:
            fmt = _FLOAT_FORMAT
        elif data_type in _NUMERIC_TYPES:
            fmt = _INT_FORMAT
        else:
            fmt = None

        cols.append(
            Column(
                name=column_name,
                sql=col_name,
                type=data_type,
                db_type=db_type,
                primary_key=is_pk,
                unique=(col_name in unique_set),
                format=fmt,
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
    )


def _sqlite_probe_integer_columns(
    *,
    sa_engine: sa.Engine,
    sql_table: str,
    columns: list[tuple],
) -> list[tuple]:
    """DEV-1538: per-column SQLite affinity probe.

    Walks the tuples ``(col_name, DataType, is_pk, is_float, db_type)`` produced by
    :func:`_introspect_query_columns_via_inspector` and, for every base
    column (alias without ``.``) that the SA inspector reported as
    :class:`DataType.INT`, runs
    :func:`slayer.sql.sqlite_introspect.probe_sqlite_integer_column` against
    the actual storage classes. Mutates the tuple to the widened
    :class:`DataType` whenever the probe disagrees with the declared
    affinity.

    No-op on non-SQLite engines.

    Failure modes:
    * Non-SQLite engine → input returned verbatim.
    * Probe returns ``None`` (failure or saturation) → keep the SA-derived
      INT type, leave the warning already logged by the probe in place.
    * Joined-column alias (``"."`` in the name) → skipped; joined references
      inherit their type from the target model's own probe pass.
    """
    if sa_engine.dialect.name != "sqlite":
        return columns

    from slayer.sql.sqlite_introspect import probe_sqlite_integer_column

    schema, table = _parse_qualified_sql_table(sql_table)
    out: list[tuple] = []
    with sa_engine.connect() as conn:
        for col_name, data_type, is_pk, is_float, db_type in columns:
            if data_type is not DataType.INT or "." in col_name:
                out.append((col_name, data_type, is_pk, is_float, db_type))
                continue
            try:
                verdict = probe_sqlite_integer_column(
                    conn=conn,
                    table=table,
                    column=col_name,
                    schema=schema,
                )
            except Exception as exc:
                # Defence-in-depth: the helper catches its own errors but a
                # caller-level guard keeps ingest from aborting on unexpected
                # exceptions outside the helper's scope (e.g. import-time
                # failures on environments missing sqlite_introspect).
                logger.warning(
                    "probe call raised for %s.%s; keeping declared INT: %s",
                    sql_table,
                    col_name,
                    exc,
                )
                verdict = None
            if verdict is None or verdict is DataType.INT:
                out.append((col_name, data_type, is_pk, is_float, db_type))
                continue
            new_is_float = verdict is DataType.DOUBLE
            out.append((col_name, verdict, is_pk, new_is_float, db_type))
    return out


def _parse_qualified_sql_table(sql_table: str) -> tuple[str | None, str]:
    """Split ``"schema.table"`` into ``(schema, table)`` or ``(None, table)``.

    Only splits on a single dot — table/schema names containing dots are
    out of scope for the auto-ingest path (the dotted form would never have
    survived ``Inspector.get_table_names`` either).
    """
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
    """Introspect a single table (no FK rollup) and return a SlayerModel.

    This is the building block shared between the auto-ingest path and the
    dbt hidden-model import. It never builds joins or traverses the FK graph.

    ``source_kind=None`` means "not classified": the dbt/OSI converters don't
    know the live object's kind.
    """
    columns = _introspect_query_columns_via_inspector(
        sa_engine=sa_engine,
        inspector=inspector,
        table_name=table_name,
        schema=schema,
        rollup_sql=None,
        referenced_tables=set(),
        fk_columns_by_table={},
    )
    sql_table = f"{schema}.{table_name}" if schema else table_name
    columns = _sqlite_probe_integer_columns(
        sa_engine=sa_engine,
        sql_table=sql_table,
        columns=columns,
    )
    unique_columns = _solo_unique_columns_for_table(
        inspector=inspector, sa_engine=sa_engine,
        table_name=table_name, schema=schema,
    )
    return _columns_to_model(
        name=model_name or table_name,
        columns=columns,
        data_source=data_source,
        sql_table=sql_table,
        unique_columns=unique_columns,
        source_kind=source_kind,
    )


# ---------------------------------------------------------------------------
# Object discovery
# ---------------------------------------------------------------------------


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
    # Every recognised internal that produced a model, regardless of
    # ``surface_internals`` — the idempotent path filters this against storage.
    internal_tables: list[InternalTable] = Field(default_factory=list)
    # Every object discovered, modelled or not — lets the CLI tell an empty
    # schema apart from one whose objects were all skipped / already in sync.
    objects: list[IngestableObject] = Field(default_factory=list)

    @property
    def hidden_internals(self) -> list[InternalTable]:
        """The subset this scan hid — derived so it can't drift from
        ``internal_tables``. Effective state only for callers that persist
        ``models`` directly; the idempotent path uses
        ``_effective_hidden_internals`` instead.
        """
        return [t for t in self.internal_tables if t.hidden]


def _safe_object_names(
    *,
    accessor_name: str,
    inspector: sa.engine.Inspector,
    schema: str | None,
) -> list[str]:
    """Call an ``Inspector.get_*_names`` accessor, tolerating dialects that
    lack it.

    ``get_materialized_view_names`` raises ``NotImplementedError`` where
    unsupported, so it cannot be called bare. Driver errors are tolerated too —
    broken view discovery must not stop tables ingesting.
    """
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
    schema: str | None,
    include_views: bool = True,
) -> list[IngestableObject]:
    """Discover every ingestable object in ``schema``, classified by kind.

    Order is deterministic (tables, views, matviews) because
    :func:`_assign_model_names` resolves collisions first-come. Deduped across
    accessors — some dialects return views from ``get_table_names()``.
    """
    objects: list[IngestableObject] = []
    seen: set[str] = set()

    def _add(names: list[str], kind: ObjectKind) -> None:
        for name in names:
            if name in seen:
                continue
            seen.add(name)
            objects.append(IngestableObject(name=name, kind=kind))

    _add(list(inspector.get_table_names(schema=schema) or []), "table")
    if include_views:
        _add(
            _safe_object_names(
                accessor_name="get_view_names", inspector=inspector, schema=schema
            ),
            "view",
        )
        _add(
            _safe_object_names(
                accessor_name="get_materialized_view_names",
                inspector=inspector,
                schema=schema,
            ),
            "materialized_view",
        )
    return objects


def _assign_model_names(
    objects: list[IngestableObject],
) -> tuple[dict[str, str], list[SkippedTable]]:
    """Map each object name to its model name, returning ``(mapping, skipped)``.

    Model names may not contain ``__`` (the SQL generator reads it as a join
    path, so ``a__b`` would query ``a -> b``); only the model name is sanitized.
    Unsanitized names are reserved first so a real ``a_b`` beats a sanitized
    ``a__b``, and collisions skip rather than suffix (suffixes shift with the
    object set, churning drift). The sanitized pass walks sorted candidates so a
    dialect's listing order can't repoint a model at a different object.
    """
    assigned: dict[str, str] = {}
    taken: set[str] = {o.name for o in objects if "__" not in o.name}
    skipped: list[SkippedTable] = []

    for obj in objects:
        if "__" not in obj.name:
            assigned[obj.name] = obj.name

    for obj in sorted(
        (o for o in objects if "__" in o.name), key=lambda o: o.name
    ):
        candidate = sanitize_model_name(obj.name)
        if candidate in taken:
            skipped.append(
                SkippedTable(
                    table_name=obj.name,
                    kind=obj.kind,
                    reason=(
                        f"name collision: sanitizing '__' yields "
                        f"'{candidate}', which is already taken"
                    ),
                )
            )
            continue
        taken.add(candidate)
        assigned[obj.name] = candidate

    return assigned, skipped


# ---------------------------------------------------------------------------
# Main ingestion
# ---------------------------------------------------------------------------


def _build_one_model(
    *,
    sa_engine: sa.Engine,
    inspector: sa.engine.Inspector,
    obj: IngestableObject,
    model_name: str,
    schema: str | None,
    data_source: str,
    fk_graph: dict[str, set[str]],
    has_cycles: bool,
    fk_columns_by_table: dict[str, set[str]],
    table_set: set[str],
    model_name_by_table: dict[str, str] | None = None,
    live_name_by_model: dict[str, str] | None = None,
    internal_tool: str | None = None,
) -> SlayerModel:
    """Introspect one live object into a model. Raises on failure; the caller
    isolates per-object.

    ``internal_tool`` is the bookkeeping verdict (``None`` when unrecognised or
    when the caller surfaced internals). When set, the model is built ``hidden``
    with a ``meta.internal_table`` breadcrumb so an unexplained ``hidden: true``
    never lands in a persisted YAML.
    """
    referenced = (
        set() if has_cycles else _compute_transitive_closure(fk_graph, obj.name)
    )
    sql_table = f"{schema}.{obj.name}" if schema else obj.name

    model_joins = None
    if referenced:
        model_joins = _generate_joins(
            inspector=inspector,
            source_table=obj.name,
            referenced_tables=referenced,
            schema=schema,
            table_set=table_set,
            sa_engine=sa_engine,
            model_name_by_table=model_name_by_table,
        )

    columns = _introspect_query_columns_via_inspector(
        sa_engine=sa_engine,
        inspector=inspector,
        table_name=obj.name,
        schema=schema,
        rollup_sql=None,
        referenced_tables=referenced,
        fk_columns_by_table=fk_columns_by_table,
        joins=model_joins,
        live_name_by_model=live_name_by_model,
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
            table_name=obj.name, schema=schema,
        ),
        source_kind=obj.kind,
        hidden=internal_tool is not None,
        meta=meta,
    )


def _dispose_quietly(sa_engine: sa.Engine) -> None:
    """Dispose ``sa_engine``, logging rather than raising on failure.

    Called from ``finally``, so a raise would mask the in-flight exception.
    Logged at WARNING because a failed dispose leaks the connection, blocking
    an external ``duckdb.connect(file)`` on the same file.
    """
    try:
        sa_engine.dispose()
    except Exception as exc:  # noqa: BLE001 — teardown must not mask the cause
        logger.warning(
            "engine dispose failed; the connection may remain open: %s", exc
        )


def _collect_fk_columns(
    *,
    inspector: sa.engine.Inspector,
    table_names: list[str],
    schema: str | None,
) -> dict[str, set[str]]:
    """Map each table to its FK-constrained columns, for rollup exclusion.

    Guarded per table (see ``_get_fk_relationships``): views have no FKs and
    some dialects raise instead of returning ``[]``.
    """
    out: dict[str, set[str]] = defaultdict(set)
    for table_name in table_names:
        try:
            fks = inspector.get_foreign_keys(table_name, schema=schema)
        except Exception as exc:  # noqa: BLE001 — FK metadata is optional
            logger.debug("get_foreign_keys failed for %r: %s", table_name, exc)
            continue
        for fk in fks:
            for col in fk["constrained_columns"]:
                out[table_name].add(col)
    return out


def ingest_datasource_report(
    datasource: DatasourceConfig,
    include_tables: list[str] | None = None,
    exclude_tables: list[str] | None = None,
    schema: str | None = None,
    include_views: bool = True,
    surface_internals: bool = False,
) -> IngestionScanReport:
    """Introspect ``datasource``, returning models plus everything skipped.

    Discovers views and matviews (``include_views``); an unmodellable object is
    skipped with a reason rather than aborting. Recognised ELT/migration
    bookkeeping is modelled ``hidden`` unless ``surface_internals`` — a separate
    axis from ``include_tables`` / ``exclude_tables`` (which choose what is
    scanned), so naming an internal in ``include_tables`` still hides it.
    """
    from slayer.sql import engine_factory
    sa_engine = engine_factory.get_engine(datasource.resolve_env_vars())
    try:
        inspector = sa.inspect(sa_engine)

        objects = list_ingestable_objects(
            inspector=inspector, schema=schema, include_views=include_views
        )
        if include_tables:
            objects = [o for o in objects if o.name in include_tables]
        if exclude_tables:
            objects = [o for o in objects if o.name not in exclude_tables]

        table_names = [o.name for o in objects]
        table_set = set(table_names)

        name_by_object, skipped = _assign_model_names(objects)
        live_by_model = {model: live for live, model in name_by_object.items()}

        # Build FK graph, check for cycles
        fk_graph = _build_fk_graph(
            inspector=inspector, table_names=table_names, schema=schema
        )
        has_cycles = False
        try:
            _check_acyclic(fk_graph)
        except RollupGraphError as e:
            logger.warning(f"FK graph has cycles, skipping rollup: {e}")
            has_cycles = True

        fk_columns_by_table = _collect_fk_columns(
            inspector=inspector, table_names=table_names, schema=schema
        )

        models = []
        internal_tables: list[InternalTable] = []
        for obj in objects:
            model_name = name_by_object.get(obj.name)
            if model_name is None:
                continue  # already recorded in ``skipped`` by _assign_model_names
            # Classified on the live name, not the model name; evaluated even
            # under ``surface_internals`` so the entry is still recorded.
            tool = internal_table_rule(obj.name)
            try:
                models.append(
                    _build_one_model(
                        sa_engine=sa_engine,
                        inspector=inspector,
                        obj=obj,
                        model_name=model_name,
                        schema=schema,
                        data_source=datasource.name,
                        fk_graph=fk_graph,
                        has_cycles=has_cycles,
                        fk_columns_by_table=fk_columns_by_table,
                        table_set=table_set,
                        model_name_by_table=name_by_object,
                        live_name_by_model=live_by_model,
                        internal_tool=None if surface_internals else tool,
                    )
                )
            except Exception as exc:  # noqa: BLE001 — per-object isolation
                logger.warning(
                    "Skipping %s %r in datasource %r: %s",
                    obj.kind, obj.name, datasource.name, exc,
                )
                skipped.append(
                    SkippedTable(table_name=obj.name, kind=obj.kind, reason=str(exc))
                )
                continue
            # Recorded only after construction succeeds, so an object never
            # lands in both ``skipped`` and ``internal_tables``.
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

        return IngestionScanReport(
            models=models,
            skipped=skipped,
            objects=objects,
            internal_tables=internal_tables,
        )
    finally:
        # In a ``finally`` because discovery and the FK passes can raise a
        # driver error, and an undisposed engine holds the connection open.
        _dispose_quietly(sa_engine)


def ingest_datasource(
    datasource: DatasourceConfig,
    include_tables: list[str] | None = None,
    exclude_tables: list[str] | None = None,
    schema: str | None = None,
    include_views: bool = True,
    surface_internals: bool = False,
) -> list[SlayerModel]:
    """Models only, for callers that don't need the skip report."""
    return ingest_datasource_report(
        datasource=datasource,
        include_tables=include_tables,
        exclude_tables=exclude_tables,
        schema=schema,
        include_views=include_views,
        surface_internals=surface_internals,
    ).models


# ---------------------------------------------------------------------------
# Idempotent re-ingestion (DEV-1356)
# ---------------------------------------------------------------------------


def _existing_join_signatures(model: SlayerModel) -> set[tuple[str, tuple[tuple[str, str], ...]]]:
    """Return the set of (target_model, sorted join_pair tuples) signatures
    for joins already on ``model``. Used to detect new joins.
    """
    out: set[tuple[str, tuple[tuple[str, str], ...]]] = set()
    for j in model.joins:
        sig_pairs = tuple(sorted((p[0], p[1]) for p in j.join_pairs))
        out.add((j.target_model, sig_pairs))
    return out


def _is_auto_default_integer_format(fmt: NumberFormat | None) -> bool:
    """Return True when ``fmt`` looks like the auto-ingested ``NumberFormat
    (type=INTEGER)`` default (no custom precision / symbol set). Used by
    DEV-1538's widening path to decide whether to flip the format alongside
    the type; user-set custom formats are preserved verbatim.
    """
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
    """DEV-1538: decide whether a persisted column should be widened based
    on a freshly-probed type, and return ``(merged_column, did_widen)``.

    The widen branch only fires when ``sqlite_widen_enabled`` is True
    (SQLite-only auto-heal), the fresh column exists, the persisted column
    is ``DataType.INT``, and the fresh type is ``DataType.DOUBLE`` or
    ``DataType.TEXT``. All other cases return ``persisted_col`` unchanged.
    """
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


def _merge_joins_strict(
    persisted: SlayerModel, fresh: SlayerModel,
) -> tuple[list[ModelJoin], list[str], bool]:
    """Append joins whose signature isn't already present. Raises on the
    duplicate-target / different-pairs conflict so callers don't end up
    with two joins pointing at the same target_model.

    An unset ``cardinality`` is filled from the matching fresh join; a
    user-set one is never overwritten. The third return value flags a
    metadata-only fill, which still has to trigger a save.
    """
    existing_join_sigs = _existing_join_signatures(persisted)
    existing_join_targets = {j.target_model for j in persisted.joins}
    fresh_by_sig = {_join_sig(j): j for j in fresh.joins}

    metadata_changed = False
    new_joins: list[ModelJoin] = []
    for pj in persisted.joins:
        fj = fresh_by_sig.get(_join_sig(pj))
        if pj.cardinality is None and fj is not None and fj.cardinality is not None:
            new_joins.append(pj.model_copy(update={"cardinality": fj.cardinality}))
            metadata_changed = True
        else:
            new_joins.append(pj)

    new_join_targets: list[str] = []
    for j in fresh.joins:
        sig = (j.target_model, tuple(sorted((p[0], p[1]) for p in j.join_pairs)))
        if sig in existing_join_sigs:
            continue
        if j.target_model in existing_join_targets:
            raise ValueError(
                f"Model {persisted.name!r} already has a join targeting "
                f"{j.target_model!r} with different join_pairs; the "
                f"additive re-ingest cannot represent both join "
                f"definitions safely. Drop the existing join via "
                f"``edit_model(remove={{'joins': [{j.target_model!r}]}})`` "
                f"and re-run."
            )
        new_joins.append(j)
        new_join_targets.append(j.target_model)
    return new_joins, new_join_targets, metadata_changed


class AdditiveMergeResult(BaseModel):
    """Outcome of :func:`_additive_merge_existing`."""

    merged: SlayerModel
    new_columns: list[str] = Field(default_factory=list)
    new_joins: list[str] = Field(default_factory=list)
    widened_columns: list[str] = Field(default_factory=list)
    kind_changed: bool = False
    #: A metadata-only fill (join cardinality / column unique) that still
    #: has to be saved even when no column or join was added.
    metadata_changed: bool = False


def _additive_merge_existing(
    *,
    persisted: SlayerModel,
    fresh: SlayerModel,
    sqlite_widen_enabled: bool = False,
) -> AdditiveMergeResult:
    """Merge a freshly-ingested ``fresh`` model into ``persisted`` additively.

    * Existing columns are preserved verbatim (description / label / format /
      meta / allowed_aggregations / filter never overwritten).
    * SQLite-only carve-out (``sqlite_widen_enabled=True``): a
      fresh column whose type widened from the persisted ``DataType.INT``
      (i.e. fresh type is ``DOUBLE`` or ``TEXT``) replaces ONLY the persisted
      type — and the persisted ``format`` IF the persisted format is the
      auto-ingested ``NumberFormat(INTEGER)`` default. Custom formats are
      preserved verbatim and an INFO log line is emitted naming the column.
      Widening never narrows DOUBLE → INT. On non-SQLite datasources the
      additive contract stays strict — schema drift surfaces via
      ``slayer validate-models``, not via silent re-ingest overwrites.
    * Live columns whose names are absent from ``persisted.columns`` are
      appended from ``fresh.columns``.
    * Joins with new ``(target_model, join_pairs)`` signatures are appended.
    * Carve-out: ``source_kind`` is refreshed, not preserved — it describes the
      live object, and the view→table flip it captures often changes no columns.
      A ``None`` from a non-classifying path never erases a known value.
    """
    existing_by_name: dict[str, Column] = {c.name: c for c in persisted.columns}
    fresh_by_name: dict[str, Column] = {c.name: c for c in fresh.columns}

    widened_column_names: list[str] = []
    merged_columns: list[Column] = []
    metadata_changed = False
    for persisted_col in persisted.columns:
        merged_col, did_widen = _merge_persisted_column_with_probe(
            persisted_col=persisted_col,
            fresh_col=fresh_by_name.get(persisted_col.name),
            model_name=persisted.name,
            sqlite_widen_enabled=sqlite_widen_enabled,
        )
        # Set `unique` additively — never downgrade a user-set flag.
        fresh_col = fresh_by_name.get(persisted_col.name)
        if fresh_col is not None and fresh_col.unique and not merged_col.unique:
            merged_col = merged_col.model_copy(update={"unique": True})
            metadata_changed = True
        merged_columns.append(merged_col)
        if did_widen:
            widened_column_names.append(persisted_col.name)

    new_column_names: list[str] = []
    for fresh_col in fresh.columns:
        if fresh_col.name in existing_by_name:
            continue
        merged_columns.append(fresh_col)
        new_column_names.append(fresh_col.name)

    new_joins, new_join_targets, joins_metadata_changed = _merge_joins_strict(
        persisted, fresh
    )
    metadata_changed = metadata_changed or joins_metadata_changed

    # In the short-circuit below (not just the update dict), else a view→table
    # flip that changes nothing else would never reach the refresh.
    kind_changed = (
        fresh.source_kind is not None
        and fresh.source_kind != persisted.source_kind
    )

    if not (
        new_column_names
        or new_join_targets
        or widened_column_names
        or kind_changed
        or metadata_changed
    ):
        return AdditiveMergeResult(merged=persisted)

    update: dict[str, Any] = {"columns": merged_columns, "joins": new_joins}
    if kind_changed:
        update["source_kind"] = fresh.source_kind

    return AdditiveMergeResult(
        merged=persisted.model_copy(update=update),
        new_columns=new_column_names,
        new_joins=new_join_targets,
        widened_columns=widened_column_names,
        kind_changed=kind_changed,
        metadata_changed=metadata_changed,
    )


async def _process_one_table(
    *,
    table_name: str,
    fresh: SlayerModel,
    datasource: DatasourceConfig,
    storage: StorageBackend,
):
    """Save / merge one freshly-introspected model, returning the
    ``ModelAddition`` to record. Raises on persistence failure — the caller
    isolates errors per-model.
    """
    from slayer.engine.schema_drift import ModelAddition

    persisted = await storage.get_model(table_name, data_source=datasource.name)
    if persisted is None:
        await storage.save_model(fresh)
        return ModelAddition(
            model_name=table_name,
            data_source=datasource.name,
            created=True,
            new_columns=[c.name for c in fresh.columns],
            new_joins=[j.target_model for j in fresh.joins],
            source_kind=fresh.source_kind,
        )
    if persisted.sql or persisted.source_queries:
        # User-authored sql / query-backed model with the matching name —
        # leave it alone.
        return None
    outcome = _additive_merge_existing(
        persisted=persisted,
        fresh=fresh,
        sqlite_widen_enabled=(datasource.type or "").lower() == "sqlite",
    )
    # ``kind_changed`` and ``metadata_changed`` gate the save too: a view→table
    # flip, or a cardinality / unique fill, usually adds no columns or joins, so
    # otherwise the refreshed model would be discarded.
    if (
        outcome.new_columns
        or outcome.new_joins
        or outcome.widened_columns
        or outcome.kind_changed
        or outcome.metadata_changed
    ):
        await storage.save_model(outcome.merged)
    kind_change = None
    if outcome.kind_changed:
        before = persisted.source_kind or "unknown"
        kind_change = f"{before} → {fresh.source_kind}"
    return ModelAddition(
        model_name=table_name,
        data_source=datasource.name,
        created=False,
        new_columns=outcome.new_columns,
        new_joins=outcome.new_joins,
        widened_columns=outcome.widened_columns,
        source_kind=outcome.merged.source_kind,
        kind_change=kind_change,
    )


def _bare_table_name(sql_table: str) -> str:
    """Strip an optional schema prefix from a ``schema.table`` reference."""
    return sql_table.split(".", 1)[1] if "." in sql_table else sql_table


async def _scoped_models_for_validation(
    *,
    storage: StorageBackend,
    datasource: DatasourceConfig,
    in_scope_table_names: set[str],
) -> list[SlayerModel]:
    """Build the list of persisted models to feed to ``validate_datasource``.

    sql_table-mode models are included only when their live table is in
    scope (matches the additive pass). sql-mode and query-backed models are
    always validated within this datasource — they're not tied to a
    specific live table name.
    """
    identities = await storage._list_all_model_identities()
    ds_model_names = [n for d, n in identities if d == datasource.name]
    scoped: list[SlayerModel] = []
    for name in ds_model_names:
        m = await storage.get_model(name, data_source=datasource.name)
        if m is None:
            continue
        if m.sql_table:
            if _bare_table_name(m.sql_table) in in_scope_table_names:
                scoped.append(m)
            continue
        scoped.append(m)
    return scoped


async def _effective_hidden_internals(
    *,
    candidates: list[InternalTable],
    datasource: DatasourceConfig,
    storage: StorageBackend,
) -> list[InternalTable]:
    """Narrow scan-time classifications to models actually hidden after the merge.

    ``_process_one_table`` preserves the persisted ``hidden`` (and skips merging
    user-authored models entirely), so the scan's verdict can lie both ways — a
    since-un-hidden internal, or silence under ``--surface-internals`` for one an
    earlier run hid. Takes ``internal_tables`` (not ``hidden_internals``, empty
    under ``surface_internals``) and keys on ``model_name``, which differs from
    ``table_name`` for ``__``-sanitized objects.
    """
    effective: list[InternalTable] = []
    for entry in candidates:
        try:
            persisted = await storage.get_model(
                entry.model_name, data_source=datasource.name
            )
        except Exception as exc:  # noqa: BLE001 — reporting must not fail ingest
            logger.debug(
                "hidden-internal re-check failed for %r: %s", entry.model_name, exc
            )
            continue
        if persisted is not None and persisted.hidden:
            effective.append(entry)
    return effective


async def ingest_datasource_idempotent(
    *,
    datasource: DatasourceConfig,
    storage: StorageBackend,
    include_tables: list[str] | None = None,
    exclude_tables: list[str] | None = None,
    schema: str | None = None,
    include_views: bool = True,
    surface_internals: bool = False,
):
    """Idempotent re-ingestion.

    Walks the live datasource and, for each in-scope table:

    * Creates a fresh ``sql_table``-mode SlayerModel when none exists.
    * Appends new columns / joins to an existing ``sql_table``-mode model
      without ever overwriting existing entries.
    * Skips ``sql``-mode and query-backed models silently — those are
      user-authored.

    After the additive pass, runs ``validate_models`` scoped to the same
    in-scope set so type drift on existing columns / dropped tables show up
    in ``to_delete``.
    """
    # Local import to avoid an import cycle with engine.schema_drift.
    from slayer.engine.schema_drift import (
        IdempotentIngestResult,
        IngestionError,
        ModelAddition,
        validate_datasource,
    )

    additions: list[ModelAddition] = []
    errors: list[IngestionError] = []

    # ``ingest_datasource_report`` is sync (it drives SQLAlchemy ``Inspector``).
    # Offload to a thread so a slow / large datasource doesn't block the
    # event loop while server-facing requests are in flight.
    scan = await asyncio.to_thread(
        ingest_datasource_report,
        datasource=datasource,
        include_tables=include_tables,
        exclude_tables=exclude_tables,
        schema=schema,
        include_views=include_views,
        surface_internals=surface_internals,
    )
    fresh_models = scan.models
    fresh_by_name = {m.name: m for m in fresh_models}
    # Keyed on the live object name, not the model name: validation scoping
    # compares against ``_bare_table_name(m.sql_table)``, so any model whose
    # name differs from its table (``__``-sanitized or dbt/OSI hidden) would
    # otherwise drop out of scope.
    in_scope_table_names: set[str] = {
        _bare_table_name(m.sql_table) for m in fresh_models if m.sql_table
    }

    for table_name, fresh in fresh_by_name.items():
        try:
            addition = await _process_one_table(
                table_name=table_name,
                fresh=fresh,
                datasource=datasource,
                storage=storage,
            )
            if addition is not None:
                additions.append(addition)
        except Exception as exc:  # noqa: BLE001 — best-effort per-model isolation
            errors.append(
                IngestionError(
                    model_name=table_name,
                    data_source=datasource.name,
                    error=str(exc),
                )
            )

    scoped_models = await _scoped_models_for_validation(
        storage=storage,
        datasource=datasource,
        in_scope_table_names=in_scope_table_names,
    )
    to_delete = await validate_datasource(
        datasource=datasource, models=scoped_models
    )

    # Column sample-value profiling is NOT run at ingest time — it fires a
    # per-column full-table scan and, on a wide datasource (dozens of tables
    # × ~10 columns each), would run hundreds of full scans and dominate
    # ingest wall-clock. Samples are instead refreshed on demand on a cache
    # miss by the async ``ensure_column_sample_fresh`` helper, invoked from
    # the read paths that surface samples — ``inspect_model``, the ``inspect``
    # point-lookup, and ``search()``. Use ``slayer search refresh-samples``
    # to warm the cache explicitly.

    # DEV-1386: refresh persisted embeddings for the datasource doc plus
    # every visible model + its visible children. Best-effort: per-entity
    # failures are surfaced as IngestionError entries, never aborts
    # ingestion. When the `advanced_search` extra is not installed,
    # EmbeddingRetriever returns a single warning and does no work.
    embedding_errors = await _refresh_datasource_embeddings(
        datasource_name=datasource.name, storage=storage,
    )
    for model_name, err in embedding_errors:
        # DEV-1416: each helper inside ``_refresh_datasource_embeddings``
        # attaches the canonical entity tag (``<ds>.<model>``,
        # ``memory:<id>``, or ``""`` for the datasource doc) so a
        # startup log inspection can distinguish memory failures from
        # model / datasource-doc failures at a glance — no string
        # sniffing of free-form warning text.
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
    )

    return IdempotentIngestResult(
        additions=additions,
        to_delete=list(to_delete),
        errors=errors,
        skipped=scan.skipped,
        objects=scan.objects,
        hidden_internals=hidden_internals,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Friendly-error helper (moved from slayer/mcp/server.py — DEV-1392 so it can
# be shared by the MCP server and the boot-time orchestrator without the
# engine → mcp import edge).
# ─────────────────────────────────────────────────────────────────────────────


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


# ─────────────────────────────────────────────────────────────────────────────
# Renderers — moved from slayer/cli.py so `slayer ingest` and the boot-time
# orchestrator share one source of truth and one output channel (`file=`).
# ─────────────────────────────────────────────────────────────────────────────


def _get_schemas(ds: DatasourceConfig) -> list[str]:
    """List a datasource's schemas. Best-effort; empty means no hint."""
    try:
        from slayer.sql import engine_factory
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
    """Explain an empty ingest and point at the likely fix.

    Says "tables or views" so a views-only schema doesn't read as empty.
    ``retry_hint`` comes from the caller, keeping this interface-neutral.
    """
    schema_label = f" in schema '{schema_name}'" if schema_name else ""
    lines = [f"No tables or views found{schema_label}."]
    schemas = _get_schemas(ds)
    if schemas:
        lines.append(f"Available schemas: {', '.join(schemas)}")
        if retry_hint:
            lines.append(retry_hint)
    return "\n".join(lines)


_KIND_LABELS = {"view": " [view]", "materialized_view": " [materialized view]"}


def _print_ingest_addition(
    addition, *, file: TextIO | None = None
) -> None:
    out = file if file is not None else sys.stdout
    # Label non-table objects — a view-backed model has no PK and no joins.
    label = _KIND_LABELS.get(getattr(addition, "source_kind", None) or "", "")
    if addition.created:
        print(
            f"Created: {addition.model_name} "
            f"({len(addition.new_columns)} columns){label}",
            file=out,
        )
        return
    widened = getattr(addition, "widened_columns", []) or []
    kind_change = getattr(addition, "kind_change", None)
    if not (addition.new_columns or addition.new_joins or widened or kind_change):
        return
    details = []
    if addition.new_columns:
        details.append(f"+columns: {', '.join(addition.new_columns)}")
    if addition.new_joins:
        details.append(f"+joins: {', '.join(addition.new_joins)}")
    if widened:
        details.append(f"widened: {', '.join(widened)}")
    if kind_change:
        details.append(f"source_kind: {kind_change}")
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
    """``<table>: <tool>``, appending the model name when it differs.

    The un-hide advice takes the model name, so for a ``__``-sanitized table the
    table name alone would name something the user cannot act on.
    """
    target = entry.table_name
    if entry.model_name != entry.table_name:
        target = f"{entry.table_name} (model: {entry.model_name})"
    return f"{target}: {entry.tool}"


def _unhide_hint(data_source: str | None = None) -> str:
    """The ``edit_model`` invocation that un-hides one recognised internal.

    Qualified with ``data_source`` when known: a bare model name raises
    ``AmbiguousModelError`` across datasources, and internals collide by
    construction (``_dlt_loads`` exists in every dlt-loaded database). Shared
    with the MCP renderer so both surfaces advise the same call.
    """
    if data_source:
        return f'edit_model("<model>", data_source="{data_source}", hidden=false)'
    return 'edit_model("<model>", hidden=false)'


def _print_ingest_drift_and_errors(
    result, *, file: TextIO | None = None, data_source: str | None = None
) -> None:
    """Render the non-addition sections of an ingest.

    Fields are read through ``getattr`` because this takes both an
    ``IdempotentIngestResult`` and a bare ``IngestionScanReport`` (which has no
    ``to_delete`` / ``errors``). ``data_source`` qualifies the un-hide hint (see
    ``_unhide_hint``); it comes from the caller since neither result carries it.
    """
    out = file if file is not None else sys.stdout
    _print_report_section(
        entries=getattr(result, "to_delete", None) or [],
        header="\nPending drift (run `slayer validate-models` to inspect):",
        line=lambda e: f"{e.tool}: {e.model_name}",
        out=out,
    )
    # Skips are separate from errors: "can't be modelled" differs in cause and
    # fix from "failed to persist".
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


# ─────────────────────────────────────────────────────────────────────────────
# DEV-1392 — boot-time orchestrator
# ─────────────────────────────────────────────────────────────────────────────


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

    Sequential. Per-datasource failures are caught and accumulated; the
    function never raises on a single-datasource error and the server is
    expected to start regardless. ``storage.list_datasources()`` raising IS
    propagated — boot should not proceed with broken storage.

    All human-readable output goes through ``stream`` (default ``sys.stderr``)
    so ``slayer mcp`` stdio remains protocol-safe.

    Drift entries from each per-datasource result are printed and
    accumulated into ``summary.drift_pending``, but never auto-applied:
    ``apply_drift_deletes`` is gated behind ``slayer validate-models
    --force-clean`` and intentionally not reachable from this path.
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
) -> tuple[list[tuple[str, str]], list[SlayerModel]]:
    """Refresh embeddings for every visible model in the datasource.

    Returns ``(warnings, models_in_ds)``. Each warning is tagged with
    the model's ``<ds>.<name>`` so the orchestrator can route it to the
    right ``IngestionError.model_name``. ``models_in_ds`` is forwarded
    to the datasource-doc refresh that follows.
    """
    warnings: list[tuple[str, str]] = []
    models_in_ds: list[SlayerModel] = []
    try:
        identities = await storage._list_all_model_identities()
    except Exception as exc:  # noqa: BLE001 — defensive
        return [("", f"{datasource_name}: {exc}")], models_in_ds
    for ds, name in identities:
        if ds != datasource_name:
            continue
        tag = f"{ds}.{name}"
        try:
            m = await storage.get_model(name, data_source=ds)
        except Exception as exc:  # noqa: BLE001 — defensive per-model
            warnings.append((tag, str(exc)))
            continue
        if m is None:
            continue
        models_in_ds.append(m)
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
    """Refresh the datasource doc embedding. Warnings are tagged with
    an empty ``model_name`` since the doc has no specific entity name.

    DEV-1549: ``DatasourceConfig.description`` is threaded through so
    description text contributes to lexical + embedding recall.
    """
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
    """DEV-1428 defense-in-depth cleanup probe. Returns:

    * ``True`` when the canonical ref still resolves.
    * ``False`` when storage definitively says it does not exist.
    * ``None`` when the lookup raises (transient infra failure — treat
      as "ref intact" so we don't drop data).
    """
    if entity.startswith(_MEMORY_PREFIX):
        memory_id = entity[len(_MEMORY_PREFIX):]
        try:
            row = await storage.get_memory_row(memory_id)
        except Exception:  # noqa: BLE001 — transient
            return None
        return row is not None
    # ``<ds>[.<model>[.<leaf>]]`` shape. Datasource alone is rooted at
    # ``ds``; deeper paths probe the parent model.
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
) -> list[tuple[str, str]]:
    """Refresh embeddings for every memory whose canonical entities are
    rooted at this datasource. Each warning is tagged with
    ``memory:<id>`` so a startup log inspection can distinguish memory
    failures from datasource-doc / model failures at a glance.

    DEV-1428 defense-in-depth: also strip stale refs from every memory
    rooted at this datasource (refs that resolve to a definitive "not
    found"; transient lookup failures keep the ref intact). For memories
    with ``Memory.query`` set, emit an ``IngestionError`` when the query
    has stale references — the query itself is NOT rewritten.

    A memory linked to entities in datasources A and B is touched in
    both passes; hash-skip inside ``_apply_pending`` makes the second
    call a no-op.
    """
    try:
        memories = await storage.list_memories()
    except Exception as exc:  # noqa: BLE001 — defensive
        return [("", f"{datasource_name} (memories): {exc}")]
    warnings: list[tuple[str, str]] = []
    for memory in memories:
        rooted_at_ds = any(
            canonical_id_rooted_at(e, datasource_name)
            for e in memory.entities
        )
        # DEV-1428: ``memory:<id>`` refs are datasource-agnostic. A
        # memory carrying only such refs would otherwise never be
        # touched by any per-datasource pass and could accumulate stale
        # entries forever. Include those in the cleanup walk; the
        # embedding refresh remains datasource-rooted so we don't
        # re-embed every memory on every pass.
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
        # DEV-1428 cleanup pass: drop refs that resolve to False
        # (definitive not-found); keep refs that raise (transient).
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
        # DEV-1428: stale Memory.query warning.
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
    *, datasource_name: str, storage: StorageBackend,
) -> list[tuple[str, str]]:
    """Refresh persisted embeddings for everything reachable from this
    datasource: every visible model + its visible children, the
    datasource doc itself, and every memory whose canonical entities
    are rooted at the datasource.

    Best-effort: returns ``(model_name, error_text)`` tuples; never
    raises. ``model_name`` is the canonical entity tag
    (``<ds>.<model>``, ``memory:<id>``, or ``""`` for the datasource
    doc) used by ``ingest_datasource_idempotent`` to route per-entity
    failures to the matching ``IngestionError``.
    """
    # Local import: keep the search module off the cold-start path
    # when the optional embedding extra isn't installed.
    from slayer.search.service import SearchService

    search = SearchService(storage=storage)
    model_warnings, models_in_ds = await _refresh_models_for_datasource(
        datasource_name=datasource_name, storage=storage, search=search,
    )
    doc_warnings = await _refresh_datasource_doc(
        datasource_name=datasource_name,
        models=models_in_ds,
        search=search,
        storage=storage,
    )
    memory_warnings = await _refresh_memories_for_datasource(
        datasource_name=datasource_name, storage=storage, search=search,
    )
    return model_warnings + doc_warnings + memory_warnings
