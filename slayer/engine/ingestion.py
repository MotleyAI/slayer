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
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, TextIO, Tuple

import sqlalchemy as sa
from pydantic import BaseModel, Field

from slayer.core.enums import DataType
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.engine.profiling import refresh_all_table_backed_sampled
from slayer.engine.query_engine import SlayerQueryEngine
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
_logged_unmapped_sa_types: Set[str] = set()

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
    # Temporal
    "TIMESTAMP": DataType.TIMESTAMP,
    "DATETIME": DataType.TIMESTAMP,
    "TIMESTAMP WITHOUT TIME ZONE": DataType.TIMESTAMP,
    "TIMESTAMP WITH TIME ZONE": DataType.TIMESTAMP,
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
    }
)

# ClickHouse SA wrapper class names — peeled before type lookup.
# clickhouse-sqlalchemy exposes the inner type via .nested_type on both.
_CLICKHOUSE_WRAPPER_NAMES = frozenset({"NULLABLE", "LOWCARDINALITY"})
_CLICKHOUSE_WRAPPER_MAX_DEPTH = 8

# NUMERIC/DECIMAL type names — float-like only when scale > 0
_NUMERIC_DECIMAL_TYPES = frozenset({"NUMERIC", "DECIMAL"})

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
    type_name = type(sa_type).__name__.upper()
    type_str = str(sa_type).split("(")[0].upper().strip()
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
            "DataType.TEXT. Consider adding to _SA_TYPE_MAP.",
            type_name,
            str(sa_type),
        )
    return DataType.TEXT


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


def _get_fk_relationships(
    inspector: sa.engine.Inspector,
    table_name: str,
    schema: Optional[str],
    table_set: Set[str],
) -> List[tuple]:
    """Get FK relationships for a table, filtered to tables in table_set.

    Returns list of (source_column, target_table, target_column).
    """
    fks = inspector.get_foreign_keys(table_name, schema=schema)
    result = []
    for fk in fks:
        referred_table = fk["referred_table"]
        if referred_table not in table_set or referred_table == table_name:
            continue
        constrained = fk["constrained_columns"]
        referred = fk["referred_columns"]
        for src_col, tgt_col in zip(constrained, referred):
            result.append((src_col, referred_table, tgt_col))
    return result


def _build_fk_graph(
    inspector: sa.engine.Inspector,
    table_names: List[str],
    schema: Optional[str],
) -> Dict[str, Set[str]]:
    """Build directed graph: graph[table] = set of tables it references via FK."""
    table_set = set(table_names)
    graph: Dict[str, Set[str]] = defaultdict(set)
    for table_name in table_names:
        for _, ref_table, _ in _get_fk_relationships(
            inspector=inspector,
            table_name=table_name,
            schema=schema,
            table_set=table_set,
        ):
            graph[table_name].add(ref_table)
    return dict(graph)


def _check_acyclic(graph: Dict[str, Set[str]]) -> None:
    """Check that FK graph is a DAG. Raises RollupGraphError if cycles found."""
    visited: Set[str] = set()
    rec_stack: Set[str] = set()

    def dfs(node: str, path: List[str]) -> None:
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

    all_nodes: Set[str] = set(graph.keys())
    for neighbors in graph.values():
        all_nodes.update(neighbors)
    for node in all_nodes:
        if node not in visited:
            dfs(node, [])


def _compute_transitive_closure(graph: Dict[str, Set[str]], source: str) -> Set[str]:
    """BFS to find all tables transitively reachable from source (excluding source)."""
    reachable: Set[str] = set()
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


def _generate_joins(
    inspector: sa.engine.Inspector,
    source_table: str,
    referenced_tables: Set[str],
    schema: Optional[str],
    table_set: Set[str],
) -> List[ModelJoin]:
    """Generate direct ModelJoin objects from the source table's own FK relationships.

    Only emits joins for FKs defined on ``source_table`` itself — multi-hop
    reachability (e.g. orders → customers → regions) is resolved at query time
    by walking the join graph through each intermediate model.
    """
    fk_rels = _get_fk_relationships(
        inspector=inspector,
        table_name=source_table,
        schema=schema,
        table_set=table_set,
    )

    joins = []
    seen_signatures: Set[Tuple[str, str, str]] = set()
    for src_col, ref_table, tgt_col in fk_rels:
        if ref_table not in referenced_tables:
            continue
        signature = (ref_table, src_col, tgt_col)
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        joins.append(
            ModelJoin(
                target_model=ref_table,
                join_pairs=[[src_col, tgt_col]],
            )
        )

    return joins


# ---------------------------------------------------------------------------
# INFORMATION_SCHEMA fallbacks (for databases like DuckDB where
# the SQLAlchemy Inspector's pg_catalog queries may not be supported)
# ---------------------------------------------------------------------------


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


def _get_pk_constraint_fallback(
    sa_engine: sa.Engine,
    table_name: str,
    schema: Optional[str],
) -> Dict:
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


def _safe_get_pk_constraint(
    inspector: sa.engine.Inspector,
    sa_engine: sa.Engine,
    table_name: str,
    schema: Optional[str],
) -> Dict:
    """Get PK constraint, falling back to INFORMATION_SCHEMA on failure.

    SQLite has no information_schema views; its stock inspector reads
    PRAGMA table_info() and is authoritative — empty constrained_columns
    on SQLite means the table genuinely has no primary key.
    """
    if sa_engine.dialect.name == "sqlite":
        try:
            return inspector.get_pk_constraint(table_name, schema=schema)
        except Exception:
            return {"constrained_columns": []}
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
    schema: Optional[str],
    rollup_sql: Optional[str],
    referenced_tables: Set[str],
    fk_columns_by_table: Dict[str, Set[str]],
    joins: Optional[List[ModelJoin]] = None,
) -> List[tuple]:
    """Introspect columns from a rollup query or plain table.

    Returns list of (column_name, DataType, is_primary_key, is_float) tuples.
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
        if isinstance(col_type, DataType):
            data_type = col_type
            is_float = col.get("is_float", False)
        else:
            data_type = _sa_type_to_data_type(col_type)
            is_float = _sa_type_is_float(col_type)
        is_pk = col_name in pk_columns
        results.append((col_name, data_type, is_pk, is_float))

    # Build list of (ref_table, dotted_path) from joins — supports diamond joins
    # where the same table appears via multiple paths
    table_path_pairs: List[tuple] = []
    if joins:
        for mj in joins:
            if mj.join_pairs and "." in mj.join_pairs[0][0]:
                prefix = mj.join_pairs[0][0].split(".")[0]
                path = f"{prefix}.{mj.target_model}"
            else:
                path = mj.target_model
            table_path_pairs.append((mj.target_model, path))
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
            if isinstance(col_type, DataType):
                data_type = col_type
                is_float = col.get("is_float", False)
            else:
                data_type = _sa_type_to_data_type(col_type)
                is_float = _sa_type_is_float(col_type)
            is_pk = col["name"] in ref_pk_cols
            results.append((alias, data_type, is_pk, is_float))

    return results


# ---------------------------------------------------------------------------
# Model generation from introspected columns
# ---------------------------------------------------------------------------


def _columns_to_model(
    name: str,
    columns: List[tuple],
    data_source: str,
    sql_table: Optional[str] = None,
    joins: Optional[List[ModelJoin]] = None,
) -> SlayerModel:
    """Generate a SlayerModel from introspected (column_name, DataType, is_pk, is_float) tuples.

    In v2 every Column is potentially both a dimension and a measure — what it's
    used as is decided per query. This function emits one Column per non-joined
    column, with format inferred from the column's data type.
    """
    cols: List[Column] = []

    _INT_FORMAT = NumberFormat(type=NumberFormatType.INTEGER)
    _FLOAT_FORMAT = NumberFormat(type=NumberFormatType.FLOAT)

    for col_name, data_type, is_pk, is_float in columns:
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
                primary_key=is_pk,
                format=fmt,
            )
        )

    return SlayerModel(
        name=name,
        sql_table=sql_table,
        data_source=data_source,
        columns=cols,
        joins=joins or [],
    )


def introspect_table_to_model(
    *,
    sa_engine: sa.Engine,
    inspector: sa.engine.Inspector,
    table_name: str,
    schema: Optional[str],
    data_source: str,
    model_name: Optional[str] = None,
) -> SlayerModel:
    """Introspect a single table (no FK rollup) and return a SlayerModel.

    This is the building block shared between the auto-ingest path and the
    dbt hidden-model import. It never builds joins or traverses the FK graph.
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
    return _columns_to_model(
        name=model_name or table_name,
        columns=columns,
        data_source=data_source,
        sql_table=sql_table,
    )


# ---------------------------------------------------------------------------
# Main ingestion
# ---------------------------------------------------------------------------


def ingest_datasource(
    datasource: DatasourceConfig,
    include_tables: Optional[List[str]] = None,
    exclude_tables: Optional[List[str]] = None,
    schema: Optional[str] = None,
) -> List[SlayerModel]:
    sa_engine = sa.create_engine(datasource.resolve_env_vars().get_connection_string())
    inspector = sa.inspect(sa_engine)

    table_names = inspector.get_table_names(schema=schema)
    if include_tables:
        table_names = [t for t in table_names if t in include_tables]
    if exclude_tables:
        table_names = [t for t in table_names if t not in exclude_tables]

    table_set = set(table_names)

    # Build FK graph, check for cycles
    fk_graph = _build_fk_graph(inspector=inspector, table_names=table_names, schema=schema)
    has_cycles = False
    try:
        _check_acyclic(fk_graph)
    except RollupGraphError as e:
        logger.warning(f"FK graph has cycles, skipping rollup: {e}")
        has_cycles = True

    # Collect FK columns per table (for excluding from rollup)
    fk_columns_by_table: Dict[str, Set[str]] = defaultdict(set)
    for table_name in table_names:
        fks = inspector.get_foreign_keys(table_name, schema=schema)
        for fk in fks:
            for col in fk["constrained_columns"]:
                fk_columns_by_table[table_name].add(col)

    models = []
    for table_name in table_names:
        referenced = set() if has_cycles else _compute_transitive_closure(fk_graph, table_name)
        sql_table = f"{schema}.{table_name}" if schema else table_name

        if referenced:
            # Build explicit joins and introspect columns
            model_joins = _generate_joins(
                inspector=inspector,
                source_table=table_name,
                referenced_tables=referenced,
                schema=schema,
                table_set=table_set,
            )
            columns = _introspect_query_columns_via_inspector(
                sa_engine=sa_engine,
                inspector=inspector,
                table_name=table_name,
                schema=schema,
                rollup_sql=None,
                referenced_tables=referenced,
                fk_columns_by_table=fk_columns_by_table,
                joins=model_joins,
            )
            model = _columns_to_model(
                name=table_name,
                columns=columns,
                data_source=datasource.name,
                sql_table=sql_table,
                joins=model_joins,
            )
        else:
            # Simple table — introspect directly
            columns = _introspect_query_columns_via_inspector(
                sa_engine=sa_engine,
                inspector=inspector,
                table_name=table_name,
                schema=schema,
                rollup_sql=None,
                referenced_tables=set(),
                fk_columns_by_table=fk_columns_by_table,
            )
            model = _columns_to_model(
                name=table_name,
                columns=columns,
                data_source=datasource.name,
                sql_table=sql_table,
            )

        models.append(model)

    sa_engine.dispose()
    return models


# ---------------------------------------------------------------------------
# Idempotent re-ingestion (DEV-1356)
# ---------------------------------------------------------------------------


def _existing_join_signatures(model: SlayerModel) -> Set[Tuple[str, Tuple[Tuple[str, str], ...]]]:
    """Return the set of (target_model, sorted join_pair tuples) signatures
    for joins already on ``model``. Used to detect new joins.
    """
    out: Set[Tuple[str, Tuple[Tuple[str, str], ...]]] = set()
    for j in model.joins:
        sig_pairs = tuple(sorted((p[0], p[1]) for p in j.join_pairs))
        out.add((j.target_model, sig_pairs))
    return out


def _additive_merge_existing(
    *,
    persisted: SlayerModel,
    fresh: SlayerModel,
) -> Tuple[SlayerModel, List[str], List[str]]:
    """Merge a freshly-ingested ``fresh`` model into ``persisted`` additively.

    Returns ``(merged, new_column_names, new_join_target_names)``.

    * Existing columns are preserved verbatim (description / label / format /
      meta / allowed_aggregations / filter never overwritten).
    * Live columns whose names are absent from ``persisted.columns`` are
      appended from ``fresh.columns``.
    * Joins with new ``(target_model, join_pairs)`` signatures are appended.
    """
    existing_col_names = {c.name for c in persisted.columns}
    new_columns: List[Column] = list(persisted.columns)
    new_column_names: List[str] = []
    for c in fresh.columns:
        if c.name in existing_col_names:
            continue
        new_columns.append(c)
        new_column_names.append(c.name)

    existing_join_sigs = _existing_join_signatures(persisted)
    existing_join_targets = {j.target_model for j in persisted.joins}
    new_joins: List[ModelJoin] = list(persisted.joins)
    new_join_targets: List[str] = []
    for j in fresh.joins:
        sig = (j.target_model, tuple(sorted((p[0], p[1]) for p in j.join_pairs)))
        if sig in existing_join_sigs:
            continue
        if j.target_model in existing_join_targets:
            # Same target_model already present with a different
            # join_pairs signature. Downstream consumers key joins by
            # target_model only — appending a second one would let the
            # stale join shadow the live one and ``remove.joins=[name]``
            # would wipe both. Surface the conflict so the user can
            # decide instead of silently breaking.
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

    if not new_column_names and not new_join_targets:
        return persisted, [], []

    merged = persisted.model_copy(
        update={"columns": new_columns, "joins": new_joins}
    )
    return merged, new_column_names, new_join_targets


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
        )
    if persisted.sql or persisted.source_queries:
        # User-authored sql / query-backed model with the matching name —
        # leave it alone.
        return None
    merged, new_cols, new_joins = _additive_merge_existing(
        persisted=persisted, fresh=fresh
    )
    if new_cols or new_joins:
        await storage.save_model(merged)
    return ModelAddition(
        model_name=table_name,
        data_source=datasource.name,
        created=False,
        new_columns=new_cols,
        new_joins=new_joins,
    )


def _bare_table_name(sql_table: str) -> str:
    """Strip an optional schema prefix from a ``schema.table`` reference."""
    return sql_table.split(".", 1)[1] if "." in sql_table else sql_table


async def _scoped_models_for_validation(
    *,
    storage: StorageBackend,
    datasource: DatasourceConfig,
    in_scope_table_names: Set[str],
) -> List[SlayerModel]:
    """Build the list of persisted models to feed to ``validate_datasource``.

    sql_table-mode models are included only when their live table is in
    scope (matches the additive pass). sql-mode and query-backed models are
    always validated within this datasource — they're not tied to a
    specific live table name.
    """
    identities = await storage._list_all_model_identities()
    ds_model_names = [n for d, n in identities if d == datasource.name]
    scoped: List[SlayerModel] = []
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


async def ingest_datasource_idempotent(
    *,
    datasource: DatasourceConfig,
    storage: StorageBackend,
    include_tables: Optional[List[str]] = None,
    exclude_tables: Optional[List[str]] = None,
    schema: Optional[str] = None,
):
    """Idempotent re-ingestion (DEV-1356).

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

    additions: List[ModelAddition] = []
    errors: List[IngestionError] = []

    # ``ingest_datasource`` is sync (it drives SQLAlchemy ``Inspector``).
    # Offload to a thread so a slow / large datasource doesn't block the
    # event loop while server-facing requests are in flight.
    fresh_models = await asyncio.to_thread(
        ingest_datasource,
        datasource=datasource,
        include_tables=include_tables,
        exclude_tables=exclude_tables,
        schema=schema,
    )
    fresh_by_name = {m.name: m for m in fresh_models}
    in_scope_table_names: Set[str] = set(fresh_by_name.keys())

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

    # DEV-1375: refresh persisted Column.sampled values for every
    # table-backed model in this datasource. Best-effort: per-column
    # failures are accumulated as IngestionError entries; an unexpected
    # raise is also caught so ingestion's idempotent contract holds.
    refresh_engine = SlayerQueryEngine(storage=storage)
    try:
        refresh_errors = await refresh_all_table_backed_sampled(
            engine=refresh_engine,
            storage=storage,
            data_source=datasource.name,
        )
    except Exception as exc:
        refresh_errors = [f"{datasource.name}: {exc}"]
    for err in refresh_errors:
        errors.append(IngestionError(
            model_name=err.split(".", 1)[0] if "." in err else "",
            data_source=datasource.name,
            error=f"sample-value refresh: {err}",
        ))

    # DEV-1386: refresh persisted embeddings for the datasource doc plus
    # every visible model + its visible children. Best-effort: per-entity
    # failures are surfaced as IngestionError entries, never aborts
    # ingestion. When the `embedding_search` extra is not installed,
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

    return IdempotentIngestResult(
        additions=additions,
        to_delete=list(to_delete),
        errors=errors,
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


def _print_ingest_addition(
    addition, *, file: Optional[TextIO] = None
) -> None:
    out = file if file is not None else sys.stdout
    if addition.created:
        print(
            f"Created: {addition.model_name} ({len(addition.new_columns)} columns)",
            file=out,
        )
        return
    if not (addition.new_columns or addition.new_joins):
        return
    details = []
    if addition.new_columns:
        details.append(f"+columns: {', '.join(addition.new_columns)}")
    if addition.new_joins:
        details.append(f"+joins: {', '.join(addition.new_joins)}")
    print(f"Updated: {addition.model_name} ({'; '.join(details)})", file=out)


def _print_ingest_drift_and_errors(
    result, *, file: Optional[TextIO] = None
) -> None:
    out = file if file is not None else sys.stdout
    if result.to_delete:
        print("\nPending drift (run `slayer validate-models` to inspect):", file=out)
        for entry in result.to_delete:
            print(f"  - {entry.tool}: {entry.model_name}", file=out)
    if result.errors:
        print(f"\nErrors ({len(result.errors)}):", file=out)
        for err in result.errors:
            print(f"  - {err.model_name}: {err.error}", file=out)


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

    succeeded: List[str] = Field(default_factory=list)
    failures: List[StartupIngestFailure] = Field(default_factory=list)
    drift_pending: List[Any] = Field(default_factory=list)


async def ingest_all_datasources_idempotent(
    *,
    storage: StorageBackend,
    stream: Optional[TextIO] = None,
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
        _print_ingest_drift_and_errors(result, file=out)
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
) -> Tuple[List[Tuple[str, str]], List[SlayerModel]]:
    """Refresh embeddings for every visible model in the datasource.

    Returns ``(warnings, models_in_ds)``. Each warning is tagged with
    the model's ``<ds>.<name>`` so the orchestrator can route it to the
    right ``IngestionError.model_name``. ``models_in_ds`` is forwarded
    to the datasource-doc refresh that follows.
    """
    warnings: List[Tuple[str, str]] = []
    models_in_ds: List[SlayerModel] = []
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
    models: List[SlayerModel],
    search: "SearchService",
) -> List[Tuple[str, str]]:
    """Refresh the datasource doc embedding. Warnings are tagged with
    an empty ``model_name`` since the doc has no specific entity name."""
    try:
        doc_warnings = await search.refresh_datasource(
            name=datasource_name, models=models,
        )
    except Exception as exc:  # noqa: BLE001 — defensive
        return [("", f"{datasource_name} (datasource doc): {exc}")]
    return [("", w) for w in doc_warnings]


async def _entity_ref_exists(
    *, entity: str, storage: StorageBackend,
) -> Optional[bool]:
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
) -> List[Tuple[str, str]]:
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
    warnings: List[Tuple[str, str]] = []
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
        cleaned: List[str] = []
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
) -> List[Tuple[str, str]]:
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
        datasource_name=datasource_name, models=models_in_ds, search=search,
    )
    memory_warnings = await _refresh_memories_for_datasource(
        datasource_name=datasource_name, storage=storage, search=search,
    )
    return model_warnings + doc_warnings + memory_warnings
