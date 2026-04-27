"""Auto-ingestion: introspect a database and generate SlayerModels with rollup-style joins.

Flow:
1. Get table names, build FK graph, check for cycles
2. For each table, build rollup SQL (with LEFT JOINs for referenced tables)
3. Introspect the rollup query's result columns for types
4. Generate dimensions and measures from those columns
"""

import logging
from collections import defaultdict, deque
from typing import Dict, List, Optional, Set, Tuple

import sqlalchemy as sa

from slayer.core.enums import DataType
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import DatasourceConfig, Dimension, Measure, ModelJoin, SlayerModel

logger = logging.getLogger(__name__)

# Map SQLAlchemy types to SLayer DataTypes
_SA_TYPE_MAP = {
    "INTEGER": DataType.NUMBER,
    "BIGINT": DataType.NUMBER,
    "SMALLINT": DataType.NUMBER,
    "FLOAT": DataType.NUMBER,
    "REAL": DataType.NUMBER,
    "DOUBLE": DataType.NUMBER,
    "DOUBLE_PRECISION": DataType.NUMBER,
    "NUMERIC": DataType.NUMBER,
    "DECIMAL": DataType.NUMBER,
    "VARCHAR": DataType.STRING,
    "CHAR": DataType.STRING,
    "TEXT": DataType.STRING,
    "STRING": DataType.STRING,
    "BOOLEAN": DataType.BOOLEAN,
    "BOOL": DataType.BOOLEAN,
    "TIMESTAMP": DataType.TIMESTAMP,
    "DATETIME": DataType.TIMESTAMP,
    "TIMESTAMP WITHOUT TIME ZONE": DataType.TIMESTAMP,
    "TIMESTAMP WITH TIME ZONE": DataType.TIMESTAMP,
    "DATE": DataType.DATE,
    "TIME": DataType.TIMESTAMP,
    "SERIAL": DataType.NUMBER,
    "BIGSERIAL": DataType.NUMBER,
}

_NUMERIC_TYPES = {DataType.NUMBER}
_ID_SUFFIXES = ("_id", "_key", "_pk", "_fk")

# Float-like SA type names — these columns get measures only, no dimensions.
# NUMERIC/DECIMAL are handled separately via scale inspection in _sa_type_is_float.
_FLOAT_LIKE_SA_TYPES = frozenset(
    {
        "FLOAT",
        "REAL",
        "DOUBLE",
        "DOUBLE_PRECISION",
    }
)

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

# Map INFORMATION_SCHEMA type names to SLayer DataTypes (for DuckDB fallback)
_INFO_SCHEMA_TYPE_MAP = {
    "INTEGER": DataType.NUMBER,
    "BIGINT": DataType.NUMBER,
    "SMALLINT": DataType.NUMBER,
    "TINYINT": DataType.NUMBER,
    "HUGEINT": DataType.NUMBER,
    "FLOAT": DataType.NUMBER,
    "DOUBLE": DataType.NUMBER,
    "REAL": DataType.NUMBER,
    "VARCHAR": DataType.STRING,
    "CHAR": DataType.STRING,
    "TEXT": DataType.STRING,
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


def _sa_type_to_data_type(sa_type: sa.types.TypeEngine) -> DataType:
    type_name = type(sa_type).__name__.upper()
    if type_name in _SA_TYPE_MAP:
        return _SA_TYPE_MAP[type_name]
    type_str = str(sa_type).split("(")[0].upper().strip()
    if type_str in _SA_TYPE_MAP:
        return _SA_TYPE_MAP[type_str]
    return DataType.STRING


def _sa_type_is_float(sa_type: sa.types.TypeEngine) -> bool:
    """Return True if the SQLAlchemy type is float-like.

    FLOAT/REAL/DOUBLE are always float-like. NUMERIC/DECIMAL are float-like
    only when their scale is > 0 (or unknown), so NUMERIC(10,0) is treated as
    integer-like.
    """
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
            sa_type = sa_type or DataType.NUMBER
            is_float = _parse_info_schema_is_float(data_type_str)
        elif sa_type is None and "INT" in base_type:
            sa_type = DataType.NUMBER
        elif sa_type is None and ("CHAR" in base_type or "TEXT" in base_type):
            sa_type = DataType.STRING
        result.append({"name": col_name, "type": sa_type or DataType.STRING, "is_float": is_float})
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
    """Generate a SlayerModel from introspected (column_name, DataType, is_pk, is_float) tuples."""
    dimensions = []
    measures = []
    numeric_columns: List[tuple] = []
    non_numeric_columns: List[str] = []

    _INT_FORMAT = NumberFormat(type=NumberFormatType.INTEGER)
    _FLOAT_FORMAT = NumberFormat(type=NumberFormatType.FLOAT)

    for col_name, data_type, is_pk, is_float in columns:
        # Skip joined columns — their dimensions/measures live on the target
        # model and are resolved via the join graph at query time.
        if "." in col_name:
            continue

        # Float-like columns get measures only, no dimension
        if not is_float:
            dim_format = _INT_FORMAT if (data_type in _NUMERIC_TYPES) else None
            dimensions.append(
                Dimension(
                    name=col_name,
                    sql=col_name,
                    type=data_type,
                    primary_key=is_pk,
                    format=dim_format,
                )
            )

        if is_pk or _is_id_column(col_name):
            continue
        if data_type in _NUMERIC_TYPES:
            numeric_columns.append((col_name, is_float))
        else:
            non_numeric_columns.append(col_name)

    # One measure per non-ID column. Aggregation is specified at query time
    # using colon syntax (e.g., "revenue:sum", "customer_id:count_distinct").
    # *:count is always available for COUNT(*) without any measure definition.
    for col_name, is_float in numeric_columns:
        measure_name = "count_col" if col_name == "_count" else col_name
        measure_format = _FLOAT_FORMAT if is_float else _INT_FORMAT
        measures.append(Measure(name=measure_name, sql=col_name, format=measure_format))

    for col_name in non_numeric_columns:
        measure_name = "count_col" if col_name == "_count" else col_name
        measures.append(Measure(name=measure_name, sql=col_name))

    return SlayerModel(
        name=name,
        sql_table=sql_table,
        data_source=data_source,
        dimensions=dimensions,
        measures=measures,
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
