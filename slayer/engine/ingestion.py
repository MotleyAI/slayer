"""Auto-ingestion: introspect a database and generate SlayerModels with rollup-style joins.

Flow:
1. Get table names, build FK graph, check for cycles
2. For each table, build rollup SQL (with LEFT JOINs for referenced tables)
3. Introspect the rollup query's result columns for types
4. Generate dimensions and measures from those columns
"""

import logging
from collections import defaultdict, deque
from typing import Dict, List, Optional, Set

import sqlalchemy as sa

from slayer.core.enums import DataType
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
            inspector=inspector, table_name=table_name, schema=schema, table_set=table_set,
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
                raise RollupGraphError(
                    f"Foreign key graph contains a cycle: {' -> '.join(cycle)}"
                )
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
    """Generate ModelJoin objects from FK relationships via BFS traversal.

    Supports diamond joins: the same table can be reached via multiple paths
    (e.g., orders → customers → regions AND orders → warehouses → regions).
    Each path produces a separate ModelJoin with path-qualified source columns.
    """
    joins = []
    # Track (referencing_table, target_table) edges already processed
    processed_edges: Set[tuple] = set()
    # BFS queue entries: table name
    queue = deque([source_table])
    visited_for_expansion = {source_table}

    while queue:
        current = queue.popleft()
        current_fk_rels = _get_fk_relationships(
            inspector=inspector, table_name=current, schema=schema, table_set=table_set,
        )

        for src_col, ref_table, tgt_col in current_fk_rels:
            if ref_table not in referenced_tables:
                continue
            edge = (current, ref_table)
            if edge in processed_edges:
                continue
            processed_edges.add(edge)

            # Build join pair: qualify source column with table name for non-root
            if current == source_table:
                source_dim = src_col
            else:
                source_dim = f"{current}.{src_col}"
            join_pairs = [[source_dim, tgt_col]]

            joins.append(ModelJoin(
                target_model=ref_table,
                join_pairs=join_pairs,
            ))

            # Continue BFS from the referenced table (but only expand once)
            if ref_table not in visited_for_expansion:
                visited_for_expansion.add(ref_table)
                queue.append(ref_table)

    return joins


# ---------------------------------------------------------------------------
# INFORMATION_SCHEMA fallbacks (for databases like DuckDB where
# the SQLAlchemy Inspector's pg_catalog queries may not be supported)
# ---------------------------------------------------------------------------

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
        if sa_type is None and "INT" in base_type:
            sa_type = DataType.NUMBER
        elif sa_type is None and ("CHAR" in base_type or "TEXT" in base_type):
            sa_type = DataType.STRING
        elif sa_type is None and ("DECIMAL" in base_type or "NUMERIC" in base_type):
            sa_type = DataType.NUMBER
        result.append({"name": col_name, "type": sa_type or DataType.STRING})
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
    """Get PK constraint, falling back to INFORMATION_SCHEMA on failure."""
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

    Returns list of (column_name, DataType, is_primary_key) tuples.
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
        data_type = col_type if isinstance(col_type, DataType) else _sa_type_to_data_type(col_type)
        is_pk = col_name in pk_columns
        results.append((col_name, data_type, is_pk))

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
            data_type = col_type if isinstance(col_type, DataType) else _sa_type_to_data_type(col_type)
            is_pk = col["name"] in ref_pk_cols
            results.append((alias, data_type, is_pk))

    return results


# ---------------------------------------------------------------------------
# Model generation from introspected columns
# ---------------------------------------------------------------------------

def _columns_to_model(
    name: str,
    columns: List[tuple],
    data_source: str,
    sql_table: Optional[str] = None,
    source_table_pk_columns: Optional[Set[str]] = None,
    joins: Optional[List[ModelJoin]] = None,
) -> SlayerModel:
    """Generate a SlayerModel from introspected (column_name, DataType, is_pk) tuples."""
    dimensions = []
    measures = []
    numeric_columns = []

    non_numeric_columns = []

    for col_name, data_type, is_pk in columns:
        # For joined columns (path.col), sql uses path-based alias:
        # "customers.name" → sql="customers.name" (table alias = path with __ )
        # "customers.regions.name" → sql="customers__regions.name"
        is_joined = "." in col_name
        if is_joined:
            parts = col_name.split(".")
            raw_col = parts[-1]
            path = parts[:-1]  # e.g., ["customers", "regions"]
            table_alias = "__".join(path)  # e.g., "customers__regions"
            sql_expr = f"{table_alias}.{raw_col}"
        else:
            raw_col = col_name
            sql_expr = col_name
            path = None

        dimensions.append(Dimension(
            name=col_name,
            sql=sql_expr,
            type=data_type,
            primary_key=is_pk and not is_joined,
            description=f"From {'.'.join(path)}" if path else None,
        ))

        if is_pk or _is_id_column(raw_col):
            continue
        if data_type in _NUMERIC_TYPES:
            numeric_columns.append(col_name)
        else:
            non_numeric_columns.append(col_name)

    # Add COUNT measure
    measures.append(Measure(name="count", type=DataType.COUNT))

    # Add SUM, AVG, MIN, MAX, COUNT_DISTINCT for numeric non-ID columns
    for col_name in numeric_columns:
        is_joined = "." in col_name
        if is_joined:
            parts = col_name.split(".")
            table_alias = "__".join(parts[:-1])
            sql_expr = f"{table_alias}.{parts[-1]}"
        else:
            sql_expr = col_name
        measures.append(Measure(name=f"{col_name}_sum", sql=sql_expr, type=DataType.SUM))
        measures.append(Measure(name=f"{col_name}_avg", sql=sql_expr, type=DataType.AVERAGE))
        measures.append(Measure(name=f"{col_name}_min", sql=sql_expr, type=DataType.MIN))
        measures.append(Measure(name=f"{col_name}_max", sql=sql_expr, type=DataType.MAX))
        measures.append(Measure(name=f"{col_name}_distinct", sql=sql_expr, type=DataType.COUNT_DISTINCT))

    # Add COUNT_DISTINCT and COUNT (non-null) for non-numeric non-ID columns
    for col_name in non_numeric_columns:
        is_joined = "." in col_name
        if is_joined:
            parts = col_name.split(".")
            table_alias = "__".join(parts[:-1])
            sql_expr = f"{table_alias}.{parts[-1]}"
        else:
            sql_expr = col_name
        measures.append(Measure(name=f"{col_name}_distinct", sql=sql_expr, type=DataType.COUNT_DISTINCT))
        measures.append(Measure(name=f"{col_name}_count", sql=sql_expr, type=DataType.COUNT))

    # Add count_distinct for joined table PKs
    seen_tables = set()
    for col_name, data_type, is_pk in columns:
        if "." in col_name and is_pk:
            ref_table = col_name.rsplit(".", 1)[0]
            if ref_table not in seen_tables:
                seen_tables.add(ref_table)
                parts = col_name.split(".")
                table_alias = "__".join(parts[:-1])
                raw_col = parts[-1]
                measures.append(Measure(
                    name=f"{ref_table}.count",
                    sql=raw_col,
                    type=DataType.COUNT_DISTINCT,
                    description=f"Distinct count of {ref_table}",
                ))

    return SlayerModel(
        name=name,
        sql_table=sql_table,
        data_source=data_source,
        dimensions=dimensions,
        measures=measures,
        joins=joins or [],
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
                inspector=inspector, source_table=table_name,
                referenced_tables=referenced, schema=schema, table_set=table_set,
            )
            columns = _introspect_query_columns_via_inspector(
                sa_engine=sa_engine, inspector=inspector,
                table_name=table_name, schema=schema,
                rollup_sql=None, referenced_tables=referenced,
                fk_columns_by_table=fk_columns_by_table,
                joins=model_joins,
            )
            model = _columns_to_model(
                name=table_name, columns=columns,
                data_source=datasource.name, sql_table=sql_table,
                joins=model_joins,
            )
        else:
            # Simple table — introspect directly
            columns = _introspect_query_columns_via_inspector(
                sa_engine=sa_engine, inspector=inspector,
                table_name=table_name, schema=schema,
                rollup_sql=None, referenced_tables=set(),
                fk_columns_by_table=fk_columns_by_table,
            )
            model = _columns_to_model(
                name=table_name, columns=columns,
                data_source=datasource.name, sql_table=sql_table,
            )

        models.append(model)

    sa_engine.dispose()
    return models
