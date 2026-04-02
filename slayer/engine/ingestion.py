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
    """Generate ModelJoin objects from FK relationships via BFS traversal."""
    joins = []
    processed = {source_table}
    queue = deque([source_table])

    # Build reverse FK mapping (who references whom)
    reverse_refs: Dict[str, List[tuple]] = defaultdict(list)
    all_tables = {source_table} | referenced_tables
    for table in all_tables:
        for src_col, ref_table, tgt_col in _get_fk_relationships(
            inspector=inspector, table_name=table, schema=schema, table_set=table_set,
        ):
            if ref_table in all_tables:
                reverse_refs[ref_table].append((table, src_col, tgt_col))

    while queue:
        current = queue.popleft()
        current_fk_rels = _get_fk_relationships(
            inspector=inspector, table_name=current, schema=schema, table_set=table_set,
        )
        next_tables = {ref_table for _, ref_table, _ in current_fk_rels
                       if ref_table in referenced_tables and ref_table not in processed}

        for ref_table in next_tables:
            # Find the join condition: which processed table references this ref_table
            for referencing_table, fk_col, tgt_col in reverse_refs[ref_table]:
                if referencing_table in processed:
                    # For transitive joins, the source dim needs to be qualified
                    # with the referencing table if it's not the source table
                    if referencing_table == source_table:
                        source_dim = fk_col
                    else:
                        source_dim = f"{referencing_table}__{fk_col}"
                    joins.append(ModelJoin(
                        target_model=ref_table,
                        join_pairs=[[source_dim, tgt_col]],
                    ))
                    processed.add(ref_table)
                    queue.append(ref_table)
                    break

    return joins


# ---------------------------------------------------------------------------
# Rollup SQL generation (legacy — still used for backward compatibility)
# ---------------------------------------------------------------------------

def _generate_rollup_sql(
    inspector: sa.engine.Inspector,
    source_table: str,
    referenced_tables: Set[str],
    schema: Optional[str],
    table_set: Set[str],
) -> str:
    """Generate SELECT ... FROM source LEFT JOIN ref1 ON ... LEFT JOIN ref2 ON ... SQL."""
    sql_table = f"{schema}.{source_table}" if schema else source_table

    # Collect source columns
    source_cols = inspector.get_columns(source_table, schema=schema)
    select_parts = [f"{sql_table}.{col['name']} AS {col['name']}" for col in source_cols]

    # Build reverse FK mapping
    reverse_refs: Dict[str, List[tuple]] = defaultdict(list)
    all_tables = {source_table} | referenced_tables
    for table in all_tables:
        for src_col, ref_table, tgt_col in _get_fk_relationships(
            inspector=inspector, table_name=table, schema=schema, table_set=table_set,
        ):
            if ref_table in all_tables:
                reverse_refs[ref_table].append((table, src_col, tgt_col))

    # Collect FK columns per referenced table (to exclude from rollup)
    fk_columns_by_table: Dict[str, Set[str]] = defaultdict(set)
    for table in referenced_tables:
        fks = inspector.get_foreign_keys(table, schema=schema)
        for fk in fks:
            for col in fk["constrained_columns"]:
                fk_columns_by_table[table].add(col)

    # BFS from source to process joins in dependency order
    join_clauses = []
    processed = {source_table}
    queue = deque([source_table])

    while queue:
        current = queue.popleft()
        current_fk_rels = _get_fk_relationships(
            inspector=inspector, table_name=current, schema=schema, table_set=table_set,
        )
        next_tables = {ref_table for _, ref_table, _ in current_fk_rels
                       if ref_table in referenced_tables and ref_table not in processed}

        for ref_table in next_tables:
            ref_sql_table = f"{schema}.{ref_table}" if schema else ref_table
            ref_cols = inspector.get_columns(ref_table, schema=schema)
            ref_fk_cols = fk_columns_by_table[ref_table]

            for col in ref_cols:
                if col["name"] not in ref_fk_cols:
                    alias = f"{ref_table}__{col['name']}"
                    select_parts.append(f"{ref_sql_table}.{col['name']} AS {alias}")

            join_condition = None
            for referencing_table, fk_col, tgt_col in reverse_refs[ref_table]:
                if referencing_table in processed:
                    ref_full = f"{schema}.{referencing_table}" if schema else referencing_table
                    join_condition = f"{ref_full}.{fk_col} = {ref_sql_table}.{tgt_col}"
                    break

            if join_condition:
                join_clauses.append(f"LEFT JOIN {ref_sql_table} ON {join_condition}")
                processed.add(ref_table)
                queue.append(ref_table)

    select_clause = "SELECT\n    " + ",\n    ".join(select_parts)
    from_clause = f"FROM {sql_table}"
    if join_clauses:
        return f"{select_clause}\n{from_clause}\n" + "\n".join(join_clauses)
    return f"{select_clause}\n{from_clause}"


# ---------------------------------------------------------------------------
# Query introspection — derive column types from a SQL query
# ---------------------------------------------------------------------------

def _introspect_query_columns(
    sa_engine: sa.Engine,
    sql: str,
) -> List[tuple]:
    """Execute a query with LIMIT 0 and return (column_name, DataType) pairs."""
    # Wrap in a subquery to avoid executing the full query
    probe_sql = f"SELECT * FROM ({sql}) AS _probe LIMIT 0"
    with sa_engine.connect() as conn:
        result = conn.execute(sa.text(probe_sql))
        columns = []
        for col_name, col_type in zip(result.keys(), result.cursor.description):
            # col_type[1] is the type_code from DB-API; use SQLAlchemy's type mapping
            # For portability, re-inspect using column name from cursor description
            sa_type_code = col_type[1]
            # Try to map via the type name from cursor description
            type_name = type(sa_type_code).__name__.upper() if sa_type_code else ""
            if type_name in _SA_TYPE_MAP:
                data_type = _SA_TYPE_MAP[type_name]
            else:
                # Fallback: use string representation
                data_type = DataType.STRING
            columns.append((col_name, data_type))
        return columns


def _introspect_query_columns_via_inspector(
    sa_engine: sa.Engine,
    inspector: sa.engine.Inspector,
    table_name: str,
    schema: Optional[str],
    rollup_sql: Optional[str],
    referenced_tables: Set[str],
    fk_columns_by_table: Dict[str, Set[str]],
) -> List[tuple]:
    """Introspect columns from a rollup query or plain table.

    Returns list of (column_name, DataType, is_primary_key) tuples.
    For rollup queries, uses per-table inspector data since LIMIT 0
    type inference can be unreliable across databases.
    """
    results = []

    # Source table columns
    columns = inspector.get_columns(table_name, schema=schema)
    pk_constraint = inspector.get_pk_constraint(table_name, schema=schema)
    pk_columns = set(pk_constraint.get("constrained_columns", []))

    for col in columns:
        col_name = col["name"]
        data_type = _sa_type_to_data_type(col["type"])
        is_pk = col_name in pk_columns
        results.append((col_name, data_type, is_pk))

    # Referenced table columns (rollup)
    for ref_table in referenced_tables:
        ref_cols = inspector.get_columns(ref_table, schema=schema)
        ref_pk = inspector.get_pk_constraint(ref_table, schema=schema)
        ref_pk_cols = set(ref_pk.get("constrained_columns", []))
        ref_fk_cols = fk_columns_by_table.get(ref_table, set())

        for col in ref_cols:
            if col["name"] in ref_fk_cols:
                continue
            alias = f"{ref_table}__{col['name']}"
            data_type = _sa_type_to_data_type(col["type"])
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
    sql: Optional[str] = None,
    source_table_pk_columns: Optional[Set[str]] = None,
    joins: Optional[List[ModelJoin]] = None,
) -> SlayerModel:
    """Generate a SlayerModel from introspected (column_name, DataType, is_pk) tuples."""
    dimensions = []
    measures = []
    numeric_columns = []

    for col_name, data_type, is_pk in columns:
        # For rollup columns (table__col), primary_key is only for the source table
        dimensions.append(Dimension(
            name=col_name,
            sql=col_name,
            type=data_type,
            primary_key=is_pk and "__" not in col_name,  # Rollup PKs aren't model PKs
            description=f"From {col_name.split('__')[0]}" if "__" in col_name else None,
        ))

        if data_type in _NUMERIC_TYPES and not is_pk and not _is_id_column(col_name):
            numeric_columns.append(col_name)

    # Add COUNT measure
    measures.append(Measure(name="count", type=DataType.COUNT))

    # Add SUM and AVG for numeric non-ID columns
    for col_name in numeric_columns:
        measures.append(Measure(name=f"{col_name}_sum", sql=col_name, type=DataType.SUM))
        measures.append(Measure(name=f"{col_name}_avg", sql=col_name, type=DataType.AVERAGE))

    # Add count_distinct for rollup table PKs
    seen_tables = set()
    for col_name, data_type, is_pk in columns:
        if "__" in col_name and is_pk:
            ref_table = col_name.split("__")[0]
            if ref_table not in seen_tables:
                seen_tables.add(ref_table)
                measures.append(Measure(
                    name=f"{ref_table}__count",
                    sql=col_name,
                    type=DataType.COUNT_DISTINCT,
                    description=f"Distinct count of {ref_table}",
                ))

    return SlayerModel(
        name=name,
        sql_table=sql_table,
        sql=sql,
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
            # Build rollup SQL and explicit joins
            rollup_sql = _generate_rollup_sql(
                inspector=inspector, source_table=table_name,
                referenced_tables=referenced, schema=schema, table_set=table_set,
            )
            model_joins = _generate_joins(
                inspector=inspector, source_table=table_name,
                referenced_tables=referenced, schema=schema, table_set=table_set,
            )
            columns = _introspect_query_columns_via_inspector(
                sa_engine=sa_engine, inspector=inspector,
                table_name=table_name, schema=schema,
                rollup_sql=rollup_sql, referenced_tables=referenced,
                fk_columns_by_table=fk_columns_by_table,
            )
            model = _columns_to_model(
                name=table_name, columns=columns,
                data_source=datasource.name, sql=rollup_sql,
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
