"""Graph-backed Cypher pre-filter for search (DEV-1464).

Builds an ephemeral in-memory LadybugDB property graph from a StorageBackend
and executes openCypher queries to return a frozenset of canonical IDs that
pre-filter the three search channels.

Requires the ``advanced_search`` extra (``pip install motley-slayer[advanced_search]``),
which pulls in ``ladybug`` — the active successor to KuzuDB (same codebase,
new name after the original KuzuDB repo was archived post-acquisition).
If LadybugDB is not installed, ``is_available()`` returns ``False``
and no graph code is reachable.

Graph schema
------------
Node tables (one per entity kind):
  Memory        id STRING (canonical ``memory:<id>`` form), learning STRING
  Datasource    id STRING, name STRING
  Model         id STRING, name STRING, description STRING
  Column        id STRING, name STRING, data_type STRING, description STRING
  Measure       id STRING, name STRING, description STRING
  Aggregation   id STRING, name STRING

Relationship tables:
  MENTIONS   Memory → {Datasource, Model, Column, Measure, Aggregation, Memory}
  CONTAINS   Datasource → Model, Model → {Column, Measure, Aggregation}
  JOINS      Model → Model

All queries must be read-only ``MATCH … RETURN … AS id`` statements.
The ``MATCH (n:A:B)`` multi-label pattern returns nodes from BOTH table A
and table B (union semantics — Kuzu/LadybugDB behaviour).
"""

from __future__ import annotations

import asyncio
import os
import re
from functools import lru_cache
from typing import Any, FrozenSet, Optional

from pydantic import BaseModel

from slayer.memories.models import MEMORY_CANONICAL_PREFIX as _MEMORY_PREFIX
from slayer.storage.base import StorageBackend


# ---------------------------------------------------------------------------
# Availability
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def is_available() -> bool:
    """Return True when LadybugDB is importable."""
    try:
        import ladybug  # noqa: F401
        return True
    except ImportError:
        return False


def _import_graph_module() -> Any:
    """Import LadybugDB."""
    try:
        import ladybug
        return ladybug
    except ImportError:
        raise ImportError(
            "LadybugDB not installed; "
            "install with: pip install motley-slayer[advanced_search]"
        )


# ---------------------------------------------------------------------------
# Cypher validation
# ---------------------------------------------------------------------------


_MUTATION_RE = re.compile(
    r"\b(CREATE|MERGE|DELETE|SET|DROP|CALL)\b", re.IGNORECASE
)
_AS_ID_RE = re.compile(r"\bAS\s+id\b", re.IGNORECASE)
# Matches single- and double-quoted string literals (with backslash-escape
# support) so we can strip them before scanning for mutation keywords and
# avoid false-positive rejections on property values like 'call me'.
_QUOTED_STRING_RE = re.compile(
    r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"",
    re.DOTALL,
)


def _validate_cypher(cypher: str) -> None:
    """Validate that ``cypher`` is a safe read-only ``MATCH … RETURN … AS id``.

    Raises ``ValueError`` on:
    * semicolons (multiple statements)
    * mutation keywords: CREATE, MERGE, DELETE, SET, DROP, CALL
    * missing ``AS id`` alias in the RETURN clause
    """
    if ";" in cypher:
        raise ValueError(
            "cypher_filter must be a single statement; "
            "semicolons are not allowed."
        )
    bare = _QUOTED_STRING_RE.sub("", cypher)
    match = _MUTATION_RE.search(bare)
    if match:
        raise ValueError(
            f"cypher_filter must be read-only; "
            f"mutation keyword {match.group()!r} is not allowed."
        )
    if not _AS_ID_RE.search(cypher):
        raise ValueError(
            "cypher_filter must return exactly one column aliased 'id' "
            "(e.g. 'RETURN n.id AS id')."
        )


# ---------------------------------------------------------------------------
# Graph construction helpers
# ---------------------------------------------------------------------------


def _create_schema(conn: Any) -> None:
    """Create all node and relationship tables."""
    conn.execute(
        "CREATE NODE TABLE Memory("
        "id STRING, learning STRING, PRIMARY KEY(id))"
    )
    conn.execute(
        "CREATE NODE TABLE Datasource("
        "id STRING, name STRING, PRIMARY KEY(id))"
    )
    conn.execute(
        "CREATE NODE TABLE Model("
        "id STRING, name STRING, description STRING, PRIMARY KEY(id))"
    )
    conn.execute(
        "CREATE NODE TABLE Column("
        "id STRING, name STRING, data_type STRING, description STRING, PRIMARY KEY(id))"
    )
    conn.execute(
        "CREATE NODE TABLE Measure("
        "id STRING, name STRING, description STRING, PRIMARY KEY(id))"
    )
    conn.execute(
        "CREATE NODE TABLE Aggregation("
        "id STRING, name STRING, PRIMARY KEY(id))"
    )
    conn.execute(
        "CREATE REL TABLE MENTIONS("
        "FROM Memory TO Datasource, "
        "FROM Memory TO Model, "
        "FROM Memory TO Column, "
        "FROM Memory TO Measure, "
        "FROM Memory TO Aggregation, "
        "FROM Memory TO Memory"
        ")"
    )
    conn.execute(
        "CREATE REL TABLE CONTAINS("
        "FROM Datasource TO Model, "
        "FROM Model TO Column, "
        "FROM Model TO Measure, "
        "FROM Model TO Aggregation"
        ")"
    )
    conn.execute("CREATE REL TABLE JOINS(FROM Model TO Model)")


def _insert_nodes(
    conn: Any,
    datasource_names: list[str],
    visible_models: dict,
    memories: list,
) -> None:
    """Insert all node rows into the graph."""
    for name in datasource_names:
        conn.execute(
            "CREATE (:Datasource {id: $id, name: $name})",
            {"id": name, "name": name},
        )

    for canonical_model, model in visible_models.items():
        ds, model_name = canonical_model.split(".", 1)
        conn.execute(
            "CREATE (:Model {id: $id, name: $name, description: $desc})",
            {
                "id": canonical_model,
                "name": model_name,
                "desc": model.description or "",
            },
        )
        for col in model.columns:
            if col.hidden:
                continue
            conn.execute(
                "CREATE (:Column {"
                "id: $id, name: $name, data_type: $dt, description: $desc"
                "})",
                {
                    "id": f"{canonical_model}.{col.name}",
                    "name": col.name,
                    "dt": col.type.value if col.type is not None else "",
                    "desc": col.description or "",
                },
            )
        for measure in model.measures:
            if not measure.name:
                continue
            conn.execute(
                "CREATE (:Measure {id: $id, name: $name, description: $desc})",
                {
                    "id": f"{canonical_model}.{measure.name}",
                    "name": measure.name,
                    "desc": measure.description or "",
                },
            )
        for agg in model.aggregations:
            conn.execute(
                "CREATE (:Aggregation {id: $id, name: $name})",
                {"id": f"{canonical_model}.{agg.name}", "name": agg.name},
            )

    for mem in memories:
        conn.execute(
            "CREATE (:Memory {id: $id, learning: $learning})",
            {
                "id": f"{_MEMORY_PREFIX}{mem.id}",
                "learning": mem.learning,
            },
        )


def _insert_contains_edges(
    conn: Any,
    datasource_names: list[str],
    visible_models: dict,
) -> None:
    """Insert CONTAINS edges: Datasource→Model and Model→{Column/Measure/Agg}."""
    ds_set = set(datasource_names)
    for canonical_model, model in visible_models.items():
        ds = canonical_model.split(".", 1)[0]
        if ds in ds_set:
            conn.execute(
                "MATCH (d:Datasource {id: $ds}), (m:Model {id: $model}) "
                "CREATE (d)-[:CONTAINS]->(m)",
                {"ds": ds, "model": canonical_model},
            )
        for col in model.columns:
            if col.hidden:
                continue
            conn.execute(
                "MATCH (m:Model {id: $model}), (c:Column {id: $col}) "
                "CREATE (m)-[:CONTAINS]->(c)",
                {"model": canonical_model, "col": f"{canonical_model}.{col.name}"},
            )
        for measure in model.measures:
            if not measure.name:
                continue
            conn.execute(
                "MATCH (m:Model {id: $model}), (ms:Measure {id: $ms}) "
                "CREATE (m)-[:CONTAINS]->(ms)",
                {"model": canonical_model, "ms": f"{canonical_model}.{measure.name}"},
            )
        for agg in model.aggregations:
            conn.execute(
                "MATCH (m:Model {id: $model}), (a:Aggregation {id: $agg}) "
                "CREATE (m)-[:CONTAINS]->(a)",
                {"model": canonical_model, "agg": f"{canonical_model}.{agg.name}"},
            )


def _insert_joins_edges(conn: Any, visible_models: dict) -> None:
    """Insert JOINS edges: Model→Model (via model.joins). Missing targets silently skipped."""
    for canonical_model, model in visible_models.items():
        ds = canonical_model.split(".", 1)[0]
        for join in model.joins:
            target_canonical = f"{ds}.{join.target_model}"
            if target_canonical not in visible_models:
                continue
            conn.execute(
                "MATCH (src:Model {id: $src}), (tgt:Model {id: $tgt}) "
                "CREATE (src)-[:JOINS]->(tgt)",
                {"src": canonical_model, "tgt": target_canonical},
            )


def _insert_mentions_edges(
    conn: Any,
    memories: list,
    visible_models: dict,
    datasource_names: list[str],
) -> None:
    """Insert MENTIONS edges: Memory → {Datasource, Model, Column, Measure, Agg, Memory}."""
    ds_set = set(datasource_names)
    valid_models: set[str] = set(visible_models)
    valid_columns: set[str] = set()
    valid_measures: set[str] = set()
    valid_aggs: set[str] = set()
    for canonical_model, model in visible_models.items():
        for col in model.columns:
            if not col.hidden:
                valid_columns.add(f"{canonical_model}.{col.name}")
        for measure in model.measures:
            if measure.name:
                valid_measures.add(f"{canonical_model}.{measure.name}")
        for agg in model.aggregations:
            valid_aggs.add(f"{canonical_model}.{agg.name}")
    valid_memory_canonicals = {f"{_MEMORY_PREFIX}{m.id}" for m in memories}

    for mem in memories:
        src = f"{_MEMORY_PREFIX}{mem.id}"
        for entity in mem.entities:
            if entity in valid_memory_canonicals:
                conn.execute(
                    "MATCH (m1:Memory {id: $src}), (m2:Memory {id: $tgt}) "
                    "CREATE (m1)-[:MENTIONS]->(m2)",
                    {"src": src, "tgt": entity},
                )
            elif entity in ds_set:
                conn.execute(
                    "MATCH (m:Memory {id: $src}), (d:Datasource {id: $tgt}) "
                    "CREATE (m)-[:MENTIONS]->(d)",
                    {"src": src, "tgt": entity},
                )
            elif entity in valid_models:
                conn.execute(
                    "MATCH (m:Memory {id: $src}), (n:Model {id: $tgt}) "
                    "CREATE (m)-[:MENTIONS]->(n)",
                    {"src": src, "tgt": entity},
                )
            elif entity in valid_measures:
                conn.execute(
                    "MATCH (m:Memory {id: $src}), (ms:Measure {id: $tgt}) "
                    "CREATE (m)-[:MENTIONS]->(ms)",
                    {"src": src, "tgt": entity},
                )
            elif entity in valid_aggs:
                conn.execute(
                    "MATCH (m:Memory {id: $src}), (a:Aggregation {id: $tgt}) "
                    "CREATE (m)-[:MENTIONS]->(a)",
                    {"src": src, "tgt": entity},
                )
            elif entity in valid_columns:
                conn.execute(
                    "MATCH (m:Memory {id: $src}), (c:Column {id: $tgt}) "
                    "CREATE (m)-[:MENTIONS]->(c)",
                    {"src": src, "tgt": entity},
                )


async def build_graph(storage: StorageBackend) -> tuple[Any, Any]:
    """Build an ephemeral in-memory LadybugDB graph from ``storage``.

    Returns ``(db, conn)``. Hidden models and hidden columns are excluded.
    Memory canonical IDs are stored in ``memory:<id>`` form.
    """
    mod = _import_graph_module()
    # No-argument Database() creates an ephemeral in-memory instance in
    # LadybugDB (and kuzu ≥ 0.3); no files are written to the working directory.
    db = mod.Database()
    conn = db.connect()
    _create_schema(conn)

    datasource_names = await storage.list_datasources()
    identities = await storage._list_all_model_identities()

    visible_models: dict = {}
    for ds, model_name in identities:
        model = await storage.get_model(model_name, data_source=ds)
        if model is not None and not model.hidden:
            visible_models[f"{ds}.{model_name}"] = model

    memories = await storage.list_memories(entities=None)

    _insert_nodes(conn, datasource_names, visible_models, memories)
    _insert_contains_edges(conn, datasource_names, visible_models)
    _insert_joins_edges(conn, visible_models)
    _insert_mentions_edges(conn, memories, visible_models, datasource_names)

    return db, conn


# ---------------------------------------------------------------------------
# Per-storage cache with double-checked locking
# ---------------------------------------------------------------------------


class _GraphCache(BaseModel):
    fingerprint: str
    db: Any
    conn: Any

    model_config = {"arbitrary_types_allowed": True}


_cache: dict[str, _GraphCache] = {}
_locks: dict[str, asyncio.Lock] = {}


def _storage_key(storage: StorageBackend) -> str:
    """Stable path key for cache lookups."""
    from slayer.storage.sqlite_storage import SQLiteStorage
    from slayer.storage.yaml_storage import YAMLStorage

    if isinstance(storage, YAMLStorage):
        return os.path.abspath(storage.base_dir)
    if isinstance(storage, SQLiteStorage):
        return os.path.abspath(storage.db_path)
    return str(id(storage))


def _get_lock(key: str) -> asyncio.Lock:
    """Return the per-key asyncio.Lock, creating it if absent.

    Safe without an outer lock because no ``await`` separates the
    membership check from the insertion (asyncio is single-threaded).
    """
    if key not in _locks:
        _locks[key] = asyncio.Lock()
    return _locks[key]


def _close_entry(entry: "_GraphCache") -> None:
    """Best-effort close of a cached graph entry to release kuzu handles."""
    for obj in (entry.conn, entry.db):
        try:
            obj.close()
        except Exception:  # noqa: BLE001
            pass


def clear_cache() -> None:
    """Discard all cached graphs and locks. Primarily used in tests."""
    for entry in _cache.values():
        _close_entry(entry)
    _cache.clear()
    _locks.clear()


async def _get_or_rebuild(storage: StorageBackend) -> tuple[Any, Any]:
    """Return cached (db, conn) if the fingerprint matches; else rebuild."""
    key = _storage_key(storage)

    try:
        current_fp: Optional[str] = storage.graph_fingerprint()
    except Exception:
        current_fp = None

    # Fast path: no lock needed when cache is warm and fingerprint matches.
    cached = _cache.get(key)
    if cached is not None and current_fp is not None and cached.fingerprint == current_fp:
        return cached.db, cached.conn

    lock = _get_lock(key)
    async with lock:
        # Double-check under the lock so only one rebuild fires.
        cached = _cache.get(key)
        if cached is not None and current_fp is not None and cached.fingerprint == current_fp:
            return cached.db, cached.conn

        old = _cache.get(key)
        db, conn = await build_graph(storage)
        if old is not None:
            _close_entry(old)
        _cache[key] = _GraphCache(
            fingerprint=current_fp if current_fp is not None else "",
            db=db,
            conn=conn,
        )
        return db, conn


async def get_filtered_ids(
    cypher: str,
    storage: StorageBackend,
) -> FrozenSet[str]:
    """Execute a Cypher query against the storage graph and return the
    frozenset of id strings from the result's ``id`` column.

    Raises ``ValueError`` if the query fails validation or if LadybugDB
    is not installed.
    """
    if not is_available():
        raise ValueError(
            "cypher_filter requires LadybugDB; "
            "install with: pip install motley-slayer[advanced_search]"
        )
    _validate_cypher(cypher)
    _db, conn = await _get_or_rebuild(storage)
    result = conn.execute(cypher)
    ids: set[str] = set()
    while result.has_next():
        row = result.get_next()
        if row and row[0] is not None:
            ids.add(str(row[0]))
    return frozenset(ids)
