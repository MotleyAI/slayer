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
  Dataset       id STRING, name STRING, description STRING
  DatasetField  id STRING, name STRING, data_type STRING, description STRING
  Metric        id STRING, name STRING, description STRING
  Aggregation   id STRING, name STRING

Relationship tables:
  MENTIONS       Memory → {Datasource, Dataset, DatasetField, Metric, Aggregation, Memory}
  CONTAINS       Datasource → Dataset, Dataset → {DatasetField, Metric, Aggregation}
  RELATIONSHIPS  Dataset → Dataset

All queries must be read-only ``MATCH … RETURN … AS id`` statements.
The ``MATCH (n:A:B)`` multi-label pattern returns nodes from BOTH table A
and table B (union semantics — LadybugDB behaviour).
"""

from __future__ import annotations

import asyncio
import re
import threading
from collections.abc import Hashable
from functools import lru_cache
from typing import Any

from pydantic import BaseModel

from slayer.core.models import SlayerDataset
from slayer.memories.models import MEMORY_CANONICAL_PREFIX as _MEMORY_PREFIX
from slayer.storage.base import StorageBackend
from slayer.storage.document_loading import DocumentLoadFailures


# ---------------------------------------------------------------------------
# Availability
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def is_available() -> bool:
    """Return True when LadybugDB is importable."""
    try:
        import ladybug  # noqa: F401  # ALLOW(import-not-top): optional dep, guarded
        return True
    except ImportError:
        return False


def _import_graph_module() -> Any:
    """Import LadybugDB."""
    try:
        import ladybug  # ALLOW(import-not-top): optional dep, guarded
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
    r"\b(CREATE|MERGE|DELETE|SET|REMOVE|DROP|CALL)\b", re.IGNORECASE
)
_AS_ID_RE = re.compile(r"\bAS\s+id\b", re.IGNORECASE)
# DEV-1464: the validator is fundamentally allowlist-shaped — start with
# MATCH or reject. A denylist over Kuzu's evolving keyword surface (LOAD,
# IMPORT, EXPORT, INSTALL, UNWIND-from-the-front, …) is too fragile to
# defend the search endpoint, since non-mutating but still-dangerous
# clauses like ``LOAD FROM '/etc/passwd' RETURN 1 AS id`` would otherwise
# pass the mutation-keyword check and reach ``conn.execute()``.
_STARTS_WITH_MATCH_RE = re.compile(r"^\s*MATCH\b", re.IGNORECASE)
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
    * queries that don't start with ``MATCH`` (read-only allowlist)
    * semicolons (multiple statements)
    * mutation keywords: CREATE, MERGE, DELETE, SET, REMOVE, DROP, CALL
    * missing ``AS id`` alias in the RETURN clause
    """
    if not _STARTS_WITH_MATCH_RE.match(cypher):
        raise ValueError(
            "cypher_filter must start with MATCH (read-only graph "
            "traversal); other Cypher clauses are not allowed."
        )
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
        "id STRING, learning STRING, description STRING, PRIMARY KEY(id))"
    )
    conn.execute(
        "CREATE NODE TABLE Datasource("
        "id STRING, name STRING, description STRING, PRIMARY KEY(id))"
    )
    conn.execute(
        "CREATE NODE TABLE Dataset("
        "id STRING, name STRING, description STRING, PRIMARY KEY(id))"
    )
    conn.execute(
        "CREATE NODE TABLE DatasetField("
        "id STRING, name STRING, data_type STRING, description STRING, PRIMARY KEY(id))"
    )
    conn.execute(
        "CREATE NODE TABLE Metric("
        "id STRING, name STRING, description STRING, PRIMARY KEY(id))"
    )
    conn.execute(
        "CREATE NODE TABLE Aggregation("
        "id STRING, name STRING, description STRING, PRIMARY KEY(id))"
    )
    conn.execute(
        "CREATE REL TABLE MENTIONS("
        "FROM Memory TO Datasource, "
        "FROM Memory TO Dataset, "
        "FROM Memory TO DatasetField, "
        "FROM Memory TO Metric, "
        "FROM Memory TO Aggregation, "
        "FROM Memory TO Memory"
        ")"
    )
    conn.execute(
        "CREATE REL TABLE CONTAINS("
        "FROM Datasource TO Dataset, "
        "FROM Dataset TO DatasetField, "
        "FROM Dataset TO Metric, "
        "FROM Dataset TO Aggregation"
        ")"
    )
    conn.execute("CREATE REL TABLE RELATIONSHIPS(FROM Dataset TO Dataset, cardinality STRING)")


def _insert_dataset_child_nodes(conn: Any, canonical_dataset: str, dataset: SlayerDataset) -> None:
    """Insert DatasetField, Metric, and Aggregation nodes for one dataset."""
    for col in dataset.fields:
        if col.hidden:
            continue
        conn.execute(
            "CREATE (:DatasetField {"
            "id: $id, name: $name, data_type: $dt, description: $descr"
            "})",
            {
                "id": f"{canonical_dataset}.{col.name}",
                "name": col.name,
                "dt": col.datatype.value if col.datatype is not None else "",
                "descr": col.description or "",
            },
        )
    for measure in dataset.metrics:
        if not measure.name:
            continue
        conn.execute(
            "CREATE (:Metric {id: $id, name: $name, description: $descr})",
            {
                "id": f"{canonical_dataset}.{measure.name}",
                "name": measure.name,
                "descr": measure.description or "",
            },
        )
    for agg in dataset.aggregations:
        conn.execute(
            "CREATE (:Aggregation {id: $id, name: $name, description: $descr})",
            {
                "id": f"{canonical_dataset}.{agg.name}",
                "name": agg.name,
                "descr": agg.description or "",
            },
        )


def _insert_nodes(
    conn: Any,
    datasource_names: list[str],
    visible_datasets: dict[str, SlayerDataset],
    memories: list,
    datasource_descriptions: dict[str, str | None] | None = None,
) -> None:
    """Insert all node rows into the graph.

    DEV-1549: ``datasource_descriptions`` carries
    ``{ds_name → DatasourceConfig.description}`` so the graph schema
    can expose ``Datasource.description`` for cypher_filter queries,
    in parity with the lexical and embedding channels. Likewise
    ``Memory.description`` is inserted directly from the memory.
    """
    descriptions = datasource_descriptions or {}
    for name in datasource_names:
        conn.execute(
            "CREATE (:Datasource {id: $id, name: $name, description: $descr})",
            {
                "id": name,
                "name": name,
                "descr": descriptions.get(name) or "",
            },
        )

    for canonical_model, model in visible_datasets.items():
        _, model_name = canonical_model.split(".", 1)
        conn.execute(
            "CREATE (:Dataset {id: $id, name: $name, description: $descr})",
            {
                "id": canonical_model,
                "name": model_name,
                "descr": model.description or "",
            },
        )
        _insert_dataset_child_nodes(conn, canonical_model, model)

    for mem in memories:
        conn.execute(
            "CREATE (:Memory {id: $id, learning: $learning, description: $descr})",
            {
                "id": f"{_MEMORY_PREFIX}{mem.id}",
                "learning": mem.learning,
                "descr": mem.description or "",
            },
        )


def _insert_contains_edges(
    conn: Any,
    datasource_names: list[str],
    visible_datasets: dict[str, SlayerDataset],
) -> None:
    """Insert CONTAINS edges: Datasource→Dataset and Dataset→{DatasetField/Metric/Agg}."""
    ds_set = set(datasource_names)
    for canonical_model, model in visible_datasets.items():
        ds = canonical_model.split(".", 1)[0]
        if ds in ds_set:
            conn.execute(
                "MATCH (d:Datasource {id: $ds}), (m:Dataset {id: $dataset}) "
                "CREATE (d)-[:CONTAINS]->(m)",
                {"ds": ds, "dataset": canonical_model},
            )
        for col in model.fields:
            if col.hidden:
                continue
            conn.execute(
                "MATCH (m:Dataset {id: $dataset}), (c:DatasetField {id: $field}) "
                "CREATE (m)-[:CONTAINS]->(c)",
                {"dataset": canonical_model, "field": f"{canonical_model}.{col.name}"},
            )
        for measure in model.metrics:
            if not measure.name:
                continue
            conn.execute(
                "MATCH (m:Dataset {id: $dataset}), (ms:Metric {id: $ms}) "
                "CREATE (m)-[:CONTAINS]->(ms)",
                {"dataset": canonical_model, "ms": f"{canonical_model}.{measure.name}"},
            )
        for agg in model.aggregations:
            conn.execute(
                "MATCH (m:Dataset {id: $dataset}), (a:Aggregation {id: $agg}) "
                "CREATE (m)-[:CONTAINS]->(a)",
                {"dataset": canonical_model, "agg": f"{canonical_model}.{agg.name}"},
            )


def _insert_relationships_edges(conn: Any, visible_datasets: dict[str, SlayerDataset]) -> None:
    """Insert RELATIONSHIPS edges: Dataset→Dataset (via dataset.relationships). Missing targets silently skipped."""
    for canonical_model, model in visible_datasets.items():
        ds = canonical_model.split(".", 1)[0]
        for join in model.relationships:
            target_canonical = f"{ds}.{join.to}"
            if target_canonical not in visible_datasets:
                continue
            conn.execute(
                "MATCH (src:Dataset {id: $src}), (tgt:Dataset {id: $tgt}) "
                "CREATE (src)-[:RELATIONSHIPS {cardinality: $card}]->(tgt)",
                {
                    "src": canonical_model,
                    "tgt": target_canonical,
                    "card": str(join.cardinality) if join.cardinality else "",
                },
            )


def _connect_entity_mention(
    conn: Any,
    src: str,
    entity: str,
    ds_set: set[str],
    valid_datasets: set[str],
    valid_fields: set[str],
    valid_metrics: set[str],
    valid_aggs: set[str],
    valid_memory_canonicals: set[str],
) -> None:
    """Create one MENTIONS edge from memory *src* to the matching entity node."""
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
    elif entity in valid_datasets:
        conn.execute(
            "MATCH (m:Memory {id: $src}), (n:Dataset {id: $tgt}) "
            "CREATE (m)-[:MENTIONS]->(n)",
            {"src": src, "tgt": entity},
        )
    elif entity in valid_metrics:
        conn.execute(
            "MATCH (m:Memory {id: $src}), (ms:Metric {id: $tgt}) "
            "CREATE (m)-[:MENTIONS]->(ms)",
            {"src": src, "tgt": entity},
        )
    elif entity in valid_aggs:
        conn.execute(
            "MATCH (m:Memory {id: $src}), (a:Aggregation {id: $tgt}) "
            "CREATE (m)-[:MENTIONS]->(a)",
            {"src": src, "tgt": entity},
        )
    elif entity in valid_fields:
        conn.execute(
            "MATCH (m:Memory {id: $src}), (c:DatasetField {id: $tgt}) "
            "CREATE (m)-[:MENTIONS]->(c)",
            {"src": src, "tgt": entity},
        )


def _build_valid_entity_sets(
    visible_datasets: dict[str, SlayerDataset],
    memories: list,
) -> tuple[set[str], set[str], set[str], set[str]]:
    """Build (valid_fields, valid_metrics, valid_aggs, valid_memory_canonicals)."""
    valid_fields: set[str] = set()
    valid_metrics: set[str] = set()
    valid_aggs: set[str] = set()
    for canonical_model, model in visible_datasets.items():
        for col in model.fields:
            if not col.hidden:
                valid_fields.add(f"{canonical_model}.{col.name}")
        for measure in model.metrics:
            if measure.name:
                valid_metrics.add(f"{canonical_model}.{measure.name}")
        for agg in model.aggregations:
            valid_aggs.add(f"{canonical_model}.{agg.name}")
    valid_memory_canonicals = {f"{_MEMORY_PREFIX}{m.id}" for m in memories}
    return valid_fields, valid_metrics, valid_aggs, valid_memory_canonicals


def _insert_mentions_edges(
    conn: Any,
    memories: list,
    visible_datasets: dict[str, SlayerDataset],
    datasource_names: list[str],
) -> None:
    """Insert MENTIONS edges: Memory → {Datasource, Dataset, DatasetField, Metric, Agg, Memory}."""
    ds_set = set(datasource_names)
    valid_datasets: set[str] = set(visible_datasets)
    valid_columns, valid_measures, valid_aggs, valid_memory_canonicals = (
        _build_valid_entity_sets(visible_datasets, memories)
    )

    for mem in memories:
        src = f"{_MEMORY_PREFIX}{mem.id}"
        for entity in mem.entities:
            _connect_entity_mention(
                conn, src, entity,
                ds_set, valid_datasets, valid_columns, valid_measures, valid_aggs,
                valid_memory_canonicals,
            )


async def build_graph(storage: StorageBackend, *, failures: DocumentLoadFailures | None = None) -> tuple[Any, Any]:
    """Build an ephemeral in-memory LadybugDB graph from ``storage``.

    Returns ``(db, conn)``. Hidden datasets and hidden fields are excluded.
    Memory canonical IDs are stored in ``memory:<id>`` form.
    """
    mod = _import_graph_module()
    # No-argument Database() creates an ephemeral in-memory instance;
    # no files are written to the working directory.
    db = mod.Database()
    conn = mod.Connection(db)
    _create_schema(conn)

    datasource_names = await storage.list_datasources()
    failures = failures or DocumentLoadFailures()
    visible_datasets: dict[str, SlayerDataset] = {
        f"{m.data_source}.{m.name}": m
        for m in failures.skip(await storage.load_datasets()) if not m.hidden
    }

    memories = await storage.list_memories(entities=None, failures=failures)

    # DEV-1549: load datasource descriptions so the graph schema can
    # expose `Datasource.description` for cypher_filter queries.
    datasource_descriptions: dict[str, str | None] = {}
    for ds_name in datasource_names:
        cfg = await storage.get_datasource(ds_name)
        datasource_descriptions[ds_name] = (
            cfg.description if cfg is not None else None
        )

    _insert_nodes(
        conn,
        datasource_names,
        visible_datasets,
        memories,
        datasource_descriptions=datasource_descriptions,
    )
    _insert_contains_edges(conn, datasource_names, visible_datasets)
    _insert_relationships_edges(conn, visible_datasets)
    _insert_mentions_edges(conn, memories, visible_datasets, datasource_names)

    return db, conn


# ---------------------------------------------------------------------------
# Per-storage cache with double-checked locking
# ---------------------------------------------------------------------------


#: Wall-clock limit on one ``cypher_filter``, set on each cached connection when it is created.
CYPHER_FILTER_TIMEOUT_MS = 10_000


class _GraphCache(BaseModel):
    fingerprint: str
    db: Any
    conn: Any
    # Serialises filters on the connection and its close; ``closed`` is set under it.
    lock: Any
    closed: bool = False

    model_config = {"arbitrary_types_allowed": True}


_cache: dict[Hashable, _GraphCache] = {}
_locks: dict[Hashable, asyncio.Lock] = {}


def _get_lock(key: Hashable) -> asyncio.Lock:
    """Return the per-key asyncio.Lock, creating it if absent.

    Safe without an outer lock because no ``await`` separates the
    membership check from the insertion (asyncio is single-threaded).
    """
    if key not in _locks:
        _locks[key] = asyncio.Lock()
    return _locks[key]


def _close_entry(entry: "_GraphCache") -> None:
    """Best-effort close of a cached graph entry, after any in-flight filter on it."""
    with entry.lock:
        entry.closed = True
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


async def _get_or_rebuild(
    storage: StorageBackend, *, failures: DocumentLoadFailures | None = None,
) -> _GraphCache:
    """Return the cached graph if the fingerprint matches; else rebuild. Keyed by the store's view."""
    key = await storage.cache_identity()

    try:
        current_fp: str | None = await storage.graph_fingerprint()
    except OSError:
        current_fp = None

    # Fast path: no lock needed when cache is warm and fingerprint matches.
    cached = _cache.get(key)
    if cached is not None and current_fp is not None and cached.fingerprint == current_fp:
        return cached

    lock = _get_lock(key)
    async with lock:
        # Double-check under the lock so only one rebuild fires.
        cached = _cache.get(key)
        if cached is not None and current_fp is not None and cached.fingerprint == current_fp:
            return cached

        old = _cache.get(key)
        db, conn = await build_graph(storage, failures=failures)
        conn.set_query_timeout(CYPHER_FILTER_TIMEOUT_MS)
        if old is not None:
            await asyncio.to_thread(_close_entry, old)
        entry = _GraphCache(
            fingerprint=current_fp if current_fp is not None else "",
            db=db, conn=conn, lock=threading.Lock(),
        )
        _cache[key] = entry
        return entry


def _run_filter(entry: _GraphCache, cypher: str, limit_ms: int) -> frozenset[str] | None:
    """Execute and drain ``cypher`` on ``entry``'s connection; ``None`` when the entry was closed meanwhile."""
    with entry.lock:
        if entry.closed:
            return None
        try:
            result = entry.conn.execute(cypher)
            col_names: list[str] = result.get_column_names()
            if "id" not in col_names:
                raise ValueError(
                    "cypher_filter must return a column named 'id'; "
                    f"got columns: {col_names!r}."
                )
            id_idx = col_names.index("id")
            ids: set[str] = set()
            while result.has_next():
                row = result.get_next()
                if row and row[id_idx] is not None:
                    ids.add(str(row[id_idx]))
        except ValueError:
            raise
        except Exception as exc:
            if "interrupt" in str(exc).lower():
                raise ValueError(f"cypher_filter timed out after {limit_ms} ms.") from exc
            raise ValueError(f"cypher_filter execution failed: {exc}") from exc
        return frozenset(ids)


async def get_filtered_ids(
    cypher: str,
    storage: StorageBackend,
    *,
    failures: DocumentLoadFailures | None = None,
) -> frozenset[str]:
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
    while True:
        entry = await _get_or_rebuild(storage, failures=failures)
        ids = await asyncio.to_thread(_run_filter, entry, cypher, CYPHER_FILTER_TIMEOUT_MS)
        if ids is not None:
            return ids
