"""SQLite-based storage for models and datasources.

v4 (DEV-1330): the ``models`` table has a composite ``(data_source, name)``
primary key so two datasources can share a table name without collision. A
``settings`` table stores singleton state — currently just the datasource
priority list used to disambiguate bare-name lookups. ``migrate_sqlite_schema``
runs at open time to upgrade legacy v3 single-PK databases in place.
"""

import asyncio
import json
import sqlite3
from typing import Dict, List, Optional, Tuple

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.embeddings.models import Embedding
from slayer.memories.models import Memory
from slayer.storage.base import StorageBackend, _validate_path_component
from slayer.storage.sidecar_embedding_store import SidecarEmbeddingStore
from slayer.storage.v4_migration import migrate_sqlite_schema


_PRIORITY_KEY = "datasource_priority"


class SQLiteStorage(StorageBackend):
    def __init__(self, db_path: str):
        self.db_path = db_path
        # Idempotent: rebuilds a v3 ``models`` table if needed; no-op on v4.
        migrate_sqlite_schema(db_path)
        self._init_db()
        # DEV-1386 / DEV-1405: the embeddings sidecar owns its own table
        # + index. CREATE-IF-NOT-EXISTS makes co-existence with our own
        # schema trivial.
        self._embeddings_store = SidecarEmbeddingStore(db_path=self.db_path)

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS models (
                    data_source TEXT NOT NULL,
                    name TEXT NOT NULL,
                    data TEXT NOT NULL,
                    PRIMARY KEY (data_source, name)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS datasources (
                    name TEXT PRIMARY KEY,
                    data TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            # DEV-1357 v2: unified memories.
            conn.execute("""
                CREATE TABLE IF NOT EXISTS memories (
                    id INTEGER PRIMARY KEY,
                    data TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS memory_entities (
                    memory_id INTEGER NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
                    entity TEXT NOT NULL,
                    PRIMARY KEY (memory_id, entity)
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_memory_entities_entity "
                "ON memory_entities(entity)"
            )

    # --- Sync helpers (run in thread to avoid blocking the event loop) ---

    def _save_model_sync(self, model: SlayerModel) -> None:
        data = json.dumps(model.model_dump(mode="json", exclude_none=True))
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO models (data_source, name, data) VALUES (?, ?, ?)",
                (model.data_source, model.name, data),
            )

    def _list_all_identities_sync(self) -> List[Tuple[str, str]]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT data_source, name FROM models ORDER BY data_source, name"
            ).fetchall()
        return [(r[0], r[1]) for r in rows]

    def _get_model_sync(self, data_source: str, name: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT data FROM models WHERE data_source = ? AND name = ?",
                (data_source, name),
            ).fetchone()
        return row[0] if row else None

    def _delete_model_sync(self, data_source: str, name: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM models WHERE data_source = ? AND name = ?",
                (data_source, name),
            )
            return cursor.rowcount > 0

    def _save_datasource_sync(self, datasource: DatasourceConfig) -> None:
        data = json.dumps(datasource.model_dump(mode="json", exclude_none=True))
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO datasources (name, data) VALUES (?, ?)",
                (datasource.name, data),
            )

    def _get_datasource_sync(self, name: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT data FROM datasources WHERE name = ?", (name,)
            ).fetchone()
        return row[0] if row else None

    def _list_datasources_sync(self) -> List[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT name FROM datasources ORDER BY name"
            ).fetchall()
        return [r[0] for r in rows]

    def _delete_datasource_sync(self, name: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM datasources WHERE name = ?", (name,)
            )
            return cursor.rowcount > 0

    def _get_priority_sync(self) -> List[str]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT value FROM settings WHERE key = ?", (_PRIORITY_KEY,)
            ).fetchone()
        if not row:
            return []
        try:
            value = json.loads(row[0])
        except (TypeError, ValueError):
            return []
        if not isinstance(value, list):
            return []
        return [str(p) for p in value]

    def _set_priority_sync(self, priority: List[str]) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (_PRIORITY_KEY, json.dumps(list(priority))),
            )

    # --- Async interface ---

    async def save_model(self, model: SlayerModel) -> None:
        await asyncio.to_thread(self._save_model_sync, model)

    async def _list_all_model_identities(self) -> List[Tuple[str, str]]:
        return await asyncio.to_thread(self._list_all_identities_sync)

    async def get_model(
        self,
        name: str,
        data_source: Optional[str] = None,
    ) -> Optional[SlayerModel]:
        target = await self._resolve_target_or_none(name, data_source=data_source)
        if target is None:
            return None
        data_source, name = target
        raw = await asyncio.to_thread(self._get_model_sync, data_source, name)
        if not raw:
            return None
        data = json.loads(raw)
        return await self._migrate_and_refine_on_load(
            name=name, data=data, data_source=data_source,
        )

    async def _delete_model_row(
        self, *, data_source: str, name: str,
    ) -> bool:
        return await asyncio.to_thread(self._delete_model_sync, data_source, name)

    def _update_column_sampled_sync(
        self, *, data_source: str, model_name: str,
        column_name: str, sampled: Optional[str],
    ) -> None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT data FROM models WHERE data_source = ? AND name = ?",
                (data_source, model_name),
            ).fetchone()
            if not row:
                raise ValueError(
                    f"update_column_sampled: model {model_name!r} in datasource "
                    f"{data_source!r} not found."
                )
            data = json.loads(row[0])
            cols = data.get("columns") or []
            for col in cols:
                if isinstance(col, dict) and col.get("name") == column_name:
                    if sampled is None:
                        col.pop("sampled", None)
                    else:
                        col["sampled"] = sampled
                    break
            else:
                raise ValueError(
                    f"update_column_sampled: column {column_name!r} not found "
                    f"on model {model_name!r} in datasource {data_source!r}."
                )
            conn.execute(
                "UPDATE models SET data = ? WHERE data_source = ? AND name = ?",
                (json.dumps(data), data_source, model_name),
            )

    async def update_column_sampled(
        self,
        *,
        data_source: str,
        model_name: str,
        column_name: str,
        sampled: Optional[str],
    ) -> None:
        await asyncio.to_thread(
            self._update_column_sampled_sync,
            data_source=data_source, model_name=model_name,
            column_name=column_name, sampled=sampled,
        )

    async def save_datasource(self, datasource: DatasourceConfig) -> None:
        await asyncio.to_thread(self._save_datasource_sync, datasource)

    async def get_datasource(self, name: str) -> Optional[DatasourceConfig]:
        # DEV-1405: sanitize the raw name. Mirrors the YAML backend; the
        # SQLite lookup is parameterised so injection isn't the risk —
        # validation here keeps the public ABC contract uniform across
        # backends.
        _validate_path_component(name, kind="datasource name")
        raw = await asyncio.to_thread(self._get_datasource_sync, name)
        if raw is None:
            return None
        ds = DatasourceConfig.model_validate(json.loads(raw))
        return ds.resolve_env_vars()

    async def list_datasources(self) -> List[str]:
        return await asyncio.to_thread(self._list_datasources_sync)

    async def _delete_datasource_row(self, name: str) -> bool:
        return await asyncio.to_thread(self._delete_datasource_sync, name)

    async def get_datasource_priority(self) -> List[str]:
        return await asyncio.to_thread(self._get_priority_sync)

    async def _set_datasource_priority_raw(self, priority: List[str]) -> None:
        await asyncio.to_thread(self._set_priority_sync, list(priority))

    # ---- memories (DEV-1357 v2) -------------------------------------------
    #
    # DEV-1405: ids are derived from the ``memories`` table itself, not
    # from a dedicated counter table. ``save_memory`` runs the insert
    # inside a single transaction with ``INSERT ... RETURNING id`` so the
    # id assignment is atomic with the write — SQLite serializes write
    # transactions, so two concurrent ``save_memory`` calls can never
    # both reserve the same id (which would happen if we read
    # ``MAX(id) + 1`` then issued a separate insert).
    #
    # Any legacy ``id_counters`` table on a pre-DEV-1405 DB is left in
    # place as harmless dead data; nothing reads it.

    def _save_memory_atomic_sync(
        self,
        learning: str,
        entities: List[str],
        query: Optional[SlayerQuery],
    ) -> Memory:
        """Reserve the next id and persist the new memory inside one
        SQLite transaction. Returns the persisted :class:`Memory` with
        the DB-assigned ``id``."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            # Reserve an id atomically. SQLite's ``INTEGER PRIMARY KEY``
            # is a rowid alias; inserting NULL assigns the next free id
            # inside the write lock (max(rowid) + 1 semantics; reuses
            # ids of tail-deleted rows per DEV-1405).
            cursor = conn.execute(
                "INSERT INTO memories (data) VALUES ('') RETURNING id"
            )
            row = cursor.fetchone()
            memory_id = int(row[0])
            memory = Memory(
                id=memory_id,
                learning=learning,
                entities=list(entities),
                query=query,
            )
            conn.execute(
                "UPDATE memories SET data = ? WHERE id = ?",
                (json.dumps(memory.model_dump(mode="json")), memory_id),
            )
            for entity in entities:
                conn.execute(
                    "INSERT OR IGNORE INTO memory_entities "
                    "(memory_id, entity) VALUES (?, ?)",
                    (memory_id, entity),
                )
        return memory

    async def save_memory(
        self,
        *,
        learning: str,
        entities: List[str],
        query: Optional[SlayerQuery] = None,
    ) -> Memory:
        return await asyncio.to_thread(
            self._save_memory_atomic_sync,
            learning, list(entities), query,
        )

    # ``_save_memory_row`` and ``_next_memory_seq`` are kept to satisfy
    # the ABC contract (third-party code or migrations that bypass the
    # public ``save_memory`` API still expect these primitives). Both
    # implementations match the documented contract and would themselves
    # be racy under concurrent writes — callers should use the public
    # ``save_memory`` above, which holds SQLite's write lock atomically.

    def _save_memory_sync(self, memory: Memory) -> None:
        data = json.dumps(memory.model_dump(mode="json"))
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute(
                "INSERT OR REPLACE INTO memories (id, data) VALUES (?, ?)",
                (memory.id, data),
            )
            conn.execute(
                "DELETE FROM memory_entities WHERE memory_id = ?",
                (memory.id,),
            )
            for entity in memory.entities:
                conn.execute(
                    "INSERT OR IGNORE INTO memory_entities "
                    "(memory_id, entity) VALUES (?, ?)",
                    (memory.id, entity),
                )

    async def _save_memory_row(self, memory: Memory) -> None:
        await asyncio.to_thread(self._save_memory_sync, memory)

    def _next_memory_seq_sync(self) -> int:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(id), 0) + 1 FROM memories"
            ).fetchone()
        return int(row[0])

    async def _next_memory_seq(self) -> int:
        return await asyncio.to_thread(self._next_memory_seq_sync)

    def _get_memory_sync(self, memory_id: int) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT data FROM memories WHERE id = ?", (memory_id,)
            ).fetchone()
        return row[0] if row else None

    async def _get_memory_row(self, memory_id: int) -> Optional[Memory]:
        raw = await asyncio.to_thread(self._get_memory_sync, memory_id)
        return Memory.model_validate(json.loads(raw)) if raw else None

    def _list_memories_sync(
        self, entities: Optional[List[str]]
    ) -> List[str]:
        with sqlite3.connect(self.db_path) as conn:
            if entities is None:
                rows = conn.execute(
                    "SELECT data FROM memories ORDER BY id"
                ).fetchall()
            elif not entities:
                return []
            else:
                placeholders = ",".join("?" * len(entities))
                rows = conn.execute(
                    f"SELECT DISTINCT m.data FROM memories m "
                    f"JOIN memory_entities me ON me.memory_id = m.id "
                    f"WHERE me.entity IN ({placeholders}) "
                    f"ORDER BY m.id",
                    tuple(entities),
                ).fetchall()
        return [r[0] for r in rows]

    async def _list_memories_rows(
        self, *, entities: Optional[List[str]]
    ) -> List[Memory]:
        raws = await asyncio.to_thread(self._list_memories_sync, entities)
        return [Memory.model_validate(json.loads(r)) for r in raws]

    def _delete_memory_sync(self, memory_id: int) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            cursor = conn.execute(
                "DELETE FROM memories WHERE id = ?", (memory_id,)
            )
            return cursor.rowcount > 0

    async def _delete_memory_row(self, memory_id: int) -> bool:
        return await asyncio.to_thread(self._delete_memory_sync, memory_id)

    # ---- embeddings (DEV-1386, refactored DEV-1405) -----------------------
    #
    # All embedding I/O delegates to ``self._embeddings_store``, the
    # :class:`SidecarEmbeddingStore` helper. The helper owns the
    # ``embeddings`` table inside the same SQLite file as everything
    # else; the SQL lives in one place so the YAML backend (which points
    # its own helper at a separate ``embeddings.db``) shares the impl.

    async def save_embedding(self, row: Embedding) -> None:
        await self._embeddings_store.save(row)

    async def save_embeddings(self, rows: List[Embedding]) -> None:
        await self._embeddings_store.save_many(list(rows))

    async def get_embedding(
        self, *, canonical_id: str, embedding_model_name: str,
    ) -> Optional[Embedding]:
        return await self._embeddings_store.get(
            canonical_id=canonical_id,
            embedding_model_name=embedding_model_name,
        )

    async def get_embeddings_for_canonical_ids(
        self,
        *,
        canonical_ids: List[str],
        embedding_model_name: str,
    ) -> Dict[str, Embedding]:
        return await self._embeddings_store.get_many(
            canonical_ids=list(canonical_ids),
            embedding_model_name=embedding_model_name,
        )

    async def list_embeddings(
        self, *, embedding_model_name: str,
    ) -> List[Embedding]:
        return await self._embeddings_store.list_for_model(
            embedding_model_name=embedding_model_name,
        )

    async def delete_embeddings_for_canonical(
        self, *, canonical_id_prefix: str,
    ) -> int:
        return await self._embeddings_store.delete_for_canonical(
            canonical_id_prefix=canonical_id_prefix,
        )
