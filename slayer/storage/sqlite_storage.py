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
from slayer.embeddings.models import Embedding
from slayer.memories.models import Memory
from slayer.storage.base import StorageBackend
from slayer.storage.v4_migration import migrate_sqlite_schema


_PRIORITY_KEY = "datasource_priority"


class SQLiteStorage(StorageBackend):
    def __init__(self, db_path: str):
        self.db_path = db_path
        # Idempotent: rebuilds a v3 ``models`` table if needed; no-op on v4.
        migrate_sqlite_schema(db_path)
        self._init_db()

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
            conn.execute("""
                CREATE TABLE IF NOT EXISTS id_counters (
                    counter_name TEXT PRIMARY KEY,
                    last_value INTEGER NOT NULL
                )
            """)
            # DEV-1386: embeddings sidecar table.
            conn.execute("""
                CREATE TABLE IF NOT EXISTS embeddings (
                    canonical_id TEXT NOT NULL,
                    embedding_model_name TEXT NOT NULL,
                    entity_kind TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    embedding TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (canonical_id, embedding_model_name)
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_embeddings_model "
                "ON embeddings(embedding_model_name)"
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

    # Per-counter recovery seed: when ``id_counters`` has no row for the
    # given counter but the data table already has rows (e.g. someone
    # restored ``memories`` from a backup without the matching counters
    # row), seed ``last_value`` from ``MAX(id)`` so the next allocation
    # skips past existing rows. Otherwise ``_save_memory_sync``'s
    # INSERT OR REPLACE would clobber memory id 1.
    _COUNTER_SEED_TABLES: Dict[str, str] = {"memory_seq": "memories"}

    def _seed_counter_sync(
        self, conn: sqlite3.Connection, counter_name: str
    ) -> None:
        existing = conn.execute(
            "SELECT 1 FROM id_counters WHERE counter_name = ?",
            (counter_name,),
        ).fetchone()
        if existing is not None:
            return
        seed = 0
        table = self._COUNTER_SEED_TABLES.get(counter_name)
        if table is not None:
            row = conn.execute(
                f"SELECT COALESCE(MAX(id), 0) FROM {table}"
            ).fetchone()
            seed = int(row[0]) if row else 0
        conn.execute(
            "INSERT INTO id_counters (counter_name, last_value) "
            "VALUES (?, ?)",
            (counter_name, seed),
        )

    def _next_seq_sync(self, counter_name: str) -> int:
        with sqlite3.connect(self.db_path) as conn:
            self._seed_counter_sync(conn, counter_name)
            row = conn.execute(
                "UPDATE id_counters SET last_value = last_value + 1 "
                "WHERE counter_name = ? RETURNING last_value",
                (counter_name,),
            ).fetchone()
        return int(row[0])

    async def _next_memory_seq(self) -> int:
        return await asyncio.to_thread(self._next_seq_sync, "memory_seq")

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

    # ---- embeddings (DEV-1386) -------------------------------------------

    def _save_embedding_sync(self, row: Embedding) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO embeddings "
                "(canonical_id, embedding_model_name, entity_kind, "
                "content_hash, embedding, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    row.canonical_id,
                    row.embedding_model_name,
                    row.entity_kind,
                    row.content_hash,
                    json.dumps(row.embedding),
                    row.created_at.isoformat(),
                ),
            )

    async def save_embedding(self, row: Embedding) -> None:
        await asyncio.to_thread(self._save_embedding_sync, row)

    def _get_embedding_sync(
        self, canonical_id: str, embedding_model_name: str,
    ) -> Optional[Tuple]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT canonical_id, embedding_model_name, entity_kind, "
                "content_hash, embedding, created_at "
                "FROM embeddings "
                "WHERE canonical_id = ? AND embedding_model_name = ?",
                (canonical_id, embedding_model_name),
            ).fetchone()
        return row

    async def get_embedding(
        self, *, canonical_id: str, embedding_model_name: str,
    ) -> Optional[Embedding]:
        raw = await asyncio.to_thread(
            self._get_embedding_sync, canonical_id, embedding_model_name,
        )
        if raw is None:
            return None
        return Embedding.model_validate({
            "canonical_id": raw[0],
            "embedding_model_name": raw[1],
            "entity_kind": raw[2],
            "content_hash": raw[3],
            "embedding": json.loads(raw[4]),
            "created_at": raw[5],
        })

    def _list_embeddings_sync(self, embedding_model_name: str) -> List[Tuple]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT canonical_id, embedding_model_name, entity_kind, "
                "content_hash, embedding, created_at "
                "FROM embeddings "
                "WHERE embedding_model_name = ? "
                "ORDER BY canonical_id",
                (embedding_model_name,),
            ).fetchall()
        return rows

    async def list_embeddings(
        self, *, embedding_model_name: str,
    ) -> List[Embedding]:
        raws = await asyncio.to_thread(
            self._list_embeddings_sync, embedding_model_name,
        )
        return [
            Embedding.model_validate({
                "canonical_id": r[0],
                "embedding_model_name": r[1],
                "entity_kind": r[2],
                "content_hash": r[3],
                "embedding": json.loads(r[4]),
                "created_at": r[5],
            })
            for r in raws
        ]

    def _delete_embeddings_by_prefix_sync(self, prefix: str) -> int:
        with sqlite3.connect(self.db_path) as conn:
            # SQLite LIKE uses ``%`` for wildcards. ``GLOB`` would be
            # case-sensitive but doesn't support escaping; canonical ids
            # never legitimately contain ``%`` or ``_`` so we escape both
            # to be safe and use ESCAPE.
            escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            cursor = conn.execute(
                "DELETE FROM embeddings WHERE canonical_id LIKE ? ESCAPE '\\'",
                (escaped + "%",),
            )
            return int(cursor.rowcount or 0)

    async def delete_embeddings_for_canonical(
        self, *, canonical_id_prefix: str,
    ) -> int:
        return await asyncio.to_thread(
            self._delete_embeddings_by_prefix_sync, canonical_id_prefix,
        )
