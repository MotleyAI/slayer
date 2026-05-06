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
from typing import List, Optional, Tuple

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.learnings.models import Learning, SavedQuery
from slayer.storage.base import StorageBackend, _validate_path_component
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
            # DEV-1357: learnings + saved queries.
            conn.execute("""
                CREATE TABLE IF NOT EXISTS learnings (
                    id TEXT PRIMARY KEY,
                    data TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS learning_entities (
                    learning_id TEXT NOT NULL REFERENCES learnings(id) ON DELETE CASCADE,
                    entity TEXT NOT NULL,
                    PRIMARY KEY (learning_id, entity)
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_learning_entities_entity "
                "ON learning_entities(entity)"
            )
            conn.execute("""
                CREATE TABLE IF NOT EXISTS saved_queries (
                    id TEXT PRIMARY KEY,
                    data TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS saved_query_entities (
                    query_id TEXT NOT NULL REFERENCES saved_queries(id) ON DELETE CASCADE,
                    entity TEXT NOT NULL,
                    PRIMARY KEY (query_id, entity)
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_saved_query_entities_entity "
                "ON saved_query_entities(entity)"
            )
            conn.execute("""
                CREATE TABLE IF NOT EXISTS id_counters (
                    counter_name TEXT PRIMARY KEY,
                    last_value INTEGER NOT NULL
                )
            """)

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
        _validate_path_component(name, kind="model name")
        if data_source is not None:
            _validate_path_component(data_source, kind="data_source")
        if data_source is None:
            identity = await self.resolve_model_identity(name)
            if identity is None:
                return None
            data_source, name = identity
        raw = await asyncio.to_thread(self._get_model_sync, data_source, name)
        return SlayerModel.model_validate(json.loads(raw)) if raw else None

    async def delete_model(
        self,
        name: str,
        data_source: Optional[str] = None,
    ) -> bool:
        _validate_path_component(name, kind="model name")
        if data_source is not None:
            _validate_path_component(data_source, kind="data_source")
        if data_source is None:
            identity = await self.resolve_model_identity(name)
            if identity is None:
                return False
            data_source, name = identity
        return await asyncio.to_thread(self._delete_model_sync, data_source, name)

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

    async def delete_datasource(self, name: str) -> bool:
        return await asyncio.to_thread(self._delete_datasource_sync, name)

    async def get_datasource_priority(self) -> List[str]:
        return await asyncio.to_thread(self._get_priority_sync)

    async def _set_datasource_priority_raw(self, priority: List[str]) -> None:
        await asyncio.to_thread(self._set_priority_sync, list(priority))

    # ---- learnings + saved queries (DEV-1357) -----------------------------

    def _next_seq_sync(self, counter_name: str) -> int:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO id_counters (counter_name, last_value) "
                "VALUES (?, 0)",
                (counter_name,),
            )
            row = conn.execute(
                "UPDATE id_counters SET last_value = last_value + 1 "
                "WHERE counter_name = ? RETURNING last_value",
                (counter_name,),
            ).fetchone()
        return int(row[0])

    async def _next_learning_seq(self) -> int:
        return await asyncio.to_thread(self._next_seq_sync, "learning_seq")

    async def _next_saved_query_seq(self) -> int:
        return await asyncio.to_thread(self._next_seq_sync, "saved_query_seq")

    def _save_learning_sync(self, learning: Learning) -> None:
        data = json.dumps(learning.model_dump(mode="json"))
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute(
                "INSERT OR REPLACE INTO learnings (id, data) VALUES (?, ?)",
                (learning.id, data),
            )
            conn.execute(
                "DELETE FROM learning_entities WHERE learning_id = ?",
                (learning.id,),
            )
            for entity in learning.entities:
                conn.execute(
                    "INSERT OR IGNORE INTO learning_entities "
                    "(learning_id, entity) VALUES (?, ?)",
                    (learning.id, entity),
                )

    async def _save_learning_row(self, learning: Learning) -> None:
        await asyncio.to_thread(self._save_learning_sync, learning)

    def _get_learning_sync(self, learning_id: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT data FROM learnings WHERE id = ?", (learning_id,)
            ).fetchone()
        return row[0] if row else None

    async def _get_learning_row(self, learning_id: str) -> Optional[Learning]:
        raw = await asyncio.to_thread(self._get_learning_sync, learning_id)
        return Learning.model_validate(json.loads(raw)) if raw else None

    def _list_learnings_sync(
        self, entities: Optional[List[str]]
    ) -> List[str]:
        with sqlite3.connect(self.db_path) as conn:
            if entities is None:
                rows = conn.execute(
                    "SELECT data FROM learnings ORDER BY id"
                ).fetchall()
            elif not entities:
                return []
            else:
                placeholders = ",".join("?" * len(entities))
                rows = conn.execute(
                    f"SELECT DISTINCT l.data FROM learnings l "
                    f"JOIN learning_entities le ON le.learning_id = l.id "
                    f"WHERE le.entity IN ({placeholders}) "
                    f"ORDER BY l.id",
                    tuple(entities),
                ).fetchall()
        return [r[0] for r in rows]

    async def _list_learnings_rows(
        self, *, entities: Optional[List[str]]
    ) -> List[Learning]:
        raws = await asyncio.to_thread(self._list_learnings_sync, entities)
        return [Learning.model_validate(json.loads(r)) for r in raws]

    def _delete_learning_sync(self, learning_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            cursor = conn.execute(
                "DELETE FROM learnings WHERE id = ?", (learning_id,)
            )
            return cursor.rowcount > 0

    async def _delete_learning_row(self, learning_id: str) -> bool:
        return await asyncio.to_thread(
            self._delete_learning_sync, learning_id
        )

    def _save_saved_query_sync(self, saved: SavedQuery) -> None:
        data = json.dumps(saved.model_dump(mode="json"))
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute(
                "INSERT OR REPLACE INTO saved_queries (id, data) VALUES (?, ?)",
                (saved.id, data),
            )
            conn.execute(
                "DELETE FROM saved_query_entities WHERE query_id = ?",
                (saved.id,),
            )
            for entity in saved.entities:
                conn.execute(
                    "INSERT OR IGNORE INTO saved_query_entities "
                    "(query_id, entity) VALUES (?, ?)",
                    (saved.id, entity),
                )

    async def _save_saved_query_row(self, saved: SavedQuery) -> None:
        await asyncio.to_thread(self._save_saved_query_sync, saved)

    def _get_saved_query_sync(self, query_id: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT data FROM saved_queries WHERE id = ?", (query_id,)
            ).fetchone()
        return row[0] if row else None

    async def _get_saved_query_row(
        self, query_id: str
    ) -> Optional[SavedQuery]:
        raw = await asyncio.to_thread(self._get_saved_query_sync, query_id)
        return SavedQuery.model_validate(json.loads(raw)) if raw else None

    def _list_saved_queries_sync(
        self, entities: Optional[List[str]]
    ) -> List[str]:
        with sqlite3.connect(self.db_path) as conn:
            if entities is None:
                rows = conn.execute(
                    "SELECT data FROM saved_queries ORDER BY id"
                ).fetchall()
            elif not entities:
                return []
            else:
                placeholders = ",".join("?" * len(entities))
                rows = conn.execute(
                    f"SELECT DISTINCT sq.data FROM saved_queries sq "
                    f"JOIN saved_query_entities sqe ON sqe.query_id = sq.id "
                    f"WHERE sqe.entity IN ({placeholders}) "
                    f"ORDER BY sq.id",
                    tuple(entities),
                ).fetchall()
        return [r[0] for r in rows]

    async def _list_saved_queries_rows(
        self, *, entities: Optional[List[str]]
    ) -> List[SavedQuery]:
        raws = await asyncio.to_thread(
            self._list_saved_queries_sync, entities
        )
        return [SavedQuery.model_validate(json.loads(r)) for r in raws]

    def _delete_saved_query_sync(self, query_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            cursor = conn.execute(
                "DELETE FROM saved_queries WHERE id = ?", (query_id,)
            )
            return cursor.rowcount > 0

    async def _delete_saved_query_row(self, query_id: str) -> bool:
        return await asyncio.to_thread(self._delete_saved_query_sync, query_id)
