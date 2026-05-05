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
