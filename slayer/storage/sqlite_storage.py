"""SQLite-based storage for models and datasources."""

import asyncio
import json
import sqlite3
from typing import List, Optional

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.storage.base import StorageBackend


class SQLiteStorage(StorageBackend):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS models (
                    name TEXT PRIMARY KEY,
                    data TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS datasources (
                    name TEXT PRIMARY KEY,
                    data TEXT NOT NULL
                )
            """)

    # --- Sync helpers (run in thread to avoid blocking the event loop) ---

    def _save_model_sync(self, model: SlayerModel) -> None:
        data = json.dumps(model.model_dump(mode="json", exclude_none=True))
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT OR REPLACE INTO models (name, data) VALUES (?, ?)", (model.name, data))

    def _get_model_sync(self, name: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT data FROM models WHERE name = ?", (name,)).fetchone()
        return row[0] if row else None

    def _list_models_sync(self) -> List[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT name FROM models ORDER BY name").fetchall()
        return [r[0] for r in rows]

    def _delete_model_sync(self, name: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM models WHERE name = ?", (name,))
            return cursor.rowcount > 0

    def _save_datasource_sync(self, datasource: DatasourceConfig) -> None:
        data = json.dumps(datasource.model_dump(mode="json", exclude_none=True))
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO datasources (name, data) VALUES (?, ?)", (datasource.name, data)
            )

    def _get_datasource_sync(self, name: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT data FROM datasources WHERE name = ?", (name,)).fetchone()
        return row[0] if row else None

    def _list_datasources_sync(self) -> List[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT name FROM datasources ORDER BY name").fetchall()
        return [r[0] for r in rows]

    def _delete_datasource_sync(self, name: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM datasources WHERE name = ?", (name,))
            return cursor.rowcount > 0

    # --- Async interface ---

    async def save_model(self, model: SlayerModel) -> None:
        await asyncio.to_thread(self._save_model_sync, model)

    async def get_model(self, name: str) -> Optional[SlayerModel]:
        raw = await asyncio.to_thread(self._get_model_sync, name)
        return SlayerModel.model_validate(json.loads(raw)) if raw else None

    async def list_models(self) -> List[str]:
        return await asyncio.to_thread(self._list_models_sync)

    async def delete_model(self, name: str) -> bool:
        return await asyncio.to_thread(self._delete_model_sync, name)

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
