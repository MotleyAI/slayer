"""SQLite-based storage for models and datasources."""

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

    async def save_model(self, model: SlayerModel) -> None:
        data = json.dumps(model.model_dump(mode="json", exclude_none=True))
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO models (name, data) VALUES (?, ?)",
                (model.name, data),
            )

    async def get_model(self, name: str) -> Optional[SlayerModel]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT data FROM models WHERE name = ?", (name,)).fetchone()
        if row is None:
            return None
        return SlayerModel.model_validate(json.loads(row[0]))

    async def list_models(self) -> List[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT name FROM models ORDER BY name").fetchall()
        return [r[0] for r in rows]

    async def delete_model(self, name: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM models WHERE name = ?", (name,))
            return cursor.rowcount > 0

    async def save_datasource(self, datasource: DatasourceConfig) -> None:
        data = json.dumps(datasource.model_dump(mode="json", exclude_none=True))
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO datasources (name, data) VALUES (?, ?)",
                (datasource.name, data),
            )

    async def get_datasource(self, name: str) -> Optional[DatasourceConfig]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT data FROM datasources WHERE name = ?", (name,)).fetchone()
        if row is None:
            return None
        ds = DatasourceConfig.model_validate(json.loads(row[0]))
        return ds.resolve_env_vars()

    async def list_datasources(self) -> List[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT name FROM datasources ORDER BY name").fetchall()
        return [r[0] for r in rows]

    async def delete_datasource(self, name: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM datasources WHERE name = ?", (name,))
            return cursor.rowcount > 0
