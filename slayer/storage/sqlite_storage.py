"""SQLite-based storage for models and datasources.

v4 (DEV-1330): the ``models`` table has a composite ``(data_source, name)``
primary key so two datasources can share a table name without collision. A
``settings`` table stores singleton state — currently just the datasource
priority list used to disambiguate bare-name lookups. ``migrate_sqlite_schema``
runs at open time to upgrade legacy v3 single-PK databases in place.
"""

import asyncio
import json
import os
import sqlite3
from typing import Any

from slayer.core.errors import IdCollisionError
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.memories.models import Memory, _validate_memory_id_charset
from slayer.storage.base import (
    StorageBackend,
    _find_case_colliding_id,
    _validate_path_component,
    _write_sample_fields,
)
from slayer.storage.sidecar_embedding_store import (
    SidecarEmbeddingsMixin,
    SidecarEmbeddingStore,
)
from slayer.storage.v4_migration import migrate_sqlite_schema


_PRIORITY_KEY = "datasource_priority"
_PRAGMA_FOREIGN_KEYS_ON = "PRAGMA foreign_keys = ON"


class SQLiteStorage(SidecarEmbeddingsMixin, StorageBackend):
    def __init__(self, db_path: str):
        self.db_path = db_path
        # Idempotent: rebuilds a v3 ``models`` table if needed; no-op on v4.
        migrate_sqlite_schema(db_path)
        self._migrate_memories_to_text_pk()
        self._init_db()
        # DEV-1386 / DEV-1405: the embeddings sidecar owns its own table
        # + index. CREATE-IF-NOT-EXISTS makes co-existence with our own
        # schema trivial.
        self._embeddings_store = SidecarEmbeddingStore(db_path=self.db_path)

    def _migrate_memories_to_text_pk(self) -> None:
        """DEV-1428: rebuild pre-DEV-1428 ``memories`` / ``memory_entities``
        tables whose primary key / foreign key columns are INTEGER so
        string ids can be stored.

        Idempotent under partial state: a crash after the first rebuild
        but before the second leaves ``memories.id`` as TEXT and
        ``memory_entities.memory_id`` still INTEGER. The next startup
        must recognise the split state and finish the migration — so we
        check each table independently, and combine both rebuilds into
        a single transaction whenever both need migrating.
        """
        with sqlite3.connect(self.db_path) as conn:
            memories_needs_rebuild = self._memories_needs_text_pk(conn)
            me_needs_rebuild = self._memory_entities_needs_text_fk(conn)
            if not memories_needs_rebuild and not me_needs_rebuild:
                return
            # Combine into one transaction so the cross-table FK rebuild
            # is atomic. Each ALTER/INSERT/DROP gated by the per-table
            # flags above; an empty branch is a no-op (no DDL emitted).
            script_parts: list[str] = ["BEGIN;"]
            if memories_needs_rebuild:
                script_parts.append(
                    "ALTER TABLE memories RENAME TO _memories_legacy;"
                )
                script_parts.append(
                    "CREATE TABLE memories ("
                    "id TEXT PRIMARY KEY, data TEXT NOT NULL);"
                )
                script_parts.append(
                    "INSERT INTO memories (id, data) "
                    "SELECT CAST(id AS TEXT), data FROM _memories_legacy;"
                )
                script_parts.append("DROP TABLE _memories_legacy;")
            if me_needs_rebuild:
                script_parts.append(
                    "ALTER TABLE memory_entities RENAME TO _memory_entities_legacy;"
                )
                script_parts.append(
                    "CREATE TABLE memory_entities ("
                    "memory_id TEXT NOT NULL REFERENCES memories(id) "
                    "ON DELETE CASCADE, "
                    "entity TEXT NOT NULL, "
                    "PRIMARY KEY (memory_id, entity));"
                )
                script_parts.append(
                    "INSERT INTO memory_entities (memory_id, entity) "
                    "SELECT CAST(memory_id AS TEXT), entity "
                    "FROM _memory_entities_legacy;"
                )
                script_parts.append("DROP TABLE _memory_entities_legacy;")
                script_parts.append(
                    "CREATE INDEX IF NOT EXISTS idx_memory_entities_entity "
                    "ON memory_entities(entity);"
                )
            script_parts.append("COMMIT;")
            conn.executescript("\n".join(script_parts))

    @staticmethod
    def _memories_needs_text_pk(conn: sqlite3.Connection) -> bool:
        cur = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='memories'"
        )
        if cur.fetchone() is None:
            return False
        cur = conn.execute("PRAGMA table_info(memories)")
        cols = cur.fetchall()
        id_col = next((c for c in cols if c[1] == "id"), None)
        return id_col is not None and id_col[2].upper() != "TEXT"

    @staticmethod
    def _memory_entities_needs_text_fk(conn: sqlite3.Connection) -> bool:
        cur = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='memory_entities'"
        )
        if cur.fetchone() is None:
            return False
        cur = conn.execute("PRAGMA table_info(memory_entities)")
        me_cols = cur.fetchall()
        me_col = next((c for c in me_cols if c[1] == "memory_id"), None)
        return me_col is not None and me_col[2].upper() != "TEXT"

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
            # DEV-1357 v2 / DEV-1428: unified memories with string ids.
            conn.execute("""
                CREATE TABLE IF NOT EXISTS memories (
                    id TEXT PRIMARY KEY,
                    data TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS memory_entities (
                    memory_id TEXT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
                    entity TEXT NOT NULL,
                    PRIMARY KEY (memory_id, entity)
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_memory_entities_entity "
                "ON memory_entities(entity)"
            )

    # ---- graph fingerprint -------------------------------------------------

    async def graph_fingerprint(self) -> str:
        """mtime of the SQLite database file as a string.

        Any write to the database changes the file's mtime, so this is
        an O(1) staleness check — no table scans required.
        """
        return str(os.path.getmtime(self.db_path))

    # --- Sync helpers (run in thread to avoid blocking the event loop) ---

    def _save_model_sync(self, model: SlayerModel) -> None:
        data = json.dumps(model.model_dump(mode="json", exclude_none=True))
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO models (data_source, name, data) VALUES (?, ?, ?)",
                (model.data_source, model.name, data),
            )

    def _list_all_identities_sync(self) -> list[tuple[str, str]]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT data_source, name FROM models ORDER BY data_source, name"
            ).fetchall()
        return [(r[0], r[1]) for r in rows]

    def _get_model_sync(self, data_source: str, name: str) -> str | None:
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

    def _get_datasource_sync(self, name: str) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT data FROM datasources WHERE name = ?", (name,)
            ).fetchone()
        return row[0] if row else None

    def _list_datasources_sync(self) -> list[str]:
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

    def _get_priority_sync(self) -> list[str]:
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

    def _set_priority_sync(self, priority: list[str]) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (_PRIORITY_KEY, json.dumps(list(priority))),
            )

    # --- Async interface ---

    async def _save_model_impl(self, model: SlayerModel) -> None:
        await asyncio.to_thread(self._save_model_sync, model)

    async def _list_all_model_identities(self) -> list[tuple[str, str]]:
        return await asyncio.to_thread(self._list_all_identities_sync)

    async def get_model(
        self,
        name: str,
        data_source: str | None = None,
    ) -> SlayerModel | None:
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
        column_name: str, sampled: str | None,
        sampled_values: list[str] | None,
        distinct_count: int | None,
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
                    _write_sample_fields(
                        col,
                        sampled=sampled,
                        sampled_values=sampled_values,
                        distinct_count=distinct_count,
                    )
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
        sampled: str | None,
        sampled_values: list[str] | None,
        distinct_count: int | None,
    ) -> None:
        await asyncio.to_thread(
            self._update_column_sampled_sync,
            data_source=data_source, model_name=model_name,
            column_name=column_name, sampled=sampled,
            sampled_values=sampled_values, distinct_count=distinct_count,
        )

    async def save_datasource(self, datasource: DatasourceConfig) -> None:
        await self.check_datasource_id_collision(datasource.name)
        await asyncio.to_thread(self._save_datasource_sync, datasource)

    async def get_datasource(self, name: str) -> DatasourceConfig | None:
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

    async def list_datasources(self) -> list[str]:
        return await asyncio.to_thread(self._list_datasources_sync)

    async def _delete_datasource_row(self, name: str) -> bool:
        return await asyncio.to_thread(self._delete_datasource_sync, name)

    async def get_datasource_priority(self) -> list[str]:
        return await asyncio.to_thread(self._get_priority_sync)

    async def _set_datasource_priority_raw(self, priority: list[str]) -> None:
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

    @staticmethod
    def _is_int_shaped_id(value: Any) -> bool:
        """DEV-1428 allocator predicate: pure-digit, no-leading-zero
        string. ``"0"`` counts; ``"001"`` / ``"42abc"`` do not."""
        if not isinstance(value, str) or not value:
            return False
        if not value.isdigit():
            return False
        if value != "0" and value.startswith("0"):
            return False
        return True

    def _save_memory_atomic_sync(
        self,
        *,
        memory_id: str | None,
        learning: str,
        entities: list[str],
        query: SlayerQuery | None,
        description: str | None = None,
    ) -> Memory:
        """Reserve / accept an id and persist the new memory inside one
        SQLite transaction. Returns the persisted :class:`Memory`.

        DEV-1428: ``memory_id=None`` triggers allocator (max int-shaped
        id + 1); ``memory_id="..."`` is a user-supplied id — upserts on
        collision, preserving ``created_at``.

        Concurrency: opens with ``isolation_level=None`` and starts an
        explicit ``BEGIN IMMEDIATE`` so the SELECT-then-INSERT sequence
        runs under SQLite's write lock. Two concurrent saves block each
        other rather than racing on a stale max.
        """
        # ``isolation_level=None`` puts us in autocommit; the explicit
        # BEGIN IMMEDIATE acquires a write lock at the start of the
        # transaction (cf. SQLite's default deferred BEGIN, which only
        # promotes to a write lock when a write actually happens — by
        # which time another writer may have updated ``memories``).
        conn = sqlite3.connect(self.db_path, isolation_level=None, timeout=30.0)
        try:
            conn.execute(_PRAGMA_FOREIGN_KEYS_ON)
            conn.execute("BEGIN IMMEDIATE")
            try:
                preserved_created_at = None
                if memory_id is None:
                    memory_id = self._next_memory_seq_sync_from_conn(conn)
                else:
                    # Duplicated here because this override bypasses the
                    # base save_memory template.
                    ids = [
                        row[0] if isinstance(row[0], str) else str(row[0])
                        for row in conn.execute("SELECT id FROM memories")
                    ]
                    collide = _find_case_colliding_id(memory_id, ids)
                    if collide is not None:
                        raise IdCollisionError(
                            kind="memory",
                            new_id=memory_id,
                            existing_id=collide,
                        )
                    existing_row = conn.execute(
                        "SELECT data FROM memories WHERE id = ?",
                        (memory_id,),
                    ).fetchone()
                    if existing_row is not None:
                        existing_memory = Memory.model_validate(
                            json.loads(existing_row[0])
                        )
                        preserved_created_at = existing_memory.created_at
                kwargs: dict[str, Any] = {
                    "id": memory_id,
                    "learning": learning,
                    "description": description,
                    "entities": list(entities),
                    "query": query,
                }
                if preserved_created_at is not None:
                    kwargs["created_at"] = preserved_created_at
                memory = Memory(**kwargs)
                conn.execute(
                    "INSERT OR REPLACE INTO memories (id, data) VALUES (?, ?)",
                    (memory_id, json.dumps(memory.model_dump(mode="json"))),
                )
                conn.execute(
                    "DELETE FROM memory_entities WHERE memory_id = ?",
                    (memory_id,),
                )
                for entity in entities:
                    conn.execute(
                        "INSERT OR IGNORE INTO memory_entities "
                        "(memory_id, entity) VALUES (?, ?)",
                        (memory_id, entity),
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        return memory

    def _next_memory_seq_sync_from_conn(
        self, conn: sqlite3.Connection,
    ) -> str:
        """Allocator on an open connection (so it can share a write lock
        with the surrounding transaction)."""
        rows = conn.execute("SELECT id FROM memories").fetchall()
        max_id = 0
        for (raw,) in rows:
            value = raw if isinstance(raw, str) else str(raw)
            if self._is_int_shaped_id(value):
                max_id = max(max_id, int(value))
        return str(max_id + 1)

    async def save_memory(
        self,
        *,
        learning: str,
        entities: list[str],
        query: SlayerQuery | None = None,
        id: str | None = None,  # noqa: A002 — public kwarg
        description: str | None = None,
    ) -> Memory:
        if id is not None:
            _validate_memory_id_charset(id)
        return await asyncio.to_thread(
            self._save_memory_atomic_sync,
            memory_id=id,
            learning=learning,
            entities=list(entities),
            query=query,
            description=description,
        )

    # ``_save_memory_row`` and ``_next_memory_seq`` are kept to satisfy
    # the ABC contract (third-party code or the cascade-strip path that
    # bypasses the public ``save_memory`` API still expect these
    # primitives). The cascade write path calls ``_save_memory_row``
    # directly to avoid triggering ``EmbeddingRetriever.upsert_memory``.

    def _save_memory_sync(self, memory: Memory) -> None:
        data = json.dumps(memory.model_dump(mode="json"))
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(_PRAGMA_FOREIGN_KEYS_ON)
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

    def _next_memory_seq_sync(self) -> str:
        with sqlite3.connect(self.db_path) as conn:
            return self._next_memory_seq_sync_from_conn(conn)

    async def _next_memory_seq(self) -> str:
        return await asyncio.to_thread(self._next_memory_seq_sync)

    def _list_memory_ids_sync(self) -> list[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT id FROM memories").fetchall()
        return [r[0] if isinstance(r[0], str) else str(r[0]) for r in rows]

    async def _list_memory_ids(self) -> list[str]:
        return await asyncio.to_thread(self._list_memory_ids_sync)

    def _get_memory_sync(self, memory_id: str) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT data FROM memories WHERE id = ?", (memory_id,)
            ).fetchone()
        return row[0] if row else None

    async def _get_memory_row(self, memory_id: str) -> Memory | None:
        raw = await asyncio.to_thread(self._get_memory_sync, memory_id)
        return Memory.model_validate(json.loads(raw)) if raw else None

    def _list_memories_sync(
        self, entities: list[str] | None
    ) -> list[str]:
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
        self, *, entities: list[str] | None
    ) -> list[Memory]:
        raws = await asyncio.to_thread(self._list_memories_sync, entities)
        return [Memory.model_validate(json.loads(r)) for r in raws]

    def _delete_memory_sync(self, memory_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(_PRAGMA_FOREIGN_KEYS_ON)
            cursor = conn.execute(
                "DELETE FROM memories WHERE id = ?", (memory_id,)
            )
            return cursor.rowcount > 0

    async def _delete_memory_row(self, memory_id: str) -> bool:
        return await asyncio.to_thread(self._delete_memory_sync, memory_id)

    # Embedding CRUD lives in :class:`SidecarEmbeddingsMixin`, which
    # forwards to ``self._embeddings_store`` set in ``__init__`` above.
    # The mixin owns the SQL once and both backends consume it — see
    # ``slayer/storage/sidecar_embedding_store.py``.
