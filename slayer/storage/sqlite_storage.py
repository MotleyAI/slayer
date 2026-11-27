"""SQLite-based storage for datasets and datasources.

v4: the ``models`` table has a composite ``(data_source, name)``
primary key so two datasources can share a table name without collision. A
``settings`` table stores singleton state — currently just the datasource
priority list used to disambiguate bare-name lookups. ``migrate_sqlite_schema``
runs at open time to upgrade legacy v3 single-PK databases in place.
"""

import asyncio
import json
import os
import sqlite3
from collections.abc import Callable, Hashable, Sequence
from typing import Any

from sqlglot import exp

from slayer.core.errors import StoredDocumentLoadError
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.memories.models import Memory, _validate_memory_id_charset
from slayer.storage.base import (
    DATASOURCE_NAME,
    StorageBackend,
    _validate_path_component,
    _write_sample_fields,
    without_none,
)
from slayer.storage.content_fingerprint import stat_key
from slayer.storage.document_loading import Loaded, stored_document_boundary
from slayer.storage.folder_documents import FolderDocuments
from slayer.storage.stored_layout import Folder, FolderWrite
from slayer.storage.sidecar_embedding_store import (
    SidecarEmbeddingsMixin,
    SidecarEmbeddingStore,
)
from slayer.storage.sqlite_conn import open_connection, transaction
from slayer.storage.migrations import stamp_stored
from slayer.storage.v4_migration import migrate_sqlite_schema
from slayer.storage.document_keys import keys_for


_PRIORITY_KEY = "datasource_priority"
_PRAGMA_FOREIGN_KEYS_ON = "PRAGMA foreign_keys = ON"
_BEGIN_IMMEDIATE = "BEGIN IMMEDIATE"
_SELECT_DATASET = "SELECT data FROM models WHERE data_source = ? AND name = ?"
_UPDATE_DATASET = "UPDATE models SET data = ? WHERE data_source = ? AND name = ?"
_SELECT_MEMORY = "SELECT data FROM memories WHERE id = ?"
_UPDATE_MEMORY = "UPDATE memories SET data = ? WHERE id = ?"
_SELECT_DATASOURCE = "SELECT data FROM datasources WHERE name = ?"
_UPDATE_DATASOURCE = "UPDATE datasources SET data = ? WHERE name = ?"
#: Each stored document kind's row ``data`` read and update, keyed by its identity columns.
_ROW_DATA_STATEMENTS = {
    "dataset": (_SELECT_DATASET, _UPDATE_DATASET),
    "memory": (_SELECT_MEMORY, _UPDATE_MEMORY),
    "datasource": (_SELECT_DATASOURCE, _UPDATE_DATASOURCE),
}


def _decoded_row(raw: str) -> dict | None:
    """A ``models`` row's stored dict; ``None`` when corrupt or not a mapping."""
    try:
        data = json.loads(raw)
    except ValueError:
        return None
    return data if isinstance(data, dict) else None


def _legacy_file(name: str) -> str:
    """The file name a 2.0 ``semantic_models`` row reads under: the 2.0 YAML store's."""
    return f"{name}.yaml"


class SQLiteStorage(SidecarEmbeddingsMixin, FolderDocuments, StorageBackend):
    def __init__(self, db_path: str):
        self.db_path = db_path
        # Idempotent: rebuilds a v3 ``models`` table if needed; no-op on v4.
        migrate_sqlite_schema(db_path)
        self._migrate_memories_to_text_pk()
        self._init_db()
        # The embeddings sidecar owns its own table
        # + index. CREATE-IF-NOT-EXISTS makes co-existence with our own
        # schema trivial.
        self._embeddings_store = SidecarEmbeddingStore(db_path=self.db_path)

    def _migrate_memories_to_text_pk(self) -> None:
        """Rebuild legacy ``memories`` / ``memory_entities``
        tables whose primary key / foreign key columns are INTEGER so
        string ids can be stored.

        Idempotent under partial state: a crash after the first rebuild
        but before the second leaves ``memories.id`` as TEXT and
        ``memory_entities.memory_id`` still INTEGER. The next startup
        must recognise the split state and finish the migration — so we
        check each table independently, and combine both rebuilds into
        a single transaction whenever both need migrating.
        """
        with transaction(self.db_path) as conn:
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
        with transaction(self.db_path) as conn:
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
                CREATE TABLE IF NOT EXISTS semantic_models (
                    data_source TEXT NOT NULL,
                    name TEXT NOT NULL,
                    text TEXT NOT NULL,
                    PRIMARY KEY (data_source, name)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    data_source TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    text TEXT NOT NULL,
                    PRIMARY KEY (data_source, file_name)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ontologies (
                    name TEXT PRIMARY KEY,
                    text TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ontology_mappings (
                    data_source TEXT NOT NULL,
                    name TEXT NOT NULL,
                    text TEXT NOT NULL,
                    PRIMARY KEY (data_source, name)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            # Unified memories with string ids.
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

    # ---- cache keys --------------------------------------------------------

    async def cache_identity(self) -> Hashable:
        return os.path.abspath(self.db_path)

    async def graph_fingerprint(self) -> str | None:
        """The header's file change counter, which every committed write bumps (rollback-journal mode), plus the file's stat."""
        return await asyncio.to_thread(self._fingerprint_sync)

    # --- Sync helpers (run in thread to avoid blocking the event loop) ---

    def _fingerprint_sync(self) -> str:
        with open(self.db_path, "rb") as f:
            counter = f.read(28)[24:28]
        return f"{counter.hex()}:{stat_key(self.db_path)}"

    def _save_dataset_sync(self, dataset: SlayerDataset) -> None:
        data = json.dumps(without_none(dataset.model_dump(mode="json")))
        with transaction(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO models (data_source, name, data) VALUES (?, ?, ?)",
                (dataset.data_source, dataset.name, data),
            )

    def _list_all_identities_sync(self) -> list[tuple[str, str]]:
        with transaction(self.db_path) as conn:
            rows = conn.execute(
                "SELECT data_source, name FROM models ORDER BY data_source, name"
            ).fetchall()
        return [(r[0], r[1]) for r in rows]

    def _get_dataset_sync(self, data_source: str, name: str) -> str | None:
        with transaction(self.db_path) as conn:
            row = conn.execute(
                _SELECT_DATASET,
                (data_source, name),
            ).fetchone()
        return row[0] if row else None

    def _delete_dataset_sync(self, data_source: str, name: str) -> bool:
        with transaction(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM models WHERE data_source = ? AND name = ?",
                (data_source, name),
            )
            return cursor.rowcount > 0

    # ---- the stored format: one folder of Ossie documents per datasource ---

    @staticmethod
    def _folder_from(conn: sqlite3.Connection, data_source: str) -> Folder:
        """``data_source``'s documents; a 2.0 ``semantic_models`` row reads as ``<name>.yaml``."""
        texts = {f: t for f, t in conn.execute("SELECT file_name, text FROM documents WHERE data_source = ?",
                                               (data_source,))}
        legacy: list[str] = []
        for name, text in conn.execute("SELECT name, text FROM semantic_models WHERE data_source = ?", (data_source,)):
            if _legacy_file(name) not in texts:
                texts[_legacy_file(name)] = text
                legacy.append(_legacy_file(name))
        return Folder(data_source=data_source, texts=texts, legacy_files=legacy)

    def _read_folder_sync(self, data_source: str) -> Folder:
        with transaction(self.db_path) as conn:
            return self._folder_from(conn, data_source)

    async def _read_folder(self, data_source: str) -> Folder:
        return await asyncio.to_thread(self._read_folder_sync, data_source)

    def _folder_data_sources_sync(self) -> list[str]:
        with transaction(self.db_path) as conn:
            rows = conn.execute("SELECT data_source FROM documents UNION SELECT data_source FROM semantic_models "
                                "ORDER BY data_source").fetchall()
        return [r[0] for r in rows]

    async def _folder_data_sources(self) -> list[str]:
        return await asyncio.to_thread(self._folder_data_sources_sync)

    def _write_folder_sync(self, data_source: str, plan: Callable[[Folder], FolderWrite],
                           legacy: Sequence[str]) -> None:
        with open_connection(self.db_path, isolation_level=None) as conn:
            conn.execute(_BEGIN_IMMEDIATE)
            try:
                folder = self._folder_from(conn, data_source)
                raws = {}
                for name in legacy:
                    row = conn.execute(_SELECT_DATASET, (data_source, name)).fetchone()
                    raws[name] = _decoded_row(row[0]) if row else None
                write = plan(folder.model_copy(update={"legacy": raws}))
                for file, text in write.ordered_writes():
                    conn.execute("INSERT OR REPLACE INTO documents (data_source, file_name, text) VALUES (?, ?, ?)",
                                 (data_source, file, text))
                for file in [f for f, _ in write.ordered_writes()] + write.deletes:
                    if file.endswith(".yaml"):
                        conn.execute("DELETE FROM semantic_models WHERE data_source = ? AND name = ?",
                                     (data_source, file.removesuffix(".yaml")))
                for file in write.deletes:
                    conn.execute("DELETE FROM documents WHERE data_source = ? AND file_name = ?", (data_source, file))
                for name in write.retired:
                    conn.execute("DELETE FROM models WHERE data_source = ? AND name = ?", (data_source, name))
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")

    async def _write_folder(self, data_source: str, plan: Callable[[Folder], FolderWrite], *,
                            legacy: Sequence[str] = ()) -> None:
        await asyncio.to_thread(self._write_folder_sync, data_source, plan, legacy)

    # ---- ontology and mapping documents ------------------------------------

    def _write_sync(self, sql: str, params: tuple[str, ...]) -> int:
        with transaction(self.db_path) as conn:
            return conn.execute(sql, params).rowcount

    def _rows_sync(self, sql: str, params: tuple[str, ...] = ()) -> list[tuple[Any, ...]]:
        with transaction(self.db_path) as conn:
            return [tuple(r) for r in conn.execute(sql, params).fetchall()]

    async def _save_ontology_text_impl(self, *, name: str, text: str) -> None:
        await asyncio.to_thread(self._write_sync, "INSERT OR REPLACE INTO ontologies (name, text) VALUES (?, ?)",
                                (name, text))

    async def _get_ontology_text_impl(self, *, name: str) -> str | None:
        rows = await asyncio.to_thread(self._rows_sync, "SELECT text FROM ontologies WHERE name = ?", (name,))
        return rows[0][0] if rows else None

    async def _list_ontology_names(self) -> list[str]:
        return [r[0] for r in await asyncio.to_thread(self._rows_sync, "SELECT name FROM ontologies ORDER BY name")]

    async def _delete_ontology_row(self, *, name: str) -> bool:
        return await asyncio.to_thread(self._write_sync, "DELETE FROM ontologies WHERE name = ?", (name,)) > 0

    async def _save_ontology_mapping_text_impl(self, *, data_source: str, name: str, text: str) -> None:
        await asyncio.to_thread(
            self._write_sync, "INSERT OR REPLACE INTO ontology_mappings (data_source, name, text) VALUES (?, ?, ?)",
            (data_source, name, text))

    async def _get_ontology_mapping_text_impl(self, *, data_source: str, name: str) -> str | None:
        rows = await asyncio.to_thread(
            self._rows_sync, "SELECT text FROM ontology_mappings WHERE data_source = ? AND name = ?", (data_source, name))
        return rows[0][0] if rows else None

    async def _list_ontology_mapping_identities(self) -> list[tuple[str, str]]:
        rows = await asyncio.to_thread(
            self._rows_sync, "SELECT data_source, name FROM ontology_mappings ORDER BY data_source, name")
        return [(r[0], r[1]) for r in rows]

    async def _delete_ontology_mapping_row(self, *, data_source: str, name: str) -> bool:
        deleted = await asyncio.to_thread(
            self._write_sync, "DELETE FROM ontology_mappings WHERE data_source = ? AND name = ?", (data_source, name))
        return deleted > 0

    def _save_datasource_sync(self, datasource: DatasourceConfig) -> None:
        data = json.dumps(without_none(datasource.model_dump(mode="json")))
        with transaction(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO datasources (name, data) VALUES (?, ?)",
                (datasource.name, data),
            )

    def _get_datasource_sync(self, name: str) -> str | None:
        with transaction(self.db_path) as conn:
            row = conn.execute(
                _SELECT_DATASOURCE, (name,)
            ).fetchone()
        return row[0] if row else None

    def _list_datasources_sync(self) -> list[str]:
        with transaction(self.db_path) as conn:
            rows = conn.execute(
                "SELECT name FROM datasources ORDER BY name"
            ).fetchall()
        return [r[0] for r in rows]

    def _delete_datasource_sync(self, name: str) -> bool:
        with transaction(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM datasources WHERE name = ?", (name,)
            )
            return cursor.rowcount > 0

    def _get_priority_sync(self) -> list[str]:
        with transaction(self.db_path) as conn:
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
        with transaction(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (_PRIORITY_KEY, json.dumps(list(priority))),
            )

    # --- Async interface ---

    async def _save_dataset_impl(self, dataset: SlayerDataset) -> None:
        await asyncio.to_thread(self._save_dataset_sync, dataset)

    async def _list_all_dataset_identities(self) -> list[tuple[str, str]]:
        return await asyncio.to_thread(self._list_all_identities_sync)

    async def get_dataset(
        self,
        name: str,
        data_source: str | None = None,
    ) -> SlayerDataset | None:
        target = await self._resolve_target_or_none(name, data_source=data_source)
        if target is None:
            return None
        data_source, name = target
        raw = await asyncio.to_thread(self._get_dataset_sync, data_source, name)
        if not raw:
            return None
        with stored_document_boundary(kind="dataset", name=name, data_source=data_source):
            return await self._migrate_and_refine_on_load(
                name=name, data=json.loads(raw), data_source=data_source,
            )

    async def _load_raw_dataset_dict(
        self, *, name: str, data_source: str,
    ) -> dict | None:
        """Read the stored JSON verbatim (no migration / no
        validation) for sibling-hop resolution during the legacy-``__``
        rewrite. Returns ``None`` when the row is absent, corrupt or not a mapping."""
        raw = await asyncio.to_thread(self._get_dataset_sync, data_source, name)
        return _decoded_row(raw) if raw else None

    def _raw_rows_sync(self, sql: str) -> list[tuple[str, dict]]:
        with transaction(self.db_path) as conn:
            rows = conn.execute(sql).fetchall()
        out = []
        for key, raw in rows:
            try:
                data = json.loads(raw)
            except ValueError:
                continue
            if isinstance(data, dict):
                out.append((key, data))
        return out

    def _update_sync(self, sql: str, params: tuple) -> None:
        with transaction(self.db_path) as conn:
            conn.execute(sql, params)

    async def _write_raw_dataset_dict(self, *, name: str, data_source: str, data: dict) -> None:
        await asyncio.to_thread(
            self._update_sync, _UPDATE_DATASET,
            (json.dumps(data), data_source, name),
        )
        self._forget_raw(data_source=data_source, name=name)

    async def _raw_datasource_dicts(self) -> dict[str, dict]:
        return dict(await asyncio.to_thread(self._raw_rows_sync, "SELECT name, data FROM datasources ORDER BY name"))

    def _write_if_unchanged_sync(self, *, kind: str, params: tuple[str, ...], expected: dict, data: dict) -> bool:
        """``UPDATE`` one row's ``data`` in the transaction that checks it still reads ``expected``."""
        select, update = _ROW_DATA_STATEMENTS[kind]
        with open_connection(self.db_path, isolation_level=None) as conn:
            conn.execute(_BEGIN_IMMEDIATE)
            try:
                row = conn.execute(select, params).fetchone()
                try:
                    current = json.loads(row[0]) if row else None
                except ValueError:
                    current = None
                unchanged = current == expected
                if unchanged:
                    conn.execute(update, (json.dumps(data), *params))
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        return unchanged

    async def _write_raw_if_unchanged(
        self, *, kind: str, data_source: str | None, name: str, expected: dict, data: dict,
    ) -> bool:
        if kind == "dataset" and data_source is not None:
            row, params = "dataset", (data_source, name)
        else:
            row, params = ("memory" if kind == "memory" else "datasource"), (name,)
        written = await asyncio.to_thread(self._write_if_unchanged_sync, kind=row, params=params, expected=expected,
                                          data=data)
        if written and kind == "dataset" and data_source is not None:
            self._forget_raw(data_source=data_source, name=name)
        return written

    async def _write_raw_datasource_dict(self, *, name: str, data: dict) -> None:
        await asyncio.to_thread(self._update_sync, _UPDATE_DATASOURCE, (json.dumps(data), name))

    async def _raw_memory_dicts(self) -> dict[str, dict]:
        return dict(await asyncio.to_thread(self._raw_rows_sync, "SELECT id, data FROM memories ORDER BY id"))

    async def _write_raw_memory_dict(self, *, memory_id: str, data: dict) -> None:
        await asyncio.to_thread(self._update_sync, _UPDATE_MEMORY, (json.dumps(data), memory_id))

    async def _delete_dataset_row(
        self, *, data_source: str, name: str,
    ) -> bool:
        return await asyncio.to_thread(self._delete_dataset_sync, data_source, name)

    def _update_field_sampled_sync(
        self, *, data_source: str, dataset_name: str,
        field_name: str, sampled: str | None,
        sampled_values: list[str] | None,
        distinct_count: int | None,
    ) -> None:
        with transaction(self.db_path) as conn:
            row = conn.execute(
                _SELECT_DATASET,
                (data_source, dataset_name),
            ).fetchone()
            if not row:
                raise ValueError(
                    f"update_field_sampled: dataset {dataset_name!r} in datasource "
                    f"{data_source!r} not found."
                )
            data = json.loads(row[0])
            version = data.get("version")
            fields_key = keys_for(entity="SlayerModel", version=version if isinstance(version, int) else 1).fields
            cols = data.get(fields_key) or []
            for col in cols:
                if isinstance(col, dict) and col.get("name") == field_name:
                    _write_sample_fields(
                        col,
                        sampled=sampled,
                        sampled_values=sampled_values,
                        distinct_count=distinct_count,
                    )
                    break
            else:
                raise ValueError(
                    f"update_field_sampled: field {field_name!r} not found "
                    f"on dataset {dataset_name!r} in datasource {data_source!r}."
                )
            conn.execute(
                _UPDATE_DATASET,
                (json.dumps(data), data_source, dataset_name),
            )

    async def update_field_sampled(
        self,
        *,
        data_source: str,
        dataset_name: str,
        field_name: str,
        sampled: str | None,
        sampled_values: list[str] | None,
        distinct_count: int | None,
    ) -> None:
        await asyncio.to_thread(
            self._update_field_sampled_sync,
            data_source=data_source, dataset_name=dataset_name,
            field_name=field_name, sampled=sampled,
            sampled_values=sampled_values, distinct_count=distinct_count,
        )
        self._forget_raw(data_source=data_source, name=dataset_name)

    async def _save_datasource_impl(self, datasource: DatasourceConfig) -> None:
        await asyncio.to_thread(self._save_datasource_sync, datasource)

    async def get_datasource(self, name: str) -> DatasourceConfig | None:
        # Sanitize the raw name. Mirrors the YAML backend; the
        # SQLite lookup is parameterised so injection isn't the risk —
        # validation here keeps the public ABC contract uniform across
        # backends.
        _validate_path_component(name, kind=DATASOURCE_NAME)
        raw = await asyncio.to_thread(self._get_datasource_sync, name)
        if raw is None:
            return None
        ds = await self._datasource_from_stored(name=name, data=json.loads(raw))
        return ds.resolve_env_vars()

    async def list_datasources(self) -> list[str]:
        return await asyncio.to_thread(self._list_datasources_sync)

    async def _delete_datasource_row(self, name: str) -> bool:
        return await asyncio.to_thread(self._delete_datasource_sync, name)

    async def get_datasource_priority(self) -> list[str]:
        return await asyncio.to_thread(self._get_priority_sync)

    async def _set_datasource_priority_raw(self, priority: list[str]) -> None:
        await asyncio.to_thread(self._set_priority_sync, list(priority))

    # ---- memories -------------------------------------------
    #
    # Ids are derived from the ``memories`` table itself, not
    # from a dedicated counter table. ``save_memory`` runs the insert
    # inside a single transaction with ``INSERT ... RETURNING id`` so the
    # id assignment is atomic with the write — SQLite serializes write
    # transactions, so two concurrent ``save_memory`` calls can never
    # both reserve the same id (which would happen if we read
    # ``MAX(id) + 1`` then issued a separate insert).
    #
    # Any legacy ``id_counters`` table on an older DB is left in
    # place as harmless dead data; nothing reads it.

    @staticmethod
    def _is_int_shaped_id(value: Any) -> bool:
        """Allocator predicate: pure-digit, no-leading-zero
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

        ``memory_id=None`` triggers allocator (max int-shaped
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
        # ``open_connection`` (the door) only closes; this path drives its own
        # BEGIN IMMEDIATE / COMMIT / ROLLBACK under autocommit.
        with open_connection(self.db_path, isolation_level=None, timeout=30.0) as conn:
            conn.execute(_PRAGMA_FOREIGN_KEYS_ON)
            conn.execute(_BEGIN_IMMEDIATE)
            try:
                preserved_created_at = None
                if memory_id is None:
                    memory_id = self._next_memory_seq_sync_from_conn(conn)
                else:
                    existing_row = conn.execute(
                        _SELECT_MEMORY,
                        (memory_id,),
                    ).fetchone()
                    if existing_row is not None:
                        existing_memory = Memory.model_validate(
                            stamp_stored(json.loads(existing_row[0]))
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
                    (memory_id, json.dumps(without_none(memory.model_dump(mode="json")))),
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
        data = json.dumps(without_none(memory.model_dump(mode="json")))
        with transaction(self.db_path) as conn:
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
        with transaction(self.db_path) as conn:
            return self._next_memory_seq_sync_from_conn(conn)

    async def _next_memory_seq(self) -> str:
        return await asyncio.to_thread(self._next_memory_seq_sync)

    def _get_memory_sync(self, memory_id: str) -> str | None:
        with transaction(self.db_path) as conn:
            row = conn.execute(
                _SELECT_MEMORY, (memory_id,)
            ).fetchone()
        return row[0] if row else None

    async def _get_memory_row(self, memory_id: str) -> Memory | None:
        raw = await asyncio.to_thread(self._get_memory_sync, memory_id)
        return await self._memory_from_stored(memory_id=memory_id, decode=lambda: json.loads(raw)) if raw else None

    def _list_memories_sync(
        self, entities: list[str] | None
    ) -> list[tuple[str, str]]:
        with transaction(self.db_path) as conn:
            if entities is None:
                rows = conn.execute(
                    "SELECT id, data FROM memories ORDER BY id"
                ).fetchall()
            elif not entities:
                return []
            else:
                select = (
                    exp.select("m.id", "m.data").distinct().from_("memories AS m")
                    .join("memory_entities AS me", on="me.memory_id = m.id")
                    .where(exp.column("entity", table="me").isin(*(exp.Placeholder() for _ in entities)))
                    .order_by("m.id")
                )
                rows = conn.execute(select.sql(dialect="sqlite"), tuple(entities)).fetchall()
        return [(r[0], r[1]) for r in rows]

    async def _list_memories_rows(
        self, *, entities: list[str] | None
    ) -> Loaded[Memory]:
        memories: list[Memory] = []
        errors: list[StoredDocumentLoadError] = []
        for memory_id, raw in await asyncio.to_thread(self._list_memories_sync, entities):
            try:
                memories.append(await self._memory_from_stored(memory_id=memory_id, decode=lambda r=raw: json.loads(r)))
            except StoredDocumentLoadError as exc:
                errors.append(exc)
        return memories, errors

    def _delete_memory_sync(self, memory_id: str) -> bool:
        with transaction(self.db_path) as conn:
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
