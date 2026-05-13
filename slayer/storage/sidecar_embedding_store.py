"""SQLite-backed sidecar for embedding rows (DEV-1405).

Owns a single ``embeddings`` table inside the SQLite file at ``db_path``
and exposes the four CRUD methods + two batched variants that the
embedding sidecar contract requires. Both :class:`SQLiteStorage` and
:class:`YAMLStorage` instantiate one and forward their abstract
:class:`StorageBackend` methods to it — so the SQL lives once and the
two backends differ only in where their ``db_path`` points (the main
storage DB for SQLite; ``<base_dir>/embeddings.db`` for YAML).

Connection lifecycle: ``sqlite3.connect(self.db_path)`` per call.
Matches the pattern in :mod:`slayer.storage.sqlite_storage`; no pool.

Cascade semantics for :meth:`delete_for_canonical` (DEV-1405 fix):
matches the supplied prefix exactly **or** as a strict dotted-path
descendant (``prefix + "." + …``). Never a character prefix —
``"orders"`` does not match ``"orders_archive"``, ``"memory:4"`` does
not match ``"memory:42"``.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from typing import Dict, List, Optional, Tuple

from slayer.embeddings.models import Embedding


class SidecarEmbeddingStore:
    """SQLite-backed embedding sidecar."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._init_db()

    # ------------------------------------------------------------------
    # Init
    # ------------------------------------------------------------------

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
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

    # ------------------------------------------------------------------
    # Sync core
    # ------------------------------------------------------------------

    @staticmethod
    def _row_tuple(row: Embedding) -> Tuple[str, str, str, str, str, str]:
        return (
            row.canonical_id,
            row.embedding_model_name,
            row.entity_kind,
            row.content_hash,
            json.dumps(row.embedding),
            row.created_at.isoformat(),
        )

    @staticmethod
    def _row_from_db(raw: Tuple[str, str, str, str, str, str]) -> Embedding:
        return Embedding.model_validate({
            "canonical_id": raw[0],
            "embedding_model_name": raw[1],
            "entity_kind": raw[2],
            "content_hash": raw[3],
            "embedding": json.loads(raw[4]),
            "created_at": raw[5],
        })

    def _save_sync(self, row: Embedding) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO embeddings "
                "(canonical_id, embedding_model_name, entity_kind, "
                "content_hash, embedding, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                self._row_tuple(row),
            )

    def _save_many_sync(self, rows: List[Embedding]) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO embeddings "
                "(canonical_id, embedding_model_name, entity_kind, "
                "content_hash, embedding, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [self._row_tuple(r) for r in rows],
            )

    def _get_sync(
        self, canonical_id: str, embedding_model_name: str,
    ) -> Optional[Tuple[str, str, str, str, str, str]]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT canonical_id, embedding_model_name, entity_kind, "
                "content_hash, embedding, created_at "
                "FROM embeddings "
                "WHERE canonical_id = ? AND embedding_model_name = ?",
                (canonical_id, embedding_model_name),
            ).fetchone()
        return row

    # SQLite's host-parameter limit is 32766 on 3.32+ builds, 250000 on
    # the newest ones — but third-party callers may pass arbitrarily long
    # ``canonical_ids`` lists through the public API. Chunking the IN
    # clause well below the worst-case limit keeps the query safe under
    # every reasonable SQLite build.
    _GET_MANY_CHUNK_SIZE = 900

    def _get_many_sync(
        self,
        canonical_ids: List[str],
        embedding_model_name: str,
    ) -> List[Tuple[str, str, str, str, str, str]]:
        rows: List[Tuple[str, str, str, str, str, str]] = []
        with sqlite3.connect(self.db_path) as conn:
            for start in range(0, len(canonical_ids), self._GET_MANY_CHUNK_SIZE):
                chunk = canonical_ids[start : start + self._GET_MANY_CHUNK_SIZE]
                placeholders = ",".join("?" * len(chunk))
                rows.extend(conn.execute(
                    "SELECT canonical_id, embedding_model_name, entity_kind, "
                    "content_hash, embedding, created_at "
                    "FROM embeddings "
                    f"WHERE embedding_model_name = ? AND canonical_id IN ({placeholders})",
                    (embedding_model_name, *chunk),
                ).fetchall())
        return rows

    def _list_sync(
        self, embedding_model_name: str,
    ) -> List[Tuple[str, str, str, str, str, str]]:
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

    def _delete_by_prefix_sync(self, prefix: str) -> int:
        # SQLite LIKE uses ``%`` and ``_`` as wildcards. Escape them in
        # the supplied prefix so a prefix containing wildcard characters
        # cannot match arbitrary other ids.
        like_descendants = (
            prefix.replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
            + ".%"
        )
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM embeddings "
                "WHERE canonical_id = ? OR canonical_id LIKE ? ESCAPE '\\'",
                (prefix, like_descendants),
            )
            return int(cursor.rowcount or 0)

    # ------------------------------------------------------------------
    # Async surface
    # ------------------------------------------------------------------

    async def save(self, row: Embedding) -> None:
        await asyncio.to_thread(self._save_sync, row)

    async def save_many(self, rows: List[Embedding]) -> None:
        if not rows:
            return
        await asyncio.to_thread(self._save_many_sync, list(rows))

    async def get(
        self, *, canonical_id: str, embedding_model_name: str,
    ) -> Optional[Embedding]:
        raw = await asyncio.to_thread(
            self._get_sync, canonical_id, embedding_model_name,
        )
        if raw is None:
            return None
        return self._row_from_db(raw)

    async def get_many(
        self,
        *,
        canonical_ids: List[str],
        embedding_model_name: str,
    ) -> Dict[str, Embedding]:
        if not canonical_ids:
            return {}
        raws = await asyncio.to_thread(
            self._get_many_sync, list(canonical_ids), embedding_model_name,
        )
        return {raw[0]: self._row_from_db(raw) for raw in raws}

    async def list_for_model(
        self, *, embedding_model_name: str,
    ) -> List[Embedding]:
        raws = await asyncio.to_thread(
            self._list_sync, embedding_model_name,
        )
        return [self._row_from_db(raw) for raw in raws]

    async def delete_for_canonical(
        self, *, canonical_id_prefix: str,
    ) -> int:
        return await asyncio.to_thread(
            self._delete_by_prefix_sync, canonical_id_prefix,
        )


class SidecarEmbeddingsMixin:
    """Mixin providing the embedding CRUD surface by forwarding to
    ``self._embeddings_store``.

    Both :class:`slayer.storage.sqlite_storage.SQLiteStorage` and
    :class:`slayer.storage.yaml_storage.YAMLStorage` use this mixin so
    the six abstract :class:`~slayer.storage.base.StorageBackend`
    embedding methods (four single-row + two batched) implement once,
    not twice. The mixin assumes the consuming class assigns
    ``self._embeddings_store`` to a :class:`SidecarEmbeddingStore` in
    its ``__init__``.
    """

    _embeddings_store: SidecarEmbeddingStore

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


__all__ = ["SidecarEmbeddingStore", "SidecarEmbeddingsMixin"]
