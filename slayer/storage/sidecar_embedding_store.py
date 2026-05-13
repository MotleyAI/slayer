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

    def _get_many_sync(
        self,
        canonical_ids: List[str],
        embedding_model_name: str,
    ) -> List[Tuple[str, str, str, str, str, str]]:
        placeholders = ",".join("?" * len(canonical_ids))
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT canonical_id, embedding_model_name, entity_kind, "
                "content_hash, embedding, created_at "
                "FROM embeddings "
                f"WHERE embedding_model_name = ? AND canonical_id IN ({placeholders})",
                (embedding_model_name, *canonical_ids),
            ).fetchall()
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


__all__ = ["SidecarEmbeddingStore"]
