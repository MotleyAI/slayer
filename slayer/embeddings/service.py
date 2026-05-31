"""EmbeddingService — orchestrates embedding refresh + corpus fetch.

Refresh routines are called from the same write-side edges that maintain
``Column.sampled``: ``save_memory``, ``edit_model``, and ``slayer ingest``.
Each refresh hashes the rendered text of the affected entity, compares to
the stored ``content_hash`` for ``(canonical_id, embedding_model_name)``,
and only calls litellm when the text has actually changed.

Per-entity embed failures are non-fatal: the corresponding row is simply
not written. Search degrades gracefully via the remaining tantivy + BM25
channels. When the ``embedding_search`` extra is not installed,
``is_available()`` returns ``False`` and all refresh methods short-circuit
to "no-op + warning".
"""

from __future__ import annotations

import hashlib
import logging
from typing import List, Optional, Tuple

from slayer.core.models import SlayerModel
from slayer.embeddings import client as embedding_client
from slayer.embeddings.client import current_model, embed_batch
from slayer.embeddings.models import Embedding, EntityKind
from slayer.memories.models import MEMORY_CANONICAL_PREFIX as _MEMORY_PREFIX
from slayer.memories.models import Memory
from slayer.search.render import (
    collect_model_entity_pairs,
    render_datasource_pair,
    render_memory_text_for_embedding,
)
from slayer.storage.base import StorageBackend


_log = logging.getLogger(__name__)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _memory_canonical_id(memory_id: str) -> str:
    return f"{_MEMORY_PREFIX}{memory_id}"


class _PendingRefresh:
    """One unit of work — rendered text needing an embedding."""

    __slots__ = ("canonical_id", "entity_kind", "text", "content_hash")

    canonical_id: str
    entity_kind: EntityKind
    text: str
    content_hash: str

    def __init__(
        self,
        *,
        canonical_id: str,
        entity_kind: EntityKind,
        text: str,
    ) -> None:
        self.canonical_id = canonical_id
        self.entity_kind = entity_kind
        self.text = text
        self.content_hash = _sha256(text)


class EmbeddingService:
    """Orchestrates refresh + corpus retrieval for embedding-based search."""

    def __init__(
        self,
        *,
        storage: StorageBackend,
        model_name: Optional[str] = None,
    ) -> None:
        self._storage = storage
        self._model_name = model_name or current_model()

    @property
    def model_name(self) -> str:
        return self._model_name

    # ------------------------------------------------------------------
    # Refresh — write-side hooks
    # ------------------------------------------------------------------

    async def refresh_memory(self, memory: Memory) -> List[str]:
        """Refresh the embedding for a single memory. Returns warning
        strings (empty on success or hash-skip)."""
        if not embedding_client.is_available():
            # Channel disabled (no extra installed, or no API key
            # configured for the active embedding model). Stay silent on
            # the write path — this is "feature not configured", not a
            # runtime failure. The search-side surface emits one
            # user-visible warning into ``SearchResponse.warnings`` on
            # the next query.
            return []
        pending = _PendingRefresh(
            canonical_id=_memory_canonical_id(memory.id),
            entity_kind="memory",
            text=render_memory_text_for_embedding(memory=memory),
        )
        return await self._apply_pending([pending])

    async def refresh_datasource(
        self, *, name: str, models: List[SlayerModel],
    ) -> List[str]:
        """Refresh the embedding for one datasource doc.

        Routes through the unified ``render_datasource_pair`` so the
        visibility filter is applied in exactly one place (DEV-1513).
        """
        if not embedding_client.is_available():
            # Channel disabled (no extra installed, or no API key
            # configured for the active embedding model). Stay silent on
            # the write path — this is "feature not configured", not a
            # runtime failure. The search-side surface emits one
            # user-visible warning into ``SearchResponse.warnings`` on
            # the next query.
            return []
        pair = render_datasource_pair(name=name, models=models)
        pending = _PendingRefresh(
            canonical_id=pair.canonical_id,
            entity_kind="datasource",
            text=pair.text,
        )
        return await self._apply_pending([pending])

    async def refresh_model_subtree(self, model: SlayerModel) -> List[str]:
        """Refresh the model doc + every visible column + named measures +
        custom aggregations in a single batch call.

        Routes through the unified ``collect_model_entity_pairs`` so the
        "what counts as an indexable entity" filter rules (hidden model
        -> empty; hidden column skipped; unnamed measure skipped) live
        in exactly one place (DEV-1513).
        """
        if not embedding_client.is_available():
            # Channel disabled (no extra installed, or no API key
            # configured for the active embedding model). Stay silent on
            # the write path — this is "feature not configured", not a
            # runtime failure. The search-side surface emits one
            # user-visible warning into ``SearchResponse.warnings`` on
            # the next query.
            return []
        pending: List[_PendingRefresh] = [
            _PendingRefresh(
                canonical_id=re.canonical_id,
                entity_kind=re.kind,  # type: ignore[arg-type]
                text=re.text,
            )
            for re in collect_model_entity_pairs(model=model)
        ]
        return await self._apply_pending(pending)

    # ------------------------------------------------------------------
    # Read — search-side
    # ------------------------------------------------------------------

    async def fetch_corpus(self) -> List[Embedding]:
        """Return every embedding row under the active model name."""
        return await self._storage.list_embeddings(
            embedding_model_name=self._model_name,
        )

    async def embed_question(self, question: str) -> Optional[List[float]]:
        """Embed a search query string. ``None`` when unavailable / failed.

        Calls through the module attribute (``embedding_client.embed_query``)
        rather than an import-time binding so tests can monkeypatch the
        client module without having to also reach into this module.
        """
        return await embedding_client.embed_query(
            question, model=self._model_name,
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _apply_pending(
        self, pending: List[_PendingRefresh],
    ) -> List[str]:
        """Hash-skip, batch-embed, and persist. Returns warning strings.

        DEV-1405: hot-path uses two batched storage round-trips per call —
        one ``get_embeddings_for_canonical_ids`` for the hash-skip filter,
        one ``save_embeddings`` for the persist step. The previous code
        did M point ``get_embedding`` + M point ``save_embedding`` calls.
        """
        if not pending:
            return []
        stale, fresh_count = await self._filter_stale(pending)
        if not stale:
            return []
        texts = [p.text for p in stale]
        vectors = await embed_batch(texts, model=self._model_name)
        warnings: List[str] = []
        rows: List[Embedding] = []
        for p, vec in zip(stale, vectors):
            if vec is None:
                warnings.append(
                    f"embedding refresh failed for {p.canonical_id}; "
                    f"skipped (search will still find this entity via "
                    f"tantivy + BM25)."
                )
                continue
            rows.append(Embedding(
                canonical_id=p.canonical_id,
                embedding_model_name=self._model_name,
                entity_kind=p.entity_kind,
                content_hash=p.content_hash,
                embedding=vec,
            ))
        if rows:
            try:
                await self._storage.save_embeddings(rows)
            except Exception as exc:  # NOSONAR(S112) — best-effort persistence
                # Include canonical ids so a caller doing failure
                # attribution by entity (e.g. ``ingest_datasource_idempotent``
                # tagging memory failures as ``model_name="memory:<id>"``)
                # can see which rows did not land.
                canonical_ids = ", ".join(r.canonical_id for r in rows)
                warnings.append(
                    f"embedding batch persist failed for "
                    f"{len(rows)} row(s) [{canonical_ids}]: {exc}"
                )
        _log.debug(
            "EmbeddingService: refreshed=%d stale=%d total=%d warnings=%d",
            fresh_count, len(stale), len(pending), len(warnings),
        )
        return warnings

    async def _filter_stale(
        self, pending: List[_PendingRefresh],
    ) -> Tuple[List[_PendingRefresh], int]:
        """Drop pending entries whose stored content_hash already matches.

        Returns ``(stale_entries, fresh_skipped_count)``. DEV-1405: one
        batched ``get_embeddings_for_canonical_ids`` call replaces the
        previous M-iteration point-read loop.
        """
        existing = await self._storage.get_embeddings_for_canonical_ids(
            canonical_ids=[p.canonical_id for p in pending],
            embedding_model_name=self._model_name,
        )
        stale: List[_PendingRefresh] = []
        fresh = 0
        for p in pending:
            match = existing.get(p.canonical_id)
            if match is not None and match.content_hash == p.content_hash:
                fresh += 1
                continue
            stale.append(p)
        return stale, fresh
