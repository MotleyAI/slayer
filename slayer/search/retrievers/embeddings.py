"""Embedding retriever — dense cosine over a sidecar embedding store
(DEV-1514; absorbs the former ``slayer.embeddings.service``).

Owns the litellm refresh pipeline (write side) and the cosine ranking
(read side). The read side runs ``fetch_corpus`` ONCE, ``embed_question``
ONCE, and the dim-check ONCE per :meth:`retrieve` call — both the memory
and entity rankings are produced from the same setup (Codex Finding 1).
"""

from __future__ import annotations

import hashlib
import logging
from typing import List, Optional, Set, Tuple

from slayer.core.models import SlayerModel
from slayer.embeddings import client as embedding_client
from slayer.embeddings.client import current_model, embed_batch
from slayer.embeddings.models import Embedding, EntityKind
from slayer.memories.models import MEMORY_CANONICAL_PREFIX as _MEMORY_PREFIX
from slayer.memories.models import Memory
from slayer.memories.resolver import canonical_id_rooted_at
from slayer.search.index import Corpus
from slayer.search.render import (
    collect_model_entity_pairs,
    render_datasource_pair,
    render_memory_text_for_embedding,
)
from slayer.search.retriever import RetrievalResult, Retriever
from slayer.storage.base import StorageBackend


_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Canonical id helpers
# ---------------------------------------------------------------------------


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _memory_canonical_id(memory_id: str) -> str:
    return f"{_MEMORY_PREFIX}{memory_id}"


def _memory_id_from_canonical(canonical_id: str) -> Optional[str]:
    """Parse a memory row's canonical id back into the str memory id.

    Returns ``None`` when the input is not exactly of the shape
    ``memory:<non-empty-id>`` — a corrupted / stale embedding row
    carrying ``foo:bar`` must not be mis-mapped to a memory hit
    (DEV-1428 review)."""
    if not canonical_id.startswith(_MEMORY_PREFIX):
        return None
    memory_id = canonical_id[len(_MEMORY_PREFIX):]
    return memory_id or None


# ---------------------------------------------------------------------------
# Read-side helpers
# ---------------------------------------------------------------------------


def _filter_embedding_corpus_by_datasource(
    rows: List[Embedding],
    *,
    datasource: str,
    eligible_memory_canonicals: Set[str],
) -> List[Embedding]:
    """DEV-1409: narrow the embedding corpus to rows that survive a
    datasource filter. Memory rows must appear in
    ``eligible_memory_canonicals`` (already datasource-filtered
    upstream); entity rows must be rooted at ``datasource`` per the
    dotted-namespace rule."""
    return [
        r for r in rows
        if (
            (r.entity_kind == "memory"
                and r.canonical_id in eligible_memory_canonicals)
            or (r.entity_kind != "memory"
                and canonical_id_rooted_at(
                    canonical_id=r.canonical_id, datasource=datasource,
                ))
        )
    ]


def _rank_embedding_kind(
    *, rows, normalised_query, np, normalise_matrix, top_k_cosine,
) -> List[str]:
    """Rank one kind of embedding rows by cosine similarity to the
    pre-normalised query vector. Returns the rows' ``canonical_id``
    strings in descending similarity order."""
    if not rows:
        return []
    matrix = np.array([r.embedding for r in rows], dtype=np.float32)
    pairs = top_k_cosine(
        query=normalised_query,
        matrix=normalise_matrix(matrix),
        k=len(rows),
    )
    return [rows[idx].canonical_id for idx, _score in pairs]


# ---------------------------------------------------------------------------
# Pending refresh unit (write side)
# ---------------------------------------------------------------------------


class _PendingRefresh:
    """One unit of work — rendered text needing an embedding."""

    __slots__ = ("canonical_id", "entity_kind", "text", "content_hash")

    canonical_id: str
    entity_kind: EntityKind
    text: str
    content_hash: str

    def __init__(
        self, *,
        canonical_id: str,
        entity_kind: EntityKind,
        text: str,
    ) -> None:
        self.canonical_id = canonical_id
        self.entity_kind = entity_kind
        self.text = text
        self.content_hash = _sha256(text)


# ---------------------------------------------------------------------------
# Retriever
# ---------------------------------------------------------------------------


class EmbeddingRetriever(Retriever):
    """Cosine-similarity retriever over a SQLite-sidecar embedding
    store, plus the refresh pipeline that keeps the sidecar in step
    with model / memory edits.

    The delete hooks intentionally default to ABC no-op:
    :class:`StorageBackend` cascade-deletes embedding rows
    transactionally with the model / datasource / memory row delete,
    so the retriever has nothing to do on delete this PR."""

    name = "embeddings"

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
    # Read side
    # ------------------------------------------------------------------

    async def fetch_corpus(self) -> List[Embedding]:
        """Return every embedding row under the active model name."""
        return await self._storage.list_embeddings(
            embedding_model_name=self._model_name,
        )

    async def embed_question(self, question: str) -> Optional[List[float]]:
        """Embed a search query string. ``None`` when the channel is
        unavailable or the call fails."""
        return await embedding_client.embed_query(
            question, model=self._model_name,
        )

    async def retrieve(
        self,
        *,
        query_entities: List[str],
        question: Optional[str],
        all_memories: List[Memory],
        valid_canonicals: set,
        corpus: Optional[Corpus],
        datasource: Optional[str],
    ) -> RetrievalResult:
        """Run cosine over the embedding corpus, returning BOTH memory
        and entity rankings from a single ``fetch_corpus`` +
        ``embed_question`` + dim-check (Codex Finding 1).

        Skipped (with a warning) when:

        * ``question`` is blank,
        * the ``advanced_search`` extra is not installed,
        * the active model has no embedding rows in storage,
        * the query embedding call fails,
        * dim mismatch between query vec and corpus.
        """
        if corpus is None or not question or not question.strip():
            return RetrievalResult()
        if not embedding_client.is_available():
            return RetrievalResult(warnings=[
                "embedding channel skipped: `advanced_search` extra not "
                "installed or no API key configured for the active "
                "embedding model.",
            ])

        rows = await self.fetch_corpus()
        if datasource is not None:
            eligible_memory_canonicals = {
                f"{_MEMORY_PREFIX}{m.id}" for m in all_memories
            }
            rows = _filter_embedding_corpus_by_datasource(
                rows,
                datasource=datasource,
                eligible_memory_canonicals=eligible_memory_canonicals,
            )
        # Drop sidecar rows that don't correspond to anything in the
        # live tantivy corpus (DEV-1414).
        live_canonicals = corpus.canonical_to_kind.keys()
        rows = [r for r in rows if r.canonical_id in live_canonicals]
        if not rows:
            return RetrievalResult(warnings=[
                f"embedding channel skipped: no embedding rows for model "
                f"{self._model_name!r}. Run `slayer ingest` to populate.",
            ])

        # Inline imports: ``numpy`` and ``slayer.embeddings.ranker``
        # require the optional ``advanced_search`` extra. When the extra
        # is not installed, we fall through to a soft warning instead of
        # raising at module import time so the rest of slayer keeps
        # working without the extra.
        try:
            import numpy as np
            from slayer.embeddings.ranker import (
                normalise,
                normalise_matrix,
                top_k_cosine,
            )
        except ImportError:
            return RetrievalResult(warnings=[
                "embedding channel skipped: numpy not installed "
                "(reinstall with the `advanced_search` extra).",
            ])

        query_vec = await self.embed_question(question or "")
        if query_vec is None:
            return RetrievalResult(warnings=[
                "embedding channel skipped: query embedding failed.",
            ])
        if len(rows[0].embedding) != len(query_vec):
            return RetrievalResult(warnings=[
                f"embedding channel skipped: dim mismatch "
                f"(query={len(query_vec)}, corpus={len(rows[0].embedding)}). "
                f"Re-run `slayer ingest` to refresh embeddings against "
                f"the current model.",
            ])

        memory_rows = [r for r in rows if r.entity_kind == "memory"]
        entity_rows = [r for r in rows if r.entity_kind != "memory"]
        normalised_query = normalise(query_vec)
        ranked_memory_canonicals = _rank_embedding_kind(
            rows=memory_rows,
            normalised_query=normalised_query,
            np=np,
            normalise_matrix=normalise_matrix,
            top_k_cosine=top_k_cosine,
        )
        memory_ranking: List[str] = []
        for canonical in ranked_memory_canonicals:
            memory_id = _memory_id_from_canonical(canonical)
            if memory_id is not None:
                memory_ranking.append(memory_id)
        entity_ranking = _rank_embedding_kind(
            rows=entity_rows,
            normalised_query=normalised_query,
            np=np,
            normalise_matrix=normalise_matrix,
            top_k_cosine=top_k_cosine,
        )
        return RetrievalResult(
            memory_ranking=memory_ranking,
            entity_ranking=entity_ranking,
        )

    # ------------------------------------------------------------------
    # Write side — create / refresh hooks
    # ------------------------------------------------------------------

    async def upsert_memory(self, memory: Memory) -> List[str]:
        """Refresh the embedding for a single memory. Returns warning
        strings (empty on success or hash-skip). Stays silent on the
        write path when the channel is unavailable — that's "feature
        not configured", not a runtime failure (the search-side surface
        emits one user-visible warning into ``SearchResponse.warnings``
        on the next query)."""
        if not embedding_client.is_available():
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

        Routes through the unified :func:`render_datasource_pair` so the
        visibility filter is applied in exactly one place (DEV-1513).
        """
        if not embedding_client.is_available():
            return []
        pair = render_datasource_pair(name=name, models=models)
        pending = _PendingRefresh(
            canonical_id=pair.canonical_id,
            entity_kind="datasource",
            text=pair.text,
        )
        return await self._apply_pending([pending])

    async def refresh_model_subtree(self, model: SlayerModel) -> List[str]:
        """Refresh the model doc + every visible column + named measures
        + custom aggregations in a single batch call.

        Routes through the unified :func:`collect_model_entity_pairs` so
        the "what counts as an indexable entity" filter rules (hidden
        model -> empty; hidden column skipped; unnamed measure skipped)
        live in exactly one place (DEV-1513).
        """
        if not embedding_client.is_available():
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
    # Internals
    # ------------------------------------------------------------------

    async def _apply_pending(
        self, pending: List[_PendingRefresh],
    ) -> List[str]:
        """Hash-skip, batch-embed, and persist. Returns warning strings.

        DEV-1405: two batched storage round-trips per call — one
        ``get_embeddings_for_canonical_ids`` for the hash-skip filter,
        one ``save_embeddings`` for the persist step.
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
                canonical_ids = ", ".join(r.canonical_id for r in rows)
                warnings.append(
                    f"embedding batch persist failed for "
                    f"{len(rows)} row(s) [{canonical_ids}]: {exc}"
                )
        _log.debug(
            "EmbeddingRetriever: refreshed=%d stale=%d total=%d warnings=%d",
            fresh_count, len(stale), len(pending), len(warnings),
        )
        return warnings

    async def _filter_stale(
        self, pending: List[_PendingRefresh],
    ) -> Tuple[List[_PendingRefresh], int]:
        """Drop pending entries whose stored content_hash already
        matches. Returns ``(stale_entries, fresh_skipped_count)``.
        DEV-1405: one batched ``get_embeddings_for_canonical_ids`` call
        replaces the previous M-iteration point-read loop."""
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
