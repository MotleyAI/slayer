"""Retriever ABC + shared result type for SLayer's pluggable search
facade (DEV-1514).

A ``Retriever`` is one ranking channel. The orchestrator (see
:class:`slayer.search.service.SearchService`) holds a list of them,
calls each one's :meth:`Retriever.retrieve` in parallel, and fuses the
returned rankings via RRF. Concrete retrievers also own their own
persistence — write-side hooks (``upsert_memory``,
``refresh_model_subtree``, ``refresh_datasource``, plus the three
``delete_*`` futures) default to no-op on the ABC; the embedding
retriever overrides the create/refresh ones to maintain its sidecar
table. The ``delete_*`` hooks default to no-op for every shipping
retriever this PR — :class:`StorageBackend` owns embedding-row cascade
transactionally with the model/datasource/memory row delete; the
hooks live on the ABC so future persistent retrievers (e.g.
persistent tantivy) can override them without an ABC change.

Returning ``memory_ranking`` + ``entity_ranking`` from a SINGLE
``retrieve`` call (rather than splitting them across two methods) lets
retrievers that share expensive setup across the two partitions —
litellm embed, dim-check, tantivy index handle — do that setup once
per search call. The orchestrator gathers across retrievers but never
within one (DEV-1514, Codex Findings 1 & 2).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from slayer.core.models import SlayerModel
from slayer.memories.models import Memory
from slayer.search.index import Corpus


class RetrievalResult(BaseModel):
    """Combined memory + entity ranking produced by ONE retrieve call.

    * ``memory_ranking`` — memory ids best-first.
    * ``text_by_id`` — recovered hit text keyed by memory id, only
      populated by retrievers that rendered text for the hit
      (tantivy today). Lets :func:`_build_memory_hit` surface the
      tantivy hit text in ``MemoryHit.text`` instead of always
      falling back to ``Memory.learning``.
    * ``entity_ranking`` — canonical entity ids best-first
      (``"<ds>"`` / ``"<ds>.<model>"`` / ``"<ds>.<model>.<leaf>"``).
    * ``warnings`` — per-call warnings the orchestrator should
      surface to the caller (e.g. "embedding channel skipped:
      query embedding failed").
    """

    memory_ranking: List[str] = Field(default_factory=list)
    text_by_id: Dict[str, str] = Field(default_factory=dict)
    entity_ranking: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class Retriever(ABC):
    """One ranking channel. Subclasses set ``name`` and implement
    :meth:`retrieve`; they may override any of the write-side hooks
    to maintain their own persistence."""

    name: str = ""

    @abstractmethod
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
        """Return the channel's memory + entity rankings (plus any
        per-call warnings). Empty rankings indicate "channel inactive
        for this input"; the orchestrator skips empty channels in the
        RRF fusion."""

    # ------------------------------------------------------------------
    # Create / refresh hooks — default no-op
    # ------------------------------------------------------------------

    async def upsert_memory(self, memory: Memory) -> List[str]:  # NOSONAR(S7503) — async signature required by Retriever ABC; subclasses override with truly-async hooks
        return []

    async def refresh_model_subtree(self, model: SlayerModel) -> List[str]:  # NOSONAR(S7503) — async signature required by Retriever ABC; subclasses override with truly-async hooks
        return []

    async def refresh_datasource(  # NOSONAR(S7503) — async signature required by Retriever ABC; subclasses override with truly-async hooks
        self, *, name: str, models: List[SlayerModel],
    ) -> List[str]:
        return []

    # ------------------------------------------------------------------
    # Delete hooks — default no-op
    #
    # All three shipping retrievers no-op these hooks this PR.
    # :class:`StorageBackend` owns embedding-row cascade transactionally
    # with the model / datasource / memory row delete. The hooks live
    # on the ABC so future persistent retrievers (persistent tantivy,
    # third-party vector DBs) can override them without an ABC change.
    # ------------------------------------------------------------------

    async def delete_memory(self, memory_id: str) -> None:  # NOSONAR(S7503) — async signature required by Retriever ABC; future persistent retrievers will override with truly-async hooks
        return None

    async def delete_model(  # NOSONAR(S7503) — async signature required by Retriever ABC; future persistent retrievers will override with truly-async hooks
        self, *, data_source: str, name: str,
    ) -> None:
        return None

    async def delete_datasource(self, name: str) -> None:  # NOSONAR(S7503) — async signature required by Retriever ABC; future persistent retrievers will override with truly-async hooks
        return None


__all__ = ["Retriever", "RetrievalResult"]
