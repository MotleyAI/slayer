"""Service layer for the three Memory tools (DEV-1357 v2).

Sits between the storage backend and the surface layers (MCP, REST,
CLI, Python client). Responsibilities:

* Validate tool-level input (empty learning, empty entity list,
  non-numeric ids).
* Dispatch on the polymorphic ``linked_entities`` / ``about`` arg —
  ``list[str]`` triggers strict per-token resolution; ``SlayerQuery``
  / ``dict`` triggers query-walk extraction (warnings are non-fatal,
  the query is persisted on the memory in the save path).
* Compose the typed response objects — surface layers serialise these.

Errors raise typed exceptions (``ValueError``, ``EntityResolutionError``,
``MemoryNotFoundError``, ``AmbiguousModelError``) — the MCP / REST / CLI
wrappers catch and format them per their convention.
"""

from __future__ import annotations

from typing import List, Optional, Union

from slayer.core.query import SlayerQuery
from slayer.memories.models import (
    ForgetMemoryResponse,
    Memory,
    RecallHit,
    RecallResponse,
    SaveMemoryResponse,
)
from slayer.memories.ranker import bm25_rank
from slayer.memories.resolver import (
    extract_entities_from_query,
    resolve_entity,
)
from slayer.storage.base import StorageBackend


QueryInput = Union[SlayerQuery, dict]
LinkedEntities = Union[List[str], SlayerQuery, dict]
About = Union[List[str], SlayerQuery, dict]


def _coerce_query(query: QueryInput) -> SlayerQuery:
    """Normalise an inline-query arg into a ``SlayerQuery`` instance.

    Strings (run-by-name) are intentionally rejected: the new surface
    only takes either an entity list (each item resolved strictly) or a
    full query body. A bare model name carries no entities to extract,
    so the previous string-coercion pathway has no useful behaviour
    here.
    """
    if isinstance(query, SlayerQuery):
        return query
    if isinstance(query, dict):
        return SlayerQuery.model_validate(query)
    raise ValueError(
        f"Expected a SlayerQuery or dict; got {type(query).__name__}."
    )


def _coerce_int_id(identifier: Union[int, str]) -> int:
    """Accept native ``int`` or its decimal string form."""
    if isinstance(identifier, bool):  # bool is a subclass of int
        raise ValueError(f"id must be a positive int; got {identifier!r}.")
    if isinstance(identifier, int):
        if identifier <= 0:
            raise ValueError(
                f"id must be a positive int; got {identifier}."
            )
        return identifier
    if isinstance(identifier, str):
        s = identifier.strip()
        if not s.isdigit():
            raise ValueError(
                f"id '{identifier}' is not a valid memory id "
                f"(must be a positive int)."
            )
        value = int(s)
        if value <= 0:
            raise ValueError(
                f"id must be a positive int; got {value}."
            )
        return value
    raise ValueError(
        f"id must be int or its decimal string form; "
        f"got {type(identifier).__name__}."
    )


def _dedup(items: List[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _to_hit(memory: Memory, matched: List[str], score: float) -> RecallHit:
    return RecallHit(
        id=memory.id,
        score=score,
        matched_entities=matched,
        learning=memory.learning,
        query=memory.query,
    )


class MemoryService:
    """Orchestrates entity resolution + storage CRUD for the three
    Memory tools."""

    def __init__(self, storage: StorageBackend) -> None:
        self._storage = storage

    # ---- save_memory ---------------------------------------------------

    async def save_memory(
        self,
        *,
        learning: str,
        linked_entities: LinkedEntities,
    ) -> SaveMemoryResponse:
        if not learning or not learning.strip():
            raise ValueError("learning text must be a non-empty string.")

        canonical: List[str] = []
        warnings: List[str] = []
        attached_query: Optional[SlayerQuery] = None

        if isinstance(linked_entities, list):
            if not linked_entities:
                raise ValueError(
                    "linked_entities must be a non-empty list of entity "
                    "references (or a SlayerQuery / dict)."
                )
            for raw in linked_entities:
                if not isinstance(raw, str):
                    raise ValueError(
                        f"linked_entities list items must be strings; "
                        f"got {type(raw).__name__}."
                    )
                result = await resolve_entity(raw, storage=self._storage)
                canonical.extend(result.canonical_forms)
                warnings.extend(result.warnings)
        else:
            attached_query = _coerce_query(linked_entities)
            extraction = await extract_entities_from_query(
                attached_query, storage=self._storage
            )
            canonical.extend(extraction.canonical_forms)
            warnings.extend(extraction.warnings)

        canonical = _dedup(canonical)
        warnings = _dedup(warnings)
        memory = await self._storage.save_memory(
            learning=learning,
            entities=canonical,
            query=attached_query,
        )
        return SaveMemoryResponse(
            memory_id=memory.id,
            resolved_entities=canonical,
            warnings=warnings,
        )

    # ---- forget_memory -------------------------------------------------

    async def forget_memory(
        self, *, identifier: Union[int, str]
    ) -> ForgetMemoryResponse:
        memory_id = _coerce_int_id(identifier)
        await self._storage.delete_memory(memory_id)
        return ForgetMemoryResponse(deleted_id=memory_id)

    # ---- recall_memories -----------------------------------------------

    async def recall_memories(
        self,
        *,
        about: About,
        max_learnings: Optional[int] = None,
        max_queries: Optional[int] = 2,
    ) -> RecallResponse:
        # Negative caps would silently slice "all but the last N"
        # entries; reject up front so the API behaves predictably.
        if max_learnings is not None and max_learnings < 0:
            raise ValueError(
                "max_learnings must be >= 0 or None; "
                f"got {max_learnings}."
            )
        if max_queries is not None and max_queries < 0:
            raise ValueError(
                f"max_queries must be >= 0 or None; got {max_queries}."
            )

        canonical: List[str] = []
        warnings: List[str] = []

        if isinstance(about, list):
            for raw in about:
                if not isinstance(raw, str):
                    raise ValueError(
                        f"about list items must be strings; "
                        f"got {type(raw).__name__}."
                    )
                result = await resolve_entity(raw, storage=self._storage)
                canonical.extend(result.canonical_forms)
                warnings.extend(result.warnings)
        else:
            extraction = await extract_entities_from_query(
                _coerce_query(about), storage=self._storage
            )
            canonical.extend(extraction.canonical_forms)
            warnings.extend(extraction.warnings)

        canonical = _dedup(canonical)
        warnings = _dedup(warnings)

        # Empty input or zero-extracted entities → recency fallback.
        if not canonical:
            warnings.append(
                "no entities supplied; returning all memories ranked by "
                "recency (newest first)."
            )
            candidates = await self._storage.list_memories(entities=None)
            return self._build_recency_response(
                candidates=candidates,
                resolved=[],
                warnings=warnings,
                max_learnings=max_learnings,
                max_queries=max_queries,
            )

        # BM25 needs the full corpus to compute correct IDF / avgdl.
        candidates = await self._storage.list_memories(entities=None)
        if not candidates:
            return RecallResponse(
                learnings=[],
                queries=[],
                resolved_input_entities=canonical,
                warnings=warnings,
            )
        return self._build_bm25_response(
            candidates=candidates,
            wanted=set(canonical),
            query_entities=canonical,
            resolved=canonical,
            warnings=warnings,
            max_learnings=max_learnings,
            max_queries=max_queries,
        )

    def _build_recency_response(
        self,
        *,
        candidates: List[Memory],
        resolved: List[str],
        warnings: List[str],
        max_learnings: Optional[int],
        max_queries: Optional[int],
    ) -> RecallResponse:
        # No query → no BM25. Score is a placeholder (0.0); the
        # accompanying warning makes the meaning explicit. Sort by
        # ``created_at`` desc.
        scored = sorted(candidates, key=lambda m: m.created_at, reverse=True)
        learnings = [_to_hit(m, [], 0.0) for m in scored if m.query is None]
        queries = [_to_hit(m, [], 0.0) for m in scored if m.query is not None]
        if max_learnings is not None:
            learnings = learnings[:max_learnings]
        if max_queries is not None:
            queries = queries[:max_queries]
        return RecallResponse(
            learnings=learnings,
            queries=queries,
            resolved_input_entities=resolved,
            warnings=warnings,
        )

    def _build_bm25_response(
        self,
        *,
        candidates: List[Memory],
        wanted: set[str],
        query_entities: List[str],
        resolved: List[str],
        warnings: List[str],
        max_learnings: Optional[int],
        max_queries: Optional[int],
    ) -> RecallResponse:
        # Stable secondary sort: pre-sort the corpus by ``created_at``
        # desc so that ``bm25_rank``'s stable score-desc sort yields
        # (score desc, created_at desc) overall.
        ordered = sorted(candidates, key=lambda m: m.created_at, reverse=True)
        ranked = bm25_rank(ordered, query_entities)
        hits = [
            _to_hit(memory, sorted(wanted & set(memory.entities)), score)
            for memory, score in ranked
        ]
        learnings = [hit for hit in hits if hit.query is None]
        queries = [hit for hit in hits if hit.query is not None]
        if max_learnings is not None:
            learnings = learnings[:max_learnings]
        if max_queries is not None:
            queries = queries[:max_queries]
        return RecallResponse(
            learnings=learnings,
            queries=queries,
            resolved_input_entities=resolved,
            warnings=warnings,
        )


def _format_friendly_error(exc: Exception) -> str:
    """Render a typed error for surface layers as a single-line string.

    Matches the existing convention in ``slayer/mcp/server.py`` of never
    raising back to the agent — the response text carries the message.
    """
    return f"Error: {type(exc).__name__}: {exc}"


__all__ = [
    "About",
    "LinkedEntities",
    "MemoryService",
    "QueryInput",
    "_format_friendly_error",
]
