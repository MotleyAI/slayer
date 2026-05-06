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


def _to_hit(memory: Memory, matched: List[str]) -> RecallHit:
    return RecallHit(
        id=memory.id,
        match_count=len(matched),
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
            return self._build_response(
                candidates=candidates,
                wanted=set(),
                resolved=[],
                warnings=warnings,
                max_learnings=max_learnings,
                max_queries=max_queries,
            )

        wanted = set(canonical)
        candidates = await self._storage.list_memories(entities=canonical)
        return self._build_response(
            candidates=candidates,
            wanted=wanted,
            resolved=canonical,
            warnings=warnings,
            max_learnings=max_learnings,
            max_queries=max_queries,
        )

    def _build_response(
        self,
        *,
        candidates: List[Memory],
        wanted: set[str],
        resolved: List[str],
        warnings: List[str],
        max_learnings: Optional[int],
        max_queries: Optional[int],
    ) -> RecallResponse:
        # When ``wanted`` is empty (recency fallback) every candidate
        # has match_count=0; we still include them in insertion order
        # with the most recent first.
        scored: List[tuple] = []
        for memory in candidates:
            if not memory.entities and wanted:
                # Filter out rows with empty stored entity sets when the
                # caller supplied entities — they cannot match anything.
                continue
            matched = sorted(wanted & set(memory.entities)) if wanted else []
            scored.append((memory.created_at, _to_hit(memory, matched), memory))

        scored.sort(
            key=lambda t: (t[1].match_count, t[0]),
            reverse=True,
        )

        learnings: List[RecallHit] = [
            hit for _, hit, m in scored if m.query is None
        ]
        queries: List[RecallHit] = [
            hit for _, hit, m in scored if m.query is not None
        ]
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
