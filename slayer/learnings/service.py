"""Service layer for the four DEV-1357 MCP tools.

Sits between the storage backend and the ``@mcp.tool()`` decorators in
``slayer/mcp/server.py``. Responsibilities:

* Validate tool-level input (empty lists, missing args, ID prefixes).
* Drive entity resolution / extraction via ``slayer/learnings/resolver.py``.
* Materialise run-by-name input into a concrete ``SlayerQuery`` so saved
  records are always self-describing (no model-name references that
  could break if the model is later renamed).
* Compose the typed response objects (``SaveLearningResponse``,
  ``RecallResponse`` etc.) — the MCP wrappers serialise these as JSON
  for the agent.

Errors raise typed exceptions (``ValueError``,
``EntityResolutionError``, ``LearningOrQueryNotFoundError``,
``AmbiguousModelError``) — the MCP wrappers catch and format them.
"""

from __future__ import annotations

from typing import Any, List, Optional, Union

from slayer.core.query import SlayerQuery
from slayer.learnings.models import (
    DeleteResponse,
    Learning,
    RecallHit,
    RecallResponse,
    SavedQuery,
    SaveLearningResponse,
    SaveQueryResponse,
)
from slayer.learnings.resolver import (
    extract_entities_from_query,
    resolve_entity,
)
from slayer.storage.base import StorageBackend


QueryInput = Union[SlayerQuery, dict, str]


def _coerce_query(query: QueryInput) -> SlayerQuery:
    """Normalise the polymorphic ``query`` arg accepted by ``save_query``
    and ``recall``.

    * ``SlayerQuery`` → returned as-is.
    * ``dict`` → validated through Pydantic.
    * ``str`` → treated as a model name; wrapped into
      ``SlayerQuery(source_model=str)`` so the persisted record is
      self-describing (matches the spec's "always store a fully
      materialised SlayerQuery" rule). Resolution-time materialisation
      runs against this wrapped form, then any subsequent
      ``engine.execute(saved.query)`` call resolves the named model
      via the existing engine path — matching the run-by-name
      behaviour of ``engine.execute(<str>)``.
    """
    if isinstance(query, SlayerQuery):
        return query
    if isinstance(query, dict):
        return SlayerQuery.model_validate(query)
    if isinstance(query, str):
        return SlayerQuery(source_model=query)
    raise ValueError(
        f"query must be a SlayerQuery, dict, or model name string; "
        f"got {type(query).__name__}."
    )


def _dedup(items: List[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _classify_id(identifier: str) -> str:
    """Return ``"learning"`` for ``L<int>`` / ``"query"`` for ``Q<int>``."""
    if not identifier:
        raise ValueError("id must be a non-empty string.")
    if identifier[0] == "L" and identifier[1:].isdigit():
        return "learning"
    if identifier[0] == "Q" and identifier[1:].isdigit():
        return "query"
    raise ValueError(
        f"id '{identifier}' does not match the expected format "
        f"(L<int> for learnings, Q<int> for saved queries)."
    )


def _render_query_body(saved: SavedQuery) -> str:
    """Body string for a saved query in a ``RecallHit``.

    Format: ``<description>\\n\\n<query JSON>``. Agents can read the
    description and parse the JSON to reconstruct the query.
    """
    return f"{saved.description}\n\n{saved.query.model_dump_json(indent=2)}"


def _learning_to_hit(
    learning: Learning, matched: List[str]
) -> RecallHit:
    return RecallHit(
        id=learning.id,
        kind="learning",
        match_count=len(matched),
        matched_entities=matched,
        body=learning.body,
    )


def _saved_query_to_hit(
    saved: SavedQuery, matched: List[str]
) -> RecallHit:
    return RecallHit(
        id=saved.id,
        kind="query",
        match_count=len(matched),
        matched_entities=matched,
        body=_render_query_body(saved),
    )


class LearningService:
    """Orchestrates entity resolution + storage CRUD for the four
    DEV-1357 MCP tools."""

    def __init__(self, storage: StorageBackend) -> None:
        self._storage = storage

    # ---- save_learning -------------------------------------------------

    async def save_learning(
        self, *, learning: str, linked_entities: List[str]
    ) -> SaveLearningResponse:
        if not learning or not learning.strip():
            raise ValueError("learning text must be a non-empty string.")
        if not linked_entities:
            raise ValueError(
                "linked_entities must be a non-empty list of entity "
                "references."
            )
        canonical: List[str] = []
        warnings: List[str] = []
        for raw in linked_entities:
            result = await resolve_entity(raw, storage=self._storage)
            canonical.extend(result.canonical_forms)
            warnings.extend(result.warnings)
        canonical = _dedup(canonical)
        warnings = _dedup(warnings)
        saved = await self._storage.save_learning(
            body=learning, entities=canonical
        )
        return SaveLearningResponse(
            learning_id=saved.id,
            resolved_entities=canonical,
            warnings=warnings,
        )

    # ---- save_query ----------------------------------------------------

    async def save_query(
        self, *, query: QueryInput, description: str
    ) -> SaveQueryResponse:
        if not description or not description.strip():
            raise ValueError("description must be a non-empty string.")
        materialised = _coerce_query(query)
        extraction = await extract_entities_from_query(
            materialised, storage=self._storage
        )
        saved = await self._storage.save_saved_query(
            query=materialised,
            description=description,
            entities=extraction.canonical_forms,
        )
        return SaveQueryResponse(
            query_id=saved.id,
            resolved_entities=extraction.canonical_forms,
            warnings=extraction.warnings,
        )

    # ---- delete_learning_or_query --------------------------------------

    async def delete_learning_or_query(
        self, *, identifier: str
    ) -> DeleteResponse:
        kind = _classify_id(identifier)
        if kind == "learning":
            await self._storage.delete_learning(identifier)
        else:
            await self._storage.delete_saved_query(identifier)
        return DeleteResponse(deleted_id=identifier, kind=kind)

    # ---- recall --------------------------------------------------------

    async def recall(
        self,
        *,
        entities: Optional[List[str]] = None,
        query: Optional[QueryInput] = None,
        max_learnings: Optional[int] = None,
        max_queries: Optional[int] = 2,
    ) -> RecallResponse:
        # Treat empty list identically to None so callers don't have to
        # special-case "I want everything matching the query alone".
        ent_list = entities or None
        if not ent_list and query is None:
            raise ValueError(
                "recall requires at least one of `entities` or `query`."
            )

        canonical: List[str] = []
        warnings: List[str] = []
        if ent_list:
            for raw in ent_list:
                result = await resolve_entity(raw, storage=self._storage)
                canonical.extend(result.canonical_forms)
                warnings.extend(result.warnings)
        if query is not None:
            extraction = await extract_entities_from_query(
                _coerce_query(query), storage=self._storage
            )
            canonical.extend(extraction.canonical_forms)
            warnings.extend(extraction.warnings)
        canonical = _dedup(canonical)
        warnings = _dedup(warnings)
        if not canonical:
            return RecallResponse(
                resolved_input_entities=[], warnings=warnings
            )

        wanted = set(canonical)

        # Storage already filters down to non-empty intersections. Re-
        # rank in Python: descending by match_count, then descending by
        # created_at so ties prefer the most recent record.
        candidate_learnings = await self._storage.list_learnings(
            entities=canonical
        )
        candidate_queries = await self._storage.list_saved_queries(
            entities=canonical
        )

        learning_hits = [
            _learning_to_hit(
                lr, sorted(wanted & set(lr.entities))
            )
            for lr in candidate_learnings
            if lr.entities
        ]
        query_hits = [
            _saved_query_to_hit(
                sq, sorted(wanted & set(sq.entities))
            )
            for sq in candidate_queries
            if sq.entities
        ]
        # Recency tiebreaker — pull created_at from the source records
        # for stable sort. We can't read created_at off the hit (it's
        # not on the response shape), so zip the source list with the
        # hits.
        learning_hits_with_ts = [
            (lr.created_at, hit)
            for lr, hit in zip(
                [lr for lr in candidate_learnings if lr.entities],
                learning_hits,
            )
        ]
        query_hits_with_ts = [
            (sq.created_at, hit)
            for sq, hit in zip(
                [sq for sq in candidate_queries if sq.entities],
                query_hits,
            )
        ]
        learning_hits_with_ts.sort(
            key=lambda t: (t[1].match_count, t[0]), reverse=True
        )
        query_hits_with_ts.sort(
            key=lambda t: (t[1].match_count, t[0]), reverse=True
        )
        learnings = [hit for _, hit in learning_hits_with_ts]
        queries = [hit for _, hit in query_hits_with_ts]
        if max_learnings is not None:
            learnings = learnings[:max_learnings]
        if max_queries is not None:
            queries = queries[:max_queries]

        return RecallResponse(
            learnings=learnings,
            queries=queries,
            resolved_input_entities=canonical,
            warnings=warnings,
        )


def _format_friendly_error(exc: Exception) -> str:
    """Render a typed error for the MCP surface as a single-line string.

    Matches the existing convention in ``slayer/mcp/server.py`` of never
    raising back to the agent — the response text carries the message.
    """
    return f"Error: {type(exc).__name__}: {exc}"


# Public helper used by the MCP layer.
__all__ = [
    "LearningService",
    "QueryInput",
    "_format_friendly_error",
]


# Pyright's narrow attribute-access checks on dynamic args are noisy here;
# the value of these helpers is the runtime contract, not static typing.
_ = Any  # keep `Any` referenced for future signature changes
