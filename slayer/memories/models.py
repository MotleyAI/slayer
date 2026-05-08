"""Persisted Pydantic models for the unified Memory entity (DEV-1357 v2).

A ``Memory`` carries a free-form ``learning`` text, the canonical
entities it is indexed under, and an optional ``query`` (a fully-
materialised ``SlayerQuery``). Memories without a ``query`` are surfaced
in ``inspect_model``'s Learnings section; query-bearing memories appear
only via ``recall_memories``.

Ids are monotonic positive ints allocated by the storage layer at save
time. The default ``id=0`` lets callers construct a draft and let
storage fill in the persisted id; ``created_at`` defaults to UTC now.
Canonical entity strings are produced by the resolver before rows reach
storage; the ``entities`` field is stored verbatim with no further
validation.
"""

from datetime import datetime, timezone
from typing import Any, List, Optional

from pydantic import BaseModel, Field, model_validator

from slayer.core.query import SlayerQuery
from slayer.storage.migrations import migrate as _migrate_schema


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Memory(BaseModel):
    """A single agent memory: a note plus its canonical entity tags,
    optionally bundled with a ``SlayerQuery`` example."""

    version: int = 1
    id: int = 0
    learning: str
    entities: List[str] = Field(default_factory=list)
    query: Optional[SlayerQuery] = None
    created_at: datetime = Field(default_factory=_utcnow)

    @model_validator(mode="before")
    @classmethod
    def _apply_schema_migrations(cls, data: Any) -> Any:
        return _migrate_schema(entity="Memory", data=data)


# ---------------------------------------------------------------------------
# Tool / endpoint response models
# ---------------------------------------------------------------------------


class SaveMemoryResponse(BaseModel):
    memory_id: int
    resolved_entities: List[str]
    warnings: List[str] = Field(default_factory=list)


class ForgetMemoryResponse(BaseModel):
    deleted_id: int


class RecallHit(BaseModel):
    """A single result from ``recall_memories``.

    ``learning`` is the body text. ``score`` is the BM25 relevance
    score (DEV-1365); higher is better. ``query`` is populated only
    when the memory was saved with an attached query — query-less
    memories carry ``None`` here, matching the split that
    ``RecallResponse.learnings`` vs ``.queries`` exposes.
    """

    id: int
    score: float
    matched_entities: List[str]
    learning: str
    query: Optional[SlayerQuery] = None


class RecallResponse(BaseModel):
    """``learnings`` lists memories where ``query is None``;
    ``queries`` lists memories where ``query`` is set. Each list is
    capped independently by ``max_learnings`` / ``max_queries``."""

    learnings: List[RecallHit] = Field(default_factory=list)
    queries: List[RecallHit] = Field(default_factory=list)
    resolved_input_entities: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
