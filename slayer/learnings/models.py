"""Persisted Pydantic models for Learnings + saved queries (DEV-1357).

``Learning`` and ``SavedQuery`` are the row-shaped records that the storage
backends write and read. They carry a ``version: int = 1`` field so they
plug into the existing ``slayer/storage/migrations.py`` framework without
any further changes — v1 is the only schema this PR ships.

The IDs (``L<int>`` / ``Q<int>``) are allocated by the storage layer at
save time, not by the caller — the field defaults to ``""`` so callers can
construct a draft and let storage fill in the persisted ID. ``created_at``
defaults to the current UTC time so callers do not have to supply it.

Canonical entity strings are produced by the entity resolver
(``slayer/learnings/resolver.py``) before the rows ever reach storage; the
``entities`` field is therefore stored verbatim with no further validation.
"""

from datetime import datetime, timezone
from typing import Any, List, Literal

from pydantic import BaseModel, Field, model_validator

from slayer.core.query import SlayerQuery
from slayer.storage.migrations import migrate as _migrate_schema


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Learning(BaseModel):
    """A free-form note an agent has recorded against a set of canonical
    entities (e.g., ``mydb.orders.is_returned``)."""

    version: int = 1
    id: str = ""
    body: str
    entities: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=_utcnow)

    @model_validator(mode="before")
    @classmethod
    def _apply_schema_migrations(cls, data: Any) -> Any:
        return _migrate_schema(entity="Learning", data=data)


class SavedQuery(BaseModel):
    """An example ``SlayerQuery`` an agent saved alongside a human description.

    The ``query`` field always carries a fully-materialised ``SlayerQuery``
    — run-by-name input (a string model name) is materialised into a
    concrete query at save time and the materialised form is what gets
    persisted.
    """

    version: int = 1
    id: str = ""
    description: str
    query: SlayerQuery
    entities: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=_utcnow)

    @model_validator(mode="before")
    @classmethod
    def _apply_schema_migrations(cls, data: Any) -> Any:
        return _migrate_schema(entity="SavedQuery", data=data)


# ---------------------------------------------------------------------------
# MCP tool response models
# ---------------------------------------------------------------------------


class SaveLearningResponse(BaseModel):
    learning_id: str
    resolved_entities: List[str]
    warnings: List[str] = Field(default_factory=list)


class SaveQueryResponse(BaseModel):
    query_id: str
    resolved_entities: List[str]
    warnings: List[str] = Field(default_factory=list)


class DeleteResponse(BaseModel):
    deleted_id: str
    kind: Literal["learning", "query"]


class RecallHit(BaseModel):
    """A single result from ``recall``.

    ``body`` carries the renderable text — the learning's note, or a
    saved query's description plus a serialized form of the query.
    """

    id: str
    kind: Literal["learning", "query"]
    match_count: int
    matched_entities: List[str]
    body: str


class RecallResponse(BaseModel):
    learnings: List[RecallHit] = Field(default_factory=list)
    queries: List[RecallHit] = Field(default_factory=list)
    resolved_input_entities: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
