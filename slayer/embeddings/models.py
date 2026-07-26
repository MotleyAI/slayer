"""Persisted Pydantic models for the Embedding sidecar table (DEV-1386).

An ``Embedding`` carries a single embedding vector for one indexable entity
(memory, datasource, model, column, named measure, custom aggregation) under
one configured embedding model name. Rows are keyed by
``(canonical_id, embedding_model_name)`` — switching the configured embedding
model leaves old rows in place but inert (they don't match the active model
name on read).

``canonical_id`` is either ``f"memory:{int}"`` for a memory or the dotted
canonical entity string (``"<ds>"``, ``"<ds>.<model>"``,
``"<ds>.<model>.<leaf>"``) for everything else. ``entity_kind`` records what
kind of doc produced the embedding so the search service can route hits.

``content_hash`` is the SHA256 of the rendered indexed text the embedding
was generated from; the service uses it to skip the litellm API call when
the source text hasn't changed since the last refresh.
"""

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from slayer.storage.migrations import migrate as _migrate_schema


EntityKind = Literal[
    "memory", "datasource", "model", "column", "measure", "aggregation",
]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Embedding(BaseModel):
    """One persisted embedding row."""

    version: int = 1
    canonical_id: str
    embedding_model_name: str
    entity_kind: EntityKind
    content_hash: str
    embedding: list[float]
    created_at: datetime = Field(default_factory=_utcnow)

    @model_validator(mode="before")
    @classmethod
    def _apply_schema_migrations(cls, data: Any) -> Any:
        return _migrate_schema(entity="Embedding", data=data)
