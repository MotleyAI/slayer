"""Persisted Pydantic models for the unified Memory entity (DEV-1357 v2).

A ``Memory`` carries a free-form ``learning`` text, the canonical
entities it is indexed under, and an optional ``query`` (a fully-
materialised ``SlayerQuery``). Memories without a ``query`` are surfaced
in ``inspect_model``'s Learnings section; query-bearing memories appear
only via ``search`` (in the ``example_queries`` bucket).

DEV-1428: ids are non-empty strings. The default ``id=""`` is the
"unassigned" sentinel the storage layer recognises (it allocates a
monotonic int-shaped id then). Auto-allocation only counts pure-digit,
no-leading-zero ids when picking the next value, so ``"001"`` and
``"42abc"`` do not pollute the max-int walk. User-supplied ids share
the namespace and may collide intentionally → upsert.

Forbidden charset on ``id``: ``:``, ``/``, ``?``, ``#``, whitespace,
ASCII control. Bare-name resolution never resolves to a memory — refs
must use the ``memory:<id>`` prefix.
"""

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from slayer.core.query import SlayerQuery
from slayer.storage.migrations import migrate as _migrate_schema


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# DEV-1658: ``\`` is forbidden too so a memory id is a safe single path
# segment on every platform (YAMLStorage writes ``memories/<id>.md``).
_FORBIDDEN_ID_CHARS = (":", "/", "?", "#", "\\")

#: Canonical-id prefix for cross-memory references (`memory:<id>`).
#: Re-exported from this module so the resolver, search service, and
#: ingestion cleanup all share one definition (Sonar S1192).
MEMORY_CANONICAL_PREFIX = "memory:"


def _validate_memory_id_charset(value: str) -> None:
    """Reject the forbidden charset on a memory id.

    Called from both ``Memory.id`` validation (model layer) and
    ``resolve_entity("memory:<id>")`` (resolver layer) so the rule
    has exactly one definition.
    """
    if not isinstance(value, str):
        raise ValueError(
            f"memory id must be a string; got {type(value).__name__}."
        )
    if not value:
        raise ValueError("memory id must be non-empty.")
    for ch in _FORBIDDEN_ID_CHARS:
        if ch in value:
            raise ValueError(
                f"memory id {value!r}: forbidden character {ch!r}."
            )
    for ch in value:
        if ch.isspace() or ord(ch) < 0x20 or ord(ch) == 0x7F:
            raise ValueError(
                f"memory id {value!r}: whitespace and ASCII control "
                "characters are not allowed."
            )


def is_valid_memory_id(value: str) -> bool:
    """Return ``True`` iff ``value`` would pass ``_validate_memory_id_charset``."""
    try:
        _validate_memory_id_charset(value)
    except ValueError:
        return False
    return True


#: DEV-1549: hard cap on Memory.description length. The compact-mode
#: first-paragraph fallback of learning shares the same cap so the two
#: code paths never disagree on payload size.
MEMORY_DESCRIPTION_MAX_CHARS = 500


class Memory(BaseModel):
    """A single agent memory: a note plus its canonical entity tags,
    optionally bundled with a ``SlayerQuery`` example."""

    version: int = 2
    id: str = ""
    learning: str
    description: str | None = None
    entities: list[str] = Field(default_factory=list)
    query: SlayerQuery | None = None
    created_at: datetime = Field(default_factory=_utcnow)

    @model_validator(mode="before")
    @classmethod
    def _apply_schema_migrations(cls, data: Any) -> Any:
        return _migrate_schema(entity="Memory", data=data)

    @field_validator("id", mode="before")
    @classmethod
    def _coerce_id(cls, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bool):
            raise ValueError(
                f"memory id must be a string; got bool {value!r}."
            )
        if isinstance(value, int):
            return str(value)
        return value

    @field_validator("id")
    @classmethod
    def _check_id_charset(cls, value: str) -> str:
        if value == "":
            return value
        _validate_memory_id_charset(value)
        return value

    @field_validator("learning")
    @classmethod
    def _check_learning_non_whitespace(cls, value: str) -> str:
        """DEV-1549 Codex#4: reject whitespace-only learning at the model
        layer so direct construction can never persist an unusable
        memory."""
        if not value.strip():
            raise ValueError("learning must be a non-empty string.")
        return value

    @field_validator("description", mode="before")
    @classmethod
    def _normalise_description(cls, value: Any) -> Any:
        """DEV-1549 Codex#1: empty / whitespace-only ``description`` is
        not a deliberate empty preview — coerce to ``None`` so the
        downstream compact-mode renderer falls back to the first
        paragraph of ``learning``."""
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator("description")
    @classmethod
    def _check_description_length(cls, value: str | None) -> str | None:
        """DEV-1549: hard cap so a single memory hit can never balloon
        the search payload."""
        if value is not None and len(value) > MEMORY_DESCRIPTION_MAX_CHARS:
            raise ValueError(
                f"description must be <= {MEMORY_DESCRIPTION_MAX_CHARS} "
                f"chars; got {len(value)}."
            )
        return value


# ---------------------------------------------------------------------------
# Tool / endpoint response models
# ---------------------------------------------------------------------------


class SaveMemoryResponse(BaseModel):
    memory_id: str
    resolved_entities: list[str]
    warnings: list[str] = Field(default_factory=list)


class ForgetMemoryResponse(BaseModel):
    deleted_id: str
