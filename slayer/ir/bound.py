"""Bound expression forms: the binder's output types, read by the renderer."""

from __future__ import annotations

from typing import Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.keys import Phase, ValueKey


class BoundFilter(BaseModel):
    """A bound filter predicate: ``value_key`` (like ``BoundExpr``), ``phase``
    (max phase any referenced slot reaches), and ``referenced_keys`` (every
    ``ValueKey`` in the tree, for the cross-model planner's filter routing)."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    value_key: ValueKey
    phase: Phase
    referenced_keys: Tuple[ValueKey, ...] = Field(default_factory=tuple)


class BoundExpr(BaseModel):
    """A bound expression — its leaves are resolved ``ValueKey``s. ``routed_dotted``
    is the full routed dotted path when the whole field is a short-form
    ``DottedRef`` that auto-routed (DEV-1856), else ``None`` — the naming layer
    surfaces a routed dimension under this full path, not the short form typed."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    value_key: ValueKey
    routed_dotted: Optional[str] = None

    @property
    def phase(self) -> Phase:
        return self.value_key.phase
