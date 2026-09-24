"""Bound expression forms: the binder's output types, read by the renderer."""

from __future__ import annotations

from typing import Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.format import NumberFormat
from slayer.core.keys import (
    Phase,
    ValueKey,
    walk_value_keys,
)


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


class BoundTimeDimension(BaseModel):
    """A bound time dimension: the wrapped ``BoundExpr`` (``bound``) plus the column
    facts the checker judges — ``column_type`` and the recorded bucket
    ``upstream_granularity`` (from a bucketed stage column or a model ``Column``'s
    ``granularity``; ``None`` for an un-bucketed column)."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    bound: BoundExpr
    column_type: Optional[DataType] = None
    upstream_granularity: Optional[TimeGranularity] = None


def bound_filter_from_key(vk: ValueKey) -> BoundFilter:
    """A ``BoundFilter`` over ``vk`` with phase and referenced keys recomputed."""
    refs = tuple(walk_value_keys(vk))
    phase = max((k.phase for k in refs), default=vk.phase)
    return BoundFilter(value_key=vk, phase=phase, referenced_keys=refs)


class DeclaredMeasure(BaseModel):
    """One declared measure; ``type`` follows the aggregation (count → INT, avg → DOUBLE, else source type)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    bound: BoundExpr
    declared_name: str
    public_name: Optional[str] = None
    label: Optional[str] = None
    canonical_alias: Optional[str] = None
    type: Optional[DataType] = None
    type_is_explicit: bool = False
    preserve_native_type: bool = False
    format: Optional[NumberFormat] = None
    description: Optional[str] = None
    # A computed dimension is a ROW-phase composite projected AND grouped; the
    # flag distinguishes it from a bare-measure expression.
    is_dimension: bool = False


class OrderSpec(BaseModel):
    """One ORDER BY entry on a query."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    bound: BoundExpr
    direction: str = "asc"
