"""Typed plan shapes (``PlannedQuery`` et al.) the SQL generator compiles to SQL."""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from slayer.core.enums import DataType, JoinType
from slayer.core.format import NumberFormat
from slayer.core.keys import Phase, ValueKey
from slayer.core.models import SlayerModel
from slayer.core.scope import StageSchema
from slayer.engine.binding import BoundExpr  # re-exported below


SlotId = str
BoundFilterId = str


__all__ = [
    "BoundExpr",
    "BoundFilterId",
    "EmptyBaseGrainPlan",
    "FilterPhase",
    "FilterReachability",
    "JoinRequirement",
    "OrderEntry",
    "OrderScope",
    "PlainProducerKernel",
    "PlannedQuery",
    "ProducerKernel",
    "RankedGrainMember",
    "RankedProducerKernel",
    "RegroupAttachPlan",
    "RegroupSubstitution",
    "SemiJoinFilter",
    "SemiJoinHop",
    "SlotId",
    "TrailingWindowProducerKernel",
    "TransformLayer",
    "ValueSlot",
]


class ValueSlot(BaseModel):
    """One materialised slot: identity is ``key`` (equal keys share a slot), rendering metadata here."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: SlotId
    key: ValueKey
    declared_name: str
    public_name: Optional[str] = None
    public_aliases: List[str] = Field(default_factory=list)
    hidden: bool = False
    phase: Phase
    label: Optional[str] = None
    type: Optional[DataType] = None
    type_is_explicit: bool = False
    preserve_native_type: bool = False
    is_dimension: bool = False
    expression: Optional[BoundExpr] = None
    format: Optional[NumberFormat] = None
    description: Optional[str] = None

    @property
    def cast_type(self) -> Optional[DataType]:
        """Return only casts that preserve the inferred database value."""
        if not self.type_is_explicit and (
            self.preserve_native_type
            or (self.type == DataType.INT and self.phase != Phase.ROW)
        ):
            return None
        return self.type

    @model_validator(mode="after")
    def _hidden_invariant(self) -> "ValueSlot":
        if self.hidden and (self.public_name is not None or self.public_aliases):
            raise ValueError(
                f"ValueSlot(id={self.id!r}) is hidden but carries "
                f"public_name={self.public_name!r} / "
                f"public_aliases={self.public_aliases!r}; hidden slots "
                f"must have public_name=None and public_aliases=[]."
            )
        return self


class JoinRequirement(BaseModel):
    """One hop in a cross-model join chain (typed-plan mirror of ``ModelJoin``)."""

    source_model: str
    target_model: str
    join_pairs: List[List[str]]
    join_type: JoinType = JoinType.LEFT

    @field_validator("join_pairs")
    @classmethod
    def _non_empty(cls, v: List[List[str]]) -> List[List[str]]:
        if not v:
            raise ValueError("join_pairs must be non-empty")
        for i, pair in enumerate(v):
            if len(pair) != 2 or not all(isinstance(s, str) and s for s in pair):
                raise ValueError(
                    f"join_pairs[{i}] must be [source_dim, target_dim] "
                    f"with non-empty strings, got {pair!r}"
                )
        return v


class SrcFilterRewrite(BaseModel):
    """A ROW filter whose CTE-local form keeps only its population half (frame bounds dropped)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    filter_id: BoundFilterId
    expression: BoundExpr


class RankedGrainMember(BaseModel):
    """A grain member in both coordinate systems; one list so PARTITION BY and the join-back can't drift."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    host_slot_id: SlotId
    ranked_key: ValueKey


class TransformLayer(BaseModel):
    """Window/temporal transforms grouped so the generator emits them in the right order."""

    op: str
    slot_ids: List[SlotId]


class FilterPhase(BaseModel):
    """A bound filter routed to its phase (ROW→WHERE, AGGREGATE→HAVING, POST→outer)."""

    id: BoundFilterId
    phase: Phase
    text: Optional[str] = None
    expression: Optional[BoundExpr] = None


class OrderScope(str, Enum):
    """WHERE the ordered value lives — the one fact a renderer needs to build a sort term."""

    HOST_BASE = "host_base"
    #: In ``_base`` but trimmed from the public projection (order-only / unprojected).
    HOST_BASE_HIDDEN = "host_base_hidden"
    CROSS_MODEL_CTE = "cross_model_cte"
    WINDOWED_CTE = "windowed_cte"
    RANKED_CTE = "ranked_cte"
    TRANSFORM_STEP = "transform_step"
    OUTER_COMPOSITE = "outer_composite"


class OrderEntry(BaseModel):
    """One ORDER BY entry. ``scope``/``phase`` are required so an unclassified sort fails loudly."""

    slot_id: SlotId
    direction: Literal["asc", "desc"]
    scope: OrderScope
    phase: Phase
    #: ``"default"`` = NULLs last on every dialect; the dialect strategy owns the spelling.
    nulls: Literal["default", "first", "last"] = "default"


class FilterReachability(BaseModel):
    """One filter's structural reachability in THIS plan's coordinates (recomputed per plan)."""

    model_config = ConfigDict(frozen=True)

    filter_id: BoundFilterId
    crossed_join_paths: Tuple[Tuple[str, ...], ...] = ()
    has_host_local_ref: bool = False


class EmptyBaseGrainPlan(BaseModel):
    """Host base has no columns of its own — ``_base`` is a one-row spine for the CROSS
    JOIN. ``host_filter_ids`` (if any) gate it via ``FROM <host> WHERE ... LIMIT 1``, the
    LIMIT stopping the N filtered rows from repeating the scalar N times."""

    host_filter_ids: List[BoundFilterId] = Field(default_factory=list)


class SemiJoinHop(BaseModel):
    """One node of a semi-join correlation tree, joined from its parent node (the
    producer root for a first hop) on oriented ``join_pairs`` (parent_col, hop_col).
    ``node_path`` is the node's identity — repeated models bind distinct aliases."""

    model_config = ConfigDict(frozen=True)

    target_model: str
    join_pairs: Tuple[Tuple[str, str], ...]
    node_path: Tuple[str, ...]

    @property
    def node_id(self) -> str:
        return "/".join(self.node_path)


class SemiJoinFilter(BaseModel):
    """One correlated EXISTS pushed into a producer: ``hops`` (parents first) form
    the subquery's join tree; ``conjuncts`` (producer-root coordinates, paths =
    tree-node paths) AND together inside it; ``filter_texts`` are diagnostics."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    hops: List[SemiJoinHop]
    conjuncts: List[ValueKey]
    filter_texts: List[Optional[str]] = Field(default_factory=list)


class RegroupSubstitution(BaseModel):
    """One consumed aggregate: ``placeholder`` resolves to ``producer_slot_id``'s column."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    placeholder: ValueKey
    producer_slot_id: SlotId
    original_key: ValueKey


class PlainProducerKernel(BaseModel):
    """A grouped-aggregate producer (the default)."""

    kind: Literal["plain"] = "plain"


class RankedProducerKernel(BaseModel):
    """A ``first``/``last`` producer: one row per grain group, picked by ranking."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    kind: Literal["ranked"] = "ranked"
    agg: Literal["first", "last"]
    ranking_time_key: ValueKey


class TrailingWindowProducerKernel(BaseModel):
    """A trailing-window producer: per bucket, aggregate source rows in the trailing interval."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    kind: Literal["trailing-window"] = "trailing-window"
    window_raw: str
    window_parts: List[Tuple[int, str]]
    window_granularity: str
    bucket_slot_id: SlotId
    #: ROW filters inherited into ``_src`` — frame bounds excluded.
    src_where_filter_ids: List[BoundFilterId] = Field(default_factory=list)
    src_filter_rewrites: List["SrcFilterRewrite"] = Field(default_factory=list)


ProducerKernel = Union[
    PlainProducerKernel, RankedProducerKernel, TrailingWindowProducerKernel,
]


class RegroupAttachPlan(BaseModel):
    """A planner-synthesized producer (isolated ``_cm_*`` CTE) attached on its partition
    grain via the null-safe grain join, without changing consumer cardinality.
    ``attach_phase`` is ``"row"`` (base FROM, before aggregation) or ``"combined"`` (after)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    producer_plan: "PlannedQuery"
    alias_hint: str
    attach_phase: Literal["row", "combined"] = "row"
    kernel: ProducerKernel = Field(
        default_factory=PlainProducerKernel, discriminator="kind",
    )
    join_pairs: List[Tuple[ValueKey, SlotId]] = Field(default_factory=list)
    substitutions: List[RegroupSubstitution] = Field(default_factory=list)
    partition_display: List[str] = Field(default_factory=list)
    producer_root_model: Optional[str] = None
    dropped_filter_warnings: List[Any] = Field(default_factory=list)
    broadcast_measure: Optional[str] = None
    broadcast_dimensions: List[Tuple[str, str]] = Field(default_factory=list)


class PlannedQuery(BaseModel):
    """The fully typed plan for one query stage, consumed by the SQL generator."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    source_relation: str
    join_plan: List[JoinRequirement] = Field(default_factory=list)
    row_slots: List[ValueSlot] = Field(default_factory=list)
    aggregate_slots: List[ValueSlot] = Field(default_factory=list)
    regroup_attach_plans: List["RegroupAttachPlan"] = Field(default_factory=list)
    combined_expression_slots: List[ValueSlot] = Field(default_factory=list)
    transform_layers: List[TransformLayer] = Field(default_factory=list)
    filters_by_phase: List[FilterPhase] = Field(default_factory=list)
    projection: List[SlotId] = Field(default_factory=list)
    order: List[OrderEntry] = Field(default_factory=list)
    limit: Optional[int] = None
    offset: Optional[int] = None
    stage_schema: Optional[StageSchema] = None
    # Active-TD slot (None if none); time-needing transforms use it for the OVER ORDER BY.
    active_time_dimension_slot_id: Optional[SlotId] = None
    render_source_model: Optional[SlayerModel] = None
    distinct_dimension_values: bool = True
    # Time-dim columns where an explicit bound is a FRAME bound, not a population filter.
    frame_bound_columns: List[ValueKey] = Field(default_factory=list)
    # AGGREGATE filters applied on the OUTER SELECT, not HAVING in a ``_cm_*`` CTE —
    # HAVING there + the LEFT JOIN would resurface a NULL-aggregate host row.
    outer_where_filter_ids: List[BoundFilterId] = Field(default_factory=list)
    filter_reachability: List[FilterReachability] = Field(default_factory=list)
    empty_base_plan: Optional[EmptyBaseGrainPlan] = None
    # Filters pushed into this (producer) plan as correlated EXISTS semi-joins.
    semi_join_filters: List[SemiJoinFilter] = Field(default_factory=list)

    @model_validator(mode="after")
    def _projection_is_public_and_well_formed(self) -> "PlannedQuery":
        """``projection`` is the one public column list: no hidden slot, no slot over its declared-name count."""
        by_id = {
            slot.id: slot
            for slot in (
                list(self.row_slots)
                + list(self.aggregate_slots)
                + list(self.combined_expression_slots)
            )
        }
        counts: Dict[SlotId, int] = {}
        for sid in self.projection:
            counts[sid] = counts.get(sid, 0) + 1
        for sid, count in counts.items():
            slot = by_id.get(sid)
            if slot is None:
                continue
            if slot.hidden:
                raise ValueError(
                    f"hidden slot {sid!r} appears in the public projection; "
                    f"hidden slots carry no public name and must be absent",
                )
            declared = len(slot.public_aliases) or (1 if slot.public_name else 0)
            if declared and count > declared:
                raise ValueError(
                    f"slot {sid!r} appears {count} times in the public "
                    f"projection but declares only {declared} public name(s) "
                    f"{list(slot.public_aliases) or [slot.public_name]!r} — "
                    f"the extra occurrence would emit a duplicate column",
                )
        return self


# ``producer_plan`` forward-references ``PlannedQuery``.
RegroupAttachPlan.model_rebuild()
