"""Typed plan shapes (``PlannedQuery`` et al.) the SQL generator compiles to SQL."""

from __future__ import annotations

import functools
from enum import Enum, IntEnum
from typing import Dict, List, Literal, Optional, Tuple, Union, Hashable

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from slayer.core.enums import DataType, JoinType
from slayer.core.errors import MaterialisationStageError
from slayer.core.format import NumberFormat
from slayer.core.keys import (
    AggregateKey,
    Phase,
    TransformKey,
    ValueKey,
    walk_value_keys,
)
from slayer.core.models import SlayerModel
from slayer.core.scope import StageSchema
from slayer.ir.bound import BoundExpr

SlotId = str
BoundFilterId = str


__all__ = [
    "BoundFilterId",
    "EmptyBaseGrainPlan",
    "FilterReachability",
    "JoinRequirement",
    "MaskEntry",
    "MaskTyping",
    "ModeAFilter",
    "OrderEntry",
    "PickedParam",
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
    "Stage",
    "StageKind",
    "TrailingWindowProducerKernel",
    "TransformLayer",
    "ValueSlot",
]


class StageKind(IntEnum):
    """The relation in the emitted pipeline a value materialises in (P6/P11).

    Ordered: a value's inputs always materialise at an earlier stage. ``DERIVED``
    carries a 1-based ``level`` — only transforms stratify (D10): a value sits one
    level above the deepest transform it reads, composites rendering inline.
    """

    BASE = 0
    PRODUCER = 1
    COMBINED = 2
    DERIVED = 3


@functools.total_ordering
class Stage(BaseModel):
    """One materialisation stage: ``kind`` plus a 1-based ``level`` for DERIVED."""

    model_config = ConfigDict(frozen=True)

    kind: StageKind
    level: int = 0

    @model_validator(mode="after")
    def _level_matches_kind(self) -> "Stage":
        if self.kind is StageKind.DERIVED:
            if self.level < 1:
                raise ValueError("DERIVED stage requires a 1-based level >= 1")
        elif self.level != 0:
            raise ValueError(f"{self.kind.name} stage must not carry a level")
        return self

    def _order_key(self) -> Tuple[int, int]:
        return (int(self.kind), self.level)

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Stage):
            return NotImplemented
        return self._order_key() < other._order_key()


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
    #: Planner-assigned materialisation stage (P6); None only on a plan the
    #: staging pass has not run over — the generator refuses to render one.
    stage: Optional[Stage] = None
    #: Whether this value must be projected as a column of its own relation for
    #: a later stage / consumer (D4); a mask-only value renders as a predicate.
    needs_column: bool = False
    #: For a transform slot, whether it shifts its materialised series (True) or
    #: re-aggregates (False); None for non-transform values (D6).
    series: Optional[bool] = None
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


class MaskTyping(str, Enum):
    """How a position expression evaluates: field (row-level) or measure (query grain)."""

    FIELD = "field"
    MEASURE = "measure"


class MaskEntry(BaseModel):
    """One typed filter conjunct compiled to a hidden slot; the value masks, is never returned.

    ``stratum`` 0 = aggregate-free field conjunct defining the row population every
    producer sees; 1 = masks at its own grain (attached-ref field / measure)."""

    slot_id: SlotId
    typing: MaskTyping
    stratum: int


class ModeAFilter(BaseModel):
    """A Mode-A model-filter text (model definition, not a query position); renders in the base WHERE."""

    id: BoundFilterId
    text: str


class OrderEntry(BaseModel):
    """One ORDER BY entry; the producing scope is classified at emission-side lowering."""

    slot_id: SlotId
    direction: Literal["asc", "desc"]
    phase: Phase
    #: ``"default"`` = NULLs last on every dialect; the dialect strategy owns the spelling.
    nulls: Literal["default", "first", "last"] = "default"


class FilterReachability(BaseModel):
    """One mask's structural reachability in THIS plan's coordinates (recomputed per plan); ``filter_id`` is the mask's slot id."""

    model_config = ConfigDict(frozen=True)

    filter_id: BoundFilterId
    crossed_join_paths: Tuple[Tuple[str, ...], ...] = ()
    has_host_local_ref: bool = False


class EmptyBaseGrainPlan(BaseModel):
    """Host base has no columns of its own — ``_base`` is a one-row spine for the CROSS
    JOIN. ``host_filter_ids`` (if any) gate it via ``FROM <host> WHERE ... LIMIT 1``, the
    LIMIT stopping the N filtered rows from repeating the scalar N times.
    ``host_gated`` (DEV-1909) marks a population restricted by a correlated semi-join, so
    the spine builds the host FROM and applies the EXISTS even without a plain field mask."""

    host_filter_ids: List[BoundFilterId] = Field(default_factory=list)
    host_gated: bool = False


class SemiJoinHop(BaseModel):
    """One node of a semi-join correlation tree, joined from its parent node (the
    producer root for a first hop) on oriented ``join_pairs`` (parent_col, hop_col).
    ``node_path`` is the node's identity — repeated models bind distinct aliases."""

    model_config = ConfigDict(frozen=True)

    target_model: str
    join_pairs: Tuple[Tuple[str, str], ...]
    node_path: Tuple[str, ...]
    #: The declared edge is LEFT (default) rather than INNER — the null-rejection
    #: analysis's input (a declared-INNER hop is never null-extended) (DEV-1935).
    declared_left: bool = True
    #: The hop renders LEFT-joined from a one-row spine (its NULL extension can
    #: satisfy the predicate); otherwise today's inner correlation (DEV-1935).
    null_extended: bool = False

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


class PickedParam(BaseModel):
    """An aggregation parameter lifted onto the two-level kernel:
    picked once per level-1 cell as ``MAX(<value>) AS _p<i>`` and read by level 2
    as ``_base._p<i>``. Exactly one source form is set — ``key`` (a column /
    placeholder / composite value key rendered through the scope, with a
    ``ColumnSqlKey`` taking the derived expansion) or ``sql`` (a canonical Mode-A
    fragment for an expression default, in producer-root coordinates — DEV-1908 D8
    — so it always enters at the producer root)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    key: Optional[ValueKey] = None
    sql: Optional[str] = None


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
    #: Set for a windowed ``first``/``last``: the column ``_src`` ranks by within
    #: the interval (``_w_rank``); the outer picks rank 1 per bucket.
    ranking_time_key: Optional[ValueKey] = None
    #: Reference-bearing parameters (column / attached-aggregate / column-naming
    #: default) read per interval row as ``_src._w_p<i>``; literals never lift.
    picked_params: List[PickedParam] = Field(default_factory=list)


class AssociationProducerKernel(BaseModel):
    """A distinct-entity association producer (DEV-1841): level 1 groups by
    (grain × the root's ``entity_keys``) picking each input once per entity;
    level 2 aggregates over the picked rows per grain. ``entity_keys`` are the
    root's unique-key columns in the producer's coordinates.

    ``null_safe`` (DEV-1847) keeps NULL entity cells as distinct cells instead of
    excluding them — a re-aggregation's entity is an inner-grain cell whose NULL
    component is its own cell (null-safe second-order aggregation).

    ``picked_params`` are the aggregation parameters the grain
    determines, each picked once per level-1 cell alongside the aggregate's own
    value; level 2 reads them as ``_base._p<i>``."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    kind: Literal["association"] = "association"
    entity_keys: List[ValueKey] = Field(default_factory=list)
    null_safe: bool = False
    picked_params: List[PickedParam] = Field(default_factory=list)
    #: Host-side join columns of the reverse hop (in the home-rooted producer's
    #: coordinates), guarded ``NOT (<col> IS NULL)`` in level 1 so a home entity
    #: absent from the population is excluded from a cell it reaches only back
    #: through the population root (DEV-1910); empty for a home-side dimension.
    present_keys: List[ValueKey] = Field(default_factory=list)


ProducerKernel = Union[
    PlainProducerKernel, RankedProducerKernel, TrailingWindowProducerKernel,
    AssociationProducerKernel,
]


class RegroupAttachPlan(BaseModel):
    """A planner-synthesized producer (isolated ``_cm_*`` CTE) attached on its partition
    grain via the null-safe grain join, without changing consumer cardinality.
    ``attach_phase`` is ``"row"`` (base FROM, before aggregation), ``"combined"`` (after),
    or ``"shifted"`` (the frame-free relation a ``time_shift`` slot ``shift_of`` looks up,
    reading ``answer_slot_id``; no substitutions)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    producer_plan: "PlannedQuery"
    alias_hint: str
    attach_phase: Literal["row", "combined", "shifted"] = "row"
    shift_of: Optional[SlotId] = None
    answer_slot_id: Optional[SlotId] = None
    kernel: ProducerKernel = Field(
        default_factory=PlainProducerKernel, discriminator="kind",
    )
    join_pairs: List[Tuple[ValueKey, SlotId]] = Field(default_factory=list)
    substitutions: List[RegroupSubstitution] = Field(default_factory=list)
    partition_display: List[str] = Field(default_factory=list)
    producer_root_model: Optional[str] = None
    broadcast_measure: Optional[str] = None
    broadcast_dimensions: List[Tuple[str, str]] = Field(default_factory=list)
    # Associate-mode counterparts (DEV-1841): the aggregate resolved by
    # distinct-entity association, and the unattributable dimensions its cells
    # are not additive across (empty for an explicit ``partition_by=`` grain).
    associated_measure: Optional[str] = None
    associated_dimensions: List[str] = Field(default_factory=list)
    # Reachable-but-unsafe conjuncts inlined on a home-rooted association
    # producer's joins (DEV-1910): carried here since ``semi_join_filters`` is
    # empty for association producers, so the informational entry a semi-join
    # push would raise is kept.
    association_restricted_filter_texts: List[str] = Field(default_factory=list)
    # Public measure names for a population semi-join inherited into a host-rooted
    # producer (DEV-1909): the informational entry names each of the producer's own
    # public measures (a producer may carry several), not its internal stage alias.
    population_semi_join_measures: List[str] = Field(default_factory=list)
    # Public measure name for a semi-join pushed into a target-rooted producer whose
    # ``alias_hint`` is the CANONICAL stage alias (DEV-1935): the informational entry
    # names the public measure, not that internal alias.
    semi_join_measure: Optional[str] = None
    # Degenerate second-order aggregation (DEV-1847): the re-aggregation whose
    # operand grain equals the outer grain (the identity), with both grains for
    # the warning.
    degenerate_measure: Optional[str] = None
    degenerate_operand_grain: List[str] = Field(default_factory=list)
    degenerate_outer_grain: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _shift_fields_only_on_shifted(self) -> "RegroupAttachPlan":
        shifted = self.attach_phase == "shifted"
        if shifted != (self.shift_of is not None) or shifted != (self.answer_slot_id is not None):
            raise ValueError("shift_of / answer_slot_id are set exactly on a shifted attach")
        return self


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
    # Typed filter conjuncts over hidden slots; the first ``n_date_range_masks``
    # are synthesized date-range bounds (they render before the Mode-A texts).
    masks: List[MaskEntry] = Field(default_factory=list)
    n_date_range_masks: int = Field(default=0, ge=0)
    mode_a_filters: List[ModeAFilter] = Field(default_factory=list)
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

    @model_validator(mode="after")
    def _shifted_attaches_target_shift_slots(self) -> "PlannedQuery":
        """Each non-series ``time_shift`` slot of this plan has exactly one shifted attach."""
        by_id = {s.id: s for s in _own_slots(self)}
        seen: set = set()
        for attach in self.regroup_attach_plans:
            if attach.attach_phase != "shifted":
                continue
            slot = by_id.get(attach.shift_of) if attach.shift_of is not None else None
            if slot is None or not (
                isinstance(slot.key, TransformKey) and slot.key.op == "time_shift"
            ):
                raise ValueError(
                    f"shifted attach targets {attach.shift_of!r}, not a time_shift slot",
                )
            if slot.series:
                raise ValueError(
                    f"shifted attach targets the series-regime slot {slot.id!r}",
                )
            if slot.id in seen:
                raise ValueError(f"duplicate shifted attach for slot {slot.id!r}")
            seen.add(slot.id)
            if attach.substitutions:
                raise ValueError("a shifted attach substitutes nothing")
            if attach.answer_slot_id not in {s.id for s in _own_slots(attach.producer_plan)}:
                raise ValueError(
                    f"shifted attach answer slot {attach.answer_slot_id!r} "
                    f"is not a producer slot",
                )
        orphans = sorted(
            s.id for s in by_id.values()
            if isinstance(s.key, TransformKey) and s.key.op == "time_shift"
            and s.series is False and s.id not in seen
        )
        if orphans:
            raise ValueError(f"time_shift slot(s) {orphans} have no shifted attach")
        return self

    @model_validator(mode="after")
    def _materialisation_stage_invariant(self) -> "PlannedQuery":
        """Every value carries one stage, and no value references a value staged
        later than itself (it would render before its inputs). Recurses into
        every producer plan (D7)."""
        _validate_stage_order(self)
        return self


def _own_slots(pq: "PlannedQuery") -> List[ValueSlot]:
    return [*pq.row_slots, *pq.aggregate_slots, *pq.combined_expression_slots]


def _transforms_read(key: ValueKey):
    """Yield the ``TransformKey``s ``key`` reads: composites are transparent, a
    transform or aggregate is terminal (D3/D10 traversal)."""

    def visit(node: ValueKey):
        if isinstance(node, TransformKey):
            yield node
            return
        if isinstance(node, AggregateKey):
            return
        for child in node.children():
            yield from visit(child)

    for child in key.children():
        yield from visit(child)


def _check_no_later_reference(slot: "ValueSlot", *, by_key: Dict) -> None:
    for ref in walk_value_keys(slot.key):
        if ref is slot.key:
            continue
        dep = by_key.get(ref)
        if dep is None or dep is slot:
            continue
        assert slot.stage is not None and dep.stage is not None
        if slot.stage < dep.stage:
            raise MaterialisationStageError(
                f"value {slot.id!r} (stage {slot.stage.kind.name}) "
                f"references {dep.id!r} staged later "
                f"({dep.stage.kind.name}); it would render before its "
                f"inputs are materialised.",
            )


def _check_transform_strictness(slot: "ValueSlot", *, by_key: Dict) -> None:
    """D7 explicit strictness: a value is staged strictly later than every
    transform it reads (a composite operand renders inline, so a consumer
    may share a composite's level — never a transform's). A transform
    read must resolve to a slot: nothing else can ever emit it."""
    for t_key in _transforms_read(slot.key):
        dep = by_key.get(t_key)
        if dep is None:
            raise MaterialisationStageError(
                f"value {slot.id!r} reads a transform "
                f"({t_key.op!r}) that no slot in this plan materialises.",
            )
        if dep is slot:
            continue
        assert slot.stage is not None and dep.stage is not None
        if dep.stage >= slot.stage:
            raise MaterialisationStageError(
                f"value {slot.id!r} (stage {slot.stage.kind.name} level "
                f"{slot.stage.level}) must be staged strictly later than "
                f"the transform {dep.id!r} it reads (level "
                f"{dep.stage.level}).",
            )


def _validate_stage_order(pq: "PlannedQuery") -> None:
    """Enforce the materialisation-stage invariant on one plan and, recursively,
    every producer plan it attaches."""
    slots = _own_slots(pq)
    unstaged = [s.id for s in slots if s.stage is None]
    if unstaged:
        raise MaterialisationStageError(
            f"plan on {pq.source_relation!r} leaves value(s) {sorted(unstaged)} "
            f"unstaged; every value must carry one materialisation stage.",
        )
    by_key = {s.key: s for s in slots}
    for slot in slots:
        # A computed-dimension slot resolves by its grouped alias (BASE); its
        # internals (a placeholder joined into _base) are dependency-terminal and
        # impose no ordering (Codex F7).
        if slot.is_dimension:
            continue
        _check_no_later_reference(slot, by_key=by_key)
        _check_transform_strictness(slot, by_key=by_key)
    for attach in pq.regroup_attach_plans:
        _validate_stage_order(attach.producer_plan)


# ``producer_plan`` forward-references ``PlannedQuery``.
RegroupAttachPlan.model_rebuild()


def regroup_producer_identity(attach: RegroupAttachPlan) -> Hashable:
    """Interning identity of a regroup producer: its root plus the full structural spec of the producer body (never the render-level attach coordinates)."""
    return (
        attach.producer_root_model,
        _structural_fingerprint(attach.kernel),
        _structural_fingerprint(attach.producer_plan),
    )


def _structural_fingerprint(obj) -> Hashable:
    if isinstance(obj, BaseModel):
        return (
            type(obj).__name__,
            tuple(
                (name, _structural_fingerprint(getattr(obj, name)))
                for name in type(obj).model_fields
            ),
        )
    if isinstance(obj, (list, tuple)):
        return tuple(_structural_fingerprint(x) for x in obj)
    if isinstance(obj, (set, frozenset)):
        return frozenset(_structural_fingerprint(x) for x in obj)
    if isinstance(obj, dict):
        return tuple(sorted(
            (
                (_structural_fingerprint(k), _structural_fingerprint(v))
                for k, v in obj.items()
            ),
            key=repr,
        ))
    if isinstance(obj, Enum) or obj is None or isinstance(
        obj, (str, int, float, bool, bytes),
    ):
        return obj
    return (type(obj).__name__, repr(obj))
