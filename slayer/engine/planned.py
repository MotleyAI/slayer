"""Stage 7a.1 (DEV-1450) — typed plan shapes consumed by the SQL generator.

A ``PlannedQuery`` is the final, fully resolved plan that the SQL
generator (stage 7b) compiles to SQL. The plan carries everything a
renderer needs: row slots, aggregate slots, cross-model aggregate
sub-plans, transform layers, filter routing, projection / order /
limit, and an emitted ``StageSchema`` for downstream stages to bind
against.

Identity-bearing structure is in ``slayer/core/keys.py`` (the
``ValueKey`` family); the planner here associates each key with a
``SlotId`` and the rendering metadata (alias, hidden, label).

The planning logic that produces a ``PlannedQuery`` lives in other
7a substages — ``planning.py`` (ValueRegistry, TransformLowerer,
ProjectionPlanner), ``cross_model_planner.py`` (I1 strategy),
``stage_planner.py`` (multi-stage DAG). This file is the typed
target.

These types are dormant in stage 7a — no engine code consumes them
yet. Stage 7b's engine cutover routes through them.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from slayer.core.enums import DataType, JoinType
from slayer.core.errors import UnreachableFilterDroppedWarning
from slayer.core.keys import Phase, ValueKey
from slayer.core.scope import StageSchema
from slayer.engine.binding import BoundExpr  # re-exported below


# Opaque identifier types — kept as plain ``str`` for now. SlotId is
# allocated by the planner's ValueRegistry; BoundFilterId by the
# FilterBinder. The string form keeps tracebacks readable and lets
# tests assert on them without exotic comparisons.
SlotId = str
BoundFilterId = str


# ---------------------------------------------------------------------------
# BoundExpr — re-exported from slayer.engine.binding (DEV-1450 stage 7b.6).
# ---------------------------------------------------------------------------
#
# Until stage 7b.6 the planned-side BoundExpr was a separate scaffold
# Pydantic class with an optional ``sql_text`` cache. The binder
# produced its own ``BoundExpr`` shape, so ``ValueSlot.expression`` and
# ``FilterPhase.expression`` could not store binder output directly
# without type unification (Codex HIGH F2 in the earlier round). 7b.6
# folds the two: the binder's ``BoundExpr(value_key=ValueKey)`` is the
# canonical shape. The render artifact ``sql_text`` is dropped — the
# generator renders from the typed ``value_key`` against the slot
# registry, not a cached string.
__all__ = [
    "BoundExpr",
    "BoundFilterId",
    "CrossModelAggregatePlan",
    "FilterPhase",
    "JoinRequirement",
    "OrderEntry",
    "PlannedQuery",
    "SlotId",
    "TransformLayer",
    "ValueSlot",
]


# ---------------------------------------------------------------------------
# ValueSlot
# ---------------------------------------------------------------------------


class ValueSlot(BaseModel):
    """One materialised slot in a ``PlannedQuery`` (P6).

    Identity comes from ``key`` (a ``ValueKey`` from
    ``slayer.core.keys``). Two structurally equal keys share one slot.
    Rendering metadata (alias, hidden, label, type) lives here, not on
    the key.

    ``declared_name`` is either the user-supplied ``name`` or the
    canonical form derived from the formula. ``public_name`` is the
    user-facing alias when the slot is part of the public projection
    (None for hidden slots). ``public_aliases`` carries multiple
    aliases when the same structural key was declared with multiple
    explicit names (P4 / C13).
    """

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
    expression: Optional[BoundExpr] = None

    @model_validator(mode="after")
    def _hidden_invariant(self) -> "ValueSlot":
        # Hidden slots are materialised but never surfaced — they must
        # not carry a public_name or public_aliases, otherwise the
        # generator would emit them in the public projection.
        if self.hidden and (self.public_name is not None or self.public_aliases):
            raise ValueError(
                f"ValueSlot(id={self.id!r}) is hidden but carries "
                f"public_name={self.public_name!r} / "
                f"public_aliases={self.public_aliases!r}; hidden slots "
                f"must have public_name=None and public_aliases=[]."
            )
        return self


# ---------------------------------------------------------------------------
# JoinRequirement
# ---------------------------------------------------------------------------


class JoinRequirement(BaseModel):
    """One hop in a cross-model join chain.

    Mirrors the shape of ``slayer.core.models.ModelJoin`` but is
    rooted on the typed-plan side — the planner builds these from
    resolved bundle models so the SQL generator never re-walks the
    model graph.
    """

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


# ---------------------------------------------------------------------------
# CrossModelAggregatePlan
# ---------------------------------------------------------------------------


class CrossModelAggregatePlan(BaseModel):
    """Plan for one cross-model aggregate slot (P3 / I1).

    The strategy that populated this plan (today's isolated-CTE form
    or a future alternative — see ``cross_model_planner.py``) lives
    outside this struct; this is the typed result, not the algorithm.

    Filter routing is route-explicit so the SQL generator (stage 7b)
    can render each route without re-classifying:
    - ``where_filter_ids`` — host filters propagated to the CTE's WHERE
      (decision-table rows: host-local-but-targeted, joined-target-path).
    - ``having_filter_ids`` — host filters propagated as HAVING (decision-
      table row: cross-model agg-ref on the same target).
    - ``target_model_filters`` — the target model's own
      ``SlayerModel.filters`` (always-applied WHERE).
    ``applied_filter_ids`` is the audit union of where + having for
    backward compatibility with the spec's external surface.

    ``hidden=True`` is used for order-only / filter-only refs whose
    aggregate value is materialised but not surfaced in the public
    projection; ``public_alias`` is ``None`` in that case.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    aggregate_slot_id: SlotId
    target_model: str
    datasource: str
    join_chain: List[JoinRequirement]
    join_back_pairs: List[Tuple[ValueKey, ValueKey]] = Field(default_factory=list)
    cte_stage_schema: StageSchema
    shared_grain_slots: List[SlotId]
    applied_filter_ids: List[BoundFilterId] = Field(default_factory=list)
    where_filter_ids: List[BoundFilterId] = Field(default_factory=list)
    having_filter_ids: List[BoundFilterId] = Field(default_factory=list)
    target_model_filters: List[str] = Field(default_factory=list)
    dropped_filter_warnings: List[UnreachableFilterDroppedWarning] = Field(default_factory=list)
    hidden: bool = False
    public_alias: Optional[str] = None


# ---------------------------------------------------------------------------
# TransformLayer
# ---------------------------------------------------------------------------


class TransformLayer(BaseModel):
    """One transform layer in the planned query.

    Window / temporal transforms (``cumsum``, ``time_shift``,
    ``rank``, ``lag``, ``lead``, ...) are grouped into layers so the
    SQL generator can emit them in the right order (window functions
    in an inner SELECT, time_shift as a self-join CTE, etc.). The
    layer carries the slot ids that belong to it; rendering details
    are decided by the generator per ``op``.
    """

    op: str
    slot_ids: List[SlotId]


# ---------------------------------------------------------------------------
# FilterPhase
# ---------------------------------------------------------------------------


class FilterPhase(BaseModel):
    """A bound filter expression routed to its phase (P8).

    ``phase`` is the maximum phase of the slots the filter
    references: ROW → WHERE, AGGREGATE → HAVING, POST → post-filter
    on the outer SELECT.

    Two carrier modes, mutually exclusive in practice:

    * ``expression`` is a typed ``BoundExpr`` — used for the Mode-B
      DSL filters bound by ``bind_filter`` and the planner-emitted
      ``BetweenKey`` for ``TimeDimension.date_range``. The renderer
      walks the typed value-key tree.
    * ``text`` is a Mode-A SQL fragment — used for
      ``SlayerModel.filters`` (always-applied WHERE). The renderer
      qualifies bare-identifier column refs in ``text_columns`` with
      the source-relation alias and emits the result verbatim
      (matching legacy ``_build_where_and_having`` qualification).
    """

    id: BoundFilterId
    phase: Phase
    text: Optional[str] = None
    text_columns: Tuple[str, ...] = ()
    expression: Optional[BoundExpr] = None


# ---------------------------------------------------------------------------
# OrderEntry
# ---------------------------------------------------------------------------


class OrderEntry(BaseModel):
    """One entry in the ORDER BY of a planned query."""

    slot_id: SlotId
    direction: str  # "asc" or "desc"

    @field_validator("direction")
    @classmethod
    def _validate_direction(cls, v: str) -> str:
        if v not in ("asc", "desc"):
            raise ValueError(
                f"OrderEntry.direction must be 'asc' or 'desc', got {v!r}"
            )
        return v


# ---------------------------------------------------------------------------
# PlannedQuery
# ---------------------------------------------------------------------------


class PlannedQuery(BaseModel):
    """The fully typed plan for one query stage (P7).

    Consumed by the SQL generator (stage 7b). Carries everything
    needed to emit SQL without re-walking the model graph.

    ``stage_schema`` is the projection emitted by this stage —
    downstream stages bind against it (P6). Top-level queries that
    aren't part of a multi-stage DAG can leave ``stage_schema`` as
    ``None``.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    source_relation: str
    join_plan: List[JoinRequirement] = Field(default_factory=list)
    row_slots: List[ValueSlot] = Field(default_factory=list)
    aggregate_slots: List[ValueSlot] = Field(default_factory=list)
    cross_model_aggregate_plans: List[CrossModelAggregatePlan] = Field(default_factory=list)
    combined_expression_slots: List[ValueSlot] = Field(default_factory=list)
    transform_layers: List[TransformLayer] = Field(default_factory=list)
    filters_by_phase: List[FilterPhase] = Field(default_factory=list)
    projection: List[SlotId] = Field(default_factory=list)
    order: List[OrderEntry] = Field(default_factory=list)
    limit: Optional[int] = None
    offset: Optional[int] = None
    stage_schema: Optional[StageSchema] = None
    # Stage 7b.10 — the slot id of the active TD (resolved via
    # ``_resolve_main_time_dimension``). ``None`` when the stage has no
    # time dimension. Time-needing transforms (cumsum / lag / lead /
    # first / last / time_shift / consecutive_periods) carry this slot's
    # key in ``TransformKey.time_key``; the generator uses it for the
    # ``ORDER BY`` clause of the OVER expression.
    active_time_dimension_slot_id: Optional[SlotId] = None
