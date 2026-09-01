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

from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from slayer.core.enums import DataType, JoinType
from slayer.core.errors import UnreachableFilterDroppedWarning
from slayer.core.format import NumberFormat
from slayer.core.keys import Phase, ValueKey
from slayer.core.models import SlayerModel
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
    "EmptyBaseGrainPlan",
    "FilterPhase",
    "FilterReachability",
    "JoinRequirement",
    "OrderEntry",
    "OrderScope",
    "PlannedQuery",
    "RankedAggregatePlan",
    "RankedGrainMember",
    "RegroupAttachPlan",
    "RegroupSubstitution",
    "SlotId",
    "TransformLayer",
    "ValueSlot",
    "WindowedAggregatePlan",
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
    type_is_explicit: bool = False
    # DEV-1740: True for a computed (expression) dimension — a ROW-phase
    # composite that projects AND groups, distinct from a bare-measure mistake.
    is_dimension: bool = False
    expression: Optional[BoundExpr] = None
    # DEV-1452 Stage B decision #8 — typed format / description propagated
    # by the planner from the source ``ModelMeasure`` / ``Column`` and
    # ``_infer_aggregated_format``. Consumed by the migrated
    # ``_expand_query_backed_model`` (via the public ``StageSchema``) so
    # query-backed virtual-model columns carry the same display metadata
    # the legacy enrichment pipeline produced.
    format: Optional[NumberFormat] = None
    description: Optional[str] = None

    @property
    def cast_type(self) -> Optional[DataType]:
        """Inferred INT is metadata, not a request to narrow an aggregate (#347)."""
        if self.type == DataType.INT and not self.type_is_explicit and self.phase != Phase.ROW:
            return None
        return self.type

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

    ``applied_filter_ids`` is the AUDIT: which host filters some scope
    evaluates. On the forward path that is exactly ``where ∪ having``. On a
    RE-ROOTED plan the two diverge on purpose — ``rerooted_plan`` carries the
    filters itself, and there is no forward CTE to route to, so where/having
    are empty while the audit still records them. The distinction matters
    because where/having are also an instruction to the host base to SKIP the
    filter; a re-rooted CTE duplicates a host-evaluable predicate rather than
    relocating it, so the host must keep applying it (DEV-1747 B6).

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

    # DEV-1450 stage 7b.15e (C1) — re-rooted sub-plan. When the host query
    # carries dimensions that are reachable from the target by re-rooting
    # through the target's join graph (the legacy ``_build_rerooted_enriched``
    # case), the cross-model CTE is rendered as a full nested ``PlannedQuery``
    # rooted at the target (FROM target + joins), preserving the host
    # dimension grain instead of collapsing to a scalar CROSS JOIN. ``None``
    # keeps the forward-path "FROM bare target" rendering.
    #
    # ``rerooted_grain_pairs`` maps (host_dim_slot_id, rerooted_dim_slot_id)
    # for the combined LEFT JOIN ON; the generator resolves each side's SQL
    # alias independently (host alias vs sub-plan alias need not match).
    # ``rerooted_agg_slot_id`` is the sub-plan slot id of the local aggregate;
    # the combined SELECT projects it ``AS`` the canonical / public alias.
    rerooted_plan: Optional["PlannedQuery"] = None
    rerooted_grain_pairs: List[Tuple[SlotId, SlotId]] = Field(default_factory=list)
    rerooted_agg_slot_id: Optional[SlotId] = None

    # DEV-1503 — host-rooted CTE for a cross-model-FILTERED local measure.
    # The cross-model planner has two distinct cases that produce a nested
    # ``rerooted_plan``:
    #
    # * Cross-model aggregate re-rooting (``target_model`` is the join target,
    #   the sub-plan is rooted at the target so the target's own join graph
    #   reaches host dims that the forward-path CTE collapses): ``cte_root_model``
    #   stays ``None`` and the renderer uses ``target_model`` for the FROM /
    #   joins (existing pre-DEV-1503 behaviour).
    # * Filtered-local isolation (``AggregateKey.source.path`` is empty but the
    #   measure's ``Column.filter`` crosses a join, so the aggregate must
    #   evaluate in its own CTE rooted at the HOST + the filter-target join):
    #   ``cte_root_model`` is set to the HOST model name and the renderer uses
    #   the sub-plan's own ``source_relation`` / ``render_source_model`` for
    #   the FROM. ``target_model`` is conventionally set to the host name in
    #   this case but the renderer reads ``cte_root_model`` to disambiguate.
    cte_root_model: Optional[str] = None


# ---------------------------------------------------------------------------
# WindowedAggregatePlan
# ---------------------------------------------------------------------------


class WindowedAggregatePlan(BaseModel):
    """Plan for one duration-windowed aggregate slot (DEV-1714 Stage 10).

    A windowed measure (``revenue:sum(window='90d')``) is a trailing rolling
    aggregate: for each output bucket, SLayer sums source rows in the trailing
    ``window`` interval ending at that bucket's end. It renders as a HOST-ROOTED
    ``_wm_<model>__<measure>`` CTE — an inner ``_src`` self-join subquery joined
    to ``_base`` on the query grain with an ``INTERVAL`` range predicate — then
    LEFT-JOINed back to ``_base`` on the shared grain (same join-back machinery
    as the cross-model ``_cm_*`` CTEs, so adding a windowed measure never
    changes host cardinality).

    The renderer looks up the aggregate ``ValueSlot`` (source column, ``agg``,
    result ``type``, ``column_filter_key``) and the grain ``ValueSlot``s by id
    from the owning ``PlannedQuery``; this plan carries the window duration, the
    resolved window time-dimension slot + its granularity, the per-role grain
    slot partition, and the WHERE-phase filter ids inherited into ``_src``.

    Frame bounds are excluded from ``where_filter_ids`` so the trailing window
    reaches rows before the visible frame starts (DEV-1732). A filter that is
    only PARTLY a frame bound (``created_at >= X and status = 'paid'``) stays in
    ``where_filter_ids`` and gets a ``src_filter_rewrites`` entry carrying the
    residual — the population half — which the renderer substitutes for the
    host's predicate.

    Scope for Stage 10 is ``sum``/``avg`` local measures only; cross-model,
    transform-combined, composite, hidden, and mixed-filter windowed shapes are
    guarded loudly at plan time (DEV-1504 lifts those guards).
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    aggregate_slot_id: SlotId
    agg: str
    window_raw: str
    window_parts: List[Tuple[int, str]]
    window_time_dimension_slot_id: SlotId
    window_granularity: str
    dimension_slot_ids: List[SlotId] = Field(default_factory=list)
    other_time_dimension_slot_ids: List[SlotId] = Field(default_factory=list)
    grain_slot_ids: List[SlotId] = Field(default_factory=list)
    where_filter_ids: List[BoundFilterId] = Field(default_factory=list)
    src_filter_rewrites: List["SrcFilterRewrite"] = Field(default_factory=list)
    public_alias: Optional[str] = None
    hidden: bool = False


# ---------------------------------------------------------------------------
# SrcFilterRewrite — DEV-1732
# ---------------------------------------------------------------------------


class SrcFilterRewrite(BaseModel):
    """A ROW filter whose CTE-local form differs from the host's (DEV-1732).

    Emitted when a filter is only PARTLY a frame bound, so it must still apply
    inside the CTE but with its frame-bound conjuncts removed:
    ``created_at >= '2024-06-01' and status = 'paid'`` becomes ``status =
    'paid'``.

    A filter that is ENTIRELY a frame bound needs no rewrite — the planner just
    leaves its id out of ``where_filter_ids``.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    filter_id: BoundFilterId
    expression: BoundExpr


# ---------------------------------------------------------------------------
# RankedAggregatePlan — DEV-1748
# ---------------------------------------------------------------------------


class RankedGrainMember(BaseModel):
    """One member of a ranked aggregate's grain, in BOTH coordinate systems.

    ``host_slot_id`` names the host row slot the CTE joins back to;
    ``ranked_key`` is the same value anchored in the RANKED scope, which for a
    target-rooted plan is a different expression against a different root.

    Carried as one list because the renderer derives two things from it that
    must agree: the ``PARTITION BY`` the ranking runs over, and the join-back
    predicate. Deriving them separately is how a partition and a grain drift
    apart — and a partition COARSER than the grain silently returns more than
    one row per group, which the LEFT JOIN then multiplies into the host.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    host_slot_id: SlotId
    ranked_key: ValueKey


class RankedAggregatePlan(BaseModel):
    """Plan for one ``first`` / ``last`` aggregate slot (DEV-1748, B9).

    A ranked aggregate needs its OWN ROW ORDERING, which is one of the three
    things P-C says an aggregate can need its own rows for — so like a crossing
    aggregate (``_cm_``) and a windowed one (``_wm_``) it compiles to a
    plan-shaped CTE rooted where its rows live and joined back on the query
    grain null-safely. The host base keeps only purely-local aggregates, so
    adding a first/last cannot change host cardinality.

    It replaces a shape that did the opposite: ONE first/last anywhere wrapped
    the entire host base in a ``ROW_NUMBER`` subquery, so every sibling
    aggregate in the query was computed over the ranked row set, and several
    rankings shared one scope — which is what the retired rn-suffix scheme
    (``_last_rn_2``) and the filtered sentinel columns (``_last_rn_f0`` plus a
    ``_match_f0`` flag consulted by alias) existed to disambiguate. One
    aggregate per CTE removes the need for all of it.

    ``ranking_time_key`` is resolved at PLAN time (P-D) and anchored in the
    ranked scope's coordinates; the renderer emits it and never re-derives the
    precedence. A measure's ``Column.filter`` is not carried here — it lives on
    the aggregate's own key, and the renderer applies it as a predicate inside
    this CTE, which is what "filtered variants are plan data" means.

    Filter routing uses the same vocabulary, and the same audit-versus-
    instruction distinction, as ``CrossModelAggregatePlan``: ``where`` and
    ``having`` are instructions about where a filter is EVALUATED, while
    ``applied_filter_ids`` only records that some scope evaluates it.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    aggregate_slot_id: SlotId
    #: ``first`` ranks ascending, ``last`` descending. The direction is not a
    #: separate field because it is not a separate decision.
    agg: Literal["first", "last"]
    #: The model the ranked rows come FROM — the host for a local aggregate,
    #: the join target for a cross-model one.
    root_model: str
    datasource: str
    #: The host-relative join path to ``root_model``; empty when host-rooted.
    target_path: Tuple[str, ...] = ()
    join_chain: List[JoinRequirement] = Field(default_factory=list)
    ranking_time_key: ValueKey
    grain: List[RankedGrainMember] = Field(default_factory=list)
    where_filter_ids: List[BoundFilterId] = Field(default_factory=list)
    having_filter_ids: List[BoundFilterId] = Field(default_factory=list)
    applied_filter_ids: List[BoundFilterId] = Field(default_factory=list)
    target_model_filters: List[str] = Field(default_factory=list)
    dropped_filter_warnings: List[UnreachableFilterDroppedWarning] = Field(
        default_factory=list,
    )
    public_alias: Optional[str] = None
    hidden: bool = False


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
      enters it through the source scope's Mode-A door, which
      qualifies bare-identifier column refs and discovers crossed
      joins as a side effect of rendering.
    """

    id: BoundFilterId
    phase: Phase
    text: Optional[str] = None
    expression: Optional[BoundExpr] = None


# ---------------------------------------------------------------------------
# OrderEntry
# ---------------------------------------------------------------------------


class OrderScope(str, Enum):
    """WHERE the ordered value lives — the one thing a renderer needs to know
    to build a sort term (DEV-1747 §5.10).

    Every render site used to re-derive this, and they disagreed: one
    dispatched on the slot KIND, one ran a five-way precedence chain over
    alias maps, and one knew about neither. Naming the producing scope in the
    plan is what lets a single resolver replace all of them (P-D).
    """

    #: Materialised in ``_base`` and projected publicly.
    HOST_BASE = "host_base"
    #: Materialised in ``_base`` but trimmed from the public projection —
    #: an order-only aggregate or an unprojected host dimension.
    HOST_BASE_HIDDEN = "host_base_hidden"
    #: Lives in a cross-model / host-rooted isolated (``_cm_``) CTE.
    CROSS_MODEL_CTE = "cross_model_cte"
    #: Lives in a windowed (``_wm_``) CTE.
    WINDOWED_CTE = "windowed_cte"
    #: Lives in a ranked (``_rk_``) first/last CTE.
    RANKED_CTE = "ranked_cte"
    #: Produced by a step of the transform chain.
    TRANSFORM_STEP = "transform_step"
    #: A composite whose operands span scopes, so it can only be evaluated in
    #: the outer combined SELECT — never inside ``_base``.
    OUTER_COMPOSITE = "outer_composite"


class OrderEntry(BaseModel):
    """One entry in the ORDER BY of a planned query.

    ``scope`` and ``phase`` are REQUIRED and have no default: a planner path
    that forgets to classify must fail at construction rather than fall through
    to the ``_base.``-qualified branch, which is how an order term silently
    attached to the wrong scope.
    """

    slot_id: SlotId
    direction: Literal["asc", "desc"]
    scope: OrderScope
    phase: Phase
    #: Null-ordering policy. ``"default"`` is NULLs last, which is what SLayer
    #: means by unstated on every dialect — a semantic layer whose NULLs sort
    #: first on SQLite and last on Postgres answers the same question two ways.
    #: The dialect strategy owns the SPELLING (P-H) — an explicit clause, an
    #: emulation, or T-SQL's native pin where the emulation does not run — so
    #: no render site emits a NULLS clause of its own.
    nulls: Literal["default", "first", "last"] = "default"


# ---------------------------------------------------------------------------
# PlannedQuery
# ---------------------------------------------------------------------------


class FilterReachability(BaseModel):
    """DEV-1745 (W4 / D9) — one filter's structural reachability summary.

    ``crossed_join_paths`` is every join path the filter's dependency tree is
    anchored at, in THIS plan's coordinate system. ``has_host_local_ref`` marks
    a dependency anchored at the plan's own root, which cannot be evaluated
    inside a CTE rooted elsewhere.

    Carried per filter on the owning ``PlannedQuery`` rather than on
    ``ColumnSqlKey`` (interned, and rerooting copies unknown fields through
    stale) or ``ValueSlot`` (slot-less filter-only keys are silently skipped,
    and slots are copied into nested plans).
    """

    model_config = ConfigDict(frozen=True)

    filter_id: BoundFilterId
    crossed_join_paths: Tuple[Tuple[str, ...], ...] = ()
    has_host_local_ref: bool = False


class EmptyBaseGrainPlan(BaseModel):
    """The host base has no columns of its own (DEV-1503, §5.12).

    Set when every projected value is an isolated aggregate — no host row
    slots, no host-local aggregates — so ``_base`` has nothing to project and
    becomes a one-row spine for the combined ``CROSS JOIN`` to hang off. Its
    PRESENCE is the discriminator; there is no ``grain_slot_ids`` field because
    in this shape the grain is empty by definition, which is precisely why the
    join-back degenerates to a CROSS JOIN.

    ``host_filter_ids`` are the ROW-phase filters that stay host-local (not
    routed into a ``_cm_*`` CTE or the outer WHERE). When any exist the spine is
    emitted as ``SELECT 1 AS _placeholder FROM <host> WHERE ... LIMIT 1``;
    otherwise as a bare ``SELECT 1 AS _placeholder`` with no FROM at all.

    The ``LIMIT 1`` is load-bearing rather than an optimisation: the filtered
    form keeps the host FROM so the WHERE can gate the result, but a host FROM
    yields N rows and CROSS JOINing N rows to a one-row scalar aggregate would
    repeat the answer N times. ``LIMIT 1`` collapses the spine to a single row
    while an empty match still yields zero rows overall. The unfiltered form
    drops the FROM entirely for the same reason.
    """

    host_filter_ids: List[BoundFilterId] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# RegroupAttachPlan — DEV-1825
# ---------------------------------------------------------------------------


class RegroupSubstitution(BaseModel):
    """One partitioned aggregate consumed from a regroup producer.

    ``placeholder`` is the reserved-leaf ``ColumnKey`` the discovery pass
    substituted for the ``original_key`` (a partitioned ``AggregateKey``)
    everywhere it appeared in dimension / filter / order trees.
    ``producer_slot_id`` names the aggregate slot INSIDE ``producer_plan``
    whose rendered column the placeholder resolves to at attach time.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    placeholder: ValueKey
    producer_slot_id: SlotId
    original_key: ValueKey


class RegroupAttachPlan(BaseModel):
    """A planner-synthesized producer stage attached on its partition grain.

    An aggregate-then-consume node (DEV-1825): ``producer_plan`` is a full
    nested ``PlannedQuery`` computing one or more aggregates at
    ``partition_display``'s grain (finer than / disjoint from the consumer's
    query grain). It renders as an isolated ``_cm_*`` CTE and attaches into the
    consumer through the P-I null-safe grain join, so the consumed values are
    visible to the consumer's expressions without changing its cardinality.

    ``attach_phase`` selects WHERE the join lands. ``"row"`` (the first
    consumer — DEV-1740 B2 aggregation-based dimensions) joins into the base
    FROM BEFORE aggregation, so a computed dimension over the value groups the
    raw rows once. ``"combined"`` (reserved for the DEV-1739 measure migration,
    DEV-1829) joins at the combined SELECT after aggregation, the position the
    per-aggregate ``_cm_`` CTEs occupy today.

    ``join_pairs`` pairs each host-coordinate partition key (rendered in the
    CONSUMER scope) with the producer's matching grain slot; an empty list is a
    grand-total producer and attaches as a single-row CROSS JOIN.
    ``substitutions`` is one entry per distinct consumed aggregate — several may
    share one producer (one partition set, N aggregates).
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    producer_plan: "PlannedQuery"
    alias_hint: str
    attach_phase: Literal["row", "combined"] = "row"
    join_pairs: List[Tuple[ValueKey, SlotId]] = Field(default_factory=list)
    substitutions: List[RegroupSubstitution] = Field(default_factory=list)
    partition_display: List[str] = Field(default_factory=list)
    # DEV-1836 — a target-rooted (cross-model) producer roots its FROM at the
    # aggregate's source model, not the consumer's root. ``None`` = a local
    # producer rooted at the consumer (today's DEV-1825/1829 behavior).
    producer_root_model: Optional[str] = None
    # DEV-1836 D6 — silent-semantics events carried on the IR so the collector
    # walks them recursively (nested producers included). ``dropped_filter_warnings``
    # are ROW conjuncts excluded from THIS producer (unreachable/unsafe from its
    # root); ``broadcast_measure`` + ``broadcast_dimensions`` name a metric whose
    # implicit grain lost dimensions to broadcasting, each with a reason.
    dropped_filter_warnings: List[Any] = Field(default_factory=list)
    broadcast_measure: Optional[str] = None
    broadcast_dimensions: List[Tuple[str, str]] = Field(default_factory=list)


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
    windowed_aggregate_plans: List["WindowedAggregatePlan"] = Field(default_factory=list)
    # DEV-1748 (B9) — one entry per ``first`` / ``last`` aggregate slot. A
    # sibling of ``windowed_aggregate_plans``: both name aggregates that need
    # their own rows and therefore their own CTE, joined back on the query
    # grain (P-C). A RE-ROOTED cross-model first/last is NOT here — it stays on
    # ``CrossModelAggregatePlan``, whose nested sub-plan carries the ranked plan
    # instead, in the sub-plan's own coordinate system.
    ranked_aggregate_plans: List["RankedAggregatePlan"] = Field(default_factory=list)
    # DEV-1825 — planner-synthesized regroup producers attached on their
    # partition grain (aggregation-based dimensions, and later the migrated
    # partitioned-measure join-backs). One per distinct partition-key set.
    regroup_attach_plans: List["RegroupAttachPlan"] = Field(default_factory=list)
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
    # DEV-1450 stage 7b.15d — the concrete ``SlayerModel`` this stage renders
    # against, carried from the planner so the generator binds the stage's
    # FROM / joins against the SAME model the binder used. For a multi-stage
    # DAG this is the stage's OWN source (e.g. ``orders`` for a stage the root
    # never reads from), a ModelExtension overlay, or a synthetic model over a
    # sibling stage's CTE. ``None`` for a StageSchema-scoped chain stage (the
    # generator builds a synthetic model from the upstream schema) and for a
    # plain single-model query (the generator uses ``bundle.source_model``).
    render_source_model: Optional[SlayerModel] = None
    # DEV-1543 — pass-through of ``SlayerQuery.distinct_dimension_values``. When
    # ``False`` the generator skips the dim-only dedup GROUP BY and emits raw
    # rows for a measure-less dimension query.
    distinct_dimension_values: bool = True
    # DEV-1732 — raw column keys of this stage's NON-HIDDEN time dimensions: the
    # set of columns on which an explicit relational bound counts as a FRAME
    # bound rather than a population filter. Computed once here so the windowed
    # ``_src`` path (planner) and the ``time_shift`` shifted-CTE path
    # (generator) cannot drift apart.
    #
    # Hidden ``TimeTruncKey`` slots are excluded deliberately: they are not
    # equality-joined into ``_src`` (``_build_windowed_plans`` skips them), so
    # stripping a bound on one would leave that axis unconstrained.
    frame_bound_columns: List[ValueKey] = Field(default_factory=list)
    # DEV-1745 (W3 / P-D) — ids of the AGGREGATE-phase filters that must be
    # applied as a plain WHERE on the OUTER combined SELECT rather than as
    # HAVING inside a ``_cm_*`` CTE (DEV-1503).
    #
    # A filtered-local ISOLATED aggregate lives in a CTE that LEFT JOINs back
    # to ``_base``. Applying the comparison as HAVING inside that CTE drops CTE
    # rows, but the LEFT JOIN then resurfaces the host row with a NULL
    # aggregate — the wrong semantic. On the outer, non-aggregating SELECT the
    # same comparison drops the row.
    #
    # Decided HERE because it is a routing decision, not an emission detail:
    # the generator used to re-walk ``filters_by_phase`` at render time to
    # rediscover it, which is policy chosen during emission. The generator now
    # reads this field and never re-derives it, so clearing the field removes
    # the outer WHERE.
    outer_where_filter_ids: List[BoundFilterId] = Field(default_factory=list)
    # DEV-1745 (W4 / D9) — per-filter structural reachability, in THIS plan's
    # coordinate system. Recomputed for every plan (including the nested
    # rerooted plan a cross-model CTE compiles), never copied down from a
    # parent: the paths only mean anything relative to the root they were
    # anchored at. Read via ``filter_reachability_for``.
    filter_reachability: List[FilterReachability] = Field(default_factory=list)
    # DEV-1746 (§5.12) — set when the host base has no columns of its own and
    # is emitted as a one-row placeholder spine. Decided at plan time; the
    # generator consumes it and never re-derives the shape.
    empty_base_plan: Optional[EmptyBaseGrainPlan] = None

    @model_validator(mode="after")
    def _projection_is_public_and_well_formed(self) -> "PlannedQuery":
        """``projection`` is the ONE authoritative public column list (§5.2).

        Every renderer consumes it verbatim, which is what makes hidden-slot
        trimming the absence of a step rather than a step. Two ways that could
        break, both checked here rather than discovered as wrong SQL:

        * a HIDDEN slot appearing in it — hidden slots carry no public name, so
          the renderer would have nothing to alias the column as;
        * a slot appearing MORE times than it has declared names. A slot may
          legitimately repeat: C13 lets one key be selected under several user
          names, and the plan lists it once per name, each occurrence consuming
          the next alias. One occurrence too many means a column emitted twice
          under the same name.
        """
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
                # Slot tables can legitimately be partial in nested plans; the
                # renderer resolves what it needs. Only slots we can SEE are
                # checked, so this validator never rejects a plan for a reason
                # it cannot substantiate.
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


# ``CrossModelAggregatePlan.rerooted_plan`` is a forward reference to
# ``PlannedQuery`` (defined above only after the CMA plan). Resolve it now
# that both classes exist (DEV-1450 stage 7b.15e, C1).
CrossModelAggregatePlan.model_rebuild()
# ``WindowedAggregatePlan.src_filter_rewrites`` forward-references
# ``SrcFilterRewrite``, declared just after it (DEV-1732).
WindowedAggregatePlan.model_rebuild()
# ``RegroupAttachPlan.producer_plan`` forward-references ``PlannedQuery`` (DEV-1825).
RegroupAttachPlan.model_rebuild()
