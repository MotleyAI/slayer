"""Stage 7a.2 (DEV-1450) — CrossModelPlanner Protocol + IsolatedCte impl (I1).

The cross-model aggregate strategy is a substitutable component (I1):
``CrossModelPlanner`` is a Protocol; ``IsolatedCteCrossModelPlanner`` is
the default impl encoding today's "one CTE per (target_model,
shared_grain)" pattern and the ``inherited_filter_policy`` decision
table from the DEV-1450 spec.

The Protocol's ``plan(...)`` consumes:

* the aggregate slot id + ``AggregateKey`` (whose ``source.path``
  identifies the cross-model target),
* a ``ResolvedSourceBundle`` (the eagerly-resolved model graph),
* ``host_slots`` (every ``ValueSlot`` on the host query — used to
  classify filter routing and compute shared grain),
* ``host_filters`` as ``HostFilterRouting`` records (filter id +
  phase + referenced slot ids).

It produces a ``CrossModelAggregatePlan`` (in ``planned.py``) with
explicit ``where_filter_ids`` / ``having_filter_ids`` /
``target_model_filters`` routes so the SQL generator (stage 7b) doesn't
re-classify.

Decision table (host filter routing only):

| Filter references                            | Route                  |
| -------------------------------------------- | ---------------------- |
| Host-local row slot only                     | DROP_HOST_LOCAL        |
| All on joined-target path (row)              | PROPAGATE_WHERE        |
| Cross-model agg-ref on same target           | PROPAGATE_HAVING       |
| Slots on a different joined branch           | DROP_UNREACHABLE       |
| Mixed reachable + unreachable                | DROP_UNREACHABLE       |
| Transform / POST phase                       | STAY_AT_HOST_POST      |

Target model's own ``SlayerModel.filters`` and ``Column.filter`` on the
aggregated column are intrinsic — they ride on the target / the
``AggregateKey`` itself and don't go through host-filter classification.

Dormant in 7a — no engine code calls these yet. ProjectionPlanner
(stage 7a.6) is the first consumer.
"""

from __future__ import annotations

from enum import Enum
from typing import Callable, List, Optional, Protocol, Tuple

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.enums import DataType
from slayer.core.errors import (
    AmbiguousReferenceError,
    IllegalScopeReferenceError,
    UnknownReferenceError,
    UnreachableFilterDroppedWarning,
)
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    Phase,
    StarKey,
    TimeTruncKey,
    ValueKey,
    column_path,
    reroot_aggregate_key,
    reroot_value_key,
)
from slayer.core.models import SlayerModel
from slayer.sql.naming import canonical_aggregate_alias
from slayer.core.scope import StageColumn, StageSchema
from slayer.engine.aggregate_input_paths import (
    compute_aggregate_input_join_paths,
)
from slayer.engine.binding import (
    BoundExpr,
    BoundFilter,
    walk_value_keys,
)
from slayer.engine.filter_reachability import path_is_reachable
from slayer.engine.planned import (
    BoundFilterId,
    CrossModelAggregatePlan,
    JoinRequirement,
    PlannedQuery,
    SlotId,
    ValueSlot,
)
from slayer.engine.planning import DeclaredMeasure, _canonical_name
from slayer.engine.prebound import (
    PreboundQuery,
    StrictQueryCarrier,
    dimension_key_metadata,
    measure_key_format_description,
    measure_key_type,
    walk_key_path,
)
from slayer.engine.source_bundle import ResolvedSourceBundle


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class FilterRoute(str, Enum):
    """Routing decision for one host filter on a cross-model CTE."""

    DROP_HOST_LOCAL = "drop_host_local"
    PROPAGATE_WHERE = "propagate_where"
    PROPAGATE_HAVING = "propagate_having"
    DROP_UNREACHABLE = "drop_unreachable"
    STAY_AT_HOST_POST = "stay_at_host_post"


class HostFilterRouting(BaseModel):
    """A host filter + the slot ids it references.

    The planner consumes a list of these; each is classified per
    ``classify_host_filter`` and routed into the resulting
    ``CrossModelAggregatePlan``.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    filter_id: BoundFilterId
    phase: Phase
    referenced_slot_ids: List[SlotId] = Field(default_factory=list)
    text: Optional[str] = None
    # §5.4 — the typed predicate behind ``text``. A sub-plan that inherits a
    # host filter re-roots THIS rather than re-parsing the string, so the
    # inherited predicate keeps its structural identity. Optional because
    # direct callers and test doubles build routings without one.
    bound: Optional[BoundFilter] = None
    # DEV-1745 (W4 / D9) — the filter's structural reachability summary, in the
    # host plan's coordinate system. ``crossed_join_paths`` is every join path
    # its dependency tree is anchored at; ``has_host_local_ref`` marks a
    # dependency anchored at the host root, which no CTE rooted elsewhere can
    # evaluate. Computed at plan time by ``filter_reachability``.
    crossed_join_paths: Tuple[Tuple[str, ...], ...] = ()
    has_host_local_ref: bool = False


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------


def _classify_referenced_slots(
    *,
    referenced_slot_ids: List[SlotId],
    host_slots: List[ValueSlot],
    target_path: Tuple[str, ...],
) -> Tuple[List[SlotId], List[SlotId], List[SlotId]]:
    """Split a filter's referenced slots into
    ``(unknown, aggregate_on_target, aggregate_other)``.

    Only AGGREGATE slots are sorted here — row-level reachability comes from
    the structural summary, not from slot keys. An aggregate is routed by WHERE
    it is computed: one whose source path IS the target can be propagated as a
    HAVING inside that CTE, one computed anywhere else cannot be evaluated
    there at all.
    """
    by_id = {s.id: s for s in host_slots}
    unknown: List[SlotId] = []
    on_target: List[SlotId] = []
    other: List[SlotId] = []
    for sid in referenced_slot_ids:
        slot = by_id.get(sid)
        if slot is None:
            # Unknown slot id — be conservative, treat as unreachable.
            unknown.append(sid)
        elif isinstance(slot.key, AggregateKey):
            agg_path = getattr(slot.key.source, "path", ())
            bucket = on_target if agg_path == target_path else other
            bucket.append(sid)
    return unknown, on_target, other


def classify_host_filter(
    *,
    host_filter: HostFilterRouting,
    host_slots: List[ValueSlot],
    target_path: Tuple[str, ...],
    host_model_name: Optional[str] = None,
    reachable_paths: "Optional[frozenset]" = None,
) -> FilterRoute:
    """Classify one host filter for cross-model CTE propagation.

    See the module docstring for the decision table. The classifier is
    pure: same inputs → same output, no side effects.

    Row-level reachability is decided EXCLUSIVELY from the filter's structural
    summary (``crossed_join_paths`` / ``has_host_local_ref``, computed at plan
    time by :mod:`slayer.engine.filter_reachability`) under one rule for every
    key kind: a dependency is reachable iff its anchored path is a PREFIX of
    ``target_path``. It is an ALL-DEPENDENCIES predicate — one unreachable
    dependency drops the filter, however many others are reachable.

    This replaces a flat model-NAME membership test for derived columns, which
    counted a SIBLING branch as reachable whenever it happened to share a model
    name with the target path, and counted a host-model derived column whose
    ``Column.sql`` crossed INTO the target as host-local.

    Aggregates keep their own arm: an aggregate is routed by WHERE it is
    computed (on the target vs elsewhere), not by the reachability of its
    inputs. ``host_model_name`` is accepted for signature compatibility and is
    no longer consulted — the structural summary carries what it approximated.
    """
    if host_filter.phase == Phase.POST:
        return FilterRoute.STAY_AT_HOST_POST

    unknown, aggregate_on_target, aggregate_other = _classify_referenced_slots(
        referenced_slot_ids=host_filter.referenced_slot_ids,
        host_slots=host_slots,
        target_path=target_path,
    )

    crossed = tuple(host_filter.crossed_join_paths)
    # THE rule lives in one place. Repeating the prefix comparison here would
    # be a second copy free to drift from it — the exact failure this PR removes.
    unreachable_paths = [
        p for p in crossed
        if not path_is_reachable(
            path=p, target_path=target_path, reachable_paths=reachable_paths,
        )
    ]

    if unknown or aggregate_other or unreachable_paths:
        return FilterRoute.DROP_UNREACHABLE
    if host_filter.has_host_local_ref:
        # Mixed host-local + reachable. The local refs cannot be evaluated in
        # a CTE rooted elsewhere, so the whole filter stays at the host.
        return FilterRoute.DROP_HOST_LOCAL
    if not crossed and not aggregate_on_target:
        # Nothing crosses and no aggregate to propagate — purely host-local.
        return FilterRoute.DROP_HOST_LOCAL
    if aggregate_on_target:
        return FilterRoute.PROPAGATE_HAVING
    return FilterRoute.PROPAGATE_WHERE


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


class CrossModelPlanner(Protocol):
    """Strategy for compiling one cross-model aggregate slot.

    DEV-1450 follow-up #2: re-rooting is owned by the strategy, not a
    post-hoc mutation in ``plan_query``. When the host carries dimensions /
    filters reachable from the target only by walking the TARGET's own join
    graph (off the host→target forward path), the strategy may build a nested
    re-rooted ``PlannedQuery`` and attach it to the returned plan. To do so it
    needs the host query, its public projection, and a callback that compiles
    a sub-query — all keyword-only and defaulting to ``None`` so direct
    callers (and test doubles) that don't re-root keep working unchanged.
    """

    def plan(
        self,
        *,
        aggregate_slot_id: SlotId,
        aggregate_key: AggregateKey,
        bundle: ResolvedSourceBundle,
        host_slots: List[ValueSlot],
        host_filters: List[HostFilterRouting],
        public_alias: Optional[str] = None,
        hidden: bool = False,
        host_query: Optional[StrictQueryCarrier] = None,
        public_projection: Optional[List[SlotId]] = None,
        subplan_builder: Optional[
            Callable[[StrictQueryCarrier, ResolvedSourceBundle], PlannedQuery]
        ] = None,
    ) -> CrossModelAggregatePlan:
        ...


# ---------------------------------------------------------------------------
# Default impl
# ---------------------------------------------------------------------------


def _walk_chain(
    *,
    host_model: SlayerModel,
    hops: Tuple[str, ...],
    bundle: ResolvedSourceBundle,
) -> Tuple[SlayerModel, List[JoinRequirement]]:
    """Walk the join graph from ``host_model`` through ``hops``.

    Returns ``(terminal_model, [JoinRequirement, ...])``. Raises
    ``ValueError`` if a hop has no matching join on the current model
    or the referenced model isn't in ``bundle.referenced_models``.

    The walker is sync — the bundle holds eagerly-resolved models, so
    no async I/O is needed (P11).
    """
    current = host_model
    chain: List[JoinRequirement] = []
    for hop in hops:
        join = next(
            (j for j in current.joins if j.target_model == hop), None,
        )
        if join is None:
            raise ValueError(
                f"Model {current.name!r} has no join to {hop!r}. "
                f"Available joins: {[j.target_model for j in current.joins]}"
            )
        nxt = bundle.get_referenced_model(hop)
        if nxt is None:
            raise ValueError(
                f"Join target {hop!r} from {current.name!r} not found in "
                f"resolved source bundle."
            )
        chain.append(JoinRequirement(
            source_model=current.name,
            target_model=hop,
            join_pairs=[list(p) for p in join.join_pairs],
            join_type=join.join_type,
        ))
        current = nxt
    return current, chain


def _aggregate_alias(*, key: AggregateKey) -> str:
    """Canonical alias for the aggregate's output column in the CTE.

    Mirrors the result-key contract: ``leaf`` + ``_`` + ``agg`` plus an
    args/kwargs signature suffix that disambiguates parameterised
    aggregates (``revenue:percentile(p=0.5)`` vs ``p=0.95``). The
    ``*:count`` star form collapses to ``_count``.

    Built on ``slayer.core.refs.canonical_agg_name`` so the signature
    suffix matches the rest of the engine (legacy enrichment, search,
    DBT converter).
    """
    # The derivation itself lives in ``slayer.sql.naming`` (P-F, one naming
    # authority). This is the ``cte_schema`` profile — the bare canonical
    # name with no relation or path prefix, since the alias names a column
    # INSIDE the CTE.
    #
    # The kwarg suffix is preserved -- the deleted legacy enrichment dropped it,
    # causing two parametric aggs with different kwargs to collide on CTE alias.
    # ``test_cross_model_planner_wiring.py::
    # test_parameterized_aggregates_get_distinct_cte_aliases`` pins this;
    # parity with legacy for cross-model parametric aggs is not achievable on
    # this axis.
    return canonical_aggregate_alias(key, profile="cte_schema")


def _make_cte_schema(
    *,
    aggregate_owner: SlayerModel,
    join_back_target_owner: SlayerModel,
    aggregate_key: AggregateKey,
    join_back_pairs: List[Tuple],
) -> StageSchema:
    """Build the typed projection schema for the CTE.

    The CTE walks the join chain inside its body but groups at the
    FIRST hop's target grain — so the projection's join-back keys are
    columns on ``join_back_target_owner`` (the first hop's target model),
    while the aggregate output column's type comes from
    ``aggregate_owner`` (the terminal/aggregated model).

    For single-hop plans the two owners are the same model. For multi-
    hop (``orders → customers → regions``), ``aggregate_owner`` is
    ``regions`` and ``join_back_target_owner`` is ``customers``.

    Stage 7b's SQL generator consumes the schema when emitting the CTE
    body and the join-back ON clause.
    """
    columns: List[StageColumn] = []
    agg_alias = _aggregate_alias(key=aggregate_key)
    src_leaf = (
        getattr(aggregate_key.source, "leaf", None)
        or getattr(aggregate_key.source, "column_name", None)
    )
    agg_type: Optional[DataType] = None
    if src_leaf and hasattr(aggregate_owner, "get_column"):
        src_col = aggregate_owner.get_column(src_leaf)
        if src_col is not None:
            agg_type = src_col.type
    columns.append(StageColumn(
        name=agg_alias,
        sql_alias=agg_alias,
        public_alias=None,
        hidden=True,
        type=agg_type or DataType.DOUBLE,
        provenance=f"agg:{aggregate_key.agg}",
    ))
    for _, target_key in join_back_pairs:
        leaf = getattr(target_key, "leaf", None)
        if leaf is None:
            continue
        if any(c.name == leaf for c in columns):
            continue
        target_col = (
            join_back_target_owner.get_column(leaf)
            if hasattr(join_back_target_owner, "get_column") else None
        )
        col_type = target_col.type if target_col is not None else None
        columns.append(StageColumn(
            name=leaf,
            sql_alias=leaf,
            public_alias=None,
            hidden=True,
            type=col_type,
            provenance="join_back_key",
        ))
    return StageSchema(
        relation_name=f"cm_{aggregate_owner.name}",
        columns=columns,
    )


def _match_filtered_local_grain_pairs(
    *,
    host_slots: List[ValueSlot],
    public_projection: List[SlotId],
    sub_plan: PlannedQuery,
) -> List[Tuple[SlotId, SlotId]]:
    """Pair each host dimension / time-dimension slot with the sub-plan's
    corresponding row slot for the LEFT JOIN ON clause.

    Both plans bind against the SAME underlying column on the host model,
    so slot identity (the ValueKey) matches across plans.
    """
    sub_row_by_key = {s.key: s.id for s in sub_plan.row_slots}
    grain_pairs: List[Tuple[SlotId, SlotId]] = []
    for host_sid in public_projection:
        host_slot = next(
            (s for s in host_slots if s.id == host_sid), None,
        )
        if host_slot is None:
            continue
        sub_sid = sub_row_by_key.get(host_slot.key)
        if sub_sid is not None:
            grain_pairs.append((host_sid, sub_sid))
    return grain_pairs


def _find_filtered_local_sub_agg_slot(
    *,
    sub_plan: PlannedQuery,
    aggregate_key: AggregateKey,
    host_model: SlayerModel,
) -> SlotId:
    """Locate the sub-plan's slot for the isolated aggregate.

    The exact key match comes first (§5.4: the sub-plan is planned FROM this
    key, so it interns under the same identity). The path-less fallback covers
    a sub-plan whose own pass rewrote the key — recursion suppression
    guarantees no nested cross-model plans, so at most one local aggregate is
    present to match.
    """
    for s in sub_plan.aggregate_slots:
        if s.key == aggregate_key:
            return s.id
    for s in sub_plan.aggregate_slots:
        if isinstance(s.key, AggregateKey) and not getattr(
            s.key.source, "path", (),
        ):
            return s.id
    raise ValueError(
        "DEV-1503 sub-plan produced no local aggregate slot for "
        f"{aggregate_key!r} on {host_model.name!r} — planner bug."
    )


def _build_filtered_local_cte_schema(
    *,
    aggregate_key: AggregateKey,
    host_model: SlayerModel,
) -> StageSchema:
    """Build the minimal CTE schema for a filtered-local plan.

    The actual CTE columns are derived from the sub-plan's stage_schema /
    projection at render time; this entry exists so external consumers see
    a schema shape that matches the existing CrossModelAggregatePlan
    contract.
    """
    agg_alias = _aggregate_alias(key=aggregate_key)
    leaf = getattr(aggregate_key.source, "leaf", None) or getattr(
        aggregate_key.source, "column_name", None,
    )
    agg_type: Optional[DataType] = None
    if leaf is not None and hasattr(host_model, "get_column"):
        col = host_model.get_column(leaf)
        if col is not None:
            agg_type = col.type
    return StageSchema(
        relation_name=f"cm_{host_model.name}",
        columns=[StageColumn(
            name=agg_alias,
            sql_alias=agg_alias,
            public_alias=None,
            hidden=True,
            type=agg_type or DataType.DOUBLE,
            provenance=f"agg:{aggregate_key.agg}",
        )],
    )


def _route_host_rooted_filters(
    *, host_filters: List[HostFilterRouting],
) -> _FilterRoutes:
    """Route host filters for a HOST-ROOTED CTE (DEV-1503 / DEV-1747 D6).

    The re-rooted table's question is reachability — can a CTE rooted at the
    TARGET evaluate this predicate? A host-rooted CTE is rooted at the host, so
    reachability is never in doubt and the question is PHASE instead:

    * ROW — propagate. The sub-plan applies it to the aggregate's rowset;
      without it a predicate like ``status = 'active'`` would not affect the
      value that joins back.
    * AGGREGATE — do NOT propagate. As a HAVING inside the CTE it would drop
      CTE rows where the aggregate fails, and the outer LEFT JOIN would then
      surface the host row with a NULL aggregate instead of dropping it. The
      generator's outer-WHERE wrapper applies it on the joined-back column, so
      the row is actually dropped.
    * POST — do NOT propagate; it stays at the host's post-transform wrapper.

    Nothing is ever DROPPED here, so no warning can arise: every filter is
    applied somewhere, either in the CTE or at the host.
    """
    where_ids: List[BoundFilterId] = []
    for routing in host_filters:
        if routing.text is None:
            continue  # date_range bound — re-attached by the caller, in order
        if routing.phase in (Phase.POST, Phase.AGGREGATE):
            continue
        if routing.bound is None:
            continue
        where_ids.append(routing.filter_id)
    return _FilterRoutes(applied=list(where_ids), where_ids=where_ids)


def _classify_subplan_filters(
    *,
    host_filters: List[HostFilterRouting],
) -> List[BoundFilter]:
    """Decide which host-query filters propagate into the DEV-1503 sub-plan.

    ROW: pass through — the sub-plan applies them to the aggregate's rowset
    (otherwise a non-dim filter like ``status = 'active'`` has no effect on
    the join-back aggregate value).
    AGGREGATE (any slot ref): skip. Pushing such a filter into the sub-plan
    as HAVING would drop CTE rows where the aggregate fails the test; the
    outer LEFT JOIN then surfaces the host row with a NULL aggregate
    instead of dropping it — wrong semantics. The generator's outer-WHERE
    wrapper applies the filter on the joined-back column so the row is
    actually dropped (DEV-1503 spec).
    POST: skip — stays at the existing host post-transform wrapper.

    Consume ``routing.text`` directly — it carries the original user-filter
    string for user-filter routings (None for date_range bounds) and is
    populated by ``stage_planner`` from the deduped ``bound_filters`` list,
    so it stays in lock-step with ``host_filters`` even after Mode-B
    dedup-by-bound-key collapses two textually-different filter spellings
    onto one routing (CR PR #153 thread r3350000254). Slicing
    ``host_query.filters`` here would mis-pair phases when a ``date_range``-
    bearing time_dimension is present (Codex review) OR when dedup drops
    user-filter entries.

    §5.4 — the TYPED ``routing.bound`` rides into the sub-plan; ``routing.text``
    now only distinguishes a user filter from a synthesized date-range bound
    (which the caller re-attaches itself, in date-range-first order).
    """
    inherited: List[BoundFilter] = []
    for routing in host_filters:
        if routing.text is None:
            # date_range bound — not a user filter, do not propagate.
            continue
        if routing.phase in (Phase.POST, Phase.AGGREGATE):
            continue
        if routing.bound is None:
            continue
        # ROW phase — propagate.
        inherited.append(routing.bound)
    return inherited


class _FilterRoutes(BaseModel):
    """The routing decision for one CTE's whole host-filter set.

    Grouped into a record so it can be produced once and threaded to the plan
    constructor without four positional lists (DEV-1747 D6).
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    applied: List[BoundFilterId] = Field(default_factory=list)
    where_ids: List[BoundFilterId] = Field(default_factory=list)
    having_ids: List[BoundFilterId] = Field(default_factory=list)
    dropped: List[UnreachableFilterDroppedWarning] = Field(default_factory=list)


def _route_host_filters(
    *,
    host_filters: List[HostFilterRouting],
    host_slots: List[ValueSlot],
    target_path: Tuple[str, ...],
    host_model: SlayerModel,
    terminal_model: SlayerModel,
    reachable_paths: "Optional[frozenset]" = None,
) -> _FilterRoutes:
    """Classify each host filter via the ``inherited_filter_policy`` decision
    table (``classify_host_filter``) — extracted from
    ``IsolatedCteCrossModelPlanner.plan`` (DEV-1708) to keep that method
    focused. ``DROP_HOST_LOCAL`` / ``STAY_AT_HOST_POST`` are neither propagated
    nor warned.

    ``reachable_paths``, when supplied, is the set of host-coordinate join
    paths a RE-ROOTED CTE can actually evaluate — walked from the target's own
    join graph by the caller. It replaces the forward-path prefix test, so a
    filter the re-rooted CTE will genuinely apply is not reported as dropped."""
    applied: List[BoundFilterId] = []
    where_ids: List[BoundFilterId] = []
    having_ids: List[BoundFilterId] = []
    dropped: List[UnreachableFilterDroppedWarning] = []
    for hf in host_filters:
        route = classify_host_filter(
            host_filter=hf,
            host_slots=host_slots,
            target_path=target_path,
            host_model_name=host_model.name,
            reachable_paths=reachable_paths,
        )
        if route is FilterRoute.PROPAGATE_WHERE:
            where_ids.append(hf.filter_id)
            applied.append(hf.filter_id)
        elif route is FilterRoute.PROPAGATE_HAVING:
            having_ids.append(hf.filter_id)
            applied.append(hf.filter_id)
        elif route is FilterRoute.DROP_UNREACHABLE:
            dropped.append(UnreachableFilterDroppedWarning(
                filter_text=hf.text or hf.filter_id,
                # Deliberately target-INDEPENDENT (D8). The same user filter is
                # classified once per cross-model plan, and the boundary dedups
                # those to one warning while asserting the reasons AGREE. A
                # reason naming this plan's target would make two plans
                # disagree about one filter and trip that assertion.
                reason=(
                    f"filter {hf.filter_id!r} depends on join path(s) that are "
                    f"not reachable from the cross-model aggregate's CTE root; "
                    f"it still applies at the host, and is dropped from the CTE."
                ),
            ))
    return _FilterRoutes(
        applied=applied, where_ids=where_ids,
        having_ids=having_ids, dropped=dropped,
    )


def _compute_shared_grain_slots(
    *, host_slots: List[ValueSlot], target_path: Tuple[str, ...],
) -> List[SlotId]:
    """Host ROW slots (dimensions / time-dimensions) whose path lies on the
    target's join chain flow through as the cross-model CTE's shared grain
    (extracted from ``IsolatedCteCrossModelPlanner.plan`` — DEV-1708). Cross-
    branch and aggregate/transform slots do not.

    A path-bearing **plain derived** (``ColumnSqlKey``) dimension on the target
    path flows through identically to a base ``ColumnKey`` dim (DEV-1728): the
    generator expands its ``Column.sql`` inside the ``_cm_*`` CTE, groups by it,
    and joins back on the DOTTED host alias (the DEV-1708 raise is gone now that
    DEV-1713 fixed the naming half). ``path == ()`` (a host-local derived dim)
    still broadcasts by design — the generator's grain loop skips empty-path
    slots — and a hidden filter-only derived ref is excluded there via the
    ``base_projection_ids`` intersection, so no ``not s.hidden`` guard is needed.
    """
    shared_grain: List[SlotId] = []
    for s in host_slots:
        # Base and derived dims carry their path directly; a time dimension
        # carries it on the wrapped column. One prefix test then serves all
        # three kinds — the DEV-1728 merge of what were two identical branches.
        if isinstance(s.key, (ColumnKey, ColumnSqlKey)):
            p = s.key.path
        elif isinstance(s.key, TimeTruncKey):
            p = column_path(s.key.column)
        else:
            continue
        if not p or p == target_path[: len(p)]:
            shared_grain.append(s.id)
    return shared_grain


class IsolatedCteCrossModelPlanner:
    """Default impl — one CTE per (target_model, shared_grain) tuple.

    Encodes the ``inherited_filter_policy`` decision table from the
    DEV-1450 spec via ``classify_host_filter`` for host filters; pulls
    target ``SlayerModel.filters`` automatically.
    """

    def plan(
        self,
        *,
        aggregate_slot_id: SlotId,
        aggregate_key: AggregateKey,
        bundle: ResolvedSourceBundle,
        host_slots: List[ValueSlot],
        host_filters: List[HostFilterRouting],
        public_alias: Optional[str] = None,
        hidden: bool = False,
        host_query: Optional[StrictQueryCarrier] = None,
        public_projection: Optional[List[SlotId]] = None,
        subplan_builder: Optional[
            Callable[[StrictQueryCarrier, ResolvedSourceBundle], PlannedQuery]
        ] = None,
    ) -> CrossModelAggregatePlan:
        host_model = bundle.source_model
        if host_model is None:
            raise ValueError(
                "ResolvedSourceBundle.source_model is None — "
                "IsolatedCteCrossModelPlanner needs a host model anchor "
                "(I2 anchor-less mode is not yet implemented)."
            )

        agg_source = aggregate_key.source
        path = getattr(agg_source, "path", ())
        # DEV-1747 D2 — ``grain="host"`` routes to the HOST-rooted CTE even
        # though the source carries a path. The path says WHERE the value is
        # read from; the grain says WHERE it is grouped. A joined ORDER BY wrap
        # reads through the join but must be grouped per HOST row-group, so it
        # belongs on the same route as a crossing-input local aggregate.
        if not path or getattr(aggregate_key, "grain", "target") == "host":
            return self._dispatch_filtered_local(
                aggregate_slot_id=aggregate_slot_id,
                aggregate_key=aggregate_key,
                bundle=bundle,
                host_model=host_model,
                host_slots=host_slots,
                host_filters=host_filters,
                public_alias=public_alias,
                hidden=hidden,
                host_query=host_query,
                public_projection=public_projection,
                subplan_builder=subplan_builder,
            )

        terminal_model, join_chain = _walk_chain(
            host_model=host_model, hops=path, bundle=bundle,
        )

        # Build join_back_pairs from the FIRST hop's join_pairs. The CTE
        # is grouped at the first hop's target columns; the host joins
        # back on those.
        join_back_pairs: List[Tuple] = []
        if join_chain:
            first_hop = join_chain[0]
            for pair in first_hop.join_pairs:
                host_col, target_col = pair
                join_back_pairs.append((
                    ColumnKey(path=(), leaf=host_col),
                    ColumnKey(path=(), leaf=target_col),
                ))

        target_path = path
        target_model_filters = list(terminal_model.filters or [])

        # Shared grain: host ROW dimensions / time-dimensions on the target's
        # join chain flow through (a plain derived one on the target path
        # raises — DEV-1708). Extracted to keep this method focused.
        shared_grain = _compute_shared_grain_slots(
            host_slots=host_slots, target_path=target_path,
        )

        first_hop = join_chain[0]
        first_hop_target = (
            bundle.get_referenced_model(first_hop.target_model)
            or terminal_model
        )
        cte_schema = _make_cte_schema(
            aggregate_owner=terminal_model,
            join_back_target_owner=first_hop_target,
            aggregate_key=aggregate_key,
            join_back_pairs=join_back_pairs,
        )

        def _make_plan(routes: "_FilterRoutes") -> CrossModelAggregatePlan:
            return CrossModelAggregatePlan(
                aggregate_slot_id=aggregate_slot_id,
                target_model=terminal_model.name,
                datasource=host_model.data_source,
                join_chain=join_chain,
                join_back_pairs=join_back_pairs,
                cte_stage_schema=cte_schema,
                shared_grain_slots=shared_grain,
                applied_filter_ids=routes.applied,
                where_filter_ids=routes.where_ids,
                having_filter_ids=routes.having_ids,
                target_model_filters=target_model_filters,
                dropped_filter_warnings=routes.dropped,
                hidden=hidden,
                public_alias=public_alias,
            )

        # DEV-1450 #2: re-rooting is the strategy's call. When the caller
        # supplies the host query + a sub-plan builder, decide forward-plan
        # vs re-rooted-plan here; without them (direct ``plan(...)`` callers /
        # test doubles) return the forward plan unchanged.
        #
        # DEV-1747 D6 — the reroot decision is made BEFORE the filters are
        # classified, so the one classification runs in the coordinate system
        # of the CTE that will actually exist. Classifying against the forward
        # path and then re-rooting is how a reachable filter ended up judged
        # unreachable, and then had that judgement blanked.
        if subplan_builder is not None and host_query is not None:
            return _maybe_reroot_cross_model_plan(
                make_plan=_make_plan,
                query=host_query,
                agg_key=aggregate_key,
                bundle=bundle,
                host_model=host_model,
                host_slots=host_slots,
                host_filters=host_filters,
                public_projection=public_projection or [],
                subplan_builder=subplan_builder,
                target_model_name=terminal_model.name,
                target_path=target_path,
            )
        return _make_plan(_route_host_filters(
            host_filters=host_filters,
            host_slots=host_slots,
            target_path=target_path,
            host_model=host_model,
            terminal_model=terminal_model,
        ))

    # ----------------------------------------------------------------------
    # DEV-1503 — filtered-local isolation
    # ----------------------------------------------------------------------

    def _dispatch_filtered_local(
        self,
        *,
        aggregate_slot_id: SlotId,
        aggregate_key: AggregateKey,
        bundle: ResolvedSourceBundle,
        host_model: SlayerModel,
        host_slots: List[ValueSlot],
        host_filters: List[HostFilterRouting],
        public_alias: Optional[str],
        hidden: bool,
        host_query: Optional[StrictQueryCarrier],
        public_projection: Optional[List[SlotId]],
        subplan_builder: Optional[
            Callable[[StrictQueryCarrier, ResolvedSourceBundle], PlannedQuery]
        ],
    ) -> CrossModelAggregatePlan:
        """Validate the host-rooted trigger preconditions and dispatch
        into ``_plan_filtered_local`` — the aggregate is on a HOST column
        but at least one of its inputs crosses a join (``Column.filter``
        per DEV-1503; source ``Column.sql`` / positional args / kwargs per
        DEV-1709), so a host-rooted nested sub-plan owns the aggregation
        and the host base LEFT JOINs back.
        """
        agg_source = aggregate_key.source
        cfk = aggregate_key.column_filter_key
        has_crossing_filter = cfk is not None and bool(
            cfk.referenced_join_paths,
        )
        # DEV-1747 D2 — for a ``grain="host"`` aggregate the SOURCE PATH is
        # itself the crossing input: the value is read through a join that the
        # CTE has to pull in. Omitting it here would make the check below
        # reject the wrap as "a plain local aggregate", since a path-bearing
        # source carries no ``column_filter_key`` and no crossing arg.
        has_crossing_source = (
            getattr(aggregate_key, "grain", "target") == "host"
            and bool(getattr(agg_source, "path", ()))
        )
        has_crossing_input = has_crossing_filter or has_crossing_source or bool(
            compute_aggregate_input_join_paths(
                key=aggregate_key,
                anchor_model=host_model,
                anchor_relation=host_model.name,
                bundle=bundle,
            ),
        )
        if not has_crossing_input:
            raise ValueError(
                f"AggregateKey on {agg_source!r} has empty source.path, "
                f"no cross-model column_filter_key, AND no other crossing "
                f"input — this is a plain local aggregate. The cross-model "
                f"planner should not have been invoked."
            )
        if subplan_builder is None or host_query is None:
            # The DEV-1503 strategy requires a sub-plan builder + the host
            # query for grain-pair matching. Direct callers without these
            # (legacy test doubles) can't trigger filtered-local — raise
            # loudly so the call site is fixed rather than emitting
            # silently wrong SQL.
            raise ValueError(
                "DEV-1503 filtered-local isolation requires host_query "
                "and subplan_builder; received None for one or both. "
                "Confirm the stage_planner is wired to pass them."
            )
        return self._plan_filtered_local(
            aggregate_slot_id=aggregate_slot_id,
            aggregate_key=aggregate_key,
            bundle=bundle,
            host_model=host_model,
            host_slots=host_slots,
            host_filters=host_filters,
            host_query=host_query,
            public_alias=public_alias,
            public_projection=public_projection or [],
            hidden=hidden,
            subplan_builder=subplan_builder,
        )

    def _plan_filtered_local(
        self,
        *,
        aggregate_slot_id: SlotId,
        aggregate_key: AggregateKey,
        bundle: ResolvedSourceBundle,
        host_model: SlayerModel,
        host_slots: List[ValueSlot],
        host_filters: List[HostFilterRouting],
        host_query: StrictQueryCarrier,
        public_alias: Optional[str],
        public_projection: List[SlotId],
        hidden: bool,
        subplan_builder: Callable[
            [StrictQueryCarrier, ResolvedSourceBundle], PlannedQuery,
        ],
    ) -> CrossModelAggregatePlan:
        """Build a host-rooted nested sub-plan for a cross-model-FILTERED
        local measure (DEV-1503).

        The sub-plan is a ``PreboundQuery`` rooted at the SAME host model,
        carrying the filtered measure and the host's bound dimensions /
        time dimensions. The sub-plan's ``plan_query`` recursion handles the
        filter-target join (its ``Column.filter`` pulls in the joined table at
        the generator's inline path), the host model's own
        ``SlayerModel.filters``, and the per-dimension GROUP BY — producing a
        per-grain aggregate that the host base LEFT JOINs back.

        Only ROW-phase host filters propagate (see
        ``_classify_subplan_filters``); the rest stay at the host base or at
        the generator's outer-WHERE wrapper (DEV-1503 spec).
        """
        # §5.4 — the sub-plan is rooted at the SAME host model, so the
        # aggregate key and the host's bound dimensions carry over VERBATIM;
        # there is nothing to re-root and so nothing to serialize. The
        # user-supplied alias rides along so a host filter referencing the
        # rename (``latest_pmt > 500`` for a measure named ``latest_pmt``)
        # resolves against the same alias in the sub-plan rather than the
        # canonical ``latest_payment_last_updated_at`` form.
        host_prebound = host_query.prebound
        if host_prebound is None:
            raise ValueError(
                "DEV-1503 filtered-local isolation needs the host's typed "
                "bind product; the carrier arrived without one. The "
                "stage_planner must pass the PreboundQuery it planned from."
            )
        host_rooted_routes = _route_host_rooted_filters(
            host_filters=host_filters,
        )
        routing_by_id = {hf.filter_id: hf for hf in host_filters}
        sub_prebound = _nested_prebound(
            host_prebound=host_prebound,
            aggregate_measure=_aggregate_declared_measure(
                key=aggregate_key,
                model=host_model,
                public_alias=public_alias,
            ),
            grain_measures=list(_grain_declared_measures(host_prebound)),
            inherited_filters=[
                routing_by_id[fid].bound
                for fid in host_rooted_routes.applied
                if routing_by_id[fid].bound is not None
            ],
        )
        sub_plan = subplan_builder(
            StrictQueryCarrier(
                source_model=host_model.name, prebound=sub_prebound,
            ),
            bundle,
        )

        grain_pairs = _match_filtered_local_grain_pairs(
            host_slots=host_slots,
            public_projection=public_projection,
            sub_plan=sub_plan,
        )
        sub_agg_sid = _find_filtered_local_sub_agg_slot(
            sub_plan=sub_plan, aggregate_key=aggregate_key,
            host_model=host_model,
        )
        cte_schema = _build_filtered_local_cte_schema(
            aggregate_key=aggregate_key, host_model=host_model,
        )

        return CrossModelAggregatePlan(
            aggregate_slot_id=aggregate_slot_id,
            # ``target_model`` is conventionally set to the host name for
            # filtered-local; ``cte_root_model`` is the disambiguator the
            # renderer reads.
            target_model=host_model.name,
            cte_root_model=host_model.name,
            datasource=host_model.data_source,
            join_chain=[],
            join_back_pairs=[],
            cte_stage_schema=cte_schema,
            shared_grain_slots=[host_sid for host_sid, _ in grain_pairs],
            # DEV-1747 D6 — the plan states which host filters the CTE applies
            # rather than reporting an empty routing while the sub-plan quietly
            # carries them. ``where_filter_ids`` stays empty: the sub-plan
            # already holds these predicates as its OWN filters (they were
            # handed to it typed), so listing them again would render them
            # twice. Nothing is dropped on this route, so no warning can arise.
            applied_filter_ids=host_rooted_routes.applied,
            where_filter_ids=[],
            having_filter_ids=[],
            target_model_filters=[],
            dropped_filter_warnings=[],
            hidden=hidden,
            public_alias=public_alias,
            rerooted_plan=sub_plan,
            rerooted_grain_pairs=grain_pairs,
            rerooted_agg_slot_id=sub_agg_sid,
        )


# ---------------------------------------------------------------------------
# Cross-model re-rooting (DEV-1450 stage 7b.15e, C1; relocated here in #2)
# ---------------------------------------------------------------------------
#
# When a cross-model aggregate (``policy_amount.total:sum``) is queried with
# host dimensions that are reachable from the TARGET by walking the target's
# own join graph (``policy_amount -> policy -> policy_number``), the
# forward-path CTE ("FROM bare target, GROUP BY forward-path dims only")
# collapses the host dimension to a scalar CROSS JOIN -- every host row gets
# the global aggregate.
#
# The fix: re-anchor the host's bound keys into the target's coordinate
# system (so all of the target's joins are in scope for dimensions AND
# filters), plan them via ``subplan_builder``, and attach the sub-plan to the
# ``CrossModelAggregatePlan``. The generator renders the sub-plan as the
# ``_cm_*`` CTE and joins it back to the host base on the re-rooted dimension.
# Dimensions / filters that don't resolve from the target are dropped.
#
# DEV-1742 §5.4: the re-anchoring is STRUCTURAL. Until then this pass
# regenerated formula text for every ref and let the planner re-bind it, so a
# key's identity survived only as far as the string could carry it — which is
# why a path-bearing source or a host-grain marker could not be expressed at
# all. ``_reroot_host_key`` transforms the key; nothing is re-parsed.
#
# DEV-1450 #2: this used to be a post-hoc pass in ``stage_planner.plan_query``;
# it now lives behind ``IsolatedCteCrossModelPlanner.plan`` so the
# render-strategy decision (forward vs re-rooted) is owned by the strategy.
# The recursive ``plan_query`` call is injected as ``subplan_builder`` so this
# module does not import ``stage_planner`` (no cycle).


def _grain_declared_measures(prebound: PreboundQuery) -> List[DeclaredMeasure]:
    """The host's dimension + time-dimension declarations — the grain prefix of
    ``declared_measures``, which the nested plan groups by unchanged."""
    n = prebound.n_dims + prebound.n_time_dimensions
    return list(prebound.declared_measures[:n])


def _aggregate_declared_measure(
    *,
    key: AggregateKey,
    model: SlayerModel,
    public_alias: Optional[str],
) -> DeclaredMeasure:
    """The nested plan's single measure declaration, built from the key.

    Reproduces what ``_declared_measures_from_query`` derives for a measure —
    canonical alias, type, format, description — without a formula string to
    re-parse. An explicit ``public_alias`` surfaces as the name while the
    canonical form is retained as ``canonical_alias``, so a colon-form filter
    or ORDER BY still resolves onto the same slot (DEV-1443).
    """
    canonical = canonical_aggregate_alias(key, profile="stage_formula")
    if canonical is None:  # pragma: no cover — binder restricts source shapes
        canonical = _canonical_name(key)
    fmt, desc = measure_key_format_description(model=model, key=key)
    return DeclaredMeasure(
        bound=BoundExpr(value_key=key),
        declared_name=public_alias or canonical,
        public_name=public_alias or canonical,
        canonical_alias=canonical if public_alias else None,
        type=measure_key_type(model=model, key=key),
        format=fmt,
        description=desc,
    )


def _nested_prebound(
    *,
    host_prebound: PreboundQuery,
    aggregate_measure: DeclaredMeasure,
    grain_measures: List[DeclaredMeasure],
    inherited_filters: List[BoundFilter],
    date_range_filters: Optional[List[BoundFilter]] = None,
    n_dims: Optional[int] = None,
    n_time_dimensions: Optional[int] = None,
    main_time_key: Optional[TimeTruncKey] = None,
) -> PreboundQuery:
    """Assemble the nested plan's bind product from typed pieces (§5.4).

    Filter order mirrors ``bind_query_inputs``: date-range bounds first (so
    ``n_date_range`` still slices them off), then inherited user filters. The
    nested plan is never ordered or paginated — the host owns both.
    """
    bounds = list(
        host_prebound.bound_filters[: host_prebound.n_date_range]
        if date_range_filters is None else date_range_filters
    )
    return PreboundQuery(
        declared_measures=[*grain_measures, aggregate_measure],
        bound_filters=[*bounds, *inherited_filters],
        bound_filter_texts=(
            [None] * len(bounds) + [None] * len(inherited_filters)
        ),
        n_date_range=len(bounds),
        order_specs=[],
        main_time_key=(
            host_prebound.main_time_key if main_time_key is None
            else main_time_key
        ),
        n_dims=host_prebound.n_dims if n_dims is None else n_dims,
        n_time_dimensions=(
            host_prebound.n_time_dimensions if n_time_dimensions is None
            else n_time_dimensions
        ),
        distinct_dimension_values=True,
    )


def _reroot_host_path(
    path: Tuple[str, ...], *, target_path: Tuple[str, ...],
    host_model_name: str,
) -> Tuple[str, ...]:
    """The path-level half of :func:`_reroot_host_key` — the same three rules,
    applied to a bare join path so reachability can be decided without a key."""
    path = tuple(path)
    if not path:
        return (host_model_name,)
    if path[: len(target_path)] == tuple(target_path):
        return path[len(target_path):]
    return path


def _rerooted_reachable_paths(
    *,
    host_filters: List[HostFilterRouting],
    target_path: Tuple[str, ...],
    target_model: SlayerModel,
    host_model_name: str,
    bundle: ResolvedSourceBundle,
) -> frozenset:
    """Which host-coordinate join paths a RE-ROOTED CTE can evaluate.

    A prefix test cannot answer this: the CTE is rooted at the target with the
    target's whole join graph in scope, so a host-side SIBLING branch is
    reachable whenever the target happens to join to it too — common in star
    schemas, where several fact-adjacent tables share a dimension. Walking the
    graph is the only honest test, and getting it wrong in either direction is
    a correctness bug: too narrow drops a filter the user wrote, too wide
    emits SQL referencing an unbound table.
    """
    reachable = set()
    for hf in host_filters:
        for p in hf.crossed_join_paths:
            if not p or p in reachable:
                continue
            rr = _reroot_host_path(
                p, target_path=target_path, host_model_name=host_model_name,
            )
            if walk_key_path(
                model=target_model, path=rr, bundle=bundle,
            ) is not None:
                reachable.add(tuple(p))
    return frozenset(reachable)


def _reroot_host_key(
    key: ValueKey, *, target_path: Tuple[str, ...], host_model_name: str,
) -> ValueKey:
    """Re-anchor one host-coordinate key into the target's coordinate system.

    The typed counterpart of :func:`_reroot_ref`, and the same three rules:

    * host-local (empty path) → reached FROM the target by naming the host as
      the first hop, so ``status`` becomes ``orders.status``;
    * on or through the target → the target prefix is stripped, which is
      exactly ``reroot_value_key``;
    * anywhere else → unchanged, to be resolved through the target's own joins.

    The host-local prepend is per-key rather than per-leaf: a composite whose
    leaves sit at different depths would need each leaf re-anchored
    separately, and no such shape reaches re-rooting today (dimensions and
    time dimensions are single references). ``_key_reaches_from`` rejects
    anything that does not resolve, so a future composite is dropped rather
    than mis-anchored.
    """
    inner = key.column if isinstance(key, TimeTruncKey) else key
    path = tuple(getattr(inner, "path", ()) or ())
    if not path:
        if not hasattr(inner, "path"):
            return key
        rerooted = inner.model_copy(update={"path": (host_model_name,)})
        if isinstance(key, TimeTruncKey):
            return key.model_copy(update={"column": rerooted})
        return rerooted
    return reroot_value_key(key, target_path=target_path)


def _key_reaches_from(
    *, key: ValueKey, model: SlayerModel, bundle: ResolvedSourceBundle,
) -> bool:
    """Whether every column-like leaf of ``key`` resolves from ``model``.

    The structural stand-in for "does this bind against the target scope?" —
    re-rooting must not call the binder (§5.4), so reachability is decided by
    walking the join graph and checking the terminal model owns the leaf.
    """
    saw_column = False
    for k in walk_value_keys(key):
        if isinstance(k, TimeTruncKey):
            continue  # its wrapped column is walked in its own right
        if not isinstance(k, (ColumnKey, ColumnSqlKey, StarKey)):
            continue
        saw_column = True
        terminal = walk_key_path(
            model=model, path=tuple(k.path), bundle=bundle,
        )
        if terminal is None:
            return False
        if isinstance(k, StarKey):
            continue
        leaf = getattr(k, "leaf", None) or getattr(k, "column_name", None)
        if leaf is None or terminal.get_column(leaf) is None:
            return False
    return saw_column


def _rerooted_dimension_measure(
    *,
    key: ValueKey,
    label: Optional[str],
    target_model: SlayerModel,
    bundle: ResolvedSourceBundle,
) -> DeclaredMeasure:
    """One re-rooted dimension / time-dimension declaration for the sub-plan.

    ``_canonical_name`` produces the same ``__``-flattened alias the text path
    derived from the re-rooted dotted reference, so the CTE's column names and
    the host's join-back are unchanged by the switch to typed re-rooting.
    """
    if isinstance(key, TimeTruncKey):
        return DeclaredMeasure(
            bound=BoundExpr(value_key=key),
            declared_name=_canonical_name(key),
            public_name=_canonical_name(key),
            label=label,
            type=DataType.TIMESTAMP,
        )
    dim_type, fmt, desc = dimension_key_metadata(
        model=target_model, key=key, bundle=bundle,
    )
    return DeclaredMeasure(
        bound=BoundExpr(value_key=key),
        declared_name=_canonical_name(key),
        public_name=_canonical_name(key),
        label=label,
        type=dim_type,
        format=fmt,
        description=desc,
    )


# ---------------------------------------------------------------------------
# Superseded by the typed re-rooting above (DEV-1742 §5.4 / P-J state 1)
# ---------------------------------------------------------------------------
#
# ``_reroot_ref``, ``_host_ref_path``, ``_render_ref_formula``,
# ``_scalar_formula_literal``, ``_local_agg_formula`` and
# ``_REROOT_BIND_ERRORS`` are the formula-text round-trip these functions
# replaced. They are PRODUCTION-UNREFERENCED as of this change; their tests
# stay green so the two mechanisms can be compared, and deletion happens in
# one sweep (PR 6) rather than being smeared across the series.
#
# ``_filter_ref_paths`` is NOT in this group — the typed path still uses it.


def _reroot_ref(
    *, model_prefix: Optional[str], name: str, host_model_name: str,
    target_model_name: str,
) -> str:
    """Re-root one Mode-B ref from the host's perspective to the target's.

    Mirrors legacy ``_build_rerooted_enriched``:

    * host-local (``model_prefix is None``) -> ``<host>.<name>`` (now a
      cross-model dim from the target's view),
    * on the target itself -> bare ``<name>`` (local on target),
    * a path through the target -> strip the target prefix,
    * any other dotted ref -> kept as-is (resolved via the target's joins).
    """
    if model_prefix is None:
        return f"{host_model_name}.{name}"
    if model_prefix == target_model_name:
        return name
    if model_prefix.startswith(target_model_name + "."):
        return f"{model_prefix[len(target_model_name) + 1:]}.{name}"
    return f"{model_prefix}.{name}"


def _host_ref_path(model_prefix: Optional[str]) -> Tuple[str, ...]:
    """The join path a host ColumnRef / TimeDimension prefix denotes."""
    if not model_prefix:
        return ()
    return tuple(model_prefix.split("."))


def _scalar_formula_literal(value) -> str:
    """Render a normalized scalar back into formula text."""
    if isinstance(value, bool):
        return "True" if value else "False"
    if value is None:
        return "None"
    if isinstance(value, str):
        return repr(value)
    return str(value)


def _filter_ref_paths(value_key: ValueKey) -> List[Tuple[str, ...]]:
    """Join paths of every column-like leaf a (bound) filter references."""
    paths: List[Tuple[str, ...]] = []
    for k in walk_value_keys(value_key):
        if isinstance(k, (ColumnKey, ColumnSqlKey, StarKey)):
            paths.append(tuple(k.path))
        elif isinstance(k, TimeTruncKey):
            paths.append(tuple(column_path(k.column)))
    return paths


def _render_ref_formula(ref) -> str:
    """Render one already-rerooted embedded reference back into formula text.

    Column-like refs dot-join their (residual) path with the leaf; scalars
    fall through to ``_scalar_formula_literal``. Contains NO path-stripping
    decisions — the reroot has already happened (DEV-1707).
    """
    if isinstance(ref, ColumnSqlKey):
        return ".".join((*ref.path, ref.column_name))
    if isinstance(ref, ColumnKey):
        return ".".join((*ref.path, ref.leaf))
    return _scalar_formula_literal(ref)


def _local_agg_formula(key: AggregateKey) -> str:
    """Reconstruct the LOCAL colon-formula for a cross-model aggregate
    (``customers.revenue:sum`` -> ``revenue:sum``) so it can be re-planned
    against the target model as a plain local measure.

    Every embedded reference — source, positional args, and column-valued
    kwargs — is re-anchored symmetrically via the unified
    ``reroot_aggregate_key`` (DEV-1707), then rendered by the path-free
    ``_render_ref_formula``. A kwarg / arg one hop past the target keeps its
    residual path (``other=regions.code``); an exact match becomes local
    (``other=region_id``). The public string contract is unchanged — the
    strip logic simply no longer lives here.
    """
    local = reroot_aggregate_key(
        key, target_path=tuple(getattr(key.source, "path", ())),
    )
    src = local.source
    if isinstance(src, StarKey):
        base = "*"
    elif isinstance(src, ColumnSqlKey):
        base = ".".join((*src.path, src.column_name))
    else:  # ColumnKey
        base = ".".join((*src.path, src.leaf))

    formula = f"{base}:{local.agg}"
    parts: List[str] = [_render_ref_formula(a) for a in local.args]
    parts += [f"{k}={_render_ref_formula(v)}" for k, v in local.kwargs]
    if parts:
        formula += "(" + ", ".join(parts) + ")"
    return formula


_REROOT_BIND_ERRORS = (
    UnknownReferenceError,
    AmbiguousReferenceError,
    IllegalScopeReferenceError,
    ValueError,
    NotImplementedError,
)


def _maybe_reroot_cross_model_plan(  # NOSONAR(S3776) — one re-rooting decision over three parallel input kinds (dimensions, time dimensions, filters). Each loop re-anchors, tests target-reachability, and votes on `needs_reroot`; the vote is the shared state that makes them one pass rather than three functions.
    *,
    make_plan: Callable[["_FilterRoutes"], CrossModelAggregatePlan],
    query: StrictQueryCarrier,
    agg_key: AggregateKey,
    bundle: ResolvedSourceBundle,
    host_model: SlayerModel,
    host_slots: List[ValueSlot],
    host_filters: List[HostFilterRouting],
    public_projection: List[str],
    subplan_builder: Callable[
        [StrictQueryCarrier, ResolvedSourceBundle], PlannedQuery,
    ],
    target_model_name: str,
    target_path: Tuple[str, ...],
):
    """Decide forward-vs-re-rooted, classify the host filters ONCE for whichever
    shape won, and build the plan.

    Re-rooting applies when the host carries dimensions or filters reachable
    from the target only by walking the TARGET's own join graph — off the
    host→target forward path, which the forward CTE cannot evaluate.

    §5.4 — every input arrives already bound, on the carrier's
    ``PreboundQuery``. Re-rooting re-anchors those keys structurally and hands
    them straight back to the planner, so the sub-plan's slot identities are
    the host's, transformed — never re-derived from a regenerated string.

    D6 — the decision precedes the classification. The two used to run in the
    other order, with the reroot then BLANKING what the classifier had decided
    against a coordinate system that no longer applied.
    """
    target_model = bundle.get_referenced_model(target_model_name)
    host_prebound = query.prebound

    def _forward_only():
        return make_plan(_route_host_filters(
            host_filters=host_filters, host_slots=host_slots,
            target_path=target_path, host_model=host_model,
            terminal_model=target_model or host_model,
        ))

    if target_model is None or host_prebound is None:
        return _forward_only()
    rerooted_bundle = bundle.model_copy(update={"source_model": target_model})

    def _reroot(key: ValueKey) -> ValueKey:
        return _reroot_host_key(
            key, target_path=target_path, host_model_name=host_model.name,
        )

    def _reaches(key: ValueKey) -> bool:
        return _key_reaches_from(
            key=key, model=target_model, bundle=rerooted_bundle,
        )

    def _is_forward(path: Tuple[str, ...]) -> bool:
        # On the host->target path (handled by the forward-path CTE already).
        return bool(path) and path == target_path[: len(path)]

    n_dims = host_prebound.n_dims
    n_tds = host_prebound.n_time_dimensions
    grain_declared: List[DeclaredMeasure] = []
    grain_host_sids: List[str] = []
    grain_rerooted_keys: List[ValueKey] = []
    needs_reroot = False

    for i, dm in enumerate(host_prebound.declared_measures[: n_dims + n_tds]):
        host_sid = public_projection[i] if i < len(public_projection) else None
        host_key = dm.bound.value_key
        inner = (
            host_key.column if isinstance(host_key, TimeTruncKey) else host_key
        )
        host_path = tuple(getattr(inner, "path", ()) or ())
        rr_key = _reroot(host_key)
        if not _reaches(rr_key):
            continue  # unreachable from target -> drop
        if not _is_forward(host_path):
            needs_reroot = True
        if host_sid is None:
            continue
        grain_declared.append(_rerooted_dimension_measure(
            key=rr_key, label=dm.label, target_model=target_model,
            bundle=rerooted_bundle,
        ))
        grain_host_sids.append(host_sid)
        grain_rerooted_keys.append(rr_key)

    # Filters vote structurally, before any classification. A filter that
    # reaches OFF the host→target forward path is exactly what the forward CTE
    # cannot evaluate, so wanting it is a reason to re-root — the same vote a
    # non-forward dimension casts. Only a filter the RE-ROOTED CTE could
    # actually evaluate votes: an ``order_tags`` predicate is unreachable
    # either way and must not drag the plan into a shape that does not help it.
    reachable_paths = _rerooted_reachable_paths(
        host_filters=host_filters,
        target_path=target_path,
        target_model=target_model,
        host_model_name=host_model.name,
        bundle=rerooted_bundle,
    )
    for hf in host_filters:
        crossed = [p for p in hf.crossed_join_paths if p]
        if not crossed:
            continue  # host-local -> applied at the host base only
        # Through the shared predicate, not a second copy of the membership
        # half: the rule lives in ONE place precisely so the two cannot drift.
        if not all(
            path_is_reachable(
                path=p, target_path=target_path,
                reachable_paths=reachable_paths,
            )
            for p in crossed
        ):
            continue
        if any(not _is_forward(p) for p in crossed):
            needs_reroot = True
            break

    if not needs_reroot:
        return _forward_only()

    routes = _route_host_filters(
        host_filters=host_filters,
        host_slots=host_slots,
        target_path=target_path,
        host_model=host_model,
        terminal_model=target_model,
        reachable_paths=reachable_paths,
    )
    if not (grain_declared or routes.applied):
        # The re-rooted shape lost, so the plan that ships is the FORWARD one
        # and its routing must be classified under the forward PREFIX rule.
        # Returning ``make_plan(routes)`` here shipped a forward plan carrying
        # re-rooted-coordinate routing: a path that is a strict prefix of
        # ``target_path`` is reachable under the prefix rule but absent from
        # ``reachable_paths``, so the filter the forward CTE can evaluate was
        # dropped from it AND warned about. That is the same "classified
        # against a coordinate system that no longer applies" failure D6
        # exists to remove — one branch below it, doing it again.
        return _forward_only()
    plan = make_plan(routes)

    # The CTE applies exactly what the routing says it applies — one decision,
    # consumed, never re-derived. ``routing_by_id`` maps those ids back to the
    # typed predicates so each rides in re-anchored rather than re-parsed.
    #
    # Date-range bounds (the ``[:n_date_range]`` prefix of ``bound_filters``)
    # are not user filters and carry no routing id; they are re-anchored below
    # alongside their time dimension.
    routing_by_id = {hf.filter_id: hf for hf in host_filters}
    rerooted_filters: List[BoundFilter] = []
    for fid in routes.applied:
        hf = routing_by_id.get(fid)
        if hf is None or hf.bound is None:
            continue
        rr_key = reroot_value_key(hf.bound.value_key, target_path=target_path)
        rerooted_filters.append(BoundFilter(
            value_key=rr_key,
            phase=hf.bound.phase,
            referenced_keys=tuple(walk_value_keys(rr_key)),
        ))

    # Date-range bounds ride into the CTE alongside their re-rooted time
    # dimension, so the sub-plan applies the same window the host does.
    rerooted_bounds = [
        BoundFilter(
            value_key=reroot_value_key(bf.value_key, target_path=target_path),
            phase=bf.phase,
            referenced_keys=tuple(walk_value_keys(
                reroot_value_key(bf.value_key, target_path=target_path),
            )),
        )
        for bf in host_prebound.bound_filters[: host_prebound.n_date_range]
        if _reaches(reroot_value_key(bf.value_key, target_path=target_path))
    ]
    n_rerooted_tds = sum(
        1 for dm in grain_declared
        if isinstance(dm.bound.value_key, TimeTruncKey)
    )
    sub_prebound = _nested_prebound(
        host_prebound=host_prebound,
        aggregate_measure=_aggregate_declared_measure(
            key=reroot_value_key(agg_key, target_path=target_path),
            model=target_model,
            public_alias=None,
        ),
        grain_measures=grain_declared,
        inherited_filters=rerooted_filters,
        date_range_filters=rerooted_bounds,
        n_dims=len(grain_declared) - n_rerooted_tds,
        n_time_dimensions=n_rerooted_tds,
        main_time_key=next(
            (
                dm.bound.value_key for dm in grain_declared
                if isinstance(dm.bound.value_key, TimeTruncKey)
            ),
            None,
        ),
    )
    sub_plan = subplan_builder(
        StrictQueryCarrier(
            source_model=target_model_name, prebound=sub_prebound,
        ),
        rerooted_bundle,
    )

    sub_row_by_key = {s.key: s.id for s in sub_plan.row_slots}
    grain_pairs: List[Tuple[str, str]] = []
    for host_sid, rr_key in zip(grain_host_sids, grain_rerooted_keys):
        sub_sid = sub_row_by_key.get(rr_key)
        if sub_sid is not None:
            grain_pairs.append((host_sid, sub_sid))

    sub_agg_sid = None
    for s in sub_plan.aggregate_slots:
        if isinstance(s.key, AggregateKey) and not getattr(
            s.key.source, "path", (),
        ):
            sub_agg_sid = s.id
            break
    if sub_agg_sid is None:
        # Same abandon, same reason: no sub-plan means the FORWARD plan ships,
        # so it must carry forward-coordinate routing.
        return _forward_only()

    # DEV-1747 B6/D6 — the AUDIT survives the reroot; the ROUTING does not,
    # because they are different statements and the reroot changes only one.
    #
    # ``applied_filter_ids`` records what SOME scope evaluates, which is what
    # makes a genuinely unreachable filter distinguishable from a reachable one
    # instead of every case reporting ``applied=[] dropped=[]``. Those ids ride
    # into ``sub_prebound`` above, so the sub-plan really does apply them.
    #
    # ``where_filter_ids`` / ``having_filter_ids`` say something narrower: this
    # filter MOVED to the forward CTE, so the host base must not apply it. A
    # re-rooted plan has no forward CTE — the sub-plan replaces it and carries
    # its own filters — and the predicate is host-evaluable by construction
    # (it was bound against the host). Leaving the ids there tells the host base
    # to skip a filter nothing else applies at the host, so rows the user
    # excluded come back with a NULL measure attached.
    return plan.model_copy(update={
        "rerooted_plan": sub_plan,
        "rerooted_grain_pairs": grain_pairs,
        "rerooted_agg_slot_id": sub_agg_sid,
        "where_filter_ids": [],
        "having_filter_ids": [],
    })
