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
)
from slayer.core.models import ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.sql.naming import canonical_aggregate_alias
from slayer.core.scope import ModelScope, StageColumn, StageSchema
from slayer.engine.aggregate_input_paths import (
    compute_aggregate_input_join_paths,
)
from slayer.engine.binding import (
    bind_expr,
    bind_filter,
    bind_time_dimension,
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
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.syntax import parse_expr, parse_filter_expr


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
        if not path_is_reachable(path=p, target_path=target_path)
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
        host_query: Optional[SlayerQuery] = None,
        public_projection: Optional[List[SlotId]] = None,
        subplan_builder: Optional[
            Callable[[SlayerQuery, ResolvedSourceBundle], PlannedQuery]
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
    formula: str,
    host_model: SlayerModel,
) -> SlotId:
    """Locate the sub-plan's single local aggregate slot.

    Recursion suppression guarantees no nested cross-model plans so the
    sub-plan has exactly one local aggregate — the filtered measure being
    isolated.
    """
    for s in sub_plan.aggregate_slots:
        if isinstance(s.key, AggregateKey) and not getattr(
            s.key.source, "path", (),
        ):
            return s.id
    raise ValueError(
        "DEV-1503 sub-plan produced no local aggregate slot for "
        f"{formula!r} on {host_model.name!r} — planner bug."
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


def _classify_subplan_filters(
    *,
    host_filters: List[HostFilterRouting],
) -> Optional[List[str]]:
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
    """
    sub_filter_texts: List[str] = []
    for routing in host_filters:
        if routing.text is None:
            # date_range bound — not a user filter, do not propagate.
            continue
        if routing.phase in (Phase.POST, Phase.AGGREGATE):
            continue
        # ROW phase — propagate.
        sub_filter_texts.append(routing.text)
    return sub_filter_texts or None


def _route_host_filters(
    *,
    host_filters: List[HostFilterRouting],
    host_slots: List[ValueSlot],
    target_path: Tuple[str, ...],
    host_model: SlayerModel,
    terminal_model: SlayerModel,
) -> Tuple[
    List[BoundFilterId], List[BoundFilterId], List[BoundFilterId],
    List[UnreachableFilterDroppedWarning],
]:
    """Classify each host filter via the ``inherited_filter_policy`` decision
    table (``classify_host_filter``) into ``(applied, where_ids, having_ids,
    dropped)`` — extracted from ``IsolatedCteCrossModelPlanner.plan`` (DEV-1708)
    to keep that method focused. ``DROP_HOST_LOCAL`` / ``STAY_AT_HOST_POST`` are
    neither propagated nor warned."""
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
    return applied, where_ids, having_ids, dropped


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
        host_query: Optional[SlayerQuery] = None,
        public_projection: Optional[List[SlotId]] = None,
        subplan_builder: Optional[
            Callable[[SlayerQuery, ResolvedSourceBundle], PlannedQuery]
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
        if not path:
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
        applied, where_ids, having_ids, dropped = _route_host_filters(
            host_filters=host_filters,
            host_slots=host_slots,
            target_path=target_path,
            host_model=host_model,
            terminal_model=terminal_model,
        )

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

        forward_plan = CrossModelAggregatePlan(
            aggregate_slot_id=aggregate_slot_id,
            target_model=terminal_model.name,
            datasource=host_model.data_source,
            join_chain=join_chain,
            join_back_pairs=join_back_pairs,
            cte_stage_schema=cte_schema,
            shared_grain_slots=shared_grain,
            applied_filter_ids=applied,
            where_filter_ids=where_ids,
            having_filter_ids=having_ids,
            target_model_filters=target_model_filters,
            dropped_filter_warnings=dropped,
            hidden=hidden,
            public_alias=public_alias,
        )

        # DEV-1450 #2: re-rooting is the strategy's call. When the caller
        # supplies the host query + a sub-plan builder, decide forward-plan
        # vs re-rooted-plan here; without them (direct ``plan(...)`` callers /
        # test doubles) return the forward plan unchanged.
        if subplan_builder is not None and host_query is not None:
            return _maybe_reroot_cross_model_plan(
                plan=forward_plan,
                query=host_query,
                agg_key=aggregate_key,
                bundle=bundle,
                host_model=host_model,
                public_projection=public_projection or [],
                subplan_builder=subplan_builder,
            )
        return forward_plan

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
        host_query: Optional[SlayerQuery],
        public_projection: Optional[List[SlotId]],
        subplan_builder: Optional[
            Callable[[SlayerQuery, ResolvedSourceBundle], PlannedQuery]
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
        has_crossing_input = has_crossing_filter or bool(
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
        host_query: SlayerQuery,
        public_alias: Optional[str],
        public_projection: List[SlotId],
        hidden: bool,
        subplan_builder: Callable[
            [SlayerQuery, ResolvedSourceBundle], PlannedQuery,
        ],
    ) -> CrossModelAggregatePlan:
        """Build a host-rooted nested sub-plan for a cross-model-FILTERED
        local measure (DEV-1503).

        The sub-plan is a ``SlayerQuery`` rooted at the SAME host model with
        ``measures=[<the filtered measure>]`` and the host's dimensions /
        time_dimensions. The sub-plan's ``plan_query`` recursion handles
        the filter-target join (its ``Column.filter`` will pull in the
        joined table at the generator's inline path), the host model's own
        ``SlayerModel.filters``, and the per-dimension GROUP BY — producing a
        per-grain aggregate that the host base LEFT JOINs back.

        Host query filters are NOT propagated into the sub-plan here — the
        host base CTE applies them. The generator's outer-WHERE wrapper
        handles aggregate-referencing filters separately (DEV-1503 spec).
        """
        # Reconstruct the local measure formula from the AggregateKey. The
        # source.path is empty so ``_local_agg_formula`` emits a bare
        # ``leaf:agg`` shape (plus any args / kwargs). Carry the user-
        # supplied alias through so a host filter referencing the rename
        # (``latest_pmt > 500`` for a measure named ``latest_pmt``) binds
        # against the same alias in the sub-plan rather than the canonical
        # ``latest_payment_last_updated_at`` form.
        formula = _local_agg_formula(aggregate_key)
        measure_name_for_subplan = public_alias
        sub_filters = _classify_subplan_filters(host_filters=host_filters)
        rerooted_query = SlayerQuery(
            source_model=host_model.name,
            measures=[ModelMeasure(
                formula=formula, name=measure_name_for_subplan,
            )],
            dimensions=list(host_query.dimensions or []) or None,
            time_dimensions=list(host_query.time_dimensions or []) or None,
            filters=sub_filters,
        )
        sub_plan = subplan_builder(rerooted_query, bundle)

        grain_pairs = _match_filtered_local_grain_pairs(
            host_slots=host_slots,
            public_projection=public_projection,
            sub_plan=sub_plan,
        )
        sub_agg_sid = _find_filtered_local_sub_agg_slot(
            sub_plan=sub_plan, formula=formula, host_model=host_model,
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
            applied_filter_ids=[],
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
# The fix mirrors legacy ``_build_rerooted_enriched``: build a full nested
# ``SlayerQuery`` rooted at the target (so all of the target's joins are in
# scope for dimensions AND filters), compile it via ``subplan_builder``, and
# attach the sub-plan to the ``CrossModelAggregatePlan``. The generator
# renders the sub-plan as the ``_cm_*`` CTE and joins it back to the host base
# on the (re-rooted) dimension. Dimensions / filters that don't resolve from
# the target are dropped -- matching legacy's drop-unreachable behaviour.
#
# DEV-1450 #2: this used to be a post-hoc pass in ``stage_planner.plan_query``;
# it now lives behind ``IsolatedCteCrossModelPlanner.plan`` so the
# render-strategy decision (forward vs re-rooted) is owned by the strategy.
# The recursive ``plan_query`` call is injected as ``subplan_builder`` so this
# module does not import ``stage_planner`` (no cycle).


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


def _maybe_reroot_cross_model_plan(
    *,
    plan,
    query: SlayerQuery,
    agg_key: AggregateKey,
    bundle: ResolvedSourceBundle,
    host_model: SlayerModel,
    public_projection: List[str],
    subplan_builder: Callable[[SlayerQuery, ResolvedSourceBundle], PlannedQuery],
):
    """Attach a re-rooted sub-``PlannedQuery`` to ``plan`` when the host
    query carries dimensions reachable from the target by re-rooting through
    the target's join graph. Returns ``plan`` unchanged when re-rooting is
    unnecessary (only forward-path or genuinely unreachable dims)."""
    target_model_name = plan.target_model
    target_model = bundle.get_referenced_model(target_model_name)
    if target_model is None:
        return plan
    target_path = tuple(getattr(agg_key.source, "path", ()))
    rerooted_bundle = bundle.model_copy(update={"source_model": target_model})
    target_scope = ModelScope(source_model=target_model)

    def _resolvable_ref(ref_str: str) -> Optional[ValueKey]:
        try:
            return bind_expr(
                parse_expr(ref_str),
                scope=target_scope,
                bundle=rerooted_bundle,
            ).value_key
        except _REROOT_BIND_ERRORS:
            return None

    def _is_forward(path: Tuple[str, ...]) -> bool:
        # On the host->target path (handled by the forward-path CTE already).
        return bool(path) and path == target_path[: len(path)]

    n_dims = len(query.dimensions or [])
    rerooted_dims: List[ColumnRef] = []
    rerooted_tds: List[TimeDimension] = []
    grain_host_sids: List[str] = []
    grain_rerooted_keys: List[ValueKey] = []
    needs_reroot = False

    for i, dim in enumerate(query.dimensions or []):
        host_sid = public_projection[i] if i < len(public_projection) else None
        host_path = _host_ref_path(dim.model)
        rr = _reroot_ref(
            model_prefix=dim.model, name=dim.name,
            host_model_name=host_model.name, target_model_name=target_model_name,
        )
        rr_key = _resolvable_ref(rr)
        if rr_key is None:
            continue  # unreachable from target -> drop
        if not _is_forward(host_path):
            needs_reroot = True
        if host_sid is None:
            continue
        rerooted_dims.append(ColumnRef(name=rr, label=dim.label))
        grain_host_sids.append(host_sid)
        grain_rerooted_keys.append(rr_key)

    for j, td in enumerate(query.time_dimensions or []):
        idx = n_dims + j
        host_sid = public_projection[idx] if idx < len(public_projection) else None
        host_path = _host_ref_path(td.dimension.model)
        rr = _reroot_ref(
            model_prefix=td.dimension.model, name=td.dimension.name,
            host_model_name=host_model.name, target_model_name=target_model_name,
        )
        rr_td = TimeDimension(
            dimension=ColumnRef(name=rr),
            granularity=td.granularity,
            date_range=td.date_range,
            label=td.label,
        )
        try:
            rr_key = bind_time_dimension(
                rr_td, scope=target_scope, bundle=rerooted_bundle,
            ).value_key
        except _REROOT_BIND_ERRORS:
            continue
        if not _is_forward(host_path):
            needs_reroot = True
        if host_sid is None:
            continue
        rerooted_tds.append(rr_td)
        grain_host_sids.append(host_sid)
        grain_rerooted_keys.append(rr_key)

    # Filters. A purely host-local filter (every ref on the host's own
    # columns) filters host rows -- it stays at the host base; the join-back
    # propagates the cardinality reduction, so adding it to the CTE would risk
    # binding a bare name to a same-named TARGET column. A join-traversing
    # filter affects the aggregate value and rides into the re-rooted CTE; one
    # that reaches OFF the host->target forward path is exactly what the
    # forward-path classifier drops, so it also triggers re-rooting (covers a
    # cross-model agg filtered through the target's graph with no dimensions).
    host_scope = ModelScope(source_model=host_model)
    rerooted_filters: List[str] = []
    for f in (query.filters or []):
        try:
            host_bound = bind_filter(
                parse_filter_expr(f), scope=host_scope, bundle=bundle,
            )
        except _REROOT_BIND_ERRORS:
            continue
        host_paths = _filter_ref_paths(host_bound.value_key)
        if all(p == () for p in host_paths):
            continue  # host-local -> applied at the host base only
        # The binder strips a same-model self-prefix (C14), so a
        # ``<target>.col`` ref binds locally against the target scope without
        # any string surgery -- pass the filter through verbatim.
        try:
            bind_filter(
                parse_filter_expr(f), scope=target_scope, bundle=rerooted_bundle,
            )
        except _REROOT_BIND_ERRORS:
            continue
        rerooted_filters.append(f)
        if any(p != target_path[: len(p)] for p in host_paths if p):
            needs_reroot = True

    if not needs_reroot or not (
        rerooted_dims or rerooted_tds or rerooted_filters
    ):
        return plan

    rerooted_query = SlayerQuery(
        source_model=target_model_name,
        measures=[ModelMeasure(formula=_local_agg_formula(agg_key))],
        dimensions=rerooted_dims or None,
        time_dimensions=rerooted_tds or None,
        filters=rerooted_filters or None,
    )
    sub_plan = subplan_builder(rerooted_query, rerooted_bundle)

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
        return plan

    return plan.model_copy(update={
        "rerooted_plan": sub_plan,
        "rerooted_grain_pairs": grain_pairs,
        "rerooted_agg_slot_id": sub_agg_sid,
        # The forward-path classifier marked these host filters
        # DROP_UNREACHABLE, but the re-rooted CTE re-applies every
        # target-reachable filter (and the host base keeps the rest for
        # cardinality), so nothing is silently dropped -- clear the now-stale
        # warnings and forward-only routing ids.
        "dropped_filter_warnings": [],
        "where_filter_ids": [],
        "having_filter_ids": [],
        "applied_filter_ids": [],
    })
