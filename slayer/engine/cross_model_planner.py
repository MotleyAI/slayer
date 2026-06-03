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
)
from slayer.core.models import ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.core.refs import agg_kwarg_canonical_str, canonical_agg_name
from slayer.core.scope import ModelScope, StageColumn, StageSchema
from slayer.engine.binding import (
    bind_expr,
    bind_filter,
    bind_time_dimension,
    walk_value_keys,
)
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


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------


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

    ``host_model_name`` is used to route ``ColumnSqlKey`` refs (derived
    columns): the key carries its host model name but not a path, so we
    compare it against the host model name and the path to decide
    reachable / local / unreachable. When ``host_model_name`` is None,
    ColumnSqlKey refs default to local — conservative for callers that
    don't have the host model in scope.
    """
    if host_filter.phase == Phase.POST:
        return FilterRoute.STAY_AT_HOST_POST
    if not host_filter.referenced_slot_ids:
        # No referenced slots — nothing to route into the CTE.
        return FilterRoute.STAY_AT_HOST_POST

    by_id = {s.id: s for s in host_slots}

    local_row: List[SlotId] = []
    reachable_path: List[SlotId] = []
    unreachable: List[SlotId] = []
    aggregate_on_target: List[SlotId] = []
    aggregate_other: List[SlotId] = []

    for sid in host_filter.referenced_slot_ids:
        s = by_id.get(sid)
        if s is None:
            # Unknown slot id — be conservative, treat as unreachable.
            unreachable.append(sid)
            continue
        if isinstance(s.key, AggregateKey):
            agg_source = s.key.source
            agg_path = getattr(agg_source, "path", ())
            if agg_path == target_path:
                aggregate_on_target.append(sid)
            else:
                aggregate_other.append(sid)
        elif isinstance(s.key, ColumnKey):
            if not s.key.path:
                local_row.append(sid)
            elif s.key.path == target_path[: len(s.key.path)]:
                reachable_path.append(sid)
            else:
                unreachable.append(sid)
        elif isinstance(s.key, ColumnSqlKey):
            # Derived column. Route by its host model: on host → local;
            # on any model in target_path → reachable; otherwise →
            # unreachable.
            cm = s.key.model
            if host_model_name is not None and cm == host_model_name:
                local_row.append(sid)
            elif cm in target_path:
                reachable_path.append(sid)
            elif host_model_name is None:
                # No host name to compare against — conservative default.
                local_row.append(sid)
            else:
                unreachable.append(sid)
        else:
            # Transform / Arithmetic / ScalarCall: phase already decided.
            # POST was checked above; ROW/AGGREGATE land here from
            # arithmetic / scalar calls. Treat as local for routing.
            local_row.append(sid)

    if unreachable or aggregate_other:
        # Any unreachable ref → drop + warn (covers pure-unreachable AND
        # mixed-with-reachable cases per decision table rows 6/7).
        return FilterRoute.DROP_UNREACHABLE
    if local_row and not (aggregate_on_target or reachable_path):
        return FilterRoute.DROP_HOST_LOCAL
    if local_row:
        # Mixed local + (target-path / target-agg). The local refs can't
        # be evaluated in the CTE, so the filter stays at host.
        return FilterRoute.DROP_HOST_LOCAL
    if aggregate_on_target:
        return FilterRoute.PROPAGATE_HAVING
    if reachable_path:
        return FilterRoute.PROPAGATE_WHERE
    return FilterRoute.STAY_AT_HOST_POST


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
    # ColumnKey -> leaf, ColumnSqlKey (derived agg source) -> column_name,
    # StarKey -> "*" (CR / Codex: a derived source must alias as
    # ``net_sum``, not ``_sum``).
    measure_name = (
        getattr(key.source, "leaf", None)
        or getattr(key.source, "column_name", None)
        or "*"
    )
    # AggregateKey.args / kwargs are normalised tuples of scalars /
    # ColumnKey-shaped values; convert to the (List[str],
    # Dict[str, Any]) shape ``canonical_agg_name`` expects. DEV-1450
    # stage 7b.13: route through ``agg_kwarg_canonical_str`` so a
    # ColumnKey kwarg renders as ``leaf`` (or ``path.leaf`` for joined
    # paths) instead of Pydantic-repr noise from naive ``str(v)``.
    # The kwarg suffix is preserved here -- the legacy enrichment at
    # ``query_engine.py:2160`` drops it, causing two parametric aggs
    # with different kwargs to collide on CTE alias (legacy bug).
    # The 7b.5 fix added kwarg-aware aliases here as a correctness
    # improvement -- ``test_cross_model_planner_wiring.py::
    # test_parameterized_aggregates_get_distinct_cte_aliases`` pins
    # this. Parity with legacy for cross-model parametric aggs is
    # not achievable on this axis.
    args_list = [agg_kwarg_canonical_str(a) for a in key.args]
    kwargs_dict = {k: agg_kwarg_canonical_str(v) for k, v in key.kwargs}
    return canonical_agg_name(
        measure_name=measure_name,
        aggregation_name=key.agg,
        agg_args=args_list or None,
        agg_kwargs=kwargs_dict or None,
    )


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
    host_query: SlayerQuery,
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

    ``host_filters`` is assembled by ``stage_planner`` as
    ``[date_range_routings..., user_filter_routings...]`` (in order), so the
    user portion is the trailing slice; do NOT look up by ``f"f{i}"``
    against ``host_query.filters[i]`` — when a ``date_range``-bearing
    time_dimension is present the index is off by ``n_date_range`` and
    aggregate-phase user filters get classified against a leading
    date_range routing (Codex review).
    """
    user_query_filters = host_query.filters or []
    if not user_query_filters:
        return None
    user_routings = list(host_filters[-len(user_query_filters):])
    sub_filter_texts: List[str] = []
    for routing, filt in zip(user_routings, user_query_filters):
        if routing.phase in (Phase.POST, Phase.AGGREGATE):
            continue
        # ROW phase — propagate.
        sub_filter_texts.append(filt)
    return sub_filter_texts or None


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
                    reason=(
                        f"filter {hf.filter_id!r} references slot(s) outside "
                        f"the join path to {terminal_model.name!r}; "
                        f"unreachable filters are dropped."
                    ),
                ))
            # DROP_HOST_LOCAL and STAY_AT_HOST_POST: not propagated, not warned.

        target_model_filters = list(terminal_model.filters or [])

        # Shared grain: local ROW slots on host (dimensions) flow through.
        # Cross-branch ROW slots and aggregate / transform slots do not.
        # DEV-1450 stage 7b.12: ``TimeTruncKey`` slots count as grain
        # candidates too — a joined TD (``customers.created_at`` MONTH)
        # whose column path lies on the target's join chain is shared
        # between the host base and the cross-model CTE, so legacy
        # ``LEFT JOIN`` on the truncated alias replaces the global
        # ``CROSS JOIN``.
        shared_grain: List[SlotId] = []
        for s in host_slots:
            if isinstance(s.key, ColumnKey):
                p = s.key.path
                if not p or p == target_path[: len(p)]:
                    shared_grain.append(s.id)
            elif isinstance(s.key, TimeTruncKey):
                td_path = column_path(s.key.column)
                if not td_path or td_path == target_path[: len(td_path)]:
                    shared_grain.append(s.id)

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
        """Validate the filtered-local trigger preconditions and dispatch
        into ``_plan_filtered_local`` — the aggregate is on a HOST column
        but its ``Column.filter`` crosses a join, so a host-rooted nested
        sub-plan owns the aggregation and the host base LEFT JOINs back.
        """
        agg_source = aggregate_key.source
        cfk = aggregate_key.column_filter_key
        if cfk is None or not cfk.referenced_join_paths:
            raise ValueError(
                f"AggregateKey on {agg_source!r} has empty source.path "
                f"AND no cross-model column_filter_key — this is a plain "
                f"local aggregate. The cross-model planner should not "
                f"have been invoked."
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
        sub_filters = _classify_subplan_filters(
            host_query=host_query,
            host_filters=host_filters,
        )
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


def _local_agg_formula(key: AggregateKey) -> str:
    """Reconstruct the LOCAL colon-formula for a cross-model aggregate
    (``customers.revenue:sum`` -> ``revenue:sum``) so it can be re-planned
    against the target model as a plain local measure.

    Column-valued kwargs (``corr(other=customers.region_id)``) are
    re-rooted too: their join path is bound from the HOST, so the leading
    agg-source (target) prefix is stripped to express the ref in the
    target's local scope (``other=region_id``; a deeper hop keeps its
    residual path, ``other=regions.code``). Dropping the path outright
    would mis-bind or fail to bind the nested sub-query (CR review)."""
    src = key.source
    target_path = tuple(getattr(src, "path", ()))
    if isinstance(src, StarKey):
        base = "*"
    elif isinstance(src, ColumnSqlKey):
        base = src.column_name
    else:  # ColumnKey
        base = src.leaf

    def _reroot_col_kwarg(v) -> str:
        leaf = v.leaf if isinstance(v, ColumnKey) else v.column_name
        vpath = tuple(getattr(v, "path", ()))
        # Strip the agg-source (target) prefix so the ref is target-local.
        residual = (
            vpath[len(target_path):]
            if vpath[: len(target_path)] == target_path
            else vpath
        )
        return ".".join((*residual, leaf))

    formula = f"{base}:{key.agg}"
    parts: List[str] = []
    # Positional args may carry ColumnKey / ColumnSqlKey just like kwargs do
    # (rerooting needs path-aware handling on both — CR review). Falling
    # through to ``_scalar_formula_literal`` would emit Pydantic-repr noise
    # for a column-valued positional arg, mis-binding the nested sub-query.
    for a in key.args:
        if isinstance(a, (ColumnKey, ColumnSqlKey)):
            parts.append(_reroot_col_kwarg(a))
        else:
            parts.append(_scalar_formula_literal(a))
    for k, v in key.kwargs:
        if isinstance(v, (ColumnKey, ColumnSqlKey)):
            parts.append(f"{k}={_reroot_col_kwarg(v)}")
        else:
            parts.append(f"{k}={_scalar_formula_literal(v)}")
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
