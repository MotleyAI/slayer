"""Stage 7a.7 (DEV-1450) — multi-stage source_queries planner.

Orchestrates a list of ``SlayerQuery`` stages into a list of
``PlannedQuery``s, the typed input the SQL generator (stage 7b) will
consume.

Per-stage pipeline:

  raw SlayerQuery → parse (per measure / filter / order) → bind →
  ProjectionPlanner → PlannedQuery (+ emitted StageSchema)

Multi-stage:

* Stages are topologically sorted so each stage appears after the
  siblings it references via ``source_model``.
* Downstream stages bind against the upstream ``StageSchema`` (P6) —
  flat namespace, no dotted-join walking. ``IllegalScopeReferenceError``
  on dotted refs (DEV-1449).
* Each stage's ``StageSchema`` columns use the user-supplied ``name``
  (or canonical alias) as the column ``name`` (DEV-1448).

Dormant in 7a — no engine wiring. Stage 7b's engine cutover flips
``engine.execute`` / ``engine.save_model`` over to ``plan_stages``.
"""

from __future__ import annotations

from typing import Dict, FrozenSet, List, Optional, Tuple, Union

from slayer.core.enums import DataType
from slayer.core.format import NumberFormat
from slayer.core.errors import (
    AmbiguousReferenceError,
    UnknownReferenceError,
    UnresolvableOrderColumnError,
)
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    InKey,
    LiteralKey,
    Phase,
    ScalarCallKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    normalize_scalar,
)
from slayer.core.models import SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery, TimeDimension
from slayer.core.refs import agg_kwarg_canonical_str, canonical_agg_name
from slayer.core.time_bounds import strip_frame_bounds
from slayer.core.window_duration import parse_window_duration
from slayer.core.scope import ModelScope, StageColumn, StageSchema
from slayer.engine.aggregate_input_paths import (
    compute_aggregate_input_join_paths,
)
from slayer.engine.binding import (
    BoundExpr as BinderBoundExpr,
    BoundFilter,
    bind_expr,
    bind_filter,
    bind_time_dimension,
    walk_value_keys,
)
from slayer.engine.cross_model_planner import (
    CrossModelPlanner,
    HostFilterRouting,
    IsolatedCteCrossModelPlanner,
)
from slayer.engine.measure_expansion import expand_model_measures
from slayer.engine.response_meta import _infer_aggregated_format
from slayer.engine.planned import (
    BoundExpr as PlannedBoundExpr,
    FilterPhase,
    OrderEntry,
    PlannedQuery,
    SrcFilterRewrite,
    TransformLayer,
    ValueSlot,
    WindowedAggregatePlan,
)
from slayer.engine.planning import (
    DeclaredMeasure,
    OrderSpec,
    ProjectionPlanner,
    _iter_slot_deps,
    filter_referenced_slot_ids,
    lower_sugar_transforms,
    rewrite_rank_partition_keys,
)
from slayer.engine.source_bundle import (
    ResolvedSourceBundle,
    _apply_extension_overlay,
    _source_name_if_sibling,
    stage_bundle_with_siblings,
    synthetic_model_from_stage_schema,
)
from slayer.engine.syntax import parse_expr, parse_filter_expr
from slayer.sql.naming import flat_name
from slayer.sql.sql_expr import has_window_function
from slayer.sql.sql_predicate import parse_sql_predicate


__all__ = ["plan_query", "plan_stages"]


# Stage 7b.10 — TIME_NEEDING transform ops that require a resolvable
# time dimension to render their OVER ``ORDER BY``. Mirrors the legacy
# ``TIME_TRANSFORMS`` set at ``slayer/core/formula.py:33``.
_TIME_NEEDING_TRANSFORM_OPS = frozenset({
    "cumsum",
    "change",
    "change_pct",
    "time_shift",
    "first",
    "last",
    "lag",
    "lead",
    "consecutive_periods",
})


def _attach_time_keys(
    key: ValueKey, *, td_key: TimeTruncKey,
) -> ValueKey:
    """Walk ``key``; for every ``TransformKey`` whose op needs a time
    dimension and whose ``time_key`` is ``None``, return a copy with
    ``time_key=td_key``. Identity-preserving when nothing changes.

    Mirrors ``lower_sugar_transforms``' walker shape so identity
    semantics line up: nested TransformKey/ArithmeticKey/ScalarCallKey/
    BetweenKey trees are rebuilt only on the path containing a patch.
    """
    if isinstance(key, TransformKey):
        new_input = _attach_time_keys(key.input, td_key=td_key)
        out = key
        if new_input is not key.input:
            out = out.model_copy(update={"input": new_input})
        if out.op in _TIME_NEEDING_TRANSFORM_OPS and out.time_key is None:
            out = out.model_copy(update={"time_key": td_key})
        return out
    if isinstance(key, ArithmeticKey):
        new_ops = tuple(
            _attach_time_keys(o, td_key=td_key) for o in key.operands
        )
        if all(a is b for a, b in zip(new_ops, key.operands)):
            return key
        return ArithmeticKey(op=key.op, operands=new_ops)
    if isinstance(key, ScalarCallKey):
        new_args = tuple(
            _attach_time_keys(a, td_key=td_key)
            if isinstance(
                a, (TransformKey, ArithmeticKey, ScalarCallKey, BetweenKey),
            )
            else a
            for a in key.args
        )
        if all(a is b for a, b in zip(new_args, key.args)):
            return key
        return ScalarCallKey(name=key.name, args=new_args)
    if isinstance(key, BetweenKey):
        nc = _attach_time_keys(key.column, td_key=td_key)
        nl = _attach_time_keys(key.low, td_key=td_key)
        nh = _attach_time_keys(key.high, td_key=td_key)
        if nc is key.column and nl is key.low and nh is key.high:
            return key
        return BetweenKey(column=nc, low=nl, high=nh)
    if isinstance(key, InKey):
        # DEV-1475: ``InKey.values`` is a literal-only tuple — no
        # transforms to attach a time key to. Only the LHS column path
        # can carry a transform; rebuild only if it changed.
        nc = _attach_time_keys(key.column, td_key=td_key)
        if nc is key.column:
            return key
        return InKey(column=nc, values=key.values, negated=key.negated)
    return key


def _partition_key_display(pk: ValueKey) -> str:
    """Human-readable name of a rank ``partition_by`` key for error messages
    (DEV-1497). Local refs surface as the bare leaf; joined refs keep the
    dotted path."""
    if isinstance(pk, ColumnKey):
        return ".".join([*pk.path, pk.leaf])
    if isinstance(pk, ColumnSqlKey):
        return ".".join([*pk.path, pk.column_name])
    if isinstance(pk, TimeTruncKey):
        return _partition_key_display(pk.column)
    return str(pk)


def _row_key_path(key: ValueKey) -> tuple:
    """Join path of a ROW value key (``()`` for local, non-empty for joined).
    Unwraps a ``TimeTruncKey`` to its underlying column."""
    if isinstance(key, TimeTruncKey):
        return _row_key_path(key.column)
    return tuple(getattr(key, "path", ()))


def _find_unresolved_time_needing_op(key: ValueKey) -> Optional[str]:
    """Return the op name of the first time-needing TransformKey reached
    that has ``time_key is None``, or ``None`` if every time-needing
    transform in the tree is resolved.
    """
    if isinstance(key, TransformKey):
        if key.op in _TIME_NEEDING_TRANSFORM_OPS and key.time_key is None:
            return key.op
        return _find_unresolved_time_needing_op(key.input)
    if isinstance(key, ArithmeticKey):
        for o in key.operands:
            found = _find_unresolved_time_needing_op(o)
            if found:
                return found
        return None
    if isinstance(key, ScalarCallKey):
        for a in key.args:
            if isinstance(
                a, (TransformKey, ArithmeticKey, ScalarCallKey, BetweenKey),
            ):
                found = _find_unresolved_time_needing_op(a)
                if found:
                    return found
        return None
    if isinstance(key, BetweenKey):
        for k in (key.column, key.low, key.high):
            found = _find_unresolved_time_needing_op(k)
            if found:
                return found
        return None
    if isinstance(key, InKey):
        # DEV-1475: only the LHS column can host a time-needing
        # transform; the RHS values are literals.
        return _find_unresolved_time_needing_op(key.column)
    return None


# ---------------------------------------------------------------------------
# DEV-1714 Stage 10 — duration-windowed measures (``window='90d'``).
# ---------------------------------------------------------------------------


def _window_kwarg_of(key: ValueKey):
    """The ``window`` kwarg value of an ``AggregateKey``, or ``None``.

    ``window`` is a globally reserved aggregation kwarg name (legacy parity —
    the enrichment pipeline pops it unconditionally before dispatch), so its
    presence marks a windowed measure regardless of the aggregation.
    """
    if isinstance(key, AggregateKey):
        for k, v in key.kwargs:
            if k == "window":
                return v
    return None


def _windowed_agg_keys(vk: ValueKey) -> list:
    """Every windowed ``AggregateKey`` in ``vk``'s value-key tree."""
    return [k for k in walk_value_keys(vk) if _window_kwarg_of(k) is not None]


def _reject_unsupported_windowed_key(key: AggregateKey) -> None:
    """Per-key guards shared by selected and filter/order-referenced windowed
    aggregates: sum/avg-only (G1), string duration + compact syntax (G8), and
    no cross-model source (G3). Raises with the pinned-message contract."""
    if key.agg not in ("sum", "avg"):
        raise ValueError(
            f"Aggregation parameter 'window' is only supported for sum and avg, "
            f"not '{key.agg}'."
        )
    window_val = _window_kwarg_of(key)
    if not isinstance(window_val, str):
        raise ValueError(
            f"Window duration must be a compact duration string like '90d', got "
            f"{window_val!r}. Use syntax like '1y2m3w5d6h7min8s'."
        )
    parse_window_duration(window_val)  # G8 — raises on empty / malformed
    if getattr(key.source, "path", ()):  # G3
        raise NotImplementedError(
            "Windowed cross-model aggregates (e.g. customers.revenue:sum("
            "window='90d')) are not yet supported (DEV-1504)."
        )


def _guard_windowed_measures(  # NOSONAR(S3776) — one cohesive ordered guard pass (G1→G8→G3→G4→G5→G7→G6→G2) over the original value-key trees; each branch is a distinct unsupported-shape rejection sharing the windowed-key scan, and splitting would scatter the precedence contract.
    *,
    measure_vks: list,
    filter_vks: list,
    order_vks: list,
    active_td_key,
) -> dict:
    """Reject unsupported windowed-measure shapes at plan time and return the
    cleanly-SELECTED windowed ``AggregateKey``s (the ones that get a
    ``WindowedAggregatePlan``) as an insertion-ordered mapping in measure
    declaration order — so the emitted ``_wm_`` CTEs and combined-SELECT columns
    are DETERMINISTIC (a set made the SQL output order vary across runs, which
    breaks the SQL-text cache key of DEV-1587).

    Runs on the ORIGINAL declared-measure / filter / order value-key trees —
    before projection interning would hide a transform / composite dependency
    slot — so the transform (G4) and composite (G5) guards win over the
    hidden-slot guard (G6). Precedence: G1 → G8 → G3 → G4 → G5 → G7 → G6 → G2.
    """
    all_vks = [*measure_vks, *filter_vks, *order_vks]
    if not any(_windowed_agg_keys(vk) for vk in all_vks):
        return {}

    # G1 / G8 / G3 — per-key validation runs FIRST (documented precedence), so a
    # windowed key with an invalid aggregation / malformed duration / cross-model
    # source reports its specific error even when it is also wrapped in a
    # transform (G4) or composite (G5).
    for vk in all_vks:
        for key in _windowed_agg_keys(vk):
            _reject_unsupported_windowed_key(key)

    # G4 — a windowed measure cannot coexist with (or be the input of) any
    # transform. Checked before the hidden-slot guard so
    # ``cumsum(revenue:sum(window='90d'))`` reports 'transform', never 'selected'.
    if any(isinstance(k, TransformKey) for vk in all_vks for k in walk_value_keys(vk)):
        raise NotImplementedError(
            "Windowed measures (window='…') combined with transforms are not yet "
            "supported (DEV-1504). Compute the windowed measure in a separate "
            "query stage."
        )

    # G5 — a top-level declared measure that IS a windowed AggregateKey is
    # cleanly selected; a windowed key nested in an arithmetic / scalar composite
    # measure is rejected. ``dict`` (not ``set``) preserves measure order.
    selected_windowed: dict = {}
    for vk in measure_vks:
        if not _windowed_agg_keys(vk):
            continue
        if _window_kwarg_of(vk) is not None:
            selected_windowed.setdefault(vk, None)
        else:
            raise NotImplementedError(  # G5
                "Windowed measures (window='…') inside arithmetic / composite / "
                "scalar expressions are not yet supported (DEV-1504)."
            )

    # G7 (mixed) then G6 (hidden) for filter-referenced windowed measures.
    for vk in filter_vks:
        wkeys = _windowed_agg_keys(vk)
        if not wkeys:
            continue
        has_plain_agg = any(
            isinstance(k, AggregateKey) and _window_kwarg_of(k) is None
            for k in walk_value_keys(vk)
        )
        if has_plain_agg:
            raise NotImplementedError(  # G7
                "A single filter that mixes a windowed measure (window='…') with "
                "a plain aggregate is not yet supported (DEV-1504)."
            )
        if any(k not in selected_windowed for k in wkeys):
            raise NotImplementedError(  # G6
                "Filtering on a windowed measure (window='…') requires that "
                "measure to also be selected (DEV-1504)."
            )
    # NB: an ORDER-BY-only windowed reference is not a reachable hidden shape —
    # ``OrderItem`` coercion canonicalises ``revenue:sum(window='90d')`` to the
    # column name ``revenue_sum``, dropping the window kwarg before the planner
    # sees it, so there is never an order-only hidden windowed slot to guard.

    # G2 — a windowed measure needs a resolvable time dimension.
    if active_td_key is None:
        raise ValueError(
            "Windowed measure could not resolve its time dimension. Add a single "
            "time_dimensions entry, or set main_time_dimension to select among "
            "multiple time dimensions."
        )
    return selected_windowed


def _build_windowed_plans(
    *,
    selected_windowed: dict,
    registry,
    row_slots: list,
    active_td_key,
    active_td_slot_id,
) -> Tuple[list, set]:
    """Build one ``WindowedAggregatePlan`` per selected windowed measure and
    return ``(plans, windowed_slot_ids)``. The window time dimension is the
    query's resolved main/active TD (same one first/last/time_shift use)."""
    plans: list = []
    windowed_slot_ids: set = set()
    if not selected_windowed:
        return plans, windowed_slot_ids

    # CR#3 / G2 (post-projection): the window time dimension must be a SELECTED
    # query time dimension (interned as a row slot so it becomes part of the
    # bucket grain). A model ``default_time_dimension`` the query does not select
    # resolves ``active_td_key`` (so the pre-projection G2 passes) but is never
    # interned — ``active_td_slot_id`` is then None. Raise the G2 message rather
    # than crash on the required ``window_time_dimension_slot_id: SlotId`` field.
    if active_td_slot_id is None:
        raise ValueError(
            "Windowed measure could not resolve its time dimension. Add a single "
            "time_dimensions entry, or set main_time_dimension to select among "
            "multiple time dimensions."
        )

    # Grain partition from the PROJECTED (non-hidden) ROW slots: plain
    # dimensions → ``_w_dim_<n>``, non-window time dimensions → ``_w_td_<n>``.
    dim_slot_ids: list = []
    other_td_slot_ids: list = []
    for rs in row_slots:
        if rs.hidden:
            continue
        if isinstance(rs.key, TimeTruncKey):
            if rs.id != active_td_slot_id:
                other_td_slot_ids.append(rs.id)
        else:
            dim_slot_ids.append(rs.id)
    grain_slot_ids = (
        dim_slot_ids
        + ([active_td_slot_id] if active_td_slot_id is not None else [])
        + other_td_slot_ids
    )

    for key in selected_windowed:
        sid = registry.find_by_key(key)
        if sid is None:
            # CR#4: the guard pass already proved this is a cleanly-selected
            # top-level windowed measure, so a missing slot is planner/projection
            # drift — fail loudly rather than let the measure degrade to a plain
            # (non-windowed) aggregate in the base (the silent-wrong-results mode
            # the guards exist to prevent).
            raise RuntimeError(
                f"Windowed measure {key!r} was selected but has no projection "
                f"slot; planner/projection drift (DEV-1714).",
            )
        window_raw = _window_kwarg_of(key)
        plans.append(WindowedAggregatePlan(
            aggregate_slot_id=sid,
            agg=key.agg,
            window_raw=window_raw,
            window_parts=parse_window_duration(window_raw),
            window_time_dimension_slot_id=active_td_slot_id,
            window_granularity=active_td_key.granularity,
            dimension_slot_ids=dim_slot_ids,
            other_time_dimension_slot_ids=other_td_slot_ids,
            grain_slot_ids=grain_slot_ids,
        ))
        windowed_slot_ids.add(sid)
    return plans, windowed_slot_ids


def plan_query(  # NOSONAR(S3776) — planner entry-point dispatcher. The DEV-1503 addition is a small trigger-predicate branch + a kwarg pass-through; the function's pre-existing complexity is owned by the multi-stage scope / bundle / projection / filter-routing wiring it orchestrates and is tracked as a separate refactor.
    *,
    query: SlayerQuery,
    bundle: ResolvedSourceBundle,
    scope: Optional[Union[ModelScope, StageSchema]] = None,
    cross_model_planner: Optional[CrossModelPlanner] = None,
    stage_schemas: Optional[Dict[str, StageSchema]] = None,
    disable_host_rooted_isolation: bool = False,
) -> PlannedQuery:
    """Compile one ``SlayerQuery`` into a typed ``PlannedQuery``.

    ``scope`` defaults to a ``ModelScope`` over ``bundle.source_model``;
    pass an explicit ``StageSchema`` to bind against an upstream stage.
    ``stage_schemas`` is a name → StageSchema map used by
    ``plan_stages`` to wire multi-stage references.

    ``disable_host_rooted_isolation`` (DEV-1503; renamed and widened by
    DEV-1709) suppresses the HOST-ROOTED half of the Law-3 trigger — the
    isolation of a LOCAL aggregate whose ``Column.filter`` or any other
    input (source ``Column.sql``, positional args, kwargs) crosses a join.
    It never affects target-rooted isolation (``source.path`` non-empty).
    Threaded through ``subplan_builder`` whenever a cross-model strategy
    recurses for a host-rooted (or target-rooted) nested sub-plan, so the
    sub-plan's same crossing measure is rendered inline (not infinitely
    re-isolated) and so re-rooting of a genuine cross-model aggregate
    doesn't redundantly isolate the target's own filter joins.
    """
    stage_schemas = stage_schemas or {}
    cross_model_planner = (
        cross_model_planner or IsolatedCteCrossModelPlanner()
    )

    if scope is None:
        source = query.source_model
        if isinstance(source, str) and source in stage_schemas:
            scope = stage_schemas[source]
        else:
            scope = ModelScope(source_model=bundle.source_model)

    # The generator must render this stage's FROM / joins against the SAME
    # model the binder used. For a ModelScope that's the (possibly overlaid /
    # synthetic) host; for a StageSchema chain stage it's None (the generator
    # builds a synthetic model from the upstream schema).
    render_source_model = (
        scope.source_model if isinstance(scope, ModelScope) else None
    )

    # Downstream stages bind against a flat StageSchema — ``__`` is legal
    # in their refs (the upstream's flattened multi-hop aliases); model-
    # scoped stages keep the P1 rejection.
    flat_scope = isinstance(scope, StageSchema)

    declared_measures = _declared_measures_from_query(
        query=query, scope=scope, bundle=bundle,
    )

    # DEV-1450 stage 7b.8 — alias lookup for ORDER BY resolution.
    # A user-supplied order column may reference the declared measure
    # by its public name (user-supplied ``name``), declared name
    # (canonical OR user), or canonical alias. The order pass below
    # checks this map BEFORE falling back to ``bind_expr`` so refs to
    # aggregate aliases like ``amount_sum`` resolve through the
    # projection registry rather than against model scope (where they
    # don't exist as columns).
    declared_alias_to_bound: Dict[str, BinderBoundExpr] = {}
    for dm in declared_measures:
        for alias in (dm.public_name, dm.declared_name, dm.canonical_alias):
            if alias is not None:
                declared_alias_to_bound.setdefault(alias, dm.bound)

    # DEV-1450 stage 7b.15 (DEV-1445, C5): declared-MEASURE aliases a
    # filter may reference by name. A filter ``rev >= 100`` for a measure
    # declared ``{"formula": "customers.revenue:sum", "name": "rev"}``
    # interns ``rev`` onto the cross-model aggregate slot rather than
    # failing to resolve against the model columns; the dotted/colon form
    # already interns structurally, so both forms share one slot (P2/P4).
    #
    # Only MEASURE aliases enter this map — never dimension / time-
    # dimension names. A time dimension's declared name IS its raw column
    # (e.g. ``created_at``), so a WHERE filter ``created_at <= '...'``
    # (such as the one ``snap_to_whole_periods`` injects) must resolve to
    # the raw column, not to the truncated dimension slot. ``declared_
    # measures`` is built in dim → time-dim → measure order, so the
    # measure entries are the tail past the dim/time-dim prefix.
    n_dims = len(query.dimensions or [])
    n_tds = len(query.time_dimensions or [])
    filter_alias_map: Dict[str, ValueKey] = {}
    for dm in declared_measures[n_dims + n_tds:]:
        for alias in (dm.public_name, dm.declared_name, dm.canonical_alias):
            if alias is not None:
                filter_alias_map.setdefault(alias, dm.bound.value_key)

    # DEV-1450 stage 7b.9 — filter list construction in legacy WHERE
    # order: date_range filters first, then SlayerModel.filters
    # (Mode-A SQL), then user query filters (Mode-B DSL). The legacy
    # generator emits date_range BEFORE iterating ``enriched.filters``
    # (slayer/sql/generator.py:2527 vs :2540), and ``enriched.filters``
    # itself is model filters then query filters (enrichment.py:1192).
    #
    # ``bound_filters`` carries the typed-BoundFilter entries (date_range
    # + query filters) for the cross-model routing and projection
    # planner passes. Model filters bypass ``bound_filters`` since
    # they're Mode-A SQL text without a typed value-key — they're
    # appended directly to ``filters_by_phase`` between the two
    # bound-filter buckets.
    bound_filters: List[BoundFilter] = []
    # Parallel to bound_filters — original query-filter text for user
    # filters (None for date_range bounds, which are synthesized from
    # TimeDimension.date_range and have no caller-visible source string).
    # Wired into HostFilterRouting.text below so cross_model_planner can
    # recover ROW-phase user-filter text WITHOUT slicing host_query.filters
    # (which has not been deduped, unlike bound_filters) — CR PR #153
    # thread r3350000254.
    bound_filter_texts: List[Optional[str]] = []
    text_filter_entries: List[FilterPhase] = []

    # 1. date_range filters (one per TD with a 2-element date_range)
    for td in (query.time_dimensions or []):
        if not td.date_range or len(td.date_range) != 2:
            continue
        if not isinstance(scope, ModelScope):
            continue
        bf = _build_date_range_filter(td=td, scope=scope, bundle=bundle)
        bound_filters.append(bf)
        bound_filter_texts.append(None)
    n_date_range = len(bound_filters)

    # 2. SlayerModel.filters — Mode-A SQL, always-applied WHERE.
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        for j, mf in enumerate(scope.source_model.filters or []):
            text_filter_entries.append(_validate_model_filter(
                mf=mf, idx=j, model=scope.source_model,
            ))

    # 3. user query filters (Mode-B DSL).
    #
    # DEV-1450 stage 7b.15 (DEV-1445): two filter strings that bind to the
    # same structural ``ValueKey`` are one predicate (P2). The alias and
    # dotted/colon forms of a renamed cross-model aggregate ref
    # (``rev >= 100`` and ``customers.revenue:sum >= 100``) intern onto the
    # same slot, so emitting both would duplicate the HAVING clause —
    # dedupe by bound key, keeping first occurrence.
    for f in (query.filters or []):
        if not isinstance(f, str):
            continue
        bf = bind_filter(
            parsed=parse_filter_expr(f, allow_dunder=flat_scope),
            scope=scope,
            bundle=bundle,
            alias_map=filter_alias_map,
        )
        if any(existing.value_key == bf.value_key for existing in bound_filters):
            continue
        bound_filters.append(bf)
        bound_filter_texts.append(f)

    order_specs = []
    # Host identity for the qualifier check below — the source model for a
    # ``ModelScope``, the stage relation name (``s1``) for a downstream
    # ``StageSchema`` (so a self-qualified ``s1.metric`` order stays host-local,
    # Codex). Same resolution ``_host_model_name`` uses everywhere else.
    _order_host_name = _host_model_name(scope)
    for o in (query.order or []):
        col_name = o.column.name
        full_name = o.column.full_name
        # An order ref qualified with a FOREIGN model (``owners.status`` when
        # the host is ``orders``) must not resolve to a same-named local column
        # via the bare-leaf shortcut — otherwise a joined sort key silently
        # binds to the local column and sorts by the wrong field (Codex). The
        # bare-name lookups below apply only to unqualified refs or refs
        # qualified with the host itself; a foreign-qualified ref falls through
        # to the dotted/flattened/`bind_expr` paths, where a truly-joined ref
        # is then rejected by the plan-time order validation.
        _order_qualifier = getattr(o.column, "model", None)
        _order_host_local = (
            _order_qualifier is None or _order_qualifier == _order_host_name
        )
        # Prefer declared-measure alias resolution over model-scope
        # binding (DEV-1450 stage 7b.8 — gap fix): aggregate canonical
        # aliases like ``amount_sum`` are not columns on the model, so
        # ``bind_expr`` would raise. The alias map covers user-supplied
        # ``name``, canonical alias, and the declared name itself.
        #
        # DEV-1450 stage 7b.15 (DEV-1443/1445): a cross-model order key
        # written ``customers.revenue:sum`` is coerced by ``OrderItem``
        # to ColumnRef(model="customers", name="revenue_sum"), so the
        # leaf alone (``col_name``) never matches the declared canonical
        # ``customers.revenue_sum``. Try the full dotted form too, then
        # fall back to binding the preserved colon/path ``raw_formula``
        # so the order key interns onto the same cross-model aggregate
        # slot (P2/P4) rather than raising.
        if _order_host_local and col_name in declared_alias_to_bound:
            bo = declared_alias_to_bound[col_name]
        elif full_name in declared_alias_to_bound:
            bo = declared_alias_to_bound[full_name]
        elif _flatten_dotted(full_name) in declared_alias_to_bound:
            # A joined dimension / time dimension is declared under its
            # flattened ``__`` form (``stores.opened_at`` →
            # ``stores__opened_at``; DEV-1449 / C4). An ORDER BY entry
            # written in dotted form must intern onto that same declared
            # slot rather than binding the raw column as a fresh slot.
            bo = declared_alias_to_bound[_flatten_dotted(full_name)]
        elif _order_host_local and f"_{col_name}" in declared_alias_to_bound:
            # ``*:count`` surfaces as the alias ``_count`` (the ``*`` is
            # dropped, the leading ``_`` kept as a marker); users naturally
            # order by the bare ``count``. Mirror the legacy
            # ``_resolve_order_column`` ``_name`` fallback.
            bo = declared_alias_to_bound[f"_{col_name}"]
        elif o.raw_formula:
            bo = bind_expr(
                parsed=parse_expr(o.raw_formula, allow_dunder=flat_scope),
                scope=scope,
                bundle=bundle,
            )
        else:
            # Bind the FULL reference (``customers.region``), not just the
            # leaf — otherwise a structured dotted ORDER ColumnRef without a
            # raw_formula rebinds as ``region`` and hits the wrong host
            # column or fails as ambiguous (CR).
            bo = bind_expr(
                parsed=parse_expr(full_name, allow_dunder=flat_scope),
                scope=scope,
                bundle=bundle,
            )
        order_specs.append(OrderSpec(bound=bo, direction=o.direction))

    # Stage 7b.10 — attach the active TD as ``time_key`` on every
    # time-needing TransformKey (cumsum / lag / lead / first / last /
    # time_shift / consecutive_periods / change / change_pct) whose
    # binder-output left ``time_key`` as ``None``. Closes the 7b.4
    # carry-over gap: ``_bind_transform`` does not have query / scope
    # context to resolve the TD, so the planner does it here after all
    # binding completes. Validation mirrors legacy
    # ``enrichment.py:564-569`` -- any time-needing transform with no
    # resolvable TD raises with the legacy phrase.
    active_td_key: Optional[TimeTruncKey] = None
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        active_td = _resolve_main_time_dimension(
            query=query, model=scope.source_model,
        )
        if active_td is not None:
            active_td_bound = bind_time_dimension(
                td=active_td, scope=scope, bundle=bundle,
            )
            atd_key = active_td_bound.value_key
            assert isinstance(atd_key, TimeTruncKey)
            active_td_key = atd_key

    if active_td_key is not None:
        declared_measures = [
            DeclaredMeasure(
                bound=BinderBoundExpr(
                    value_key=_attach_time_keys(
                        dm.bound.value_key, td_key=active_td_key,
                    ),
                ),
                declared_name=dm.declared_name,
                public_name=dm.public_name,
                label=dm.label,
                canonical_alias=dm.canonical_alias,
                type=dm.type,
                format=dm.format,
                description=dm.description,
            )
            for dm in declared_measures
        ]
        bound_filters = [
            BoundFilter(
                value_key=_attach_time_keys(
                    bf.value_key, td_key=active_td_key,
                ),
                phase=bf.phase,
                referenced_keys=tuple(
                    walk_value_keys(
                        _attach_time_keys(
                            bf.value_key, td_key=active_td_key,
                        ),
                    ),
                ),
            )
            for bf in bound_filters
        ]
        order_specs = [
            OrderSpec(
                bound=BinderBoundExpr(
                    value_key=_attach_time_keys(
                        spec.bound.value_key, td_key=active_td_key,
                    ),
                ),
                direction=spec.direction,
            )
            for spec in order_specs
        ]

    # Validation: any time-needing transform that still has
    # ``time_key=None`` after patching means there was no resolvable TD.
    for bucket in (
        [dm.bound.value_key for dm in declared_measures],
        [bf.value_key for bf in bound_filters],
        [spec.bound.value_key for spec in order_specs],
    ):
        for vk in bucket:
            op = _find_unresolved_time_needing_op(vk)
            if op is not None:
                raise ValueError(
                    f"Transform '{op}' requires an unambiguous time "
                    f"dimension. Add a single time_dimensions entry, or "
                    f"set main_time_dimension to select among multiple "
                    f"time dimensions."
                )

    # Sugar lowering for ``change`` / ``change_pct`` runs AFTER the
    # patching pass so the desugared ``time_shift`` inherits the patched
    # ``time_key`` (DEV-1446 identity preservation still holds — the
    # inner AggregateKey instance is not rebuilt by lowering).
    declared_measures = [
        DeclaredMeasure(
            bound=BinderBoundExpr(
                value_key=lower_sugar_transforms(dm.bound.value_key),
            ),
            declared_name=dm.declared_name,
            public_name=dm.public_name,
            label=dm.label,
            canonical_alias=dm.canonical_alias,
            type=dm.type,
            format=dm.format,
            description=dm.description,
        )
        for dm in declared_measures
    ]
    bound_filters = [
        BoundFilter(
            value_key=lower_sugar_transforms(bf.value_key),
            phase=bf.phase,
            referenced_keys=tuple(
                walk_value_keys(lower_sugar_transforms(bf.value_key)),
            ),
        )
        for bf in bound_filters
    ]
    order_specs = [
        OrderSpec(
            bound=BinderBoundExpr(
                value_key=lower_sugar_transforms(spec.bound.value_key),
            ),
            direction=spec.direction,
        )
        for spec in order_specs
    ]

    # DEV-1497: validate that every rank-family ``partition_by`` column resolves
    # to a query dimension / time-dimension, and rewrite a time-dimension source
    # column to its truncated-bucket ``TimeTruncKey`` (partition by the bucket,
    # not the raw timestamp — which would silently widen the grain). Runs BEFORE
    # interning so a rewritten key never leaves a stale slot behind (identity is
    # only touched on the rewritten rank transform).
    _dim_dms = declared_measures[:n_dims]
    _td_dms = declared_measures[n_dims:n_dims + n_tds]
    _dim_key_set = {dm.bound.value_key for dm in _dim_dms}
    # A source column carrying two time-dimension granularities (``created_at``
    # at both month and day) maps to two distinct ``TimeTruncKey`` buckets — a
    # bare ``partition_by=created_at`` is then ambiguous, so track those columns
    # and reject rather than silently pick whichever bucket comes last.
    _td_by_source: Dict[ValueKey, TimeTruncKey] = {}
    _td_ambiguous_sources: set = set()
    for dm in _td_dms:
        vk = dm.bound.value_key
        if not isinstance(vk, TimeTruncKey):
            continue
        # Ambiguous only when the SAME source column already mapped to a
        # DIFFERENT bucket (a different granularity) — two identical
        # ``created_at:month`` declarations resolve to one bucket, not a clash.
        if vk.column in _td_by_source and _td_by_source[vk.column] != vk:
            _td_ambiguous_sources.add(vk.column)
        _td_by_source[vk.column] = vk
    _td_key_set = set(_td_by_source.values())
    _available_dims = [dm.declared_name for dm in (*_dim_dms, *_td_dms)]

    def _validate_partition_keys(tk: TransformKey) -> frozenset:
        new_pks = []
        for pk in tk.partition_keys:
            if pk in _dim_key_set or pk in _td_key_set:
                new_pks.append(pk)          # already a query dim / td bucket
            elif pk in _td_ambiguous_sources:
                raise ValueError(
                    f"Transform '{tk.op}': partition_by column "
                    f"'{_partition_key_display(pk)}' is ambiguous — it is a "
                    f"time dimension at multiple granularities. Partition by a "
                    f"single query dimension instead."
                )
            elif pk in _td_by_source:
                new_pks.append(_td_by_source[pk])  # td source col -> bucket
            else:
                raise ValueError(
                    f"Transform '{tk.op}': partition_by column "
                    f"'{_partition_key_display(pk)}' is not a query dimension. "
                    f"Add it to dimensions/time_dimensions, or choose one of: "
                    f"{', '.join(_available_dims) or '(none)'}."
                )
        return frozenset(new_pks)

    def _rw(vk: ValueKey) -> ValueKey:
        return rewrite_rank_partition_keys(vk, rewrite_fn=_validate_partition_keys)

    declared_measures = [
        DeclaredMeasure(
            bound=BinderBoundExpr(value_key=_rw(dm.bound.value_key)),
            declared_name=dm.declared_name,
            public_name=dm.public_name,
            label=dm.label,
            canonical_alias=dm.canonical_alias,
            type=dm.type,
            format=dm.format,
            description=dm.description,
        )
        for dm in declared_measures
    ]
    _rewritten_filters = []
    for bf in bound_filters:
        bf_vk = _rw(bf.value_key)
        _rewritten_filters.append(BoundFilter(
            value_key=bf_vk,
            phase=bf.phase,
            referenced_keys=tuple(walk_value_keys(bf_vk)),
        ))
    bound_filters = _rewritten_filters
    order_specs = [
        OrderSpec(
            bound=BinderBoundExpr(value_key=_rw(spec.bound.value_key)),
            direction=spec.direction,
        )
        for spec in order_specs
    ]

    source_col_names = _source_column_names(scope)
    host_model_name = _host_model_name(scope)

    # DEV-1714 Stage 10 — windowed-measure guards on the ORIGINAL (pre-
    # projection) value-key trees. Raises on unsupported shapes (non-sum/avg,
    # no time dim, cross-model, transform, composite, hidden, mixed, malformed
    # duration); returns the set of cleanly-selected windowed AggregateKeys.
    selected_windowed = _guard_windowed_measures(
        measure_vks=[dm.bound.value_key for dm in declared_measures],
        filter_vks=[bf.value_key for bf in bound_filters],
        order_vks=[sp.bound.value_key for sp in order_specs],
        active_td_key=active_td_key,
    )

    projection = ProjectionPlanner().plan(
        measures=declared_measures,
        filters=bound_filters,
        order=order_specs,
        source_column_names=source_col_names,
        host_model_name=host_model_name,
    )

    row_slots, agg_slots, combined_slots = _bucket_slots(
        projection.registry.slots,
    )

    # DEV-1714 Stage 10 — build one WindowedAggregatePlan per selected windowed
    # measure. The window time dimension is the query's resolved active TD.
    active_td_slot_id = (
        projection.registry.find_by_key(active_td_key)
        if active_td_key is not None
        else None
    )
    windowed_plans, windowed_slot_ids = _build_windowed_plans(
        selected_windowed=selected_windowed,
        registry=projection.registry,
        row_slots=row_slots,
        active_td_key=active_td_key,
        active_td_slot_id=active_td_slot_id,
    )

    # DEV-1712 (Law 2): plan-time classification of every ORDER BY target that
    # is not a declared/public slot. Order-only AGGREGATES (local or
    # cross-model) always materialise and order — never rejected. The rest:
    #   * joined row column           -> UnresolvableOrderColumnError (the sort
    #     scope is relocated where the joined table is unbound);
    #   * local row column, grouped   -> ValueError (no valid SQL — the column
    #     isn't in GROUP BY; add it to dims, or order by an aggregate of it);
    #   * local row column, ungrouped -> allowed (split emission in the
    #     generator, ``_apply_order_limit_from_planned``);
    #   * transform / composite       -> ValueError (deferred to DEV-1733;
    #     declare it as a measure and order by that).
    _has_grouping = bool(agg_slots) or (
        bool(query.dimensions or query.time_dimensions)
        and query.distinct_dimension_values
    )
    for spec in order_specs:
        okey = spec.bound.value_key
        osid = projection.registry.find_by_key(okey)
        if osid is not None and not projection.registry.get(osid).hidden:
            continue  # declared / projected output — orders on a real column
        if isinstance(okey, AggregateKey):
            continue  # hidden aggregate (local base or cross-model CTE)
        if isinstance(okey, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
            disp = _partition_key_display(okey)
            path = _row_key_path(okey)
            if path:
                # ``UnresolvableOrderColumnError`` formats ``qualifier.column``;
                # pass the bare leaf as ``column`` and the joined path as the
                # qualifier so the message reads ``customers.regions.name`` and
                # not a duplicated ``customers.customers.regions.name``.
                raise UnresolvableOrderColumnError(
                    column=disp.rsplit(".", 1)[-1], qualifier=".".join(path),
                )
            if _has_grouping:
                raise ValueError(
                    f"ORDER BY column '{disp}' is a row column that this "
                    f"aggregated query does not project, so it is not in the "
                    f"GROUP BY. Add it to dimensions/time_dimensions, or order "
                    f"by an aggregate of it (e.g. '{disp}:max')."
                )
            continue  # ungrouped local row column -> split emission
        # TransformKey / ArithmeticKey / ScalarCallKey — an inline transform or
        # composite expression that is only referenced in ORDER BY.
        raise ValueError(
            "ORDER BY references a transform / composite expression that is "
            "not a declared measure. Declare it as a measure (optionally with "
            "a name) and order by that measure. (DEV-1733: materialising inline "
            "transform / composite ORDER BY targets is not yet supported.)"
        )

    # Build filters_by_phase in legacy WHERE order:
    #   1. date_range bound filters (bound_filters[:n_date_range])
    #   2. model.filters (text_filter_entries)
    #   3. user query bound filters (bound_filters[n_date_range:])
    # bound_filter_ids preserves the mapping back to bound_filters for
    # the cross-model routing pass that follows (text_filter_entries
    # are excluded — model filters never feed cross-model routing).
    # DEV-1714 Stage 10 — a filter referencing a windowed slot is reclassified
    # to Phase.POST: the windowed value is computed in the ``_wm_`` CTE and
    # joined back, so the predicate must apply on the combined SELECT (outer
    # WHERE), never as a HAVING on the plain base aggregate.
    def _windowed_phase(bf: BoundFilter) -> Phase:
        if windowed_slot_ids and (
            filter_referenced_slot_ids(bf, projection.registry) & windowed_slot_ids
        ):
            return Phase.POST
        return bf.phase

    filters_by_phase: List[FilterPhase] = []
    bound_filter_ids: List[str] = []
    for i, bf in enumerate(bound_filters[:n_date_range]):
        fid = f"f{i}"
        filters_by_phase.append(
            FilterPhase(
                id=fid, phase=_windowed_phase(bf), text=None,
                expression=PlannedBoundExpr(value_key=bf.value_key),
            ),
        )
        bound_filter_ids.append(fid)
    filters_by_phase.extend(text_filter_entries)
    for i, bf in enumerate(bound_filters[n_date_range:], start=n_date_range):
        fid = f"f{i}"
        filters_by_phase.append(
            FilterPhase(
                id=fid, phase=_windowed_phase(bf), text=None,
                expression=PlannedBoundExpr(value_key=bf.value_key),
            ),
        )
        bound_filter_ids.append(fid)
    # Stage 7b.5 — cross-model planner wiring. For every aggregate slot
    # whose source carries a non-empty join path (cross-model agg-ref
    # like ``customers.revenue:sum``), invoke the cross_model_planner
    # to produce a CrossModelAggregatePlan with explicit WHERE/HAVING/
    # target_model_filters routes. HostFilterRouting records carry the
    # post-projection slot ids each filter references (via
    # filter_referenced_slot_ids — Codex HIGH #3/#4 fold-in).
    # host_filter_routings only carries entries that have a typed
    # BoundFilter (date_range + user filters). Model.filters (text-only)
    # are always row-phase host-local WHERE and never need to be routed
    # to a cross-model CTE — they're skipped here.
    host_filter_routings: List[HostFilterRouting] = []
    for fid, bf, ftext in zip(
        bound_filter_ids, bound_filters, bound_filter_texts,
    ):
        host_filter_routings.append(HostFilterRouting(
            filter_id=fid,
            phase=bf.phase,
            referenced_slot_ids=sorted(filter_referenced_slot_ids(
                bf, projection.registry,
            )),
            text=ftext,
        ))

    cross_model_plans = []
    host_slots_for_classifier = projection.registry.slots
    for slot in agg_slots:
        # DEV-1714 Stage 10 — a windowed slot renders via its own ``_wm_`` CTE
        # (host-rooted range join), never a cross-model ``_cm_`` CTE, even when
        # its ``Column.filter`` crosses a join (which would otherwise trip the
        # host-rooted isolation trigger below).
        if slot.id in windowed_slot_ids:
            continue
        key = slot.key
        if not isinstance(key, AggregateKey):
            continue
        agg_path = getattr(key.source, "path", ())
        # DEV-1503 / DEV-1709 — Law-3 trigger predicate. Invoke the
        # cross-model planner when the aggregate's source carries a
        # non-empty join path (target-rooted, existing behaviour) OR when
        # ANY other input of a LOCAL aggregate crosses a join (host-rooted
        # isolation): ``Column.filter`` (typed ``referenced_join_paths``
        # from binder time — DEV-1503), source ``Column.sql``, positional
        # args incl. the explicit first/last time arg, kwargs (column
        # refs, user template fragments, and non-overridden model-default
        # ``AggregationParam`` fragments) — DEV-1709's widened trigger,
        # computed plan-time by ``compute_aggregate_input_join_paths``.
        has_crossing_filter = (
            key.column_filter_key is not None
            and bool(key.column_filter_key.referenced_join_paths)
        )
        has_crossing_input = (
            not disable_host_rooted_isolation
            and not agg_path
            and (
                has_crossing_filter
                or bool(compute_aggregate_input_join_paths(
                    key=key,
                    anchor_model=bundle.source_model,
                    anchor_relation=(
                        bundle.source_model.name
                        if bundle.source_model is not None else ""
                    ),
                    bundle=bundle,
                ))
            )
        )
        if not agg_path and not has_crossing_input:
            continue
        # DEV-1450 #2: re-rooting (C1) is owned by the strategy. We hand it
        # the host query, the public projection, and a sub-plan builder so it
        # can compile a nested re-rooted PlannedQuery when the host carries
        # dimensions / filters reachable only through the TARGET's join graph;
        # otherwise it returns the forward plan unchanged. The builder is the
        # same ``plan_query`` recursion the post-hoc pass used, injected here
        # so cross_model_planner.py needn't import stage_planner.
        #
        # DEV-1503 / DEV-1709 — the subplan_builder ALWAYS suppresses
        # host-rooted isolation: the host-rooted sub-plan contains the same
        # crossing measure and would otherwise recurse infinitely; for the
        # existing cross-model re-rooting case, the sub-plan's target-rooted
        # local aggregate would redundantly isolate its own crossing inputs
        # (already handled by the surrounding cross-model CTE). Inside the
        # sub-plan, crossing inputs render INLINE (base-pull) — legal there
        # because the CTE is the aggregate's own scope.
        reroot_enabled = (
            isinstance(scope, ModelScope) and scope.source_model is not None
        )
        plan = cross_model_planner.plan(
            aggregate_slot_id=slot.id,
            aggregate_key=key,
            bundle=bundle,
            host_slots=host_slots_for_classifier,
            host_filters=host_filter_routings,
            public_alias=slot.public_name,
            hidden=slot.hidden,
            host_query=query if reroot_enabled else None,
            public_projection=(
                projection.public_projection if reroot_enabled else None
            ),
            subplan_builder=(
                (lambda q, b: plan_query(
                    query=q, bundle=b, cross_model_planner=cross_model_planner,
                    disable_host_rooted_isolation=True,
                ))
                if reroot_enabled else None
            ),
        )
        cross_model_plans.append(plan)

    order_entries = []
    for spec in order_specs:
        sid = projection.registry.find_by_key(spec.bound.value_key)
        if sid is not None:
            order_entries.append(
                OrderEntry(slot_id=sid, direction=spec.direction),
            )

    transform_layers = _emit_transform_layers(slots=projection.registry.slots)
    stage_schema = _emit_stage_schema(
        query=query, projection=projection,
    )
    source_relation = (
        query.source_model
        if isinstance(query.source_model, str)
        else host_model_name
    )

    # Stage 7b.10 — the active TD's slot id (``active_td_slot_id``) is resolved
    # right after projection above so the windowed-plan builder can use it.

    # DEV-1732 — the frame-bound column set: raw columns of this stage's
    # NON-HIDDEN time dimensions. Computed once and carried on the plan so the
    # windowed ``_src`` path (below) and the generator's ``time_shift``
    # shifted-CTE path read the SAME set.
    frame_bound_columns = _frame_bound_columns(row_slots=row_slots)

    # DEV-1714 Stage 10 / DEV-1732 — the ``_wm_`` ``_src`` scope inherits
    # WHERE-phase row filters (model + user) MINUS their frame bounds: the
    # trailing window must reach rows before the visible frame starts.
    # POST-reclassified windowed-measure filters are already excluded
    # (phase != ROW).
    if windowed_plans:
        date_range_fids = {f"f{i}" for i in range(n_date_range)}
        src_where_ids, src_rewrites = _plan_src_row_filters(
            filters_by_phase=filters_by_phase,
            date_range_fids=date_range_fids,
            frame_bound_columns=frame_bound_columns,
        )
        for wp in windowed_plans:
            wp.where_filter_ids = src_where_ids
            wp.src_filter_rewrites = src_rewrites

    return PlannedQuery(
        source_relation=source_relation,
        row_slots=row_slots,
        aggregate_slots=agg_slots,
        cross_model_aggregate_plans=cross_model_plans,
        windowed_aggregate_plans=windowed_plans,
        combined_expression_slots=combined_slots,
        transform_layers=transform_layers,
        filters_by_phase=filters_by_phase,
        projection=projection.public_projection,
        order=order_entries,
        limit=query.limit,
        offset=query.offset,
        stage_schema=stage_schema,
        active_time_dimension_slot_id=active_td_slot_id,
        render_source_model=render_source_model,
        distinct_dimension_values=query.distinct_dimension_values,
        frame_bound_columns=frame_bound_columns,
    )


def _frame_bound_columns(*, row_slots: list) -> List[ValueKey]:
    """Raw column keys of the stage's NON-HIDDEN time dimensions (DEV-1732).

    An explicit relational bound on one of these is a FRAME bound — the
    caller restating what ``TimeDimension.date_range`` expresses — and is
    stripped from CTEs that must read outside the frame. A bound on any other
    column, temporal or not, is a population filter and is left alone.

    Hidden ``TimeTruncKey`` slots are excluded deliberately, and the exclusion
    is load-bearing rather than cosmetic: ``_build_windowed_plans`` skips hidden
    row slots when building ``other_time_dimension_slot_ids``, so a hidden time
    axis is never equality-joined into ``_src``. Stripping a bound on one would
    leave that axis wholly unconstrained — an unbounded over-count — where
    keeping it merely preserves the pre-DEV-1732 result.

    Order-stable and de-duplicated: the same column carried at two
    granularities contributes one entry.
    """
    out: List[ValueKey] = []
    seen: set = set()
    for rs in row_slots:
        if rs.hidden or not isinstance(rs.key, TimeTruncKey):
            continue
        col = rs.key.column
        if col in seen:
            continue
        seen.add(col)
        out.append(col)
    return out


def _plan_src_row_filters(
    *,
    filters_by_phase: list,
    date_range_fids: set,
    frame_bound_columns: List[ValueKey],
) -> "Tuple[List[str], List[SrcFilterRewrite]]":
    """Partition ROW-phase filters for a windowed measure's ``_src`` scope.

    Returns ``(where_filter_ids, src_filter_rewrites)``:

    * a filter that is ENTIRELY a frame bound is omitted from the ids;
    * a filter that is PARTLY one keeps its id and gains a rewrite carrying the
      residual population predicate;
    * everything else keeps its id with no rewrite.

    Mode-A model filters (``FilterPhase.text``, no typed expression) are exempt
    by design — a model filter defines which rows EXIST rather than which frame
    the query looks at, there is no ``date_range`` spelling at model level, and
    analysing raw dialect SQL would make a silent mis-strip possible.

    ``date_range_fids`` is skipped up front. That is redundant with
    ``strip_frame_bounds`` (which recognises ``BetweenKey`` too) and kept
    deliberately: it makes a Stage-10 regression structurally impossible even if
    a ``date_range`` ever binds to a shape the helper does not match.
    """
    time_cols = frozenset(frame_bound_columns)
    where_ids: List[str] = []
    rewrites: List[SrcFilterRewrite] = []
    for fp in filters_by_phase:
        if fp.phase != Phase.ROW or fp.id in date_range_fids:
            continue
        if fp.expression is None:
            where_ids.append(fp.id)  # Mode-A model filter — exempt.
            continue
        residual = strip_frame_bounds(
            key=fp.expression.value_key, time_columns=time_cols,
        )
        if residual is None:
            continue  # wholly a frame bound
        where_ids.append(fp.id)
        if residual is not fp.expression.value_key:
            rewrites.append(SrcFilterRewrite(
                filter_id=fp.id, expression=PlannedBoundExpr(value_key=residual),
            ))
    return where_ids, rewrites


def _coerce_extension(spec) -> ModelExtension:
    """Coerce a ``ModelExtension`` / dict-with-``source_name`` to a typed
    ``ModelExtension`` (for overlaying onto a synthetic sibling model)."""
    if isinstance(spec, ModelExtension):
        return spec
    return ModelExtension.model_validate(spec)


def _stage_scope_and_bundle(
    *,
    query: SlayerQuery,
    bundle: ResolvedSourceBundle,
    stage_schemas: Dict[str, StageSchema],
    data_source: str,
    is_root: bool,
) -> "Tuple[Union[ModelScope, StageSchema], ResolvedSourceBundle]":
    """Resolve one DAG stage's ``(scope, per-stage bundle)``.

    Each stage binds against its OWN source — not the root's — so a
    heterogeneous DAG (stage A over ``orders``, stage B over ``customers``)
    resolves each host correctly. Synthetic models for already-planned sibling
    stages are threaded into the per-stage bundle so a join / cross-model ref
    that targets a sibling resolves against the sibling's flat output columns.
    """
    src = query.source_model
    sibling_names = set(stage_schemas)
    sib = _source_name_if_sibling(src, sibling_names)

    # 1. ``ModelExtension`` / dict OVER a sibling stage: overlay the extra
    #    columns / measures / joins onto a synthetic model of the sibling CTE
    #    and bind ModelScope-style (so derived overlay columns resolve).
    if sib is not None and not isinstance(src, str):
        base = synthetic_model_from_stage_schema(
            name=sib, schema=stage_schemas[sib], data_source=data_source,
        )
        overlaid = _apply_extension_overlay(base, _coerce_extension(src))
        others = {n: s for n, s in stage_schemas.items() if n != sib}
        sb = stage_bundle_with_siblings(
            bundle=bundle, source_model=overlaid,
            sibling_schemas=others, data_source=data_source,
        )
        return ModelScope(source_model=overlaid), sb

    # 2. Bare-string sibling source (chain): bind against the upstream flat
    #    StageSchema (P6 / DEV-1449). The synthetic upstream model is the
    #    per-stage host for any cross-model planning / generation consistency.
    if isinstance(src, str) and src in stage_schemas:
        synth = synthetic_model_from_stage_schema(
            name=src, schema=stage_schemas[src], data_source=data_source,
        )
        others = {n: s for n, s in stage_schemas.items() if n != src}
        sb = stage_bundle_with_siblings(
            bundle=bundle, source_model=synth,
            sibling_schemas=others, data_source=data_source,
        )
        return stage_schemas[src], sb

    # 3. Model-scoped: the stage's own resolved source model. The root uses the
    #    bundle's source_model (the chain bottoms out at the root's source);
    #    a named sibling uses its pre-resolved per-stage model.
    if is_root:
        stage_model = bundle.source_model
    else:
        stage_model = bundle.stage_source_models.get(query.name) or bundle.source_model
    sb = stage_bundle_with_siblings(
        bundle=bundle, source_model=stage_model,
        sibling_schemas=stage_schemas, data_source=data_source,
    )
    return ModelScope(source_model=stage_model), sb


def plan_stages(
    *,
    queries: List[SlayerQuery],
    bundle: ResolvedSourceBundle,
    cross_model_planner: Optional[CrossModelPlanner] = None,
) -> List[PlannedQuery]:
    """Plan a multi-stage DAG. Topo sort, then plan each stage against its own
    resolved source + the synthetic models of its already-planned siblings."""
    if len(queries) == 1:
        return [plan_query(
            query=queries[0],
            bundle=bundle,
            cross_model_planner=cross_model_planner,
        )]
    ordered = _topo_sort(queries)
    root = ordered[-1]
    data_source = (
        (bundle.source_model.data_source if bundle.source_model else None)
        or "_stage"
    )
    stage_schemas: Dict[str, StageSchema] = {}
    results: List[PlannedQuery] = []
    for q in ordered:
        scope, stage_bundle = _stage_scope_and_bundle(
            query=q,
            bundle=bundle,
            stage_schemas=stage_schemas,
            data_source=data_source,
            is_root=q is root,
        )
        planned = plan_query(
            query=q,
            bundle=stage_bundle,
            scope=scope,
            cross_model_planner=cross_model_planner,
            stage_schemas=stage_schemas,
        )
        results.append(planned)
        if q.name and planned.stage_schema is not None:
            stage_schemas[q.name] = planned.stage_schema
    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _format_description_for_dimension(
    *, scope: Union[ModelScope, StageSchema], full_name: str,
) -> Tuple[Optional[NumberFormat], Optional[str]]:
    """Lift ``format`` / ``description`` for a plain dimension off the
    source ``Column``. Returns ``(None, None)`` when the ref can't be
    resolved (joined / time-truncated / stage-scoped refs) — those
    paths surface their metadata through ``response_meta`` instead.

    DEV-1452 Stage B decision #8 — the planner threads these into the
    public slot so the migrated query-backed virtual model carries the
    same display contract the legacy enrichment pipeline did.
    """
    if not isinstance(scope, ModelScope) or scope.source_model is None:
        return None, None
    if "." in full_name:
        return None, None
    col = scope.source_model.get_column(full_name)
    if col is None:
        return None, None
    return col.format, col.description


_COUNT_AGGREGATIONS: FrozenSet[str] = frozenset(
    {"count", "count_distinct", "count_distinct_approx"}
)
_FLOAT_AGGREGATIONS: FrozenSet[str] = frozenset({
    "avg", "weighted_avg", "median",
    "stddev_samp", "stddev_pop", "var_samp", "var_pop",
    "corr", "covar_samp", "covar_pop", "percentile",
})


def _infer_aggregated_type(
    *,
    model: SlayerModel,
    measure_name: Optional[str],
    aggregation: str,
) -> Optional[DataType]:
    """Type for an aggregated measure slot. Mirrors
    ``_infer_aggregated_format`` (decision #2 of the Stage B plan):

    * ``*:count`` (measure_name=``"*"``) → ``INT``
    * ``count`` / ``count_distinct`` / ``count_distinct_approx`` → ``INT``
    * ``avg`` / ``weighted_avg`` / ``median`` / parametric / stat aggs →
      ``DOUBLE``
    * ``sum`` / ``min`` / ``max`` / ``first`` / ``last`` → inherit from
      source column type (DOUBLE if absent).
    """
    if measure_name == "*":
        return DataType.INT
    if aggregation in _COUNT_AGGREGATIONS:
        return DataType.INT
    if aggregation in _FLOAT_AGGREGATIONS:
        return DataType.DOUBLE
    # sum / min / max / first / last — preserve source column type.
    if measure_name is None:
        return None
    col = model.get_column(measure_name)
    if col is not None and col.type is not None:
        return col.type
    return None


def _format_description_for_measure_formula(
    *, scope: Union[ModelScope, StageSchema], bound,
) -> Tuple[Optional[NumberFormat], Optional[str]]:
    """Lift ``format`` / ``description`` for a measure formula. The
    aggregation-aware format comes from ``_infer_aggregated_format`` when
    the bound expression is a bare local aggregate; description follows
    the source ``Column`` (sum / min / max preserve documentation).
    """
    if not isinstance(scope, ModelScope) or scope.source_model is None:
        return None, None
    if not isinstance(bound.value_key, AggregateKey):
        return None, None
    src = bound.value_key.source
    if isinstance(src, StarKey):
        # ``*:count`` — INTEGER format inferred by helper; no description.
        return (
            _infer_aggregated_format(
                model=scope.source_model,
                measure_name="*",
                aggregation=bound.value_key.agg,
            ),
            None,
        )
    if not isinstance(src, (ColumnKey, ColumnSqlKey)):
        return None, None
    if getattr(src, "path", ()):  # cross-model — handled by response_meta
        return None, None
    bare = getattr(src, "leaf", None) or getattr(src, "column_name", None)
    if bare is None:
        return None, None
    fmt = _infer_aggregated_format(
        model=scope.source_model,
        measure_name=bare,
        aggregation=bound.value_key.agg,
    )
    col = scope.source_model.get_column(bare)
    desc = col.description if col is not None else None
    return fmt, desc


def _type_for_measure_formula(
    *, scope: Union[ModelScope, StageSchema], bound,
) -> Optional[DataType]:
    """Lift ``type`` for a measure-formula slot.

    Mirrors ``_format_description_for_measure_formula`` — sources the
    type from ``_infer_aggregated_type`` for local aggregates so the
    migrated query-backed virtual model carries ``*:count → INT``,
    ``avg → DOUBLE``, ``sum → source column type``. Declared
    ``ModelMeasure.type`` overrides — that flow runs through
    ``expand_model_measures`` so the bound key already carries the
    declared type via the source column lookup.
    """
    if not isinstance(scope, ModelScope) or scope.source_model is None:
        return None
    if not isinstance(bound.value_key, AggregateKey):
        return None
    src = bound.value_key.source
    if isinstance(src, StarKey):
        return _infer_aggregated_type(
            model=scope.source_model,
            measure_name="*",
            aggregation=bound.value_key.agg,
        )
    if not isinstance(src, (ColumnKey, ColumnSqlKey)):
        return None
    if getattr(src, "path", ()):  # cross-model — handled elsewhere
        return None
    bare = getattr(src, "leaf", None) or getattr(src, "column_name", None)
    if bare is None:
        return None
    return _infer_aggregated_type(
        model=scope.source_model,
        measure_name=bare,
        aggregation=bound.value_key.agg,
    )


def _joined_column_type(
    *, source_model: SlayerModel, full_name: str, bundle: ResolvedSourceBundle,
) -> Optional[DataType]:
    """Best-effort type of a dotted (joined) dimension by walking the join
    chain — mirrors ``binding._resolve_dotted`` (``parts[:-1]`` are join
    hops matched on ``target_model``, ``parts[-1]`` is the leaf column),
    but returns ``None`` on any miss instead of raising. The binder has
    already validated the ref, so this is a guard rather than a primary
    check.
    """
    parts = full_name.split(".")
    if parts and parts[0] == source_model.name:  # C14 self-prefix strip
        parts = parts[1:]
    if not parts:
        return None
    *hops, leaf = parts
    current = source_model
    visited = {current.name}
    for hop in hops:
        if not any(j.target_model == hop for j in current.joins):
            return None
        nxt = bundle.get_referenced_model(hop)
        if nxt is None or nxt.name in visited:
            return None
        visited.add(nxt.name)
        current = nxt
    col = current.get_column(leaf)
    return col.type if col is not None else None


def _type_for_dimension(
    *,
    scope: Union[ModelScope, StageSchema],
    full_name: str,
    bundle: ResolvedSourceBundle,
) -> Optional[DataType]:
    """Lift ``type`` for a dimension. Local refs read the source column;
    joined (dotted) refs walk the join chain to the terminal column's
    type. Returning ``None`` for joined refs (the old behaviour) made
    ``_query_as_model`` coerce them to ``DOUBLE``, mistyping joined
    string / temporal dimensions on the persisted virtual model.
    """
    if not isinstance(scope, ModelScope) or scope.source_model is None:
        return None
    if "." in full_name:
        return _joined_column_type(
            source_model=scope.source_model, full_name=full_name, bundle=bundle,
        )
    col = scope.source_model.get_column(full_name)
    return col.type if col is not None else None


def _opaque_dim_type(
    *,
    scope: Union[ModelScope, StageSchema],
    full_name: str,
    bundle: ResolvedSourceBundle,
) -> Optional[DataType]:
    """Declared type of a query dimension, for the opaque-grouping guard only.

    Resolves BOTH a ``ModelScope`` origin (via ``_type_for_dimension``) AND a
    downstream ``StageSchema`` (via its typed ``columns``). ``_type_for_dimension``
    deliberately returns ``None`` for a StageSchema — that ``None`` is load-bearing
    for downstream typing (``_query_as_model`` coercion) and must not change — so
    the guard needs its own resolver to catch an opaque column projected in one
    stage and grouped in the next.
    """
    if isinstance(scope, StageSchema):
        col = scope.get(full_name)
        return col.type if col is not None else None
    return _type_for_dimension(scope=scope, full_name=full_name, bundle=bundle)


def _reject_opaque_grouping_dim(
    *,
    query: SlayerQuery,
    scope: Union[ModelScope, StageSchema],
    full_name: str,
    bundle: ResolvedSourceBundle,
) -> None:
    """Raise if ``full_name`` is an opaque dimension this query will GROUP BY.

    Grouping by an opaque column (``DataType.UNKNOWN`` — e.g. a PostGIS ``point``
    or any type with no equality operator) emits SQL the database rejects, so we
    fail with an actionable message instead of a raw driver error. Only an
    *actually grouped* dimension is refused: aggregating queries and dim-only
    DISTINCT queries group, but raw-row mode (``distinct_dimension_values=False``
    with no measures, DEV-1543) projects dimensions without a top-level GROUP BY,
    so an opaque column is legal there — and a downstream stage that groups such a
    projected value is still caught via the StageSchema (see ``_opaque_dim_type``).
    Checked on the declared type *before* ``bind_expr`` expands the column's
    ``sql``, so an opaque *derived* column is caught by its type rather than
    tripping the DEV-1410 cycle check first. (PR #259 "Unknown type" main-parity:
    the legacy guard lived in ``enrichment._resolve_dimensions``, which the typed
    pipeline bypasses.)
    """
    if not (bool(query.measures) or query.distinct_dimension_values):
        return
    dim_type = _opaque_dim_type(scope=scope, full_name=full_name, bundle=bundle)
    if dim_type is not None and dim_type.is_opaque:
        raise ValueError(
            f"Column '{full_name}' cannot be used as a dimension: its type does "
            f"not support the GROUP BY / DISTINCT this query requires. Define a "
            f"derived column that extracts a comparable value instead, e.g. "
            f"sql=\"payload->>'status'\" with type TEXT."
        )


def _saved_model_measure_type(
    *, scope: Union[ModelScope, StageSchema], formula: str,
) -> Optional[DataType]:
    """Lift the explicit ``type`` from a saved ``ModelMeasure`` when the
    query formula is a bare reference to one.

    ``expand_model_measures`` rewrites ``adjusted_total`` to the saved
    measure's underlying formula AST but doesn't surface the measure's
    explicit ``type=`` to downstream consumers — so an explicit
    ``ModelMeasure(formula="amount:sum * 1.0", type=DataType.DOUBLE)``
    on a reusable named measure would otherwise be lost unless
    ``_type_for_measure_formula`` happens to infer the same value. This
    helper rescues it by re-looking-up the saved measure here.

    Only fires when the formula text is itself a bare identifier
    matching a ``ModelMeasure.name`` on the source model; arithmetic /
    function-call / colon-suffix formulas always fall through to
    inference (the saved-measure type only applies when the user
    references the saved measure by name directly).
    """
    if not isinstance(scope, ModelScope) or scope.source_model is None:
        return None
    bare = formula.strip()
    if not bare.isidentifier():
        return None
    saved = scope.source_model.get_measure(bare)
    return saved.type if saved is not None else None


def _bare_saved_measure_name(
    *, scope: Union[ModelScope, StageSchema], formula: str,
) -> Optional[str]:
    """The saved ``ModelMeasure.name`` when the query formula is a BARE
    reference to one (DEV-1713 / DEV-1495 bare-named-measure aliasing).

    ``expand_model_measures`` rewrites ``rev_total`` to the saved measure's
    underlying formula AST, so without this the measure would surface under
    the formula-derived canonical (``revenue_sum``) instead of the name the
    user referenced (``rev_total``). Fires ONLY for a bare identifier matching
    a ``ModelMeasure.name`` on the source model — the same gate as
    :func:`_saved_model_measure_type`; qualified / arithmetic / colon-suffix
    formulas fall through (name-preservation is scoped to the bare form).
    """
    if not isinstance(scope, ModelScope) or scope.source_model is None:
        return None
    bare = formula.strip()
    if not bare.isidentifier():
        return None
    saved = scope.source_model.get_measure(bare)
    return saved.name if saved is not None else None


def _declared_measures_from_query(
    *,
    query: SlayerQuery,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> List[DeclaredMeasure]:
    # Downstream stages bind against a flat StageSchema whose columns ARE
    # the ``__``-flattened multi-hop aliases of the upstream stage, so
    # ``__`` is legal in their refs (P5 / DEV-1449). Model-scoped stages
    # keep the P1 rejection.
    flat_scope = isinstance(scope, StageSchema)
    declared: List[DeclaredMeasure] = []
    for d in (query.dimensions or []):
        full = d.full_name
        _reject_opaque_grouping_dim(
            query=query, scope=scope, full_name=full, bundle=bundle,
        )
        bound = bind_expr(
            parsed=parse_expr(full, allow_dunder=flat_scope),
            scope=scope,
            bundle=bundle,
        )
        flat_name = _flatten_dotted(full)
        fmt, desc = _format_description_for_dimension(
            scope=scope, full_name=full,
        )
        dim_type = _type_for_dimension(
            scope=scope, full_name=full, bundle=bundle,
        )
        declared.append(DeclaredMeasure(
            bound=bound,
            declared_name=flat_name,
            public_name=flat_name,
            label=d.label,
            type=dim_type,
            format=fmt,
            description=desc,
        ))
    # Time dimensions follow dimensions in the public projection — matches
    # the legacy ``user_projection`` order (dims, then time dims, then
    # measures).
    for td in (query.time_dimensions or []):
        full = td.dimension.full_name
        bound = bind_time_dimension(td=td, scope=scope, bundle=bundle)
        flat_name = _flatten_dotted(full)
        declared.append(DeclaredMeasure(
            bound=bound,
            declared_name=flat_name,
            public_name=flat_name,
            label=td.label,
            type=DataType.TIMESTAMP,
        ))
    for m in (query.measures or []):
        formula = m.formula
        explicit_name = m.name
        parsed = parse_expr(formula, allow_dunder=flat_scope)
        # DEV-1450 stage 7b.8 — pre-bind ModelMeasure expansion. A bare
        # ``Ref`` whose name matches a saved ``ModelMeasure`` on the
        # host model is rewritten to the measure's formula AST so the
        # binder resolves the underlying columns. Only applies against
        # ModelScope (downstream stages bind against StageSchema and
        # don't expose saved measures).
        if isinstance(scope, ModelScope) and scope.source_model is not None:
            parsed = expand_model_measures(
                expr=parsed,
                model=scope.source_model,
            )
        bound = bind_expr(parsed=parsed, scope=scope, bundle=bundle)
        # Stage 7b.10: sugar-lowering of ``change`` / ``change_pct`` now
        # runs in ``plan_query`` AFTER time-key patching, so the inner
        # ``time_shift`` inherits a patched ``time_key`` instead of
        # ``None``. Identity-preservation for the inner aggregate slot
        # (DEV-1446) still holds — ``lower_sugar_transforms`` keeps the
        # inner ``AggregateKey`` instance unchanged.
        canonical = _canonical_alias_for_formula(formula, bound=bound)
        # DEV-1713: a bare reference to a saved ModelMeasure surfaces under the
        # measure NAME, not the formula-derived canonical. Explicit query
        # ``name`` still wins; the saved name is an implicit ``name``.
        saved_name = _bare_saved_measure_name(scope=scope, formula=formula)
        alias_name = explicit_name or saved_name
        declared_name = alias_name or canonical
        public_name = alias_name or canonical
        fmt, desc = _format_description_for_measure_formula(
            scope=scope, bound=bound,
        )
        # Codex: type-priority chain (highest wins):
        #   1. ``m.type`` — user-supplied override on the query measure.
        #   2. Saved ``ModelMeasure.type`` — when the query formula is a
        #      bare reference to a reusable saved measure on the source
        #      model, that measure's explicit type wins over inference.
        #      ``expand_model_measures`` rewrites the AST but drops the
        #      source measure's type metadata; re-look-up here.
        #   3. ``_type_for_measure_formula`` — aggregation-aware inference.
        # Mirrors how the legacy ``EnrichedMeasure.type`` honored an
        # explicit type before falling back to inference.
        m_type = (
            m.type
            or _saved_model_measure_type(scope=scope, formula=formula)
            or _type_for_measure_formula(scope=scope, bound=bound)
        )
        declared.append(DeclaredMeasure(
            bound=bound,
            declared_name=declared_name,
            public_name=public_name,
            label=m.label,
            # DEV-1443: keep the canonical alias whenever the surfaced name
            # differs from it (explicit ``name`` OR an implicit saved-measure
            # name) so a colon-form filter / ORDER BY still resolves.
            canonical_alias=canonical if alias_name else None,
            type=m_type,
            format=fmt,
            description=desc,
        ))
    return declared


def _topo_sort(queries: List[SlayerQuery]) -> List[SlayerQuery]:
    """Kahn's algorithm: order stages so each appears after its
    siblings it references via ``source_model``.

    Raises ``ValueError`` on:
    * duplicate stage names,
    * a cycle in the dependency graph.

    Stages without a ``name`` (typically the final / root) are appended
    last in input order.
    """
    if len(queries) <= 1:
        return list(queries)
    named = [q for q in queries if q.name]
    names = [q.name for q in named]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ValueError(
            f"Duplicate stage names in source_queries DAG: {duplicates}"
        )
    by_name = {q.name: q for q in named}
    in_degree = {q.name: 0 for q in named}
    edges: Dict[str, List[str]] = {q.name: [] for q in named}
    for q in named:
        # A stage depends on a sibling when its ``source_model`` reads from it —
        # either the bare-string form OR a ``ModelExtension`` / dict over the
        # sibling. Capturing both keeps the topo order + cycle detection correct
        # for extension-over-sibling stages (not just join-target deps, which
        # the engine's runtime list sorter handles upstream).
        dep = _source_name_if_sibling(q.source_model, by_name)
        if dep is not None and dep != q.name:
            in_degree[q.name] += 1
            edges[dep].append(q.name)
    sorted_names: List[str] = []
    queue = [n for n, d in in_degree.items() if d == 0]
    while queue:
        n = queue.pop(0)
        sorted_names.append(n)
        for dep in edges[n]:
            in_degree[dep] -= 1
            if in_degree[dep] == 0:
                queue.append(dep)
    if len(sorted_names) != len(in_degree):
        remaining = sorted(set(in_degree) - set(sorted_names))
        raise ValueError(
            f"Cycle detected in source_queries DAG involving stages: "
            f"{remaining}"
        )
    sorted_named = [by_name[n] for n in sorted_names]
    unnamed = [q for q in queries if q.name is None]
    return sorted_named + unnamed


def _flatten_dotted(name: str) -> str:
    # DEV-1713: the ``__``-flatten is owned by the naming module.
    return flat_name(name)


def _canonical_alias_for_formula(
    formula: str,
    *,
    bound: Optional[BinderBoundExpr] = None,
) -> str:
    """Compute the canonical public alias for a measure formula.

    Mirrors ``canonical_agg_name`` for any formula whose bound root is
    an ``AggregateKey`` (covers bare ``revenue:sum`` AND parametric
    forms like ``revenue:percentile(p=0.5)`` / ``corr(other=quantity)``).
    Pre-binding text-shape recognition is used only as a fallback when
    no bound expression is supplied. For arbitrary formulas
    (transforms, arithmetic), sanitise the formula text so the alias
    remains a valid identifier.

    DEV-1450 stage 7b.13: parametric aggregations route through
    ``canonical_agg_name`` so kwargs are sanitised consistently with the
    legacy enrichment path (``p=0.5`` -> ``_p_0_5``). Without this, the
    naive text-replace fallback below leaks the ``=`` literally into the
    alias (``amount_percentile_p=0_5_``), breaking parity.
    """
    if bound is not None and isinstance(bound.value_key, AggregateKey):
        key = bound.value_key
        if isinstance(key.source, StarKey):
            measure_name: Optional[str] = "*"
            # Cross-model star (``customers.*:count``) carries its join
            # path so the canonical alias keeps the ``customers.`` prefix
            # (result key ``orders.customers._count``).
            path: Tuple[str, ...] = key.source.path
        else:
            # ColumnKey exposes ``.leaf``; ColumnSqlKey exposes
            # ``.column_name``. Both shapes can appear as aggregate
            # sources (the synth adapter rejects ``ColumnSqlKey`` with
            # a typed deferral; the planner still needs to derive an
            # alias before the generator runs). Mirror ``_canonical_name``
            # at ``planning.py:540-545``.
            measure_name = (
                getattr(key.source, "leaf", None)
                or getattr(key.source, "column_name", None)
            )
            path = getattr(key.source, "path", ())
        if measure_name is not None:
            prefix = ".".join(path) + "." if path else ""
            # Local aggregates retain the kwarg suffix to match legacy
            # ``enrichment.py:349`` (``percentile(p=0.5)`` ->
            # ``_p_0_5``). Cross-model aggregates ALSO retain it -- the
            # legacy ``query_engine.py:2160`` drops it, causing CTE alias
            # collision on two parametric variants, which the 7b.5 fix
            # corrected at the planner layer. Result-key shape for
            # cross-model parametric aggs therefore diverges from
            # legacy in this slice (no parity tests for that combination;
            # structural correctness over bit-identical legacy output).
            return prefix + canonical_agg_name(
                measure_name=measure_name,
                aggregation_name=key.agg,
                agg_args=[agg_kwarg_canonical_str(a) for a in key.args] or None,
                agg_kwargs={
                    k: agg_kwarg_canonical_str(v) for k, v in key.kwargs
                } or None,
            )
        # Fall through to text-based path -- AggregateKey source is
        # neither StarKey, ColumnKey, nor ColumnSqlKey (shouldn't be
        # reachable in practice; the binder restricts sources to
        # those three shapes).
    text = formula.strip()
    if ":" in text and "(" not in text:
        base, agg = text.rsplit(":", 1)
        return canonical_agg_name(
            measure_name=base, aggregation_name=agg,
        )
    return (
        text.replace(".", "_").replace(":", "_").replace(" ", "_")
            .replace("(", "_").replace(")", "_").replace(",", "_")
    )


def _source_column_names(
    scope: Union[ModelScope, StageSchema],
) -> FrozenSet[str]:
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        return frozenset(c.name for c in scope.source_model.columns)
    if isinstance(scope, StageSchema):
        return frozenset(c.name for c in scope.columns)
    return frozenset()


def _host_model_name(
    scope: Union[ModelScope, StageSchema],
) -> str:
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        return scope.source_model.name
    if isinstance(scope, StageSchema):
        return scope.relation_name
    return "(stage)"


def _bucket_slots(slots: List[ValueSlot]):
    row: List[ValueSlot] = []
    agg: List[ValueSlot] = []
    combined: List[ValueSlot] = []
    for s in slots:
        if s.phase == Phase.ROW:
            row.append(s)
        elif s.phase == Phase.AGGREGATE:
            agg.append(s)
        else:
            combined.append(s)
    return row, agg, combined


def _emit_stage_schema(
    *,
    query: SlayerQuery,
    projection,
) -> StageSchema:
    """Build the StageSchema from the projection plan.

    Only public slots appear (hidden slots are trimmed). One column per
    occurrence in ``public_projection`` so multi-alias declarations
    (same key with two ``name``s) emit one column per alias rather
    than two copies of ``public_aliases[0]``.
    """
    columns: List[StageColumn] = []
    alias_idx: Dict[str, int] = {}
    for sid in projection.public_projection:
        slot = projection.registry.get(sid)
        if slot.hidden:
            continue
        idx = alias_idx.setdefault(sid, 0)
        if idx < len(slot.public_aliases):
            alias = slot.public_aliases[idx]
        else:
            alias = slot.declared_name
        alias_idx[sid] = idx + 1
        # The downstream bind name + CTE column name are the ``__``-flattened
        # form so a later stage can reference a cross-model aggregate
        # (``customers.revenue_sum`` → ``customers__revenue_sum``), matching
        # how dimensions already flatten and how the legacy virtual-model
        # rename exposed these columns (P5/DEV-1449). ``public_alias`` keeps
        # the dotted result-key form. Dimensions / local / user-named
        # measures have no dot, so flattening is a no-op for them.
        flat = _flatten_dotted(alias)
        # Two distinct public columns that flatten to the same downstream
        # name (e.g. a joined ``customers.region`` and a literal model column
        # ``customers__region`` via the C11 carve-out) would make the stage's
        # CTE column ambiguous. Surface it instead of silently binding the
        # first match downstream.
        if any(c.name == flat for c in columns):
            raise ValueError(
                f"Stage column name collision on {flat!r}: two projected "
                f"columns flatten to the same downstream name. Give one an "
                f"explicit measure `name` to disambiguate."
            )
        columns.append(StageColumn(
            name=flat,
            sql_alias=flat,
            public_alias=alias,
            type=slot.type,
            label=slot.label,
            hidden=False,
            format=slot.format,
            description=slot.description,
        ))
    relation_name = query.name or "(unnamed_stage)"
    return StageSchema(relation_name=relation_name, columns=columns)


def _emit_transform_layers(*, slots: List[ValueSlot]) -> List[TransformLayer]:
    """One TransformLayer per ``TransformKey`` slot, emitted in
    dependency order (innermost transform first).

    Nested transforms (``cumsum(change(amount:sum))``) require
    per-slot layers so the generator can render the inner window /
    self-join before the outer one consumes it. Repeated ops at
    different nesting levels stay in separate layers; collapsing by
    op would lose the ordering invariant.

    Per-slot transform metadata (partition_keys, time_key, args,
    kwargs) lives on the slot's ``key`` (TransformKey); the generator
    slices read it from there.
    """
    transform_slots = [
        s for s in slots if isinstance(s.key, TransformKey)
    ]
    # Topological order: a slot whose TransformKey.input references
    # another slot's key must come AFTER that other slot. Walk
    # `_iter_slot_deps` to discover dependencies among transform slots.
    slot_by_key = {s.key: s for s in transform_slots}
    in_degree = {s.id: 0 for s in transform_slots}
    deps_of: Dict[str, List[str]] = {s.id: [] for s in transform_slots}
    for s in transform_slots:
        # The slot's transform depends on whatever transform slots
        # appear inside its ValueKey tree (e.g. cumsum(change(...))'s
        # cumsum slot depends on the change/time_shift slot).
        for dep in _iter_slot_deps(s.key):
            if dep is s.key or not isinstance(dep, TransformKey):
                continue
            dep_slot = slot_by_key.get(dep)
            if dep_slot is None:
                continue
            deps_of[dep_slot.id].append(s.id)
            in_degree[s.id] += 1
    # Kahn's algorithm: start from independent layers.
    ready = [s.id for s in transform_slots if in_degree[s.id] == 0]
    ordered_ids: List[str] = []
    while ready:
        nxt = ready.pop(0)
        ordered_ids.append(nxt)
        for child in deps_of[nxt]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                ready.append(child)
    # Fallback: any remaining slots (shouldn't happen with the typed
    # pipeline's identity-via-key, but guard) get appended in input order.
    seen = set(ordered_ids)
    for s in transform_slots:
        if s.id not in seen:
            ordered_ids.append(s.id)
    by_id = {s.id: s for s in transform_slots}
    return [
        TransformLayer(op=by_id[sid].key.op, slot_ids=[sid])
        for sid in ordered_ids
    ]


# ---------------------------------------------------------------------------
# Stage 7b.3c — date_range → filter + main-TD disambiguation
# ---------------------------------------------------------------------------


def _validate_model_filter(
    *,
    mf: str,
    idx: int,
    model: SlayerModel,
) -> FilterPhase:
    """Validate a ``SlayerModel.filters`` entry and emit a text-only
    ``FilterPhase`` for it.

    Replicates legacy validation (``slayer/engine/enrichment.py:1138-1219``):

    * ``parse_sql_predicate`` rejects DSL constructs (colon aggregation,
      transform calls) and raw ``OVER(...)`` window functions.
    * Reject references to a ``ModelMeasure`` declared on the same
      model — model filters are WHERE-clause SQL, can't reference
      aggregates (legacy ``enrichment.py:1147-1153``).
    * Reject references to a column whose ``Column.sql`` contains a
      window function (legacy ``enrichment.py:1205-1219``).
    * DEV-1450 follow-up #4b: references to a NON-windowed derived
      ``Column.sql`` column are now accepted — the generator inlines the
      column's expanded SQL at render time
      (``SQLGenerator._render_model_filter_sql``) and pulls any joins the
      expansion crosses into the FROM, matching legacy
      ``resolve_filter_columns``.
    """
    parsed = parse_sql_predicate(mf)
    measure_names = {m.name for m in (model.measures or [])}
    windowed_columns = {
        c.name for c in model.columns
        if c.sql and has_window_function(c.sql)
    }
    for col in parsed.columns:
        if col in measure_names:
            raise ValueError(
                f"Model filter {mf!r} references measure {col!r}. "
                f"Model filters can only reference table columns (WHERE). "
                f"Use query-level filters for measure conditions."
            )
        if col in windowed_columns:
            raise ValueError(
                f"Model filter {mf!r} references column {col!r} whose "
                f"SQL contains a window function. Factor it into a "
                f"multi-stage source_queries model or use a rank-family "
                f"transform at query time."
            )
    return FilterPhase(
        id=f"mf{idx}",
        phase=Phase.ROW,
        text=mf,
        text_columns=tuple(parsed.columns),
        expression=None,
    )


def _build_date_range_filter(
    *,
    td: TimeDimension,
    scope: ModelScope,
    bundle: ResolvedSourceBundle,
) -> BoundFilter:
    """Build a row-phase ``BoundFilter`` from a ``TimeDimension``'s
    ``date_range``.

    The predicate binds against the bare underlying ``ColumnKey``
    (not the ``TimeTruncKey``) so generator slice 7b.11 can apply the
    filter to the outer projection while the shifted self-join CTE
    reads raw data. Shape:

        BetweenKey(column=col, low=start, high=end)

    Inclusive on both sides — matches legacy ``column BETWEEN start
    AND end``. The typed BetweenKey lets the SQL generator emit
    ``exp.Between`` rather than ``col >= start AND col <= end``,
    closing the syntactic parity gap with the legacy generator
    (DEV-1450 stage 7b.9).

    Bound literals are normalised via ``normalize_scalar``; strings
    pass through unchanged.
    """
    full = td.dimension.full_name
    parsed = parse_expr(full)
    bound_col_expr = bind_expr(parsed=parsed, scope=scope, bundle=bundle)
    col_key = bound_col_expr.value_key
    # DEV-1450 #4a: a derived (Column.sql) temporal column binds to a
    # ColumnSqlKey; the BetweenKey accepts both kinds and the generator
    # renders a ColumnSqlKey by expanding (``<expanded sql> BETWEEN ...``).
    if not isinstance(col_key, (ColumnKey, ColumnSqlKey)):
        raise ValueError(
            f"date_range filter for TimeDimension {full!r} expected a "
            f"column reference; got {type(col_key).__name__}."
        )

    start, end = td.date_range[0], td.date_range[1]
    predicate = BetweenKey(
        column=col_key,
        low=LiteralKey(value=normalize_scalar(start)),
        high=LiteralKey(value=normalize_scalar(end)),
    )
    refs = tuple(walk_value_keys(predicate))
    phase = max((k.phase for k in refs), default=predicate.phase)
    return BoundFilter(
        value_key=predicate, phase=phase, referenced_keys=refs,
    )


def _resolve_main_time_dimension(
    *,
    query: SlayerQuery,
    model: SlayerModel,
) -> Optional[TimeDimension]:
    """Resolve the active time dimension for transform / windowing.

    * 0 TDs → ``None``.
    * 1 TD → that TD (``query.main_time_dimension`` is ignored —
      matches legacy semantics).
    * 2+ TDs:
      * ``query.main_time_dimension`` set → match by ``full_name``
        first, then by ``leaf``; raise ``UnknownReferenceError`` if
        neither matches.
      * Else ``model.default_time_dimension`` set → match by leaf;
        return ``None`` if it doesn't match a TD in this query
        (legacy graceful no-op — the default points at a column the
        user didn't include in this query's time_dimensions).
      * Else → ``None``.
    """
    tds = list(query.time_dimensions or [])
    if not tds:
        return None
    if len(tds) == 1:
        return tds[0]

    if query.main_time_dimension:
        target = query.main_time_dimension
        # Prefer full-name (more specific) over leaf match.
        for td in tds:
            if td.dimension.full_name == target:
                return td
        leaf_matches = [td for td in tds if td.dimension.name == target]
        if len(leaf_matches) == 1:
            return leaf_matches[0]
        if len(leaf_matches) > 1:
            # Ambiguous: multiple TDs share the same leaf (e.g.
            # ``customers.created_at`` and ``payments.created_at``).
            # Force the user to disambiguate via full_name.
            raise AmbiguousReferenceError(
                name=target,
                candidates=[td.dimension.full_name for td in leaf_matches],
            )
        raise UnknownReferenceError(
            name=target,
            scope_kind="TimeDimension",
            scope_summary=(
                f"time_dimensions: "
                f"{[td.dimension.full_name for td in tds]}"
            ),
            suggestion=None,
        )

    default = model.default_time_dimension
    if default:
        # Legacy ``_resolve_time_alias`` returns
        # ``f"{model.name}.{default_time_dimension}"``, which only points
        # at the host model — never at a joined TD. Preserve that: prefer
        # a host-local TD (``td.dimension.model is None``) over any
        # joined TD that happens to share the leaf name.
        for td in tds:
            if td.dimension.model is None and td.dimension.name == default:
                return td
    return None
