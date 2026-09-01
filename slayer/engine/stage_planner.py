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

from enum import Enum
from typing import (
    AbstractSet,
    Any,
    Callable,
    Dict,
    FrozenSet,
    Hashable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from pydantic import BaseModel

from slayer.core.enums import DataType
from slayer.core.formula import TIME_TRANSFORMS
from slayer.core.format import NumberFormat
from slayer.core.errors import (
    AmbiguousReferenceError,
    DistinctDimensionValuesError,
    UnknownReferenceError,
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
    column_leaf,
    normalize_scalar,
    reroot_value_key,
    substitute_value_keys,
)
from slayer.core.errors import UnreachableFilterDroppedWarning
from slayer.core.models import SlayerModel
from slayer.engine.aggregate_input_paths import compute_aggregate_input_join_paths
from slayer.engine.join_safety import (
    may_inline_crossing_inputs,
    provably_to_one,
    safe_reachable,
)
from slayer.core.query import (
    ORDER_PLACEHOLDER_NAMES,
    ComputedDimension,
    ModelExtension,
    SlayerQuery,
    TimeDimension,
)
from slayer.core.refs import canonical_agg_name
from slayer.sql.naming import canonical_aggregate_alias
from slayer.core.time_bounds import strip_frame_bounds
from slayer.core.window_duration import parse_window_duration
from slayer.core.scope import ModelScope, StageColumn, StageSchema
from slayer.engine.binding import (
    BoundExpr as BinderBoundExpr,
    BoundFilter,
    bind_expr,
    bind_filter,
    bind_time_dimension,
    walk_value_keys,
)
from slayer.engine.measure_expansion import expand_model_measures
from slayer.engine.filter_reachability import (
    compute_key_join_paths,
    key_has_host_local_ref,
)
from slayer.engine.planned import (
    BoundExpr as PlannedBoundExpr,
    BoundFilterId,
    EmptyBaseGrainPlan,
    FilterPhase,
    FilterReachability,
    OrderEntry,
    OrderScope,
    PlannedQuery,
    RankedProducerKernel,
    RegroupAttachPlan,
    RegroupSubstitution,
    SlotId,
    SrcFilterRewrite,
    TrailingWindowProducerKernel,
    TransformLayer,
    ValueSlot,
)
from slayer.engine.ranked_planner import (
    RANKED_AGGREGATIONS,
    ordered_row_keys,
    resolve_ranking_time_key,
)
from slayer.engine.planning import (
    DeclaredMeasure,
    OrderSpec,
    ProjectionPlanner,
    _canonical_name,
    _iter_slot_deps,
    filter_referenced_slot_ids,
    lower_sugar_transforms,
    rewrite_rank_partition_keys,
)
from slayer.engine.prebound import (
    PreboundQuery,
    StrictQueryCarrier,
    dimension_key_metadata,
    measure_key_format_description,
    measure_key_type,
    partition_declared_measures,
    walk_key_path,
)
from slayer.engine.regroup_planner import (
    REGROUP_LEAF_PREFIX,
    RegroupPlaceholderRegistry,
    classify_regroup_filter,
    combined_partitioned_aggregates,
    conjunct_scope,
    dimension_partitioned_aggregates,
    dimension_regroup_roots,
    is_local_combined_regroup_ref,
    regroup_root_grain,
    reserved_prefix_columns,
    split_top_level_and,
    substitute_in_bound_filter,
)
from slayer.engine.source_bundle import (
    ResolvedSourceBundle,
    _apply_extension_overlay,
    _source_name_if_sibling,
    stage_bundle_with_siblings,
    synthetic_model_from_stage_schema,
)
from slayer.engine.normalization import func_style_agg_to_colon
from slayer.engine.syntax import (
    AggCall,
    Ref,
    TransformCall,
    parse_expr,
    parse_filter_expr,
)
from slayer.sql.naming import flat_name
from slayer.sql.sql_expr import has_window_function
from slayer.sql.sql_predicate import parse_sql_predicate


__all__ = [
    "PreboundQuery",
    "StrictQueryCarrier",
    "bind_query_inputs",
    "plan_query",
    "plan_stages",
]


# Stage 7b.10 — transform ops that require a resolvable time dimension to render
# their OVER ``ORDER BY``: the canonical ``TIME_TRANSFORMS`` set (single-sourced
# from ``slayer/core/formula.py``).
_TIME_NEEDING_TRANSFORM_OPS = TIME_TRANSFORMS


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


def _guard_dimension_temporal_axis(declared_measures) -> None:
    """DEV-1839 D9 — a time-ordered transform inside a dimension expression must
    have its resolved time-ordering key inside its evaluation grain (the union of
    its inner aggregates' grains). Otherwise the producer accumulates per
    time-bucket rows and joins back on the coarser grain, DUPLICATING result rows
    (a live defect for the single-grain form). Fail closed, directing the author
    to include the time key in ``partition_by``. Runs after ``time_key`` is
    attached and partition keys are rewritten to their time buckets."""
    for dm in declared_measures:
        if not dm.is_dimension:
            continue
        for tk in walk_value_keys(dm.bound.value_key):
            if not isinstance(tk, TransformKey):
                continue
            if tk.op not in TIME_TRANSFORMS or tk.time_key is None:
                continue
            if tk.time_key not in regroup_root_grain(tk):
                axis = _partition_key_display(tk.time_key)
                raise NotImplementedError(
                    f"A time-ordered transform '{tk.op}' inside a computed "
                    f"dimension evaluates at a grain that does not contain its "
                    f"time axis '{axis}'; a producer bucketed by time joined back "
                    f"on the coarser grain would duplicate result rows. Include "
                    f"the time key in the aggregate's partition_by= so the "
                    f"transform accumulates within its own grain (DEV-1839)."
                )


# ---------------------------------------------------------------------------
# DEV-1714 Stage 10 — duration-windowed measures (``window='90d'``).
# ---------------------------------------------------------------------------


def _window_kwarg_of(key: ValueKey):
    """The ``window`` kwarg value of an ``AggregateKey``, or ``None``.

    ``window`` is a globally reserved aggregation kwarg name (legacy parity —
    the legacy enrichment pipeline popped it unconditionally before dispatch), so its
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
    # DEV-1836 — a windowed cross-model aggregate is a target-rooted windowed
    # producer (its re-rooted measure is local inside the producer), so the
    # former cross-model deferral (G3) is lifted.


def _guard_windowed_measures(
    *,
    measure_vks: list,
    filter_vks: list,
    order_vks: list,
    active_td_key,
) -> dict:
    """Validate the windowed-measure shapes that STILL fail closed and return the
    cleanly-SELECTED windowed ``AggregateKey``s (those answered by a
    trailing-window kernel producer, DEV-1838 D4) in measure-declaration
    order — so the emitted CTEs and combined-SELECT columns are DETERMINISTIC.

    DEV-1835 dissolved the transform / composite / filter coexistence guards
    (G4/G5/G6/G7): a bare windowed measure now desugars onto the regroup primitive
    (``_plan_regroups``) before this runs, so a local windowed reference is a
    placeholder here and never a windowed key. What remains is the per-key
    contract that survives the migration — sum/avg only (G1), a compact duration
    string (G8), cross-model deferred to stage 3 (G3), and a resolvable time
    dimension (G2) — raised here at top level and inside a windowed producer's own
    ``plan_query`` (where its lone windowed measure IS selected).
    """
    all_vks = [*measure_vks, *filter_vks, *order_vks]
    if not any(_windowed_agg_keys(vk) for vk in all_vks):
        return {}

    # G1 / G8 / G3 — per-key validation (sum/avg only, compact duration,
    # cross-model deferred).
    for vk in all_vks:
        for key in _windowed_agg_keys(vk):
            _reject_unsupported_windowed_key(key)

    # A declared windowed measure is selected; ``dict`` (not ``set``) preserves
    # measure order, the value being the slot's ``hidden`` flag (False here, True
    # for an order-only target below).
    selected_windowed: dict = {}
    for vk in measure_vks:
        if _window_kwarg_of(vk) is not None:
            selected_windowed.setdefault(vk, False)
    # DEV-1733 — order-only windowed targets. This IS a reachable shape (the
    # pre-DEV-1733 comment here claimed otherwise): ``OrderItem`` canonicalises
    # ``revenue:sum(window='90d')`` to the column name ``revenue_sum``, but
    # ``OrderItem.raw_formula`` preserves the original text and the planner
    # binds from it whenever the canonical name matches no declared measure. It
    # used to fall through with no windowed plan at all, materialising a
    # PLAIN ``SUM`` in the base and ordering by it — the window silently gone.
    #
    # A windowed key referenced ONLY by ORDER BY is registered here as a HIDDEN
    # plan (S-a top-level, S-b nested in a composite). Registering it after the
    # measure loop means an also-declared key keeps ``hidden=False``.
    for vk in order_vks:
        for key in _windowed_agg_keys(vk):
            selected_windowed.setdefault(key, True)

    # G2 — a windowed measure needs a resolvable time dimension.
    if active_td_key is None:
        raise ValueError(
            "Windowed measure could not resolve its time dimension. Add a single "
            "time_dimensions entry, or set main_time_dimension to select among "
            "multiple time dimensions."
        )
    return selected_windowed


def _partitioned_agg_keys(
    vk: ValueKey, *, exclude: AbstractSet[AggregateKey] = frozenset(),
) -> list:
    """Every ``AggregateKey`` in ``vk`` carrying an explicit ``partition_by``,
    minus any in ``exclude`` (DEV-1829: the computed-dimension row-attach
    aggregates, which the regroup desugar owns and must not be re-guarded)."""
    return [
        k for k in walk_value_keys(vk)
        if isinstance(k, AggregateKey)
        and k.partition_keys is not None
        and k not in exclude
    ]


def _guard_partitioned_measures(
    *, measure_vks: list, filter_vks: list, order_vks: list,
    exclude: AbstractSet[AggregateKey] = frozenset(),
) -> None:
    """Reject the partition_by shapes deferred to DEV-1824: combined with
    ``window=``, on ``first``/``last``, nested inside a transform, or referenced
    in a query filter. Runs on the ORIGINAL pre-substitution value-key trees
    (D5), ``exclude``-ing the computed-dimension aggregates the row regroup
    desugar legitimately consumes — so a genuine partitioned-measure filter still
    raises while a legitimate computed-dimension filter does not."""
    def _part(vk: ValueKey) -> list:
        return _partitioned_agg_keys(vk, exclude=exclude)

    def _cross_model(k: AggregateKey) -> bool:
        return bool(getattr(k.source, "path", ()))

    all_vks = [*measure_vks, *filter_vks, *order_vks]
    part_keys = [k for vk in all_vks for k in _part(vk)]
    if not part_keys:
        return
    # DEV-1824 (task 3.3) — a LOCAL window=+partition_by measure is lifted: its
    # producer is a windowed aggregate at the (partition ∪ active-TD) grain (D5).
    # DEV-1836 — a CROSS-MODEL window+partition source is now a target-rooted
    # windowed producer (same shape at the aggregate's root), so no longer
    # deferred here.
    # DEV-1824 (task 3.4) — a LOCAL first/last with partition_by is lifted: its
    # producer computes the ranked pick at the partition grain (hoisted CTE) and
    # attaches. A CROSS-MODEL source stays deferred (stage 3).
    if any(k.agg in ("first", "last") and _cross_model(k) for k in part_keys):
        raise NotImplementedError(
            "partition_by on a cross-model first/last aggregation is not yet "
            "supported (DEV-1824); the aggregate must be local to the query's "
            "source."
        )
    # DEV-1824 (task 3.5) — a LOCAL partitioned aggregate nested in a transform
    # is lifted: discovery finds it, it desugars into a combined regroup producer
    # (a plain grouped aggregate), and the transform runs at the query grain over
    # the attached value (D4). A CROSS-MODEL source stays deferred (stage 3) —
    # ``combined_partitioned_aggregates`` never desugars it, so it must fail
    # closed here rather than fall through to a wrong render.
    if any(
        isinstance(tk, TransformKey) and any(_cross_model(k) for k in _part(tk.input))
        for vk in all_vks for tk in walk_value_keys(vk)
    ):
        raise NotImplementedError(
            "A cross-model partition_by aggregate nested inside a transform is "
            "not yet supported (DEV-1824); the partitioned aggregate must be "
            "local to the query's source."
        )
    # DEV-1824 (task 3.6) — a LOCAL partitioned aggregate referenced in a query
    # filter is lifted: the router splits top-level conjuncts and routes each to
    # the earliest scope where its operands resolve (combined placeholders render
    # at the outer WHERE). A CROSS-MODEL source stays deferred (stage 3).
    if any(_cross_model(k) for vk in filter_vks for k in _part(vk)):
        raise NotImplementedError(
            "Filtering on a cross-model partition_by aggregate is not yet "
            "supported (DEV-1824); the aggregate must be local to the query's "
            "source."
        )


def _windowed_slot_id_set(
    *,
    selected_windowed: dict,
    registry,
    active_td_slot_id,
) -> set:
    """Slot ids of the cleanly-selected windowed measures — detection only:
    the emission lives on the attach kernel (DEV-1838 D4). The set still
    drives POST reclassification, classifier skips, and order scoping."""
    windowed_slot_ids: set = set()
    if not selected_windowed:
        return windowed_slot_ids

    # CR#3 / G2 (post-projection): the window time dimension must be a SELECTED
    # query time dimension (interned as a row slot so it becomes part of the
    # bucket grain). A model ``default_time_dimension`` the query does not select
    # resolves ``active_td_key`` (so the pre-projection G2 passes) but is never
    # interned — ``active_td_slot_id`` is then None.
    if active_td_slot_id is None:
        raise ValueError(
            "Windowed measure could not resolve its time dimension. Add a single "
            "time_dimensions entry, or set main_time_dimension to select among "
            "multiple time dimensions."
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
        windowed_slot_ids.add(sid)
    return windowed_slot_ids


_RAW_ROW_FIX_HINT = (
    "Either remove the measure reference, or set "
    "distinct_dimension_values=True (the default) to keep the "
    "auto-aggregating behaviour."
)


def _iter_expr_children(node):
    """Yield the child nodes of a parsed Mode-B expression node.

    Attribute-driven rather than type-driven so one walker covers every node
    shape the typed parser emits. ``str`` / ``bool`` scalars are skipped —
    they are leaf payloads (an operator name, a flag), never child nodes.
    """
    for attr in ("input", "left", "right", "this", "operand"):
        child = getattr(node, attr, None)
        if child is not None and not isinstance(child, (str, bool)):
            yield child
    for attr in ("args", "operands", "kwargs"):
        for item in getattr(node, attr, None) or ():
            # kwargs come through as (name, value) pairs; take the value.
            yield item[1] if isinstance(item, tuple) and len(item) == 2 else item


def _expr_has_measure_ref(node, *, measure_names: FrozenSet[str]) -> bool:
    """True if a parsed Mode-B expression references an aggregation, a
    transform, or a saved ``ModelMeasure`` by bare name.

    Structural walk over the typed parser's AST — the legacy check re-parsed
    the raw text with the enrichment parsers, which no longer exist.
    """
    if node is None:
        return False
    if isinstance(node, (AggCall, TransformCall)):
        return True
    if isinstance(node, Ref) and node.name in measure_names:
        return True
    return any(
        _expr_has_measure_ref(child, measure_names=measure_names)
        for child in _iter_expr_children(node)
    )


def _reject_measure_refs_for_raw_rows(*, query: SlayerQuery, scope) -> None:
    """DEV-1543: with ``distinct_dimension_values=False`` the caller asked for
    RAW ROWS, so no measure reference may appear in ``filters`` or ``order``.

    ``SlayerQuery``'s validator already rejects the model-free cases (a
    non-empty ``measures``, no dimensions at all). This is the half that needs
    the resolved model: a bare ``aov`` is only a measure reference if ``aov``
    is a saved ``ModelMeasure``. Unparseable text is left alone — the binder
    raises on it downstream with a message tied to the original string.
    """
    src = getattr(scope, "source_model", None)
    measure_names: FrozenSet[str] = frozenset(
        m.name for m in (getattr(src, "measures", None) or []) if m.name
    )
    custom_agg_names: FrozenSet[str] = frozenset(
        a.name for a in (getattr(src, "aggregations", None) or []) if a.name
    )
    _reject_measure_refs_in_filters(query=query, measure_names=measure_names)
    _reject_measure_refs_in_order(
        query=query,
        measure_names=measure_names,
        custom_agg_names=custom_agg_names,
        source_name=getattr(src, "name", None),
    )


def _reject_measure_refs_in_filters(
    *, query: SlayerQuery, measure_names: FrozenSet[str],
) -> None:
    """Filter half of :func:`_reject_measure_refs_for_raw_rows`."""
    for f in (query.filters or []):
        if not isinstance(f, str):
            continue
        try:
            parsed = parse_filter_expr(f)
        except Exception:  # noqa: BLE001 — binder reports parse errors properly
            continue
        if _expr_has_measure_ref(parsed, measure_names=measure_names):
            raise DistinctDimensionValuesError(
                f"distinct_dimension_values=False rejects measure references, "
                f"but filter {f!r} contains one. {_RAW_ROW_FIX_HINT}"
            )


def _parse_order_formula(raw: str, *, custom_agg_names: FrozenSet[str]):
    """Parse an ``OrderItem.raw_formula`` to a Mode-B AST, or ``None``.

    Function-style aggregations (``sum(amount)``) are not valid Mode-B; the
    slack layer rewrites them to colon form, so do the same here — otherwise
    this check would miss the aggregation and let the binder report a generic
    "function not allowed" instead.
    """
    try:
        text = func_style_agg_to_colon(raw, custom_agg_names=custom_agg_names)
    except Exception:  # noqa: BLE001 — fall back to the original text
        text = raw
    try:
        return parse_expr(text)
    except Exception:  # noqa: BLE001 — binder reports parse errors properly
        return None


def _reject_measure_refs_in_order(
    *,
    query: SlayerQuery,
    measure_names: FrozenSet[str],
    custom_agg_names: FrozenSet[str],
    source_name: Optional[str],
) -> None:
    """ORDER BY half of :func:`_reject_measure_refs_for_raw_rows`."""
    for item in (query.order or []):
        raw = getattr(item, "raw_formula", None)
        if raw:
            parsed = _parse_order_formula(raw, custom_agg_names=custom_agg_names)
            if parsed is not None and _expr_has_measure_ref(
                parsed, measure_names=measure_names,
            ):
                raise DistinctDimensionValuesError(
                    f"distinct_dimension_values=False rejects measure "
                    f"references, but order item {raw!r} contains one. "
                    f"{_RAW_ROW_FIX_HINT}"
                )
        name = getattr(getattr(item, "column", None), "name", None)
        if name and name in measure_names:
            raise DistinctDimensionValuesError(
                f"distinct_dimension_values=False rejects measure references, "
                f"but order item {name!r} resolves to a saved measure on "
                f"{source_name or 'the source model'!r}. "
                f"{_RAW_ROW_FIX_HINT}"
            )


def _resolve_scope(
    *,
    query: SlayerQuery,
    bundle: ResolvedSourceBundle,
    stage_schemas: Optional[Dict[str, StageSchema]],
) -> Union[ModelScope, StageSchema]:
    """Default binding scope: an upstream ``StageSchema`` when the query's
    ``source_model`` names a sibling stage, else a ``ModelScope`` over the
    bundle's host model."""
    source = query.source_model
    if isinstance(source, str) and source in (stage_schemas or {}):
        return (stage_schemas or {})[source]
    return ModelScope(source_model=bundle.source_model)


def _map_bound_keys(
    key_fn: Callable[[ValueKey], ValueKey],
    *,
    declared_measures: List[DeclaredMeasure],
    bound_filters: List[BoundFilter],
    order_specs: List[OrderSpec],
) -> Tuple[List[DeclaredMeasure], List[BoundFilter], List[OrderSpec]]:
    """Apply ``key_fn`` to every declared-measure / filter / order value_key,
    rebuilding each carrier with all other fields preserved and recomputing each
    filter's ``referenced_keys`` from its rewritten key."""
    new_measures = [
        DeclaredMeasure(
            bound=BinderBoundExpr(value_key=key_fn(dm.bound.value_key)),
            declared_name=dm.declared_name,
            public_name=dm.public_name,
            label=dm.label,
            canonical_alias=dm.canonical_alias,
            type=dm.type,
            type_is_explicit=dm.type_is_explicit,
            format=dm.format,
            description=dm.description,
            is_dimension=dm.is_dimension,
        )
        for dm in declared_measures
    ]
    new_filters = []
    for bf in bound_filters:
        new_vk = key_fn(bf.value_key)
        new_filters.append(
            BoundFilter(
                value_key=new_vk,
                phase=bf.phase,
                referenced_keys=tuple(walk_value_keys(new_vk)),
            )
        )
    new_specs = [
        OrderSpec(
            bound=BinderBoundExpr(value_key=key_fn(spec.bound.value_key)),
            direction=spec.direction,
        )
        for spec in order_specs
    ]
    return new_measures, new_filters, new_specs


def bind_query_inputs(  # NOSONAR(S3776) — one cohesive bind pass. The stages are strictly sequential and share the growing `declared_measures` / `bound_filters` / `order_specs` triple: parse+bind, time-key attachment, sugar lowering, rank-partition validation. Splitting them would thread the same three lists through four signatures without removing a branch.
    *,
    query: SlayerQuery,
    bundle: ResolvedSourceBundle,
    scope: Optional[Union[ModelScope, StageSchema]] = None,
    stage_schemas: Optional[Dict[str, StageSchema]] = None,
) -> PreboundQuery:
    """Parse and bind every text surface of a ``SlayerQuery`` (DEV-1742 §5.4).

    This is the ONLY door into the parser. ``plan_query`` calls it when no
    ``prebound`` is supplied; a caller that already holds typed keys builds a
    ``PreboundQuery`` structurally and skips it entirely, which is what makes
    re-rooting free of formula-text round-trips (P-E).

    The returned keys are fully normalized — time keys attached, ``change`` /
    ``change_pct`` sugar lowered, rank ``partition_by`` columns validated and
    rewritten to their time buckets — so planning sees exactly one key shape.

    Model filters are deliberately NOT included: they are a property of the
    SCOPE, not the query, and ``plan_query`` lifts them from ``scope``
    directly so a pre-bound caller inherits its own model's filters rather
    than the host's.
    """
    if scope is None:
        scope = _resolve_scope(
            query=query, bundle=bundle, stage_schemas=stage_schemas,
        )

    # DEV-1543: raw-rows mode rejects measure references in filters / order.
    # Runs BEFORE binding so the targeted, actionable error wins over the
    # binder's generic "cannot resolve reference" / "function not allowed"
    # for a saved-measure or function-style-aggregate reference.
    if query.distinct_dimension_values is False:
        _reject_measure_refs_for_raw_rows(query=query, scope=scope)

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
    _, _, _agg_dms = partition_declared_measures(
        declared_measures=declared_measures, n_dims=n_dims, n_time_dimensions=n_tds,
    )
    for dm in _agg_dms:
        for alias in (dm.public_name, dm.declared_name, dm.canonical_alias):
            if alias is not None:
                filter_alias_map.setdefault(alias, dm.bound.value_key)
    # DEV-1740: a computed dimension's name is a query-local alias resolvable
    # in filters and order (unlike a plain/time dimension, whose name is a raw
    # column that must resolve to the column, not a slot).
    for dm in declared_measures:
        if dm.is_dimension and dm.public_name is not None:
            filter_alias_map.setdefault(dm.public_name, dm.bound.value_key)

    # DEV-1450 stage 7b.9 — filter list construction in legacy WHERE
    # order: date_range filters first, then SlayerModel.filters
    # (Mode-A SQL), then user query filters (Mode-B DSL). date_range is
    # emitted before the model/query filters, and model filters precede
    # query filters.
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
    # Carried on the prebound so dropped-filter warnings can quote the
    # user's original filter text WITHOUT slicing host_query.filters
    # (which has not been deduped, unlike bound_filters).
    bound_filter_texts: List[Optional[str]] = []

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

    # 2. SlayerModel.filters — Mode-A SQL, always-applied WHERE. Lifted from
    #    the SCOPE in ``plan_query``, not here (§5.4): they belong to the model
    #    being planned against, so a re-rooted sub-plan must pick up the
    #    TARGET's model filters rather than inherit the host's through the
    #    carrier.

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
            parsed=parse_filter_expr(f),
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
        # DEV-1733: a placeholder ColumnRef means the item is an EXPRESSION,
        # not a column reference — bind ``raw_formula`` and skip the
        # declared-alias lookups below (which could otherwise match a real
        # model column that happens to share the sentinel's name). BOTH the
        # sentinel AND a captured ``raw_formula`` are required, so a model with
        # a genuine ``_expr_pending`` column, or a hand-built / deserialized
        # ``OrderItem``, still resolves through the normal path.
        if col_name in ORDER_PLACEHOLDER_NAMES and o.raw_formula:
            order_specs.append(OrderSpec(
                bound=bind_expr(
                    parsed=parse_expr(o.raw_formula),
                    scope=scope,
                    bundle=bundle,
                ),
                direction=o.direction,
            ))
            continue
        # DEV-1829 — an ORDER BY over a partition_by aggregate must bind its
        # ``raw_formula`` so the partition survives: ``OrderItem`` canonicalises
        # ``amount:sum(partition_by=region)`` to the column name ``amount_sum``,
        # which the declared-alias shortcut below would collapse onto a plain
        # ``amount:sum`` measure of the same source — silently dropping the
        # partition and sorting by the finer per-row total. The structural check
        # (after a cheap ``partition_by`` prefilter) confirms it really is a
        # partitioned aggregate before preferring the raw form.
        # DEV-1835 D1 — the same collision drops a bare ``window=``: an order-only
        # ``amount:sum(window='90d')`` canonicalises to ``amount_sum`` and binds
        # to the plain measure, so ``_bare_combined_roots`` never sees the window
        # in the order role. Preserve the raw form for a windowed order ref too.
        if o.raw_formula and (
            "partition_by" in o.raw_formula or "window" in o.raw_formula
        ):
            _part_bound = bind_expr(
                parsed=parse_expr(o.raw_formula),
                scope=scope, bundle=bundle,
            )
            if any(
                isinstance(k, AggregateKey) and (
                    k.partition_keys is not None or _window_kwarg_of(k) is not None
                )
                for k in walk_value_keys(_part_bound.value_key)
            ):
                order_specs.append(OrderSpec(
                    bound=_part_bound, direction=o.direction,
                ))
                continue
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
                parsed=parse_expr(o.raw_formula),
                scope=scope,
                bundle=bundle,
            )
        else:
            # Bind the FULL reference (``customers.region``), not just the
            # leaf — otherwise a structured dotted ORDER ColumnRef without a
            # raw_formula rebinds as ``region`` and hits the wrong host
            # column or fails as ambiguous (CR).
            bo = bind_expr(
                parsed=parse_expr(full_name),
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
    # binding completes. Any time-needing transform with no resolvable
    # time dimension raises.
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
        declared_measures, bound_filters, order_specs = _map_bound_keys(
            lambda vk: _attach_time_keys(vk, td_key=active_td_key),
            declared_measures=declared_measures,
            bound_filters=bound_filters,
            order_specs=order_specs,
        )

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
    declared_measures, bound_filters, order_specs = _map_bound_keys(
        lower_sugar_transforms,
        declared_measures=declared_measures,
        bound_filters=bound_filters,
        order_specs=order_specs,
    )

    # DEV-1497: validate that every rank-family ``partition_by`` column resolves
    # to a query dimension / time-dimension, and rewrite a time-dimension source
    # column to its truncated-bucket ``TimeTruncKey`` (partition by the bucket,
    # not the raw timestamp — which would silently widen the grain). Runs BEFORE
    # interning so a rewritten key never leaves a stale slot behind (identity is
    # only touched on the rewritten rank transform).
    _dim_dms, _td_dms, _ = partition_declared_measures(
        declared_measures=declared_measures, n_dims=n_dims, n_time_dimensions=n_tds,
    )
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
    # DEV-1825 — a partitioned aggregate INSIDE a computed dimension declares
    # the grain of a synthesized producer stage, so its partition_by may be ANY
    # groupable key (a finer grain than the query), not only a query dimension.
    # The time-bucket rewrite and multi-granularity guard still apply.
    _dim_agg_keys = frozenset(dimension_partitioned_aggregates(declared_measures))
    # DEV-1824 D9 / CR — a partitioned aggregate consumed in a COMBINED position
    # (a non-dimension measure, composite, filter, or raw ORDER target) must have
    # query-dimension partition keys so the join-back finds its host slots. The
    # lenient (finer-grain) exemption is safe ONLY for a key used exclusively as a
    # computed-dimension row attach; a key ALSO used as a combined consumer is
    # validated strictly, so a non-dimension grain raises the clean error rather
    # than an internal join-back failure.
    _combined_consumer_keys = frozenset(
        combined_partitioned_aggregates(
            declared_measures, order_specs,
            row_agg_set=_dim_agg_keys, bound_filters=bound_filters,
        )[0]
    )

    def _validate_partition_keys(key: ValueKey) -> frozenset:
        label = (
            f"Transform '{key.op}'" if isinstance(key, TransformKey)
            else f"Aggregation '{key.agg}'"
        )
        lenient = key in _dim_agg_keys and key not in _combined_consumer_keys
        new_pks = []
        for pk in key.partition_keys or ():
            # DEV-1836 — a partition key reached over a join must be attributable
            # from the aggregate's root (provably many-to-one hops only); an
            # unproven/fanning hop is a hard error naming the remedy, in both
            # modes, before the query-dimension checks below.
            _assert_partition_key_attributable(
                key=key, pk=pk, label=label, scope=scope, bundle=bundle,
            )
            if pk in _dim_key_set or pk in _td_key_set:
                new_pks.append(pk)          # already a query dim / td bucket
            elif pk in _td_ambiguous_sources:
                raise ValueError(
                    f"{label}: partition_by column "
                    f"'{_partition_key_display(pk)}' is ambiguous — it is a "
                    f"time dimension at multiple granularities. Partition by a "
                    f"single query dimension instead."
                )
            elif pk in _td_by_source:
                new_pks.append(_td_by_source[pk])  # td source col -> bucket
            elif lenient:
                new_pks.append(pk)          # finer-grain producer key (DEV-1825)
            else:
                raise ValueError(
                    f"{label}: partition_by column "
                    f"'{_partition_key_display(pk)}' is not a query dimension. "
                    f"Add it to dimensions/time_dimensions, or choose one of: "
                    f"{', '.join(_available_dims) or '(none)'}."
                )
        return frozenset(new_pks)

    def _rw(vk: ValueKey) -> ValueKey:
        return rewrite_rank_partition_keys(vk, rewrite_fn=_validate_partition_keys)

    declared_measures, bound_filters, order_specs = _map_bound_keys(
        _rw,
        declared_measures=declared_measures,
        bound_filters=bound_filters,
        order_specs=order_specs,
    )

    # DEV-1839 D9 — temporal-axis containment for time-ordered transforms in
    # dimensions, once time_key is attached and partition keys are bucket-rewritten.
    _guard_dimension_temporal_axis(declared_measures)

    return PreboundQuery(
        declared_measures=declared_measures,
        bound_filters=bound_filters,
        bound_filter_texts=bound_filter_texts,
        n_date_range=n_date_range,
        order_specs=order_specs,
        main_time_key=active_td_key,
        n_dims=n_dims,
        n_time_dimensions=n_tds,
        limit=query.limit,
        offset=query.offset,
        distinct_dimension_values=query.distinct_dimension_values,
    )


# ---------------------------------------------------------------------------
# DEV-1825 — regroup desugar: synthesize a producer stage per partition set.
# ---------------------------------------------------------------------------


def _regroup_grain_name(pk: ValueKey) -> str:
    """A producer output column name for a partition key (its join-back key)."""
    if isinstance(pk, TimeTruncKey):
        return f"{column_leaf(pk.column)}_{pk.granularity}"
    path = tuple(getattr(pk, "path", ()) or ())
    leaf = getattr(pk, "leaf", None) or getattr(pk, "column_name", None) or "grain"
    return "__".join([*path, leaf])


def _regroup_partition_order(pks: FrozenSet[ValueKey]) -> List[ValueKey]:
    """Deterministic member order: plain dimensions first, then time buckets."""
    return sorted(
        pks, key=lambda k: (isinstance(k, TimeTruncKey), _regroup_grain_name(k), repr(k)),
    )


def _regroup_producer_prebound(  # NOSONAR(S3776) — one producer-prebound assembly; the grain / aggregate / inherited-filter / order arms share the prebound under construction.
    *,
    pks: FrozenSet[ValueKey],
    aggs: List[AggregateKey],
    model: Optional[SlayerModel],
    bundle: ResolvedSourceBundle,
    inherited: List[BoundFilter],
    n_date_range: int,
    partition_order: Callable[
        [FrozenSet[ValueKey]], List[ValueKey],
    ] = _regroup_partition_order,
    public_alias_by_agg: Optional[Mapping[AggregateKey, str]] = None,
    explicit_types: Optional[Mapping[ValueKey, DataType]] = None,
    grain_name_by_key: Optional[Mapping[ValueKey, str]] = None,
    window_td_key: Optional[ValueKey] = None,
) -> Tuple[PreboundQuery, List[ValueKey]]:
    """The producer's bind product: grain from the partition keys, one measure
    per consumed aggregate (verbatim, ``partition_keys`` retained so it renders
    as a plain grouped aggregate at this grain), the base-row filters inherited.
    Returns the prebound plus the ordered grain keys (for join-pair building).

    ``partition_order`` picks the grain-key order (DEV-1829: a combined attach
    keeps the CONSUMER's dimension order for byte-identity with the DEV-1739
    join-back; the default alphabetical order is for row attaches whose partition
    keys need not be query dimensions). ``public_alias_by_agg`` (F1 / D4) names a
    directly-consumed measure's producer output by the consumer's public alias;
    an aggregate absent from it keeps the canonical alias.

    ``window_td_key`` (DEV-1824 D5) — a windowed producer synthesizes the
    consumer's active time dimension into its grain as the declared main time
    key, evaluated per bucket and included verbatim in the attach keys.
    """
    public_alias_by_agg = public_alias_by_agg or {}
    explicit_types = explicit_types or {}
    grain_name_by_key = grain_name_by_key or {}
    ordered = partition_order(pks)
    dims = [pk for pk in ordered if not isinstance(pk, TimeTruncKey)]
    tds = [pk for pk in ordered if isinstance(pk, TimeTruncKey)]
    if window_td_key is not None and window_td_key not in pks:
        tds = [*tds, window_td_key]
    grain_dms: List[DeclaredMeasure] = []
    for pk in [*dims, *tds]:
        if model is not None:
            d_type, d_fmt, d_desc = dimension_key_metadata(
                model=model, key=pk, bundle=bundle,
            )
        else:
            d_type, d_fmt, d_desc = None, None, None
        # DEV-1829 — a combined attach names its grain by the CONSUMER's
        # dimension name (``ordered_at``, not the row attach's ``ordered_at_month``)
        # so the producer column + join-back match the DEV-1739 baseline.
        name = grain_name_by_key.get(pk) or _regroup_grain_name(pk)
        grain_dms.append(DeclaredMeasure(
            bound=BinderBoundExpr(value_key=pk),
            declared_name=name, public_name=name,
            type=d_type, format=d_fmt, description=d_desc,
            # DEV-1835 D4 — a grain key is a dimension the producer GROUPS BY; a
            # computed one (band / scalar-expr / rank) must be marked so its inner
            # aggregate is discovered as a ROW attach (grouped into the producer's
            # own ``_base``) rather than a combined broadcast.
            is_dimension=True,
        ))
    agg_dms: List[DeclaredMeasure] = []
    for agg in aggs:
        canonical = (
            public_alias_by_agg.get(agg)
            or (canonical_aggregate_alias(agg, profile="stage_formula")
                if isinstance(agg, AggregateKey) else None)
            or getattr(agg, "agg", None)
            or getattr(agg, "op", None)
            or "regroup"
        )
        # A transform root (D4) carries no model-measure metadata — its type is
        # inferred from the transform. A consumer's EXPLICIT type (query-field
        # override) wins over the source-column type, mirroring the local chain.
        if model is not None and isinstance(agg, AggregateKey):
            a_type = measure_key_type(model=model, key=agg)
            a_fmt, a_desc = measure_key_format_description(model=model, key=agg)
        else:
            a_type, a_fmt, a_desc = None, None, None
        agg_dms.append(DeclaredMeasure(
            bound=BinderBoundExpr(value_key=agg),
            declared_name=canonical, public_name=canonical,
            type=explicit_types.get(agg, a_type), format=a_fmt, description=a_desc,
            type_is_explicit=agg in explicit_types,
        ))
    prebound = PreboundQuery(
        declared_measures=[*grain_dms, *agg_dms],
        bound_filters=list(inherited),
        bound_filter_texts=[None] * len(inherited),
        n_date_range=n_date_range,
        order_specs=[],
        main_time_key=window_td_key,
        n_dims=len(dims),
        n_time_dimensions=len(tds),
        distinct_dimension_values=True,
    )
    return prebound, [*dims, *tds]


def _regroup_inherited_filters(
    prebound: PreboundQuery, dim_agg_set: FrozenSet[AggregateKey],
) -> Tuple[List[BoundFilter], int]:
    """The base-row filters to copy into every producer, date-range bounds
    first (so ``n_date_range`` still slices them). Raises on a mixed predicate."""
    date_bounds: List[BoundFilter] = []
    others: List[BoundFilter] = []
    for idx, bf in enumerate(prebound.bound_filters):
        if classify_regroup_filter(bf, dim_agg_set) != "row_inherit":
            continue
        if idx < prebound.n_date_range:
            date_bounds.append(bf)
        else:
            others.append(bf)
    return [*date_bounds, *others], len(date_bounds)


def _find_regroup_slot(slots: List[ValueSlot], key: ValueKey, *, role: str) -> SlotId:
    for slot in slots:
        if slot.key == key:
            return slot.id
    raise ValueError(
        f"Regroup producer plan is missing the {role} slot for "
        f"{type(key).__name__}; synthesis and planning disagree on its grain.",
    )


def _regroup_answer_slot_id(
    *, value_slots: List[ValueSlot], key: ValueKey, fallback: Optional[SlotId],
) -> SlotId:
    """The producer slot that answers a regroup root. Matches by structural
    identity first (a bare aggregate / single-grain transform root, byte-stable);
    DEV-1839 — a union-grain producer that desugared its own inner aggregates no
    longer carries the pre-desugar key, so fall back to the producer's public
    projection position (``fallback``)."""
    for slot in value_slots:
        if slot.key == key:
            return slot.id
    if fallback is not None:
        return fallback
    raise ValueError(
        f"Regroup producer plan is missing the answer slot for "
        f"{type(key).__name__}; synthesis and planning disagree on its grain.",
    )


def _producer_grain_slot_ids(producer_plan) -> set:
    """The planned producer's actual grouping grain: its PROJECTED row slots.
    An aggregating producer can only project a grouped column, and its grain dims
    are public, so this excludes filter-only row slots (which are hidden)."""
    projected = set(producer_plan.projection)
    return {slot.id for slot in producer_plan.row_slots if slot.id in projected}


def _assert_attach_covers_producer_grain(
    *, joined_slot_ids: set, producer_grain_slot_ids: set,
) -> None:
    """D8 — the attach MUST join on the producer's COMPLETE grouping grain; a
    coarser join multiplies rows. Keyless (``partition_by=[]``) has an empty
    grain, so its aggregating producer is provably single-row. The grain is taken
    from the planned producer (``_producer_grain_slot_ids``), independent of the
    join's own key list, so a planner-added or dropped grouping key is caught."""
    if joined_slot_ids != producer_grain_slot_ids:
        raise ValueError(
            "Regroup attach join keys do not match the producer's grouping grain; "
            "the join must cover the complete grain or it changes cardinality "
            "(DEV-1824)."
        )


def _validate_nested_producer_plan(
    *, producer_plan, producer_grain: FrozenSet[ValueKey],
) -> None:
    """DEV-1839 D4 — a union-grain producer MAY carry nested COMBINED regroup
    attaches (its strict-subset inner aggregates broadcast into the union). Admit
    only well-formed ones; anything else fails closed. Each nested attach must be
    combined-phase (no row attaches inside a producer), local (no cross-model),
    carry no deeper regroup / cross-model CTE of its own (own-grain exclusion
    terminates the recursion), and join on a STRICT subset of the producer's
    grain (a coarser aggregate broadcast to the finer union rows)."""
    for attach in producer_plan.regroup_attach_plans:
        # DEV-1835 D4 — a ROW attach inside a producer is a computed dimension the
        # producer groups by (a band / scalar-expr / rank grain key), built from
        # the original pre-substitution expression; it joins into the producer's
        # own ``_base`` before aggregation and may sit at any grain. Only COMBINED
        # nested attaches (DEV-1839 union broadcast) carry the strict-subset rule.
        if attach.attach_phase == "row":
            continue
        nested = attach.producer_plan
        if nested.regroup_attach_plans:
            raise NotImplementedError(
                "A union-grain producer's nested attach itself needs a further "
                "regroup producer CTE, which is not supported (DEV-1839)."
            )
        grain = frozenset(host_key for host_key, _ in attach.join_pairs)
        # DEV-1835 D9 — a WINDOWED nested attach joins at the FULL union grain
        # (partition ∪ bucket): its value varies per bucket, so it is not a
        # strict-subset broadcast. A plain broadcast stays a strict subset.
        windowed_attach = any(
            _window_kwarg_of(sub.original_key) is not None
            for sub in attach.substitutions
        )
        ok = grain <= producer_grain if windowed_attach else grain < producer_grain
        if not ok:
            raise NotImplementedError(
                "A union-grain producer's nested attach grain is not a subset "
                "of the producer grain; only subset inner grains broadcast "
                "(DEV-1839)."
            )


def _is_local_partitioned_agg(k: ValueKey) -> bool:
    """A LOCAL partitioned ``AggregateKey`` (the shape the regroup primitive
    desugars): a declared partition grain and no cross-model source path."""
    return (
        isinstance(k, AggregateKey)
        and k.partition_keys is not None
        and not getattr(k.source, "path", ())
    )


def _bound_filter_from_key(vk: ValueKey) -> BoundFilter:
    """A ``BoundFilter`` for a split conjunct, phase recomputed from its refs."""
    refs = tuple(walk_value_keys(vk))
    phase = max((k.phase for k in refs), default=vk.phase)
    return BoundFilter(value_key=vk, phase=phase, referenced_keys=refs)


def _split_partitioned_filter_conjuncts(
    prebound: PreboundQuery,
    *,
    crossing_root: Optional[Callable[[ValueKey], bool]] = None,
) -> Tuple[PreboundQuery, List[int]]:
    """DEV-1824 (D7) / DEV-1837 (D9) — split each top-level AND conjunct of any
    filter that references a LOCAL partitioned aggregate (combined OR consumed
    by a computed dimension) into its own predicate, deciding placement on the
    ORIGINAL (pre-substitution) tree. Each conjunct then routes to its own
    phase — a row-attach conjunct through ``classify_regroup_filter``, a
    combined one to the outer WHERE. Returns the rebuilt prebound and the
    indices (into its new ``bound_filters``) whose conjunct routes to the
    COMBINED scope (rendered at the outer WHERE after attachment).
    """
    old = list(prebound.bound_filters)
    # ``conjunct_scope`` routes only COMBINED partitioned aggregates; a
    # partitioned aggregate consumed by a computed dimension is a ROW attach
    # whose conjuncts ``classify_regroup_filter`` classifies individually (D9).
    row_agg_set = frozenset(
        dimension_partitioned_aggregates(prebound.declared_measures),
    )

    def _has_partitioned_ref(vk: ValueKey) -> bool:
        # DEV-1837 D9 — a top-level AND that references ANY local partitioned
        # aggregate must split so each conjunct routes to its own phase (identical
        # to the separate-filters form). That includes a bare windowed / first-last
        # measure (combined ref) AND a computed-dimension aggregate (a row-attach
        # ref, which ``is_local_combined_regroup_ref`` excludes via ``row_agg_set``
        # — hence the explicit ``_is_local_partitioned_agg`` arm, restoring the
        # split for band-style filters, DEV-1835).
        return any(
            is_local_combined_regroup_ref(k, row_agg_set=row_agg_set)
            or _is_local_partitioned_agg(k)
            # DEV-1836 — a cross-model aggregate becomes a combined-attach
            # placeholder; a filter over it resolves only after the join-back, so
            # it must split off and route to the outer WHERE like a local one.
            or _is_cross_model_agg(k)
            # DEV-1838 D5 — a crossing-input local aggregate desugars onto a
            # host-rooted producer, so a filter over it resolves the same way.
            or (crossing_root is not None and crossing_root(k))
            for k in walk_value_keys(vk)
        )

    if not any(_has_partitioned_ref(bf.value_key) for bf in old):
        return prebound, []
    dim_keys = frozenset(
        dm.bound.value_key
        for dm in prebound.declared_measures[
            : prebound.n_dims + prebound.n_time_dimensions
        ]
    )
    texts = list(prebound.bound_filter_texts)
    new_filters: List[BoundFilter] = []
    new_texts: List[Optional[str]] = []
    combined_idx: List[int] = []
    for i, bf in enumerate(old):
        if not _has_partitioned_ref(bf.value_key):
            new_filters.append(bf)
            new_texts.append(texts[i])
            continue
        for cj in split_top_level_and(bf.value_key):
            cj_refs = list(walk_value_keys(cj))
            # A transform-wrapped conjunct (``cumsum(x) > 0``) is POST-phase:
            # the transform wrapper owns it, never the outer combined WHERE.
            cj_has_transform = any(isinstance(k, TransformKey) for k in cj_refs)
            if any(_is_cross_model_agg(k) for k in cj_refs) or (
                not cj_has_transform
                and crossing_root is not None
                and any(crossing_root(k) for k in cj_refs)
            ):
                # A cross-model / crossing-input aggregate predicate resolves at
                # the combined SELECT (after its producer joins back).
                scope = "combined"
            else:
                scope = conjunct_scope(
                    cj, dim_keys=dim_keys, row_agg_set=row_agg_set,
                )
            if scope == "combined":
                combined_idx.append(len(new_filters))
            new_filters.append(_bound_filter_from_key(cj))
            new_texts.append(None)
    updated = prebound.model_copy(update={
        "bound_filters": new_filters,
        "bound_filter_texts": new_texts,
    })
    return updated, combined_idx


# --------------------------------------------------------------------------- #
# DEV-1835 — bare windowed / first-last measures desugar onto the regroup
# primitive as combined-attach roots at the full projected grain (design D1).
# --------------------------------------------------------------------------- #
def _is_bare_local_regroup_root(k: ValueKey) -> bool:
    """A bare (no ``partition_by=``) LOCAL windowed or ``first``/``last``
    aggregate — the shape DEV-1835 routes into the regroup primitive as a
    combined-attach root at the full projected query grain."""
    return (
        isinstance(k, AggregateKey)
        and k.partition_keys is None
        and not getattr(k.source, "path", ())
        and (_window_kwarg_of(k) is not None or k.agg in RANKED_AGGREGATIONS)
    )


def _bare_combined_roots(  # NOSONAR(S3776) — straight-line discovery walk over projected slots + filters collecting bare regroup roots; each branch is independently simple
    prebound: PreboundQuery,
    *,
    extra_root: Optional[Callable[[ValueKey], bool]] = None,
) -> Tuple[List[AggregateKey], Dict[AggregateKey, str]]:
    """Bare windowed / first-last aggregates — plus any key ``extra_root``
    admits (DEV-1838 D5: crossing-input local aggregates) — reachable from a
    NON-dimension measure, an order spec, or a query filter (the
    combined-attach roles, DEV-1835 D1). First-seen order, deduped by
    structural identity; a directly-named measure maps to its public alias for
    producer naming."""

    def _is_root(k: ValueKey) -> bool:
        return _is_bare_local_regroup_root(k) or (
            extra_root is not None and extra_root(k)
        )

    seen: set = set()
    out: List[AggregateKey] = []
    alias: Dict[AggregateKey, str] = {}
    for dm in prebound.declared_measures:
        if dm.is_dimension:
            continue
        vk = dm.bound.value_key
        for k in walk_value_keys(vk):
            if _is_root(k) and k not in seen:
                seen.add(k)
                out.append(k)
        if _is_root(vk) and dm.public_name is not None:
            alias.setdefault(vk, dm.public_name)
    for sp in prebound.order_specs:
        for k in walk_value_keys(sp.bound.value_key):
            if _is_root(k) and k not in seen:
                seen.add(k)
                out.append(k)
    for bf in prebound.bound_filters:
        for k in walk_value_keys(bf.value_key):
            if _is_root(k) and k not in seen:
                seen.add(k)
                out.append(k)
    return out, alias


def _effective_root_grain(
    agg: ValueKey,
    *,
    projected_dim_keys: List[ValueKey],
    projected_td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey],
) -> Tuple[FrozenSet[ValueKey], bool]:
    """A combined-root's producer grain and windowedness (DEV-1835 D1/D5).

    An explicitly-partitioned aggregate keeps ``regroup_root_grain`` (its
    partition set). A bare windowed / first-last root takes the FULL projected
    grain: a windowed root's active time bucket enters via ``window_td_key`` (so
    it is excluded here, mirroring the explicit-partition case), a ranked root's
    grain is every projected slot.
    """
    windowed = _window_kwarg_of(agg) is not None
    if getattr(agg, "partition_keys", None) is not None:
        grain = regroup_root_grain(agg)
        # DEV-1835 D9 — a transform root is not itself windowed, but over a
        # window= inner aggregate its effective (union) grain gains the query's
        # active bucket and it renders windowed there. First/last inners are
        # timeless, so they keep the plain partition-set union.
        if (
            not windowed and active_bucket is not None
            and any(_window_kwarg_of(k) is not None for k in walk_value_keys(agg))
        ):
            return grain | {active_bucket}, True
        return grain, windowed
    if windowed:
        grain = frozenset(projected_dim_keys) | (
            frozenset(projected_td_keys) - ({active_bucket} if active_bucket else frozenset())
        )
    else:
        grain = frozenset(projected_dim_keys) | frozenset(projected_td_keys)
    return grain, windowed


def _scalar_free_columns(node: ValueKey, out: set) -> None:
    """Raw ``ColumnKey``s referenced at the SCALAR level of ``node`` — i.e. not
    consumed inside an aggregate. Aggregate internals are opaque here (their
    value is fixed by ``partition_keys``, checked separately)."""
    if isinstance(node, ColumnKey):
        out.add(node)
    elif isinstance(node, ArithmeticKey):
        for op in node.operands:
            _scalar_free_columns(node=op, out=out)
    elif isinstance(node, ScalarCallKey):
        for arg in node.args:
            if isinstance(arg, (ColumnKey, ArithmeticKey, ScalarCallKey, TransformKey)):
                _scalar_free_columns(node=arg, out=out)
    elif isinstance(node, TransformKey):
        _scalar_free_columns(node=node.input, out=out)


def _prune_functionally_determined_grain(
    pks: FrozenSet[ValueKey],
) -> FrozenSet[ValueKey]:
    """Drop computed-dimension grain keys functionally determined by the raw
    dimensions already in the grain (DEV-1835 D6 / D10). A ``rband =
    f(amount:sum(partition_by=region))`` grain key is constant within each
    ``region`` group, so grouping by it is redundant and its nested producer is
    never synthesized (avoids a cross-producer structural twin — a non-goal to
    dedup after the fact). A key referencing a raw column or an aggregate
    partition OUTSIDE the retained grain is a real axis and stays."""
    raw = frozenset(k for k in pks if isinstance(k, ColumnKey))
    kept = set(pks)
    for k in pks:
        if isinstance(k, ColumnKey):
            continue
        aggs = [a for a in walk_value_keys(k) if isinstance(a, AggregateKey)]
        if not aggs or any(
            a.partition_keys is None or not (frozenset(a.partition_keys) <= raw)
            for a in aggs
        ):
            continue
        free: set = set()
        _scalar_free_columns(k, free)
        if free <= raw:
            kept.discard(k)
    return frozenset(kept)


def _windowed_or_ranked_identity(agg: ValueKey):
    """A hashable, partition-free identity for a windowed / ranked aggregate, so
    each distinct such measure gets its OWN producer (DEV-1835 D6) — a bare form
    and an explicit ``partition_by=`` twin at the same grain collapse to one.
    ``None`` for a plain aggregate: those share one producer per grain (DEV-1824,
    unchanged)."""
    if not isinstance(agg, AggregateKey):
        return None
    windowed = _window_kwarg_of(agg) is not None
    ranked = agg.agg in RANKED_AGGREGATIONS
    if not windowed and not ranked:
        return None
    return (
        "windowed" if windowed else "ranked",
        agg.source, agg.agg, tuple(agg.args), tuple(agg.kwargs),
        agg.column_filter_key,
    )


def _partition_free_identity(agg: ValueKey):  # NOSONAR(S8495) — distinct-shape identity tuples are intentional dict keys: a plain aggregate's 5-field identity and an "other" 2-tuple never collide (different lengths compare unequal)
    """A hashable identity ignoring ``partition_keys`` (DEV-1835 D6). Two
    aggregates that differ ONLY in partition (a bare form and an explicit
    ``partition_by=`` twin, already grouped at the same grain) share one producer
    measure; a transform root or other non-aggregate is its own identity."""
    if not isinstance(agg, AggregateKey):
        return ("other", agg)
    return (agg.source, agg.agg, tuple(agg.args), tuple(agg.kwargs),
            agg.column_filter_key)


# --------------------------------------------------------------------------- #
# DEV-1836 — cross-model aggregates as target-rooted regroup producers.
# A cross-model aggregate (``source.path`` non-empty) roots a producer at the
# model whose rows it aggregates, computes at the fan-out-safe subset of its
# requested grain, and broadcasts across the rest — the unification of the
# bespoke ``_cm_`` path onto the regroup primitive (design D2/D3).
# --------------------------------------------------------------------------- #
def _is_cross_model_agg(k: ValueKey) -> bool:
    """A cross-model ``AggregateKey`` — its source names another model. A
    host-grain wrap (grain="host") is excluded: it READS through the join but
    is grouped per host row-group, so it roots at the HOST (DEV-1747 D2)."""
    return (
        isinstance(k, AggregateKey)
        and bool(getattr(k.source, "path", ()))
        and getattr(k, "grain", "target") != "host"
    )


def _key_host_path(key: ValueKey) -> Tuple[str, ...]:
    """The host-coordinate join path of a dimension / grain key."""
    if isinstance(key, TimeTruncKey):
        return tuple(getattr(key.column, "path", ()) or ())
    return tuple(getattr(key, "path", ()) or ())


def _attributable_from_root(
    *, host_path: Tuple[str, ...], target_path: Tuple[str, ...],
    root_model: SlayerModel, models_by_name: Dict[str, SlayerModel],
    host_name: Optional[str] = None,
) -> bool:
    """Is a host-coordinate path attributable from the aggregate's root, over
    provably many-to-one hops only (design D2)? Three rules (D3): under the
    target → prefix-strip; with ``host_name`` given, also a sibling path the
    root's own graph reaches directly, or any host path reached by prepending a
    provably to-one hop back to the host."""
    tp, hp = tuple(target_path), tuple(host_path)
    if hp[: len(tp)] == tp:
        return safe_reachable(
            root=root_model, path=hp[len(tp):], models_by_name=models_by_name,
        )
    if host_name is None or (tp and host_name == tp[0]):
        return False
    if hp and safe_reachable(root=root_model, path=hp, models_by_name=models_by_name):
        return True
    return safe_reachable(
        root=root_model, path=(host_name, *hp), models_by_name=models_by_name,
    )


def _reroot_from_root(
    key: ValueKey, *, target_path: Tuple[str, ...], root_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], host_name: str,
) -> ValueKey:
    """Re-anchor a host-coordinate key into the root's coordinates, per leaf,
    by the same rules ``_attributable_from_root`` proves safety with:
    target-prefix → strip; else prepend the hop back to the host (the exact
    reproduction of the host's join instance) when that path is provable;
    else a direct-reachable sibling stays unchanged (the root's own join —
    legacy star-schema parity, safe-arity but instance-approximate)."""
    tp = tuple(target_path)
    mapping: Dict[ValueKey, ValueKey] = {}
    for r in walk_value_keys(key):
        # walk_value_keys does NOT descend into TimeTruncKey.column, so the
        # bucket key is rerooted here as a whole.
        if not isinstance(r, (ColumnKey, ColumnSqlKey, StarKey, TimeTruncKey)):
            continue
        hp = _key_host_path(r)
        if hp[: len(tp)] == tp:
            continue  # reroot_value_key strips the prefix below
        if tp and host_name == tp[0]:
            continue
        via_host = (host_name, *hp)
        if not safe_reachable(
            root=root_model, path=via_host, models_by_name=models_by_name,
        ):
            if hp and safe_reachable(
                root=root_model, path=hp, models_by_name=models_by_name,
            ):
                continue  # resolved through the root's own join to the sibling
        if isinstance(r, TimeTruncKey):
            mapping[r] = r.model_copy(update={
                "column": r.column.model_copy(update={"path": via_host}),
            })
        else:
            mapping[r] = r.model_copy(update={"path": via_host})
    if mapping:
        key = substitute_value_keys(key, mapping)
    return reroot_value_key(key, target_path=tp)


def _broadcast_reason(
    *, host_path: Tuple[str, ...], target_path: Tuple[str, ...],
    root_model: SlayerModel, models_by_name: Dict[str, SlayerModel],
) -> str:
    """Why a dimension broadcasts: unreachable from the root (no stored edge at
    all), or crosses an unproven/fanning join hop."""
    tp, hp = tuple(target_path), tuple(host_path)
    if hp[: len(tp)] != tp:
        return "unreachable from the aggregate's root (no join path from it)"
    current = root_model
    for name in hp[len(tp):]:
        join = next((j for j in current.joins if j.target_model == name), None)
        if join is None:
            return "unreachable from the aggregate's root (no join path from it)"
        tgt = models_by_name.get(name)
        if tgt is None or not provably_to_one(join=join, target_model=tgt):
            return f"crosses an unproven join hop to {name}"
        current = tgt
    return "unreachable from the aggregate's root"


def _assert_partition_key_attributable(
    *, key: ValueKey, pk: ValueKey, label: str,
    scope: Union[ModelScope, StageSchema], bundle: ResolvedSourceBundle,
) -> None:
    """A partition key reached over a join must be attributable from the
    aggregate's root; an unproven/fanning hop is a hard error (design D2, F4)."""
    hp = _key_host_path(pk)
    if not hp:
        return  # a local column — no join to cross
    host_m = scope.source_model if isinstance(scope, ModelScope) else None
    if host_m is None:
        return
    agg_target = tuple(key.source.path) if isinstance(key, AggregateKey) else ()
    models_by_name = {m.name: m for m in bundle.referenced_models}
    root = walk_key_path(model=host_m, path=agg_target, bundle=bundle) or host_m
    if _attributable_from_root(
        host_path=hp, target_path=agg_target, root_model=root,
        models_by_name=models_by_name,
        host_name=host_m.name if agg_target else None,
    ):
        return
    reason = _broadcast_reason(
        host_path=hp, target_path=agg_target, root_model=root,
        models_by_name=models_by_name,
    )
    raise ValueError(
        f"{label}: partition_by column '{_regroup_grain_name(pk)}' {reason}; "
        f"every partition key must be attributable from the aggregate's root — "
        f"declare join cardinality or a covering unique key on the target."
    )


def _shared_join_key_reroot(
    *, key: ValueKey, target_path: Tuple[str, ...], host_model: SlayerModel,
) -> Optional[ValueKey]:
    """A host-local dimension that IS a source-side join column of the single hop
    to the aggregate's root equals the root's target-side column per row (the
    hop is provably many-to-one when this producer is target-rooted). Return that
    target-side ``ColumnKey`` (local to the root), else ``None`` — the shared-key
    attribution the prefix rule alone cannot see (a query-backed grain join)."""
    if not isinstance(key, ColumnKey) or _key_host_path(key) or len(target_path) != 1:
        return None
    root_join = next(
        (j for j in host_model.joins if j.target_model == target_path[0]), None,
    )
    if root_join is None:
        return None
    for src, tgt in root_join.join_pairs:
        if src == key.leaf:
            return key.model_copy(update={"leaf": tgt, "path": ()})
    return None


def _grain_member_attributable(
    *, key: ValueKey, target_path: Tuple[str, ...], root_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], host_name: Optional[str] = None,
) -> bool:
    """Is a grain member attributable from the aggregate's root? A plain column
    follows its own path (D3 rules, incl. the safe hop back to the host); a
    computed dimension is attributable iff every column and aggregate it
    references is (D4 — its producer can nest inside; aggregates stay
    prefix-only, their producers re-root separately)."""
    saw = False
    for r in walk_value_keys(key):
        if isinstance(r, AggregateKey):
            saw = True
            if not _attributable_from_root(
                host_path=tuple(r.source.path), target_path=target_path,
                root_model=root_model, models_by_name=models_by_name,
            ):
                return False
        elif isinstance(r, (ColumnKey, ColumnSqlKey, TimeTruncKey, StarKey)):
            saw = True
            if not _attributable_from_root(
                host_path=_key_host_path(r), target_path=target_path,
                root_model=root_model, models_by_name=models_by_name,
                host_name=host_name,
            ):
                return False
    return saw


def _cross_model_input_paths(
    *, agg_R: AggregateKey, root_model: SlayerModel, root_name: str,
    bundle: ResolvedSourceBundle,
) -> List[Tuple[str, ...]]:
    """The join paths an aggregate's inputs (source ``Column.sql``, kwargs, and
    its ``Column.filter``) cross, in the ROOT's coordinates."""
    out: List[Tuple[str, ...]] = []
    if agg_R.column_filter_key is not None:
        for p in agg_R.column_filter_key.referenced_join_paths:
            if tuple(p) not in out:
                out.append(tuple(p))
    for p in compute_aggregate_input_join_paths(
        key=agg_R, anchor_model=root_model, anchor_relation=root_name,
        bundle=bundle,
    ):
        if tuple(p) not in out:
            out.append(tuple(p))
    return out


def _assert_cross_model_inputs_safe(
    *, agg: AggregateKey, agg_R: AggregateKey, root_model: SlayerModel,
    root_name: str, target_path: Tuple[str, ...], bundle: ResolvedSourceBundle,
    models_by_name: Dict[str, SlayerModel],
) -> None:
    """Every input of a cross-model aggregate must be attributable from its root
    (design D2, F4). Reading through a fanning/unproven join is ambiguous:
    hard error naming the input and the remedy, in both modes."""
    remedy = "declare join cardinality or a covering unique key on the target"
    # Source-column / kwarg / column-filter refs, in the root's coordinates.
    for path in _cross_model_input_paths(
        agg_R=agg_R, root_model=root_model, root_name=root_name, bundle=bundle,
    ):
        if not safe_reachable(
            root=root_model, path=path, models_by_name=models_by_name,
        ):
            hop = path[-1] if path else root_name
            raise ValueError(
                f"Cross-model aggregate {canonical_aggregate_alias(agg, profile='stage_formula')!r} "
                f"reads an input across an unproven join hop to {hop} from "
                f"{root_name}; {remedy}."
            )
    # Positional args in HOST coordinates (a ranking first/last time key): each
    # column-like arg must be attributable from the root.
    for arg in agg.args:
        if not isinstance(arg, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
            continue
        hp = _key_host_path(arg)
        if not _attributable_from_root(
            host_path=hp, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name,
        ):
            leaf = getattr(arg, "leaf", None) or getattr(
                getattr(arg, "column", None), "leaf", None,
            ) or "input"
            raise ValueError(
                f"Cross-model aggregate {canonical_aggregate_alias(agg, profile='stage_formula')!r} "
                f"ranks/reads by {leaf}, which is not attributable from "
                f"{root_name} (crosses a fanning join); {remedy}."
            )


def _cross_model_inherited_filters(
    *, base_filters: List[Tuple[BoundFilter, Optional[str]]],
    target_path: Tuple[str, ...], root_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel], host_name: Optional[str] = None,
) -> Tuple[List[BoundFilter], List[UnreachableFilterDroppedWarning]]:
    """Split base ROW filters into conjuncts; a conjunct all of whose references
    are attributable from the root inherits into the producer (re-rooted); one
    that is unreachable/unsafe is excluded and warned (design D3)."""
    inherited: List[BoundFilter] = []
    dropped: List[UnreachableFilterDroppedWarning] = []

    def _attributable(r: ValueKey) -> bool:
        return _attributable_from_root(
            host_path=_key_host_path(r), target_path=target_path,
            root_model=root_model, models_by_name=models_by_name,
            host_name=host_name,
        )

    for bf, text in base_filters:
        if bf.phase != Phase.ROW:
            continue
        for cj in split_top_level_and(bf.value_key):
            refs = [
                k for k in walk_value_keys(cj)
                if isinstance(k, (ColumnKey, ColumnSqlKey, TimeTruncKey, StarKey))
            ]
            if all(_attributable(r) for r in refs):
                rerooted = (
                    _reroot_from_root(
                        cj, target_path=target_path, root_model=root_model,
                        models_by_name=models_by_name, host_name=host_name,
                    )
                    if host_name is not None
                    else reroot_value_key(cj, target_path=target_path)
                )
                inherited.append(_bound_filter_from_key(rerooted))
            else:
                unsafe = next((r for r in refs if not _attributable(r)), None)
                reason = _broadcast_reason(
                    host_path=_key_host_path(unsafe) if unsafe else (),
                    target_path=target_path, root_model=root_model,
                    models_by_name=models_by_name,
                )
                dropped.append(UnreachableFilterDroppedWarning(
                    filter_text=text or _canonical_name(cj), reason=reason,
                ))
    return inherited, dropped


def _local_crossing_input_paths(
    *, key: AggregateKey, bundle: ResolvedSourceBundle,
    host_model: SlayerModel,
) -> List[Tuple[str, ...]]:
    """Join paths a LOCAL aggregate's inputs cross: typed ``Column.filter``
    references first, then source ``Column.sql`` / args / kwargs / model-default
    aggregation params (structural). Order-stable, de-duplicated."""
    out: List[Tuple[str, ...]] = []
    if key.column_filter_key is not None:
        for p in key.column_filter_key.referenced_join_paths:
            if p not in out:
                out.append(tuple(p))
    for p in compute_aggregate_input_join_paths(
        key=key,
        anchor_model=host_model,
        anchor_relation=host_model.name,
        bundle=bundle,
    ):
        if p not in out:
            out.append(tuple(p))
    return out


def _crossing_local_root_predicate(
    *, scope: Union[ModelScope, StageSchema], bundle: ResolvedSourceBundle,
) -> Callable[[ValueKey], bool]:
    """DEV-1838 D5 — predicate for a LOCAL plain aggregate whose inputs cross a
    join (filter references / args / kwargs / aggregation params): the shape
    that desugars onto a HOST-rooted producer. Windowed / ranked roots are
    excluded (they desugar on their own trigger and are gated at synthesis)."""
    host_model = scope.source_model if isinstance(scope, ModelScope) else None

    def _pred(k: ValueKey) -> bool:
        return (
            isinstance(k, AggregateKey)
            and k.partition_keys is None
            and not getattr(k.source, "path", ())
            and _window_kwarg_of(k) is None
            and k.agg not in RANKED_AGGREGATIONS
            and host_model is not None
            and _crosses(k)
        )

    def _crosses(k: AggregateKey) -> bool:
        crossed = _local_crossing_input_paths(
            key=k, bundle=bundle, host_model=host_model,
        )
        return bool(crossed) and not may_inline_crossing_inputs(crossed)

    return _pred


def _assert_local_producer_inputs_safe(
    *,
    agg: AggregateKey,
    host_model: SlayerModel,
    bundle: ResolvedSourceBundle,
    models_by_name: Dict[str, SlayerModel],
) -> None:
    """D5 — per-role crossing-input safety for a HOST-rooted producer answer.

    A *filter reference* or *argument* crossing an unproven hop is ambiguous
    (the fanned join multiplies the aggregate) — hard error naming the input,
    the hop, and the remedy. A host-grain wrap's crossing SOURCE path stays
    legal: the wrap is defined over the join result (DEV-1747 D2).
    """
    remedy = "declare join cardinality or a covering unique key on the target"
    alias = canonical_aggregate_alias(agg, profile="stage_formula")

    def _safe(path: Tuple[str, ...]) -> bool:
        return safe_reachable(
            root=host_model, path=tuple(path), models_by_name=models_by_name,
        )

    # Role: crossed argument, explicitly named (a first/last ranking arg).
    for arg in agg.args:
        if not isinstance(arg, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
            continue
        path = _key_host_path(arg)
        if path and not _safe(path):
            leaf = getattr(arg, "leaf", None) or getattr(
                getattr(arg, "column", None), "leaf", None,
            ) or "input"
            raise ValueError(
                f"Aggregate {alias!r} ranks/reads by {leaf}, which crosses an "
                f"unproven join hop to {path[-1]} from {host_model.name}; "
                f"{remedy}."
            )

    # Roles: crossed predicate (Column.filter references) and remaining
    # crossed arguments (kwargs, model-default aggregation params). The
    # SOURCE's own crossings are exempt — a source read through a join
    # consumes the target's values per-match, which is legal (D5; the
    # host-grain wrap's crossing source path is the same carve-out).
    gated: List[Tuple[str, ...]] = []
    if agg.column_filter_key is not None:
        for p in agg.column_filter_key.referenced_join_paths:
            if tuple(p) not in gated:
                gated.append(tuple(p))
    for p in compute_aggregate_input_join_paths(
        key=agg, anchor_model=host_model, anchor_relation=host_model.name,
        bundle=bundle, include_source=False,
    ):
        if tuple(p) not in gated:
            gated.append(tuple(p))
    for path in gated:
        if not path:
            continue
        if not _safe(path):
            raise ValueError(
                f"Aggregate {alias!r} reads an input across an unproven join "
                f"hop to {path[-1]} from {host_model.name}; {remedy}."
            )


def _trailing_window_kernel(
    *,
    producer_plan: PlannedQuery,
    agg_key: AggregateKey,
    n_date_range: int,
) -> TrailingWindowProducerKernel:
    """DEV-1838 D4 — the trailing-window kernel for a windowed producer attach,
    derived from the planned producer (bucket = its active TD; ``_src`` filters
    = its ROW filters minus frame bounds, DEV-1732)."""
    window_raw = _window_kwarg_of(agg_key)
    bucket_sid = producer_plan.active_time_dimension_slot_id
    bucket_slot = next(
        (s for s in producer_plan.row_slots if s.id == bucket_sid), None,
    )
    if window_raw is None or bucket_slot is None:
        raise RuntimeError(
            "Windowed producer is missing its window duration or bucket slot; "
            "synthesis and planning disagree (DEV-1838)."
        )
    src_where_ids, src_rewrites = _plan_src_row_filters(
        filters_by_phase=producer_plan.filters_by_phase,
        date_range_fids={f"f{i}" for i in range(n_date_range)},
        frame_bound_columns=producer_plan.frame_bound_columns,
    )
    return TrailingWindowProducerKernel(
        window_raw=window_raw,
        window_parts=parse_window_duration(window_raw),
        window_granularity=bucket_slot.key.granularity,
        bucket_slot_id=bucket_sid,
        src_where_filter_ids=src_where_ids,
        src_filter_rewrites=src_rewrites,
    )


def _ranked_kernel(
    *,
    producer_plan: PlannedQuery,
    agg_key: AggregateKey,
    root_model: SlayerModel,
    bundle: ResolvedSourceBundle,
) -> RankedProducerKernel:
    """DEV-1838 D4 — the ranked kernel for a first/last producer attach. The
    ranking key resolves in the producer's own coordinates, from the same
    inputs the retired ``build_host_ranked_plan`` used."""
    return RankedProducerKernel(
        agg=agg_key.agg,
        ranking_time_key=resolve_ranking_time_key(
            key=agg_key,
            root_model=root_model,
            bundle=bundle,
            row_keys=ordered_row_keys(
                row_slots=producer_plan.row_slots,
                public_projection=producer_plan.projection,
            ),
        ),
    )


def _synthesize_wrap_attach(
    *,
    wrap_key: AggregateKey,
    prebound: PreboundQuery,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    stage_schemas: Dict[str, StageSchema],
    producer_registry: Optional[Dict[Hashable, PlannedQuery]],
    producer_source_model: Optional[str],
    row_attaches: Sequence[RegroupAttachPlan] = (),
) -> RegroupAttachPlan:
    """DEV-1838 D5 — a host-grain ORDER-BY wrap as a HOST-rooted producer.

    Synthesized late (the wrap key is interned post-projection, so the regroup
    desugar cannot see it): a combined attach at the full projected grain whose
    substitution placeholder IS the wrap key — the hidden wrap slot then
    resolves from the producer column like any combined placeholder.

    Because it runs post-desugar, a computed-dimension grain member arrives
    carrying a ``__regroup__`` placeholder the sub-plan could not resolve.
    ``row_attaches`` undoes that: the placeholder maps back to its raw key, the
    producer plan re-desugars it (its nested producer interns onto the outer
    one, D3), and the attach's raw grain keys match host slots through the
    renderer's row-desugar map like any combined attach with a computed grain.
    """
    producer_model = scope.source_model if isinstance(scope, ModelScope) else None
    models_by_name = {m.name: m for m in bundle.referenced_models}
    if producer_model is not None:
        _assert_local_producer_inputs_safe(
            agg=wrap_key, host_model=producer_model, bundle=bundle,
            models_by_name=models_by_name,
        )
    dim_dms, td_dms, _ = partition_declared_measures(
        declared_measures=prebound.declared_measures,
        n_dims=prebound.n_dims, n_time_dimensions=prebound.n_time_dimensions,
    )
    undo_desugar = {
        sub.placeholder: sub.original_key
        for attach in row_attaches
        for sub in attach.substitutions
    }

    def _raw(key: ValueKey) -> ValueKey:
        return substitute_value_keys(key, undo_desugar) if undo_desugar else key

    projected = [_raw(dm.bound.value_key) for dm in (*dim_dms, *td_dms)]
    consumer_order = {k: i for i, k in enumerate(projected)}
    grain_name_by_key = {
        _raw(dm.bound.value_key): dm.declared_name
        for dm in (*dim_dms, *td_dms)
        if dm.declared_name is not None
    }
    inherited, n_inherited_date = _regroup_inherited_filters(
        prebound, frozenset(),
    )
    producer_prebound, ordered_pks = _regroup_producer_prebound(
        pks=frozenset(projected), aggs=[wrap_key], model=producer_model,
        bundle=bundle, inherited=inherited, n_date_range=n_inherited_date,
        partition_order=lambda pks: sorted(
            pks, key=lambda k: consumer_order.get(k, len(consumer_order)),
        ),
        grain_name_by_key=grain_name_by_key,
    )
    producer_plan = plan_query(
        query=StrictQueryCarrier(
            source_model=producer_source_model, prebound=producer_prebound,
        ),
        bundle=bundle,
        scope=scope,
        stage_schemas=stage_schemas,
        disable_host_rooted_isolation=True,
        # A computed-dimension grain member nests its own producer inside the
        # wrap (its nested attach interns onto the outer one, D3).
        enable_producer_regroups=any(
            isinstance(pk, (ScalarCallKey, ArithmeticKey, TransformKey))
            or _is_local_partitioned_agg(pk)
            for pk in projected
        ),
        prebound=producer_prebound,
        producer_registry=producer_registry,
    )
    producer_answer_ids = list(producer_plan.projection)[len(ordered_pks):]
    answer_slot = _regroup_answer_slot_id(
        value_slots=[
            *producer_plan.aggregate_slots,
            *producer_plan.combined_expression_slots,
        ],
        key=wrap_key,
        fallback=producer_answer_ids[0] if producer_answer_ids else None,
    )
    producer_grain_ids = list(producer_plan.projection)[: len(ordered_pks)]
    join_pairs: List[Tuple[ValueKey, SlotId]] = []
    for i, pk in enumerate(ordered_pks):
        slot_id = next(
            (s.id for s in producer_plan.row_slots if s.key == pk), None,
        )
        if slot_id is None:
            slot_id = producer_grain_ids[i]
        join_pairs.append((pk, slot_id))
    _assert_attach_covers_producer_grain(
        joined_slot_ids={slot_id for _, slot_id in join_pairs},
        producer_grain_slot_ids=_producer_grain_slot_ids(producer_plan),
    )
    return RegroupAttachPlan(
        producer_plan=producer_plan,
        alias_hint=canonical_aggregate_alias(wrap_key, profile="stage_formula"),
        attach_phase="combined",
        join_pairs=join_pairs,
        substitutions=[RegroupSubstitution(
            placeholder=wrap_key, producer_slot_id=answer_slot,
            original_key=wrap_key,
        )],
        partition_display=[_regroup_grain_name(pk) for pk in ordered_pks],
    )


def _synthesize_cross_model_producer(  # NOSONAR(S3776) — one cohesive target-rooted producer synthesis (root / safe-grain / broadcast / inputs / filter-inheritance / recursive plan / attach); the arms share the re-rooting coordinate state.
    *,
    agg: AggregateKey,
    placeholder: ValueKey,
    attach_phase: str,
    public_alias: Optional[str],
    prebound: PreboundQuery,
    bundle: ResolvedSourceBundle,
    host_model: SlayerModel,
    models_by_name: Dict[str, SlayerModel],
    projected_dim_keys: List[ValueKey],
    projected_td_keys: List[ValueKey],
    base_filters_with_text: List[Tuple[BoundFilter, Optional[str]]],
    scope: Union[ModelScope, StageSchema],
    stage_schemas: Dict[str, StageSchema],
    declared_type: Optional[DataType] = None,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
) -> RegroupAttachPlan:
    """Build one target-rooted regroup producer for a cross-model aggregate
    (design D2/D3): root R at the aggregate's source model, compute at the
    fan-out-safe subset of the requested grain, broadcast across the rest, with
    inherited filters, and a null-safe grain attach back onto the consumer."""
    target_path = tuple(agg.source.path)
    root_model = walk_key_path(model=host_model, path=target_path, bundle=bundle)
    if root_model is None:  # pragma: no cover — bind resolved the path already
        raise ValueError(
            f"Cross-model aggregate source path {target_path!r} does not resolve "
            f"to a model from {host_model.name}."
        )
    root_name = root_model.name
    alias = public_alias or canonical_aggregate_alias(agg, profile="stage_formula")

    # Requested grain G: explicit partition_by else the query dimensions.
    if agg.partition_keys is not None:
        requested = list(agg.partition_keys)
        explicit = True
    else:
        requested = [*projected_dim_keys, *projected_td_keys]
        explicit = False

    # Safe grain S (attributable from R) vs broadcast; an explicit key that is
    # unattributable is a hard error (design D2, F4).
    safe_pairs: List[Tuple[ValueKey, ValueKey]] = []  # (host_key, rerooted_key)
    broadcast: List[Tuple[str, str]] = []
    for g in requested:
        hp = _key_host_path(g)
        shared = _shared_join_key_reroot(
            key=g, target_path=target_path, host_model=host_model,
        )
        if shared is not None:
            # The join-key identity needs no join in the producer at all.
            safe_pairs.append((g, shared))
        elif _grain_member_attributable(
            key=g, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name, host_name=host_model.name,
        ):
            safe_pairs.append((g, _reroot_from_root(
                g, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name, host_name=host_model.name,
            )))
        elif explicit:
            reason = _broadcast_reason(
                host_path=hp, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name,
            )
            raise ValueError(
                f"Cross-model aggregate {alias!r} declares partition_by="
                f"{_regroup_grain_name(g)}, which {reason}; every explicit "
                f"partition key must be attributable from {root_name} — declare "
                f"join cardinality or a covering unique key on the target."
            )
        else:
            broadcast.append((_regroup_grain_name(g), _broadcast_reason(
                host_path=hp, target_path=target_path, root_model=root_model,
                models_by_name=models_by_name,
            )))

    agg_R = reroot_value_key(agg, target_path=target_path)
    _assert_cross_model_inputs_safe(
        agg=agg, agg_R=agg_R, root_model=root_model, root_name=root_name,
        target_path=target_path, bundle=bundle, models_by_name=models_by_name,
    )

    # A windowed cross-model aggregate synthesizes the query's active time
    # dimension into its producer grain as the window bucket; the TD must be
    # attributable from the root (design D5).
    window_td_key: Optional[ValueKey] = None
    if _window_kwarg_of(agg) is not None:
        active_td = prebound.main_time_key
        if active_td is None:
            raise ValueError(
                f"Windowed cross-model aggregate {alias!r} has no active time "
                f"dimension; add a single time_dimensions entry."
            )
        if not _attributable_from_root(
            host_path=_key_host_path(active_td), target_path=target_path,
            root_model=root_model, models_by_name=models_by_name,
            host_name=host_model.name,
        ):
            raise ValueError(
                f"Windowed cross-model aggregate {alias!r} needs the query's "
                f"active time dimension ('{_regroup_grain_name(active_td)}') "
                f"attributable from {root_name}, but it crosses a fanning join; "
                f"declare join cardinality or a covering unique key on the target."
            )
        window_td_key = _reroot_from_root(
            active_td, target_path=target_path, root_model=root_model,
            models_by_name=models_by_name, host_name=host_model.name,
        )

    inherited, dropped = _cross_model_inherited_filters(
        base_filters=base_filters_with_text, target_path=target_path,
        root_model=root_model, models_by_name=models_by_name,
        host_name=host_model.name,
    )

    root_bundle = bundle.model_copy(update={"source_model": root_model})
    root_scope = (
        ModelScope(source_model=root_model)
        if isinstance(scope, ModelScope) else scope
    )
    host_by_rerooted = {rr: hk for hk, rr in safe_pairs}
    if window_td_key is not None:
        # The bucket joins back on the consumer's own active TD.
        host_by_rerooted.setdefault(window_td_key, prebound.main_time_key)
    grain_keys = frozenset(rr for _, rr in safe_pairs)
    # The producer measure keeps the CANONICAL aggregate alias, not the
    # consumer's public name — a target-rooted producer roots at the aggregate's
    # model, whose own columns could shadow the public name (e.g. a ``pop``
    # measure over ``regions.pop``). The public name lands on the CONSUMER's
    # projection via the placeholder substitution.
    producer_prebound, ordered_pks = _regroup_producer_prebound(
        pks=grain_keys, aggs=[agg_R], model=root_model, bundle=root_bundle,
        inherited=inherited, n_date_range=0, window_td_key=window_td_key,
        explicit_types=(
            {agg_R: declared_type} if declared_type is not None else None
        ),
    )
    # A computed-dimension grain member (D4) nests its own producer inside this
    # one; a windowed producer runs its own windowed-CTE discovery — either way
    # re-enable regroup discovery for the producer's strict-subset inner
    # aggregates.
    enable_nested = window_td_key is not None or any(
        isinstance(rr, (ScalarCallKey, ArithmeticKey, TransformKey))
        or _is_local_partitioned_agg(rr)
        for rr in grain_keys
    )
    producer_plan = plan_query(
        query=StrictQueryCarrier(source_model=root_name, prebound=producer_prebound),
        bundle=root_bundle, scope=root_scope,
        stage_schemas=stage_schemas,
        disable_host_rooted_isolation=True,
        enable_producer_regroups=enable_nested,
        prebound=producer_prebound,
        producer_registry=producer_registry,
    )
    producer_answer_ids = list(producer_plan.projection)[len(ordered_pks):]
    answer_slot = _regroup_answer_slot_id(
        value_slots=[
            *producer_plan.aggregate_slots,
            *producer_plan.combined_expression_slots,
        ],
        key=agg_R,
        fallback=producer_answer_ids[0] if producer_answer_ids else None,
    )
    producer_grain_ids = list(producer_plan.projection)[: len(ordered_pks)]
    join_pairs: List[Tuple[ValueKey, SlotId]] = []
    for i, rr in enumerate(ordered_pks):
        slot_id = next(
            (s.id for s in producer_plan.row_slots if s.key == rr), None,
        )
        if slot_id is None:
            slot_id = producer_grain_ids[i]
        join_pairs.append((host_by_rerooted[rr], slot_id))
    _assert_attach_covers_producer_grain(
        joined_slot_ids={slot_id for _, slot_id in join_pairs},
        producer_grain_slot_ids=_producer_grain_slot_ids(producer_plan),
    )
    cm_attach_kwargs: Dict[str, Any] = {}
    if window_td_key is not None:
        cm_attach_kwargs["kernel"] = _trailing_window_kernel(
            producer_plan=producer_plan, agg_key=agg_R, n_date_range=0,
        )
    elif isinstance(agg_R, AggregateKey) and agg_R.agg in RANKED_AGGREGATIONS:
        cm_attach_kwargs["kernel"] = _ranked_kernel(
            producer_plan=producer_plan, agg_key=agg_R,
            root_model=root_model, bundle=root_bundle,
        )
    return RegroupAttachPlan(
        producer_plan=producer_plan,
        alias_hint=canonical_aggregate_alias(agg, profile="stage_formula"),
        attach_phase=attach_phase,
        join_pairs=join_pairs,
        substitutions=[RegroupSubstitution(
            placeholder=placeholder, producer_slot_id=answer_slot,
            original_key=agg,
        )],
        partition_display=[_regroup_grain_name(rr) for rr in ordered_pks],
        producer_root_model=root_name,
        dropped_filter_warnings=dropped,
        broadcast_measure=alias if broadcast else None,
        broadcast_dimensions=broadcast,
        **cm_attach_kwargs,
    )


def _discover_cross_model_combined(
    prebound: PreboundQuery,
) -> Tuple[List[AggregateKey], Dict[AggregateKey, str], Dict[AggregateKey, DataType]]:
    """Distinct cross-model aggregates reachable from a non-dimension measure,
    an order spec, or a query filter (the combined-attach roles). First-seen
    order; a directly-named measure maps to its public alias for naming, and its
    EXPLICIT (user-declared) type so the producer casts to it rather than the
    source column's; an inferred type never forces a producer cast (#347)."""
    seen: set = set()
    out: List[AggregateKey] = []
    alias: Dict[AggregateKey, str] = {}
    declared_type: Dict[AggregateKey, DataType] = {}
    for dm in prebound.declared_measures:
        if dm.is_dimension:
            continue
        vk = dm.bound.value_key
        for k in walk_value_keys(vk):
            if _is_cross_model_agg(k) and k not in seen:
                seen.add(k)
                out.append(k)
        if _is_cross_model_agg(vk):
            if dm.public_name is not None:
                alias.setdefault(vk, dm.public_name)
            if dm.type_is_explicit and dm.type is not None:
                declared_type.setdefault(vk, dm.type)
    for sp in prebound.order_specs:
        for k in walk_value_keys(sp.bound.value_key):
            if _is_cross_model_agg(k) and k not in seen:
                seen.add(k)
                out.append(k)
    for bf in prebound.bound_filters:
        for k in walk_value_keys(bf.value_key):
            if _is_cross_model_agg(k) and k not in seen:
                seen.add(k)
                out.append(k)
    return out, alias, declared_type


def _assert_total_routing(prebound: PreboundQuery) -> None:
    """D7 — post-discovery total-routing invariant. After the top-level regroup
    rewrite, every cross-model or partitioned aggregate leaf must have been
    disposed (producer substitution or an earlier explicit rejection); a
    survivor is an unrouted shape and raises here — never a silent drop."""
    roles: Tuple[Tuple[str, List[ValueKey]], ...] = (
        ("measure", [dm.bound.value_key for dm in prebound.declared_measures]),
        ("filter", [bf.value_key for bf in prebound.bound_filters]),
        ("order", [sp.bound.value_key for sp in prebound.order_specs]),
    )
    for role, keys in roles:
        for vk in keys:
            for k in walk_value_keys(vk):
                if _is_cross_model_agg(k) or (
                    isinstance(k, AggregateKey) and k.partition_keys is not None
                ):
                    raise ValueError(
                        f"Aggregate {_canonical_name(k)!r} in a {role} received "
                        f"no routing disposition (inline, producer substitution, "
                        f"or explicit rejection) — the planner cannot compile "
                        f"this shape."
                    )


def _structural_fingerprint(obj) -> Hashable:
    """Recursive hashable fingerprint with value equality; sets/dicts are
    order-canonicalized, unknown leaves fall back to their repr."""
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


def regroup_producer_identity(attach: RegroupAttachPlan) -> Hashable:
    """DEV-1838 D3 — interning identity of a regroup producer: the complete
    structural spec of the producer body plus its root, never the render-level
    attach coordinates (join keys / placeholders / phase). Equal identities
    denote one producer: one shared plan object, one rendered CTE."""
    return (
        attach.producer_root_model,
        _structural_fingerprint(attach.kernel),
        _structural_fingerprint(attach.producer_plan),
    )


def _intern_producer(
    attach: RegroupAttachPlan,
    registry: Optional[Dict[Hashable, PlannedQuery]],
) -> RegroupAttachPlan:
    """D3 — swap in the registered plan object for an already-seen identity so
    every consuming scope shares one producer node (slot ids match: identical
    structure plans identically)."""
    if registry is None:
        return attach
    ident = regroup_producer_identity(attach)
    shared = registry.get(ident)
    if shared is None:
        registry[ident] = attach.producer_plan
        return attach
    if shared is attach.producer_plan:
        return attach
    return attach.model_copy(update={"producer_plan": shared})


def _plan_regroups(  # NOSONAR(S3776) — one cohesive desugar: discover row (computed-dim) + combined (measure/order) partitioned aggregates, synthesize one producer per (partition set, phase), and rewrite the prebound to placeholders. The two phases share the registry / inherited-filter / substitution state; splitting scatters it.
    *,
    prebound: PreboundQuery,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
    stage_schemas: Dict[str, StageSchema],
    producer_source_model: Optional[str],
    in_producer: bool = False,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
    local_discovery: bool = True,
) -> Optional[Tuple[PreboundQuery, List[RegroupAttachPlan]]]:
    """Discover partitioned aggregates and desugar them into synthesized producer
    stages + reserved-leaf placeholders (DEV-1825 / DEV-1829).

    Two consumers: a partitioned aggregate INSIDE a computed dimension attaches
    at the base FROM (``attach_phase="row"``, DEV-1740); a partitioned MEASURE /
    composite / order-only leaf attaches at the combined SELECT
    (``attach_phase="combined"``, DEV-1829 — the position the retired DEV-1739
    cross-model plan occupied). ``None`` when there is nothing to
    regroup (the zero-cost common case, and the producer-recursion terminator).

    ``local_discovery=False`` (a plain disabled sub-plan, DEV-1838 2.5) keeps
    ONLY the cross-model discovery: local crossing/bare roots stay inline —
    the recursion guard — while cross-model roots still become target-rooted
    producers (their producer measure is local, so no recursion).
    """
    # Row-attach ROOTS: a partitioned aggregate, or (D4) a transform over one
    # evaluated at the producer grain. ``row_inner_aggs`` are the bare aggregates
    # inside dimensions — used for filter classification and combined exclusion.
    if local_discovery:
        row_aggs = dimension_regroup_roots(prebound.declared_measures)
        row_inner_aggs = dimension_partitioned_aggregates(
            prebound.declared_measures,
        )
        combined_aggs, public_alias_by_agg = combined_partitioned_aggregates(
            prebound.declared_measures, prebound.order_specs,
            row_agg_set=frozenset(row_inner_aggs),
            bound_filters=prebound.bound_filters,
        )
    else:
        row_aggs, row_inner_aggs = [], []
        combined_aggs, public_alias_by_agg = [], {}
    # DEV-1835 D1 — bare windowed / first-last measures join the COMBINED roots at
    # the full projected grain; the projected dimension keys define that grain.
    dim_dms, td_dms, _ = partition_declared_measures(
        declared_measures=prebound.declared_measures,
        n_dims=prebound.n_dims, n_time_dimensions=prebound.n_time_dimensions,
    )
    projected_dim_keys = [dm.bound.value_key for dm in dim_dms]
    projected_td_keys = [dm.bound.value_key for dm in td_dms]
    active_bucket = prebound.main_time_key

    # DEV-1838 D5 — a LOCAL aggregate whose inputs (Column.filter references,
    # args, kwargs, aggregation params) cross a join needs its own rows, so it
    # desugars onto a HOST-rooted producer like the bare windowed / first-last
    # roots — where the per-role safety gate then applies.
    _is_crossing_local_root = _crossing_local_root_predicate(
        scope=scope, bundle=bundle,
    )

    if local_discovery:
        bare_combined, bare_alias = _bare_combined_roots(
            prebound, extra_root=_is_crossing_local_root,
        )
        for agg in bare_combined:
            if agg not in combined_aggs:
                combined_aggs.append(agg)
        for agg, name in bare_alias.items():
            public_alias_by_agg.setdefault(agg, name)

    def _root_grain(agg: ValueKey) -> FrozenSet[ValueKey]:
        grain, windowed = _effective_root_grain(
            agg, projected_dim_keys=projected_dim_keys,
            projected_td_keys=projected_td_keys, active_bucket=active_bucket,
        )
        # The FULL grain for own-grain comparison folds the windowed axis back in
        # (``_effective_root_grain`` routes it through ``window_td_key``): a bare
        # windowed measure IS its producer's own answer, so its full grain equals
        # the producer grain and it must be excluded to terminate the recursion.
        if windowed and active_bucket is not None:
            grain = grain | {active_bucket}
        return grain

    # DEV-1839 D2 — inside a union-grain producer, an aggregate / root at EXACTLY
    # the producer's own grain compiles inline (a plain grouped aggregate, as it
    # did pre-DEV-1839 when the producer skipped desugaring entirely); only
    # STRICT-subset grains become nested attaches. Excluding own-grain roots
    # terminates the recursion — grains strictly decrease at every level.
    # DEV-1835 D9 — exception: a WINDOWED inner aggregate that feeds a transform
    # sits at the union's own grain (partition ∪ bucket) yet must still nest into
    # its own windowed producer at the partition grain, else the window frame is
    # dropped and it renders as a plain per-bucket sum. A windowed DIRECT answer
    # (a bare windowed measure) stays excluded — it renders inline as the
    # windowed CTE and terminates the recursion.
    if in_producer:
        own_grain = frozenset([*projected_dim_keys, *projected_td_keys])
        windowed_transform_inputs = {
            k
            for dm in prebound.declared_measures
            for tk in walk_value_keys(dm.bound.value_key)
            if isinstance(tk, TransformKey)
            for k in walk_value_keys(tk.input)
            if _window_kwarg_of(k) is not None
        }
        combined_aggs = [
            k for k in combined_aggs
            if _root_grain(k) != own_grain or k in windowed_transform_inputs
        ]
        row_aggs = [k for k in row_aggs if regroup_root_grain(k) != own_grain]
    # DEV-1836 — cross-model aggregates (source names another model) become
    # target-rooted producers; discovered here so they share the registry /
    # substitution with the local desugar and never fall to the retired
    # isolation-classifier dispatch. A cross-model root inside a computed dimension
    # is a ROW-phase target-rooted producer; it is pulled off the local ``row_aggs``
    # (which would wrongly root it at the host) and synthesized separately.
    cm_row = [k for k in row_aggs if _is_cross_model_agg(k)]
    row_aggs = [k for k in row_aggs if not _is_cross_model_agg(k)]
    cm_combined, cm_alias, cm_type = _discover_cross_model_combined(prebound)
    for agg, name in cm_alias.items():
        public_alias_by_agg.setdefault(agg, name)
    if not row_aggs and not combined_aggs and not cm_combined and not cm_row:
        return None
    # DEV-1824 (task 3.2) — a row attach (computed dimension) and a combined
    # attach (partitioned measure) coexist: the row producer joins into ``_base``
    # before aggregation and the combined producer joins at the combined SELECT
    # after it, in one flat WITH. The generator wires both (D10 ships the
    # same-aggregate-both-roles case as duplicate producers).
    # Codex F4 — a real column sharing the reserved placeholder prefix would
    # shadow a placeholder at render; reject it while a regroup is active. Scan
    # a downstream StageSchema's own columns too (CR): its ``columns`` carry
    # ``.name``, so when scope is a StageSchema (producer_model None) the guard
    # would otherwise skip an upstream ``__regroup__*`` column.
    producer_model = scope.source_model if isinstance(scope, ModelScope) else None
    reserved = reserved_prefix_columns(
        producer_model if isinstance(scope, ModelScope) else scope
    )
    if reserved:
        raise ValueError(
            f"Column(s) {reserved!r} use the reserved '__regroup__' prefix, which "
            f"collides with the regroup primitive's placeholders. Rename them."
        )
    registry = RegroupPlaceholderRegistry()
    mapping: Dict[ValueKey, ValueKey] = {
        agg: registry.placeholder_for(agg)
        for agg in (*row_aggs, *combined_aggs, *cm_row, *cm_combined)
    }
    # DEV-1836 — a cross-model computed-dimension aggregate is a ROW attach, so
    # its ROW conjuncts classify like the local ones (``classify_regroup_filter``).
    dim_agg_set = frozenset([*row_inner_aggs, *cm_row])

    inherited, n_inherited_date = _regroup_inherited_filters(prebound, dim_agg_set)

    # A combined producer keeps the CONSUMER's dimension order for its grain
    # (byte-identity with the DEV-1739 join-back, which narrowed the host's
    # projection-ordered shared grain). Row producers keep the alphabetical
    # default — their partition keys need not be query dimensions (DEV-1825).
    consumer_order: Dict[ValueKey, int] = {
        dm.bound.value_key: idx for idx, dm in enumerate([*dim_dms, *td_dms])
    }
    # A combined producer names its grain by the CONSUMER's dimension name so
    # the producer column + join-back are byte-identical to the DEV-1739 path
    # (e.g. a month time dimension is ``ordered_at``, not ``ordered_at_month``).
    grain_name_by_key: Dict[ValueKey, str] = {
        dm.bound.value_key: dm.declared_name
        for dm in [*dim_dms, *td_dms]
        if dm.declared_name is not None
    }

    def _combined_order(pks: FrozenSet[ValueKey]) -> List[ValueKey]:
        return sorted(pks, key=lambda k: consumer_order.get(k, len(consumer_order)))

    attaches: List[RegroupAttachPlan] = []
    for phase, phase_aggs, order_fn, alias_map, grain_names in (
        ("row", row_aggs, _regroup_partition_order, {}, {}),
        ("combined", combined_aggs, _combined_order, public_alias_by_agg,
         grain_name_by_key),
    ):
        if not phase_aggs:
            continue
        # Group roots by their PRODUCER grain (D4: a transform root's grain is
        # its inner aggregate's partition set; DEV-1835 D1: a bare windowed /
        # first-last root's is the full projected grain) and, for windowed /
        # ranked roots, their partition-free identity — so each distinct windowed
        # / ranked measure gets its own producer while a bare and an explicit
        # ``partition_by=`` twin at the same grain collapse to one (D6). Plain
        # partitioned aggregates keep the grain-only grouping (multiple share).
        groups: Dict[Tuple, List[ValueKey]] = {}
        group_meta: Dict[Tuple, Tuple[FrozenSet[ValueKey], bool]] = {}
        for agg in phase_aggs:
            grain, windowed = _effective_root_grain(
                agg, projected_dim_keys=projected_dim_keys,
                projected_td_keys=projected_td_keys, active_bucket=active_bucket,
            )
            ident = _windowed_or_ranked_identity(agg)
            # D5 — a crossing-input root needs its OWN producer: sharing one
            # base with another aggregate's crossed joins would fan its rows.
            if ident is None and _is_crossing_local_root(agg):
                ident = ("crossing", agg.source, agg.agg, tuple(agg.args),
                         tuple(agg.kwargs), agg.column_filter_key)
            gkey = (grain, ident)
            groups.setdefault(gkey, []).append(agg)
            group_meta[gkey] = (grain, windowed)
        for gkey, aggs in groups.items():
            pks, windowed = group_meta[gkey]
            pks = _prune_functionally_determined_grain(pks)
            # DEV-1835 D6 — one producer measure per partition-free identity: a
            # bare and an explicit ``partition_by=`` twin collapse to one column
            # both placeholders resolve to. A plain group's aggregates each have a
            # distinct identity, so ``producer_aggs == aggs`` (unchanged).
            canonical_by_identity: Dict = {}
            producer_aggs: List[ValueKey] = []
            canonical_of: Dict[ValueKey, ValueKey] = {}
            for agg in aggs:
                ident = _partition_free_identity(agg)
                canon = canonical_by_identity.get(ident)
                if canon is None:
                    canonical_by_identity[ident] = agg
                    producer_aggs.append(agg)
                    canon = agg
                canonical_of[agg] = canon
            canon_index = {c: i for i, c in enumerate(producer_aggs)}
            # D5 — per-role crossing-input safety for every host-rooted
            # producer answer, whatever its kernel.
            if producer_model is not None:
                for agg_k in producer_aggs:
                    if isinstance(agg_k, AggregateKey):
                        _assert_local_producer_inputs_safe(
                            agg=agg_k, host_model=producer_model,
                            bundle=bundle,
                            models_by_name={
                                m.name: m for m in bundle.referenced_models
                            },
                        )
            producer_prebound, ordered_pks = _regroup_producer_prebound(
                pks=pks, aggs=producer_aggs, model=producer_model, bundle=bundle,
                inherited=inherited, n_date_range=n_inherited_date,
                partition_order=order_fn, public_alias_by_agg=alias_map,
                explicit_types={
                    dm.bound.value_key: dm.type
                    for dm in prebound.declared_measures
                    if phase == "combined" and dm.type_is_explicit and dm.type is not None
                },
                grain_name_by_key=grain_names,
                window_td_key=prebound.main_time_key if windowed else None,
            )
            producer_plan = plan_query(
                query=StrictQueryCarrier(
                    source_model=producer_source_model, prebound=producer_prebound,
                ),
                bundle=bundle,
                scope=scope,
                stage_schemas=stage_schemas,
                disable_host_rooted_isolation=True,
                # A producer re-runs regroup discovery for its own STRICT-subset
                # inner aggregates (DEV-1839 union grain) and for a computed OR
                # bare-partitioned-aggregate dimension in its grain (DEV-1835 D4:
                # a band / scalar-expr / rank grain key, or a bare
                # ``amount:sum(partition_by=city)`` consumed AS a dimension, needs
                # a nested row attach so the producer can group by its value). A
                # windowed producer's own windowed measure is at the producer's
                # FULL grain (its active TD folded in by ``_root_grain``), so
                # own-grain exclusion drops it — enabling discovery there is a
                # no-op unless the grain carries such a dimension OR the answer is
                # a transform root (DEV-1835 D9: a windowed union-grain transform
                # broadcasts its plain strict-subset inner into the producer).
                enable_producer_regroups=(
                    (not windowed) or any(
                        isinstance(pk, (ScalarCallKey, ArithmeticKey, TransformKey))
                        or _is_local_partitioned_agg(pk)
                        for pk in pks
                    ) or any(isinstance(a, TransformKey) for a in producer_aggs)
                ),
                prebound=producer_prebound,
                producer_registry=producer_registry,
            )
            # DEV-1824 (task 3.1 hoist) — a producer that itself needs an internal
            # WITH (a ranked first/last, a windowed producer at the synthesized
            # active-TD grain (D5), or a transform-at-producer-grain (D4)) renders
            # its WITH, which the generator hoists into the one flat WITH with
            # allocator-uniquified base names. DEV-1839 D4 — a union-grain producer
            # MAY carry nested COMBINED attaches (its strict-subset inner
            # aggregates); those are admitted after structural validation.
            _validate_nested_producer_plan(
                producer_plan=producer_plan, producer_grain=pks,
            )
            # A bare aggregate root resolves to an aggregate slot; a transform
            # root (D4) resolves to the producer's combined-expression slot.
            producer_value_slots = [
                *producer_plan.aggregate_slots,
                *producer_plan.combined_expression_slots,
            ]
            # DEV-1839 — a union-grain producer desugars its OWN inner aggregates
            # to placeholders, so a transform root's producer slot key no longer
            # equals the pre-desugar ``agg``. Fall back to the producer's public
            # projection position: answers follow the grain, in ``aggs`` order.
            producer_answer_ids = list(producer_plan.projection)[len(ordered_pks):]
            substitutions = [
                RegroupSubstitution(
                    placeholder=mapping[agg],
                    producer_slot_id=_regroup_answer_slot_id(
                        value_slots=producer_value_slots, key=canonical_of[agg],
                        fallback=producer_answer_ids[canon_index[canonical_of[agg]]]
                        if canon_index[canonical_of[agg]] < len(producer_answer_ids)
                        else None,
                    ),
                    original_key=agg,
                )
                for agg in aggs
            ]
            # DEV-1835 D5 — match each grain key to its producer slot by structural
            # identity, falling back to projection POSITION when the producer
            # desugared a computed grain dimension (a band's inner aggregate became
            # a placeholder, so the producer's grain slot key no longer equals the
            # consumer's original expression). The producer projects its grain
            # first, in ``ordered_pks`` order, then its answer(s).
            producer_grain_ids = list(producer_plan.projection)[:len(ordered_pks)]
            join_pairs = []
            for i, pk in enumerate(ordered_pks):
                slot_id = next(
                    (s.id for s in producer_plan.row_slots if s.key == pk), None,
                )
                if slot_id is None:
                    slot_id = producer_grain_ids[i]
                join_pairs.append((pk, slot_id))
            _assert_attach_covers_producer_grain(
                joined_slot_ids={slot_id for _, slot_id in join_pairs},
                producer_grain_slot_ids=_producer_grain_slot_ids(producer_plan),
            )
            # D4 — a producer whose answer IS a windowed / ranked aggregate
            # carries the matching kernel (a transform root over such an inner
            # stays plain: the inner nests into its OWN attach).
            attach_kwargs: Dict[str, Any] = {}
            if (
                windowed
                and isinstance(producer_aggs[0], AggregateKey)
                and _window_kwarg_of(producer_aggs[0]) is not None
            ):
                attach_kwargs["kernel"] = _trailing_window_kernel(
                    producer_plan=producer_plan, agg_key=producer_aggs[0],
                    n_date_range=n_inherited_date,
                )
            elif (
                isinstance(producer_aggs[0], AggregateKey)
                and producer_aggs[0].agg in RANKED_AGGREGATIONS
            ):
                attach_kwargs["kernel"] = _ranked_kernel(
                    producer_plan=producer_plan, agg_key=producer_aggs[0],
                    root_model=(
                        producer_plan.render_source_model or bundle.source_model
                    ),
                    bundle=bundle,
                )
            attaches.append(RegroupAttachPlan(
                producer_plan=producer_plan,
                alias_hint=(
                    (canonical_aggregate_alias(aggs[0], profile="stage_formula")
                     if isinstance(aggs[0], AggregateKey) else None)
                    or getattr(aggs[0], "agg", None)
                    or getattr(aggs[0], "op", None)
                    or "regroup"
                ),
                attach_phase=phase,
                join_pairs=join_pairs,
                substitutions=substitutions,
                partition_display=[_regroup_grain_name(pk) for pk in ordered_pks],
                **attach_kwargs,
            ))

    # DEV-1836 — one target-rooted producer per distinct cross-model aggregate,
    # attached at the combined SELECT (the DEV-1739 position). Roles (measure /
    # order / filter) share one producer + placeholder, so a broadcast or dropped
    # filter warns once.
    host_model_for_cm = (
        scope.source_model if isinstance(scope, ModelScope) else bundle.source_model
    )
    models_by_name_cm = {m.name: m for m in bundle.referenced_models}
    base_filters_with_text = list(zip(
        prebound.bound_filters,
        prebound.bound_filter_texts
        + [None] * (len(prebound.bound_filters) - len(prebound.bound_filter_texts)),
    ))
    for phase, cm_aggs in (("combined", cm_combined), ("row", cm_row)):
        for agg in cm_aggs:
            attaches.append(_synthesize_cross_model_producer(
                agg=agg, placeholder=mapping[agg], attach_phase=phase,
                public_alias=public_alias_by_agg.get(agg), prebound=prebound,
                bundle=bundle, host_model=host_model_for_cm,
                models_by_name=models_by_name_cm,
                projected_dim_keys=projected_dim_keys,
                projected_td_keys=projected_td_keys,
                base_filters_with_text=base_filters_with_text, scope=scope,
                stage_schemas=stage_schemas,
                declared_type=cm_type.get(agg),
                producer_registry=producer_registry,
            ))

    # DEV-1839 — the ROW substitution (a transform root / bare aggregate → its
    # row placeholder, at the union grain) applies ONLY to computed DIMENSIONS.
    # A NON-dimension measure structurally equal to a row root (the dual-role
    # case: the same ``rank(...)`` as both a dimension and a measure) must keep
    # its query-grain evaluation — its INNER aggregates desugar to COMBINED
    # placeholders instead, so it is never rewritten to the union-grain producer.
    combined_mapping: Dict[ValueKey, ValueKey] = {
        agg: mapping[agg] for agg in (*combined_aggs, *cm_combined)
    }
    rewritten = PreboundQuery(
        declared_measures=[
            DeclaredMeasure(
                bound=BinderBoundExpr(
                    value_key=substitute_value_keys(
                        dm.bound.value_key,
                        mapping if dm.is_dimension else combined_mapping,
                    ),
                ),
                declared_name=dm.declared_name,
                public_name=dm.public_name,
                label=dm.label,
                canonical_alias=dm.canonical_alias,
                type=dm.type,
                type_is_explicit=dm.type_is_explicit,
                format=dm.format,
                description=dm.description,
                is_dimension=dm.is_dimension,
            )
            for dm in prebound.declared_measures
        ],
        bound_filters=[
            substitute_in_bound_filter(bf, mapping) for bf in prebound.bound_filters
        ],
        bound_filter_texts=list(prebound.bound_filter_texts),
        n_date_range=prebound.n_date_range,
        order_specs=[
            OrderSpec(
                bound=BinderBoundExpr(
                    value_key=substitute_value_keys(sp.bound.value_key, mapping),
                ),
                direction=sp.direction,
            )
            for sp in prebound.order_specs
        ],
        main_time_key=prebound.main_time_key,
        n_dims=prebound.n_dims,
        n_time_dimensions=prebound.n_time_dimensions,
        limit=prebound.limit,
        offset=prebound.offset,
        distinct_dimension_values=prebound.distinct_dimension_values,
    )
    # DEV-1838 D3 — intern every producer: a structurally identical producer
    # already planned in ANY scope of this query becomes the same plan object.
    attaches = [_intern_producer(a, producer_registry) for a in attaches]
    return rewritten, attaches


def plan_query(  # NOSONAR(S3776) — planner entry-point dispatcher. The DEV-1503 addition is a small trigger-predicate branch + a kwarg pass-through; the function's pre-existing complexity is owned by the multi-stage scope / bundle / projection / filter-routing wiring it orchestrates and is tracked as a separate refactor.
    *,
    query: Union[SlayerQuery, StrictQueryCarrier],
    bundle: ResolvedSourceBundle,
    scope: Optional[Union[ModelScope, StageSchema]] = None,
    stage_schemas: Optional[Dict[str, StageSchema]] = None,
    disable_host_rooted_isolation: bool = False,
    enable_producer_regroups: bool = False,
    prebound: Optional[PreboundQuery] = None,
    producer_registry: Optional[Dict[Hashable, PlannedQuery]] = None,
) -> PlannedQuery:
    """Compile one query into a typed ``PlannedQuery``.

    ``scope`` defaults to a ``ModelScope`` over ``bundle.source_model``;
    pass an explicit ``StageSchema`` to bind against an upstream stage.
    ``stage_schemas`` is a name → StageSchema map used by
    ``plan_stages`` to wire multi-stage references.

    ``prebound`` (DEV-1742 §5.4) supplies the bind product directly, skipping
    the parser. ``query`` is then a ``StrictQueryCarrier`` holding only the
    post-bind scalars the planner still needs — anything else it is asked for
    raises, so a new read cannot silently fall back to a default.

    ``disable_host_rooted_isolation`` (DEV-1503/DEV-1709, retargeted by
    DEV-1838) suppresses the LOCAL half of the regroup desugar — the
    isolation of a LOCAL aggregate whose ``Column.filter`` or arguments
    cross a join. A producer sub-plan contains the same crossing measure and
    would otherwise recurse forever; inside the producer the crossing input
    renders inline (base-pull), legal because the CTE is the aggregate's own
    scope. Cross-model roots (``source.path`` non-empty) always desugar —
    a target-rooted producer's own measure is local, so there is no
    recursion to guard.
    """
    stage_schemas = stage_schemas or {}
    # DEV-1838 D3 — one interning registry per top-level plan; nested producer
    # plan_query calls thread it down so every scope shares it.
    if producer_registry is None:
        producer_registry = {}

    if scope is None:
        scope = _resolve_scope(
            query=query, bundle=bundle, stage_schemas=stage_schemas,
        )

    # The generator must render this stage's FROM / joins against the SAME
    # model the binder used. For a ModelScope that's the (possibly overlaid /
    # synthetic) host; for a StageSchema chain stage it's None (the generator
    # builds a synthetic model from the upstream schema).
    render_source_model = (
        scope.source_model if isinstance(scope, ModelScope) else None
    )

    if prebound is None:
        # A raw SlayerQuery is the only bindable input; a StrictQueryCarrier
        # arrives only paired with its own prebound product (§5.4), so reaching
        # the parser with one is a wiring bug, not a fall-through.
        assert isinstance(query, SlayerQuery)
        prebound = bind_query_inputs(
            query=query, bundle=bundle, scope=scope,
            stage_schemas=stage_schemas,
        )
    # DEV-1824 (D7) — split top-level AND conjuncts of any filter that
    # references a LOCAL partitioned aggregate, deciding per-conjunct placement
    # on the pre-substitution tree. ``combined_filter_indices`` (into the rebuilt
    # bound_filters, order-preserved through the substitution below) route to the
    # outer WHERE. Runs when planning the real query, not a synthesized producer.
    combined_filter_indices: List[int] = []
    if not disable_host_rooted_isolation:
        prebound, combined_filter_indices = _split_partitioned_filter_conjuncts(
            prebound,
            crossing_root=_crossing_local_root_predicate(
                scope=scope, bundle=bundle,
            ),
        )
    declared_measures = list(prebound.declared_measures)
    bound_filters = list(prebound.bound_filters)
    n_date_range = prebound.n_date_range
    order_specs = list(prebound.order_specs)
    active_td_key = prebound.main_time_key
    n_dims = prebound.n_dims
    n_tds = prebound.n_time_dimensions
    distinct_dimension_values = prebound.distinct_dimension_values

    # DEV-1824 / D5 — the deferred partition_by shape guards (window= /
    # first-last / nested-transform / in-filter) run on the ORIGINAL,
    # pre-substitution trees, so the generalized regroup desugar below cannot
    # hide a deferred shape behind a placeholder. The computed-dimension
    # (row-attach) aggregates are excluded — the desugar legitimately consumes
    # them, and a legitimate filter over a computed dimension must NOT raise.
    _orig_row_aggs = frozenset(
        dimension_partitioned_aggregates(declared_measures),
    )
    _guard_partitioned_measures(
        measure_vks=[dm.bound.value_key for dm in declared_measures],
        filter_vks=[bf.value_key for bf in bound_filters],
        order_vks=[sp.bound.value_key for sp in order_specs],
        exclude=_orig_row_aggs,
    )

    # DEV-1825 / DEV-1829 — desugar partitioned aggregates (inside computed
    # dimensions → row attach; as measures / composites / order leaves →
    # combined attach) into synthesized producer stages + reserved-leaf
    # placeholders. Runs AFTER the guard above so a deferred shape has already
    # raised; the substituted trees then carry no partitioned aggregate.
    #
    # Skipped when planning a synthesized producer / inlined sub-plan
    # (``disable_host_rooted_isolation``): there the partitioned aggregate IS the
    # producer's own answer and renders inline as a plain grouped aggregate — a
    # recursive desugar would re-discover it and synthesize a producer forever.
    #
    # DEV-1839 D3 — a UNION-grain producer re-enables regroup discovery
    # (``enable_producer_regroups``) so its strict-subset inner aggregates desugar
    # into nested combined attaches; host-rooted isolation stays OFF. Own-grain
    # exclusion (``in_producer``) keeps aggregates AT the producer grain inline
    # and terminates the recursion (grains strictly decrease).
    regroup_attach_plans: List[RegroupAttachPlan] = []
    if isinstance(query.source_model, str):
        _producer_source_model = query.source_model
    elif render_source_model is not None:
        _producer_source_model = render_source_model.name
    else:
        _producer_source_model = None
    # DEV-1838 (2.5) — the desugar always runs. The LOCAL half is suppressed
    # in a plain disabled sub-plan (the recursion guard: a producer contains
    # its own crossing measure); cross-model roots always desugar — a
    # target-rooted producer's own measure is local, so no recursion.
    regroup_result = _plan_regroups(
        prebound=prebound, scope=scope, bundle=bundle,
        stage_schemas=stage_schemas,
        producer_source_model=_producer_source_model,
        in_producer=enable_producer_regroups,
        producer_registry=producer_registry,
        local_discovery=(
            not disable_host_rooted_isolation or enable_producer_regroups
        ),
    )
    if regroup_result is not None:
        prebound, regroup_attach_plans = regroup_result
        declared_measures = list(prebound.declared_measures)
        bound_filters = list(prebound.bound_filters)
        n_date_range = prebound.n_date_range
        order_specs = list(prebound.order_specs)
        active_td_key = prebound.main_time_key
        n_dims = prebound.n_dims
        n_tds = prebound.n_time_dimensions
        distinct_dimension_values = prebound.distinct_dimension_values
    # D7 — at the top consumer level every cross-model / partitioned leaf must
    # now be a placeholder; sub-plans (producers, host-rooted recursion) render
    # theirs inline by design and are exempt.
    if not disable_host_rooted_isolation and not enable_producer_regroups:
        _assert_total_routing(prebound)

    # SlayerModel.filters — Mode-A SQL, always-applied WHERE. Scope-derived
    # (see ``bind_query_inputs``), so a pre-bound sub-plan picks up its OWN
    # model's filters.
    text_filter_entries: List[FilterPhase] = []
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        for j, mf in enumerate(scope.source_model.filters or []):
            text_filter_entries.append(_validate_model_filter(
                mf=mf, idx=j, model=scope.source_model,
            ))

    source_col_names = _source_column_names(scope)
    host_model_name = _host_model_name(scope)

    # DEV-1714 Stage 10 — windowed-measure guards on the ORIGINAL (pre-
    # projection) value-key trees. Raises on unsupported shapes (non-sum/avg,
    # no time dim, cross-model, transform, composite, hidden, mixed, malformed
    # duration); returns the set of cleanly-selected windowed AggregateKeys.
    # (The partition_by deferred-shape guard already ran on the pre-substitution
    # trees above — D5.)
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

    # DEV-1543: ``distinct_dimension_values=False`` asks for RAW ROWS, so no
    # measure reference may appear anywhere in the query. ``SlayerQuery``'s
    # validator already rejects the cheap structural cases (a non-empty
    # ``measures``, or no dimensions at all) without needing a model; the
    # remaining references hide in ``filters`` and ``order``, which only
    # resolve once bound.
    #
    # The typed pipeline checks this structurally instead of re-parsing the
    # filter/order TEXT the way the legacy stack did: after binding, ANY
    # aggregate-phase slot in a raw-rows query can only have come from a
    # filter or an order item, since measures were rejected upstream. Without
    # this the query silently aggregates — a ``filters=["amount:sum > 100"]``
    # raw-rows query materialised a hidden aggregate and emitted
    # ``GROUP BY ... HAVING ...``, the exact auto-aggregation the flag exists
    # to turn off.
    if distinct_dimension_values is False and agg_slots:
        offender = _canonical_name(agg_slots[0].key)
        raise DistinctDimensionValuesError(
            f"distinct_dimension_values=False rejects measure references, but "
            f"this query references the aggregation {offender!r} in its "
            f"filters or order. Either remove the measure reference, or set "
            f"distinct_dimension_values=True (the default) to keep the "
            f"auto-aggregating behaviour."
        )

    # DEV-1714 Stage 10 / DEV-1838 D4 — detect the selected windowed slots.
    # The window time dimension is the query's resolved active TD; the
    # emission itself lives on the attach kernel.
    active_td_slot_id = (
        projection.registry.find_by_key(active_td_key)
        if active_td_key is not None
        else None
    )
    windowed_slot_ids = _windowed_slot_id_set(
        selected_windowed=selected_windowed,
        registry=projection.registry,
        active_td_slot_id=active_td_slot_id,
    )

    # DEV-1835 — the post-projection windowed-mixed-filter guard (the DEV-1504 G7
    # twin) dissolved: a bare windowed filter reference is a combined regroup
    # placeholder by the time filters route, and its top-level AND conjuncts are
    # split per-scope by ``_split_partitioned_filter_conjuncts`` (an OR spanning
    # scopes still raises the no-common-scope directive from ``conjunct_scope``).

    # DEV-1712 (Law 2) / DEV-1703 Phase 1: plan-time classification of every
    # ORDER BY target that is not a declared/public slot.
    #
    # ONE RULE: an order-only ref resolves exactly like a filter ref. Law 1
    # pulls whatever joins it crosses into the scope that owns its rows — even
    # when ORDER BY is the only thing referencing it — and the ref is then
    # emitted in whatever form that scope makes legal:
    #   * order-only AGGREGATE (local or cross-model) -> hidden materialised
    #     slot, ordered by its alias (unchanged);
    #   * row column, UNGROUPED query -> split ``orders.created_at`` /
    #     ``customers__regions.name`` emission in the generator's
    #     ``_apply_planned_order_limit`` (the row IS the grain, so the
    #     bare reference is legal);
    #   * LOCAL row column, GROUPED query -> the bare reference is NOT legal
    #     (the column is not in GROUP BY), so it materialises as a hidden
    #     direction-aware aggregate slot (``:min`` for ASC, ``:max`` for DESC)
    #     and the order entry is repointed at it. Both are order-preserving
    #     per group and portable across every Tier-1 dialect;
    #   * JOINED row column, GROUPED query -> the same wrap, marked
    #     ``grain="host"`` (DEV-1747 D2). The marker is what separates WHERE
    #     the value is READ (through the join, per ``source.path``) from WHERE
    #     it is GROUPED (per host row-group). Without it a path-bearing source
    #     always routed to a TARGET-rooted CTE, which for a host-grain sort key
    #     degenerates to a scalar CROSS JOIN — every group gets the same global
    #     value and the sort silently does nothing. That case used to be
    #     rejected outright rather than sorted wrongly;
    #   * transform / composite -> materialised as a hidden slot and ordered at
    #     the outer wrap (DEV-1733), same Law-2 discipline as aggregates.
    #
    # The hidden MAX is interned post-bind, so the bind-time aggregation gate
    # (PK columns, ``allowed_aggregations``, per-type defaults) deliberately
    # does not apply: the caller asked to SORT by a column, not to aggregate
    # it, and a sort must not fail because ``max`` is not whitelisted on the
    # column being sorted.
    _has_grouping = bool(agg_slots) or (
        bool(n_dims or n_tds) and distinct_dimension_values
    )
    # ORDER BY targets rewritten to a hidden aggregate wrap, keyed by
    # (original key, DIRECTION). DEV-1747 D10 makes the wrap direction-aware,
    # so ``ORDER BY a ASC, a DESC`` needs MIN(a) and MAX(a) — two different
    # values over one column. Keying by the value key alone would collapse them
    # onto whichever slot was interned first.
    order_key_remap: Dict[Tuple[ValueKey, str], ValueKey] = {}
    # DEV-1838 D5 — host-grain / crossing wraps synthesized as late producers,
    # and the slots they answer (skipped by the isolation loop below).
    late_wrap_keys: set = set()
    late_attach_answered: set = set()
    host_model_for_wraps = (
        scope.source_model if isinstance(scope, ModelScope) else None
    )
    for spec in order_specs:
        okey = spec.bound.value_key
        osid = projection.registry.find_by_key(okey)
        if osid is not None and not projection.registry.get(osid).hidden:
            continue  # declared / projected output — orders on a real column
        if isinstance(okey, AggregateKey):
            continue  # hidden aggregate (local base or cross-model CTE)
        if isinstance(okey, ColumnKey) and okey.leaf.startswith(REGROUP_LEAF_PREFIX):
            # DEV-1829 — a combined regroup placeholder resolves via its ``_cm_``
            # producer at the combined SELECT (like a cross-model aggregate), so
            # it must NOT be wrapped into a hidden MIN/MAX over ``_base``.
            continue
        if isinstance(okey, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
            if not _has_grouping:
                continue  # raw-rows query -> split emission, no wrap needed
            path = _row_key_path(okey)
            # A TimeTruncKey is not a legal aggregate source; wrap its
            # underlying column instead. DATE_TRUNC is monotonic
            # non-decreasing, so the wrap over the trunc and over the raw
            # column sort a group identically.
            src = okey.column if isinstance(okey, TimeTruncKey) else okey
            # DEV-1747 D10 — ASC orders each group by its MINIMUM and DESC by
            # its MAXIMUM: the extreme the direction actually puts first.
            # An unconditional MAX (the pre-DEV-1747 behaviour) sorts ASC by
            # each group's LARGEST member, which is not what the user asked
            # for whenever groups overlap in range.
            wrap_key = AggregateKey(
                source=src,
                agg="min" if spec.direction == "asc" else "max",
                # DEV-1747 D2 — a JOINED sort key is host-grain: it must be
                # computed in a CTE rooted at the HOST with the crossed join
                # pulled inside, grouped on the query grain. Routing it to a
                # target-rooted CTE (which a bare path-bearing source does)
                # degenerates to a scalar CROSS JOIN, giving every group the
                # same global value.
                grain="host" if path else "target",
            )
            if projection.registry.find_by_key(wrap_key) is None:
                projection.registry.intern(
                    key=wrap_key,
                    declared_name=_canonical_name(wrap_key),
                    hidden=True,
                    phase=wrap_key.phase,
                )
            order_key_remap[(okey, spec.direction)] = wrap_key
            # DEV-1838 D5 — a JOINED (host-grain) wrap, or a local wrap whose
            # source ``Column.sql`` crosses a join, is a HOST-rooted producer:
            # synthesized late (its key only exists post-projection) and
            # attached at the combined SELECT like any regroup producer.
            _wrap_crosses = path or (
                host_model_for_wraps is not None
                and _local_crossing_input_paths(
                    key=wrap_key, bundle=bundle,
                    host_model=host_model_for_wraps,
                )
            )
            if _wrap_crosses and wrap_key not in late_wrap_keys:
                late_wrap_keys.add(wrap_key)
                regroup_attach_plans.append(_intern_producer(
                    _synthesize_wrap_attach(
                        wrap_key=wrap_key, prebound=prebound, scope=scope,
                        bundle=bundle,
                        stage_schemas=stage_schemas,
                        producer_registry=producer_registry,
                        producer_source_model=_producer_source_model,
                        row_attaches=[
                            a for a in regroup_attach_plans
                            if a.attach_phase == "row"
                        ],
                    ),
                    producer_registry,
                ))
                wrap_sid = projection.registry.find_by_key(wrap_key)
                if wrap_sid is not None:
                    late_attach_answered.add(wrap_sid)
        # DEV-1733: TransformKey / ArithmeticKey / ScalarCallKey — an inline
        # transform or composite expression referenced only in ORDER BY. These
        # materialise as hidden slots (a step CTE on the transform path, a
        # trimmed base-SELECT column on the no-transform path, an inline
        # combined-SELECT term when an operand is cross-model or windowed) and
        # order at the outer wrap. Stage 8 rejected them here; nothing left to
        # reject.

    # Re-bucket: the pass above may have interned hidden ``:max`` order slots,
    # which must reach the PlannedQuery's aggregate bucket (and hence the
    # generator's slot maps) like any other aggregate.
    if order_key_remap:
        row_slots, agg_slots, combined_slots = _bucket_slots(
            projection.registry.slots,
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
    # DEV-1745 (W4 / D9) — the per-filter structural reachability summary, in
    # THIS plan's coordinate system. Computed once here and carried on the plan.
    reachability_anchor_model = render_source_model or bundle.source_model
    source_relation = (
        query.source_model
        if isinstance(query.source_model, str)
        else host_model_name
    )
    filter_reachability: List[FilterReachability] = []
    # One expansion cache for the whole plan — the two visitors ask for the
    # same derived column's expansion, and so does every filter that mentions it.
    reachability_cache: dict = {}
    for fp in filters_by_phase:
        if fp.expression is None:
            continue
        filter_reachability.append(FilterReachability(
            filter_id=fp.id,
            crossed_join_paths=compute_key_join_paths(
                key=fp.expression.value_key,
                anchor_model=reachability_anchor_model,
                anchor_relation=source_relation,
                bundle=bundle,
                cache=reachability_cache,
            ),
            has_host_local_ref=key_has_host_local_ref(
                key=fp.expression.value_key,
                anchor_model=reachability_anchor_model,
                anchor_relation=source_relation,
                bundle=bundle,
                cache=reachability_cache,
            ),
        ))
    # DEV-1838 (2.5) — the isolation classifier is gone: every aggregate that
    # needs its own rows desugars onto a regroup producer before this point.
    # A D8 backstop (raise, not assert) catches a cross-model slot that
    # somehow survived the desugar rather than letting it render as a
    # fan-multiplying inline join.
    for slot in agg_slots:
        if slot.id in late_attach_answered:
            # Answered by a late host-grain wrap producer (D5).
            continue
        key = slot.key
        if (
            isinstance(key, AggregateKey)
            and getattr(key.source, "path", ())
            and getattr(key, "grain", "target") != "host"
        ):
            raise RuntimeError(
                f"Cross-model aggregate slot {slot.id!r} survived the regroup "
                f"desugar (DEV-1838 D8); every cross-model aggregate must "
                f"become a target-rooted producer."
            )

    # Loop-invariant lookups for order-scope classification, hoisted out of the
    # per-spec loop below (none depend on ``spec``).
    order_cross_model_slot_ids: set = set()
    # DEV-1829 — a combined regroup placeholder lives in its ``_cm_`` producer,
    # so an ORDER BY over it (directly, or through a composite) resolves at the
    # combined SELECT exactly like a cross-model aggregate. Feeding its slot ids
    # to the cross-model set covers both the direct (CROSS_MODEL_CTE) and the
    # composite (OUTER_COMPOSITE) classifications.
    for _rap in regroup_attach_plans:
        if _rap.attach_phase != "combined":
            continue
        for _sub in _rap.substitutions:
            _psid = projection.registry.find_by_key(_sub.placeholder)
            if _psid is not None:
                order_cross_model_slot_ids.add(_psid)
    order_windowed_slot_ids = set(windowed_slot_ids)
    order_slot_by_key = {s.key: s.id for s in projection.registry.slots}
    order_entries = []
    for spec in order_specs:
        # A grouped row-column sort key was rewritten to a hidden direction-
        # aware aggregate wrap above; order on that slot, not the bare row key.
        okey = order_key_remap.get(
            (spec.bound.value_key, spec.direction), spec.bound.value_key,
        )
        sid = projection.registry.find_by_key(okey)
        if sid is None:
            # DEV-1733: an order target that reached here without a slot would
            # be SILENTLY DROPPED — the query runs unsorted and returns wrong
            # rows with no error. That was the original `change(...)` /
            # scalar-call bug, and the entry-point relaxation makes new key
            # shapes reachable (e.g. a top-level `IN` / `BETWEEN` predicate,
            # which `_iter_slot_deps` treats as WHERE-inlined and never slots).
            # Fail loudly for ANY unslotted shape rather than enumerating them,
            # so this whole bug class cannot come back.
            raise ValueError(
                f"ORDER BY expression is not supported: "
                f"{type(spec.bound.value_key).__name__} has no materialisable "
                f"slot. Order by an aggregate, a transform, a composite "
                f"arithmetic / scalar expression, a dimension, or declare the "
                f"expression as a measure and order by its name."
            )
        order_slot = projection.registry.get(sid)
        order_entries.append(OrderEntry(
            slot_id=sid,
            direction=spec.direction,
            scope=_classify_order_scope(
                slot=order_slot,
                cross_model_slot_ids=order_cross_model_slot_ids,
                windowed_slot_ids=order_windowed_slot_ids,
                public_projection=projection.public_projection,
                slot_by_key=order_slot_by_key,
            ),
            phase=order_slot.key.phase,
        ))

    transform_layers = _emit_transform_layers(slots=projection.registry.slots)
    stage_schema = _emit_stage_schema(
        stage_name=query.name, projection=projection,
    )
    # Stage 7b.10 — the active TD's slot id (``active_td_slot_id``) is resolved
    # right after projection above so the windowed-plan builder can use it.

    # DEV-1732 — the frame-bound column set: raw columns of this stage's
    # NON-HIDDEN time dimensions. Computed once and carried on the plan so the
    # windowed ``_src`` path (below) and the generator's ``time_shift``
    # shifted-CTE path read the SAME set.
    frame_bound_columns = _frame_bound_columns(row_slots=row_slots)

    # DEV-1824 (D7) — a filter conjunct routed to the COMBINED scope references a
    # partitioned-aggregate placeholder resolvable only after attachment, so it
    # renders at the outer WHERE (never the base). ``bound_filters[i] -> f{i}``.
    outer_where_filter_ids: List[BoundFilterId] = []
    for idx in combined_filter_indices:
        fid = f"f{idx}"
        if fid not in outer_where_filter_ids:
            outer_where_filter_ids.append(fid)

    # DEV-1835 — a COMBINED regroup attach (a partitioned / bare windowed / bare
    # first-last measure) is an isolated aggregate too: its value lives in the
    # producer CTE and attaches at the combined SELECT, never in ``_base``. So a
    # projection of only such placeholders is still an empty-base spine.
    regroup_combined_slot_ids: set = set()
    for attach in regroup_attach_plans:
        if attach.attach_phase != "combined":
            continue
        for sub in attach.substitutions:
            sid = projection.registry.find_by_key(sub.placeholder)
            if sid is not None:
                regroup_combined_slot_ids.add(sid)
    empty_base_plan = _plan_empty_base_grain(
        projection=projection.public_projection,
        agg_slots=agg_slots,
        windowed_slot_ids=windowed_slot_ids,
        regroup_combined_slot_ids=regroup_combined_slot_ids,
        order_entries=order_entries,
        filters_by_phase=filters_by_phase,
        outer_where_filter_ids=outer_where_filter_ids,
    )

    planned = PlannedQuery(
        source_relation=source_relation,
        row_slots=row_slots,
        aggregate_slots=agg_slots,
        regroup_attach_plans=regroup_attach_plans,
        combined_expression_slots=combined_slots,
        transform_layers=transform_layers,
        filters_by_phase=filters_by_phase,
        projection=projection.public_projection,
        order=order_entries,
        limit=prebound.limit,
        offset=prebound.offset,
        stage_schema=stage_schema,
        active_time_dimension_slot_id=active_td_slot_id,
        render_source_model=render_source_model,
        distinct_dimension_values=distinct_dimension_values,
        frame_bound_columns=frame_bound_columns,
        outer_where_filter_ids=outer_where_filter_ids,
        filter_reachability=filter_reachability,
        empty_base_plan=empty_base_plan,
    )
    return planned




def _plan_empty_base_grain(
    *,
    projection: List[SlotId],
    agg_slots: list,
    windowed_slot_ids: AbstractSet[SlotId],
    order_entries: list,
    filters_by_phase: list,
    outer_where_filter_ids: List[BoundFilterId],
    regroup_combined_slot_ids: Optional[set] = None,
) -> "EmptyBaseGrainPlan | None":
    """Decide the DEV-1503 empty-base spine at plan time (§5.12).

    The host base has nothing of its own exactly when every value the query
    asks for is an isolated aggregate: no row slots, no host-LOCAL aggregates,
    no combined expressions, and nothing ordered that would have to be
    materialised there. The generator used to re-derive this from its own
    render order; deciding it here keeps the policy on the plan (P-D).

    ``host_filter_ids`` are the ROW-phase filters that remain host-local — not
    routed into a ``_cm_*`` CTE and not lifted to the outer WHERE. Without them
    the spine would aggregate across host rows the user filtered out.
    """
    isolated = set(windowed_slot_ids)
    isolated |= (regroup_combined_slot_ids or set())
    if not projection or any(sid not in isolated for sid in projection):
        return None
    # A host-LOCAL aggregate would have to be computed in ``_base``, which then
    # has a column of its own and is not a placeholder spine.
    if any(slot.id not in isolated for slot in agg_slots):
        return None
    # An order target that is not itself isolated must be materialised in
    # ``_base`` too, for the same reason.
    if any(entry.slot_id not in isolated for entry in order_entries):
        return None
    routed: set = set(outer_where_filter_ids)
    host_filter_ids = [
        fp.id
        for fp in filters_by_phase
        if fp.phase == Phase.ROW
        and fp.id not in routed
        and (fp.expression is not None or fp.text is not None)
    ]
    return EmptyBaseGrainPlan(host_filter_ids=host_filter_ids)


def _frame_bound_columns(*, row_slots: list) -> List[ValueKey]:
    """Raw column keys of the stage's NON-HIDDEN time dimensions (DEV-1732).

    An explicit relational bound on one of these is a FRAME bound — the
    caller restating what ``TimeDimension.date_range`` expresses — and is
    stripped from CTEs that must read outside the frame. A bound on any other
    column, temporal or not, is a population filter and is left alone.

    Hidden ``TimeTruncKey`` slots are excluded deliberately, and the exclusion
    is load-bearing rather than cosmetic: the windowed emission skips hidden
    row slots when deriving ``other_time_dimension_slot_ids``, so a hidden time
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
) -> List[PlannedQuery]:
    """Plan a multi-stage DAG. Topo sort, then plan each stage against its own
    resolved source + the synthetic models of its already-planned siblings."""
    if len(queries) == 1:
        return [plan_query(
            query=queries[0],
            bundle=bundle,
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


def _format_description_for_measure_formula(
    *, scope: Union[ModelScope, StageSchema], bound,
) -> Tuple[Optional[NumberFormat], Optional[str]]:
    """Scope adapter over ``measure_key_format_description`` — a StageSchema
    scope has no source model to lift a column's display contract from."""
    if not isinstance(scope, ModelScope) or scope.source_model is None:
        return None, None
    return measure_key_format_description(
        model=scope.source_model, key=bound.value_key,
    )


def _type_for_measure_formula(
    *, scope: Union[ModelScope, StageSchema], bound,
) -> Optional[DataType]:
    """Scope adapter over ``measure_key_type``.

    Declared ``ModelMeasure.type`` overrides — that flow runs through
    ``expand_model_measures`` so the bound key already carries the declared
    type via the source column lookup.
    """
    if not isinstance(scope, ModelScope) or scope.source_model is None:
        return None
    return measure_key_type(model=scope.source_model, key=bound.value_key)


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
    type. Returning ``None`` for joined refs (the old behaviour) made the
    query-backed virtual-model wrap coerce them to ``DOUBLE`` (its
    ``sc.type or DataType.DOUBLE`` fallback), mistyping joined string /
    temporal dimensions on the persisted virtual model.
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
    for downstream typing (the virtual-model ``DOUBLE`` coercion) and must not
    change — so
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


def _reject_computed_dim_name_collision(
    *, name: str, query: SlayerQuery, scope: Union[ModelScope, StageSchema],
) -> None:
    """A computed dimension's name must not shadow a resolvable column / measure
    (fail-closed: a shadowed reference in a filter or order would be ambiguous)."""
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        model = scope.source_model
        if model.get_column(name) is not None or model.get_measure(name) is not None:
            raise ValueError(
                f"Computed dimension name {name!r} collides with an existing "
                f"column or measure on model {model.name!r}. Choose a different "
                f"name."
            )
    for m in (query.measures or []):
        if m.name == name:
            raise ValueError(
                f"Computed dimension name {name!r} collides with a query measure "
                f"of the same name. Choose a different name."
            )


def _guard_computed_dimension(*, d: ComputedDimension, bound, query: SlayerQuery) -> None:  # NOSONAR(S3776) — sequential fail-closed guard checks over one shared walk (all_keys / transforms / inner_aggs); each arm raises its own contract error, and extracting them scatters the shared state and the ordered narrative.
    """Grain-self-containment rules for a computed dimension (DEV-1740/1824).

    Every aggregate must carry ``partition_by=`` (else the group key is a pure
    function of the query's own dimensions and adds no grouping); LOCAL
    transforms, ``first``/``last`` and ``window=`` are LIFTED when so grained
    (DEV-1824). A transform over aggregates at DIFFERENT grains unions the grains
    and broadcasts each to the union (DEV-1839 D1); a windowed / ``first`` /
    ``last`` inner aggregate joins that union at its effective grain — windowed
    adds the query's bucketed time dimension, first/last is timeless (DEV-1835
    D9). Still fail closed: a cross-model aggregate source and an
    aggregate-referencing dimension with raw-rows mode. The temporal-axis
    containment rule (D9) runs later, once ``time_key`` is attached.
    """
    all_keys = list(walk_value_keys(bound.value_key))
    transforms = [k for k in all_keys if isinstance(k, TransformKey)]
    for tk in transforms:
        inner_aggs = [
            k for k in walk_value_keys(tk.input) if isinstance(k, AggregateKey)
        ]
        # Grain-self-containment (measure⇔dimension symmetry): a transform is
        # legal in a dimension only over an explicitly-grained aggregate.
        if not inner_aggs or any(a.partition_keys is None for a in inner_aggs):
            raise NotImplementedError(
                f"A transform inside computed dimension {d.name!r} must wrap an "
                f"explicitly-grained aggregate — declare partition_by= on the "
                f"aggregate it transforms (DEV-1824)."
            )
    # DEV-1824 (task 3.7 / D4) — a grain-self-contained transform-in-dimension is
    # lifted: its row-attach producer computes the transform at the producer grain.
    aggs = [k for k in all_keys if isinstance(k, AggregateKey)]
    if not aggs:
        return  # row-level (B1)
    if not query.distinct_dimension_values:
        raise DistinctDimensionValuesError(
            f"Computed dimension {d.name!r} references an aggregate, so it cannot "
            f"be used with distinct_dimension_values=False (raw rows). Remove the "
            f"flag (the default aggregates) or drop the aggregate from the "
            f"dimension."
        )
    for agg in aggs:
        if agg.partition_keys is None:
            raise ValueError(
                f"The aggregate inside computed dimension {d.name!r} must declare "
                f"the grain it aggregates over with partition_by=, e.g. "
                f"'CASE WHEN amount:sum(partition_by=city) > 5000 THEN 1 ELSE 0 END'. "
                f"Without partition_by the group key is a function of the query's "
                f"own dimensions and adds no grouping."
            )
        # DEV-1824 (task 3.7) — a LOCAL first/last / window= with partition_by
        # inside a dimension is lifted (measure⇔dimension symmetry): the row-attach
        # producer collapses to a ranked CTE (first/last) or synthesizes the
        # active-TD grain (window=, D5). A cross-model source is still rejected in
        # ``_plan_regroups``.
    # A valid partitioned-aggregate dimension is desugared into a synthesized
    # producer stage by ``_plan_regroups`` (DEV-1825).


def _computed_dim_names(query: SlayerQuery) -> FrozenSet[str]:
    return frozenset(
        d.name for d in (query.dimensions or []) if isinstance(d, ComputedDimension)
    )


def _reraise_nested_attach(
    err: UnknownReferenceError, *, computed_dim_names: FrozenSet[str],
) -> None:
    """A partition_by / expression that references a computed dimension would
    aggregate over an attached value — the nested-attach shape (D3), failed closed
    with a clear DEV-1824 message rather than the raw unresolved-reference error."""
    if err.name in computed_dim_names:
        raise NotImplementedError(
            f"An aggregate references the computed dimension {err.name!r} (e.g. "
            f"via partition_by=), which would require a nested attach — not yet "
            f"supported (DEV-1824)."
        ) from err
    raise err


def _declared_computed_dimension(
    d: ComputedDimension,
    *,
    query: SlayerQuery,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> DeclaredMeasure:
    """Bind a computed (expression) dimension to a declared slot (DEV-1740)."""
    _reject_computed_dim_name_collision(name=d.name, query=query, scope=scope)
    parsed = parse_expr(d.expression)
    if isinstance(scope, ModelScope) and scope.source_model is not None:
        parsed = expand_model_measures(expr=parsed, model=scope.source_model)
    try:
        bound = bind_expr(parsed=parsed, scope=scope, bundle=bundle)
    except UnknownReferenceError as err:
        _reraise_nested_attach(err, computed_dim_names=_computed_dim_names(query))
    _guard_computed_dimension(d=d, bound=bound, query=query)
    dim_type = _type_for_measure_formula(scope=scope, bound=bound)
    return DeclaredMeasure(
        bound=bound,
        declared_name=d.name,
        public_name=d.name,
        type=dim_type,
        is_dimension=True,
    )


def _flatten_collision_message(flat_name: str) -> str:
    """Shared wording for the ``.``→``__`` flatten collision (DEV-1743 [D5]),
    used both here (pre-interning, over declared names) and by
    :func:`_emit_stage_schema` (at CTE emission)."""
    return (
        f"Stage column name collision on {flat_name!r}: two projected "
        f"columns flatten to the same downstream name. Give one an "
        f"explicit measure `name` to disambiguate."
    )


def _declared_measures_from_query(  # NOSONAR(S3776) — three sequential projection passes (dimensions incl. computed, time dimensions, measures) building one ordered declared list; each pass is one contract and the order (dims → tds → measures) is the public projection order the function pins.
    *,
    query: SlayerQuery,
    scope: Union[ModelScope, StageSchema],
    bundle: ResolvedSourceBundle,
) -> List[DeclaredMeasure]:
    declared: List[DeclaredMeasure] = []
    # DEV-1743 [D5]: two DISTINCT projected names (a joined ``customers.region``
    # and a literal ``customers__region``) can flatten to one downstream name.
    # Detect that here, before interning, so the clear collision message wins
    # over the generic ``DuplicateMeasureNameError``.
    seen_flat: Dict[str, str] = {}

    def _guard_flatten(*, flat_name: str, origin: str) -> None:
        prior = seen_flat.get(flat_name)
        if prior is not None and prior != origin:
            raise ValueError(_flatten_collision_message(flat_name))
        seen_flat[flat_name] = origin

    for d in (query.dimensions or []):
        if isinstance(d, ComputedDimension):
            dm = _declared_computed_dimension(
                d, query=query, scope=scope, bundle=bundle,
            )
            # [D5] a computed dim whose name flattens onto a dotted dim's
            # downstream name collides too — surface the clear message here.
            _guard_flatten(flat_name=_flatten_dotted(d.name), origin=d.name)
            declared.append(dm)
            continue
        full = d.full_name
        _reject_opaque_grouping_dim(
            query=query, scope=scope, full_name=full, bundle=bundle,
        )
        bound = bind_expr(
            parsed=parse_expr(full),
            scope=scope,
            bundle=bundle,
        )
        flat_name = _flatten_dotted(full)
        _guard_flatten(flat_name=flat_name, origin=full)
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
        _guard_flatten(flat_name=flat_name, origin=full)
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
        parsed = parse_expr(formula)
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
        try:
            bound = bind_expr(parsed=parsed, scope=scope, bundle=bundle)
        except UnknownReferenceError as err:
            _reraise_nested_attach(
                err, computed_dim_names=_computed_dim_names(query),
            )
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
        # An explicit type wins over inference at every level of this chain.
        explicit_type = m.type or _saved_model_measure_type(scope=scope, formula=formula)
        m_type = explicit_type or _type_for_measure_formula(scope=scope, bound=bound)
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
            type_is_explicit=explicit_type is not None,
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
        # The derivation lives in ``slayer.sql.naming`` (P-F). The
        # ``stage_formula`` profile prefixes the join path RELATIVE to the stage
        # (no source relation) and keeps a cross-model star's own path, so
        # ``customers.*:count`` aliases as ``customers._count`` and surfaces as
        # the result key ``orders.customers._count``.
        #
        # Both local and cross-model aggregates retain the kwarg suffix
        # (``percentile(p=0.5)`` -> ``_p_0_5``). For cross-model parametric
        # aggregates that DIVERGES from the deleted legacy pipeline, which
        # dropped the suffix and thereby collided two parametric variants onto
        # one alias — a ratified divergence, pinned by
        # tests/test_dev1744_result_key_contract.py.
        alias = canonical_aggregate_alias(
            bound.value_key, profile="stage_formula",
        )
        if alias is not None:
            return alias
        # ``None`` means the aggregate's source exposes neither a leaf nor a
        # column name — not reachable in practice (the binder restricts sources
        # to ColumnKey / ColumnSqlKey / StarKey) — so fall through to the
        # text-shape path below.
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


def _composite_reads_an_isolated_cte(
    *,
    key: ValueKey,
    slot_by_key: Dict[ValueKey, SlotId],
    isolated_slot_ids: AbstractSet[SlotId],
) -> bool:
    """Whether any aggregate leaf of a composite lives in an isolated CTE.

    One such leaf is enough: the composite then cannot be evaluated inside
    ``_base`` at all, because that leaf's value is a column of a CTE joined back
    to it. Falling back to a host-base scope would silently substitute a plain
    aggregate for the cross-model, rolling, or ranked one.
    """
    for dep in walk_value_keys(key):
        # DEV-1829 — a combined regroup placeholder (reserved-leaf ColumnKey)
        # lives in its ``_cm_`` producer exactly like a cross-model aggregate, so
        # a composite reading one is also an outer composite.
        is_isolated_leaf = isinstance(dep, AggregateKey) or (
            isinstance(dep, ColumnKey) and dep.leaf.startswith(REGROUP_LEAF_PREFIX)
        )
        if is_isolated_leaf and slot_by_key.get(dep) in isolated_slot_ids:
            return True
    return False


def _classify_order_scope(
    *,
    slot: ValueSlot,
    cross_model_slot_ids: Set[SlotId],
    windowed_slot_ids: Set[SlotId],
    public_projection: List[SlotId],
    slot_by_key: Dict[ValueKey, SlotId],
    ranked_slot_ids: AbstractSet[SlotId] = frozenset(),
) -> OrderScope:
    """Name the scope that PRODUCES ``slot``'s value (DEV-1747 §5.10).

    Order matters. A slot can satisfy more than one test — the DEV-1735 order
    wrap is both hidden and cross-model — and the producing scope is the
    narrower fact, so isolated scopes are checked before the host base.

    A composite is classified OUTER_COMPOSITE when any operand lives in an
    isolated CTE: it cannot be evaluated inside ``_base`` at all, and falling
    back to a host-base scope would silently substitute a plain aggregate for
    the cross-model or rolling one.
    """
    if slot.id in cross_model_slot_ids:
        return OrderScope.CROSS_MODEL_CTE
    if slot.id in ranked_slot_ids:
        return OrderScope.RANKED_CTE
    if slot.id in windowed_slot_ids:
        return OrderScope.WINDOWED_CTE
    if isinstance(slot.key, TransformKey):
        return OrderScope.TRANSFORM_STEP
    if isinstance(slot.key, (ArithmeticKey, ScalarCallKey)) and _composite_reads_an_isolated_cte(
        key=slot.key,
        slot_by_key=slot_by_key,
        isolated_slot_ids=(
            cross_model_slot_ids | windowed_slot_ids | set(ranked_slot_ids)
        ),
    ):
        return OrderScope.OUTER_COMPOSITE
    if slot.hidden or slot.id not in public_projection:
        return OrderScope.HOST_BASE_HIDDEN
    return OrderScope.HOST_BASE


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
    stage_name: Optional[str],
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
            raise ValueError(_flatten_collision_message(flat))
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
    return StageSchema(
        relation_name=stage_name or "(unnamed_stage)", columns=columns,
    )


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

    Validation:

    * ``parse_sql_predicate`` rejects DSL constructs (colon aggregation,
      transform calls) and raw ``OVER(...)`` window functions.
    * Reject references to a ``ModelMeasure`` declared on the same
      model — model filters are WHERE-clause SQL, can't reference
      aggregates.
    * Reject references to a column whose ``Column.sql`` contains a
      window function.
    * DEV-1450 follow-up #4b: references to a NON-windowed derived
      ``Column.sql`` column are accepted — the generator inlines the
      column's expanded SQL at render time through the Mode-A door
      (``ScopeFrame.enter_predicate``) and pulls any joins the expansion
      crosses into the FROM.
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
