"""Plan-time construction of ``RankedAggregatePlan`` — the ``first`` / ``last``
route (P-C / P-D).

A ranked aggregate needs its own ROW ORDERING, so under P-C it compiles to a
plan-shaped CTE rooted where its rows live and joined back on the query grain.
Everything that decision needs is settled HERE — which column the ranking runs
over, what the grain is in the ranked scope's own coordinates, which filters the
CTE evaluates — so the renderer emits a plan rather than re-deriving one.

The ranking column in particular used to be resolved at render time, twice: once
for the host base's ranked wrap and once, by a different precedence, inside the
cross-model CTE. They agreed by accident on the cases anyone had tried. One
resolver with an explicit per-scope precedence replaces both:

* **host-rooted** — an explicit positional time arg, else the first
  ``DATE``/``TIMESTAMP`` row dimension, else the first time dimension's RAW
  column (never the truncated bucket: ranking within a month by the month ties
  every row in it), else the model's ``default_time_dimension``.
* **target-rooted** — the same explicit arg, re-anchored in the target's
  coordinates, else the TARGET model's ``default_time_dimension``. Host
  dimensions are deliberately not candidates: the CTE ranks target rows, and a
  host column is not in scope there.

Both ends of the precedence raise rather than fall through, with the message the
scope's users already see.
"""

from __future__ import annotations

from typing import Any, FrozenSet, List, Optional, Sequence, Tuple

from slayer.core.enums import DataType
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    TimeTruncKey,
    ValueKey,
    column_path,
    reroot_value_key,
)
from slayer.core.models import SlayerModel
from slayer.engine.planned import (
    RankedAggregatePlan,
    RankedGrainMember,
    SlotId,
    ValueSlot,
)
from slayer.engine.source_bundle import ResolvedSourceBundle

__all__ = [
    "RANKED_AGGREGATIONS",
    "build_host_ranked_plan",
    "build_target_ranked_plan",
    "explicit_ranking_time_arg",
    "resolve_ranking_time_key",
]

#: The aggregations that rank. Named once so the classifier, the planner and
#: the renderer cannot drift on what "a ranked aggregate" is.
RANKED_AGGREGATIONS = ("first", "last")

_TEMPORAL_TYPES = (DataType.DATE, DataType.TIMESTAMP)


def explicit_ranking_time_arg(key: AggregateKey) -> Optional[ValueKey]:
    """The explicit positional ranking-time arg of a ``first`` / ``last``, or
    ``None``.

    The FIRST positional arg iff it is a column ref; ``None`` for anything else
    — first/last never takes a leading non-column positional, so a different
    shape means the caller passed something this aggregation does not read.
    """
    if key.agg not in RANKED_AGGREGATIONS:
        return None
    for arg in key.args:
        return arg if isinstance(arg, (ColumnKey, ColumnSqlKey)) else None
    return None


def _ranking_key_name(key: ValueKey) -> str:
    """The user-facing name of a ranking-time key, for an error message."""
    if isinstance(key, ColumnKey):
        return ".".join((*key.path, key.leaf))
    if isinstance(key, ColumnSqlKey):
        return ".".join((*key.path, key.column_name))
    return type(key).__name__


def _resolves_on(*, key: ValueKey, model: SlayerModel) -> bool:
    """Whether ``key`` names something reachable FROM ``model``.

    A shallow check on purpose: a local leaf must be a column of the model, and
    a path-bearing one must start at a model this one joins to. Walking the
    whole chain is the renderer's job and it raises its own errors; this exists
    so the common mistake — naming a HOST column as a TARGET-rooted ranking key
    — is caught where the plan is made rather than at the database.
    """
    if isinstance(key, ColumnKey):
        leaf, path = key.leaf, key.path
    elif isinstance(key, ColumnSqlKey):
        leaf, path = key.column_name, key.path
    else:
        return True
    if path:
        return any(j.target_model == path[0] for j in (model.joins or []))
    return any(c.name == leaf for c in model.columns)


def _temporal_row_dimension_key(
    *,
    row_keys: Sequence[ValueKey],
    source_model: SlayerModel,
    bundle: ResolvedSourceBundle,
) -> Optional[ValueKey]:
    """The first row dimension whose declared type is ``DATE``/``TIMESTAMP``."""
    for key in row_keys:
        if not isinstance(key, ColumnKey):
            continue
        model: Optional[SlayerModel] = source_model
        for hop in key.path:
            model = bundle.get_referenced_model(hop)
            if model is None:
                break
        if model is None:
            continue
        col = next((c for c in model.columns if c.name == key.leaf), None)
        if col is not None and col.type in _TEMPORAL_TYPES:
            return key
    return None


def _time_dimension_raw_column(
    *, row_keys: Sequence[ValueKey],
) -> Optional[ValueKey]:
    """The first time dimension's RAW column, un-truncated."""
    for key in row_keys:
        if isinstance(key, TimeTruncKey):
            return key.column
    return None


def resolve_ranking_time_key(
    *,
    key: AggregateKey,
    root_model: SlayerModel,
    bundle: ResolvedSourceBundle,
    row_keys: Sequence[ValueKey] = (),
    target_path: Tuple[str, ...] = (),
) -> ValueKey:
    """The column a ranked aggregate's ``ROW_NUMBER`` orders by, in the RANKED
    scope's coordinates.

    ``row_keys`` are the host's row-dimension keys in render order; they are
    candidates only for a HOST-rooted plan (``target_path`` empty). A
    target-rooted CTE ranks the target's rows, so a host dimension is not in
    scope there and the precedence goes straight from the explicit arg to the
    target's own default.
    """
    arg = explicit_ranking_time_arg(key)
    if arg is not None:
        # The arg arrives in the HOST's coordinates; a target-rooted plan needs
        # it in the target's, which is the same re-anchoring the aggregate
        # source itself undergoes — done in lockstep so the ORDER BY and the
        # value cannot end up rooted at different relations.
        rerooted = reroot_value_key(arg, target_path=target_path)
        if target_path and not _resolves_on(key=rerooted, model=root_model):
            # A HOST column as the ranking key of a TARGET-rooted CTE. The rows
            # being ranked are the target's, and a host column is not one of
            # their attributes — the relationship runs the other way, usually
            # one-to-many, so there is no single host value per target row to
            # rank by. This used to emit ``ORDER BY <target>.<host column>``,
            # a reference to a column that does not exist, and fail at the
            # database with no indication of which measure caused it.
            raise ValueError(
                f"first/last ranking column "
                f"{_ranking_key_name(rerooted)!r} is not resolvable on model "
                f"{root_model.name!r}, where a cross-model first/last ranks "
                f"its rows. Name a column of {root_model.name!r} (or one it "
                f"joins to), or drop the argument to rank by its "
                f"default_time_dimension."
            )
        return rerooted

    if not target_path:
        temporal = _temporal_row_dimension_key(
            row_keys=row_keys, source_model=root_model, bundle=bundle,
        )
        if temporal is not None:
            return temporal
        raw = _time_dimension_raw_column(row_keys=row_keys)
        if raw is not None:
            return raw
        if root_model.default_time_dimension:
            return ColumnKey(path=(), leaf=root_model.default_time_dimension)
        raise ValueError(
            "first/last aggregation requires a ranking time column "
            "(a time_dimension, a DATE/TIMESTAMP dimension, or the "
            "model's default_time_dimension); none is resolvable for "
            f"model {root_model.name!r}."
        )

    if root_model.default_time_dimension:
        return ColumnKey(path=(), leaf=root_model.default_time_dimension)
    raise ValueError(
        f"first/last aggregation requires a ranking time column "
        f"(an explicit positional time arg, or the target "
        f"model's default_time_dimension); none is resolvable "
        f"for cross-model aggregate on target "
        f"{root_model.name!r}."
    )


def ordered_row_keys(
    *, row_slots: Sequence[ValueSlot], public_projection: Sequence[SlotId],
) -> List[ValueKey]:
    """Row-dimension keys in the order the base SELECT renders them.

    Publicly projected slots first, in projection order, then the rest. That is
    the order the superseded render-time resolver walked, and the ranking-column
    precedence is order-sensitive — two temporal dimensions rank by whichever
    comes first.
    """
    by_id = {s.id: s for s in row_slots}
    seen: set = set()
    ordered: List[ValueKey] = []
    for sid in public_projection:
        slot = by_id.get(sid)
        if slot is not None and sid not in seen:
            seen.add(sid)
            ordered.append(slot.key)
    for slot in row_slots:
        if slot.id not in seen:
            seen.add(slot.id)
            ordered.append(slot.key)
    return ordered


def _host_grain(
    *,
    row_slots: Sequence[ValueSlot],
    combined_placeholder_keys: FrozenSet[ValueKey] = frozenset(),
) -> List[RankedGrainMember]:
    """The host grain: every VISIBLE row slot, in one coordinate system.

    Hidden row slots are excluded for the same reason the windowed plan excludes
    them — they are filter/order scaffolding the host base does not group by, so
    partitioning the ranking over one would split groups the result never has.
    """
    return [
        RankedGrainMember(host_slot_id=slot.id, ranked_key=slot.key)
        for slot in row_slots
        if not slot.hidden
        # DEV-1824 — a COMBINED regroup placeholder (a partitioned MEASURE
        # attached at the combined SELECT) is a ColumnKey ROW slot by
        # substitution but is an aggregate value, not a query dimension: it is
        # not in the ranked ``_rk_`` scope and must not partition the ranking.
        # DEV-1835 D4 — a ROW-attach placeholder is a computed dimension the
        # producer groups by, so only the combined set is excluded.
        and slot.key not in combined_placeholder_keys
    ]


def _target_grain(
    *,
    row_slots: Sequence[ValueSlot],
    shared_grain_slots: Sequence[SlotId],
    public_projection: Sequence[SlotId],
    target_path: Tuple[str, ...],
) -> List[RankedGrainMember]:
    """The grain a TARGET-rooted ranked CTE shares with the host.

    Only host dimensions that lie ON the target's join path can be expressed in
    the target's coordinates at all; the rest broadcast (the CTE is scalar and
    CROSS JOINed), which is the forward cross-model path's existing semantics.
    Re-rooting is what makes the rest reachable, and that route keeps its
    ``CrossModelAggregatePlan``.
    """
    by_id = {s.id: s for s in row_slots}
    projected = set(public_projection)
    members: List[RankedGrainMember] = []
    for sid in shared_grain_slots:
        slot = by_id.get(sid)
        if slot is None or slot.hidden or sid not in projected:
            continue
        path = _row_key_path(slot.key)
        if not path or path != target_path:
            # Empty: host-local, broadcast. Non-terminal / off-branch: not this
            # target's grain. Both are the forward path's existing behaviour.
            continue
        members.append(RankedGrainMember(
            host_slot_id=sid,
            ranked_key=reroot_value_key(slot.key, target_path=target_path),
        ))
    return members


def _row_key_path(key: ValueKey) -> Tuple[str, ...]:
    if isinstance(key, (ColumnKey, ColumnSqlKey)):
        return key.path
    if isinstance(key, TimeTruncKey):
        return column_path(key.column)
    return ()


def build_host_ranked_plan(
    *,
    slot: ValueSlot,
    row_slots: Sequence[ValueSlot],
    public_projection: Sequence[SlotId],
    source_model: SlayerModel,
    bundle: ResolvedSourceBundle,
    where_filter_ids: Sequence[str] = (),
    combined_placeholder_keys: FrozenSet[ValueKey] = frozenset(),
) -> RankedAggregatePlan:
    """One host-rooted ranked plan.

    ``where_filter_ids`` are the ROW-phase host filters (user filters AND the
    model's own ``filters``) the CTE ALSO evaluates. They are duplicated rather
    than relocated: a LEFT JOIN back propagates a value but never an exclusion,
    so a row filter that only reached the CTE would silently become "blank out
    their measure" instead of "exclude these rows" (the PR-4 B6 ruling, one
    route over).
    """
    key = slot.key
    assert isinstance(key, AggregateKey)
    return RankedAggregatePlan(
        aggregate_slot_id=slot.id,
        agg=key.agg,
        root_model=source_model.name,
        datasource=source_model.data_source,
        target_path=(),
        join_chain=[],
        ranking_time_key=resolve_ranking_time_key(
            key=key,
            root_model=source_model,
            bundle=bundle,
            row_keys=ordered_row_keys(
                row_slots=row_slots, public_projection=public_projection,
            ),
        ),
        grain=_host_grain(
            row_slots=row_slots,
            combined_placeholder_keys=combined_placeholder_keys,
        ),
        # The model's own ``filters`` are ROW-phase entries of
        # ``filters_by_phase`` like any other, so they arrive here by id rather
        # than as text — unlike the target-rooted case, where the TARGET's
        # filters are not in the host's filter list at all.
        where_filter_ids=list(where_filter_ids),
        applied_filter_ids=list(where_filter_ids),
        hidden=slot.hidden,
        public_alias=None if slot.hidden else slot.public_name,
    )


def build_target_ranked_plan(
    *,
    slot: ValueSlot,
    cross_model_plan: Any,
    row_slots: Sequence[ValueSlot],
    public_projection: Sequence[SlotId],
    bundle: ResolvedSourceBundle,
) -> RankedAggregatePlan:
    """One target-rooted ranked plan, re-shaped from the forward cross-model
    plan the same strategy produced.

    The routing decisions — which host filters this CTE evaluates as WHERE, as
    HAVING, which are unreachable — are the cross-model planner's decision table
    and are taken verbatim. Only the two things that are genuinely about RANKING
    are computed here: the ranking column in the target's coordinates, and the
    grain in the same.
    """
    key = slot.key
    assert isinstance(key, AggregateKey)
    target_path = tuple(getattr(key.source, "path", ()))
    target_model = bundle.get_referenced_model(cross_model_plan.target_model)
    if target_model is None:
        raise ValueError(
            f"Ranked cross-model target {cross_model_plan.target_model!r} is "
            f"not in the resolved source bundle.",
        )
    return RankedAggregatePlan(
        aggregate_slot_id=slot.id,
        agg=key.agg,
        root_model=target_model.name,
        datasource=cross_model_plan.datasource,
        target_path=target_path,
        join_chain=list(cross_model_plan.join_chain),
        ranking_time_key=resolve_ranking_time_key(
            key=key,
            root_model=target_model,
            bundle=bundle,
            target_path=target_path,
        ),
        grain=_target_grain(
            row_slots=row_slots,
            shared_grain_slots=cross_model_plan.shared_grain_slots,
            public_projection=public_projection,
            target_path=target_path,
        ),
        where_filter_ids=list(cross_model_plan.where_filter_ids),
        # A ranked CTE NEVER emits a HAVING, so it must not claim one. The
        # strategy routes an aggregate-phase filter there for a plain
        # cross-model CTE; on a ranked one the same predicate goes to the outer
        # combined SELECT instead, because this CTE is LEFT JOINed back and
        # dropping its row would resurrect the host row carrying NULL
        # (DEV-1503). ``where``/``having`` are INSTRUCTIONS about where a filter
        # is evaluated — carrying an id the renderer ignores would be an
        # instruction nothing follows. The audit (``applied_filter_ids``) keeps
        # the full record, and ``_assert_ranked_having_is_covered`` proves the
        # predicate really is applied somewhere.
        having_filter_ids=[],
        applied_filter_ids=list(cross_model_plan.applied_filter_ids),
        target_model_filters=list(cross_model_plan.target_model_filters),
        dropped_filter_warnings=list(cross_model_plan.dropped_filter_warnings),
        hidden=cross_model_plan.hidden,
        public_alias=cross_model_plan.public_alias,
    )
