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
from typing import List, Optional, Protocol, Tuple

from pydantic import BaseModel, ConfigDict, Field

from slayer.core.enums import DataType
from slayer.core.errors import UnreachableFilterDroppedWarning
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    Phase,
    TimeTruncKey,
)
from slayer.core.models import SlayerModel
from slayer.core.refs import canonical_agg_name
from slayer.core.scope import StageColumn, StageSchema
from slayer.engine.planned import (
    BoundFilterId,
    CrossModelAggregatePlan,
    JoinRequirement,
    SlotId,
    ValueSlot,
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
    """Strategy for compiling one cross-model aggregate slot."""

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
    if hasattr(key.source, "leaf"):
        measure_name = key.source.leaf
    else:
        measure_name = "*"
    # AggregateKey.args / kwargs are normalised tuples of scalars /
    # ColumnKey-shaped values; convert to the (List[str],
    # Dict[str, Any]) shape ``canonical_agg_name`` expects.
    args_list = [str(a) for a in key.args]
    kwargs_dict = {k: str(v) for k, v in key.kwargs}
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
    src_leaf = getattr(aggregate_key.source, "leaf", None)
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
            raise ValueError(
                f"AggregateKey on {agg_source!r} has empty source.path — "
                f"this is a local aggregate, not cross-model. The cross-"
                f"model planner should only be invoked for cross-model "
                f"aggregates."
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
                if not s.key.path:
                    shared_grain.append(s.id)
                elif s.key.path == target_path[: len(s.key.path)]:
                    shared_grain.append(s.id)
            elif isinstance(s.key, TimeTruncKey):
                td_path = s.key.column.path
                if not td_path:
                    shared_grain.append(s.id)
                elif td_path == target_path[: len(td_path)]:
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

        return CrossModelAggregatePlan(
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
