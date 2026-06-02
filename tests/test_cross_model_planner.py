"""Stage 7a.2 (DEV-1450) — CrossModelPlanner Protocol + IsolatedCte impl (I1).

The cross-model planner takes an aggregate slot whose ``AggregateKey``
targets a joined model and produces a ``CrossModelAggregatePlan`` (in
``planned.py``). The Protocol formalises that strategy is a substitutable
component; the default ``IsolatedCteCrossModelPlanner`` implementation
encodes today's "isolated CTE per (target, shared-grain)" pattern and the
``inherited_filter_policy`` decision table.

These tests cover:
- Protocol substitution (a ``NullCrossModelPlanner`` stub proves the
  Protocol works for substitution).
- Join-chain walking from host to target.
- ``shared_grain_slots`` computation from host dimensions / join keys.
- Each row of the ``inherited_filter_policy`` decision table.
- ``cte_stage_schema`` shape.

The planner is dormant in stage 7a — no engine code calls it yet.
"""

from __future__ import annotations

from typing import List, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    Phase,
    SqlExprKey,
)
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.scope import StageSchema
from slayer.engine.cross_model_planner import (
    CrossModelPlanner,
    FilterRoute,
    HostFilterRouting,
    IsolatedCteCrossModelPlanner,
    classify_host_filter,
)
from slayer.engine.planned import (
    CrossModelAggregatePlan,
    JoinRequirement,
    SlotId,
    ValueSlot,
)
from slayer.engine.source_bundle import ResolvedSourceBundle


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
        ],
        joins=[
            ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
            ),
        ],
    )


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers",
        data_source="prod",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="revenue", type=DataType.DOUBLE),
        ],
        joins=[
            ModelJoin(
                target_model="regions",
                join_pairs=[["region_id", "id"]],
            ),
        ],
        filters=["deleted_at IS NULL"],
    )


def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions",
        data_source="prod",
        sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders(),
        referenced_models=[_customers(), _regions()],
    )


def _planner() -> IsolatedCteCrossModelPlanner:
    return IsolatedCteCrossModelPlanner()


# A trivial AggregateKey for cross-model customers.revenue:sum.
def _customers_revenue_sum() -> AggregateKey:
    return AggregateKey(
        source=ColumnKey(path=("customers",), leaf="revenue"),
        agg="sum",
    )


def _local_amount_sum() -> AggregateKey:
    # Local — no cross-model. The planner should reject this.
    return AggregateKey(
        source=ColumnKey(path=(), leaf="amount"),
        agg="sum",
    )


def _row_slot(slot_id: SlotId, *, leaf: str, path=()) -> ValueSlot:
    return ValueSlot(
        id=slot_id,
        key=ColumnKey(path=path, leaf=leaf),
        declared_name=leaf if not path else ".".join(path + (leaf,)),
        phase=Phase.ROW,
        public_name=None,
        hidden=True,
    )


def _agg_slot(slot_id: SlotId, *, key: AggregateKey) -> ValueSlot:
    return ValueSlot(
        id=slot_id,
        key=key,
        declared_name=f"{key.source.leaf}_{key.agg}",
        phase=Phase.AGGREGATE,
        hidden=True,
    )


# ---------------------------------------------------------------------------
# Protocol substitution
# ---------------------------------------------------------------------------


class _NullCrossModelPlanner:
    """Trivial Protocol-satisfying stub returning an empty plan.

    Proves that ``CrossModelPlanner`` is structurally substitutable.
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
        # DEV-1450 follow-up #2: the reroot-strategy kwargs are optional;
        # a Protocol-complete double absorbs them without re-rooting.
        **_: object,
    ) -> CrossModelAggregatePlan:
        return CrossModelAggregatePlan(
            aggregate_slot_id=aggregate_slot_id,
            target_model=aggregate_key.source.path[-1] if aggregate_key.source.path else "",
            datasource="(null)",
            join_chain=[],
            cte_stage_schema=StageSchema(relation_name="null", columns=[]),
            shared_grain_slots=[],
            applied_filter_ids=[],
            hidden=hidden,
            public_alias=public_alias,
        )


class TestProtocolSubstitution:
    def test_null_planner_satisfies_protocol(self):
        # Structural Protocol substitution — duck-typed.
        planner: CrossModelPlanner = _NullCrossModelPlanner()
        result = planner.plan(
            aggregate_slot_id="s1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=[],
            host_filters=[],
        )
        assert isinstance(result, CrossModelAggregatePlan)
        assert result.target_model == "customers"

    def test_isolated_cte_planner_satisfies_protocol(self):
        planner: CrossModelPlanner = IsolatedCteCrossModelPlanner()
        result = planner.plan(
            aggregate_slot_id="s1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=[],
            host_filters=[],
        )
        assert isinstance(result, CrossModelAggregatePlan)


# ---------------------------------------------------------------------------
# Join-chain walking
# ---------------------------------------------------------------------------


class TestJoinChainWalk:
    def test_single_hop(self):
        planner = _planner()
        plan = planner.plan(
            aggregate_slot_id="cm1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=[],
            host_filters=[],
        )
        assert plan.target_model == "customers"
        assert plan.datasource == "prod"
        assert len(plan.join_chain) == 1
        hop = plan.join_chain[0]
        assert isinstance(hop, JoinRequirement)
        assert hop.source_model == "orders"
        assert hop.target_model == "customers"
        assert hop.join_pairs == [["customer_id", "id"]]

    def test_multi_hop(self):
        planner = _planner()
        key = AggregateKey(
            source=ColumnKey(path=("customers", "regions"), leaf="name"),
            agg="count_distinct",
        )
        plan = planner.plan(
            aggregate_slot_id="cm2",
            aggregate_key=key,
            bundle=_bundle(),
            host_slots=[],
            host_filters=[],
        )
        assert plan.target_model == "regions"
        assert len(plan.join_chain) == 2
        hop1, hop2 = plan.join_chain
        # First hop: orders → customers via [["customer_id","id"]].
        assert hop1.source_model == "orders"
        assert hop1.target_model == "customers"
        assert hop1.join_pairs == [["customer_id", "id"]]
        # Second hop: customers → regions via [["region_id","id"]].
        assert hop2.source_model == "customers"
        assert hop2.target_model == "regions"
        assert hop2.join_pairs == [["region_id", "id"]]

    def test_multi_hop_join_back_pairs_use_first_hop(self):
        # For multi-hop, the CTE groups at the FIRST hop's target grain
        # (customers.id), not the terminal model's id. Join-back pairs
        # must reference the first hop's columns: host.customer_id ↔
        # customers.id, NOT regions.id.
        planner = _planner()
        key = AggregateKey(
            source=ColumnKey(path=("customers", "regions"), leaf="name"),
            agg="count_distinct",
        )
        plan = planner.plan(
            aggregate_slot_id="cm_multi",
            aggregate_key=key,
            bundle=_bundle(),
            host_slots=[],
            host_filters=[],
        )
        assert len(plan.join_back_pairs) == 1
        host_key, target_key = plan.join_back_pairs[0]
        assert host_key.leaf == "customer_id"
        assert target_key.leaf == "id"
        # CTE schema must project the first-hop's target key (customers.id).
        names = {c.name for c in plan.cte_stage_schema.columns}
        assert "id" in names

    def test_rejects_local_aggregate(self):
        # AggregateKey with empty source.path is a local agg, not cross-model.
        planner = _planner()
        with pytest.raises(ValueError, match="not cross-model|local"):
            planner.plan(
                aggregate_slot_id="local1",
                aggregate_key=_local_amount_sum(),
                bundle=_bundle(),
                host_slots=[],
                host_filters=[],
            )

    def test_unknown_target_raises(self):
        planner = _planner()
        key = AggregateKey(
            source=ColumnKey(path=("nonexistent",), leaf="x"),
            agg="sum",
        )
        with pytest.raises(ValueError, match="no join"):
            planner.plan(
                aggregate_slot_id="bad",
                aggregate_key=key,
                bundle=_bundle(),
                host_slots=[],
                host_filters=[],
            )


# ---------------------------------------------------------------------------
# CTE stage schema
# ---------------------------------------------------------------------------


class TestCteStageSchema:
    def test_schema_relation_name_includes_target(self):
        planner = _planner()
        plan = planner.plan(
            aggregate_slot_id="cm1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=[],
            host_filters=[],
        )
        assert "customers" in plan.cte_stage_schema.relation_name

    def test_schema_contains_aggregate_output_and_join_keys(self):
        planner = _planner()
        plan = planner.plan(
            aggregate_slot_id="cm1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=[],
            host_filters=[],
        )
        names = {c.name for c in plan.cte_stage_schema.columns}
        # Aggregate output column.
        assert any("revenue_sum" in n or "sum" in n for n in names), (
            f"expected aggregate output in CTE projection, got {names}"
        )
        # Join-back key (CTE-side: the target's join column, here `id`).
        # The CTE must project the join column so the host can join back.
        target_join_keys = {pair[1].leaf for pair in plan.join_back_pairs}
        for key_name in target_join_keys:
            assert key_name in names, (
                f"join-back target key {key_name!r} must be in CTE projection"
            )

    def test_schema_includes_grain_slots(self):
        # If host carries a dimension `status` that's part of the grain,
        # the CTE projects status too so the join-back can carry it
        # through.
        planner = _planner()
        host_slots = [_row_slot("d_status", leaf="status")]
        plan = planner.plan(
            aggregate_slot_id="cm1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=host_slots,
            host_filters=[],
        )
        # When status is part of the host grain, it appears as a
        # join-back equality (so the CTE side carries it).
        host_keys = {pair[0].leaf for pair in plan.join_back_pairs}
        assert "status" in host_keys or "d_status" in plan.shared_grain_slots


# ---------------------------------------------------------------------------
# Shared-grain slots
# ---------------------------------------------------------------------------


class TestSharedGrainSlots:
    def test_local_row_dimensions_in_grain(self):
        # If the host carries a dimension `status` (local row slot), the
        # CTE must group at the grain {status, customer_id}: dimension +
        # join key. The grain ids are passed through to the plan.
        planner = _planner()
        host_slots = [
            _row_slot("d1", leaf="status"),
        ]
        plan = planner.plan(
            aggregate_slot_id="cm1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=host_slots,
            host_filters=[],
        )
        assert "d1" in plan.shared_grain_slots

    def test_aggregate_slots_excluded_from_grain(self):
        # Aggregate slots and POST-phase slots on host are NOT part of
        # the shared grain (they'd recurse).
        planner = _planner()
        host_slots = [
            _row_slot("d1", leaf="status"),
            _agg_slot("a1", key=AggregateKey(
                source=ColumnKey(path=(), leaf="amount"), agg="sum",
            )),
        ]
        plan = planner.plan(
            aggregate_slot_id="cm1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=host_slots,
            host_filters=[],
        )
        assert "a1" not in plan.shared_grain_slots
        assert "d1" in plan.shared_grain_slots

    def test_other_branch_row_slot_excluded_from_grain(self):
        # A ROW slot whose path goes to a DIFFERENT joined branch (not
        # the path to the cross-model target) is NOT part of grain.
        planner = _planner()
        host_slots = [
            _row_slot("d_other", leaf="name", path=("warehouses",)),
        ]
        plan = planner.plan(
            aggregate_slot_id="cm1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=host_slots,
            host_filters=[],
        )
        assert "d_other" not in plan.shared_grain_slots

    def test_join_back_pairs_present(self):
        # The host's customer_id (the LHS of the FK join to customers) is
        # part of the join_back_pairs — anchoring how the CTE joins back.
        planner = _planner()
        plan = planner.plan(
            aggregate_slot_id="cm1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=[],
            host_filters=[],
        )
        assert plan.join_back_pairs
        host_keys = {pair[0].leaf for pair in plan.join_back_pairs}
        assert "customer_id" in host_keys


# ---------------------------------------------------------------------------
# inherited_filter_policy decision table
# ---------------------------------------------------------------------------


class TestInheritedFilterPolicy:
    """One test per row of the decision table."""

    def test_host_local_row_slot_only_drops_from_cte(self):
        # Filter touches only ROW slots on the host model — not propagated.
        planner = _planner()
        host_slots = [_row_slot("h_status", leaf="status")]
        host_filters = [HostFilterRouting(
            filter_id="f1",
            phase=Phase.ROW,
            referenced_slot_ids=["h_status"],
            text="status = 'paid'",
        )]
        plan = planner.plan(
            aggregate_slot_id="cm1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=host_slots,
            host_filters=host_filters,
        )
        assert "f1" not in plan.applied_filter_ids
        assert not any(
            "f1" in str(w) for w in plan.dropped_filter_warnings
        )

    def test_joined_target_path_propagates_as_where(self):
        # Filter touches a ROW slot whose path goes through the target.
        planner = _planner()
        host_slots = [_row_slot("rs_revenue", leaf="revenue", path=("customers",))]
        host_filters = [HostFilterRouting(
            filter_id="f1",
            phase=Phase.ROW,
            referenced_slot_ids=["rs_revenue"],
            text="customers.revenue > 100",
        )]
        plan = planner.plan(
            aggregate_slot_id="cm1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=host_slots,
            host_filters=host_filters,
        )
        # Routed to WHERE specifically — not HAVING.
        assert "f1" in plan.where_filter_ids
        assert "f1" not in plan.having_filter_ids
        # Also in applied_filter_ids (audit union).
        assert "f1" in plan.applied_filter_ids

    def test_target_model_own_filters_always_propagate(self):
        # _customers() has SlayerModel.filters=["deleted_at IS NULL"] —
        # the plan must always propagate these to the CTE as always-applied
        # WHERE entries on a separate field (not user-keyed).
        planner = _planner()
        plan = planner.plan(
            aggregate_slot_id="cm1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=[],
            host_filters=[],
        )
        assert any("deleted_at" in f for f in plan.target_model_filters)
        # No host filters were passed → no user-keyed applied ids.
        assert plan.applied_filter_ids == []
        assert plan.where_filter_ids == []
        assert plan.having_filter_ids == []

    def test_cross_model_aggref_same_target_propagates_as_having(self):
        # Host filter references a cross-model aggregate slot whose
        # target IS this same target. The filter routes to HAVING on the
        # CTE.
        planner = _planner()
        cm_agg_slot = _agg_slot("cm_rev", key=_customers_revenue_sum())
        host_slots = [cm_agg_slot]
        host_filters = [HostFilterRouting(
            filter_id="f_having",
            phase=Phase.AGGREGATE,
            referenced_slot_ids=["cm_rev"],
            text="customers.revenue:sum >= 100",
        )]
        plan = planner.plan(
            aggregate_slot_id="cm1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=host_slots,
            host_filters=host_filters,
        )
        # Routed to HAVING specifically — not WHERE.
        assert "f_having" in plan.having_filter_ids
        assert "f_having" not in plan.where_filter_ids
        assert "f_having" in plan.applied_filter_ids

    def test_source_column_filter_carried_via_aggregate_key(self):
        # Decision-table row 4: source Column.filter on the aggregated
        # column. This is intrinsic to the aggregate (lives on
        # AggregateKey.column_filter_key); the plan preserves it on the
        # aggregate key itself, NOT as a host filter routing.
        planner = _planner()
        col_filter = SqlExprKey(canonical_sql="status = 'active'")
        key = AggregateKey(
            source=ColumnKey(path=("customers",), leaf="revenue"),
            agg="sum",
            column_filter_key=col_filter,
        )
        plan = planner.plan(
            aggregate_slot_id="cm1",
            aggregate_key=key,
            bundle=_bundle(),
            host_slots=[],
            host_filters=[],
        )
        # The aggregate key on the cte_stage_schema's aggregate slot must
        # still carry column_filter_key — it's how 7b emits CASE-WHEN.
        # The plan does not synthesise a separate host-filter for this.
        assert "status = 'active'" not in plan.target_model_filters
        assert plan.applied_filter_ids == []
        assert plan.where_filter_ids == []
        assert plan.having_filter_ids == []

    def test_unreachable_branch_drops_and_warns(self):
        # Host filter references a slot whose path goes to a DIFFERENT
        # branch (e.g., a `warehouses` join — not customers). The target
        # CTE doesn't reach that branch, so the filter is dropped and a
        # warning is emitted.
        planner = _planner()
        host_slots = [
            _row_slot("rs_other", leaf="name", path=("warehouses",)),
        ]
        host_filters = [HostFilterRouting(
            filter_id="f_unreach",
            phase=Phase.ROW,
            referenced_slot_ids=["rs_other"],
            text="warehouses.name = 'EU'",
        )]
        plan = planner.plan(
            aggregate_slot_id="cm1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=host_slots,
            host_filters=host_filters,
        )
        assert "f_unreach" not in plan.applied_filter_ids
        assert any(
            "f_unreach" in str(w) or "warehouses" in str(w)
            for w in plan.dropped_filter_warnings
        )

    def test_mixed_refs_drops_and_warns(self):
        # Filter touches BOTH reachable (target-path) and unreachable
        # (other-branch) slots → drop + warn.
        planner = _planner()
        host_slots = [
            _row_slot("rs_target", leaf="revenue", path=("customers",)),
            _row_slot("rs_other", leaf="name", path=("warehouses",)),
        ]
        host_filters = [HostFilterRouting(
            filter_id="f_mixed",
            phase=Phase.ROW,
            referenced_slot_ids=["rs_target", "rs_other"],
            text="customers.revenue > warehouses.x",
        )]
        plan = planner.plan(
            aggregate_slot_id="cm1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=host_slots,
            host_filters=host_filters,
        )
        assert "f_mixed" not in plan.applied_filter_ids
        assert any(
            "f_mixed" in str(w) or "mixed" in str(w).lower()
            for w in plan.dropped_filter_warnings
        )

    def test_transform_post_phase_stays_at_host(self):
        # POST-phase filter cannot apply at CTE level. Not in applied,
        # not in dropped — stays at host.
        planner = _planner()
        host_slots = [_row_slot("rs_revenue", leaf="revenue", path=("customers",))]
        host_filters = [HostFilterRouting(
            filter_id="f_post",
            phase=Phase.POST,
            referenced_slot_ids=["rs_revenue"],
            text="change(customers.revenue:sum) > 0",
        )]
        plan = planner.plan(
            aggregate_slot_id="cm1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=host_slots,
            host_filters=host_filters,
        )
        assert "f_post" not in plan.applied_filter_ids
        assert "f_post" not in plan.where_filter_ids
        assert "f_post" not in plan.having_filter_ids
        # Not dropped (host applies it).
        assert not any(
            "f_post" in str(w) for w in plan.dropped_filter_warnings
        )


# ---------------------------------------------------------------------------
# HostFilterRouting edge cases
# ---------------------------------------------------------------------------


class TestHostFilterRoutingEdgeCases:
    def test_unknown_slot_id_treated_as_unreachable(self):
        # A referenced slot id that isn't in host_slots — conservative:
        # treat as unreachable (drop + warn) so misuse is surfaced.
        planner = _planner()
        host_filters = [HostFilterRouting(
            filter_id="f_unknown",
            phase=Phase.ROW,
            referenced_slot_ids=["nonexistent_slot"],
        )]
        plan = planner.plan(
            aggregate_slot_id="cm1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=[],
            host_filters=host_filters,
        )
        assert "f_unknown" not in plan.applied_filter_ids
        assert any(
            "f_unknown" in str(w) for w in plan.dropped_filter_warnings
        )

    def test_empty_referenced_slots_stays_at_host(self):
        # A filter referencing no slots (e.g., a literal-only predicate
        # like `TRUE`) — no routing applies. Stays at host. Not in any
        # CTE list; not dropped.
        planner = _planner()
        host_filters = [HostFilterRouting(
            filter_id="f_empty",
            phase=Phase.ROW,
            referenced_slot_ids=[],
        )]
        plan = planner.plan(
            aggregate_slot_id="cm1",
            aggregate_key=_customers_revenue_sum(),
            bundle=_bundle(),
            host_slots=[],
            host_filters=host_filters,
        )
        assert "f_empty" not in plan.applied_filter_ids
        assert not any(
            "f_empty" in str(w) for w in plan.dropped_filter_warnings
        )


# ---------------------------------------------------------------------------
# classify_host_filter — direct classifier API
# ---------------------------------------------------------------------------


class TestClassifyHostFilter:
    def test_classify_host_local_row(self):
        host_slots = [_row_slot("h_status", leaf="status")]
        hf = HostFilterRouting(
            filter_id="f1", phase=Phase.ROW,
            referenced_slot_ids=["h_status"], text="status = 'paid'",
        )
        route = classify_host_filter(
            host_filter=hf,
            host_slots=host_slots,
            target_path=("customers",),
        )
        assert route == FilterRoute.DROP_HOST_LOCAL

    def test_classify_target_path(self):
        host_slots = [_row_slot("h", leaf="revenue", path=("customers",))]
        hf = HostFilterRouting(
            filter_id="f1", phase=Phase.ROW,
            referenced_slot_ids=["h"], text="",
        )
        route = classify_host_filter(
            host_filter=hf,
            host_slots=host_slots,
            target_path=("customers",),
        )
        assert route == FilterRoute.PROPAGATE_WHERE

    def test_classify_cross_model_agg_same_target(self):
        agg_slot = _agg_slot("a", key=_customers_revenue_sum())
        hf = HostFilterRouting(
            filter_id="f1", phase=Phase.AGGREGATE,
            referenced_slot_ids=["a"], text="",
        )
        route = classify_host_filter(
            host_filter=hf,
            host_slots=[agg_slot],
            target_path=("customers",),
        )
        assert route == FilterRoute.PROPAGATE_HAVING

    def test_classify_unreachable(self):
        host_slots = [_row_slot("h", leaf="x", path=("warehouses",))]
        hf = HostFilterRouting(
            filter_id="f1", phase=Phase.ROW,
            referenced_slot_ids=["h"], text="",
        )
        route = classify_host_filter(
            host_filter=hf,
            host_slots=host_slots,
            target_path=("customers",),
        )
        assert route == FilterRoute.DROP_UNREACHABLE

    def test_classify_post_phase_stays(self):
        host_slots = [_row_slot("h", leaf="x")]
        hf = HostFilterRouting(
            filter_id="f1", phase=Phase.POST,
            referenced_slot_ids=["h"], text="",
        )
        route = classify_host_filter(
            host_filter=hf,
            host_slots=host_slots,
            target_path=("customers",),
        )
        assert route == FilterRoute.STAY_AT_HOST_POST

    def test_classify_columnsqlkey_on_host_is_local(self):
        # ColumnSqlKey with model matching host_model_name → local.
        derived = ValueSlot(
            id="d", key=ColumnSqlKey(model="orders", column_name="x"),
            declared_name="x", phase=Phase.ROW, hidden=True,
        )
        hf = HostFilterRouting(
            filter_id="f1", phase=Phase.ROW, referenced_slot_ids=["d"],
        )
        route = classify_host_filter(
            host_filter=hf,
            host_slots=[derived],
            target_path=("customers",),
            host_model_name="orders",
        )
        assert route == FilterRoute.DROP_HOST_LOCAL

    def test_classify_columnsqlkey_on_target_is_reachable(self):
        # ColumnSqlKey with model in target_path → PROPAGATE_WHERE.
        derived = ValueSlot(
            id="d", key=ColumnSqlKey(model="customers", column_name="x"),
            declared_name="x", phase=Phase.ROW, hidden=True,
        )
        hf = HostFilterRouting(
            filter_id="f1", phase=Phase.ROW, referenced_slot_ids=["d"],
        )
        route = classify_host_filter(
            host_filter=hf,
            host_slots=[derived],
            target_path=("customers",),
            host_model_name="orders",
        )
        assert route == FilterRoute.PROPAGATE_WHERE

    def test_classify_columnsqlkey_on_other_branch_is_unreachable(self):
        # ColumnSqlKey on a model not in target_path and not host → unreachable.
        derived = ValueSlot(
            id="d", key=ColumnSqlKey(model="warehouses", column_name="x"),
            declared_name="x", phase=Phase.ROW, hidden=True,
        )
        hf = HostFilterRouting(
            filter_id="f1", phase=Phase.ROW, referenced_slot_ids=["d"],
        )
        route = classify_host_filter(
            host_filter=hf,
            host_slots=[derived],
            target_path=("customers",),
            host_model_name="orders",
        )
        assert route == FilterRoute.DROP_UNREACHABLE
