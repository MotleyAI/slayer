"""Stage 7b.5 (DEV-1450) — cross-model planner wiring + HostFilterRouting.

The dormant pieces:

* ``slayer/engine/cross_model_planner.py`` defines the
  ``CrossModelPlanner`` Protocol, ``IsolatedCteCrossModelPlanner``
  concrete impl, ``HostFilterRouting`` model, ``FilterRoute`` enum, and
  the ``classify_host_filter`` decision-table dispatcher.
* ``stage_planner.plan_query`` instantiates ``IsolatedCteCrossModelPlanner``
  but never calls ``.plan(...)``; ``PlannedQuery.cross_model_aggregate_plans``
  is always empty.

This stage wires it together:

1. New ``filter_referenced_slot_ids(bound_filter, registry) -> set[SlotId]``
   post-projection helper (in ``planning.py``). Walks
   ``_iter_slot_deps(bound_filter.value_key)`` and looks each slottable
   key up in the registry. Codex HIGH #3/#4: do NOT mutate
   ``BoundFilter.referenced_keys``; do walk composite predicates so a
   filter like ``rev >= 100 AND customers.revenue:sum < 500`` resolves
   correctly.

2. ``plan_query`` builds ``HostFilterRouting`` records from each bound
   filter, iterates aggregate slots whose ``source.path`` is non-empty,
   and calls ``cross_model_planner.plan(...)`` per slot. The resulting
   ``CrossModelAggregatePlan``s populate
   ``PlannedQuery.cross_model_aggregate_plans``.
"""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    LiteralKey,
    Phase,
)
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.binding import BoundFilter
from slayer.engine.cross_model_planner import (
    FilterRoute,
    HostFilterRouting,
    classify_host_filter,
)
from slayer.engine.planning import ValueRegistry
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _orders_model() -> SlayerModel:
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
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
    )


def _customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers",
        data_source="prod",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="region", type=DataType.TEXT),
        ],
    )


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders_model(),
        referenced_models=[_customers_model()],
    )


# ---------------------------------------------------------------------------
# filter_referenced_slot_ids helper
# ---------------------------------------------------------------------------


class TestFilterReferencedSlotIds:
    def test_simple_column_filter(self) -> None:
        from slayer.engine.planning import filter_referenced_slot_ids

        reg = ValueRegistry()
        col = ColumnKey(path=(), leaf="amount")
        sid = reg.intern(key=col, declared_name="amount", phase=Phase.ROW)
        # Filter: amount > 0
        bf = BoundFilter(
            value_key=ArithmeticKey(
                op=">",
                operands=(col, LiteralKey(value=0)),
            ),
            phase=Phase.ROW,
            referenced_keys=(col, LiteralKey(value=0)),
        )
        result = filter_referenced_slot_ids(bf, reg)
        assert result == {sid}

    def test_composite_predicate_collects_all_slot_leaves(self) -> None:
        # Filter: ``status == 'paid' AND customers.revenue:sum < 500``
        from slayer.engine.planning import filter_referenced_slot_ids

        reg = ValueRegistry()
        status = ColumnKey(path=(), leaf="status")
        agg = AggregateKey(
            source=ColumnKey(path=("customers",), leaf="revenue"),
            agg="sum",
        )
        sid_status = reg.intern(key=status, declared_name="status", phase=Phase.ROW)
        sid_agg = reg.intern(
            key=agg, declared_name="customers_revenue_sum", phase=Phase.AGGREGATE,
        )

        cmp_left = ArithmeticKey(
            op="==", operands=(status, LiteralKey(value="paid")),
        )
        cmp_right = ArithmeticKey(
            op="<", operands=(agg, LiteralKey(value=500)),
        )
        predicate = ArithmeticKey(op="and", operands=(cmp_left, cmp_right))
        bf = BoundFilter(
            value_key=predicate,
            phase=Phase.AGGREGATE,
            referenced_keys=(predicate, cmp_left, cmp_right, status, agg),
        )
        result = filter_referenced_slot_ids(bf, reg)
        assert result == {sid_status, sid_agg}

    def test_composite_only_nodes_not_in_result(self) -> None:
        # Top-level ArithmeticKey itself doesn't show up — only leaf
        # slottable refs.
        from slayer.engine.planning import filter_referenced_slot_ids

        reg = ValueRegistry()
        col = ColumnKey(path=(), leaf="amount")
        reg.intern(key=col, declared_name="amount", phase=Phase.ROW)
        predicate = ArithmeticKey(
            op=">",
            operands=(col, LiteralKey(value=0)),
        )
        bf = BoundFilter(
            value_key=predicate,
            phase=Phase.ROW,
            referenced_keys=(predicate, col, LiteralKey(value=0)),
        )
        result = filter_referenced_slot_ids(bf, reg)
        # No ArithmeticKey id in result.
        assert len(result) == 1

    def test_unknown_slot_silently_skipped(self) -> None:
        # If a referenced key isn't in the registry (e.g., a literal,
        # or an arithmetic that didn't intern), the walker silently
        # skips it rather than raising.
        from slayer.engine.planning import filter_referenced_slot_ids

        reg = ValueRegistry()
        # No interns.
        col = ColumnKey(path=(), leaf="amount")
        bf = BoundFilter(
            value_key=ArithmeticKey(
                op=">",
                operands=(col, LiteralKey(value=0)),
            ),
            phase=Phase.ROW,
            referenced_keys=(col, LiteralKey(value=0)),
        )
        result = filter_referenced_slot_ids(bf, reg)
        assert result == set()


# ---------------------------------------------------------------------------
# plan_query populates cross_model_aggregate_plans
# ---------------------------------------------------------------------------


class TestPlanQueryCrossModelWiring:
    def test_local_aggregate_no_cross_model_plan(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "amount:sum"}],
        )
        planned = plan_query(query=q, bundle=_bundle())
        assert planned.cross_model_aggregate_plans == []

    def test_cross_model_aggregate_emits_plan(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "customers.revenue:sum"}],
        )
        planned = plan_query(query=q, bundle=_bundle())
        assert len(planned.cross_model_aggregate_plans) == 1
        plan = planned.cross_model_aggregate_plans[0]
        assert plan.target_model == "customers"
        assert plan.datasource == "prod"

    def test_cross_model_plan_carries_aggregate_slot_id(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "customers.revenue:sum"}],
        )
        planned = plan_query(query=q, bundle=_bundle())
        plan = planned.cross_model_aggregate_plans[0]
        # The plan references the slot id of the aggregate it's
        # materialising.
        all_slots = (
            planned.row_slots
            + planned.aggregate_slots
            + planned.combined_expression_slots
        )
        agg_slot = next(
            s for s in all_slots
            if isinstance(s.key, AggregateKey)
            and getattr(s.key.source, "path", ()) == ("customers",)
        )
        assert plan.aggregate_slot_id == agg_slot.id

    def test_two_distinct_cross_model_aggregates_emit_separate_plans(self) -> None:
        # customers.revenue:sum + customers.revenue:avg → two CTEs
        # (one per AggregateKey).
        q = SlayerQuery(
            source_model="orders",
            measures=[
                {"formula": "customers.revenue:sum"},
                {"formula": "customers.revenue:avg"},
            ],
        )
        planned = plan_query(query=q, bundle=_bundle())
        assert len(planned.cross_model_aggregate_plans) == 2
        target_aggs = {
            (p.target_model, _slot_agg(planned, p.aggregate_slot_id).key.agg)
            for p in planned.cross_model_aggregate_plans
        }
        assert target_aggs == {("customers", "sum"), ("customers", "avg")}

    def test_local_filter_does_not_propagate_to_cte(self) -> None:
        # ``status == 'paid'`` is a host-local row filter → DROP_HOST_LOCAL
        # → not in the CTE's WHERE or HAVING.
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "customers.revenue:sum"}],
            filters=["status == 'paid'"],
        )
        planned = plan_query(query=q, bundle=_bundle())
        plan = planned.cross_model_aggregate_plans[0]
        assert plan.where_filter_ids == []
        assert plan.having_filter_ids == []

    def test_parameterized_aggregates_get_distinct_cte_aliases(self) -> None:
        # ``customers.revenue:percentile(p=0.5)`` and ``p=0.95`` produce
        # two distinct cross-model aggregate slots; the CTE column
        # aliases must differ so the generator can target them
        # independently. (Codex HIGH #1 fold-in for 7b.5: include the
        # aggregate signature in the CTE column alias.)
        host = _orders_model()
        target = _customers_model()
        bundle = ResolvedSourceBundle(
            source_model=host, referenced_models=[target],
        )
        q = SlayerQuery(
            source_model="orders",
            measures=[
                {"formula": "customers.revenue:percentile(p=0.5)"},
                {"formula": "customers.revenue:percentile(p=0.95)"},
            ],
        )
        planned = plan_query(query=q, bundle=bundle)
        assert len(planned.cross_model_aggregate_plans) == 2
        cte_aliases = {
            col.name
            for plan in planned.cross_model_aggregate_plans
            for col in plan.cte_stage_schema.columns
            if col.provenance == "agg:percentile"
        }
        # Two distinct aliases — not just "revenue_percentile" twice.
        assert len(cte_aliases) == 2, cte_aliases

    def test_target_model_filters_propagate(self) -> None:
        # Customers has an always-applied filter — must surface on the
        # cross-model CTE plan.
        host = _orders_model()
        target = _customers_model().model_copy(
            update={"filters": ["region IS NOT NULL"]},
        )
        bundle = ResolvedSourceBundle(
            source_model=host, referenced_models=[target],
        )
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "customers.revenue:sum"}],
        )
        planned = plan_query(query=q, bundle=bundle)
        plan = planned.cross_model_aggregate_plans[0]
        assert plan.target_model_filters == ["region IS NOT NULL"]


# ---------------------------------------------------------------------------
# classify_host_filter exercise via plan_query
# ---------------------------------------------------------------------------


class TestHostFilterRoutingViaPlanQuery:
    def test_classifier_returns_expected_route_for_local_only(self) -> None:
        # Direct unit on classify_host_filter — make sure the wiring
        # passes the correct host_model_name through.
        host = _orders_model()
        status_slot = type("S", (), {"id": "s1", "key": ColumnKey(path=(), leaf="status")})()
        hf = HostFilterRouting(
            filter_id="f1", phase=Phase.ROW,
            referenced_slot_ids=["s1"], text="status == 'paid'",
        )
        route = classify_host_filter(
            host_filter=hf,
            host_slots=[status_slot],
            target_path=("customers",),
            host_model_name=host.name,
        )
        assert route is FilterRoute.DROP_HOST_LOCAL


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _slot_agg(planned, slot_id):
    for s in (
        planned.row_slots
        + planned.aggregate_slots
        + planned.combined_expression_slots
    ):
        if s.id == slot_id:
            return s
    raise AssertionError(f"slot {slot_id!r} not found")
