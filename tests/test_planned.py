"""Stage 7a.1 (DEV-1450) — typed plan shapes.

`PlannedQuery` is the final output of the planning pipeline that the
SQL generator (stage 7b) consumes. The shapes here are the typed
containers; the planning logic that fills them lives in
`planning.py`, `cross_model_planner.py`, and `stage_planner.py`
(other 7a substages).

This file pins the field surface so downstream substages have a
stable target.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType, JoinType
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    Phase,
    StarKey,
)
from slayer.core.scope import StageColumn, StageSchema
from slayer.engine.planned import (
    BoundExpr,
    CrossModelAggregatePlan,
    FilterPhase,
    JoinRequirement,
    OrderEntry,
    PlannedQuery,
    TransformLayer,
    ValueSlot,
)


# ---------------------------------------------------------------------------
# ValueSlot
# ---------------------------------------------------------------------------


class TestValueSlot:
    def test_minimal(self):
        key = ColumnKey(path=(), leaf="status")
        s = ValueSlot(id="s1", key=key, declared_name="status", phase=Phase.ROW)
        assert s.id == "s1"
        assert s.key == key
        assert s.declared_name == "status"
        assert s.public_name is None
        assert s.public_aliases == []
        assert s.hidden is False
        assert s.label is None
        assert s.type is None

    def test_with_metadata(self):
        key = AggregateKey(source=ColumnKey(path=(), leaf="rev"), agg="sum")
        s = ValueSlot(
            id="a1",
            key=key,
            declared_name="revenue_sum",
            public_name="revenue_sum",
            public_aliases=["revenue_sum"],
            phase=Phase.AGGREGATE,
            label="Total revenue",
            type=DataType.DOUBLE,
        )
        assert s.public_name == "revenue_sum"
        assert s.public_aliases == ["revenue_sum"]
        assert s.label == "Total revenue"
        assert s.type is DataType.DOUBLE

    def test_hidden_no_public_alias(self):
        # Order-only / filter-only refs intern as hidden slots with no
        # public alias (P11).
        key = AggregateKey(source=ColumnKey(path=(), leaf="rev"), agg="sum")
        s = ValueSlot(
            id="h1",
            key=key,
            declared_name="_h_revenue_sum",
            phase=Phase.AGGREGATE,
            hidden=True,
        )
        assert s.hidden is True
        assert s.public_name is None
        assert s.public_aliases == []

    def test_multi_alias(self):
        # P4: two measures with the same structural key but different
        # declared `name`s share one slot internally; both aliases appear
        # in the public projection.
        key = AggregateKey(source=ColumnKey(path=(), leaf="rev"), agg="sum")
        s = ValueSlot(
            id="m1",
            key=key,
            declared_name="revenue_sum",
            public_name="rev",
            public_aliases=["rev", "revenue_sum"],
            phase=Phase.AGGREGATE,
        )
        assert s.public_aliases == ["rev", "revenue_sum"]

    def test_hidden_with_public_name_rejected(self):
        key = AggregateKey(source=ColumnKey(path=(), leaf="rev"), agg="sum")
        with pytest.raises(ValueError, match="hidden but carries"):
            ValueSlot(
                id="h",
                key=key,
                declared_name="_h",
                phase=Phase.AGGREGATE,
                hidden=True,
                public_name="rev",
            )

    def test_hidden_with_public_aliases_rejected(self):
        key = AggregateKey(source=ColumnKey(path=(), leaf="rev"), agg="sum")
        with pytest.raises(ValueError, match="hidden but carries"):
            ValueSlot(
                id="h",
                key=key,
                declared_name="_h",
                phase=Phase.AGGREGATE,
                hidden=True,
                public_aliases=["rev"],
            )

    def test_with_expression_payload(self):
        # ValueSlot carries a BoundExpr so the SQL generator can
        # render without a side map. After 7b.6 the planned-side
        # BoundExpr is a re-export of the binder's class; the
        # ``sql_text`` cache field is gone (rendering walks the
        # ``value_key`` against the slot registry instead of a
        # cached string).
        key = ColumnKey(path=(), leaf="status")
        expr = BoundExpr(value_key=key)
        s = ValueSlot(
            id="s1",
            key=key,
            declared_name="status",
            phase=Phase.ROW,
            expression=expr,
        )
        assert s.expression is expr
        assert s.expression.value_key == key


# ---------------------------------------------------------------------------
# JoinRequirement
# ---------------------------------------------------------------------------


class TestJoinRequirement:
    def test_basic(self):
        j = JoinRequirement(
            source_model="orders",
            target_model="customers",
            join_pairs=[["customer_id", "id"]],
        )
        assert j.source_model == "orders"
        assert j.target_model == "customers"
        assert j.join_pairs == [["customer_id", "id"]]

    def test_multi_column_join(self):
        j = JoinRequirement(
            source_model="orders",
            target_model="line_items",
            join_pairs=[["id", "order_id"], ["region", "region"]],
        )
        assert len(j.join_pairs) == 2

    def test_default_join_type_left(self):
        # Codex review fix: join_type was missing originally — defaults
        # to LEFT to match ModelJoin.
        j = JoinRequirement(
            source_model="orders",
            target_model="customers",
            join_pairs=[["customer_id", "id"]],
        )
        assert j.join_type is JoinType.LEFT

    def test_inner_join_type(self):
        j = JoinRequirement(
            source_model="orders",
            target_model="customers",
            join_pairs=[["customer_id", "id"]],
            join_type=JoinType.INNER,
        )
        assert j.join_type is JoinType.INNER

    def test_empty_join_pairs_rejected(self):
        with pytest.raises(ValueError, match="non-empty"):
            JoinRequirement(
                source_model="orders",
                target_model="customers",
                join_pairs=[],
            )

    def test_malformed_pair_rejected(self):
        with pytest.raises(ValueError, match="must be"):
            JoinRequirement(
                source_model="orders",
                target_model="customers",
                join_pairs=[["only_one"]],
            )


# ---------------------------------------------------------------------------
# CrossModelAggregatePlan
# ---------------------------------------------------------------------------


class TestCrossModelAggregatePlan:
    def test_minimal(self):
        cte_schema = StageSchema(
            relation_name="cma_customers__rev",
            columns=[
                StageColumn(name="customer_id", sql_alias="customer_id"),
                StageColumn(name="revenue_sum", sql_alias="revenue_sum"),
            ],
        )
        plan = CrossModelAggregatePlan(
            aggregate_slot_id="a1",
            target_model="customers",
            datasource="prod",
            join_chain=[
                JoinRequirement(
                    source_model="orders",
                    target_model="customers",
                    join_pairs=[["customer_id", "id"]],
                ),
            ],
            cte_stage_schema=cte_schema,
            shared_grain_slots=["d_customer_id"],
            applied_filter_ids=[],
        )
        assert plan.aggregate_slot_id == "a1"
        assert plan.target_model == "customers"
        assert plan.hidden is False
        assert plan.public_alias is None
        assert plan.dropped_filter_warnings == []
        assert plan.join_back_pairs == []

    def test_hidden_no_public_alias(self):
        cte_schema = StageSchema(relation_name="cma_h", columns=[])
        plan = CrossModelAggregatePlan(
            aggregate_slot_id="a1",
            target_model="customers",
            datasource="prod",
            join_chain=[],
            cte_stage_schema=cte_schema,
            shared_grain_slots=[],
            applied_filter_ids=[],
            hidden=True,
        )
        assert plan.hidden is True
        assert plan.public_alias is None


# ---------------------------------------------------------------------------
# TransformLayer
# ---------------------------------------------------------------------------


class TestTransformLayer:
    def test_basic(self):
        layer = TransformLayer(op="cumsum", slot_ids=["t1", "t2"])
        assert layer.op == "cumsum"
        assert layer.slot_ids == ["t1", "t2"]

    def test_time_shift_layer(self):
        # time_shift transforms are emitted as self-join CTEs; the
        # layer struct only carries the slot ids — generator picks the
        # render strategy per op.
        layer = TransformLayer(op="time_shift", slot_ids=["t1"])
        assert layer.op == "time_shift"


# ---------------------------------------------------------------------------
# FilterPhase
# ---------------------------------------------------------------------------


class TestFilterPhase:
    def test_where_phase(self):
        f = FilterPhase(
            id="f1",
            phase=Phase.ROW,
            text="status = 'paid'",
        )
        assert f.phase is Phase.ROW

    def test_having_phase(self):
        f = FilterPhase(
            id="f2",
            phase=Phase.AGGREGATE,
            text="revenue:sum > 100",
        )
        assert f.phase is Phase.AGGREGATE

    def test_post_phase(self):
        f = FilterPhase(
            id="f3",
            phase=Phase.POST,
            text="change(revenue:sum) > 0",
        )
        assert f.phase is Phase.POST


# ---------------------------------------------------------------------------
# OrderEntry
# ---------------------------------------------------------------------------


class TestOrderEntry:
    def test_asc(self):
        o = OrderEntry(slot_id="s1", direction="asc")
        assert o.direction == "asc"

    def test_desc(self):
        o = OrderEntry(slot_id="s1", direction="desc")
        assert o.direction == "desc"

    def test_invalid_direction_rejected(self):
        with pytest.raises(ValueError):
            OrderEntry(slot_id="s1", direction="random")  # type: ignore[arg-type]

    def test_uppercase_direction_rejected(self):
        # OrderEntry is planner-produced — strict lowercase is intentional.
        # If user input ever feeds it directly, the caller must lowercase.
        with pytest.raises(ValueError):
            OrderEntry(slot_id="s1", direction="ASC")  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            OrderEntry(slot_id="s1", direction="DESC")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# PlannedQuery
# ---------------------------------------------------------------------------


class TestPlannedQuery:
    def test_minimal_zero_dimension_count(self):
        # The simplest possible PlannedQuery: just *:count on a model.
        agg_key = AggregateKey(source=StarKey(), agg="count")
        slot = ValueSlot(
            id="a1",
            key=agg_key,
            declared_name="_count",
            public_name="_count",
            public_aliases=["_count"],
            phase=Phase.AGGREGATE,
        )
        pq = PlannedQuery(
            source_relation="orders",
            aggregate_slots=[slot],
            projection=["a1"],
        )
        assert pq.source_relation == "orders"
        assert pq.aggregate_slots == [slot]
        assert pq.row_slots == []
        assert pq.projection == ["a1"]
        assert pq.cross_model_aggregate_plans == []
        assert pq.transform_layers == []
        assert pq.filters_by_phase == []
        assert pq.order == []
        assert pq.limit is None
        assert pq.offset is None
        assert pq.stage_schema is None

    def test_with_dimension_and_filter(self):
        dim_key = ColumnKey(path=(), leaf="status")
        dim_slot = ValueSlot(
            id="d1",
            key=dim_key,
            declared_name="status",
            public_name="status",
            public_aliases=["status"],
            phase=Phase.ROW,
        )
        agg_key = AggregateKey(source=ColumnKey(path=(), leaf="rev"), agg="sum")
        agg_slot = ValueSlot(
            id="a1",
            key=agg_key,
            declared_name="revenue_sum",
            public_name="revenue_sum",
            public_aliases=["revenue_sum"],
            phase=Phase.AGGREGATE,
        )
        filt = FilterPhase(id="f1", phase=Phase.ROW, text="status IS NOT NULL")
        pq = PlannedQuery(
            source_relation="orders",
            row_slots=[dim_slot],
            aggregate_slots=[agg_slot],
            filters_by_phase=[filt],
            projection=["d1", "a1"],
        )
        assert len(pq.row_slots) == 1
        assert len(pq.aggregate_slots) == 1
        assert pq.filters_by_phase == [filt]

    def test_with_combined_expression(self):
        # An ArithmeticKey-keyed slot at aggregate phase (max of operands).
        a = AggregateKey(source=ColumnKey(path=(), leaf="rev"), agg="sum")
        b = AggregateKey(source=StarKey(), agg="count")
        arith = ArithmeticKey(op="/", operands=(a, b))
        slot = ValueSlot(
            id="e1",
            key=arith,
            declared_name="aov",
            phase=Phase.AGGREGATE,
        )
        pq = PlannedQuery(
            source_relation="orders",
            combined_expression_slots=[slot],
        )
        assert pq.combined_expression_slots == [slot]

    def test_with_stage_schema(self):
        # The planner emits a StageSchema so downstream stages can bind
        # against it (P6).
        ss = StageSchema(
            relation_name="stage_a",
            columns=[StageColumn(name="rev", sql_alias="rev")],
        )
        pq = PlannedQuery(source_relation="orders", stage_schema=ss)
        assert pq.stage_schema is ss

    def test_limit_offset(self):
        pq = PlannedQuery(source_relation="orders", limit=10, offset=20)
        assert pq.limit == 10
        assert pq.offset == 20


# ---------------------------------------------------------------------------
# Smoke: types compose
# ---------------------------------------------------------------------------


class TestCompose:
    def test_cross_model_in_planned(self):
        cte_schema = StageSchema(relation_name="cma1", columns=[])
        cma = CrossModelAggregatePlan(
            aggregate_slot_id="a1",
            target_model="customers",
            datasource="prod",
            join_chain=[],
            cte_stage_schema=cte_schema,
            shared_grain_slots=[],
            applied_filter_ids=[],
        )
        pq = PlannedQuery(
            source_relation="orders",
            cross_model_aggregate_plans=[cma],
        )
        assert len(pq.cross_model_aggregate_plans) == 1

    def test_transform_layer_in_planned(self):
        layer = TransformLayer(op="cumsum", slot_ids=["t1"])
        pq = PlannedQuery(source_relation="orders", transform_layers=[layer])
        assert pq.transform_layers == [layer]

    def test_order_in_planned(self):
        oe = OrderEntry(slot_id="s1", direction="desc")
        pq = PlannedQuery(source_relation="orders", order=[oe])
        assert pq.order == [oe]
