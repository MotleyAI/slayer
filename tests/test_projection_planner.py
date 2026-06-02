"""Stage 7a.6 (DEV-1450) — ProjectionPlanner tests.

The ProjectionPlanner allocates slots for declared measures + creates
hidden slots for refs that appear ONLY in order/filter (the "hidden
slot" semantics from the issue spec). Outputs a ``ProjectionPlan``
carrying the ValueRegistry, the public-projection ordering, the order
entries, and the bound filters by phase.
"""

from __future__ import annotations

from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    LiteralKey,
    Phase,
)
from decimal import Decimal
from slayer.engine.binding import BoundExpr, BoundFilter
from slayer.engine.planning import (
    DeclaredMeasure,
    OrderSpec,
    ProjectionPlanner,
)


def _amount_sum() -> AggregateKey:
    return AggregateKey(
        source=ColumnKey(path=(), leaf="amount"), agg="sum",
    )


def _status() -> ColumnKey:
    return ColumnKey(path=(), leaf="status")


class TestDeclaredMeasures:
    def test_declared_measure_becomes_public_slot(self):
        planner = ProjectionPlanner()
        plan = planner.plan(
            measures=[
                DeclaredMeasure(
                    bound=BoundExpr(value_key=_amount_sum()),
                    declared_name="amount_sum",
                    public_name="amount_sum",
                ),
            ],
            filters=[],
            order=[],
        )
        public_slots = [
            plan.registry.get(sid) for sid in plan.public_projection
        ]
        assert len(public_slots) == 1
        slot = public_slots[0]
        assert not slot.hidden
        assert "amount_sum" in slot.public_aliases

    def test_two_measures_with_same_key_share_slot(self):
        planner = ProjectionPlanner()
        plan = planner.plan(
            measures=[
                DeclaredMeasure(
                    bound=BoundExpr(value_key=_amount_sum()),
                    declared_name="rev1",
                    public_name="rev1",
                ),
                DeclaredMeasure(
                    bound=BoundExpr(value_key=_amount_sum()),
                    declared_name="rev2",
                    public_name="rev2",
                ),
            ],
            filters=[],
            order=[],
        )
        # Both public_projection entries point at the same slot.
        assert plan.public_projection[0] == plan.public_projection[1]
        slot = plan.registry.get(plan.public_projection[0])
        assert set(slot.public_aliases) == {"rev1", "rev2"}


class TestHiddenSlots:
    def test_filter_only_ref_becomes_hidden(self):
        # ORDER BY amount:sum DESC LIMIT 10 with no declared measure for
        # amount:sum. The slot is materialised hidden.
        planner = ProjectionPlanner()
        plan = planner.plan(
            measures=[
                DeclaredMeasure(
                    bound=BoundExpr(value_key=_status()),
                    declared_name="status",
                    public_name="status",
                ),
            ],
            filters=[
                BoundFilter(
                    value_key=ArithmeticKey(
                        op=">",
                        operands=(_amount_sum(), LiteralKey(value=Decimal(100))),
                    ),
                    phase=Phase.AGGREGATE,
                    referenced_keys=(_amount_sum(),),
                ),
            ],
            order=[],
        )
        # amount:sum is allocated as a hidden slot — not in public projection.
        agg_slot_id = plan.registry.find_by_key(_amount_sum())
        assert agg_slot_id is not None
        assert agg_slot_id not in plan.public_projection
        slot = plan.registry.get(agg_slot_id)
        assert slot.hidden

    def test_order_only_ref_becomes_hidden(self):
        planner = ProjectionPlanner()
        plan = planner.plan(
            measures=[
                DeclaredMeasure(
                    bound=BoundExpr(value_key=_status()),
                    declared_name="status",
                    public_name="status",
                ),
            ],
            filters=[],
            order=[
                OrderSpec(
                    bound=BoundExpr(value_key=_amount_sum()),
                    direction="desc",
                ),
            ],
        )
        agg_slot_id = plan.registry.find_by_key(_amount_sum())
        assert agg_slot_id is not None
        slot = plan.registry.get(agg_slot_id)
        assert slot.hidden

    def test_declared_then_used_in_filter_stays_public(self):
        planner = ProjectionPlanner()
        plan = planner.plan(
            measures=[
                DeclaredMeasure(
                    bound=BoundExpr(value_key=_amount_sum()),
                    declared_name="rev",
                    public_name="rev",
                ),
            ],
            filters=[
                BoundFilter(
                    value_key=ArithmeticKey(
                        op=">",
                        operands=(_amount_sum(), LiteralKey(value=Decimal(100))),
                    ),
                    phase=Phase.AGGREGATE,
                    referenced_keys=(_amount_sum(),),
                ),
            ],
            order=[],
        )
        agg_slot_id = plan.registry.find_by_key(_amount_sum())
        assert agg_slot_id in plan.public_projection
        slot = plan.registry.get(agg_slot_id)
        assert not slot.hidden


class TestPlanFilters:
    def test_filters_attached_to_plan(self):
        planner = ProjectionPlanner()
        f = BoundFilter(
            value_key=ArithmeticKey(
                op="==", operands=(_status(), LiteralKey(value="paid")),
            ),
            phase=Phase.ROW,
            referenced_keys=(_status(),),
        )
        plan = planner.plan(
            measures=[
                DeclaredMeasure(
                    bound=BoundExpr(value_key=_status()),
                    declared_name="status",
                    public_name="status",
                ),
            ],
            filters=[f],
            order=[],
        )
        assert len(plan.filters) == 1

    def test_filter_only_interns_slot_worthy_keys(self):
        # `amount:sum > 100`: only `amount:sum` is slot-worthy. The
        # ArithmeticKey root and the literal 100 must NOT create slots.
        planner = ProjectionPlanner()
        agg = _amount_sum()
        f = BoundFilter(
            value_key=ArithmeticKey(
                op=">", operands=(agg, LiteralKey(value=Decimal(100))),
            ),
            phase=Phase.AGGREGATE,
            referenced_keys=(agg, LiteralKey(value=Decimal(100))),
        )
        plan = planner.plan(
            measures=[
                DeclaredMeasure(
                    bound=BoundExpr(value_key=_status()),
                    declared_name="status",
                    public_name="status",
                ),
            ],
            filters=[f],
            order=[],
        )
        # Slots: status (public) + amount:sum (hidden). No more.
        assert len(plan.registry.slots) == 2
        assert plan.registry.find_by_key(agg) is not None
        assert plan.registry.find_by_key(_status()) is not None

    def test_order_arithmetic_walks_to_aggregate(self):
        # `ORDER BY amount:sum + 1` with no declared amount:sum: the
        # planner must intern the inner aggregate as a hidden slot, NOT
        # the arithmetic root.
        planner = ProjectionPlanner()
        agg = _amount_sum()
        order_expr = ArithmeticKey(
            op="+", operands=(agg, LiteralKey(value=Decimal(1))),
        )
        plan = planner.plan(
            measures=[
                DeclaredMeasure(
                    bound=BoundExpr(value_key=_status()),
                    declared_name="status",
                    public_name="status",
                ),
            ],
            filters=[],
            order=[
                OrderSpec(bound=BoundExpr(value_key=order_expr),
                          direction="desc"),
            ],
        )
        # Slots: status (public) + amount:sum (hidden).
        assert plan.registry.find_by_key(agg) is not None
        # ArithmeticKey root NOT slotted.
        assert plan.registry.find_by_key(order_expr) is None
        # LiteralKey NOT slotted.
        assert plan.registry.find_by_key(LiteralKey(value=Decimal(1))) is None
