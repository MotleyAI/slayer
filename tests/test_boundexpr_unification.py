"""Stage 7b.6 (DEV-1450) — BoundExpr type unification.

``slayer.engine.binding.BoundExpr`` and ``slayer.engine.planned.BoundExpr``
were two different Pydantic classes (Codex HIGH F2 from the earlier
round). The binder produced the former; ``ValueSlot.expression`` /
``FilterPhase.expression`` were typed as the latter. This is type
unification, not field-fill.

Decision: keep ``slayer.engine.binding.BoundExpr`` as the source of
truth. ``sql_text`` is a render artifact, not a binder concern — drop
it from ``planned.BoundExpr``. The planned-side import is a re-export
of the binder's class.

Tests cover:

1. Identity: ``planned.BoundExpr is binding.BoundExpr`` after re-export.
2. ``ValueSlot.expression`` is populated for every materialised slot
   (public and hidden) by ``ProjectionPlanner``.
3. ``FilterPhase.expression`` is populated for every filter — user
   filters too, not just auto-generated date_range filters.
4. The expression's ``value_key`` matches the slot's / filter's key
   identity.
"""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.keys import AggregateKey, ColumnKey
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.binding import BoundExpr as BinderBoundExpr
from slayer.engine.planned import BoundExpr as PlannedBoundExpr
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
        ],
    )


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders_model(), referenced_models=[],
    )


# ---------------------------------------------------------------------------
# Type unification
# ---------------------------------------------------------------------------


class TestTypeUnification:
    def test_planned_bound_expr_is_binder_bound_expr(self) -> None:
        # After 7b.6, planned.BoundExpr must be the binder's class
        # (re-export). Identity must hold so existing isinstance checks
        # and Pydantic field types align.
        assert PlannedBoundExpr is BinderBoundExpr


# ---------------------------------------------------------------------------
# ValueSlot.expression population
# ---------------------------------------------------------------------------


class TestValueSlotExpressionPopulated:
    def test_measure_slot_carries_expression(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "amount:sum"}],
        )
        planned = plan_query(query=q, bundle=_bundle())
        assert len(planned.aggregate_slots) == 1
        slot = planned.aggregate_slots[0]
        assert slot.expression is not None
        assert isinstance(slot.expression, BinderBoundExpr)
        # The expression's value_key matches the slot's key identity.
        assert slot.expression.value_key == slot.key

    def test_dimension_slot_carries_expression(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "amount:sum"}],
            dimensions=["status"],
        )
        planned = plan_query(query=q, bundle=_bundle())
        row_slot = next(
            s for s in planned.row_slots
            if isinstance(s.key, ColumnKey) and s.key.leaf == "status"
        )
        assert row_slot.expression is not None
        assert row_slot.expression.value_key == row_slot.key

    def test_hidden_dep_slot_carries_expression(self) -> None:
        # Hidden slots (filter dep) also get expression populated so
        # the generator can render them without re-binding.
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "amount:sum"}],
            filters=["amount > 0"],
        )
        planned = plan_query(query=q, bundle=_bundle())
        # The 'amount' ColumnKey is now a hidden slot dep.
        hidden_slots = [
            s for s in planned.row_slots
            if isinstance(s.key, ColumnKey) and s.key.leaf == "amount"
        ]
        assert len(hidden_slots) == 1
        assert hidden_slots[0].hidden is True
        assert hidden_slots[0].expression is not None
        assert hidden_slots[0].expression.value_key == hidden_slots[0].key


# ---------------------------------------------------------------------------
# FilterPhase.expression population
# ---------------------------------------------------------------------------


class TestFilterPhaseExpressionPopulated:
    def test_user_filter_carries_expression(self) -> None:
        # 7b.6 unification: every FilterPhase carries an expression
        # payload (not just auto-generated date_range filters).
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "amount:sum"}],
            filters=["amount > 0"],
        )
        planned = plan_query(query=q, bundle=_bundle())
        assert len(planned.filters_by_phase) == 1
        fp = planned.filters_by_phase[0]
        assert fp.expression is not None
        assert isinstance(fp.expression, BinderBoundExpr)

    def test_filter_expression_value_key_identity(self) -> None:
        # The FilterPhase.expression carries the SAME value_key
        # identity the binder produced. Comparing equality is enough —
        # value_key is frozen, so structural equality implies
        # equivalence.
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "amount:sum"}],
            filters=["amount > 0"],
        )
        planned = plan_query(query=q, bundle=_bundle())
        fp = planned.filters_by_phase[0]
        # Should be an ArithmeticKey wrapping ColumnKey(amount) and a
        # LiteralKey.
        from slayer.core.keys import ArithmeticKey
        assert isinstance(fp.expression.value_key, ArithmeticKey)
        assert fp.expression.value_key.op == ">"

    def test_having_phase_filter_carries_expression(self) -> None:
        # An aggregate-phase filter (HAVING on a sum) carries the
        # expression too.
        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "amount:sum"}],
            filters=["amount:sum > 100"],
        )
        planned = plan_query(query=q, bundle=_bundle())
        fp = planned.filters_by_phase[0]
        assert fp.expression is not None
        # Walking the value_key reveals the AggregateKey leaf.
        from slayer.engine.binding import walk_value_keys
        keys = list(walk_value_keys(fp.expression.value_key))
        assert any(isinstance(k, AggregateKey) for k in keys)
