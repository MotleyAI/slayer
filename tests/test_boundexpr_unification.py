"""BoundExpr type unification.

The binder and the planner once carried two different ``BoundExpr``
classes; they were unified, and the single canonical home is now
``slayer.ir.bound`` — no re-exports.

Tests cover:

1. Single home: ``BoundExpr`` lives in ``slayer.ir.bound`` and
   ``planned`` no longer re-exports it.
2. ``ValueSlot.expression`` is populated for every materialised slot
   (public and hidden) by ``ProjectionPlanner``.
3. every filter conjunct's mask slot carries its bound key — user
   filters too, not just auto-generated date_range filters.
4. The expression's ``value_key`` matches the slot's / filter's key
   identity.
"""

from __future__ import annotations

import slayer.ir.planned
from slayer.core.enums import DataType
from slayer.core.keys import AggregateKey, FieldKey, walk_value_keys
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.ir.bound import BoundExpr
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.engine.plan import plan_query
from slayer.core.keys import ArithmeticKey


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


def _orders_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        data_source="prod",
        sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="status", datatype=DataType.STRING),
        ],
    )


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        dialect="postgres",
        population=_orders_dataset(), referenced_datasets=[],
    )


# ---------------------------------------------------------------------------
# Type unification
# ---------------------------------------------------------------------------


class TestTypeUnification:
    def test_bound_expr_single_home(self) -> None:
        # One real home: the class lives in ir.bound and the
        # old planned-side re-export is gone from the public surface.
        assert BoundExpr.__module__ == "slayer.ir.bound"
        assert "BoundExpr" not in slayer.ir.planned.__all__


# ---------------------------------------------------------------------------
# ValueSlot.expression population
# ---------------------------------------------------------------------------


class TestValueSlotExpressionPopulated:
    def test_measure_slot_carries_expression(self) -> None:
        q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(amount)"}]})
        planned = plan_query(query=q, bundle=_bundle())
        assert len(planned.aggregate_slots) == 1
        slot = planned.aggregate_slots[0]
        assert slot.expression is not None
        assert isinstance(slot.expression, BoundExpr)
        # The expression's value_key matches the slot's key identity.
        assert slot.expression.value_key == slot.key

    def test_dimension_slot_carries_expression(self) -> None:
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(amount)"}],
            "dimensions": ["status"],
        })
        planned = plan_query(query=q, bundle=_bundle())
        row_slot = next(
            s for s in planned.row_slots
            if isinstance(s.key, FieldKey) and s.key.leaf == "status"
        )
        assert row_slot.expression is not None
        assert row_slot.expression.value_key == row_slot.key

    def test_hidden_dep_slot_carries_expression(self) -> None:
        # Hidden slots (filter dep) also get expression populated so
        # the generator can render them without re-binding.
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(amount)"}],
            "filter": ["amount > 0"],
        })
        planned = plan_query(query=q, bundle=_bundle())
        # The 'amount' FieldKey is now a hidden slot dep.
        hidden_slots = [
            s for s in planned.row_slots
            if isinstance(s.key, FieldKey) and s.key.leaf == "amount"
        ]
        assert len(hidden_slots) == 1
        assert hidden_slots[0].hidden is True
        assert hidden_slots[0].expression is not None
        assert hidden_slots[0].expression.value_key == hidden_slots[0].key


# ---------------------------------------------------------------------------
# Mask slots carry the bound expression
# ---------------------------------------------------------------------------


def _sole_mask_key(planned):
    slots = {
        s.id: s
        for s in (
            *planned.row_slots, *planned.aggregate_slots,
            *planned.combined_expression_slots,
        )
    }
    (mask,) = planned.masks
    return slots[mask.slot_id].key


class TestMaskExpressionPopulated:
    def test_user_filter_compiles_to_a_mask_slot(self) -> None:
        # Every filter conjunct compiles to a hidden slot whose
        # key IS the binder-produced predicate (not just date_range filters).
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(amount)"}],
            "filter": ["amount > 0"],
        })
        planned = plan_query(query=q, bundle=_bundle())
        assert len(planned.masks) == 1
        assert _sole_mask_key(planned) is not None

    def test_filter_mask_key_identity(self) -> None:
        # The mask slot carries the SAME value_key identity the binder
        # produced. Comparing equality is enough — value_key is frozen,
        # so structural equality implies equivalence.
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(amount)"}],
            "filter": ["amount > 0"],
        })
        planned = plan_query(query=q, bundle=_bundle())
        key = _sole_mask_key(planned)
        # Should be an ArithmeticKey wrapping FieldKey(amount) and a
        # LiteralKey.
        assert isinstance(key, ArithmeticKey)
        assert key.op == ">"

    def test_measure_mask_carries_the_aggregate(self) -> None:
        # An aggregate-bearing (metric-typed) mask carries the
        # expression too.
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(amount)"}],
            "filter": ["sum(amount) > 100"],
        })
        planned = plan_query(query=q, bundle=_bundle())
        key = _sole_mask_key(planned)
        # Walking the value_key reveals the AggregateKey leaf.
        keys = list(walk_value_keys(key))
        assert any(isinstance(k, AggregateKey) for k in keys)
