"""Stage 7a.6 (DEV-1450) — TransformLowerer tests.

The TransformLowerer desugars sugar transforms into their underlying
form so the planner and SQL generator see a uniform shape:

* ``change(x)`` → ``x - time_shift(x, periods=1)``
* ``change_pct(x)`` → ``(x - time_shift(x, periods=1)) / NULLIF(time_shift(x, periods=1), 0)``

The inner ``time_shift`` carries any ``partition_by`` kwarg that the
sugar form was called with (C6 — DEV-1450). Crucially, the inner ``x``
keeps the same structural identity (``ValueKey``) across all
occurrences, so the ValueRegistry interns it once and downstream
``cumsum/change/filter`` over the same ``x`` all share one slot
(DEV-1446).
"""

from __future__ import annotations

from decimal import Decimal

from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    Phase,
    ScalarCallKey,
    TransformKey,
)
from slayer.engine.planning import desugar_change, desugar_change_pct


def _amount_sum() -> AggregateKey:
    return AggregateKey(
        source=ColumnKey(path=(), leaf="amount"), agg="sum",
    )


class TestDesugarChange:
    def test_change_becomes_subtraction(self):
        # change(amount:sum) → amount:sum - time_shift(amount:sum, periods=1)
        change_key = TransformKey(op="change", input=_amount_sum())
        lowered = desugar_change(change_key)
        assert isinstance(lowered, ArithmeticKey)
        assert lowered.op == "-"
        left, right = lowered.operands
        # Left operand is the inner aggregate, preserved by identity.
        assert left == _amount_sum()
        # Right operand is time_shift over the SAME inner aggregate.
        assert isinstance(right, TransformKey)
        assert right.op == "time_shift"
        assert right.input == _amount_sum()
        # Same identity for the inner aggregate is the C6 guarantee.
        assert left is right.input or left == right.input

    def test_partition_by_threaded(self):
        # change(amount:sum, partition_by=region) — the binder lifts
        # partition_by onto TransformKey.partition_keys. The lowerer
        # threads that frozenset through to the underlying time_shift.
        region = ColumnKey(path=(), leaf="region")
        change_key = TransformKey(
            op="change",
            input=_amount_sum(),
            partition_keys=frozenset({region}),
        )
        lowered = desugar_change(change_key)
        assert isinstance(lowered, ArithmeticKey)
        right = lowered.operands[1]
        assert isinstance(right, TransformKey)
        assert right.op == "time_shift"
        assert region in right.partition_keys

    def test_phase_preserved(self):
        change_key = TransformKey(op="change", input=_amount_sum())
        lowered = desugar_change(change_key)
        # Arithmetic of an aggregate and a time_shift over an aggregate
        # is POST-phase (time_shift is POST).
        assert lowered.phase == Phase.POST


class TestDesugarChangePct:
    def test_change_pct_becomes_division(self):
        # change_pct(amount:sum)
        #   → (amount:sum - time_shift(amount:sum))
        #       / NULLIF(time_shift(amount:sum), 0)
        change_pct = TransformKey(op="change_pct", input=_amount_sum())
        lowered = desugar_change_pct(change_pct)
        assert isinstance(lowered, ArithmeticKey)
        assert lowered.op == "/"
        numerator, denominator = lowered.operands
        assert isinstance(numerator, ArithmeticKey)
        assert numerator.op == "-"
        # Divisor is NULLIF(<time_shift>, 0) — the divide-by-zero guard.
        assert isinstance(denominator, ScalarCallKey)
        assert denominator.name == "nullif"
        shifted, zero = denominator.args
        assert isinstance(shifted, TransformKey)
        assert shifted.op == "time_shift"
        assert shifted.input == _amount_sum()
        assert zero == Decimal(0)
        # The guarded divisor's time_shift is the SAME instance the numerator
        # subtracts (identity preserved → one CTE).
        assert numerator.operands[1] is shifted
