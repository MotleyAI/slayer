"""DEV-1740 Part A — the reserved-name ``ScalarCallKey("iif")`` conditional.

Regression pins that the GENERIC ScalarCallKey machinery covers conditionals:
identity, phase-join, structural traversal, and rerooting. The traversal case
is load-bearing for Part B2: a partitioned aggregate nested inside any branch
of a conditional MUST be discoverable (else the desugar misses it and B2 breaks).
"""

from __future__ import annotations

import pytest

from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    LiteralKey,
    Phase,
    ScalarCallKey,
    reroot_value_key,
)
from slayer.engine.binding import walk_value_keys
from slayer.sql.render.value_expr import contains_aggregate


def _amount(**kw) -> AggregateKey:
    return AggregateKey(source=ColumnKey(path=(), leaf="amount"), agg="sum", **kw)


def _iif(cond, then, otherwise) -> ScalarCallKey:
    return ScalarCallKey(name="iif", args=(cond, then, otherwise))


def _band_over_agg() -> ScalarCallKey:
    """``CASE WHEN amount:sum > 5000 THEN 1 ELSE 0 END`` as a key."""
    cond = ArithmeticKey(op=">", operands=(_amount(), LiteralKey(value=5000)))
    return _iif(cond, LiteralKey(value=1), LiteralKey(value=0))


class TestConstruction:
    def test_holds_three_branches(self) -> None:
        key = _band_over_agg()
        assert key.args[1] == LiteralKey(value=1)
        assert key.args[2] == LiteralKey(value=0)


class TestPhaseJoin:
    def test_all_row_parts_is_row(self) -> None:
        key = _iif(
            ArithmeticKey(
                op=">", operands=(ColumnKey(path=(), leaf="amount"),
                                  LiteralKey(value=5)),
            ),
            LiteralKey(value=1),
            LiteralKey(value=0),
        )
        assert key.phase == Phase.ROW

    def test_phase_is_max_of_parts(self) -> None:
        key = _band_over_agg()
        expected = max(a.phase for a in key.args)
        assert key.phase == expected
        assert key.phase == Phase.AGGREGATE  # the cond carries an aggregate


class TestIdentity:
    def test_equal_parts_intern(self) -> None:
        a, b = _band_over_agg(), _band_over_agg()
        assert a == b
        assert hash(a) == hash(b)
        assert len({a, b}) == 1

    def test_swapped_branches_differ(self) -> None:
        cond = ArithmeticKey(op=">", operands=(_amount(), LiteralKey(value=5000)))
        a = _iif(cond, LiteralKey(value=1), LiteralKey(value=0))
        b = _iif(cond, LiteralKey(value=0), LiteralKey(value=1))
        assert a != b
        assert len({a, b}) == 2


class TestStructuralTraversal:
    @pytest.mark.parametrize("where", [0, 1, 2])
    def test_aggregate_in_any_branch_is_discovered(self, where: int) -> None:
        agg = _amount(partition_keys=frozenset({ColumnKey(path=(), leaf="city")}))
        parts = [LiteralKey(value=True), LiteralKey(value=1), LiteralKey(value=0)]
        parts[where] = agg
        key = _iif(*parts)
        assert agg in list(walk_value_keys(key))


class TestNestedInScalarCall:
    def test_aggregate_under_conditional_under_scalar_call_is_found(self) -> None:
        # coalesce(iif(amount:sum > 5, 1, 0), -1): the aggregate sits two levels
        # down (coalesce → iif → cond). Both the structural walk and
        # contains_aggregate must reach it.
        agg = _amount()
        cond = _iif(
            ArithmeticKey(op=">", operands=(agg, LiteralKey(value=5))),
            LiteralKey(value=1),
            LiteralKey(value=0),
        )
        call = ScalarCallKey(name="coalesce", args=(cond, LiteralKey(value=-1)))
        assert agg in list(walk_value_keys(call))
        assert contains_aggregate(call) is True


class TestReroot:
    def test_reroot_strips_prefix_in_every_branch(self) -> None:
        key = _iif(
            ArithmeticKey(
                op=">",
                operands=(
                    AggregateKey(source=ColumnKey(path=("customers",), leaf="spend"),
                                 agg="sum"),
                    LiteralKey(value=100),
                ),
            ),
            ColumnKey(path=("customers",), leaf="tier"),
            LiteralKey(value=None),
        )
        rerooted = reroot_value_key(key, target_path=("customers",))
        assert rerooted.args[1] == ColumnKey(path=(), leaf="tier")
        assert rerooted.args[0].operands[0].source == ColumnKey(path=(), leaf="spend")
