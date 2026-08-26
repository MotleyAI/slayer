"""DEV-1740 Part A — the ``ConditionalKey`` typed key.

Identity, phase-join, structural traversal, and rerooting. The traversal case
is load-bearing for Part B2: a partitioned aggregate nested inside any branch
of a conditional MUST be discoverable (else the desugar misses it and B2 breaks).
"""

from __future__ import annotations

import pytest

from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    ConditionalKey,
    LiteralKey,
    Phase,
    reroot_value_key,
)
from slayer.engine.binding import walk_value_keys


def _amount(**kw) -> AggregateKey:
    return AggregateKey(source=ColumnKey(path=(), leaf="amount"), agg="sum", **kw)


def _band_over_agg() -> ConditionalKey:
    """``CASE WHEN amount:sum > 5000 THEN 1 ELSE 0 END`` as a key."""
    cond = ArithmeticKey(op=">", operands=(_amount(), LiteralKey(value=5000)))
    return ConditionalKey(
        cond=cond, then=LiteralKey(value=1), otherwise=LiteralKey(value=0),
    )


class TestConstruction:
    def test_holds_three_branches(self) -> None:
        key = _band_over_agg()
        assert key.then == LiteralKey(value=1)
        assert key.otherwise == LiteralKey(value=0)


class TestPhaseJoin:
    def test_all_row_parts_is_row(self) -> None:
        key = ConditionalKey(
            cond=ArithmeticKey(
                op=">", operands=(ColumnKey(path=(), leaf="amount"),
                                  LiteralKey(value=5)),
            ),
            then=LiteralKey(value=1),
            otherwise=LiteralKey(value=0),
        )
        assert key.phase == Phase.ROW

    def test_phase_is_max_of_parts(self) -> None:
        key = _band_over_agg()
        expected = max(key.cond.phase, key.then.phase, key.otherwise.phase)
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
        a = ConditionalKey(cond=cond, then=LiteralKey(value=1),
                           otherwise=LiteralKey(value=0))
        b = ConditionalKey(cond=cond, then=LiteralKey(value=0),
                           otherwise=LiteralKey(value=1))
        assert a != b
        assert len({a, b}) == 2


class TestStructuralTraversal:
    @pytest.mark.parametrize("where", ["cond", "then", "otherwise"])
    def test_aggregate_in_any_branch_is_discovered(self, where: str) -> None:
        agg = _amount(partition_keys=frozenset({ColumnKey(path=(), leaf="city")}))
        parts = {
            "cond": LiteralKey(value=True),
            "then": LiteralKey(value=1),
            "otherwise": LiteralKey(value=0),
        }
        parts[where] = agg
        key = ConditionalKey(**parts)
        assert agg in list(walk_value_keys(key))


class TestNestedInScalarCall:
    def test_aggregate_under_conditional_under_scalar_call_is_found(self) -> None:
        # coalesce(iif(amount:sum > 5, 1, 0), -1): the aggregate sits two levels
        # down (ScalarCallKey → ConditionalKey → cond). Both the structural walk
        # and contains_aggregate must reach it.
        from slayer.core.keys import ScalarCallKey
        from slayer.sql.render.value_expr import contains_aggregate

        agg = _amount()
        cond = ConditionalKey(
            cond=ArithmeticKey(op=">", operands=(agg, LiteralKey(value=5))),
            then=LiteralKey(value=1),
            otherwise=LiteralKey(value=0),
        )
        call = ScalarCallKey(name="coalesce", args=(cond, LiteralKey(value=-1)))
        assert agg in list(walk_value_keys(call))
        assert contains_aggregate(call) is True


class TestReroot:
    def test_reroot_strips_prefix_in_every_branch(self) -> None:
        key = ConditionalKey(
            cond=ArithmeticKey(
                op=">",
                operands=(
                    AggregateKey(source=ColumnKey(path=("customers",), leaf="spend"),
                                 agg="sum"),
                    LiteralKey(value=100),
                ),
            ),
            then=ColumnKey(path=("customers",), leaf="tier"),
            otherwise=LiteralKey(value=None),
        )
        rerooted = reroot_value_key(key, target_path=("customers",))
        assert rerooted.then == ColumnKey(path=(), leaf="tier")
        assert rerooted.cond.operands[0].source == ColumnKey(path=(), leaf="spend")
