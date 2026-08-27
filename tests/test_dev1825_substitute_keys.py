"""DEV-1825 — ``substitute_value_keys``: the total, fail-closed structural
rewriter the regroup desugar substitutes placeholders with.

Mirrors the ``reroot_value_key`` totality contract (DEV-1747): every
``ValueKey`` union member has an explicit case, an unhandled kind RAISES, and
a matched subtree is replaced ATOMICALLY (its children are never recursed —
"by key identity, never text").
"""
from __future__ import annotations

from decimal import Decimal
from typing import get_args

import pytest

from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    InKey,
    LiteralKey,
    ScalarCallKey,
    SqlExprKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    substitute_value_keys,
)

CITY = ColumnKey(path=(), leaf="city")
AMOUNT = ColumnKey(path=(), leaf="amount")
AGG = AggregateKey(source=AMOUNT, agg="sum", partition_keys=frozenset({CITY}))
PLACEHOLDER = ColumnKey(path=(), leaf="__regroup__0__amount_sum")
MAPPING = {AGG: PLACEHOLDER}


class TestDirectAndNestedHits:
    def test_direct_hit_replaces_whole_key(self) -> None:
        assert substitute_value_keys(AGG, MAPPING) == PLACEHOLDER

    def test_no_hit_returns_equal_key(self) -> None:
        assert substitute_value_keys(AGG, {}) == AGG

    def test_nested_in_arithmetic(self) -> None:
        tree = ArithmeticKey(op=">", operands=(AGG, LiteralKey(value=Decimal("5000"))))
        out = substitute_value_keys(tree, MAPPING)
        assert out.operands[0] == PLACEHOLDER
        assert out.operands[1] == LiteralKey(value=Decimal("5000"))

    def test_nested_in_scalar_call(self) -> None:
        tree = ScalarCallKey(name="iif", args=(
            ArithmeticKey(op=">", operands=(AGG, LiteralKey(value=Decimal("0")))),
            Decimal("1"),
            Decimal("0"),
        ))
        out = substitute_value_keys(tree, MAPPING)
        assert out.args[0].operands[0] == PLACEHOLDER
        assert out.args[1] == Decimal("1")

    def test_nested_in_transform_input(self) -> None:
        tree = TransformKey(op="cumsum", input=AGG)
        assert substitute_value_keys(tree, MAPPING).input == PLACEHOLDER

    def test_nested_in_between_and_in(self) -> None:
        bt = BetweenKey(column=AGG, low=LiteralKey(value="a"), high=LiteralKey(value="b"))
        assert substitute_value_keys(bt, MAPPING).column == PLACEHOLDER
        ik = InKey(column=AGG, values=(LiteralKey(value="gold"),))
        assert substitute_value_keys(ik, MAPPING).column == PLACEHOLDER

    def test_column_to_column_inside_time_trunc(self) -> None:
        # TimeTruncKey.column is typed ColumnKey/ColumnSqlKey — a column-to-
        # column mapping must reach through it.
        other = ColumnKey(path=(), leaf="shipped_at")
        tt = TimeTruncKey(column=ColumnKey(path=(), leaf="ordered_at"), granularity="month")
        out = substitute_value_keys(tt, {ColumnKey(path=(), leaf="ordered_at"): other})
        assert out.column == other


class TestAtomicity:
    def test_matched_subtree_children_are_not_recursed(self) -> None:
        # CITY appears both inside AGG (partition_keys) and as a sibling
        # operand. The AGG match is atomic: its inner CITY must survive, while
        # the sibling CITY is replaced.
        other = ColumnKey(path=(), leaf="region")
        tree = ArithmeticKey(op="+", operands=(AGG, CITY))
        out = substitute_value_keys(tree, {AGG: PLACEHOLDER, CITY: other})
        assert out.operands == (PLACEHOLDER, other)

    def test_aggregate_internals_reachable_when_agg_itself_unmatched(self) -> None:
        # Without an AGG-level match the rewriter descends into source /
        # partition_keys, so a producer-side rewrite can retarget them.
        other = ColumnKey(path=(), leaf="city2")
        out = substitute_value_keys(AGG, {CITY: other})
        assert out.partition_keys == frozenset({other})
        assert out.source == AMOUNT


class TestTotality:
    def test_every_union_member_has_a_sample(self) -> None:
        samples = {
            ColumnKey: CITY,
            ColumnSqlKey: ColumnSqlKey(path=(), model="orders", column_name="margin"),
            TimeTruncKey: TimeTruncKey(
                column=ColumnKey(path=(), leaf="ordered_at"), granularity="month",
            ),
            StarKey: StarKey(),
            LiteralKey: LiteralKey(value=Decimal("1")),
            AggregateKey: AGG,
            TransformKey: TransformKey(op="cumsum", input=AGG),
            ArithmeticKey: ArithmeticKey(op="+", operands=(CITY, AMOUNT)),
            ScalarCallKey: ScalarCallKey(name="coalesce", args=(CITY, "x")),
            BetweenKey: BetweenKey(
                column=CITY, low=LiteralKey(value="a"), high=LiteralKey(value="b"),
            ),
            InKey: InKey(column=CITY, values=(LiteralKey(value="gold"),)),
        }
        members = set(get_args(ValueKey))
        missing = members - set(samples)
        assert not missing, (
            f"ValueKey grew {sorted(m.__name__ for m in missing)}; add a sample "
            f"here and a case to substitute_value_keys (totality)."
        )
        for member in members:
            assert substitute_value_keys(samples[member], {}) == samples[member]

    def test_column_filter_key_is_not_traversed(self) -> None:
        # SqlExprKey is Mode-A identity, not a ValueKey — it rides through
        # unchanged like reroot's column_filter_key invariance.
        filt = SqlExprKey(canonical_sql="status = 'ok'", referenced_join_paths=())
        key = AggregateKey(source=AMOUNT, agg="sum", column_filter_key=filt)
        assert substitute_value_keys(key, {CITY: AMOUNT}).column_filter_key == filt

    def test_unknown_kind_raises(self) -> None:
        class NotAKey:
            pass

        bad = NotAKey()
        with pytest.raises(TypeError):
            substitute_value_keys(bad, MAPPING)  # type: ignore[arg-type]
