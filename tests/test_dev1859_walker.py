"""DEV-1859 task 4.7 — the shared row-leaf walker (design decision 16). One
``_first_row_leaf(key, *, exempt)`` serves both transform checkers: aggregates
opaque, a transform descended through its input ONLY (never its partition / time
keys), exempt keys legal at any node. References the §5.5 symbol, so the module
is red until the shared walker lands.

Spec: openspec …/specs/queries/transforms — "Non-shift transforms reject
grain-refining row-level leaves".
"""

from __future__ import annotations

from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    Grain,
    TimeTruncKey,
    TransformKey,
)
from slayer.engine.elaborate_env import _first_row_leaf

WEIGHT = ColumnKey(leaf="weight")
QTY = ColumnKey(leaf="qty")
STORE = ColumnKey(leaf="store")
AGG = AggregateKey(source=WEIGHT, agg="sum", partition_keys=Grain.of([STORE]))


def _walk(key, exempt=frozenset()):
    return _first_row_leaf(key, exempt=exempt)


class TestSharedRowLeafWalker:
    def test_finds_bare_row_leaf(self):
        assert _walk(TransformKey(op="rank", input=WEIGHT)) == WEIGHT

    def test_exempt_leaf_is_legal(self):
        assert _walk(TransformKey(op="rank", input=WEIGHT),
                     exempt=frozenset({WEIGHT})) is None

    def test_aggregate_input_is_opaque(self):
        assert _walk(TransformKey(op="rank", input=AGG)) is None

    def test_partition_keys_are_not_row_leaves(self):
        key = TransformKey(op="rank", input=AGG, partition_keys=Grain.of([STORE]))
        assert _walk(key) is None

    def test_time_key_is_not_a_row_leaf(self):
        key = TransformKey(
            op="rank", input=AGG,
            time_key=TimeTruncKey(column=ColumnKey(leaf="ordered_at"),
                                  granularity="month"))
        assert _walk(key) is None

    def test_composite_returns_first_unexempt_leaf(self):
        comp = TransformKey(op="rank",
                            input=ArithmeticKey(op="*", operands=(WEIGHT, QTY)))
        assert _walk(comp, exempt=frozenset({WEIGHT})) == QTY
        assert _walk(comp, exempt=frozenset({WEIGHT, QTY})) is None
