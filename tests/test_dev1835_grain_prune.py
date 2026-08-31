"""DEV-1835 — functionally-determined grain pruning must not drop a real axis
hidden inside a transform's input (CodeRabbit PR #346 Thread 2).

``_prune_functionally_determined_grain`` removes a computed-dim grain key that
is constant within the raw dimensions already in the grain. It reads a key's
free scalar columns via ``_scalar_free_columns``; if that helper fails to
descend a ``TransformKey``'s input, a discriminating column inside e.g.
``rank(amount:sum(partition_by=region) + city)`` goes unseen and the axis is
wrongly pruned, collapsing the producer to ``region`` alone.
"""

from slayer.core.keys import AggregateKey, ArithmeticKey, ColumnKey, TransformKey
from slayer.engine.stage_planner import (
    _prune_functionally_determined_grain,
    _scalar_free_columns,
)

REGION = ColumnKey(path=(), leaf="region")
CITY = ColumnKey(path=(), leaf="city")
SUM_BY_REGION = AggregateKey(
    source=ColumnKey(path=(), leaf="amount"), agg="sum",
    partition_keys=frozenset({REGION}),
)
# rank(amount:sum(partition_by=region) + city) — city is a free scalar axis.
RANK_WITH_FREE_CITY = TransformKey(
    op="rank", input=ArithmeticKey(op="+", operands=(SUM_BY_REGION, CITY)),
)
# rank(amount:sum(partition_by=region)) — fully determined by region.
RANK_DETERMINED = TransformKey(op="rank", input=SUM_BY_REGION)


def test_scalar_free_columns_descends_transform_input():
    out: set = set()
    _scalar_free_columns(RANK_WITH_FREE_CITY, out)
    assert CITY in out, "the free column inside the transform input was missed"
    assert REGION not in out, "aggregate internals must stay opaque"


def test_prune_keeps_transform_dim_with_free_column():
    pks = frozenset({REGION, RANK_WITH_FREE_CITY})
    kept = _prune_functionally_determined_grain(pks)
    assert RANK_WITH_FREE_CITY in kept, (
        "a transform grain key with a free discriminating column is a real "
        "axis and must be kept"
    )


def test_prune_drops_fully_determined_transform_dim():
    # Control: no free column → constant within region → correctly pruned.
    pks = frozenset({REGION, RANK_DETERMINED})
    kept = _prune_functionally_determined_grain(pks)
    assert RANK_DETERMINED not in kept
    assert REGION in kept
