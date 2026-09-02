"""DEV-1838 — a transform-wrapped cross-model aggregate predicate routes POST,
never to the combined WHERE (``_partitioned_conjunct_scope`` guard)."""

from __future__ import annotations

from decimal import Decimal

from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    LiteralKey,
    TransformKey,
)
from slayer.engine.stage_planner import _partitioned_conjunct_scope

_SPEND = AggregateKey(source=ColumnKey(path=("customers",), leaf="spend"), agg="sum")
_GT0 = LiteralKey(value=Decimal(0))


def _scope(cj) -> str:
    return _partitioned_conjunct_scope(
        cj, dim_keys=frozenset(), row_agg_set=frozenset(), crossing_root=None,
    )


def test_transform_wrapped_cross_model_predicate_is_not_combined() -> None:
    cj = ArithmeticKey(op=">", operands=(TransformKey(op="cumsum", input=_SPEND), _GT0))
    # POST-phase: the transform owns the predicate, so it falls through instead
    # of routing to the combined SELECT (regressed to "combined" before the fix).
    assert _scope(cj) == "other"


def test_bare_cross_model_predicate_still_routes_combined() -> None:
    cj = ArithmeticKey(op=">", operands=(_SPEND, _GT0))
    assert _scope(cj) == "combined"
