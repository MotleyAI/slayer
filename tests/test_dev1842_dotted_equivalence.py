"""A dotted saved-metric reference is bound-tree-
identical to the hand-written host-prefixed expression.

Each ``EQUIV_PAIRS`` entry pairs a dotted spelling (``customers.aov``) with its
hand-expanded twin. The guarantee is total: identical generated SQL (byte-for-
byte) AND identical executed values on SQLite and DuckDB, across composite,
nested-saved, self-qualified, nested-relationship, field-filter, partitioned, transform,
mixed local+dotted, and computed-dimension-source forms.
"""

from __future__ import annotations

import pytest

from tests._dev1842_fixtures import (
    EQUIV_PAIRS,
    GOLD_SPEND_BY_REGION,
    PARITY_ERROR_PAIRS,
    POP_TOTAL,
    SPEND_BY_TIER,
    TRANSFORM_PAIR,
    gen,
    make_exec_engine,
    month_td,
    q,
    rows_by,
)

ALL_PAIRS = {**EQUIV_PAIRS, "transform": TRANSFORM_PAIR}


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


def _needs_time(expression: str) -> bool:
    # The transform pair's DOTTED spelling (``customers.spend_run``) hides its
    # cumsum behind the metric name, so match the pair as well as the literal.
    return "cumsum" in expression or expression in TRANSFORM_PAIR


def _metric_query(spelling: str):
    return q(
        dimensions=["customers.tier"],
        metrics=[{"metric": spelling, "alias": "x"}],
        time_dimensions=month_td() if _needs_time(spelling) else None,
    )


def _canon(resp) -> list:
    """Order-independent canonical form of a response's rows (floats rounded)."""
    out = []
    for r in resp.data:
        items = []
        for k, v in r.items():
            items.append((k, round(v, 6) if isinstance(v, float) else v))
        out.append(tuple(sorted(items, key=lambda kv: kv[0])))
    return sorted(out, key=repr)


class TestSqlByteEquality:
    @pytest.mark.parametrize("label", list(ALL_PAIRS))
    async def test_dotted_sql_equals_hand_expanded(self, label) -> None:
        dotted, hand = ALL_PAIRS[label]
        dotted_sql = await gen(_metric_query(dotted))
        hand_sql = await gen(_metric_query(hand))
        assert dotted_sql == hand_sql


class TestExecutedValueEquality:
    @pytest.mark.parametrize("label", list(ALL_PAIRS))
    async def test_dotted_values_equal_hand_expanded(self, label, exec_backend) -> None:
        _, engine = exec_backend
        dotted, hand = ALL_PAIRS[label]
        dotted_resp = await engine.execute(_metric_query(dotted))
        hand_resp = await engine.execute(_metric_query(hand))
        assert _canon(dotted_resp) == _canon(hand_resp)
        # The metric actually projected a value (not an all-NULL degenerate).
        assert any(r.get("x") is not None for r in dotted_resp.data)


def _approx(actual, expected) -> None:
    if expected is None:
        assert actual is None, f"expected NULL, got {actual!r}"
    else:
        assert actual is not None
        assert float(actual) == pytest.approx(expected)


class TestExecutedValuesAgainstOracles:
    """Independent anchors (hand-computed oracles) so a shared
    regression that produces identical-but-wrong dotted/hand values can't pass."""

    async def test_nested_join_broadcasts_pop_total(self, exec_backend) -> None:
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"],
            metrics=[{"metric": "customers.pop_total", "alias": "x"}],
            to_many_handling="broadcast"))
        for row in resp.data:
            _approx(row["x"], POP_TOTAL)

    async def test_partitioned_matches_spend_by_tier(self, exec_backend) -> None:
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"],
            metrics=[{"metric": "customers.spend_by_tier", "alias": "x"}]))
        by = rows_by(resp, "customers.tier")
        for tier, expected in SPEND_BY_TIER.items():
            _approx(by[(tier,)]["x"], expected)
        _approx(by[(None,)]["x"], None)

    async def test_filtered_matches_gold_spend_by_region(self, exec_backend) -> None:
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.regions.name"],
            metrics=[{"metric": "customers.gold_spend_total", "alias": "x"}]))
        by = rows_by(resp, "customers.regions.name")
        for name, expected in GOLD_SPEND_BY_REGION.items():
            _approx(by[(name,)]["x"], expected)


class TestComputedDimensionSource:
    """1.1 — a dotted saved metric used inside a computed-dimension expression
    (the aggregate carries ``partition_by=`` as computed dimensions require)."""

    _HAND = ("CASE WHEN sum(customers.spend, partition_by=customers.tier) > 100 "
             "THEN 'hi' ELSE 'lo' END")
    _DOTTED = "CASE WHEN customers.spend_by_tier > 100 THEN 'hi' ELSE 'lo' END"

    def _dim_query(self, expr: str):
        return q(
            dimensions=[{"field": expr, "alias": "band"}],
            metrics=[{"metric": "sum(orders.amount)", "alias": "a"}],
        )

    async def test_computed_dimension_sql_equals_hand(self) -> None:
        assert await gen(self._dim_query(self._DOTTED)) == await gen(
            self._dim_query(self._HAND)
        )

    async def test_computed_dimension_values_equal_hand(self, exec_backend) -> None:
        _, engine = exec_backend
        dotted = await engine.execute(self._dim_query(self._DOTTED))
        hand = await engine.execute(self._dim_query(self._HAND))
        assert _canon(dotted) == _canon(hand)


class TestUnsafeInputParity:
    """1.2 — a field filter crossing the OWNER's UNPROVEN relationship inherits the
    cross-dataset unsafe-input rejection. The dotted spelling raises the SAME error
    as the hand-written form (parity is 'both raise', not 'same values')."""

    @pytest.mark.parametrize("label", list(PARITY_ERROR_PAIRS))
    async def test_dotted_inherits_unproven_hop_rejection(self, label) -> None:
        dotted, hand = PARITY_ERROR_PAIRS[label]
        hand_query = _metric_query(hand)
        dotted_query = _metric_query(dotted)
        with pytest.raises(ValueError) as hand_err:
            await gen(hand_query)
        assert "unproven relationship hop" in str(hand_err.value)
        with pytest.raises(ValueError) as dotted_err:
            await gen(dotted_query)
        assert "unproven relationship hop" in str(dotted_err.value)
