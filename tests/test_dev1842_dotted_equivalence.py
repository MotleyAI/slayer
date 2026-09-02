"""DEV-1842 tasks 1.1 / 1.2 — a dotted saved-measure reference is bound-tree-
identical to the hand-written host-prefixed formula.

Each ``EQUIV_PAIRS`` entry pairs a dotted spelling (``customers.aov``) with its
hand-expanded twin (``customers.spend:sum / customers.*:count``). The guarantee
is total: identical generated SQL (byte-for-byte) AND identical executed values
on SQLite and DuckDB. Coverage spans composite, nested-saved (recursion),
self-qualified (no double-prefix), nested-join, column-filter (owner-local and
join-crossing over a proven hop), partitioned (target-local and ``[]``),
transform, transform-wrapping-dotted, mixed local+dotted, and a
computed-dimension source.

Today every dotted spelling raises ``UnknownReferenceError`` (``_resolve_dotted``
has no saved-measure fall-through), so these fail for the right reason.
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


def _needs_time(formula: str) -> bool:
    return "cumsum" in formula


def _measure_query(spelling: str):
    return q(
        dimensions=["customers.tier"],
        measures=[{"formula": spelling, "name": "x"}],
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
        dotted_sql = await gen(_measure_query(dotted))
        hand_sql = await gen(_measure_query(hand))
        assert dotted_sql == hand_sql


class TestExecutedValueEquality:
    @pytest.mark.parametrize("label", list(ALL_PAIRS))
    async def test_dotted_values_equal_hand_expanded(self, label, exec_backend) -> None:
        _, engine = exec_backend
        dotted, hand = ALL_PAIRS[label]
        dotted_resp = await engine.execute(_measure_query(dotted))
        hand_resp = await engine.execute(_measure_query(hand))
        assert _canon(dotted_resp) == _canon(hand_resp)
        # The measure actually projected a value (not an all-NULL degenerate).
        assert any(r.get("orders.x") is not None for r in dotted_resp.data)


def _approx(actual, expected) -> None:
    if expected is None:
        assert actual is None, f"expected NULL, got {actual!r}"
    else:
        assert actual is not None and float(actual) == pytest.approx(expected)


class TestExecutedValuesAgainstOracles:
    """Independent anchors (DEV-1836 hand-computed oracles) so a shared
    regression that produces identical-but-wrong dotted/hand values can't pass."""

    async def test_nested_join_broadcasts_pop_total(self, exec_backend) -> None:
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"],
            measures=[{"formula": "customers.pop_total", "name": "x"}]))
        for row in resp.data:
            _approx(row["orders.x"], POP_TOTAL)

    async def test_partitioned_matches_spend_by_tier(self, exec_backend) -> None:
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"],
            measures=[{"formula": "customers.spend_by_tier", "name": "x"}]))
        by = rows_by(resp, "orders.customers.tier")
        for tier, expected in SPEND_BY_TIER.items():
            _approx(by[(tier,)]["orders.x"], expected)
        _approx(by[(None,)]["orders.x"], None)

    async def test_filtered_matches_gold_spend_by_region(self, exec_backend) -> None:
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.regions.name"],
            measures=[{"formula": "customers.gold_spend_total", "name": "x"}]))
        by = rows_by(resp, "orders.customers.regions.name")
        for name, expected in GOLD_SPEND_BY_REGION.items():
            _approx(by[(name,)]["orders.x"], expected)


class TestComputedDimensionSource:
    """1.1 — a dotted saved measure used inside a computed-dimension expression
    (the aggregate carries ``partition_by=`` as computed dimensions require)."""

    _HAND = ("CASE WHEN customers.spend:sum(partition_by=customers.tier) > 100 "
             "THEN 'hi' ELSE 'lo' END")
    _DOTTED = "CASE WHEN customers.spend_by_tier > 100 THEN 'hi' ELSE 'lo' END"

    def _dim_query(self, expr: str):
        return q(
            dimensions=[{"expression": expr, "name": "band"}],
            measures=[{"formula": "amount:sum", "name": "a"}],
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
    """1.2 — a column filter crossing the OWNER's UNPROVEN join inherits the
    DEV-1836 unsafe-input rejection. The dotted spelling raises the SAME error
    as the hand-written form (parity is 'both raise', not 'same values')."""

    @pytest.mark.parametrize("label", list(PARITY_ERROR_PAIRS))
    async def test_dotted_inherits_unproven_hop_rejection(self, label) -> None:
        dotted, hand = PARITY_ERROR_PAIRS[label]
        with pytest.raises(ValueError) as hand_err:
            await gen(_measure_query(hand))
        assert "unproven join hop" in str(hand_err.value)
        with pytest.raises(ValueError) as dotted_err:
            await gen(_measure_query(dotted))
        assert "unproven join hop" in str(dotted_err.value)
