"""DEV-1836 task 1.5 — executed values for target-rooted cross-model producers
(SQLite + DuckDB).

Spec: openspec …/specs/queries/cross-model-aggregates — "Target-rooted
computation with metric independence", "Fan-out-safe grain with broadcast",
"Existing cross-model behavior is preserved where already safe". Oracles in
``tests/_dev1836_fixtures.py``; the naive join-multiplied spend (470) is the
fan-out defect value that must never appear.
"""

from __future__ import annotations

import pytest

from slayer.core.query import OrderItem
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1836_fixtures import (
    AMOUNT_BY_LABEL,
    AMOUNT_BY_STATUS,
    AMOUNT_BY_TIER,
    AMOUNT_BY_TIER_STATUS,
    GOLD_SPEND_BY_REGION,
    ModelMeasure,
    SPEND_BY_REGION,
    SPEND_BY_TIER,
    SPEND_TOTAL,
    make_exec_engine,
    q,
    rows_by,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


M = ModelMeasure(formula="amount:sum", name="m")
CM = ModelMeasure(formula="customers.spend:sum", name="cm")


async def _dry_scope_closed(engine, query, dialect) -> None:
    dry = await engine.execute(query, dry_run=True)
    assert dry.sql is not None
    assert "__regroup__" not in dry.sql, f"placeholder leaked:\n{dry.sql}"
    assert_scope_closed(dry.sql, dialect=dialect)


def _approx(actual, expected, *, key) -> None:
    if expected is None:
        assert actual is None, f"{key}: expected NULL, got {actual!r}"
    else:
        assert actual is not None, f"{key}: expected {expected}, got NULL"
        assert float(actual) == pytest.approx(expected), f"{key}"


class TestTargetRootedExactness:
    async def test_joined_sum_is_not_multiplied_by_fan_out(self, exec_backend):
        """c1 and c3 each have several orders; a fanned join would inflate
        gold to 260 and the grand total to 470."""
        dialect, engine = exec_backend
        query = q(dimensions=["customers.tier"], measures=[M, CM])
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.customers.tier")
        assert set(by) == {("gold",), ("silver",), ("bronze",), (None,)}
        for tier, expected in SPEND_BY_TIER.items():
            _approx(by[(tier,)]["orders.cm"], expected, key=tier)
        # The NULL-tier row comes from the orphan order — no customer slice.
        _approx(by[(None,)]["orders.cm"], None, key="NULL tier")
        for (tier,), row in by.items():
            _approx(row["orders.m"], AMOUNT_BY_TIER[tier], key=f"m:{tier}")
        await _dry_scope_closed(engine, query, dialect)

    async def test_safe_two_hop_dim_keeps_exact_values(self, exec_backend):
        """customers → regions is PK-proven: exact per-region spend, NULL
        grain key included (null-safe attach)."""
        dialect, engine = exec_backend
        query = q(dimensions=["customers.regions.name"], measures=[M, CM])
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.customers.regions.name")
        assert set(by) == {("North",), ("South",), (None,)}
        for name, expected in SPEND_BY_REGION.items():
            _approx(by[(name,)]["orders.cm"], expected, key=name)
        await _dry_scope_closed(engine, query, dialect)

    async def test_adding_cm_measure_is_cardinality_neutral(self, exec_backend):
        _, engine = exec_backend
        solo = await engine.execute(q(dimensions=["customers.tier"], measures=[M]))
        both = await engine.execute(q(dimensions=["customers.tier"], measures=[M, CM]))
        solo_by = rows_by(solo, "orders.customers.tier")
        both_by = rows_by(both, "orders.customers.tier")
        assert set(solo_by) == set(both_by)
        for key, row in both_by.items():
            _approx(row["orders.m"], solo_by[key]["orders.m"], key=key)


class TestBroadcast:
    async def test_unattributable_host_dim_broadcasts(self, exec_backend):
        """orders.status is unreachable from customers over safe hops: every
        status row carries the grand total; the grain is unchanged."""
        dialect, engine = exec_backend
        query = q(dimensions=["status"], measures=[M, CM])
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.status")
        assert set(by) == {("ok",), ("new",)}
        for (status,), row in by.items():
            _approx(row["orders.cm"], SPEND_TOTAL, key=status)
            _approx(row["orders.m"], AMOUNT_BY_STATUS[status], key=f"m:{status}")
        await _dry_scope_closed(engine, query, dialect)

    async def test_unproven_hop_dim_broadcasts(self, exec_backend):
        """customers → segments has unproven arity: the metric never joins
        through it, broadcasting instead of double-counting."""
        dialect, engine = exec_backend
        query = q(dimensions=["customers.segments.label"], measures=[M, CM])
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.customers.segments.label")
        assert set(by) == {("Alpha",), ("Beta",), (None,)}
        for (label,), row in by.items():
            _approx(row["orders.cm"], SPEND_TOTAL, key=label)
            _approx(row["orders.m"], AMOUNT_BY_LABEL[label], key=f"m:{label}")
        await _dry_scope_closed(engine, query, dialect)

    async def test_mixed_grain_broadcasts_only_the_unsafe_member(self, exec_backend):
        """S(A) = {tier}: exact per tier, broadcast across status."""
        dialect, engine = exec_backend
        query = q(dimensions=["customers.tier", "status"], measures=[M, CM])
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.customers.tier", "orders.status")
        assert set(by) == set(AMOUNT_BY_TIER_STATUS)
        for (tier, status), row in by.items():
            expected_cm = SPEND_BY_TIER.get(tier)  # None for the NULL tier
            _approx(row["orders.cm"], expected_cm, key=(tier, status))
            _approx(row["orders.m"], AMOUNT_BY_TIER_STATUS[(tier, status)],
                    key=("m", tier, status))
        await _dry_scope_closed(engine, query, dialect)


class TestProducerShapes:
    async def test_filtered_target_column_restricts_the_metric(self, exec_backend):
        """gold_spend (Column.filter local to the target) with a safe grain."""
        dialect, engine = exec_backend
        query = q(
            dimensions=["customers.regions.name"],
            measures=[ModelMeasure(formula="customers.gold_spend:sum", name="gs")],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.customers.regions.name")
        assert set(by) == {("North",), ("South",), (None,)}
        for name, expected in GOLD_SPEND_BY_REGION.items():
            _approx(by[(name,)]["orders.gs"], expected, key=name)
        await _dry_scope_closed(engine, query, dialect)

    async def test_order_only_cm_orders_rows(self, exec_backend):
        dialect, engine = exec_backend
        query = q(
            dimensions=["customers.tier"], measures=[M],
            order=[OrderItem(column="customers.spend:sum", direction="desc")],
        )
        resp = await engine.execute(query)
        tiers = [r["orders.customers.tier"] for r in resp.data]
        # NULL placement is backend-defined; the non-null order is the pin.
        assert [t for t in tiers if t is not None] == ["gold", "silver", "bronze"]
        assert len(tiers) == 4
        await _dry_scope_closed(engine, query, dialect)

    async def test_filter_only_cm_filters_groups(self, exec_backend):
        """A hidden (filter-only) cross-model aggregate use."""
        dialect, engine = exec_backend
        query = q(
            dimensions=["customers.tier"], measures=[M],
            filters=["customers.spend:sum > 100"],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.customers.tier")
        assert set(by) == {("gold",), ("silver",)}
        _approx(by[("gold",)]["orders.m"], AMOUNT_BY_TIER["gold"], key="gold")
        _approx(by[("silver",)]["orders.m"], AMOUNT_BY_TIER["silver"], key="silver")
        await _dry_scope_closed(engine, query, dialect)

    async def test_two_targets_in_one_expression(self, exec_backend):
        """Local and cross-model aggregates compose arithmetically."""
        dialect, engine = exec_backend
        query = q(
            dimensions=["customers.tier"],
            measures=[ModelMeasure(
                formula="customers.spend:sum / amount:sum", name="ratio",
            )],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.customers.tier")
        for tier, spend in SPEND_BY_TIER.items():
            _approx(by[(tier,)]["orders.ratio"],
                    spend / AMOUNT_BY_TIER[tier], key=tier)
        await _dry_scope_closed(engine, query, dialect)
