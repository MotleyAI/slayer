"""DEV-1836 task 1.8 — unsafe aggregate inputs and unsafe explicit partition
keys fail closed (design D2, F4; class-(d) pins).

Spec: openspec …/specs/queries/cross-model-aggregates — "Unsafe aggregate
inputs fail closed", "Explicit grain and window on cross-model aggregates";
…/specs/queries/partitioned-aggregates — "Partition keys are attributable from
the aggregate's root". Reading through a fanning/unproven hop is ambiguous:
hard error in lenient and strict mode alike, naming the remedy.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1836_fixtures import (
    AMOUNT_BY_TIER,
    AMOUNT_BY_TIER_STATUS,
    ModelMeasure,
    make_exec_engine,
    q,
    rows_by,
)

RAISES = (SlayerError, ValueError, NotImplementedError)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


M = ModelMeasure(formula="amount:sum", name="m")


def _assert_clear(ei, *needles: str, remedy: bool = False) -> None:
    message = str(ei.value)
    for needle in needles:
        assert needle in message, f"{needle!r} missing from: {message}"
    assert "__regroup__" not in message
    if remedy:
        lowered = message.lower()
        assert any(w in lowered for w in ("cardinality", "unique", "declare")), (
            f"no remedy named in: {message}"
        )


class TestUnsafeInputs:
    async def test_source_expression_over_unproven_hop_errors(self, exec_backend):
        """customers.seg_label reads segments.label across the unproven hop."""
        _, engine = exec_backend
        with pytest.raises(RAISES) as ei:
            await engine.execute(q(
                dimensions=["status"],
                measures=[ModelMeasure(formula="customers.seg_label:count",
                                       name="x")],
            ))
        _assert_clear(ei, "segments", remedy=True)

    async def test_column_filter_over_unproven_hop_errors(self, exec_backend):
        """customers.vip_spend's Column.filter references segments.label."""
        _, engine = exec_backend
        with pytest.raises(RAISES) as ei:
            await engine.execute(q(
                dimensions=["status"],
                measures=[ModelMeasure(formula="customers.vip_spend:sum",
                                       name="x")],
            ))
        _assert_clear(ei, "segments", remedy=True)

    async def test_ranking_time_arg_over_unsafe_hop_errors(self, exec_backend):
        """The explicit first/last time key is an input: orders.ordered_at is
        reachable from customers only across the declared 1:N edge."""
        _, engine = exec_backend
        with pytest.raises(RAISES) as ei:
            await engine.execute(q(
                dimensions=["customers.tier"],
                measures=[ModelMeasure(formula="customers.spend:last(ordered_at)",
                                       name="x")],
            ))
        _assert_clear(ei, "ordered_at")


class TestUnsafeExplicitPartitionKeys:
    async def test_cross_model_partition_key_over_unsafe_hop_errors(
        self, exec_backend,
    ):
        """F4 — a cross-model aggregate whose declared grain crosses the 1:N
        edge: hard error naming the key and the remedy, never a fanning join."""
        _, engine = exec_backend
        with pytest.raises(RAISES) as ei:
            await engine.execute(q(
                dimensions=["status"],
                measures=[ModelMeasure(
                    formula="customers.spend:sum(partition_by=status)", name="x",
                )],
            ))
        _assert_clear(ei, "status")
        message = str(ei.value).lower()
        assert "cardinality" in message or "unique" in message

    async def test_cross_model_partition_key_over_unproven_hop_errors(
        self, exec_backend,
    ):
        _, engine = exec_backend
        with pytest.raises(RAISES) as ei:
            await engine.execute(q(
                dimensions=["customers.tier"],
                measures=[ModelMeasure(
                    formula="customers.spend:sum(partition_by=customers.segments.label)",
                    name="x",
                )],
            ))
        _assert_clear(ei, "label", remedy=True)

    async def test_local_aggregate_with_unproven_joined_key_errors(
        self, exec_backend,
    ):
        """The rule applies to local aggregates too: orders → customers is
        safe but customers → segments is not."""
        _, engine = exec_backend
        with pytest.raises(RAISES) as ei:
            await engine.execute(q(
                dimensions=["status"],
                measures=[ModelMeasure(
                    formula="amount:sum(partition_by=customers.segments.label)",
                    name="x",
                )],
            ))
        _assert_clear(ei, "label", remedy=True)

    async def test_unattributable_partition_key_in_dimension_expression_errors(
        self, exec_backend,
    ):
        """The rule reaches dimension expressions too (computed-dimensions
        delta: grain self-containment error surface)."""
        _, engine = exec_backend
        with pytest.raises(RAISES) as ei:
            await engine.execute(q(
                dimensions=[{
                    "expression": ("CASE WHEN amount:sum(partition_by="
                                   "customers.segments.label) > 10 "
                                   "THEN 1 ELSE 0 END"),
                    "name": "b",
                }],
                measures=[M],
            ))
        _assert_clear(ei, "label", remedy=True)

    async def test_errors_apply_in_strict_mode_alike(self, exec_backend):
        _, engine = exec_backend
        with pytest.raises(RAISES) as ei:
            await engine.execute(q(
                strict=True, dimensions=["status"],
                measures=[ModelMeasure(
                    formula="customers.spend:sum(partition_by=status)", name="x",
                )],
            ))
        _assert_clear(ei, "status")


class TestSafeJoinedPartitionKey:
    async def test_partition_key_over_proven_hop_works(self, exec_backend):
        """Positive control — a local aggregate MAY declare a joined partition
        key when every hop is provably many-to-one."""
        dialect, engine = exec_backend
        query = q(
            dimensions=["customers.tier", "status"],
            measures=[M, ModelMeasure(
                formula="amount:sum(partition_by=customers.tier)", name="pt",
            )],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.customers.tier", "orders.status")
        assert set(by) == set(AMOUNT_BY_TIER_STATUS)
        for (tier, status), row in by.items():
            assert float(row["orders.pt"]) == pytest.approx(
                AMOUNT_BY_TIER[tier],
            ), (tier, status)
        dry = await engine.execute(query, dry_run=True)
        assert "__regroup__" not in dry.sql
        assert_scope_closed(dry.sql, dialect=dialect)
