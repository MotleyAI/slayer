"""DEV-1824 guard preservation — the error surface that REMAINS after the
stage-1 lifts (design Non-Goals): cross-model partitioned aggregates in the
lifted shapes (stage 3), the measure-side grain rule, and the
grain-self-containment boundary inside dimension expressions. The DEV-1504
G4/G5 pins for plain ``window=`` measures moved to the lifted direction with
DEV-1835 (tests/test_dev1835_guard_dissolution.py).

The lifted-direction tests live in tests/test_dev1824_partitioned_execution.py
and tests/test_dev1824_computed_dim_execution.py; the reserved-prefix and
raw-rows guards stay pinned by the DEV-1825/1740 suites.
"""

from __future__ import annotations

import re

import pytest

from slayer.engine.stage_planner import _assert_attach_covers_producer_grain
from tests._dev1824_fixtures import ModelMeasure, gen, month_td, q


class TestCrossModelPartitionedStillGuarded:
    """Cross-model SOURCES stay off the stage-1 primitive: each lifted shape
    keeps failing closed (never a placeholder leak) when the aggregate's source
    crosses a join path."""

    async def test_cross_model_window_plus_partition(self) -> None:
        # DEV-1836: fails closed with a precise attributability error (the active
        # time dimension is a host column, unreachable from customers).
        query = q(
            dimensions=["customers.tier"], time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="customers.spend:sum(window='1y', partition_by=customers.tier)",
                name="w",
            )],
        )
        with pytest.raises(ValueError, match=r"(?i)cross-model"):
            await gen(query)

    async def test_cross_model_first_last_plus_partition(self) -> None:
        query = q(
            dimensions=["region"],
            measures=[ModelMeasure(
                formula="customers.spend:last(partition_by=region)", name="l",
            )],
        )
        with pytest.raises((NotImplementedError, ValueError)) as ei:
            await gen(query)
        assert re.search(r"(?i)cross-model|partition", str(ei.value))
        assert "__regroup__" not in str(ei.value)

    async def test_cross_model_partitioned_inside_transform(self) -> None:
        query = q(
            dimensions=["customers.tier"], time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="cumsum(customers.spend:sum(partition_by=customers.tier))",
                name="c",
            )],
        )
        with pytest.raises((NotImplementedError, ValueError)) as ei:
            await gen(query)
        assert re.search(r"(?i)cross-model|partition", str(ei.value))
        assert "__regroup__" not in str(ei.value)

    async def test_filter_on_cross_model_partitioned(self) -> None:
        query = q(
            dimensions=["customers.tier"],
            filters=["customers.spend:sum(partition_by=customers.tier) > 100"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        with pytest.raises((NotImplementedError, ValueError)) as ei:
            await gen(query)
        assert re.search(r"(?i)cross-model|partition", str(ei.value))
        assert "__regroup__" not in str(ei.value)


class TestMeasureGrainRulePreserved:
    async def test_partition_key_must_be_a_query_dimension(self) -> None:
        # The COMBINED-attach rule (design D4) survives the generalized
        # discovery; only ROW attaches may use finer-than-query partitions.
        query = q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum(partition_by=city)", name="m")],
        )
        with pytest.raises(ValueError, match=r"not a query dimension"):
            await gen(query)

    async def test_dual_role_aggregate_is_validated_strictly(self) -> None:
        # CR — the SAME aggregate as a computed-dimension row attach AND a bare
        # measure: the finer-grain leniency must not carry into the combined
        # measure role, or the join-back fails with an internal RuntimeError.
        # It must raise the clean "not a query dimension" error instead.
        band = "CASE WHEN amount:sum(partition_by=city) > 50 THEN 1 ELSE 0 END"
        query = q(
            dimensions=["region", {"expression": band, "name": "band"}],
            measures=[
                ModelMeasure(formula="amount:sum", name="s"),
                ModelMeasure(formula="amount:sum(partition_by=city)", name="ct"),
            ],
        )
        with pytest.raises(ValueError, match=r"not a query dimension"):
            await gen(query)

    async def test_dual_role_via_filter_routes_through_row_attach(self) -> None:
        # CR — a filter on the computed dimension's OWN aggregate references the
        # row attach, not a combined consumer, so a finer-than-query grain is
        # legal and must render cleanly (no combined join-back, no internal error).
        band = "CASE WHEN amount:sum(partition_by=city) > 50 THEN 1 ELSE 0 END"
        query = q(
            dimensions=["region", {"expression": band, "name": "band"}],
            filters=["amount:sum(partition_by=city) > 100"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        sql = await gen(query)  # must not raise
        assert "__regroup__" not in sql


class TestDimensionGrainSelfContainment:
    """Inside a dimension expression the lift is CONDITIONAL on an explicit
    grain: bare-grain forms keep erroring, now with the partition_by directive
    (measure⇔dimension symmetry admits only grain-self-contained expressions)."""

    async def test_transform_over_bare_aggregate_in_dimension(self) -> None:
        band = "CASE WHEN cumsum(amount:sum) > 50 THEN 1 ELSE 0 END"
        query = q(
            dimensions=["region", {"expression": band, "name": "b"}],
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        with pytest.raises((NotImplementedError, ValueError)) as ei:
            await gen(query)
        assert "partition_by" in str(ei.value)

    async def test_bare_windowed_aggregate_in_dimension(self) -> None:
        band = "CASE WHEN amount:sum(window='1y') > 50 THEN 1 ELSE 0 END"
        query = q(
            dimensions=["region", {"expression": band, "name": "b"}],
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        with pytest.raises((NotImplementedError, ValueError)) as ei:
            await gen(query)
        assert "partition_by" in str(ei.value)

    async def test_bare_first_last_in_dimension(self) -> None:
        band = "CASE WHEN amount:last > 50 THEN 1 ELSE 0 END"
        query = q(
            dimensions=["region", {"expression": band, "name": "b"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        with pytest.raises((NotImplementedError, ValueError)) as ei:
            await gen(query)
        assert "partition_by" in str(ei.value)

    async def test_transform_over_mixed_grain_aggregates_lifted(self) -> None:
        # Two grains in one transform union and broadcast (DEV-1839) — the
        # former fail-closed guard is gone. Executed ground truth lives in
        # tests/test_dev1839_union_dim_execution.py.
        band = "rank(amount:sum(partition_by=region) - amount:sum(partition_by=city))"
        query = q(
            dimensions=["region", "city", {"expression": band, "name": "rk"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        sql = await gen(query)  # must not raise
        assert "__regroup__" not in sql


class TestAttachGrainCoverage:
    """D8 structural check: the attach join keys must equal the producer's
    grouping-grain slot ids; keyless (empty grain) is provably single-row."""

    def test_complete_cover_passes(self) -> None:
        _assert_attach_covers_producer_grain(
            joined_slot_ids={"s1"}, producer_grain_slot_ids={"s1"},
        )

    def test_coarser_join_than_grain_raises(self) -> None:
        with pytest.raises(ValueError, match=r"complete grain"):
            _assert_attach_covers_producer_grain(
                joined_slot_ids={"s1"}, producer_grain_slot_ids={"s1", "s3"},
            )

    def test_keyless_empty_grain_passes(self) -> None:
        _assert_attach_covers_producer_grain(
            joined_slot_ids=set(), producer_grain_slot_ids=set(),
        )

    def test_keyless_with_producer_grain_raises(self) -> None:
        with pytest.raises(ValueError, match=r"complete grain"):
            _assert_attach_covers_producer_grain(
                joined_slot_ids=set(), producer_grain_slot_ids={"s1"},
            )
