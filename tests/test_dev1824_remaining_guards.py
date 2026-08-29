"""DEV-1824 guard preservation — the error surface that REMAINS after the
stage-1 lifts (design Non-Goals): cross-model partitioned aggregates in the
lifted shapes (stage 3), DEV-1504 G4/G5 for plain ``window=`` measures
(stage 2), the measure-side grain rule, and the grain-self-containment boundary
inside dimension expressions.

The lifted-direction tests live in tests/test_dev1824_partitioned_execution.py
and tests/test_dev1824_computed_dim_execution.py; the reserved-prefix and
raw-rows guards stay pinned by the DEV-1825/1740 suites.
"""

from __future__ import annotations

import re

import pytest

from tests._dev1824_fixtures import ModelMeasure, gen, month_td, q


class TestCrossModelPartitionedStillGuarded:
    """Cross-model SOURCES stay off the stage-1 primitive: each lifted shape
    keeps failing closed (never a placeholder leak) when the aggregate's source
    crosses a join path."""

    async def test_cross_model_window_plus_partition(self) -> None:
        query = q(
            dimensions=["customers.tier"], time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="customers.spend:sum(window='1y', partition_by=customers.tier)",
                name="w",
            )],
        )
        with pytest.raises(NotImplementedError, match=r"(?i)cross-model"):
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


class TestWindowedCompositeGuardsPreserved:
    """DEV-1504 G4/G5 stay: a PLAIN windowed measure still rejects transform
    and composite nesting (only the partition_by-bearing forms were lifted)."""

    async def test_transform_over_plain_windowed_measure_raises(self) -> None:
        query = q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula="cumsum(amount:sum(window='1y'))", name="c")],
        )
        with pytest.raises(
            NotImplementedError, match=r"combined with transforms.*DEV-1504",
        ):
            await gen(query)

    async def test_plain_windowed_measure_in_composite_raises(self) -> None:
        query = q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum(window='1y') / 2", name="h")],
        )
        with pytest.raises(
            NotImplementedError,
            match=r"arithmetic / composite / scalar expressions.*DEV-1504",
        ):
            await gen(query)


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
