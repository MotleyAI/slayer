"""DEV-1868 W2 — cross-model partitioned aggregates nested inside transforms,
executed values (spec: queries/partitioned-aggregates, nest-inside-transforms
requirement)."""

from __future__ import annotations

import pytest

from tests._dev1836_fixtures import (
    ModelMeasure,
    make_exec_engine,
    month_td,
    q,
)
from tests._dev1868_fixtures import (
    CHANGE_LAST_BY_REGION_MONTH,
    CM_PART_TIER,
    CUMSUM_PART_BY_TIER_MONTH,
    LAST_REGION,
    LAST_SPEND_BY_REGION,
    keyed_by_month,
)

TIER = "orders.customers.tier"
REGION = "orders.customers.regions.name"


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


class TestCumsumOverCrossModelPartitioned:
    async def test_accumulates_the_per_tier_totals(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.tier"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula=f"cumsum({CM_PART_TIER})", name="c")],
        ))
        by = keyed_by_month(resp, TIER)
        assert set(by) == set(CUMSUM_PART_BY_TIER_MONTH)
        for key, expected in CUMSUM_PART_BY_TIER_MONTH.items():
            cell = by[key]["orders.c"]
            if expected is None:
                assert cell is None
            else:
                assert float(cell) == pytest.approx(expected)


class TestChangeOverCrossModelRanked:
    async def test_period_over_period_of_the_region_last_value(
        self, exec_engine,
    ) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.regions.name"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula=f"change({LAST_REGION})", name="ch"),
                      ModelMeasure(formula=LAST_REGION, name="l")],
        ))
        by = keyed_by_month(resp, REGION)
        assert set(by) == set(CHANGE_LAST_BY_REGION_MONTH)
        for (region, month), expected in CHANGE_LAST_BY_REGION_MONTH.items():
            cell = by[(region, month)]["orders.ch"]
            if expected is None:
                assert cell is None
            else:
                assert float(cell) == pytest.approx(expected)
            # The inner attached value stays position-parity correct.
            assert float(by[(region, month)]["orders.l"]) == pytest.approx(
                LAST_SPEND_BY_REGION[region])


class TestTransformFilterAndOrderPositions:
    async def test_transform_in_filter_position(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.tier"], time_dimensions=month_td(),
            filters=[f"cumsum({CM_PART_TIER}) > 200"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        by = keyed_by_month(resp, TIER)
        # Only gold's running total clears 200, from February on.
        assert set(by) == {("gold", "2024-02"), ("gold", "2024-03")}
        for key in by:
            assert float(by[key]["orders.s"]) == pytest.approx(5.0)

    async def test_transform_as_order_target(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.tier"], time_dimensions=month_td(),
            filters=["customers.tier is not null"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
            order=[{"column": f"cumsum({CM_PART_TIER})", "direction": "desc"}],
        ))
        by = keyed_by_month(resp, TIER)
        # Sorted by the hidden running total: 480, 320, 160, 150, 40.
        assert list(by) == [
            ("gold", "2024-03"), ("gold", "2024-02"), ("gold", "2024-01"),
            ("silver", "2024-02"), ("bronze", "2024-03"),
        ]


class TestSharedInnerAcrossConsumers:
    async def test_transform_and_filter_share_one_inner(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.tier"], time_dimensions=month_td(),
            filters=[f"{CM_PART_TIER} > 100"],
            measures=[ModelMeasure(formula=f"cumsum({CM_PART_TIER})", name="c")],
        ))
        by = keyed_by_month(resp, TIER)
        expected = {k: v for k, v in CUMSUM_PART_BY_TIER_MONTH.items()
                    if k[0] in ("gold", "silver")}  # per-tier totals > 100
        assert set(by) == set(expected)
        for key, value in expected.items():
            assert float(by[key]["orders.c"]) == pytest.approx(value)
