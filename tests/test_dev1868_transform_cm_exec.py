"""W2 — cross-dataset partitioned aggregates nested inside transforms,
executed values (spec: queries/partitioned-aggregates, nest-inside-transforms
requirement)."""

from __future__ import annotations

import pytest

from slayer.core.errors import TimeAxisError
from slayer.core.query import QueryMetric

from tests._dev1836_fixtures import (
    make_exec_engine,
    month_td,
    q,
)
from tests._dev1868_fixtures import (
    CM_PART_TIER,
    LAST_REGION,
    LAST_SPEND_BY_REGION,
    RANK_LAST_BY_REGION,
    RANK_PART_BY_TIER,
    REGION_MONTHS,
    TIER_MONTHS,
    keyed_by_month,
)

TIER = "customers.tier"
REGION = "customers.regions.name"


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


RANK = f"rank({CM_PART_TIER}, direction='desc')"


class TestTransformOverCrossModelPartitioned:
    async def test_ranks_the_per_tier_totals(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.tier"], time_dimensions=month_td(),
            metrics=[QueryMetric(metric=RANK, alias="r")],
        ))
        by = keyed_by_month(resp, TIER)
        assert set(by) == TIER_MONTHS
        for (tier, _month), row in by.items():
            assert row["r"] == RANK_PART_BY_TIER[tier]

    async def test_cumsum_without_the_time_key_fails(self, exec_engine) -> None:
        query = q(dimensions=["customers.tier"], time_dimensions=month_td(),
                  metrics=[QueryMetric(metric=f"cumsum({CM_PART_TIER})", alias="c")])
        with pytest.raises(TimeAxisError):
            await exec_engine.execute(query)


class TestChangeOverCrossModelRanked:
    async def test_rank_of_the_region_last_value(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.regions.name"], time_dimensions=month_td(),
            metrics=[QueryMetric(metric=f"rank({LAST_REGION}, direction='desc')", alias="r"),
                     QueryMetric(metric=LAST_REGION, alias="l")],
        ))
        by = keyed_by_month(resp, REGION)
        assert set(by) == REGION_MONTHS
        for (region, _month), row in by.items():
            assert row["r"] == RANK_LAST_BY_REGION[region]
            assert float(row["l"]) == pytest.approx(LAST_SPEND_BY_REGION[region])

    @pytest.mark.parametrize("position", ["measure", "filter", "order"])
    async def test_change_of_the_region_last_value_fails(self, exec_engine, position) -> None:
        change = f"change({LAST_REGION})"
        kw = {
            "measure": {"metrics": [QueryMetric(metric=change, alias="ch")]},
            "filter": {"metrics": [QueryMetric(metric="sum(orders.amount)", alias="s")],
                       "filter": [f"{change} = 0"]},
            "order": {"metrics": [QueryMetric(metric="sum(orders.amount)", alias="s")],
                      "order": [{"field": change, "direction": "asc"}]},
        }[position]
        query = q(dimensions=["customers.regions.name"], time_dimensions=month_td(), **kw)
        with pytest.raises(TimeAxisError):
            await exec_engine.execute(query)


class TestTransformFilterAndOrderPositions:
    async def test_transform_in_filter_position(self, exec_engine) -> None:
        base = dict(dimensions=["customers.tier"], time_dimensions=month_td(),
                    metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")])
        plain = keyed_by_month(await exec_engine.execute(q(**base)), TIER)
        by = keyed_by_month(await exec_engine.execute(q(**base, filter=[f"{RANK} = 1"])), TIER)
        assert set(by) == {k for k in TIER_MONTHS if k[0] == "gold"}
        for key, row in by.items():
            assert row["s"] == plain[key]["s"]

    async def test_transform_as_order_target(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.tier"], time_dimensions=month_td(),
            filter=["customers.tier is not null"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
            order=[{"field": RANK, "direction": "asc"}],
        ))
        # Sorted by the hidden tier rank: gold, silver, bronze.
        assert [r[TIER] for r in resp.data] == ["gold", "gold", "gold", "silver", "bronze"]


class TestSharedInnerAcrossConsumers:
    async def test_transform_and_filter_share_one_inner(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.tier"], time_dimensions=month_td(),
            filter=[f"{CM_PART_TIER} > 100"],
            metrics=[QueryMetric(metric=RANK, alias="r")],
        ))
        by = keyed_by_month(resp, TIER)
        assert set(by) == {k for k in TIER_MONTHS if k[0] in ("gold", "silver")}  # per-tier totals > 100
        for (tier, _month), row in by.items():
            assert row["r"] == RANK_PART_BY_TIER[tier]
