"""Cross-model first/last × partition_by, executed values in every
position (spec: queries/partitioned-aggregates, cross-model ranked requirement).
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField

from slayer.core.query import QueryMetric

from tests._dev1836_fixtures import (
    AMOUNT_BY_TIER,
    make_exec_engine,
    q,
    rows_by,
)
from tests._dev1868_fixtures import (
    AMOUNT_BY_REGION_NAME,
    FIRST_REGION,
    FIRST_SPEND_BY_REGION,
    LAST_SPEND_BY_TIER,
    LAST_TIER,
)
from slayer.engine.query_engine import SlayerQueryEngine

TIER = "customers.tier"
REGION = "customers.regions.name"


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


class TestMeasurePosition:
    async def test_last_by_tier_with_plain_sibling(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.tier"],
            metrics=[QueryMetric(metric=LAST_TIER, alias="l"),
                      QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        by = rows_by(resp, TIER)
        assert len(resp.data) == 4  # sibling grain unchanged
        for tier, expected in LAST_SPEND_BY_TIER.items():
            cell = by[(tier,)]["l"]
            if expected is None:
                assert cell is None
            else:
                assert float(cell) == pytest.approx(expected)
            sibling = AMOUNT_BY_TIER.get(tier, 7.0)
            assert float(by[(tier,)]["s"]) == pytest.approx(sibling)

    async def test_first_by_region(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.regions.name"],
            metrics=[QueryMetric(metric=FIRST_REGION, alias="f"),
                      QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        by = rows_by(resp, REGION)
        for region, expected in FIRST_SPEND_BY_REGION.items():
            assert float(by[(region,)]["f"]) == pytest.approx(expected)
            assert float(by[(region,)]["s"]) == pytest.approx(
                AMOUNT_BY_REGION_NAME[region])


class TestFilterPosition:
    async def test_filter_masks_by_the_measure_form_value(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.tier"],
            filter=[f"{LAST_TIER} > 100"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        # Only silver's last spend (150) clears 100.
        assert [(r[TIER], float(r["s"])) for r in resp.data] == [
            ("silver", 30.0)]


class TestOrderPosition:
    async def test_order_sorts_by_the_undeclared_aggregate(self, exec_engine: SlayerQueryEngine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.tier"],
            filter=["customers.tier is not null"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
            order=[{"field": LAST_TIER, "direction": "desc"}],
        ))
        # silver 150 > gold 60 > bronze 40; the sort key stays hidden.
        assert [r[TIER] for r in resp.data] == ["silver", "gold", "bronze"]
        assert set(resp.columns) == {TIER, "s"}


class TestDimensionPosition:
    async def test_ranked_band_groups_rows(self, exec_engine) -> None:
        band = f"CASE WHEN {LAST_TIER} > 100 THEN 'hi' ELSE 'lo' END"
        resp = await exec_engine.execute(q(
            dimensions=[{"field": band, "alias": "band"}],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        by = rows_by(resp, "band")
        # hi = silver (30); lo = gold + bronze + NULL-customer (40+40+7).
        assert float(by[("hi",)]["s"]) == pytest.approx(30.0)
        assert float(by[("lo",)]["s"]) == pytest.approx(87.0)


class TestRanklessTargetStaysTyped:
    async def test_no_ranking_column_is_a_clear_error(self, exec_engine: SlayerQueryEngine) -> None:
        # customers has no default_time_dimension and, with a second temporal
        # column, no sole-column default: the typed ranking-time error names the remedy.
        customers = await exec_engine.storage.get_dataset("customers")
        assert customers is not None
        await exec_engine.storage.save_dataset(customers.model_copy(update={"fields": [
            *customers.fields, DatasetField(name="signup_copy", sql="signup_at", datatype=DataType.DATETIME),
        ]}))
        query = q(
            dimensions=["customers.tier"],
            metrics=[QueryMetric(
                metric="last(customers.spend, partition_by=customers.tier)",
                alias="l")],
        )
        with pytest.raises(ValueError, match=r"ranking time field"):
            await exec_engine.execute(query)
