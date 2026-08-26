"""DEV-1739 execution ground truth on SQLite AND DuckDB. Every expectation is
hand-computed from the dataset in ``tests/_dev1739_fixtures.py``."""

from __future__ import annotations

import pytest

from tests._dev1739_fixtures import (
    ModelMeasure,
    SlayerQuery,
    approx_sum,
    make_exec_engine,
    month_key,
    month_td,
    rows_by,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


REGION_TOTAL = {"North": 100.0, "South": 50.0, None: 60.0}
CITY_REV = {
    ("North", "CityA"): 30.0, ("North", "CityB"): 40.0, ("North", None): 30.0,
    ("South", "CityC"): 50.0, (None, "CityD"): 60.0,
}


class TestCoarserRepeatAndShare:
    async def test_region_total_repeats_across_cities(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(formula="amount:sum", name="city_rev"),
                ModelMeasure(formula="amount:sum(partition_by=region)", name="region_rev"),
            ],
        ))
        by = rows_by(resp, "orders.region", "orders.city")
        for key, city_rev in CITY_REV.items():
            assert float(by[key]["orders.city_rev"]) == pytest.approx(city_rev)
            assert float(by[key]["orders.region_rev"]) == pytest.approx(
                REGION_TOTAL[key[0]]
            )

    async def test_share_of_region_sums_to_one_per_region(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(
                    formula="amount:sum / amount:sum(partition_by=region)",
                    name="share",
                ),
            ],
        ))
        per_region: dict = {}
        for r in resp.data:
            per_region.setdefault(r["orders.region"], 0.0)
            per_region[r["orders.region"]] += float(r["orders.share"])
        for total in per_region.values():
            assert total == pytest.approx(1.0)

    async def test_share_of_total_sums_to_one(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(
                    formula="amount:sum / amount:sum(partition_by=[])",
                    name="share",
                ),
            ],
        ))
        assert approx_sum(resp, "orders.share") == pytest.approx(1.0)


class TestNullGrainSurvival:
    async def test_null_region_group_keeps_its_total(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(formula="amount:sum(partition_by=region)", name="region_rev"),
            ],
        ))
        by = rows_by(resp, "orders.region", "orders.city")
        assert (None, "CityD") in by
        assert float(by[(None, "CityD")]["orders.region_rev"]) == pytest.approx(60.0)


class TestCardinalityAndSiblingInvariance:
    async def test_adding_partition_measure_changes_nothing_else(self, exec_engine) -> None:
        base = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula="amount:sum", name="city_rev")],
        ))
        widened = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(formula="amount:sum", name="city_rev"),
                ModelMeasure(formula="amount:sum(partition_by=region)", name="region_rev"),
            ],
        ))
        assert len(widened.data) == len(base.data)
        b = rows_by(base, "orders.region", "orders.city")
        w = rows_by(widened, "orders.region", "orders.city")
        for key in b:
            assert float(w[key]["orders.city_rev"]) == pytest.approx(
                float(b[key]["orders.city_rev"])
            )


class TestFullTotalUnderHavingAndPagination:
    async def test_having_does_not_shrink_parent_total(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            filters=["amount:sum > 30"],
            measures=[
                ModelMeasure(formula="amount:sum", name="city_rev"),
                ModelMeasure(formula="amount:sum(partition_by=region)", name="region_rev"),
            ],
        ))
        by = rows_by(resp, "orders.region", "orders.city")
        assert ("North", "CityA") not in by  # 30, removed by HAVING
        assert float(by[("North", "CityB")]["orders.region_rev"]) == pytest.approx(100.0)

    async def test_pagination_does_not_shrink_parent_total(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            order=[{"column": "city_rev", "direction": "asc"}],
            limit=1, offset=1,
            measures=[
                ModelMeasure(formula="amount:sum", name="city_rev"),
                ModelMeasure(formula="amount:sum(partition_by=region)", name="region_rev"),
            ],
        ))
        assert len(resp.data) == 1
        row = resp.data[0]
        assert float(row["orders.region_rev"]) == pytest.approx(
            REGION_TOTAL[row["orders.region"]]
        )


class TestFilteredAggregateShare:
    async def test_filter_applies_inside_partition_cte(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(formula="ok_amount:sum", name="city_ok"),
                ModelMeasure(formula="ok_amount:sum(partition_by=region)", name="region_ok"),
            ],
        ))
        by = rows_by(resp, "orders.region", "orders.city")
        assert float(by[("South", "CityC")]["orders.region_ok"]) == pytest.approx(25.0)
        assert float(by[("North", "CityB")]["orders.region_ok"]) == pytest.approx(100.0)
        assert float(by[(None, "CityD")]["orders.region_ok"]) == pytest.approx(60.0)


class TestRowLevelFilterAppliesToTotal:
    async def test_row_filter_shrinks_partition_total(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            filters=["status == 'ok'"],
            measures=[
                ModelMeasure(formula="amount:sum(partition_by=region)", name="region_rev"),
            ],
        ))
        by = rows_by(resp, "orders.region", "orders.city")
        assert float(by[("North", "CityB")]["orders.region_rev"]) == pytest.approx(100.0)
        assert float(by[("South", "CityC")]["orders.region_rev"]) == pytest.approx(25.0)
        assert float(by[(None, "CityD")]["orders.region_rev"]) == pytest.approx(60.0)


class TestGrandTotal:
    async def test_empty_partition_is_grand_total(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(formula="amount:sum(partition_by=[])", name="grand"),
            ],
        ))
        for r in resp.data:
            assert float(r["orders.grand"]) == pytest.approx(210.0)


class TestZeroSurvivingGrandTotal:
    async def test_empty_input_grand_total_keeps_rows(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(formula="amount:sum", name="city_rev"),
                ModelMeasure(formula="nomatch:sum(partition_by=[])", name="nsum"),
                ModelMeasure(formula="nomatch:count(partition_by=[])", name="ncount"),
            ],
        ))
        assert len(resp.data) == len(CITY_REV)
        by = rows_by(resp, "orders.region", "orders.city")
        for key, city_rev in CITY_REV.items():
            assert float(by[key]["orders.city_rev"]) == pytest.approx(city_rev)
            assert by[key]["orders.nsum"] is None
            assert int(by[key]["orders.ncount"]) == 0


class TestTimeDimensionPartition:
    async def test_partition_by_month_bucket(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region"],
            time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum", name="cell"),
                ModelMeasure(
                    formula="amount:sum(partition_by=ordered_at)", name="month_total",
                ),
            ],
        ))
        month_total = {"2024-01": 55.0, "2024-02": 70.0, "2024-03": 85.0}
        for r in resp.data:
            mk = month_key(r["orders.ordered_at"])
            assert float(r["orders.month_total"]) == pytest.approx(month_total[mk])
        by = {
            (r["orders.region"], month_key(r["orders.ordered_at"])): r
            for r in resp.data
        }
        assert float(by[(None, "2024-03")]["orders.month_total"]) == pytest.approx(85.0)


class TestStarCountPartition:
    async def test_star_count_repeats_region_count(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(formula="*:count(partition_by=region)", name="region_n"),
            ],
        ))
        by = rows_by(resp, "orders.region", "orders.city")
        assert int(by[("North", "CityA")]["orders.region_n"]) == 4
        assert int(by[("South", "CityC")]["orders.region_n"]) == 2
        assert int(by[(None, "CityD")]["orders.region_n"]) == 1


class TestMultiKeyPartition:
    async def test_partition_by_region_and_channel(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", "channel", "city"],
            measures=[
                ModelMeasure(
                    formula="amount:sum(partition_by=[region, channel])", name="rc",
                ),
            ],
        ))
        expect = {
            ("North", "web"): 80.0, ("North", "app"): 20.0,
            ("South", "web"): 25.0, ("South", "app"): 25.0, (None, "web"): 60.0,
        }
        for r in resp.data:
            key = (r["orders.region"], r["orders.channel"])
            assert float(r["orders.rc"]) == pytest.approx(expect[key])


class TestDegeneratePartition:
    async def test_full_grain_partition_equals_plain(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(formula="amount:sum", name="plain"),
                ModelMeasure(
                    formula="amount:sum(partition_by=[region, city])", name="deg",
                ),
            ],
        ))
        for r in resp.data:
            assert float(r["orders.deg"]) == pytest.approx(float(r["orders.plain"]))


class TestCrossModelNarrowedGrain:
    async def test_grand_total_over_target(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["customers.tier"],
            measures=[
                ModelMeasure(formula="customers.spend:sum", name="sp"),
                ModelMeasure(formula="customers.spend:sum(partition_by=[])", name="total"),
                ModelMeasure(
                    formula="customers.spend:sum / customers.spend:sum(partition_by=[])",
                    name="share",
                ),
            ],
        ))
        by = rows_by(resp, "orders.customers.tier")
        assert float(by[("gold",)]["orders.sp"]) == pytest.approx(150.0)
        assert float(by[("silver",)]["orders.sp"]) == pytest.approx(200.0)
        for r in resp.data:
            assert float(r["orders.total"]) == pytest.approx(350.0)
        assert approx_sum(resp, "orders.share") == pytest.approx(1.0)

    async def test_degenerate_equals_plain(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["customers.tier"],
            measures=[
                ModelMeasure(formula="customers.spend:sum", name="sp"),
                ModelMeasure(
                    formula="customers.spend:sum(partition_by=customers.tier)",
                    name="deg",
                ),
            ],
        ))
        for r in resp.data:
            assert float(r["orders.deg"]) == pytest.approx(float(r["orders.sp"]))

    async def test_rerooted_grand_total(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["customers.regions.name"],
            measures=[
                ModelMeasure(formula="customers.spend:sum", name="sp"),
                ModelMeasure(formula="customers.spend:sum(partition_by=[])", name="total"),
            ],
        ))
        by = rows_by(resp, "orders.customers.regions.name")
        assert float(by[("RegN",)]["orders.sp"]) == pytest.approx(300.0)
        assert float(by[("RegS",)]["orders.sp"]) == pytest.approx(50.0)
        for r in resp.data:
            assert float(r["orders.total"]) == pytest.approx(350.0)

    async def test_rerooted_partition_key_narrows_grain(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["customers.regions.name", "customers.tier"],
            measures=[
                ModelMeasure(formula="customers.spend:sum", name="sp"),
                ModelMeasure(
                    formula="customers.spend:sum(partition_by=customers.regions.name)",
                    name="region_sp",
                ),
            ],
        ))
        region_total = {"RegN": 300.0, "RegS": 50.0}
        for r in resp.data:
            assert float(r["orders.region_sp"]) == pytest.approx(
                region_total[r["orders.customers.regions.name"]]
            )


class TestCrossModelFilterRouting:
    async def test_host_local_filter_does_not_change_target_total(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["customers.tier"],
            filters=["region == 'North'"],
            measures=[
                ModelMeasure(formula="customers.spend:sum(partition_by=[])", name="total"),
            ],
        ))
        assert len(resp.data) == 2
        for r in resp.data:
            assert float(r["orders.total"]) == pytest.approx(350.0)

    async def test_target_reachable_filter_applies_to_total(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["customers.regions.name"],
            filters=["customers.tier == 'gold'"],
            measures=[
                ModelMeasure(formula="customers.spend:sum", name="sp"),
                ModelMeasure(formula="customers.spend:sum(partition_by=[])", name="total"),
            ],
        ))
        by = rows_by(resp, "orders.customers.regions.name")
        assert float(by[("RegN",)]["orders.sp"]) == pytest.approx(100.0)
        assert float(by[("RegS",)]["orders.sp"]) == pytest.approx(50.0)
        for r in resp.data:
            assert float(r["orders.total"]) == pytest.approx(150.0)
