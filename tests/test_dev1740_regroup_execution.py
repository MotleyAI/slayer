"""DEV-1740 Part B2 — grouping by an expression over an aggregate, executed on
SQLite AND DuckDB.

The flagship: band cities by whether their total crosses 5000, then regroup by
(region, band) — the aggregate lives at a FINER grain (city) than the query
(region). The single-stage form must reproduce the verified two-stage workaround
EXACTLY, and every measure must aggregate the raw rows ONCE (no join-back
fan-out), so non-additive measures reconcile. Expectations are hand-computed in
``tests/_dev1740_fixtures.py``.
"""

from __future__ import annotations

import pytest

from slayer.core.query import ModelMeasure, SlayerQuery

from tests._dev1740_fixtures import (
    BAND_AVG,
    BAND_COUNT,
    BAND_DISTINCT_CUST,
    BAND_OK_SUM,
    BAND_SUM,
    REGION_COUNT,
    REGION_SUM,
    band_value_tuples,
    make_exec_engine,
    month_key,
    month_td,
    rows_by,
    two_stage_banding,
)

@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


BAND = "CASE WHEN amount:sum(partition_by=city) > 5000 THEN 1 ELSE 0 END"


def _flagship(**extra) -> SlayerQuery:
    return _q(
        dimensions=["region", {"expression": BAND, "name": "band"}],
        measures=[ModelMeasure(formula="amount:sum", name="band_total")],
        **extra,
    )


# --------------------------------------------------------------------------- #
# The flagship oracle
# --------------------------------------------------------------------------- #
class TestFlagship:
    async def test_single_stage_matches_hand_computed(self, exec_engine) -> None:
        resp = await exec_engine.execute(_flagship())
        got = band_value_tuples(
            resp, region_key="orders.region", band_key="orders.band",
            total_key="orders.band_total",
        )
        want = {(reg, band, round(v, 3)) for (reg, band), v in BAND_SUM.items()}
        assert got == want

    async def test_single_stage_equals_two_stage_workaround(self, exec_engine) -> None:
        single = await exec_engine.execute(_flagship())
        two = await exec_engine.execute(two_stage_banding())
        single_t = band_value_tuples(
            single, region_key="orders.region", band_key="orders.band",
            total_key="orders.band_total",
        )
        two_t = band_value_tuples(
            two, region_key="per_city.region", band_key="per_city.band",
            total_key="per_city.band_total",
        )
        assert single_t == two_t

    async def test_null_city_group_survives(self, exec_engine) -> None:
        # The NULL-city order (6000) bands to 1 and must keep its US/1 group.
        resp = await exec_engine.execute(_flagship())
        by = rows_by(resp, "orders.region", "orders.band")
        assert ("US", 1) in {(r["orders.region"], int(r["orders.band"])) for r in resp.data}
        assert float(by[("US", 1)]["orders.band_total"]) == pytest.approx(6000.0)


# --------------------------------------------------------------------------- #
# Non-additive measures reconcile — proves the join-back does not fan out rows.
# --------------------------------------------------------------------------- #
class TestAggregateOncePerGroup:
    async def test_every_measure_matches_oracle(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", {"expression": BAND, "name": "band"}],
            measures=[
                ModelMeasure(formula="amount:sum", name="s"),
                ModelMeasure(formula="*:count", name="n"),
                ModelMeasure(formula="amount:avg", name="a"),
                ModelMeasure(formula="ok_amount:sum", name="ok"),
                ModelMeasure(formula="customer_id:count_distinct", name="cd"),
            ],
        ))
        by = {(r["orders.region"], int(r["orders.band"])): r for r in resp.data}
        for k in BAND_SUM:
            assert float(by[k]["orders.s"]) == pytest.approx(BAND_SUM[k])
            assert int(by[k]["orders.n"]) == BAND_COUNT[k]
            assert float(by[k]["orders.a"]) == pytest.approx(BAND_AVG[k])
            assert float(by[k]["orders.ok"]) == pytest.approx(BAND_OK_SUM[k])
            assert int(by[k]["orders.cd"]) == BAND_DISTINCT_CUST[k]

    async def test_row_count_reconciles_across_bands(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["region", {"expression": BAND, "name": "band"}],
            measures=[ModelMeasure(formula="*:count", name="n")],
        ))
        per_region: dict = {}
        for r in resp.data:
            per_region[r["orders.region"]] = per_region.get(r["orders.region"], 0) + int(r["orders.n"])
        assert per_region == REGION_COUNT
        assert sum(per_region.values()) == 9


# --------------------------------------------------------------------------- #
# Partition-key variants — the aggregate stage is synthesized against the
# original resolved scope (cross-model join; query-time extension column).
# --------------------------------------------------------------------------- #
class TestCrossModelPartition:
    async def test_partition_by_joined_column(self, exec_engine) -> None:
        band = ("CASE WHEN amount:sum(partition_by=customers.region_id) > 10000 "
                "THEN 1 ELSE 0 END")
        resp = await exec_engine.execute(_q(
            dimensions=["region", {"expression": band, "name": "band"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        by = {(r["orders.region"], int(r["orders.band"])): float(r["orders.s"])
              for r in resp.data}
        assert by == {("EU", 1): pytest.approx(16000.0),
                      ("EU", 0): pytest.approx(1000.0),
                      ("US", 0): pytest.approx(8000.0)}


class TestExtensionColumnPartition:
    async def test_partition_aggregate_sees_extension_column(self, exec_engine) -> None:
        # amount2 = 2*amount is added at query time; the synthesized aggregate
        # stage must resolve it. Doubling preserves the >5000 banding.
        band = "CASE WHEN amount2:sum(partition_by=city) > 10000 THEN 1 ELSE 0 END"
        resp = await exec_engine.execute(SlayerQuery(
            source_model={
                "source_name": "orders",
                "columns": [{"name": "amount2", "sql": "amount * 2", "type": "DOUBLE"}],
            },
            dimensions=["region", {"expression": band, "name": "band"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        by = {(r["orders.region"], int(r["orders.band"])): float(r["orders.s"])
              for r in resp.data}
        want = {k: pytest.approx(v) for k, v in BAND_SUM.items()}
        assert by == want


# --------------------------------------------------------------------------- #
# Name references — filter / order on the computed dimension (Codex: the band
# predicate stays in the final stage; an independent base-row filter still
# reaches the partition CTE per DEV-1739).
# --------------------------------------------------------------------------- #
class TestBandFilterAndOrder:
    async def test_filter_on_band_keeps_only_that_band(self, exec_engine) -> None:
        resp = await exec_engine.execute(_flagship(filters=["band == 1"]))
        by = {(r["orders.region"], int(r["orders.band"])): float(r["orders.band_total"])
              for r in resp.data}
        assert by == {("EU", 1): pytest.approx(16000.0),
                      ("US", 1): pytest.approx(6000.0)}

    async def test_band_filter_composes_with_base_row_filter(self, exec_engine) -> None:
        # region == 'EU' is an independent base-row filter; it drops US but does
        # not change any EU city's cross-5000 banding.
        resp = await exec_engine.execute(_flagship(filters=["band == 1", "region == 'EU'"]))
        by = {(r["orders.region"], int(r["orders.band"])): float(r["orders.band_total"])
              for r in resp.data}
        assert by == {("EU", 1): pytest.approx(16000.0)}

    async def test_base_row_filter_reaches_partition_total(self, exec_engine) -> None:
        # status == 'ok' drops Berlin's 4000 'hold' row, so Berlin's city total
        # falls to 5000 (NOT > 5000) -> band 0. Only Paris (EU) and NULL (US)
        # remain band 1.
        resp = await exec_engine.execute(_flagship(filters=["band == 1", "status == 'ok'"]))
        by = {(r["orders.region"], int(r["orders.band"])): float(r["orders.band_total"])
              for r in resp.data}
        assert by == {("EU", 1): pytest.approx(7000.0),
                      ("US", 1): pytest.approx(6000.0)}

    async def test_order_by_band(self, exec_engine) -> None:
        resp = await exec_engine.execute(_flagship(order=[{"column": "band", "direction": "asc"}]))
        bands = [int(r["orders.band"]) for r in resp.data]
        assert bands == sorted(bands)


# --------------------------------------------------------------------------- #
# Grand total ([]) and time-bucket partition keys.
# --------------------------------------------------------------------------- #
class TestGrandTotalPartitionKey:
    async def test_partition_by_empty_bands_on_grand_total(self, exec_engine) -> None:
        # Grand total 25000 > 20000 -> every city bands to 1 (a single-row
        # cross join). Group (region, 1) == region totals.
        band = "CASE WHEN amount:sum(partition_by=[]) > 20000 THEN 1 ELSE 0 END"
        resp = await exec_engine.execute(_q(
            dimensions=["region", {"expression": band, "name": "band"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        by = {(r["orders.region"], int(r["orders.band"])): float(r["orders.s"])
              for r in resp.data}
        assert by == {("EU", 1): pytest.approx(REGION_SUM["EU"]),
                      ("US", 1): pytest.approx(REGION_SUM["US"])}


class TestTimeBucketPartitionKey:
    async def test_partition_by_month_bucket(self, exec_engine) -> None:
        # Band by whether the month's total (across regions) crosses 5000:
        # Jan 9200 -> 1, Feb 14000 -> 1, Mar 1800 -> 0.
        band = "CASE WHEN amount:sum(partition_by=ordered_at) > 5000 THEN 1 ELSE 0 END"
        resp = await exec_engine.execute(_q(
            dimensions=["region", {"expression": band, "name": "band"}],
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        by = {(r["orders.region"], month_key(r["orders.ordered_at"]),
               int(r["orders.band"])): float(r["orders.s"]) for r in resp.data}
        assert by == {
            ("EU", "2024-01", 1): pytest.approx(8400.0),
            ("EU", "2024-02", 1): pytest.approx(8000.0),
            ("EU", "2024-03", 0): pytest.approx(600.0),
            ("US", "2024-01", 1): pytest.approx(800.0),
            ("US", "2024-02", 1): pytest.approx(6000.0),
            ("US", "2024-03", 0): pytest.approx(1200.0),
        }


# --------------------------------------------------------------------------- #
# Multiple aggregates in one dimension expression — two distinct partition-key
# sets => two synthesized aggregate stages.
# --------------------------------------------------------------------------- #
class TestMultipleAggregatesInOneDimension:
    async def test_two_distinct_partition_sets(self, exec_engine) -> None:
        # band = 1 iff the city total > 5000 AND the region total > 10000.
        # city>5000: Paris, Berlin, NULL ; region>10000: EU only.
        band = ("CASE WHEN amount:sum(partition_by=city) > 5000 "
                "AND amount:sum(partition_by=region) > 10000 THEN 1 ELSE 0 END")
        resp = await exec_engine.execute(_q(
            dimensions=["region", {"expression": band, "name": "band"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        by = {(r["orders.region"], int(r["orders.band"])): float(r["orders.s"])
              for r in resp.data}
        assert by == {("EU", 1): pytest.approx(16000.0),
                      ("EU", 0): pytest.approx(1000.0),
                      ("US", 0): pytest.approx(8000.0)}


# --------------------------------------------------------------------------- #
# Sibling-stage source — the desugar must synthesize the aggregate stage
# against a PRIOR stage's schema, not the base model.
# --------------------------------------------------------------------------- #
def _suffix(row, suffix):
    (key,) = [k for k in row if k.endswith(suffix)]
    return row[key]


class TestSiblingStageSource:
    async def test_banding_over_a_prior_stage(self, exec_engine) -> None:
        stage1 = SlayerQuery(
            name="per_city", source_model="orders",
            dimensions=["city", "region"],
            measures=[ModelMeasure(formula="amount:sum", name="amt")],
        )
        stage2 = SlayerQuery(
            source_model="per_city",
            dimensions=[
                "region",
                {"expression": "CASE WHEN amt:sum(partition_by=city) > 5000 "
                               "THEN 1 ELSE 0 END", "name": "band"},
            ],
            measures=[ModelMeasure(formula="amt:sum", name="band_total")],
        )
        resp = await exec_engine.execute([stage1, stage2])
        got = {(_suffix(r, ".region"), int(_suffix(r, ".band")),
                round(float(_suffix(r, ".band_total")), 3)) for r in resp.data}
        want = {(reg, band, round(v, 3)) for (reg, band), v in BAND_SUM.items()}
        assert got == want


# --------------------------------------------------------------------------- #
# Cardinality invariant — adding the computed dimension only splits groups.
# --------------------------------------------------------------------------- #
class TestCardinalityInvariant:
    async def test_region_totals_unchanged_when_summed_over_bands(self, exec_engine) -> None:
        banded = await exec_engine.execute(_flagship())
        plain = await exec_engine.execute(_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="band_total")],
        ))
        plain_by = {r["orders.region"]: float(r["orders.band_total"]) for r in plain.data}
        summed: dict = {}
        for r in banded.data:
            summed[r["orders.region"]] = summed.get(r["orders.region"], 0.0) + float(
                r["orders.band_total"]
            )
        for region, total in plain_by.items():
            assert summed[region] == pytest.approx(total)
