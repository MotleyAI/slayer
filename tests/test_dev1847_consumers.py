"""DEV-1847 task 1.3 — the re-aggregated value in every consumer position
(SQLite + DuckDB): transform input, explicit outer grain, filter, row-phase
filter reaching the inner producer, first/last transform dispatch, and the
sparse composite operand. Feature tests fail until the outer wrap compiles;
the first/last dispatch guard holds today and must keep holding.

Spec: openspec …/specs/queries/partitioned-aggregates — "Re-aggregation
consumes attached operands as datasets"; queries/semantics — "Composite operand
keeps the population's cells".
"""

from __future__ import annotations

import pytest

from tests._dev1847_fixtures import (
    AVG_CITY_TOTAL_BY_REGION,
    COMPOSITE_AVG_BY_REGION,
    INNER_CR,
    ROWPHASE_P_AVG_BY_REGION,
    ModelMeasure,
    broadcast_warnings,
    make_exec_engine,
    reagg,
    region_key,
    rows_by,
    sales_q,
)

COMPOSITE = ("q_amount:sum(partition_by=[city, region]) + "
             "amount:sum(partition_by=region)")


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


class TestTransformOverReaggregated:
    async def test_rank_over_reaggregated_value(self, exec_engine):
        """Scenario: Transform over a re-aggregated value — rows rank at the
        query grain by the attached re-aggregated value; no other column changes."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[reagg("avg", INNER_CR, name="acr"),
                      ModelMeasure(formula=f"rank(avg({INNER_CR}))", name="rnk")]))
        by = {k[0]: v for k, v in region_key(resp).items()}
        # South has the largest per-region average -> rank 1 (descending).
        assert int(by["South"]["sales.rnk"]) == 1
        assert int(by["South"]["sales.rnk"]) < int(by["North"]["sales.rnk"])
        # acr unchanged by the presence of the rank transform.
        for region, expected in AVG_CITY_TOTAL_BY_REGION.items():
            assert float(by[region]["sales.acr"]) == pytest.approx(expected)


class TestArithmeticAndOrderConsumers:
    async def test_arithmetic_consumer(self, exec_engine):
        """Requirement: the re-aggregated value behaves as a normal attached
        value inside an arithmetic composite."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=f"avg({INNER_CR}) / 2", name="half")]))
        vals = {k[0]: v["sales.half"] for k, v in region_key(resp).items()}
        assert float(vals["North"]) == pytest.approx(
            AVG_CITY_TOTAL_BY_REGION["North"] / 2)
        assert float(vals["South"]) == pytest.approx(
            AVG_CITY_TOTAL_BY_REGION["South"] / 2)

    async def test_order_by_target_without_selecting(self, exec_engine):
        """Requirement: the re-aggregated value is a valid ORDER BY target even
        when not selected — rows come back in its descending order."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="tot")],
            order=[{"column": f"avg({INNER_CR})", "direction": "desc"}]))
        order = [r["sales.region"] for r in resp.data]
        # South(70) > East(60) > North(45) > Gap(10); Void(NULL) placement free.
        assert order.index("South") < order.index("East") < order.index("North") \
            < order.index("Gap")


class TestExplicitOuterGrain:
    async def test_explicit_outer_grain_broadcasts_across_product(self, exec_engine):
        """Scenario: Explicit outer grain broadcasts per the combined rules — the
        per-region average repeats across product with no implicit-broadcast
        warning."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", "product"],
            measures=[reagg("avg", INNER_CR, name="acr", partition_by="region")]))
        by = rows_by(resp, "sales.region", "sales.product")
        for region in ("North", "South"):
            for product in ("P", "Q"):
                assert float(by[(region, product)]["sales.acr"]) == pytest.approx(
                    AVG_CITY_TOTAL_BY_REGION[region])
        assert broadcast_warnings(resp) == []


class TestFilterOnReaggregated:
    async def test_filter_prunes_only(self, exec_engine):
        """Scenario: Filter on the re-aggregated value prunes only — surviving
        rows keep the unfiltered value."""
        unfiltered = await exec_engine.execute(sales_q(
            dimensions=["region"], measures=[reagg("avg", INNER_CR, name="acr")]))
        filtered = await exec_engine.execute(sales_q(
            dimensions=["region"], measures=[reagg("avg", INNER_CR, name="acr")],
            filters=[f"avg({INNER_CR}) > 50"]))
        survivors = {k[0] for k in region_key(filtered)}
        assert survivors == {"South", "East"}
        ref = {k[0]: v["sales.acr"] for k, v in region_key(unfiltered).items()}
        for k, v in region_key(filtered).items():
            assert float(v["sales.acr"]) == pytest.approx(float(ref[k[0]]))

    async def test_row_phase_filter_reaches_inner_producer(self, exec_engine):
        """Scenario: Row-phase filters reach the inner producer — product='P'
        restricts the population feeding the inner producer, shifting the value."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"], measures=[reagg("avg", INNER_CR, name="acr")],
            filters=["product = 'P'"]))
        vals = {k[0]: v["sales.acr"] for k, v in region_key(resp).items()}
        for region, expected in ROWPHASE_P_AVG_BY_REGION.items():
            assert float(vals[region]) == pytest.approx(expected)


class TestCompositeSparseOperand:
    async def test_composite_keeps_population_cells(self, exec_engine):
        """Scenario: Composite operand keeps the population's cells — a NULL
        constituent contributes NULL to its cell without removing it."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=f"avg({COMPOSITE})", name="comp")]))
        vals = {k[0]: v["sales.comp"] for k, v in region_key(resp).items()}
        for region, expected in COMPOSITE_AVG_BY_REGION.items():
            assert float(vals[region]) == pytest.approx(expected)

    async def test_sparse_cell_present_at_operand_grain(self, exec_engine):
        """The operand dataset keeps (South, Alpha) even though its composite
        value is NULL (the Q-filtered constituent has no row there). This holds
        at the combined grain today and pins the carrier's cell set."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula=COMPOSITE, name="comp")]))
        by = rows_by(resp, "sales.region", "sales.city")
        assert ("South", "Alpha") in by
        assert by[("South", "Alpha")]["sales.comp"] is None


class TestFirstLastDispatch:
    async def test_last_over_aggregate_stays_a_transform(self, exec_engine):
        """Scenario: First and last keep transform dispatch — last(sum(...)) is
        the last transform (needs a time dimension), never a re-aggregation."""
        with pytest.raises(ValueError, match="(?i)time"):
            await exec_engine.execute(sales_q(
                dimensions=["region"],
                measures=[ModelMeasure(
                    formula=f"last({INNER_CR})", name="L")]))
