"""DEV-1847 tasks 1.2–1.4 — executed-value re-aggregation tests (SQLite +
DuckDB). The headline oracle and its distinguishable wrong value, the outer
operator family, to-one chain attribution, and the null / all-null / keyless
pins. All fail until the feature lands (the parse gate rejects the outer wrap).

Spec: openspec …/specs/queries/semantics — "Second-order aggregation over
attached values"; queries/partitioned-aggregates — "Re-aggregation consumes
attached operands as datasets", "Re-aggregation null, empty, and keyless cases".
"""

from __future__ import annotations

import pytest

from tests._dev1847_fixtures import (
    AVG_CITY_TOTAL_BY_REGION,
    CHAIN_AVG_BY_REGION,
    COUNT_CITY_CELLS_BY_REGION,
    DEGENERATE_SUM_BY_REGION,
    EAST_CITY_TOTALS,
    GAP_AVG,
    GAP_NULL_CELL_TOTAL,
    INNER_CR,
    KEYLESS_GRAND_TOTAL,
    ROW_WEIGHTED_WRONG,
    ColumnRef,
    ModelMeasure,
    SlayerQuery,
    broadcast_warnings,
    chain_q,
    degenerate_warnings,
    make_exec_engine,
    reagg,
    region_key,
    rows_by,
    sales_q,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _region_vals(resp, measure="sales.acr"):
    return {k[0]: v[measure] for k, v in region_key(resp).items()}


class TestHeadlineOracle:
    async def test_average_of_city_totals_per_region(self, exec_engine):
        """Scenario: Average of city totals per region — unweighted, by executed
        values, distinguishable from the row-count-weighted wrong value."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[reagg("avg", INNER_CR, name="acr")]))
        vals = _region_vals(resp)
        for region, expected in AVG_CITY_TOTAL_BY_REGION.items():
            assert float(vals[region]) == pytest.approx(expected)
        # The naive broadcast-into-avg weights by row count — must be unreachable.
        for region, wrong in ROW_WEIGHTED_WRONG.items():
            assert float(vals[region]) != pytest.approx(wrong)

    async def test_adding_reaggregated_measure_is_cardinality_neutral(self, exec_engine):
        """Adding the re-aggregated measure changes neither the row set nor the
        sibling plain measure (the core SLayer invariant)."""
        base = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="tot")]))
        with_reagg = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="tot"),
                      reagg("avg", INNER_CR, name="acr")]))
        a = {k: v["sales.tot"] for k, v in region_key(base).items()}
        b = {k: v["sales.tot"] for k, v in region_key(with_reagg).items()}
        assert set(a) == set(b)
        for k in a:
            assert a[k] == b[k]


class TestDegenerateIdentity:
    async def test_degenerate_is_identity_plus_warning(self, exec_engine):
        """Scenario: Degenerate re-aggregation is identity plus a warning —
        avg(sum(amount)) equals the per-cell sum and warns with the partition_by
        remedy (operand grain == outer grain)."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="avg(sum(amount))", name="acr")]))
        vals = _region_vals(resp)
        for region, total in DEGENERATE_SUM_BY_REGION.items():
            assert float(vals[region]) == pytest.approx(total)
        warns = degenerate_warnings(resp)
        assert len(warns) == 1, "exactly one degenerate warning per semantic event"
        msg = warns[0].human_message().lower()
        assert "partition_by" in msg        # the remedy
        assert "region" in msg              # names the (equal) operand/outer grain


class TestChainAttribution:
    async def test_outer_dimension_attributed_through_to_one_chain(self, exec_engine):
        """Scenario: Outer dimension attributed through a to-one chain — inner
        grain customer_id, dimension region reached over provably to-one hops;
        partitions exactly with NO broadcast warning."""
        resp = await exec_engine.execute(chain_q(
            dimensions=["customers.regions.name"],
            measures=[reagg("avg", "sum(amount, partition_by=customer_id)",
                            name="acc")]))
        by = rows_by(resp, "corders.customers.regions.name")
        for region, expected in CHAIN_AVG_BY_REGION.items():
            assert float(by[(region,)]["corders.acc"]) == pytest.approx(expected)
        assert broadcast_warnings(resp) == []


class TestOuterOperatorFamily:
    async def test_count_counts_cells_with_nonnull_total(self, exec_engine):
        """Scenario: count over an attached operand counts the inner cells with a
        non-null value."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[reagg("count", INNER_CR, name="nc")]))
        vals = _region_vals(resp, "sales.nc")
        for region, n in COUNT_CITY_CELLS_BY_REGION.items():
            assert int(vals[region]) == n

    @pytest.mark.parametrize("agg,expected", [
        ("min", min(EAST_CITY_TOTALS)),
        ("max", max(EAST_CITY_TOTALS)),
        ("median", 50.0),            # middle of [50, 50, 80]
        ("count_distinct", 2),       # {50, 80}
    ])
    async def test_scalar_family_over_east_cells(self, exec_engine, agg, expected):
        """Requirement: the plain scalar family resolves over the inner grain
        rows — East's city totals are [50, 50, 80]."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[reagg(agg, INNER_CR, name="v")]))
        east = _region_vals(resp, "sales.v")["East"]
        assert float(east) == pytest.approx(float(expected))

    async def test_percentile_is_bounded_by_the_cell_totals(self, exec_engine):
        """Scenario: parametric outer aggregation — the 0.9-percentile of a
        region's city totals lies within [min, max] of those totals (exact
        interpolation differs by engine, so the bound is the portable oracle)."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[reagg("percentile", INNER_CR, name="p", p=0.9)]))
        north = float(_region_vals(resp, "sales.p")["North"])
        # North city totals are [30, 60]; the 0.9 percentile sits strictly above
        # the mean/median (45) and at most the max (60), excluding min/avg/median
        # — robust across PERCENTILE_CONT/DISC interpolation.
        assert 45.0 < north <= 60.0

    async def test_custom_model_defined_outer_aggregation(self, exec_engine):
        """Requirement: a model-defined custom aggregation is a valid OUTER
        aggregation — ``dsum`` (SUM) of the city totals equals the region total."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[reagg("dsum", INNER_CR, name="ds")]))
        vals = _region_vals(resp, "sales.ds")
        for region, total in DEGENERATE_SUM_BY_REGION.items():
            assert float(vals[region]) == pytest.approx(total)


class TestNullEmptyKeyless:
    async def test_null_grain_component_is_its_own_cell(self, exec_engine):
        """Scenario: Null grain component is one cell — Gap's two NULL-city rows
        (7 + 5) coalesce null-safely into ONE cell (12), so the region averages
        avg(12, 8) = 10, not avg(7, 5, 8)=6.67 nor the NULL-excluded avg(8)=8."""
        # Inner producer grain: the (Gap, NULL) cell totals both NULL-city rows.
        cells = await exec_engine.execute(sales_q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula=INNER_CR, name="cr")]))
        by_cell = rows_by(cells, "sales.region", "sales.city")
        assert float(by_cell[("Gap", None)]["sales.cr"]) == pytest.approx(
            GAP_NULL_CELL_TOTAL)
        # Re-aggregated by region, that single null cell participates once.
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[reagg("avg", INNER_CR, name="acr")]))
        assert float(_region_vals(resp)["Gap"]) == pytest.approx(GAP_AVG)

    async def test_empty_population_cell_is_not_fabricated(self, exec_engine):
        """Scenario: a population cell with no operand rows is governed by the
        population rules — a row filter that empties a region drops it, never
        fabricating a row. Filtering to product='Q' leaves Gap/Void with no
        rows (both are P-only)."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"], filters=["product = 'Q'"],
            measures=[reagg("avg", INNER_CR, name="acr")]))
        regions = {k[0] for k in region_key(resp)}
        assert "Gap" not in regions and "Void" not in regions
        assert {"North", "South", "East"} <= regions

    async def test_all_null_operand_values(self, exec_engine):
        """Scenario: All-null operand values — Void's only cell sums to NULL, so
        avg yields NULL and count yields 0 for that cell."""
        avg_resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[reagg("avg", INNER_CR, name="acr")]))
        assert region_key(avg_resp)[("Void",)]["sales.acr"] is None
        cnt_resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[reagg("count", INNER_CR, name="nc")]))
        assert int(region_key(cnt_resp)[("Void",)]["sales.nc"]) == 0

    async def test_keyless_inner_is_global_identity_plus_warning(self, exec_engine):
        """Scenario: Keyless inner aggregate — avg(sum(amount, partition_by=[]))
        executes as the global total with the degenerate warning, no error."""
        resp = await exec_engine.execute(sales_q(
            measures=[reagg("avg", "sum(amount, partition_by=[])", name="g")]))
        assert float(resp.data[0]["sales.g"]) == pytest.approx(KEYLESS_GRAND_TOTAL)
        assert degenerate_warnings(resp)


class TestCrossModelInner:
    async def test_cross_model_inner_aggregate_executes(self, exec_engine):
        """Deferred from stage 2: a cross-model inner aggregate (corders.amount
        summed from customers at the region_id grain) re-aggregates over its
        cells — region 1 orders 10+20+40, region 2 order 100."""
        resp = await exec_engine.execute(SlayerQuery(
            source_model="customers",
            dimensions=[ColumnRef(name="region_id")],
            measures=[ModelMeasure(
                formula="avg(sum(corders.amount, partition_by=region_id))",
                name="art")]))
        by = rows_by(resp, "customers.region_id")
        assert float(by[(1,)]["customers.art"]) == pytest.approx(70.0)
        assert float(by[(2,)]["customers.art"]) == pytest.approx(100.0)
