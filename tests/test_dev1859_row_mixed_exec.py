"""DEV-1859 tasks 1.1–1.3 — executed-value tests for mixed row/attached
aggregation sources (SQLite + DuckDB). The smoke class re-derives every oracle
constant from the widened rows and passes today; every feature class fails
until the mixed source compiles at row grain.

Spec: openspec …/specs/queries/semantics — "Row-grain aggregation sources";
queries/partitioned-aggregates — "Mixed sources carry the full
expression-source surface".
"""

from __future__ import annotations

from collections import defaultdict
from statistics import mean

import pytest

from slayer.sql.scope_check import assert_scope_closed

from slayer.engine.plan import plan_query
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1846_fixtures import (
    _SALES_ROWS as _ROWS_1846,
    dev1846_models,
    make_exec_engine as make_1846_engine,
    month_key,
    month_td,
)
from tests._dev1847_fixtures import (
    _CORDERS_ROWS,
    _CUSTOMERS_ROWS,
    _SALES_ROWS_WIDE,
    AMOUNT_BY_MIXED_BAND,
    AVG_UP_BY_PRODUCT,
    CITY_MIXED_BY_REGION,
    COALESCE_CITY_MIXED_BY_REGION,
    COUNT_CITY_MIXED_BY_REGION,
    CORR_MIXED_BY_REGION,
    CROSS_MODEL_MIXED_BY_REGION_ID,
    DEGENERATE_SUM_BY_REGION,
    DISTINCT_MIXED_BY_REGION,
    FILTERED_AVG_UP_BY_PRODUCT,
    FILTERED_MIXED_BY_REGION,
    FILTERED_MIXED_NORTH_WRONG,
    INNER_UP_CITY,
    INNER_UP_PRODUCT,
    MIXED_BAND_EXPR,
    MIXED_BAND_THRESHOLD,
    MIXED_SUM,
    MIXED_SUM_BY_REGION,
    PURE_CELL_SUM_BY_REGION,
    ROW_PRODUCT_SUM_BY_REGION,
    UNGRAINED_MIXED_BY_REGION,
    WAVG_MIXED_BY_REGION,
    ColumnRef,
    ModelMeasure,
    SlayerQuery,
    broadcast_warnings,
    degenerate_warnings,
    gen,
    make_exec_engine,
    region_key,
    rows_by,
    sales_q,
)

#: sum(qty * sum(revenue, partition_by=store), window='90d') on the dev1846
#: rows: per-month row-weighted sums 1100/1200/800, all gaps within 90 days.
MIXED_W90_BY_MONTH = {"2024-01": 1100.0, "2024-02": 2300.0, "2024-03": 3100.0}
#: sum(qty * sum(revenue, window='90d')) — the INNER-windowed constituent: the
#: trailing-90d revenue (cumulative here: 60/160/220) times each month's qty sum
#: (10/11/7). Independent raw-row oracle for the derivation cross-check.
MIXED_INNER_W90_BY_MONTH = {"2024-01": 600.0, "2024-02": 1760.0, "2024-03": 1540.0}


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine_1846(request):
    async for engine in make_1846_engine(request):
        yield engine


def _region_vals(resp, measure):
    return {k[0]: v[measure] for k, v in region_key(resp).items()}


def _avg(vals):
    nn = [v for v in vals if v is not None]
    return mean(nn) if nn else None


def _ssum(vals):
    nn = [v for v in vals if v is not None]
    return sum(nn) if nn else None


def _group(keyfn, valfn):
    d = defaultdict(list)
    for r in _SALES_ROWS_WIDE:
        d[keyfn(r)].append(valfn(r))
    return d


# (id, region, city, product, amount, quantity, unit_price)
def _avg_up_by(keyfn, rows=None):
    d = defaultdict(list)
    for r in rows if rows is not None else _SALES_ROWS_WIDE:
        d[keyfn(r)].append(r[6])
    return {k: _avg(v) for k, v in d.items()}


class TestOracleReDerivation:
    """Double-entry guard: every dev1859 constant from the raw rows."""

    def test_constituent_averages(self):
        assert _avg_up_by(lambda r: r[3]) == AVG_UP_BY_PRODUCT

    def test_headline_and_distinguishable_values(self):
        avg_p = _avg_up_by(lambda r: r[3])
        mixed = defaultdict(float)
        rowprod = defaultdict(list)
        cells = defaultdict(set)
        for r in _SALES_ROWS_WIDE:
            mixed[r[1]] += r[5] * avg_p[r[3]]
            rowprod[r[1]].append(None if r[6] is None else r[5] * r[6])
            cells[r[1]].add(r[3])
        assert dict(mixed) == MIXED_SUM_BY_REGION
        assert {k: _ssum(v) for k, v in rowprod.items()} == ROW_PRODUCT_SUM_BY_REGION
        derived_pure = {
            k: sum(v for p in ps if (v := avg_p[p]) is not None)
            for k, ps in cells.items() if k != "Void"
        }
        assert derived_pure == PURE_CELL_SUM_BY_REGION
        for region in MIXED_SUM_BY_REGION:
            assert MIXED_SUM_BY_REGION[region] != ROW_PRODUCT_SUM_BY_REGION[region]
            assert MIXED_SUM_BY_REGION[region] != PURE_CELL_SUM_BY_REGION.get(region)

    def test_ungrained_city_and_coalesce(self):
        avg_region = _avg_up_by(lambda r: r[1])
        qty_region = _group(lambda r: r[1], lambda r: r[5])
        derived = {k: None if (a := avg_region[k]) is None else a * sum(v)
                   for k, v in qty_region.items()}
        assert derived == UNGRAINED_MIXED_BY_REGION
        avg_city = _avg_up_by(lambda r: r[2])
        bare = defaultdict(list)
        coalesced = defaultdict(float)
        for r in _SALES_ROWS_WIDE:
            c = avg_city[r[2]]
            bare[r[1]].append(None if c is None else r[5] * c)
            coalesced[r[1]] += r[5] * (0.0 if c is None else c)
        assert {k: _ssum(v) for k, v in bare.items()} == CITY_MIXED_BY_REGION
        assert dict(coalesced) == COALESCE_CITY_MIXED_BY_REGION

    def test_filtered_counts_and_distinct(self):
        kept = [r for r in _SALES_ROWS_WIDE if r[5] >= 2]
        avg_f = _avg_up_by(lambda r: r[3], rows=kept)
        assert avg_f == FILTERED_AVG_UP_BY_PRODUCT
        filtered = defaultdict(float)
        for r in kept:
            filtered[r[1]] += r[5] * avg_f[r[3]]
        assert dict(filtered) == FILTERED_MIXED_BY_REGION
        assert FILTERED_MIXED_BY_REGION["North"] != FILTERED_MIXED_NORTH_WRONG
        avg_city = _avg_up_by(lambda r: r[2])
        counts = {k: sum(1 for r in v if avg_city[r[2]] is not None)
                  for k, v in _group(lambda r: r[1], lambda r: r).items()}
        assert counts == COUNT_CITY_MIXED_BY_REGION
        avg_p = _avg_up_by(lambda r: r[3])
        distinct = {k: len({r[5] * avg_p[r[3]] for r in v})
                    for k, v in _group(lambda r: r[1], lambda r: r).items()}
        assert distinct == DISTINCT_MIXED_BY_REGION

    def test_cross_model_wavg_corr_band(self):
        region_of = {cid: rid for (cid, rid) in _CUSTOMERS_ROWS}
        totals = defaultdict(float)
        for (_id, cid, amt) in _CORDERS_ROWS:
            totals[region_of[cid]] += amt
        derived = defaultdict(float)
        for (_cid, rid) in _CUSTOMERS_ROWS:
            derived[rid] += rid * totals[rid]
        assert dict(derived) == CROSS_MODEL_MIXED_BY_REGION_ID
        avg_p = _avg_up_by(lambda r: r[3])
        for region, expected in WAVG_MIXED_BY_REGION.items():
            rows = [r for r in _SALES_ROWS_WIDE if r[1] == region]
            got = (sum(r[5] ** 2 * avg_p[r[3]] for r in rows)
                   / sum(r[5] for r in rows))
            assert got == pytest.approx(expected)
        for region, expected in CORR_MIXED_BY_REGION.items():
            pts = [(r[5] * avg_p[r[3]], r[5]) for r in _SALES_ROWS_WIDE
                   if r[1] == region]
            mx, my = mean(p[0] for p in pts), mean(p[1] for p in pts)
            num = sum((x - mx) * (y - my) for x, y in pts)
            den = (sum((x - mx) ** 2 for x, _ in pts)
                   * sum((y - my) ** 2 for _, y in pts)) ** 0.5
            assert num / den == pytest.approx(expected)
        band_amt = defaultdict(list)
        for r in _SALES_ROWS_WIDE:
            band = "hi" if MIXED_SUM_BY_REGION[r[1]] > MIXED_BAND_THRESHOLD else "lo"
            band_amt[band].append(r[4])
        assert {b: _ssum(v) for b, v in band_amt.items()} == AMOUNT_BY_MIXED_BAND

    def test_w90_oracle(self):
        # dev1846: (id, region_id, store, status, revenue, qty, ...)
        store_total = defaultdict(float)
        for r in _ROWS_1846:
            store_total[r[2]] += r[4]
        monthly = defaultdict(float)
        for r in _ROWS_1846:
            monthly[r[9][:7]] += r[5] * store_total[r[2]]
        running, derived = 0.0, {}
        for m in sorted(monthly):
            running += monthly[m]
            derived[m] = running
        assert derived == MIXED_W90_BY_MONTH


class TestHeadlineOracle:
    async def test_row_weighted_value_by_region(self, exec_engine):
        """Scenario: Row-weighted value distinguishable from pure
        re-aggregation — and from the plain per-row product."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=MIXED_SUM, name="m")]))
        vals = _region_vals(resp, "sales.m")
        for region, expected in MIXED_SUM_BY_REGION.items():
            assert float(vals[region]) == pytest.approx(expected)
            row_prod = ROW_PRODUCT_SUM_BY_REGION[region]
            if row_prod is not None:
                assert vals[region] != pytest.approx(row_prod)
            if region in PURE_CELL_SUM_BY_REGION:
                assert vals[region] != pytest.approx(PURE_CELL_SUM_BY_REGION[region])
        assert broadcast_warnings(resp) == []

    async def test_adding_mixed_measure_is_cardinality_neutral(self, exec_engine):
        """Scenario: Adding a mixed measure is cardinality-neutral."""
        base = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="tot")]))
        with_mixed = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="tot"),
                      ModelMeasure(formula=MIXED_SUM, name="m")]))
        assert len(with_mixed.data) == len(base.data)
        a = {k: v["sales.tot"] for k, v in region_key(base).items()}
        b = {k: v["sales.tot"] for k, v in region_key(with_mixed).items()}
        assert len(a) == len(base.data)  # no duplicate region keys either
        assert a == b


class TestUngrainedInner:
    async def test_ungrained_inner_types_at_query_dimensions(self, exec_engine):
        """Scenario: Ungrained inner constituent types at the query's
        dimensions — no degenerate-re-aggregation warning."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="sum(quantity * avg(unit_price))",
                                   name="m")]))
        vals = _region_vals(resp, "sales.m")
        for region, expected in UNGRAINED_MIXED_BY_REGION.items():
            if expected is None:
                assert vals[region] is None
            else:
                assert float(vals[region]) == pytest.approx(expected)
        assert degenerate_warnings(resp) == []


class TestNullConstituent:
    async def test_missing_constituent_is_null_on_surviving_rows(self, exec_engine):
        """Scenario: Missing constituent value is NULL on a surviving row —
        Xi's all-NULL city cell broadcasts NULL, restored by coalesce."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[
                ModelMeasure(formula=f"sum(quantity * {INNER_UP_CITY})",
                             name="bare"),
                ModelMeasure(
                    formula=f"sum(coalesce({INNER_UP_CITY}, 0) * quantity)",
                    name="cz"),
            ]))
        bare = _region_vals(resp, "sales.bare")
        cz = _region_vals(resp, "sales.cz")
        for region, expected in CITY_MIXED_BY_REGION.items():
            if expected is None:
                assert bare[region] is None
            else:
                assert float(bare[region]) == pytest.approx(expected)
        for region, expected in COALESCE_CITY_MIXED_BY_REGION.items():
            assert float(cz[region]) == pytest.approx(expected)


class TestFilterInheritance:
    async def test_row_filter_bounds_population_and_constituents(self, exec_engine):
        """Scenario: Row filters bound both the population and the
        constituents — quantity >= 2 shifts avg_P from 5 to 6."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"], filters=["quantity >= 2"],
            measures=[ModelMeasure(formula=MIXED_SUM, name="m")]))
        vals = _region_vals(resp, "sales.m")
        for region, expected in FILTERED_MIXED_BY_REGION.items():
            assert float(vals[region]) == pytest.approx(expected)
        assert vals["North"] != pytest.approx(FILTERED_MIXED_NORTH_WRONG)


class TestCountFamily:
    async def test_count_counts_rows_with_nonnull_operand(self, exec_engine):
        """Scenario: Count semantics over a mixed source — Void's rows carry a
        NULL city constituent, so its count is 0, not a dropped row."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=f"count(quantity * {INNER_UP_CITY})",
                                   name="n")]))
        vals = _region_vals(resp, "sales.n")
        for region, expected in COUNT_CITY_MIXED_BY_REGION.items():
            assert int(vals[region]) == expected

    async def test_count_distinct_over_mixed_values(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(
                formula=f"count_distinct(quantity * {INNER_UP_PRODUCT})",
                name="nd")]))
        vals = _region_vals(resp, "sales.nd")
        for region, expected in DISTINCT_MIXED_BY_REGION.items():
            assert int(vals[region]) == expected


class TestCrossModelConstituent:
    async def test_cross_model_inner_constituent_executes(self, exec_engine):
        """Requirement: attached constituents may be cross-model."""
        resp = await exec_engine.execute(SlayerQuery(
            source_model="customers",
            dimensions=[ColumnRef(name="region_id")],
            measures=[ModelMeasure(
                formula="sum(region_id * sum(corders.amount, "
                        "partition_by=region_id))",
                name="xm")]))
        by = rows_by(resp, "customers.region_id")
        for rid, expected in CROSS_MODEL_MIXED_BY_REGION_ID.items():
            assert float(by[(rid,)]["customers.xm"]) == pytest.approx(expected)


class TestFilterAndOrderPositions:
    async def test_filter_position_prunes_without_altering_values(self, exec_engine):
        """Scenario: Filter and order positions — the filter types as a
        measure, pruning rows with surviving values unchanged."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"], filters=[f"{MIXED_SUM} > 40"],
            measures=[ModelMeasure(formula="amount:sum", name="tot")]))
        vals = _region_vals(resp, "sales.tot")
        assert set(vals) == {"North", "South", "East"}
        for region in vals:
            assert float(vals[region]) == pytest.approx(
                DEGENERATE_SUM_BY_REGION[region])

    async def test_order_position_sorts_by_the_mixed_value(self, exec_engine):
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="tot")],
            order=[{"column": MIXED_SUM, "direction": "desc"}]))
        order = [r["sales.region"] for r in resp.data]
        assert order == ["South", "North", "East", "Gap", "Void"]


class TestDimensionPosition:
    async def test_grain_self_contained_banding_dimension(self, exec_engine):
        """Scenario: Grain-self-contained dimension position — the banded
        mixed value groups rows with unchanged cardinality."""
        resp = await exec_engine.execute(sales_q(
            dimensions=[{"expression": MIXED_BAND_EXPR, "name": "mixed_band"}],
            measures=[ModelMeasure(formula="amount:sum", name="tot")]))
        by = rows_by(resp, "sales.mixed_band")
        assert len(resp.data) == len(AMOUNT_BY_MIXED_BAND)
        for band, expected in AMOUNT_BY_MIXED_BAND.items():
            assert float(by[(band,)]["sales.tot"]) == pytest.approx(expected)


class TestOuterModifiers:
    async def test_outer_partition_by_broadcasts_region_totals(self, exec_engine):
        """Scenario: Outer partition_by over a mixed source."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region", "product"],
            measures=[ModelMeasure(
                formula=f"sum(quantity * {INNER_UP_PRODUCT}, "
                        f"partition_by=region)",
                name="m")]))
        by = rows_by(resp, "sales.region", "sales.product")
        assert len(resp.data) == 8  # the (region, product) pairs in the rows
        assert len(by) == len(resp.data)  # and no duplicate pair keys
        for (region, _product), row in by.items():
            assert float(row["sales.m"]) == pytest.approx(
                MIXED_SUM_BY_REGION[region])

    async def test_parametric_outer_with_row_valued_weight(self, exec_engine):
        """Scenario: Parametric outer with a row-valued parameter."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(
                formula=f"wavg(quantity * {INNER_UP_PRODUCT}, weight=quantity)",
                name="w")]))
        vals = _region_vals(resp, "sales.w")
        for region, expected in WAVG_MIXED_BY_REGION.items():
            assert float(vals[region]) == pytest.approx(expected)

    async def test_custom_model_defined_outer(self, exec_engine):
        """Scenario: Custom and multi-input aggregations — dsum is SUM."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=f"dsum(quantity * {INNER_UP_PRODUCT})",
                                   name="ds")]))
        vals = _region_vals(resp, "sales.ds")
        for region, expected in MIXED_SUM_BY_REGION.items():
            assert float(vals[region]) == pytest.approx(expected)

    async def test_corr_two_input_builtin(self, exec_engine):
        """Scenario: Custom and multi-input aggregations — corr's own
        rendering path; Gap's operand is exactly linear in quantity."""
        resp = await exec_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(
                formula=f"corr(quantity * {INNER_UP_PRODUCT}, other=quantity)",
                name="c")]))
        vals = _region_vals(resp, "sales.c")
        for region, expected in CORR_MIXED_BY_REGION.items():
            assert float(vals[region]) == pytest.approx(expected)

    async def test_outer_window_trailing_90d(self, exec_engine_1846):
        """Scenario: Outer window over a mixed source (dev1846 rows: every
        gap is within 90 days, so each bucket carries the running total)."""
        resp = await exec_engine_1846.execute(SlayerQuery(
            source_model="sales", time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="sum(qty * sum(revenue, partition_by=store), "
                        "window='90d')",
                name="w")]))
        got = {month_key(r["sales.ordered_at"]): r["sales.w"] for r in resp.data}
        assert len(resp.data) == len(MIXED_W90_BY_MONTH)
        for month, expected in MIXED_W90_BY_MONTH.items():
            assert float(got[month]) == pytest.approx(expected)


class TestWindowedConstituent:
    _FORMULA = "sum(qty * sum(revenue, window='90d'))"

    def test_windowed_inner_is_exactly_one_nested_producer(self):
        """Scenario: the windowed inner is exactly one nested producer at the
        bucket grain, row-attached — never an attach of the enclosing level."""
        bundle = ResolvedSourceBundle(
            source_model=dev1846_models()[0],
            referenced_models=dev1846_models()[1:])
        planned = plan_query(
            query=SlayerQuery(source_model="sales", time_dimensions=month_td(),
                              measures=[ModelMeasure(formula=self._FORMULA, name="m")]),
            bundle=bundle)
        [attach] = planned.regroup_attach_plans
        assert attach.attach_phase == "row"

    async def test_windowed_inner_executed_values(self, exec_engine_1846):
        """Scenario: each bucket carries the hand-computed row-weighted value —
        the raw-row oracle, cross-checked against the plain windowed measure."""
        resp = await exec_engine_1846.execute(SlayerQuery(
            source_model="sales", time_dimensions=month_td(),
            measures=[ModelMeasure(formula=self._FORMULA, name="m"),
                      ModelMeasure(formula="sum(revenue, window='90d')", name="w"),
                      ModelMeasure(formula="qty:sum", name="q")]))
        got = {month_key(r["sales.ordered_at"]):
               (r["sales.m"], r["sales.w"], r["sales.q"]) for r in resp.data}
        assert set(got) == set(MIXED_INNER_W90_BY_MONTH)
        for month, expected in MIXED_INNER_W90_BY_MONTH.items():
            m, w, q = got[month]
            assert float(m) == pytest.approx(expected)          # raw oracle
            assert float(m) == pytest.approx(float(w) * float(q))  # derivation


class TestSqlHygiene:
    @pytest.mark.parametrize("formula", [
        MIXED_SUM,
        f"sum(quantity * {INNER_UP_PRODUCT}, partition_by=region)",
        f"count(quantity * {INNER_UP_CITY})",
    ])
    async def test_no_placeholder_leak_and_closed_scopes(self, formula):
        sql = await gen(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=formula, name="m")]))
        assert "__regroup__" not in sql, f"placeholder leaked:\n{sql}"
        assert_scope_closed(sql)
