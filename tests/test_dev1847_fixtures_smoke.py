"""DEV-1847 task 1.1 — the fixture smoke test.

Re-derives every oracle constant from the raw rows in pure Python (the
double-entry guard against hand-arithmetic drift) and confirms the manual
``source_queries`` encoding realizes the headline oracle in-engine. This file
passes WITHOUT the feature — it validates the dataset, not the re-aggregation.
"""

from __future__ import annotations

from collections import defaultdict
from statistics import mean

import pytest

from tests._dev1847_fixtures import (
    _CORDERS_ROWS,
    _CUSTOMERS_ROWS,
    _REGIONS_ROWS,
    _SALES_ROWS,
    ASSOCIATE_AVG_CITY_BY_REGION,
    AVG_CITY_TOTAL_BY_REGION,
    BROADCAST_GLOBAL_AVG_CITY,
    CHAIN_AVG_BY_REGION,
    COMPOSITE_AVG_BY_REGION,
    COUNT_CITY_CELLS_BY_REGION,
    DEGENERATE_SUM_BY_REGION,
    DEPTH3_MAX_AVG_BY_PRODUCT,
    EAST_CITY_TOTALS,
    FILTERED_COALESCE_AVG_BY_REGION,
    GAP_AVG,
    GAP_NULL_CELL_TOTAL,
    KEYLESS_GRAND_TOTAL,
    ROWPHASE_P_AVG_BY_REGION,
    ROW_WEIGHTED_WRONG,
    SHAPE_B_ACR,
    SHAPE_B_BAND_THRESHOLD,
    SHAPE_B_BAND_TOTAL,
    SHAPE_B_GROUP_SUM,
    dev1847_models,
    make_exec_engine,
    source_queries_equiv_model,
)


def _ssum(vals):
    nn = [v for v in vals if v is not None]
    return sum(nn) if nn else None


def _avg(vals):
    nn = [v for v in vals if v is not None]
    return mean(nn) if nn else None


def _group(keyfn):
    d = defaultdict(list)
    for row in _SALES_ROWS:
        d[keyfn(row)].append(row[4])  # amount
    return d


# (id, region, city, product, amount)
def _cr_totals():
    return {k: _ssum(v) for k, v in _group(lambda r: (r[2], r[1])).items()}


class TestOracleReDerivation:
    def test_headline_and_wrong_values(self):
        cr = _cr_totals()
        by_region = defaultdict(list)
        for (city, region), tot in cr.items():
            by_region[region].append(tot)
        derived = {r: _avg(v) for r, v in by_region.items() if _avg(v) is not None}
        assert derived == AVG_CITY_TOTAL_BY_REGION
        # row-weighted wrong value: broadcast each city total onto its rows.
        for region in ("North", "South"):
            rows = [r for r in _SALES_ROWS if r[1] == region]
            bc = [v for r in rows if (v := cr[(r[2], r[1])]) is not None]
            assert mean(bc) == pytest.approx(ROW_WEIGHTED_WRONG[region])
            assert ROW_WEIGHTED_WRONG[region] != AVG_CITY_TOTAL_BY_REGION[region]

    def test_broadcast_and_associate_city_alone(self):
        city_tot = {k: _ssum(v) for k, v in _group(lambda r: r[2]).items()}
        allv = [t for t in city_tot.values() if t is not None]
        assert mean(allv) == pytest.approx(BROADCAST_GLOBAL_AVG_CITY)
        region_cities = defaultdict(set)
        for (_id, region, city, _p, _a) in _SALES_ROWS:
            region_cities[region].add(city)
        for region, expected in ASSOCIATE_AVG_CITY_BY_REGION.items():
            vals = [v for c in region_cities[region]
                    if (v := city_tot[c]) is not None]
            assert mean(vals) == pytest.approx(expected)

    def test_degenerate_keyless_and_gap(self):
        for region, total in DEGENERATE_SUM_BY_REGION.items():
            assert _ssum([r[4] for r in _SALES_ROWS if r[1] == region]) == total
        assert _ssum([r[4] for r in _SALES_ROWS]) == KEYLESS_GRAND_TOTAL
        assert AVG_CITY_TOTAL_BY_REGION["Gap"] == GAP_AVG

    def test_null_city_rows_coalesce_into_one_cell(self):
        cr = _cr_totals()
        assert cr[(None, "Gap")] == GAP_NULL_CELL_TOTAL  # 7 + 5, one cell
        null_rows = [r for r in _SALES_ROWS if r[1] == "Gap" and r[2] is None]
        assert len(null_rows) >= 2  # more than one NULL-city row coalesced

    def test_operator_family_counts(self):
        cr = _cr_totals()
        for region, n in COUNT_CITY_CELLS_BY_REGION.items():
            cells = [t for (c, r), t in cr.items() if r == region and t is not None]
            assert len(cells) == n
        east = sorted(t for (c, r), t in cr.items() if r == "East" and t is not None)
        assert east == EAST_CITY_TOTALS

    def test_depth3(self):
        crp = {k: _ssum(v) for k, v in
               _group(lambda r: (r[2], r[1], r[3])).items()}
        mid = defaultdict(list)
        for (city, region, product), tot in crp.items():
            if tot is not None:
                mid[(region, product)].append(tot)
        mid_avg = {k: mean(v) for k, v in mid.items()}
        prod = defaultdict(list)
        for (region, product), v in mid_avg.items():
            prod[product].append(v)
        assert {p: max(vs) for p, vs in prod.items()} == DEPTH3_MAX_AVG_BY_PRODUCT

    def test_composite_sparse_and_rowphase(self):
        cr = _cr_totals()
        cr_q = {k: _ssum(v) for k, v in
                _group(lambda r: (r[2], r[1]) if r[3] == "Q" else ("_skip", r[1])).items()}
        region_tot = {r: _ssum([x[4] for x in _SALES_ROWS if x[1] == r])
                      for r in {x[1] for x in _SALES_ROWS}}
        comp_by_region = defaultdict(list)
        for (city, region) in cr:
            aq = cr_q.get((city, region))
            b = region_tot[region]
            val = None if (aq is None or b is None) else aq + b
            comp_by_region[region].append(val)
        for region, expected in COMPOSITE_AVG_BY_REGION.items():
            assert _avg(comp_by_region[region]) == pytest.approx(expected)
        # row-phase product='P' shifts the inner totals.
        pcells = {k: _ssum(v) for k, v in
                  _group(lambda r: (r[2], r[1]) if r[3] == "P" else ("_skip", r[1])).items()}
        p_region = defaultdict(list)
        for (city, region), t in pcells.items():
            if city != "_skip":
                p_region[region].append(t)
        for region, expected in ROWPHASE_P_AVG_BY_REGION.items():
            assert _avg(p_region[region]) == pytest.approx(expected)

    def test_filtered_coalesce(self):
        # product='Q' population: only Q rows form cells, so coalesce never
        # fires; P-only cities/regions contribute no cell at all.
        qcells = {k: _ssum(v) for k, v in
                  _group(lambda r: (r[2], r[1]) if r[3] == "Q" else ("_skip", r[1])).items()}
        q_region = defaultdict(list)
        for (city, region), t in qcells.items():
            if city != "_skip":
                q_region[region].append(0.0 if t is None else t)
        derived = {r: _avg(v) for r, v in q_region.items()}
        assert derived == FILTERED_COALESCE_AVG_BY_REGION

    def test_shape_b_band_totals(self):
        cr = _cr_totals()
        def band(city, region):
            t = cr[(city, region)]
            return "hi" if (t is not None and t > SHAPE_B_BAND_THRESHOLD) else "lo"
        band_total = defaultdict(list)
        group_sum = defaultdict(list)
        for (_id, region, city, _p, amount) in _SALES_ROWS:
            b = band(city, region)
            band_total[b].append(amount)
            group_sum[(region, b)].append(amount)
        assert {b: _ssum(v) for b, v in band_total.items()} == SHAPE_B_BAND_TOTAL
        assert {k: _ssum(v) for k, v in group_sum.items()} == SHAPE_B_GROUP_SUM
        # acr = avg of city totals per (region, band); band determines exactly.
        acr_cells = defaultdict(list)
        for (city, region), tot in cr.items():
            acr_cells[(region, band(city, region))].append(tot)
        derived_acr = {k: _avg(v) for k, v in acr_cells.items()}
        assert derived_acr == SHAPE_B_ACR

    def test_chain_oracle(self):
        region_of = {cid: rid for (cid, rid) in _CUSTOMERS_ROWS}
        name_of = {rid: nm for (rid, nm) in _REGIONS_ROWS}
        cust_tot = defaultdict(float)
        for (_id, cid, amt) in _CORDERS_ROWS:
            cust_tot[cid] += amt
        region_cust = defaultdict(list)
        for cid, tot in cust_tot.items():
            region_cust[name_of[region_of[cid]]].append(tot)
        assert {r: mean(v) for r, v in region_cust.items()} == CHAIN_AVG_BY_REGION


class TestSourceQueriesRealizesOracle:
    """The manual two-stage encoding produces the headline oracle in-engine —
    both the arithmetic AND the dataset agree. The equivalence sweep
    (test_dev1847_equivalence) later pins the re-aggregation against this."""

    @pytest.fixture(params=["sqlite", "duckdb"])
    async def engine(self, request):
        models = dev1847_models() + [source_queries_equiv_model()]
        async for eng in make_exec_engine(request, models=models):
            yield eng

    async def test_source_queries_matches_headline_oracle(self, engine):
        resp = await engine.execute("avg_city_total_by_region")
        region_col = next(c for c in resp.columns if c.endswith(".region"))
        acr_col = next(c for c in resp.columns if c.endswith(".acr"))
        got = {row[region_col]: row[acr_col] for row in resp.data
               if row[acr_col] is not None}
        for region, expected in AVG_CITY_TOTAL_BY_REGION.items():
            assert got[region] == pytest.approx(expected)
