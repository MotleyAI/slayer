"""DEV-1892 task 1.1 — executed values (SQLite + DuckDB) for the parameter
shapes the one rule types as legal: column-reference and aggregate-valued
parameters lifted onto the association and re-aggregation kernels, with
hand-computed oracles and the manual two-stage encoding as the independent
reference. All fail until the lift lands (the three gates reject them today).

Spec: queries/semantics — "Aggregation parameters are typed by the home
dataset's grain"; queries/attribution-modes — "Association eligibility and
input handling"; queries/partitioned-aggregates — "Re-aggregation consumes
attached operands as datasets".
"""

from __future__ import annotations

import pytest

from tests._dev1841_fixtures import (
    ModelMeasure,
    assoc_q,
    associated_warnings,
    column_default_agg_models,
    make_exec_engine as make_assoc_engine,
    status_key,
)
from tests._dev1847_fixtures import (
    INNER_CR,
    chain_q,
    degenerate_warnings,
    make_exec_engine as make_reagg_engine,
    region_key,
    rows_by,
    sales_q,
)
from tests._dev1892_fixtures import (
    ASSOC_WAVG_OK_FAN_DEFECT,
    ASSOC_WAVG_POP_BY_STATUS,
    ASSOC_WAVG_SPEND_BY_STATUS,
    ASSOC_WSUM_BY_STATUS,
    CORDERS_GLOBAL_UNWEIGHTED,
    CORDERS_GLOBAL_WAVG,
    DEGENERATE_WAVG_BY_REGION,
    UNWEIGHTED_CITY_BY_REGION,
    WAVG_CITY_BY_REGION,
    weighted_sales_models,
)

_WEIGHT_COUNT = "weight=count(id, partition_by=[city, region])"
_WAVG_REAGG = f"weighted_avg({INNER_CR}, {_WEIGHT_COUNT})"


@pytest.fixture(params=["sqlite", "duckdb"])
async def assoc_engine(request):
    async for engine in make_assoc_engine(request):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def assoc_coldef_engine(request):
    async for engine in make_assoc_engine(request, models=column_default_agg_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def reagg_engine(request):
    async for engine in make_reagg_engine(request, models=weighted_sales_models()):
        yield engine


def _region_vals(resp, measure):
    return {k[0]: v[measure] for k, v in region_key(resp).items()}


class TestAssociationColumnParameter:
    async def test_weighted_avg_by_status(self, assoc_engine):
        """Scenario: parameter determined by the entity key — the weight column
        is picked once per distinct customer (never join-multiplied)."""
        resp = await assoc_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(
                formula="customers.spend:weighted_avg(weight=customers.spend)",
                name="w")]))
        by = status_key(resp)
        assert set(by) == {("ok",), ("new",)}  # grain unchanged
        for status, expected in ASSOC_WAVG_SPEND_BY_STATUS.items():
            assert float(by[(status,)]["orders.w"]) == pytest.approx(expected)
        # c1 has two ok orders: a naive join weights c1 twice — must be unreachable.
        assert float(by[("ok",)]["orders.w"]) != pytest.approx(ASSOC_WAVG_OK_FAN_DEFECT)

    async def test_explicit_and_positional_spellings_identical(self, assoc_engine):
        """Requirement: a positional parameter folds onto the declared name —
        weighted_avg(spend) equals weighted_avg(weight=spend)."""
        kw = ModelMeasure(
            formula="customers.spend:weighted_avg(weight=customers.spend)", name="w")
        pos = ModelMeasure(
            formula="customers.spend:weighted_avg(customers.spend)", name="w")
        kw_by = status_key(await assoc_engine.execute(
            assoc_q(dimensions=["status"], measures=[kw])))
        pos_by = status_key(await assoc_engine.execute(
            assoc_q(dimensions=["status"], measures=[pos])))
        for status in ("ok", "new"):
            assert float(pos_by[(status,)]["orders.w"]) == pytest.approx(
                float(kw_by[(status,)]["orders.w"]))

    async def test_to_one_weight_with_null(self, assoc_engine):
        """Scenario: parameter over a to-one chain from the entity key, with a
        NULL weight (c4 has no region) contributing nothing to the ``new`` cell
        per SQL SUM semantics."""
        resp = await assoc_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(
                formula="customers.spend:weighted_avg(weight=customers.regions.pop)",
                name="w")]))
        by = status_key(resp)
        for status, expected in ASSOC_WAVG_POP_BY_STATUS.items():
            assert float(by[(status,)]["orders.w"]) == pytest.approx(expected)

    async def test_definition_default_parameter(self, assoc_coldef_engine):
        """Scenario: a definition default (``wsum`` weight -> spend) applies once
        per distinct entity, and is identical to spelling the parameter
        explicitly (``wsum(weight=customers.spend)``)."""
        default = await assoc_coldef_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="customers.spend:wsum", name="w")]))
        explicit = await assoc_coldef_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(
                formula="customers.spend:wsum(weight=customers.spend)", name="w")]))
        by, ex = status_key(default), status_key(explicit)
        for status, expected in ASSOC_WSUM_BY_STATUS.items():
            assert float(by[(status,)]["orders.w"]) == pytest.approx(expected)
            assert float(ex[(status,)]["orders.w"]) == pytest.approx(expected)

    async def test_percentile_literal_parameter_over_association(self, assoc_engine):
        """Scenario: percentile attributes over the association — a literal ``p``
        rides the pick unchanged; each cell's 0.5-percentile lies within its
        distinct population's [min, max]."""
        resp = await assoc_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(
                formula="customers.spend:percentile(p=0.5)", name="p")]))
        by = status_key(resp)
        bounds = {"ok": (30.0, 150.0), "new": (40.0, 150.0)}
        for status, (lo, hi) in bounds.items():
            assert lo <= float(by[(status,)]["orders.p"]) <= hi

    async def test_adding_weighted_measure_keeps_sibling(self, assoc_engine):
        """The weighted measure is cardinality-neutral — adding it changes
        neither the row set nor the sibling plain measure."""
        base = await assoc_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="amount:sum", name="m")]))
        withw = await assoc_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="amount:sum", name="m"),
                      ModelMeasure(
                          formula="customers.spend:weighted_avg(weight=customers.spend)",
                          name="w")]))
        a, b = status_key(base), status_key(withw)
        assert set(a) == set(b)
        for k in a:
            assert a[k]["orders.m"] == b[k]["orders.m"]


class TestReaggregationOperandParameter:
    async def test_weighted_avg_matches_hand_oracle(self, reagg_engine):
        """Scenario: parameter as a cell of the operand dataset — city totals
        averaged with each city's row count as weight, distinguishable from the
        unweighted average where the weights differ."""
        resp = await reagg_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=_WAVG_REAGG, name="w")]))
        vals = _region_vals(resp, "sales.w")
        for region, expected in WAVG_CITY_BY_REGION.items():
            if expected is None:
                assert vals[region] is None
            else:
                assert float(vals[region]) == pytest.approx(expected)
        # North/Gap have unequal city weights: the unweighted avg differs.
        assert float(vals["North"]) != pytest.approx(UNWEIGHTED_CITY_BY_REGION["North"])
        assert float(vals["Gap"]) != pytest.approx(UNWEIGHTED_CITY_BY_REGION["Gap"])

    async def test_matches_manual_two_stage_encoding(self, reagg_engine):
        """The re-aggregation equals the manual stage-1-sum/count →
        stage-2-weighted_avg ``source_queries`` model, region by region."""
        reagg = await reagg_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=_WAVG_REAGG, name="w")]))
        manual = await reagg_engine.execute("wavg_city_by_region")
        got = _region_vals(reagg, "sales.w")
        man_r = next(c for c in manual.columns if c.endswith(".region"))
        man_w = next(c for c in manual.columns if c.endswith(".w"))
        want = {row[man_r]: row[man_w] for row in manual.data}
        for region, w in want.items():
            if w is None:
                assert got[region] is None
            else:
                assert float(got[region]) == pytest.approx(float(w))

    async def test_operand_grain_key_to_one_weight(self, reagg_engine):
        """Scenario: parameter determined over a to-one chain from the operand
        grain key — weight ``customers.region_id`` read once per customer cell,
        distinguishable from the unweighted global average."""
        resp = await reagg_engine.execute(chain_q(
            measures=[ModelMeasure(
                formula=("weighted_avg(sum(amount, partition_by=customer_id), "
                         "weight=customers.region_id)"),
                name="w")]))
        got = float(resp.data[0]["corders.w"])
        assert got == pytest.approx(CORDERS_GLOBAL_WAVG)
        assert got != pytest.approx(CORDERS_GLOBAL_UNWEIGHTED)

    async def test_explicit_outer_grain_broadcasts_weighted_value(self, reagg_engine):
        """Scenario: explicit outer grain with a parameter — the per-region
        weighted value broadcasts identically across ``product``."""
        resp = await reagg_engine.execute(sales_q(
            dimensions=["region", "product"],
            measures=[ModelMeasure(
                formula=f"weighted_avg({INNER_CR}, {_WEIGHT_COUNT}, partition_by=region)",
                name="w")]))
        by = rows_by(resp, "sales.region", "sales.product")
        for key, row in by.items():
            expected = WAVG_CITY_BY_REGION[key[0]]
            if expected is None:
                assert row["sales.w"] is None
            else:
                assert float(row["sales.w"]) == pytest.approx(expected)

    async def test_positional_custom_weight_matches_keyword(self, reagg_engine):
        """Scenario: operand-grain parameter executes — the custom ``wavg`` with
        the weight passed positionally equals the keyword spelling, region by
        region."""
        kw = await reagg_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(
                formula=f"wavg({INNER_CR}, weight=count(id, partition_by=[city, region]))",
                name="w")]))
        pos = await reagg_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(
                formula=f"wavg({INNER_CR}, count(id, partition_by=[city, region]))",
                name="w")]))
        kw_v, pos_v = _region_vals(kw, "sales.w"), _region_vals(pos, "sales.w")
        assert set(kw_v) == set(pos_v)
        for region in kw_v:
            if kw_v[region] is None:
                assert pos_v[region] is None
            else:
                assert float(pos_v[region]) == pytest.approx(float(kw_v[region]))

    async def test_degenerate_reaggregation_with_parameter(self, reagg_engine):
        """Scenario: degenerate re-aggregation (operand grain == outer grain) with
        a parameter is identity (weight cancels over the single cell) plus the
        degenerate warning; Void's NULL-total cell stays NULL."""
        resp = await reagg_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(
                formula=("weighted_avg(sum(amount, partition_by=region), "
                         "weight=count(id, partition_by=region))"),
                name="w")]))
        vals = _region_vals(resp, "sales.w")
        for region, total in DEGENERATE_WAVG_BY_REGION.items():
            if total is None:
                assert vals[region] is None
            else:
                assert float(vals[region]) == pytest.approx(total)
        assert degenerate_warnings(resp)


class TestWeightedValueAsConsumer:
    async def test_filter_only_reference_prunes_rows(self, reagg_engine):
        """A weighted re-aggregated value behaves as a normal attached value in a
        filter-only position: filtering ``> 50`` keeps only South/East (60/60),
        dropping North (37.5), Gap (10.67), Void (NULL), and every surviving
        sibling equals its unfiltered value."""
        full = await reagg_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="tot")]))
        pruned = await reagg_engine.execute(sales_q(
            dimensions=["region"], filters=[f"{_WAVG_REAGG} > 50"],
            measures=[ModelMeasure(formula="amount:sum", name="tot")]))
        kept = {k[0] for k in region_key(pruned)}
        assert kept == {"South", "East"}
        full_tot = _region_vals(full, "sales.tot")
        for region, row in region_key(pruned).items():
            assert float(row["sales.tot"]) == pytest.approx(float(full_tot[region[0]]))


class TestAssociateModeNoSpuriousWarnings:
    async def test_attributable_weighted_slice_has_no_warning(self, assoc_engine):
        """A weighted aggregate over a customer-attributable slice (tier) stays
        exact under associate — no associated-cell warning."""
        resp = await assoc_engine.execute(assoc_q(
            dimensions=["customers.tier"],
            measures=[ModelMeasure(
                formula="customers.spend:weighted_avg(weight=customers.spend)",
                name="w")]))
        assert associated_warnings(resp) == []
