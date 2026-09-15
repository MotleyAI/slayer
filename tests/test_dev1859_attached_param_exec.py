"""DEV-1859 task 4.4 — leg C executed values (SQLite + DuckDB): an attached
(aggregate-valued) parameter on a row-level / literal / mixed source compiles by
row-attaching the parameter's producer into the aggregation's input relation.
Every oracle is a raw-row reduction in ``tests/_dev1859_fixtures``. The one
residue: under broadcast a parameter reading host columns inside a
target-rooted producer is a typed error (DEV-1906 re-roots it).

Spec: openspec …/specs/queries/partitioned-aggregates — "Attached parameters on
row-level sources"; queries/semantics — "Ungrained aggregate parameters type at
the query grain".
"""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1841_fixtures import (
    ModelMeasure,
    assoc_q,
    bcast_q,
    broadcast_warnings,
    dev1840_models,
    error_q,
    make_exec_engine as make_orders_engine,
    q as orders_q,
    status_key,
)
from tests._dev1847_fixtures import (
    make_exec_engine as make_sales_engine,
    region_key,
    sales_q,
)
from tests._dev1859_fixtures import (
    assoc_wavg_new_without_c4,
    assoc_wavg_region_weight,
    broadcast_wavg_global,
    broadcast_wavg_target_side,
    cross_model_param_by_status,
    customers_wsum_models,
    literal_source_wsum_by_region,
    mixed_plus_param_by_region,
    ordinary_avg_amount_global,
    ordinary_wavg_by_region,
    ordinary_wavg_global,
    sales_wsum_models,
    ungrained_wsum_assoc_by_status,
    ungrained_wsum_reagg_by_region,
    ungrained_wsum_row_by_region,
)

_HEADLINE = ("customers.spend:weighted_avg("
             "weight=sum(amount, partition_by=customers.regions.name))")
_ORDINARY = "weighted_avg(amount, weight=sum(amount, partition_by=region))"
_MIXED_PARAM = ("weighted_avg(quantity * avg(unit_price, partition_by=product), "
                "weight=sum(amount, partition_by=region))")
_CROSS = ("amount:weighted_avg("
          "weight=sum(customers.spend, partition_by=customers.regions.name))")
_TARGET_SIDE = ("customers.spend:weighted_avg("
                "weight=sum(customers.spend, partition_by=customers.regions.name))")


@pytest.fixture(params=["sqlite", "duckdb"])
async def orders_engine(request):
    async for engine in make_orders_engine(request):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def orders_wsum_engine(request):
    async for engine in make_orders_engine(request, models=customers_wsum_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def sales_engine(request):
    async for engine in make_sales_engine(request, models=sales_wsum_models()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def weak_plans_engine(request):
    async for engine in make_orders_engine(
            request, models=dev1840_models(strong_plans=False)):
        yield engine


def _region_vals(resp, measure):
    return {k[0]: v[measure] for k, v in region_key(resp).items()}


def _assert_region(vals, oracle):
    for region, expected in oracle.items():
        if expected is None:
            assert vals[region] is None, region
        else:
            assert float(vals[region]) == pytest.approx(expected), region


class TestAssociateHeadline:
    async def test_region_weighted_average_by_status(self, orders_engine):
        """Scenario: Associate-mode attached parameter executes."""
        resp = await orders_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula=_HEADLINE, name="w")]))
        by = status_key(resp)
        assert set(by) == {("ok",), ("new",)}
        for status, expected in assoc_wavg_region_weight().items():
            assert float(by[(status,)]["orders.w"]) == pytest.approx(expected)

    async def test_null_region_entity_weights_by_the_null_cell(self, orders_engine):
        """Scenario: A NULL-region entity weights by the NULL cell — c4's NULL
        region cell (its order + the orphan order) weights c4 in, distinct from
        dropping c4 entirely."""
        resp = await orders_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula=_HEADLINE, name="w")]))
        new = float(status_key(resp)[("new",)]["orders.w"])
        assert new == pytest.approx(assoc_wavg_region_weight()["new"])
        assert new != pytest.approx(assoc_wavg_new_without_c4())


class TestDefaultAndErrorModeTwins:
    async def test_default_mode_refuses_the_host_rooted_parameter(self, orders_engine):
        """Scenario: Default-mode twin — the broadcast producer is rooted at
        customers and the parameter reads orders.amount, which customers cannot
        reach: a plan-time typed error names the root, the leaf and the
        associate remedy (decision 14 residue; DEV-1906 re-roots it)."""
        with pytest.raises(SlayerError) as ei:
            await orders_engine.execute(orders_q(
                dimensions=["status"],
                measures=[ModelMeasure(formula=_HEADLINE, name="w")]))
        msg = str(ei.value)
        assert "'customers'" in msg and "'amount'" in msg
        assert "to_many_handling='associate'" in msg

    @pytest.mark.xfail(strict=True, reason="host-rooted parameter re-rooting (DEV-1906)")
    async def test_default_mode_broadcasts_the_global_value(self, orders_engine):
        """DEV-1906 target: the OMITTED (default) to_many_handling broadcasts the
        customers-rooted global weighted value identically to both status cells,
        with the broadcast warning."""
        resp = await orders_engine.execute(orders_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula=_HEADLINE, name="w")]))
        by = status_key(resp)
        expected = broadcast_wavg_global()
        for status in ("ok", "new"):
            assert float(by[(status,)]["orders.w"]) == pytest.approx(expected)
        assert broadcast_warnings(resp)

    async def test_broadcast_mode_target_side_parameter_executes(self, orders_engine):
        """Control: a parameter reading only customers-side columns nests inside
        the customers-rooted producer and broadcasts to both cells."""
        resp = await orders_engine.execute(bcast_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula=_TARGET_SIDE, name="w")]))
        by = status_key(resp)
        expected = broadcast_wavg_target_side()
        for status in ("ok", "new"):
            assert float(by[(status,)]["orders.w"]) == pytest.approx(expected)
        assert broadcast_warnings(resp)

    async def test_error_mode_refuses_the_dimension(self, orders_engine):
        """Scenario: Default-mode twin — error mode refuses the unattributable
        dimension, never a parameter/determination error."""
        with pytest.raises(SlayerError) as ei:
            await orders_engine.execute(error_q(
                dimensions=["status"],
                measures=[ModelMeasure(formula=_HEADLINE, name="w")]))
        msg = str(ei.value)
        assert "status" in msg
        assert "row-level value" not in msg  # not the deleted parameter remedy
        assert "not determined" not in msg


class TestOrdinaryKernel:
    async def test_global_row_attached_weight(self, sales_engine):
        """Scenario: Ordinary-mode attached parameter executes — a global query
        so per-region weights vary, distinguishable from the plain average."""
        resp = await sales_engine.execute(sales_q(
            measures=[ModelMeasure(formula=_ORDINARY, name="w")]))
        got = float(resp.data[0]["sales.w"])
        assert got == pytest.approx(ordinary_wavg_global())
        assert got != pytest.approx(ordinary_avg_amount_global())

    async def test_positional_parameter_matches_keyword(self, sales_engine):
        """Requirement: a parameter may be positional — the second positional arg
        folds onto ``weight`` and equals the keyword spelling."""
        kw = await sales_engine.execute(sales_q(
            measures=[ModelMeasure(formula=_ORDINARY, name="w")]))
        pos = await sales_engine.execute(sales_q(
            measures=[ModelMeasure(
                formula="weighted_avg(amount, sum(amount, partition_by=region))",
                name="w")]))
        assert float(pos.data[0]["sales.w"]) == pytest.approx(
            float(kw.data[0]["sales.w"]))


class TestMixedPlusParameter:
    async def test_both_inputs_row_attached(self, sales_engine):
        """Scenario: Attached parameter beside a mixed source — the source
        constituent and the parameter both row-attach; no placeholder leak."""
        resp = await sales_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=_MIXED_PARAM, name="w")]))
        _assert_region(_region_vals(resp, "sales.w"), mixed_plus_param_by_region())


class TestUngrainedParameterThreeKernels:
    async def test_row_source(self, sales_engine):
        """Scenario: Ungrained parameter on a row-level source — wsum(amount,
        weight=sum(amount)); the weight types at [region] (region total²)."""
        resp = await sales_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="wsum(amount, weight=sum(amount))",
                                   name="w")]))
        _assert_region(_region_vals(resp, "sales.w"), ungrained_wsum_row_by_region())

    async def test_association(self, orders_wsum_engine):
        """Scenario: Ungrained parameter under association — the weight types at
        [status], broadcast onto each distinct customer."""
        resp = await orders_wsum_engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(
                formula="customers.spend:wsum(weight=sum(amount))", name="w")]))
        by = status_key(resp)
        for status, expected in ungrained_wsum_assoc_by_status().items():
            assert float(by[(status,)]["orders.w"]) == pytest.approx(expected)

    async def test_reaggregation(self, sales_engine):
        """Scenario: Ungrained parameter on a re-aggregation — the ungrained
        count(id) types at [region] and is carried onto each city cell."""
        resp = await sales_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(
                formula=("wsum(sum(amount, partition_by=[city, region]), "
                         "weight=count(id))"),
                name="w")]))
        _assert_region(_region_vals(resp, "sales.w"),
                       ungrained_wsum_reagg_by_region())


class TestLiteralSource:
    async def test_literal_source_with_attached_parameter_is_row_grain(self, sales_engine):
        """Scenario: Literal source with an attached parameter is row grain —
        wsum(1, weight=…) = row count × region total, never a re-aggregation
        over an empty grain."""
        resp = await sales_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(
                formula="wsum(1, weight=sum(amount, partition_by=region))",
                name="w")]))
        _assert_region(_region_vals(resp, "sales.w"),
                       literal_source_wsum_by_region())


class TestCrossModelParameterOnLocalRoot:
    async def test_cross_model_attached_parameter_executes(self, orders_engine):
        """Scenario: Cross-model attached parameter on a local root — each order
        weighted by its customer's region SPEND total, attached per order row."""
        resp = await orders_engine.execute(orders_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula=_CROSS, name="w")]))
        by = status_key(resp)
        for status, expected in cross_model_param_by_status().items():
            assert float(by[(status,)]["orders.w"]) == pytest.approx(expected)

    async def test_unattributable_parameter_stays_rejected(self, weak_plans_engine):
        """Scenario: Undetermined attached parameter stays rejected — a
        partition key across the unproven customers→plans hop fails with the
        typed input-safety error, never the deleted attached-parameter remedy
        and never wrong values."""
        with pytest.raises((SlayerError, ValueError)) as ei:
            await weak_plans_engine.execute(orders_q(
                dimensions=["status"],
                measures=[ModelMeasure(
                    formula=("amount:weighted_avg(weight="
                             "sum(customers.spend, partition_by=customers.plans.level))"),
                    name="w")]))
        msg = str(ei.value)
        assert "DEV-" not in msg
        assert "plans" in msg or "level" in msg or "attribut" in msg.lower()


class TestUndeterminedParameterRejected:
    async def test_associate_parameter_not_entity_determined(self, orders_engine):
        """Scenario: Undetermined attached parameter stays rejected — an associate
        parameter grained by ``status`` (an order attribute the customer entity
        does not determine) fails with the typed determination error, never wrong
        values. Distinct from the cross-model input-safety residue above."""
        with pytest.raises(SlayerError) as ei:
            await orders_engine.execute(assoc_q(
                dimensions=["status"],
                measures=[ModelMeasure(
                    formula=("customers.spend:weighted_avg("
                             "weight=sum(amount, partition_by=status))"),
                    name="w")]))
        msg = str(ei.value)
        assert "DEV-" not in msg
        assert "determine" in msg.lower()


class TestFilterAndOrderPositions:
    _FILT = "wsum(amount, weight=sum(amount))"

    async def test_filter_only_prunes_with_values_unchanged(self, sales_engine):
        """Scenario: Attached parameter in filter and order positions — the
        filter types as a measure, pruning rows with survivors unchanged."""
        full = await sales_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="tot")]))
        pruned = await sales_engine.execute(sales_q(
            dimensions=["region"], filters=[f"{self._FILT} > 10000"],
            measures=[ModelMeasure(formula="amount:sum", name="tot")]))
        kept = {k[0] for k in region_key(pruned)}
        assert kept == {"South", "East"}  # 19600, 32400 > 10000
        full_tot = _region_vals(full, "sales.tot")
        for region, row in region_key(pruned).items():
            assert float(row["sales.tot"]) == pytest.approx(float(full_tot[region[0]]))

    async def test_order_by_projected_measure_name(self, sales_engine):
        """Scenario: ordered by the name of its projected measure."""
        resp = await sales_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=_ORDINARY, name="w")],
            order=[{"column": "w", "direction": "desc"}]))
        order = [r["sales.region"] for r in resp.data if r["sales.w"] is not None]
        by_val = ordinary_wavg_by_region()
        assert order == sorted(order, key=lambda reg: by_val[reg], reverse=True)

    async def test_order_by_raw_formula(self, sales_engine):
        """Scenario: appears only as a raw ORDER BY formula (no projection)."""
        resp = await sales_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="tot")],
            order=[{"column": _ORDINARY, "direction": "desc"}]))
        by_val = ordinary_wavg_by_region()
        present = [r["sales.region"] for r in resp.data
                   if by_val[r["sales.region"]] is not None]
        assert present == sorted(present, key=lambda reg: by_val[reg], reverse=True)


#: the SAME attached aggregate as source constituent AND parameter — one producer.
_DEDUP = ("weighted_avg(quantity * sum(amount, partition_by=region), "
          "weight=sum(amount, partition_by=region))")


class TestSqlHygiene:
    @pytest.mark.parametrize("formula", [
        _ORDINARY, _MIXED_PARAM, _DEDUP,
        "wsum(1, weight=sum(amount, partition_by=region))"])
    async def test_no_placeholder_leak_and_scope_closed(self, sales_engine, formula):
        resp = await sales_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=formula, name="w")]), dry_run=True)
        assert "__regroup__" not in resp.sql, resp.sql
        assert_scope_closed(resp.sql)

    async def test_adding_parameter_measure_is_cardinality_neutral(self, sales_engine):
        base = await sales_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="tot")]))
        withp = await sales_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="tot"),
                      ModelMeasure(formula=_ORDINARY, name="w")]))
        assert len(withp.data) == len(base.data)
        a = {k: v["sales.tot"] for k, v in region_key(base).items()}
        b = {k: v["sales.tot"] for k, v in region_key(withp).items()}
        assert len(a) == len(base.data)  # no collapsed duplicate region keys
        assert a == b
