"""DEV-1892 task 1.3 — plan-structure pins for the lifted parameter: the kernel
carries ``picked_params``; a weight aggregate's partition keys need not be query
dimensions (the combined-consumer exemption extends to args/kwargs); the operand
carrier is one producer; the emitted SQL is scope-closed; adding the weighted
measure is cardinality-neutral.

Spec: queries/partitioned-aggregates — "Parameter partition keys need not be
query dimensions"; queries/semantics — "Aggregation parameters are typed by the
home dataset's grain".
"""

from __future__ import annotations

import re

import pytest

from slayer.engine.plan import plan_query
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1841_fixtures import assoc_q, dev1840_models
from tests._dev1847_fixtures import (
    INNER_CR,
    INNER_CRP,
    ModelMeasure,
    dev1847_models,
    gen,
    make_exec_engine,
    region_key,
    sales_q,
)
from tests._dev1892_fixtures import weighted_sales_models

_WEIGHT_COUNT = "weight=count(id, partition_by=[city, region])"
_WAVG_REAGG = f"weighted_avg({INNER_CR}, {_WEIGHT_COUNT})"


def _sales_bundle() -> ResolvedSourceBundle:
    models = dev1847_models()
    return ResolvedSourceBundle(source_model=models[0], referenced_models=models[1:])


def _orders_bundle() -> ResolvedSourceBundle:
    models = dev1840_models()
    return ResolvedSourceBundle(source_model=models[0], referenced_models=models[1:])


def _association_attaches(planned) -> list:
    return [a for a in planned.regroup_attach_plans
            if getattr(a.kernel, "kind", None) == "association"]


@pytest.fixture(params=["sqlite", "duckdb"])
async def reagg_engine(request):
    async for engine in make_exec_engine(request, models=weighted_sales_models()):
        yield engine


class TestKernelCarriesPickedParams:
    def test_reaggregation_kernel_picks_the_weight(self) -> None:
        planned = plan_query(
            query=sales_q(dimensions=["region"],
                          measures=[ModelMeasure(formula=_WAVG_REAGG, name="w")]),
            bundle=_sales_bundle())
        [attach] = _association_attaches(planned)
        names = [p.name for p in attach.kernel.picked_params]
        assert "weight" in names

    def test_association_kernel_picks_the_weight(self) -> None:
        planned = plan_query(
            query=assoc_q(dimensions=["status"], measures=[ModelMeasure(
                formula="customers.spend:weighted_avg(weight=customers.spend)",
                name="w")]),
            bundle=_orders_bundle())
        [attach] = _association_attaches(planned)
        names = [p.name for p in attach.kernel.picked_params]
        assert "weight" in names


class TestCombinedConsumerExemption:
    def test_weight_partition_keys_need_not_be_query_dims(self) -> None:
        """The weight ``count(id, partition_by=[city, region])`` carries ``city``,
        which is not a query dimension; planning must not raise the
        combined-consumer partition-key error (the exemption covers kwargs)."""
        planned = plan_query(
            query=sales_q(dimensions=["region"],
                          measures=[ModelMeasure(formula=_WAVG_REAGG, name="w")]),
            bundle=_sales_bundle())
        assert _association_attaches(planned)

    def test_depth2_reaggregation_with_parameter_plans(self) -> None:
        """A re-aggregation nested inside another, carrying a weight, plans."""
        formula = (
            f"avg(weighted_avg({INNER_CRP}, "
            "weight=count(id, partition_by=[city, region, product]), "
            "partition_by=[region, product]))")
        planned = plan_query(
            query=sales_q(dimensions=["product"],
                          measures=[ModelMeasure(formula=formula, name="d")]),
            bundle=_sales_bundle())
        assert _association_attaches(planned)


class TestEmittedShape:
    async def test_operand_carrier_is_one_producer_and_scope_closed(self) -> None:
        sql = await gen(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=_WAVG_REAGG, name="w")]))
        # value (amount:sum) and weight (id:count) ride ONE operand carrier.
        ctes = re.findall(r"(_cm_\w+) AS \(", sql)
        carriers = [n for n in ctes if "weighted_avg" not in n]
        assert carriers == ["_cm_amount_sum_partition_by_city_region"], ctes
        assert_scope_closed(sql)

    @pytest.mark.parametrize("dialect", ["sqlite", "duckdb", "postgres", "bigquery"])
    async def test_no_placeholder_leak(self, dialect) -> None:
        sql = await gen(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=_WAVG_REAGG, name="w")]), dialect=dialect)
        assert "__regroup__" not in sql, sql


class TestCardinalityInvariant:
    async def test_adding_weighted_measure_keeps_rows_and_siblings(
        self, reagg_engine,
    ) -> None:
        base = await reagg_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="tot")]))
        withw = await reagg_engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="tot"),
                      ModelMeasure(formula=_WAVG_REAGG, name="w")]))
        a = {k: v["sales.tot"] for k, v in region_key(base).items()}
        b = {k: v["sales.tot"] for k, v in region_key(withw).items()}
        assert set(a) == set(b)
        for k in a:
            assert a[k] == b[k]
