"""The dimension anchor, when inference fails closed, and datasource scoping (spec: queries/population ›
Dimension-determined default, Inference fails closed, Datasource scoping for root-less queries)."""

from __future__ import annotations

import pytest

from slayer.core.errors import PopulationErrorReason, PopulationInferenceError
from slayer.core.models import DatasetMetric

from tests._dev2212_fixtures import (
    BACKENDS, customers_dataset, orders_dataset, rows, run, seed_datasets, seed_engine, table, to_customers,
)
from tests._dev2277_fixtures import add_other_datasource, cycle_engine

R, REV, RET = "customers.region", "orders.total_revenue", "returns.total_returns"
CYCLE_DIMS = ["x.v", "y.v", "z.v"]


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


async def test_a_field_filter_is_not_a_determination_item(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], filter=["orders.status = 'completed'"])
    assert resp.population == "customers"
    assert table(resp, R) == rows(("EAST",))


async def test_a_measure_typed_filter_does_not_move_the_anchor(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], metrics=[REV], filter=["orders.total_revenue > 100"])
    assert resp.population == "customers"
    assert table(resp, R, REV) == rows(("EAST", 350), ("WEST", 300))


@pytest.mark.parametrize("metrics", [[], [REV], [REV, RET], ["tickets.ticket_count"], ["visits.visit_count"]])
async def test_the_anchor_is_invariant_under_measures(backend: str, metrics: list[str]) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], **({"metrics": metrics} if metrics else {}))
    assert resp.population == "customers"
    assert resp.population_inferred is True


async def test_unconnected_dimension_groups_take_a_product(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R, "tickets.priority"])
    assert table(resp, R, "tickets.priority") == rows(
        *((region, p) for region in ("EAST", "WEST", "NORTH") for p in ("high", "low")))
    assert resp.population == "customers × tickets"


async def test_a_directed_cycle_dimension_only_query_fails_closed(backend: str) -> None:
    async with cycle_engine(backend) as eng:
        with pytest.raises(PopulationInferenceError) as ei:
            await run(eng, dimensions=CYCLE_DIMS)
        assert ei.value.reason is PopulationErrorReason.TIE
        assert set(ei.value.candidates) == {"x", "y", "z"}
        anchored = await run(eng, population="x", dimensions=CYCLE_DIMS)
    assert table(anchored, "x.v", "y.v", "z.v") == rows(("x1", "y1", "z1"), ("x2", "y2", "z2"))


async def test_an_undefined_anchor_a_query_does_not_need(backend: str) -> None:
    async with cycle_engine(backend) as eng:
        resp = await run(eng, dimensions=CYCLE_DIMS, metrics=[{"metric": "count(x.*)", "alias": "n"}])
    assert table(resp, "x.v", "y.v", "z.v", "n") == rows(("x1", "y1", "z1", 1), ("x2", "y2", "z2", 1))
    assert resp.population is None
    assert resp.population_inferred is True


async def test_a_metric_with_no_full_grain_operand_needs_the_anchor(backend: str) -> None:
    async with cycle_engine(backend) as eng:
        with pytest.raises(PopulationInferenceError) as ei:
            await run(eng, dimensions=CYCLE_DIMS,
                      metrics=[{"metric": "sum(x.amount, partition_by=[x.v])", "alias": "p"}])
    assert ei.value.reason is PopulationErrorReason.TIE


async def test_a_measures_only_query_raises_no_empty_determination(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, metrics=[REV], filter=["sum(orders.amount) > 0"])
    assert table(resp, REV) == rows((650,))


# --------------------------------------------------------------------------- #
# Datasource scoping
# --------------------------------------------------------------------------- #
async def test_a_measures_only_query_scopes_by_its_metrics(backend: str) -> None:
    async with seed_engine(backend) as eng:
        await add_other_datasource(eng)
        resp = await run(eng, metrics=[REV])
    assert table(resp, REV) == rows((650,))


async def test_metric_homes_in_two_datasources_fail_closed(backend: str) -> None:
    async with seed_engine(backend) as eng:
        await add_other_datasource(eng)
        with pytest.raises(PopulationInferenceError) as ei:
            await run(eng, metrics=[REV, "count(events.*)"])
    assert ei.value.reason is PopulationErrorReason.NO_DATASOURCE
    assert set(ei.value.datasources) >= {"test", "other"}


async def test_a_dimensioned_query_scopes_by_its_metric_homes_too(backend: str) -> None:
    async with seed_engine(backend) as eng:
        await add_other_datasource(eng)
        with pytest.raises(PopulationInferenceError) as ei:
            await run(eng, dimensions=[R], metrics=["count(events.*)"])
    assert ei.value.reason is PopulationErrorReason.NO_DATASOURCE


async def test_a_row_level_saved_metric_dimension_determines_its_dataset(backend: str) -> None:
    base = orders_dataset()
    orders = base.model_copy(update={"metrics": [*base.metrics, DatasetMetric(name="amt2", expression="amount * 2")]})
    async with seed_engine(backend, datasets=seed_datasets(orders=orders)) as eng:
        resp = await run(eng, dimensions=[{"field": "orders.amt2", "alias": "a2"}])
    assert sorted(r["a2"] for r in resp.data) == [100, 200, 400, 600]
    assert resp.population == "orders"


async def test_recommendation_counts_an_unknown_cardinality_hop_as_inference_does() -> None:
    datasets = seed_datasets(customers=customers_dataset(keyed=False),
                             orders=orders_dataset(relationship=to_customers(cardinality=None)))
    async with seed_engine("sqlite", datasets=datasets) as eng:
        inferred = await run(eng, dimensions=["orders.status", R])
        rec = await eng.recommend_population(["orders.status", R])
    assert rec.reachable
    assert rec.population == inferred.population == "orders"
