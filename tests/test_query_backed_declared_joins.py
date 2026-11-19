"""Saved query-backed models retain their declared relationship graph."""

from __future__ import annotations

import pytest

from slayer.core.errors import UnresolvableDimensionRelationshipError

from tests._dev1902_fixtures import (
    AMOUNT_BY_CUSTOMER,
    cells,
    make_exec_engine,
    orders_dataset,
    renamed_graph,
)
from tests.test_dev1902_join_key_spelling import _customer_totals, _query


@pytest.fixture(params=["sqlite", "duckdb"])
async def saved_totals_engine(request):
    async for engine in make_exec_engine(request, datasets=renamed_graph()):
        await engine.save_dataset(_customer_totals())
        yield engine


async def test_saved_query_backed_join_filters_population(saved_totals_engine):
    response = await saved_totals_engine.execute(_query(
        population="customer_totals",
        metrics=[{"metric": "sum(customer_totals.amt)", "alias": "total"}],
        filter=["customers.tier = 'gold'"],
    ))
    assert response.data == [{"total": 120.0}]


async def test_saved_query_backed_join_traverses_multihop_for_aggregate(saved_totals_engine):
    response = await saved_totals_engine.execute(_query(
        population="customer_totals",
        dimensions=["customers.regions.name"],
        metrics=[{"metric": "sum(customers.credit)", "alias": "credit"}],
    ))
    assert cells(response, dim_suffix="regions.name", metric="credit") == {
        "North": 300.0,
        "South": 300.0,
    }


async def test_repeating_saved_join_in_extension_is_still_usable(saved_totals_engine):
    model = _customer_totals()
    response = await saved_totals_engine.execute(_query(
        population="customer_totals",
        extensions=[{
            "dataset": "customer_totals",
            "relationships": [join.model_dump() for join in model.relationships],
        }],
        dimensions=["customers.name"],
        metrics=[{"metric": "sum(customer_totals.amt)", "alias": "total"}],
    ))
    assert cells(response, dim_suffix="customers.name", metric="total") == {
        "Ann": 30.0,
        "Bob": 30.0,
        "Cid": 90.0,
    }


async def test_repeating_stored_join_in_table_model_extension_is_that_edge(saved_totals_engine):
    response = await saved_totals_engine.execute(_query(
        population="orders",
        extensions=[{"dataset": "orders", "relationships": [
            orders_dataset().relationships[0].model_copy(update={"description": "repeated"}).model_dump()]}],
        dimensions=["customers.name"],
        metrics=[{"metric": "sum(orders.amount)", "alias": "total"}],
    ))
    assert cells(response, dim_suffix="customers.name", metric="total") == AMOUNT_BY_CUSTOMER


async def test_extension_join_differing_from_stored_one_stays_a_parallel_edge(saved_totals_engine):
    query = _query(
        population="orders",
        extensions=[{"dataset": "orders", "relationships": [
            {"to": "customers", "from_columns": ["id"], "to_columns": ["id"]}]}],
        dimensions=["customers.name"],
        metrics=[{"metric": "sum(orders.amount)", "alias": "total"}],
    )
    with pytest.raises(UnresolvableDimensionRelationshipError):
        await saved_totals_engine.execute(query)
