"""Saved query-backed models retain their declared relationship graph."""

from __future__ import annotations

import pytest

from slayer.core.errors import AmbiguousJoinPathError

from tests._dev1902_fixtures import (
    AMOUNT_BY_CUSTOMER,
    cells,
    make_exec_engine,
    orders_model,
    renamed_graph,
)
from tests.test_dev1902_join_key_spelling import _customer_totals, _query


@pytest.fixture(params=["sqlite", "duckdb"])
async def saved_totals_engine(request):
    async for engine in make_exec_engine(request, models=renamed_graph()):
        await engine.save_model(_customer_totals())
        yield engine


async def test_saved_query_backed_join_filters_population(saved_totals_engine):
    response = await saved_totals_engine.execute(_query(
        source_model="customer_totals",
        measures=[{"formula": "sum(amt)", "name": "total"}],
        filters=["customers.tier = 'gold'"],
    ))
    assert response.data == [{"customer_totals.total": 120.0}]


async def test_saved_query_backed_join_traverses_multihop_for_aggregate(saved_totals_engine):
    response = await saved_totals_engine.execute(_query(
        source_model="customer_totals",
        dimensions=["customers.regions.name"],
        measures=[{"formula": "sum(customers.credit)", "name": "credit"}],
    ))
    assert cells(response, dim_suffix="regions.name", measure="credit") == {
        "North": 300.0,
        "South": 300.0,
    }


async def test_repeating_saved_join_in_extension_is_still_usable(saved_totals_engine):
    model = _customer_totals()
    response = await saved_totals_engine.execute(_query(
        source_model={
            "source_name": "customer_totals",
            "joins": [join.model_dump() for join in model.joins],
        },
        dimensions=["customers.name"],
        measures=[{"formula": "sum(amt)", "name": "total"}],
    ))
    assert cells(response, dim_suffix="customers.name", measure="total") == {
        "Ann": 30.0,
        "Bob": 30.0,
        "Cid": 90.0,
    }


async def test_repeating_stored_join_in_table_model_extension_is_that_edge(saved_totals_engine):
    response = await saved_totals_engine.execute(_query(
        source_model={"source_name": "orders", "joins": [
            orders_model().joins[0].model_copy(update={"description": "repeated"}).model_dump()]},
        dimensions=["customers.name"],
        measures=[{"formula": "sum(amount)", "name": "total"}],
    ))
    assert cells(response, dim_suffix="customers.name", measure="total") == AMOUNT_BY_CUSTOMER


async def test_extension_join_differing_from_stored_one_stays_a_parallel_edge(saved_totals_engine):
    query = _query(
        source_model={"source_name": "orders", "joins": [
            {"target_model": "customers", "join_pairs": [["id", "id"]]}]},
        dimensions=["customers.name"],
        measures=[{"formula": "sum(amount)", "name": "total"}],
    )
    with pytest.raises(AmbiguousJoinPathError):
        await saved_totals_engine.execute(query)
