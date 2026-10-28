"""Saved relation joins survive consumption without leaking into ordinary stages."""

from __future__ import annotations

import pytest

from slayer.core.errors import UnrelatedDatasetError
from slayer.core.models import ModelJoin

from tests._dev1902_fixtures import (
    AMOUNT_BY_CUSTOMER,
    _seed_duckdb,
    _seed_sqlite,
    cells,
    customers_model,
    orders_model,
    renamed_graph,
)
from tests._engine_helpers import seeded_exec_engine
from tests.test_dev1902_join_key_spelling import _customer_totals, _query


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    seed = _seed_duckdb if request.param == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(
        dialect=request.param, seed=seed, models=renamed_graph()
    ) as (query_engine, _db):
        yield query_engine


async def test_join_target_keeps_its_onward_relationship(engine):
    await engine.save_model(_customer_totals())
    consumer = orders_model().model_copy(update={
        "name": "consumer",
        "joins": [ModelJoin(target_model="customer_totals", join_pairs=[["customer_id", "customer_id"]])],
    })
    await engine.save_model(consumer)

    response = await engine.execute(_query(
        source_model="consumer",
        dimensions=["customer_totals.customers.name"],
        measures=[{"formula": "sum(amount)", "name": "total"}],
    ))

    assert cells(response, dim_suffix="customers.name", measure="total") == AMOUNT_BY_CUSTOMER


async def test_saved_relation_without_outgoing_joins_can_use_incoming_edge(engine):
    await engine.save_model(_customer_totals().model_copy(update={"joins": []}))
    customers = customers_model()
    await engine.save_model(customers.model_copy(update={"joins": [
        *customers.joins,
        ModelJoin(target_model="customer_totals", join_pairs=[["id", "customer_id"]], name="totals"),
    ]}))

    response = await engine.execute(_query(
        source_model="customer_totals",
        dimensions=["customers.name"],
        measures=[{"formula": "sum(amt)", "name": "total"}],
    ))

    assert cells(response, dim_suffix="name", measure="total") == AMOUNT_BY_CUSTOMER


async def test_extension_keeps_distinct_named_stored_and_added_joins(engine):
    totals = _customer_totals()
    await engine.save_model(totals.model_copy(update={
        "joins": [totals.joins[0].model_copy(update={"name": "owner"})],
    }))

    response = await engine.execute(_query(
        source_model={"source_name": "customer_totals", "joins": [{
            "target_model": "customers", "join_pairs": [["customer_id", "id"]], "name": "reviewer",
        }]},
        dimensions=["owner.name", "reviewer.tier"],
        measures=[{"formula": "sum(amt)", "name": "total"}],
    ))

    assert {
        row["customer_totals.owner.name"]: (
            row["customer_totals.reviewer.tier"], row["customer_totals.total"]
        )
        for row in response.data
    } == {"Ann": ("gold", 30.0), "Bob": ("silver", 30.0), "Cid": ("gold", 90.0)}


async def test_ordinary_stage_does_not_inherit_backing_source_joins(engine):
    stage = _query(
        name="s", source_model="orders", dimensions=["customer_id"],
        measures=[{"formula": "sum(amount)", "name": "amt"}],
    )
    consumer = _query(
        source_model="s", dimensions=["customers.name"],
        measures=[{"formula": "sum(amt)", "name": "total"}],
    )

    with pytest.raises(UnrelatedDatasetError):
        await engine.execute([stage, consumer])


async def test_legal_stage_named_join_target_is_not_a_synthetic_self_join(engine):
    await engine.save_model(customers_model().model_copy(update={"name": "_stage"}))
    totals = _customer_totals()
    await engine.save_model(totals.model_copy(update={
        "joins": [totals.joins[0].model_copy(update={"target_model": "_stage"})],
    }))

    unjoined = await engine.execute(_query(
        source_model="customer_totals",
        measures=[{"formula": "sum(amt)", "name": "total"}],
    ))
    assert unjoined.data == [{"customer_totals.total": 150.0}]

    joined = await engine.execute(_query(
        source_model="customer_totals", dimensions=["_stage.name"],
        measures=[{"formula": "sum(amt)", "name": "total"}],
    ))
    assert cells(joined, dim_suffix="_stage.name", measure="total") == AMOUNT_BY_CUSTOMER
