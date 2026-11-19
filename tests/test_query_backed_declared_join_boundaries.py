"""Saved relation joins survive consumption without leaking into ordinary stages."""

from __future__ import annotations

import pytest

from slayer.core.errors import UnrelatedDatasetError
from slayer.core.models import DatasetRelationship

from tests._dev1902_fixtures import (
    AMOUNT_BY_CUSTOMER,
    _seed_duckdb,
    _seed_sqlite,
    cells,
    customers_dataset,
    orders_dataset,
    renamed_graph,
)
from tests._engine_helpers import seeded_exec_engine
from tests.test_dev1902_join_key_spelling import _customer_totals, _query
from slayer.engine.query_engine import SlayerQueryEngine


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    seed = _seed_duckdb if request.param == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(
        dialect=request.param, seed=seed, datasets=renamed_graph()
    ) as (query_engine, _db):
        yield query_engine


async def test_join_target_keeps_its_onward_relationship(engine: SlayerQueryEngine):
    await engine.save_dataset(_customer_totals())
    consumer = orders_dataset().model_copy(update={
        "name": "consumer",
        "relationships": [DatasetRelationship(to="customer_totals", from_columns=["customer_id"], to_columns=["customer_id"])],
    })
    await engine.save_dataset(consumer)

    response = await engine.execute(_query(
        population="consumer",
        dimensions=["customer_totals.customers.name"],
        metrics=[{"metric": "sum(amount)", "alias": "total"}],
    ))

    assert cells(response, dim_suffix="customers.name", metric="total") == AMOUNT_BY_CUSTOMER


async def test_saved_relation_without_outgoing_joins_can_use_incoming_edge(engine: SlayerQueryEngine):
    await engine.save_dataset(_customer_totals().model_copy(update={"relationships": []}))
    customers = customers_dataset()
    await engine.save_dataset(customers.model_copy(update={"relationships": [
        *customers.relationships,
        DatasetRelationship(to="customer_totals", from_columns=["id"], to_columns=["customer_id"], name="totals"),
    ]}))

    response = await engine.execute(_query(
        population="customer_totals",
        dimensions=["customers.name"],
        metrics=[{"metric": "sum(amt)", "alias": "total"}],
    ))

    assert cells(response, dim_suffix="name", metric="total") == AMOUNT_BY_CUSTOMER


async def test_extension_join_alias_resolves_to_the_named_stored_edge(engine: SlayerQueryEngine):
    totals = _customer_totals()
    await engine.save_dataset(totals.model_copy(update={
        "relationships": [totals.relationships[0].model_copy(update={"name": "owner"})],
    }))

    response = await engine.execute(_query(
        population="customer_totals", extensions=[{"dataset": "customer_totals", "relationships": [{
            "to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"], "name": "reviewer",
        }]}],
        dimensions=["owner.name", "reviewer.tier"],
        metrics=[{"metric": "sum(amt)", "alias": "total"}],
    ))

    assert {
        row["customer_totals.owner.name"]: (
            row["customer_totals.owner.tier"], row["total"]
        )
        for row in response.data
    } == {"Ann": ("gold", 30.0), "Bob": ("silver", 30.0), "Cid": ("gold", 90.0)}


async def test_ordinary_stage_does_not_inherit_backing_source_joins(engine):
    stage = _query(
        name="s", population="orders", dimensions=["customer_id"],
        metrics=[{"metric": "sum(amount)", "alias": "amt"}],
    )
    consumer = _query(
        population="s", dimensions=["customers.name"],
        metrics=[{"metric": "sum(amt)", "alias": "total"}],
    )

    with pytest.raises(UnrelatedDatasetError):
        await engine.execute([stage, consumer])


async def test_legal_stage_named_join_target_is_not_a_synthetic_self_join(engine: SlayerQueryEngine):
    await engine.save_dataset(customers_dataset().model_copy(update={"name": "_stage"}))
    totals = _customer_totals()
    await engine.save_dataset(totals.model_copy(update={
        "relationships": [totals.relationships[0].model_copy(update={"to": "_stage"})],
    }))

    unjoined = await engine.execute(_query(
        population="customer_totals",
        metrics=[{"metric": "sum(amt)", "alias": "total"}],
    ))
    assert unjoined.data == [{"total": 150.0}]

    joined = await engine.execute(_query(
        population="customer_totals", dimensions=["_stage.name"],
        metrics=[{"metric": "sum(amt)", "alias": "total"}],
    ))
    assert cells(joined, dim_suffix="_stage.name", metric="total") == AMOUNT_BY_CUSTOMER
