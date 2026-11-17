"""Row-level security scopes every table inside a filtered dataset, and the query reports every model it reads."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.policy import FieldFilterRuleset, RelationshipFilterRule, RelationshipFilterRuleset, SessionPolicy
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2210_fixtures import by_id, ids, seed
from tests._engine_helpers import seeded_exec_engine

DDL = (
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, tenant VARCHAR(5))",
    "INSERT INTO orders VALUES (1, 't1'), (2, 't1'), (3, 't2'), (4, 't2'), (5, 't1')",
    # Shipment 21 (of t1's order 2) belongs to t2: only a scoped ``shipments`` hides it.
    "CREATE TABLE shipments (id INTEGER PRIMARY KEY, order_ref INTEGER, carrier VARCHAR(5), tenant VARCHAR(5))",
    "INSERT INTO shipments VALUES (20, 1, 'ups', 't1'), (21, 2, 'ups', 't2'), (22, 3, 'ups', 't2')",
    "CREATE TABLE returns (id INTEGER PRIMARY KEY, order_id INTEGER, tenant VARCHAR(5))",
    "INSERT INTO returns VALUES (10, 1, 't1'), (11, 2, 't1'), (12, 3, 't2'), (14, 5, 't1')",
)

COLUMN_POLICY = SessionPolicy(ruleset=FieldFilterRuleset(field="tenant", value="t1"))
JOIN_POLICY = SessionPolicy(ruleset=RelationshipFilterRuleset(
    table="orders", field="tenant", value="t1",
    relationships=(
        RelationshipFilterRule(target_table="shipments", relationship_path=("shipments.order_ref = orders.id",)),
        RelationshipFilterRule(target_table="returns", relationship_path=("returns.order_id = orders.id",)),
    ),
))


def _id() -> DatasetField:
    return DatasetField(name="id", datatype=DataType.INT, primary_key=True)


def _datasets() -> list[SlayerDataset]:
    return [
        SlayerDataset(name="shipments", data_source="test", sql_table="shipments", fields=[
            _id(), DatasetField(name="order_ref", datatype=DataType.INT), DatasetField(name="carrier", datatype=DataType.TEXT),
        ]),
        SlayerDataset(name="ups_orders", data_source="test", sql_table="orders", fields=[_id()],
                    relationships=[DatasetRelationship(to="shipments", join_pairs=[["id", "order_ref"]])],
                    filters=["shipments.carrier = 'ups'"]),
        SlayerDataset(name="returns", data_source="test", sql_table="returns",
                    fields=[_id(), DatasetField(name="order_id", datatype=DataType.INT)],
                    relationships=[DatasetRelationship(to="ups_orders", join_pairs=[["order_id", "id"]])]),
    ]


@pytest.fixture
async def base() -> AsyncIterator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect="sqlite", seed=lambda p: seed("sqlite", p, DDL), datasets=_datasets(),
    ) as (eng, _db):
        yield eng


def _scoped(base: SlayerQueryEngine, policy: SessionPolicy) -> SlayerQueryEngine:
    return SlayerQueryEngine(storage=base.storage, policy=policy)


async def _execute(engine: SlayerQueryEngine, body: dict[str, Any]):
    try:
        return await engine.execute(SlayerQuery.model_validate(body))
    finally:
        engine.close()


class TestColumnRuleset:
    async def test_scopes_the_table_only_a_filter_reads(self, base):
        resp = await _execute(_scoped(base, COLUMN_POLICY), {"population": "ups_orders", "dimensions": ["id"]})
        assert ids(resp, "ups_orders") == {1}

    async def test_scopes_a_filtered_join_target(self, base):
        resp = await _execute(_scoped(base, COLUMN_POLICY), {"population": "returns", "dimensions": ["id", "ups_orders.id"]})
        assert by_id(resp, "returns", "ups_orders.id") == {10: 1, 11: None, 14: None}


class TestJoinRuleset:
    async def test_scopes_the_filtered_root(self, base):
        resp = await _execute(_scoped(base, JOIN_POLICY), {"population": "ups_orders", "dimensions": ["id"]})
        assert ids(resp, "ups_orders") == {1, 2}

    async def test_scopes_a_filtered_join_target(self, base):
        resp = await _execute(_scoped(base, JOIN_POLICY), {"population": "returns", "dimensions": ["id", "ups_orders.id"]})
        assert by_id(resp, "returns", "ups_orders.id") == {10: 1, 11: 2, 14: None}


@pytest.mark.parametrize(("body", "expected"), [
    ({"population": "ups_orders", "dimensions": ["id"]}, {"ups_orders", "shipments"}),
    ({"population": "returns", "dimensions": ["id", "ups_orders.id"]}, {"returns", "ups_orders", "shipments"}),
], ids=["filtered-root", "filtered-join-target"])
async def test_query_reports_every_model_read(base, body, expected):
    main, named, ds, chain = await base._normalize_input(body, runtime_kwarg={}, prefer_data_source=None)
    prepared = await base._prepare_pipeline(
        query=main, named_queries=named, runtime_kwarg={}, prefer_data_source=ds, splice_chain=chain,
    )
    assert set(prepared.touched) == expected
