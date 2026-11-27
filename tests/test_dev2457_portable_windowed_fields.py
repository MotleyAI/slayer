"""A portable or document windowed field is a column of its dataset, like a dialect-entry one.

Spec: models/field-expressions › A windowed field is a column of its dataset (scenarios "A portable windowed
field" and "A document's windowed field"). Tables and rows: ``tests/test_dev2432_windowed_columns.py``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.field_expression import ExpressionObject
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2210_fixtures import seed
from tests._dev2243_fixtures import dataset, document_text, field
from tests._engine_helpers import seeded_exec_engine
from tests.test_dev2432_windowed_columns import (
    DDL,
    RN,
    RN_OF_ID,
    _by,
    _by_id,
    _col,
    _grouped,
    _num,
    _orders_like,
    _raw,
    _run,
    _save,
    _to_customers,
)

POS = "ROW_NUMBER() OVER (ORDER BY sort_key)"
BY_RN = {1: 120, 2: 70, 3: 90}
BY_LABEL = {"first": 120, "second": 70, "third": 90}


def _portable(name: str, text: str, **kw: Any) -> DatasetField:
    return DatasetField(name=name, datatype=DataType.INTEGER, expression=ExpressionObject(sql=text), **kw)


def _to(target: str, source_key: str) -> DatasetRelationship:
    return DatasetRelationship(to=target, from_columns=[source_key], to_columns=["pos"],
                               cardinality=RelationshipCardinality.MANY_TO_ONE)


def _twin() -> list[SlayerDataset]:
    """The datasets reading ``rn``, every window a portable expression; join targets first."""
    return [
        SlayerDataset(name="customers", data_source="test", sql_table="customers",
                      fields=[_col("id", primary_key=True), _col("tier", DataType.STRING)]),
        SlayerDataset(name="slots", data_source="test", sql_table="slots",
                      fields=[_col("pos", primary_key=True), _col("label", DataType.STRING)]),
        SlayerDataset(name="slots_w", data_source="test", sql_table="slot_src",
                      fields=[_col("label", DataType.STRING), _col("sort_key"), _portable("pos", POS)]),
        _orders_like("orders", relationships=[_to_customers()], fields=[
            _portable("rn", RN),
            DatasetField(name="rn10", datatype=DataType.INTEGER, expression="orders.rn * 10"),
            _portable("ww", "SUM(orders.rn) OVER ()"),
            _portable("w3", "MAX(orders.ww) OVER (PARTITION BY region)"),
            _portable("rn_eu", RN, filter="region = 'EU'"),
        ]),
        _orders_like("big_orders", fields=[_portable("rn", RN)], filters=["amount >= 20"]),
        _orders_like("sql_orders", fields=[_portable("rn", RN)], sql_table=None, sql="SELECT * FROM orders"),
        _orders_like("orders_s", fields=[_portable("rn", RN)], relationships=[_to("slots", "rn")]),
        _orders_like("orders_wt", relationships=[_to("slots_w", "slot")]),
        _orders_like("orders_ws", fields=[_portable("rn", RN)], relationships=[_to("slots_w", "rn")]),
    ]


@pytest.fixture(params=["sqlite", "duckdb"])
async def bare(request: pytest.FixtureRequest) -> AsyncIterator[SlayerQueryEngine]:
    """The seeded tables with no dataset stored."""
    dialect = request.param
    async with seeded_exec_engine(dialect=dialect, seed=lambda p: seed(dialect, p, DDL), datasets=[]) as (eng, _db):
        yield eng


@pytest.fixture(params=["engine", "storage"])
def door(request: pytest.FixtureRequest) -> str:
    return request.param


async def _saved(engine: SlayerQueryEngine, door: str) -> SlayerQueryEngine:
    for twin in _twin():
        await _save(engine, twin, door=door)
    return engine


def _ordered(key: str) -> dict[str, Any]:
    return _grouped("orders", "orders.region", "sum(orders.amount)", order=[
        {"field": key, "direction": "desc"}, {"field": "orders.region", "direction": "asc"}])


class TestAPortableWindowedField:
    async def test_both_saves_succeed(self, bare, door):
        engine = await _saved(bare, door)
        for twin in _twin():
            stored = await engine.storage.get_dataset(twin.name, data_source="test")
            assert stored is not None
            # The stored form keeps no position between core and SLayer-only fields.
            assert sorted(f.name for f in stored.fields) == sorted(f.name for f in twin.fields)

    async def test_as_a_dimension(self, bare, door):
        engine = await _saved(bare, door)
        resp = await _run(engine, _grouped("orders", "orders.rn", "sum(orders.amount)"))
        assert _num(_by(resp, "orders.rn", "m")) == BY_RN

    async def test_as_an_aggregation_source(self, bare, door):
        engine = await _saved(bare, door)
        resp = await _run(engine, {"population": "orders", "metrics": [{"metric": "max(orders.rn)", "alias": "m"}]})
        assert [int(r["m"]) for r in resp.data] == [3]

    async def test_inside_an_aggregates_filter(self, bare, door):
        engine = await _saved(bare, door)
        resp = await _run(engine, {"population": "orders", "metrics": [
            {"metric": "sum(orders.amount) FILTER (WHERE orders.rn = 1)", "alias": "m"}]})
        assert [int(r["m"]) for r in resp.data] == [120]

    async def test_as_a_partition_key(self, bare, door):
        engine = await _saved(bare, door)
        resp = await _run(engine, {"population": "orders", "dimensions": ["orders.region", "orders.rn"],
                                   "metrics": [{"metric": "sum(orders.amount, partition_by=orders.rn)", "alias": "m"}]})
        assert len(resp.data) == 7
        assert {(int(r["orders.rn"]), int(r["m"])) for r in resp.data} == {(1, 120), (2, 70), (3, 90)}

    async def test_as_a_ranking_key(self, bare, door):
        engine = await _saved(bare, door)
        resp = await _run(engine, _grouped("orders", "orders.region", "last(orders.amount, orders.rn)"))
        assert _num(_by(resp, "orders.region", "m")) == {"EU": 30, "US": 60, "AS": 70}

    async def test_as_an_order_key_of_a_grouped_query(self, bare, door):
        engine = await _saved(bare, door)
        windowed = [r["orders.region"] for r in (await _run(engine, _ordered("orders.rn"))).data]
        stored = [r["orders.region"] for r in (await _run(engine, _ordered("orders.slot"))).data]
        assert windowed == stored == ["EU", "US", "AS"]

    async def test_as_a_query_filter(self, bare, door):
        engine = await _saved(bare, door)
        resp = await _run(engine, _grouped("orders", "orders.region", "sum(orders.amount)", filter=["orders.rn = 1"]))
        assert _num(_by(resp, "orders.region", "m")) == {"EU": 10, "US": 40, "AS": 70}

    async def test_raw_rows_under_a_query_filter_keep_their_values(self, bare, door):
        engine = await _saved(bare, door)
        resp = await _run(engine, _raw("orders", "id", "rn", filter=["orders.id > 1"],
                                       order=[{"field": "orders.rn", "direction": "asc"}]))
        assert _num(_by_id(resp, "orders", "orders.rn")) == {i: RN_OF_ID[i] for i in range(2, 8)}
        assert [int(r["orders.rn"]) for r in resp.data] == [1, 1, 2, 2, 3, 3]

    async def test_a_derived_field_over_a_windowed_field(self, bare, door):
        engine = await _saved(bare, door)
        resp = await _run(engine, _grouped("orders", "orders.rn10", "count(orders.*)"))
        assert _num(_by(resp, "orders.rn10", "m")) == {10: 3, 20: 2, 30: 2}

    async def test_a_window_over_a_windowed_field(self, bare, door):
        engine = await _saved(bare, door)
        raw = await _run(engine, _raw("orders", "id", "ww", "w3"))
        assert _num(_by_id(raw, "orders", "orders.ww")) == dict.fromkeys(range(1, 8), 13)
        assert _num(_by_id(raw, "orders", "orders.w3")) == dict.fromkeys(range(1, 8), 13)
        grouped = await _run(engine, _grouped("orders", "orders.ww", "count(orders.*)"))
        assert _num(_by(grouped, "orders.ww", "m")) == {13: 7}

    async def test_a_windowed_fields_filter_masks_its_value(self, bare, door):
        engine = await _saved(bare, door)
        total = await _run(engine, {"population": "orders", "metrics": [{"metric": "sum(orders.rn_eu)", "alias": "m"}]})
        assert [int(r["m"]) for r in total.data] == [6]
        grouped = await _run(engine, _grouped("orders", "orders.rn_eu", "count(orders.*)"))
        assert _num(_by(grouped, "orders.rn_eu", "m")) == {1: 1, 2: 1, 3: 1, None: 4}

    async def test_the_window_runs_over_the_datasets_filtered_rows(self, bare, door):
        engine = await _saved(bare, door)
        resp = await _run(engine, _grouped("big_orders", "big_orders.rn", "sum(big_orders.amount)"))
        assert _num(_by(resp, "big_orders.rn", "m")) == {1: 130, 2: 80, 3: 60}

    async def test_a_sql_backed_dataset(self, bare, door):
        engine = await _saved(bare, door)
        grouped = await _run(engine, _grouped("sql_orders", "sql_orders.rn", "sum(sql_orders.amount)"))
        assert _num(_by(grouped, "sql_orders.rn", "m")) == BY_RN
        filtered = await _run(engine, _grouped("sql_orders", "sql_orders.region", "sum(sql_orders.amount)",
                                               filter=["sql_orders.rn = 1"]))
        assert _num(_by(filtered, "sql_orders.region", "m")) == {"EU": 10, "US": 40, "AS": 70}
        raw = await _run(engine, _raw("sql_orders", "id", "rn", filter=["sql_orders.id > 1"]))
        assert _num(_by_id(raw, "sql_orders", "sql_orders.rn")) == {i: RN_OF_ID[i] for i in range(2, 8)}

    @pytest.mark.parametrize("source", ["orders_s", "orders_wt", "orders_ws"],
                             ids=["windowed-source", "windowed-target", "windowed-both"])
    async def test_a_windowed_relationship_key(self, bare, door, source):
        engine = await _saved(bare, door)
        target = "slots" if source == "orders_s" else "slots_w"
        resp = await _run(engine, _grouped(source, f"{source}.{target}.label", f"sum({source}.amount)"))
        assert _num(_by(resp, f"{source}.{target}.label", "m")) == BY_LABEL

    async def test_a_filter_across_a_fanning_hop_reads_the_computed_key(self, bare, door):
        engine = await _saved(bare, door)
        resp = await _run(engine, _grouped("slots", "slots.label", "count(slots.*)", filter=["orders_s.amount >= 60"]))
        assert _num(_by(resp, "slots.label", "m")) == {"first": 1, "third": 1}


def _document(rn: str = RN) -> str:
    """Document ``m_orders``: dataset ``orders`` whose field ``rn`` is the ``OSSIE_SQL_2026`` entry ``rn``."""
    windowed = {"name": "rn", "datatype": "Integer",
                "expression": {"dialects": [{"dialect": "OSSIE_SQL_2026", "expression": rn}]}}
    return document_text(name="m_orders", datasets=[dataset("orders", primary_key=["id"], fields=[
        field("id", datatype="Integer"), field("region"), field("amount", datatype="Integer"),
        field("ordered_at", datatype="Date"), windowed])])


class TestADocumentsWindowedField:
    async def test_the_document_loads_with_no_warning_on_rn(self, bare):
        await bare.storage.save_semantic_model_text(data_source="test", name="m_orders", text=_document())
        orders = await bare.storage.get_dataset("orders", data_source="test")
        assert orders is not None
        assert {f.name: f.load_warning for f in orders.fields} == dict.fromkeys(
            ["id", "region", "amount", "ordered_at", "rn"])

    async def test_the_grouped_query_returns_the_computed_values(self, bare):
        await bare.storage.save_semantic_model_text(data_source="test", name="m_orders", text=_document())
        resp = await _run(bare, _grouped("orders", "orders.rn", "sum(orders.amount)"))
        assert _num(_by(resp, "orders.rn", "m")) == BY_RN
