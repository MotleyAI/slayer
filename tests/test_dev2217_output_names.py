"""Result columns carry Ossie output names under the strict binding; native keys stay under the default. A user alias
may equal a field name under every conformance (Ruling 1).

Spec: queries/ossie-l3-binding › Result columns carry Ossie output names; queries/computed-dimensions › A named
computed dimension keys by its name.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, DatasourceConfig, SlayerDataset
from slayer.engine.ossie_l3 import _relabelled
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.engine.response_meta import ResponseAttributes
from tests._dev2217_fixtures import DS, NATIVE, STRICT, fixture_engine, rows, run_as, sql_engine, strict, strict_error
from tests._dev2243_fixtures import dataset, document_text, field, relationship

IDS_SQL = """
CREATE TABLE customers (id VARCHAR PRIMARY KEY, region VARCHAR);
INSERT INTO customers VALUES ('C1', 'EAST'), ('C2', 'WEST');
CREATE TABLE orders (id VARCHAR PRIMARY KEY, customer_id VARCHAR, unit_price INTEGER);
INSERT INTO orders VALUES ('O1', 'C1', 10), ('O2', 'C2', 20);
"""
IDS = document_text(name="ids", datasets=[
    dataset("customers", primary_key=["id"], fields=[field("id"), field("region")]),
    dataset("orders", primary_key=["id"], fields=[field("id"), field("customer_id")]),
], relationships=[relationship("orders_to_customers", src="orders", to="customers", from_columns=["customer_id"],
                               to_columns=["id"])])
LABELLED = [
    SlayerDataset(name="customers", sql_table="customers", data_source=DS, fields=[
        DatasetField(name="id", datatype=DataType.STRING, primary_key=True),
        DatasetField(name="region", datatype=DataType.STRING, label="Region")]),
    SlayerDataset(name="orders", sql_table="orders", data_source=DS, fields=[
        DatasetField(name="id", datatype=DataType.STRING, primary_key=True),
        DatasetField(name="customer_id", datatype=DataType.STRING)],
        metrics=[DatasetMetric(name="order_count", expression="count(*)")],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])]),
]
DOTTED = document_text(name="dotted", datasets=[
    dataset("orders", primary_key=["id"], fields=[field("id"), field("unit.price", "unit_price", datatype="Integer")]),
])


def _no_database(_self: SlayerQueryEngine, _datasource: DatasourceConfig) -> Any:
    raise AssertionError("SQL executed")


async def test_aliases_name_the_columns(tmp_path: Path) -> None:
    """Aliases name the columns."""
    async with fixture_engine(tmp_path) as engine:
        response = await strict(engine, {"dimensions": [{"field": "customers.region", "alias": "area"}],
                                         "metrics": [{"metric": "orders.total_revenue", "alias": "revenue"}]})
    assert response.columns == ["area", "revenue"]
    assert rows(response) == [("EAST", 350.0), ("WEST", 300.0)]


async def test_last_segments_collide(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Last segments collide."""
    query = {"dimensions": ["orders.id", "customers.id"]}
    async with sql_engine(tmp_path, sql=IDS_SQL, documents=[IDS]) as engine:
        assert (await engine.execute(query)).columns == ["orders.id", "customers.id"]
        with monkeypatch.context() as patch:
            patch.setattr(SlayerQueryEngine, "client_for", _no_database)
            await strict_error(engine, query, "E_NAME_COLLISION")


async def test_one_unaliased_reference_twice_collides(tmp_path: Path) -> None:
    async with fixture_engine(tmp_path) as engine:
        await strict_error(engine, {"dimensions": ["customers.region", "customers.region"]}, "E_NAME_COLLISION")


@pytest.mark.parametrize(("first", "second"), [("foo", '"FOO"'), ("foo", "FOO"), ('"foo"', '"foo"')])
async def test_normalization_decides_a_collision(tmp_path: Path, first: str, second: str) -> None:
    """Normalization decides a collision: names equal after normalization collide."""
    async with fixture_engine(tmp_path) as engine:
        await strict_error(engine, {"metrics": [{"metric": "orders.total_revenue", "alias": first},
                                                {"metric": "returns.total_returns", "alias": second}]},
                           "E_NAME_COLLISION")


async def test_distinct_names_never_share_a_label(tmp_path: Path) -> None:
    """Normalization decides a collision: ``foo`` and ``"foo"`` answer, labelled ``FOO`` and ``foo``."""
    async with fixture_engine(tmp_path) as engine:
        response = await strict(engine, {"metrics": [{"metric": "orders.total_revenue", "alias": "foo"},
                                                     {"metric": "returns.total_returns", "alias": '"foo"'}]})
    assert response.columns == ["FOO", "foo"]
    assert response.data == [{"FOO": 650, "foo": 35}]


async def test_a_quoted_alias_loses_its_quotes(tmp_path: Path) -> None:
    async with fixture_engine(tmp_path) as engine:
        response = await strict(engine, {"metrics": [{"metric": "orders.total_revenue", "alias": '"Revenue"'}]})
    assert response.columns == ["Revenue"]


@pytest.mark.xfail(strict=True, reason="DEV-2244: a field name may not contain a dot yet")
async def test_a_quoted_dotted_field_keeps_its_whole_name(tmp_path: Path) -> None:
    sql = "CREATE TABLE orders (id VARCHAR PRIMARY KEY, unit_price INTEGER); INSERT INTO orders VALUES ('O1', 10);"
    async with sql_engine(tmp_path, sql=sql, documents=[DOTTED]) as engine:
        response = await strict(engine, {"dimensions": ['orders."unit.price"']})
    assert response.columns == ["unit.price"]
    assert response.data == [{"unit.price": 10}]


async def test_dimensions_first_then_metrics_in_query_order(tmp_path: Path) -> None:
    query = {"metrics": ["returns.total_returns", "orders.total_revenue"], "dimensions": ["customers.region"]}
    async with fixture_engine(tmp_path) as engine:
        response = await strict(engine, query)
        native = await engine.execute(query)
    assert response.columns == ["region", "total_returns", "total_revenue"]
    assert [list(row) for row in response.data] == [response.columns] * len(response.data)
    assert rows(response) == [("EAST", 25.0, 350.0), ("NORTH", 10.0, None), ("WEST", None, 300.0)]
    assert native.columns == ["customers.region", "returns.total_returns", "orders.total_revenue"]


async def test_dry_run_columns_carry_output_names(tmp_path: Path) -> None:
    async with fixture_engine(tmp_path) as engine:
        response = await strict(engine, {"dimensions": ["customers.region"], "metrics": ["orders.total_revenue"]},
                                dry_run=True)
    assert response.columns == ["region", "total_revenue"]


async def test_attribute_metadata_is_keyed_by_output_names(tmp_path: Path) -> None:
    async with sql_engine(tmp_path, sql=IDS_SQL, datasets=LABELLED) as engine:
        query = {"dimensions": ["customers.region"], "metrics": ["orders.order_count"]}
        native = await engine.execute(query)
        response = await strict(engine, query)
    assert (set(native.attributes.dimensions), set(native.attributes.metrics)) == (
        {"customers.region"}, {"orders.order_count"})
    assert (set(response.attributes.dimensions), set(response.attributes.metrics)) == ({"region"}, {"order_count"})
    assert response.attributes.dimensions["region"].label == "Region"
    assert response.attributes.metrics["order_count"] == native.attributes.metrics["orders.order_count"]


# --------------------------------------------------------------------------- #
# Ruling 1: an alias may equal a field name, under every conformance
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("conformance", [NATIVE, STRICT])
async def test_a_dimension_alias_equal_to_a_field_name(tmp_path: Path, conformance: str) -> None:
    query = {"dimensions": [{"field": "customers.region", "alias": "amount"}], "metrics": ["orders.total_revenue"]}
    async with fixture_engine(tmp_path) as engine:
        response = await run_as(engine, query, conformance)
    revenue = "total_revenue" if conformance == STRICT else "orders.total_revenue"
    assert response.columns == ["amount", revenue]
    assert rows(response) == [("EAST", 350.0), ("WEST", 300.0)]


@pytest.mark.parametrize("conformance", [NATIVE, STRICT])
async def test_a_metric_alias_equal_to_a_field_name(tmp_path: Path, conformance: str) -> None:
    query = {"dimensions": ["orders.order_id"], "metrics": [{"metric": "orders.total_revenue", "alias": "amount"}]}
    async with fixture_engine(tmp_path) as engine:
        response = await run_as(engine, query, conformance)
    order_id = "order_id" if conformance == STRICT else "orders.order_id"
    assert response.columns == [order_id, "amount"]
    assert rows(response) == [("O1", 100.0), ("O2", 200.0), ("O3", 50.0), ("O4", 300.0)]


async def test_a_metric_alias_equal_to_a_host_field_under_a_population(tmp_path: Path) -> None:
    async with fixture_engine(tmp_path) as engine:
        response = await engine.execute({"population": "orders", "dimensions": ["orders.order_id"],
                                         "metrics": [{"metric": "sum(orders.amount)", "alias": "amount"}]})
    assert response.columns == ["orders.order_id", "amount"]
    assert rows(response) == [("O1", 100.0), ("O2", 200.0), ("O3", 50.0), ("O4", 300.0)]


def test_labels_follow_the_projection_not_a_rows_key_order() -> None:
    """Each value goes to its column's label, whatever order a row's keys come in."""
    response = SlayerResponse(data=[{"orders.amount": 2, "customers.region": "EAST"}],
                              columns=["customers.region", "orders.amount"], attributes=ResponseAttributes())
    relabelled = _relabelled(response, ["region", "amount"])
    assert relabelled.columns == ["region", "amount"]
    assert relabelled.data == [{"amount": 2, "region": "EAST"}]


ORDER_SQL = """
CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER);
INSERT INTO orders SELECT i, 10 FROM range(100) t(i);
INSERT INTO orders VALUES (1000, 20);
"""
ORDERS_ONLY = SlayerDataset(name="orders", sql_table="orders", data_source=DS, fields=[
    DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
    DatasetField(name="amount", datatype=DataType.INTEGER)])


@pytest.mark.parametrize(("key", "expected"), [
    pytest.param("amount", [{"orders.amount": 10, "amount": 100}], id="bare-is-the-alias"),
    pytest.param("orders.amount", [{"orders.amount": 20, "amount": 1}], id="qualified-is-the-field"),
])
async def test_an_order_key_beside_an_alias_equal_to_a_field_name(tmp_path: Path, key: str,
                                                                  expected: list[dict[str, Any]]) -> None:
    """With an alias equal to a field name, a bare order key is the alias and a qualified one the field."""
    async with sql_engine(tmp_path, sql=ORDER_SQL, datasets=[ORDERS_ONLY]) as engine:
        response = await engine.execute({
            "dimensions": ["orders.amount"], "metrics": [{"metric": "count(orders.*)", "alias": "amount"}],
            "order": [{"field": key, "direction": "desc"}], "limit": 1})
    assert response.data == expected
