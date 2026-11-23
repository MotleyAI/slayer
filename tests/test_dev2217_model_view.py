"""The strict binding reads the datasource as an Ossie model: Ossie steps, paths, SLayer-only content honoured.

Spec: queries/ossie-l3-binding › The strict binding reads the model as an Ossie model.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest
import yaml

from slayer.core import slayer_extension as ext
from slayer.core.enums import DataType, JoinType, RelationshipCardinality
from slayer.core.errors import UnresolvableDimensionRelationshipError
from slayer.engine.ossie_l3 import _Model, _Step
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, SlayerDataset
from tests._dev2217_fixtures import DS, rows, sql_engine, strict, strict_error
from tests._dev2243_fixtures import dataset, document_text, field, metric, relationship

SQL = """
CREATE TABLE customers (customer_id VARCHAR PRIMARY KEY, region VARCHAR);
INSERT INTO customers VALUES ('C1', 'EAST'), ('C2', 'EAST'), ('C3', 'WEST'), ('C4', 'NORTH');
CREATE TABLE orders (order_id VARCHAR PRIMARY KEY, customer_id VARCHAR, amount INTEGER);
INSERT INTO orders VALUES ('O1', 'C1', 100), ('O2', 'C1', 200), ('O3', 'C2', 50), ('O4', 'C3', 300);
"""
QUERY = {"dimensions": ["customers.region"], "metrics": ["orders.total_revenue"]}
BY_REGION = [("EAST", 350.0), ("WEST", 300.0)]


def _customers(*relationships: DatasetRelationship) -> SlayerDataset:
    return SlayerDataset(name="customers", sql_table="customers", data_source=DS, fields=[
        DatasetField(name="customer_id", datatype=DataType.STRING, primary_key=True),
        DatasetField(name="region", datatype=DataType.STRING),
    ], relationships=list(relationships))


def _orders(*relationships: DatasetRelationship, fields: tuple[DatasetField, ...] = (),
            metrics: tuple[DatasetMetric, ...] = ()) -> SlayerDataset:
    return SlayerDataset(
        name="orders", sql_table="orders", data_source=DS,
        fields=[DatasetField(name="order_id", datatype=DataType.STRING, primary_key=True),
                DatasetField(name="customer_id", datatype=DataType.STRING),
                DatasetField(name="amount", datatype=DataType.INTEGER), *fields],
        metrics=[DatasetMetric(name="total_revenue", expression="sum(amount)"), *metrics],
        relationships=list(relationships),
    )


def _rel(to: str, cardinality: RelationshipCardinality) -> DatasetRelationship:
    return DatasetRelationship(to=to, from_columns=["customer_id"], to_columns=["customer_id"], cardinality=cardinality)


def _document_declaring_one_to_many() -> str:
    """``orders_to_customers`` from ``orders`` to ``customers``, its ``SLAYER`` entry declaring it one-to-many."""
    keys = {"from_columns": ["customer_id"], "to_columns": ["customer_id"]}
    payload = ext.RelationshipPayload(entity="orders_to_customers", declarations=[ext.RelationshipDeclaration(
        dataset="orders", relationship=DatasetRelationship(
            to="customers", name="orders_to_customers", cardinality=RelationshipCardinality.ONE_TO_MANY,
            from_columns=keys["from_columns"], to_columns=keys["to_columns"]))])
    entry = ext.encode(payload, ossie=ext.relationship_digest(from_dataset="orders", to="customers", **keys))
    rel = {**relationship("orders_to_customers", src="orders", to="customers", **keys),
           "nonportable_extensions": [entry]}
    return document_text(name="sales", datasets=[
        dataset("customers", primary_key=["customer_id"], fields=[field("customer_id"), field("region")]),
        dataset("orders", primary_key=["order_id"],
                fields=[field("order_id"), field("customer_id"), field("amount", datatype="Integer")],
                metrics=[metric("total_revenue", "SUM(amount)")]),
    ], relationships=[rel])


async def test_a_one_to_many_relationship_is_a_backward_step(tmp_path: Path) -> None:
    """A one-to-many relationship is a backward step."""
    datasets = [_customers(_rel("orders", RelationshipCardinality.ONE_TO_MANY)), _orders()]
    async with sql_engine(tmp_path, sql=SQL, datasets=datasets) as engine:
        assert rows(await strict(engine, QUERY)) == BY_REGION


async def test_a_many_to_one_relationship_is_a_forward_step(tmp_path: Path) -> None:
    datasets = [_customers(), _orders(_rel("customers", RelationshipCardinality.MANY_TO_ONE))]
    async with sql_engine(tmp_path, sql=SQL, datasets=datasets) as engine:
        assert rows(await strict(engine, QUERY)) == BY_REGION


async def test_an_undetermined_relationship_follows_its_keys(tmp_path: Path) -> None:
    """No declared cardinality: ``customers.customer_id`` is the key, so the step runs ``orders`` → ``customers``."""
    datasets = [_customers(), _orders(DatasetRelationship(to="customers", from_columns=["customer_id"],
                                                          to_columns=["customer_id"]))]
    async with sql_engine(tmp_path, sql=SQL, datasets=datasets) as engine:
        assert rows(await strict(engine, QUERY)) == BY_REGION


async def test_a_many_to_many_relationship_gives_no_path(tmp_path: Path) -> None:
    """A many-to-many relationship gives no path."""
    datasets = [_customers(), _orders(_rel("customers", RelationshipCardinality.MANY_TO_MANY))]
    async with sql_engine(tmp_path, sql=SQL, datasets=datasets) as engine:
        await strict_error(engine, QUERY, "E_NO_PATH")


async def test_a_document_relationship_keeps_its_direction(tmp_path: Path) -> None:
    """A document relationship keeps its direction."""
    async with sql_engine(tmp_path, sql=SQL, documents=[_document_declaring_one_to_many()]) as engine:
        assert rows(await strict(engine, QUERY)) == BY_REGION


BUYER = _rel("customers", RelationshipCardinality.MANY_TO_ONE).model_copy(update={"name": "buyer"})
PLACED = _rel("orders", RelationshipCardinality.ONE_TO_MANY).model_copy(update={"name": "placed"})
BOTH_SIDES = [ext.RelationshipDeclaration(dataset="orders", relationship=BUYER),
              ext.RelationshipDeclaration(dataset="customers", relationship=PLACED)]


def _document_declaring(declarations: list[ext.RelationshipDeclaration]) -> str:
    """``orders_to_customers``, its ``SLAYER`` entry carrying ``declarations``."""
    keys = {"from_columns": ["customer_id"], "to_columns": ["customer_id"]}
    payload = ext.RelationshipPayload(entity="orders_to_customers", declarations=declarations)
    entry = ext.encode(payload, ossie=ext.relationship_digest(from_dataset="orders", to="customers", **keys))
    rel = {**relationship("orders_to_customers", src="orders", to="customers", **keys),
           "nonportable_extensions": [entry]}
    return document_text(name="sales", datasets=[
        dataset("customers", primary_key=["customer_id"], fields=[field("customer_id"), field("region")]),
        dataset("orders", primary_key=["order_id"],
                fields=[field("order_id"), field("customer_id"), field("amount", datatype="Integer")],
                metrics=[metric("total_revenue", "SUM(amount)")]),
    ], relationships=[rel])


async def _assert_plain_relationship(engine: Any) -> None:
    orders = await engine.storage.get_dataset("orders", data_source=DS)
    assert orders is not None
    [edge] = orders.relationships
    assert (edge.name, edge.to, edge.cardinality, edge.join_type.value) == (
        "orders_to_customers", "customers", RelationshipCardinality.MANY_TO_ONE, "left")


@pytest.mark.parametrize("declarations", [BOTH_SIDES, []], ids=["several", "none"])
async def test_an_entry_not_declaring_exactly_one_relationship_is_ignored(
        tmp_path: Path, declarations: list[ext.RelationshipDeclaration]) -> None:
    """One document relationship restores one edge, so native and strict both answer what Ossie answers."""
    async with sql_engine(tmp_path, sql=SQL, documents=[_document_declaring(declarations)]) as engine:
        await _assert_plain_relationship(engine)
        orders = await engine.storage.get_dataset("orders", data_source=DS)
        customers = await engine.storage.get_dataset("customers", data_source=DS)
        assert orders is not None
        assert customers is not None
        assert not customers.relationships
        assert [n.entity for n in orders.extension_notes if n.kind == "relationship"] == ["orders_to_customers"]
        assert rows(await engine.execute(QUERY)) == BY_REGION
        assert rows(await strict(engine, QUERY)) == BY_REGION


async def test_editing_the_other_side_keeps_the_ignored_entrys_relationship(tmp_path: Path) -> None:
    """The relationship belongs to the dataset a load puts it on, so editing ``customers`` leaves it alone."""
    async with sql_engine(tmp_path, sql=SQL, documents=[_document_declaring(BOTH_SIDES)]) as engine:
        customers = await engine.storage.get_dataset("customers", data_source=DS)
        assert customers is not None
        customers.relationships.append(PLACED)
        await engine.storage.save_dataset(customers)
        await _assert_plain_relationship(engine)


LINKS_SQL = """
CREATE TABLE c (id VARCHAR PRIMARY KEY);
CREATE TABLE d (id VARCHAR PRIMARY KEY);
CREATE TABLE a (id VARCHAR PRIMARY KEY, c_id VARCHAR);
CREATE TABLE b (id VARCHAR PRIMARY KEY, d_id VARCHAR);
"""


def _links_document() -> str:
    """``a.link → c`` and ``b.link → d``: one native name, so the document names the second ``link_2``."""
    keys = {"from_columns": ["d_id"], "to_columns": ["id"]}
    payload = ext.RelationshipPayload(entity="link_2", declarations=[ext.RelationshipDeclaration(
        dataset="b", relationship=DatasetRelationship(to="d", name="link", cardinality=RelationshipCardinality.MANY_TO_ONE,
                                                      from_columns=["d_id"], to_columns=["id"]))])
    entry = ext.encode(payload, ossie=ext.relationship_digest(from_dataset="b", to="d", **keys))
    return document_text(name="links", datasets=[
        dataset("a", primary_key=["id"], fields=[field("id"), field("c_id")]),
        dataset("b", primary_key=["id"], fields=[field("id"), field("d_id")]),
        dataset("c", primary_key=["id"], fields=[field("id")]),
        dataset("d", primary_key=["id"], fields=[field("id")]),
    ], relationships=[relationship("link", src="a", to="c", from_columns=["c_id"], to_columns=["id"]),
                      {**relationship("link_2", src="b", to="d", **keys), "nonportable_extensions": [entry]}])


async def test_editing_a_relationship_keeps_its_document_name(tmp_path: Path) -> None:
    """An edit rewrites the relationship under its allocated name, so it stays one Ossie relationship."""
    async with sql_engine(tmp_path, sql=LINKS_SQL, documents=[_links_document()]) as engine:
        b = await engine.storage.get_dataset("b", data_source=DS)
        assert b is not None
        b.relationships = [b.relationships[0].model_copy(update={"description": "b's d"})]
        await engine.storage.save_dataset(b)
        text = await engine.storage.get_semantic_model_text("links", data_source=DS)
        assert sorted(r["name"] for r in yaml.safe_load(text or "")["relationships"]) == ["link", "link_2"]
        b = await engine.storage.get_dataset("b", data_source=DS)
        assert b is not None
        assert [(r.name, r.to, r.description) for r in b.relationships] == [("link", "d", "b's d")]


PARALLEL_SQL = """
CREATE TABLE x (id VARCHAR PRIMARY KEY);
CREATE TABLE e (id VARCHAR PRIMARY KEY, x_id VARCHAR, y_id VARCHAR);
"""
PARALLEL = document_text(name="parallel", datasets=[
    dataset("e", primary_key=["id"], fields=[field("id"), field("x_id"), field("y_id")]),
    dataset("x", primary_key=["id"], fields=[field("id")]),
])


@pytest.mark.parametrize("keys", [("x_id", "y_id"), ("x_id", "x_id")], ids=["other-keys", "same-keys"])
async def test_unnamed_parallel_relationships_stay_two_ossie_relationships(tmp_path: Path, keys: tuple[str, str]) -> None:
    unnamed = [DatasetRelationship(to="x", from_columns=[key], to_columns=["id"], join_type=join,
                                   cardinality=RelationshipCardinality.MANY_TO_ONE)
               for key, join in zip(keys, (JoinType.LEFT, JoinType.INNER))]
    async with sql_engine(tmp_path, sql=PARALLEL_SQL, documents=[PARALLEL]) as engine:
        for edit in (lambda _held: unnamed, lambda held: [r.model_copy(update={"description": "edited"}) for r in held]):
            e = await engine.storage.get_dataset("e", data_source=DS)
            assert e is not None
            await engine.storage.save_dataset(e.model_copy(update={"relationships": edit(e.relationships)}))
            text = await engine.storage.get_semantic_model_text("parallel", data_source=DS)
            assert sorted(r["name"] for r in yaml.safe_load(text or "")["relationships"]) == ["e_x", "e_x_2"]
        e = await engine.storage.get_dataset("e", data_source=DS)
        assert e is not None
        assert sorted((r.name, r.from_columns[0], r.join_type.value, r.description) for r in e.relationships) == sorted([
            (None, keys[0], "left", "edited"), (None, keys[1], "inner", "edited")])


async def test_rekeying_an_unnamed_relationship_keeps_its_document_entity(tmp_path: Path) -> None:
    """Changing an unnamed relationship's keys rewrites its entity in place, ``ai_context`` kept."""
    rel = DatasetRelationship(to="x", from_columns=["x_id"], to_columns=["id"],
                              cardinality=RelationshipCardinality.MANY_TO_ONE)
    async with sql_engine(tmp_path, sql=PARALLEL_SQL, documents=[PARALLEL]) as engine:
        e = await engine.storage.get_dataset("e", data_source=DS)
        assert e is not None
        await engine.storage.save_dataset(e.model_copy(update={"relationships": [rel]}))
        doc = yaml.safe_load(await engine.storage.get_semantic_model_text("parallel", data_source=DS) or "")
        doc["relationships"][0]["ai_context"] = "keep me"
        await engine.storage.save_semantic_model_text(data_source=DS, name="parallel", text=yaml.safe_dump(doc))
        e = await engine.storage.get_dataset("e", data_source=DS)
        assert e is not None
        await engine.storage.save_dataset(e.model_copy(update={"relationships": [
            e.relationships[0].model_copy(update={"from_columns": ["y_id"]})]}))
        doc = yaml.safe_load(await engine.storage.get_semantic_model_text("parallel", data_source=DS) or "")
    [entity] = doc["relationships"]
    assert (entity["from_columns"], entity.get("ai_context")) == (["y_id"], "keep me")


async def test_two_named_relationships_on_the_same_keys_are_two_paths(tmp_path: Path) -> None:
    """They export as two relationships, so Ossie refuses as native does."""
    named = [_rel("orders", RelationshipCardinality.ONE_TO_MANY).model_copy(update={"name": "placed"}),
             _rel("customers", RelationshipCardinality.MANY_TO_ONE).model_copy(update={"name": "buyer"})]
    async with sql_engine(tmp_path, sql=SQL, datasets=[_customers(named[0]), _orders(named[1])]) as engine:
        await strict_error(engine, QUERY, "E_AMBIGUOUS_PATH")
        with pytest.raises(UnresolvableDimensionRelationshipError, match="more than one path"):
            await engine.execute(QUERY)


SELF_SQL = """
CREATE TABLE departments (id VARCHAR PRIMARY KEY, name VARCHAR);
INSERT INTO departments VALUES ('D1', 'ops'), ('D2', 'eng');
CREATE TABLE employees (id VARCHAR PRIMARY KEY, manager_id VARCHAR, department_id VARCHAR);
INSERT INTO employees VALUES ('E1', NULL, 'D1'), ('E2', 'E1', 'D1'), ('E3', 'E1', 'D2');
"""
SELF = document_text(name="staff", datasets=[
    dataset("departments", primary_key=["id"], fields=[field("id"), field("name")]),
    dataset("employees", primary_key=["id"], fields=[field("id"), field("manager_id"), field("department_id")],
            metrics=[metric("headcount", "COUNT(*)")]),
], relationships=[
    relationship("employees_to_departments", src="employees", to="departments", from_columns=["department_id"],
                 to_columns=["id"]),
    relationship("employees_to_managers", src="employees", to="employees", from_columns=["manager_id"],
                 to_columns=["id"]),
])


async def test_a_self_relationship_gives_no_step(tmp_path: Path) -> None:
    """The self-relationship adds no second path from ``employees`` to ``departments``."""
    async with sql_engine(tmp_path, sql=SELF_SQL, documents=[SELF]) as engine:
        response = await strict(engine, {"dimensions": ["departments.name"], "metrics": ["employees.headcount"]})
    assert rows(response) == [("eng", 1.0), ("ops", 2.0)]


async def test_slayer_only_content_is_honoured(tmp_path: Path) -> None:
    """SLayer-only content is honoured."""
    orders = _orders(
        _rel("customers", RelationshipCardinality.MANY_TO_ONE),
        fields=(DatasetField(name="customer_region", expression="customers.region", datatype=DataType.STRING),),
        metrics=(DatasetMetric(name="revenue_share", expression="sum(amount) / sum(amount, partition_by=[])"),),
    )
    query = {"dimensions": ["orders.customer_region"], "metrics": ["orders.revenue_share"]}
    async with sql_engine(tmp_path, sql=SQL, datasets=[_customers(), orders]) as engine:
        native = await engine.execute(query)
        response = await strict(engine, query)
    assert response.columns == ["customer_region", "revenue_share"]
    assert rows(response) == rows(native, "orders.customer_region", "orders.revenue_share")


def test_an_unreachable_target_is_settled_without_walking_every_path() -> None:
    """A chain of many-to-one diamonds doubles the paths per diamond; a target none reaches is settled at once."""
    n = 20
    steps = []
    for i in range(n):
        steps += [_Step(source=f"n{i}", target=f"l{i}", name=f"a{i}"), _Step(source=f"n{i}", target=f"r{i}", name=f"b{i}"),
                  _Step(source=f"l{i}", target=f"n{i + 1}", name=f"c{i}"),
                  _Step(source=f"r{i}", target=f"n{i + 1}", name=f"d{i}")]
    model = _Model(data_source=DS, datasets={}, metrics=[], steps=steps)
    start = time.monotonic()
    assert model.paths("n0", "elsewhere") == []
    left = [f"{x}{i}" for i in range(n - 1) for x in ("a", "c")]
    assert model.paths("n0", f"n{n}", cap=2) == [[*left, f"a{n - 1}", f"c{n - 1}"], [*left, f"b{n - 1}", f"d{n - 1}"]]
    assert time.monotonic() - start < 1


def test_a_cycle_back_to_the_source_is_not_walked() -> None:
    """Diamonds leading back to the source reach the target only through the source, so the walk never enters them."""
    n = 20
    steps = []
    for i in range(n):
        steps += [_Step(source=f"n{i}", target=f"l{i}", name=f"a{i}"), _Step(source=f"n{i}", target=f"r{i}", name=f"b{i}"),
                  _Step(source=f"l{i}", target=f"n{i + 1}", name=f"c{i}"),
                  _Step(source=f"r{i}", target=f"n{i + 1}", name=f"d{i}")]
    steps += [_Step(source=f"n{n}", target="n0", name="back"), _Step(source="n0", target="goal", name="direct")]
    model = _Model(data_source=DS, datasets={}, metrics=[], steps=steps)
    start = time.monotonic()
    assert model.paths("n0", "goal") == [["direct"]]
    assert time.monotonic() - start < 1
