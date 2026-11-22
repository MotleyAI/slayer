"""The strict binding reads the datasource as an Ossie model: Ossie steps, paths, SLayer-only content honoured.

Spec: queries/ossie-l3-binding › The strict binding reads the model as an Ossie model.
"""

from __future__ import annotations

from pathlib import Path

from slayer.core import slayer_extension as ext
from slayer.core.enums import DataType, RelationshipCardinality
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
