"""Exact NUMERIC/DECIMAL aggregates must not inherit a lossy DOUBLE cast (#364)."""

from decimal import Decimal

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig, ModelJoin
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

duckdb = pytest.importorskip("duckdb")
pytestmark = pytest.mark.integration


@pytest.fixture
async def decimal_engine(tmp_path):
    database = str(tmp_path / "decimal.duckdb")
    with duckdb.connect(database) as connection:
        connection.execute("CREATE TABLE orders (id INT, amount DECIMAL(18,2), created_at TIMESTAMP)")
        connection.execute(
            query="INSERT INTO orders VALUES (1, ?, '2024-01-01'), (2, ?, '2024-01-02')",
            parameters=[Decimal("90071992547409.91"), Decimal("0.02")],
        )

    datasource = DatasourceConfig(name="test", type="duckdb", database=database)
    model = next(model for model in ingest_datasource(datasource=datasource) if model.name == "orders")
    amount = model.get_column("amount")
    assert amount is not None
    assert amount.type is DataType.DOUBLE
    assert amount.db_type == "DECIMAL(18,2)"

    storage = YAMLStorage(base_dir=str(tmp_path / "models"))
    await storage.save_datasource(datasource)
    customers = model.model_copy(update={"name": "customers", "joins": []})
    model.joins = [ModelJoin(target_model="customers", join_pairs=[["id", "id"]])]
    await storage.save_model(model)
    await storage.save_model(customers)
    return SlayerQueryEngine(storage=storage)


@pytest.mark.parametrize(
    argnames="measure,query_fields,expected",
    argvalues=[
        ("amount:sum", {}, [Decimal("90071992547409.93")]),
        (
            {"formula": "amount:sum(window='2d')", "name": "total"},
            {"time_dimensions": [{"dimension": "created_at", "granularity": "day"}]},
            [Decimal("90071992547409.91"), Decimal("90071992547409.93")],
        ),
        (
            {"formula": "amount:sum(partition_by=created_at)", "name": "total"},
            {"dimensions": ["id", "created_at"]},
            [Decimal("0.02"), Decimal("90071992547409.91")],
        ),
    ],
)
async def test_inferred_decimal_aggregates_preserve_native_precision(
    decimal_engine,
    measure,
    query_fields,
    expected,
):
    query = {"source_model": "orders", "measures": [measure], **query_fields}
    result = await decimal_engine.execute(query=query)
    sql = (await decimal_engine.execute(query=query, dry_run=True)).sql

    assert " AS DOUBLE" not in sql
    values = [row[next(reversed(row))] for row in result.data]
    assert all(isinstance(value, Decimal) for value in values)
    assert sorted(values) == expected


async def test_explicit_double_measure_still_casts(decimal_engine):
    query = {
        "source_model": "orders",
        "measures": [{"formula": "amount:sum", "type": "DOUBLE"}],
    }
    result = await decimal_engine.execute(query=query)
    sql = (await decimal_engine.execute(query=query, dry_run=True)).sql

    assert "CAST(SUM(orders.amount) AS DOUBLE)" in sql
    assert isinstance(result.data[0]["orders.amount_sum"], float)


async def test_cross_model_decimal_aggregate_preserves_native_precision(decimal_engine):
    query = {"source_model": "orders", "measures": ["customers.amount:sum"]}
    result = await decimal_engine.execute(query=query)
    sql = (await decimal_engine.execute(query=query, dry_run=True)).sql

    assert " AS DOUBLE" not in sql
    assert list(result.data[0].values()) == [Decimal("90071992547409.93")]
