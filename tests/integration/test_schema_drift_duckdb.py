"""Integration tests for ``validate_models`` against a real DuckDB database.

DuckDB lets us exercise live DDL mutations (DROP COLUMN, DROP TABLE) and assert
that the validator surfaces them correctly without Docker. See DEV-1356.
"""

from __future__ import annotations

import pytest

from slayer.async_utils import run_sync
from slayer.core.enums import DataType
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    SlayerDataset,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema.schema_drift import DriftReportEntry, EditDatasetDelete, WholeDatasetDelete
from slayer.storage.yaml_storage import YAMLStorage
from typing import Any

duckdb = pytest.importorskip("duckdb")

pytestmark = pytest.mark.integration


@pytest.fixture
def duckdb_drift_env(tmp_path):
    """Per-test DuckDB file + persisted SlayerModels for orders/customers."""
    db_path = tmp_path / "drift.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            region VARCHAR NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            amount DECIMAL(10,2) NOT NULL,
            status VARCHAR NOT NULL,
            customer_id INTEGER REFERENCES customers(id)
        );
        """
    )
    # A standalone table with no FK relationships — used by the drop-column
    # test, since DuckDB's ALTER blocks columns whose later siblings carry
    # an index (FK-generated or otherwise).
    conn.execute(
        """
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            sku VARCHAR NOT NULL,
            description VARCHAR
        );
        """
    )
    conn.execute("INSERT INTO products VALUES (1, 'A1', 'thing')")
    conn.executemany("INSERT INTO customers VALUES (?, ?)", [(1, "US"), (2, "EU")])
    conn.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?)",
        [(1, 100, "completed", 1), (2, 200, "pending", 2)],
    )
    conn.close()

    storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
    run_sync(
        storage.save_datasource(
            DatasourceConfig(name="dduckdb", type="duckdb", database=str(db_path))
        )
    )
    run_sync(
        storage.save_dataset(
            SlayerDataset(
                name="customers",
                sql_table="customers",
                data_source="dduckdb",
                fields=[
                    DatasetField(
                        name="id", sql="id", datatype=DataType.DOUBLE, primary_key=True
                    ),
                    DatasetField(name="region", sql="region", datatype=DataType.TEXT),
                ],
            )
        )
    )
    run_sync(
        storage.save_dataset(
            SlayerDataset(
                name="products",
                sql_table="products",
                data_source="dduckdb",
                fields=[
                    DatasetField(
                        name="id", sql="id", datatype=DataType.DOUBLE, primary_key=True
                    ),
                    DatasetField(name="sku", sql="sku", datatype=DataType.TEXT),
                    DatasetField(
                        name="description",
                        sql="description",
                        datatype=DataType.TEXT,
                    ),
                ],
            )
        )
    )
    run_sync(
        storage.save_dataset(
            SlayerDataset(
                name="orders",
                sql_table="orders",
                data_source="dduckdb",
                fields=[
                    DatasetField(
                        name="id", sql="id", datatype=DataType.DOUBLE, primary_key=True
                    ),
                    DatasetField(name="amount", sql="amount", datatype=DataType.DOUBLE),
                    DatasetField(name="status", sql="status", datatype=DataType.TEXT),
                    DatasetField(
                        name="customer_id",
                        sql="customer_id",
                        datatype=DataType.DOUBLE,
                    ),
                ],
                relationships=[
                    DatasetRelationship(
                        to="customers",
                        join_pairs=[["customer_id", "id"]],
                    ),
                ],
            )
        )
    )
    engine = SlayerQueryEngine(storage=storage)
    return engine, str(db_path)


def _find(name: str, entries: list[DriftReportEntry]) -> DriftReportEntry | None:
    for e in entries:
        if e.dataset_name == name:
            return e
    return None


async def test_no_drift_returns_empty(duckdb_drift_env: tuple[SlayerQueryEngine, Any]) -> None:
    engine, _ = duckdb_drift_env
    result = await engine.validate_datasets(data_source="dduckdb")
    assert result == []


async def test_sql_model_typed_columns_do_not_report_false_drift(
    duckdb_drift_env: tuple[SlayerQueryEngine, Any],
) -> None:
    engine, _ = duckdb_drift_env
    await engine.storage.save_dataset(
        SlayerDataset(
            name="typed_sql",
            data_source="dduckdb",
            sql=(
                "SELECT 7::INTEGER AS qty, TRUE AS active, "
                "TIMESTAMP '2026-01-02 03:04:05' AS occurred_at, "
                "'ok'::VARCHAR AS label"
            ),
            fields=[
                DatasetField(name="qty", datatype=DataType.INT),
                DatasetField(name="active", datatype=DataType.BOOLEAN),
                DatasetField(name="occurred_at", datatype=DataType.TIMESTAMP),
                DatasetField(name="label", datatype=DataType.TEXT),
            ],
        )
    )

    result = await engine.validate_datasets(data_source="dduckdb")

    assert result == []


async def test_drop_column_yields_edit_model_delete(duckdb_drift_env: tuple[SlayerQueryEngine, Any]) -> None:
    engine, db_path = duckdb_drift_env
    conn = duckdb.connect(db_path)
    # DuckDB rejects ALTER on tables with FK relationships, so use the
    # standalone ``products`` table and drop the trailing ``description``
    # column (no later-positioned indexes to block the operation).
    conn.execute("ALTER TABLE products DROP COLUMN description")
    conn.close()

    result = await engine.validate_datasets(data_source="dduckdb")
    entry = _find("products", result)
    assert isinstance(entry, EditDatasetDelete)
    assert "description" in entry.remove.fields


async def test_drop_table_yields_whole_model_delete(duckdb_drift_env: tuple[SlayerQueryEngine, Any]) -> None:
    engine, db_path = duckdb_drift_env
    conn = duckdb.connect(db_path)
    conn.execute("DROP TABLE products")
    conn.close()

    result = await engine.validate_datasets(data_source="dduckdb")
    entry = _find("products", result)
    assert isinstance(entry, WholeDatasetDelete)
