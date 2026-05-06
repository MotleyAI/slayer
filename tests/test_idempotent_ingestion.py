"""Tests for idempotent ``ingest_datasource_idempotent``. See DEV-1356.

The function:

* Adds new columns / joins / tables that appeared in the live schema since
  the last ingest.
* Never overwrites existing column / join definitions.
* Skips ``sql``-mode and query-backed models.
* Returns a combined ``IdempotentIngestResult`` with ``additions``,
  ``to_delete`` (verbatim ``validate_models`` output), and per-model
  ``errors``.
"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from typing import Iterable

import pytest

from slayer.core.enums import DataType
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.engine.ingestion import ingest_datasource_idempotent
from slayer.engine.schema_drift import (
    IdempotentIngestResult,
    ModelAddition,
    WholeModelDelete,
    EditModelDelete,
)
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def workspace():
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


def _create_schema(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            region TEXT NOT NULL
        );
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            amount REAL NOT NULL,
            status TEXT NOT NULL,
            customer_id INTEGER REFERENCES customers(id)
        );
        INSERT INTO customers VALUES (1, 'US'), (2, 'EU');
        INSERT INTO orders VALUES (1, 100.0, 'completed', 1);
        """
    )
    conn.commit()
    conn.close()


async def _setup(workspace: Path, *, persist_models: bool = True) -> tuple:
    db_path = str(workspace / "live.db")
    _create_schema(db_path)
    storage = YAMLStorage(base_dir=str(workspace / "storage"))
    ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
    await storage.save_datasource(ds)
    if persist_models:
        # Run ingest once so that subsequent calls test idempotency.
        await ingest_datasource_idempotent(datasource=ds, storage=storage)
    return storage, ds, db_path


def _addition_for(name: str, additions: Iterable[ModelAddition]):
    for a in additions:
        if a.model_name == name:
            return a
    return None


# ---------------------------------------------------------------------------


class TestIdempotency:
    async def test_re_run_on_identical_schema_is_no_op(self, workspace: Path) -> None:
        storage, ds, _ = await _setup(workspace)
        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        assert isinstance(result, IdempotentIngestResult)
        # Nothing new
        for addition in result.additions:
            assert addition.created is False
            assert addition.new_columns == []
            assert addition.new_joins == []
        assert result.to_delete == []
        assert result.errors == []


class TestAdditive:
    async def test_new_column_appends_to_existing_model(
        self, workspace: Path
    ) -> None:
        storage, ds, db_path = await _setup(workspace)
        conn = sqlite3.connect(db_path)
        conn.execute("ALTER TABLE orders ADD COLUMN delivery_address TEXT")
        conn.commit()
        conn.close()

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        addition = _addition_for("orders", result.additions)
        assert addition is not None
        assert "delivery_address" in addition.new_columns
        assert addition.created is False
        # Persisted model now has the new column
        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded is not None
        assert any(c.name == "delivery_address" for c in loaded.columns)

    async def test_new_table_creates_model(self, workspace: Path) -> None:
        storage, ds, db_path = await _setup(workspace)
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE products (id INTEGER PRIMARY KEY, sku TEXT NOT NULL)"
        )
        conn.execute("INSERT INTO products VALUES (1, 'A1')")
        conn.commit()
        conn.close()

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        addition = _addition_for("products", result.additions)
        assert addition is not None
        assert addition.created is True
        assert {"id", "sku"} <= set(addition.new_columns)
        loaded = await storage.get_model("products", data_source="ds")
        assert loaded is not None
        assert {c.name for c in loaded.columns} == {"id", "sku"}

    async def test_new_fk_creates_join(self, workspace: Path) -> None:
        # Build a DB with no FK initially, then add one. SQLite doesn't
        # support adding FK to an existing table via ALTER, so this test
        # builds a scenario where the orders.customer_id exists but no
        # FK was defined initially.
        db_path = str(workspace / "live.db")
        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT NOT NULL);
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                amount REAL NOT NULL,
                customer_id INTEGER
            );
            INSERT INTO customers VALUES (1, 'US');
            INSERT INTO orders VALUES (1, 100.0, 1);
            """
        )
        conn.commit()
        conn.close()
        storage = YAMLStorage(base_dir=str(workspace / "storage"))
        ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
        await storage.save_datasource(ds)
        # Persist orders model with no joins
        await storage.save_model(
            SlayerModel(
                name="customers",
                sql_table="customers",
                data_source="ds",
                columns=[
                    Column(
                        name="id", sql="id", type=DataType.NUMBER, primary_key=True
                    ),
                    Column(name="region", sql="region", type=DataType.STRING),
                ],
            )
        )
        await storage.save_model(
            SlayerModel(
                name="orders",
                sql_table="orders",
                data_source="ds",
                columns=[
                    Column(
                        name="id", sql="id", type=DataType.NUMBER, primary_key=True
                    ),
                    Column(name="amount", sql="amount", type=DataType.NUMBER),
                    Column(
                        name="customer_id",
                        sql="customer_id",
                        type=DataType.NUMBER,
                    ),
                ],
                joins=[],
            )
        )
        # Recreate orders with an FK now via DROP + CREATE (SQLite has no
        # ALTER to add FKs).
        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            CREATE TABLE orders_new (
                id INTEGER PRIMARY KEY,
                amount REAL NOT NULL,
                customer_id INTEGER REFERENCES customers(id)
            );
            INSERT INTO orders_new SELECT * FROM orders;
            DROP TABLE orders;
            ALTER TABLE orders_new RENAME TO orders;
            """
        )
        conn.commit()
        conn.close()

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded is not None
        assert any(j.target_model == "customers" for j in loaded.joins)
        addition = _addition_for("orders", result.additions)
        assert addition is not None
        assert "customers" in addition.new_joins


class TestPreservation:
    async def test_existing_column_metadata_preserved(self, workspace: Path) -> None:
        storage, ds, db_path = await _setup(workspace)
        # Mutate the persisted orders model: customize amount's metadata
        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded is not None
        for c in loaded.columns:
            if c.name == "amount":
                c.description = "Order amount in USD"
                c.label = "Amount"
                c.format = NumberFormat(type=NumberFormatType.CURRENCY)
                c.meta = {"unit": "usd"}
        await storage.save_model(loaded)

        # Add a new live column to trigger the additive pass.
        conn = sqlite3.connect(db_path)
        conn.execute("ALTER TABLE orders ADD COLUMN extra TEXT")
        conn.commit()
        conn.close()

        await ingest_datasource_idempotent(datasource=ds, storage=storage)
        loaded2 = await storage.get_model("orders", data_source="ds")
        assert loaded2 is not None
        amount = next(c for c in loaded2.columns if c.name == "amount")
        # Untouched: customizations preserved
        assert amount.description == "Order amount in USD"
        assert amount.label == "Amount"
        assert amount.format is not None
        assert amount.format.type == NumberFormatType.CURRENCY
        assert amount.meta == {"unit": "usd"}
        # New column appended
        assert any(c.name == "extra" for c in loaded2.columns)


class TestSkipNonSqlTableModes:
    async def test_sql_mode_model_skipped(self, workspace: Path) -> None:
        storage, ds, _ = await _setup(workspace, persist_models=False)
        await storage.save_model(
            SlayerModel(
                name="custom_orders",
                sql="SELECT id, amount FROM orders WHERE amount > 0",
                data_source="ds",
                columns=[
                    Column(
                        name="id", sql="id", type=DataType.NUMBER, primary_key=True
                    ),
                    Column(name="amount", sql="amount", type=DataType.NUMBER),
                ],
            )
        )
        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        # custom_orders is sql-mode — additive pass must not touch it.
        loaded = await storage.get_model("custom_orders", data_source="ds")
        assert loaded is not None
        assert loaded.sql is not None
        assert _addition_for("custom_orders", result.additions) is None

    async def test_query_backed_model_skipped(self, workspace: Path) -> None:
        storage, ds, _ = await _setup(workspace)
        # Save a query-backed model that references orders. Use the engine
        # so the cache populates correctly.
        from slayer.engine.query_engine import SlayerQueryEngine

        engine = SlayerQueryEngine(storage=storage)
        await engine.create_model_from_query(
            query=SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum", "name": "total"}],
            ),
            name="orders_summary",
        )
        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        assert _addition_for("orders_summary", result.additions) is None
        # And the persisted query-backed model is unchanged.
        loaded = await storage.get_model("orders_summary", data_source="ds")
        assert loaded is not None
        assert loaded.source_queries is not None


class TestCombinedReturnShape:
    async def test_type_drift_surfaces_in_to_delete_not_additions(
        self, workspace: Path
    ) -> None:
        storage, ds, db_path = await _setup(workspace)
        # Persisted orders.amount = NUMBER. Mutate live so it returns text:
        # SQLite has dynamic typing, so we drop and recreate as TEXT.
        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            CREATE TABLE orders_new (
                id INTEGER PRIMARY KEY,
                amount TEXT NOT NULL,
                status TEXT NOT NULL,
                customer_id INTEGER REFERENCES customers(id)
            );
            INSERT INTO orders_new (id, amount, status, customer_id)
                SELECT id, CAST(amount AS TEXT), status, customer_id FROM orders;
            DROP TABLE orders;
            ALTER TABLE orders_new RENAME TO orders;
            """
        )
        conn.commit()
        conn.close()

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        # No additions for this drift — the additive pass simply skips the
        # name (it is taken in the persisted model) and lets validate_models
        # surface the bucket mismatch.
        addition = _addition_for("orders", result.additions)
        assert addition is None or "amount" not in addition.new_columns
        # And the to_delete payload lists the bucket-mismatched column.
        orders_drops = [
            e for e in result.to_delete if e.model_name == "orders"
        ]
        assert orders_drops, "expected validate_models to flag orders.amount"
        assert isinstance(orders_drops[0], (EditModelDelete, WholeModelDelete))


class TestErrorIsolation:
    async def test_per_model_save_failure_captured_in_errors(
        self, workspace: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        storage, ds, db_path = await _setup(workspace)
        # Add two new tables so the additive pass tries to save two models.
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE a_new (id INTEGER PRIMARY KEY, x TEXT)")
        conn.execute("CREATE TABLE b_new (id INTEGER PRIMARY KEY, y TEXT)")
        conn.commit()
        conn.close()

        # Patch save_model on the storage to fail for "a_new" only.
        original_save = storage.save_model

        async def flaky_save(model):
            if model.name == "a_new":
                raise RuntimeError("disk full")
            return await original_save(model)

        monkeypatch.setattr(storage, "save_model", flaky_save)

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        # One success (b_new), one failure (a_new)
        assert _addition_for("b_new", result.additions) is not None
        assert any(e.model_name == "a_new" for e in result.errors)


class TestExcludeTables:
    async def test_excluded_tables_not_touched(self, workspace: Path) -> None:
        storage, ds, db_path = await _setup(workspace)
        # Add a column to orders, but exclude orders from the pass.
        conn = sqlite3.connect(db_path)
        conn.execute("ALTER TABLE orders ADD COLUMN extra TEXT")
        conn.commit()
        conn.close()

        result = await ingest_datasource_idempotent(
            datasource=ds, storage=storage, exclude_tables=["orders"]
        )
        # No addition for orders
        assert _addition_for("orders", result.additions) is None
        # to_delete also doesn't reference orders
        assert not any(e.model_name == "orders" for e in result.to_delete)
        # Persisted orders model is unchanged (no extra column appended)
        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded is not None
        assert not any(c.name == "extra" for c in loaded.columns)
