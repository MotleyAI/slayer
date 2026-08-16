"""Idempotent re-ingest must persist metadata-only cardinality/unique fills."""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from slayer.core.enums import JoinCardinality
from slayer.core.models import DatasourceConfig
from slayer.engine.ingestion import ingest_datasource_idempotent
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
            email TEXT UNIQUE,
            region TEXT NOT NULL
        );
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            amount REAL NOT NULL,
            customer_id INTEGER REFERENCES customers(id)
        );
        INSERT INTO customers VALUES (1, 'a@x.com', 'US');
        INSERT INTO orders VALUES (1, 100.0, 1);
        """
    )
    conn.commit()
    conn.close()


async def _setup(workspace: Path) -> tuple:
    db_path = str(workspace / "live.db")
    _create_schema(db_path)
    storage = YAMLStorage(base_dir=str(workspace / "storage"))
    ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
    await storage.save_datasource(ds)
    await ingest_datasource_idempotent(datasource=ds, storage=storage)
    return storage, ds


def _order_join(model):
    return next(j for j in model.joins if j.target_model == "customers")


class TestFillsMetadataAndPersists:
    async def test_reingest_refills_none_cardinality_and_persists(
        self, workspace: Path
    ) -> None:
        storage, ds = await _setup(workspace)

        # Simulate legacy persisted data: join without a cardinality.
        orders = await storage.get_model("orders", data_source="ds")
        orders.joins[0] = _order_join(orders).model_copy(update={"cardinality": None})
        await storage.save_model(orders)

        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        reloaded = await storage.get_model("orders", data_source="ds")
        # Refilled AND persisted (reloaded from storage proves the save).
        assert _order_join(reloaded).cardinality is JoinCardinality.MANY_TO_ONE

    async def test_reingest_sets_unique_and_persists(self, workspace: Path) -> None:
        storage, ds = await _setup(workspace)

        customers = await storage.get_model("customers", data_source="ds")
        email = next(c for c in customers.columns if c.name == "email")
        idx = customers.columns.index(email)
        customers.columns[idx] = email.model_copy(update={"unique": False})
        await storage.save_model(customers)

        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        reloaded = await storage.get_model("customers", data_source="ds")
        email = next(c for c in reloaded.columns if c.name == "email")
        assert email.unique is True


class TestAdditiveContract:
    async def test_reingest_does_not_overwrite_user_cardinality(
        self, workspace: Path
    ) -> None:
        storage, ds = await _setup(workspace)

        # A deliberate user override that disagrees with the structural guess.
        orders = await storage.get_model("orders", data_source="ds")
        orders.joins[0] = _order_join(orders).model_copy(
            update={"cardinality": JoinCardinality.ONE_TO_ONE}
        )
        await storage.save_model(orders)

        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        reloaded = await storage.get_model("orders", data_source="ds")
        assert _order_join(reloaded).cardinality is JoinCardinality.ONE_TO_ONE

    async def test_reingest_does_not_downgrade_user_unique(
        self, workspace: Path
    ) -> None:
        storage, ds = await _setup(workspace)

        # User marked a column unique that has no DB constraint.
        customers = await storage.get_model("customers", data_source="ds")
        region = next(c for c in customers.columns if c.name == "region")
        idx = customers.columns.index(region)
        customers.columns[idx] = region.model_copy(update={"unique": True})
        await storage.save_model(customers)

        await ingest_datasource_idempotent(datasource=ds, storage=storage)

        reloaded = await storage.get_model("customers", data_source="ds")
        region = next(c for c in reloaded.columns if c.name == "region")
        assert region.unique is True
