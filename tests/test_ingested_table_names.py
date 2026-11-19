"""Ingestion stores table names that read back as their live tables, on Postgres (spec: models/table-names)."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.live_schema.ingestion import ingest_datasource_idempotent
from slayer.live_schema.schema_drift import IdempotentIngestResult
from slayer.database import engine_factory
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import owned_query_engine
from tests._pg_helpers import client_datasource

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

DS = "ds"
_SEED = (
    "CREATE SCHEMA sales", 'CREATE TABLE sales."order items" (qty INTEGER)', 'INSERT INTO sales."order items" VALUES (6)',
    'CREATE SCHEMA "sales.v2"', 'CREATE TABLE "sales.v2".orders (qty INTEGER)', 'INSERT INTO "sales.v2".orders VALUES (5)',
    'CREATE TABLE "OrderItems" (qty INTEGER)', 'INSERT INTO "OrderItems" VALUES (4)',
    "CREATE TABLE orderitems (qty TEXT)", "INSERT INTO orderitems VALUES ('t')",
    'CREATE TABLE "ArtifactsCore" (qty INTEGER)', 'INSERT INTO "ArtifactsCore" VALUES (3)',
)


@pytest.fixture
async def shop(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncGenerator[tuple[DatasourceConfig, YAMLStorage]]:
    conn = request.getfixturevalue("postgresql")
    cur = conn.cursor()
    for statement in _SEED:
        cur.execute(statement)
    conn.commit()
    datasource = client_datasource(conn.info, name=DS)
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(datasource)
    try:
        yield datasource, storage
    finally:
        engine_factory.invalidate_engine(datasource)


async def _qty(storage: YAMLStorage, dataset: str) -> list[Any]:
    async with owned_query_engine(storage=storage) as engine:
        rows = (await engine.execute({"population": dataset, "dimensions": ["qty"]})).data
    return [r[f"{dataset}.qty"] for r in rows]


async def _drift(storage: YAMLStorage) -> list[Any]:
    async with owned_query_engine(storage=storage) as engine:
        return await engine.validate_datasets(data_source=DS)


async def _ingest(*, datasource: DatasourceConfig, storage: YAMLStorage, **scope: Any) -> IdempotentIngestResult:
    async with owned_query_engine(storage=storage) as engine:
        return await ingest_datasource_idempotent(datasource=datasource, storage=storage,
                                                  client=engine.client_for(datasource), **scope)


async def _ingested(storage: YAMLStorage) -> SlayerDataset:
    (name,) = await storage.list_datasets()
    model = await storage.get_dataset(name)
    assert model is not None
    return model


async def test_table_name_with_a_space(shop: tuple[DatasourceConfig, YAMLStorage]) -> None:
    datasource, storage = shop
    await _ingest(datasource=datasource, storage=storage, schema="sales")
    model = await _ingested(storage)
    assert model.sql_table == 'sales."order items"'
    assert await _qty(storage, model.name) == [6]


async def test_schema_name_with_a_dot(shop: tuple[DatasourceConfig, YAMLStorage]) -> None:
    datasource, storage = shop
    await _ingest(datasource=datasource, storage=storage, schema="sales.v2")
    model = await _ingested(storage)
    assert model.sql_table == '"sales.v2".orders'
    assert await _qty(storage, model.name) == [5]
    again = await _ingest(datasource=datasource, storage=storage, schema="sales.v2")
    assert [(a.created, a.new_fields) for a in again.additions] == [(False, [])]
    assert again.to_delete == []
    assert again.errors == []
    assert await _drift(storage) == []


async def test_mixed_case_live_name(shop: tuple[DatasourceConfig, YAMLStorage]) -> None:
    datasource, storage = shop
    await _ingest(datasource=datasource, storage=storage, include_tables=["OrderItems"])
    model = await _ingested(storage)
    assert model.sql_table == "OrderItems"
    assert await _qty(storage, model.name) == [4]


@pytest.mark.parametrize(("sql_table", "value"), [("ArtifactsCore", 3), ('"sales.v2".orders', 5)])
async def test_stored_name_reads_its_table(shop: tuple[DatasourceConfig, YAMLStorage], sql_table: str,
                                           value: int) -> None:
    _, storage = shop
    await storage.save_dataset(SlayerDataset(name="m", data_source=DS, sql_table=sql_table,
                                         fields=[DatasetField(name="qty", datatype=DataType.INT)]))
    assert await _qty(storage, "m") == [value]
    assert await _drift(storage) == []
