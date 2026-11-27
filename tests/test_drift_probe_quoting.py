"""Drift vs broken SQL over sql-mode datasets reading mixed-case and reserved-word tables."""

from __future__ import annotations

from pathlib import Path

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema.schema_drift import WholeDatasetDelete
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage

TABLES = ['"order"', '"Order Items"']


async def _engine(tmp_path: Path) -> tuple[SlayerQueryEngine, str]:
    db = str(tmp_path / "live.db")
    with transaction(db) as conn:
        for table in TABLES:
            conn.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, amount REAL)")  # noqa: S608 — test seed
    storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
    await storage.save_datasource(DatasourceConfig(name="ds", type="sqlite", database=db))
    return SlayerQueryEngine(storage=storage), db


def _dataset(sql: str) -> SlayerDataset:
    return SlayerDataset(name="m", sql=sql, data_source="ds", fields=[
        DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
        DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
    ])


async def _verdict(engine: SlayerQueryEngine) -> list[WholeDatasetDelete]:
    entries = await engine.validate_datasets(data_source="ds")
    return [e for e in entries if e.dataset_name == "m" and isinstance(e, WholeDatasetDelete)]


@pytest.mark.parametrize("table", TABLES)
async def test_broken_sql_over_a_quoted_table_is_invalid_sql(tmp_path: Path, table: str) -> None:
    engine, _ = await _engine(tmp_path)
    try:
        await engine.storage.save_dataset(_dataset(f"SELECT id, NO_SUCH_FUNCTION(amount) AS amount FROM {table}"))
        (entry,) = await _verdict(engine)
        assert entry.cause == "invalid_sql"
    finally:
        engine.close()


@pytest.mark.parametrize("table", TABLES)
async def test_dropped_column_of_a_quoted_table_is_drift(tmp_path: Path, table: str) -> None:
    engine, db = await _engine(tmp_path)
    try:
        await engine.storage.save_dataset(_dataset(f"SELECT id, amount FROM {table}"))
        with transaction(db) as conn:
            conn.execute(f"ALTER TABLE {table} DROP COLUMN amount")
        (entry,) = await _verdict(engine)
        assert entry.cause == "schema_drift"
    finally:
        engine.close()
