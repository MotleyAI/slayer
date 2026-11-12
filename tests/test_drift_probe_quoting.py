"""Drift vs broken SQL over sql-mode models reading mixed-case and reserved-word tables."""

from __future__ import annotations

from pathlib import Path

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema.schema_drift import WholeModelDelete
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


def _model(sql: str) -> SlayerModel:
    return SlayerModel(name="m", sql=sql, data_source="ds", columns=[
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name="amount", sql="amount", type=DataType.DOUBLE),
    ])


async def _verdict(engine: SlayerQueryEngine) -> list[WholeModelDelete]:
    entries = await engine.validate_models(data_source="ds")
    return [e for e in entries if e.model_name == "m" and isinstance(e, WholeModelDelete)]


@pytest.mark.parametrize("table", TABLES)
async def test_broken_sql_over_a_quoted_table_is_invalid_sql(tmp_path: Path, table: str) -> None:
    engine, _ = await _engine(tmp_path)
    try:
        await engine.storage.save_model(_model(f"SELECT id, NO_SUCH_FUNCTION(amount) AS amount FROM {table}"))
        (entry,) = await _verdict(engine)
        assert entry.cause == "invalid_sql"
    finally:
        engine.close()


@pytest.mark.parametrize("table", TABLES)
async def test_dropped_column_of_a_quoted_table_is_drift(tmp_path: Path, table: str) -> None:
    engine, db = await _engine(tmp_path)
    try:
        await engine.storage.save_model(_model(f"SELECT id, amount FROM {table}"))
        with transaction(db) as conn:
            conn.execute(f"ALTER TABLE {table} DROP COLUMN amount")
        (entry,) = await _verdict(engine)
        assert entry.cause == "schema_drift"
    finally:
        engine.close()
