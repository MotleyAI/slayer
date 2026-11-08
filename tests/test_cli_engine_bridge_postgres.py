"""A multi-step CLI command runs its whole workflow in one event loop (spec: database/execution)."""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest
from psycopg import sql as pg_sql
from pytest_postgresql import factories

from slayer.async_utils import run_sync
from slayer.cli import _run_validate_models
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.engine.cardinality import CardinalityVerdict
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage

postgresql_proc = factories.postgresql_proc(port=None)


@pytest.fixture
def pg_datasource(postgresql_proc: Any) -> Iterator[DatasourceConfig]:
    info = postgresql_proc
    db_name = f"cli_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(host=info.host, port=info.port, user=info.user, dbname="postgres", autocommit=True) as admin:
        admin.execute(pg_sql.SQL("CREATE DATABASE {}").format(pg_sql.Identifier(db_name)))
    with psycopg.connect(host=info.host, port=info.port, user=info.user, dbname=db_name) as conn:
        conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT)")
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers(id))")
        conn.execute("INSERT INTO customers VALUES (1, 'US'), (2, 'EU')")
        conn.execute("INSERT INTO orders VALUES (1, 1), (2, 1), (3, 2)")
        conn.commit()
    ds = DatasourceConfig(
        name="pg", type="postgres", host=info.host, port=info.port,
        database=db_name, username=info.user, password="",
    )
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)
        with psycopg.connect(host=info.host, port=info.port, user=info.user, dbname="postgres", autocommit=True) as admin:
            admin.execute(pg_sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(pg_sql.Identifier(db_name)))


def _store(base: Path, ds: DatasourceConfig) -> str:
    storage = YAMLStorage(base_dir=str(base))
    run_sync(storage.save_datasource(ds))
    run_sync(storage.save_model(SlayerModel(
        name="customers", sql_table="customers", data_source=ds.name,
        columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                 Column(name="region", sql="region", type=DataType.TEXT)],
    )))
    run_sync(storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source=ds.name,
        columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                 Column(name="customer_id", sql="customer_id", type=DataType.INT)],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )))
    return str(base)


def test_validate_models_with_cardinality_uses_one_event_loop(
    tmp_path: Path, pg_datasource: DatasourceConfig, capsys: pytest.CaptureFixture[str],
) -> None:
    store = _store(tmp_path / "store", pg_datasource)
    args = SimpleNamespace(
        datasource=pg_datasource.name, model=None, cardinality=True, persist_cardinality=False,
        format="json", force_clean=False, yes=False, storage=store, models_dir=None,
    )
    _run_validate_models(args)
    data = json.loads(capsys.readouterr().out)
    assert data["drift"] == []
    findings = data["cardinality"]["findings"]
    assert findings
    assert all(f["verdict"] != CardinalityVerdict.SCAN_FAILED.value for f in findings), findings
    orders = next(f for f in findings if f["model"] == "orders")
    assert orders["detected"] == "many_to_one"
