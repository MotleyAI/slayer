"""CLI ``slayer joins detect-cardinality`` (DEV-1688).

Handlers are imported and called directly with a ``SimpleNamespace`` args
object; output is captured with ``capsys`` (matching ``test_cli_import_osi.py``).
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from slayer.async_utils import run_sync
from slayer.cli import _run_joins_detect_cardinality
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def workspace():
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


def _seed(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT);
        CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER);
        INSERT INTO customers VALUES (1,'US'),(2,'EU');
        INSERT INTO orders VALUES (1,1),(2,1),(3,2);
        """
    )
    conn.commit()
    conn.close()


def _setup(workspace: Path) -> str:
    db = str(workspace / "d.db")
    _seed(db)
    store = str(workspace / "store")
    storage = YAMLStorage(base_dir=store)
    run_sync(storage.save_datasource(DatasourceConfig(name="ds", type="sqlite", database=db)))
    run_sync(
        storage.save_model(
            SlayerModel(
                name="customers",
                sql_table="customers",
                data_source="ds",
                columns=[
                    Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                    Column(name="region", sql="region", type=DataType.TEXT),
                ],
            )
        )
    )
    run_sync(
        storage.save_model(
            SlayerModel(
                name="orders",
                sql_table="orders",
                data_source="ds",
                columns=[
                    Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                    Column(name="customer_id", sql="customer_id", type=DataType.INT),
                ],
                joins=[
                    ModelJoin(
                        target_model="customers", join_pairs=[["customer_id", "id"]]
                    )
                ],
            )
        )
    )
    return store


def _args(store: str, **kw) -> SimpleNamespace:
    base = dict(
        datasource="ds",
        model=None,
        persist=False,
        format="text",
        storage=store,
        models_dir=None,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def test_detect_cardinality_text_output(workspace: Path, capsys) -> None:
    store = _setup(workspace)
    _run_joins_detect_cardinality(_args(store))
    out = capsys.readouterr().out
    assert "orders" in out
    assert "many_to_one" in out


def test_detect_cardinality_json_output(workspace: Path, capsys) -> None:
    store = _setup(workspace)
    _run_joins_detect_cardinality(_args(store, format="json"))
    out = capsys.readouterr().out
    data = json.loads(out)
    finding = next(f for f in data["findings"] if f["model"] == "orders")
    assert finding["detected"] == "many_to_one"


def test_detect_cardinality_persist_writes(workspace: Path, capsys) -> None:
    store = _setup(workspace)
    _run_joins_detect_cardinality(_args(store, persist=True))
    storage = YAMLStorage(base_dir=store)
    orders = run_sync(storage.get_model("orders", data_source="ds"))
    assert orders.joins[0].cardinality is not None
