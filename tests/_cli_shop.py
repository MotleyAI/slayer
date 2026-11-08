"""A join-less two-model SQLite shop in YAML storage, for CLI surface tests."""

from __future__ import annotations

import json
from pathlib import Path

from slayer.async_utils import run_sync
from slayer.core.enums import DataType
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage

SHOP_DS = "shop"

README_QUERY = json.dumps({
    "source_model": "orders",
    "measures": ["sum(order_total)", "change_pct(sum(order_total))"],
    "dimensions": ["stores.name", "month(ordered_at)"],
})

TOTAL_QUERY = json.dumps({"source_model": "orders", "measures": ["sum(order_total)"]})


def build_shop_storage(base: Path) -> str:
    """Storage dir whose ``orders`` (currency ``order_total``, sum 193647.33) has no join to ``stores``."""
    db = base / "shop.db"
    with transaction(db) as con:
        con.executescript(
            "CREATE TABLE stores (id INTEGER PRIMARY KEY, name TEXT);"
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, store_id INTEGER, status TEXT,"
            " ordered_at TEXT, order_total REAL);"
            "INSERT INTO stores VALUES (1, 'Philly');"
            "INSERT INTO orders VALUES (1, 1, 'paid', '2024-01-05', 193000.0),"
            " (2, 1, 'open', '2024-01-06', 647.33);"
        )
    storage = YAMLStorage(base_dir=str(base / "store"))

    async def _seed() -> None:
        await storage.save_datasource(DatasourceConfig(name=SHOP_DS, type="sqlite", database=str(db)))
        await storage.save_model(SlayerModel(
            name="stores", sql_table="stores", data_source=SHOP_DS,
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="name", type=DataType.TEXT),
            ],
        ))
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source=SHOP_DS,
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="store_id", type=DataType.INT),
                Column(name="status", type=DataType.TEXT),
                Column(name="ordered_at", type=DataType.TIMESTAMP),
                Column(
                    name="order_total", type=DataType.DOUBLE,
                    format=NumberFormat(type=NumberFormatType.CURRENCY, precision=2),
                ),
            ],
        ))

    run_sync(_seed())
    return storage.base_dir
