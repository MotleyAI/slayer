"""Shared fixtures/helpers for the DEV-1471 cross-stage time-dimension tests.

Underscore-prefixed so pytest skips collection while ``from tests._dev1471_fixtures
import ...`` still works. Builds a seeded sqlite or duckdb engine from declarative
table specs so every executed-value test runs identically on both backends
(spec: SQLite + DuckDB), avoiding per-file engine-builder duplication.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Iterable, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

BACKENDS = ("sqlite", "duckdb")

# Logical kind → concrete column type per backend. SQLite stores dates as TEXT
# (numeric affinity), DuckDB natively.
_SQLITE_TYPE = {"INT": "INTEGER", "DOUBLE": "REAL", "TIMESTAMP": "TEXT", "TEXT": "TEXT"}
_DUCKDB_TYPE = {"INT": "INTEGER", "DOUBLE": "DOUBLE", "TIMESTAMP": "TIMESTAMP", "TEXT": "VARCHAR"}


def date_str(value: Any) -> Optional[str]:
    """First 10 chars of a date/timestamp value (``2025-03-20``) — backend-agnostic
    (SQLite returns text, DuckDB a ``datetime``)."""
    return None if value is None else str(value)[:10]


def one_value(row: dict, *, exclude: Iterable[str] = ()) -> Any:
    """The single measure value in ``row`` once the excluded dimension keys are dropped."""
    vals = [v for k, v in row.items() if k not in set(exclude)]
    assert len(vals) == 1, f"expected one value, got {row}"
    return vals[0]


def _orders_columns(extra: Iterable[Column] = ()) -> list[Column]:
    cols = [
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
        Column(name="amount", sql="amount", type=DataType.DOUBLE),
        Column(name="region", sql="region", type=DataType.TEXT),
        Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        Column(name="shipped_at", sql="shipped_at", type=DataType.TIMESTAMP),
    ]
    cols.extend(extra)
    return cols


def orders_model(
    *,
    data_source: str = "ds",
    extra_columns: Iterable[Column] = (),
    default_time_dimension: Optional[str] = "created_at",
) -> SlayerModel:
    """The single-table ``orders`` model (id, customer_id, amount, region, created_at, shipped_at)."""
    return SlayerModel(
        name="orders",
        sql_table="orders",
        data_source=data_source,
        default_time_dimension=default_time_dimension,
        columns=_orders_columns(extra_columns),
    )


def orders_table_spec(rows: list[tuple], *, extra_columns: Iterable[tuple[str, str]] = ()) -> dict:
    """Table spec for the ``orders`` seed; ``rows`` column order must match."""
    columns = [
        ("id", "INT"), ("customer_id", "INT"), ("amount", "DOUBLE"),
        ("region", "TEXT"), ("created_at", "TIMESTAMP"), ("shipped_at", "TIMESTAMP"),
    ]
    columns.extend(extra_columns)
    return {"name": "orders", "columns": columns, "rows": rows}


# --- orders → customers → regions join chain (multi-hop flat-name tests) ---

def region_chain_models(*, data_source: str = "ds") -> list[SlayerModel]:
    regions = SlayerModel(
        name="regions", sql_table="regions", data_source=data_source,
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="last_activity_at", sql="last_activity_at", type=DataType.TIMESTAMP),
        ],
    )
    customers = SlayerModel(
        name="customers", sql_table="customers", data_source=data_source,
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )
    orders = SlayerModel(
        name="orders", sql_table="orders", data_source=data_source,
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )
    return [regions, customers, orders]


def region_chain_tables(
    *, regions: list[tuple], customers: list[tuple], orders: list[tuple]
) -> list[dict]:
    return [
        {"name": "regions",
         "columns": [("id", "INT"), ("name", "TEXT"), ("last_activity_at", "TIMESTAMP")],
         "rows": regions},
        {"name": "customers",
         "columns": [("id", "INT"), ("region_id", "INT")], "rows": customers},
        {"name": "orders",
         "columns": [("id", "INT"), ("customer_id", "INT")], "rows": orders},
    ]


async def make_engine(
    backend: str, *, base_dir: str, db_path: str, tables: list[dict], models: list[SlayerModel],
) -> SlayerQueryEngine:
    """Seed ``tables`` into a fresh ``backend`` DB and return an engine over ``models``.

    ``tables`` items: ``{"name", "columns": [(col, kind)], "rows": [tuple, ...]}``.
    DuckDB is skipped (``importorskip``) when the driver is absent.
    """
    types = _SQLITE_TYPE if backend == "sqlite" else _DUCKDB_TYPE
    if backend == "sqlite":
        con: Any = sqlite3.connect(db_path)
    else:
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(db_path)
    try:
        for tbl in tables:
            col_ddl = ", ".join(f"{name} {types[kind]}" for name, kind in tbl["columns"])
            con.execute(f"CREATE TABLE {tbl['name']} ({col_ddl})")
            placeholders = ", ".join(["?"] * len(tbl["columns"]))
            con.executemany(
                f"INSERT INTO {tbl['name']} VALUES ({placeholders})", tbl["rows"],
            )
        if backend == "sqlite":
            con.commit()
    finally:
        con.close()
    storage = YAMLStorage(base_dir=base_dir)
    await storage.save_datasource(
        DatasourceConfig(name=models[0].data_source, type=backend, database=db_path),
    )
    for model in models:
        await storage.save_model(model)
    return SlayerQueryEngine(storage=storage)
