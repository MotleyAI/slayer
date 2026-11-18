"""Shared fixtures for the cross-stage time-dimension tests: a seeded sqlite/duckdb engine from declarative table specs so executed-value tests run on both backends."""

from __future__ import annotations

import contextlib
from typing import Any, Iterable, Optional

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
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


def _orders_columns(extra: Iterable[DatasetField] = ()) -> list[DatasetField]:
    cols = [
        DatasetField(name="id", sql="id", datatype=DataType.DOUBLE, primary_key=True),
        DatasetField(name="customer_id", sql="customer_id", datatype=DataType.DOUBLE),
        DatasetField(name="amount", sql="amount", datatype=DataType.DOUBLE),
        DatasetField(name="region", sql="region", datatype=DataType.TEXT),
        DatasetField(name="created_at", sql="created_at", datatype=DataType.TIMESTAMP),
        DatasetField(name="shipped_at", sql="shipped_at", datatype=DataType.TIMESTAMP),
    ]
    cols.extend(extra)
    return cols


def orders_dataset(
    *,
    data_source: str = "ds",
    extra_columns: Iterable[DatasetField] = (),
    default_time_dimension: Optional[str] = "created_at",
) -> SlayerDataset:
    """The single-table ``orders`` model (id, customer_id, amount, region, created_at, shipped_at)."""
    return SlayerDataset(
        name="orders",
        sql_table="orders",
        data_source=data_source,
        default_time_dimension=default_time_dimension,
        fields=_orders_columns(extra_columns),
    )


def orders_table_spec(rows: list[tuple], *, extra_columns: Iterable[tuple[str, str]] = ()) -> dict:
    """Table spec for the ``orders`` seed; ``rows`` column order must match."""
    columns = [
        ("id", "INT"), ("customer_id", "INT"), ("amount", "DOUBLE"),
        ("region", "TEXT"), ("created_at", "TIMESTAMP"), ("shipped_at", "TIMESTAMP"),
    ]
    columns.extend(extra_columns)
    return {"name": "orders", "columns": columns, "rows": rows}


# One order per month, Jan–Apr 2025 (id, customer_id, amount, region, created_at, shipped_at).
MONTHLY_ROWS = [
    (1, 100, 100.0, "W", "2025-01-10", "2025-02-10"),
    (2, 101, 200.0, "E", "2025-02-05", "2025-03-05"),
    (3, 102, 300.0, "N", "2025-03-15", "2025-04-15"),
    (4, 103, 400.0, "S", "2025-04-20", "2025-05-20"),
]

# Inner stage: monthly revenue over ``orders.created_at`` (shared stage-axis fixture).
MONTHLY_INNER = SlayerQuery.model_validate({
    "name": "s1", "population": "orders",
    "time_dimensions": [TimeDimension(field="created_at", granularity=TimeGranularity.MONTH)],
    "metrics": [{"expression": "sum(amount)", "name": "rev"}],
})


# --- orders → customers → regions join chain (multi-hop flat-name tests) ---

def region_chain_datasets(*, data_source: str = "ds") -> list[SlayerDataset]:
    regions = SlayerDataset(
        name="regions", sql_table="regions", data_source=data_source,
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.DOUBLE, primary_key=True),
            DatasetField(name="name", sql="name", datatype=DataType.TEXT),
            DatasetField(name="last_activity_at", sql="last_activity_at", datatype=DataType.TIMESTAMP),
        ],
    )
    customers = SlayerDataset(
        name="customers", sql_table="customers", data_source=data_source,
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.DOUBLE, primary_key=True),
            DatasetField(name="region_id", sql="region_id", datatype=DataType.DOUBLE),
        ],
        relationships=[DatasetRelationship(to="regions", join_pairs=[["region_id", "id"]])],
    )
    orders = SlayerDataset(
        name="orders", sql_table="orders", data_source=data_source,
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.DOUBLE, primary_key=True),
            DatasetField(name="customer_id", sql="customer_id", datatype=DataType.DOUBLE),
        ],
        relationships=[DatasetRelationship(to="customers", join_pairs=[["customer_id", "id"]])],
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
    backend: str, *, base_dir: str, db_path: str, tables: list[dict], datasets: list[SlayerDataset],
) -> SlayerQueryEngine:
    """Seed ``tables`` into a fresh ``backend`` DB and return an engine over ``models``.

    ``tables`` items: ``{"name", "columns": [(col, kind)], "rows": [tuple, ...]}``.
    DuckDB is skipped (``importorskip``) when the driver is absent.

    DEV-1943: not folded onto ``seeded_exec_engine`` — the caller supplies
    ``base_dir``/``db_path`` and a dynamic ``tables`` spec, so it does not fit
    the tempdir-owning context; its sqlite seeding already routes through the door.
    """
    types = _SQLITE_TYPE if backend == "sqlite" else _DUCKDB_TYPE
    con_cm: Any
    if backend == "sqlite":
        con_cm = transaction(db_path)
    else:
        duckdb = pytest.importorskip("duckdb")
        con_cm = contextlib.closing(duckdb.connect(db_path))
    with con_cm as con:
        for tbl in tables:
            col_ddl = ", ".join(f"{name} {types[kind]}" for name, kind in tbl["columns"])
            con.execute(f"CREATE TABLE {tbl['name']} ({col_ddl})")
            placeholders = ", ".join(["?"] * len(tbl["columns"]))
            con.executemany(
                f"INSERT INTO {tbl['name']} VALUES ({placeholders})", tbl["rows"],
            )
    storage = YAMLStorage(base_dir=base_dir)
    await storage.save_datasource(
        DatasourceConfig(name=datasets[0].data_source, type=backend, database=db_path),
    )
    for model in datasets:
        await storage.save_dataset(model)
    return SlayerQueryEngine(storage=storage)
