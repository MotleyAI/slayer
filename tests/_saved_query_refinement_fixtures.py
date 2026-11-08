"""Seed, models and saved queries for the saved-query refinement tests (datasource ``test``)."""

from __future__ import annotations

import json
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Tuple, cast

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import seeded_exec_engine

# (id, customer_id, region, status, amount, ordered_at)
ORDERS_ROWS = [
    (1, "c1", "US", "paid", 100.0, "2025-01-05"),
    (2, "c1", "US", "refunded", 40.0, "2025-01-20"),
    (3, "c2", "EU", "paid", 70.0, "2025-02-03"),
    (4, "c3", "EU", "paid", 35.0, "2025-02-17"),
    (5, "c2", "US", "paid", 50.0, "2025-03-09"),
]

REVENUE = {"formula": "sum(amount)", "name": "revenue"}
MONTH_TD = {"dimension": "ordered_at", "granularity": "month"}
Q1_RANGE = ["2025-01-01", "2025-02-28"]

MONTHLY_REVENUE: Dict[str, Any] = {
    "source_model": "orders",
    "time_dimensions": [MONTH_TD],
    "measures": [REVENUE],
    "filters": ["status = 'paid'"],
}
MONTHLY_REVENUE_Q1: Dict[str, Any] = {
    "source_model": "orders",
    "time_dimensions": [{**MONTH_TD, "date_range": Q1_RANGE}],
    "measures": [REVENUE],
    "filters": ["status = 'paid'"],
    "order": [{"column": "revenue", "direction": "desc"}],
    "limit": 2,
}
AVG_CUSTOMER_REVENUE: List[Dict[str, Any]] = [
    {
        "name": "per_customer", "source_model": "orders",
        "dimensions": ["customer_id", "region"], "measures": [REVENUE],
        "filters": ["status = 'paid'"],
    },
    {"source_model": "per_customer", "measures": [{"formula": "avg(revenue)", "name": "avg_revenue"}]},
]
REVENUE_BY_STATUS: Dict[str, Any] = {
    "source_model": "orders", "dimensions": ["region"], "measures": [REVENUE],
    "filters": ["status = '{status}'"],
}

MONTHLY = {"2025-01": 100.0, "2025-02": 105.0, "2025-03": 50.0}
BY_REGION_MONTH = {("US", "2025-01"): 100.0, ("EU", "2025-02"): 105.0, ("US", "2025-03"): 50.0}


def orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        default_time_dimension="ordered_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.TEXT),
            Column(name="region", type=DataType.TEXT),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="ordered_at", type=DataType.DATE),
        ],
    )


def seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        con.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id TEXT, region TEXT, "
            "status TEXT, amount REAL, ordered_at TEXT)")
        con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?)", ORDERS_ROWS)


def seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    try:
        con.execute(
            "CREATE TABLE orders (id INTEGER, customer_id VARCHAR, region VARCHAR, "
            "status VARCHAR, amount DOUBLE, ordered_at DATE)")
        con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?)", ORDERS_ROWS)
    finally:
        con.close()


DESCRIPTIONS = {
    "monthly_revenue": "Paid revenue per calendar month",
    "monthly_revenue_q1": "Top paid months of the first quarter",
    "avg_customer_revenue": "Average paid revenue per customer",
    "revenue_by_status": "Revenue per region for one order status",
}


async def save_saved_queries(engine: SlayerQueryEngine) -> None:
    """Save the four saved queries through ``create_model_from_query``."""
    for name, query in (
        ("monthly_revenue", MONTHLY_REVENUE),
        ("monthly_revenue_q1", MONTHLY_REVENUE_Q1),
        ("avg_customer_revenue", AVG_CUSTOMER_REVENUE),
    ):
        await engine.create_model_from_query(query=query, name=name, description=DESCRIPTIONS[name])
    await engine.create_model_from_query(
        query=REVENUE_BY_STATUS, name="revenue_by_status", variables={"status": "paid"},
        description=DESCRIPTIONS["revenue_by_status"],
    )


async def populate_refine_storage(storage: StorageBackend, *, db_path: str) -> None:
    """Register the seeded SQLite ``db_path``, the ``orders`` model and the saved queries in ``storage``."""
    await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=db_path))
    await storage.save_model(orders_model())
    engine = SlayerQueryEngine(storage=storage)
    try:
        await save_saved_queries(engine)
    finally:
        await engine.aclose()


async def build_refine_storage(base_dir: str) -> YAMLStorage:
    """A YAML storage under ``base_dir/store`` over a freshly seeded SQLite file."""
    db_path = os.path.join(base_dir, "seed.db")
    seed_sqlite(db_path)
    storage = YAMLStorage(base_dir=os.path.join(base_dir, "store"))
    await populate_refine_storage(storage, db_path=db_path)
    return storage


@asynccontextmanager
async def refine_engine(dialect: str) -> AsyncGenerator[Tuple[SlayerQueryEngine, str]]:
    """``(engine, db_path)`` over the seeded ``orders`` model with the saved queries stored."""
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = seed_duckdb if dialect == "duckdb" else seed_sqlite
    async with seeded_exec_engine(dialect=dialect, seed=seed, models=[orders_model()]) as (engine, db_path):
        await save_saved_queries(engine)
        yield engine, db_path


async def call_tool_json(server: Any, *, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """JSON payload of an MCP tool call (FastMCP annotates the pre-conversion result type)."""
    blocks, _ = cast(Any, await server.call_tool(name=name, arguments=arguments))
    return json.loads(blocks[0].text)


def month(value: Any) -> str:
    """``YYYY-MM`` of a time-bucket cell (string or date/datetime)."""
    return str(value)[:7]


def by_month(rows: List[Dict[str, Any]], *, value: str = "orders.revenue",
             key: str = "orders.ordered_at") -> Dict[str, Any]:
    return {month(r[key]): r[value] for r in rows}
