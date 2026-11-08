"""Shared fixtures for empty cells taking the aggregation's empty value.

Graph (datasource ``test``): ``orders`` → ``customers`` → ``regions``, all many-to-one.

* East (3) has no customers; West (4) has customers (Eve, Grace) but no orders.
* Frank (6), Eve (5), Grace (7) have no orders; Bob's only order is ``bad``.
* Signup months: Dec-23 Frank · Jan Alice, Bob · Feb Carol · (Mar gap) · Apr Dave, Eve ·
  May Grace · Jun Heidi.

Hand-computed expectations (orders counted per customer, per region, per signup month):
``ORDERS_BY_CUSTOMER``, ``ORDERS_BY_REGION``, ``CUSTOMERS_BY_REGION``, ``ORDERS_BY_MONTH``.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Sequence

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Aggregation, Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import seeded_exec_engine

# (id, name)
REGIONS_ROWS = [(1, "North"), (2, "South"), (3, "East"), (4, "West")]
# (id, name, region_id, signup_date)
CUSTOMERS_ROWS = [
    (1, "Alice", 1, "2024-01-05"),
    (2, "Bob", 1, "2024-01-20"),
    (3, "Carol", 2, "2024-02-10"),
    (4, "Dave", 2, "2024-04-03"),
    (5, "Eve", 4, "2024-04-15"),
    (6, "Frank", 1, "2023-12-10"),
    (7, "Grace", 4, "2024-05-10"),
    (8, "Heidi", 2, "2024-06-12"),
]
# (id, customer_id, amount, status, order_date)
ORDERS_ROWS = [
    (1, 1, 10.0, "ok", "2024-01-10"),
    (2, 1, 20.0, "bad", "2024-02-15"),
    (3, 2, 5.0, "bad", "2024-01-25"),
    (4, 3, 7.0, "ok", "2024-02-12"),
    (5, 3, 8.0, "bad", "2024-02-20"),
    (6, 3, 9.0, "ok", "2024-03-01"),
    (7, 4, 30.0, "ok", "2024-04-05"),
    (8, 8, 11.0, "ok", "2024-06-15"),
    (9, 8, 12.0, "bad", "2024-06-20"),
]

CHILDLESS = ("Eve", "Frank", "Grace")
ORDERS_BY_CUSTOMER = {
    "Alice": 2, "Bob": 1, "Carol": 3, "Dave": 1, "Eve": 0, "Frank": 0, "Grace": 0, "Heidi": 2,
}
SUM_BY_CUSTOMER: Dict[str, Optional[float]] = {
    "Alice": 30.0, "Bob": 5.0, "Carol": 24.0, "Dave": 30.0,
    "Eve": None, "Frank": None, "Grace": None, "Heidi": 23.0,
}
AVG_BY_CUSTOMER: Dict[str, Optional[float]] = {
    "Alice": 15.0, "Bob": 5.0, "Carol": 8.0, "Dave": 30.0,
    "Eve": None, "Frank": None, "Grace": None, "Heidi": 11.5,
}
#: ``last(orders.amount)`` ranked by ``order_date``.
LAST_BY_CUSTOMER: Dict[str, Optional[float]] = {
    "Alice": 20.0, "Bob": 5.0, "Carol": 9.0, "Dave": 30.0,
    "Eve": None, "Frank": None, "Grace": None, "Heidi": 12.0,
}
REGION_OF = {
    "Alice": "North", "Bob": "North", "Frank": "North",
    "Carol": "South", "Dave": "South", "Heidi": "South",
    "Eve": "West", "Grace": "West",
}
ORDERS_BY_REGION = {"North": 3, "South": 6, "East": 0, "West": 0}
CUSTOMERS_BY_REGION = {"North": 3, "South": 3, "East": 0, "West": 2}
#: Per-customer order count averaged over every customer (childless ones as 0).
AVG_ORDERS_PER_CUSTOMER = 9 / 8
AVG_ORDERS_PER_CUSTOMER_BY_REGION = {"North": 1.0, "South": 2.0, "West": 0.0}
#: Orders of customers who signed up in each month (no March bucket).
ORDERS_BY_MONTH = {
    "2023-12": 0, "2024-01": 3, "2024-02": 3, "2024-04": 1, "2024-05": 0, "2024-06": 2,
}
CUMSUM_BY_MONTH = {
    "2023-12": 0, "2024-01": 3, "2024-02": 6, "2024-04": 7, "2024-05": 7, "2024-06": 9,
}
#: ``time_shift(…, -1)``: NULL where the prior month is absent (Nov-23, Mar-24).
SHIFTED_BY_MONTH: Dict[str, Optional[int]] = {
    "2023-12": None, "2024-01": 0, "2024-02": 3, "2024-04": None, "2024-05": 1, "2024-06": 0,
}
CHANGE_BY_MONTH: Dict[str, Optional[int]] = {
    "2023-12": None, "2024-01": 3, "2024-02": 0, "2024-04": None, "2024-05": -1, "2024-06": 2,
}
#: ``not orders.status = 'bad'``: at least one non-bad order (Bob's only order is bad).
HAS_NON_BAD_ORDER = ("Alice", "Carol", "Dave", "Heidi")


def query(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate(kw)


def m(formula: str, name: str) -> Dict[str, str]:
    return {"formula": formula, "name": name}


# --------------------------------------------------------------------------- #
# Models.
# --------------------------------------------------------------------------- #
def regions_model(*, aggregations: Sequence[Aggregation] = ()) -> SlayerModel:
    return SlayerModel(
        name="regions", data_source="test", sql_table="regions",
        aggregations=list(aggregations),
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )


def customers_model(*, aggregations: Sequence[Aggregation] = ()) -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        default_time_dimension="signup_date",
        aggregations=list(aggregations),
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="region_id", type=DataType.INT),
            Column(name="signup_date", type=DataType.DATE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


def orders_model(*, aggregations: Sequence[Aggregation] = ()) -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        default_time_dimension="order_date",
        aggregations=list(aggregations),
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="order_date", type=DataType.DATE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


def dev1994_models() -> List[SlayerModel]:
    return [regions_model(), customers_model(), orders_model()]


# --------------------------------------------------------------------------- #
# Engines.
# --------------------------------------------------------------------------- #
def seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        cur = con.cursor()
        cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT)")
        cur.executemany("INSERT INTO regions VALUES (?,?)", REGIONS_ROWS)
        cur.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, region_id INTEGER, "
            "signup_date TEXT)")
        cur.executemany("INSERT INTO customers VALUES (?,?,?,?)", CUSTOMERS_ROWS)
        cur.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL, "
            "status TEXT, order_date TEXT)")
        cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", ORDERS_ROWS)


def seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    try:
        con.execute("CREATE TABLE regions (id INTEGER, name VARCHAR)")
        con.executemany("INSERT INTO regions VALUES (?,?)", REGIONS_ROWS)
        con.execute(
            "CREATE TABLE customers (id INTEGER, name VARCHAR, region_id INTEGER, "
            "signup_date DATE)")
        con.executemany("INSERT INTO customers VALUES (?,?,?,?)", CUSTOMERS_ROWS)
        con.execute(
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount DOUBLE, "
            "status VARCHAR, order_date DATE)")
        con.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", ORDERS_ROWS)
    finally:
        con.close()


@asynccontextmanager
async def dev1994_engine(
    dialect: str, *, models: Optional[List[SlayerModel]] = None,
) -> AsyncGenerator[SlayerQueryEngine]:
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = seed_duckdb if dialect == "duckdb" else seed_sqlite
    async with seeded_exec_engine(
        dialect=dialect, seed=seed, models=dev1994_models() if models is None else models,
    ) as (engine, _):
        yield engine


# --------------------------------------------------------------------------- #
# Result helpers.
# --------------------------------------------------------------------------- #
def col(columns: List[str], name: str) -> str:
    """The unique result column whose last segment is ``name``."""
    (hit,) = [c for c in columns if c == name or c.endswith(f".{name}")]
    return hit


def by(rows: List[Dict[str, Any]], columns: List[str], *, key: str, value: str) -> Dict[Any, Any]:
    k, v = col(columns, key), col(columns, value)
    return {r[k]: r[v] for r in rows}


def month(value: Any) -> str:
    """``YYYY-MM`` of a time-bucket cell (string or date/datetime)."""
    return str(value)[:7]
