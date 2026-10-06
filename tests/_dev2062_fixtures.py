"""Shared fixtures for DEV-2062 — a selected composite alongside a consumer containing it.

Underscore-prefixed so pytest skips collection. ``orders.customer_id →
customers.id`` is many-to-one; ``orders.aov`` is the saved ``sum(amount) / count(*)``.

orders (id, customer_id, region, order_date, amount):
   1  100  east   2025-01-05  10
   2  200  west   2025-01-20  30
   3  100  north  2025-02-10  10
   4  200  east   2025-03-03  30
   5  300  west   2025-03-15  45
   6  100  north  2025-03-28  15
customers (id, name, score):  100 A 2 | 200 B 5 | 300 C 3

By month: sum 40 / 10 / 90, count 2 / 1 / 3, aov 20 / 10 / 30.
aov: cumsum 20 / 30 / 60, lag - / 20 / 10, lead 10 / 30 / -, change - / -10 / 20,
change_pct - / -0.5 / 2, consecutive_periods(aov > 15) 1 / 0 / 1, rank desc 2 / 3 / 1.
cumsum(sum(amount)) 40 / 50 / 140 → / aov 2 / 5 / 14/3; cumsum(aov) + aov 40 / 40 / 90;
cumsum(aov * 2) 40 / 60 / 120.
max(customers.score) broadcasts its grand total 5 (month is not a customers attribute)
→ sum(amount) / max(customers.score) 8 / 2 / 18, cumsum 8 / 10 / 28.
Regions: east, north, west (one row each when grouped by region alone).
"""

from __future__ import annotations

from typing import AsyncIterator, List, Optional

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction

from tests._engine_helpers import seeded_exec_engine


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
AOV = "sum(amount) / count(*)"
CROSS_RATIO = "sum(amount) / max(customers.score)"


def orders_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source=data_source,
        default_time_dimension="order_date",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="region", type=DataType.TEXT),
            Column(name="order_date", type=DataType.DATE),
            Column(name="amount", type=DataType.INT),
        ],
        measures=[ModelMeasure(name="aov", formula=AOV)],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


def customers_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source=data_source,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="score", type=DataType.INT),
        ],
    )


def dev2062_models(*, data_source: str = "test") -> List[SlayerModel]:
    return [orders_model(data_source=data_source), customers_model(data_source=data_source)]


# --------------------------------------------------------------------------- #
# Query shorthands
# --------------------------------------------------------------------------- #
DATE_RANGE: List[Optional[str]] = ["2025-01-01", "2025-12-31"]
DATE_FILTER = "order_date >= '2025-01-01'"


def m(formula: str, name: str = "v") -> ModelMeasure:
    return ModelMeasure(formula=formula, name=name)


def orders_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def month_td(*, date_range: Optional[List[Optional[str]]] = None) -> List[TimeDimension]:
    return [TimeDimension(
        dimension=ColumnRef(name="order_date"), granularity=TimeGranularity.MONTH, date_range=date_range,
    )]


def monthly_q(*measures: ModelMeasure, **kw) -> SlayerQuery:
    kw.setdefault("time_dimensions", month_td())
    return orders_q(measures=list(measures), **kw)


# --------------------------------------------------------------------------- #
# Oracles (derived in the module docstring)
# --------------------------------------------------------------------------- #
JAN, FEB, MAR = "2025-01", "2025-02", "2025-03"
MONTHS = (JAN, FEB, MAR)
AOV_BY_MONTH = {JAN: 20, FEB: 10, MAR: 30}
AOV_CUMSUM_BY_MONTH = {JAN: 20, FEB: 30, MAR: 60}
CROSS_RATIO_BY_MONTH = {JAN: 8, FEB: 2, MAR: 18}
REGIONS = ("east", "north", "west")


# --------------------------------------------------------------------------- #
# Seed + engine
# --------------------------------------------------------------------------- #
_ORDER_ROWS = [
    (1, 100, "east", "2025-01-05", 10),
    (2, 200, "west", "2025-01-20", 30),
    (3, 100, "north", "2025-02-10", 10),
    (4, 200, "east", "2025-03-03", 30),
    (5, 300, "west", "2025-03-15", 45),
    (6, 100, "north", "2025-03-28", 15),
]
_CUSTOMER_ROWS = [(100, "A", 2), (200, "B", 5), (300, "C", 3)]


def _values(rows: list) -> str:
    return ", ".join(
        "(" + ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in row) + ")" for row in rows
    )


def seed_statements(text_type: str) -> List[str]:
    return [
        f"CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, region {text_type}, "
        f"order_date DATE, amount INTEGER)",
        f"INSERT INTO orders VALUES {_values(_ORDER_ROWS)}",
        f"CREATE TABLE customers (id INTEGER PRIMARY KEY, name {text_type}, score INTEGER)",
        f"INSERT INTO customers VALUES {_values(_CUSTOMER_ROWS)}",
    ]


def seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        for stmt in seed_statements("TEXT"):
            con.execute(stmt)


def seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    for stmt in seed_statements("VARCHAR"):
        con.execute(stmt)
    con.close()


async def make_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = seed_duckdb if dialect == "duckdb" else seed_sqlite
    async with seeded_exec_engine(dialect=dialect, seed=seed, models=dev2062_models()) as (engine, _):
        yield engine


# --------------------------------------------------------------------------- #
# Result readers
# --------------------------------------------------------------------------- #
def month_key(value) -> str:
    """Stable per-month key across SQLite text and DuckDB date values."""
    return str(value)[:7]


def by_month(resp, name: str = "v") -> dict:
    tkey = next(k for k in resp.columns if "order_date" in k)
    out = {month_key(r[tkey]): r[f"orders.{name}"] for r in resp.data}
    assert len(out) == len(resp.data), "duplicate result rows for one month"
    return out


def month_order(resp) -> List[str]:
    tkey = next(k for k in resp.columns if "order_date" in k)
    return [month_key(r[tkey]) for r in resp.data]


def approx_map(expected: dict) -> dict:
    """``expected`` with numeric values wrapped in ``pytest.approx`` (NULLs kept)."""
    return {k: (None if v is None else pytest.approx(v)) for k, v in expected.items()}
