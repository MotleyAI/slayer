"""Shared fixtures for DEV-2046 — boolean aggregation inputs.

Underscore-prefixed so pytest skips collection. ``orders.customer_id →
customers.id`` is many-to-one; ``big_order`` is the derived ``amount > 15``.

orders (id, customer_id, region, status, ordered_at, amount, flag):
   1  100  east  ok    2025-01-10    10  true
   2  100  east  hold  2025-02-10    20  true
   3  200  east  ok    2025-01-20    30  false
   4  200  west  void  2025-02-20  NULL  NULL
   5  200  west  ok    2025-03-10  NULL  false
customers (id, name, vip, score):  100 A true NULL | 200 B false NULL | 300 C NULL NULL (no orders)

flag: sum 2, avg 0.5, min false, max true, count 4, count_distinct 2.
By region: east sum 2 / avg 2/3 / min false / max true; west sum 0 / avg 0 / min false / max false.
amount > 15 (= big_order): sum 2, avg 2/3, count 3.  status in ('ok', 'hold'): sum 4.
By customer: 100 sum 2 / max true; 200 sum 0 / max false; 300 sum NULL (cross-model).
By month (Jan rows 1, 3; Feb 2, 4; Mar 5): sum 1 / 1 / 0, cumsum 1 / 2 / 2.
Trailing 90d by month (bucket end exclusive): Jan 1, Feb 2, Mar 2.
count(amount) per customer: 100 → 2, 200 → 1.
Associate by region (distinct customers' vip): east {100, 200} → 1, west {200} → 0.
"""

from __future__ import annotations

from typing import AsyncIterator, List, Optional

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.format import NumberFormat
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction

from tests._engine_helpers import seeded_exec_engine


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
def orders_model(*, data_source: str = "test", flag_format: Optional[NumberFormat] = None) -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source=data_source,
        default_time_dimension="ordered_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="region", type=DataType.TEXT),
            Column(name="status", type=DataType.TEXT),
            Column(name="ordered_at", type=DataType.DATE),
            Column(name="amount", type=DataType.INT),
            Column(name="flag", type=DataType.BOOLEAN, format=flag_format),
            Column(name="big_order", type=DataType.BOOLEAN, sql="amount > 15"),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


def customers_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source=data_source,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="vip", type=DataType.BOOLEAN),
            Column(name="score", type=DataType.INT),
        ],
    )


def dev2046_models(*, data_source: str = "test") -> List[SlayerModel]:
    return [orders_model(data_source=data_source), customers_model(data_source=data_source)]


# --------------------------------------------------------------------------- #
# Query shorthands
# --------------------------------------------------------------------------- #
def m(formula: str, name: str = "v") -> ModelMeasure:
    return ModelMeasure(formula=formula, name=name)


def orders_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def month_td() -> List[TimeDimension]:
    return [TimeDimension(dimension=ColumnRef(name="ordered_at"), granularity=TimeGranularity.MONTH)]


# --------------------------------------------------------------------------- #
# Oracles (derived in the module docstring)
# --------------------------------------------------------------------------- #
FLAG_TOTALS = {"sum": 2, "avg": 0.5, "min": False, "max": True, "count": 4, "count_distinct": 2}
FLAG_BY_REGION = {
    "sum": {"east": 2, "west": 0},
    "avg": {"east": 2 / 3, "west": 0.0},
    "min": {"east": False, "west": False},
    "max": {"east": True, "west": False},
}
GT15_TOTALS = {"sum": 2, "avg": 2 / 3, "count": 3}
STATUS_IN_SUM = 4
FLAG_SUM_BY_CUSTOMER = {100: 2, 200: 0}
FLAG_MAX_BY_CUSTOMER = {100: True, 200: False}
CROSS_MODEL_FLAG_SUM_BY_NAME = {"A": 2, "B": 0, "C": None}
JAN, FEB, MAR = "2025-01", "2025-02", "2025-03"
FLAG_SUM_BY_MONTH = {JAN: 1, FEB: 1, MAR: 0}
FLAG_CUMSUM_BY_MONTH = {JAN: 1, FEB: 2, MAR: 2}
FLAG_90D_BY_MONTH = {JAN: 1, FEB: 2, MAR: 2}
VIP_ASSOC_SUM_BY_REGION = {"east": 1, "west": 0}


# --------------------------------------------------------------------------- #
# Seed + engine
# --------------------------------------------------------------------------- #
_ORDER_ROWS = [
    # (id, customer_id, region, status, ordered_at, amount, flag)
    (1, 100, "east", "ok", "2025-01-10", 10, True),
    (2, 100, "east", "hold", "2025-02-10", 20, True),
    (3, 200, "east", "ok", "2025-01-20", 30, False),
    (4, 200, "west", "void", "2025-02-20", None, None),
    (5, 200, "west", "ok", "2025-03-10", None, False),
]
_CUSTOMER_ROWS = [(100, "A", True, None), (200, "B", False, None), (300, "C", None, None)]

#: Per-dialect (text type, boolean type, true, false) spellings for the seed DDL / literals.
_SPELLINGS = {
    "sqlite": ("TEXT", "BOOLEAN", "1", "0"),
    "duckdb": ("VARCHAR", "BOOLEAN", "TRUE", "FALSE"),
    "postgres": ("TEXT", "BOOLEAN", "TRUE", "FALSE"),
    "tsql": ("NVARCHAR(50)", "BIT", "1", "0"),
}


def _literal(value, *, dialect: str) -> str:
    _, _, true, false = _SPELLINGS[dialect]
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return true if value else false
    if isinstance(value, str):
        return f"'{value}'"
    return str(value)


def seed_statements(dialect: str) -> List[str]:
    """CREATE + INSERT statements for both tables, spelled for ``dialect``."""
    text, boolean, _, _ = _SPELLINGS[dialect]

    def insert(table: str, rows: list) -> str:
        values = ", ".join(
            "(" + ", ".join(_literal(v, dialect=dialect) for v in row) + ")" for row in rows
        )
        return f"INSERT INTO {table} VALUES {values}"

    return [
        f"CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, region {text}, "
        f"status {text}, ordered_at DATE, amount INTEGER, flag {boolean})",
        insert("orders", _ORDER_ROWS),
        f"CREATE TABLE customers (id INTEGER PRIMARY KEY, name {text}, vip {boolean}, score INTEGER)",
        insert("customers", _CUSTOMER_ROWS),
    ]


def seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        for stmt in seed_statements("sqlite"):
            con.execute(stmt)


def seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    for stmt in seed_statements("duckdb"):
        con.execute(stmt)
    con.close()


async def make_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = seed_duckdb if dialect == "duckdb" else seed_sqlite
    async with seeded_exec_engine(dialect=dialect, seed=seed, models=dev2046_models()) as (engine, _):
        yield engine


# --------------------------------------------------------------------------- #
# Result readers
# --------------------------------------------------------------------------- #
def month_key(value) -> str:
    """Stable per-month key across SQLite text and DuckDB date values."""
    return str(value)[:7]


def as_bool(value) -> Optional[bool]:
    """SQLite has no boolean type: 0 / 1 read as False / True."""
    return None if value is None else bool(value)


def by_dim(resp, dim: str, name: str = "v", *, model: str = "orders") -> dict:
    out = {r[f"{model}.{dim}"]: r[f"{model}.{name}"] for r in resp.data}
    assert len(out) == len(resp.data), f"duplicate result rows for one {dim}"
    return out


def by_month(resp, name: str = "v") -> dict:
    tkey = next(k for k in resp.columns if "ordered_at" in k)
    out = {month_key(r[tkey]): r[f"orders.{name}"] for r in resp.data}
    assert len(out) == len(resp.data), "duplicate result rows for one month"
    return out


def single(resp, name: str = "v", *, model: str = "orders"):
    assert len(resp.data) == 1, resp.data
    return resp.data[0][f"{model}.{name}"]
