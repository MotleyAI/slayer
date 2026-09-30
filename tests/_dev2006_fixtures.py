"""Shared fixtures for DEV-2006 — ranked / windowed re-aggregation operands.

Underscore-prefixed so pytest skips collection. One snapshot dataset seeded into
SQLite and DuckDB; ``account_snapshots.customer_id → customers.id`` is many-to-one.

account_snapshots (id, account_id, customer_id, snapshot_date, recorded_at, balance):
   1  10  100  2025-01-05  2025-01-05   100
   2  10  100  2025-01-25  2025-01-25   150
   3  10  100  2025-02-10  2025-02-10   160
   4  11  100  2025-01-10  2025-02-01    50
   5  11  100  2025-01-28  2025-01-29    70
   6  12  200  2025-01-15  2025-01-15    30
   7  13  200  2025-01-20  2025-01-20    40
   8  13  200  2025-02-05  2025-02-05  NULL
customers (id, name):  100 A | 200 B | 300 C (no snapshots)

Per (account, customer) — last / first by snapshot_date, last by recorded_at, max:
   10/100: last 160, first 100, last_rec 160, max 160
   11/100: last  70, first  50, last_rec  50, max  70
   12/200: last  30, first  30, last_rec  30, max  30
   13/200: last NULL, first 40, last_rec NULL, max 40
Summed per customer (NULL cells skipped): last 230 / 30; last_rec 210 / 30;
first 150 / 70; avg(last) 115 / 30; count(last) 2 / 1; min / max / median / count_distinct
of last 70 / 30, 160 / 30, 115 / 30, 2 / 1; last - first 80 / 0;
last + max 460 / 60; last + last_rec 440 / 60 (both-snapshot 460, both-recorded 420).
Global (partition [account] only): 160 + 70 + 30 = 260.

Per month (last within the month, by snapshot_date):
   Jan: 10 → 150, 11 → 70, 12 → 30, 13 → 40;  Feb: 10 → 160, 13 → NULL
   → (100, Jan) 220, (100, Feb) 160, (200, Jan) 70, (200, Feb) NULL;
   cumsum over month → 100: 220, 380; 200: 70, 70.

Trailing windows over snapshot_date (bucket end exclusive):
   30d  Jan [2025-01-02, 02-01)  Feb [2025-01-30, 03-01)
   60d  Jan [2024-12-02, 02-01)  Feb [2024-12-31, 03-01)
sum(balance, window='30d') by customer, month: (100, Jan) 370, (100, Feb) 160,
(200, Jan) 70, (200, Feb) NULL. Per (account, customer, month) cell, 30d + 60d:
Jan 10 → 250 + 250, 11 → 120 + 120, 12 → 30 + 30, 13 → 40 + 40;
Feb 10 → 160 + 410, 13 → NULL + 40 → summed (100, Jan) 740, (100, Feb) 570,
(200, Jan) 140, (200, Feb) NULL (both-30d Feb 320, both-60d Feb 820).
"""

from __future__ import annotations

from typing import AsyncIterator, List, Optional

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.storage.sqlite_conn import transaction

from tests._engine_helpers import seeded_exec_engine


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
def snapshots_model() -> SlayerModel:
    return SlayerModel(
        name="account_snapshots", sql_table="account_snapshots", data_source="test",
        default_time_dimension="snapshot_date",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="account_id", type=DataType.INT),
            Column(name="customer_id", type=DataType.INT),
            Column(name="snapshot_date", type=DataType.DATE),
            Column(name="recorded_at", type=DataType.DATE),
            Column(name="balance", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


def customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )


def dev2006_models() -> List[SlayerModel]:
    return [snapshots_model(), customers_model()]


def dev2006_bundle(*, root: str = "account_snapshots") -> ResolvedSourceBundle:
    models = {m.name: m for m in dev2006_models()}
    return ResolvedSourceBundle(
        dialect="duckdb", source_model=models[root],
        referenced_models=[m for n, m in models.items() if n != root],
    )


# --------------------------------------------------------------------------- #
# Query shorthands
# --------------------------------------------------------------------------- #
PB = "partition_by=[account_id, customer_id]"
PB_MONTH = "partition_by=[account_id, customer_id, snapshot_date]"
LAST = f"last(balance, {PB})"
FIRST = f"first(balance, {PB})"


def snap_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "account_snapshots")
    return SlayerQuery(**kw)


def m(formula: str, name: str = "v") -> ModelMeasure:
    return ModelMeasure(formula=formula, name=name)


def month_td(column: str = "snapshot_date") -> List[TimeDimension]:
    return [TimeDimension(dimension=ColumnRef(name=column), granularity=TimeGranularity.MONTH)]


# --------------------------------------------------------------------------- #
# Oracles (derived in the module docstring)
# --------------------------------------------------------------------------- #
JAN, FEB = "2025-01", "2025-02"
LAST_BY_CUSTOMER = {100: 230.0, 200: 30.0}
LAST_RECORDED_BY_CUSTOMER = {100: 210.0, 200: 30.0}
FIRST_BY_CUSTOMER = {100: 150.0, 200: 70.0}
AVG_LAST_BY_CUSTOMER = {100: 115.0, 200: 30.0}
COUNT_LAST_BY_CUSTOMER = {100: 2, 200: 1}
#: Outer aggregation → per-customer value over the last cells (NULL cells skipped).
LAST_FAMILY_BY_CUSTOMER = {
    "min": {100: 70.0, 200: 30.0},
    "max": {100: 160.0, 200: 30.0},
    "median": {100: 115.0, 200: 30.0},
    "count_distinct": {100: 2, 200: 1},
}
LAST_MINUS_FIRST_BY_CUSTOMER = {100: 80.0, 200: 0.0}
LAST_PLUS_MAX_BY_CUSTOMER = {100: 460.0, 200: 60.0}
LAST_PLUS_LAST_RECORDED_BY_CUSTOMER = {100: 440.0, 200: 60.0}
GLOBAL_LAST_BY_ACCOUNT = 260.0
LAST_BY_CUSTOMER_MONTH = {(100, JAN): 220.0, (100, FEB): 160.0, (200, JAN): 70.0, (200, FEB): None}
CUMSUM_LAST_BY_CUSTOMER_MONTH = {(100, JAN): 220.0, (100, FEB): 380.0, (200, JAN): 70.0, (200, FEB): 70.0}
WINDOW_30D_BY_CUSTOMER_MONTH = {(100, JAN): 370.0, (100, FEB): 160.0, (200, JAN): 70.0, (200, FEB): None}
WINDOW_30D_PLUS_60D_BY_CUSTOMER_MONTH = {
    (100, JAN): 740.0, (100, FEB): 570.0, (200, JAN): 140.0, (200, FEB): None,
}
#: Rows per customer (count(*)): 100 → 5, 200 → 3.
LAST_PER_ROW_BY_CUSTOMER = {100: 46.0, 200: 10.0}
#: Ungrouped weighted_avg(balance, weight=<customer's summed last>) = SUM(v·w) / SUM(w):
#: (530·230 + 70·30) / (5·230 + 3·30); the max-weighted twin is 93.24.
WEIGHTED_AVG_GLOBAL = 100.0
#: CASE WHEN <customer's summed last> > 100 THEN 'big' ELSE 'small' END → sum(balance).
BALANCE_BY_BAND = {"big": 530.0, "small": 70.0}
#: time_shift(<x>, -1) by customer, month: Feb reads Jan (last 70 / 40; 30d 370 / 70).
SHIFTED_LAST_BY_CUSTOMER_MONTH = {(100, JAN): None, (100, FEB): 70.0, (200, JAN): None, (200, FEB): 40.0}
SHIFTED_30D_BY_CUSTOMER_MONTH = {(100, JAN): None, (100, FEB): 370.0, (200, JAN): None, (200, FEB): 70.0}
CROSS_MODEL_SUM_BY_NAME ={"A": 230.0, "B": 30.0, "C": None}
CROSS_MODEL_COUNT_BY_NAME = {"A": 2, "B": 1, "C": 0}


# --------------------------------------------------------------------------- #
# Seed + engine
# --------------------------------------------------------------------------- #
_SNAPSHOT_ROWS = [
    # (id, account_id, customer_id, snapshot_date, recorded_at, balance)
    (1, 10, 100, "2025-01-05", "2025-01-05", 100.0),
    (2, 10, 100, "2025-01-25", "2025-01-25", 150.0),
    (3, 10, 100, "2025-02-10", "2025-02-10", 160.0),
    (4, 11, 100, "2025-01-10", "2025-02-01", 50.0),
    (5, 11, 100, "2025-01-28", "2025-01-29", 70.0),
    (6, 12, 200, "2025-01-15", "2025-01-15", 30.0),
    (7, 13, 200, "2025-01-20", "2025-01-20", 40.0),
    (8, 13, 200, "2025-02-05", "2025-02-05", None),
]
_CUSTOMER_ROWS = [(100, "A"), (200, "B"), (300, "C")]


def seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        con.execute(
            "CREATE TABLE account_snapshots (id INTEGER PRIMARY KEY, account_id INTEGER, "
            "customer_id INTEGER, snapshot_date TEXT, recorded_at TEXT, balance REAL)"
        )
        con.executemany("INSERT INTO account_snapshots VALUES (?,?,?,?,?,?)", _SNAPSHOT_ROWS)
        con.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
        con.executemany("INSERT INTO customers VALUES (?,?)", _CUSTOMER_ROWS)


def seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute(
        "CREATE TABLE account_snapshots (id INTEGER, account_id INTEGER, customer_id INTEGER, "
        "snapshot_date DATE, recorded_at DATE, balance DOUBLE)"
    )
    con.executemany("INSERT INTO account_snapshots VALUES (?,?,?,?,?,?)", _SNAPSHOT_ROWS)
    con.execute("CREATE TABLE customers (id INTEGER, name VARCHAR)")
    con.executemany("INSERT INTO customers VALUES (?,?)", _CUSTOMER_ROWS)
    con.close()


async def make_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = seed_duckdb if dialect == "duckdb" else seed_sqlite
    async with seeded_exec_engine(dialect=dialect, seed=seed, models=dev2006_models()) as (engine, _):
        yield engine


# --------------------------------------------------------------------------- #
# Result readers
# --------------------------------------------------------------------------- #
def month_key(value) -> str:
    """Stable per-month key across SQLite text and DuckDB date values."""
    return str(value)[:7]


def num(value) -> Optional[float]:
    return None if value is None else float(value)


def by_customer(resp, name: str = "v") -> dict:
    out = {r["account_snapshots.customer_id"]: num(r[f"account_snapshots.{name}"]) for r in resp.data}
    assert len(out) == len(resp.data), "duplicate result rows for one customer"
    return out


def by_customer_month(resp, name: str = "v") -> dict:
    tkey = next(k for k in resp.columns if "snapshot_date" in k)
    out = {
        (r["account_snapshots.customer_id"], month_key(r[tkey])): num(r[f"account_snapshots.{name}"])
        for r in resp.data
    }
    assert len(out) == len(resp.data), "duplicate result rows for one (customer, month)"
    return out


def warnings_of(resp, kind: str) -> list:
    return [w for w in (resp.warnings or []) if getattr(w, "kind", None) == kind]
