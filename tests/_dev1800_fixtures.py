"""Shared fixtures for DEV-1800 — change/change_pct over a cross-model inner
aggregate combined into a composite.

The failing composite class (``change(cm) + amount:sum`` etc.) is exercised on
the DEV-1750 dataset (imported where used — it carries ``wscaled_sum`` and
multi-order customers). This module adds the *attributable* dataset: the
cross-model inner varies along the query's time axis (customer signup month),
so ``change``/``change_pct`` of a cross-model aggregate has hand-computable
per-period values.

Underscore-prefixed so pytest skips collection here (like ``_engine_helpers``).

Dataset — one customer cohort per signup month (Jan/Feb/Mar 2024)::

    customers (id, signup_at, spend)
      1  2024-01-10  100.0
      2  2024-02-10  150.0
      3  2024-03-10   30.0
    orders (id, customer_id, amount, ordered_at)
      1  c1  20.0  2024-01-20
      2  c1  15.0  2024-02-05     c1 order amounts sum = 35
      3  c2  60.0  2024-02-15
      4  c2  40.0  2024-03-10     c2 order amounts sum = 100
      5  c3   7.0  2024-03-20     c3 order amounts sum = 7

Oracles, grouped by customer signup month (re-aggregation dedups spend to one
row per customer — a naive fan-out would double c1/c2's spend and miss these)::

    orders.amount:sum    by signup month = [35, 100, 7]
      change      = [None, 65, -93]
      change_pct  = [None, 65/35, -93/100]   = [None, 1.857142857…, -0.93]
    customers.spend:sum  by signup month = [100, 150, 30]
      change      = [None, 50, -120]
    spend:sum / amount:sum (ratio)        = [100/35, 150/100, 30/7]
      change_pct  = [None, -0.475, 13/7]     = [None, -0.475, 1.857142857…]
"""

from __future__ import annotations

from typing import AsyncIterator, List

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import (
    Column,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import seeded_exec_engine


# --------------------------------------------------------------------------- #
# Models — orders (host) → customers.
# --------------------------------------------------------------------------- #
def customers_model() -> SlayerModel:
    """``spend`` is the cross-model (target-grain) inner; ``signup_at`` is the
    attributable time axis the inner varies along."""
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        default_time_dimension="signup_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="spend", type=DataType.DOUBLE),
            Column(name="signup_at", type=DataType.TIMESTAMP),
        ],
    )


def orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        default_time_dimension="ordered_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


def dev1800_models() -> List[SlayerModel]:
    """``[host, *referenced]`` in the order the engine helpers want."""
    return [orders_model(), customers_model()]


# --------------------------------------------------------------------------- #
# Execution dataset — hand-computable (see module docstring for the oracle).
# --------------------------------------------------------------------------- #
_CUSTOMERS_ROWS = [
    # (id, spend, signup_at)
    (1, 100.0, "2024-01-10"),
    (2, 150.0, "2024-02-10"),
    (3, 30.0, "2024-03-10"),
]
_ORDERS_ROWS = [
    # (id, customer_id, amount, ordered_at)
    (1, 1, 20.0, "2024-01-20"),
    (2, 1, 15.0, "2024-02-05"),
    (3, 2, 60.0, "2024-02-15"),
    (4, 2, 40.0, "2024-03-10"),
    (5, 3, 7.0, "2024-03-20"),
]


def _seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        cur = con.cursor()
        cur.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, spend REAL, signup_at TEXT)"
        )
        cur.executemany("INSERT INTO customers VALUES (?,?,?)", _CUSTOMERS_ROWS)
        cur.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
            "amount REAL, ordered_at TEXT)"
        )
        cur.executemany("INSERT INTO orders VALUES (?,?,?,?)", _ORDERS_ROWS)


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE customers (id INTEGER, spend DOUBLE, signup_at TIMESTAMP)")
    con.executemany("INSERT INTO customers VALUES (?,?,?)", _CUSTOMERS_ROWS)
    con.execute(
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount DOUBLE, "
        "ordered_at TIMESTAMP)"
    )
    con.executemany("INSERT INTO orders VALUES (?,?,?,?)", _ORDERS_ROWS)
    con.close()


async def make_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture over the attributable
    dataset. A test module wraps this in ``@pytest.fixture`` so the name lives
    where it is consumed."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(dialect=dialect, seed=seed, models=dev1800_models()) as (engine, _db):
        yield engine


def signup_td() -> List[TimeDimension]:
    """Month time dimension on the customer signup axis (the attributable axis).
    Dotted so it works whether the query is rooted at orders or customers."""
    return [TimeDimension(
        dimension=ColumnRef(name="customers.signup_at"),
        granularity=TimeGranularity.MONTH,
    )]


def month_key(value) -> str:
    """First 7 chars of a DATE_TRUNC'd month value — stable across backends."""
    return str(value)[:7]


def by_signup(resp) -> dict:
    """``resp.data`` re-keyed to the ``YYYY-MM`` signup month; the signup column
    is found by suffix so it works under either root's key prefix."""
    signup_col = next(k for k in resp.data[0] if k.endswith("signup_at"))
    out = {month_key(r[signup_col]): r for r in resp.data}
    assert len(out) == len(resp.data), "duplicate result rows for one signup month"
    return out


# --------------------------------------------------------------------------- #
# Oracles (keyed by signup month "YYYY-MM"). See the module docstring.
# --------------------------------------------------------------------------- #
AMOUNT_BY_SIGNUP = {"2024-01": 35.0, "2024-02": 100.0, "2024-03": 7.0}
CHANGE_AMOUNT_BY_SIGNUP = {"2024-01": None, "2024-02": 65.0, "2024-03": -93.0}
CHANGE_PCT_AMOUNT_BY_SIGNUP = {
    "2024-01": None, "2024-02": 65.0 / 35.0, "2024-03": -93.0 / 100.0,
}
SPEND_BY_SIGNUP = {"2024-01": 100.0, "2024-02": 150.0, "2024-03": 30.0}
CHANGE_SPEND_BY_SIGNUP = {"2024-01": None, "2024-02": 50.0, "2024-03": -120.0}
RATIO_BY_SIGNUP = {
    "2024-01": 100.0 / 35.0, "2024-02": 150.0 / 100.0, "2024-03": 30.0 / 7.0,
}
CHANGE_PCT_RATIO_BY_SIGNUP = {
    "2024-01": None, "2024-02": -0.475, "2024-03": 13.0 / 7.0,
}


__all__ = [
    "orders_model", "customers_model", "dev1800_models",
    "make_exec_engine", "signup_td", "month_key", "by_signup",
    "AMOUNT_BY_SIGNUP", "CHANGE_AMOUNT_BY_SIGNUP", "CHANGE_PCT_AMOUNT_BY_SIGNUP",
    "SPEND_BY_SIGNUP", "CHANGE_SPEND_BY_SIGNUP",
    "RATIO_BY_SIGNUP", "CHANGE_PCT_RATIO_BY_SIGNUP",
    "SlayerQuery", "ModelMeasure", "ColumnRef", "TimeDimension", "TimeGranularity",
]
