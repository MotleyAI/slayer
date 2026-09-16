"""Shared fixtures for DEV-1910 — the association producer roots at the home,
so a home entity absent from the query population still counts in the cells its
own path reaches.

Built on ``tests/_dev1900_fixtures.py`` (the ``orders → customers → regions →
region_events`` graph, ``bad_pop`` the derived fanning dimension) and the
DEV-1840 dataset. c7 is a South customer with ZERO orders; the seed here adds
one extra NULL-status order for c1 so a NULL-status cell exists and the
presence rule (a population-root-reached dimension needs a population row) is
observable against c7.

The standard engine (no extra order) comes from ``_dev1900_fixtures`` /
``_dev1841_fixtures``; ``make_null_status_engine`` below is the null-status
variant, seeded into SQLite AND DuckDB from the DEV-1840 dataset + the extra
order. Every oracle traces to that dataset.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator, List, Optional

import pytest

from slayer.core.models import DatasourceConfig, ModelMeasure, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1840_fixtures import (
    _CUSTOMERS_ROWS,
    _ORDERS_ROWS,
    _PLANS_ROWS,
    _REGIONS_ROWS,
    _STORES_ROWS,
    rows_by,
)
from tests._dev1900_fixtures import (
    _REGION_EVENTS_ROWS,
    BAD_POP,
    associated_warnings,
    bad_pop_vals,
    cust_q,
    dev1900_models,
    orders_q,
)

# --------------------------------------------------------------------------- #
# The extra order: c1 gets a NULL-status order, so the NULL-status cell exists
# and holds exactly c1 (never c7, who has no order at all).
# --------------------------------------------------------------------------- #
#: (id, customer_id, status, channel, amount, ordered_at, store_co, store_no)
NULL_STATUS_ORDER = (11, 1, None, "web", 8.0, "2024-01-30", "A", 1)
_ORDERS_WITH_NULL = [*_ORDERS_ROWS, NULL_STATUS_ORDER]


def _seed_sqlite(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT, pop REAL)")
    cur.executemany("INSERT INTO regions VALUES (?,?,?)", _REGIONS_ROWS)
    cur.execute("CREATE TABLE plans (code TEXT PRIMARY KEY, level TEXT, fee REAL)")
    cur.executemany("INSERT INTO plans VALUES (?,?,?)", _PLANS_ROWS)
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
        "plan_code TEXT, tier TEXT, spend REAL, signup_at TEXT)")
    cur.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?)", _CUSTOMERS_ROWS)
    cur.execute(
        "CREATE TABLE stores (co TEXT, no INTEGER, city TEXT, rent REAL, "
        "PRIMARY KEY (co, no))")
    cur.executemany("INSERT INTO stores VALUES (?,?,?,?)", _STORES_ROWS)
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
        "status TEXT, channel TEXT, amount REAL, ordered_at TEXT, "
        "store_co TEXT, store_no INTEGER)")
    cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)", _ORDERS_WITH_NULL)
    cur.execute(
        "CREATE TABLE region_events (id INTEGER PRIMARY KEY, region_id INTEGER, "
        "value REAL)")
    cur.executemany("INSERT INTO region_events VALUES (?,?,?)", _REGION_EVENTS_ROWS)
    con.commit()
    con.close()


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE regions (id INTEGER, name VARCHAR, pop DOUBLE)")
    con.executemany("INSERT INTO regions VALUES (?,?,?)", _REGIONS_ROWS)
    con.execute("CREATE TABLE plans (code VARCHAR, level VARCHAR, fee DOUBLE)")
    con.executemany("INSERT INTO plans VALUES (?,?,?)", _PLANS_ROWS)
    con.execute(
        "CREATE TABLE customers (id INTEGER, region_id INTEGER, "
        "plan_code VARCHAR, tier VARCHAR, spend DOUBLE, signup_at TIMESTAMP)")
    con.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?)", _CUSTOMERS_ROWS)
    con.execute(
        "CREATE TABLE stores (co VARCHAR, no INTEGER, city VARCHAR, rent DOUBLE)")
    con.executemany("INSERT INTO stores VALUES (?,?,?,?)", _STORES_ROWS)
    con.execute(
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, status VARCHAR, "
        "channel VARCHAR, amount DOUBLE, ordered_at TIMESTAMP, "
        "store_co VARCHAR, store_no INTEGER)")
    con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)", _ORDERS_WITH_NULL)
    con.execute(
        "CREATE TABLE region_events (id INTEGER, region_id INTEGER, value DOUBLE)")
    con.executemany("INSERT INTO region_events VALUES (?,?,?)", _REGION_EVENTS_ROWS)
    con.close()


async def _engine_for(*, dialect: str, db_path: str,
                      models: List[SlayerModel]) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=os.path.join(os.path.dirname(db_path), "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type=dialect, database=db_path))
    for model in models:
        await storage.save_model(model, _validate=False)
    return SlayerQueryEngine(storage=storage)


async def make_null_status_engine(
    request, *, models: Optional[List[SlayerModel]] = None,
) -> AsyncIterator[SlayerQueryEngine]:
    """``params=["sqlite", "duckdb"]`` engine seeded with the extra
    NULL-status order for c1."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, f"data.{dialect}")
        if dialect == "sqlite":
            _seed_sqlite(db_path)
        else:
            _seed_duckdb(db_path)
        yield await _engine_for(
            dialect=dialect, db_path=db_path,
            models=models if models is not None else dev1900_models())


# --------------------------------------------------------------------------- #
# Measures.
# --------------------------------------------------------------------------- #
SPEND_SUM = ModelMeasure(formula="customers.spend:sum", name="csp")
AMOUNT_SUM = ModelMeasure(formula="amount:sum", name="amt")
LOCAL_SPEND_SUM = ModelMeasure(formula="spend:sum", name="sp")  # customers-rooted
REGION_POP_SUM = ModelMeasure(formula="customers.regions.pop:sum", name="rp")
STORE_RENT_SUM = ModelMeasure(formula="stores.rent:sum", name="rent")
#: picked parameters: a home-determined weight (includes c7) vs a fanning host
#: column picked across the reverse hop (c7's NULL weight drops it).
SPEND_WAVG_HOME = ModelMeasure(
    formula="customers.spend:weighted_avg(weight=customers.spend)", name="wa")
SPEND_WAVG_AMOUNT = ModelMeasure(
    formula="customers.spend:weighted_avg(weight=amount)", name="wa")


# --------------------------------------------------------------------------- #
# Cell helpers.
# --------------------------------------------------------------------------- #
def status_vals(resp, measure: str, *, root: str = "orders") -> dict:
    """``{status: measure value}`` for a ``root``-rooted query."""
    col = "orders.status" if root == "orders" else "customers.orders.status"
    return {k[0]: v[measure] for k, v in rows_by(resp, col).items()}


def bad_pop_status_cells(resp, measure: str) -> dict:
    """``{(bad_pop, status): measure value}`` for an orders-rooted mixed query."""
    return {k: v[measure]
            for k, v in rows_by(resp, "orders." + BAD_POP, "orders.status").items()}


# --------------------------------------------------------------------------- #
# Oracles — every value traced to the DEV-1840 dataset (+ the extra order).
# --------------------------------------------------------------------------- #
#: Issue bar: cross-model spend by bad_pop, each customer once, incl. orderless
#: c7 in South (the fix); local amount unchanged (orders home, c7 has none).
SPEND_BY_BAD_POP = {150.0: 280.0, 230.0: 195.0, None: 40.0}
AMOUNT_BY_BAD_POP = {150.0: 100.0, 230.0: 20.0, None: 47.0}
#: Host channel='app' filter, cross-model spend by bad_pop: only app customers
#: (c1,c2 North; c3,c5 South); c7 has no app order so it is excluded.
APP_SPEND_BY_BAD_POP = {150.0: 250.0, 230.0: 140.0}
#: Mixed (bad_pop, status): the only South cell is (230, ok) = c3+c5; c7 (no
#: order → no status) is in NO cell — 195 minus its 55.
MIXED_SOUTH_OK = 140.0
MIXED_SOUTH_WITH_C7_BUG = 195.0
#: Presence guard, orders-rooted, NULL-status seed: the NULL cell holds exactly
#: c1 (owner of the NULL-status order); never c7 (no population row).
PRESENCE_NULL_CELL = 100.0
PRESENCE_NULL_CELL_C7_BUG = 155.0
SPEND_BY_STATUS_NULLSEED = {"ok": 420.0, "new": 290.0, None: 100.0}
#: Customers-rooted twin, NULL-status seed: home == host, so the population's
#: own LEFT JOIN keeps the orderless c7 in the NULL cell alongside c1.
CUST_NULL_CELL = 155.0
LOCAL_SPEND_BY_STATUS_NULLSEED = {"ok": 420.0, "new": 290.0, None: 155.0}
#: Two-hop home (regions), pop:sum by bad_pop — one region per bad_pop cell.
REGION_POP_BY_BAD_POP = {150.0: 100.0, 230.0: 200.0}
#: Composite back hop (orders → stores), stores.rent:sum by status — distinct
#: stores per status cell.
RENT_BY_STATUS = {"ok": 1100.0, "new": 800.0}
#: Local same-branch coupling (customers-rooted): channel='app' AND status must
#: hold on ONE order; today decoupled to the app total.
LOCAL_COUPLED_OK = 140.0
LOCAL_DECOUPLED_OK = 390.0
#: Picked home-determined weight: weighted_avg(spend, weight=spend) by bad_pop,
#: each distinct customer once — South includes c7 (the fix).
SPEND_WAVG_SOUTH = (60.0 ** 2 + 80.0 ** 2 + 55.0 ** 2) / (60.0 + 80.0 + 55.0)
SPEND_WAVG_SOUTH_C7_DROPPED = (60.0 ** 2 + 80.0 ** 2) / (60.0 + 80.0)
#: Picked fanning host column (weight=amount): amount is picked once per customer
#: across the reverse hop; c7 has no order so its weight is NULL and it drops —
#: South = 75 (only c3, c5 contribute), typing accepted (design decision 2).
SPEND_WAVG_AMOUNT_SOUTH = (60.0 * 5.0 + 80.0 * 15.0) / (5.0 + 15.0)
#: Dice–slice: the 230 (South) slice equals filtering to bad_pop = 230.
DICE_SLICE_SOUTH = 195.0


__all__ = [
    "ModelMeasure", "SlayerModel",
    "make_null_status_engine", "dev1900_models",
    "orders_q", "cust_q", "rows_by", "bad_pop_vals",
    "associated_warnings", "BAD_POP",
    "SPEND_SUM", "AMOUNT_SUM", "LOCAL_SPEND_SUM", "REGION_POP_SUM",
    "STORE_RENT_SUM", "SPEND_WAVG_HOME", "SPEND_WAVG_AMOUNT",
    "status_vals", "bad_pop_status_cells",
    "SPEND_BY_BAD_POP", "AMOUNT_BY_BAD_POP", "APP_SPEND_BY_BAD_POP",
    "MIXED_SOUTH_OK", "MIXED_SOUTH_WITH_C7_BUG",
    "PRESENCE_NULL_CELL", "PRESENCE_NULL_CELL_C7_BUG",
    "SPEND_BY_STATUS_NULLSEED", "CUST_NULL_CELL",
    "LOCAL_SPEND_BY_STATUS_NULLSEED", "REGION_POP_BY_BAD_POP", "RENT_BY_STATUS",
    "LOCAL_COUPLED_OK", "LOCAL_DECOUPLED_OK",
    "SPEND_WAVG_SOUTH", "SPEND_WAVG_SOUTH_C7_DROPPED", "SPEND_WAVG_AMOUNT_SOUTH",
    "DICE_SLICE_SOUTH",
    "NULL_STATUS_ORDER",
]
