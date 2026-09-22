"""DEV-1910 fixtures: the association producer roots at the home, so an entity
absent from the population still counts in the cells its path reaches. Built on
``_dev1900_fixtures`` + the DEV-1840 dataset; c7 is a South customer with ZERO
orders, and ``make_null_status_engine`` adds a NULL-status order for c1 (SQLite
+ DuckDB) so the back-hop presence rule is observable against c7."""

from __future__ import annotations

from typing import AsyncIterator, List, Optional

import pytest

from slayer.core.models import ModelMeasure, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction

from tests._dev1840_fixtures import rows_by
from tests._dev1900_fixtures import (
    BAD_POP,
    associated_warnings,
    bad_pop_vals,
    cust_q,
    dev1900_models,
    make_exec_engine,
    orders_q,
)

#: c1 gets a NULL-status order so the NULL-status cell exists and holds exactly
#: c1 (never c7, who has no order at all).
#: (id, customer_id, status, channel, amount, ordered_at, store_co, store_no)
NULL_STATUS_ORDER = (11, 1, None, "web", 8.0, "2024-01-30", "A", 1)


def _append_order(*, dialect: str, db_path: str, row: tuple) -> None:
    """Insert one order into an already-seeded db (before the engine opens it)."""
    sql = "INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)"
    if dialect == "duckdb":
        con = pytest.importorskip("duckdb").connect(db_path)
        con.execute(sql, list(row))
        con.close()
    else:
        with transaction(db_path) as con:
            con.execute(sql, row)


async def make_null_status_engine(
    request, *, models: Optional[List[SlayerModel]] = None,
) -> AsyncIterator[SlayerQueryEngine]:
    """``params=["sqlite", "duckdb"]`` engine — the _dev1900 seed plus c1's
    NULL-status order (appended before the engine opens the db)."""
    async for engine in make_exec_engine(request, models=models):
        ds = await engine.storage.get_datasource("test")
        assert ds is not None
        assert ds.type is not None
        assert ds.database is not None
        _append_order(dialect=ds.type, db_path=ds.database, row=NULL_STATUS_ORDER)
        yield engine


SPEND_SUM = ModelMeasure(formula="customers.spend:sum", name="csp")
AMOUNT_SUM = ModelMeasure(formula="amount:sum", name="amt")
LOCAL_SPEND_SUM = ModelMeasure(formula="spend:sum", name="sp")  # customers-rooted
REGION_POP_SUM = ModelMeasure(formula="customers.regions.pop:sum", name="rp")
STORE_RENT_SUM = ModelMeasure(formula="stores.rent:sum", name="rent")
# home-determined weight (includes c7) vs a fanning host column across the hop.
SPEND_WAVG_HOME = ModelMeasure(
    formula="customers.spend:weighted_avg(weight=customers.spend)", name="wa")
SPEND_WAVG_AMOUNT = ModelMeasure(
    formula="customers.spend:weighted_avg(weight=amount)", name="wa")


def status_vals(resp, measure: str, *, root: str = "orders") -> dict:
    col = "orders.status" if root == "orders" else "customers.orders.status"
    return {k[0]: v[measure] for k, v in rows_by(resp, col).items()}


def bad_pop_status_cells(resp, measure: str) -> dict:
    return {k: v[measure]
            for k, v in rows_by(resp, "orders." + BAD_POP, "orders.status").items()}


# Oracles traced to the DEV-1840 dataset (+ the extra NULL-status order).
# Issue bar: spend incl. orderless c7 in South (195); local amount unchanged.
SPEND_BY_BAD_POP = {150.0: 280.0, 230.0: 195.0, None: 40.0}
AMOUNT_BY_BAD_POP = {150.0: 100.0, 230.0: 20.0, None: 47.0}
# channel='app' filter: only app customers; c7 has no app order.
APP_SPEND_BY_BAD_POP = {150.0: 250.0, 230.0: 140.0}
# Mixed (bad_pop, status): South = (230, ok) = c3+c5; c7 in NO cell.
MIXED_SOUTH_OK = 140.0
MIXED_SOUTH_WITH_C7_BUG = 195.0
# Presence guard, orders-rooted: NULL cell = c1 (owner) only, never c7.
PRESENCE_NULL_CELL = 100.0
PRESENCE_NULL_CELL_C7_BUG = 155.0
SPEND_BY_STATUS_NULLSEED = {"ok": 420.0, "new": 290.0, None: 100.0}
# Customers-rooted twin (home == host): the LEFT JOIN keeps c7 in the NULL cell.
CUST_NULL_CELL = 155.0
LOCAL_SPEND_BY_STATUS_NULLSEED = {"ok": 420.0, "new": 290.0, None: 155.0}
# Two-hop home (regions); composite back hop (orders → stores).
REGION_POP_BY_BAD_POP = {150.0: 100.0, 230.0: 200.0}
RENT_BY_STATUS = {"ok": 1100.0, "new": 800.0}
# Local same-branch coupling: channel='app' AND status on ONE order (140 vs 390).
LOCAL_COUPLED_OK = 140.0
LOCAL_DECOUPLED_OK = 390.0
# weighted_avg(spend, weight=spend): each distinct customer once, South incl. c7.
SPEND_WAVG_SOUTH = (60.0 ** 2 + 80.0 ** 2 + 55.0 ** 2) / (60.0 + 80.0 + 55.0)
SPEND_WAVG_SOUTH_C7_DROPPED = (60.0 ** 2 + 80.0 ** 2) / (60.0 + 80.0)
# weight=amount: picked once per customer across the hop; c7's NULL weight drops.
SPEND_WAVG_AMOUNT_SOUTH = (60.0 * 5.0 + 80.0 * 15.0) / (5.0 + 15.0)
DICE_SLICE_SOUTH = 195.0  # the 230 (South) slice equals filtering bad_pop = 230


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
