"""Fixtures for ranked and windowed association, on the ``_dev1900_fixtures`` graph and data.

Reference data (``_dev1840_fixtures``): customers c1 gold 100 (signup Jan 5), c2 silver 150 (Feb 10), c3 gold 60
(Mar 15), c4 bronze 40 (Mar 20), c5 silver 80 (Apr 15), c6 gold 30 (Apr 18), c7 gold 55 (Apr 20, no orders); orders
c1 ok Jan 10 / new Jan 20 / ok Jan 25, c2 ok Feb 10 / new Feb 20, c3 ok Mar 5, c4 new Mar 10, c5 ok Apr 2,
c6 ok Apr 10, and an orphan ok order Mar 12 with no customer.

``make_same_row_engine`` adds c8 (spend 1000): an in-frame ``new`` order (2024-03-15) and an ``ok`` order outside
every 2024 frame (2023-01-15). ``ranked_fanout_*`` is a separate accounts/events dataset for the ranked pick.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import QueryMetric, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.storage.sqlite_conn import transaction
from tests._dev1840_fixtures import (
    _CUSTOMERS_ROWS,
    _ORDERS_ROWS,
    _PLANS_ROWS,
    _REGIONS_ROWS,
    _STORES_ROWS,
    broadcast_warnings,
    dev1840_datasets,
    month_key,
)
from tests._dev1841_fixtures import associated_warnings
from tests._dev1900_fixtures import dev1900_datasets, make_exec_engine
from tests._dev2216_fixtures import seed_with
from tests._engine_helpers import seeded_exec_engine

# --------------------------------------------------------------------------- #
# Query shorthands
# --------------------------------------------------------------------------- #
ORDERED_MONTH = [{"field": "orders.ordered_at", "granularity": "month"}]
SIGNUP_MONTH = [{"field": "customers.signup_at", "granularity": "month"}]
OK = "orders.status = 'ok'"


def query(mode: Optional[str] = None, **kw: Any) -> SlayerQuery:
    if mode is not None:
        kw["to_many_handling"] = mode
    return SlayerQuery.model_validate(kw)


def metric(expression: str, alias: str) -> QueryMetric:
    return QueryMetric(metric=expression, alias=alias)


def num(value: Any) -> Optional[float]:
    return None if value is None else float(value)


def by_month(resp: SlayerResponse, value: str, *, td: str = "orders.ordered_at") -> dict:
    """``{YYYY-MM or None: value}`` over a month-bucketed time dimension."""
    return {(None if r[td] is None else month_key(r[td])): num(r[value]) for r in resp.data}


def by_status(resp: SlayerResponse, value: str) -> dict:
    return {r["orders.status"]: num(r[value]) for r in resp.data}


def by_status_month(resp: SlayerResponse, value: str, *, td: str = "customers.signup_at") -> dict:
    return {(r["orders.status"], None if r[td] is None else month_key(r[td])): num(r[value]) for r in resp.data}


def associated_dimensions(resp: SlayerResponse) -> list[str]:
    return [d for w in associated_warnings(resp) for d in w.dimensions]


# --------------------------------------------------------------------------- #
# Oracles (hand-computed from the reference data)
# --------------------------------------------------------------------------- #
#: Cross-model first/last(customers.spend, customers.signup_at) by status: new = c1, c2, c4; ok = c1, c2, c3, c5, c6.
LAST_BY_STATUS = {"new": 40.0, "ok": 30.0}
FIRST_BY_STATUS = {"new": 100.0, "ok": 100.0}
#: Customers-rooted local last by orders.status: the NULL status holds the orderless c7.
LOCAL_LAST_BY_STATUS = {"new": 40.0, "ok": 30.0, None: 55.0}
LOCAL_LAST_BROADCAST = 55.0
#: Trailing-1y spend by order month, each customer once per month.
WINDOW_BY_ORDER_MONTH = {"2024-01": 100.0, "2024-02": 250.0, "2024-03": 350.0, "2024-04": 460.0}
#: The same with ``orders.status = 'ok'`` binding to the in-frame order (c4's only order is new).
WINDOW_OK_BY_ORDER_MONTH = {"2024-01": 100.0, "2024-02": 250.0, "2024-03": 310.0, "2024-04": 420.0}
#: Active customers per trailing year by order month.
WINDOW_COUNT_BY_ORDER_MONTH = {"2024-01": 1.0, "2024-02": 2.0, "2024-03": 4.0, "2024-04": 6.0}
#: last(spend, signup_at, window='1y') by order month: c1; c2; c4 (Mar 20); c6 (Apr 18).
WINDOW_LAST_BY_ORDER_MONTH = {"2024-01": 100.0, "2024-02": 150.0, "2024-03": 40.0, "2024-04": 30.0}
#: sum(spend, window='1y', partition_by=tier) by tier × order month: gold c1, c3 (Mar), c6 (Apr); orderless c7 is NULL.
PARTITIONED_WINDOW_BY_TIER_MONTH = {
    ("bronze", "2024-03"): 40.0, ("gold", "2024-01"): 100.0, ("gold", "2024-03"): 160.0, ("gold", "2024-04"): 190.0,
    ("gold", None): None, ("silver", "2024-02"): 150.0, ("silver", "2024-04"): 230.0,
}
#: The same with last(spend, signup_at, …): the latest signup among the tier's in-frame customers.
PARTITIONED_WINDOW_LAST_BY_TIER_MONTH = {
    ("bronze", "2024-03"): 40.0, ("gold", "2024-01"): 100.0, ("gold", "2024-03"): 60.0, ("gold", "2024-04"): 30.0,
    ("silver", "2024-02"): 150.0, ("silver", "2024-04"): 80.0,
}
#: Customers-rooted sum(spend, window='1y') by orders.status × signup month, associated.
WINDOW_BY_STATUS_SIGNUP = {
    ("new", "2024-01"): 100.0, ("new", "2024-02"): 250.0, ("new", "2024-03"): 290.0,
    ("ok", "2024-01"): 100.0, ("ok", "2024-02"): 250.0, ("ok", "2024-03"): 310.0, ("ok", "2024-04"): 420.0,
    (None, "2024-04"): 55.0,
}
#: Broadcast: each month's trailing total on every status.
WINDOW_BROADCAST_BY_SIGNUP = {"2024-01": 100.0, "2024-02": 250.0, "2024-03": 350.0, "2024-04": 515.0}
#: The pre-fix multiplied (ok, Jan) cell (c1's two ok orders).
LOCAL_WINDOW_OK_JAN_FAN_DEFECT = 200.0
#: With c8: the split-EXISTS reading counts c8 (an ok order + an in-frame order) in March and April.
SAME_ROW_SPLIT_DEFECT = {"2024-03": 1310.0, "2024-04": 1420.0}
#: c8's own ok order opens a 2023-01 bucket holding only c8.
SAME_ROW_C8_BUCKET = {"2023-01": 1000.0}
#: Ranked re-aggregation inner, by status (= the associated last).
REAGG_LAST_BY_STATUS = LAST_BY_STATUS
#: Orders-rooted last(customers.spend) ranked by the signup time dimension, per (status, signup month).
LAST_BY_STATUS_SIGNUP = {
    ("new", "2024-01"): 100.0, ("new", "2024-02"): 150.0, ("new", "2024-03"): 40.0,
    ("ok", "2024-01"): 100.0, ("ok", "2024-02"): 150.0, ("ok", "2024-03"): 60.0, ("ok", "2024-04"): 30.0,
}
#: Orders-rooted sum(customers.spend, window='90d') per (status, signup month): April's frame drops c1 (Jan 5).
WINDOW_90D_BY_STATUS_SIGNUP = {
    ("new", "2024-01"): 100.0, ("new", "2024-02"): 250.0, ("new", "2024-03"): 290.0,
    ("ok", "2024-01"): 100.0, ("ok", "2024-02"): 250.0, ("ok", "2024-03"): 310.0, ("ok", "2024-04"): 320.0,
}


def known_status_signup(resp: SlayerResponse, value: str) -> dict:
    """``by_status_month`` without the orphan order's NULL signup cell."""
    return {k: v for k, v in by_status_month(resp, value).items() if k[1] is not None}


# --------------------------------------------------------------------------- #
# Datasets and engines
# --------------------------------------------------------------------------- #
def keyless(datasets: list[SlayerDataset], *, name: str) -> list[SlayerDataset]:
    """``datasets`` with ``name`` stripped of its primary key."""
    for ds in datasets:
        if ds.name == name:
            ds.fields = [f.model_copy(update={"primary_key": False}) for f in ds.fields]
    return datasets


def keyless_dev1900_datasets() -> list[SlayerDataset]:
    return keyless(dev1900_datasets(), name="customers")


C8_CUSTOMER = (8, None, None, "gold", 1000.0, "2023-01-01")
C8_ORDERS = [
    (12, 8, "new", "web", 1.0, "2024-03-15", "A", 1),
    (13, 8, "ok", "web", 1.0, "2023-01-15", "A", 1),
]


def append_rows(*, dialect: str, db_path: str, table: str, rows: Sequence[tuple]) -> None:
    """Insert ``rows`` into an already-seeded ``table`` (before the engine reads it)."""
    marks = ",".join("?" for _ in rows[0])
    sql = f"INSERT INTO {table} VALUES ({marks})"
    if dialect == "duckdb":
        con = pytest.importorskip("duckdb").connect(db_path)
        try:
            con.executemany(sql, [list(r) for r in rows])
        finally:
            con.close()
        return
    with transaction(db_path) as con:
        con.executemany(sql, rows)


async def make_same_row_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """``params=["sqlite", "duckdb"]`` engine: the ``_dev1900`` seed plus c8 and its two orders."""
    async for engine in make_exec_engine(request):
        ds = await engine.storage.get_datasource("test")
        assert ds is not None
        assert ds.type is not None
        assert ds.database is not None
        append_rows(dialect=ds.type, db_path=ds.database, table="customers", rows=[C8_CUSTOMER])
        append_rows(dialect=ds.type, db_path=ds.database, table="orders", rows=C8_ORDERS)
        yield engine


# --- Ranked fan-out: accounts (home) ← events (many per account) --------------------------------------------------
#: (id, score, ranked_at): A2 holds the highest key; A3 and A5 have NULL keys.
ACCOUNTS = [(1, 10.0, "2024-01-01"), (2, 20.0, "2024-03-01"), (3, 30.0, None), (4, 40.0, "2024-02-01"),
            (5, 50.0, None)]
#: (id, account_id, kind): in ``x`` A1 has three rows and A2 one; ``z`` holds only the NULL-key A3.
EVENTS = [(1, 1, "x"), (2, 1, "x"), (3, 1, "x"), (4, 2, "x"), (5, 3, "x"), (6, 3, "x"),
          (7, 4, "y"), (8, 4, "y"), (9, 5, "y"), (10, 3, "z")]
RANKED_LAST_BY_KIND = {"x": 20.0, "y": 40.0, "z": None}
RANKED_FIRST_BY_KIND = {"x": 10.0, "y": 40.0, "z": None}


def _lit(value: Any) -> str:
    if value is None:
        return "NULL"
    return f"'{value}'" if isinstance(value, str) else str(value)


def _values(rows: Sequence[tuple]) -> str:
    return ", ".join("(" + ", ".join(_lit(v) for v in row) + ")" for row in rows)


def ranked_fanout_statements() -> list[str]:
    return [
        "CREATE TABLE accounts (id INTEGER, score DOUBLE PRECISION, ranked_at TIMESTAMP)",
        "CREATE TABLE events (id INTEGER, account_id INTEGER, kind VARCHAR(8))",
        f"INSERT INTO accounts VALUES {_values(ACCOUNTS)}",
        f"INSERT INTO events VALUES {_values(EVENTS)}",
    ]


def ranked_fanout_datasets(*, keyed: bool = True) -> list[SlayerDataset]:
    accounts = SlayerDataset(
        name="accounts", data_source="test", sql_table="accounts",
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=keyed),
                DatasetField(name="score", datatype=DataType.FLOAT),
                DatasetField(name="ranked_at", datatype=DataType.DATETIME)],
    )
    events = SlayerDataset(
        name="events", data_source="test", sql_table="events",
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="account_id", datatype=DataType.INTEGER),
                DatasetField(name="kind", datatype=DataType.STRING)],
        relationships=[DatasetRelationship(to="accounts", from_columns=["account_id"], to_columns=["id"])],
    )
    return [events, accounts]


async def make_ranked_fanout_engine(*, dialect: str, keyed: bool) -> AsyncIterator[SlayerQueryEngine]:
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    async with seeded_exec_engine(dialect=dialect, seed=seed_with(dialect, ranked_fanout_statements()),
                                  datasets=ranked_fanout_datasets(keyed=keyed)) as (engine, _):
        yield engine


# --- A list child as the time axis: members (home) with a list of visit timestamps ---------------------------------
#: (id, score, visits): M1 visits twice in January; M3 shares M1's score, so equal values stay separate rows.
MEMBERS = [(1, 10.0, ["2024-01-10", "2024-01-20"]), (2, 20.0, ["2024-02-05"]), (3, 10.0, ["2024-01-15"])]
VISIT_MONTH = [{"field": "members.visits.value", "granularity": "month"}]
#: By visit month, each member once per bucket: plain = the month's visitors; windowed = the trailing year's.
VISITS_PLAIN_COUNT = {"2024-01": 2.0, "2024-02": 1.0}
VISITS_PLAIN_SUM = {"2024-01": 20.0, "2024-02": 20.0}
VISITS_WINDOW_COUNT = {"2024-01": 2.0, "2024-02": 3.0}
VISITS_WINDOW_SUM = {"2024-01": 20.0, "2024-02": 40.0}


def list_axis_statements() -> list[str]:
    rows = ", ".join(
        f"({i}, {score}, [{', '.join(f'TIMESTAMP {_lit(v)}' for v in visits)}])" for i, score, visits in MEMBERS
    )
    return ["CREATE TABLE members (id INTEGER, score DOUBLE, visits TIMESTAMP[])", f"INSERT INTO members VALUES {rows}"]


def list_axis_datasets(*, keyed: bool) -> list[SlayerDataset]:
    return [SlayerDataset(
        name="members", data_source="test", sql_table="members",
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=keyed),
                DatasetField(name="score", datatype=DataType.FLOAT),
                DatasetField(name="visits", datatype=DataType.ARRAY, element_type=DataType.DATETIME)],
    )]


async def make_list_axis_engine(*, keyed: bool) -> AsyncIterator[SlayerQueryEngine]:
    """DuckDB: the members table with a native TIMESTAMP list."""
    pytest.importorskip("duckdb")
    async with seeded_exec_engine(dialect="duckdb", seed=seed_with("duckdb", list_axis_statements()),
                                  datasets=list_axis_datasets(keyed=keyed)) as (engine, _):
        yield engine


# --- The reference data on a server backend (Postgres) -------------------------------------------------------------
def postgres_statements() -> list[str]:
    return [
        "CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT, pop DOUBLE PRECISION)",
        "CREATE TABLE plans (code TEXT PRIMARY KEY, level TEXT, fee DOUBLE PRECISION)",
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, plan_code TEXT, tier TEXT, "
        "spend DOUBLE PRECISION, signup_at TIMESTAMP)",
        "CREATE TABLE stores (co TEXT, no INTEGER, city TEXT, rent DOUBLE PRECISION, PRIMARY KEY (co, no))",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, status TEXT, channel TEXT, "
        "amount DOUBLE PRECISION, ordered_at TIMESTAMP, store_co TEXT, store_no INTEGER)",
        f"INSERT INTO regions VALUES {_values(_REGIONS_ROWS)}",
        f"INSERT INTO plans VALUES {_values(_PLANS_ROWS)}",
        f"INSERT INTO customers VALUES {_values(_CUSTOMERS_ROWS)}",
        f"INSERT INTO stores VALUES {_values(_STORES_ROWS)}",
        f"INSERT INTO orders VALUES {_values(_ORDERS_ROWS)}",
    ]


__all__ = [
    "ACCOUNTS", "EVENTS", "C8_CUSTOMER", "C8_ORDERS", "OK", "ORDERED_MONTH", "SIGNUP_MONTH",
    "FIRST_BY_STATUS", "LAST_BY_STATUS", "LAST_BY_STATUS_SIGNUP", "WINDOW_90D_BY_STATUS_SIGNUP",
    "known_status_signup", "LOCAL_LAST_BROADCAST", "LOCAL_LAST_BY_STATUS",
    "LOCAL_WINDOW_OK_JAN_FAN_DEFECT", "RANKED_FIRST_BY_KIND", "RANKED_LAST_BY_KIND", "REAGG_LAST_BY_STATUS",
    "SAME_ROW_C8_BUCKET", "SAME_ROW_SPLIT_DEFECT", "WINDOW_BROADCAST_BY_SIGNUP", "WINDOW_BY_ORDER_MONTH",
    "WINDOW_BY_STATUS_SIGNUP", "WINDOW_COUNT_BY_ORDER_MONTH", "WINDOW_LAST_BY_ORDER_MONTH",
    "WINDOW_OK_BY_ORDER_MONTH",
    "append_rows", "associated_dimensions", "associated_warnings", "broadcast_warnings", "by_month", "by_status",
    "by_status_month", "dev1840_datasets", "dev1900_datasets", "keyless", "keyless_dev1900_datasets",
    "make_exec_engine", "make_ranked_fanout_engine", "make_same_row_engine", "metric", "num",
    "postgres_statements", "query", "ranked_fanout_datasets", "ranked_fanout_statements",
]
