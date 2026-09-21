"""Shared fixtures for DEV-1915 — a trailing ``window=`` on every aggregation.

Underscore-prefixed so pytest skips collection (mirrors ``tests/_dev1836_fixtures.py``,
whose dual-engine shape this follows). One hand-computed dataset seeded into
SQLite AND DuckDB; ``make_exec_engine`` yields an engine per backend.

Dataset (monthly buckets, 2024 is a leap year; the window axis is
``orders.created_at``). The ``orders.customer_id → customers`` hop is to-one
(``customers.id`` is PK); the inverted ``customers → orders`` orientation is the
unproven fan-out used by the fail-closed probes.

orders (id, customer_id, amount, qty, created_at, updated_at, region):
   1  c1  100  1  2024-01-01  2024-01-02  US
   2  c2  200  2  2024-01-15  2024-01-16  US
   3  c3  300  3  2024-02-15  NULL        EU
   4  c4  400  4  2024-03-15  2024-03-25  EU
   5  c4  300  5  2024-03-20  2024-03-16  US
customers (id, spend, signup_at, tier, discount):   [the DEV-1836 seed + discount]
   1  100  2024-01-05  gold    0.1
   2  150  2024-02-10  silver  0.2
   3   60  2024-03-15  gold    0.5
   4   40  2024-03-20  bronze  1.0

``window='90d'`` trailing intervals over ``created_at`` (bucket_end exclusive):
   Jan [2023-11-03, 2024-02-01) → {1, 2}
   Feb [2023-12-02, 2024-03-01) → {1, 2, 3}
   Mar [2024-01-02, 2024-04-01) → {2, 3, 4, 5}   (row 1 dated 2024-01-01 is out)
``window='1y'`` over ``customers.signup_at``: Jan {1}, Feb {1,2}, Mar {1,2,3,4}.
See the oracle constants below (re-derived from the raw rows in
``tests/test_dev1915_fixtures_smoke.py``).
"""

from __future__ import annotations

import sqlite3
from typing import AsyncIterator, List

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine

from tests._engine_helpers import _engine_generate
from tests._exec_fixture_helpers import (
    make_exec_engine as _shared_exec_engine,
    month_key,
    rows_by,
)


# --------------------------------------------------------------------------- #
# Models — orders (host) → customers; customers → orders is the inverted
# fan-out orientation (DEV-1853) the probes cross.
# --------------------------------------------------------------------------- #
def orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="qty", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="updated_at", type=DataType.TIMESTAMP),
            Column(name="region", type=DataType.TEXT),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
        aggregations=[
            # Custom aggregation with literal defaults (lo=0, hi=1000).
            Aggregation(
                name="trimmed_mean",
                formula="AVG(CASE WHEN {value} BETWEEN {lo} AND {hi} THEN {value} END)",
                params=[
                    AggregationParam(name="lo", sql="0"),
                    AggregationParam(name="hi", sql="1000"),
                ],
            ),
        ],
    )


def customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        default_time_dimension="signup_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="spend", type=DataType.DOUBLE),
            Column(name="signup_at", type=DataType.TIMESTAMP),
            Column(name="tier", type=DataType.TEXT),
            Column(name="discount", type=DataType.DOUBLE),
        ],
        aggregations=[
            # Custom aggregation whose definition default names a target column.
            Aggregation(
                name="disc_spend", formula="SUM({value} * {d})",
                params=[AggregationParam(name="d", sql="discount")],
            ),
            # A custom agg on customers whose param can be overridden by a root
            # (orders) column, widening the home above the source (def-owner test).
            Aggregation(
                name="wprod", formula="SUM({value} * {w})",
                params=[AggregationParam(name="w", sql="1")],
            ),
        ],
    )


def dev1915_models() -> List[SlayerModel]:
    """``[host, *referenced]`` in the order ``_engine_generate`` wants."""
    return [orders_model(), customers_model()]


# --------------------------------------------------------------------------- #
# Query shorthands.
# --------------------------------------------------------------------------- #
def q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def month_td(column: str = "created_at") -> List[TimeDimension]:
    return [TimeDimension(
        dimension=ColumnRef(name=column),
        granularity=TimeGranularity.MONTH,
    )]


async def gen(query: SlayerQuery, *, dialect: str = "duckdb",
              validate: bool = False) -> str:
    models = dev1915_models()
    return await _engine_generate(
        query=query, model=models[0], extra_models=models[1:],
        dialect=dialect, validate=validate,
    )


# --------------------------------------------------------------------------- #
# Oracles — window='90d' over created_at, keyed by bucket month (see smoke test).
# --------------------------------------------------------------------------- #
#: Which order ids fall in each month's trailing 90d interval.
MEMBERS_90D = {"2024-01": [1, 2], "2024-02": [1, 2, 3], "2024-03": [2, 3, 4, 5]}
#: Which customer ids fall in each signup-month's trailing 1y interval.
MEMBERS_CUST_1Y = {"2024-01": [1], "2024-02": [1, 2], "2024-03": [1, 2, 3, 4]}

COUNT_90D = {"2024-01": 2, "2024-02": 3, "2024-03": 4}
COUNT_DISTINCT_90D = {"2024-01": 2, "2024-02": 3, "2024-03": 3}
MIN_90D = {"2024-01": 100.0, "2024-02": 100.0, "2024-03": 200.0}
MAX_90D = {"2024-01": 200.0, "2024-02": 300.0, "2024-03": 400.0}
AVG_90D = {"2024-01": 150.0, "2024-02": 200.0, "2024-03": 300.0}
MEDIAN_90D = {"2024-01": 150.0, "2024-02": 200.0, "2024-03": 300.0}
STDDEV_SAMP_90D = {"2024-01": 70.71068, "2024-02": 100.0, "2024-03": 81.64966}
VAR_POP_90D = {"2024-01": 2500.0, "2024-02": 6666.66667, "2024-03": 5000.0}
CORR_90D = {"2024-01": 1.0, "2024-02": 1.0, "2024-03": 0.63245553}
COVAR_SAMP_90D = {"2024-01": 50.0, "2024-02": 100.0, "2024-03": 66.66667}
WEIGHTED_AVG_90D = {"2024-01": 166.66667, "2024-02": 233.33333, "2024-03": 314.28571}
#: weighted_avg(weight=qty:sum(partition_by=region)) — each row weighted by its
#: region's total qty (US=1+2+5=8, EU=3+4=7).
WEIGHTED_AVG_ATTACHED_90D = {"2024-01": 150.0, "2024-02": 195.65217, "2024-03": 296.66667}
TRIMMED_MEAN_150_350_90D = {"2024-01": 200.0, "2024-02": 250.0, "2024-03": 266.66667}
#: trimmed_mean() with the 0..1000 defaults keeps every row → the rolling avg.
TRIMMED_MEAN_DEFAULT_90D = AVG_90D
FIRST_90D = {"2024-01": 100.0, "2024-02": 100.0, "2024-03": 200.0}
LAST_90D = {"2024-01": 200.0, "2024-02": 300.0, "2024-03": 300.0}
#: last(updated_at): the NULL-keyed row 3 is skipped under descending native
#: ordering on both backends (Feb picks row 2's 200, not the later NULL key).
LAST_UPDATED_90D = {"2024-01": 200.0, "2024-02": 200.0, "2024-03": 400.0}
#: first(updated_at) February is pinned to the dialect's native NULL ordering:
#: SQLite sorts NULLs first (the NULL-keyed row 3 wins → 300), DuckDB last (row 1 → 100).
FIRST_UPDATED_FEB = {"sqlite": 300.0, "duckdb": 100.0}
FIRST_UPDATED_JAN = 100.0

#: partition_by=region, keyed by (region, month) — only populated cells exist.
COUNT_BY_REGION_90D = {
    ("US", "2024-01"): 2, ("EU", "2024-02"): 1,
    ("EU", "2024-03"): 2, ("US", "2024-03"): 2,
}

#: Cross-model, rooted at orders over customers.signup_at (window='1y').
CM_COUNT_1Y = {"2024-01": 1, "2024-02": 2, "2024-03": 4}
CM_LAST_1Y = {"2024-01": 100.0, "2024-02": 150.0, "2024-03": 40.0}
#: disc_spend = SUM(spend * discount) over the interval customers.
DISC_SPEND_1Y = {"2024-01": 10.0, "2024-02": 40.0, "2024-03": 110.0}

#: window='1d' — no row falls on the last day of its month, so every interval
#: is empty: counts 0, everything else NULL, every bucket still returned.
EMPTY_MONTHS = ["2024-01", "2024-02", "2024-03"]

#: measure-filtered: amount:count(window='90d') >= 3 keeps Feb (3) and Mar (4).
FILTER_COUNT_GE3_MONTHS = ["2024-02", "2024-03"]
#: order-only: amount:max(window='90d') desc → Mar 400, Feb 300, Jan 200.
ORDER_MAX_DESC_MONTHS = ["2024-03", "2024-02", "2024-01"]


# --------------------------------------------------------------------------- #
# Execution dataset.
# --------------------------------------------------------------------------- #
_ORDERS_ROWS = [
    # (id, customer_id, amount, qty, created_at, updated_at, region)
    (1, 1, 100.0, 1.0, "2024-01-01", "2024-01-02", "US"),
    (2, 2, 200.0, 2.0, "2024-01-15", "2024-01-16", "US"),
    (3, 3, 300.0, 3.0, "2024-02-15", None, "EU"),
    (4, 4, 400.0, 4.0, "2024-03-15", "2024-03-25", "EU"),
    (5, 4, 300.0, 5.0, "2024-03-20", "2024-03-16", "US"),
]
_CUSTOMERS_ROWS = [
    # (id, spend, signup_at, tier, discount)
    (1, 100.0, "2024-01-05", "gold", 0.1),
    (2, 150.0, "2024-02-10", "silver", 0.2),
    (3, 60.0, "2024-03-15", "gold", 0.5),
    (4, 40.0, "2024-03-20", "bronze", 1.0),
]


def _seed_sqlite(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
        "amount REAL, qty REAL, created_at TEXT, updated_at TEXT, region TEXT)"
    )
    cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?)", _ORDERS_ROWS)
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, spend REAL, "
        "signup_at TEXT, tier TEXT, discount REAL)"
    )
    cur.executemany("INSERT INTO customers VALUES (?,?,?,?,?)", _CUSTOMERS_ROWS)
    con.commit()
    con.close()


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute(
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount DOUBLE, "
        "qty DOUBLE, created_at TIMESTAMP, updated_at TIMESTAMP, region VARCHAR)"
    )
    con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?)", _ORDERS_ROWS)
    con.execute(
        "CREATE TABLE customers (id INTEGER, spend DOUBLE, signup_at TIMESTAMP, "
        "tier VARCHAR, discount DOUBLE)"
    )
    con.executemany("INSERT INTO customers VALUES (?,?,?,?,?)", _CUSTOMERS_ROWS)
    con.close()


async def make_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Wrap the shared exec-engine body with this module's seeds and models."""
    async for engine in _shared_exec_engine(
        request, seed_sqlite=_seed_sqlite, seed_duckdb=_seed_duckdb, models=dev1915_models(),
    ):
        yield engine


def by_month(resp, value_key: str) -> dict:
    """``{month: value}`` for a single windowed measure grouped by the time bucket."""
    time_key = next(k for k in resp.columns if k.endswith("created_at")
                    or k.endswith("signup_at"))
    return {month_key(r[time_key]): r[value_key] for r in resp.data}


def broadcast_warnings(resp) -> list:
    return [w for w in (resp.warnings or [])
            if getattr(w, "kind", None) == "broadcast"]


__all__ = [
    "orders_model", "customers_model", "dev1915_models",
    "q", "gen", "month_td", "month_key", "rows_by", "by_month",
    "broadcast_warnings",
    "make_exec_engine",
    "MEMBERS_90D", "MEMBERS_CUST_1Y",
    "COUNT_90D", "COUNT_DISTINCT_90D", "MIN_90D", "MAX_90D", "AVG_90D",
    "MEDIAN_90D", "STDDEV_SAMP_90D", "VAR_POP_90D", "CORR_90D",
    "COVAR_SAMP_90D", "WEIGHTED_AVG_90D", "WEIGHTED_AVG_ATTACHED_90D",
    "TRIMMED_MEAN_150_350_90D", "TRIMMED_MEAN_DEFAULT_90D",
    "FIRST_90D", "LAST_90D", "LAST_UPDATED_90D",
    "FIRST_UPDATED_FEB", "FIRST_UPDATED_JAN", "COUNT_BY_REGION_90D",
    "CM_COUNT_1Y", "CM_LAST_1Y", "DISC_SPEND_1Y",
    "EMPTY_MONTHS", "FILTER_COUNT_GE3_MONTHS", "ORDER_MAX_DESC_MONTHS",
    "_ORDERS_ROWS", "_CUSTOMERS_ROWS",
    "SlayerQuery", "ModelMeasure", "ColumnRef", "TimeDimension",
    "TimeGranularity", "ModelJoin", "Column", "SlayerModel", "DataType",
    "Aggregation", "AggregationParam",
]
