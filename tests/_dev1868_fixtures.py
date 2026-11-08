"""Shared DEV-1868 closure-sweep constants: formulas + hand-computed oracles.

W1/W2/W3 run on the DEV-1836 stack (orders → customers → {regions, segments});
the series-shift suite runs on the DEV-1846 stack (sales → regions), plus a
tiny month-end ``daily`` stack for the many-to-one calendar-shift edge.
Oracles derive from the seed data below and those modules' docstrings.
Underscore-prefixed so pytest skips collection.
"""

from __future__ import annotations

from typing import AsyncIterator, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from tests._engine_helpers import seeded_exec_engine
from slayer.storage.sqlite_conn import transaction

# --------------------------------------------------------------------------- #
# W1 — cross-model first/last × partition_by (DEV-1836 stack).
# --------------------------------------------------------------------------- #
LAST_TIER = "customers.spend:last(customers.signup_at, partition_by=customers.tier)"
FIRST_REGION = (
    "customers.spend:first(customers.signup_at, partition_by=customers.regions.name)"
)
LAST_REGION = (
    "customers.spend:last(customers.signup_at, partition_by=customers.regions.name)"
)
CM_PART_TIER = "customers.spend:sum(partition_by=customers.tier)"

#: last spend by signup within tier: gold c3 (03-15) wins over c1 (01-05).
LAST_SPEND_BY_TIER = {"gold": 60.0, "silver": 150.0, "bronze": 40.0, None: None}
#: first spend by signup within regions.name; NULL name = c4.
FIRST_SPEND_BY_REGION = {"North": 100.0, "South": 60.0, None: 40.0}
#: last spend by signup within regions.name: North c2 (02-10) wins over c1.
LAST_SPEND_BY_REGION = {"North": 150.0, "South": 60.0, None: 40.0}
#: amount:sum grouped by customers.regions.name (o6 NULL-region + o7 orphan → NULL).
AMOUNT_BY_REGION_NAME = {"North": 60.0, "South": 10.0, None: 47.0}

# --------------------------------------------------------------------------- #
# W2 — cross-model partitioned aggregates nested in transforms.
# --------------------------------------------------------------------------- #
#: cumsum(spend:sum(partition_by=tier)) per (tier, order month): the attached
#: per-tier total (160/150/40) accumulates over each tier's order months.
CUMSUM_PART_BY_TIER_MONTH = {
    ("gold", "2024-01"): 160.0,
    ("gold", "2024-02"): 320.0,
    ("gold", "2024-03"): 480.0,
    ("silver", "2024-02"): 150.0,
    ("bronze", "2024-03"): 40.0,
    (None, "2024-03"): None,
}
#: change(last-by-region) per (regions.name, order month): constant series →
#: NULL at each region's first month, 0 after.
CHANGE_LAST_BY_REGION_MONTH = {
    ("North", "2024-01"): None,
    ("North", "2024-02"): 0.0,
    ("South", "2024-02"): None,
    ("South", "2024-03"): 0.0,
    (None, "2024-03"): None,
}

# --------------------------------------------------------------------------- #
# W3 — composites with cross-model operands (dims = customers.tier).
# --------------------------------------------------------------------------- #
RATIO_BY_TIER = {"gold": 0.25, "silver": 0.2, "bronze": 1.0, None: None}
ROUND_BY_TIER = {"gold": 4.0, "silver": 5.0, "bronze": 1.0, None: None}
TIMES2_BY_TIER = {"gold": 320.0, "silver": 300.0, "bronze": 80.0, None: None}
SUM_MINUS_MAX_BY_TIER = {"gold": 60.0, "silver": 0.0, "bronze": 0.0, None: None}
#: spend:sum / regions.pop:sum, both producer-rooted, dims = regions.name.
SPEND_OVER_POP_BY_REGION = {"North": 2.5, "South": 0.3, None: None}

# --------------------------------------------------------------------------- #
# Series-shift oracles (DEV-1846 stack; monthly revenue 60/100/60).
# --------------------------------------------------------------------------- #
CUMSUM_REVENUE = {"2024-01": 60.0, "2024-02": 160.0, "2024-03": 220.0}
SHIFTED_CUMSUM = {"2024-01": None, "2024-02": 60.0, "2024-03": 160.0}
SHIFTED_CUMSUM_BY_STORE = {
    ("A", "2024-01"): None, ("A", "2024-02"): 30.0, ("A", "2024-03"): 70.0,
    ("B", "2024-01"): None, ("B", "2024-02"): 30.0, ("B", "2024-03"): 90.0,
}
#: revenue:sum + regions.factor:sum (factor broadcasts to 5.0), shifted -1.
SHIFTED_REV_PLUS_FACTOR = {"2024-01": None, "2024-02": 65.0, "2024-03": 105.0}
#: revenue:sum in (100, 999) → F/T/F, shifted -1.
SHIFTED_IN_PRED = {"2024-01": None, "2024-02": False, "2024-03": True}
#: revenue:sum > 80 → F/T/F, shifted -1.
SHIFTED_GT_PRED = {"2024-01": None, "2024-02": False, "2024-03": True}
#: cumsum over status='a' rows only (a-revenue 40/40/50 → 40/80/130), shifted.
SHIFTED_CUMSUM_STATUS_A = {"2024-01": None, "2024-02": 40.0, "2024-03": 80.0}


# --------------------------------------------------------------------------- #
# Month-end daily stack: day buckets whose +1-month images collide (leap-year
# Feb 2024: Jan 29/30/31 all clamp to Feb 29 on clamping dialects). A series
# shift must look each row's own bucket up, never fan the join out.
# --------------------------------------------------------------------------- #
_DAILY_ROWS = [
    # (id, revenue, ordered_at)
    (1, 10.0, "2024-01-28"),
    (2, 20.0, "2024-01-29"),
    (3, 30.0, "2024-01-30"),
    (4, 40.0, "2024-01-31"),
    (5, 50.0, "2024-02-28"),
    (6, 60.0, "2024-02-29"),
]

#: time_shift(cumsum(revenue:sum), -1, 'month') per day: cumsum runs
#: 10/30/60/100/150/210; January looks into December (absent → NULL).
SHIFTED_CUMSUM_MONTH_END = {
    "2024-01-28": None, "2024-01-29": None, "2024-01-30": None,
    "2024-01-31": None, "2024-02-28": 10.0, "2024-02-29": 30.0,
}


def daily_model() -> SlayerModel:
    return SlayerModel(
        name="daily", data_source="test", sql_table="daily",
        default_time_dimension="ordered_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
        ],
    )


def _seed_daily_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        con.execute(
            "CREATE TABLE daily (id INTEGER PRIMARY KEY, revenue REAL, "
            "ordered_at TEXT)"
        )
        con.executemany("INSERT INTO daily VALUES (?,?,?)", _DAILY_ROWS)


def _seed_daily_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute(
        "CREATE TABLE daily (id INTEGER, revenue DOUBLE, ordered_at TIMESTAMP)"
    )
    con.executemany("INSERT INTO daily VALUES (?,?,?)", _DAILY_ROWS)
    con.close()


async def make_daily_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture over the daily stack."""
    dialect = request.param
    seed = _seed_daily_duckdb if dialect == "duckdb" else _seed_daily_sqlite
    async with seeded_exec_engine(
        dialect=dialect, seed=seed, models=[daily_model()],
    ) as (engine, _db):
        yield engine


def as_bool(value) -> Optional[bool]:
    """Engine-neutral boolean cell (SQLite 0/1, DuckDB bool, NULL → None)."""
    return None if value is None else bool(value)


def keyed_by_month(resp, *dim_keys: str) -> dict:
    """Rows keyed by (*dims, YYYY-MM), engine-neutral on the month value."""
    td_key = next(k for k in resp.data[0] if k.endswith(".ordered_at"))
    out = {}
    for r in resp.data:
        out[(*(r[k] for k in dim_keys), str(r[td_key])[:7])] = r
    assert len(out) == len(resp.data), "duplicate result rows for one group key"
    return out
