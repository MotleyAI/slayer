"""Shared data and models for the calendar-aware ``consecutive_periods`` tests.

``orders``: customer 1 in 2024-01/02/03/12 and 2025-01/02/03/05/06 (amounts
150, 500, 120, 220, 140, 200, 140, 225, 210); customer 2 in
2024-02/03/04/06/07/08 (300, 50, 300, 80, 300, 300). ``ticks`` holds one
four-row series per granularity (``streak_<g>``: buckets 1, 2, 4, 5;
``shift_<g>``: buckets 1, 2, 3, 5 with amounts 1, 2, 3, 5).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction

from tests._engine_helpers import seeded_exec_engine

C1_STREAK = [1, 2, 3, 1, 2, 3, 4, 1, 2]
C1_MONTHS = [
    "2024-01", "2024-02", "2024-03", "2024-12",
    "2025-01", "2025-02", "2025-03", "2025-05", "2025-06",
]

ORDERS_ROWS = [
    # (id, customer_id, amount, status, order_date)
    (1, 1, 150.0, "ok", "2024-01-10 09:00:00"),
    (2, 1, 500.0, "ok", "2024-02-10 09:00:00"),
    (3, 1, 120.0, "ok", "2024-03-10 09:00:00"),
    (4, 1, 220.0, "ok", "2024-12-10 09:00:00"),
    (5, 1, 140.0, "ok", "2025-01-10 09:00:00"),
    (6, 1, 200.0, "ok", "2025-02-10 09:00:00"),
    (7, 1, 140.0, "ok", "2025-03-10 09:00:00"),
    (8, 1, 225.0, "ok", "2025-05-10 09:00:00"),
    (9, 1, 210.0, "ok", "2025-06-10 09:00:00"),
    (10, 2, 300.0, "ok", "2024-02-12 09:00:00"),
    (11, 2, 50.0, "ok", "2024-03-12 09:00:00"),
    (12, 2, 300.0, "ok", "2024-04-12 09:00:00"),
    (13, 2, 80.0, "ok", "2024-06-12 09:00:00"),
    (14, 2, 300.0, "ok", "2024-07-12 09:00:00"),
    (15, 2, 300.0, "ok", "2024-08-12 09:00:00"),
]
NULL_DATE_ROW = (16, 1, 300.0, "ok", None)

#: granularity -> timestamps of buckets 1, 2, 4, 5 (bucket 3 empty).
STREAK_SERIES = {
    "second": ["2024-03-10 10:00:01", "2024-03-10 10:00:02",
               "2024-03-10 10:00:04", "2024-03-10 10:00:05"],
    "minute": ["2024-03-10 10:01:30", "2024-03-10 10:02:30",
               "2024-03-10 10:04:30", "2024-03-10 10:05:30"],
    "hour": ["2024-03-10 01:15:00", "2024-03-10 02:15:00",
             "2024-03-10 04:15:00", "2024-03-10 05:15:00"],
    "day": ["2024-03-01 08:00:00", "2024-03-02 08:00:00",
            "2024-03-04 08:00:00", "2024-03-05 08:00:00"],
    # Monday weeks of 01-01, 01-08, 01-22, 01-29.
    "week": ["2024-01-03 08:00:00", "2024-01-10 08:00:00",
             "2024-01-24 08:00:00", "2024-01-31 08:00:00"],
    # Sunday weeks of 12-31, 01-07, 01-21, 01-28 (Sat 01-06 and Sun 01-07
    # would share one Monday week).
    "week_sunday": ["2024-01-06 08:00:00", "2024-01-07 08:00:00",
                    "2024-01-21 08:00:00", "2024-01-28 08:00:00"],
    "month": ["2024-01-15 08:00:00", "2024-02-15 08:00:00",
              "2024-04-15 08:00:00", "2024-05-15 08:00:00"],
    "quarter": ["2024-01-15 08:00:00", "2024-04-15 08:00:00",
                "2024-10-15 08:00:00", "2025-01-15 08:00:00"],
    "year": ["2020-06-01 08:00:00", "2021-06-01 08:00:00",
             "2023-06-01 08:00:00", "2024-06-01 08:00:00"],
}

#: granularity -> timestamps of buckets 1, 2, 3, 5 (bucket 4 empty).
SHIFT_SERIES = {
    "hour": ["2024-03-10 01:00:00", "2024-03-10 02:00:00",
             "2024-03-10 03:00:00", "2024-03-10 05:00:00"],
    "minute": ["2024-03-10 01:01:00", "2024-03-10 01:02:00",
               "2024-03-10 01:03:00", "2024-03-10 01:05:00"],
    "second": ["2024-03-10 01:00:01", "2024-03-10 01:00:02",
               "2024-03-10 01:00:03", "2024-03-10 01:00:05"],
}
_SHIFT_AMOUNTS = [1.0, 2.0, 3.0, 5.0]


def ticks_rows() -> list:
    rows: list = []
    for g, stamps in STREAK_SERIES.items():
        rows += [(f"streak_{g}", 1.0, ts) for ts in stamps]
    for g, stamps in SHIFT_SERIES.items():
        rows += [(f"shift_{g}", a, ts) for a, ts in zip(_SHIFT_AMOUNTS, stamps)]
    return [(i, *r) for i, r in enumerate(rows, start=1)]


def calendar_models() -> list[SlayerModel]:
    """``[orders, ticks]``."""
    return [
        SlayerModel(
            name="orders", data_source="test", sql_table="orders",
            default_time_dimension="order_date",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="status", type=DataType.TEXT),
                Column(name="order_date", type=DataType.TIMESTAMP),
            ],
        ),
        SlayerModel(
            name="ticks", data_source="test", sql_table="ticks",
            default_time_dimension="ts",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="series", type=DataType.TEXT),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="ts", type=DataType.TIMESTAMP),
            ],
        ),
    ]


def _seeder(dialect: str, orders_rows: list):
    ticks = ticks_rows()

    def _seed_sqlite(db_path: str) -> None:
        with transaction(db_path) as con:
            con.execute(
                "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
                "amount REAL, status TEXT, order_date TEXT)"
            )
            con.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", orders_rows)
            con.execute(
                "CREATE TABLE ticks (id INTEGER PRIMARY KEY, series TEXT, "
                "amount REAL, ts TEXT)"
            )
            con.executemany("INSERT INTO ticks VALUES (?,?,?,?)", ticks)

    def _seed_duckdb(db_path: str) -> None:
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(db_path)
        con.execute(
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount DOUBLE, "
            "status VARCHAR, order_date TIMESTAMP)"
        )
        con.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", orders_rows)
        con.execute(
            "CREATE TABLE ticks (id INTEGER, series VARCHAR, amount DOUBLE, "
            "ts TIMESTAMP)"
        )
        con.executemany("INSERT INTO ticks VALUES (?,?,?,?)", ticks)
        con.close()

    return _seed_duckdb if dialect == "duckdb" else _seed_sqlite


@asynccontextmanager
async def calendar_engine(
    dialect: str, orders_rows: list,
) -> AsyncGenerator[SlayerQueryEngine]:
    """An executing engine over ``orders_rows`` plus the ``ticks`` series."""
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    async with seeded_exec_engine(
        dialect=dialect, seed=_seeder(dialect, orders_rows), models=calendar_models(),
    ) as (engine, _):
        yield engine


def orders_query(*, formula: str, filters=None, dimensions=None, date_range=None,
                 granularity: str = "month") -> SlayerQuery:
    """One measure ``v`` over customer 1's series unless ``filters`` is given."""
    td: dict = {"dimension": "order_date", "granularity": granularity}
    if date_range is not None:
        td["date_range"] = date_range
    return SlayerQuery.model_validate({
        "source_model": "orders",
        "time_dimensions": [td],
        "dimensions": dimensions or [],
        "filters": ["customer_id = 1"] if filters is None else filters,
        "measures": [{"formula": formula, "name": "v"}],
    })


def ticks_query(*, series: str, granularity: str, measures: list) -> SlayerQuery:
    return SlayerQuery.model_validate({
        "source_model": "ticks",
        "time_dimensions": [{"dimension": "ts", "granularity": granularity}],
        "filters": [f"series = '{series}'"],
        "measures": measures,
    })


def month_of(value) -> Optional[str]:
    return None if value is None else str(value)[:7]


def int_or_none(value) -> Optional[int]:
    return None if value is None else int(value)
