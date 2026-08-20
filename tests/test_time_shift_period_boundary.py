"""time_shift/change must not lose rows at period boundaries (DEV-1811 audit).

The shifted CTE applied the calendar offset to RAW timestamps before
DATE_TRUNC. On non-clamping dialects (SQLite: Jan 31 + 1 month = Mar 2) rows
from the tail of a month land in the wrong shifted bucket, so the previous
period's re-aggregation silently drops them. Found by the audit's pandas
oracle: both the branch and PyPI 0.9.12 returned change(Feb) computed from a
January total that excluded an order created 2024-01-31 23:59:59.

The fix shifts the TRUNCATED period start instead (month-start + 1 month can
never overflow), then re-truncates for non-aligned offsets.
"""

from __future__ import annotations

import asyncio
import sqlite3
import tempfile
from pathlib import Path

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine


def _make_engine(rows: list[tuple[int, float, str]]) -> SlayerQueryEngine:
    from slayer.storage.yaml_storage import YAMLStorage

    tmp = tempfile.mkdtemp()
    db_path = str(Path(tmp) / "boundary.db")
    con = sqlite3.connect(db_path)
    con.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, cost REAL, created_at TEXT)")
    con.executemany("INSERT INTO orders VALUES (?, ?, ?)", rows)
    con.commit()
    con.close()

    storage = YAMLStorage(base_dir=tmp)
    asyncio.run(storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=db_path)))
    asyncio.run(storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="cost", sql="cost", type=DataType.DOUBLE),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        ],
    )))
    return SlayerQueryEngine(storage=storage)


def _monthly(engine: SlayerQueryEngine, shift: str) -> dict[str, float | None]:
    resp = engine.execute_sync(query={
        "source_model": "orders",
        "measures": ["cost:sum",
                     {"formula": f"time_shift(cost:sum, -1, '{shift}')", "name": "prev"}],
        "time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
        "order": [{"column": "created_at", "direction": "asc"}],
    })
    return {str(r["orders.created_at"])[:7]: r["orders.prev"] for r in resp.data}


def test_month_shift_keeps_last_instant_of_month():
    engine = _make_engine([
        (1, 100.0, "2024-01-10 12:00:00"),
        (2, 50.0, "2024-01-31 23:59:59"),  # lost pre-fix: Jan 31 + 1m = Mar 2
        (3, 70.0, "2024-02-10 09:00:00"),
    ])
    prev = _monthly(engine, "month")
    assert prev["2024-02"] == pytest.approx(150.0)


def test_month_shift_keeps_days_29_to_31():
    engine = _make_engine([
        (1, 10.0, "2024-01-29 08:00:00"),
        (2, 20.0, "2024-01-30 08:00:00"),
        (3, 40.0, "2024-01-31 08:00:00"),
        (4, 7.0, "2024-02-05 08:00:00"),
    ])
    prev = _monthly(engine, "month")
    assert prev["2024-02"] == pytest.approx(70.0)


def test_year_shift_keeps_leap_day():
    engine = _make_engine([
        (1, 500.0, "2024-02-29 10:00:00"),  # lost pre-fix: +1y = Mar 1
        (2, 5.0, "2025-02-10 10:00:00"),
    ])
    prev = _monthly(engine, "year")
    assert prev["2025-02"] == pytest.approx(500.0)


def test_change_uses_full_previous_month():
    engine = _make_engine([
        (1, 100.0, "2024-01-10 12:00:00"),
        (2, 50.0, "2024-01-31 23:59:59"),
        (3, 70.0, "2024-02-10 09:00:00"),
    ])
    resp = engine.execute_sync(query={
        "source_model": "orders",
        "measures": ["cost:sum", {"formula": "change(cost:sum)", "name": "chg"}],
        "time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
        "order": [{"column": "created_at", "direction": "asc"}],
    })
    by_month = {str(r["orders.created_at"])[:7]: r["orders.chg"] for r in resp.data}
    assert by_month["2024-02"] == pytest.approx(70.0 - 150.0)
