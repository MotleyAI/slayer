"""Date arithmetic shared with date_add: exact time_shift offsets and month-end-clamped trailing windows."""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from tests._dev1737_fixtures import BACKENDS, TableSpec, exec_engine


def _ev_model() -> SlayerModel:
    return SlayerModel(
        name="ev", sql_table="ev", data_source="test", default_time_dimension="ts",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="ts", type=DataType.TIMESTAMP),
            Column(name="d", type=DataType.DATE),
            Column(name="v", type=DataType.DOUBLE),
        ],
    )


def _ev(rows: list[tuple]) -> TableSpec:
    return TableSpec(name="ev", columns=[("id", "INT"), ("ts", "TIMESTAMP"), ("d", "DATE"), ("v", "DOUBLE")], rows=rows)


async def _run(backend: str, rows: list[tuple], query: dict, *, width: int) -> dict[str, object]:
    async with exec_engine(backend, tables=[_ev(rows)], models=[_ev_model()]) as engine:
        resp = await engine.execute(SlayerQuery.model_validate({"source_model": "ev", **query}))
    dim = next(k for k in resp.columns if k not in ("ev.v_sum", "ev.x"))
    return {str(r[dim])[:width]: r["ev.x"] for r in resp.data}


@pytest.mark.parametrize("backend", BACKENDS)
async def test_hourly_time_shift(backend: str) -> None:
    rows = [
        (1, "2024-01-01 10:15:00", "2024-01-01", 1.0),
        (2, "2024-01-01 11:20:00", "2024-01-01", 2.0),
        (3, "2024-01-01 12:30:00", "2024-01-01", 4.0),
    ]
    got = await _run(backend, rows, {
        "time_dimensions": [{"dimension": "ts", "granularity": "hour"}],
        "measures": ["v:sum", {"formula": "time_shift(v:sum, -1, 'hour')", "name": "x"}],
    }, width=16)
    assert got == {"2024-01-01 10:00": None, "2024-01-01 11:00": 1.0, "2024-01-01 12:00": 2.0}


@pytest.mark.parametrize("backend", BACKENDS)
async def test_month_shift_of_month_end_daily_bucket(backend: str) -> None:
    rows = [
        (1, "2024-02-29 09:00:00", "2024-02-29", 10.0),
        (2, "2024-03-02 09:00:00", "2024-03-02", 5.0),
        (3, "2024-03-31 09:00:00", "2024-03-31", 1.0),
    ]
    got = await _run(backend, rows, {
        "time_dimensions": [{"dimension": "d", "granularity": "day"}],
        "measures": ["v:sum", {"formula": "time_shift(v:sum, -1, 'month')", "name": "x"}],
    }, width=10)
    assert got == {"2024-02-29": None, "2024-03-02": None, "2024-03-31": 10.0}


@pytest.mark.parametrize("backend", BACKENDS)
async def test_one_month_window_ending_at_month_end(backend: str) -> None:
    rows = [
        (1, "2024-02-28 09:00:00", "2024-02-28", 1.0),
        (2, "2024-02-29 09:00:00", "2024-02-29", 10.0),
        (3, "2024-03-01 09:00:00", "2024-03-01", 100.0),
        (4, "2024-03-30 09:00:00", "2024-03-30", 1000.0),
    ]
    got = await _run(backend, rows, {
        "time_dimensions": [{"dimension": "d", "granularity": "day"}],
        "measures": [{"formula": "v:sum(window='1m')", "name": "x"}],
    }, width=10)
    assert got["2024-03-30"] == 1110.0
    assert got["2024-03-01"] == 111.0
