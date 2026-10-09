"""Gapped ``streak_events`` series for the server-dialect calendar-streak cases.

One series per granularity (buckets 1, 2, 4, 5; bucket 3 empty), stored in a
DATE column ``d`` and a TIMESTAMP column ``ts``; the streak is 1, 2, 1, 2.
"""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine

#: granularity -> dates of buckets 1, 2, 4, 5.
_SERIES = {
    "month": ["2024-01-15", "2024-02-15", "2024-04-15", "2024-05-15"],
    "quarter": ["2024-01-15", "2024-04-15", "2024-10-15", "2025-01-15"],
    # Sunday weeks of 12-31, 01-07, 01-21, 01-28.
    "week_sunday": ["2024-01-06", "2024-01-07", "2024-01-21", "2024-01-28"],
}

#: ``(column, granularity)`` pairs every server dialect executes.
CALENDAR_CASES = [(col, g) for col in ("d", "ts") for g in _SERIES]


def seed_statements(
    *,
    date_type: str = "DATE",
    timestamp_type: str = "TIMESTAMP",
    int_type: str = "INTEGER",
    text_type: str = "VARCHAR(32)",
    float_type: str = "FLOAT",
    table_options: str = "",
    typed_temporals: bool = False,
) -> list[str]:
    """``CREATE TABLE streak_events`` plus one literal multi-row INSERT (typed DATE / TIMESTAMP literals on request)."""
    date_kw, ts_kw = ("DATE ", "TIMESTAMP ") if typed_temporals else ("", "")
    values = ", ".join(
        f"({i}, '{g}', 1, {date_kw}'{day}', {ts_kw}'{day} 13:45:00')"
        for i, (g, day) in enumerate(
            ((g, day) for g, days in _SERIES.items() for day in days), start=1,
        )
    )
    return [
        f"CREATE TABLE streak_events (id {int_type}, series {text_type}, "
        f"amount {float_type}, d {date_type}, ts {timestamp_type}) {table_options}",
        f"INSERT INTO streak_events (id, series, amount, d, ts) VALUES {values}",
    ]


def streak_model(data_source: str) -> SlayerModel:
    return SlayerModel(
        name="streak_events", sql_table="streak_events", data_source=data_source,
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="series", sql="series", type=DataType.TEXT),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="d", sql="d", type=DataType.DATE),
            Column(name="ts", sql="ts", type=DataType.TIMESTAMP),
        ],
    )


async def assert_calendar_streak(
    engine: SlayerQueryEngine, *, column: str, granularity: str,
) -> None:
    resp = await engine.execute(SlayerQuery.model_validate({
        "source_model": "streak_events",
        "time_dimensions": [{"dimension": column, "granularity": granularity}],
        "filters": [f"series = '{granularity}'"],
        "measures": [{"formula": "consecutive_periods(sum(amount) > 0)", "name": "v"}],
    }))
    rows = sorted(resp.data, key=lambda r: str(r[f"streak_events.{column}"]))
    assert [int(r["streak_events.v"]) for r in rows] == [1, 2, 1, 2], rows
