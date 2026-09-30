"""Shared fixtures for time points: a pinned clock, seeded SQLite/DuckDB tables, a Python oracle, query helpers."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, Iterable
from datetime import date, datetime, timedelta
from typing import Any, Optional

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._engine_helpers import seeded_exec_engine

BACKENDS = ("sqlite", "duckdb")

NOW = datetime(2026, 9, 29, 12, 0, 0)  # a Tuesday
ROLLOVER_NOW = datetime(2027, 1, 1, 0, 0, 0, 500000)


class PinnedClock:
    """Returns ``now`` (advanced by ``step`` per call) and counts calls."""

    def __init__(self, now: datetime = NOW, *, step: timedelta = timedelta(0)) -> None:
        self.now = now
        self.step = step
        self.calls = 0

    def __call__(self) -> datetime:
        value = self.now + self.step * self.calls
        self.calls += 1
        return value


# id → ts; midnight and mid-day rows on every boundary the scenarios use.
EV_TS: dict[int, str] = {
    1: "2024-01-01 00:00:00", 2: "2024-01-01 10:00:00",
    3: "2024-06-01 00:00:00", 4: "2024-06-01 10:00:00",
    5: "2024-12-31 00:00:00", 6: "2024-12-31 10:00:00",
    7: "2025-01-01 00:00:00", 8: "2025-01-26 23:59:59",
    9: "2025-01-27 00:00:00", 10: "2025-02-02 23:00:00", 11: "2025-02-03 00:00:00",
    12: "2025-03-01 09:30:00", 13: "2025-03-01 10:30:00",
    14: "2025-03-15 00:00:00", 15: "2025-03-15 18:00:00",
    16: "2025-03-31 23:59:59", 17: "2025-04-01 00:00:00",
    18: "2025-09-15 08:00:00", 19: "2025-12-31 23:00:00", 20: "2026-01-01 00:00:00",
    21: "2026-04-10 10:00:00", 22: "2026-05-20 10:00:00", 23: "2026-06-15 10:00:00",
    24: "2026-08-01 00:00:00", 25: "2026-08-31 23:59:59", 26: "2026-09-01 00:00:00",
    27: "2026-09-28 15:00:00", 28: "2026-09-29 06:00:00",
    29: "2026-09-29 11:10:00", 30: "2026-09-29 12:10:00", 31: "2026-10-01 00:00:00",
    32: "2024-02-29 12:00:00", 33: "2020-12-28 00:00:00",
    34: "2021-01-03 23:00:00", 35: "2021-01-04 00:00:00",
    36: "2024-01-10 00:00:00", 37: "2024-01-10 00:00:01",
    38: "2024-05-15 12:00:00", 39: "2026-07-10 10:00:00",
}

# Ids shipped in a later month than ordered, and never shipped; every other row ships the same day.
SHIPPED_NEXT_MONTH = frozenset({6, 16, 25})
UNSHIPPED = frozenset({14, 23})

CODES = {1: "2025", 2: "2025-Q1", 3: "x"}


def code_of(i: int) -> str:
    return CODES.get(i, "y")


def ev_dt(i: int) -> datetime:
    return datetime.fromisoformat(EV_TS[i])


def shipped_dt(i: int) -> Optional[datetime]:
    if i in UNSHIPPED:
        return None
    ts = ev_dt(i)
    return ts + timedelta(days=2) if i in SHIPPED_NEXT_MONTH else ts


def _shipped(i: int, sep: str) -> Optional[str]:
    shipped = shipped_dt(i)
    return None if shipped is None else str(shipped).replace(" ", sep)


def _ev_rows(*, sep: str = " ") -> list[tuple]:
    return [
        (i, ts.replace(" ", sep), ts[:10], _shipped(i, sep), code_of(i),
         float(i), 1 + i % 3, ts)
        for i, ts in EV_TS.items()
    ]


_EV_COLUMNS = [
    ("id", "INT"), ("ts", "TIMESTAMP"), ("d", "DATE"), ("shipped_at", "TIMESTAMP"),
    ("code", "TEXT"), ("amount", "DOUBLE"), ("customer_id", "INT"), ("raw_ts", "TEXT"),
]

EV = TableSpec(name="ev", columns=_EV_COLUMNS, rows=_ev_rows())
# SQLite ``T``-separated timestamp storage (DuckDB normalises on insert).
EV_T = TableSpec(name="ev_t", columns=_EV_COLUMNS, rows=_ev_rows(sep="T"))

# SQLite date-only, space-, ``T``- and millisecond-spelled text in one TIMESTAMP column.
MIXED = TableSpec(
    name="mixed", columns=[("id", "INT"), ("ts", "TIMESTAMP")],
    rows=[
        (1, "2025-03-01"), (2, "2025-03-01 00:00:00"), (3, "2025-02-28 23:00:00"),
        (4, "2025-03-01 10:00:00.250"), (5, "2025-03-01 10:00:00.249"), (6, "2025-03-01T10:00:00.251"),
    ],
)

CUSTOMERS = TableSpec(
    name="customers",
    columns=[("id", "INT"), ("created_at", "TIMESTAMP")],
    rows=[(1, "2025-01-15 10:00:00"), (2, "2025-04-01 00:00:00"), (3, "2024-12-31 23:00:00")],
)


def _ev_columns() -> list[Column]:
    return [
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="ts", type=DataType.TIMESTAMP),
        Column(name="d", type=DataType.DATE),
        Column(name="shipped_at", type=DataType.TIMESTAMP),
        Column(name="code", type=DataType.TEXT),
        Column(name="amount", type=DataType.DOUBLE),
        Column(name="customer_id", type=DataType.INT),
        Column(name="raw_ts"),  # untyped
        Column(name="ts_copy", sql="ts", type=DataType.TIMESTAMP),  # derived
    ]


def ev_model(*, name: str = "ev", table: str = "ev") -> SlayerModel:
    return SlayerModel(
        name=name, sql_table=table, data_source="test", default_time_dimension="ts",
        columns=_ev_columns(),
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


def customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="created_at", type=DataType.TIMESTAMP),
        ],
    )


def mixed_model() -> SlayerModel:
    return SlayerModel(
        name="mixed", sql_table="mixed", data_source="test",
        columns=[Column(name="id", type=DataType.INT, primary_key=True), Column(name="ts", type=DataType.TIMESTAMP)],
    )


def daily_stage_model() -> SlayerModel:
    """A multi-stage (``source_queries``) model with a TIMESTAMP stage column ``ts``."""
    return SlayerModel(
        name="daily", data_source="test",
        source_queries=[SlayerQuery.model_validate({
            "source_model": "ev",
            "time_dimensions": [{"dimension": "ts", "granularity": "day"}],
            "measures": [{"formula": "sum(amount)", "name": "rev"}],
        })],
    )


def all_models() -> list[SlayerModel]:
    return [
        ev_model(), ev_model(name="ev_t", table="ev_t"), customers_model(), mixed_model(), daily_stage_model(),
    ]


@contextlib.asynccontextmanager
async def tp_engine(
    backend: str, *, clock: Optional[PinnedClock] = None, tables: Optional[Iterable[TableSpec]] = None,
) -> AsyncGenerator[SlayerQueryEngine]:
    """Executing engine over the seeded time-point tables with a pinned clock."""
    seeded = list(tables) if tables is not None else [EV, CUSTOMERS, MIXED] + ([EV_T] if backend == "sqlite" else [])
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, seeded),
        models=all_models(), clock=clock if clock is not None else PinnedClock(),
    ) as (engine, _db):
        yield engine


# ---------------------------------------------------------------------------
# Oracle
# ---------------------------------------------------------------------------

def ids_where(pred) -> set[int]:
    return {i for i in EV_TS if pred(ev_dt(i))}


def ids_in(start: datetime, end: datetime) -> set[int]:
    """Row ids with ``start <= ts < end``."""
    return ids_where(lambda t: start <= t < end)


def month_key(value: Any) -> str:
    """A bucket value (ISO text on SQLite, date/datetime on DuckDB) as ``YYYY-MM``."""
    return str(value)[:7]


def bucket_text(value: Any) -> str:
    """A bucket value as ``YYYY-MM-DD HH:MM:SS`` text."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return f"{value.isoformat()} 00:00:00"
    text = str(value).replace("T", " ")
    return text if len(text) > 10 else f"{text} 00:00:00"


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

async def ids(engine: SlayerQueryEngine, *filters: str, model: str = "ev", **extra: Any) -> set[int]:
    """Row ids of ``model`` passing ``filters`` (plus any extra query fields)."""
    resp = await engine.execute(SlayerQuery.model_validate({
        "source_model": model, "dimensions": ["id"], "filters": list(filters), **extra,
    }))
    return {int(r[f"{model}.id"]) for r in resp.data}


async def ids_with_range(
    engine: SlayerQueryEngine, date_range: Any, *, column: str = "ts", granularity: str = "second",
    model: str = "ev",
) -> set[int]:
    """Row ids counted under a time dimension on ``column`` carrying ``date_range``."""
    resp = await engine.execute(SlayerQuery.model_validate({
        "source_model": model, "dimensions": ["id"],
        "time_dimensions": [{"dimension": column, "granularity": granularity, "date_range": date_range}],
    }))
    return {int(r[f"{model}.id"]) for r in resp.data}


async def monthly(
    engine: SlayerQueryEngine, *, measures: list[Any], filters: Optional[list[str]] = None,
    date_range: Any = None, whole_periods_only: bool = False, column: str = "ts", model: str = "ev",
) -> dict[str, dict[str, Any]]:
    """Monthly buckets → {measure name: value}."""
    td: dict[str, Any] = {"dimension": column, "granularity": "month"}
    if date_range is not None:
        td["date_range"] = date_range
    resp = await engine.execute(SlayerQuery.model_validate({
        "source_model": model, "measures": measures, "time_dimensions": [td],
        "filters": filters or [], "whole_periods_only": whole_periods_only,
    }))
    names = [m["name"] if isinstance(m, dict) else m for m in measures]
    return {
        month_key(r[f"{model}.{column}"]): {n: r[f"{model}.{n}"] for n in names}
        for r in resp.data
    }
