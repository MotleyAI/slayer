"""Time-of-day scenario data (spec: queries/time-of-day), seeded on SQLite and DuckDB.

shifts (id, starts_at, ends_at, created_at, seen_at, order_date, amount, is_paid, label):
   1  09:30:00  17:00:00     2024-01-15 10:30:45  2025-01-01 03:30:00Z  2024-01-15  10  true   a
   2  09:30:00  NULL         2024-01-16 08:00:00  2025-01-01 03:30:00Z  2024-01-16  20  false  b
   3  17:00:00  23:00:00.25  2024-01-17 23:59:59  2025-01-01 03:30:00Z  2024-01-17  30  true   c
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, Callable, Iterable, Mapping
from datetime import datetime, time
from typing import Any, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.keys import ValueKey
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.core.scope import DatasetScope
from slayer.core.syntax import parse_expr
from slayer.engine.binding import bind_expr
from slayer.engine.key_metadata import SlotTyping
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._engine_helpers import _engine_generate, seed_tables, seeded_exec_engine

BACKENDS = ("sqlite", "duckdb")
NY = "America/New_York"
SEEN = "2025-01-01 03:30:00+00:00"  # 2024-12-31 22:30 in New York

#: Per-backend (time, timestamp, instant, date, double, boolean, text) column types.
_TYPES = {
    "sqlite": ("TEXT", "TEXT", "TEXT", "TEXT", "REAL", "INTEGER", "TEXT"),
    "duckdb": ("TIME", "TIMESTAMP", "TIMESTAMPTZ", "DATE", "DOUBLE", "BOOLEAN", "VARCHAR"),
}
ROWS: list[tuple[Any, ...]] = [
    (1, "09:30:00", "17:00:00", "2024-01-15 10:30:45", SEEN, "2024-01-15", 10.0, True, "a"),
    (2, "09:30:00", None, "2024-01-16 08:00:00", SEEN, "2024-01-16", 20.0, False, "b"),
    (3, "17:00:00", "23:00:00.25", "2024-01-17 23:59:59", SEEN, "2024-01-17", 30.0, True, "c"),
]
ALL_IDS = frozenset({1, 2, 3})


def shifts_columns(backend: str) -> str:
    tod, ts, tstz, day, double, boolean, text = _TYPES[backend]
    return (f"id INTEGER PRIMARY KEY, starts_at {tod}, ends_at {tod}, created_at {ts}, seen_at {tstz}, "
            f"order_date {day}, amount {double}, is_paid {boolean}, label {text}")


def shifts_dataset(*, data_source: str = "test", extra_fields: Iterable[DatasetField] = (),
                   **extra: Any) -> SlayerDataset:
    return SlayerDataset.model_validate({
        "name": "shifts", "sql_table": "shifts", "data_source": data_source,
        "fields": [
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="starts_at", datatype=DataType.TIME),
            DatasetField(name="ends_at", datatype=DataType.TIME),
            DatasetField(name="created_at", datatype=DataType.DATETIME),
            DatasetField(name="seen_at", datatype=DataType.DATETIME_TZ),
            DatasetField(name="order_date", datatype=DataType.DATE),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="is_paid", datatype=DataType.BOOLEAN),
            DatasetField(name="label", datatype=DataType.STRING),
            *extra_fields,
        ],
        **extra,
    })


@contextlib.asynccontextmanager
async def shifts_engine(
    backend: str, *, dataset: Optional[SlayerDataset] = None, rows: Optional[list[tuple[Any, ...]]] = None,
    columns: Optional[str] = None, clock: Optional[Callable[[], datetime]] = None,
    datasource_fields: Optional[Mapping[str, Any]] = None,
) -> AsyncGenerator[SlayerQueryEngine]:
    """An executing engine over ``shifts`` (``rows`` / ``columns`` override the seeded table)."""
    if backend == "duckdb":
        pytest.importorskip("duckdb")
    table = {"shifts": (columns or shifts_columns(backend), ROWS if rows is None else rows)}
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_tables(p, table), datasets=[dataset or shifts_dataset()],
        clock=clock, datasource_fields=datasource_fields,
    ) as (engine, _):
        yield engine


def q(**fields: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"population": "shifts", **fields})


def d(expression: str, alias: str = "v") -> dict[str, str]:
    return {"field": expression, "alias": alias}


def m(expression: str, alias: str = "v") -> dict[str, str]:
    return {"metric": expression, "alias": alias}


def ids(resp: SlayerResponse) -> set[int]:
    return {int(r["shifts.id"]) for r in resp.data}


async def filtered_ids(engine: SlayerQueryEngine, *filters: str, **extra: Any) -> set[int]:
    return ids(await engine.execute(q(dimensions=["shifts.id"], filter=list(filters), **extra)))


async def value_of(engine: SlayerQueryEngine, expression: str, *, row: int = 1, **extra: Any) -> Any:
    """``expression`` as a dimension at ``shifts.id = row``."""
    resp = await engine.execute(q(dimensions=[d(expression)], filter=[f"shifts.id = {row}"], **extra))
    assert len(resp.data) == 1, resp.data
    return resp.data[0]["v"]


def is_time(value: Any, expected: time) -> bool:
    """A naive ``datetime.time`` equal to ``expected``: no text, duration or zoned time."""
    return type(value) is time and value.tzinfo is None and value == expected


def bind(expression: str, *, dialect: str = "duckdb") -> ValueKey:
    dataset = shifts_dataset()
    bundle = ResolvedSourceBundle(dialect=dialect, population=dataset)
    return bind_expr(parse_expr(expression), scope=DatasetScope(population=dataset), bundle=bundle,
                     allow_metrics=True).value_key


def planned_type(expression: str, *, dialect: str = "duckdb") -> Optional[DataType]:
    dataset = shifts_dataset()
    bundle = ResolvedSourceBundle(dialect=dialect, population=dataset)
    return SlotTyping(scope=DatasetScope(population=dataset), bundle=bundle).value_type(bind(expression, dialect=dialect))


async def emit(query: SlayerQuery, *, dialect: str, clock: Optional[Callable[[], datetime]] = None,
               datasource_fields: Optional[Mapping[str, Any]] = None) -> str:
    """Dry-run SQL for ``query`` over ``shifts`` on ``dialect``."""
    return await _engine_generate(query=query, dataset=shifts_dataset(), dialect=dialect, clock=clock,
                                  datasource_fields=datasource_fields)
