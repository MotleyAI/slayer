"""Shared fixtures for ``SLAYER_NOW``: one seeded SQLite ``ev`` table, its model, and an id oracle."""

from __future__ import annotations

import contextlib
import os
from collections.abc import AsyncGenerator, Callable
from datetime import datetime
from typing import Optional

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._engine_helpers import seeded_exec_engine

PIN = "2025-07-15T12:00:00"
LAST_3_MONTHS = "ts = 'last 3 months'"

EV_TS: dict[int, str] = {
    1: "2025-03-31 12:00:00", 2: "2025-04-01 00:00:00", 3: "2025-05-10 12:00:00",
    4: "2025-06-30 12:00:00", 5: "2025-07-01 00:00:00",
    6: "2025-07-14 17:59:59", 7: "2025-07-14 18:00:00", 8: "2025-07-14 23:59:59",
    9: "2025-07-15 00:00:00", 10: "2025-07-15 23:59:59", 11: "2025-07-16 00:00:00",
    12: "2025-10-15 12:00:00", 13: "2025-12-31 23:00:00", 14: "2026-07-01 00:00:00",
}

EV = TableSpec(
    name="ev", columns=[("id", "INT"), ("ts", "TIMESTAMP"), ("amount", "DOUBLE")],
    rows=[(i, ts, float(i)) for i, ts in EV_TS.items()],
)


def ev_model() -> SlayerModel:
    return SlayerModel(
        name="ev", sql_table="ev", data_source="test", default_time_dimension="ts",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="ts", type=DataType.TIMESTAMP),
            Column(name="amount", type=DataType.DOUBLE),
        ],
    )


def ids_in(start: datetime, end: datetime) -> set[int]:
    """Row ids with ``start <= ts < end``."""
    return {i for i, ts in EV_TS.items() if start <= datetime.fromisoformat(ts) < end}


PINNED_WINDOW = ids_in(datetime(2025, 4, 1), datetime(2025, 7, 1))


def ids_query(*filters: str) -> dict:
    return {"source_model": "ev", "dimensions": ["id"], "filters": list(filters)}


def row_ids(rows: list[dict]) -> set[int]:
    return {int(r["ev.id"]) for r in rows}


async def ids(engine: SlayerQueryEngine, *filters: str) -> set[int]:
    resp = await engine.execute(SlayerQuery.model_validate(ids_query(*filters)))
    return row_ids(resp.data)


@contextlib.asynccontextmanager
async def now_engine(
    *, clock: Optional[Callable[[], datetime]] = None,
) -> AsyncGenerator[SlayerQueryEngine]:
    """Executing SQLite engine over ``ev``; built without a clock unless ``clock`` is given."""
    async with seeded_exec_engine(
        dialect="sqlite", seed=lambda p: seed_backend("sqlite", p, [EV]), models=[ev_model()], clock=clock,
    ) as (engine, _db):
        yield engine


async def seeded_storage(base_dir: str) -> YAMLStorage:
    """A YAML storage over a seeded SQLite ``ev`` table under ``base_dir``."""
    db_path = os.path.join(base_dir, "ev.db")
    seed_backend("sqlite", db_path, [EV])
    storage = YAMLStorage(base_dir=os.path.join(base_dir, "store"))
    await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=db_path))
    await storage.save_model(ev_model())
    return storage
