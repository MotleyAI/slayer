"""A persistent store over the time-zone scenario data, for the surface tests (REST, MCP, CLI)."""

from __future__ import annotations

import os
from typing import Any

from pydantic import ValidationError

from slayer.core.errors import TimeZoneError
from slayer.core.models import DatasourceConfig
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev2000_fixtures import DS, FISCAL_WEEK, NY, default_datasets, default_tables, naive, seed
from tests._engine_helpers import owned_query_engine

#: The spec's local-day scenario: day buckets of ``ordered_at`` in New York.
LOCAL_DAY: dict[str, Any] = {
    "population": "orders",
    "time_dimensions": [{"field": "orders.ordered_at", "granularity": "day"}],
    "metrics": [{"metric": "sum(orders.amount)", "alias": "total"}],
    "time_zone": NY,
}
LOCAL_DAY_ROWS = {"2024-12-31": 10.0, "2025-01-01": 20.0, "2025-01-31": 30.0, "2025-03-09": 40.0}

MONTHLY: dict[str, Any] = {
    "population": "orders",
    "time_dimensions": [{"field": "orders.ordered_at", "granularity": "month"}],
    "metrics": [{"metric": "sum(orders.amount)", "alias": "revenue"}],
}
MONTHLY_STORED = {"2025-01": 60.0, "2025-03": 40.0}
MONTHLY_NY = {"2024-12": 10.0, "2025-01": 50.0, "2025-03": 40.0}
MONTHLY_TOKYO = {"2025-01": 30.0, "2025-02": 30.0, "2025-03": 40.0}


def local_days(rows: list[dict[str, Any]]) -> dict[str, float]:
    return {naive(r["orders.ordered_at"]).date().isoformat(): float(r["total"]) for r in rows}


def months(rows: list[dict[str, Any]], *, value: str = "revenue") -> dict[str, float]:
    return {str(r["orders.ordered_at"])[:7]: float(r[value]) for r in rows}


async def scenario_store(base_dir: str, *, saved: dict[str, Any] | None = None) -> YAMLStorage:
    """A SQLite-backed store under ``base_dir`` with datasource ``shop`` (``time_zone: UTC``), the scenario
    datasets and the ``saved`` query-backed datasets (name → query)."""
    db = os.path.join(base_dir, "shop.db")
    seed("sqlite", db, default_tables())
    storage = YAMLStorage(base_dir=os.path.join(base_dir, "store"))
    datasource = DatasourceConfig.model_validate(
        {"name": DS, "type": "sqlite", "database": db, "time_zone": "UTC", "granularities": [FISCAL_WEEK]})
    async with owned_query_engine(storage=storage, datasources=[datasource]) as engine:
        for dataset in default_datasets():
            await storage.save_dataset(dataset)
        for name, query in (saved or {}).items():
            await engine.create_dataset_from_query(query=query, name=name)
    return storage


def zone_error(exc: BaseException) -> TimeZoneError:
    """The ``TimeZoneError`` ``exc`` is, or that a pydantic validation error wraps."""
    if isinstance(exc, TimeZoneError):
        return exc
    assert isinstance(exc, ValidationError), f"not a time-zone error: {exc!r}"
    found = [(e.get("ctx") or {})["error"] for e in exc.errors()
             if isinstance((e.get("ctx") or {}).get("error"), TimeZoneError)]
    assert found, f"no TimeZoneError inside: {exc}"
    return found[0]
