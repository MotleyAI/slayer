"""``slayer`` with its startup preparation stubbed and held until the ``GATE_RELEASE`` file exists.

Run as ``python -m tests._mcp_gated_server <scenario> <slayer args...>``. Scenarios:

- ``demo_ok``: the demo setup (``cli.ensure_demo_datasource``) waits, then stores a 3-row ``orders`` model.
- ``demo_fail``: the demo setup waits, then raises ``generator exploded``.
- ``progress_raises``: ``demo_ok`` with every progress notification failing to send.
- ``ingest_errors``: each startup ingest waits; datasource ``broken`` raises, others report a table error.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path
from typing import Any

from mcp.server.session import ServerSession

from slayer import cli
from slayer.async_utils import run_sync
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine import ingestion
from slayer.engine.schema_drift import IdempotentIngestResult, IngestionError
from slayer.storage.sqlite_conn import transaction

ORDERS_ROWS = 3
_RELEASE = Path(os.environ.get("GATE_RELEASE", "/nonexistent-gate-release"))


def tiny_orders_datasource(*, directory: str, name: str) -> tuple[DatasourceConfig, SlayerModel]:
    """A SQLite datasource with an ``orders`` table of ``ORDERS_ROWS`` rows, and its model."""
    db = os.path.join(directory, f"{name}.sqlite")
    with transaction(db) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY)")
        conn.execute("DELETE FROM orders")
        conn.executemany("INSERT INTO orders (id) VALUES (?)", [(i,) for i in range(ORDERS_ROWS)])
    ds = DatasourceConfig(name=name, type="sqlite", database=db)
    model = SlayerModel(
        name="orders", sql_table="orders", data_source=name,
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
    )
    return ds, model


def _wait_release() -> None:
    while not _RELEASE.exists():
        time.sleep(0.05)


async def _await_release() -> None:
    while not _RELEASE.exists():
        await asyncio.sleep(0.05)


def _demo_stub(scenario: str):
    def setup(storage: Any, **kwargs: Any) -> tuple[DatasourceConfig, list[SlayerModel], bool]:
        print("stub demo: building", file=kwargs.get("stream") or sys.stderr, flush=True)
        _wait_release()
        if scenario == "demo_fail":
            raise RuntimeError("generator exploded")
        ds, model = tiny_orders_datasource(directory=kwargs["storage_path"], name="jaffle_shop")
        run_sync(storage.save_datasource(ds))
        run_sync(storage.save_model(model))
        return ds, [model], True

    return setup


async def _ingest_stub(*, datasource: DatasourceConfig, **_kwargs: Any) -> IdempotentIngestResult:
    await _await_release()
    if datasource.name == "broken":
        raise RuntimeError("cannot reach broken")
    return IdempotentIngestResult(errors=[
        IngestionError(model_name="widgets", data_source=datasource.name, error="table exploded"),
    ])


async def _progress_fails(*_args: Any, **_kwargs: Any) -> None:
    raise RuntimeError("progress channel broke")


def main() -> None:
    scenario, *rest = sys.argv[1:]
    if scenario in ("demo_ok", "demo_fail", "progress_raises"):
        cli.ensure_demo_datasource = _demo_stub(scenario)
    if scenario == "progress_raises":
        ServerSession.send_progress_notification = _progress_fails
    if scenario == "ingest_errors":
        ingestion.ingest_datasource_idempotent = _ingest_stub
    sys.argv = ["slayer", *rest]
    cli.main()


if __name__ == "__main__":
    main()
