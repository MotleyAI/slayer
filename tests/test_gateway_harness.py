"""The routing harness itself: the guard refuses direct access and admits only the gateway's backend."""

from __future__ import annotations

import sqlite3
import sqlite3.dbapi2 as sqlite_dbapi2
from pathlib import Path
from typing import Any

import duckdb
import psycopg
import pytest

from slayer.core.enums import DataType
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.client import ResultColumnType, SlayerSQLClient
from tests._gateway_harness import (
    DirectAccessAttempted,
    RecordingClient,
    backend_of,
    customers_model,
    no_direct_access,
    orders_model,
    seed_shop,
    shop_datasource,
    shop_storage,
)

COUNT_ORDERS = {"source_model": "orders", "measures": [{"formula": "*:count", "name": "n"}]}


async def test_a_plain_engine_query_is_refused(tmp_path: Path) -> None:
    db = str(tmp_path / "shop.db")
    seed_shop(db)
    storage = await shop_storage(
        str(tmp_path / "store"), datasource=shop_datasource(db), models=[orders_model(), customers_model()],
    )
    engine = SlayerQueryEngine(storage=storage)
    with no_direct_access() as guard, pytest.raises(Exception) as excinfo:  # NOSONAR(S5958) — may surface wrapped
        await engine.execute(COUNT_ORDERS)
    engine.close()
    assert guard.attempts
    assert any(isinstance(e, DirectAccessAttempted) for e in _chain(excinfo.value))


@pytest.mark.parametrize("module", [sqlite3, sqlite_dbapi2, duckdb, psycopg], ids=lambda m: m.__name__)
def test_raw_driver_connects_are_refused(module: Any) -> None:
    with no_direct_access() as guard, pytest.raises(DirectAccessAttempted):
        module.connect(":memory:")
    assert guard.attempts


def _chain(exc: BaseException) -> list[BaseException]:
    out: list[BaseException] = []
    current: BaseException | None = exc
    while current is not None and current not in out:
        out.append(current)
        current = current.__cause__ or current.__context__
    return out


async def test_the_backend_client_runs_under_the_guard(tmp_path: Path) -> None:
    db = str(tmp_path / "shop.db")
    seed_shop(db)
    ds = shop_datasource(db)
    client = RecordingClient(inner=SlayerSQLClient(datasource=backend_of(ds)), datasource=ds)
    with no_direct_access(backends=[backend_of(ds)]) as guard:
        rows = (await client.execute("SELECT COUNT(*) AS n FROM orders")).rows
        types = await client.get_result_column_types("SELECT id FROM orders")
    client.close()
    assert guard.attempts == []
    assert rows == [{"n": 4}]
    assert types == {"id": ResultColumnType(type=DataType.INT)}
    assert client.statements() == ["SELECT COUNT(*) AS n FROM orders", "SELECT id FROM orders"]
