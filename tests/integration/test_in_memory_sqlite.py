"""Regression guard: file-backed SQLite must not be affected by the in-memory fix.

The fix for #72 special-cases ``:memory:`` SQLite to use a per-client
``StaticPool`` engine. File-backed SQLite must NOT take that path — the
underlying file is shared across connections, and the existing
module-level engine cache must remain in effect.

Marked integration because it touches a real on-disk SQLite file.
"""

from pathlib import Path
from typing import Iterable

import pytest
import sqlalchemy as sa

from slayer.core.models import DatasourceConfig
from slayer.sql import client as sql_client
from slayer.sql.client import SlayerSQLClient


def _setup(client: SlayerSQLClient, statements: Iterable[str]) -> None:
    """Run DDL/DML on whichever engine the client would use.

    Mirrors the helper in ``tests/test_sql_client_in_memory_async.py``.
    For file-backed SQLite the per-client accessor returns ``None`` and
    the helper falls back to the module-level engine cache.
    """
    getter = getattr(client, "_get_sync_engine_for_client", None)
    engine = getter() if getter is not None else None
    if engine is None:
        engine = sql_client._get_sync_engine(client.datasource.get_connection_string())
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(sa.text(stmt))


@pytest.mark.integration
async def test_file_backed_sqlite_still_shares_state_across_async_calls(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "test.db"
    client = SlayerSQLClient(
        datasource=DatasourceConfig(
            name="file_backed",
            type="sqlite",
            connection_string=f"sqlite:///{db_path}",
        ),
    )
    _setup(
        client,
        [
            "CREATE TABLE t (x INTEGER)",
            "INSERT INTO t VALUES (1), (2), (3)",
        ],
    )
    rows = await client.execute("SELECT x FROM t ORDER BY x")
    assert rows == [{"x": 1}, {"x": 2}, {"x": 3}]


@pytest.mark.integration
async def test_file_backed_sqlite_engine_cached_in_module(tmp_path: Path) -> None:
    """White-box: file-backed routes through module cache, not per-client engine."""
    db_path = tmp_path / "cached.db"
    conn_str = f"sqlite:///{db_path}"
    client = SlayerSQLClient(
        datasource=DatasourceConfig(
            name="file_cached",
            type="sqlite",
            connection_string=conn_str,
        ),
    )
    await client.execute("SELECT 1")
    assert conn_str in sql_client._sync_engines
    assert client._get_sync_engine_for_client() is None
