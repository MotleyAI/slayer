"""Cross-call semantics for in-memory SQLite via SlayerSQLClient.

Today the async path dispatches through ``asyncio.to_thread``. SQLAlchemy's
default pool for ``:memory:`` (``SingletonThreadPool``) gives each thread
its own connection, hence its own in-memory database. Schema/data set up
on the main thread is invisible to a SlayerSQLClient call that lands on a
worker thread.

Each test reproduces the bug via a deliberate asymmetry: ``_setup`` writes
on the main thread (through whichever engine the client resolves), then
``await client.execute(...)`` reads on a worker thread. Before the fix:
worker-thread reads see an empty database and the assertions fail. After
the fix (per-client engine + ``StaticPool`` + ``check_same_thread=False``)
all calls share one connection and all reads see the same data.

DDL/DML can't go through ``client.execute()``: ``_execute_sql_sync``
unconditionally calls ``result.keys()``, which raises
``ResourceClosedError`` for non-row-returning statements. ``_setup``
bypasses the client and writes directly through the engine.
"""

import asyncio
from typing import Any, Iterable

import pytest
import sqlalchemy as sa
import sqlalchemy.exc

from slayer.core.models import DatasourceConfig
from slayer.sql import client as sql_client
from slayer.sql.client import SlayerSQLClient


def _in_memory_client(name: str = "test") -> SlayerSQLClient:
    return SlayerSQLClient(
        datasource=DatasourceConfig(
            name=name,
            type="sqlite",
            connection_string="sqlite:///:memory:",
        ),
    )


def _setup(client: SlayerSQLClient, statements: Iterable[str]) -> None:
    """Run DDL/DML on whichever engine the client would use.

    Before the fix, ``_get_sync_engine_for_client`` does not exist, so the
    helper falls back to the module-level cache via ``_get_sync_engine``.
    That mirrors the production path's engine lookup and lets the test
    proceed to its real assertion (where the bug surfaces). After the fix,
    the per-client engine is used.
    """
    getter = getattr(client, "_get_sync_engine_for_client", None)
    engine = getter() if getter is not None else None
    if engine is None:
        engine = sql_client._get_sync_engine(client.datasource.get_connection_string())
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(sa.text(stmt))


@pytest.fixture
def no_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disable retries on the threaded path so negative-path errors surface fast.

    ``_execute_with_retry_threaded`` retries ``OperationalError`` up to 3
    times with 1s/2s/4s backoff. Tests that intentionally trigger a
    non-transient error (``no such table``) would otherwise wait several
    seconds before the exception escapes.
    """
    original = sql_client._execute_with_retry_threaded

    async def single(*args: Any, **kwargs: Any):
        kwargs["initial_delay"] = 0.0
        kwargs["max_delay"] = 0.0
        kwargs["max_attempts"] = 1
        return await original(*args, **kwargs)

    monkeypatch.setattr(sql_client, "_execute_with_retry_threaded", single)


async def test_async_cross_call_sees_same_in_memory_db() -> None:
    """Issue #72 acceptance: schema/data set up once, multiple async reads see it."""
    client = _in_memory_client()
    _setup(
        client,
        [
            "CREATE TABLE t (x INTEGER)",
            "INSERT INTO t VALUES (1), (2), (3)",
        ],
    )
    rows1 = await client.execute("SELECT x FROM t WHERE x = 1")
    rows2 = await client.execute("SELECT x FROM t WHERE x = 2")
    assert rows1 == [{"x": 1}]
    assert rows2 == [{"x": 2}]


async def test_get_column_types_cross_call_sees_same_in_memory_db() -> None:
    """Issue #72 acceptance: get_column_types() must see schema set up earlier."""
    client = _in_memory_client()
    _setup(
        client,
        [
            "CREATE TABLE u (a INTEGER, b TEXT)",
            "INSERT INTO u VALUES (1, 'x')",
        ],
    )
    types = await client.get_column_types("SELECT a, b FROM u")
    assert types == {"a": "number", "b": "string"}


async def test_two_clients_get_isolated_in_memory_dbs(no_retry: None) -> None:
    """Two SlayerSQLClient instances on ``:memory:`` must NOT share state."""
    client_a = _in_memory_client(name="a")
    client_b = _in_memory_client(name="b")
    _setup(
        client_a,
        [
            "CREATE TABLE only_a (x INTEGER)",
            "INSERT INTO only_a VALUES (1)",
        ],
    )

    engine_a = client_a._get_sync_engine_for_client()
    engine_b = client_b._get_sync_engine_for_client()
    assert engine_a is not None
    assert engine_b is not None
    assert engine_a is not engine_b

    rows_a = await client_a.execute("SELECT x FROM only_a")
    assert rows_a == [{"x": 1}]

    with pytest.raises(sqlalchemy.exc.OperationalError):
        await client_b.execute("SELECT x FROM only_a")


async def test_sync_and_async_on_same_client_share_in_memory_db() -> None:
    """execute_sync() and execute() on one client must see the same in-memory DB."""
    client = _in_memory_client()
    _setup(
        client,
        [
            "CREATE TABLE s (n INTEGER)",
            "INSERT INTO s VALUES (42)",
        ],
    )
    async_rows = await client.execute("SELECT n FROM s")
    sync_rows = client.execute_sync("SELECT n FROM s")
    assert async_rows == [{"n": 42}]
    assert sync_rows == [{"n": 42}]


async def test_sqlite_udfs_work_under_static_pool() -> None:
    """SQLite Python UDFs (median) must remain registered under StaticPool.

    StaticPool keeps a single connection; the engine's ``connect`` event
    fires once and registers ``median``/``percentile_cont``/``percentile_disc``
    on that one connection. The UDF must be callable through ``execute``.
    """
    client = _in_memory_client()
    _setup(
        client,
        [
            "CREATE TABLE m (x REAL)",
            "INSERT INTO m VALUES (1.0), (2.0), (3.0)",
        ],
    )
    rows = await client.execute("SELECT median(x) AS med FROM m")
    assert rows == [{"med": 2.0}]


async def test_concurrent_async_calls_share_in_memory_db(no_retry: None) -> None:
    """Realistic concurrent path: many awaits in flight at once.

    ``asyncio.gather`` forces several worker threads from the executor
    pool. Without StaticPool + ``check_same_thread=False``, each thread
    holds its own SQLite connection / its own in-memory DB, so SELECTs
    land on threads where the table was never seen. Either an
    ``OperationalError("no such table")`` or
    ``sqlite3.ProgrammingError`` (cross-thread connection use) is raised.
    """
    client = _in_memory_client()
    _setup(
        client,
        [
            "CREATE TABLE g (i INTEGER)",
            "INSERT INTO g VALUES " + ", ".join(f"({i})" for i in range(20)),
        ],
    )
    results = await asyncio.gather(
        *[client.execute(f"SELECT i FROM g WHERE i = {i}") for i in range(20)],
    )
    for i, rows in enumerate(results):
        assert rows == [{"i": i}], f"call {i} got {rows}"
