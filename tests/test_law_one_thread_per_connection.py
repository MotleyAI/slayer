"""Law: a database connection is used by one thread at a time (database principle 6)."""

from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
import sqlite3.dbapi2 as sqlite_dbapi2
import threading
import time
from collections import Counter, OrderedDict, defaultdict
from collections.abc import AsyncGenerator, Callable, Generator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.pool import AssertionPool, NullPool, Pool, QueuePool

from slayer.core.models import DatasourceConfig
from slayer.database import engine_factory
from slayer.database.client import SlayerSQLClient
from slayer.storage.sqlite_conn import transaction

pytestmark = pytest.mark.filterwarnings("ignore::FutureWarning")

_STATEMENTS = 24
_SLOW_SECONDS = 0.005


class _Uses:
    """Every use window of every tracked ``sqlite3`` connection, and how often two threads overlapped in one."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._inside: defaultdict[int, Counter[int]] = defaultdict(Counter)
        self.overlaps = 0

    @contextlib.contextmanager
    def window(self, connection: sqlite3.Connection) -> Generator[None]:
        me = threading.get_ident()
        with self._lock:
            inside = self._inside[id(connection)]
            if any(n for thread, n in inside.items() if thread != me):
                self.overlaps += 1
            inside[me] += 1
        try:
            yield
        finally:
            with self._lock:
                self._inside[id(connection)][me] -= 1


_USES = _Uses()


class _TrackedCursor(sqlite3.Cursor):
    def execute(self, *args: Any, **kwargs: Any) -> Any:
        with _USES.window(self.connection):
            time.sleep(_SLOW_SECONDS)  # a slow statement: concurrent uses would overlap
            return super().execute(*args, **kwargs)

    def fetchall(self) -> list[Any]:
        with _USES.window(self.connection):
            return super().fetchall()

    def fetchone(self) -> Any:
        with _USES.window(self.connection):
            return super().fetchone()

    def fetchmany(self, *args: Any, **kwargs: Any) -> list[Any]:
        with _USES.window(self.connection):
            return super().fetchmany(*args, **kwargs)


class _TrackedConnection(sqlite3.Connection):
    def cursor(self, *_args: Any, **_kwargs: Any) -> Any:
        return super().cursor(_TrackedCursor)

    def commit(self) -> None:
        with _USES.window(self):
            super().commit()

    def rollback(self) -> None:
        with _USES.window(self):
            super().rollback()


@pytest.fixture
def uses(monkeypatch: pytest.MonkeyPatch) -> _Uses:
    """Every ``sqlite3`` connection SQLAlchemy opens from here on is tracked."""
    global _USES
    _USES = _Uses()
    real = sqlite_dbapi2.connect

    def _connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        return real(*args, factory=_TrackedConnection, **kwargs)

    monkeypatch.setattr(sqlite_dbapi2, "connect", _connect)
    return _USES


@contextlib.asynccontextmanager
async def _client(ds: DatasourceConfig) -> AsyncGenerator[SlayerSQLClient]:
    client = SlayerSQLClient(datasource=ds)
    try:
        yield client
    finally:
        await client.aclose()
        client.close()
        engine_factory.invalidate_engine(ds)


async def _concurrent_rows(client: SlayerSQLClient) -> list[list[dict[str, Any]]]:
    results = await asyncio.gather(*(client.execute(f"SELECT i FROM g WHERE i = {i}") for i in range(_STATEMENTS)))
    return [r.rows for r in results]


_ROWS = [(i,) for i in range(_STATEMENTS)]


class TestConcurrentSqliteCalls:
    async def test_in_memory(self, uses: _Uses) -> None:
        async with _client(DatasourceConfig(name="mem", type="sqlite", connection_string="sqlite:///:memory:")) as client:
            with client._get_sync_engine_for_client().begin() as conn:
                conn.exec_driver_sql("CREATE TABLE g (i INTEGER)")
                conn.exec_driver_sql("INSERT INTO g VALUES " + ", ".join(f"({i})" for i in range(_STATEMENTS)))
            rows = await _concurrent_rows(client)
        assert uses.overlaps == 0
        assert rows == [[{"i": i}] for i in range(_STATEMENTS)]

    async def test_file_backed(self, uses: _Uses, tmp_path: Path) -> None:
        db = tmp_path / "file.db"
        with transaction(str(db)) as conn:
            conn.execute("CREATE TABLE g (i INTEGER)")
            conn.executemany("INSERT INTO g VALUES (?)", _ROWS)
        async with _client(DatasourceConfig(name="file", type="sqlite", database=str(db))) as client:
            rows = await _concurrent_rows(client)
        assert uses.overlaps == 0
        assert rows == [[{"i": i}] for i in range(_STATEMENTS)]


def _in_worker(fn: Callable[[], Any]) -> Any:
    """``fn`` on a worker thread, so a waiting pool cannot hang the suite."""
    box: dict[str, Any] = {}

    def run() -> None:
        try:
            box["value"] = fn()
        except BaseException as exc:  # noqa: BLE001 — re-raised on the caller thread
            box["error"] = exc

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive(), "the nested checkout waited instead of failing"
    if "error" in box:
        raise box["error"]
    return box["value"]


def test_a_nested_in_memory_checkout_fails_at_once() -> None:
    engine = engine_factory.build_in_memory_sqlite_engine(":memory:")

    def nested() -> Exception | None:
        with engine.connect():
            try:
                with engine.connect():
                    return None
            except Exception as exc:  # noqa: BLE001 — the refusal is the result
                return exc

    try:
        refusal = _in_worker(nested)
    finally:
        engine.dispose()
    assert refusal is not None, "a nested checkout shared the held connection"
    assert not isinstance(refusal, PoolTimeoutError)
    assert "nest" in str(refusal).lower(), refusal


# ---------------------------------------------------------------------------
# Every engine SLayer builds hands each connection to one caller at a time
# ---------------------------------------------------------------------------

# One datasource type per Tier 1 database (docs/database-support.md).
TIER1_TYPES = ("sqlite", "postgres", "duckdb", "mysql", "clickhouse", "sqlserver", "trino", "snowflake", "bigquery")


def _exclusive(pool: Pool) -> bool:
    """Whether ``pool`` hands each connection to one caller at a time."""
    return isinstance(pool, QueuePool | NullPool | AssertionPool)


def _config(ds_type: str, directory: Path) -> DatasourceConfig:
    if ds_type in ("sqlite", "duckdb"):
        return DatasourceConfig(name="d", type=ds_type, database=str(directory / f"law.{ds_type}"))
    if ds_type == "bigquery":
        grant = {"type": "authorized_user", "refresh_token": "r", "client_id": "c", "client_secret": "s"}  # NOSONAR(S2068) — placeholder grant
        return DatasourceConfig(
            name="d", type=ds_type, connection_string="bigquery://proj/dset", oauth_credentials_json=json.dumps(grant),
        )
    return DatasourceConfig(
        name="d", type=ds_type, host="pool.invalid", port=1234, database="d", username="u", password="p",
        warehouse="wh",
    )


@pytest.fixture
def isolated_engines(monkeypatch: pytest.MonkeyPatch) -> Generator[None]:
    monkeypatch.setattr(engine_factory, "_engine_cache", OrderedDict())
    try:
        yield
    finally:
        engine_factory.reset_cache()


@pytest.mark.usefixtures("isolated_engines")
@pytest.mark.parametrize("ds_type", TIER1_TYPES)
async def test_every_engine_hands_each_connection_to_one_caller(ds_type: str, tmp_path: Path) -> None:
    configs = [_config(ds_type, tmp_path)]
    if ds_type == "sqlite":
        configs.append(DatasourceConfig(name="mem", type="sqlite", connection_string="sqlite:///:memory:"))
    for ds in configs:
        client = SlayerSQLClient(datasource=ds)
        try:
            pools = [client._get_sync_engine_for_client().pool]
            if client._async_url() is not None:
                pools.append(client._get_async_engine().sync_engine.pool)
        finally:
            await client.aclose()
            client.close()
        assert [p for p in pools if not _exclusive(p)] == [], (ds.get_connection_string(), pools)

