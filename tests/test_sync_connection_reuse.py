"""Synchronous calls reuse networked connections and release only local files (spec: sql/execution)."""

from __future__ import annotations

import asyncio
import json
import os
import select
import signal
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from pytest_postgresql import factories

from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.sql.client import SlayerSQLClient
from slayer.storage.yaml_storage import YAMLStorage
from tests._pg_helpers import pg_database

REPO = Path(__file__).resolve().parent.parent

postgresql_proc = factories.postgresql_proc(
    port=None,
    postgres_options="-c unix_socket_directories= -c log_disconnections=on -c log_line_prefix='%d|'",
)

PID_SQL = "SELECT pg_backend_pid() AS p"

# Seconds before a wait counts as hung.
_BOUND = 15

_POSIX_MAIN_THREAD = pytest.mark.skipif(
    not hasattr(os, "fork") or threading.current_thread() is not threading.main_thread()
    or signal.getsignal(signal.SIGINT) is not signal.default_int_handler,
    reason="needs POSIX signals on the main thread with the default SIGINT handler",
)


@pytest.fixture
def pg_ds(postgresql_proc: Any) -> Iterator[DatasourceConfig]:
    with pg_database(postgresql_proc, statements=["CREATE TABLE t (x INTEGER)"], name="reuse") as ds:
        yield ds


@pytest.fixture
def duckdb_ds(tmp_path: Path) -> Iterator[DatasourceConfig]:
    ds = DatasourceConfig(name="local", type="duckdb", database=str(tmp_path / "local.duckdb"))
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


@pytest.fixture
def query_engine(tmp_path: Path) -> Iterator[SlayerQueryEngine]:
    engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(tmp_path / "store")))
    try:
        yield engine
    finally:
        engine.close()


def _backends(info: Any, *, database: str) -> list[tuple[int, str | None, str | None]]:
    """``(pid, state, query)`` of every server backend connected to ``database``."""
    with psycopg.connect(host=info.host, port=info.port, user=info.user, dbname="postgres", autocommit=True) as admin:
        rows = admin.execute(
            "SELECT pid, state, query FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()",
            (database,),
        ).fetchall()
    return [(row[0], row[1], row[2]) for row in rows]


def _eventually(predicate: Callable[[], bool]) -> bool:
    deadline = time.monotonic() + _BOUND
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.1)
    return predicate()


def _pid(rows: list[dict[str, Any]]) -> int:
    return rows[0]["p"]


def _writable_by_another_process(path: str) -> bool:
    """Whether a fresh process can open the DuckDB file for writing."""
    script = "import duckdb, sys; c = duckdb.connect(sys.argv[1]); c.execute('CREATE TABLE IF NOT EXISTS w (v INT)'); c.close()"
    proc = subprocess.run([sys.executable, "-c", script, path], capture_output=True, text=True, timeout=60)
    return proc.returncode == 0


def _read_child(fd: int, pid: int) -> str:
    ready, _, _ = select.select([fd], [], [], _BOUND)
    if not ready:
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
        pytest.fail("forked child hung")
    out = os.read(fd, 4096).decode()
    os.waitpid(pid, 0)
    return out


# ---------------------------------------------------------------------------
# Networked connections persist
# ---------------------------------------------------------------------------


def test_no_reconnect_across_synchronous_calls(pg_ds: DatasourceConfig, postgresql_proc: Any) -> None:
    client = SlayerSQLClient(pg_ds)
    try:
        pids = {_pid(client.execute_sync(PID_SQL).rows) for _ in range(3)}
        assert len(pids) == 1
        assert [b[0] for b in _backends(postgresql_proc, database=pg_ds.database or "")] == list(pids)
    finally:
        client.close()


def test_synchronous_workflows_keep_networked_connections(
    pg_ds: DatasourceConfig, query_engine: SlayerQueryEngine,
) -> None:
    client = query_engine.client_for(pg_ds)
    first = query_engine.run_sync(client.execute(PID_SQL))
    second = query_engine.run_sync(client.execute(PID_SQL))
    assert _pid(first.rows) == _pid(second.rows)


# ---------------------------------------------------------------------------
# Local files are released; in-memory databases are kept
# ---------------------------------------------------------------------------


def test_a_local_database_file_is_released_after_a_synchronous_workflow(
    duckdb_ds: DatasourceConfig, query_engine: SlayerQueryEngine,
) -> None:
    client = query_engine.client_for(duckdb_ds)
    query_engine.run_sync(client.execute("CREATE TABLE t AS SELECT 1 AS x"))
    assert _writable_by_another_process(duckdb_ds.database or "")


def test_an_in_memory_database_survives_synchronous_workflows(query_engine: SlayerQueryEngine) -> None:
    client = query_engine.client_for(DatasourceConfig(name="mem", type="sqlite", database=":memory:"))
    query_engine.run_sync(client.execute("CREATE TABLE kept (v INTEGER)"))
    query_engine.run_sync(client.execute("INSERT INTO kept VALUES (1), (2)"))
    rows = query_engine.run_sync(client.execute("SELECT v FROM kept ORDER BY v")).rows
    assert rows == [{"v": 1}, {"v": 2}]


def test_synchronous_calls_repeat_safely(
    pg_ds: DatasourceConfig, duckdb_ds: DatasourceConfig, query_engine: SlayerQueryEngine,
) -> None:
    pg = query_engine.client_for(pg_ds)
    local = query_engine.client_for(duckdb_ds)

    async def _workflow(*, fail: bool = False) -> int:
        await local.execute("CREATE TABLE IF NOT EXISTS t (x INTEGER)")
        pid = _pid((await pg.execute(PID_SQL)).rows)
        if fail:
            raise ValueError("workflow failed")
        return pid

    async def _from_a_running_loop() -> int:
        return query_engine.run_sync(_workflow())

    pids = [query_engine.run_sync(_workflow())]
    assert _writable_by_another_process(duckdb_ds.database or "")
    pids.append(asyncio.run(_from_a_running_loop()))
    assert _writable_by_another_process(duckdb_ds.database or "")
    with pytest.raises(ValueError, match="workflow failed"):
        query_engine.run_sync(_workflow(fail=True))
    assert _writable_by_another_process(duckdb_ds.database or "")
    pids.append(query_engine.run_sync(_workflow()))
    assert len(set(pids)) == 1


# ---------------------------------------------------------------------------
# Fork
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not hasattr(os, "fork"), reason="POSIX fork")
@pytest.mark.filterwarnings("ignore:.*fork.*:DeprecationWarning")
def test_a_forked_child_uses_its_own_connections(pg_ds: DatasourceConfig) -> None:
    client = SlayerSQLClient(pg_ds)
    try:
        parent_pid = _pid(client.execute_sync(PID_SQL).rows)
        read_fd, write_fd = os.pipe()
        pid = os.fork()
        if pid == 0:  # pragma: no cover - child
            try:
                os.close(read_fd)
                os.write(write_fd, json.dumps({"pid": _pid(client.execute_sync(PID_SQL).rows)}).encode())
            except BaseException as exc:  # noqa: BLE001 — reported to the parent
                os.write(write_fd, json.dumps({"error": repr(exc)[:1000]}).encode())
            finally:
                os._exit(0)
        os.close(write_fd)
        try:
            child = json.loads(_read_child(read_fd, pid))
        finally:
            os.close(read_fd)
        assert "error" not in child, child
        assert child["pid"] != parent_pid
        assert _pid(client.execute_sync(PID_SQL).rows) == parent_pid
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Interruption
# ---------------------------------------------------------------------------


@_POSIX_MAIN_THREAD
def test_an_interrupted_synchronous_call_cleans_up(pg_ds: DatasourceConfig, postgresql_proc: Any) -> None:
    database = pg_ds.database or ""
    client = SlayerSQLClient(pg_ds)

    def _sleeping() -> bool:
        return any(state == "active" and "pg_sleep" in (query or "")
                   for _, state, query in _backends(postgresql_proc, database=database))

    def _interrupt() -> None:
        if _eventually(_sleeping):
            os.kill(os.getpid(), signal.SIGINT)

    timer = threading.Thread(target=_interrupt, daemon=True)
    try:
        timer.start()
        with pytest.raises(KeyboardInterrupt):
            client.execute_sync("SELECT pg_sleep(30)")
        timer.join(_BOUND)
        assert _eventually(lambda: not _sleeping())
        assert client.execute_sync("SELECT 1 AS x").rows == [{"x": 1}]
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Closing
# ---------------------------------------------------------------------------


def test_closing_an_engine_releases_its_synchronous_connections(
    pg_ds: DatasourceConfig, postgresql_proc: Any, tmp_path: Path,
) -> None:
    engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(tmp_path / "store")))
    client = engine.client_for(pg_ds)
    try:
        for _ in range(2):
            engine.run_sync(client.execute(PID_SQL))
    finally:
        engine.close()
    assert _eventually(lambda: not _backends(postgresql_proc, database=pg_ds.database or ""))


def test_closing_a_client_releases_its_synchronous_connections(
    pg_ds: DatasourceConfig, postgresql_proc: Any,
) -> None:
    client = SlayerSQLClient(pg_ds)
    try:
        for _ in range(2):
            client.execute_sync(PID_SQL)
    finally:
        client.close()
    assert _eventually(lambda: not _backends(postgresql_proc, database=pg_ds.database or ""))


def test_aclose_fully_disposes_the_running_loops_resources(
    pg_ds: DatasourceConfig, postgresql_proc: Any,
) -> None:
    client = SlayerSQLClient(pg_ds)

    async def _workflow() -> bool:
        await client.execute(PID_SQL)
        await client.aclose()
        return await asyncio.to_thread(
            _eventually, lambda: not _backends(postgresql_proc, database=pg_ds.database or ""),
        )

    try:
        assert asyncio.run(_workflow())
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Process exit
# ---------------------------------------------------------------------------


_EXIT_SCRIPT = textwrap.dedent("""
    import json, sys
    from slayer.core.models import DatasourceConfig
    from slayer.sql.client import SlayerSQLClient

    for raw in json.loads(sys.argv[1]):
        client = SlayerSQLClient(DatasourceConfig.model_validate(raw))
        for _ in range(2):
            client.execute_sync("SELECT 1 AS x")
    print("ok")
""")


def test_process_exit_leaves_nothing_open(
    pg_ds: DatasourceConfig, postgresql_proc: Any, tmp_path: Path,
) -> None:
    local = DatasourceConfig(name="file", type="sqlite", database=str(tmp_path / "exit.db"))
    payload = json.dumps([pg_ds.model_dump(), local.model_dump()])
    proc = subprocess.run(
        [sys.executable, "-W", "error::ResourceWarning", "-c", _EXIT_SCRIPT, payload],
        cwd=str(REPO), capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "ok"
    assert "ResourceWarning" not in proc.stderr and "unclosed" not in proc.stderr.lower(), proc.stderr
    assert _eventually(lambda: not _backends(postgresql_proc, database=pg_ds.database or ""))
    prefix = f"{pg_ds.database}|"
    log = Path(postgresql_proc.logfile).read_text(errors="replace").splitlines()
    assert any(line.startswith(prefix) and "disconnection:" in line for line in log)
    unclean = [line for line in log if line.startswith(prefix) and "unexpected EOF on client connection" in line]
    assert unclean == []
