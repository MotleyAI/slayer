"""python.database.arc42.md §3.5: one request per statement on a warm connection (spec: database/execution)."""

from __future__ import annotations

import ast
import asyncio
import contextlib
import time
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import pytest
from pytest_postgresql import factories

from slayer.core.models import DatasourceConfig
from slayer.database import engine_factory
from slayer.database.client import SlayerSQLClient
from tests._pg_helpers import pg_database
from tests._pg_statement_log import LOGGED_POSTGRES_OPTIONS, LoggedEntry, ServerLog, between

pytestmark = pytest.mark.filterwarnings("ignore::FutureWarning")

postgresql_logged = factories.postgresql_proc(port=None, postgres_options=LOGGED_POSTGRES_OPTIONS)

REPO = Path(__file__).resolve().parent.parent

_SCHEMES = {"psycopg": "postgresql+psycopg", "asyncpg": "postgresql+asyncpg", "psycopg2": "postgresql+psycopg2"}

_TIMEOUT_SQL = "SELECT current_setting('statement_timeout') AS t"

_PINGS = {";", "SELECT 1"}


def _mark(n: int) -> str:
    return f"SELECT 'mark-{n}' AS m"


@contextlib.contextmanager
def _logged(info: Any, *, driver: str) -> Generator[tuple[DatasourceConfig, ServerLog]]:
    with pg_database(info, statements=["CREATE TABLE t (x INTEGER)"], name=f"law_{driver}") as plain:
        ds = DatasourceConfig(
            name=plain.name, type="postgres",
            connection_string=f"{_SCHEMES[driver]}://{plain.username}@{plain.host}:{plain.port}/{plain.database}",
        )
        assert plain.database is not None
        try:
            yield ds, ServerLog(logfile=info.logfile, database=plain.database)
        finally:
            engine_factory.invalidate_engine(ds)


def _assert_one_request_each(statements: list[LoggedEntry]) -> None:
    window = [e.sql for e in between(statements, after="'mark-1'", through="'mark-3'")]
    assert len(window) == 2, window
    assert "'mark-2'" in window[0], window
    assert "'mark-3'" in window[1], window


async def _three_async(client: SlayerSQLClient) -> None:
    try:
        for n in (1, 2, 3):
            await client.execute(_mark(n))
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# A warm statement costs one request
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("driver", ["psycopg", "asyncpg"])
def test_async_path_in_one_loop(postgresql_logged: Any, driver: str) -> None:
    with _logged(postgresql_logged, driver=driver) as (ds, log):
        client = SlayerSQLClient(ds)
        try:
            asyncio.run(_three_async(client))
        finally:
            client.close()
        _assert_one_request_each(log.statements(until="'mark-3'"))


@pytest.mark.parametrize("driver", ["psycopg", "psycopg2"])
def test_sync_path(postgresql_logged: Any, driver: str) -> None:
    """psycopg is bridged onto its async driver; psycopg2 runs on the sync engine."""
    with _logged(postgresql_logged, driver=driver) as (ds, log):
        client = SlayerSQLClient(ds)
        try:
            for n in (1, 2, 3):
                client.execute_sync(_mark(n))
        finally:
            client.close()
        _assert_one_request_each(log.statements(until="'mark-3'"))


@pytest.mark.parametrize("driver", ["psycopg", "psycopg2"])
def test_one_connection_across_sync_calls(postgresql_logged: Any, driver: str) -> None:
    with _logged(postgresql_logged, driver=driver) as (ds, log):
        client = SlayerSQLClient(ds)
        try:
            for n in (1, 2, 3):
                client.execute_sync(_mark(n))
        finally:
            client.close()
        assert len(log.connections(until="'mark-3'")) == 1


# ---------------------------------------------------------------------------
# The timeout is set once per connection, re-set only when it changes
# ---------------------------------------------------------------------------


async def _probe_between_queries(client: SlayerSQLClient) -> list[Any]:
    try:
        seen = [(await client.execute(_mark(1))).rows]
        seen.append((await client.execute(_TIMEOUT_SQL)).rows)
        await client.get_result_column_types("SELECT 1 AS n")
        seen.append((await client.execute(_TIMEOUT_SQL)).rows)
        seen.append((await client.execute(_mark(3))).rows)
        return seen
    finally:
        await client.aclose()


@pytest.mark.parametrize("driver", ["psycopg", "psycopg2"])
def test_a_probe_between_queries_costs_two_timeout_statements(postgresql_logged: Any, driver: str) -> None:
    with _logged(postgresql_logged, driver=driver) as (ds, log):
        client = SlayerSQLClient(ds)
        try:
            seen = asyncio.run(_probe_between_queries(client))
        finally:
            client.close()
        window = between(log.statements(until="'mark-3'"), after="'mark-1'", through="'mark-3'")
    timeouts = [e.sql for e in window if "statement_timeout" in e.sql and "current_setting" not in e.sql]
    assert timeouts == ["SET statement_timeout = 60000", "SET statement_timeout = 120000"], timeouts
    assert seen[1] == seen[2] == [{"t": "2min"}]


# ---------------------------------------------------------------------------
# Liveness is checked only after idle
# ---------------------------------------------------------------------------


def _advance_clock(monkeypatch: pytest.MonkeyPatch, *, seconds: float) -> None:
    """Make the connection policy's monotonic clock read ``seconds`` later."""
    real = time.monotonic
    monkeypatch.setattr("slayer.sql.connection_policy.monotonic", lambda: real() + seconds)


def _run_marks(client: SlayerSQLClient, *, between_marks: Callable[[], None]) -> None:
    async def _go() -> None:
        try:
            await client.execute(_mark(1))
            between_marks()
            await client.execute(_mark(2))
        finally:
            await client.aclose()

    asyncio.run(_go())


@pytest.mark.parametrize("driver", ["psycopg", "psycopg2"])
def test_idle_connection_costs_one_ping(postgresql_logged: Any, driver: str, monkeypatch: pytest.MonkeyPatch) -> None:
    with _logged(postgresql_logged, driver=driver) as (ds, log):
        client = SlayerSQLClient(ds)
        try:
            _run_marks(client, between_marks=lambda: _advance_clock(monkeypatch, seconds=61))
        finally:
            client.close()
        window = between(log.statements(until="'mark-2'"), after="'mark-1'", through="'mark-2'")
    sent = [e.sql for e in window]
    assert len(sent) == 2, sent
    assert sent[0] in _PINGS, sent
    assert "'mark-2'" in sent[1], sent


@pytest.mark.parametrize("driver", ["psycopg", "psycopg2"])
def test_recently_used_connection_is_not_pinged(
    postgresql_logged: Any, driver: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _logged(postgresql_logged, driver=driver) as (ds, log):
        client = SlayerSQLClient(ds)
        try:
            _run_marks(client, between_marks=lambda: _advance_clock(monkeypatch, seconds=59))
        finally:
            client.close()
        window = between(log.statements(until="'mark-2'"), after="'mark-1'", through="'mark-2'")
    assert [e.sql for e in window if e.sql in _PINGS] == []


@pytest.mark.parametrize("driver", ["psycopg", "psycopg2"])
def test_fresh_connection_is_not_pinged(postgresql_logged: Any, driver: str, monkeypatch: pytest.MonkeyPatch) -> None:
    with _logged(postgresql_logged, driver=driver) as (ds, log):
        _advance_clock(monkeypatch, seconds=3600)
        client = SlayerSQLClient(ds)
        try:
            client.execute_sync(_mark(1))
        finally:
            client.close()
        statements = log.statements(until="'mark-1'")
    assert [e.sql for e in statements if e.sql in _PINGS] == []


# ---------------------------------------------------------------------------
# Static: no pre-ping on any SLayer-built engine
# ---------------------------------------------------------------------------


def _pre_ping_values(tree: ast.AST) -> list[ast.expr]:
    """Values given to ``pool_pre_ping`` as a keyword, a dict entry or a subscript assignment."""
    values: list[ast.expr] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.keyword) and node.arg == "pool_pre_ping":
            values.append(node.value)
        elif isinstance(node, ast.Dict):
            values.extend(v for k, v in zip(node.keys, node.values) if isinstance(k, ast.Constant) and k.value == "pool_pre_ping")
        elif isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Subscript) and isinstance(t.slice, ast.Constant) and t.slice.value == "pool_pre_ping"
            for t in node.targets
        ):
            values.append(node.value)
    return [v for v in values if not (isinstance(v, ast.Constant) and v.value is False)]


def _pool_pre_ping_sites() -> list[str]:
    return [
        f"{value.lineno}({path.relative_to(REPO).as_posix()})"
        for path in sorted((REPO / "slayer").rglob("*.py"))
        for value in _pre_ping_values(ast.parse(path.read_text(encoding="utf-8")))
    ]


@pytest.mark.parametrize(("source", "found"), [
    ("create_engine(url, pool_pre_ping=True)", 1),
    ("kwargs = {'pool_pre_ping': True}", 1),
    ("kwargs['pool_pre_ping'] = flag", 1),
    ("kwargs.update(pool_pre_ping=True)", 1),
    ("dict(pool_pre_ping=True)", 1),
    ("create_engine(url, pool_pre_ping=False)", 0),
])
def test_the_pre_ping_scan_sees_every_spelling(source: str, found: int) -> None:
    assert len(_pre_ping_values(ast.parse(source))) == found


def test_no_engine_pings_on_every_checkout() -> None:
    assert _pool_pre_ping_sites() == []
