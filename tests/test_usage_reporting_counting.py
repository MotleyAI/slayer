"""Usage counters per surface: CLI, MCP, REST, Flight SQL, PostgreSQL wire."""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import gc
import json
import os
import socket
import threading
import weakref
from collections.abc import Iterator
from types import SimpleNamespace

import pyarrow.flight as fl
import pytest
import uvicorn
from fastapi.testclient import TestClient
from mcp import ClientSession
from mcp.client.sse import sse_client
from mcp.server.session import ServerSession
from mcp.shared.exceptions import McpError
from mcp.shared.memory import create_connected_server_and_client_session
from mcp.types import CancelledNotification, CancelledNotificationParams, ClientNotification, Implementation
from sqlalchemy import exc as sa_exc

from slayer import usage_reporting
from slayer.api.server import create_app
from slayer.live_schema import ingestion as engine_ingestion
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.database.client import DatabaseUnavailable
from slayer.storage.yaml_storage import YAMLStorage
from slayer.usage_reporting import settings
from slayer.usage_reporting.payload import USAGE_KEYS
from tests._usage_reporting_helpers import (
    build_storage,
    cli,
    end_process,
    quiet_config,
    run_python,
    session_rows,
    show,
    total,
    usage,
    usage_key_vocabulary,
    usage_keys,
    wait_until,
)
from tests.flight.test_handlers import _make_handlers
from tests.pg_facade.test_connection import (
    _bind,
    _describe,
    _execute,
    _parse,
    _query,
    _run,
    _startup,
    _sync,
    _terminate,
)

_QUERY = {"source_model": "orders", "measures": ["sum(amount)"]}
_PG_QUERY = "SELECT revenue_sum FROM orders"
_FLIGHT_QUERY = "SELECT revenue_sum FROM jaffle.orders"


@pytest.fixture
def store(usage_reporting_env) -> str:
    quiet_config()
    return build_storage(usage_reporting_env.root)


@pytest.fixture
def started(usage_reporting_env) -> None:
    quiet_config()
    usage_reporting.start()


# --- CLI ---------------------------------------------------------------------------------------


def test_cli_leaf_tokens(store: str) -> None:
    cli(["query", json.dumps(_QUERY), "--storage", store])
    cli(["models", "--storage", store, "list"])
    cli(["datasources", "--storage", store, "list"])
    cli(["datasources", "--storage", store, "list"])
    report = show()
    assert usage(report, "cli:query") == {"ok": 1, "errors": {}}
    assert usage(report, "cli:models.list") == {"ok": 1, "errors": {}}
    assert usage(report, "cli:datasources.list") == {"ok": 2, "errors": {}}


@pytest.mark.parametrize("args", [
    ["query"],
    ["--help"],
    ["query", "--help"],
    ["models", "--bogus"],
    ["inspect", "x", "--type", "not-a-kind"],
    [],
])
def test_parse_errors_and_help_not_counted(store: str, args: list[str]) -> None:
    cli(args)
    assert usage_keys(show(), surface="cli") <= {"cli:usage-reporting.show"}


def test_usage_reporting_commands_not_counted(store: str) -> None:
    assert not {key for key in USAGE_KEYS if key.startswith("cli:usage-reporting")}
    cli(["usage-reporting", "status"])
    cli(["usage-reporting", "show"])
    assert usage_keys(show(), surface="cli") == set()


def test_parse_error_exits_before_any_usage_reporting(usage_reporting_env) -> None:
    cli(["query"])
    assert not settings.config_dir().exists()


def test_cli_failure_counted_as_error(store: str) -> None:
    cli(["query", json.dumps({"source_model": "no_such_model", "measures": ["count(*)"]}), "--storage", store])
    cli(["models", "--storage", store, "show", "no_such_model"])
    report = show()
    for key in ("cli:query", "cli:models.show"):
        entry = usage(report, key)
        assert entry["ok"] == 0
        assert sum(entry["errors"].values()) == 1


# --- MCP ------------------------------------------------------------------------------------------


class _UnlistedError(Exception):
    pass


async def test_mcp_tool_calls_counted_once_each(store: str, started) -> None:
    server = create_mcp_server(storage=YAMLStorage(base_dir=store), _seed_help=False)
    async with create_connected_server_and_client_session(server) as session:
        await session.call_tool("list_datasources", {})
        await session.call_tool("list_datasources", {})
        await session.call_tool("inspect_model", {"model_name": "no_such_model"})
    end_process()
    report = show()
    assert usage(report, "mcp:list_datasources") == {"ok": 2, "errors": {}}
    assert total(usage(report, "mcp:inspect_model")) == 1


async def test_mcp_unlisted_exception_counted_as_other(
    store: str, started, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def explode(self, *args, **kwargs):
        raise _UnlistedError("boom")

    monkeypatch.setattr(SlayerQueryEngine, "execute", explode)
    server = create_mcp_server(storage=YAMLStorage(base_dir=store), _seed_help=False)
    async with create_connected_server_and_client_session(server) as session:
        await session.call_tool("query", {"query": _QUERY})
    end_process()
    assert usage(show(), "mcp:query") == {"ok": 0, "errors": {"other": 1}}


async def test_mcp_cancelled_tool_call_counted(
    store: str, started, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def cancelled(self, *args, **kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(SlayerQueryEngine, "execute", cancelled)
    server = create_mcp_server(storage=YAMLStorage(base_dir=store), _seed_help=False)
    with pytest.raises(asyncio.CancelledError):
        await server.call_tool("query", {"query": _QUERY})
    end_process()
    assert usage(show(), "mcp:query") == {"ok": 0, "errors": {"cancelled": 1}}


@pytest.mark.parametrize(("name", "version", "client", "major"), [
    ("claude-code", "2.3.1", "claude-code", 2),
    ("acme-finance-bot", "2.3.1", "other", None),
])
async def test_mcp_client_counted_per_session(
    store: str, started, name: str, version: str, client: str, major: int | None,
) -> None:
    server = create_mcp_server(storage=YAMLStorage(base_dir=store), _seed_help=False)
    info = Implementation(name=name, version=version)
    async with create_connected_server_and_client_session(server, client_info=info) as session:
        await session.call_tool("list_datasources", {})
        await session.call_tool("list_datasources", {})
    end_process()
    clients = show()["mcp_clients"]
    assert clients == [{"client": client, "major": major, "sessions": 1}]


async def test_mcp_client_unparseable_version(store: str, started) -> None:
    server = create_mcp_server(storage=YAMLStorage(base_dir=store), _seed_help=False)
    info = Implementation(name="claude-code", version="nightly")
    async with create_connected_server_and_client_session(server, client_info=info) as session:
        await session.call_tool("list_datasources", {})
    end_process()
    assert show()["mcp_clients"] == [{"client": "claude-code", "major": None, "sessions": 1}]


def _tool_schemas(store: str) -> str:
    server = create_mcp_server(storage=YAMLStorage(base_dir=store), _seed_help=False)
    tools = asyncio.run(server.list_tools())
    return json.dumps([t.model_dump(mode="json") for t in tools])


def test_mcp_tool_schemas_identical_with_usage_reporting_on_and_off(store: str) -> None:
    off = _tool_schemas(store)
    usage_reporting.start()
    on = _tool_schemas(store)
    end_process()
    assert on == off


# --- REST -------------------------------------------------------------------------------------------


def test_rest_counts_route_templates(store: str, started) -> None:
    client = TestClient(create_app(storage=YAMLStorage(base_dir=store)))
    assert client.get("/health").status_code == 200
    client.get("/models/orders")
    client.get("/models/no_such_model")
    assert client.post("/query", json=_QUERY).status_code == 200
    end_process()
    report = show()
    assert usage(report, "rest:/health") == {"ok": 1, "errors": {}}
    assert total(usage(report, "rest:/models/{name}")) == 2
    assert usage(report, "rest:/query")["ok"] == 1


def test_rest_unmatched_maps_to_a_fixed_token(store: str, started) -> None:
    client = TestClient(create_app(storage=YAMLStorage(base_dir=store)))
    client.get("/health")
    end_process()
    before = usage_keys(show(), surface="rest")
    usage_reporting.start()
    client.get("/no/such/path/42")
    client.get("/another/unknown")
    end_process()
    report = show()
    new = usage_keys(report, surface="rest") - before
    assert len(new) == 1
    (key,) = new
    assert key in usage_key_vocabulary()
    assert "path" not in key
    assert "unknown" not in key
    assert total(usage(report, key)) == 2


def test_rest_mount_requests_not_counted_as_rest(store: str, started) -> None:
    client = TestClient(create_app(storage=YAMLStorage(base_dir=store)))
    client.post("/mcp/messages/?session_id=00000000000000000000000000000000", json={})
    end_process()
    assert usage_keys(show(), surface="rest") == set()


@contextlib.contextmanager
def _serve(app) -> Iterator[str]:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    assert wait_until(lambda: server.started, timeout=30)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=10)


async def test_mounted_mcp_tool_call_counted_as_mcp_not_rest(store: str, started) -> None:
    app = create_app(storage=YAMLStorage(base_dir=store))
    with _serve(app) as url:
        async with sse_client(f"{url}/mcp/sse") as (read, write):
            async with ClientSession(read, write, client_info=Implementation(name="cursor", version="1.4.0")) as session:
                await session.initialize()
                await session.call_tool("list_datasources", {})
    end_process()
    report = show()
    assert usage(report, "mcp:list_datasources")["ok"] == 1
    assert usage_keys(report, surface="rest") == set()


# --- Flight SQL -------------------------------------------------------------------------------------


def test_flight_validate_then_fetch_counted_once(started) -> None:
    handlers = _make_handlers()
    descriptor = fl.FlightDescriptor.for_command(b"")  # pyright: ignore[reportPrivateImportUsage] — pyarrow re-export
    handlers.get_flight_info_for_sql(descriptor, _FLIGHT_QUERY)
    handlers.do_get_for_sql(_FLIGHT_QUERY)
    end_process()
    assert usage(show(), "flight:query") == {"ok": 1, "errors": {}}


def test_flight_prepared_statement_counted_per_fetch(started) -> None:
    handlers = _make_handlers()
    handlers.handle_create_prepared_statement(SimpleNamespace(query=_FLIGHT_QUERY))
    handlers.do_get_for_sql(_FLIGHT_QUERY)
    handlers.do_get_for_sql(_FLIGHT_QUERY)
    end_process()
    assert usage(show(), "flight:query")["ok"] == 2


def test_flight_probe_not_counted_as_query(started) -> None:
    _make_handlers().do_get_for_sql("SELECT 1")
    end_process()
    assert total(usage(show(), "flight:query")) == 0


# --- PostgreSQL wire --------------------------------------------------------------------------------


async def test_pg_multi_statement_counted_per_statement(started) -> None:
    await _run(_startup(user="u", database="jaffle") + _query(f"{_PG_QUERY}; {_PG_QUERY}") + _terminate())
    end_process()
    assert usage(show(), "pg:query") == {"ok": 2, "errors": {}}


async def test_pg_prepared_statement_counted_per_execution(started) -> None:
    executions = (_bind("", "s1") + _execute("") + _sync()) * 2
    await _run(
        _startup(user="u", database="jaffle") + _parse("s1", _PG_QUERY) + _sync() + executions + _terminate(),
    )
    end_process()
    assert usage(show(), "pg:query")["ok"] == 2


async def test_pg_describe_alone_not_counted(started) -> None:
    await _run(
        _startup(user="u", database="jaffle") + _parse("s1", _PG_QUERY) + _describe("S", "s1") + _sync() + _terminate(),
    )
    end_process()
    assert total(usage(show(), "pg:query")) == 0


async def test_pg_probe_has_its_own_token(started) -> None:
    await _run(_startup(user="u", database="jaffle") + _query("SELECT 1") + _terminate())
    end_process()
    report = show()
    assert total(usage(report, "pg:query")) == 0
    others = usage_keys(report, surface="pg") - {"pg:query"}
    assert sum(total(usage(report, key)) for key in others) == 1


class _FailingEngine:
    async def execute(self, *, query=None, data_source=None):  # NOSONAR(S7503) — awaited interface
        raise _UnlistedError("engine down")


async def test_pg_engine_error_counted(started) -> None:
    await _run(_startup(user="u", database="jaffle") + _query(_PG_QUERY) + _terminate(), engine=_FailingEngine())
    end_process()
    assert usage(show(), "pg:query") == {"ok": 0, "errors": {"other": 1}}


# --- MCP sessions -----------------------------------------------------------------------------------


_FAILING_QUERY = {"source_model": "no_such_model", "measures": ["count(*)"]}
_T0 = datetime.datetime(2026, 5, 4, 12, 0, tzinfo=datetime.timezone.utc)


async def test_session_counts_ok_and_failed_queries(store: str, started) -> None:
    server = create_mcp_server(storage=YAMLStorage(base_dir=store), _seed_help=False)
    async with create_connected_server_and_client_session(server) as session:
        await session.call_tool("query", {"query": _QUERY})
        await session.call_tool("query", {"query": _QUERY})
        assert (await session.call_tool("query", {"query": _FAILING_QUERY})).isError
    end_process()
    assert session_rows(show()) == [("2-5", "1", 1)]


async def test_session_without_queries(store: str, started) -> None:
    server = create_mcp_server(storage=YAMLStorage(base_dir=store), _seed_help=False)
    async with create_connected_server_and_client_session(server) as session:
        await session.call_tool("list_datasources", {})
    end_process()
    assert session_rows(show()) == [("0", "0", 1)]


async def test_dry_run_not_counted_for_the_session(store: str, started) -> None:
    server = create_mcp_server(storage=YAMLStorage(base_dir=store), _seed_help=False)
    async with create_connected_server_and_client_session(server) as session:
        assert not (await session.call_tool("query", {"query": _QUERY, "dry_run": True})).isError
    end_process()
    report = show()
    assert session_rows(report) == [("0", "0", 1)]
    assert usage(report, "mcp:query")["ok"] == 1


async def test_cancelled_query_counted_as_failed(
    store: str, started, monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = asyncio.Event()

    async def hang(self, *args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(SlayerQueryEngine, "execute", hang)
    server = create_mcp_server(storage=YAMLStorage(base_dir=store), _seed_help=False)
    async with create_connected_server_and_client_session(server) as session:
        call = asyncio.create_task(session.call_tool("query", {"query": _QUERY}))
        await asyncio.wait_for(entered.wait(), timeout=10)
        await session.send_notification(ClientNotification(CancelledNotification(
            params=CancelledNotificationParams(requestId=session._request_id - 1),
        )))
        with pytest.raises(McpError):
            await asyncio.wait_for(call, timeout=10)
        await session.call_tool("list_datasources", {})
    end_process()
    report = show()
    assert usage(report, "mcp:query") == {"ok": 0, "errors": {"cancelled": 1}}
    assert session_rows(report) == [("0", "1", 1)]


async def test_concurrent_calls_in_one_session_counted_once_each(store: str, started) -> None:
    server = create_mcp_server(storage=YAMLStorage(base_dir=store), _seed_help=False)
    async with create_connected_server_and_client_session(server) as session:
        await asyncio.gather(*(session.call_tool("query", {"query": _QUERY}) for _ in range(7)))
    end_process()
    assert session_rows(show()) == [("6-20", "0", 1)]


async def _sse_session(url: str, *, queries: list[dict], concurrent: bool = False) -> None:
    async with sse_client(f"{url}/mcp/sse") as (read, write):
        async with ClientSession(read, write, client_info=Implementation(name="cursor", version="1.4.0")) as session:
            await session.initialize()
            await session.call_tool("list_datasources", {})
            calls = [session.call_tool("query", {"query": query}) for query in queries]
            if concurrent:
                await asyncio.gather(*calls)
            else:
                for call in calls:
                    await call


async def test_equal_sessions_share_a_row(store: str, started) -> None:
    with _serve(create_app(storage=YAMLStorage(base_dir=store))) as url:
        await _sse_session(url, queries=[_QUERY])
        await _sse_session(url, queries=[_QUERY])
    end_process()
    assert session_rows(show()) == [("1", "0", 2)]


async def test_differing_sessions_get_their_own_rows(store: str, started) -> None:
    with _serve(create_app(storage=YAMLStorage(base_dir=store))) as url:
        await _sse_session(url, queries=[_QUERY])
        await _sse_session(url, queries=[_QUERY, _QUERY, _FAILING_QUERY])
    end_process()
    assert session_rows(show()) == [("1", "0", 1), ("2-5", "1", 1)]


async def test_concurrent_sessions_on_the_mounted_endpoint(store: str, started) -> None:
    with _serve(create_app(storage=YAMLStorage(base_dir=store))) as url:
        await asyncio.gather(
            _sse_session(url, queries=[_QUERY] * 3, concurrent=True),
            _sse_session(url, queries=[_QUERY] * 3, concurrent=True),
            _sse_session(url, queries=[_FAILING_QUERY], concurrent=True),
        )
    end_process()
    report = show()
    assert session_rows(report) == [("0", "1", 1), ("2-5", "0", 2)]
    assert report["mcp_clients"] == [{"client": "cursor", "major": 1, "sessions": 3}]


def _all_collected(refs: list[weakref.ref]) -> bool:
    gc.collect()
    return all(ref() is None for ref in refs)


async def test_ended_sessions_release_live_state_and_keep_totals(
    store: str, started, monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[weakref.ref] = []
    real_init = ServerSession.__init__

    def tracked_init(self, *args, **kwargs) -> None:
        real_init(self, *args, **kwargs)
        created.append(weakref.ref(self))

    monkeypatch.setattr(ServerSession, "__init__", tracked_init)
    with _serve(create_app(storage=YAMLStorage(base_dir=store))) as url:
        for _ in range(12):
            await _sse_session(url, queries=[_QUERY])
        assert len(created) == 12
        assert wait_until(lambda: _all_collected(created))
    end_process()
    assert session_rows(show()) == [("1", "0", 12)]


async def test_send_mid_session_carries_no_session_row(store: str, usage_reporting_env) -> None:
    quiet_config(last_sent=_T0)
    usage_reporting_env.set_now(_T0 + datetime.timedelta(hours=1))
    usage_reporting.start()
    server = create_mcp_server(storage=YAMLStorage(base_dir=store), _seed_help=False)
    async with create_connected_server_and_client_session(server) as session:
        await session.call_tool("query", {"query": _QUERY})
        usage_reporting_env.set_now(_T0 + datetime.timedelta(hours=25))
        await session.call_tool("list_datasources", {})
        (request,) = usage_reporting_env.capture.wait_for(count=1)
        await session.call_tool("query", {"query": _QUERY})
    end_process()
    sent = request.json()["properties"]
    assert sent["usage"]["mcp:query"]["ok"] == 1
    assert sent["mcp_sessions"] == []
    assert session_rows(show()) == [("2-5", "0", 1)]


async def test_send_on_the_last_call_keeps_the_session_row(store: str, usage_reporting_env) -> None:
    quiet_config(last_sent=_T0)
    usage_reporting_env.set_now(_T0 + datetime.timedelta(hours=1))
    usage_reporting.start()
    server = create_mcp_server(storage=YAMLStorage(base_dir=store), _seed_help=False)
    async with create_connected_server_and_client_session(server) as session:
        usage_reporting_env.set_now(_T0 + datetime.timedelta(hours=25))
        await session.call_tool("query", {"query": _QUERY})
        (request,) = usage_reporting_env.capture.wait_for(count=1)
    end_process()
    assert request.json()["properties"]["mcp_sessions"] == []
    assert session_rows(show()) == [("1", "0", 1)]


_FORK_SCRIPT = """
import asyncio, gc, os
from mcp.shared.memory import create_connected_server_and_client_session
from slayer import usage_reporting
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage

def fork_and_flush():
    pid = os.fork()
    if pid == 0:
        usage_reporting.flush()
        os._exit(0)
    os.waitpid(pid, 0)

async def main():
    usage_reporting.start()
    server = create_mcp_server(storage=YAMLStorage(base_dir={store!r}), _seed_help=False)
    async with create_connected_server_and_client_session(server) as session:
        await session.call_tool("query", {{"query": {query!r}}})
        fork_and_flush()
    async with create_connected_server_and_client_session(server) as session:
        await session.call_tool("list_datasources", {{}})
    gc.collect()
    fork_and_flush()

asyncio.run(main())
"""


@pytest.mark.skipif(not hasattr(os, "fork"), reason="os.fork")
def test_forked_child_holds_no_sessions(store: str, usage_reporting_env) -> None:
    result = run_python(_FORK_SCRIPT.format(store=store, query=_QUERY), env=usage_reporting_env.subprocess_env())
    assert result.returncode == 0, result.stderr
    assert session_rows(show()) == [("0", "0", 1), ("1", "0", 1)]


# --- MCP database failures --------------------------------------------------------------------------


def _operational() -> Exception:
    return sa_exc.OperationalError("SELECT 1", {}, Exception("database is locked"))


def _unavailable() -> Exception:
    try:
        raise DatabaseUnavailable("connection refused") from _operational()
    except DatabaseUnavailable as exc:
        return exc


_DB_FAILURE_CALLS = [
    ("query", (SlayerQueryEngine, "execute"), {"query": _QUERY}),
    ("create_model", (SlayerQueryEngine, "save_model"),
     {"name": "m", "sql_table": "orders", "data_source": "test", "columns": [{"name": "id", "sql": "id", "type": "number"}]}),
    ("create_model", (SlayerQueryEngine, "create_model_from_query"), {"name": "m", "query": _QUERY}),
    ("validate_models", (SlayerQueryEngine, "validate_models"), {}),
    ("ingest_datasource_models", (engine_ingestion, "ingest_datasource_idempotent"), {"datasource_name": "test"}),
]


@pytest.mark.parametrize("failure", [_operational, _unavailable], ids=["sqlalchemy", "client"])
@pytest.mark.parametrize(("tool", "target", "arguments"), _DB_FAILURE_CALLS,
                         ids=["query", "create_model_table", "create_model_query", "validate_models", "ingest"])
async def test_mcp_database_failure_counted_with_a_database_token(
    store: str, started, monkeypatch: pytest.MonkeyPatch,
    tool: str, target: tuple[object, str], arguments: dict, failure,
) -> None:
    async def fail(*args, **kwargs):
        raise failure()

    monkeypatch.setattr(*target, fail)
    server = create_mcp_server(storage=YAMLStorage(base_dir=store), _seed_help=False)
    async with create_connected_server_and_client_session(server) as session:
        result = await session.call_tool(tool, arguments)
    end_process()
    assert result.isError
    entry = usage(show(), f"mcp:{tool}")
    assert entry["ok"] == 0
    (token,) = entry["errors"]
    # A typed client failure (DatabaseUnavailable, ...) counts as a database failure too.
    assert token.startswith("db_"), token


async def test_session_counts_a_database_failure_as_failed(
    store: str, started, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail(*args, **kwargs):
        raise _operational()

    server = create_mcp_server(storage=YAMLStorage(base_dir=store), _seed_help=False)
    async with create_connected_server_and_client_session(server) as session:
        await session.call_tool("query", {"query": _QUERY})
        monkeypatch.setattr(SlayerQueryEngine, "execute", fail)
        await session.call_tool("query", {"query": _QUERY})
    end_process()
    report = show()
    assert usage(report, "mcp:query") == {"ok": 1, "errors": {"db_operational": 1}}
    assert session_rows(report) == [("1", "1", 1)]
