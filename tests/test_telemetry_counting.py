"""Usage counters per surface: CLI, MCP, REST, Flight SQL, PostgreSQL wire."""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import threading
from collections.abc import Iterator
from types import SimpleNamespace

import pyarrow.flight as fl
import pytest
import uvicorn
from fastapi.testclient import TestClient
from mcp import ClientSession
from mcp.client.sse import sse_client
from mcp.shared.memory import create_connected_server_and_client_session
from mcp.types import Implementation

from slayer import telemetry
from slayer.api.server import create_app
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage
from slayer.telemetry import settings
from tests._telemetry_helpers import (
    build_storage,
    cli,
    end_process,
    quiet_config,
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
def store(telemetry_env) -> str:
    quiet_config()
    return build_storage(telemetry_env.root)


@pytest.fixture
def started(telemetry_env) -> None:
    quiet_config()
    telemetry.start()


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
    assert usage_keys(show(), surface="cli") <= {"cli:telemetry.show"}


def test_parse_error_exits_before_any_telemetry(telemetry_env) -> None:
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


def test_mcp_tool_schemas_identical_with_telemetry_on_and_off(store: str) -> None:
    off = _tool_schemas(store)
    telemetry.start()
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
    telemetry.start()
    client.get("/no/such/path/42")
    client.get("/another/unknown")
    end_process()
    report = show()
    new = usage_keys(report, surface="rest") - before
    assert len(new) == 1
    (key,) = new
    assert key in usage_key_vocabulary()
    assert "path" not in key and "unknown" not in key
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
