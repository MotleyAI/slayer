"""``slayer mcp`` answers the handshake at once; startup preparation runs behind a gate every tool call waits on."""

from __future__ import annotations

import asyncio
import contextlib
import sys
import threading
import time
from collections.abc import AsyncGenerator, Iterator
from pathlib import Path
from typing import Any

import pytest
from mcp.client import Client

from slayer.async_utils import run_sync
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema import ingestion as ingestion_module
from slayer.live_schema.ingestion import StartupIngestSummary
from slayer.mcp.server import StartupPreparation, create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage
from tests._mcp_gated_server import ORDERS_ROWS, tiny_orders_datasource
from tests._mcp_stdio_rpc import StdioRpc, server_env, tool_text

HANDSHAKE_S = 60.0  # a hang guard only: preparation is held, so any answer proves the handshake did not wait
NO_WAIT_S = 5.0
REPLY_S = 60.0
COUNT_QUERY = {"query": {"population": "orders", "metrics": ["count(orders.*)"]}, "format": "json"}

DEMO_FAILURE = "Failed to set up the Jaffle Shop demo: "


class Gated:
    """A gated server process plus the release file that lets its preparation finish."""

    def __init__(self, *, tmp_path: Path, scenario: str, args: list[str]) -> None:
        self.release_file = tmp_path / "release"
        env = {**server_env(), "GATE_RELEASE": str(self.release_file)}
        self.rpc = StdioRpc([sys.executable, "-m", "tests._mcp_gated_server", scenario, *args], env=env)

    def release(self) -> None:
        self.release_file.touch()


@pytest.fixture
def gated(tmp_path: Path) -> Iterator[Any]:
    servers: list[Gated] = []

    def start(scenario: str, *args: str) -> Gated:
        server = Gated(tmp_path=tmp_path, scenario=scenario, args=list(args))
        servers.append(server)
        return server

    yield start
    for server in servers:
        server.release()
        server.rpc.close()
        assert server.rpc.non_protocol_stdout == [], server.rpc.non_protocol_stdout


def _store(tmp_path: Path) -> str:
    return str(tmp_path / "store")


def _demo(gated: Any, tmp_path: Path, scenario: str = "demo_ok") -> Gated:
    return gated(scenario, "mcp", "--demo", "--storage", _store(tmp_path))


def _rows(reply: dict[str, Any]) -> str:
    is_error, text = tool_text(reply)
    assert not is_error, text
    return text


@contextlib.asynccontextmanager
async def _session(server: Any) -> AsyncGenerator[Client, None]:
    """An in-memory client session; the serving engine is released on this loop afterwards."""
    try:
        async with Client(server, mode="legacy") as client:
            yield client
    finally:
        await getattr(server, "_slayer_engine").aclose()


class TestHandshake:
    def test_handshake_answers_while_the_demo_setup_is_held(self, gated: Any, tmp_path: Path) -> None:
        server = _demo(gated, tmp_path)
        tools = server.rpc.handshake(timeout=HANDSHAKE_S)
        assert "query" in tools
        assert not server.release_file.exists()
        # Preparation starts with the server, not on the first tool call.
        server.rpc.wait_for_stderr("stub demo: building", timeout=HANDSHAKE_S)

    def test_handshake_answers_while_the_startup_ingest_is_held(self, gated: Any, tmp_path: Path) -> None:
        _store_app_and_broken(tmp_path)
        server = gated("ingest_errors", "mcp", "--ingest-on-startup", "--storage", _store(tmp_path))
        assert "query" in server.rpc.handshake(timeout=HANDSHAKE_S)
        server.rpc.wait_for_stderr("Ingesting datasource", timeout=HANDSHAKE_S)


class TestHeldCalls:
    def test_a_held_query_returns_rows_after_release(self, gated: Any, tmp_path: Path) -> None:
        server = _demo(gated, tmp_path)
        server.rpc.handshake(timeout=HANDSHAKE_S)
        call = server.rpc.call_tool("query", COUNT_QUERY)
        time.sleep(1.0)
        assert not server.rpc.has_response(call)
        server.release()
        assert str(ORDERS_ROWS) in _rows(server.rpc.response(call, timeout=REPLY_S))

    def test_progress_is_sent_to_a_call_with_a_token(self, gated: Any, tmp_path: Path) -> None:
        server = _demo(gated, tmp_path)
        server.rpc.handshake(timeout=HANDSHAKE_S)
        call = server.rpc.call_tool("query", COUNT_QUERY, progress_token="tok-a")
        server.rpc.wait_for_progress("tok-a", timeout=REPLY_S)
        server.release()
        _rows(server.rpc.response(call, timeout=REPLY_S))

    def test_no_progress_without_a_token(self, gated: Any, tmp_path: Path) -> None:
        server = _demo(gated, tmp_path)
        server.rpc.handshake(timeout=HANDSHAKE_S)
        call = server.rpc.call_tool("query", COUNT_QUERY)
        time.sleep(3.0)
        server.release()
        _rows(server.rpc.response(call, timeout=REPLY_S))
        assert server.rpc.progress() == []

    def test_concurrent_calls_each_get_their_own_progress(self, gated: Any, tmp_path: Path) -> None:
        server = _demo(gated, tmp_path)
        server.rpc.handshake(timeout=HANDSHAKE_S)
        first = server.rpc.call_tool("query", COUNT_QUERY, progress_token="tok-a")
        second = server.rpc.call_tool("list_datasources", {}, progress_token="tok-b")
        server.rpc.wait_for_progress("tok-a", timeout=REPLY_S)
        server.rpc.wait_for_progress("tok-b", timeout=REPLY_S)
        server.release()
        _rows(server.rpc.response(first, timeout=REPLY_S))
        assert "jaffle_shop" in _rows(server.rpc.response(second, timeout=REPLY_S))

    def test_a_failing_progress_notification_does_not_fail_preparation(self, gated: Any, tmp_path: Path) -> None:
        server = _demo(gated, tmp_path, scenario="progress_raises")
        server.rpc.handshake(timeout=HANDSHAKE_S)
        call = server.rpc.call_tool("query", COUNT_QUERY, progress_token="tok-a")
        time.sleep(2.0)
        server.release()
        assert str(ORDERS_ROWS) in _rows(server.rpc.response(call, timeout=REPLY_S))

    def test_a_cancelled_call_does_not_stop_preparation(self, gated: Any, tmp_path: Path) -> None:
        server = _demo(gated, tmp_path)
        server.rpc.handshake(timeout=HANDSHAKE_S)
        cancelled = server.rpc.call_tool("query", COUNT_QUERY, progress_token="tok-a")
        kept = server.rpc.call_tool("query", COUNT_QUERY, progress_token="tok-b")
        server.rpc.wait_for_progress("tok-a", timeout=REPLY_S)
        server.rpc.cancel(cancelled)
        time.sleep(1.0)
        server.release()
        assert str(ORDERS_ROWS) in _rows(server.rpc.response(kept, timeout=REPLY_S))
        later = server.rpc.call_tool("query", COUNT_QUERY)
        assert str(ORDERS_ROWS) in _rows(server.rpc.response(later, timeout=REPLY_S))


class TestDemoFailure:
    def test_every_call_returns_the_demo_error_and_the_server_stays_up(self, gated: Any, tmp_path: Path) -> None:
        server = _demo(gated, tmp_path, scenario="demo_fail")
        server.rpc.handshake(timeout=HANDSHAKE_S)
        held = server.rpc.call_tool("query", COUNT_QUERY)
        server.release()
        replies = [server.rpc.response(held, timeout=REPLY_S)]
        for tool, arguments in (("list_datasources", {}), ("query", COUNT_QUERY)):
            replies.append(server.rpc.response(server.rpc.call_tool(tool, arguments), timeout=REPLY_S))
        for reply in replies:
            is_error, text = tool_text(reply)
            assert is_error, text
            assert text.startswith(DEMO_FAILURE), text
            assert "generator exploded" in text
        assert server.rpc.proc.poll() is None
        assert "generator exploded" in server.rpc.stderr


def _store_app_and_broken(tmp_path: Path) -> None:
    store = _store(tmp_path)
    storage = YAMLStorage(base_dir=store)
    ds, dataset = tiny_orders_datasource(directory=str(tmp_path), name="app")
    run_sync(storage.save_datasource(ds))
    run_sync(storage.save_dataset(dataset))
    run_sync(storage.save_datasource(DatasourceConfig(name="broken", type="sqlite", database=str(tmp_path / "b.db"))))


class TestStartupIngest:
    def test_ingest_failures_are_reported_on_stderr_and_never_fail_calls(self, gated: Any, tmp_path: Path) -> None:
        _store_app_and_broken(tmp_path)
        server = gated("ingest_errors", "mcp", "--ingest-on-startup", "--storage", _store(tmp_path))
        server.rpc.handshake(timeout=HANDSHAKE_S)
        call = server.rpc.call_tool("query", COUNT_QUERY)
        time.sleep(1.0)
        assert not server.rpc.has_response(call)
        server.release()
        assert str(ORDERS_ROWS) in _rows(server.rpc.response(call, timeout=REPLY_S))
        server.rpc.wait_for_stderr("cannot reach broken", timeout=REPLY_S)
        assert "table exploded" in server.rpc.stderr
        assert server.rpc.non_protocol_stdout == []


class TestWithoutPreparation:
    def test_calls_do_not_wait(self, tmp_path: Path) -> None:
        rpc = StdioRpc([sys.executable, "-m", "slayer", "mcp", "--storage", _store(tmp_path)])
        try:
            rpc.handshake(timeout=HANDSHAKE_S)
            started = time.monotonic()
            reply = rpc.response(rpc.call_tool("list_datasources", {}, progress_token="tok"), timeout=REPLY_S)
            assert not tool_text(reply)[0]
            assert time.monotonic() - started < NO_WAIT_S
            assert rpc.progress() == []
            assert rpc.non_protocol_stdout == []
        finally:
            rpc.close()


class _EngineReleases:
    """Records each engine's ``aclose`` / ``close`` in call order, with the loop ``aclose`` ran on."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.calls: list[tuple[int, str]] = []
        self.aclose_loops: dict[int, asyncio.AbstractEventLoop] = {}
        real_aclose, real_close = SlayerQueryEngine.aclose, SlayerQueryEngine.close
        releases = self

        async def recording_aclose(engine: SlayerQueryEngine) -> None:
            releases.calls.append((id(engine), "aclose"))
            releases.aclose_loops[id(engine)] = asyncio.get_running_loop()
            await real_aclose(engine)

        def recording_close(engine: SlayerQueryEngine) -> None:
            releases.calls.append((id(engine), "close"))
            real_close(engine)

        monkeypatch.setattr(SlayerQueryEngine, "aclose", recording_aclose)
        monkeypatch.setattr(SlayerQueryEngine, "close", recording_close)

    def of(self, engine: object) -> list[str]:
        return [kind for engine_id, kind in self.calls if engine_id == id(engine)]


class TestPreparationEngine:
    """The startup ingest runs on a dedicated engine, released afterwards; the serving engine never holds its clients."""

    async def test_startup_ingest_uses_its_own_engine(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        storage = YAMLStorage(base_dir=_store(tmp_path))
        ds, dataset = tiny_orders_datasource(directory=str(tmp_path), name="app")
        await storage.save_datasource(ds)
        await storage.save_dataset(dataset)
        seen: dict[str, Any] = {}
        releases = _EngineReleases(monkeypatch)

        async def orchestrator(*, storage: Any, client_for: Any, stream: Any = None) -> StartupIngestSummary:
            seen["client_for"] = client_for
            seen["client"] = client_for(ds)
            seen["loop"] = asyncio.get_running_loop()
            await seen["client"].execute("SELECT 1")
            return StartupIngestSummary(succeeded=["app"])

        monkeypatch.setattr(ingestion_module, "ingest_all_datasources_idempotent", orchestrator)
        server = create_mcp_server(storage=storage, ingest_on_startup=True, _seed_help=False)
        serving: SlayerQueryEngine = getattr(server, "_slayer_engine")
        async with _session(server) as client:
            result = await client.call_tool("query", COUNT_QUERY)
            assert not result.is_error, result.content
            assert seen["client"] not in serving._sql_clients.values()
            serving_loop = asyncio.get_running_loop()
        prep_engine = getattr(seen["client_for"], "__self__", None)
        assert prep_engine is not None
        assert prep_engine is not serving
        assert seen["loop"] is serving_loop
        assert releases.aclose_loops.get(id(prep_engine)) is seen["loop"]

    @pytest.mark.parametrize("outcome", ["success", "failure", "cancellation"])
    async def test_the_preparation_engine_is_aclosed_then_closed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: str,
    ) -> None:
        entered = asyncio.Event()
        engines: list[object] = []
        releases = _EngineReleases(monkeypatch)

        async def orchestrator(*, client_for: Any, **_kwargs: Any) -> StartupIngestSummary:
            engines.append(client_for.__self__)
            entered.set()
            if outcome == "failure":
                raise RuntimeError("ingest exploded")
            if outcome == "cancellation":
                await asyncio.sleep(REPLY_S)
            return StartupIngestSummary()

        monkeypatch.setattr(ingestion_module, "ingest_all_datasources_idempotent", orchestrator)
        server = create_mcp_server(storage=YAMLStorage(base_dir=_store(tmp_path)), ingest_on_startup=True,
                                   _seed_help=False)
        async with _session(server) as client:
            await asyncio.wait_for(entered.wait(), timeout=HANDSHAKE_S)
            if outcome != "cancellation":
                result = await asyncio.wait_for(client.call_tool("list_datasources", {}), timeout=REPLY_S)
                assert not result.is_error, result.content
        (prep_engine,) = engines
        assert releases.of(prep_engine) == ["aclose", "close"]

    async def test_demo_setup_runs_before_the_startup_ingest(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        order: list[str] = []

        async def orchestrator(**_kwargs: Any) -> StartupIngestSummary:
            order.append("ingest")
            return StartupIngestSummary()

        monkeypatch.setattr(ingestion_module, "ingest_all_datasources_idempotent", orchestrator)
        server = create_mcp_server(
            storage=YAMLStorage(base_dir=_store(tmp_path)), ingest_on_startup=True,
            demo_setup=lambda: order.append("demo"), _seed_help=False,
        )
        async with _session(server) as client:
            result = await client.call_tool("list_datasources", {})
            assert not result.is_error, result.content
        assert order == ["demo", "ingest"]

    async def test_shutdown_cancels_and_awaits_a_running_preparation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        entered = asyncio.Event()
        outcome: list[str] = []
        releases = _EngineReleases(monkeypatch)

        async def orchestrator(*, client_for: Any, **_kwargs: Any) -> StartupIngestSummary:
            outcome.append(f"engine:{id(client_for.__self__)}")
            entered.set()
            try:
                await asyncio.sleep(REPLY_S)  # bounded, so a synchronous factory ingest fails instead of hanging
            except asyncio.CancelledError:
                outcome.append("cancelled")
                raise
            return StartupIngestSummary()

        monkeypatch.setattr(ingestion_module, "ingest_all_datasources_idempotent", orchestrator)
        server = create_mcp_server(storage=YAMLStorage(base_dir=_store(tmp_path)), ingest_on_startup=True,
                                   _seed_help=False)
        async with _session(server):
            await asyncio.wait_for(entered.wait(), timeout=HANDSHAKE_S)
        assert outcome[-1] == "cancelled"
        assert outcome[0] in {f"engine:{engine_id}" for engine_id, kind in releases.calls if kind == "aclose"}

    async def test_one_session_closing_does_not_stop_preparation_for_another(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        release = asyncio.Event()
        started = asyncio.Event()
        events: list[str] = []

        async def orchestrator(**_kwargs: Any) -> StartupIngestSummary:
            events.append("started")
            started.set()
            try:
                await asyncio.wait_for(release.wait(), timeout=REPLY_S)
            except asyncio.CancelledError:
                events.append("cancelled")
                raise
            events.append("finished")
            return StartupIngestSummary()

        monkeypatch.setattr(ingestion_module, "ingest_all_datasources_idempotent", orchestrator)
        server = create_mcp_server(storage=YAMLStorage(base_dir=_store(tmp_path)), ingest_on_startup=True,
                                   _seed_help=False)
        close_first = asyncio.Event()

        async def first_session() -> None:
            async with Client(server, mode="legacy"):
                await close_first.wait()

        first = asyncio.ensure_future(first_session())
        await asyncio.wait_for(started.wait(), timeout=HANDSHAKE_S)
        async with _session(server) as kept:
            close_first.set()
            await asyncio.wait_for(first, timeout=REPLY_S)
            call = asyncio.ensure_future(kept.call_tool("list_datasources", {}))
            await asyncio.sleep(0.2)
            release.set()
            result = await asyncio.wait_for(call, timeout=REPLY_S)
            assert not result.is_error, result.content
        assert events == ["started", "finished"]

    async def test_a_session_after_an_interrupted_preparation_gets_a_prepared_server(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        release = asyncio.Event()
        started = asyncio.Event()
        events: list[str] = []

        async def orchestrator(**_kwargs: Any) -> StartupIngestSummary:
            events.append("started")
            started.set()
            try:
                await asyncio.wait_for(release.wait(), timeout=REPLY_S)
            except asyncio.CancelledError:
                events.append("cancelled")
                raise
            events.append("finished")
            return StartupIngestSummary()

        monkeypatch.setattr(ingestion_module, "ingest_all_datasources_idempotent", orchestrator)
        server = create_mcp_server(storage=YAMLStorage(base_dir=_store(tmp_path)), ingest_on_startup=True,
                                   _seed_help=False)
        async with Client(server, mode="legacy"):
            await asyncio.wait_for(started.wait(), timeout=HANDSHAKE_S)
        started.clear()
        async with _session(server) as later:
            call = asyncio.ensure_future(later.call_tool("list_datasources", {}))
            await asyncio.wait_for(started.wait(), timeout=HANDSHAKE_S)
            assert not call.done()
            release.set()
            result = await asyncio.wait_for(call, timeout=REPLY_S)
            assert not result.is_error, result.content
        assert events == ["started", "cancelled", "started", "finished"]

    async def test_a_close_during_the_drain_starts_no_second_demo_worker(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        runs: list[str] = []

        def demo_setup() -> None:
            runs.append("demo")
            entered.set()
            release.wait(REPLY_S)

        preparation = StartupPreparation(demo_setup=demo_setup, ingest=None)
        first = preparation.lifespan(None)
        await first.__aenter__()
        assert await asyncio.to_thread(entered.wait, HANDSHAKE_S)
        first_close = asyncio.ensure_future(first.__aexit__(None, None, None))
        await asyncio.sleep(0.1)  # the first close is now draining the demo thread
        second = preparation.lifespan(None)
        await second.__aenter__()
        second_close = asyncio.ensure_future(second.__aexit__(None, None, None))
        await asyncio.sleep(0.1)
        third = preparation.lifespan(None)
        await third.__aenter__()
        await asyncio.sleep(0.3)
        assert runs == ["demo"]
        release.set()
        await asyncio.wait_for(asyncio.gather(first_close, second_close), timeout=REPLY_S)
        await third.__aexit__(None, None, None)

    async def test_shutdown_awaits_a_running_demo_setup(self, tmp_path: Path) -> None:
        entered = threading.Event()
        finished = threading.Event()

        def demo_setup() -> None:
            entered.set()
            time.sleep(1.0)
            finished.set()

        server = create_mcp_server(storage=YAMLStorage(base_dir=_store(tmp_path)), demo_setup=demo_setup,
                                   _seed_help=False)
        async with _session(server):
            assert await asyncio.to_thread(entered.wait, HANDSHAKE_S)
        assert finished.is_set()


def test_factory_does_not_run_the_startup_ingest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    async def orchestrator(**_kwargs: Any) -> StartupIngestSummary:
        calls.append("ingest")
        return StartupIngestSummary()

    monkeypatch.setattr(ingestion_module, "ingest_all_datasources_idempotent", orchestrator)
    create_mcp_server(storage=YAMLStorage(base_dir=_store(tmp_path)), ingest_on_startup=True, _seed_help=False)
    assert calls == []
