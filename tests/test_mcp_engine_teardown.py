"""DEV-1656: the SLayer MCP server exposes its per-task query engine for
caller-controlled teardown.

``create_mcp_server`` attaches its closure ``SlayerQueryEngine`` to the
returned FastMCP object as ``mcp._slayer_engine`` — the cross-repo contract
bird-interact-agents disposes at task teardown:

    engine = getattr(mcp, "_slayer_engine", None)
    if engine is not None:
        await engine.aclose()

The two read-only introspection tools (``validate_models`` /
``recommend_root_model``) reuse that closure engine instead of constructing
their own per-call engine, so a single engine holds every cached SQL client
for the server's lifetime and one ``aclose()`` disposes them all — including
the client the ``sql``-mode schema-drift path opens.

These tests build a *fresh* server per test (never the session-scoped shared
fixture in ``test_mcp_server.py``) so mutating / disposing the engine can't
leak across tests.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path
from typing import Any
from collections.abc import Iterator
from unittest.mock import AsyncMock, MagicMock

import pytest

import slayer.mcp.server as srv
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine, _sql_client_cache_key
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def workspace() -> Iterator[Path]:
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


def _make_sqlite_db(workspace: Path) -> Path:
    db = workspace / "live.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
    conn.execute("INSERT INTO t (v) VALUES (10), (20), (30)")
    conn.commit()
    conn.close()
    return db


def _lite_datasource(db: Path) -> DatasourceConfig:
    return DatasourceConfig(name="lite", type="sqlite", database=str(db))


async def _seed_storage(workspace: Path, *, with_sql_mode: bool = False) -> YAMLStorage:
    """A YAML store with a ``lite`` sqlite datasource, a ``sql_table`` model
    ``t`` and (optionally) a ``sql``-mode model ``sv`` over the same table."""
    db = _make_sqlite_db(workspace)
    storage = YAMLStorage(base_dir=str(workspace / "store"))
    await storage.save_datasource(_lite_datasource(db))
    await storage.save_model(SlayerModel(
        name="t", data_source="lite", sql_table="t",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="v", sql="v", type=DataType.INT),
        ],
    ))
    if with_sql_mode:
        await storage.save_model(SlayerModel(
            name="sv", data_source="lite", sql="SELECT id, v FROM t",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="v", sql="v", type=DataType.INT),
            ],
        ))
    return storage


async def _call(server, *, name: str, arguments: dict[str, Any] | None = None) -> str:
    content_blocks, _ = await server.call_tool(name=name, arguments=arguments or {})
    return content_blocks[0].text


async def _query_count(server, *, source_model: str) -> int:
    """Run the ``query`` tool for ``*:count`` and return the integer count."""
    text = await _call(
        server, name="query",
        arguments={
            "source_model": source_model,
            "measures": [{"formula": "*:count"}],
            "format": "json",
        },
    )
    # The query tool's json format appends a human-readable "Measure
    # attributes:" footer after the JSON payload — decode just the leading
    # JSON value and ignore the trailing text.
    rows, _ = json.JSONDecoder().raw_decode(text)
    row = rows[0] if isinstance(rows, list) else rows["data"][0]
    return int(row[f"{source_model}._count"])


class TestContractAttribute:
    async def test_slayer_engine_attribute_is_a_query_engine(
        self, workspace: Path,
    ) -> None:
        storage = await _seed_storage(workspace)
        server = create_mcp_server(storage=storage)
        assert isinstance(server._slayer_engine, SlayerQueryEngine)

    async def test_slayer_engine_is_the_object_query_uses(
        self, workspace: Path, monkeypatch,
    ) -> None:
        """The exposed attribute is identity-equal to the engine the ``query``
        tool executes on — not just some engine."""
        storage = await _seed_storage(workspace)
        server = create_mcp_server(storage=storage)

        captured: dict[str, Any] = {}
        original = SlayerQueryEngine.execute

        async def _spy(self, *args, **kwargs):
            captured["self"] = self
            return await original(self, *args, **kwargs)

        monkeypatch.setattr(SlayerQueryEngine, "execute", _spy)
        await _query_count(server, source_model="t")

        assert captured["self"] is server._slayer_engine

    async def test_slayer_engine_is_the_object_query_nested_uses(
        self, workspace: Path, monkeypatch,
    ) -> None:
        """query_nested shares the same closure engine (plan: one engine
        across query/query_nested/validate_models/recommend_root_model)."""
        storage = await _seed_storage(workspace)
        server = create_mcp_server(storage=storage)

        captured: dict[str, Any] = {}
        original = SlayerQueryEngine.execute

        async def _spy(self, *args, **kwargs):
            captured["self"] = self
            return await original(self, *args, **kwargs)

        monkeypatch.setattr(SlayerQueryEngine, "execute", _spy)
        await _call(
            server, name="query_nested",
            arguments={"queries": [{"source_model": "t", "measures": [{"formula": "*:count"}]}]},
        )

        assert captured["self"] is server._slayer_engine


class TestToolsReuseClosureEngine:
    async def test_validate_models_constructs_no_new_engine(
        self, workspace: Path, monkeypatch,
    ) -> None:
        storage = await _seed_storage(workspace)
        server = create_mcp_server(storage=storage)

        class _Boom:
            def __init__(self, *args, **kwargs):
                raise AssertionError("validate_models constructed a new engine")

        monkeypatch.setattr(srv, "SlayerQueryEngine", _Boom)
        out = await _call(server, name="validate_models", arguments={"data_source": "lite"})
        # Sane output: a JSON list of pending deletes (empty when no drift).
        assert isinstance(json.loads(out), list)

    async def test_recommend_root_model_constructs_no_new_engine(
        self, workspace: Path, monkeypatch,
    ) -> None:
        storage = await _seed_storage(workspace)
        server = create_mcp_server(storage=storage)

        class _Boom:
            def __init__(self, *args, **kwargs):
                raise AssertionError("recommend_root_model constructed a new engine")

        monkeypatch.setattr(srv, "SlayerQueryEngine", _Boom)
        out = await _call(
            server, name="recommend_root_model",
            arguments={"items": ["t.v"], "data_source": "lite"},
        )
        assert "t" in out


class TestAcloseDisposesCachedClients:
    async def test_aclose_disposes_async_engine_sync_pool_survives(
        self, workspace: Path, monkeypatch,
    ) -> None:
        """After a ``query`` caches a client, ``server._slayer_engine.aclose()``
        disposes the (fake) async engine while leaving the sqlite StaticPool
        sync engine intact, and a follow-up query still returns the same rows.
        File-backed sqlite stands in for ``:memory:`` — the client-cache logic
        is identical and ``:memory:`` can't be probed past DEV-1538 introspect.
        """
        storage = await _seed_storage(workspace)
        server = create_mcp_server(storage=storage)

        assert await _query_count(server, source_model="t") == 3
        engine = server._slayer_engine
        assert len(engine._sql_clients) == 1
        client = next(iter(engine._sql_clients.values()))
        sync_engine_before = client._sync_engine

        # sqlite never opens an async engine; inject a tracked one so the real
        # aclose loop (engine -> client -> dispose) is exercised.
        fake = MagicMock(name="fake_async_engine")
        fake.dispose = AsyncMock(name="dispose")
        client._async_engine = fake

        # Guard: aclose must NOT dispose the sync StaticPool engine — that's
        # what pins the (potential :memory:) connection holding the data.
        sync_dispose_spy = MagicMock(name="sync_dispose")
        monkeypatch.setattr(sync_engine_before, "dispose", sync_dispose_spy)

        await engine.aclose()

        fake.dispose.assert_awaited_once()
        assert client._async_engine is None
        assert client in engine._sql_clients.values()  # client kept
        assert client._sync_engine is sync_engine_before  # StaticPool untouched
        sync_dispose_spy.assert_not_called()

        # Reusable after disposal — the sync path still serves the same data.
        assert await _query_count(server, source_model="t") == 3


class TestAcloseNoOpSafeAndReusable:
    async def test_aclose_no_async_engine_is_noop_safe(
        self, workspace: Path,
    ) -> None:
        storage = await _seed_storage(workspace)
        server = create_mcp_server(storage=storage)
        # No query yet — nothing opened. Must not raise.
        await server._slayer_engine.aclose()
        # And the engine is still usable.
        assert await _query_count(server, source_model="t") == 3

    async def test_query_after_aclose_still_works(
        self, workspace: Path,
    ) -> None:
        storage = await _seed_storage(workspace)
        server = create_mcp_server(storage=storage)
        assert await _query_count(server, source_model="t") == 3
        await server._slayer_engine.aclose()
        # Async engine lazily recreated / sync path intact.
        assert await _query_count(server, source_model="t") == 3


class TestSqlModeDriftClientReachable:
    """DEV-1656 / Codex finding #1: the ``sql``-mode schema-drift path opens a
    ``SlayerSQLClient`` that must be cached on the shared engine so teardown
    reaches it (otherwise it leaks an asyncpg pool on Postgres)."""

    async def test_validate_models_caches_sql_mode_client_and_aclose_disposes_it(
        self, workspace: Path,
    ) -> None:
        """A ``validate_models`` call with no prior query must cache the
        ``sql``-mode drift client on the shared engine, and a subsequent
        ``aclose()`` must dispose *that specific* client's async engine."""
        storage = await _seed_storage(workspace, with_sql_mode=True)
        server = create_mcp_server(storage=storage)
        engine = server._slayer_engine
        assert len(engine._sql_clients) == 0

        await _call(server, name="validate_models", arguments={"data_source": "lite"})

        key = _sql_client_cache_key(_lite_datasource(workspace / "live.db"))
        assert key in engine._sql_clients, (
            "sql-mode drift client not cached on the engine — aclose() can't "
            "reach it, so the pool leaks"
        )

        # Prove teardown actually disposes this cached client (sqlite opens no
        # async engine, so inject a tracked one — the Postgres leak path).
        client = engine._sql_clients[key]
        fake = MagicMock(name="fake_async_engine")
        fake.dispose = AsyncMock(name="dispose")
        client._async_engine = fake

        await engine.aclose()

        fake.dispose.assert_awaited_once()
        assert client._async_engine is None

    async def test_query_then_validate_reuses_same_client(
        self, workspace: Path,
    ) -> None:
        storage = await _seed_storage(workspace, with_sql_mode=True)
        server = create_mcp_server(storage=storage)
        engine = server._slayer_engine

        # Query the sql-mode model — caches a client under the datasource key.
        assert await _query_count(server, source_model="sv") == 3
        assert len(engine._sql_clients) == 1
        key = next(iter(engine._sql_clients.keys()))
        client_before = engine._sql_clients[key]

        # validate_models reuses that cached client object; no second one is
        # built (a new client under the same key would still leak on teardown
        # of the original — assert object identity, not just key equality).
        await _call(server, name="validate_models", arguments={"data_source": "lite"})
        assert set(engine._sql_clients.keys()) == {key}
        assert engine._sql_clients[key] is client_before
