"""The MCP server's SDK runtime: identity, tool-failure text, and the SSE mount's Host check."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator
from importlib.metadata import version
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from mcp.client import Client
from mcp.shared.exceptions import MCPError
from mcp.types import INVALID_PARAMS, CallToolResult, TextContent

from slayer.api.server import create_app
from slayer.core.errors import QueryExtensionError
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import SERVER_INSTRUCTIONS, create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage

MESSAGE = "zqxj failure text"
_QUERY = {"population": "orders", "metrics": ["count(orders.*)"]}


@contextlib.asynccontextmanager
async def _client(tmp_path: Path) -> AsyncGenerator[Client, None]:
    server = create_mcp_server(storage=YAMLStorage(base_dir=str(tmp_path)), _seed_help=False)
    try:
        async with Client(server, mode="legacy") as connected:
            yield connected
    finally:
        await getattr(server, "_slayer_engine").aclose()


async def _call(tmp_path: Path, *, tool: str, arguments: dict[str, Any]) -> CallToolResult:
    async with _client(tmp_path) as client:
        return await client.call_tool(tool, arguments)


def _error_text(result: CallToolResult) -> str:
    assert result.is_error
    (block,) = result.content
    assert isinstance(block, TextContent)
    return block.text


class TestServerIdentity:
    async def test_initialize_reports_slayer_name_version_and_instructions(self, tmp_path: Path) -> None:
        async with _client(tmp_path) as client:
            info, instructions = client.server_info, client.instructions
        assert info is not None
        assert (info.name, info.version) == ("SLayer", version("motley-slayer"))
        assert instructions == SERVER_INSTRUCTIONS


class TestToolFailures:
    async def test_slayer_error_keeps_its_message(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        async def fail(*args: Any, **kwargs: Any) -> Any:
            raise QueryExtensionError(MESSAGE)

        monkeypatch.setattr(SlayerQueryEngine, "execute", fail)
        result = await _call(tmp_path, tool="query", arguments={"query": _QUERY})
        assert _error_text(result) == f"Error executing tool query: {MESSAGE}"

    async def test_real_engine_error_keeps_its_message(self, tmp_path: Path) -> None:
        result = await _call(tmp_path, tool="query", arguments={"query": {"population": "zqxj", "metrics": ["count(zqxj.*)"]}})
        assert _error_text(result) == "Error executing tool query: Dataset 'zqxj' not found"

    @pytest.mark.parametrize("exc_type", [ValueError, RuntimeError])
    async def test_non_slayer_exception_keeps_its_message(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, exc_type: type[Exception],
    ) -> None:
        async def fail(*args: Any, **kwargs: Any) -> Any:
            raise exc_type(MESSAGE)

        monkeypatch.setattr(YAMLStorage, "list_datasources", fail)
        result = await _call(tmp_path, tool="list_datasources", arguments={})
        assert _error_text(result) == f"Error executing tool list_datasources: {MESSAGE}"

    async def test_invalid_arguments_name_the_offending_argument(self, tmp_path: Path) -> None:
        result = await _call(tmp_path, tool="query", arguments={"query": _QUERY, "dry_run": {"zqxj": 1}})
        text = _error_text(result)
        assert text.startswith("Error executing tool query:")
        assert "dry_run" in text

    async def test_protocol_error_passes_through(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        async def fail(*args: Any, **kwargs: Any) -> Any:
            raise MCPError(code=INVALID_PARAMS, message=MESSAGE)

        monkeypatch.setattr(YAMLStorage, "list_datasources", fail)
        async with _client(tmp_path) as client:
            with pytest.raises(MCPError) as excinfo:
                await client.call_tool("list_datasources", {})
        assert (excinfo.value.error.code, excinfo.value.error.message) == (INVALID_PARAMS, MESSAGE)


def _post_message(app: Any, *, host: str) -> int:
    response = TestClient(app).post(
        "/mcp/messages/", headers={"Host": host, "Content-Type": "application/json"}, content="{}",
    )
    return response.status_code


class TestSseHost:
    @pytest.mark.parametrize("bind", ["0.0.0.0", "10.1.2.3"])  # NOSONAR(S1313) — bind addresses under test
    def test_non_loopback_bind_does_not_check_host(self, tmp_path: Path, bind: str) -> None:
        app = create_app(storage=YAMLStorage(base_dir=str(tmp_path)), host=bind)
        assert _post_message(app, host="slayer.example.com:5143") != 421

    @pytest.mark.parametrize("bind", ["127.0.0.1", "localhost", "::1"])
    def test_loopback_bind_refuses_a_foreign_host(self, tmp_path: Path, bind: str) -> None:
        app = create_app(storage=YAMLStorage(base_dir=str(tmp_path)), host=bind)
        assert _post_message(app, host="evil.example:5143") == 421

    def test_default_bind_is_loopback(self, tmp_path: Path) -> None:
        app = create_app(storage=YAMLStorage(base_dir=str(tmp_path)))
        assert _post_message(app, host="evil.example:5143") == 421

    def test_loopback_bind_admits_a_loopback_host(self, tmp_path: Path) -> None:
        app = create_app(storage=YAMLStorage(base_dir=str(tmp_path)), host="127.0.0.1")
        assert _post_message(app, host="127.0.0.1:5143") != 421
