"""The MCP server's SDK runtime: identity and metadata, tool-failure text, and the SSE mount's Host check."""

from __future__ import annotations

import base64
import contextlib
import struct
import zlib
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
_TITLE = "SLayer semantic layer"
_DESCRIPTION = (
    "Query databases by describing the data you want (metrics, dimensions, filters); SLayer writes and runs the SQL, "
    "joins included. Also connects datasources, builds and edits datasets, and keeps agent memories."
)
_WEBSITE = "https://docs.motley.ai/slayer/"
_DOCS_LOGO = Path(__file__).parents[1] / "docs" / "SLayer.png"
_DATA_URI_PREFIX = "data:image/png;base64,"
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_INK = (0x09, 0x1D, 0x31)


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


def _png_payload(src: str) -> bytes:
    """``src``'s decoded bytes, asserting a strict base64 PNG data URI of a 180×160 image."""
    assert src.startswith(_DATA_URI_PREFIX)
    payload = base64.b64decode(src.removeprefix(_DATA_URI_PREFIX), validate=True)
    assert payload[:8] == _PNG_SIGNATURE
    assert payload[12:16] == b"IHDR"
    assert struct.unpack(">II", payload[16:24]) == (180, 160)
    return payload


def _paeth(a: int, b: int, c: int) -> int:
    p = a + b - c
    pa, pb, pc = abs(p - a), abs(p - b), abs(p - c)
    return a if pa <= pb and pa <= pc else b if pb <= pc else c


def _rgba_pixels(png: bytes) -> list[tuple[int, ...]]:
    """The pixels of a non-interlaced 8-bit RGBA PNG."""
    width, height, depth, color_type, _, _, interlace = struct.unpack(">IIBBBBB", png[16:29])
    assert (depth, color_type, interlace) == (8, 6, 0)
    idat, pos = b"", 8
    while pos < len(png):
        (length,), kind = struct.unpack(">I", png[pos:pos + 4]), png[pos + 4:pos + 8]
        if kind == b"IDAT":
            idat += png[pos + 8:pos + 8 + length]
        pos += 12 + length
    raw, stride, prev, pixels = zlib.decompress(idat), width * 4, bytearray(width * 4), []
    for y in range(height):
        line = raw[y * (stride + 1):(y + 1) * (stride + 1)]
        kind, row = line[0], bytearray(line[1:])
        for i in range(stride):
            left, up, corner = (row[i - 4] if i >= 4 else 0), prev[i], (prev[i - 4] if i >= 4 else 0)
            row[i] = (row[i] + (0, left, up, (left + up) // 2, _paeth(left, up, corner))[kind]) & 0xFF
        pixels.extend(tuple(row[i:i + 4]) for i in range(0, stride, 4))
        prev = row
    return pixels


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

    async def test_initialize_reports_title_description_and_website(self, tmp_path: Path) -> None:
        async with _client(tmp_path) as client:
            info = client.server_info
        assert info is not None
        assert (info.title, info.description, info.website_url) == (_TITLE, _DESCRIPTION, _WEBSITE)

    async def test_initialize_reports_themed_png_icons(self, tmp_path: Path) -> None:
        async with _client(tmp_path) as client:
            info = client.server_info
        assert info is not None and info.icons is not None
        assert [icon.theme for icon in info.icons] == ["light", "dark"]
        payloads = []
        for icon in info.icons:
            assert (icon.mime_type, icon.sizes) == ("image/png", ["180x160"])
            payloads.append(_png_payload(icon.src))
        assert payloads[1] == _DOCS_LOGO.read_bytes()
        light, dark = (_rgba_pixels(p) for p in payloads)
        assert [px[3] for px in light] == [px[3] for px in dark]
        assert any(px[3] for px in light)
        assert {px[:3] for px in light if px[3]} == {_INK}


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
