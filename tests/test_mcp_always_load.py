"""The opt-in ``anthropic/alwaysLoad`` marking of the ``query`` tool, on both servers and every entry point."""

from __future__ import annotations

import argparse
import sys
import types
from typing import Any

import pytest

import slayer.api.server as api_server
import slayer.cli as cli
from slayer.api.server import create_app
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage

_ALWAYS_LOAD = "anthropic/alwaysLoad"
_ENV = "SLAYER_MCP_ALWAYS_LOAD_QUERY"


async def _marked(server: Any) -> dict[str, Any]:
    return {t.name: (t.meta or {}).get(_ALWAYS_LOAD) for t in await server.list_tools() if (t.meta or {}).get(_ALWAYS_LOAD)}


async def test_off_by_default(tmp_path) -> None:
    server = create_mcp_server(storage=YAMLStorage(base_dir=str(tmp_path)), _seed_help=False)
    assert await _marked(server) == {}


async def test_marks_only_query(tmp_path) -> None:
    server = create_mcp_server(storage=YAMLStorage(base_dir=str(tmp_path)), _seed_help=False, always_load_query=True)
    assert await _marked(server) == {"query": True}


async def test_create_app_marks_the_embedded_servers_query(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    built: list[Any] = []

    def _spy(*args: Any, **kwargs: Any) -> Any:
        server = create_mcp_server(*args, **kwargs)
        built.append(server)
        return server

    monkeypatch.setattr(api_server, "create_mcp_server", _spy)
    create_app(storage=YAMLStorage(base_dir=str(tmp_path)), always_load_query=True)
    assert len(built) == 1
    assert await _marked(built[0]) == {"query": True}


async def test_create_app_default_leaves_the_embedded_server_unmarked(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    built: list[Any] = []

    def _spy(*args: Any, **kwargs: Any) -> Any:
        server = create_mcp_server(*args, **kwargs)
        built.append(server)
        return server

    monkeypatch.setattr(api_server, "create_mcp_server", _spy)
    create_app(storage=YAMLStorage(base_dir=str(tmp_path)))
    assert await _marked(built[0]) == {}


# --------------------------------------------------------------------------- #
# CLI flag + env var
# --------------------------------------------------------------------------- #
def _args(**overrides: Any) -> argparse.Namespace:
    base = {
        "storage": None, "models_dir": None, "demo": False, "ingest_on_startup": False,
        "always_load_query": False, "host": "127.0.0.1", "port": 5143,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def _patch_entry_points(monkeypatch: pytest.MonkeyPatch, capture: list) -> None:
    monkeypatch.setattr(cli, "_resolve_storage", lambda *a, **kw: "STORAGE")

    class _FakeMCP:
        def run(self) -> None:
            pass

    fake_mcp = types.ModuleType("slayer.mcp.server")
    fake_mcp.create_mcp_server = lambda *a, **kw: capture.append(("mcp", kw)) or _FakeMCP()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "slayer.mcp.server", fake_mcp)
    fake_api = types.ModuleType("slayer.api.server")
    fake_api.create_app = lambda *a, **kw: capture.append(("app", kw)) or "APP"  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "slayer.api.server", fake_api)
    fake_uvicorn = types.ModuleType("uvicorn")
    fake_uvicorn.run = lambda *a, **kw: None  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "uvicorn", fake_uvicorn)


@pytest.mark.parametrize("command", ["serve", "mcp"])
def test_flag_parses(monkeypatch: pytest.MonkeyPatch, command: str) -> None:
    captured: list = []
    monkeypatch.setattr(cli, f"_run_{command}", captured.append)
    monkeypatch.setattr(sys, "argv", ["slayer", command, "--always-load-query"])
    cli.main()
    assert captured[0].always_load_query is True


@pytest.mark.parametrize("command", ["serve", "mcp"])
def test_flag_defaults_off(monkeypatch: pytest.MonkeyPatch, command: str) -> None:
    captured: list = []
    monkeypatch.setattr(cli, f"_run_{command}", captured.append)
    monkeypatch.setattr(sys, "argv", ["slayer", command])
    cli.main()
    assert captured[0].always_load_query is False


@pytest.mark.parametrize(("command", "target"), [("serve", "app"), ("mcp", "mcp")])
@pytest.mark.parametrize(("flag", "env", "expected"), [
    (True, None, True),
    (False, "1", True),
    (False, "0", False),
    (False, None, False),
])
def test_flag_or_env_reaches_the_server_factory(
    monkeypatch: pytest.MonkeyPatch, command: str, target: str, flag: bool, env: str | None, expected: bool,
) -> None:
    capture: list = []
    _patch_entry_points(monkeypatch, capture)
    if env is None:
        monkeypatch.delenv(_ENV, raising=False)
    else:
        monkeypatch.setenv(_ENV, env)
    getattr(cli, f"_run_{command}")(_args(always_load_query=flag))
    kwargs = next(kw for kind, kw in capture if kind == target)
    assert kwargs.get("always_load_query", False) is expected
