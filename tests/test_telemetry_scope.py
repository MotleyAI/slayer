"""Telemetry is active only in processes started by the slayer CLI."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import pytest

from slayer import telemetry
from slayer.telemetry import settings
from tests._telemetry_helpers import (
    SUBPROCESS_TIMEOUT,
    build_storage,
    cli,
    quiet_config,
    run_python,
    run_slayer,
    show,
    usage,
)

_NOTICE_URL = "docs.motley.ai/slayer/reference/telemetry"
_QUERY = {"source_model": "orders", "measures": ["sum(amount)"]}

_LIBRARY_SCRIPT = """
import asyncio
from fastapi.testclient import TestClient
from slayer.api.server import create_app
from slayer.client.slayer_client import SlayerClient
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage

query = {query!r}
storage = YAMLStorage(base_dir={store!r})
SlayerQueryEngine(storage=storage).execute_sync(query=query)
client = TestClient(create_app(storage=storage))
assert client.get("/health").status_code == 200
assert client.post("/query", json=query).status_code == 200
asyncio.run(create_mcp_server(storage=storage).call_tool("query", {{"query": query}}))
SlayerClient(storage=storage).query_sync(query)
print("done")
"""


@pytest.fixture
def store(telemetry_env) -> str:
    return build_storage(telemetry_env.root)


def test_library_use_records_nothing(store: str, telemetry_env) -> None:
    result = run_python(_LIBRARY_SCRIPT.format(query=_QUERY, store=store), env=telemetry_env.subprocess_env())
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "done"
    assert not settings.config_dir().exists()
    assert _NOTICE_URL not in result.stderr
    time.sleep(0.5)
    assert telemetry_env.capture.arrivals == 0


def test_record_without_start_is_a_no_op(telemetry_env) -> None:
    telemetry.record(surface="mcp", token="query")
    telemetry.flush()
    assert not telemetry.is_active()
    assert not settings.config_dir().exists()


def test_cli_query_records(store: str, telemetry_env) -> None:
    quiet_config()
    result = cli(["query", json.dumps(_QUERY), "--storage", store])
    assert result.returncode == 0, result.stderr
    assert usage(show(), "cli:query") == {"ok": 1, "errors": {}}


def test_python_m_slayer_records(store: str, telemetry_env) -> None:
    quiet_config()
    result = run_slayer(["models", "--storage", store, "list"], env=telemetry_env.subprocess_env())
    assert result.returncode == 0, result.stderr
    assert usage(show(), "cli:models.list")["ok"] == 1


def test_console_script_records(store: str, telemetry_env) -> None:
    script = Path(sys.executable).parent / "slayer"
    if not script.exists():
        pytest.skip("no slayer console script next to the interpreter")
    quiet_config()
    result = subprocess.run(
        [str(script), "models", "--storage", store, "list"],
        env=telemetry_env.subprocess_env(), capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    assert result.returncode == 0, result.stderr
    assert usage(show(), "cli:models.list")["ok"] == 1
