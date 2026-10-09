"""Shared helpers for the telemetry tests."""

from __future__ import annotations

import asyncio
import base64
import datetime
import hashlib
import json
import os
import queue
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from slayer import telemetry
from slayer.core.models import Aggregation, Column, DatasourceConfig, SlayerModel
from slayer.core.enums import DataType
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from slayer.telemetry import settings
from slayer.telemetry.payload import SCHEMA_VERSION, UsageReport
from tests._cli_inprocess import CliResult, run_cli_in_process
from tests._saved_query_refinement_fixtures import build_refine_storage, orders_model

SENT = "zqxjsentinel"
HOSTILE = "zqxj‮Ώ\U0001f608'\";--​" + "x" * 10_000
_NEEDLES = (SENT, HOSTILE)

SUBPROCESS_TIMEOUT = 120


# --- time ---------------------------------------------------------------------


def now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def wait_until(predicate: Callable[[], bool], *, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


# --- config + spool -------------------------------------------------------------


def read_config() -> settings.TelemetryConfig | None:
    path = settings.config_path()
    if not path.is_file():
        return None
    return settings.TelemetryConfig.model_validate_json(path.read_text())


def config() -> settings.TelemetryConfig:
    current = read_config()
    assert current is not None, "no telemetry config written"
    return current


def write_config(**fields: Any) -> settings.TelemetryConfig:
    current = read_config() or settings.TelemetryConfig()
    config = current.model_copy(update=fields)
    path = settings.config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(config.model_dump_json())
    return config


def quiet_config(*, last_sent: datetime.datetime | None = None) -> settings.TelemetryConfig:
    """Notice already shown, last send just now (nothing due)."""
    return write_config(
        notice_shown_schema=SCHEMA_VERSION,
        last_sent=last_sent or now_utc(),
    )


def make_due() -> settings.TelemetryConfig:
    return write_config(last_sent=now_utc() - datetime.timedelta(hours=25))


def spool_files() -> list[Path]:
    directory = settings.spool_dir()
    if not directory.is_dir():
        return []
    return sorted(p for p in directory.iterdir() if p.is_file() and not p.is_symlink())


def spool_bytes() -> bytes:
    return b"\n".join(p.read_bytes() for p in spool_files())


# --- reports ------------------------------------------------------------------------


def usage(report: dict, key: str) -> dict:
    return report.get("usage", {}).get(key, {"ok": 0, "errors": {}})


def total(entry: dict) -> int:
    return entry["ok"] + sum(entry["errors"].values())


def usage_keys(report: dict, *, surface: str) -> set[str]:
    return {k for k in report.get("usage", {}) if k.startswith(f"{surface}:")}


def _resolve(schema: dict, node: dict) -> dict:
    while "$ref" in node:
        node = schema["$defs"][node["$ref"].rsplit("/", 1)[-1]]
    return node


def enum_values(schema: dict, node: dict) -> set[str]:
    node = _resolve(schema, node)
    if "const" in node:
        return {node["const"]}
    if "enum" in node:
        return set(node["enum"])
    out: set[str] = set()
    for branch in node.get("anyOf", []) + node.get("oneOf", []):
        out |= enum_values(schema, branch)
    return out


def usage_key_vocabulary() -> set[str]:
    schema = UsageReport.model_json_schema()
    usage_node = _resolve(schema, schema["properties"]["usage"])
    return enum_values(schema, usage_node["propertyNames"])


# --- CLI ------------------------------------------------------------------------------


def cli(args: list[str], *, stdin: str = "") -> CliResult:
    """One in-process ``slayer`` run, ended like a process exit (flush, then reset)."""
    try:
        return run_cli_in_process(args, stdin=stdin, uncaught_exit=True)
    finally:
        telemetry.flush()
        telemetry.reset()


def show() -> dict:
    result = cli(["telemetry", "show"])
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def end_process() -> None:
    telemetry.flush()
    telemetry.reset()


def run_slayer(args: list[str], *, env: dict[str, str], timeout: float = SUBPROCESS_TIMEOUT,
               stdin: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "slayer", *args],
        env=env, capture_output=True, text=True, timeout=timeout, input=stdin,
    )


def run_python(code: str, *, env: dict[str, str], timeout: float = SUBPROCESS_TIMEOUT) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", code],
        env=env, capture_output=True, text=True, timeout=timeout,
    )


# --- sentinels --------------------------------------------------------------------------


def _derivatives(needle: str) -> set[str]:
    raw = needle.encode("utf-8")
    out = {needle[:8], needle[:8].lower(), needle[:8].upper()}
    for algo in ("md5", "sha1", "sha256"):
        digest = hashlib.new(algo, raw).hexdigest()
        out |= {digest[:12], digest[:12].upper()}
    out.add(base64.b64encode(raw).decode()[:12])
    return out


def assert_no_sentinel(blob: bytes | str) -> None:
    text = blob.decode("utf-8", errors="replace") if isinstance(blob, bytes) else blob
    folded = text.casefold()
    for needle in _NEEDLES:
        for derived in _derivatives(needle):
            assert derived.casefold() not in folded, f"sentinel derivative {derived!r} leaked"
    assert "zqxj" not in folded


def assert_clean_everywhere() -> None:
    """Spool, config and the pending report carry no sentinel."""
    assert_no_sentinel(spool_bytes())
    if settings.config_path().is_file():
        assert_no_sentinel(settings.config_path().read_bytes())
    assert_no_sentinel(json.dumps(show()))


# --- storage -------------------------------------------------------------------------------


def build_storage(base_dir: Path) -> str:
    """YAML storage over a seeded SQLite ``orders`` (+ custom ``dsum``) with saved queries; returns its path."""

    async def build() -> YAMLStorage:
        storage = await build_refine_storage(str(base_dir))
        model = orders_model().model_copy(update={
            "aggregations": [Aggregation(name="dsum", formula="SUM({value})")],
        })
        await storage.save_model(model)
        return storage

    asyncio.run(build())
    return str(base_dir / "store")


def build_sentinel_storage(base_dir: Path) -> str:
    """A storage whose datasource, model, column, aggregation and description carry the sentinel."""
    db_path = base_dir / f"{SENT}.db"

    async def build() -> None:
        storage = YAMLStorage(base_dir=str(base_dir / "store"))
        await storage.save_datasource(DatasourceConfig(
            name=f"{SENT}_ds", type="sqlite", database=str(db_path), description=HOSTILE,
        ))
        await storage.save_model(SlayerModel(
            name=f"{SENT}_model", data_source=f"{SENT}_ds", sql_table=f"{SENT}_table",
            description=HOSTILE,
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name=f"{SENT}_col", type=DataType.DOUBLE, description=HOSTILE),
            ],
            aggregations=[Aggregation(name=f"{SENT}_agg", formula="SUM({value})")],
        ))

    with transaction(str(db_path)) as con:
        con.execute(f"CREATE TABLE {SENT}_table (id INTEGER PRIMARY KEY, {SENT}_col REAL)")
        con.execute(f"INSERT INTO {SENT}_table VALUES (1, 2.5)")
    asyncio.run(build())
    return str(base_dir / "store")


def save_models(storage_dir: str, *, count: int) -> None:
    """Add ``count`` trivial models over the ``orders`` table of datasource ``test``."""

    async def build() -> None:
        storage = YAMLStorage(base_dir=storage_dir)
        for i in range(count):
            await storage.save_model(SlayerModel(
                name=f"extra_{i}", data_source="test", sql_table="orders",
                columns=[Column(name="id", type=DataType.INT, primary_key=True)],
            ))

    asyncio.run(build())


# --- stdio MCP subprocess ------------------------------------------------------------------------


class McpStdio:
    """``slayer mcp`` over stdio, driven with raw newline-delimited JSON-RPC."""

    def __init__(self, *, args: list[str], env: dict[str, str], argv: list[str] | None = None) -> None:
        cmd = argv or [sys.executable, "-m", "slayer", "mcp", *args]
        self.proc = subprocess.Popen(
            cmd, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self.stdout_lines: list[bytes] = []
        self.stderr = bytearray()
        self._lines: queue.Queue[bytes] = queue.Queue()
        self._next_id = 1
        threading.Thread(target=self._pump_stdout, daemon=True).start()
        threading.Thread(target=self._pump_stderr, daemon=True).start()

    def _pump_stdout(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            self.stdout_lines.append(line)
            self._lines.put(line)

    def _pump_stderr(self) -> None:
        assert self.proc.stderr is not None
        for chunk in iter(lambda: self.proc.stderr.read(1024), b""):  # type: ignore[union-attr]
            self.stderr.extend(chunk)

    def send(self, message: dict) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write((json.dumps(message) + "\n").encode())
        self.proc.stdin.flush()

    def request(self, method: str, params: dict, *, timeout: float = 60.0) -> dict:
        request_id = self._next_id
        self._next_id += 1
        self.send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            assert remaining > 0, f"no MCP response to {method}; stderr: {bytes(self.stderr)!r}"
            message = json.loads(self._lines.get(timeout=remaining))
            if message.get("id") == request_id:
                return message

    def initialize(self, *, client_name: str = "claude-code", client_version: str = "2.3.1") -> dict:
        response = self.request("initialize", {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": client_name, "version": client_version},
        })
        self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        return response

    def call_tool(self, name: str, arguments: dict) -> dict:
        return self.request("tools/call", {"name": name, "arguments": arguments})

    def close_stdin(self) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.close()

    def wait(self, *, timeout: float = 30.0) -> int:
        return self.proc.wait(timeout=timeout)

    def kill(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)


def is_posix() -> bool:
    return os.name == "posix"
