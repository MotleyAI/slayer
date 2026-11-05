"""Release gate: a freshly installed ``slayer`` runs the README's queries through its CLI and MCP server.

Run from the repo root: ``python -m scripts.release_gate --slayer <bin> --wheel <path>``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shlex
import subprocess
import sys
import tempfile
import zipfile
from datetime import timedelta
from email.parser import HeaderParser
from pathlib import Path
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from pydantic import BaseModel, ValidationError

from scripts.doc_examples import REPO_ROOT, DocQuery, load_page

REQUIRED_JOINS = ("customers", "stores")
CHILD_PATH = "/usr/local/bin:/usr/bin:/bin"
PASSTHROUGH_ENV = ("LANG", "LC_ALL", "TMPDIR")
COMMAND_TIMEOUT = 900.0
TRAMPOLINE = "'''exec' "
PROBE = (
    "import json, os, sys, slayer; "
    "print(json.dumps({'package': slayer.__file__, 'path': [os.path.abspath(p) for p in sys.path]}))"
)


class GateFailure(Exception):
    def __init__(self, check: str, reason: str) -> None:
        super().__init__(f"{check}: {reason}")
        self.check = check
        self.reason = reason


class ProbeResult(BaseModel):
    package: str
    path: list[str]


class Options(BaseModel):
    slayer: Path
    wheel: Path
    checkout: Path
    mcp_start_timeout: float
    mcp_call_timeout: float


def _parse_args(argv: list[str] | None) -> Options:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slayer", required=True, help="The freshly installed slayer binary.")
    parser.add_argument("--wheel", required=True, help="The built wheel it was installed from.")
    parser.add_argument("--checkout", default=str(REPO_ROOT), help="Source checkout the install must not use.")
    parser.add_argument("--mcp-start-timeout", type=float, default=300.0, help="Seconds for MCP start-up.")
    parser.add_argument("--mcp-call-timeout", type=float, default=300.0, help="Seconds per MCP tool call.")
    ns = parser.parse_args(argv)
    return Options(
        slayer=Path(ns.slayer).absolute(),
        wheel=Path(ns.wheel).absolute(),
        checkout=Path(ns.checkout).resolve(),
        mcp_start_timeout=ns.mcp_start_timeout,
        mcp_call_timeout=ns.mcp_call_timeout,
    )


def _child_env(home: Path) -> dict[str, str]:
    """A clean environment: no venv, no PYTHONPATH, its own HOME, no usage reporting."""
    env = {k: os.environ[k] for k in PASSTHROUGH_ENV if k in os.environ}
    env.update(PATH=CHILD_PATH, HOME=str(home), SLAYER_USAGE_REPORTING="off", DO_NOT_TRACK="1")
    return env


def _leg_dir(root: Path, name: str) -> Path:
    path = root / name
    path.mkdir()
    return path


def _run(argv: list[str], *, check: str, leg: Path) -> subprocess.CompletedProcess[str]:
    """Run ``argv`` in ``leg`` with a clean environment; a failure to start or finish raises ``check``."""
    try:
        return subprocess.run(
            argv, cwd=leg, env=_child_env(leg), capture_output=True, text=True, timeout=COMMAND_TIMEOUT, check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise GateFailure(check, f"{shlex.join(argv)}: {type(exc).__name__}: {exc}") from exc


def _error(result: subprocess.CompletedProcess[str]) -> str:
    lines = [line for line in (result.stderr + "\n" + result.stdout).splitlines() if line.strip()]
    return f"exit code {result.returncode}: {lines[-1].strip() if lines else 'no output'}"


def _wheel_version(wheel: Path) -> str:
    try:
        with zipfile.ZipFile(wheel) as zf:
            metadata = [n for n in zf.namelist() if n.count("/") == 1 and n.endswith(".dist-info/METADATA")]
            if len(metadata) != 1:
                raise GateFailure("provenance", f"{wheel.name} has {len(metadata)} dist-info METADATA files")
            version = HeaderParser().parsestr(zf.read(metadata[0]).decode("utf-8"))["Version"]
    except (OSError, zipfile.BadZipFile) as exc:
        raise GateFailure("provenance", f"cannot read {wheel}: {exc}") from exc
    if not version:
        raise GateFailure("provenance", f"{wheel.name} metadata has no Version")
    return version


def _interpreter(slayer: Path) -> list[str]:
    """The entry point's interpreter: its shebang, or the target of the ``/bin/sh`` exec trampoline."""
    try:
        lines = slayer.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        raise GateFailure("provenance", f"cannot read {slayer}: {exc}") from exc
    if not lines or not lines[0].startswith("#!"):
        raise GateFailure("provenance", f"{slayer} has no shebang")
    if lines[0] == "#!/bin/sh" and len(lines) > 1 and lines[1].startswith(TRAMPOLINE):
        return shlex.split(lines[1])[1:2]
    return shlex.split(lines[0][2:])


def check_provenance(opts: Options, *, leg: Path) -> None:
    """The install reports the wheel's version and imports nothing from the checkout."""
    expected = f"slayer {_wheel_version(opts.wheel)}"
    result = _run([str(opts.slayer), "--version"], check="provenance", leg=leg)
    reported = result.stdout.strip()
    if result.returncode != 0 or reported != expected:
        raise GateFailure("provenance", f"slayer --version printed {reported!r}, expected {expected!r}")
    probe = _run([*_interpreter(opts.slayer), "-c", PROBE], check="provenance", leg=leg)
    if probe.returncode != 0:
        raise GateFailure("provenance", f"the install's interpreter cannot import slayer: {_error(probe)}")
    try:
        found = ProbeResult.model_validate_json(probe.stdout)
    except ValidationError as exc:
        raise GateFailure("provenance", f"unreadable probe output {probe.stdout[:200]!r}: {exc}") from exc
    for label, entry in [("package", found.package), *(("import path entry", p) for p in found.path)]:
        try:
            inside = Path(entry).resolve().is_relative_to(opts.checkout)
        except (OSError, ValueError) as exc:
            raise GateFailure("provenance", f"{label} {entry!r} is not a usable path: {exc}") from exc
        if inside:
            raise GateFailure("provenance", f"{label} {entry} lies inside the checkout {opts.checkout}")


def readme_queries() -> list[DocQuery]:
    queries = load_page("README.md").checked_queries()
    if not queries:
        raise GateFailure("readme", "README.md yields no checked query")
    return queries


def rows_failure(text: str) -> str | None:
    """Why ``text`` is not a non-empty JSON row list, bare or in the ``data`` envelope; ``None`` when it is."""
    try:
        payload: Any = json.loads(text)
    except ValueError:
        return f"response is not JSON: {text[:200]!r}"
    rows = payload.get("data") if isinstance(payload, dict) else payload
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        return f"response is not a JSON row list: {text[:200]!r}"
    return None if rows else "returned no rows"


def joins_failure(text: str) -> str | None:
    """Why ``text`` is not a model whose ``joins_to`` has every required join; ``None`` when it is."""
    try:
        payload: Any = json.loads(text)
    except ValueError:
        return f"response is not JSON: {text[:200]!r}"
    joins = payload.get("joins_to") if isinstance(payload, dict) else None
    if not isinstance(joins, list):
        return f"response has no joins_to list: {text[:200]!r}"
    missing = [j for j in REQUIRED_JOINS if j not in joins]
    return f"orders does not join to {', '.join(missing)}" if missing else None


def cli_leg(opts: Options, queries: list[DocQuery], *, leg: Path) -> list[GateFailure]:
    slayer = str(opts.slayer)
    try:
        demo = _run([slayer, "datasources", "create", "demo", "--ingest"], check="cli demo", leg=leg)
        if demo.returncode != 0:
            return [GateFailure("cli demo", _error(demo))]
    except GateFailure as failure:
        return [failure]
    failures: list[GateFailure] = []
    for q in queries:
        try:
            result = _run([slayer, "query", json.dumps(q.query), "--format", "json"], check="cli query", leg=leg)
        except GateFailure as failure:
            failures.append(failure)
            continue
        reason = _error(result) if result.returncode != 0 else rows_failure(result.stdout)
        if reason is not None:
            failures.append(GateFailure("cli query", f"README.md:{q.line}: {reason}"))
    try:
        result = _run([slayer, "inspect", "orders", "--type", "model", "--format", "json"], check="cli joins", leg=leg)
    except GateFailure as failure:
        return [*failures, failure]
    reason = _error(result) if result.returncode != 0 else joins_failure(result.stdout)
    if reason is not None:
        failures.append(GateFailure("cli joins", reason))
    return failures


def _tool_text(result: Any) -> str:
    """The single text content item of a successful tool result."""
    texts = [c.text for c in result.content if getattr(c, "type", None) == "text"]
    if result.isError:
        raise ValueError(f"tool error: {texts[0] if texts else 'no detail'}")
    if len(result.content) != 1 or len(texts) != 1:
        raise ValueError(f"expected one text content item, got {len(result.content)} item(s)")
    return texts[0]


async def _call(session: ClientSession, opts: Options, *, tool: str, arguments: dict[str, Any]) -> str:
    result = await session.call_tool(tool, arguments, read_timeout_seconds=timedelta(seconds=opts.mcp_call_timeout))
    return _tool_text(result)


async def mcp_leg(opts: Options, queries: list[DocQuery], *, leg: Path) -> list[GateFailure]:
    params = StdioServerParameters(command=str(opts.slayer), args=["mcp", "--demo"], env=_child_env(leg), cwd=leg)
    failures: list[GateFailure] = []
    started = False
    try:
        async with stdio_client(params) as (read, write), ClientSession(
            read, write, read_timeout_seconds=timedelta(seconds=opts.mcp_start_timeout),
        ) as session:
            await session.initialize()
            started = True
            await _mcp_checks(session, opts, queries, failures=failures)
    except Exception as exc:  # noqa: BLE001 — any server lifecycle failure stops the leg
        failures.append(GateFailure("mcp transport" if started else "mcp start", f"{type(exc).__name__}: {exc}"))
    return failures


async def _mcp_checks(
    session: ClientSession, opts: Options, queries: list[DocQuery], *, failures: list[GateFailure],
) -> None:
    for q in queries:
        try:
            text = await _call(session, opts, tool="query", arguments={"query": q.query, "format": "json"})
            reason = rows_failure(text)
        except Exception as exc:  # noqa: BLE001 — any call failure is the check's failure
            reason = f"{type(exc).__name__}: {exc}"
        if reason is not None:
            failures.append(GateFailure("mcp query", f"README.md:{q.line}: {reason}"))
    try:
        text = await _call(session, opts, tool="inspect",
                           arguments={"entity_type": "model", "reference": "orders", "format": "json"})
        reason = joins_failure(text)
    except Exception as exc:  # noqa: BLE001
        reason = f"{type(exc).__name__}: {exc}"
    if reason is not None:
        failures.append(GateFailure("mcp joins", reason))


def run_gate(opts: Options, *, workdir: Path) -> list[GateFailure]:
    try:
        check_provenance(opts, leg=_leg_dir(workdir, "provenance"))
        queries = readme_queries()
    except GateFailure as failure:
        return [failure]
    return [
        *cli_leg(opts, queries, leg=_leg_dir(workdir, "cli")),
        *asyncio.run(mcp_leg(opts, queries, leg=_leg_dir(workdir, "mcp"))),
    ]


def main(argv: list[str] | None = None) -> int:
    opts = _parse_args(argv)
    with tempfile.TemporaryDirectory(prefix="slayer-release-gate-") as tmp:
        failures = run_gate(opts, workdir=Path(tmp))
    for failure in failures:
        print(f"release gate: {failure.check}: {failure.reason}", file=sys.stderr)
    if failures:
        return 1
    print(f"Release gate passed for {opts.wheel.name}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
