"""The release gate: a freshly installed wheel runs the README's queries through the CLI and MCP, or the upload stops."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import zipfile
from importlib.metadata import version
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import BaseModel

from tests._doc_examples import load_page

REPO_ROOT = Path(__file__).resolve().parent.parent
DEV_SLAYER = Path(sys.executable).parent / "slayer"
WORKFLOWS = ("publish.yml", "publish-test.yml")
HEADLINE = {
    "source_model": "orders",
    "measures": ["sum(order_total)", "change_pct(sum(order_total))"],
    "dimensions": ["stores.name", "month(ordered_at)"],
}

_FAKE_SLAYER = '''#!{interpreter}
import json, os, sys, time
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

B = json.loads({behaviour!r})
ROWS = [{{"orders.stores.name": "Brooklyn", "orders.order_total_sum": 1.0}}]
args = sys.argv[1:]
record = Path(B["record_dir"])
n = len(list(record.glob("*.json")))
(record / f"{{n:03d}}.json").write_text(json.dumps({{"argv": args, "env": dict(os.environ), "cwd": os.getcwd()}}))


def record_query(leg, query):
    (record / f"{{leg}}-query-{{time.time_ns()}}.out").write_text(json.dumps(query))


def mcp_server():
    if B["mcp"] == "hang":
        time.sleep(3600)
    server = FastMCP("fake-slayer")

    @server.tool()
    def query(query: Any, variables: Any = None, refine: Any = None, show_sql: bool = False,
              dry_run: bool = False, explain: bool = False, format: str = "markdown") -> Any:
        record_query("mcp", query)
        if B["mcp"] == "is_error":
            raise ValueError("fake failure")
        if B["mcp"] == "query_hang":
            time.sleep(3600)
        return {{
            "ok": json.dumps(ROWS),
            "inspect_hang": json.dumps(ROWS),
            "envelope_rows": json.dumps({{"data": ROWS, "warnings": [{{"type": "truncated"}}]}}),
            "non_json": "| a |\\n|---|\\n| 1 |",
            "empty": "[]",
            "envelope_empty": json.dumps({{"data": [], "warnings": []}}),
            "two_items": [json.dumps(ROWS), json.dumps(ROWS)],
            "no_items": [],
        }}[B["mcp"]]

    @server.tool()
    def inspect(entity_type: str, reference: Any = None, compact: bool = True, format: str = "markdown",
                num_rows: int = 3, show_sql: bool = False, sections: Any = None,
                descriptions_max_chars: Any = None) -> str:
        if B["mcp"] == "inspect_hang":
            time.sleep(3600)
        return json.dumps({{"name": "orders", "entity_type": "model", "joins_to": B["mcp_joins"]}})

    server.run()


def main():
    if "--version" in args:
        print(f"slayer {{B['version']}}")
        return 0
    command = next(a for a in args if not a.startswith("-"))
    if command == "datasources":
        if B["ingest"] == "error":
            print("Error: jafgen failed with exit code 1", file=sys.stderr)
            return 1
        print("Ingested: orders (9 columns, 8 measures)")
    elif command == "query":
        literal = next(a for a in reversed(args) if a.startswith(("{{", "[", "@")))
        record_query("cli", json.loads(Path(literal[1:]).read_text() if literal.startswith("@") else literal))
        if B["cli_query"] == "error":
            print("Error: Unknown column 'order_total'", file=sys.stderr)
            return 1
        print({{"rows": json.dumps(ROWS), "empty": "[]", "non_json": "| a |\\n|---|\\n| 1 |",
               "object": json.dumps({{"rows": 1}})}}[B["cli_query"]])
    elif command == "inspect":
        print(json.dumps({{"name": "orders", "entity_type": "model", "joins_to": B["cli_joins"]}}))
    elif command == "mcp":
        mcp_server()
    return 0


sys.exit(main())
'''


class Behaviour(BaseModel):
    """What the fake ``slayer`` does; the defaults pass every check."""

    version: str = "9.9.9"
    ingest: str = "ok"
    cli_query: str = "rows"
    cli_joins: list[str] = ["customers", "stores"]
    mcp: str = "ok"
    mcp_joins: list[str] = ["customers", "stores"]


class Invocation(BaseModel):
    argv: list[str]
    env: dict[str, str]
    cwd: str


def _wheel(directory: Path, wheel_version: str) -> Path:
    """A stand-in wheel carrying only its metadata."""
    path = directory / f"motley_slayer-{wheel_version}-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(
            f"motley_slayer-{wheel_version}.dist-info/METADATA",
            f"Metadata-Version: 2.1\nName: motley-slayer\nVersion: {wheel_version}\n",
        )
    return path


def _fake_slayer(directory: Path, behaviour: Behaviour, *, interpreter: str = sys.executable) -> Path:
    record_dir = directory / "record"
    record_dir.mkdir()
    path = directory / "bin" / "slayer"
    path.parent.mkdir()
    payload = json.dumps({**behaviour.model_dump(), "record_dir": str(record_dir)})
    path.write_text(_FAKE_SLAYER.format(interpreter=interpreter, behaviour=payload), encoding="utf-8")
    path.chmod(0o755)
    return path


def _invocations(directory: Path) -> list[Invocation]:
    return [Invocation.model_validate_json(p.read_text()) for p in sorted((directory / "record").glob("*.json"))]


def _queries(directory: Path, leg: str) -> list[Any]:
    return [json.loads(p.read_text()) for p in sorted((directory / "record").glob(f"{leg}-query-*.out"))]


def _gate(
    *,
    slayer: Path,
    wheel: Path,
    checkout: Path | None,
    extra: tuple[str, ...] = (),
    cwd: Path = REPO_ROOT,
    env: dict[str, str] | None = None,
    timeout: float = 300,
) -> subprocess.CompletedProcess[str]:
    argv = [sys.executable, "-m", "scripts.release_gate", "--slayer", str(slayer), "--wheel", str(wheel), *extra]
    if checkout is not None:
        argv += ["--checkout", str(checkout)]
    return subprocess.run(argv, cwd=cwd, env=env, capture_output=True, text=True, timeout=timeout, check=False)


def _failed_checks(result: subprocess.CompletedProcess[str]) -> list[str]:
    """The check named by each ``release gate: <check>: <reason>`` line; every reason must be non-empty."""
    lines = [line for line in (result.stdout + result.stderr).splitlines() if line.startswith("release gate: ")]
    parsed = [line.removeprefix("release gate: ").partition(": ") for line in lines]
    assert all(sep and reason.strip() for _, sep, reason in parsed), lines
    return [check for check, _, _ in parsed]


def _storage(inv: Invocation) -> str | None:
    """Where an invocation keeps its storage: ``--storage`` when given, else under ``HOME``."""
    return inv.argv[inv.argv.index("--storage") + 1] if "--storage" in inv.argv else inv.env.get("HOME")


@pytest.fixture
def checkout(tmp_path: Path) -> Path:
    """A stand-in checkout root that the dev env's package and import path lie outside of."""
    path = tmp_path / "checkout"
    path.mkdir()
    return path


# ---------------------------------------------------------------------------
# The passing path
# ---------------------------------------------------------------------------


def test_real_gate_passes_against_the_dev_install(tmp_path: Path, checkout: Path) -> None:
    result = _gate(slayer=DEV_SLAYER, wheel=_wheel(tmp_path, version("motley-slayer")), checkout=checkout, timeout=900)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _failed_checks(result) == []


@pytest.mark.parametrize("mcp_response", ["ok", "envelope_rows"])
def test_passing_fake_install_runs_every_leg_with_every_readme_query(
    tmp_path: Path, checkout: Path, mcp_response: str,
) -> None:
    slayer = _fake_slayer(tmp_path, Behaviour(mcp=mcp_response))
    result = _gate(slayer=slayer, wheel=_wheel(tmp_path, "9.9.9"), checkout=checkout)
    assert result.returncode == 0, result.stdout + result.stderr
    calls = [i.argv for i in _invocations(tmp_path)]
    create = next(n for n, a in enumerate(calls) if a[:1] == ["datasources"] and {"create", "demo", "--ingest"} <= set(a))
    assert any(a[:1] == ["query"] for a in calls[create + 1:])
    assert any(a[:1] == ["inspect"] and "orders" in a for a in calls)
    assert any(a[:1] == ["mcp"] and "--demo" in a for a in calls)
    readme = [q.query for q in load_page("README.md").checked_queries()]
    assert HEADLINE in readme
    for leg in ("cli", "mcp"):
        recorded = _queries(tmp_path, leg)
        assert all(q in recorded for q in readme), leg


def test_long_path_sh_trampoline_entry_point_passes(tmp_path: Path, checkout: Path) -> None:
    slayer = _fake_slayer(tmp_path, Behaviour())
    body = slayer.read_text(encoding="utf-8").split("\n", 1)[1]
    slayer.write_text(f"#!/bin/sh\n'''exec' '{sys.executable}' \"$0\" \"$@\"\n' '''\n{body}", encoding="utf-8")
    result = _gate(slayer=slayer, wheel=_wheel(tmp_path, "9.9.9"), checkout=checkout)
    assert result.returncode == 0, result.stdout + result.stderr


def test_cli_and_mcp_legs_use_separate_storage(tmp_path: Path, checkout: Path) -> None:
    slayer = _fake_slayer(tmp_path, Behaviour())
    result = _gate(slayer=slayer, wheel=_wheel(tmp_path, "9.9.9"), checkout=checkout)
    assert result.returncode == 0, result.stdout + result.stderr
    invocations = _invocations(tmp_path)
    cli = {_storage(i) for i in invocations if i.argv[:1] in (["datasources"], ["query"], ["inspect"])}
    mcp = {_storage(i) for i in invocations if i.argv[:1] == ["mcp"]}
    assert len(cli) == 1
    assert len(mcp) == 1
    assert cli.isdisjoint(mcp)


def test_child_processes_get_a_clean_environment(tmp_path: Path, checkout: Path) -> None:
    slayer = _fake_slayer(tmp_path, Behaviour())
    venv_bin = str(Path(sys.executable).parent)
    sentinel = tmp_path / "sentinel-bin"
    sentinel.mkdir()
    env = {k: v for k, v in os.environ.items() if k not in ("SLAYER_USAGE_REPORTING", "DO_NOT_TRACK")}
    env.update(VIRTUAL_ENV=str(Path(sys.executable).parent.parent), PYTHONPATH=str(tmp_path),
               PATH=os.pathsep.join([str(sentinel), venv_bin, env.get("PATH", "")]))
    result = _gate(slayer=slayer, wheel=_wheel(tmp_path, "9.9.9"), checkout=checkout, env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    invocations = _invocations(tmp_path)
    assert invocations
    for inv in invocations:
        assert inv.env.get("SLAYER_USAGE_REPORTING") == "off"
        assert inv.env.get("DO_NOT_TRACK") == "1"
        assert "VIRTUAL_ENV" not in inv.env
        assert "PYTHONPATH" not in inv.env
        path = inv.env.get("PATH", "").split(os.pathsep)
        assert venv_bin not in path
        assert str(sentinel) not in path
        assert inv.env.get("HOME") != os.environ.get("HOME")
        assert not Path(inv.cwd).resolve().is_relative_to(REPO_ROOT)


# ---------------------------------------------------------------------------
# Failure paths: each stops the gate and names its check
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("behaviour", "check"),
    [
        pytest.param(Behaviour(ingest="error"), "cli demo", id="cli-demo-ingest-error"),
        pytest.param(Behaviour(cli_query="error"), "cli query", id="cli-query-error"),
        pytest.param(Behaviour(cli_query="empty"), "cli query", id="cli-empty-rows"),
        pytest.param(Behaviour(cli_query="non_json"), "cli query", id="cli-non-json"),
        pytest.param(Behaviour(cli_query="object"), "cli query", id="cli-not-a-row-list"),
        pytest.param(Behaviour(cli_joins=["customers"]), "cli joins", id="cli-joins-missing-stores"),
        pytest.param(Behaviour(mcp="is_error"), "mcp query", id="mcp-is-error"),
        pytest.param(Behaviour(mcp="non_json"), "mcp query", id="mcp-non-json"),
        pytest.param(Behaviour(mcp="empty"), "mcp query", id="mcp-empty-list"),
        pytest.param(Behaviour(mcp="envelope_empty"), "mcp query", id="mcp-empty-envelope"),
        pytest.param(Behaviour(mcp="two_items"), "mcp query", id="mcp-two-content-items"),
        pytest.param(Behaviour(mcp="no_items"), "mcp query", id="mcp-no-content-items"),
        pytest.param(Behaviour(mcp_joins=["customers"]), "mcp joins", id="mcp-joins-missing-stores"),
        pytest.param(Behaviour(version="0.0.1"), "provenance", id="version-mismatch"),
        pytest.param(Behaviour(version="9.9.9 (editable)"), "provenance", id="version-line-not-exact"),
    ],
)
def test_failing_check_stops_the_gate(tmp_path: Path, checkout: Path, behaviour: Behaviour, check: str) -> None:
    result = _gate(slayer=_fake_slayer(tmp_path, behaviour), wheel=_wheel(tmp_path, "9.9.9"), checkout=checkout)
    assert result.returncode != 0
    assert check in _failed_checks(result), result.stdout + result.stderr


def test_mcp_start_timeout_stops_the_gate(tmp_path: Path, checkout: Path) -> None:
    slayer = _fake_slayer(tmp_path, Behaviour(mcp="hang"))
    result = _gate(slayer=slayer, wheel=_wheel(tmp_path, "9.9.9"), checkout=checkout,
                   extra=("--mcp-start-timeout", "3"), timeout=120)
    assert result.returncode != 0
    assert "mcp start" in _failed_checks(result), result.stdout + result.stderr


@pytest.mark.parametrize(("hang", "check"), [("query_hang", "mcp query"), ("inspect_hang", "mcp joins")])
def test_mcp_call_timeout_stops_the_gate(tmp_path: Path, checkout: Path, hang: str, check: str) -> None:
    slayer = _fake_slayer(tmp_path, Behaviour(mcp=hang))
    result = _gate(slayer=slayer, wheel=_wheel(tmp_path, "9.9.9"), checkout=checkout,
                   extra=("--mcp-call-timeout", "3"), timeout=120)
    assert result.returncode != 0
    assert check in _failed_checks(result), result.stdout + result.stderr


def test_package_inside_the_checkout_fails_provenance(tmp_path: Path) -> None:
    # The dev install is editable, so with the default checkout its package resolves inside it.
    result = _gate(slayer=_fake_slayer(tmp_path, Behaviour()), wheel=_wheel(tmp_path, "9.9.9"), checkout=None)
    assert result.returncode != 0
    assert "provenance" in _failed_checks(result), result.stdout + result.stderr


def test_import_path_inside_the_checkout_fails_provenance(tmp_path: Path, checkout: Path) -> None:
    (checkout / "lib").mkdir()
    wrapper = tmp_path / "python-wrapper"
    wrapper.write_text(f'#!/bin/sh\nPYTHONPATH="{checkout / "lib"}" exec "{sys.executable}" "$@"\n')
    wrapper.chmod(0o755)
    slayer = _fake_slayer(tmp_path, Behaviour(), interpreter=str(wrapper))
    result = _gate(slayer=slayer, wheel=_wheel(tmp_path, "9.9.9"), checkout=checkout)
    assert result.returncode != 0
    assert "provenance" in _failed_checks(result), result.stdout + result.stderr


def test_readme_without_a_query_fails_the_gate(tmp_path: Path, checkout: Path) -> None:
    repo = tmp_path / "repo"
    shutil.copytree(REPO_ROOT / "scripts", repo / "scripts", ignore=shutil.ignore_patterns("__pycache__"))
    (repo / "README.md").write_text("# SLayer\n\n![A diagram](x.png)\n", encoding="utf-8")
    slayer = _fake_slayer(tmp_path, Behaviour())
    result = _gate(slayer=slayer, wheel=_wheel(tmp_path, "9.9.9"), checkout=checkout, cwd=repo)
    assert result.returncode != 0
    assert "readme" in _failed_checks(result), result.stdout + result.stderr


# ---------------------------------------------------------------------------
# Workflow wiring
# ---------------------------------------------------------------------------


def _steps(workflow: str) -> list[dict[str, Any]]:
    return yaml.safe_load((REPO_ROOT / ".github" / "workflows" / workflow).read_text())["jobs"]["publish"]["steps"]


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_wheel_gate_runs_between_build_and_upload(workflow: str) -> None:
    steps = _steps(workflow)
    uses = [s.get("uses", "") for s in steps]
    build = [s.get("name") for s in steps].index("Build package")
    gate = uses.index("./.github/actions/wheel-gate")
    upload = next(n for n, u in enumerate(uses) if u.startswith("pypa/gh-action-pypi-publish"))
    assert build < gate < upload


def test_wheel_gate_action_installs_with_uv_and_runs_the_gate() -> None:
    text = (REPO_ROOT / ".github" / "actions" / "wheel-gate" / "action.yml").read_text()
    action = yaml.safe_load(text)
    assert action["runs"]["using"] == "composite"
    steps = action["runs"]["steps"]
    assert any(s.get("uses", "").startswith("astral-sh/setup-uv") for s in steps)
    runs = "\n".join(s.get("run", "") for s in steps)
    assert "uv tool install" in runs
    assert "motley-slayer[all] @ file://" in runs
    assert "UV_TOOL_DIR" in text
    assert "UV_TOOL_BIN_DIR" in text
    gate = runs[runs.index("python -m scripts.release_gate"):]
    assert "--slayer" in gate
    assert "--wheel" in gate
