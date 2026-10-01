"""Unawaited-coroutine law: an async call whose result is dropped is a silently skipped call.

Two gates: the CI ``type-check`` job's basedpyright ratchet, whose baseline must never
hold a ``reportUnusedCoroutine`` entry (static, catches untested code), and pyproject
``filterwarnings`` turning the runtime "never awaited" warning into a test failure.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parent.parent
BASELINE = REPO / ".basedpyright" / "baseline.json"
CI = REPO / ".github" / "workflows" / "ci.yml"

_UNAWAITED_TEST = """
async def save():
    return 1


def test_forgets_to_await():
    save()
"""


def test_static_gate_is_wired() -> None:
    steps = yaml.safe_load(CI.read_text())["jobs"]["type-check"]["steps"]
    assert "poetry run basedpyright" in [step.get("run") for step in steps], "the CI type-check job is gone"
    baselined = [
        path for path, errors in json.loads(BASELINE.read_text())["files"].items()
        for e in errors if e.get("code") == "reportUnusedCoroutine"
    ]
    assert not baselined, f"unawaited coroutines must be fixed, never baselined: {baselined}"


def test_runtime_gate_fails_an_unawaited_run(tmp_path: Path) -> None:
    leaky = tmp_path / "test_unawaited_scratch.py"
    leaky.write_text(_UNAWAITED_TEST)
    proc = subprocess.run(
        [
            sys.executable, "-m", "pytest", str(leaky),
            "-c", str(REPO / "pyproject.toml"),
            "-p", "no:cacheprovider", "-p", "no:xdist", "-q",
        ],
        cwd=str(tmp_path), capture_output=True, text=True,
    )
    combined = proc.stdout + proc.stderr
    assert proc.returncode != 0, f"gate let the unawaited call pass:\n{combined}"
    assert "never awaited" in combined, combined


def test_gate_config_is_pinned() -> None:
    data = tomllib.loads((REPO / "pyproject.toml").read_text())
    filters = data["tool"]["pytest"]["ini_options"]["filterwarnings"]
    assert "error:coroutine .* was never awaited:RuntimeWarning" in filters, filters
    assert "error:Exception ignored .*coroutine:pytest.PytestUnraisableExceptionWarning" in filters, filters
