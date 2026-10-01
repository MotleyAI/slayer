"""Unawaited-coroutine law: an async call whose result is dropped is a silently skipped call.

Two gates: basedpyright's ``reportUnusedCoroutine`` over the whole repo (static, catches
untested code; a serial CI step, too memory-hungry for an xdist worker), and pyproject
``filterwarnings`` turning the runtime "never awaited" warning into a test failure.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CONFIG = REPO / "pyrightconfig.unawaited.json"
CI = REPO / ".github" / "workflows" / "ci.yml"

_UNAWAITED_TEST = """
async def save():
    return 1


def test_forgets_to_await():
    save()
"""


def test_static_gate_is_wired() -> None:
    config = json.loads(CONFIG.read_text())
    assert config["reportUnusedCoroutine"] == "error"
    assert {"slayer", "tests", "examples"} <= set(config["include"])
    run = "poetry run basedpyright -p pyrightconfig.unawaited.json --baselinefile .basedpyright/no-baseline.json"
    assert run in CI.read_text(), "the CI step running the static gate is gone"
    assert not (REPO / ".basedpyright" / "no-baseline.json").exists(), "the static gate must grandfather nothing"


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
