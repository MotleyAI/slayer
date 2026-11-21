"""Spec: models/semantic-model-documents › SLayer's round trip grades portable on upstream's ladder; cli/commands ›
ossie-roundtrip imports and exports one document.

Upstream's vendored portability grader runs through ``uv run`` from a copy; the corpus root is
``$SLAYER_OSSIE_CORPUS``, defaulting to the vendored snapshot.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from tests._cli_inprocess import run_cli_in_process
from tests._dev2243_fixtures import CORPUS

ROOT = Path(os.environ.get("SLAYER_OSSIE_CORPUS") or CORPUS)
PORTABILITY = ROOT / "compliance" / "portability"
RUNGS = sorted(PORTABILITY.glob("m[0-8]/*.ossie.yaml"))
IDS = [p.relative_to(PORTABILITY).as_posix() for p in RUNGS]
#: The grader's own optional dependency set (sqlglot tells a cosmetic rewrite from a change), as validate.py pins it.
GRADER_DEPS = ("pyyaml>=6.0.3", "sqlglot>=30.19.0")


@pytest.fixture(scope="module")
def grader(tmp_path_factory: pytest.TempPathFactory) -> list[str]:
    """The command running a copy of upstream's ``grade.py`` through uv; skips without uv."""
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is not on PATH; upstream's portability grader runs through `uv run`")
    copy = tmp_path_factory.mktemp("ossie_grader") / "grader"
    shutil.copytree(PORTABILITY / "grader", copy)
    return [uv, "run", "--no-project", *(arg for dep in GRADER_DEPS for arg in ("--with", dep)), "python",
            str(copy / "grade.py")]


def grade(grader: list[str], *, original: Path, returned: Path) -> dict[str, Any]:
    run = subprocess.run([*grader, "--input", str(original), "--returned", str(returned), "--json"],
                         capture_output=True, text=True, check=False, timeout=300)
    assert run.returncode == 0, run.stderr
    return json.loads(run.stdout)


def test_the_ladder_lists_every_rung_document() -> None:
    manifest = yaml.safe_load((PORTABILITY / "manifest.yaml").read_text(encoding="utf-8"))
    listed = {variant["file"] for rung in manifest["rungs"] for variant in rung["variants"]}
    assert listed == set(IDS)
    assert {p.split("/")[0] for p in IDS} == {f"m{i}" for i in range(9)}


def test_the_grader_grades_an_identity_round_trip_portable(grader: list[str]) -> None:
    """The oracle itself: a document returned unchanged grades every element PORTABLE."""
    report = grade(grader, original=RUNGS[0], returned=RUNGS[0])
    assert report["findings"]
    assert {f["outcome"] for f in report["findings"]} == {"PORTABLE"}


@pytest.mark.parametrize("rung", RUNGS, ids=IDS)
def test_the_ladder_grades_portable(rung: Path, tmp_path: Path, grader: list[str]) -> None:
    """The ladder grades portable."""
    store = tmp_path / "store"
    returned = tmp_path / "returned" / rung.name
    returned.parent.mkdir()
    result = run_cli_in_process(["ossie-roundtrip", str(rung), str(returned)],
                                env={**os.environ, "SLAYER_STORAGE": str(store)})
    assert result.returncode == 0, result.stderr
    report = grade(grader, original=rung, returned=returned)
    assert report["rejected"] is False
    assert report["findings"]
    assert [f for f in report["findings"] if f["outcome"] != "PORTABLE"] == []
