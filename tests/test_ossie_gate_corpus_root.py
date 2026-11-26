"""Spec: compatibility/ossie-compliance › One corpus root: every suite reads ``$SLAYER_OSSIE_CORPUS``."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tests._ossie_gate import CORPUS_ENV, VENDORED

REPO = Path(__file__).resolve().parent.parent
SENTINEL_SECTION = ("### Sentinel Functions (REQUIRED)\n\n| Function | Syntax | Description |\n| :---- | :---- | :---- |\n"
                    "| `SENTINEL` | `SENTINEL(x)` | A construct only the copy has |\n\n")


@pytest.fixture(scope="module")
def sentinel_corpus(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A copy of the corpus with an added REQUIRED construct, wide-table case and harvested document."""
    root = tmp_path_factory.mktemp("alternate") / "ossie"
    shutil.copytree(VENDORED, root)
    spec = root / "core-spec" / "expression_language.md"
    text = spec.read_text(encoding="utf-8")
    spec.write_text(text.replace("### Trigonometric (RECOMMENDED)", SENTINEL_SECTION + "### Trigonometric (RECOMMENDED)"),
                    encoding="utf-8")
    cases = root / "compliance" / "wide_table"
    shutil.copy(cases / "a01_shared_dimension.ost", cases / "a99_sentinel.ost")
    shutil.copy(next(iter(sorted((root / "harvested").glob("*.yaml")))), root / "harvested" / "zz_sentinel.yaml")
    rung = root / "compliance" / "portability" / "m0"
    shutil.copy(next(iter(sorted(rung.glob("*.ossie.yaml")))), rung / "zz_sentinel.ossie.yaml")
    return root


def _pytest(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, "-m", "pytest", "-p", "no:cacheprovider", *args],
                          cwd=REPO, env={**os.environ, CORPUS_ENV: str(root)}, capture_output=True, text=True,
                          check=False, timeout=300)


@pytest.mark.parametrize(("module", "sentinel"), [
    ("tests/test_dev2446_required_constructs.py", "Sentinel Functions (REQUIRED)"),
    ("tests/test_ossie_gate.py", "a99_sentinel"),
    ("tests/integration/test_integration_ossie_gate.py", "a99_sentinel"),
    ("tests/test_dev2243_corpus.py", "harvested/zz_sentinel.yaml"),
    ("tests/test_dev2243_portability_grader.py", "m0/zz_sentinel.ossie.yaml"),
])
def test_every_suite_collects_the_alternate_corpus(sentinel_corpus: Path, module: str, sentinel: str) -> None:
    run = _pytest(sentinel_corpus, "--collect-only", "-q", module)
    assert run.returncode == 0, run.stdout + run.stderr
    assert any(line.startswith(f"{module}::") and sentinel in line for line in run.stdout.splitlines())


def test_the_completeness_check_reads_the_alternate_spec(sentinel_corpus: Path) -> None:
    run = _pytest(sentinel_corpus, "-q", "tests/test_dev2446_required_constructs.py::test_the_table_covers_the_spec_both_ways")
    assert run.returncode == 1, run.stdout + run.stderr
    assert "SENTINEL" in run.stdout
