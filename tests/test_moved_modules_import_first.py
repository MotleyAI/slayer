"""Every module moved in SLayer 2.0 imports as the first SLayer import of a fresh process."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from tests.test_moved_module_paths import MOVED_MODULES, MOVED_NAMES

REPO = Path(__file__).resolve().parent.parent
NEW_MODULES = sorted({*MOVED_MODULES.values(), *(new for new, _ in MOVED_NAMES.values())})


@pytest.mark.parametrize("module", NEW_MODULES)
def test_a_moved_module_imports_first(module: str) -> None:
    proc = subprocess.run(
        [sys.executable, "-c", f"import {module}"], capture_output=True, text=True, timeout=120, cwd=REPO,
    )
    assert proc.returncode == 0, proc.stderr.strip().splitlines()[-1]
