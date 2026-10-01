"""examples/comparisons/matrix.json (the website's feature matrix) matches matrix.yaml and probes.yaml."""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "examples" / "comparisons" / "export_matrix.py"


def test_matrix_json_is_up_to_date():
    result = subprocess.run([sys.executable, str(SCRIPT), "--check"], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
