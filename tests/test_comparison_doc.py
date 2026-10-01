"""The semantic-layer comparison page's generated parts (glance tables, probe links) match its rows and probes.yaml."""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "examples" / "comparisons" / "update_comparison_doc.py"


def test_comparison_doc_is_up_to_date():
    result = subprocess.run([sys.executable, str(SCRIPT), "--check"], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
