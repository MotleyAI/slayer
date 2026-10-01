"""examples/comparisons/matrix.json (the website's feature matrix) matches matrix.yaml and probes.yaml."""

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "examples" / "comparisons" / "export_matrix.py"

_spec = importlib.util.spec_from_file_location("export_matrix", SCRIPT)
assert _spec is not None
assert _spec.loader is not None
export_matrix = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(export_matrix)


def test_matrix_json_is_up_to_date():
    result = subprocess.run([sys.executable, str(SCRIPT), "--check"], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("url", ["https://example.com/a?b=1&c=2", "http://x.org", "/docs/page", "#q4"])
def test_inline_links_safe_urls(url):
    out = export_matrix.inline(f"see [this]({url})")
    assert out.startswith('see <a href="')
    assert out.endswith('">this</a>')


@pytest.mark.parametrize("url", ["javascript:alert(1)", "data:text/html,x", "JavaScript:alert(1)", "vbscript:x"])
def test_inline_links_unsafe_urls_stay_text(url):
    out = export_matrix.inline(f"see [this]({url})")
    assert "<a" not in out
    assert out == f"see [this]({url})"


def test_probe_on_unknown_row_is_rejected(tmp_path, monkeypatch):
    probes = tmp_path / "probes.yaml"
    probes.write_text("probes:\n  - id: x1\n    row: Q999\n    slayer: {}\n")
    monkeypatch.setattr(export_matrix, "PROBES", probes)
    with pytest.raises(SystemExit, match="row Q999 is not in matrix.yaml"):
        export_matrix.load_probes({"Q1"})
