"""examples/comparisons/matrix.json (the website's feature matrix) matches matrix.yaml and probes.yaml."""

import importlib.util
import json
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
import yaml

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


def test_list_columns_row_has_probes_on_every_engine():
    matrix = yaml.safe_load(export_matrix.MATRIX.read_text())
    rows = matrix if isinstance(matrix, list) else matrix["rows"]
    (row,) = [r for r in rows if r["id"] == "Q27"]
    assert row["group"] == "Query semantics"
    assert all(tool in row for tool, _ in export_matrix.TOOLS)
    probes = [p for p in yaml.safe_load(export_matrix.PROBES.read_text())["probes"] if p["row"] == "Q27"]
    assert len(probes) >= 5
    for probe in probes:
        assert all(engine in probe for engine in export_matrix.ENGINES), probe["id"]
    assert "tags" in (ROOT / "examples" / "comparisons" / "dataset.sql").read_text()


@pytest.mark.parametrize(("version", "ref"), [("1.8.5", "1.x"), ("2.0.0.dev0", "main"), ("2.1.0", "main")])
def test_probe_ref_follows_package_version(version, ref):
    assert export_matrix.probe_ref(version) == ref


def test_matrix_json_links_point_at_the_release_line():
    version = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]
    repo = "https://github.com/MotleyAI/slayer"
    ref = export_matrix.probe_ref(version)
    data = json.loads(export_matrix.OUT.read_text())
    assert data["source"] == f"{repo}/tree/{ref}/examples/comparisons"
    urls = [p["url"] for r in data["rows"] for p in r["probes"]]
    assert urls
    assert all(u.startswith(f"{repo}/blob/{ref}/examples/comparisons/probes.yaml#L") for u in urls)


def test_probe_on_unknown_row_is_rejected(tmp_path, monkeypatch):
    probes = tmp_path / "probes.yaml"
    probes.write_text("probes:\n  - id: x1\n    row: Q999\n    slayer: {}\n")
    monkeypatch.setattr(export_matrix, "PROBES", probes)
    with pytest.raises(SystemExit, match="row Q999 is not in matrix.yaml"):
        export_matrix.load_probes({"Q1"})
