"""The weekly Ossie drift check over the vendored schema and examples: identical, reformatted, drifted, and
unreachable upstream."""

from __future__ import annotations

import http.client
import json
import urllib.error
from collections.abc import Callable
from pathlib import Path

import pytest
import yaml

from scripts import ossie_schema_drift as drift_check

SCHEMA = "core-spec/ossie-schema.json"
EXAMPLE = "examples/renamed_fields.yaml"
PROVENANCE_PATH = Path(__file__).parent / "fixtures" / "osi" / "ossie-schema.source.yaml"
PROVENANCE = yaml.safe_load(PROVENANCE_PATH.read_text(encoding="utf-8"))
VENDORED = {upstream: (PROVENANCE_PATH.parent / local).read_bytes().decode("utf-8")
            for upstream, local in PROVENANCE["files"].items()}


def _upstream(monkeypatch: pytest.MonkeyPatch, files: dict[str, str | Exception]) -> None:
    """Serve ``files`` (upstream path → text, or the error its fetch raises); any other file is a 404."""
    def fake_get_text(url: str) -> str:
        if url == drift_check.COMMITS_URL:
            return json.dumps([{"sha": "abc123"}])
        for path, body in files.items():
            if url == drift_check.raw_url(path):
                if isinstance(body, Exception):
                    raise body
                return body
        raise urllib.error.HTTPError(url, 404, "Not Found", hdrs=http.client.HTTPMessage(), fp=None)

    monkeypatch.setattr(drift_check, "_get_text", fake_get_text)


def test_files_come_from_the_provenance() -> None:
    assert PROVENANCE["repository"] == "apache/ossie"
    assert drift_check.PROVENANCE == PROVENANCE_PATH
    files = dict(drift_check.vendored_files())
    assert files == {up: PROVENANCE_PATH.parent / local for up, local in PROVENANCE["files"].items()}
    assert SCHEMA in files
    assert len([p for p in files if p.startswith("examples/")]) == 6
    assert all(path.is_file() for path in files.values())


def test_urls_name_apache_ossie() -> None:
    assert drift_check.REPO == "apache/ossie"
    assert drift_check.raw_url(EXAMPLE) == f"https://raw.githubusercontent.com/apache/ossie/main/{EXAMPLE}"
    assert "/repos/apache/ossie/" in drift_check.COMMITS_URL


def test_workflow_installs_pyyaml_from_a_wheel() -> None:
    workflow = yaml.safe_load((drift_check.ROOT / ".github" / "workflows" / "ossie-schema-drift.yml").read_text(encoding="utf-8"))
    commands = [step.get("run", "") for step in workflow["jobs"]["drift"]["steps"]]
    assert any("--only-binary :all:" in c and "PyYAML==" in c for c in commands)
    assert "python scripts/ossie_schema_drift.py" in commands


def test_reformatted_schema_is_not_drift() -> None:
    schema = json.loads(VENDORED[SCHEMA])
    assert drift_check.drift(vendored=schema, upstream=json.loads(json.dumps(schema, sort_keys=True))) == []


def test_identical_upstream_exits_clean(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _upstream(monkeypatch, dict(VENDORED))
    assert drift_check.main() == drift_check.EXIT_SAME
    assert "abc123" in capsys.readouterr().out


def test_reformatted_upstream_schema_exits_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    _upstream(monkeypatch, {**VENDORED, SCHEMA: json.dumps(json.loads(VENDORED[SCHEMA]), sort_keys=True)})
    assert drift_check.main() == drift_check.EXIT_SAME


def test_changed_schema_reports_the_diff(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    changed = json.loads(VENDORED[SCHEMA])
    changed["$defs"]["Dataset"]["properties"]["new_prop"] = {"type": "string"}
    _upstream(monkeypatch, {**VENDORED, SCHEMA: json.dumps(changed)})
    assert drift_check.main() == drift_check.EXIT_DRIFT
    out = capsys.readouterr().out
    assert any(line.startswith("+") and "new_prop" in line for line in out.splitlines())


def test_drifted_example_reports_its_diff(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _upstream(monkeypatch, {**VENDORED, EXAMPLE: VENDORED[EXAMPLE] + "  - name: brand_new_metric\n"})
    assert drift_check.main() == drift_check.EXIT_DRIFT
    out = capsys.readouterr().out
    assert EXAMPLE in out
    assert any(line.startswith("+") and "brand_new_metric" in line for line in out.splitlines())


@pytest.mark.parametrize("upstream", [
    pytest.param(lambda text: text.rstrip("\n"), id="final-newline-removed"),
    pytest.param(lambda text: text + "\n", id="final-newline-added"),
    pytest.param(lambda text: text.replace("\n", "\r\n"), id="crlf"),
])
def test_line_ending_change_is_drift(monkeypatch: pytest.MonkeyPatch,
                                    upstream: Callable[[str], str]) -> None:
    _upstream(monkeypatch, {**VENDORED, EXAMPLE: upstream(VENDORED[EXAMPLE])})
    assert drift_check.main() == drift_check.EXIT_DRIFT


def test_missing_upstream_example_is_a_fetch_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    _upstream(monkeypatch, {path: text for path, text in VENDORED.items() if path != EXAMPLE})
    assert drift_check.main() == drift_check.EXIT_FETCH


def test_failed_example_fetch_is_a_fetch_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    _upstream(monkeypatch, {**VENDORED, EXAMPLE: urllib.error.URLError("no route to host")})
    assert drift_check.main() == drift_check.EXIT_FETCH


def test_failed_commit_lookup_still_compares(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    def no_commits(url: str) -> str:
        if url == drift_check.COMMITS_URL:
            raise urllib.error.URLError("rate limited")
        return next(text for path, text in VENDORED.items() if url == drift_check.raw_url(path))

    monkeypatch.setattr(drift_check, "_get_text", no_commits)
    assert drift_check.main() == drift_check.EXIT_SAME
    assert "unknown" in capsys.readouterr().out


@pytest.mark.parametrize("error", [
    urllib.error.URLError("no route to host"),
    http.client.IncompleteRead(b"{"),
    UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"),
])
def test_unreachable_upstream_schema_is_a_fetch_failure(monkeypatch: pytest.MonkeyPatch, error: Exception) -> None:
    _upstream(monkeypatch, {**VENDORED, SCHEMA: error})
    assert drift_check.main() == drift_check.EXIT_FETCH
