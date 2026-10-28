"""The weekly Ossie schema drift check: identical, reformatted, drifted, and unreachable upstream."""

from __future__ import annotations

import copy
import json
import urllib.error
from typing import Any

import pytest

from scripts import ossie_schema_drift as drift_check

VENDORED = json.loads(drift_check.VENDORED.read_text(encoding="utf-8"))


def _upstream(monkeypatch: pytest.MonkeyPatch, schema: Any) -> None:
    def fake_get_json(url: str) -> Any:
        if url == drift_check.COMMITS_URL:
            return [{"sha": "abc123"}]
        return schema

    monkeypatch.setattr(drift_check, "_get_json", fake_get_json)


def test_reformatted_schema_is_not_drift() -> None:
    reordered = json.loads(json.dumps(VENDORED, sort_keys=True))
    assert drift_check.drift(vendored=VENDORED, upstream=reordered) == []


def test_identical_upstream_exits_clean(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _upstream(monkeypatch, copy.deepcopy(VENDORED))
    assert drift_check.main() == drift_check.EXIT_SAME
    assert "abc123" in capsys.readouterr().out


def test_changed_upstream_reports_the_diff(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    changed = copy.deepcopy(VENDORED)
    changed["$defs"]["Dataset"]["properties"]["new_prop"] = {"type": "string"}
    _upstream(monkeypatch, changed)
    assert drift_check.main() == drift_check.EXIT_DRIFT
    out = capsys.readouterr().out
    assert "abc123" in out
    assert any(line.startswith("+") and "new_prop" in line for line in out.splitlines())


def test_unreachable_upstream_is_a_fetch_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def unreachable(url: str) -> Any:
        raise urllib.error.URLError("no route to host")

    monkeypatch.setattr(drift_check, "_get_json", unreachable)
    assert drift_check.main() == drift_check.EXIT_FETCH
