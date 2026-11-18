"""The usage report is schema 3 in the Ossie vocabulary; spooled batches of older schemas keep their counts under the
renamed keys and are sent as one schema-3 report.

Spec: usage-reporting/collection › Report identity and time resolution; A retired usage key counts as its surface's
other.

The pre-2.0 spellings below are deliberate: keep this file out of the vocabulary respelling pass.
"""

from __future__ import annotations

import datetime
import json
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

from slayer.usage_reporting import sender, spool
from slayer.usage_reporting.payload import SCHEMA_VERSION, USAGE_KEYS, Batch, Context, UsageKey, UsageReport, read_spooled
from slayer.usage_reporting.settings import UsageReportingConfig
from tests._usage_reporting_helpers import build_storage, cli, config, quiet_config, write_config

RENAMED_KEYS: dict[str, str] = {
    "cli:validate-models": "cli:validate-datasets",
    "cli:recommend-root-model": "cli:recommend-population",
    "cli:models.list": "cli:datasets.list",
    "cli:models.show": "cli:datasets.show",
    "cli:models.create": "cli:datasets.create",
    "cli:models.delete": "cli:datasets.delete",
    "mcp:create_model": "mcp:create_dataset",
    "mcp:edit_model": "mcp:edit_dataset",
    "mcp:delete_model": "mcp:delete_dataset",
    "mcp:models_summary": "mcp:datasets_summary",
    "mcp:validate_models": "mcp:validate_datasets",
    "mcp:recommend_root_model": "mcp:recommend_population",
    "mcp:ingest_datasource_models": "mcp:ingest_datasource",
    "rest:/models": "rest:/datasets",
    "rest:/models/{name}": "rest:/datasets/{name}",
    "rest:/validate-models": "rest:/validate-datasets",
    "rest:/recommend-root-model": "rest:/recommend-population",
}
NOW = datetime.datetime(2026, 11, 16, 12, tzinfo=datetime.timezone.utc)
_NOTICE_URL = "docs.motley.ai/slayer/reference/usage-reporting"


def spool_raw(directory: Path, *, prefix: str, body: dict[str, Any]) -> Path:
    """A spool file as an older SLayer wrote it (``spool-`` for schema 1, ``v2-spool-`` for schema 2)."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{prefix}{time.time_ns():020d}-{uuid.uuid4().hex[:12]}.json"
    path.write_text(json.dumps({"period": {"first": "2026-11-01", "last": "2026-11-02"}, **body}))
    return path


def paths_under(root: Path) -> sender.Paths:
    paths = sender.Paths(config=root / "config.json", spool=root / "spool", lease=root / "send.lease")
    paths.spool.mkdir(parents=True, exist_ok=True)
    due = UsageReportingConfig(install_id=uuid.uuid4(), last_sent=NOW - datetime.timedelta(hours=25))
    paths.config.write_text(due.model_dump_json())
    return paths


class Posts:
    """A stand-in for the PostHog post: records each report, answering ``outcomes`` in turn."""

    def __init__(self, *outcomes: bool) -> None:
        self.reports: list[dict[str, Any]] = []
        self._outcomes = list(outcomes)

    def __call__(self, report: UsageReport, *, url: str) -> bool:
        del url
        self.reports.append(report.model_dump(mode="json"))
        return self._outcomes.pop(0)


def ok_counts(report: dict[str, Any]) -> dict[str, int]:
    return {key: entry["ok"] for key, entry in report["usage"].items()}


class TestSchema:
    def test_the_report_is_schema_3(self) -> None:
        assert SCHEMA_VERSION == 3
        report = sender.build_report(config=UsageReportingConfig(), batches=[Batch()])
        assert report.model_dump(mode="json")["schema"] == 3

    def test_the_context_counts_datasets(self) -> None:
        assert "dataset_count" in Context.model_fields
        assert "model_count" not in Context.model_fields

    @pytest.mark.parametrize(("old", "new"), sorted(RENAMED_KEYS.items()))
    def test_a_renamed_key_is_defined_only_in_its_new_spelling(self, old: str, new: str) -> None:
        assert new in USAGE_KEYS
        assert old not in USAGE_KEYS


class TestOlderBatches:
    @pytest.mark.parametrize(("old", "new"), sorted(RENAMED_KEYS.items()))
    def test_a_renamed_key_keeps_its_counts(self, old: str, new: UsageKey) -> None:
        surface = old.partition(":")[0]
        batch = read_spooled(json.dumps({"usage": {
            old: {"ok": 2, "errors": {"timeout": 1}}, new: {"ok": 1}, f"{surface}:other": {"ok": 5},
        }}).encode())
        assert batch.usage[new].ok == 3
        assert batch.usage[new].errors == {"timeout": 1}
        assert {str(k): v.ok for k, v in batch.usage.items()}[f"{surface}:other"] == 5

    def test_the_model_count_bucket_becomes_the_dataset_count(self) -> None:
        batch = read_spooled(json.dumps({"context": {"model_count": {"11-50": 1}, "storage": {"yaml": 1}}}).encode())
        assert batch.context.model_dump(mode="json")["dataset_count"] == {"11-50": 1}
        assert batch.context.model_dump(mode="json")["storage"] == {"yaml": 1}

    def test_renamed_and_current_tool_counts_are_sent_as_one_schema_3_report(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        paths = paths_under(tmp_path)
        spool_raw(paths.spool, prefix="v2-spool-", body={
            "usage": {"mcp:create_model": {"ok": 2}}, "context": {"model_count": {"11-50": 1}},
        })
        spool.write(paths.spool, Batch.model_validate({"usage": {"mcp:create_dataset": {"ok": 1}}}))
        posts = Posts(True)
        monkeypatch.setattr(sender, "_post", posts)
        sender.send(paths, now=NOW, url="http://unused")
        [report] = posts.reports
        assert report["schema"] == 3
        assert ok_counts(report) == {"mcp:create_dataset": 3}
        assert report["context"]["dataset_count"] == {"11-50": 1}
        assert "model_count" not in report["context"]

    def test_schema_1_2_and_3_files_are_sent_together_and_removed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        paths = paths_under(tmp_path)
        spool_raw(paths.spool, prefix="spool-", body={"usage": {"mcp:search": {"ok": 1}}})
        spool_raw(paths.spool, prefix="v2-spool-", body={"usage": {"mcp:models_summary": {"ok": 2}}})
        spool.write(paths.spool, Batch.model_validate({"usage": {"mcp:datasets_summary": {"ok": 4}}}))
        posts = Posts(True)
        monkeypatch.setattr(sender, "_post", posts)
        sender.send(paths, now=NOW, url="http://unused")
        [report] = posts.reports
        assert report["schema"] == 3
        assert ok_counts(report) == {"mcp:search": 1, "mcp:datasets_summary": 6}
        assert list(paths.spool.iterdir()) == []

    def test_a_failed_send_keeps_the_files_and_a_later_send_recovers_them(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        paths = paths_under(tmp_path)
        spool_raw(paths.spool, prefix="v2-spool-", body={"usage": {"rest:/models/{name}": {"ok": 2}}})
        spool.write(paths.spool, Batch.model_validate({"usage": {"rest:/datasets/{name}": {"ok": 1}}}))
        posts = Posts(False, True)
        monkeypatch.setattr(sender, "_post", posts)
        sender.send(paths, now=NOW, url="http://unused")
        assert len(list(paths.spool.iterdir())) == 2
        sender.send(paths, now=NOW, url="http://unused")
        assert [ok_counts(r) for r in posts.reports] == [{"rest:/datasets/{name}": 3}] * 2
        assert list(paths.spool.iterdir()) == []


def test_the_notice_prints_once_more_after_the_bump(usage_reporting_env) -> None:
    store = build_storage(usage_reporting_env.root)
    quiet_config()
    write_config(notice_shown_schema=2)
    first = cli(["datasources", "--storage", store, "list"])
    second = cli(["datasources", "--storage", store, "list"])
    assert _NOTICE_URL in first.stderr
    assert _NOTICE_URL not in second.stderr
    assert config().notice_shown_schema == 3
