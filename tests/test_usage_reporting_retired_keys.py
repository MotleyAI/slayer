"""A spooled batch carrying a usage key the current version no longer defines still loads."""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

from slayer.usage_reporting import spool
from slayer.usage_reporting.payload import USAGE_KEYS, Batch, merge

_RETIRED_CLI_KEY = "cli:search.refresh-samples"
_RETIRED_PG_KEY = "pg:describe"


def _spool_raw(directory: Path, *, usage: dict) -> Path:
    """Write a batch as an older SLayer version would have spooled it."""
    path = directory / f"spool-{time.time_ns():020d}-{uuid.uuid4().hex[:12]}.json"
    path.write_text(json.dumps({"period": {"first": "2026-10-01", "last": "2026-10-02"}, "usage": usage}))
    return path


def _load_one(directory: Path) -> Batch:
    batches = spool.load(spool.pending_files(directory))
    assert len(batches) == 1
    return batches[0]


def test_retired_keys_are_not_in_the_vocabulary() -> None:
    assert _RETIRED_CLI_KEY not in USAGE_KEYS
    assert _RETIRED_PG_KEY not in USAGE_KEYS
    assert "cli:datasources.refresh-samples" in USAGE_KEYS


def test_renamed_cli_command_counts_fold_into_cli_other(tmp_path: Path) -> None:
    _spool_raw(tmp_path, usage={
        _RETIRED_CLI_KEY: {"ok": 2},
        "cli:other": {"ok": 1},
        "cli:query": {"ok": 5},
    })
    batch = _load_one(tmp_path)
    assert {key: entry.ok for key, entry in batch.usage.items()} == {"cli:other": 3, "cli:query": 5}


def test_error_counts_are_summed_per_token(tmp_path: Path) -> None:
    _spool_raw(tmp_path, usage={
        _RETIRED_CLI_KEY: {"ok": 1, "errors": {"value_error": 1}},
        "cli:other": {"errors": {"value_error": 2, "timeout": 1}},
    })
    other = _load_one(tmp_path).usage["cli:other"]
    assert other.ok == 1
    assert other.errors == {"value_error": 3, "timeout": 1}


def test_retired_key_without_existing_other_creates_it(tmp_path: Path) -> None:
    _spool_raw(tmp_path, usage={_RETIRED_CLI_KEY: {"ok": 2, "errors": {"other": 1}}})
    other = _load_one(tmp_path).usage["cli:other"]
    assert (other.ok, other.errors) == (2, {"other": 1})


def test_retired_key_on_surface_without_other_is_dropped_alone(tmp_path: Path) -> None:
    _spool_raw(tmp_path, usage={_RETIRED_PG_KEY: {"ok": 4}, "pg:query": {"ok": 3}})
    batch = _load_one(tmp_path)
    assert {key: entry.ok for key, entry in batch.usage.items()} == {"pg:query": 3}


def test_report_merges_folded_batches_with_current_ones(tmp_path: Path) -> None:
    _spool_raw(tmp_path, usage={_RETIRED_CLI_KEY: {"ok": 2}, "cli:query": {"ok": 1}})
    spool.write(tmp_path, Batch.model_validate({"usage": {"cli:other": {"ok": 1}, "cli:query": {"ok": 4}}}))
    merged = merge(spool.load(spool.claim(tmp_path)))
    assert {key: entry.ok for key, entry in merged.usage.items()} == {"cli:other": 3, "cli:query": 5}


def test_batch_validation_folds_retired_keys_directly() -> None:
    batch = Batch.model_validate({"usage": {
        _RETIRED_CLI_KEY: {"ok": 2, "errors": {"timeout": 1}},
        "cli:other": {"ok": 1, "errors": {"timeout": 2}},
        "flight:retired-token": {"ok": 9},
        "flight:query": {"ok": 6},
    }})
    assert {key: (entry.ok, entry.errors) for key, entry in batch.usage.items()} == {
        "cli:other": (3, {"timeout": 3}),
        "flight:query": (6, {}),
    }


def test_unknown_surface_key_is_dropped() -> None:
    batch = Batch.model_validate({"usage": {"nosurface": {"ok": 1}, "cli:query": {"ok": 2}}})
    assert {key: entry.ok for key, entry in batch.usage.items()} == {"cli:query": 2}
