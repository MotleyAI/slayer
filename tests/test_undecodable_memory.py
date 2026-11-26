"""A YAML memory file that is not UTF-8 text is an unreadable document, never an aborted load or upgrade.

Spec: models/stored-upgrade › slayer storage upgrade persists the current format.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
import yaml

from slayer.core.errors import StoredDocumentLoadError
from slayer.storage.base import StorageBackend
from slayer.storage.migrations import CURRENT_VERSIONS
from tests._integrator_1x import DATA_SOURCE
from tests._storage_upgrade_cli import raw, store_path, upgrade, warehouse_store
from tests._stored_upgrade_fixtures import assert_current, ossie_document

BAD = "undecodable"
BAD_BYTES = b"---\nversion: 4\nentities: []\n---\nrevenue \xff\xfe by status\n"


def _store(tmp_path: Path) -> tuple[StorageBackend, str]:
    base = str(tmp_path / "store")
    storage = warehouse_store(backend="yaml", base=base)
    Path(base, "memories", f"{BAD}.md").write_bytes(BAD_BYTES)
    return storage, base


def _version(text: str) -> int:
    return yaml.safe_load(text.split("---")[1] if text.startswith("---") else text)["version"]


def test_get_memory_names_the_memory(tmp_path: Path) -> None:
    storage, _ = _store(tmp_path)
    load = storage.get_memory(BAD)
    with pytest.raises(StoredDocumentLoadError, match=BAD):
        asyncio.run(load)


def test_listing_collects_it_as_a_failure(tmp_path: Path) -> None:
    storage, _ = _store(tmp_path)
    memories, failures = asyncio.run(storage.load_memories())
    assert [m.id for m in memories] == ["m1"]
    assert [f.name for f in failures] == [BAD]


def test_a_whole_store_upgrade_reports_it_and_upgrades_the_rest(tmp_path: Path) -> None:
    storage, base = _store(tmp_path)
    result = upgrade(store_path(storage))
    assert result.code == 1, result
    assert any(line.startswith(f"memory {BAD}: not upgraded") for line in result.out.splitlines()), result.out
    assert Path(base, "memories", f"{BAD}.md").read_bytes() == BAD_BYTES
    stored = raw(backend="yaml", base=base)
    assert _version(stored["memories/m1"]) == CURRENT_VERSIONS["Memory"]
    assert_current(ossie_document(backend="yaml", base=base, name="customers", data_source=DATA_SOURCE))


def test_a_scoped_upgrade_completes(tmp_path: Path) -> None:
    storage, base = _store(tmp_path)
    result = upgrade(store_path(storage), "--data-source", DATA_SOURCE)
    assert result.code == 0, result
    assert Path(base, "memories", f"{BAD}.md").read_bytes() == BAD_BYTES
    assert_current(ossie_document(backend="yaml", base=base, name="customers", data_source=DATA_SOURCE))
