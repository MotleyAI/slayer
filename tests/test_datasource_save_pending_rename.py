"""A datasource save waits for its pending reserved-name granularity rename (models/stored-upgrade)."""

from __future__ import annotations

from pathlib import Path

import pytest

from slayer.core.errors import GranularityDefinitionError
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.access_filtered import AccessFilteredStorage
from slayer.storage.base import StorageBackend
from slayer.storage.upgrade import upgrade_store
from tests._granularity_rename_fixtures import (
    BACKENDS,
    DS,
    EXPECTED_BUCKETS,
    bucket_totals,
    granularity_store,
    raw_documents,
    renamed,
    reopen,
)


@pytest.mark.parametrize("backend", BACKENDS)
async def test_saving_a_loaded_datasource_whose_rename_is_pending_is_refused(tmp_path: Path, backend: str) -> None:
    base = str(tmp_path / backend)
    storage = await granularity_store(backend=backend, base=base, name="left")
    loaded = await storage.get_datasource(DS)
    assert loaded is not None
    assert [g.name for g in loaded.granularities] == [renamed("left")]
    before = raw_documents(backend=backend, base=base)
    with pytest.raises(GranularityDefinitionError, match=r"'left'.*slayer storage upgrade"):
        await storage.save_datasource(loaded)
    assert raw_documents(backend=backend, base=base) == before
    assert await bucket_totals(reopen(backend=backend, base=base)) == EXPECTED_BUCKETS


def _wrapped(storage: StorageBackend, view: str) -> StorageBackend:
    if view == "engine":
        return SlayerQueryEngine(storage=storage).storage
    return AccessFilteredStorage(storage, tags=[], bypass=view == "admin")


@pytest.mark.parametrize("view", ["engine", "filtered", "admin"])
@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_storage_view_refuses_the_same_save(tmp_path: Path, backend: str, view: str) -> None:
    base = str(tmp_path / backend)
    storage = _wrapped(await granularity_store(backend=backend, base=base, name="left"), view)
    loaded = await storage.get_datasource(DS)
    assert loaded is not None
    with pytest.raises(GranularityDefinitionError, match=r"'left'.*slayer storage upgrade"):
        await storage.save_datasource(loaded)
    assert await bucket_totals(reopen(backend=backend, base=base)) == EXPECTED_BUCKETS


@pytest.mark.parametrize("backend", BACKENDS)
async def test_saving_after_the_upgrade_succeeds(tmp_path: Path, backend: str) -> None:
    base = str(tmp_path / backend)
    storage = await granularity_store(backend=backend, base=base, name="left")
    await upgrade_store(storage)
    upgraded = reopen(backend=backend, base=base)
    loaded = await upgraded.get_datasource(DS)
    assert loaded is not None
    await upgraded.save_datasource(loaded)
    assert await bucket_totals(reopen(backend=backend, base=base)) == EXPECTED_BUCKETS
