"""``save_memory(id=...)`` kwarg + upsert + ``created_at`` preservation.

DEV-1428: ``MemoryService.save_memory`` / ``StorageBackend.save_memory``
accept an optional ``id`` kwarg. Duplicate id → upsert (unconditional);
``created_at`` of the original row is preserved on upsert.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from typing import Iterator

import pytest

from slayer.memories.service import MemoryService
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture(params=["yaml", "sqlite"])
def storage(request: pytest.FixtureRequest) -> Iterator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmpdir:
        if request.param == "yaml":
            yield YAMLStorage(base_dir=os.path.join(tmpdir, "store"))
        else:
            yield SQLiteStorage(db_path=os.path.join(tmpdir, "test.db"))


class TestStorageSaveMemoryIdKwarg:
    async def test_id_none_auto_allocates(
        self, storage: StorageBackend,
    ) -> None:
        m = await storage.save_memory(
            learning="x", entities=["mydb.orders"],
        )
        assert m.id == "1"

    async def test_user_supplied_id(self, storage: StorageBackend) -> None:
        m = await storage.save_memory(
            id="kb.x", learning="x", entities=["mydb.orders"],
        )
        assert m.id == "kb.x"

    async def test_duplicate_id_upserts(
        self, storage: StorageBackend,
    ) -> None:
        await storage.save_memory(
            id="kb.x", learning="first", entities=["mydb.orders"],
        )
        await storage.save_memory(
            id="kb.x", learning="second", entities=["mydb.orders"],
        )
        loaded = await storage.get_memory("kb.x")
        assert loaded.learning == "second"

    async def test_upsert_preserves_created_at(
        self, storage: StorageBackend,
    ) -> None:
        first = await storage.save_memory(
            id="kb.x", learning="first", entities=["mydb.orders"],
        )
        original_ts = first.created_at
        # Sleep so the upsert call's "now" is strictly later — proving
        # we preserved the original timestamp on the row.
        await asyncio.sleep(0.05)
        await storage.save_memory(
            id="kb.x", learning="second", entities=["mydb.orders"],
        )
        loaded = await storage.get_memory("kb.x")
        assert loaded.created_at == original_ts


class TestMemoryServiceSaveIdKwarg:
    async def test_service_id_kwarg(
        self, storage: StorageBackend,
    ) -> None:
        # Stub out entity resolution by saving an explicit memory first
        # so the linked_entities=["memory:..."] reference resolves.
        seed = await storage.save_memory(
            learning="seed", entities=["mydb.orders"],
        )
        svc = MemoryService(storage=storage)
        # Service accepts an `id` kwarg and forwards to storage.
        resp = await svc.save_memory(
            learning="note",
            linked_entities=[f"memory:{seed.id}"],
            id="my-rule",
        )
        assert resp.memory_id == "my-rule"

    async def test_bad_charset_id_raises(
        self, storage: StorageBackend,
    ) -> None:
        svc = MemoryService(storage=storage)
        with pytest.raises(ValueError):
            await svc.save_memory(
                learning="note",
                linked_entities=["mydb.orders"],
                id="bad:id",
            )
