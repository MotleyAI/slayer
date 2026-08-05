"""Memory-to-memory entity refs in ``save_memory(linked_entities=...)``.

DEV-1428: ``memory:<id>`` is a first-class canonical entity. It is
accepted in ``linked_entities`` lists and resolves strictly (raises when
the target memory does not exist).
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator

import pytest

from slayer.core.errors import EntityResolutionError
from slayer.memories.service import MemoryService
from slayer.storage.base import StorageBackend
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def storage() -> Iterator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=os.path.join(tmpdir, "store"))


class TestMemoryToMemoryRefs:
    async def test_valid_memory_ref(self, storage: StorageBackend) -> None:
        svc = MemoryService(storage=storage)
        seed = await storage.save_memory(
            learning="seed", entities=["mydb.orders"],
        )
        resp = await svc.save_memory(
            learning="see also seed",
            linked_entities=[f"memory:{seed.id}"],
        )
        assert f"memory:{seed.id}" in resp.resolved_entities

    async def test_absent_memory_ref_raises_on_save(
        self, storage: StorageBackend,
    ) -> None:
        svc = MemoryService(storage=storage)
        with pytest.raises(EntityResolutionError):
            await svc.save_memory(
                learning="dangling",
                linked_entities=["memory:nonexistent"],
            )

    async def test_user_string_id_memory_ref(
        self, storage: StorageBackend,
    ) -> None:
        svc = MemoryService(storage=storage)
        seed = await storage.save_memory(
            id="kb.policy",
            learning="seed",
            entities=["mydb.orders"],
        )
        resp = await svc.save_memory(
            learning="see also kb.policy",
            linked_entities=[f"memory:{seed.id}"],
        )
        assert "memory:kb.policy" in resp.resolved_entities
