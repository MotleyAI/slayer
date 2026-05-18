"""DEV-1428: ``SearchService.search`` is lenient on unresolved refs.

Unresolved entity / memory refs in ``search(entities=...)`` become
warnings rather than raising. ``resolved_input_entities`` shows only
survivors.
"""

from __future__ import annotations

import os
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.search.service import SearchService
from slayer.storage.base import StorageBackend
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
async def storage() -> AsyncIterator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmpdir:
        s = YAMLStorage(base_dir=os.path.join(tmpdir, "store"))
        await s.save_datasource(
            DatasourceConfig(
                name="mydb",
                type="sqlite",
                database=":memory:",
            )
        )
        await s.save_model(
            SlayerModel(
                name="orders",
                sql_table="orders",
                data_source="mydb",
                columns=[
                    Column(name="id", sql="id", primary_key=True),
                    Column(name="amount", sql="amount"),
                ],
            )
        )
        yield s


class TestSearchLenientValidation:
    async def test_unknown_entity_becomes_warning(
        self, storage: StorageBackend,
    ) -> None:
        svc = SearchService(storage=storage)
        resp = await svc.search(
            entities=["mydb.orders.amount", "mydb.orders.does_not_exist"],
        )
        # Unknown entity should NOT raise; should appear as a warning.
        assert any("does_not_exist" in w for w in resp.warnings)
        # Survivor present in resolved.
        assert "mydb.orders.amount" in resp.resolved_input_entities
        assert "mydb.orders.does_not_exist" not in resp.resolved_input_entities

    async def test_unknown_memory_ref_becomes_warning(
        self, storage: StorageBackend,
    ) -> None:
        svc = SearchService(storage=storage)
        resp = await svc.search(entities=["memory:nonexistent"])
        assert any("memory:nonexistent" in w for w in resp.warnings)
        assert resp.resolved_input_entities == []

    async def test_known_memory_ref_resolves(
        self, storage: StorageBackend,
    ) -> None:
        seed = await storage.save_memory(
            learning="seed", entities=["mydb.orders"],
        )
        svc = SearchService(storage=storage)
        resp = await svc.search(entities=[f"memory:{seed.id}"])
        assert f"memory:{seed.id}" in resp.resolved_input_entities
