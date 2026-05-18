"""DEV-1428: all response models carry str ids.

``SaveMemoryResponse.memory_id`` and ``ForgetMemoryResponse.deleted_id``
flip to ``str``; ``MemoryHit.id`` / ``ExampleQueryHit.id`` also flip.
"""

from __future__ import annotations

import os
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.memories.service import MemoryService
from slayer.search.service import SearchService
from slayer.storage.base import StorageBackend
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
async def storage() -> AsyncIterator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmpdir:
        s = YAMLStorage(base_dir=os.path.join(tmpdir, "store"))
        await s.save_datasource(
            DatasourceConfig(
                name="mydb", type="sqlite", database=":memory:",
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


class TestStringIds:
    async def test_save_memory_response_id_is_str(
        self, storage: StorageBackend,
    ) -> None:
        svc = MemoryService(storage=storage)
        resp = await svc.save_memory(
            learning="x", linked_entities=["mydb.orders.amount"],
        )
        assert isinstance(resp.memory_id, str)
        assert resp.memory_id == "1"

    async def test_forget_memory_response_id_is_str(
        self, storage: StorageBackend,
    ) -> None:
        svc = MemoryService(storage=storage)
        await svc.save_memory(
            learning="x", linked_entities=["mydb.orders.amount"],
        )
        resp = await svc.forget_memory(identifier="1")
        assert isinstance(resp.deleted_id, str)
        assert resp.deleted_id == "1"

    async def test_memory_hit_id_is_str(
        self, storage: StorageBackend,
    ) -> None:
        await storage.save_memory(
            learning="orders revenue", entities=["mydb.orders.amount"],
        )
        svc = SearchService(storage=storage)
        resp = await svc.search(entities=["mydb.orders.amount"])
        assert resp.memories
        assert isinstance(resp.memories[0].id, str)

    async def test_example_query_hit_id_is_str(
        self, storage: StorageBackend,
    ) -> None:
        attached = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:sum")],
        )
        await storage.save_memory(
            learning="example revenue query",
            entities=["mydb.orders.amount"],
            query=attached,
        )
        svc = SearchService(storage=storage)
        resp = await svc.search(entities=["mydb.orders.amount"])
        assert resp.example_queries
        assert isinstance(resp.example_queries[0].id, str)
