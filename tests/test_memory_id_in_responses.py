"""DEV-1428: all response models carry str ids.

``SaveMemoryResponse.memory_id`` and ``ForgetMemoryResponse.deleted_id``
flip to ``str``; ``SearchHit.id`` also carries a str id for memory hits.
"""

from __future__ import annotations

import pytest

from slayer.core.models import ModelMeasure
from slayer.core.query import SlayerQuery
from slayer.memories.service import MemoryService
from slayer.search.service import SearchService
from slayer.storage.base import StorageBackend


@pytest.fixture
def storage(mydb_orders_storage: StorageBackend) -> StorageBackend:
    return mydb_orders_storage


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
        resp = await svc.search(entities=["mydb.orders.amount"], max_results=20)
        memory_hits = [h for h in resp.results if h.kind == "memory" and h.query is None]
        assert memory_hits
        assert isinstance(memory_hits[0].id, str)

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
        resp = await svc.search(entities=["mydb.orders.amount"], max_results=20)
        example_query_hits = [h for h in resp.results if h.kind == "memory" and h.query is not None]
        assert example_query_hits
        assert isinstance(example_query_hits[0].id, str)
