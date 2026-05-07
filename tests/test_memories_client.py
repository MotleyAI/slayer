"""Python client tests for the unified Memory surface (DEV-1357 v2).

Covers ``SlayerClient.save_memory`` / ``forget_memory`` /
``recall_memories`` in **local mode** — the same shape as
``tests/test_client.py``. Local mode talks to the storage backend
directly (no HTTP), which is enough to validate the client surface.
Remote-mode behaviour is exercised by ``test_memories_rest.py``.
"""

import tempfile
from typing import AsyncIterator

import pytest
import pytest_asyncio

from slayer.client.slayer_client import SlayerClient
from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelMeasure,
    SlayerModel,
)
from slayer.storage.yaml_storage import YAMLStorage


@pytest_asyncio.fixture
async def storage() -> AsyncIterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmpdir:
        s = YAMLStorage(base_dir=tmpdir)
        await s.save_datasource(
            DatasourceConfig(name="mydb", type="postgres", host="x")
        )
        await s.save_model(
            SlayerModel(
                name="orders",
                data_source="mydb",
                sql_table="orders",
                columns=[
                    Column(
                        name="id",
                        sql="id",
                        type=DataType.DOUBLE,
                        primary_key=True,
                    ),
                    Column(
                        name="amount",
                        sql="amount",
                        type=DataType.DOUBLE,
                    ),
                ],
                measures=[ModelMeasure(formula="amount:sum", name="rev")],
            )
        )
        await s.set_datasource_priority(["mydb"])
        yield s


@pytest.fixture
def client(storage: YAMLStorage) -> SlayerClient:
    return SlayerClient(storage=storage)


class TestSaveMemory:
    async def test_save_with_entities(
        self, client: SlayerClient, storage: YAMLStorage
    ) -> None:
        resp = await client.save_memory(
            learning="orders.amount in cents",
            linked_entities=["mydb.orders.amount"],
        )
        assert resp.memory_id == 1
        assert resp.resolved_entities == ["mydb.orders.amount"]
        loaded = await storage.get_memory(1)
        assert loaded.learning == "orders.amount in cents"

    async def test_save_with_query(
        self, client: SlayerClient, storage: YAMLStorage
    ) -> None:
        resp = await client.save_memory(
            learning="rev",
            linked_entities={
                "source_model": "orders",
                "measures": [{"formula": "amount:sum"}],
            },
        )
        assert resp.memory_id == 1
        loaded = await storage.get_memory(1)
        assert loaded.query is not None


class TestForgetMemory:
    async def test_forget_existing(
        self, client: SlayerClient, storage: YAMLStorage
    ) -> None:
        memory = await storage.save_memory(
            learning="x", entities=["mydb.orders"]
        )
        resp = await client.forget_memory(memory.id)
        assert resp.deleted_id == memory.id
        assert await storage.list_memories() == []

    async def test_forget_accepts_string_id(
        self, client: SlayerClient, storage: YAMLStorage
    ) -> None:
        memory = await storage.save_memory(
            learning="x", entities=["mydb.orders"]
        )
        resp = await client.forget_memory(str(memory.id))
        assert resp.deleted_id == memory.id


class TestRecallMemories:
    async def test_recall_with_entities(
        self, client: SlayerClient, storage: YAMLStorage
    ) -> None:
        await storage.save_memory(
            learning="match", entities=["mydb.orders.amount"]
        )
        await storage.save_memory(
            learning="other", entities=["mydb.orders"]
        )
        resp = await client.recall_memories(about=["mydb.orders.amount"])
        assert any(hit.learning == "match" for hit in resp.learnings)

    async def test_recall_with_query(
        self, client: SlayerClient, storage: YAMLStorage
    ) -> None:
        await storage.save_memory(
            learning="match", entities=["mydb.orders.amount"]
        )
        resp = await client.recall_memories(
            about={
                "source_model": "orders",
                "measures": [{"formula": "amount:sum"}],
            }
        )
        learnings = [hit.learning for hit in resp.learnings]
        assert "match" in learnings

    async def test_recall_max_caps_apply(
        self, client: SlayerClient, storage: YAMLStorage
    ) -> None:
        for _ in range(3):
            await storage.save_memory(
                learning="x", entities=["mydb.orders"]
            )
        resp = await client.recall_memories(
            about=["mydb.orders"], max_learnings=1
        )
        assert len(resp.learnings) == 1
