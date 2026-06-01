"""Unit tests for BM25Retriever (DEV-1514).

Pins the BM25 retriever's contract:
* Returns memory ids ranked by entity-overlap BM25 over memories
  whose ``entities`` lists have been pre-filtered to ``valid_canonicals``
  (DEV-1428 — stale tags never score).
* Empty ``memory_ranking`` when there are no ``query_entities`` (the
  channel is inactive).
* ``entity_ranking`` and ``text_by_id`` are always empty for this
  retriever — BM25 over memory tags has nothing to say about entity
  documents.
"""

from __future__ import annotations

import tempfile
from typing import AsyncIterator

import pytest_asyncio

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.memories.models import Memory
from slayer.search.retrievers.bm25 import BM25Retriever
from slayer.storage.base import StorageBackend
from slayer.storage.yaml_storage import YAMLStorage


@pytest_asyncio.fixture
async def storage() -> AsyncIterator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmp:
        s = YAMLStorage(base_dir=tmp)
        await s.save_datasource(DatasourceConfig(
            name="mydb", type="sqlite", database=":memory:",
        ))
        await s.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="mydb",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="amount", type=DataType.DOUBLE),
            ],
        ))
        yield s


def _mem(id_: str, learning: str, *entities: str) -> Memory:
    return Memory(id=id_, learning=learning, entities=list(entities))


async def test_empty_query_entities_returns_empty_memory_ranking() -> None:
    retriever = BM25Retriever()
    mems = [_mem("1", "first", "mydb.orders.amount")]
    result = await retriever.retrieve(
        query_entities=[],
        question=None,
        all_memories=mems,
        valid_canonicals={"mydb", "mydb.orders", "mydb.orders.amount"},
        corpus=None,
        datasource=None,
    )
    assert result.memory_ranking == []
    assert result.entity_ranking == []
    assert result.text_by_id == {}
    assert result.warnings == []


async def test_entity_overlap_drives_ranking() -> None:
    """A memory tagged with the query entity outranks one tagged
    with an unrelated entity."""
    retriever = BM25Retriever()
    mems = [
        _mem("1", "matches", "mydb.orders.amount"),
        _mem("2", "no overlap", "mydb.orders.id"),
    ]
    result = await retriever.retrieve(
        query_entities=["mydb.orders.amount"],
        question=None,
        all_memories=mems,
        valid_canonicals={
            "mydb", "mydb.orders",
            "mydb.orders.id", "mydb.orders.amount",
        },
        corpus=None,
        datasource=None,
    )
    assert result.memory_ranking[0] == "1"
    assert "2" not in result.memory_ranking


async def test_stale_entity_tags_filtered_before_scoring() -> None:
    """DEV-1428: a memory tagged with a stale entity (not in
    ``valid_canonicals``) does not score on that tag. If the only
    overlap is via the stale tag, the memory is dropped from the
    ranking entirely."""
    retriever = BM25Retriever()
    mems = [
        _mem("alive", "live tag", "mydb.orders.amount"),
        _mem("stale", "stale tag only", "mydb.orders.removed_column"),
    ]
    valid = {"mydb", "mydb.orders", "mydb.orders.amount"}
    result = await retriever.retrieve(
        query_entities=["mydb.orders.removed_column"],
        question=None,
        all_memories=mems,
        valid_canonicals=valid,
        corpus=None,
        datasource=None,
    )
    # The query entity itself is not in valid_canonicals, but the
    # retriever does not validate that. What it DOES is filter each
    # memory's entities list. The stale memory has no surviving live
    # tags, so BM25 gives it zero overlap and drops it.
    assert "stale" not in result.memory_ranking


async def test_entity_ranking_and_text_by_id_always_empty() -> None:
    retriever = BM25Retriever()
    mems = [_mem("1", "x", "mydb.orders.amount")]
    result = await retriever.retrieve(
        query_entities=["mydb.orders.amount"],
        question="anything",
        all_memories=mems,
        valid_canonicals={"mydb", "mydb.orders", "mydb.orders.amount"},
        corpus=None,
        datasource=None,
    )
    assert result.entity_ranking == []
    assert result.text_by_id == {}


async def test_no_memories_returns_empty_ranking() -> None:
    retriever = BM25Retriever()
    result = await retriever.retrieve(
        query_entities=["mydb.orders.amount"],
        question=None,
        all_memories=[],
        valid_canonicals={"mydb", "mydb.orders", "mydb.orders.amount"},
        corpus=None,
        datasource=None,
    )
    assert result.memory_ranking == []


async def test_default_no_op_write_hooks() -> None:
    """BM25 has no persistent state; every write hook returns the
    ABC default empty/None value."""
    retriever = BM25Retriever()
    memory = Memory(id="1", learning="x", entities=[])
    model = SlayerModel(
        name="orders", sql_table="orders", data_source="mydb",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
    )
    assert await retriever.upsert_memory(memory) == []
    assert await retriever.refresh_model_subtree(model) == []
    assert await retriever.refresh_datasource(
        name="mydb", models=[model],
    ) == []
    assert await retriever.delete_memory("1") is None
    assert await retriever.delete_model(
        data_source="mydb", name="orders",
    ) is None
    assert await retriever.delete_datasource("mydb") is None
