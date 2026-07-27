"""Retriever ABC compliance tests (DEV-1514).

Pins the shape of the `Retriever` ABC: every concrete retriever
satisfies the contract, default no-op write hooks return the documented
empty values, ``RetrievalResult`` validates as a Pydantic model, and
the three shipping retrievers expose distinct ``name`` attributes.
"""

from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.memories.models import Memory
from slayer.search.retriever import RetrievalResult, Retriever
from slayer.search.retrievers.bm25 import BM25Retriever
from slayer.search.retrievers.embeddings import EmbeddingRetriever
from slayer.search.retrievers.tantivy import TantivyRetriever
from slayer.storage.base import StorageBackend
from slayer.storage.yaml_storage import YAMLStorage


@pytest_asyncio.fixture
async def storage() -> AsyncIterator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmp:
        yield YAMLStorage(base_dir=tmp)


def test_retrieval_result_pydantic_defaults() -> None:
    r = RetrievalResult()
    assert r.memory_ranking == []
    assert r.text_by_id == {}
    assert r.entity_ranking == []
    assert r.warnings == []


def test_retrieval_result_accepts_populated_fields() -> None:
    r = RetrievalResult(
        memory_ranking=["1", "2"],
        text_by_id={"1": "hit text"},
        entity_ranking=["mydb.orders"],
        warnings=["w"],
    )
    assert r.memory_ranking == ["1", "2"]
    assert r.text_by_id == {"1": "hit text"}
    assert r.entity_ranking == ["mydb.orders"]
    assert r.warnings == ["w"]


def test_bm25_retriever_subclasses_retriever() -> None:
    assert issubclass(BM25Retriever, Retriever)
    assert BM25Retriever().name == "bm25"


def test_tantivy_retriever_subclasses_retriever() -> None:
    assert issubclass(TantivyRetriever, Retriever)
    assert TantivyRetriever().name == "tantivy"


async def test_embedding_retriever_subclasses_retriever(  # NOSONAR(S7503) — async required for pytest-asyncio to consume the async ``storage`` fixture
    storage: StorageBackend,
) -> None:
    assert issubclass(EmbeddingRetriever, Retriever)
    er = EmbeddingRetriever(storage=storage, model_name="openai/x")
    assert er.name == "embeddings"


async def test_default_no_op_write_hooks_return_documented_empties() -> None:
    """ABC default behavior: write hooks that aren't overridden return
    the documented no-op values. BM25 and Tantivy use these defaults
    for every write hook this PR."""
    memory = Memory(id="1", learning="hello", entities=[])
    model = SlayerModel(
        name="orders", sql_table="orders", data_source="mydb",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
    )

    for retriever in (BM25Retriever(), TantivyRetriever()):
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


async def test_three_concrete_retrievers_have_distinct_names(  # NOSONAR(S7503) — async required for pytest-asyncio to consume the async ``storage`` fixture
    storage: StorageBackend,
) -> None:
    names = {
        BM25Retriever().name,
        TantivyRetriever().name,
        EmbeddingRetriever(storage=storage).name,
    }
    assert names == {"bm25", "tantivy", "embeddings"}


def test_retriever_abc_cannot_instantiate_without_retrieve() -> None:
    """The ABC's `retrieve` is abstract — a subclass that does not
    implement it must not be instantiable."""

    class BadRetriever(Retriever):
        name = "bad"

    with pytest.raises(TypeError):
        BadRetriever()  # type: ignore[abstract]
