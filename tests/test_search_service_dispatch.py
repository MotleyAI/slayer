"""Write-side fan-out tests for SearchService (DEV-1514).

Pins the public write-side surface introduced by the facade refactor:

* ``SearchService.upsert_memory(mem)`` calls every registered
  retriever's ``upsert_memory(mem)`` exactly once, in declaration order.
* Same for ``refresh_model_subtree`` and ``refresh_datasource``.
* Aggregated warnings are deduped while preserving retriever
  declaration order (Codex Finding 5).
* Per-retriever exceptions are isolated: when retriever 1 raises,
  retriever 2 still runs and the exception surfaces as a prefixed
  warning in the returned list (Codex Finding 4).
* Custom retriever injection (``SearchService(retrievers=[...])``)
  works — the default factory is replaced wholesale.
* ``SearchService`` does NOT add ``delete_*`` public methods this PR:
  storage owns embedding cascade and the ABC delete hooks are reserved
  for future use (Codex Finding 3).
"""

from __future__ import annotations

import tempfile
from typing import AsyncIterator, List, Optional

import pytest_asyncio

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.memories.models import Memory
from slayer.search.retriever import RetrievalResult, Retriever
from slayer.search.service import SearchService
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
            columns=[Column(name="id", type=DataType.INT, primary_key=True)],
        ))
        yield s


class _RecordingRetriever(Retriever):
    """Records every write-hook invocation for assertions."""

    def __init__(
        self, *,
        name: str,
        upsert_warn: Optional[str] = None,
        subtree_warn: Optional[str] = None,
        ds_warn: Optional[str] = None,
        raise_on_upsert: bool = False,
        raise_on_subtree: bool = False,
        raise_on_datasource: bool = False,
    ) -> None:
        self.name = name
        self._upsert_warn = upsert_warn
        self._subtree_warn = subtree_warn
        self._ds_warn = ds_warn
        self._raise_on_upsert = raise_on_upsert
        self._raise_on_subtree = raise_on_subtree
        self._raise_on_datasource = raise_on_datasource
        self.upsert_calls: List[Memory] = []
        self.subtree_calls: List[SlayerModel] = []
        self.ds_calls: List[str] = []

    async def retrieve(self, **kwargs) -> RetrievalResult:
        return RetrievalResult()

    async def upsert_memory(self, memory: Memory) -> List[str]:
        self.upsert_calls.append(memory)
        if self._raise_on_upsert:
            raise RuntimeError(f"{self.name} boom")
        return [self._upsert_warn] if self._upsert_warn else []

    async def refresh_model_subtree(self, model: SlayerModel) -> List[str]:
        self.subtree_calls.append(model)
        if self._raise_on_subtree:
            raise RuntimeError(f"{self.name} subtree-boom")
        return [self._subtree_warn] if self._subtree_warn else []

    async def refresh_datasource(
        self, *, name: str, models: List[SlayerModel],
    ) -> List[str]:
        self.ds_calls.append(name)
        if self._raise_on_datasource:
            raise RuntimeError(f"{self.name} ds-boom")
        return [self._ds_warn] if self._ds_warn else []


async def test_default_retrievers_wired_when_none_passed(
    storage: StorageBackend,
) -> None:
    """Default factory: BM25Retriever + TantivyRetriever +
    EmbeddingRetriever, in that declaration order."""
    service = SearchService(storage=storage)
    names = [r.name for r in service.retrievers]
    assert names == ["bm25", "tantivy", "embeddings"]


async def test_custom_retrievers_replace_default(
    storage: StorageBackend,
) -> None:
    custom = [_RecordingRetriever(name="a"), _RecordingRetriever(name="b")]
    service = SearchService(storage=storage, retrievers=custom)
    assert service.retrievers is custom or list(service.retrievers) == custom
    assert [r.name for r in service.retrievers] == ["a", "b"]


async def test_upsert_memory_calls_every_retriever_in_order(
    storage: StorageBackend,
) -> None:
    r1 = _RecordingRetriever(name="r1")
    r2 = _RecordingRetriever(name="r2")
    r3 = _RecordingRetriever(name="r3")
    service = SearchService(storage=storage, retrievers=[r1, r2, r3])
    mem = Memory(id="1", learning="x", entities=[])
    await service.upsert_memory(mem)
    assert r1.upsert_calls == [mem]
    assert r2.upsert_calls == [mem]
    assert r3.upsert_calls == [mem]


async def test_refresh_model_subtree_calls_every_retriever(
    storage: StorageBackend,
) -> None:
    r1 = _RecordingRetriever(name="r1")
    r2 = _RecordingRetriever(name="r2")
    service = SearchService(storage=storage, retrievers=[r1, r2])
    model = SlayerModel(
        name="orders", sql_table="orders", data_source="mydb",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
    )
    await service.refresh_model_subtree(model)
    assert r1.subtree_calls == [model]
    assert r2.subtree_calls == [model]


async def test_refresh_datasource_calls_every_retriever(
    storage: StorageBackend,
) -> None:
    r1 = _RecordingRetriever(name="r1")
    r2 = _RecordingRetriever(name="r2")
    service = SearchService(storage=storage, retrievers=[r1, r2])
    await service.refresh_datasource(name="mydb", models=[])
    assert r1.ds_calls == ["mydb"]
    assert r2.ds_calls == ["mydb"]


async def test_warnings_aggregated_in_retriever_declaration_order(
    storage: StorageBackend,
) -> None:
    """Codex Finding 5: warning order is deterministic — it matches the
    retriever list order, not any internal scheduling."""
    r1 = _RecordingRetriever(name="r1", upsert_warn="warn-from-r1")
    r2 = _RecordingRetriever(name="r2", upsert_warn="warn-from-r2")
    r3 = _RecordingRetriever(name="r3", upsert_warn="warn-from-r3")
    service = SearchService(storage=storage, retrievers=[r1, r2, r3])
    warnings = await service.upsert_memory(
        Memory(id="1", learning="x", entities=[]),
    )
    assert warnings == ["warn-from-r1", "warn-from-r2", "warn-from-r3"]


async def test_duplicate_warnings_deduped_preserving_order(
    storage: StorageBackend,
) -> None:
    r1 = _RecordingRetriever(name="r1", upsert_warn="duplicate")
    r2 = _RecordingRetriever(name="r2", upsert_warn="duplicate")
    r3 = _RecordingRetriever(name="r3", upsert_warn="other")
    service = SearchService(storage=storage, retrievers=[r1, r2, r3])
    warnings = await service.upsert_memory(
        Memory(id="1", learning="x", entities=[]),
    )
    assert warnings == ["duplicate", "other"]


async def test_retriever_exception_isolated_and_converted_to_warning(
    storage: StorageBackend,
) -> None:
    """Codex Finding 4: an exception in retriever 1 does NOT skip
    retriever 2; the exception surfaces as a prefixed warning."""
    r1 = _RecordingRetriever(name="r1", raise_on_upsert=True)
    r2 = _RecordingRetriever(name="r2", upsert_warn="r2-still-ran")
    service = SearchService(storage=storage, retrievers=[r1, r2])
    warnings = await service.upsert_memory(
        Memory(id="1", learning="x", entities=[]),
    )
    # Both retrievers were called.
    assert len(r1.upsert_calls) == 1
    assert len(r2.upsert_calls) == 1
    # Exception became a prefixed warning naming the retriever.
    boom_warnings = [w for w in warnings if "boom" in w and "r1" in w]
    assert len(boom_warnings) == 1
    # And r2's warning survives in the returned list.
    assert "r2-still-ran" in warnings


async def test_refresh_model_subtree_warnings_ordered_and_deduped(
    storage: StorageBackend,
) -> None:
    """Codex Finding 5 (sister case): same warning-order + dedup
    contract applies to refresh_model_subtree."""
    r1 = _RecordingRetriever(name="r1", subtree_warn="dup")
    r2 = _RecordingRetriever(name="r2", subtree_warn="dup")
    r3 = _RecordingRetriever(name="r3", subtree_warn="other")
    service = SearchService(storage=storage, retrievers=[r1, r2, r3])
    model = SlayerModel(
        name="orders", sql_table="orders", data_source="mydb",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
    )
    warnings = await service.refresh_model_subtree(model)
    assert warnings == ["dup", "other"]


async def test_refresh_datasource_warnings_ordered_and_deduped(
    storage: StorageBackend,
) -> None:
    r1 = _RecordingRetriever(name="r1", ds_warn="a")
    r2 = _RecordingRetriever(name="r2", ds_warn="b")
    r3 = _RecordingRetriever(name="r3", ds_warn="a")
    service = SearchService(storage=storage, retrievers=[r1, r2, r3])
    warnings = await service.refresh_datasource(name="mydb", models=[])
    assert warnings == ["a", "b"]


async def test_refresh_model_subtree_exception_isolated(
    storage: StorageBackend,
) -> None:
    """Codex Finding 4 (sister case): exception in retriever 1's
    refresh_model_subtree must not skip retriever 2; surfaces as
    a prefixed warning."""
    r1 = _RecordingRetriever(name="r1", raise_on_subtree=True)
    r2 = _RecordingRetriever(name="r2", subtree_warn="r2-still-ran")
    service = SearchService(storage=storage, retrievers=[r1, r2])
    model = SlayerModel(
        name="orders", sql_table="orders", data_source="mydb",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
    )
    warnings = await service.refresh_model_subtree(model)
    assert len(r1.subtree_calls) == 1
    assert len(r2.subtree_calls) == 1
    boom_warnings = [
        w for w in warnings if "subtree-boom" in w and "r1" in w
    ]
    assert len(boom_warnings) == 1
    assert "r2-still-ran" in warnings


async def test_refresh_datasource_exception_isolated(
    storage: StorageBackend,
) -> None:
    r1 = _RecordingRetriever(name="r1", raise_on_datasource=True)
    r2 = _RecordingRetriever(name="r2", ds_warn="r2-still-ran")
    service = SearchService(storage=storage, retrievers=[r1, r2])
    warnings = await service.refresh_datasource(name="mydb", models=[])
    assert len(r1.ds_calls) == 1
    assert len(r2.ds_calls) == 1
    boom_warnings = [
        w for w in warnings if "ds-boom" in w and "r1" in w
    ]
    assert len(boom_warnings) == 1
    assert "r2-still-ran" in warnings


async def test_search_service_does_not_expose_delete_methods(
    storage: StorageBackend,
) -> None:
    """Codex Finding 3: this PR keeps storage-side cascade as the
    deletion path. SearchService deliberately does NOT add public
    delete_* fan-out methods. (The ABC delete hooks exist for future
    use; the orchestrator does not call them.)"""
    service = SearchService(storage=storage)
    for forbidden in ("delete_memory", "delete_model", "delete_datasource"):
        assert not hasattr(service, forbidden), (
            f"SearchService.{forbidden} should not exist this PR "
            f"(storage owns embedding cascade)."
        )
