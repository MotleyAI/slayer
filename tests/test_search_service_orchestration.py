"""Read-side orchestration tests for SearchService (DEV-1514, adapted
for DEV-1532's unified flat-results interface).

Pins the orchestrator's contract under the new Retriever ABC:

* ``valid_canonicals`` is built ONCE per search call (same object
  identity passed to every retriever).
* ``corpus`` is built ONCE per search call (when ``question`` is
  active) — asserted both via same-instance check AND patched
  builder-call count.
* Retrievers are invoked in parallel via ``asyncio.gather``.
* Warning aggregation is deterministic — declared retriever order,
  NOT gather-completion order (Codex Finding 5).
* RRF fusion runs over memory rankings from all retrievers, and over
  entity rankings from retrievers that contributed any (BM25's empty
  entity ranking contributes nothing).
* ``text_by_id`` precedence: when two retrievers supply text for the
  same memory id, the FIRST-DECLARED retriever wins (Codex Finding 6).
* Cap stability (DEV-1414 carried into DEV-1532): increasing
  ``max_results`` cannot reorder or remove items from the head of the
  list — a wider cap just appends.
* Recency fallback: when neither channel is active, retrievers are
  NOT invoked.
* ``all_memories`` and ``datasource`` are forwarded to each retriever.
"""

from __future__ import annotations

import asyncio
import tempfile
from typing import AsyncIterator, List, Optional, Set
from unittest.mock import patch

import pytest_asyncio

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.memories.models import Memory
from slayer.search.index import Corpus
from slayer.search.retriever import RetrievalResult, Retriever
from slayer.search.service import SearchService
from slayer.storage.base import StorageBackend
from slayer.storage.yaml_storage import YAMLStorage


@pytest_asyncio.fixture
async def seeded_storage() -> AsyncIterator[StorageBackend]:
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
        for i in range(1, 6):
            await s.save_memory(
                learning=f"learning note {i}",
                entities=["mydb.orders.amount"],
            )
        yield s


class _CapturingRetriever(Retriever):
    """Captures every argument passed to ``retrieve`` so tests can
    assert the orchestrator built each piece of shared state ONCE per
    search call and forwarded every argument to every retriever."""

    def __init__(
        self, *,
        name: str,
        memory_ranking: Optional[List[str]] = None,
        entity_ranking: Optional[List[str]] = None,
        text_by_id: Optional[dict] = None,
        warning: Optional[str] = None,
        delay_s: float = 0.0,
    ) -> None:
        self.name = name
        self._memory_ranking = memory_ranking or []
        self._entity_ranking = entity_ranking or []
        self._text_by_id = text_by_id or {}
        self._warning = warning
        self._delay_s = delay_s
        # Capture by identity (id()) so we can prove the SAME object
        # was passed to every retriever, not merely equal copies.
        self.captured_valid_canonicals_ids: List[int] = []
        self.captured_valid_canonicals: List[Set[str]] = []
        self.captured_corpus_ids: List[int] = []
        self.captured_all_memories_ids: List[int] = []
        self.captured_all_memories_lens: List[int] = []
        self.captured_datasources: List[Optional[str]] = []
        self.retrieve_call_count = 0

    async def retrieve(
        self, *,
        query_entities: List[str],
        question: Optional[str],
        all_memories: List[Memory],
        valid_canonicals: set,
        corpus: Optional[Corpus],
        datasource: Optional[str],
    ) -> RetrievalResult:
        self.retrieve_call_count += 1
        self.captured_valid_canonicals_ids.append(id(valid_canonicals))
        self.captured_valid_canonicals.append(set(valid_canonicals))
        self.captured_all_memories_ids.append(id(all_memories))
        self.captured_all_memories_lens.append(len(all_memories))
        self.captured_datasources.append(datasource)
        if corpus is not None:
            self.captured_corpus_ids.append(id(corpus))
        if self._delay_s > 0:
            await asyncio.sleep(self._delay_s)
        return RetrievalResult(
            memory_ranking=list(self._memory_ranking),
            entity_ranking=list(self._entity_ranking),
            text_by_id=dict(self._text_by_id),
            warnings=[self._warning] if self._warning else [],
        )


async def test_valid_canonicals_built_once_and_shared_by_identity(
    seeded_storage: StorageBackend,
) -> None:
    """Codex Finding 1: assert SAME OBJECT identity, not just equal
    contents. A bug that rebuilds an equal set per retriever would
    pass an equality test."""
    r1 = _CapturingRetriever(name="r1")
    r2 = _CapturingRetriever(name="r2")
    service = SearchService(storage=seeded_storage, retrievers=[r1, r2])
    await service.search(
        entities=["mydb.orders.amount"],
        question="amount cents",
        max_results=12,
    )
    # Same object identity for both retrievers' captured set.
    assert r1.captured_valid_canonicals_ids, (
        "r1.retrieve was not invoked"
    )
    assert r2.captured_valid_canonicals_ids, (
        "r2.retrieve was not invoked"
    )
    assert r1.captured_valid_canonicals_ids[0] == \
        r2.captured_valid_canonicals_ids[0]
    # And the set is non-empty.
    assert r1.captured_valid_canonicals[0]


async def test_corpus_built_once_when_question_active_via_patched_builder(
    seeded_storage: StorageBackend,
) -> None:
    """Codex Finding 2: pin that ``build_in_memory_corpus`` is called
    EXACTLY ONCE per search call. Same-instance check alone wouldn't
    catch a bug that builds two corpora and gives one to all retrievers."""
    r1 = _CapturingRetriever(name="r1")
    r2 = _CapturingRetriever(name="r2")
    service = SearchService(storage=seeded_storage, retrievers=[r1, r2])
    with patch(
        "slayer.search.service.build_in_memory_corpus",
        wraps=__import__(
            "slayer.search.index", fromlist=["build_in_memory_corpus"],
        ).build_in_memory_corpus,
    ) as build_spy:
        await service.search(
            entities=None,
            question="amount",
            max_results=12,
        )
    assert build_spy.call_count == 1
    # And both retrievers received the same corpus instance.
    assert r1.captured_corpus_ids
    assert r1.captured_corpus_ids == r2.captured_corpus_ids


async def test_retrievers_invoked_in_parallel_via_gather(
    seeded_storage: StorageBackend,
) -> None:
    """If retriever calls were sequential, wall-clock would be ≈3*delay.
    Parallel via gather: ≈1*delay."""
    delay = 0.2
    r1 = _CapturingRetriever(name="r1", delay_s=delay)
    r2 = _CapturingRetriever(name="r2", delay_s=delay)
    r3 = _CapturingRetriever(name="r3", delay_s=delay)
    service = SearchService(storage=seeded_storage, retrievers=[r1, r2, r3])
    start = asyncio.get_event_loop().time()
    await service.search(
        entities=["mydb.orders.amount"], question="amount",
        max_results=12,
    )
    elapsed = asyncio.get_event_loop().time() - start
    # Should be roughly one delay (parallel), not three (sequential).
    assert elapsed < delay * 2.5, (
        f"retrievers appear sequential — elapsed={elapsed:.3f}s for "
        f"3*{delay}s delays."
    )


async def test_warning_order_matches_retriever_declaration_not_completion(
    seeded_storage: StorageBackend,
) -> None:
    """Codex Finding 5: even when retrievers complete in different
    order (here, retriever 1 finishes LAST), warnings appear in
    retriever declaration order."""
    r1 = _CapturingRetriever(name="r1", warning="w-r1", delay_s=0.2)
    r2 = _CapturingRetriever(name="r2", warning="w-r2", delay_s=0.0)
    r3 = _CapturingRetriever(name="r3", warning="w-r3", delay_s=0.1)
    service = SearchService(storage=seeded_storage, retrievers=[r1, r2, r3])
    response = await service.search(
        entities=["mydb.orders.amount"], question="amount",
        max_results=12,
    )
    # Find the retriever warnings within the response (other warnings —
    # e.g. lenient resolution / stale query — may also appear, in their
    # own deterministic positions).
    retriever_warnings = [
        w for w in response.warnings if w in {"w-r1", "w-r2", "w-r3"}
    ]
    assert retriever_warnings == ["w-r1", "w-r2", "w-r3"]


async def test_text_by_id_precedence_first_declared_wins(
    seeded_storage: StorageBackend,
) -> None:
    """Codex Finding 6: when two retrievers populate text_by_id for
    the same memory id, the FIRST-declared one wins. Today only tantivy
    populates text_by_id; this rule is forward-compatible."""
    # All memory ids in seeded_storage are "1".."5".
    r1 = _CapturingRetriever(
        name="r1",
        memory_ranking=["1"],
        text_by_id={"1": "r1-text"},
    )
    r2 = _CapturingRetriever(
        name="r2",
        memory_ranking=["1"],
        text_by_id={"1": "r2-text"},
    )
    service = SearchService(storage=seeded_storage, retrievers=[r1, r2])
    response = await service.search(
        entities=["mydb.orders.amount"], question="amount",
        max_results=12,
    )
    # Memory id 1 surfaced with r1's text (first-declared).
    matched = [
        h for h in response.results
        if h.kind == "memory" and h.id == "1"
    ]
    assert matched, "memory 1 should appear in results"
    assert matched[0].text == "r1-text"


async def test_cap_stability_under_max_results_changes(
    seeded_storage: StorageBackend,
) -> None:
    """DEV-1414 / DEV-1532: a wider ``max_results`` cap must never
    reorder or remove items from the head of the list. A narrower cap
    is exactly the head prefix of the wider cap's result."""
    r1 = _CapturingRetriever(
        name="r1",
        memory_ranking=["1", "2", "3", "4", "5"],
        entity_ranking=["mydb.orders", "mydb.orders.amount", "mydb"],
    )
    service = SearchService(storage=seeded_storage, retrievers=[r1])
    response_narrow = await service.search(
        entities=["mydb.orders.amount"], question="amount",
        max_results=3,
    )
    response_wide = await service.search(
        entities=["mydb.orders.amount"], question="amount",
        max_results=12,
    )
    # Narrow == prefix of wide.
    narrow_ids = [(h.kind, h.id) for h in response_narrow.results]
    wide_ids = [(h.kind, h.id) for h in response_wide.results]
    assert narrow_ids == wide_ids[: len(narrow_ids)]


async def test_each_retriever_called_exactly_once_per_search(
    seeded_storage: StorageBackend,
) -> None:
    r1 = _CapturingRetriever(name="r1")
    r2 = _CapturingRetriever(name="r2")
    r3 = _CapturingRetriever(name="r3")
    service = SearchService(storage=seeded_storage, retrievers=[r1, r2, r3])
    await service.search(
        entities=["mydb.orders.amount"], question="amount",
        max_results=12,
    )
    assert r1.retrieve_call_count == 1
    assert r2.retrieve_call_count == 1
    assert r3.retrieve_call_count == 1


async def test_recency_fallback_does_not_invoke_retrievers(
    seeded_storage: StorageBackend,
) -> None:
    """Codex Finding 3: when neither channel is active (no entities,
    no query, no question), search returns the recency fallback WITHOUT
    invoking any retriever's ``retrieve`` method. Retrieval is
    expensive (embedding + tantivy + BM25); the orchestrator must
    short-circuit before fan-out."""
    r1 = _CapturingRetriever(name="r1")
    r2 = _CapturingRetriever(name="r2")
    r3 = _CapturingRetriever(name="r3")
    service = SearchService(storage=seeded_storage, retrievers=[r1, r2, r3])
    response = await service.search(
        entities=None, query=None, question=None,
        max_results=12,
    )
    assert r1.retrieve_call_count == 0
    assert r2.retrieve_call_count == 0
    assert r3.retrieve_call_count == 0
    # Recency fallback still returns memories from storage.
    assert any(h.kind == "memory" for h in response.results)


async def test_all_memories_and_datasource_forwarded_to_every_retriever(
    seeded_storage: StorageBackend,
) -> None:
    """Codex Finding 9: prove the orchestrator passes the SAME
    ``all_memories`` list (by identity) and the SAME ``datasource``
    value to every retriever — a bug that loses the datasource filter
    or hands out empty memory lists would surface here."""
    r1 = _CapturingRetriever(name="r1")
    r2 = _CapturingRetriever(name="r2")
    service = SearchService(storage=seeded_storage, retrievers=[r1, r2])
    # Run scoped to "mydb" — datasource filter active.
    await service.search(
        entities=["mydb.orders.amount"],
        question="amount",
        datasource="mydb",
        max_results=12,
    )
    # Same all_memories list identity for both retrievers.
    assert r1.captured_all_memories_ids, "r1.retrieve was not invoked"
    assert r2.captured_all_memories_ids, "r2.retrieve was not invoked"
    assert r1.captured_all_memories_ids[0] == \
        r2.captured_all_memories_ids[0]
    # Both saw the 5 seeded memories.
    assert r1.captured_all_memories_lens, "r1.retrieve was not invoked"
    assert r1.captured_all_memories_lens[0] == 5
    # Datasource forwarded verbatim.
    assert r1.captured_datasources == ["mydb"]
    assert r2.captured_datasources == ["mydb"]


async def test_datasource_none_forwarded_as_none(
    seeded_storage: StorageBackend,
) -> None:
    r1 = _CapturingRetriever(name="r1")
    service = SearchService(storage=seeded_storage, retrievers=[r1])
    await service.search(
        entities=["mydb.orders.amount"],
        question="amount",
        datasource=None,
        max_results=12,
    )
    assert r1.captured_datasources == [None]
