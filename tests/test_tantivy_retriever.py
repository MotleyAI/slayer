"""Unit tests for TantivyRetriever (DEV-1514).

Pins the Tantivy retriever's contract:
* Two kind-filtered queries (memory + entity) run inside ONE ``retrieve``
  call, sequentially against the same in-memory ``tantivy.Index`` — never
  concurrently (Codex review, Finding 2).
* Populates ``text_by_id`` for memory hits so the orchestrator's
  ``MemoryHit.text`` fallback works.
* Empty result when ``corpus`` is ``None`` or ``question`` is blank.
* All write hooks no-op (in-memory rebuild stays this PR — persistent
  tantivy is a future PR that overrides the hooks).
"""

from __future__ import annotations

import time
from unittest.mock import patch

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.memories.models import Memory
from slayer.search.index import (
    Corpus,
    IndexHit,
    build_in_memory_corpus,
)
from slayer.search.retrievers.tantivy import TantivyRetriever


def _model(name: str = "orders") -> SlayerModel:
    return SlayerModel(
        name=name,
        sql_table=name,
        data_source="mydb",
        description=f"{name} fact table",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE,
                   description="line amount in cents"),
        ],
    )


def _memories() -> list[Memory]:
    return [
        Memory(id="1", learning="amount column is in cents",
               entities=["mydb.orders.amount"]),
        Memory(id="2", learning="orders model holds checkout rows",
               entities=["mydb.orders"]),
    ]


def _build_corpus() -> Corpus:
    return build_in_memory_corpus(
        memories=_memories(),
        models=[_model()],
        datasources=["mydb"],
    )


async def test_returns_empty_when_corpus_is_none() -> None:
    retriever = TantivyRetriever()
    result = await retriever.retrieve(
        query_entities=[],
        question="amount",
        all_memories=_memories(),
        valid_canonicals=set(),
        corpus=None,
        datasource=None,
    )
    assert result.memory_ranking == []
    assert result.entity_ranking == []
    assert result.text_by_id == {}


async def test_returns_empty_when_question_blank() -> None:
    retriever = TantivyRetriever()
    corpus = _build_corpus()
    for question in (None, "", "   "):
        result = await retriever.retrieve(
            query_entities=[],
            question=question,
            all_memories=_memories(),
            valid_canonicals=set(),
            corpus=corpus,
            datasource=None,
        )
        assert result.memory_ranking == []
        assert result.entity_ranking == []


async def test_question_matches_memory_and_entity_docs() -> None:
    """A question that hits the rendered text of both a memory and
    an entity surfaces both rankings from a single ``retrieve`` call."""
    retriever = TantivyRetriever()
    corpus = _build_corpus()
    result = await retriever.retrieve(
        query_entities=[],
        question="amount cents",
        all_memories=_memories(),
        valid_canonicals=set(),
        corpus=corpus,
        datasource=None,
    )
    assert "1" in result.memory_ranking  # memory tagged on amount
    assert any(
        canonical.endswith(".amount") for canonical in result.entity_ranking
    )


async def test_text_by_id_populated_for_memory_hits() -> None:
    retriever = TantivyRetriever()
    corpus = _build_corpus()
    result = await retriever.retrieve(
        query_entities=[],
        question="cents",
        all_memories=_memories(),
        valid_canonicals=set(),
        corpus=corpus,
        datasource=None,
    )
    # Every memory id surfaced by tantivy must appear in text_by_id.
    for mem_id in result.memory_ranking:
        assert mem_id in result.text_by_id
        assert isinstance(result.text_by_id[mem_id], str)
        assert result.text_by_id[mem_id]


async def test_two_kind_filtered_queries_run_sequentially() -> None:
    """Codex Finding 2 regression: TantivyRetriever must not run both
    queries concurrently against the same ``tantivy.Index``. The two
    ``search_index`` calls happen one after the other inside the
    single ``retrieve`` call.
    """
    retriever = TantivyRetriever()
    corpus = _build_corpus()
    call_kind_filters: list[object] = []

    def fake_search_index(*, index, question, limit, kind_filter=None,
                          exclude_kind=None, fields=None):
        call_kind_filters.append((kind_filter, exclude_kind))
        return []

    with patch(
        "slayer.search.retrievers.tantivy.search_index",
        side_effect=fake_search_index,
    ):
        await retriever.retrieve(
            query_entities=[],
            question="amount",
            all_memories=_memories(),
            valid_canonicals=set(),
            corpus=corpus,
            datasource=None,
        )

    # One call with kind_filter="memory", one with exclude_kind="memory".
    assert ("memory", None) in call_kind_filters
    assert (None, "memory") in call_kind_filters
    assert len(call_kind_filters) == 2


async def test_two_queries_do_not_overlap_in_time() -> None:
    """Codex Finding 8: stronger sequential check — the second call
    to ``search_index`` must not start while the first is still
    executing. The fake holds an in-flight flag and sleeps briefly,
    widening the window; if the retriever wrapped the two calls in
    ``asyncio.gather`` + ``to_thread`` (or similar), the second call
    would observe the flag set and fail the test.
    """
    retriever = TantivyRetriever()
    corpus = _build_corpus()
    state = {"in_flight": 0, "overlap_seen": False}

    def fake_search_index(*, index, question, limit, kind_filter=None,
                          exclude_kind=None, fields=None):
        state["in_flight"] += 1
        if state["in_flight"] > 1:
            state["overlap_seen"] = True
        time.sleep(0.05)  # widens the in-flight window
        state["in_flight"] -= 1
        return []

    with patch(
        "slayer.search.retrievers.tantivy.search_index",
        side_effect=fake_search_index,
    ):
        await retriever.retrieve(
            query_entities=[],
            question="amount",
            all_memories=_memories(),
            valid_canonicals=set(),
            corpus=corpus,
            datasource=None,
        )
    assert not state["overlap_seen"], (
        "TantivyRetriever ran the two kind-filtered queries concurrently"
    )


async def test_memory_hits_with_no_memory_id_are_skipped() -> None:
    """Defensive: a tantivy memory hit without ``memory_id`` set must
    not pollute the ranking. Today every memory hit has the id, but the
    retriever must remain robust."""
    retriever = TantivyRetriever()
    corpus = _build_corpus()

    def fake_search_index(*, index, question, limit, kind_filter=None,
                          exclude_kind=None, fields=None):
        if kind_filter == "memory":
            return [
                IndexHit(
                    id="memory:1", kind="memory", canonical="1",
                    text="hit text", score=1.0, memory_id="1",
                ),
                IndexHit(
                    id="memory:none", kind="memory", canonical="",
                    text="orphan", score=0.5, memory_id=None,
                ),
            ]
        return []

    with patch(
        "slayer.search.retrievers.tantivy.search_index",
        side_effect=fake_search_index,
    ):
        result = await retriever.retrieve(
            query_entities=[],
            question="any",
            all_memories=_memories(),
            valid_canonicals=set(),
            corpus=corpus,
            datasource=None,
        )

    assert result.memory_ranking == ["1"]
    assert result.text_by_id == {"1": "hit text"}


async def test_default_no_op_write_hooks() -> None:
    retriever = TantivyRetriever()
    memory = Memory(id="1", learning="x", entities=[])
    model = _model()
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
