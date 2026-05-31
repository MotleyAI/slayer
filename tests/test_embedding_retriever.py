"""Unit tests for EmbeddingRetriever (DEV-1514, supersedes
test_embeddings_service.py).

Combines:

* The behavioural tests originally in `tests/test_embeddings_service.py`
  (renamed for the new module + method names: ``refresh_memory`` is now
  the ABC-conformant ``upsert_memory``).
* The Codex Finding 1 regression pin: ``retrieve(...)`` must run
  ``fetch_corpus`` ONCE, ``embed_question`` ONCE, and the dim-check
  ONCE per call. The two ranking partitions (memory + entity) share
  the same setup; splitting them across two methods would double the
  litellm call cost.

Storage is a real YAMLStorage in tempdir; ``embed_batch`` /
``embed_query`` are mocked via ``monkeypatch``.
"""

from __future__ import annotations

import tempfile
from typing import List, Optional, cast

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Aggregation, Column, ModelMeasure, SlayerModel
from slayer.embeddings import client as embedding_client
from slayer.memories.models import Memory
from slayer.search.index import build_in_memory_corpus
from slayer.search.retrievers.embeddings import EmbeddingRetriever
from slayer.storage.base import StorageBackend
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def storage():
    with tempfile.TemporaryDirectory() as tmp:
        yield YAMLStorage(base_dir=tmp)


@pytest.fixture
def stub_available(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)


class _RecordingEmbedBatch:
    def __init__(self) -> None:
        self.calls: List[List[str]] = []
        self.override_none: set = set()

    async def __call__(
        self, texts: List[str], *, model: Optional[str] = None,
    ) -> List[Optional[List[float]]]:
        self.calls.append(list(texts))
        out: List[Optional[List[float]]] = []
        for global_idx, text in enumerate(texts):
            if (len(self.calls) - 1, global_idx) in self.override_none:
                out.append(None)
            else:
                v = (hash(text) & 0xFF) / 255.0
                out.append([v, v + 0.1, v + 0.2])
        return out


@pytest.fixture
def recording_embed(monkeypatch: pytest.MonkeyPatch) -> _RecordingEmbedBatch:
    rec = _RecordingEmbedBatch()
    monkeypatch.setattr(
        "slayer.search.retrievers.embeddings.embed_batch", rec,
    )
    return rec


def _make_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="dsx",
        description="orders fact table",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE, description="cents"),
            Column(name="secret", type=DataType.TEXT, hidden=True),
        ],
        measures=[
            ModelMeasure(name="rev", formula="amount:sum"),
        ],
        aggregations=[
            Aggregation(name="my_agg", formula="SUM({x})"),
        ],
    )


# ---------------------------------------------------------------------------
# upsert_memory (migrated from refresh_memory)
# ---------------------------------------------------------------------------


async def test_upsert_memory_silent_when_channel_unavailable(
    storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Channel disabled (extra missing or no API key): stay silent on
    the write path — no per-call warning bubbles up for a "feature
    not configured" case."""
    monkeypatch.setattr(embedding_client, "is_available", lambda: False)
    retriever = EmbeddingRetriever(storage=storage, model_name="openai/x")
    memory = Memory(id="1", learning="hello", entities=["e1"])
    warnings = await retriever.upsert_memory(memory)
    assert warnings == []
    assert await storage.get_embedding(
        canonical_id="memory:1", embedding_model_name="openai/x",
    ) is None


async def test_upsert_memory_persists_row(
    storage: YAMLStorage,
    stub_available: None,
    recording_embed: _RecordingEmbedBatch,
) -> None:
    retriever = EmbeddingRetriever(storage=storage, model_name="openai/x")
    memory = Memory(id="1", learning="hello world", entities=["e1"])
    warnings = await retriever.upsert_memory(memory)
    assert warnings == []
    persisted = await storage.get_embedding(
        canonical_id="memory:1", embedding_model_name="openai/x",
    )
    assert persisted is not None
    assert persisted.entity_kind == "memory"
    assert persisted.embedding_model_name == "openai/x"
    assert len(persisted.embedding) == 3
    assert len(recording_embed.calls) == 1
    assert len(recording_embed.calls[0]) == 1


async def test_upsert_memory_skips_when_hash_matches(
    storage: YAMLStorage,
    stub_available: None,
    recording_embed: _RecordingEmbedBatch,
) -> None:
    retriever = EmbeddingRetriever(storage=storage, model_name="openai/x")
    memory = Memory(id="42", learning="unchanged", entities=["e1"])
    await retriever.upsert_memory(memory)
    await retriever.upsert_memory(memory)
    assert len(recording_embed.calls) == 1


async def test_upsert_memory_reembeds_on_text_change(
    storage: YAMLStorage,
    stub_available: None,
    recording_embed: _RecordingEmbedBatch,
) -> None:
    retriever = EmbeddingRetriever(storage=storage, model_name="openai/x")
    memory_a = Memory(id="7", learning="alpha", entities=["e1"])
    memory_b = Memory(id="7", learning="alpha-changed", entities=["e1"])
    await retriever.upsert_memory(memory_a)
    await retriever.upsert_memory(memory_b)
    assert len(recording_embed.calls) == 2
    persisted = await storage.get_embedding(
        canonical_id="memory:7", embedding_model_name="openai/x",
    )
    assert persisted is not None
    assert persisted.content_hash != ""


# ---------------------------------------------------------------------------
# refresh_model_subtree (kept name — aligns with ABC)
# ---------------------------------------------------------------------------


async def test_refresh_model_subtree_batches_all_children(
    storage: YAMLStorage,
    stub_available: None,
    recording_embed: _RecordingEmbedBatch,
) -> None:
    retriever = EmbeddingRetriever(storage=storage, model_name="openai/x")
    model = _make_model()
    warnings = await retriever.refresh_model_subtree(model)
    assert warnings == []
    assert len(recording_embed.calls) == 1
    # model + 2 visible columns + 1 named measure + 1 agg = 5; hidden col skipped.
    assert len(recording_embed.calls[0]) == 5

    listed = await storage.list_embeddings(embedding_model_name="openai/x")
    canonicals = {r.canonical_id for r in listed}
    assert canonicals == {
        "dsx.orders",
        "dsx.orders.id",
        "dsx.orders.amount",
        "dsx.orders.rev",
        "dsx.orders.my_agg",
    }


async def test_refresh_model_subtree_hash_skips_unchanged(
    storage: YAMLStorage,
    stub_available: None,
    recording_embed: _RecordingEmbedBatch,
) -> None:
    retriever = EmbeddingRetriever(storage=storage, model_name="openai/x")
    model = _make_model()
    await retriever.refresh_model_subtree(model)
    await retriever.refresh_model_subtree(model)
    assert len(recording_embed.calls) == 1


async def test_refresh_model_subtree_per_entry_failure_warns(
    storage: YAMLStorage,
    stub_available: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def partial_failure(  # NOSONAR(S7503)
        texts: List[str], *, model: Optional[str] = None,
    ) -> List[Optional[List[float]]]:
        out: List[Optional[List[float]]] = []
        for i, _ in enumerate(texts):
            out.append(None if i == 0 else [0.1, 0.2, 0.3])
        return out

    monkeypatch.setattr(
        "slayer.search.retrievers.embeddings.embed_batch", partial_failure,
    )
    retriever = EmbeddingRetriever(storage=storage, model_name="openai/x")
    model = _make_model()
    warnings = await retriever.refresh_model_subtree(model)
    assert len(warnings) == 1
    assert "embedding refresh failed" in warnings[0]
    listed = await storage.list_embeddings(embedding_model_name="openai/x")
    assert len(listed) == 4


async def test_refresh_model_subtree_hidden_model_short_circuits(
    storage: YAMLStorage,
    stub_available: None,
    recording_embed: _RecordingEmbedBatch,
) -> None:
    retriever = EmbeddingRetriever(storage=storage, model_name="openai/x")
    model = _make_model()
    model.hidden = True
    warnings = await retriever.refresh_model_subtree(model)
    assert warnings == []
    assert recording_embed.calls == []
    assert await storage.list_embeddings(
        embedding_model_name="openai/x",
    ) == []


# ---------------------------------------------------------------------------
# fetch_corpus + model_name change semantics
# ---------------------------------------------------------------------------


async def test_fetch_corpus_filters_by_active_model_name(
    storage: YAMLStorage,
    stub_available: None,
    recording_embed: _RecordingEmbedBatch,
) -> None:
    retriever_a = EmbeddingRetriever(storage=storage, model_name="openai/a")
    retriever_b = EmbeddingRetriever(storage=storage, model_name="openai/b")
    memory = Memory(id="1", learning="anything", entities=["e1"])

    await retriever_a.upsert_memory(memory)
    await retriever_b.upsert_memory(memory)

    rows_a = await retriever_a.fetch_corpus()
    rows_b = await retriever_b.fetch_corpus()
    assert len(rows_a) == 1 and rows_a[0].embedding_model_name == "openai/a"
    assert len(rows_b) == 1 and rows_b[0].embedding_model_name == "openai/b"


# ---------------------------------------------------------------------------
# Batched storage hot-path (DEV-1405)
# ---------------------------------------------------------------------------


class _CountingStorage:
    def __init__(self, inner: YAMLStorage) -> None:
        self._inner = inner
        self.get_many_calls = 0
        self.save_many_calls = 0
        self.single_get_calls = 0
        self.single_save_calls = 0

    async def get_embeddings_for_canonical_ids(
        self, *, canonical_ids, embedding_model_name,
    ):
        self.get_many_calls += 1
        return await self._inner.get_embeddings_for_canonical_ids(
            canonical_ids=canonical_ids,
            embedding_model_name=embedding_model_name,
        )

    async def save_embeddings(self, rows) -> None:
        self.save_many_calls += 1
        await self._inner.save_embeddings(rows)

    async def get_embedding(self, **kwargs):
        self.single_get_calls += 1
        return await self._inner.get_embedding(**kwargs)

    async def save_embedding(self, row) -> None:
        self.single_save_calls += 1
        await self._inner.save_embedding(row)

    def __getattr__(self, item):
        return getattr(self._inner, item)


async def test_apply_pending_uses_batched_storage_calls(
    storage: YAMLStorage,
    stub_available: None,
    recording_embed: _RecordingEmbedBatch,
) -> None:
    counting = _CountingStorage(storage)
    retriever = EmbeddingRetriever(
        storage=cast(StorageBackend, counting), model_name="openai/x",
    )
    model = _make_model()
    await retriever.refresh_model_subtree(model)

    assert counting.get_many_calls == 1
    assert counting.save_many_calls == 1
    assert counting.single_get_calls == 0
    assert counting.single_save_calls == 0


async def test_apply_pending_persists_partial_batch_on_some_embed_failures(
    storage: YAMLStorage,
    stub_available: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def partial_failure(  # NOSONAR(S7503)
        texts: List[str], *, model: Optional[str] = None,
    ) -> List[Optional[List[float]]]:
        return [None] + [[0.1, 0.2, 0.3]] * (len(texts) - 1)

    monkeypatch.setattr(
        "slayer.search.retrievers.embeddings.embed_batch", partial_failure,
    )
    counting = _CountingStorage(storage)
    retriever = EmbeddingRetriever(
        storage=cast(StorageBackend, counting), model_name="openai/x",
    )
    model = _make_model()
    warnings = await retriever.refresh_model_subtree(model)

    assert len(warnings) == 1
    assert counting.save_many_calls == 1
    listed = await storage.list_embeddings(embedding_model_name="openai/x")
    assert len(listed) == 4


# ---------------------------------------------------------------------------
# retrieve(...) — single-call setup (Codex Finding 1 regression)
# ---------------------------------------------------------------------------


class _CallCountingEmbeddingRetriever(EmbeddingRetriever):
    """Subclass that records how many times ``fetch_corpus`` and
    ``embed_question`` are invoked. Used to pin the "one of each per
    ``retrieve`` call" contract (Codex Finding 1)."""

    def __init__(self, *, storage, model_name) -> None:
        super().__init__(storage=storage, model_name=model_name)
        self.fetch_corpus_calls = 0
        self.embed_question_calls = 0

    async def fetch_corpus(self):  # type: ignore[override]
        self.fetch_corpus_calls += 1
        return await super().fetch_corpus()

    async def embed_question(self, question: str):  # type: ignore[override]
        self.embed_question_calls += 1
        return await super().embed_question(question)


async def test_retrieve_does_one_fetch_corpus_and_one_embed_question(
    storage: YAMLStorage,
    stub_available: None,
    recording_embed: _RecordingEmbedBatch,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex Finding 1: a single ``retrieve`` call must do exactly ONE
    ``fetch_corpus`` and ONE ``embed_question`` round-trip, regardless of
    how many ranking partitions (memory + entity) it returns. Splitting
    these across separate methods would double the litellm cost.
    """
    # Pre-populate one memory + one entity row so the channel produces both
    # rankings in a single retrieve call.
    retriever_for_setup = EmbeddingRetriever(
        storage=storage, model_name="openai/x",
    )
    memory = Memory(id="1", learning="hello cents",
                    entities=["dsx.orders.amount"])
    model = _make_model()
    await retriever_for_setup.upsert_memory(memory)
    await retriever_for_setup.refresh_model_subtree(model)

    async def fake_query_embed(question, *, model=None):
        return [0.1, 0.2, 0.3]

    monkeypatch.setattr(
        "slayer.embeddings.client.embed_query", fake_query_embed,
    )

    # Now run retrieve and assert call counts.
    retriever = _CallCountingEmbeddingRetriever(
        storage=storage, model_name="openai/x",
    )
    # Minimal corpus with the relevant canonicals so the live-canonical
    # filter inside retrieve does not drop everything.
    corpus = build_in_memory_corpus(
        memories=[memory], models=[model], datasources=["dsx"],
    )
    result = await retriever.retrieve(
        query_entities=[],
        question="cents",
        all_memories=[memory],
        valid_canonicals={
            "dsx", "dsx.orders", "dsx.orders.amount", "memory:1",
        },
        corpus=corpus,
        datasource=None,
    )
    assert retriever.fetch_corpus_calls == 1
    assert retriever.embed_question_calls == 1
    # Both partitions populated from the single setup.
    assert result.memory_ranking
    assert result.entity_ranking


async def test_retrieve_returns_empty_when_question_blank(
    storage: YAMLStorage,
    stub_available: None,
) -> None:
    retriever = EmbeddingRetriever(storage=storage, model_name="openai/x")
    for question in (None, "", "   "):
        result = await retriever.retrieve(
            query_entities=[],
            question=question,
            all_memories=[],
            valid_canonicals=set(),
            corpus=None,
            datasource=None,
        )
        assert result.memory_ranking == []
        assert result.entity_ranking == []


async def test_retrieve_surfaces_dim_mismatch_warning(
    storage: YAMLStorage,
    stub_available: None,
    monkeypatch: pytest.MonkeyPatch,
    recording_embed: _RecordingEmbedBatch,
) -> None:
    """Dim-mismatch (query vec dim != stored corpus dim) skips ranking
    and surfaces a single warning, with no per-partition duplication."""
    # Persist a row with dim=3.
    retriever_for_setup = EmbeddingRetriever(
        storage=storage, model_name="openai/x",
    )
    memory = Memory(id="1", learning="x", entities=[])
    await retriever_for_setup.upsert_memory(memory)

    async def fake_query_embed(question, *, model=None):
        return [0.1, 0.2]  # dim=2; mismatch vs persisted dim=3

    monkeypatch.setattr(
        "slayer.embeddings.client.embed_query", fake_query_embed,
    )

    retriever = EmbeddingRetriever(storage=storage, model_name="openai/x")
    corpus = build_in_memory_corpus(
        memories=[memory], models=[], datasources=[],
    )
    result = await retriever.retrieve(
        query_entities=[],
        question="cents",
        all_memories=[memory],
        valid_canonicals={"memory:1"},
        corpus=corpus,
        datasource=None,
    )
    assert result.memory_ranking == []
    assert result.entity_ranking == []
    # Exactly one warning about the dim mismatch — not duplicated per partition.
    dim_warnings = [w for w in result.warnings if "dim" in w.lower()]
    assert len(dim_warnings) == 1


# ---------------------------------------------------------------------------
# Delete hooks (no-op this PR — storage owns cascade)
# ---------------------------------------------------------------------------


async def test_delete_memory_hook_is_no_op_today(
    storage: YAMLStorage,
    stub_available: None,
    recording_embed: _RecordingEmbedBatch,
) -> None:
    """Storage owns embedding-row cascade transactionally with the row
    delete; EmbeddingRetriever.delete_* hooks default to ABC no-op.
    The hooks exist on the ABC for future use (persistent tantivy
    will override) but must not touch storage today."""
    retriever = EmbeddingRetriever(storage=storage, model_name="openai/x")
    memory = Memory(id="1", learning="hello", entities=[])
    await retriever.upsert_memory(memory)
    pre = await storage.list_embeddings(embedding_model_name="openai/x")
    assert len(pre) == 1

    # Calling the no-op delete hook must NOT remove the embedding row.
    await retriever.delete_memory("1")
    post = await storage.list_embeddings(embedding_model_name="openai/x")
    assert len(post) == 1


async def test_delete_model_hook_is_no_op_today(
    storage: YAMLStorage,
    stub_available: None,
    recording_embed: _RecordingEmbedBatch,
) -> None:
    """Codex Finding 6 (sister case): delete_model hook leaves
    embedding rows intact — storage owns the cascade."""
    retriever = EmbeddingRetriever(storage=storage, model_name="openai/x")
    model = _make_model()
    await retriever.refresh_model_subtree(model)
    pre = await storage.list_embeddings(embedding_model_name="openai/x")
    assert len(pre) > 0

    await retriever.delete_model(data_source="dsx", name="orders")
    post = await storage.list_embeddings(embedding_model_name="openai/x")
    assert {r.canonical_id for r in post} == {r.canonical_id for r in pre}


async def test_delete_datasource_hook_is_no_op_today(
    storage: YAMLStorage,
    stub_available: None,
    recording_embed: _RecordingEmbedBatch,
) -> None:
    retriever = EmbeddingRetriever(storage=storage, model_name="openai/x")
    model = _make_model()
    await retriever.refresh_datasource(name="dsx", models=[model])
    pre = await storage.list_embeddings(embedding_model_name="openai/x")
    assert len(pre) > 0

    await retriever.delete_datasource("dsx")
    post = await storage.list_embeddings(embedding_model_name="openai/x")
    assert {r.canonical_id for r in post} == {r.canonical_id for r in pre}
