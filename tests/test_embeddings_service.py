"""Unit tests for ``slayer.embeddings.service.EmbeddingService`` (DEV-1386).

Verifies the refresh contract: hash-skip on no-op, batch embedding for
model subtrees, per-entry failure tolerance, and the missing-extra
warning. Storage is a real YAMLStorage in tempdir; litellm is mocked
via ``monkeypatch`` on ``embed_batch``.
"""

from __future__ import annotations

import tempfile
from typing import List, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Aggregation, Column, ModelMeasure, SlayerModel
from slayer.embeddings import client as embedding_client
from slayer.embeddings.service import EmbeddingService
from slayer.memories.models import Memory
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def storage():
    with tempfile.TemporaryDirectory() as tmp:
        yield YAMLStorage(base_dir=tmp)


@pytest.fixture
def stub_available(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)


class _RecordingEmbedBatch:
    """Test double for ``embed_batch`` — captures every batch and returns
    deterministic vectors. Behaviour can be customised by setting
    ``override_n_th_to_none`` to skip particular entries (simulating
    partial litellm failures)."""

    def __init__(self) -> None:
        self.calls: List[List[str]] = []
        self.override_none: set[int] = set()

    async def __call__(
        self, texts: List[str], *, model: Optional[str] = None,
    ) -> List[Optional[List[float]]]:
        self.calls.append(list(texts))
        out: List[Optional[List[float]]] = []
        for global_idx, text in enumerate(texts):
            if (len(self.calls) - 1, global_idx) in self.override_none:
                out.append(None)
            else:
                # Use the hash of text to make distinct vectors per text.
                v = (hash(text) & 0xFF) / 255.0
                out.append([v, v + 0.1, v + 0.2])
        return out


@pytest.fixture
def recording_embed(monkeypatch: pytest.MonkeyPatch) -> _RecordingEmbedBatch:
    rec = _RecordingEmbedBatch()
    monkeypatch.setattr(
        "slayer.embeddings.service.embed_batch", rec,
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
            Column(name="amount", type=DataType.DOUBLE,
                   description="cents"),
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
# refresh_memory
# ---------------------------------------------------------------------------


async def test_refresh_memory_missing_extra_returns_warning(
    storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(embedding_client, "is_available", lambda: False)
    service = EmbeddingService(
        storage=storage, model_name="openai/x",
    )
    memory = Memory(id=1, learning="hello", entities=["e1"])
    warnings = await service.refresh_memory(memory)
    assert len(warnings) == 1
    assert "embedding_search" in warnings[0]
    # Nothing got persisted.
    assert await storage.get_embedding(
        canonical_id="memory:1", embedding_model_name="openai/x",
    ) is None


async def test_refresh_memory_persists_row(
    storage: YAMLStorage,
    stub_available: None,
    recording_embed: _RecordingEmbedBatch,
) -> None:
    service = EmbeddingService(storage=storage, model_name="openai/x")
    memory = Memory(id=1, learning="hello world", entities=["e1"])
    warnings = await service.refresh_memory(memory)
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


async def test_refresh_memory_skips_when_hash_matches(
    storage: YAMLStorage,
    stub_available: None,
    recording_embed: _RecordingEmbedBatch,
) -> None:
    service = EmbeddingService(storage=storage, model_name="openai/x")
    memory = Memory(id=42, learning="unchanged", entities=["e1"])
    await service.refresh_memory(memory)
    await service.refresh_memory(memory)
    # Only one API call across two refresh attempts.
    assert len(recording_embed.calls) == 1


async def test_refresh_memory_reembeds_on_text_change(
    storage: YAMLStorage,
    stub_available: None,
    recording_embed: _RecordingEmbedBatch,
) -> None:
    service = EmbeddingService(storage=storage, model_name="openai/x")
    memory_a = Memory(id=7, learning="alpha", entities=["e1"])
    memory_b = Memory(id=7, learning="alpha-changed", entities=["e1"])
    await service.refresh_memory(memory_a)
    await service.refresh_memory(memory_b)
    # Second refresh hits the API because the rendered text changed.
    assert len(recording_embed.calls) == 2
    persisted = await storage.get_embedding(
        canonical_id="memory:7", embedding_model_name="openai/x",
    )
    assert persisted is not None
    assert persisted.content_hash != ""


# ---------------------------------------------------------------------------
# refresh_model_subtree
# ---------------------------------------------------------------------------


async def test_refresh_model_subtree_batches_all_children(
    storage: YAMLStorage,
    stub_available: None,
    recording_embed: _RecordingEmbedBatch,
) -> None:
    """One batch call must cover model + visible columns + named measures
    + custom aggregations — and skip the hidden column."""
    service = EmbeddingService(storage=storage, model_name="openai/x")
    model = _make_model()
    warnings = await service.refresh_model_subtree(model)
    assert warnings == []
    assert len(recording_embed.calls) == 1
    # Expected entries: model + 2 visible columns + 1 named measure + 1 agg = 5.
    # Hidden "secret" column is skipped.
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
    service = EmbeddingService(storage=storage, model_name="openai/x")
    model = _make_model()
    await service.refresh_model_subtree(model)
    # Second call with no changes: zero new API calls.
    await service.refresh_model_subtree(model)
    assert len(recording_embed.calls) == 1


async def test_refresh_model_subtree_per_entry_failure_warns(
    storage: YAMLStorage,
    stub_available: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A single None in the batch surfaces a warning but doesn't abort
    persistence of the other rows."""

    async def partial_failure(  # NOSONAR(S7503) — stub matches embed_batch async signature
        texts: List[str], *, model: Optional[str] = None,
    ) -> List[Optional[List[float]]]:
        out: List[Optional[List[float]]] = []
        for i, _ in enumerate(texts):
            out.append(None if i == 0 else [0.1, 0.2, 0.3])
        return out

    monkeypatch.setattr(
        "slayer.embeddings.service.embed_batch", partial_failure,
    )
    service = EmbeddingService(storage=storage, model_name="openai/x")
    model = _make_model()
    warnings = await service.refresh_model_subtree(model)
    assert len(warnings) == 1
    assert "embedding refresh failed" in warnings[0]
    # 4 of the 5 entries land in storage.
    listed = await storage.list_embeddings(embedding_model_name="openai/x")
    assert len(listed) == 4


async def test_refresh_model_subtree_hidden_model_short_circuits(
    storage: YAMLStorage,
    stub_available: None,
    recording_embed: _RecordingEmbedBatch,
) -> None:
    service = EmbeddingService(storage=storage, model_name="openai/x")
    model = _make_model()
    model.hidden = True
    warnings = await service.refresh_model_subtree(model)
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
    """Switching ``SLAYER_EMBEDDING_MODEL`` leaves old rows in place but
    ``fetch_corpus`` reads only rows for the active model."""
    service_a = EmbeddingService(storage=storage, model_name="openai/a")
    service_b = EmbeddingService(storage=storage, model_name="openai/b")
    memory = Memory(id=1, learning="anything", entities=["e1"])

    await service_a.refresh_memory(memory)
    # Force service_b to re-embed (different model name → different row).
    await service_b.refresh_memory(memory)

    rows_a = await service_a.fetch_corpus()
    rows_b = await service_b.fetch_corpus()
    assert len(rows_a) == 1 and rows_a[0].embedding_model_name == "openai/a"
    assert len(rows_b) == 1 and rows_b[0].embedding_model_name == "openai/b"
