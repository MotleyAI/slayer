"""End-to-end tests for the three-channel ``SearchService`` (DEV-1386).

Each test sets up a ``YAMLStorage`` in tempdir, populates a few memories
plus a small model corpus, stubs ``embed_batch`` (via the service module)
and ``embed_query`` (via the client module), and exercises the search
contract. The embedding channel is exercised with deterministic vectors
so cosine similarity is predictable.
"""

from __future__ import annotations

import tempfile
from typing import Iterator, List, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    SlayerModel,
)
from slayer.embeddings import client as embedding_client
from slayer.search.service import SearchService
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def storage() -> Iterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmp:
        yield YAMLStorage(base_dir=tmp)


@pytest.fixture
def stub_available(monkeypatch: pytest.MonkeyPatch) -> None:
    """Opt in to the embedding code path. The session-wide autouse
    fixture in ``conftest.py`` defaults ``is_available`` to a lambda
    returning ``False``; this fixture replaces it with ``True``.

    The query-embedding cache is process-wide, so clear it on entry so
    a prior test's stubbed query doesn't leak in."""
    embedding_client._reset_query_cache()
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)


async def _seed_basic_corpus(storage: YAMLStorage) -> None:
    ds = DatasourceConfig(name="dsx", type="postgres", host="h", database="d")
    await storage.save_datasource(ds)
    model = SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="dsx",
        description="orders fact table",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(
                name="amount",
                type=DataType.DOUBLE,
                description="purchase amount in cents",
            ),
        ],
    )
    await storage.save_model(model)


# ---------------------------------------------------------------------------
# Channel-3 activation
# ---------------------------------------------------------------------------


async def test_question_only_warns_when_extra_missing(
    storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the extra isn't installed (or no API key is configured), the
    embedding channel emits a warning and the search still returns
    whatever tantivy + BM25 found. The session-wide autouse fixture
    already stubs ``is_available`` to ``False`` — this test relies on
    that default rather than re-patching it."""
    await _seed_basic_corpus(storage)
    service = SearchService(storage=storage)
    response = await service.search(question="how do I look up purchases?")
    assert any(
        "advanced_search" in w for w in response.warnings
    ), response.warnings


async def test_question_only_warns_when_no_embeddings_persisted(
    storage: YAMLStorage,
    monkeypatch: pytest.MonkeyPatch,
    stub_available: None,
) -> None:
    """Available extra, but no rows in storage → channel 3 emits a
    distinct warning, search degrades to tantivy + BM25."""
    await _seed_basic_corpus(storage)
    service = SearchService(storage=storage)
    response = await service.search(question="orders")
    assert any("no embedding rows" in w for w in response.warnings)


async def test_question_with_embeddings_returns_entity(
    storage: YAMLStorage,
    monkeypatch: pytest.MonkeyPatch,
    stub_available: None,
) -> None:
    """End-to-end happy path: a question that lexically misses tantivy
    can still surface an entity through cosine similarity on stored
    embeddings."""
    await _seed_basic_corpus(storage)
    # Stub embed_batch (write side) — what gets persisted as the
    # entity vectors. Real text content doesn't matter; we control
    # which canonical_id "wins" by giving it the highest-cosine vector
    # against the query embedding.
    text_to_vec: dict = {}

    async def stub_embed_batch(  # NOSONAR(S7503) — stub matches embed_batch async signature
        texts: List[str], *, model: Optional[str] = None,
    ) -> List[Optional[List[float]]]:
        out: List[Optional[List[float]]] = []
        for idx, t in enumerate(texts):
            # Distinguish "amount" column doc by a marker vector — every
            # other doc gets a flatter base vector.
            if "amount" in t and "Column:" in t:
                v = [1.0, 0.0, 0.0, 0.0]
            else:
                v = [0.0, 1.0, 0.0, 0.0]
            text_to_vec[t] = v
            out.append(v)
        return out

    monkeypatch.setattr(
        "slayer.search.retrievers.embeddings.embed_batch",
        stub_embed_batch,
    )

    # Refresh embeddings for everything in the seeded datasource via the
    # retriever so the storage table is populated.
    from slayer.search.retrievers.embeddings import EmbeddingRetriever

    model = await storage.get_model("orders", data_source="dsx")
    assert model is not None
    await EmbeddingRetriever(storage=storage).refresh_model_subtree(model)
    await EmbeddingRetriever(storage=storage).refresh_datasource(
        name="dsx", models=[model],
    )

    # Stub the query-side embedding: align with the "amount" column
    # vector so cosine ranks it #1.
    async def stub_embed_query(  # NOSONAR(S7503) — stub matches embed_query async signature
        text: str, *, model: Optional[str] = None,
    ) -> List[float]:
        # Align with amount column.
        return [1.0, 0.0, 0.0, 0.0]

    monkeypatch.setattr(
        embedding_client, "embed_query", stub_embed_query,
    )

    service = SearchService(storage=storage)
    response = await service.search(question="purchase total in dollars")
    entity_hits = [h for h in response.results if h.kind != "memory"]
    assert entity_hits
    assert entity_hits[0].id == "dsx.orders.amount"


# ---------------------------------------------------------------------------
# RRF entity fusion across channels 2 and 3
# ---------------------------------------------------------------------------


async def test_entity_hits_now_carry_rrf_fused_score(
    storage: YAMLStorage,
    monkeypatch: pytest.MonkeyPatch,
    stub_available: None,
) -> None:
    """Entity scores must be small RRF-fused fractions, not raw tantivy
    BM25 (which is typically in the single digits). 1/(60+1) ≈ 0.0164 —
    a single-channel hit through RRF emits a score < 0.05."""
    await _seed_basic_corpus(storage)

    async def stub_embed_batch(  # NOSONAR(S7503) — stub matches embed_batch async signature
        texts: List[str], *, model: Optional[str] = None,
    ) -> List[Optional[List[float]]]:
        return [[0.0, 0.0, 0.0, 0.0] for _ in texts]

    async def stub_embed_query(*_a, **_kw) -> List[float]:  # NOSONAR(S7503) — stub matches embed_query async signature
        return [0.0, 0.0, 0.0, 0.0]

    monkeypatch.setattr(
        "slayer.search.retrievers.embeddings.embed_batch",
        stub_embed_batch,
    )
    monkeypatch.setattr(
        embedding_client, "embed_query", stub_embed_query,
    )
    from slayer.search.retrievers.embeddings import EmbeddingRetriever
    model = await storage.get_model("orders", data_source="dsx")
    assert model is not None
    await EmbeddingRetriever(storage=storage).refresh_model_subtree(model)
    await EmbeddingRetriever(storage=storage).refresh_datasource(
        name="dsx", models=[model],
    )

    service = SearchService(storage=storage)
    response = await service.search(question="orders")
    entity_hits = [h for h in response.results if h.kind != "memory"]
    if entity_hits:
        # Any entity ranked #1 in *one* channel through RRF has
        # score = 1/(60+1) ≈ 0.0164. If both channels hit it #1,
        # score ≈ 0.0328. Both are well under the raw tantivy BM25
        # band that the old surface emitted (5+).
        assert entity_hits[0].score < 0.1


# ---------------------------------------------------------------------------
# Channel-3 graceful failures
# ---------------------------------------------------------------------------


async def test_query_embed_failure_warns_and_continues(
    storage: YAMLStorage,
    monkeypatch: pytest.MonkeyPatch,
    stub_available: None,
) -> None:
    await _seed_basic_corpus(storage)

    async def stub_embed_batch(  # NOSONAR(S7503) — stub matches embed_batch async signature
        texts: List[str], *, model: Optional[str] = None,
    ) -> List[Optional[List[float]]]:
        return [[0.1, 0.1, 0.1, 0.1] for _ in texts]

    monkeypatch.setattr(
        "slayer.search.retrievers.embeddings.embed_batch",
        stub_embed_batch,
    )
    from slayer.search.retrievers.embeddings import EmbeddingRetriever
    model = await storage.get_model("orders", data_source="dsx")
    assert model is not None
    await EmbeddingRetriever(storage=storage).refresh_model_subtree(model)

    async def failing_query(*_a, **_kw):  # NOSONAR(S7503) — stub matches embed_query async signature
        return None

    monkeypatch.setattr(embedding_client, "embed_query", failing_query)

    service = SearchService(storage=storage)
    response = await service.search(question="anything")
    assert any(
        "query embedding failed" in w for w in response.warnings
    ), response.warnings


# ---------------------------------------------------------------------------
# Entity-only inputs leave channel 3 dormant
# ---------------------------------------------------------------------------


async def test_entity_only_does_not_trigger_channel_3(
    storage: YAMLStorage,
    monkeypatch: pytest.MonkeyPatch,
    stub_available: None,
) -> None:
    """When ``question`` is empty, channel 3 must not fire — no embedding
    warning, no corpus fetch."""
    await _seed_basic_corpus(storage)
    await storage.save_memory(
        learning="learning", entities=["dsx.orders"], query=None,
    )

    embed_called: List[str] = []

    async def stub_embed_query(text: str, *, model: Optional[str] = None):  # NOSONAR(S7503) — stub matches embed_query async signature
        embed_called.append(text)
        return [0.1, 0.1, 0.1, 0.1]

    monkeypatch.setattr(
        embedding_client, "embed_query", stub_embed_query,
    )
    service = SearchService(storage=storage)
    response = await service.search(entities=["dsx.orders"])
    # No question → no embedding warning, no corpus lookup.
    assert embed_called == []
    assert not any(
        "embedding" in w.lower() for w in response.warnings
    ), response.warnings


# ---------------------------------------------------------------------------
# Recency fallback still works untouched
# ---------------------------------------------------------------------------


async def test_recency_fallback_when_all_inputs_empty(
    storage: YAMLStorage,
    monkeypatch: pytest.MonkeyPatch,
    stub_available: None,
) -> None:
    """No entities, no query, no question → emit the recency warning and
    don't fire any channel."""
    await _seed_basic_corpus(storage)
    await storage.save_memory(
        learning="first", entities=["dsx.orders"], query=None,
    )
    service = SearchService(storage=storage)
    response = await service.search()
    assert any("returning" in w for w in response.warnings)
    assert [h for h in response.results if h.kind != "memory"] == []
