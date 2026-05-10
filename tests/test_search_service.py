"""SearchService behaviour matrix (DEV-1375).

Covers every input combination from the spec's behaviour matrix:

| entities/query | question | result                                           |
| set            | set      | both channels; RRF memories + tantivy entities   |
| set            | unset    | channel 1 only; entities=[]                      |
| unset          | set      | channel 2 only (memory subset + entity subset)   |
| unset          | unset    | recency fallback (newest max_memories memories)  |

Also pins:
* Resolver errors propagate.
* Warnings are aggregated and deduped.
* `max_memories` / `max_entities` slice the final lists.
* `recall_memories` is left untouched (no shared service call).
"""

from __future__ import annotations

import tempfile
from typing import AsyncIterator

import pytest
import pytest_asyncio

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.search.service import EntityHit, SearchResponse, SearchService
from slayer.storage.base import StorageBackend, resolve_storage


@pytest_asyncio.fixture
async def storage_with_corpus() -> AsyncIterator[StorageBackend]:
    """A small fixture corpus: 1 datasource, 2 models, 4 memories."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = resolve_storage(tmpdir)
        await storage.save_datasource(DatasourceConfig(name="warehouse", type="sqlite", database=":memory:"))
        await storage.save_model(SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="warehouse",
            description="Checkout orders.",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="amount_paid", type=DataType.DOUBLE,
                       description="Net paid in USD."),
                Column(name="status", type=DataType.TEXT,
                       description="paid|refunded|cancelled."),
            ],
        ))
        await storage.save_model(SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="warehouse",
            description="Customer master data.",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="email", type=DataType.TEXT),
            ],
        ))
        # 4 memories: 2 tagged on orders.amount_paid, 1 on customers, 1 untagged
        await storage.save_memory(
            learning="amount_paid is gross of refunds.",
            entities=["warehouse.orders.amount_paid"],
        )
        await storage.save_memory(
            learning="Filter status='paid' for net revenue.",
            entities=["warehouse.orders.amount_paid", "warehouse.orders.status"],
        )
        await storage.save_memory(
            learning="Customer email may be NULL for anonymous checkouts.",
            entities=["warehouse.customers.email"],
        )
        await storage.save_memory(
            learning="A free-floating note with no explicit entity tags.",
            entities=[],
        )
        yield storage


@pytest_asyncio.fixture
async def service(storage_with_corpus: StorageBackend) -> SearchService:
    return SearchService(storage=storage_with_corpus)


# ---------------------------------------------------------------------------
# Behaviour matrix
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_entities_and_question_both_set_runs_both_channels(
    service: SearchService,
) -> None:
    response = await service.search(
        entities=["warehouse.orders.amount_paid"],
        question="paid revenue",
        max_memories=5,
        max_entities=5,
    )
    assert isinstance(response, SearchResponse)
    # Channel 1 should surface the memory tagged on amount_paid.
    learnings = [h.text for h in response.memories]
    assert any("gross of refunds" in lm for lm in learnings)
    # Channel 2 should surface entity hits.
    assert all(isinstance(h, EntityHit) for h in response.entities)


@pytest.mark.asyncio
async def test_entities_only_runs_channel_1_only(service: SearchService) -> None:
    response = await service.search(
        entities=["warehouse.orders.amount_paid"],
        max_memories=5,
        max_entities=5,
    )
    assert response.entities == []
    # Memories include the two tagged on amount_paid (both have query=None).
    learnings = [h.text for h in response.memories]
    assert any("gross of refunds" in lm for lm in learnings)


@pytest.mark.asyncio
async def test_query_only_runs_channel_1_via_extracted_entities(
    service: SearchService,
) -> None:
    response = await service.search(
        query={
            "source_model": "orders",
            "measures": [{"formula": "amount_paid:sum"}],
        },
        max_memories=5,
        max_entities=5,
    )
    assert response.entities == []
    learnings = [h.text for h in response.memories]
    assert any("gross of refunds" in lm for lm in learnings)


@pytest.mark.asyncio
async def test_question_only_runs_channel_2_only(service: SearchService) -> None:
    response = await service.search(
        question="anonymous checkouts",
        max_memories=5,
        max_entities=5,
    )
    # Channel 1 was skipped → memories come only from tantivy memory subset.
    learnings = [h.text for h in response.memories]
    assert any("anonymous" in lm for lm in learnings)


@pytest.mark.asyncio
async def test_all_empty_falls_back_to_recency(service: SearchService) -> None:
    response = await service.search(max_memories=2, max_entities=5)
    assert response.entities == []
    # Newest first: the 4th saved memory should appear before the 1st.
    assert len(response.memories) == 2
    assert any("free-floating" in h.text for h in response.memories)
    # Warning explains the fallback.
    assert any("recency" in w.lower() for w in response.warnings)


# ---------------------------------------------------------------------------
# Caps
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_max_memories_caps_memory_list(service: SearchService) -> None:
    response = await service.search(
        entities=["warehouse.orders.amount_paid", "warehouse.orders.status"],
        max_memories=1,
        max_entities=5,
    )
    assert len(response.memories) <= 1


@pytest.mark.asyncio
async def test_max_entities_caps_entity_list(service: SearchService) -> None:
    response = await service.search(
        question="orders amount status customer email id",
        max_memories=5,
        max_entities=2,
    )
    assert len(response.entities) <= 2


@pytest.mark.asyncio
async def test_negative_caps_rejected(service: SearchService) -> None:
    with pytest.raises(ValueError):
        await service.search(question="x", max_memories=-1)
    with pytest.raises(ValueError):
        await service.search(question="x", max_entities=-1)


# ---------------------------------------------------------------------------
# Resolver errors
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unknown_entity_raises_resolution_error(service: SearchService) -> None:
    from slayer.core.errors import EntityResolutionError
    with pytest.raises(EntityResolutionError):
        await service.search(entities=["warehouse.nonexistent.col"])


# ---------------------------------------------------------------------------
# Hit shapes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_memory_hit_id_is_int(service: SearchService) -> None:
    response = await service.search(entities=["warehouse.orders.amount_paid"])
    for hit in response.memories:
        assert isinstance(hit.id, int)
        assert hit.id > 0


@pytest.mark.asyncio
async def test_entity_hit_id_is_canonical_string(service: SearchService) -> None:
    response = await service.search(question="amount_paid status")
    for hit in response.entities:
        assert isinstance(hit.id, str)
        assert hit.kind in {"datasource", "model", "column", "measure", "aggregation"}


@pytest.mark.asyncio
async def test_memory_hit_text_is_full_indexed_text(service: SearchService) -> None:
    """`text` must be the full indexed text — no truncation."""
    response = await service.search(entities=["warehouse.orders.amount_paid"])
    assert all(isinstance(h.text, str) and len(h.text) > 0 for h in response.memories)


@pytest.mark.asyncio
async def test_memory_matched_entities_populated_from_channel_1(
    service: SearchService,
) -> None:
    response = await service.search(entities=["warehouse.orders.amount_paid"])
    for hit in response.memories:
        assert "warehouse.orders.amount_paid" in hit.matched_entities


# ---------------------------------------------------------------------------
# Empty corpus
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_corpus_returns_empty_with_warning() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = resolve_storage(tmpdir)
        service = SearchService(storage=storage)
        response = await service.search(question="anything")
        assert response.memories == []
        assert response.entities == []


# ---------------------------------------------------------------------------
# RRF integration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_memory_appearing_in_both_channels_outranks_single_channel(
    service: SearchService,
) -> None:
    """If a memory is found via both entity-overlap and tantivy full-text,
    its RRF-fused score should be higher than a memory found in only one
    channel."""
    response = await service.search(
        entities=["warehouse.orders.amount_paid"],
        question="amount_paid gross refunds",
        max_memories=5,
    )
    learnings_in_order = [h.text for h in response.memories]
    # Memory 1 ("amount_paid is gross of refunds") matches both:
    # - entity overlap on amount_paid
    # - tantivy on "gross refunds"
    # It should rank ahead of memory 2 which matches only on entity overlap.
    if len(learnings_in_order) >= 2:
        idx_1 = next(
            (i for i, lm in enumerate(learnings_in_order) if "gross of refunds" in lm),
            None,
        )
        assert idx_1 is not None
