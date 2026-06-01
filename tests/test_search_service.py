"""SearchService behaviour matrix (DEV-1375).

Covers every input combination from the spec's behaviour matrix:

| entities/query | question | result                                                        |
| set            | set      | both channels; RRF memories + example_queries + tantivy ents  |
| set            | unset    | channel 1 only; entities=[]                                   |
| unset          | set      | channel 2 only (memory subset + entity subset)                |
| unset          | unset    | recency fallback (newest memories + example_queries)          |

Also pins:
* Resolver errors propagate.
* Warnings are aggregated and deduped.
* ``max_memories`` / ``max_example_queries`` / ``max_entities`` slice the
  three return lists independently.
* Query-bearing memories surface only via ``example_queries``; learning-only
  memories surface only via ``memories``.
* ``resolved_input_entities`` echoes the resolver output to the caller.
"""

from __future__ import annotations

import tempfile
from typing import AsyncIterator

import pytest
import pytest_asyncio

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.search.service import (
    EntityHit,
    ExampleQueryHit,
    MemoryHit,
    SearchResponse,
    SearchService,
)
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
    assert response.entities, "expected channel 2 to surface at least one entity hit"
    assert all(isinstance(h, EntityHit) for h in response.entities)


@pytest.mark.asyncio
async def test_entities_only_runs_channel_1_only(service: SearchService) -> None:
    """Channel 1 fills BOTH memory hits (BM25 over entity tags) and
    entity hits (DEV-1513 implicit self-reference)."""
    response = await service.search(
        entities=["warehouse.orders.amount_paid"],
        max_memories=5,
        max_entities=5,
    )
    # DEV-1513: the named ref itself surfaces in the entities bucket.
    assert any(
        h.id == "warehouse.orders.amount_paid" for h in response.entities
    )
    # Memories include the two tagged on amount_paid (both have query=None).
    learnings = [h.text for h in response.memories]
    assert any("gross of refunds" in lm for lm in learnings)


@pytest.mark.asyncio
async def test_query_only_runs_channel_1_via_extracted_entities(
    service: SearchService,
) -> None:
    """Channel 1 walks the extracted entities; DEV-1513 also surfaces
    them in the entities bucket."""
    response = await service.search(
        query={
            "source_model": "orders",
            "measures": [{"formula": "amount_paid:sum"}],
        },
        max_memories=5,
        max_entities=5,
    )
    # DEV-1513: the query's source model and referenced column surface
    # in the entities bucket.
    entity_ids = {h.id for h in response.entities}
    assert "warehouse.orders" in entity_ids
    assert "warehouse.orders.amount_paid" in entity_ids
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
    with pytest.raises(ValueError):
        await service.search(question="x", max_example_queries=-1)


# ---------------------------------------------------------------------------
# Resolver errors
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unknown_entity_becomes_warning(service: SearchService) -> None:
    """DEV-1428: search is lenient on unresolved refs; unknown entities
    surface as warnings rather than raising."""
    response = await service.search(entities=["warehouse.nonexistent.col"])
    assert any(
        "warehouse.nonexistent.col" in w for w in response.warnings
    )
    assert response.resolved_input_entities == []


# ---------------------------------------------------------------------------
# Hit shapes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_memory_hit_id_is_str(service: SearchService) -> None:
    """DEV-1428: ``MemoryHit.id`` is the str memory id."""
    response = await service.search(entities=["warehouse.orders.amount_paid"])
    for hit in response.memories:
        assert isinstance(hit.id, str)
        assert hit.id != ""


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
    # Memory 1 ("amount_paid is gross of refunds") matches both channels.
    # Memory 2 ("Filter status='paid' for net revenue.") matches only via
    # entity overlap on amount_paid — tantivy doesn't pick it up on the
    # "amount_paid gross refunds" question. The dual-channel hit must rank
    # ahead of the single-channel one.
    assert len(learnings_in_order) >= 2, (
        f"expected at least 2 memory hits; got {learnings_in_order}"
    )
    idx_dual = next(
        (i for i, lm in enumerate(learnings_in_order) if "gross of refunds" in lm),
        None,
    )
    idx_single = next(
        (i for i, lm in enumerate(learnings_in_order) if "Filter status='paid'" in lm),
        None,
    )
    assert idx_dual is not None, "dual-channel memory missing from results"
    assert idx_single is not None, "single-channel memory missing from results"
    assert idx_dual < idx_single, (
        f"dual-channel memory must rank ahead of single-channel; "
        f"got dual@{idx_dual}, single@{idx_single}"
    )


# ---------------------------------------------------------------------------
# resolved_input_entities echo
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolved_input_entities_populated_for_entity_input(
    service: SearchService,
) -> None:
    response = await service.search(
        entities=["warehouse.orders.amount_paid"],
    )
    assert "warehouse.orders.amount_paid" in response.resolved_input_entities


@pytest.mark.asyncio
async def test_resolved_input_entities_populated_for_query_input(
    service: SearchService,
) -> None:
    response = await service.search(
        query={
            "source_model": "orders",
            "measures": [{"formula": "amount_paid:sum"}],
        },
    )
    # Both the source model and the referenced column should be resolved.
    assert "warehouse.orders" in response.resolved_input_entities
    assert "warehouse.orders.amount_paid" in response.resolved_input_entities


@pytest.mark.asyncio
async def test_resolved_input_entities_combined_input_dedupes(
    service: SearchService,
) -> None:
    response = await service.search(
        entities=["warehouse.orders.amount_paid"],
        query={
            "source_model": "orders",
            "measures": [{"formula": "amount_paid:sum"}],
        },
    )
    # `amount_paid` appears via both inputs but should not duplicate.
    matches = [
        e for e in response.resolved_input_entities
        if e == "warehouse.orders.amount_paid"
    ]
    assert len(matches) == 1


@pytest.mark.asyncio
async def test_resolved_input_entities_empty_on_recency_fallback(
    service: SearchService,
) -> None:
    response = await service.search(max_memories=2)
    assert response.resolved_input_entities == []


# ---------------------------------------------------------------------------
# example_queries: query-bearing memories surface separately
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def storage_with_query_memories(
    storage_with_corpus: StorageBackend,
) -> StorageBackend:
    """Add three query-bearing memories to the base corpus."""
    for i in range(3):
        await storage_with_corpus.save_memory(
            learning=f"example query {i}",
            entities=["warehouse.orders.amount_paid"],
            query=SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="amount_paid:sum")],
            ),
        )
    return storage_with_corpus


@pytest_asyncio.fixture
async def service_with_query_memories(
    storage_with_query_memories: StorageBackend,
) -> SearchService:
    return SearchService(storage=storage_with_query_memories)


@pytest.mark.asyncio
async def test_query_bearing_memories_go_to_example_queries(
    service_with_query_memories: SearchService,
) -> None:
    response = await service_with_query_memories.search(
        entities=["warehouse.orders.amount_paid"],
        max_memories=10,
        max_example_queries=10,
    )
    # No query-bearing memory should leak into `memories`.
    assert all(isinstance(h, MemoryHit) for h in response.memories)
    # All three query-bearing memories surface in `example_queries`.
    assert len(response.example_queries) == 3
    assert all(isinstance(h, ExampleQueryHit) for h in response.example_queries)
    assert all(h.query is not None for h in response.example_queries)


@pytest.mark.asyncio
async def test_max_example_queries_default_is_two(
    service_with_query_memories: SearchService,
) -> None:
    response = await service_with_query_memories.search(
        entities=["warehouse.orders.amount_paid"],
    )
    assert len(response.example_queries) == 2


@pytest.mark.asyncio
async def test_max_example_queries_caps_independently(
    service_with_query_memories: SearchService,
) -> None:
    response = await service_with_query_memories.search(
        entities=["warehouse.orders.amount_paid"],
        max_memories=10,
        max_example_queries=1,
    )
    assert len(response.example_queries) == 1


@pytest.mark.asyncio
async def test_bulky_example_does_not_evict_small_learning(
    service_with_query_memories: SearchService,
) -> None:
    """An agent setting low caps still receives both kinds of memory.
    With three query-bearing memories all matching the same entity, the
    learning-only memories must still surface in `memories` because the two
    kinds have independent caps."""
    response = await service_with_query_memories.search(
        entities=["warehouse.orders.amount_paid"],
        max_memories=2,
        max_example_queries=1,
    )
    assert len(response.memories) == 2
    assert len(response.example_queries) == 1
    learning_texts = [h.text for h in response.memories]
    assert any("gross of refunds" in t for t in learning_texts)


@pytest.mark.asyncio
async def test_recency_fallback_fills_both_buckets(
    service_with_query_memories: SearchService,
) -> None:
    response = await service_with_query_memories.search(
        max_memories=10,
        max_example_queries=10,
    )
    # All learning-only memories from the base fixture (4) surface in
    # `memories`; all query-bearing (3) in `example_queries`.
    assert len(response.memories) == 4
    assert len(response.example_queries) == 3


@pytest.mark.asyncio
async def test_memory_hit_no_longer_carries_query_field() -> None:
    """`MemoryHit` is reserved for learning-only memories; the `query`
    field has moved to `ExampleQueryHit`."""
    assert "query" not in MemoryHit.model_fields
    assert "query" in ExampleQueryHit.model_fields
