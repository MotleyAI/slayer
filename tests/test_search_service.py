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

import asyncio
import sqlite3
import tempfile
from typing import AsyncIterator, Tuple

import pytest
import pytest_asyncio

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine import profiling as _profiling_mod
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.search.service import (
    SearchHit,
    SearchResponse,
    SearchService,
)
from slayer.storage.base import DatasourceConfig, StorageBackend, resolve_storage

from tests.search_helpers import seed_warehouse_models


@pytest_asyncio.fixture
async def storage_with_corpus() -> AsyncIterator[StorageBackend]:
    """A small fixture corpus: 1 datasource, 2 models, 4 memories."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = resolve_storage(tmpdir)
        await seed_warehouse_models(storage)
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
    # DEV-1549: opt out of compact-by-default — the test asserts on the
    # verbose text contract.
    response = await service.search(
        entities=["warehouse.orders.amount_paid"],
        question="paid revenue",
        max_results=20,
        compact=False,
    )
    assert isinstance(response, SearchResponse)
    # Channel 1 should surface the memory tagged on amount_paid.
    memory_hits = [h for h in response.results if h.kind == "memory" and h.query is None]
    learnings = [h.text for h in memory_hits]
    assert any("gross of refunds" in lm for lm in learnings)
    # Channel 2 should surface entity hits.
    entity_hits = [h for h in response.results if h.kind != "memory"]
    assert entity_hits, "expected channel 2 to surface at least one entity hit"
    assert all(isinstance(h, SearchHit) for h in entity_hits)


@pytest.mark.asyncio
async def test_entities_only_runs_channel_1_only(service: SearchService) -> None:
    """Channel 1 fills BOTH memory hits (BM25 over entity tags) and
    entity hits (DEV-1513 implicit self-reference)."""
    response = await service.search(
        entities=["warehouse.orders.amount_paid"],
        max_results=20,
        compact=False,
    )
    entity_hits = [h for h in response.results if h.kind != "memory"]
    memory_hits = [h for h in response.results if h.kind == "memory" and h.query is None]
    # DEV-1513: the named ref itself surfaces in the entities bucket.
    assert any(
        h.id == "warehouse.orders.amount_paid" for h in entity_hits
    )
    # Memories include the two tagged on amount_paid (both have query=None).
    learnings = [h.text for h in memory_hits]
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
        max_results=20,
        compact=False,
    )
    # DEV-1513: the query's source model and referenced column surface
    # in the entities bucket.
    entity_ids = {h.id for h in response.results if h.kind != "memory"}
    assert "warehouse.orders" in entity_ids
    assert "warehouse.orders.amount_paid" in entity_ids
    memory_hits = [h for h in response.results if h.kind == "memory" and h.query is None]
    learnings = [h.text for h in memory_hits]
    assert any("gross of refunds" in lm for lm in learnings)


@pytest.mark.asyncio
async def test_question_only_runs_channel_2_only(service: SearchService) -> None:
    response = await service.search(
        question="anonymous checkouts",
        max_results=20,
        compact=False,
    )
    # Channel 1 was skipped → memories come only from tantivy memory subset.
    memory_hits = [h for h in response.results if h.kind == "memory" and h.query is None]
    learnings = [h.text for h in memory_hits]
    assert any("anonymous" in lm for lm in learnings)


@pytest.mark.asyncio
async def test_all_empty_falls_back_to_recency(service: SearchService) -> None:
    response = await service.search(max_results=2, compact=False)
    entity_hits = [h for h in response.results if h.kind != "memory"]
    memory_hits = [h for h in response.results if h.kind == "memory"]
    assert entity_hits == []
    # Newest first: the 4th saved memory should appear before the 1st.
    assert len(memory_hits) == 2
    assert any("free-floating" in h.text for h in memory_hits)
    # Warning explains the fallback.
    assert any("recency" in w.lower() for w in response.warnings)


# ---------------------------------------------------------------------------
# Caps
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_max_memories_caps_memory_list(service: SearchService) -> None:
    response = await service.search(
        entities=["warehouse.orders.amount_paid", "warehouse.orders.status"],
        max_results=1,
    )
    assert len(response.results) <= 1


@pytest.mark.asyncio
async def test_max_entities_caps_entity_list(service: SearchService) -> None:
    response = await service.search(
        question="orders amount status customer email id",
        max_results=2,
    )
    assert len(response.results) <= 2


@pytest.mark.asyncio
async def test_negative_caps_rejected(service: SearchService) -> None:
    with pytest.raises(ValueError):
        await service.search(question="x", max_results=-1)
    with pytest.raises(ValueError):
        await service.search(question="x", max_results=0)


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
    """DEV-1428: memory SearchHit.id is the str memory id."""
    response = await service.search(entities=["warehouse.orders.amount_paid"], max_results=20)
    memory_hits = [h for h in response.results if h.kind == "memory"]
    for hit in memory_hits:
        assert isinstance(hit.id, str)
        assert hit.id != ""


@pytest.mark.asyncio
async def test_entity_hit_id_is_canonical_string(service: SearchService) -> None:
    response = await service.search(question="amount_paid status", max_results=20)
    entity_hits = [h for h in response.results if h.kind != "memory"]
    for hit in entity_hits:
        assert isinstance(hit.id, str)
        assert hit.kind in {"datasource", "model", "column", "measure", "aggregation"}


@pytest.mark.asyncio
async def test_memory_hit_text_is_full_indexed_text(service: SearchService) -> None:
    """Verbose mode: `text` must be the full indexed text — no truncation."""
    response = await service.search(
        entities=["warehouse.orders.amount_paid"],
        max_results=20,
        compact=False,
    )
    memory_hits = [h for h in response.results if h.kind == "memory"]
    assert all(isinstance(h.text, str) and len(h.text) > 0 for h in memory_hits)


@pytest.mark.asyncio
async def test_memory_matched_entities_populated_from_channel_1(
    service: SearchService,
) -> None:
    response = await service.search(entities=["warehouse.orders.amount_paid"], max_results=20)
    memory_hits = [h for h in response.results if h.kind == "memory"]
    for hit in memory_hits:
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
        assert response.results == []


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
        max_results=20,
        compact=False,
    )
    memory_hits = [h for h in response.results if h.kind == "memory" and h.query is None]
    learnings_in_order = [h.text for h in memory_hits]
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
    response = await service.search(max_results=2)
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
        max_results=20,
    )
    # All memory hits are SearchHit instances.
    assert all(isinstance(h, SearchHit) for h in response.results)
    # All three query-bearing memories surface with query set.
    example_query_hits = [h for h in response.results if h.kind == "memory" and h.query is not None]
    assert len(example_query_hits) == 3
    assert all(h.query is not None for h in example_query_hits)


@pytest.mark.asyncio
async def test_search_surfaces_query_bearing_memories_within_max_results(
    service_with_query_memories: SearchService,
) -> None:
    """Query-bearing memories surface in the flat ``results`` list under
    DEV-1532. With ``max_results=20`` (>= the fixture's 3 query-bearing
    memories), at least one such hit is returned."""
    response = await service_with_query_memories.search(
        entities=["warehouse.orders.amount_paid"],
        max_results=20,
    )
    example_query_hits = [h for h in response.results if h.kind == "memory" and h.query is not None]
    assert len(example_query_hits) >= 1


@pytest.mark.asyncio
async def test_search_respects_max_results_cap(
    service_with_query_memories: SearchService,
) -> None:
    """``max_results=1`` caps the flat list at exactly one hit total —
    no independent per-bucket cap exists under DEV-1532."""
    response = await service_with_query_memories.search(
        entities=["warehouse.orders.amount_paid"],
        max_results=1,
    )
    assert len(response.results) <= 1


@pytest.mark.asyncio
async def test_bulky_example_does_not_evict_small_learning(
    service_with_query_memories: SearchService,
) -> None:
    """With max_results large enough, both learning-only and query-bearing
    memories surface in the flat list."""
    response = await service_with_query_memories.search(
        entities=["warehouse.orders.amount_paid"],
        max_results=20,
        compact=False,
    )
    memory_hits = [h for h in response.results if h.kind == "memory" and h.query is None]
    example_query_hits = [h for h in response.results if h.kind == "memory" and h.query is not None]
    assert len(memory_hits) >= 1
    assert len(example_query_hits) >= 1
    learning_texts = [h.text for h in memory_hits]
    assert any("gross of refunds" in t for t in learning_texts)


@pytest.mark.asyncio
async def test_recency_fallback_fills_both_buckets(
    service_with_query_memories: SearchService,
) -> None:
    response = await service_with_query_memories.search(
        max_results=20,
    )
    # All learning-only memories from the base fixture (4) and all
    # query-bearing (3) surface in the flat list.
    memory_hits = [h for h in response.results if h.kind == "memory" and h.query is None]
    example_query_hits = [h for h in response.results if h.kind == "memory" and h.query is not None]
    assert len(memory_hits) == 4
    assert len(example_query_hits) == 3


@pytest.mark.asyncio
async def test_memory_hit_query_field_is_on_searchhit() -> None:
    """`SearchHit` carries a ``query`` field for query-bearing memories
    (DEV-1532 unified flat-results interface)."""
    assert "query" in SearchHit.model_fields


# ---------------------------------------------------------------------------
# DEV-1516: stale-sample auto-refresh on column hits.
#
# Tests originally written against the pre-DEV-1532 split-bucket
# ``response.entities`` / ``EntityHit`` shape; adapted to the unified
# ``response.results`` / ``SearchHit`` shape — partition by ``kind`` at
# the call site instead of relying on a pre-partitioned ``entities``
# attribute.
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def stale_setup() -> AsyncIterator[Tuple[StorageBackend, SlayerQueryEngine]]:
    """SQLite-backed storage + engine with a populated ``orders`` table
    AND a stored ``orders`` model whose ``status`` column has stale
    sample-value data (``sampled_values=None``)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_file = f"{tmpdir}/data.db"
        conn = sqlite3.connect(db_file)
        conn.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL, status TEXT)"
        )
        # Populate with two distinct statuses so distinct_count is meaningful.
        rows = [
            (i, float(i), "paid" if i % 2 == 0 else "refunded")
            for i in range(1, 21)
        ]
        conn.executemany("INSERT INTO orders VALUES (?, ?, ?)", rows)
        conn.commit()
        conn.close()

        storage = resolve_storage(f"{tmpdir}/storage")
        await storage.save_datasource(DatasourceConfig(
            name="warehouse", type="sqlite", database=db_file,
        ))
        await storage.save_model(SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="warehouse",
            description="Checkout orders fixture.",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="amount", type=DataType.DOUBLE),
                # status: stale — sampled_values intentionally None.
                Column(name="status", type=DataType.TEXT,
                       description="Order status."),
            ],
        ))
        # Add a memory so search() recency/channel-1 corpora are non-empty.
        await storage.save_memory(
            learning="status='paid' captures completed orders.",
            entities=["warehouse.orders.status"],
        )
        engine = SlayerQueryEngine(storage=storage)
        yield storage, engine


@pytest.mark.asyncio
async def test_search_service_accepts_engine_kwarg(
    stale_setup: Tuple[StorageBackend, SlayerQueryEngine],
) -> None:
    """DEV-1516: SearchService now accepts an optional ``engine=`` kwarg so
    the search path can run the sample-refresh helper. Codex finding #1.
    Also pins that the supplied engine actually lands on the instance —
    a bare ``is not None`` check on the constructor result was tautological."""
    storage, engine = stale_setup
    svc = SearchService(storage=storage, engine=engine)
    assert svc._engine is engine


@pytest.mark.asyncio
async def test_search_service_engine_default_none_works(
    storage_with_corpus: StorageBackend,
) -> None:
    """The ``engine`` kwarg defaults to None so existing callers (and the
    storage-only test fixtures) keep working unchanged."""
    svc = SearchService(storage=storage_with_corpus)
    # And search() must still succeed when engine is None — refresh is just
    # skipped.
    response = await svc.search(
        entities=["warehouse.orders.status"],
        max_results=10,
    )
    assert response is not None


@pytest.mark.asyncio
async def test_search_refreshes_stale_categorical_column_hit(
    stale_setup: Tuple[StorageBackend, SlayerQueryEngine],
) -> None:
    """DEV-1516 core contract: when search returns a stale categorical
    column hit, the helper fires, persists, and the hit's ``text``
    surfaces the freshly-profiled sample values."""
    storage, engine = stale_setup
    svc = SearchService(storage=storage, engine=engine)
    response = await svc.search(
        entities=["warehouse.orders.status"],
        max_results=10,
        compact=False,
    )
    column_hits = [h for h in response.results if h.kind == "column"]
    assert column_hits, "expected the status column to surface as a column hit"
    status_hit = next(
        h for h in column_hits if h.id == "warehouse.orders.status"
    )
    # The stale text would not have any sample values. The refreshed text
    # must surface "paid" and "refunded" (the only two values in the DB).
    assert "paid" in status_hit.text
    assert "refunded" in status_hit.text
    # And persistence happened — reloading the model shows populated cache.
    reloaded = await storage.get_model("orders", data_source="warehouse")
    assert reloaded is not None
    status_col = reloaded.get_column("status")
    assert status_col is not None
    assert status_col.sampled_values is not None
    assert set(status_col.sampled_values) == {"paid", "refunded"}


@pytest.mark.asyncio
async def test_search_refreshes_stale_column_hit_via_question_corpus(
    stale_setup: Tuple[StorageBackend, SlayerQueryEngine],
) -> None:
    """Codex round-3 finding #2: refresh must also apply to column hits that
    surface via the question/corpus path (channels 2 & 3), not only the
    named-entity (channel-1) path. A wrong implementation that only refreshes
    inside the named-lookup branch would pass the entities-only test but
    miss this one.

    Uses a question that the tantivy corpus matches against the stored
    ``status`` column (description ``"Order status."``). The column has
    stale ``sampled_values=None`` from the fixture."""
    storage, engine = stale_setup
    svc = SearchService(storage=storage, engine=engine)
    response = await svc.search(
        question="order status",
        max_results=10,
        compact=False,
    )
    column_hits = [
        h for h in response.results
        if h.kind == "column" and h.id == "warehouse.orders.status"
    ]
    assert column_hits, (
        "expected the status column to surface via the question/corpus path"
    )
    text = column_hits[0].text
    # Refreshed text must include the live values.
    assert "paid" in text and "refunded" in text, (
        "question-path column hit must also receive the post-fusion refresh"
    )
    # And the refresh persisted to storage.
    reloaded = await storage.get_model("orders", data_source="warehouse")
    assert reloaded is not None
    status_col = reloaded.get_column("status")
    assert status_col is not None
    assert status_col.sampled_values is not None


@pytest.mark.asyncio
async def test_search_no_refresh_when_engine_is_none(
    stale_setup: Tuple[StorageBackend, SlayerQueryEngine],
) -> None:
    """Without an engine the refresh hook is a no-op. The hit still
    surfaces (with stale text); storage stays untouched."""
    storage, _ = stale_setup
    svc = SearchService(storage=storage)  # no engine
    response = await svc.search(
        entities=["warehouse.orders.status"],
        max_results=10,
    )
    # The hit may or may not include "paid" depending on what the stale
    # corpus rendered, but storage MUST NOT have been written to.
    reloaded = await storage.get_model("orders", data_source="warehouse")
    assert reloaded is not None
    status_col = reloaded.get_column("status")
    assert status_col is not None
    assert status_col.sampled_values is None, (
        "engine=None must skip the refresh; storage stays stale"
    )
    # And no crash — response is returned normally.
    assert response is not None


@pytest.mark.asyncio
async def test_search_stale_text_preserved_when_profile_raises(
    stale_setup: Tuple[StorageBackend, SlayerQueryEngine], monkeypatch,
) -> None:
    """If the helper's profile_column raises, the search hit falls back to
    the original (stale) rendered text. No crash. Storage stays untouched.

    Codex round-3 finding #6: assert the hit text itself survives (not just
    that storage is untouched), and finding #7: assert ``update_column_sampled``
    is never called when profile fails."""
    storage, engine = stale_setup
    svc = SearchService(storage=storage, engine=engine)

    async def explodes(**_kwargs):
        raise RuntimeError("simulated profile failure")

    persist_calls: list = []
    original_persist = storage.update_column_sampled

    async def tracking_persist(**kwargs):
        persist_calls.append(kwargs)
        return await original_persist(**kwargs)

    monkeypatch.setattr("slayer.engine.profiling.profile_column", explodes)
    monkeypatch.setattr(storage, "update_column_sampled", tracking_persist)

    response = await svc.search(
        entities=["warehouse.orders.status"],
        max_results=10,
        compact=False,
    )
    # Search still returned a response and the column hit still exists.
    assert response is not None
    status_hits = [
        h for h in response.results if h.id == "warehouse.orders.status"
    ]
    assert status_hits, (
        "even on profile failure, the column hit must still surface in "
        "the search response (with stale text)"
    )
    # Hit text retains its baseline content — at minimum the canonical id +
    # type line + description that the renderer always emits for a column.
    text = status_hits[0].text
    assert "warehouse.orders.status" in text
    assert "Order status" in text  # column description
    # Storage stays stale.
    reloaded = await storage.get_model("orders", data_source="warehouse")
    assert reloaded is not None
    status_col = reloaded.get_column("status")
    assert status_col is not None
    assert status_col.sampled_values is None
    # And no persist call was attempted.
    assert persist_calls == [], (
        "profile failure must NOT trigger update_column_sampled; "
        "a wrong implementation that catches the failure and writes "
        "None/empty would silently clobber the stale cache"
    )


@pytest.mark.asyncio
async def test_search_numeric_column_hit_not_refreshed(
    stale_setup: Tuple[StorageBackend, SlayerQueryEngine], monkeypatch,
) -> None:
    """The helper is categorical-only. A numeric column hit must NOT
    trigger profile_column from the search refresh path.

    Codex round-3 finding #5: assert the profile-call recording explicitly
    rather than relying only on "storage stays untouched". A wrong
    implementation that calls ``profile_column`` for numeric and then
    skips persistence would otherwise slip through."""
    storage, engine = stale_setup
    svc = SearchService(storage=storage, engine=engine)

    profile_call_columns: list = []
    real_profile = _profiling_mod.profile_column

    async def counting_profile(*, model, column, engine):
        profile_call_columns.append(column.name)
        return await real_profile(model=model, column=column, engine=engine)

    monkeypatch.setattr(
        "slayer.engine.profiling.profile_column", counting_profile,
    )

    response = await svc.search(
        entities=["warehouse.orders.amount"],
        max_results=10,
    )
    # The helper short-circuits for numeric/temporal columns; profile_column
    # must NEVER be invoked for the ``amount`` column from the search hook.
    assert "amount" not in profile_call_columns, (
        f"numeric column must NOT trigger profile_column from search refresh; "
        f"got calls for columns: {profile_call_columns}"
    )
    # And storage for ``amount`` stays untouched.
    reloaded = await storage.get_model("orders", data_source="warehouse")
    assert reloaded is not None
    amount_col = reloaded.get_column("amount")
    assert amount_col is not None
    assert amount_col.sampled is None, (
        "numeric column must NOT be refreshed by the search hook"
    )
    assert response is not None


@pytest.mark.asyncio
async def test_search_per_model_serialization_concurrent_hits(
    stale_setup: Tuple[StorageBackend, SlayerQueryEngine], monkeypatch,
) -> None:
    """Codex finding #2: storage.update_column_sampled does a model-level
    read-modify-write. Two stale-column hits on the SAME model must NOT
    run in parallel — that would race and one column's update would lose.
    Per-model serialization is required; cross-model parallelism is fine."""
    storage, engine = stale_setup
    # Add another stale categorical column to the SAME model so two
    # hits on the same model would trigger parallel persist if not
    # serialized.
    model = await storage.get_model("orders", data_source="warehouse")
    assert model is not None
    model.columns.append(
        Column(name="region", type=DataType.TEXT, description="Region of sale."),
    )
    await storage.save_model(model)
    # Add a region column to the underlying table. (Without it, the
    # profile query will return no rows but the column will still be
    # in the refresh path.)
    ds = await storage.get_datasource("warehouse")
    assert ds is not None
    assert ds.database is not None
    conn = sqlite3.connect(ds.database)
    try:
        conn.execute("ALTER TABLE orders ADD COLUMN region TEXT DEFAULT 'EMEA'")
        conn.commit()
    finally:
        conn.close()
    # Track the order of update_column_sampled calls per model. Capture
    # the column name and a start/stop marker so we can detect overlap.
    persist_events: list = []
    original = storage.update_column_sampled

    async def tracking_persist(**kwargs):
        persist_events.append(("start", kwargs.get("model_name"), kwargs.get("column_name")))
        # tiny sleep to widen any race window
        await asyncio.sleep(0.01)
        result = await original(**kwargs)
        persist_events.append(("stop", kwargs.get("model_name"), kwargs.get("column_name")))
        return result

    monkeypatch.setattr(storage, "update_column_sampled", tracking_persist)

    svc = SearchService(storage=storage, engine=engine)
    response = await svc.search(
        entities=[
            "warehouse.orders.status",
            "warehouse.orders.region",
        ],
        max_results=10,
    )
    assert response is not None

    # Codex round-3 finding #3: assert BOTH columns produced persist
    # events. Otherwise the test could pass with only one column refreshed
    # (no overlap by accident, not by design).
    started_columns = {
        col_name for ev, _mn, col_name in persist_events if ev == "start"
    }
    assert "status" in started_columns, (
        "expected status to be refreshed and persisted"
    )
    assert "region" in started_columns, (
        "expected region to be refreshed and persisted"
    )

    # And BOTH columns reload with populated sampled_values — proving both
    # writes landed (no last-write-wins overwrite).
    reloaded2 = await storage.get_model("orders", data_source="warehouse")
    assert reloaded2 is not None
    status_col = reloaded2.get_column("status")
    region_col = reloaded2.get_column("region")
    assert status_col is not None
    assert region_col is not None
    assert status_col.sampled_values is not None, (
        "status refresh lost — codex finding #2 regression "
        "(per-model serialization broken; one column's write overwrote the other)"
    )
    assert region_col.sampled_values is not None, (
        "region refresh lost — codex finding #2 regression"
    )

    # Verify no two persist calls for the SAME model overlap.
    open_for_model: dict = {}
    for event, model_name, col_name in persist_events:
        if event == "start":
            assert open_for_model.get(model_name) is None, (
                f"persist for {model_name}.{col_name} overlaps with another "
                f"persist on the same model "
                f"(open={open_for_model[model_name]}). "
                "Codex finding #2: per-model writes must serialize."
            )
            open_for_model[model_name] = col_name
        else:
            open_for_model[model_name] = None


@pytest.mark.asyncio
async def test_search_cross_model_concurrency_is_allowed(
    stale_setup: Tuple[StorageBackend, SlayerQueryEngine], monkeypatch,
) -> None:
    """Codex round-3 finding #4: per the plan, refresh serializes within a
    ``(data_source, model_name)`` group AND parallelizes across groups via
    ``asyncio.gather``. Pin the cross-model parallelism — two stale columns
    on DIFFERENT models must be allowed to refresh concurrently."""
    storage, engine = stale_setup
    # Add a second table + stored model so we have two stale categorical
    # columns on different models.
    ds = await storage.get_datasource("warehouse")
    assert ds is not None
    assert ds.database is not None
    conn = sqlite3.connect(ds.database)
    try:
        conn.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT)"
        )
        conn.executemany(
            "INSERT INTO customers VALUES (?, ?)",
            [(i, "EMEA" if i % 2 == 0 else "APAC") for i in range(1, 11)],
        )
        conn.commit()
    finally:
        conn.close()
    await storage.save_model(SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="warehouse",
        description="Customers.",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region", type=DataType.TEXT,
                   description="Customer region."),
        ],
    ))

    persist_events: list = []
    original = storage.update_column_sampled

    async def tracking_persist(**kwargs):
        persist_events.append(
            ("start", kwargs.get("model_name"), kwargs.get("column_name"))
        )
        # Force overlap: hold the write open. If implementation does NOT
        # parallelize across models, both writes still complete sequentially
        # — which would pass the "no overlap" test but fail this one's
        # "must overlap" check.
        await asyncio.sleep(0.05)
        result = await original(**kwargs)
        persist_events.append(
            ("stop", kwargs.get("model_name"), kwargs.get("column_name"))
        )
        return result

    monkeypatch.setattr(storage, "update_column_sampled", tracking_persist)

    svc = SearchService(storage=storage, engine=engine)
    response = await svc.search(
        entities=[
            "warehouse.orders.status",
            "warehouse.customers.region",
        ],
        max_results=10,
    )
    assert response is not None
    # Detect overlap across DIFFERENT models.
    open_models: dict = {}
    cross_model_overlap_seen = False
    for event, model_name, _col_name in persist_events:
        if event == "start":
            # Any other model already open?
            others_open = [
                k for k, v in open_models.items() if v and k != model_name
            ]
            if others_open:
                cross_model_overlap_seen = True
            open_models[model_name] = True
        else:
            open_models[model_name] = False

    assert cross_model_overlap_seen, (
        "Cross-model refresh must run concurrently (codex round-3 #4). "
        "Two columns on DIFFERENT models had their persist calls fully "
        "sequenced — the plan's asyncio.gather across (data_source, model_name) "
        "groups is not being honored."
    )
