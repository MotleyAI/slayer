"""DEV-1409: ``datasource`` filter on ``search``.

When a caller passes ``datasource=X`` the three retrieval channels
pre-filter their corpora to that one datasource:

- **Entity hits** include only docs whose ``canonical_id`` is rooted at
  ``X`` (exact match or strict dotted-path descendant).
- **Memory hits** include only memories whose ``entities`` list has at
  least one entry rooted at ``X``. Memories spanning multiple
  datasources surface from each.
- Unknown ``datasource`` → ``ValueError``.

These tests pin the behaviour of channel 1 (BM25 over memory entity
tags) and the entity-side of channels 2/3 by using ``entities`` + a
short ``question``; the embedding channel is disabled by the
test-suite autouse fixture, so channel 3 contributes nothing.
"""

from __future__ import annotations

import tempfile
from typing import AsyncIterator

import pytest
import pytest_asyncio

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.search.service import SearchService
from slayer.storage.base import StorageBackend, resolve_storage


@pytest_asyncio.fixture
async def storage_two_datasources() -> AsyncIterator[StorageBackend]:
    """Two datasources, two models each, plus memories that span them."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = resolve_storage(tmpdir)
        for ds_name in ("prod", "staging"):
            await storage.save_datasource(DatasourceConfig(
                name=ds_name, type="sqlite", database=":memory:",
            ))
            await storage.save_model(SlayerModel(
                name="orders",
                sql_table="orders",
                data_source=ds_name,
                description=f"{ds_name} orders fact table",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="amount", type=DataType.DOUBLE,
                           description=f"{ds_name} order amount"),
                ],
            ))
        # Memories:
        #  m1 — only references prod
        #  m2 — only references staging
        #  m3 — references both (cross-datasource)
        #  m4 — untagged
        await storage.save_memory(
            learning="prod-only: amount excludes tax",
            entities=["prod.orders.amount"],
        )
        await storage.save_memory(
            learning="staging-only: amount includes tax",
            entities=["staging.orders.amount"],
        )
        await storage.save_memory(
            learning="cross: amount is gross",
            entities=["prod.orders.amount", "staging.orders.amount"],
        )
        await storage.save_memory(
            learning="free-floating note", entities=[],
        )
        yield storage


@pytest_asyncio.fixture
async def service(
    storage_two_datasources: StorageBackend,
) -> SearchService:
    return SearchService(storage=storage_two_datasources)


# ---------------------------------------------------------------------------
# Memory scoping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filter_keeps_memory_tagged_only_at_datasource(
    service: SearchService,
) -> None:
    response = await service.search(
        entities=["prod.orders.amount"],
        datasource="prod",
        max_results=20,
    )
    memory_hits = [h for h in response.results if h.kind == "memory"]
    learnings = {h.text for h in memory_hits}
    assert "prod-only: amount excludes tax" in learnings
    assert "staging-only: amount includes tax" not in learnings


@pytest.mark.asyncio
async def test_filter_keeps_cross_datasource_memory(
    service: SearchService,
) -> None:
    """A memory that references both `prod.*` and `staging.*` surfaces
    from BOTH datasources when each is filtered independently."""
    response_prod = await service.search(
        entities=["prod.orders.amount"],
        datasource="prod",
        max_results=20,
    )
    learnings_prod = {h.text for h in response_prod.results if h.kind == "memory"}
    assert "cross: amount is gross" in learnings_prod

    response_staging = await service.search(
        entities=["staging.orders.amount"],
        datasource="staging",
        max_results=20,
    )
    learnings_staging = {h.text for h in response_staging.results if h.kind == "memory"}
    assert "cross: amount is gross" in learnings_staging


@pytest.mark.asyncio
async def test_filter_drops_other_datasource_memory(
    service: SearchService,
) -> None:
    """Memory tagged only at staging must NOT surface in a prod-scoped
    search even if the resolved entity list is empty (which would
    otherwise fall to recency)."""
    response = await service.search(
        entities=["prod.orders.amount"],
        datasource="prod",
        max_results=20,
    )
    learnings = {h.text for h in response.results if h.kind == "memory"}
    assert "staging-only: amount includes tax" not in learnings


@pytest.mark.asyncio
async def test_filter_drops_untagged_memory(
    service: SearchService,
) -> None:
    """An untagged memory (entities=[]) has no entity rooted at any
    datasource — it must be dropped when a filter is active."""
    response = await service.search(
        entities=["prod.orders.amount"],
        datasource="prod",
        max_results=20,
    )
    learnings = {h.text for h in response.results if h.kind == "memory"}
    assert "free-floating note" not in learnings


# ---------------------------------------------------------------------------
# Entity scoping (channel 2 — tantivy full-text)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_filter_excludes_other_datasource_entity_hits(
    service: SearchService,
) -> None:
    """Channel 2 (tantivy) must only surface entities rooted at the
    filtered datasource. The two ``orders`` models share the same name
    but live in different datasources."""
    response = await service.search(
        question="orders amount",
        datasource="prod",
        max_results=20,
    )
    canonical_ids = {h.id for h in response.results if h.kind != "memory"}
    # All entity hits must be rooted at 'prod' (exact or dotted descendant).
    for cid in canonical_ids:
        assert cid == "prod" or cid.startswith("prod.")
    # The staging.orders model MUST NOT surface.
    assert "staging.orders" not in canonical_ids
    assert "staging.orders.amount" not in canonical_ids


@pytest.mark.asyncio
async def test_no_filter_returns_both_datasources(
    service: SearchService,
) -> None:
    """Sanity: without the filter, both datasources' entities are
    eligible."""
    response = await service.search(
        question="orders amount", max_results=20,
    )
    canonical_ids = {h.id for h in response.results if h.kind != "memory"}
    has_prod = any(cid == "prod" or cid.startswith("prod.") for cid in canonical_ids)
    has_staging = any(
        cid == "staging" or cid.startswith("staging.") for cid in canonical_ids
    )
    assert has_prod, f"expected at least one prod hit; got {canonical_ids}"
    assert has_staging, f"expected at least one staging hit; got {canonical_ids}"


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unknown_datasource_raises(
    service: SearchService,
) -> None:
    with pytest.raises(ValueError, match="datasource 'does_not_exist'"):
        await service.search(
            question="anything",
            datasource="does_not_exist",
            max_results=5,
        )


@pytest.mark.asyncio
async def test_validation_runs_before_corpus_build(
    service: SearchService,
) -> None:
    """``datasource`` validation should be the first thing checked, so
    typos surface before any expensive corpus walk."""
    # The error message includes the known-list so the caller can
    # identify a near-miss.
    with pytest.raises(ValueError) as excinfo:
        await service.search(
            entities=["prod.orders"],
            question="something",
            datasource="prodd",
        )
    assert "prod" in str(excinfo.value)
    assert "staging" in str(excinfo.value)


@pytest.mark.asyncio
async def test_filter_with_recency_fallback(
    service: SearchService,
) -> None:
    """No entities, no query, no question → recency fallback. The
    datasource filter still applies to which memories are eligible."""
    response = await service.search(
        datasource="prod", max_results=20,
    )
    learnings = {h.text for h in response.results if h.kind == "memory"}
    # The prod-only and cross memories surface; staging-only and untagged don't.
    assert "prod-only: amount excludes tax" in learnings
    assert "cross: amount is gross" in learnings
    assert "staging-only: amount includes tax" not in learnings
    assert "free-floating note" not in learnings


@pytest.mark.asyncio
async def test_none_datasource_is_no_filter(
    service: SearchService,
) -> None:
    """``datasource=None`` is identical to omitting the arg — all
    memories and entities eligible."""
    response_none = await service.search(
        entities=["prod.orders.amount", "staging.orders.amount"],
        datasource=None,
        max_results=20,
    )
    learnings = {h.text for h in response_none.results if h.kind == "memory"}
    # All three tagged memories should be eligible.
    assert "prod-only: amount excludes tax" in learnings
    assert "staging-only: amount includes tax" in learnings
    assert "cross: amount is gross" in learnings


# ---------------------------------------------------------------------------
# Edge: known datasource with zero models / zero matching memories
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_datasource_returns_empty_response(
) -> None:
    """A known datasource that has no models and no memories tagged at
    it returns an empty SearchResponse without raising."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = resolve_storage(tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="empty_ds", type="sqlite", database=":memory:",
        ))
        # No models, no memories.
        svc = SearchService(storage=storage)
        response = await svc.search(
            question="anything",
            datasource="empty_ds",
            max_results=10,
        )
        memory_hits = [h for h in response.results if h.kind == "memory"]
        assert memory_hits == []
        # Entity ranking may include the datasource doc itself, depending
        # on whether build_in_memory_corpus indexes empty-model datasources.
        # Either way: zero hits from a non-existent datasource.
        for hit in response.results:
            if hit.kind != "memory":
                assert hit.id == "empty_ds" or hit.id.startswith("empty_ds.")
