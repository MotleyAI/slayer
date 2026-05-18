"""DEV-1428: retrieval-time in-memory filter for stale memory entity tags.

When a memory carries a tag for a canonical id that no longer exists in
the live corpus, the search service drops it for the BM25 path without
writing back. Recency fallback also applies the filter.
"""

from __future__ import annotations

import os
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.search.service import SearchService
from slayer.storage.base import StorageBackend
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
async def storage() -> AsyncIterator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmpdir:
        s = YAMLStorage(base_dir=os.path.join(tmpdir, "store"))
        await s.save_datasource(
            DatasourceConfig(
                name="mydb", type="sqlite", database=":memory:",
            )
        )
        await s.save_model(
            SlayerModel(
                name="orders",
                sql_table="orders",
                data_source="mydb",
                columns=[
                    Column(name="id", sql="id", primary_key=True),
                    Column(name="amount", sql="amount"),
                ],
            )
        )
        yield s


class TestSearchLazyGC:
    async def test_stale_tag_excluded_from_matched_entities(
        self, storage: StorageBackend,
    ) -> None:
        """Even if a user queries for the stale tag directly, it must
        never appear in ``matched_entities`` — the filter drops it from
        the canonical set before BM25 ranks."""
        await storage.save_memory(
            learning="x",
            entities=["mydb.orders.amount", "mydb.deleted_model"],
        )
        svc = SearchService(storage=storage)
        resp = await svc.search(
            entities=["mydb.orders.amount", "mydb.deleted_model"],
        )
        assert resp.memories, "expected the live-tag memory to surface"
        for hit in resp.memories:
            assert "mydb.deleted_model" not in hit.matched_entities

    async def test_recency_fallback_filter_excludes_stale_in_matched(
        self, storage: StorageBackend,
    ) -> None:
        """Both memories surface under recency, but the stale-only
        memory's stale tag is not promoted to ``matched_entities``."""
        await storage.save_memory(
            learning="only stale",
            entities=["mydb.does_not_exist"],
        )
        await storage.save_memory(
            learning="has live",
            entities=["mydb.orders.amount"],
        )
        svc = SearchService(storage=storage)
        resp = await svc.search()
        learnings = {m.text for m in resp.memories}
        # Both rows survive the recency fallback (no datasource filter,
        # no entity filter — just newest-N).
        assert "only stale" in learnings
        assert "has live" in learnings
        # No memory's matched_entities should ever name a stale entity.
        for hit in resp.memories:
            assert "mydb.does_not_exist" not in hit.matched_entities

    async def test_no_writeback_on_stale_filter(
        self, storage: StorageBackend,
    ) -> None:
        m = await storage.save_memory(
            learning="x",
            entities=["mydb.orders.amount", "mydb.deleted"],
        )
        svc = SearchService(storage=storage)
        await svc.search(entities=["mydb.orders.amount"])
        # The stored entity list MUST still contain both tags after
        # search — the in-memory filter never writes back.
        loaded = await storage.get_memory(m.id)
        assert "mydb.deleted" in loaded.entities
