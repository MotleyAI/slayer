"""DEV-1428: cascade-strip dangling entity refs from memories on delete.

* ``delete_model("orders")`` strips ``mydb.orders`` and ``mydb.orders.*``
* ``delete_datasource("mydb")`` strips every ``mydb.*``
* ``forget_memory("X")`` strips ``memory:X`` exact-match (NOT ``memory:X1``)
* ``edit_model_remove("orders", remove_columns=["amount"])`` strips
  ``mydb.orders.amount`` only
* Empty-after-strip → memory survives
* Embedding refresh does NOT fire (content-hash skip)
* Per-row read-modify-write — lost-update window covered by retrieval filter
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from slayer.core.models import Column, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend


@pytest.fixture
async def storage(mydb_orders_storage: StorageBackend) -> StorageBackend:
    # DEV-1428: cascade tests additionally need a second model
    # (``orders_archive``) so they can pin that the dotted-namespace
    # rule excludes substring-prefix matches.
    await mydb_orders_storage.save_model(
        SlayerModel(
            name="orders_archive",
            sql_table="orders_archive",
            data_source="mydb",
            columns=[Column(name="id", sql="id", primary_key=True)],
        )
    )
    return mydb_orders_storage


class TestDeleteModelCascade:
    async def test_strips_model_root_and_descendants(
        self, storage: StorageBackend,
    ) -> None:
        a = await storage.save_memory(
            learning="x",
            entities=["mydb.orders", "mydb.orders.amount", "mydb.orders_archive"],
        )
        await storage.delete_model("orders", data_source="mydb")
        loaded = await storage.get_memory(a.id)
        # mydb.orders + mydb.orders.amount are stripped; mydb.orders_archive
        # is NOT (substring-prefix is forbidden by the dotted-namespace rule).
        assert "mydb.orders" not in loaded.entities
        assert "mydb.orders.amount" not in loaded.entities
        assert "mydb.orders_archive" in loaded.entities

    async def test_empty_after_strip_keeps_memory(
        self, storage: StorageBackend,
    ) -> None:
        m = await storage.save_memory(
            learning="rule about orders only",
            entities=["mydb.orders.amount"],
        )
        await storage.delete_model("orders", data_source="mydb")
        loaded = await storage.get_memory(m.id)
        assert loaded.learning == "rule about orders only"
        assert loaded.entities == []


class TestDeleteDatasourceCascade:
    async def test_strips_every_ds_entity(
        self, storage: StorageBackend,
    ) -> None:
        a = await storage.save_memory(
            learning="x",
            entities=["mydb", "mydb.orders.amount", "mydb.orders_archive"],
        )
        await storage.delete_datasource("mydb")
        loaded = await storage.get_memory(a.id)
        assert loaded.entities == []


class TestDeleteMemoryCascadeExactMatch:
    async def test_exact_match_only(
        self, storage: StorageBackend,
    ) -> None:
        # Three memories: X, X1, X.y; another memory references all three.
        await storage.save_memory(
            id="X", learning="a", entities=["mydb.orders"],
        )
        await storage.save_memory(
            id="X1", learning="b", entities=["mydb.orders"],
        )
        await storage.save_memory(
            id="X.y", learning="c", entities=["mydb.orders"],
        )
        ref = await storage.save_memory(
            learning="ref",
            entities=["memory:X", "memory:X1", "memory:X.y"],
        )
        # Delete X → strip ``memory:X`` exact-match only.
        await storage.delete_memory("X")
        loaded = await storage.get_memory(ref.id)
        assert "memory:X" not in loaded.entities
        assert "memory:X1" in loaded.entities
        assert "memory:X.y" in loaded.entities


class TestEditModelRemoveCascade:
    async def test_strips_only_removed_leaf(
        self, storage: StorageBackend,
    ) -> None:
        a = await storage.save_memory(
            learning="x",
            entities=["mydb.orders", "mydb.orders.amount", "mydb.orders.id"],
        )
        engine = SlayerQueryEngine(storage=storage)
        await engine.edit_model_remove(
            model_name="orders",
            data_source="mydb",
            remove_columns=["amount"],
        )
        loaded = await storage.get_memory(a.id)
        assert "mydb.orders.amount" not in loaded.entities
        assert "mydb.orders" in loaded.entities
        assert "mydb.orders.id" in loaded.entities


class TestEmbeddingRefreshNotFired:
    async def test_cascade_does_not_route_through_refresh(
        self, storage: StorageBackend,
    ) -> None:
        """Cascade-strip writes go through ``_save_memory_row`` directly,
        bypassing ``MemoryService.save_memory`` (and therefore the
        per-memory ``EmbeddingService.refresh_memory`` hook). This is the
        "no embedding cost per deleted entity" invariant from the plan."""
        await storage.save_memory(
            learning="x",
            entities=["mydb.orders", "mydb.orders.amount"],
        )
        # Patch the refresh method itself; if cascade routed through
        # MemoryService.save_memory, it would have called refresh_memory
        # for the rewrite.
        with patch(
            "slayer.embeddings.service.EmbeddingService.refresh_memory",
        ) as mock_refresh:
            await storage.delete_model("orders", data_source="mydb")
            assert mock_refresh.call_count == 0

    async def test_cascade_does_not_call_embed_batch(
        self, storage: StorageBackend, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Belt-and-suspenders: even if refresh fired, the actual
        litellm call must not happen because cascade-strip never changes
        the embedded text (tags excluded from memory embedding text)."""
        from slayer.embeddings import client as embedding_client

        # The autouse conftest stubs ``is_available`` with a plain
        # lambda; reach past it here to force "channel available".
        monkeypatch.setattr(embedding_client, "is_available", lambda: True)
        await storage.save_memory(
            learning="x",
            entities=["mydb.orders", "mydb.orders.amount"],
        )
        with patch(
            "slayer.embeddings.service.embed_batch",
        ) as mock_embed:
            await storage.delete_model("orders", data_source="mydb")
            assert mock_embed.call_count == 0


class TestCascadeLostUpdateWindow:
    async def test_upsert_between_cascade_read_and_write_is_lost(
        self, storage: StorageBackend, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Per-row read-modify-write semantics: a concurrent
        ``save_memory(id=..)`` upsert that lands between cascade's read
        and write is observable as a lost update for the cascade write.
        The retrieval-time filter hides the stale tag at search time,
        so end users never see it.

        Deterministic simulation: monkey-patch ``_get_memory_row`` so
        the second-read inside the cascade's per-row RMW first lands an
        independent upsert that adds a fresh entity, then returns the
        original (now-stale) row. The cascade writes back the
        original-minus-cascade-target, losing the interleaved upsert's
        new entity."""
        from slayer.storage import base as base_mod

        seed = await storage.save_memory(
            id="m1",
            learning="x",
            entities=["mydb.orders.amount"],
        )

        # Save the original method so we can call it from inside the
        # interceptor + restore later.
        original_get_row = storage._get_memory_row
        interleaved = {"fired": False}

        async def _intercept(memory_id: str):
            row = await original_get_row(memory_id)
            if memory_id == "m1" and not interleaved["fired"]:
                interleaved["fired"] = True
                # Concurrent upsert: same row with an extra entity we
                # want the cascade to NOT clobber.
                await storage.save_memory(
                    id="m1",
                    learning="x",
                    entities=["mydb.orders.amount", "concurrent.tag"],
                )
            return row

        monkeypatch.setattr(storage, "_get_memory_row", _intercept)
        await storage.strip_dangling_entities_from_memories(
            canonical_id="mydb.orders.amount",
        )
        monkeypatch.undo()

        loaded = await storage.get_memory(seed.id)
        # Cascade write wins → ``concurrent.tag`` is lost.
        # This is the lost-update window the search-time filter and the
        # ingest-time cleanup sweep are designed to be benign about.
        assert seed.id == "m1"
        assert "concurrent.tag" not in loaded.entities
        # The intended cascade effect did happen.
        assert "mydb.orders.amount" not in loaded.entities
        assert interleaved["fired"], (
            "expected the interleave to actually fire — otherwise the "
            "test isn't exercising the race window"
        )
        # Silence unused-import lint.
        _ = base_mod
