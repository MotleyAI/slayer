"""Storage-backend tests for the unified ``Memory`` entity (DEV-1357 v2).

A ``Memory`` is a single row carrying ``learning`` (free-form text),
``entities`` (canonical strings) and an optional ``query`` (a
``SlayerQuery``). One unified counter allocates monotonic positive
integer ids; deleted ids are never reused. Both YAMLStorage and
SQLiteStorage must satisfy the same contract — fixtures parameterise
each test against both.

Tests exercise the public ABC API: ``save_memory``, ``get_memory``,
``list_memories``, ``delete_memory``. The ID format / monotonicity /
intersection-filter logic lives on the ABC so backends only implement
row-shaped CRUD + the seq counter.
"""

import os
import tempfile
from typing import Iterator

import pytest

from slayer.core.errors import MemoryNotFoundError
from slayer.core.models import ModelMeasure
from slayer.core.query import SlayerQuery
from slayer.memories.models import Memory
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture(params=["yaml", "sqlite"])
def storage(request: pytest.FixtureRequest) -> Iterator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmpdir:
        if request.param == "yaml":
            yield YAMLStorage(base_dir=tmpdir)
        else:
            yield SQLiteStorage(db_path=os.path.join(tmpdir, "test.db"))


@pytest.fixture
def sample_query() -> SlayerQuery:
    return SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="*:count")],
    )


# ---------------------------------------------------------------------------
# CRUD round-trips
# ---------------------------------------------------------------------------


class TestMemoryCRUD:
    async def test_save_returns_memory_with_int_id(
        self, storage: StorageBackend
    ) -> None:
        memory = await storage.save_memory(
            learning="orders.is_returned ∈ {0,1,NULL}; treat NULL as not returned",
            entities=["mydb.orders.is_returned"],
        )
        assert isinstance(memory, Memory)
        assert isinstance(memory.id, int)
        assert memory.id == 1
        assert memory.learning.startswith("orders.is_returned")
        assert memory.entities == ["mydb.orders.is_returned"]
        assert memory.query is None
        assert memory.version == 1
        assert memory.created_at is not None

    async def test_save_with_query_persists_query(
        self,
        storage: StorageBackend,
        sample_query: SlayerQuery,
    ) -> None:
        memory = await storage.save_memory(
            learning="example: total order count",
            entities=["mydb.orders"],
            query=sample_query,
        )
        assert isinstance(memory.query, SlayerQuery)
        assert memory.query.source_model == "orders"

    async def test_get_returns_saved_memory(
        self, storage: StorageBackend
    ) -> None:
        saved = await storage.save_memory(
            learning="note one", entities=["mydb.orders"]
        )
        loaded = await storage.get_memory(saved.id)
        assert loaded.id == saved.id
        assert loaded.learning == "note one"
        assert loaded.entities == ["mydb.orders"]
        assert loaded.query is None

    async def test_round_trip_preserves_query_shape(
        self,
        storage: StorageBackend,
        sample_query: SlayerQuery,
    ) -> None:
        saved = await storage.save_memory(
            learning="x",
            entities=["mydb.orders"],
            query=sample_query,
        )
        loaded = await storage.get_memory(saved.id)
        assert isinstance(loaded.query, SlayerQuery)
        assert loaded.query.source_model == "orders"
        assert loaded.query.measures is not None
        assert len(loaded.query.measures) == 1
        assert loaded.query.measures[0].formula == "*:count"

    async def test_get_missing_raises(self, storage: StorageBackend) -> None:
        with pytest.raises(MemoryNotFoundError):
            await storage.get_memory(999)

    async def test_delete_missing_raises(self, storage: StorageBackend) -> None:
        with pytest.raises(MemoryNotFoundError):
            await storage.delete_memory(999)

    async def test_delete_removes_row(self, storage: StorageBackend) -> None:
        saved = await storage.save_memory(
            learning="x", entities=["mydb.orders"]
        )
        await storage.delete_memory(saved.id)
        with pytest.raises(MemoryNotFoundError):
            await storage.get_memory(saved.id)


# ---------------------------------------------------------------------------
# list_memories — filtering by entity intersection
# ---------------------------------------------------------------------------


class TestListMemories:
    async def test_list_empty(self, storage: StorageBackend) -> None:
        assert await storage.list_memories() == []

    async def test_list_returns_all_when_entities_none(
        self, storage: StorageBackend
    ) -> None:
        a = await storage.save_memory(
            learning="a", entities=["mydb.orders"]
        )
        b = await storage.save_memory(
            learning="b", entities=["mydb.customers.name"]
        )
        ids = sorted(x.id for x in await storage.list_memories())
        assert ids == sorted([a.id, b.id])

    async def test_list_filters_by_entity_intersection(
        self, storage: StorageBackend
    ) -> None:
        a = await storage.save_memory(
            learning="a", entities=["mydb.orders", "mydb.orders.amount"]
        )
        b = await storage.save_memory(
            learning="b", entities=["mydb.customers"]
        )
        c = await storage.save_memory(
            learning="c",
            entities=["mydb.orders.amount", "mydb.customers"],
        )
        result = await storage.list_memories(entities=["mydb.orders.amount"])
        assert sorted(x.id for x in result) == sorted([a.id, c.id])
        result = await storage.list_memories(entities=["mydb.unknown"])
        assert result == []
        result = await storage.list_memories(
            entities=["mydb.customers", "mydb.orders"]
        )
        assert sorted(x.id for x in result) == sorted([a.id, b.id, c.id])

    async def test_list_with_empty_entities_returns_empty(
        self, storage: StorageBackend
    ) -> None:
        await storage.save_memory(learning="x", entities=["mydb.orders"])
        # ``entities=[]`` is a strict intersection-filter primitive: any
        # non-empty set has empty intersection with [], so nothing matches.
        # (recall_memories at the service level treats [] specially.)
        assert await storage.list_memories(entities=[]) == []

    async def test_list_includes_query_bearing_memories(
        self,
        storage: StorageBackend,
        sample_query: SlayerQuery,
    ) -> None:
        a = await storage.save_memory(
            learning="learning-only", entities=["mydb.orders"]
        )
        b = await storage.save_memory(
            learning="with-query",
            entities=["mydb.orders"],
            query=sample_query,
        )
        result = await storage.list_memories(entities=["mydb.orders"])
        assert sorted(x.id for x in result) == sorted([a.id, b.id])
        # The query field round-trips through the filter.
        with_q = next(x for x in result if x.id == b.id)
        assert isinstance(with_q.query, SlayerQuery)


# ---------------------------------------------------------------------------
# IDs — monotonic, no reuse, persisted across reopens
# ---------------------------------------------------------------------------


class TestMemoryIds:
    async def test_id_monotonic_across_saves(
        self, storage: StorageBackend
    ) -> None:
        a = await storage.save_memory(learning="a", entities=["mydb.orders"])
        b = await storage.save_memory(learning="b", entities=["mydb.orders"])
        c = await storage.save_memory(learning="c", entities=["mydb.orders"])
        assert (a.id, b.id, c.id) == (1, 2, 3)

    async def test_id_not_reused_after_delete(
        self, storage: StorageBackend
    ) -> None:
        a = await storage.save_memory(learning="a", entities=["mydb.orders"])
        b = await storage.save_memory(learning="b", entities=["mydb.orders"])
        await storage.delete_memory(b.id)
        c = await storage.save_memory(learning="c", entities=["mydb.orders"])
        assert (a.id, b.id, c.id) == (1, 2, 3)

    async def test_id_unified_across_query_and_no_query(
        self,
        storage: StorageBackend,
        sample_query: SlayerQuery,
    ) -> None:
        # Single counter — saving a learning-only memory then a
        # query-bearing one walks the same monotonic int sequence.
        a = await storage.save_memory(learning="a", entities=["mydb.orders"])
        b = await storage.save_memory(
            learning="b",
            entities=["mydb.orders"],
            query=sample_query,
        )
        c = await storage.save_memory(learning="c", entities=["mydb.orders"])
        assert (a.id, b.id, c.id) == (1, 2, 3)

    async def test_id_counter_persists_across_backend_reopen(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            yaml_dir = os.path.join(tmpdir, "yaml")
            os.makedirs(yaml_dir)
            ys = YAMLStorage(base_dir=yaml_dir)
            await ys.save_memory(learning="a", entities=["mydb.orders"])
            await ys.save_memory(learning="b", entities=["mydb.orders"])
            del ys
            ys2 = YAMLStorage(base_dir=yaml_dir)
            third = await ys2.save_memory(
                learning="c", entities=["mydb.orders"]
            )
            assert third.id == 3

            db_path = os.path.join(tmpdir, "test.db")
            ss = SQLiteStorage(db_path=db_path)
            await ss.save_memory(learning="a", entities=["mydb.orders"])
            await ss.save_memory(learning="b", entities=["mydb.orders"])
            del ss
            ss2 = SQLiteStorage(db_path=db_path)
            third = await ss2.save_memory(
                learning="c", entities=["mydb.orders"]
            )
            assert third.id == 3

    async def test_yaml_seq_recovers_when_counters_file_missing(
        self,
    ) -> None:
        """If counters.yaml is wiped but memories.yaml still has rows,
        the next allocation must skip past existing ids — not restart at
        1 and overwrite memory 1 via _save_memory_row's id-replace."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ys = YAMLStorage(base_dir=tmpdir)
            await ys.save_memory(learning="a", entities=["mydb.orders"])
            await ys.save_memory(learning="b", entities=["mydb.orders"])
            os.remove(os.path.join(tmpdir, "counters.yaml"))
            del ys
            ys2 = YAMLStorage(base_dir=tmpdir)
            third = await ys2.save_memory(
                learning="c", entities=["mydb.orders"]
            )
            assert third.id == 3
            # All three rows must still exist — no id collision.
            ids = sorted(m.id for m in await ys2.list_memories())
            assert ids == [1, 2, 3]

    async def test_sqlite_seq_seeds_from_max_id_when_counter_missing(
        self,
    ) -> None:
        """If id_counters has no row for memory_seq but memories has
        rows (e.g. someone restored memories without the counter row),
        seq must seed from MAX(id) — not return 1 and let
        _save_memory_sync clobber memory 1 via INSERT OR REPLACE."""
        import sqlite3

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            ss = SQLiteStorage(db_path=db_path)
            await ss.save_memory(learning="a", entities=["mydb.orders"])
            await ss.save_memory(learning="b", entities=["mydb.orders"])
            # Wipe just the counter row, leaving the memories rows.
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "DELETE FROM id_counters WHERE counter_name = ?",
                    ("memory_seq",),
                )
            del ss
            ss2 = SQLiteStorage(db_path=db_path)
            third = await ss2.save_memory(
                learning="c", entities=["mydb.orders"]
            )
            assert third.id == 3
            ids = sorted(m.id for m in await ss2.list_memories())
            assert ids == [1, 2, 3]


# ---------------------------------------------------------------------------
# Persisted shape preservation
# ---------------------------------------------------------------------------


class TestPersistedShape:
    async def test_entities_order_preserved(
        self, storage: StorageBackend
    ) -> None:
        saved = await storage.save_memory(
            learning="x",
            entities=["mydb.orders.amount", "mydb.orders", "mydb.customers"],
        )
        loaded = await storage.get_memory(saved.id)
        assert loaded.entities == [
            "mydb.orders.amount",
            "mydb.orders",
            "mydb.customers",
        ]
