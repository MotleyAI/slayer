"""Storage-backend tests for the unified ``Memory`` entity (DEV-1357 v2).

A ``Memory`` is a single row carrying ``learning`` (free-form text),
``entities`` (canonical strings) and an optional ``query`` (a
``SlayerQuery``). Ids are positive ints; they increase monotonically as
the corpus grows, but ids belonging to deleted memories may be reused
by future saves (DEV-1405 removed the dedicated counter). Both
YAMLStorage and SQLiteStorage must satisfy the same contract — fixtures
parameterise each test against both.

Tests exercise the public ABC API: ``save_memory``, ``get_memory``,
``list_memories``, ``delete_memory``. The ID format / intersection-
filter logic lives on the ABC so backends only implement row-shaped
CRUD + a one-line ``_next_memory_seq`` derived from the existing
``memories`` corpus.
"""

import asyncio
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
        # (The search service treats [] / "no entities" specially at the
        # service level — recency fallback instead of an empty result.)
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
# IDs — monotonic-while-corpus-grows, derived from the existing rows
# ---------------------------------------------------------------------------


class TestMemoryIds:
    async def test_id_starts_at_one(
        self, storage: StorageBackend
    ) -> None:
        """Empty corpus → first save gets id 1 (DEV-1405 edge case)."""
        m = await storage.save_memory(learning="a", entities=["mydb.orders"])
        assert m.id == 1

    async def test_id_monotonic_across_saves(
        self, storage: StorageBackend
    ) -> None:
        a = await storage.save_memory(learning="a", entities=["mydb.orders"])
        b = await storage.save_memory(learning="b", entities=["mydb.orders"])
        c = await storage.save_memory(learning="c", entities=["mydb.orders"])
        assert (a.id, b.id, c.id) == (1, 2, 3)

    async def test_id_reused_after_tail_delete(
        self,
        storage: StorageBackend,
    ) -> None:
        """DEV-1405: ids of deleted memories may be reused. Deleting the
        most recently allocated id frees it for the next save."""
        a = await storage.save_memory(learning="a", entities=["mydb.orders"])
        b = await storage.save_memory(learning="b", entities=["mydb.orders"])
        await storage.delete_memory(b.id)
        c = await storage.save_memory(learning="c", entities=["mydb.orders"])
        # c reuses b's freed id.
        assert (a.id, b.id, c.id) == (1, 2, 2)
        # Reuse points at the new record, not the deleted one.
        loaded = await storage.get_memory(c.id)
        assert loaded.learning == "c"

    async def test_new_save_never_collides_with_existing_id(
        self, storage: StorageBackend
    ) -> None:
        """Reuse only happens against *freed* ids. A new save must never
        return an id currently owned by an existing row — i.e. the
        next-id derivation is strictly above the current max."""
        a = await storage.save_memory(learning="a", entities=["mydb.orders"])
        b = await storage.save_memory(learning="b", entities=["mydb.orders"])
        c = await storage.save_memory(learning="c", entities=["mydb.orders"])
        # Delete a hole in the middle.
        await storage.delete_memory(b.id)
        # Existing rows: {a.id=1, c.id=3}. Next save must NOT pick 1 or 3.
        d = await storage.save_memory(learning="d", entities=["mydb.orders"])
        existing = {m.id for m in await storage.list_memories()}
        assert d.id not in (a.id, c.id)
        assert d.id in existing
        # Spelled out: d.id is strictly above max(remaining ids before save).
        assert d.id == 4

    async def test_id_unified_across_query_and_no_query(
        self,
        storage: StorageBackend,
        sample_query: SlayerQuery,
    ) -> None:
        # Saving a learning-only memory then a query-bearing one walks
        # the same monotonic int sequence — no separate counter per kind.
        a = await storage.save_memory(learning="a", entities=["mydb.orders"])
        b = await storage.save_memory(
            learning="b",
            entities=["mydb.orders"],
            query=sample_query,
        )
        c = await storage.save_memory(learning="c", entities=["mydb.orders"])
        assert (a.id, b.id, c.id) == (1, 2, 3)

    async def test_id_persists_across_backend_reopen(
        self,
    ) -> None:
        """After reopen, the next save's id continues above the highest
        existing memory id. DEV-1405: the value is derived from the
        memories corpus itself, not from a separate counter file."""
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

    async def test_id_derived_when_no_counter_file_exists(
        self,
    ) -> None:
        """DEV-1405: ``counters.yaml`` is no longer used — the next id is
        always derived from the current state of ``memories.yaml``. This
        used to be the recovery path; it's now the only path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ys = YAMLStorage(base_dir=tmpdir)
            await ys.save_memory(learning="a", entities=["mydb.orders"])
            await ys.save_memory(learning="b", entities=["mydb.orders"])
            # No counters.yaml is created on the new code path.
            assert not os.path.exists(os.path.join(tmpdir, "counters.yaml"))
            del ys
            ys2 = YAMLStorage(base_dir=tmpdir)
            third = await ys2.save_memory(
                learning="c", entities=["mydb.orders"]
            )
            assert third.id == 3
            ids = sorted(m.id for m in await ys2.list_memories())
            assert ids == [1, 2, 3]

    async def test_sqlite_seq_derives_from_memories_max(
        self,
    ) -> None:
        """DEV-1405: ``id_counters`` is no longer touched — next id comes
        from ``SELECT MAX(id) + 1 FROM memories``. Pre-existing
        ``id_counters`` rows are harmless dead data; the seq derivation
        ignores them entirely."""
        import sqlite3

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            ss = SQLiteStorage(db_path=db_path)
            await ss.save_memory(learning="a", entities=["mydb.orders"])
            await ss.save_memory(learning="b", entities=["mydb.orders"])
            # If id_counters table still exists from a legacy schema,
            # planting a misleading row in it must not affect future
            # allocations — we read MAX(id) from memories now.
            with sqlite3.connect(db_path) as conn:
                tables = {
                    r[0] for r in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
                if "id_counters" in tables:
                    conn.execute(
                        "INSERT OR REPLACE INTO id_counters "
                        "(counter_name, last_value) VALUES (?, ?)",
                        ("memory_seq", 99),
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


# ---------------------------------------------------------------------------
# Concurrent save_memory atomicity (DEV-1405 codex review)
# ---------------------------------------------------------------------------


async def test_sqlite_concurrent_save_memory_assigns_unique_ids(
) -> None:
    """REGRESSION (DEV-1405 / codex review): two concurrent ``save_memory``
    calls on SQLite must produce two distinct ids. The pre-fix code did
    ``SELECT MAX(id) + 1`` followed by a separate ``INSERT``; under
    concurrent calls both could read the same MAX and the later
    ``INSERT OR REPLACE`` would silently clobber the earlier row. The
    fixed flow runs the id reservation and the insert inside a single
    SQLite transaction (``INSERT ... RETURNING id``) so the write lock
    serialises them."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        store = SQLiteStorage(db_path=db_path)
        # Fire N concurrent saves.
        n = 25
        results = await asyncio.gather(*[
            store.save_memory(learning=f"m{i}", entities=["mydb.orders"])
            for i in range(n)
        ])
        ids = [m.id for m in results]
        # Every id must be unique.
        assert len(set(ids)) == n, f"id collision: {ids}"
        # Every memory must be retrievable with its own learning intact.
        for m in results:
            loaded = await store.get_memory(m.id)
            assert loaded.learning == m.learning
