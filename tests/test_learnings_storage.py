"""Storage-backend tests for Learning + SavedQuery (DEV-1357).

Both YAMLStorage and SQLiteStorage must satisfy the same CRUD + filter +
ID-allocation contract. The fixtures are parameterised so every test runs
against both backends; any divergence in observable behaviour is a bug.

Tests in this module exercise the public API on ``StorageBackend`` —
``save_learning``, ``get_learning``, ``list_learnings``, ``delete_learning``
and the saved-queries equivalents. The thin layer that allocates IDs and
formats them as ``L<int>`` / ``Q<int>`` lives on the ABC (per the
backend-agnostic feedback rule); only the row-shaped CRUD and the seq
counter are abstract.
"""

import os
import tempfile
from typing import Iterator

import pytest

from slayer.core.errors import LearningOrQueryNotFoundError
from slayer.core.models import ModelMeasure
from slayer.core.query import SlayerQuery
from slayer.learnings.models import Learning, SavedQuery
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture(params=["yaml", "sqlite"])
def storage(request: pytest.FixtureRequest) -> Iterator[StorageBackend]:
    """Yield each backend in turn so every test runs against both."""
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
# Learnings — round-trip CRUD
# ---------------------------------------------------------------------------


class TestLearningsStorage:
    async def test_save_returns_learning_with_assigned_id(
        self, storage: StorageBackend
    ) -> None:
        learning = await storage.save_learning(
            body="orders.is_returned is in {0,1,NULL}; treat NULL as not returned",
            entities=["mydb.orders.is_returned"],
        )
        assert isinstance(learning, Learning)
        assert learning.id == "L1"
        assert learning.body.startswith("orders.is_returned")
        assert learning.entities == ["mydb.orders.is_returned"]
        assert learning.version == 1
        # created_at must be populated automatically.
        assert learning.created_at is not None

    async def test_get_returns_saved_learning(
        self, storage: StorageBackend
    ) -> None:
        saved = await storage.save_learning(
            body="note one", entities=["mydb.orders"]
        )
        loaded = await storage.get_learning(saved.id)
        assert loaded.id == saved.id
        assert loaded.body == "note one"
        assert loaded.entities == ["mydb.orders"]

    async def test_get_missing_raises(self, storage: StorageBackend) -> None:
        with pytest.raises(LearningOrQueryNotFoundError):
            await storage.get_learning("L999")

    async def test_delete_missing_raises(self, storage: StorageBackend) -> None:
        with pytest.raises(LearningOrQueryNotFoundError):
            await storage.delete_learning("L999")

    async def test_delete_removes_row(self, storage: StorageBackend) -> None:
        saved = await storage.save_learning(body="x", entities=["mydb.orders"])
        await storage.delete_learning(saved.id)
        with pytest.raises(LearningOrQueryNotFoundError):
            await storage.get_learning(saved.id)

    async def test_list_empty(self, storage: StorageBackend) -> None:
        assert await storage.list_learnings() == []

    async def test_list_returns_all_when_entities_none(
        self, storage: StorageBackend
    ) -> None:
        a = await storage.save_learning(body="a", entities=["mydb.orders"])
        b = await storage.save_learning(
            body="b", entities=["mydb.customers.name"]
        )
        ids = sorted(x.id for x in await storage.list_learnings())
        assert ids == sorted([a.id, b.id])

    async def test_list_filters_by_entity_intersection(
        self, storage: StorageBackend
    ) -> None:
        a = await storage.save_learning(
            body="a", entities=["mydb.orders", "mydb.orders.amount"]
        )
        b = await storage.save_learning(
            body="b", entities=["mydb.customers"]
        )
        c = await storage.save_learning(
            body="c", entities=["mydb.orders.amount", "mydb.customers"]
        )
        # Filter by a single entity that only ``a`` and ``c`` reference.
        result = await storage.list_learnings(entities=["mydb.orders.amount"])
        assert sorted(x.id for x in result) == sorted([a.id, c.id])
        # Filter by an entity none of them reference → empty.
        result = await storage.list_learnings(entities=["mydb.unknown"])
        assert result == []
        # Filter by mixed entities returns the union of rows whose stored
        # entity set has non-empty intersection with the input.
        result = await storage.list_learnings(
            entities=["mydb.customers", "mydb.orders"]
        )
        assert sorted(x.id for x in result) == sorted([a.id, b.id, c.id])

    async def test_list_with_empty_entities_returns_empty(
        self, storage: StorageBackend
    ) -> None:
        await storage.save_learning(body="x", entities=["mydb.orders"])
        # Empty list → intersection with any stored set is empty → no matches.
        # (entity_search treats [] as None, but list_learnings is a
        # straight intersection-filter primitive.)
        assert await storage.list_learnings(entities=[]) == []

    async def test_id_monotonic_across_saves(
        self, storage: StorageBackend
    ) -> None:
        a = await storage.save_learning(body="a", entities=["mydb.orders"])
        b = await storage.save_learning(body="b", entities=["mydb.orders"])
        c = await storage.save_learning(body="c", entities=["mydb.orders"])
        assert (a.id, b.id, c.id) == ("L1", "L2", "L3")

    async def test_id_not_reused_after_delete(
        self, storage: StorageBackend
    ) -> None:
        a = await storage.save_learning(body="a", entities=["mydb.orders"])
        b = await storage.save_learning(body="b", entities=["mydb.orders"])
        await storage.delete_learning(b.id)
        c = await storage.save_learning(body="c", entities=["mydb.orders"])
        # b's id (L2) must not be reused; the next allocation yields L3.
        assert a.id == "L1"
        assert b.id == "L2"
        assert c.id == "L3"

    async def test_id_counter_persists_across_backend_reopen(
        self,
    ) -> None:
        """Closing and reopening the storage must not reset the counter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")

            # YAML
            yaml_dir = os.path.join(tmpdir, "yaml")
            os.makedirs(yaml_dir)
            ys = YAMLStorage(base_dir=yaml_dir)
            await ys.save_learning(body="a", entities=["mydb.orders"])
            await ys.save_learning(body="b", entities=["mydb.orders"])
            del ys
            ys2 = YAMLStorage(base_dir=yaml_dir)
            third = await ys2.save_learning(body="c", entities=["mydb.orders"])
            assert third.id == "L3"

            # SQLite
            ss = SQLiteStorage(db_path=db_path)
            await ss.save_learning(body="a", entities=["mydb.orders"])
            await ss.save_learning(body="b", entities=["mydb.orders"])
            del ss
            ss2 = SQLiteStorage(db_path=db_path)
            third = await ss2.save_learning(body="c", entities=["mydb.orders"])
            assert third.id == "L3"


# ---------------------------------------------------------------------------
# SavedQueries — round-trip CRUD
# ---------------------------------------------------------------------------


class TestSavedQueriesStorage:
    async def test_save_returns_saved_query_with_assigned_id(
        self, storage: StorageBackend, sample_query: SlayerQuery
    ) -> None:
        saved = await storage.save_saved_query(
            query=sample_query,
            description="Total order count",
            entities=["mydb.orders"],
        )
        assert isinstance(saved, SavedQuery)
        assert saved.id == "Q1"
        assert saved.description == "Total order count"
        assert saved.entities == ["mydb.orders"]
        assert isinstance(saved.query, SlayerQuery)
        assert saved.query.source_model == "orders"
        assert saved.version == 1

    async def test_round_trip_preserves_query_shape(
        self, storage: StorageBackend, sample_query: SlayerQuery
    ) -> None:
        saved = await storage.save_saved_query(
            query=sample_query,
            description="d",
            entities=["mydb.orders"],
        )
        loaded = await storage.get_saved_query(saved.id)
        assert isinstance(loaded.query, SlayerQuery)
        assert loaded.query.source_model == "orders"
        assert loaded.query.measures is not None
        assert len(loaded.query.measures) == 1
        assert loaded.query.measures[0].formula == "*:count"

    async def test_get_missing_raises(self, storage: StorageBackend) -> None:
        with pytest.raises(LearningOrQueryNotFoundError):
            await storage.get_saved_query("Q999")

    async def test_delete_missing_raises(
        self, storage: StorageBackend
    ) -> None:
        with pytest.raises(LearningOrQueryNotFoundError):
            await storage.delete_saved_query("Q999")

    async def test_delete_removes_row(
        self, storage: StorageBackend, sample_query: SlayerQuery
    ) -> None:
        saved = await storage.save_saved_query(
            query=sample_query, description="d", entities=["mydb.orders"]
        )
        await storage.delete_saved_query(saved.id)
        with pytest.raises(LearningOrQueryNotFoundError):
            await storage.get_saved_query(saved.id)

    async def test_list_empty(self, storage: StorageBackend) -> None:
        assert await storage.list_saved_queries() == []

    async def test_list_returns_all_when_entities_none(
        self, storage: StorageBackend, sample_query: SlayerQuery
    ) -> None:
        a = await storage.save_saved_query(
            query=sample_query, description="a", entities=["mydb.orders"]
        )
        b = await storage.save_saved_query(
            query=sample_query, description="b", entities=["mydb.customers"]
        )
        ids = sorted(x.id for x in await storage.list_saved_queries())
        assert ids == sorted([a.id, b.id])

    async def test_list_filters_by_entity_intersection(
        self, storage: StorageBackend, sample_query: SlayerQuery
    ) -> None:
        a = await storage.save_saved_query(
            query=sample_query,
            description="a",
            entities=["mydb.orders", "mydb.orders.amount"],
        )
        b = await storage.save_saved_query(
            query=sample_query,
            description="b",
            entities=["mydb.customers"],
        )
        c = await storage.save_saved_query(
            query=sample_query,
            description="c",
            entities=["mydb.orders.amount", "mydb.customers"],
        )
        result = await storage.list_saved_queries(
            entities=["mydb.orders.amount"]
        )
        assert sorted(x.id for x in result) == sorted([a.id, c.id])
        result = await storage.list_saved_queries(entities=["mydb.unknown"])
        assert result == []
        result = await storage.list_saved_queries(
            entities=["mydb.customers", "mydb.orders"]
        )
        assert sorted(x.id for x in result) == sorted([a.id, b.id, c.id])

    async def test_id_monotonic_and_no_reuse(
        self, storage: StorageBackend, sample_query: SlayerQuery
    ) -> None:
        a = await storage.save_saved_query(
            query=sample_query, description="a", entities=["mydb.orders"]
        )
        b = await storage.save_saved_query(
            query=sample_query, description="b", entities=["mydb.orders"]
        )
        await storage.delete_saved_query(b.id)
        c = await storage.save_saved_query(
            query=sample_query, description="c", entities=["mydb.orders"]
        )
        assert (a.id, b.id, c.id) == ("Q1", "Q2", "Q3")

    async def test_learning_and_query_seqs_are_independent(
        self, storage: StorageBackend, sample_query: SlayerQuery
    ) -> None:
        await storage.save_learning(body="l1", entities=["mydb.orders"])
        q1 = await storage.save_saved_query(
            query=sample_query, description="q1", entities=["mydb.orders"]
        )
        await storage.save_learning(body="l2", entities=["mydb.orders"])
        q2 = await storage.save_saved_query(
            query=sample_query, description="q2", entities=["mydb.orders"]
        )
        # Saved-query counter and learning counter are independent.
        assert q1.id == "Q1"
        assert q2.id == "Q2"
        learnings = await storage.list_learnings()
        assert sorted(x.id for x in learnings) == ["L1", "L2"]


# ---------------------------------------------------------------------------
# Cross-cutting: the persisted Learning/SavedQuery shape round-trips through
# whatever serialization the backend uses (YAML or JSON).
# ---------------------------------------------------------------------------


class TestPersistedShape:
    async def test_learning_entities_order_preserved(
        self, storage: StorageBackend
    ) -> None:
        saved = await storage.save_learning(
            body="x",
            entities=["mydb.orders.amount", "mydb.orders", "mydb.customers"],
        )
        loaded = await storage.get_learning(saved.id)
        assert loaded.entities == [
            "mydb.orders.amount",
            "mydb.orders",
            "mydb.customers",
        ]

    async def test_saved_query_entities_order_preserved(
        self, storage: StorageBackend, sample_query: SlayerQuery
    ) -> None:
        saved = await storage.save_saved_query(
            query=sample_query,
            description="d",
            entities=["mydb.orders.amount", "mydb.orders", "mydb.customers"],
        )
        loaded = await storage.get_saved_query(saved.id)
        assert loaded.entities == [
            "mydb.orders.amount",
            "mydb.orders",
            "mydb.customers",
        ]
