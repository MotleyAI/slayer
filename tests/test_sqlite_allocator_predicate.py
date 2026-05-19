"""DEV-1428: SQLite ``_next_memory_seq`` allocator predicate.

The predicate must be Python-side (after ``SELECT id FROM memories``)
because SQLite GLOB patterns can't distinguish ``"42abc"`` from ``"42"``
reliably. Tests pin: ``"42abc"`` / ``"001"`` ignored, large numeric ids
handled, empty corpus → 1.
"""

from __future__ import annotations

import os
import tempfile

import pytest

from slayer.storage.sqlite_storage import SQLiteStorage


@pytest.fixture
def sqlite_store():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield SQLiteStorage(db_path=os.path.join(tmpdir, "test.db"))


class TestSqliteAllocatorPredicate:
    async def test_empty_corpus_returns_one(self, sqlite_store) -> None:
        seq = await sqlite_store._next_memory_seq()
        assert seq == "1"

    async def test_ignores_non_digit_suffix(self, sqlite_store) -> None:
        await sqlite_store.save_memory(
            id="42abc", learning="x", entities=["mydb.orders"],
        )
        # The non-digit-suffix id must be ignored by allocator.
        seq = await sqlite_store._next_memory_seq()
        assert seq == "1"

    async def test_ignores_leading_zero_form(self, sqlite_store) -> None:
        await sqlite_store.save_memory(
            id="001", learning="x", entities=["mydb.orders"],
        )
        seq = await sqlite_store._next_memory_seq()
        assert seq == "1"

    async def test_picks_max_int_shaped_plus_one(
        self, sqlite_store,
    ) -> None:
        await sqlite_store.save_memory(
            id="5", learning="x", entities=["mydb.orders"],
        )
        await sqlite_store.save_memory(
            id="42abc", learning="x", entities=["mydb.orders"],
        )
        await sqlite_store.save_memory(
            id="001", learning="x", entities=["mydb.orders"],
        )
        seq = await sqlite_store._next_memory_seq()
        assert seq == "6"

    async def test_large_numeric_id(self, sqlite_store) -> None:
        await sqlite_store.save_memory(
            id="999999999", learning="x", entities=["mydb.orders"],
        )
        seq = await sqlite_store._next_memory_seq()
        assert seq == "1000000000"
