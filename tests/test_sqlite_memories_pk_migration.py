"""DEV-1428: ``_migrate_memories_to_text_pk`` schema rebuild.

Covers the highest-risk legacy states the SQLite migration must
recover from:

1. Pre-DEV-1428 ``memories`` INTEGER PRIMARY KEY + ``memory_entities``
   INTEGER FK (full legacy shape).
2. Partial: ``memories`` already TEXT but ``memory_entities`` still
   INTEGER (a prior crash between the two rebuilds).
3. ``memory_entities`` exists but ``memories`` does not (anomalous,
   but a possible state after a manual drop).
4. Already-migrated DB → no-op.
5. Brand-new DB → no-op (and ``_init_db`` creates fresh tables).
"""

from __future__ import annotations

import os
import sqlite3
import tempfile

import pytest

from slayer.storage.sqlite_storage import SQLiteStorage


def _create_legacy_db(db_path: str, with_data: bool = True) -> None:
    """Build the full pre-DEV-1428 schema with INTEGER PK / FK."""
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE memories (
                id INTEGER PRIMARY KEY,
                data TEXT NOT NULL
            );
            CREATE TABLE memory_entities (
                memory_id INTEGER NOT NULL REFERENCES memories(id)
                    ON DELETE CASCADE,
                entity TEXT NOT NULL,
                PRIMARY KEY (memory_id, entity)
            );
        """)
        if with_data:
            conn.execute(
                "INSERT INTO memories (id, data) VALUES (?, ?)",
                (1, '{"version": 1, "id": 1, "learning": "legacy",'
                    ' "entities": ["mydb.orders"], "query": null}'),
            )
            conn.execute(
                "INSERT INTO memory_entities (memory_id, entity) VALUES (?, ?)",
                (1, "mydb.orders"),
            )


def _id_column_type(db_path: str, table: str, col: str) -> str:
    with sqlite3.connect(db_path) as conn:
        for row in conn.execute(f"PRAGMA table_info({table})"):
            if row[1] == col:
                return row[2].upper()
    return ""


class TestSqliteMemoriesPkMigration:
    def test_full_legacy_rebuild(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "legacy.db")
            _create_legacy_db(db_path)
            # Open through SQLiteStorage → triggers the migration.
            SQLiteStorage(db_path=db_path)
            assert _id_column_type(db_path, "memories", "id") == "TEXT"
            assert (
                _id_column_type(db_path, "memory_entities", "memory_id")
                == "TEXT"
            )
            # Data preserved through the CAST.
            with sqlite3.connect(db_path) as conn:
                rows = conn.execute("SELECT id FROM memories").fetchall()
            assert rows == [("1",)]

    def test_partial_state_memories_text_entities_int_is_repaired(
        self,
    ) -> None:
        """Crash-between-rebuilds case: ``memories`` already TEXT but
        ``memory_entities`` still INTEGER. The migration must finish
        the FK side instead of returning early."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "partial.db")
            with sqlite3.connect(db_path) as conn:
                conn.executescript("""
                    CREATE TABLE memories (
                        id TEXT PRIMARY KEY,
                        data TEXT NOT NULL
                    );
                    CREATE TABLE memory_entities (
                        memory_id INTEGER NOT NULL,
                        entity TEXT NOT NULL,
                        PRIMARY KEY (memory_id, entity)
                    );
                """)
                conn.execute(
                    "INSERT INTO memories (id, data) VALUES (?, ?)",
                    (
                        "7",
                        '{"version": 2, "id": "7", "learning": "x",'
                        ' "entities": [], "query": null}',
                    ),
                )
                conn.execute(
                    "INSERT INTO memory_entities (memory_id, entity) "
                    "VALUES (?, ?)",
                    (7, "mydb.x"),
                )
            SQLiteStorage(db_path=db_path)
            assert (
                _id_column_type(db_path, "memory_entities", "memory_id")
                == "TEXT"
            )
            with sqlite3.connect(db_path) as conn:
                ent_rows = conn.execute(
                    "SELECT memory_id, entity FROM memory_entities"
                ).fetchall()
            assert ent_rows == [("7", "mydb.x")]

    def test_memory_entities_without_memories_is_recreated(self) -> None:
        """Anomalous state: ``memory_entities`` exists but ``memories``
        does not. ``_init_db`` should still create both tables fresh
        without raising."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "weird.db")
            with sqlite3.connect(db_path) as conn:
                conn.executescript("""
                    CREATE TABLE memory_entities (
                        memory_id INTEGER NOT NULL,
                        entity TEXT NOT NULL,
                        PRIMARY KEY (memory_id, entity)
                    );
                """)
            # Should not raise during the migration check OR during
            # _init_db's CREATE IF NOT EXISTS.
            SQLiteStorage(db_path=db_path)
            # memories table now exists (created by _init_db).
            with sqlite3.connect(db_path) as conn:
                tables = {
                    r[0] for r in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
            assert "memories" in tables
            assert "memory_entities" in tables

    def test_already_migrated_db_is_noop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "fresh.db")
            # First open creates fresh TEXT-PK tables.
            SQLiteStorage(db_path=db_path)
            # Second open must not raise / change anything.
            SQLiteStorage(db_path=db_path)
            assert _id_column_type(db_path, "memories", "id") == "TEXT"

    def test_brand_new_db_creates_text_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "empty.db")
            SQLiteStorage(db_path=db_path)
            assert _id_column_type(db_path, "memories", "id") == "TEXT"
            assert (
                _id_column_type(db_path, "memory_entities", "memory_id")
                == "TEXT"
            )

    async def test_legacy_int_rows_round_trip_through_v2_load(
        self,
    ) -> None:
        """End-to-end: a legacy DB with an int-id row must come out of
        ``list_memories`` as a v2 row with the id stringified."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "legacy.db")
            _create_legacy_db(db_path)
            store = SQLiteStorage(db_path=db_path)
            rows = await store.list_memories()
            assert len(rows) == 1
            assert rows[0].id == "1"
            assert rows[0].learning == "legacy"
            assert rows[0].entities == ["mydb.orders"]
            assert rows[0].version == 2


@pytest.fixture(autouse=True)
def _force_close_sqlite_after_test():
    # Best-effort: collect handles so a failing test doesn't leak file
    # locks across TemporaryDirectory teardown on Windows-style FS.
    yield
