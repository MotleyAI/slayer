"""DEV-1943 L1 — the one sqlite door (``slayer/storage/sqlite_conn.py``).

``transaction`` commits on success / rolls back on error and always closes;
``open_connection`` never commits and always closes; both pass kwargs through
and leave nothing for the garbage collector to warn about on 3.13+.
"""

from __future__ import annotations

import gc
import sqlite3
import sys
import warnings
from pathlib import Path

import pytest

from slayer.storage.sqlite_conn import open_connection, transaction

_ON_313 = sys.version_info >= (3, 13)


def _rows(db: Path) -> list[tuple]:
    with open_connection(db) as con:
        return con.execute("SELECT v FROM t ORDER BY v").fetchall()


@pytest.fixture
def seeded_db(tmp_path: Path) -> Path:
    db = tmp_path / "door.db"
    with transaction(db) as conn:
        conn.execute("CREATE TABLE t (v INTEGER)")
    return db


def test_transaction_commits_on_success(seeded_db: Path) -> None:
    with transaction(seeded_db) as conn:
        conn.execute("INSERT INTO t (v) VALUES (1)")
    assert _rows(seeded_db) == [(1,)]


def test_transaction_rolls_back_on_error(seeded_db: Path) -> None:
    def _write_then_fail() -> None:
        with transaction(seeded_db) as conn:
            conn.execute("INSERT INTO t (v) VALUES (99)")
            raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        _write_then_fail()
    assert _rows(seeded_db) == []


def test_transaction_closes_the_connection(seeded_db: Path) -> None:
    with transaction(seeded_db) as conn:
        pass
    with pytest.raises(sqlite3.ProgrammingError):
        conn.execute("SELECT 1")


def test_open_connection_closes_the_connection(seeded_db: Path) -> None:
    with open_connection(seeded_db) as conn:
        pass
    with pytest.raises(sqlite3.ProgrammingError):
        conn.execute("SELECT 1")


def test_open_connection_does_not_commit(seeded_db: Path) -> None:
    # No explicit commit -> the write is rolled back on close.
    with open_connection(seeded_db) as conn:
        conn.execute("INSERT INTO t (v) VALUES (7)")
    assert _rows(seeded_db) == []
    # With an explicit commit the caller's write survives.
    with open_connection(seeded_db) as conn:
        conn.execute("INSERT INTO t (v) VALUES (8)")
        conn.commit()
    assert _rows(seeded_db) == [(8,)]


def test_kwargs_pass_through(seeded_db: Path) -> None:
    with open_connection(seeded_db, isolation_level=None) as conn:
        assert conn.isolation_level is None
    with transaction(seeded_db, isolation_level="IMMEDIATE") as conn:
        assert conn.isolation_level == "IMMEDIATE"


@pytest.mark.skipif(not _ON_313, reason="`unclosed database` ResourceWarning is 3.13+")
def test_door_emits_no_unclosed_database_warning(tmp_path: Path) -> None:
    db = tmp_path / "warn.db"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with transaction(db) as conn:
            conn.execute("CREATE TABLE t (v INTEGER)")
        with open_connection(db) as conn:
            conn.execute("SELECT * FROM t")
        gc.collect()
    unclosed = [w for w in caught if "unclosed database" in str(w.message).lower()]
    assert not unclosed, [str(w.message) for w in unclosed]
