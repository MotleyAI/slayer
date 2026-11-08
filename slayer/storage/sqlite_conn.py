"""The one sqlite door (DEV-1943 L1): every raw ``sqlite3`` connection opens here.

``transaction`` commits on success / rolls back on error, then closes;
``open_connection`` only closes (for callers that drive ``BEGIN`` themselves).
The sqlite3 context manager commits or rolls back but never closes, so a bare
``with sqlite3.connect(...)`` leaks the connection until GC — silent before
CPython 3.13, a ``ResourceWarning`` after.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from os import PathLike
from typing import Any, Union

DbPath = Union[str, PathLike[str]]


@contextmanager
def transaction(db_path: DbPath, **kwargs: Any) -> Generator[sqlite3.Connection]:
    """Yield a connection; commit on success, roll back on error, always close."""
    conn = sqlite3.connect(db_path, **kwargs)
    try:
        with conn:
            yield conn
    finally:
        conn.close()


@contextmanager
def open_connection(db_path: DbPath, **kwargs: Any) -> Generator[sqlite3.Connection]:
    """Yield a connection and always close it; never commits (the caller decides)."""
    conn = sqlite3.connect(db_path, **kwargs)
    try:
        yield conn
    finally:
        conn.close()
