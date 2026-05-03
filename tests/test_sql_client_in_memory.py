"""Detection tests for in-memory SQLite connection strings.

Pin the contract that ``_is_in_memory_sqlite`` classifies every valid
in-memory SQLite connection string (including URI-form variants) as
in-memory, and never misclassifies file-backed or non-SQLite strings.

Will fail at collection time before the fix lands — the helper does not
exist yet.
"""

import pytest

from slayer.sql.client import _is_in_memory_sqlite


@pytest.mark.parametrize(
    "connection_string,expected",
    [
        # Bare DBAPI form
        (":memory:", True),
        # Standard SQLAlchemy in-memory form
        ("sqlite:///:memory:", True),
        # Empty path also defaults to in-memory in SQLAlchemy
        ("sqlite://", True),
        # URI-form named in-memory ("file::memory:")
        ("sqlite:///file::memory:?cache=shared&uri=true", True),
        # URI-form with explicit mode=memory
        ("sqlite:///file:foo?mode=memory&cache=shared&uri=true", True),
        # File-backed must NOT classify as in-memory
        ("sqlite:///foo.db", False),
        ("sqlite:////tmp/foo.db", False),
        # Non-SQLite connection strings must NOT classify as in-memory,
        # even if they coincidentally contain ":memory:"
        ("postgresql://u:p@h/db", False),
        ("postgresql:///:memory:", False),
        # Edge cases
        ("", False),
        ("not a url", False),
    ],
)
def test_is_in_memory_sqlite(connection_string: str, expected: bool) -> None:
    assert _is_in_memory_sqlite(connection_string) is expected
