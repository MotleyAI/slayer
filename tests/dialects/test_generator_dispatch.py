"""DEV-1542: tests for SQLGenerator's dialect-strategy dispatch.

After the refactor, ``SQLGenerator.__init__`` accepts either a sqlglot
name string OR a ``SqlDialect`` instance. ``self.dialect`` is a read-only
property derived from ``self._dialect.sqlglot_name``.
"""

from __future__ import annotations

import pytest

from slayer.sql.dialects.mysql import MysqlDialect
from slayer.sql.dialects.postgres import PostgresDialect
from slayer.sql.dialects.sqlite import SqliteDialect
from slayer.sql.generator import SQLGenerator


def test_sqlgenerator_accepts_dialect_string_postgres() -> None:
    gen = SQLGenerator(dialect="postgres")
    assert gen.dialect == "postgres"
    assert isinstance(gen._dialect, PostgresDialect)


def test_sqlgenerator_accepts_dialect_string_sqlite() -> None:
    gen = SQLGenerator(dialect="sqlite")
    assert gen.dialect == "sqlite"
    assert isinstance(gen._dialect, SqliteDialect)


def test_sqlgenerator_accepts_dialect_string_mysql() -> None:
    gen = SQLGenerator(dialect="mysql")
    assert gen.dialect == "mysql"
    assert isinstance(gen._dialect, MysqlDialect)


def test_sqlgenerator_accepts_sqldialect_instance() -> None:
    """Caller can pass a pre-constructed dialect instance — useful for tests
    that want to swap in a custom subclass without going through the registry."""
    d = SqliteDialect()
    gen = SQLGenerator(dialect=d)
    assert gen._dialect is d
    assert gen.dialect == "sqlite"


def test_sqlgenerator_default_dialect_is_postgres() -> None:
    """No-arg construction defaults to Postgres (matches today's default)."""
    gen = SQLGenerator()
    assert gen.dialect == "postgres"
    assert isinstance(gen._dialect, PostgresDialect)


def test_sqlgenerator_dialect_property_is_read_only() -> None:
    """``dialect`` is a ``@property`` derived from ``self._dialect.sqlglot_name``.

    Setting it must fail (Codex finding #5). This prevents the double-state
    bug where ``gen.dialect = "..."`` would desync the strategy object from
    the string sqlglot consumes.
    """
    gen = SQLGenerator(dialect="postgres")
    with pytest.raises(AttributeError):
        gen.dialect = "sqlite"  # type: ignore[misc]


def test_sqlgenerator_unknown_dialect_string_raises() -> None:
    """Unknown sqlglot name routes through strict ``get_dialect`` → KeyError.

    Wrapped or unwrapped is fine — the point is that
    ``SQLGenerator(dialect="not_a_dialect")`` does NOT silently fall back
    to Postgres (Codex finding #2).
    """
    with pytest.raises((KeyError, ValueError)):
        SQLGenerator(dialect="not_a_dialect")


def test_sqlgenerator_dialect_attribute_used_by_sqlglot_emission() -> None:
    """The string ``self.dialect`` is the sqlglot dialect arg in dozens of
    ``expr.sql(dialect=self.dialect)`` call sites. Validate that the
    property returns a value sqlglot recognises end-to-end by smoke-testing
    a tiny generation."""
    gen = SQLGenerator(dialect="sqlite")
    # The string must be usable as the sqlglot dialect arg
    import sqlglot
    parsed = sqlglot.parse_one("SELECT 1", dialect=gen.dialect)
    assert parsed.sql(dialect=gen.dialect) == "SELECT 1"
