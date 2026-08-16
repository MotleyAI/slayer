"""DEV-1542: tests for SQLGenerator's dialect-strategy dispatch.

After the refactor, ``SQLGenerator.__init__`` accepts either a sqlglot
name string OR a ``SqlDialect`` instance. ``self.dialect`` is a read-only
property derived from ``self._dialect.sqlglot_name``.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
import sqlglot

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
    parsed = sqlglot.parse_one("SELECT 1", dialect=gen.dialect)
    assert parsed.sql(dialect=gen.dialect) == "SELECT 1"


# DEV-1571 Bug 1 — the live outer-wrap (``_emit_planned_outer_wrap``) delegates
# to ``SqlDialect.emit_outer_wrap`` at generator.py; the hook itself is pinned
# directly by ``tests/dialects/test_base.py`` / ``test_mysql.py``. The two tests
# that drove the deleted EnrichedQuery-era ``_build_outer_wrap`` (delegation +
# text-based trailing-pagination strip) were removed with it in PR 6 (DEV-1749);
# the planned path carries pagination as detached AST, so there is nothing to
# strip from inner text.


# ---------------------------------------------------------------------------
# DEV-1716 (Codex test-review Med 4/5) — mechanism-level delegation spies.
# The end-to-end SQL-shape pins verify the *output*; these verify the
# generator actually *dispatches through the dialect strategy* so a future
# inline reimplementation that happens to match the output still fails.
# ---------------------------------------------------------------------------


def test_duration_interval_exprs_delegates_to_dialect_hook() -> None:
    """``SQLGenerator._duration_interval_exprs`` must dispatch through
    ``self._dialect.duration_interval_exprs`` — never an inline
    ``if self.dialect == 'sqlite':`` branch."""
    gen = SQLGenerator(dialect="postgres")
    sentinel = ["<<intervals>>"]
    with patch.object(
        type(gen._dialect),
        "duration_interval_exprs",
        autospec=True,
        return_value=sentinel,
    ) as spy:
        out = gen._duration_interval_exprs("90d", sign=-1)
    assert spy.called, (
        "_duration_interval_exprs must dispatch through "
        "self._dialect.duration_interval_exprs. DEV-1716 §3c."
    )
    assert out is sentinel, "Delegate must return the hook's output verbatim."


def test_add_intervals_expr_delegates_to_dialect_hook() -> None:
    """``SQLGenerator._add_intervals_expr`` must dispatch through
    ``self._dialect.add_intervals_expr`` (T-SQL overrides it to emit
    ``DATEADD`` instead of ``± INTERVAL``)."""
    gen = SQLGenerator(dialect="postgres")
    base = sqlglot.parse_one("created_at", dialect="postgres")
    with patch.object(
        type(gen._dialect),
        "add_intervals_expr",
        autospec=True,
        return_value="<<added>>",
    ) as spy:
        out = gen._add_intervals_expr(base, [], sign=1)
    assert spy.called, (
        "_add_intervals_expr must dispatch through "
        "self._dialect.add_intervals_expr. DEV-1716 §3c."
    )
    assert out == "<<added>>", "Delegate must return the hook's output verbatim."


def test_parse_delegates_rewrite_parsed_ast_to_active_dialect() -> None:
    """``SQLGenerator._parse`` must run the PARSE-dialect's
    ``rewrite_parsed_ast`` hook (SQLite's JSONExtract->func-form rewrite),
    not an inline ``if d == 'sqlite':`` branch. Pins the mechanism behind
    the JSONExtract output-shape tests in test_generator_delegation.py."""
    from slayer.sql.dialects.sqlite import SqliteDialect

    gen = SQLGenerator(dialect="sqlite")
    with patch.object(
        SqliteDialect,
        "rewrite_parsed_ast",
        autospec=True,
        side_effect=lambda self, tree: tree,
    ) as spy:
        gen._parse("json_extract(payload, '$.tier')")
    assert spy.called, (
        "_parse must dispatch through the active dialect's rewrite_parsed_ast "
        "(SQLite JSONExtract rewrite). DEV-1716 §3b."
    )


def test_parse_predicate_delegates_rewrite_parsed_ast_to_active_dialect() -> None:
    """Same contract for the bare-predicate parser ``_parse_predicate``."""
    from slayer.sql.dialects.sqlite import SqliteDialect

    gen = SQLGenerator(dialect="sqlite")
    with patch.object(
        SqliteDialect,
        "rewrite_parsed_ast",
        autospec=True,
        side_effect=lambda self, tree: tree,
    ) as spy:
        gen._parse_predicate("json_extract(payload, '$.tier') = 'gold'")
    assert spy.called, (
        "_parse_predicate must dispatch through the active dialect's "
        "rewrite_parsed_ast. DEV-1716 §3b."
    )
