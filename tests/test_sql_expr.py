"""Stage 7a.4 (DEV-1450) — Mode-A sqlglot wrapper tests.

The public surface in ``slayer.sql.sql_expr``:

- ``parse_sql_expr(text, *, dialect=None) -> SqlExprKey`` — parses
  Mode-A SQL, applies dialect-specific rewrites (json_extract on
  SQLite, log10 / log2 preservation), and returns a structural-identity
  key.
- ``canonicalize_sql(text, *, dialect=None) -> str`` — the canonical
  sqlglot form (used by tests and debug).
- ``has_window_function(text) -> bool`` — re-export of
  ``slayer.sql.window_detect.has_window_function``.
- ``assert_no_window_in_filter(text, *, source) -> None`` — raises
  ``IllegalWindowInFilterError`` if ``text`` contains ``OVER(...)``.

The wrapper is dormant in 7a — the binder (7a.5) is the first consumer
(``AggregateKey.column_filter_key`` is the headline use case).
"""

from __future__ import annotations

import pytest

from slayer.core.errors import IllegalWindowInFilterError
from slayer.core.keys import SqlExprKey
from slayer.sql.sql_expr import (
    assert_no_window_in_filter,
    canonicalize_sql,
    has_window_function,
    parse_sql_expr,
)


# ---------------------------------------------------------------------------
# parse_sql_expr — structural identity
# ---------------------------------------------------------------------------


class TestParseSqlExpr:
    def test_returns_sql_expr_key(self):
        key = parse_sql_expr("status = 'paid'")
        assert isinstance(key, SqlExprKey)
        assert key.canonical_sql

    def test_whitespace_normalised(self):
        # Two inputs differing only in whitespace produce the same key.
        a = parse_sql_expr("status = 'paid'")
        b = parse_sql_expr("status   =    'paid'")
        assert a == b
        assert hash(a) == hash(b)

    def test_different_filters_different_keys(self):
        a = parse_sql_expr("status = 'paid'")
        b = parse_sql_expr("status = 'open'")
        assert a != b

    def test_arithmetic_whitespace_equal(self):
        # Whitespace inside arithmetic normalises — keys equal.
        a = parse_sql_expr("amount + 1")
        b = parse_sql_expr("amount+1")
        c = parse_sql_expr("amount +    1")
        assert a == b == c

    def test_dialect_kw_accepted(self):
        # Dialect kwarg is accepted and influences emission for dialect-
        # specific constructs (json_extract, log10).
        key = parse_sql_expr("status = 'paid'", dialect="postgres")
        assert isinstance(key, SqlExprKey)

    def test_replace_function_call_on_sqlite_parses_correctly(self):
        # Without the wrap-and-extract guard, sqlglot falls back to a
        # `Command` node and emits `REPLACE (status, ...)` (a MySQL/SQLite
        # statement keyword) instead of the function call. Regression
        # guard for Codex's finding 1 (Stage 7a.4 review).
        key = parse_sql_expr(
            "replace(status, ',', '') = 'foo'", dialect="sqlite",
        )
        # The canonical form must have REPLACE as a function call with
        # parens-around-args, not as a statement keyword.
        assert "REPLACE(" in key.canonical_sql or "replace(" in key.canonical_sql

    def test_replace_function_call_on_mysql_parses_correctly(self):
        key = parse_sql_expr(
            "replace(status, ',', '')", dialect="mysql",
        )
        assert "REPLACE(" in key.canonical_sql or "replace(" in key.canonical_sql


# ---------------------------------------------------------------------------
# Dialect-specific rewrites
# ---------------------------------------------------------------------------


class TestSqliteJsonExtractPreservation:
    def test_json_extract_function_form_preserved(self):
        # On SQLite, json_extract(col, '$.path') must NOT be rewritten
        # to col -> '$.path' (the operator returns JSON-quoted form;
        # the function returns the unquoted scalar).
        key = parse_sql_expr(
            "json_extract(data, '$.kind') = 'Owned'", dialect="sqlite",
        )
        # The function form survives.
        assert "json_extract" in key.canonical_sql.lower()
        # The operator form does NOT appear.
        assert "->" not in key.canonical_sql

    def test_json_extract_other_dialects_unchanged(self):
        # On Postgres, json_extract is not a native function — leave
        # whatever the parser produces alone (no SQLite-specific rewrite).
        key = parse_sql_expr(
            "json_extract(data, '$.kind') = 'Owned'", dialect="postgres",
        )
        assert isinstance(key, SqlExprKey)


class TestLogAliasPreservation:
    def test_log10_preserved_on_sqlite(self):
        key = parse_sql_expr("log10(revenue) > 2", dialect="sqlite")
        assert "log10" in key.canonical_sql.lower()
        # Not rewritten to the 2-arg form.
        assert "log(10," not in key.canonical_sql.lower().replace(" ", "")

    def test_log2_preserved_on_postgres(self):
        key = parse_sql_expr("log2(revenue) > 1", dialect="postgres")
        assert "log2" in key.canonical_sql.lower()

    def test_explicit_2arg_log_left_alone(self):
        # `log(3, x)` is not a `log10`/`log2` alias — leave canonical.
        key = parse_sql_expr("log(3, revenue) > 0", dialect="postgres")
        # The base 3 must survive.
        assert "3" in key.canonical_sql


# ---------------------------------------------------------------------------
# Window detection
# ---------------------------------------------------------------------------


class TestWindowDetection:
    def test_detects_over_clause(self):
        assert has_window_function("SUM(x) OVER (PARTITION BY y)")

    def test_detects_case_insensitive(self):
        assert has_window_function("count(*) over ()")

    def test_no_window_returns_false(self):
        assert not has_window_function("status = 'paid'")
        assert not has_window_function("amount + 1")

    def test_empty_string_returns_false(self):
        assert not has_window_function("")

    def test_assert_raises_on_window(self):
        with pytest.raises(IllegalWindowInFilterError) as exc:
            assert_no_window_in_filter(
                "SUM(x) OVER (PARTITION BY y)",
                source="Column.filter on orders.foo",
            )
        # The supplied source is part of the public diagnostic surface.
        assert "Column.filter on orders.foo" in str(exc.value)

    def test_assert_passes_on_plain_filter(self):
        # No exception — function returns None.
        assert assert_no_window_in_filter(
            "status = 'paid'", source="Column.filter on orders.foo",
        ) is None

    def test_assert_passes_on_empty(self):
        assert assert_no_window_in_filter("", source="(test)") is None


# ---------------------------------------------------------------------------
# canonicalize_sql — debug helper
# ---------------------------------------------------------------------------


class TestCanonicalizeSql:
    def test_canonicalize_returns_string(self):
        text = canonicalize_sql("status = 'paid'")
        assert isinstance(text, str)
        assert "status" in text.lower()

    def test_canonicalize_matches_parse_canonical(self):
        text = canonicalize_sql("status = 'paid'", dialect="postgres")
        key = parse_sql_expr("status = 'paid'", dialect="postgres")
        assert text == key.canonical_sql


# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------


class TestFailures:
    def test_empty_input_raises(self):
        with pytest.raises(ValueError):
            parse_sql_expr("")

    def test_syntax_error_raises(self):
        with pytest.raises(ValueError):
            parse_sql_expr("status =")
