"""Stage 7a.4 (DEV-1450) — Mode-A sqlglot wrapper tests.

The public surface in ``slayer.sql.sql_expr``:

- ``canonicalize_sql(text, *, dialect=None) -> str`` — canonicalises Mode-A
  SQL, applying dialect-specific rewrites (json_extract on SQLite, log10 / log2
  preservation), and returns the canonical string.
- ``has_window_function(text) -> bool`` — re-export of
  ``slayer.sql.window_detect.has_window_function``.
- ``assert_no_window_in_filter(text, *, source) -> None`` — raises
  ``IllegalWindowInFilterError`` if ``text`` contains ``OVER(...)``.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import IllegalWindowInFilterError
from slayer.sql.sql_expr import (
    assert_no_window_in_filter,
    canonicalize_sql,
    has_window_function,
)


# ---------------------------------------------------------------------------
# canonicalize_sql — canonical identity
# ---------------------------------------------------------------------------


class TestCanonicalize:
    def test_returns_string(self):
        text = canonicalize_sql("status = 'paid'")
        assert isinstance(text, str)
        assert text

    def test_whitespace_normalised(self):
        # Two inputs differing only in whitespace canonicalise identically.
        assert canonicalize_sql("status = 'paid'") == canonicalize_sql(
            "status   =    'paid'",
        )

    def test_different_filters_differ(self):
        assert canonicalize_sql("status = 'paid'") != canonicalize_sql(
            "status = 'open'",
        )

    def test_arithmetic_whitespace_equal(self):
        # Whitespace inside arithmetic normalises.
        a = canonicalize_sql("amount + 1")
        b = canonicalize_sql("amount+1")
        c = canonicalize_sql("amount +    1")
        assert a == b == c

    def test_dialect_kw_accepted(self):
        # Dialect kwarg is accepted and influences emission for dialect-
        # specific constructs (json_extract, log10).
        assert isinstance(canonicalize_sql("status = 'paid'", dialect="postgres"), str)

    def test_replace_function_call_on_sqlite_parses_correctly(self):
        # Without the wrap-and-extract guard, sqlglot falls back to a
        # `Command` node and emits `REPLACE (status, ...)` (a MySQL/SQLite
        # statement keyword) instead of the function call. Regression
        # guard for Codex's finding 1 (Stage 7a.4 review).
        canonical = canonicalize_sql(
            "replace(status, ',', '') = 'foo'", dialect="sqlite",
        )
        # REPLACE stays a function call with parens-around-args, not a keyword.
        assert "REPLACE(" in canonical or "replace(" in canonical

    def test_replace_function_call_on_mysql_parses_correctly(self):
        canonical = canonicalize_sql("replace(status, ',', '')", dialect="mysql")
        assert "REPLACE(" in canonical or "replace(" in canonical


# ---------------------------------------------------------------------------
# Dialect-specific rewrites
# ---------------------------------------------------------------------------


class TestSqliteJsonExtractPreservation:
    def test_json_extract_function_form_preserved(self):
        # On SQLite, json_extract(col, '$.path') must NOT be rewritten
        # to col -> '$.path' (the operator returns JSON-quoted form;
        # the function returns the unquoted scalar).
        canonical = canonicalize_sql(
            "json_extract(data, '$.kind') = 'Owned'", dialect="sqlite",
        )
        # The function form survives; the operator form does NOT appear.
        assert "json_extract" in canonical.lower()
        assert "->" not in canonical

    def test_json_extract_other_dialects_unchanged(self):
        # On Postgres, json_extract is not a native function — leave
        # whatever the parser produces alone (no SQLite-specific rewrite).
        assert isinstance(
            canonicalize_sql("json_extract(data, '$.kind') = 'Owned'", dialect="postgres"),
            str,
        )


# Golden canonical output for ``log(<base>, revenue)`` under the single-source
# DEV-1784 log-alias policy. The rewrite to a native single-arg LOG10/LOG2 only
# fires where sqlglot parses the base as a literal in the ``this`` position AND
# the dialect supports the native alias (SqlDialect.should_use_native_log);
# elsewhere the generic 2-arg form is preserved — Presto/ClickHouse reorder to
# ``LOG(revenue, <base>)``, T-SQL/BigQuery/Oracle keep ``LOG(<base>, revenue)``,
# and Snowflake/Redshift have LOG10 but no native LOG2. Pinned so a policy or
# sqlglot change cannot silently drift any dialect's rendering.
_LOG_GOLDEN = {
    ('sqlite', 10): 'LOG10(revenue)',
    ('sqlite', 2): 'LOG2(revenue)',
    ('postgres', 10): 'LOG10(revenue)',
    ('postgres', 2): 'LOG2(revenue)',
    ('duckdb', 10): 'LOG10(revenue)',
    ('duckdb', 2): 'LOG2(revenue)',
    ('mysql', 10): 'LOG10(revenue)',
    ('mysql', 2): 'LOG2(revenue)',
    ('clickhouse', 10): 'LOG(revenue, 10)',
    ('clickhouse', 2): 'LOG(revenue, 2)',
    ('snowflake', 10): 'LOG10(revenue)',
    ('snowflake', 2): 'LOG(2, revenue)',
    ('bigquery', 10): 'LOG(10, revenue)',
    ('bigquery', 2): 'LOG(2, revenue)',
    ('redshift', 10): 'LOG10(revenue)',
    ('redshift', 2): 'LOG(2, revenue)',
    ('trino', 10): 'LOG10(revenue)',
    ('trino', 2): 'LOG2(revenue)',
    ('presto', 10): 'LOG(revenue, 10)',
    ('presto', 2): 'LOG(revenue, 2)',
    ('databricks', 10): 'LOG10(revenue)',
    ('databricks', 2): 'LOG2(revenue)',
    ('spark', 10): 'LOG10(revenue)',
    ('spark', 2): 'LOG2(revenue)',
    ('tsql', 10): 'LOG(10, revenue)',
    ('tsql', 2): 'LOG(2, revenue)',
    ('oracle', 10): 'LOG(10, revenue)',
    ('oracle', 2): 'LOG(2, revenue)',
}


class TestLogAliasPreservation:
    def test_log10_preserved_on_sqlite(self):
        canonical = canonicalize_sql("log10(revenue) > 2", dialect="sqlite")
        assert "log10" in canonical.lower()
        # Not rewritten to the 2-arg form.
        assert "log(10," not in canonical.lower().replace(" ", "")

    def test_log2_preserved_on_postgres(self):
        canonical = canonicalize_sql("log2(revenue) > 1", dialect="postgres")
        assert "log2" in canonical.lower()

    def test_explicit_2arg_log_left_alone(self):
        # `log(3, x)` is not a `log10`/`log2` alias — leave canonical.
        canonical = canonicalize_sql("log(3, revenue) > 0", dialect="postgres")
        # The base 3 must survive.
        assert "3" in canonical

    @pytest.mark.parametrize(("dialect", "base"), sorted(_LOG_GOLDEN))
    def test_log_alias_canonical_per_dialect(self, dialect, base):
        """Pin ``log(<base>, revenue)``'s canonical form for every dialect under
        the single-source log-alias policy (DEV-1784), so a policy or sqlglot
        change that alters any dialect's rendering is caught."""
        canonical = canonicalize_sql(f"log({base}, revenue)", dialect=dialect)
        assert canonical == _LOG_GOLDEN[(dialect, base)]

    def test_log_alias_skipped_without_a_dialect(self):
        # dialect=None has no policy to key on — leave the generic 2-arg form.
        canonical = canonicalize_sql("log(10, revenue) > 0")
        assert "log(10," in canonical.lower().replace(" ", "")

    def test_unregistered_sqlglot_dialect_does_not_raise(self):
        # canonicalize_sql is total over any sqlglot-parseable dialect: a name
        # outside SLayer's registry ("hive") skips the log-alias policy rather
        # than raising, leaving the generic 2-arg form (DEV-1784 — Codex).
        canonical = canonicalize_sql("log(10, revenue)", dialect="hive")
        assert "log(10," in canonical.lower().replace(" ", "")


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
# Failure modes
# ---------------------------------------------------------------------------


class TestFailures:
    def test_empty_input_raises(self):
        with pytest.raises(ValueError):
            canonicalize_sql("")

    def test_syntax_error_raises(self):
        with pytest.raises(ValueError):
            canonicalize_sql("status =")
