"""Unit tests for slayer.sql.sql_predicate.parse_sql_predicate.

Mode A SQL-mode filter validator (DEV-1369 round 2). Pre-rejects DSL
constructs (aggregation colon syntax, transform calls, raw OVER) and
extracts column-shaped identifier tokens. Does not invoke sqlglot —
dialect-aware parsing happens at SQL generation time.
"""
from __future__ import annotations

import pytest

from slayer.sql.sql_predicate import parse_sql_predicate


class TestSqlPredicateAccepts:
    """SQL-mode filters with arbitrary SQL function calls and operators."""

    def test_arbitrary_function_call_passes(self) -> None:
        pf = parse_sql_predicate("json_extract(metadata, '$.active') = 1")
        assert pf.sql == "json_extract(metadata, '$.active') = 1"
        assert "metadata" in pf.columns

    def test_coalesce_function_passes(self) -> None:
        pf = parse_sql_predicate("coalesce(status, 'unknown') = 'active'")
        assert pf.sql == "coalesce(status, 'unknown') = 'active'"
        assert "status" in pf.columns

    def test_case_when_passes(self) -> None:
        pf = parse_sql_predicate("CASE WHEN status = 'active' THEN 1 ELSE 0 END = 1")
        assert "CASE WHEN" in pf.sql
        assert "status" in pf.columns

    def test_dialect_specific_operator_passes(self) -> None:
        # Postgres jsonb contains operator. parse_sql_predicate does NOT parse
        # SQL — it just rejects DSL constructs and extracts column tokens.
        pf = parse_sql_predicate("payload @> '{\"k\": 1}'")
        assert "@>" in pf.sql
        assert "payload" in pf.columns

    def test_lower_function_passes(self) -> None:
        pf = parse_sql_predicate("lower(status) = 'active'")
        assert pf.sql == "lower(status) = 'active'"
        assert "status" in pf.columns


class TestSqlPredicateColumnExtraction:
    """``ParsedFilter.columns`` regex-extracts column-shaped tokens."""

    def test_single_dot_column_kept_intact(self) -> None:
        pf = parse_sql_predicate("orders.status = 'active'")
        assert "orders.status" in pf.columns

    def test_double_underscore_join_path_kept_intact(self) -> None:
        # `__`-delimited join aliases are valid Mode A syntax; downstream
        # join detection (enrichment._scan_filter_column_ref) splits on
        # the dunder.
        pf = parse_sql_predicate("customers__regions.name = 'EU'")
        assert "customers__regions.name" in pf.columns

    def test_function_head_excluded_from_columns(self) -> None:
        # ``json_extract`` is the function being called, not a column.
        pf = parse_sql_predicate("json_extract(metadata, '$.x') = 1")
        assert "json_extract" not in pf.columns
        assert "metadata" in pf.columns

    def test_string_literals_stripped_before_extraction(self) -> None:
        # ``status`` would also be a column-shaped token if literal stripping
        # didn't happen, since `'status'` looks like it.
        pf = parse_sql_predicate("note = 'status active'")
        # ``note`` is a real column ref; ``status`` and ``active`` are inside
        # the literal and must NOT appear as columns.
        assert "note" in pf.columns
        assert "status" not in pf.columns
        assert "active" not in pf.columns

    def test_sql_keywords_excluded_from_columns(self) -> None:
        # ``AND`` / ``OR`` / ``IS`` / ``NULL`` etc. are SQL keywords, not
        # column refs; ``_bare_column_refs`` filters them out.
        pf = parse_sql_predicate("status IS NOT NULL AND amount > 0")
        assert "status" in pf.columns
        assert "amount" in pf.columns
        for kw in ("AND", "OR", "NOT", "IS", "NULL"):
            assert kw not in pf.columns


class TestSqlPredicateRejects:
    """DSL constructs are pre-rejected at construction time."""

    def test_aggregation_colon_syntax_rejected(self) -> None:
        with pytest.raises(ValueError, match="aggregation colon syntax"):
            parse_sql_predicate("revenue:sum > 100")

    def test_star_count_colon_rejected(self) -> None:
        with pytest.raises(ValueError, match="aggregation colon syntax"):
            parse_sql_predicate("*:count > 10")

    def test_slayer_transform_call_rejected(self) -> None:
        with pytest.raises(ValueError, match="transform call"):
            parse_sql_predicate("cumsum(amount) > 0")

    def test_rank_transform_rejected(self) -> None:
        with pytest.raises(ValueError, match="transform call"):
            parse_sql_predicate("rank(revenue) <= 10")

    def test_raw_over_window_rejected(self) -> None:
        with pytest.raises(ValueError, match="window function"):
            parse_sql_predicate("row_number() OVER (PARTITION BY x ORDER BY y) = 1")
