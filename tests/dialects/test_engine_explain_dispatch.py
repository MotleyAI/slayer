"""DEV-1716: the engine's EXPLAIN emission must route through the DEV-1542
dialect strategy package.

``SlayerQueryEngine`` builds the dialect-appropriate ``EXPLAIN`` statement via
the module-level ``_build_explain_sql`` helper. Historically that helper carried
its own inline ``_EXPLAIN_PREFIX`` / ``_EXPLAIN_POSTFIX`` maps — inline
dialect logic that duplicated ``SqlDialect.build_explain_sql``. After the
delegation port it must dispatch through ``get_dialect(dialect).build_explain_sql``
so a future regression that re-introduces a string-keyed map fails here.

These pin the engine-surface delegation (the pure ``SqlDialect.build_explain_sql``
unit tests live in ``tests/dialects/test_base.py`` and the per-dialect files).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from slayer.engine.query_engine import _build_explain_sql
from slayer.sql.dialects import BigqueryDialect, PostgresDialect


def test_build_explain_sql_delegates_to_dialect_hook() -> None:
    """``_build_explain_sql`` must dispatch through the active dialect's
    ``build_explain_sql`` — not a hard-coded prefix/postfix map."""
    with patch.object(
        PostgresDialect,
        "build_explain_sql",
        autospec=True,
        return_value="<<stubbed-explain>>",
    ) as spy:
        out = _build_explain_sql(dialect="postgres", sql="SELECT 1")
    assert spy.called, (
        "_build_explain_sql must dispatch through "
        "get_dialect(dialect).build_explain_sql. DEV-1716 plan §3g."
    )
    assert out == "<<stubbed-explain>>", (
        "Delegate must return the dialect hook's output verbatim."
    )


@pytest.mark.parametrize(
    "dialect,expected",
    [
        ("postgres", "EXPLAIN ANALYZE SELECT 1"),
        ("sqlite", "EXPLAIN QUERY PLAN SELECT 1"),
        ("mysql", "EXPLAIN FORMAT=JSON SELECT 1"),
        ("tsql", "SET SHOWPLAN_ALL ON; SELECT 1; SET SHOWPLAN_ALL OFF"),
    ],
)
def test_build_explain_sql_preserves_legacy_output(dialect: str, expected: str) -> None:
    """Delegation must be byte-for-byte identical to the legacy inline map
    for supported dialects (no behavior change, only routing)."""
    assert _build_explain_sql(dialect=dialect, sql="SELECT 1") == expected


def test_build_explain_sql_unsupported_dialect_raises() -> None:
    """BigQuery has no SQL-level EXPLAIN (``explain_prefix is None``) — the
    hook raises ``ValueError`` with the explain-unsupported message, and the
    engine helper surfaces it unchanged (preserves today's semantics)."""
    with pytest.raises(ValueError, match="EXPLAIN is not supported"):
        _build_explain_sql(dialect="bigquery", sql="SELECT 1")


def test_bigquery_dialect_explain_unsupported_matches_helper() -> None:
    """Cross-check: the dialect class and the engine helper agree that
    BigQuery EXPLAIN is unsupported (same failure mode, single source)."""
    with pytest.raises(ValueError, match="EXPLAIN is not supported"):
        BigqueryDialect().build_explain_sql("SELECT 1")


def test_build_explain_sql_unsupported_dispatches_through_hook() -> None:
    """The unsupported-dialect path must ALSO route through the dialect hook,
    not an inline ``prefix is None`` check. Patch BigqueryDialect.build_explain_sql
    to raise a sentinel and assert THAT error surfaces — an inline
    ``_EXPLAIN_PREFIX['bigquery'] is None`` branch would raise the generic
    ValueError instead and this test would fail."""

    class _Sentinel(RuntimeError):
        pass

    with patch.object(
        BigqueryDialect,
        "build_explain_sql",
        autospec=True,
        side_effect=_Sentinel("routed-through-hook"),
    ):
        with pytest.raises(_Sentinel, match="routed-through-hook"):
            _build_explain_sql(dialect="bigquery", sql="SELECT 1")
