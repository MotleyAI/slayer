"""Tier-2 dialects (Redshift, Presto, Databricks, Spark, Oracle): scalar config only.

Promoted Tier-1 dialects have their own test files (``test_bigquery.py``,
``test_snowflake.py``, ``test_trino.py``).
"""

from __future__ import annotations

import pytest

from slayer.sql.dialects._tier2 import (
    DatabricksDialect,
    OracleDialect,
    PrestoDialect,
    RedshiftDialect,
    SparkDialect,
)


# ---------------------------------------------------------------------------
# Redshift
# ---------------------------------------------------------------------------


def test_redshift_explain_prefix() -> None:
    assert RedshiftDialect().explain_prefix == "EXPLAIN"


def test_redshift_log_native_flags() -> None:
    """Redshift has LOG10 but no LOG2 — preserves today's
    ``_LOG2_NATIVE_DIALECTS`` exclusion."""
    d = RedshiftDialect()
    assert d.should_use_native_log(10) is True
    assert d.should_use_native_log(2) is False


# ---------------------------------------------------------------------------
# Presto (Athena)
# ---------------------------------------------------------------------------


def test_presto_explain_prefix() -> None:
    assert PrestoDialect().explain_prefix == "EXPLAIN ANALYZE"


def test_presto_log_native_flags() -> None:
    d = PrestoDialect()
    assert d.should_use_native_log(10) is True
    assert d.should_use_native_log(2) is True


def test_presto_aliases_include_athena() -> None:
    """Athena uses Presto dialect — preserves the legacy alias."""
    assert "athena" in PrestoDialect().ds_type_aliases


# ---------------------------------------------------------------------------
# Databricks / Spark
# ---------------------------------------------------------------------------


def test_databricks_explain_prefix() -> None:
    assert DatabricksDialect().explain_prefix == "EXPLAIN EXTENDED"


def test_spark_explain_prefix() -> None:
    assert SparkDialect().explain_prefix == "EXPLAIN EXTENDED"


def test_databricks_log_native_flags() -> None:
    d = DatabricksDialect()
    assert d.should_use_native_log(10) is True
    assert d.should_use_native_log(2) is True


def test_spark_log_native_flags() -> None:
    d = SparkDialect()
    assert d.should_use_native_log(10) is True
    assert d.should_use_native_log(2) is True


# ---------------------------------------------------------------------------
# Oracle — log10 + log2 both unsupported
# ---------------------------------------------------------------------------


def test_oracle_explain_prefix() -> None:
    assert OracleDialect().explain_prefix == "EXPLAIN PLAN FOR"


def test_oracle_log_native_flags() -> None:
    """Oracle has neither LOG10 nor LOG2 — preserves today's exclusion
    from both ``_LOG10_NATIVE_DIALECTS`` and ``_LOG2_NATIVE_DIALECTS``."""
    d = OracleDialect()
    assert d.should_use_native_log(10) is False
    assert d.should_use_native_log(2) is False


# ---------------------------------------------------------------------------
# Tier-2 explain SQL is just prefix + sql + postfix (where postfix is "")
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "dialect_cls,expected",
    [
        (RedshiftDialect, "EXPLAIN SELECT 1"),
        (PrestoDialect, "EXPLAIN ANALYZE SELECT 1"),
        (DatabricksDialect, "EXPLAIN EXTENDED SELECT 1"),
        (SparkDialect, "EXPLAIN EXTENDED SELECT 1"),
        (OracleDialect, "EXPLAIN PLAN FOR SELECT 1"),
    ],
)
def test_tier2_build_explain_sql(dialect_cls, expected: str) -> None:
    assert dialect_cls().build_explain_sql("SELECT 1") == expected
