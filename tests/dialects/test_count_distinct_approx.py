"""DEV-1595: dialect-aware ``count_distinct_approx`` aggregation.

Each dialect emits the database-native approximate-distinct function where the
backend supports one, and falls back to an exact ``COUNT(DISTINCT col)`` where
it does not (Postgres / SQLite / MySQL). The fallback is exact — more accurate,
never approximate — so it is consistent with the "no approximate SQL" rule.

The extension point mirrors the existing ``build_median`` / ``build_percentile``
per-dialect hooks: a base ``build_approx_count_distinct`` on ``SqlDialect`` that
the native-supporting dialects override.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from slayer.sql.dialects._tier2 import (
    DatabricksDialect,
    OracleDialect,
    PrestoDialect,
    RedshiftDialect,
    SparkDialect,
    TrinoDialect,
)
from slayer.sql.dialects.bigquery import BigqueryDialect
from slayer.sql.dialects.clickhouse import ClickhouseDialect
from slayer.sql.dialects.duckdb import DuckdbDialect
from slayer.sql.dialects.mysql import MysqlDialect
from slayer.sql.dialects.postgres import PostgresDialect
from slayer.sql.dialects.snowflake import SnowflakeDialect
from slayer.sql.dialects.sqlite import SqliteDialect
from slayer.sql.dialects.tsql import TsqlDialect


def _parse(dialect: str):
    def _inner(sql: str) -> exp.Expression:
        return sqlglot.parse_one(sql, dialect=dialect)

    return _inner


# ---------------------------------------------------------------------------
# Exact fallback — backends with no native approximate-distinct
# ---------------------------------------------------------------------------


def test_postgres_falls_back_to_exact_count_distinct() -> None:
    out = PostgresDialect().build_approx_count_distinct("customer_id", parse=_parse("postgres"))
    sql = out.sql(dialect="postgres").upper()
    assert "COUNT(DISTINCT" in sql
    assert "customer_id".upper() in sql
    assert "APPROX" not in sql


def test_sqlite_falls_back_to_exact_count_distinct() -> None:
    out = SqliteDialect().build_approx_count_distinct("customer_id", parse=_parse("sqlite"))
    sql = out.sql(dialect="sqlite").upper()
    assert "COUNT(DISTINCT" in sql
    assert "APPROX" not in sql


def test_mysql_falls_back_to_exact_count_distinct() -> None:
    out = MysqlDialect().build_approx_count_distinct("customer_id", parse=_parse("mysql"))
    sql = out.sql(dialect="mysql").upper()
    assert "COUNT(DISTINCT" in sql
    assert "APPROX" not in sql


# ---------------------------------------------------------------------------
# Native approximate-distinct
# ---------------------------------------------------------------------------


def test_duckdb_native_approx_count_distinct() -> None:
    out = DuckdbDialect().build_approx_count_distinct("customer_id", parse=_parse("duckdb"))
    sql = out.sql(dialect="duckdb").lower()
    assert "approx_count_distinct(" in sql
    assert "customer_id" in sql


def test_clickhouse_native_uniq() -> None:
    out = ClickhouseDialect().build_approx_count_distinct("customer_id", parse=_parse("clickhouse"))
    sql = out.sql(dialect="clickhouse").lower()
    assert "uniq(" in sql
    assert "customer_id" in sql


def test_bigquery_native_approx_count_distinct() -> None:
    out = BigqueryDialect().build_approx_count_distinct("customer_id", parse=_parse("bigquery"))
    sql = out.sql(dialect="bigquery").upper()
    assert "APPROX_COUNT_DISTINCT(" in sql


def test_snowflake_native_approx_count_distinct() -> None:
    out = SnowflakeDialect().build_approx_count_distinct("customer_id", parse=_parse("snowflake"))
    sql = out.sql(dialect="snowflake").upper()
    assert "APPROX_COUNT_DISTINCT(" in sql


def test_tsql_native_approx_count_distinct() -> None:
    out = TsqlDialect().build_approx_count_distinct("customer_id", parse=_parse("tsql"))
    sql = out.sql(dialect="tsql").upper()
    assert "APPROX_COUNT_DISTINCT(" in sql


def test_redshift_native_approximate_count_distinct() -> None:
    out = RedshiftDialect().build_approx_count_distinct("customer_id", parse=_parse("redshift"))
    sql = out.sql(dialect="redshift").upper()
    # Redshift spelling: APPROXIMATE COUNT(DISTINCT col)
    assert "APPROXIMATE" in sql
    assert "COUNT(DISTINCT" in sql


def test_trino_native_approx_distinct() -> None:
    out = TrinoDialect().build_approx_count_distinct("customer_id", parse=_parse("trino"))
    sql = out.sql(dialect="trino").lower()
    assert "approx_distinct(" in sql


def test_presto_native_approx_distinct() -> None:
    out = PrestoDialect().build_approx_count_distinct("customer_id", parse=_parse("presto"))
    sql = out.sql(dialect="presto").lower()
    assert "approx_distinct(" in sql


def test_databricks_native_approx_count_distinct() -> None:
    out = DatabricksDialect().build_approx_count_distinct("customer_id", parse=_parse("databricks"))
    sql = out.sql(dialect="databricks").lower()
    assert "approx_count_distinct(" in sql


def test_spark_native_approx_count_distinct() -> None:
    out = SparkDialect().build_approx_count_distinct("customer_id", parse=_parse("spark"))
    sql = out.sql(dialect="spark").lower()
    assert "approx_count_distinct(" in sql


def test_oracle_native_approx_count_distinct() -> None:
    out = OracleDialect().build_approx_count_distinct("customer_id", parse=_parse("oracle"))
    sql = out.sql(dialect="oracle").upper()
    assert "APPROX_COUNT_DISTINCT(" in sql
