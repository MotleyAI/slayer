"""DEV-1817: byte-exact characterization locks for the dialect dedup refactor.

Pins the exact emitted SQL of ``build_approx_count_distinct`` per dialect
(finding 4a: config attributes must not shift casing / function name / the
Oracle+T-SQL ``exp.Anonymous`` shape) and the alias-mangling contract shared
by BigQuery and T-SQL (finding 4b: mixin move must preserve fit/emit/mangle).
Stricter than the pre-existing substring assertions in ``test_tsql.py`` /
``test_bigquery.py`` / ``test_count_distinct_approx.py``.
"""

from __future__ import annotations

import sqlglot
import pytest
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
from slayer.sql.naming import decode_alias


def _parse(dialect: str):
    return lambda sql: sqlglot.parse_one(sql, dialect=dialect)


_APPROX_EXACT = {
    "postgres": (PostgresDialect(), "COUNT(DISTINCT customer_id)"),
    "sqlite": (SqliteDialect(), "COUNT(DISTINCT customer_id)"),
    "mysql": (MysqlDialect(), "COUNT(DISTINCT customer_id)"),
    "duckdb": (DuckdbDialect(), "APPROX_COUNT_DISTINCT(customer_id)"),
    "clickhouse": (ClickhouseDialect(), "uniq(customer_id)"),
    "bigquery": (BigqueryDialect(), "APPROX_COUNT_DISTINCT(customer_id)"),
    "snowflake": (SnowflakeDialect(), "APPROX_COUNT_DISTINCT(customer_id)"),
    "tsql": (TsqlDialect(), "APPROX_COUNT_DISTINCT(customer_id)"),
    "redshift": (RedshiftDialect(), "APPROXIMATE COUNT(DISTINCT customer_id)"),
    "trino": (TrinoDialect(), "APPROX_DISTINCT(customer_id)"),
    "presto": (PrestoDialect(), "APPROX_DISTINCT(customer_id)"),
    "databricks": (DatabricksDialect(), "APPROX_COUNT_DISTINCT(customer_id)"),
    "spark": (SparkDialect(), "APPROX_COUNT_DISTINCT(customer_id)"),
    "oracle": (OracleDialect(), "APPROX_COUNT_DISTINCT(customer_id)"),
}


@pytest.mark.parametrize("dialect", sorted(_APPROX_EXACT))
def test_approx_count_distinct_emits_exact_sql(dialect: str) -> None:
    d, expected = _APPROX_EXACT[dialect]
    out = d.build_approx_count_distinct("customer_id", parse=_parse(dialect))
    assert out.sql(dialect=dialect) == expected


@pytest.mark.parametrize("dialect", ["tsql", "oracle"])
def test_approx_count_distinct_anonymous_survives_reemission(dialect: str) -> None:
    # T-SQL / Oracle build an exp.Anonymous so sqlglot does not re-emit
    # APPROX_COUNT_DISTINCT as its Presto-family APPROX_DISTINCT canonical.
    d, _ = _APPROX_EXACT[dialect]
    out = d.build_approx_count_distinct("customer_id", parse=_parse(dialect))
    assert isinstance(out, exp.Anonymous)
    assert out.name == "APPROX_COUNT_DISTINCT"


_ALIAS_DIALECTS = {"tsql": TsqlDialect(), "bigquery": BigqueryDialect()}


@pytest.mark.parametrize("name", sorted(_ALIAS_DIALECTS))
def test_fit_alias_under_budget_is_identity(name: str) -> None:
    assert _ALIAS_DIALECTS[name].fit_alias("orders.id") == "orders.id"


@pytest.mark.parametrize("name", sorted(_ALIAS_DIALECTS))
def test_emit_alias_mangles_dots(name: str) -> None:
    assert _ALIAS_DIALECTS[name].emit_alias("orders.id") == "orders___id"


@pytest.mark.parametrize("name", sorted(_ALIAS_DIALECTS))
def test_emit_alias_over_budget_fits_and_mangles(name: str) -> None:
    d = _ALIAS_DIALECTS[name]
    budget = d.max_identifier_bytes
    over = "orders." + ("x" * (budget + 50)) + ".leaf"
    emitted = d.emit_alias(over)
    assert "." not in emitted  # dots mangled to ___
    assert len(emitted.encode()) <= budget  # length-fitted within budget


def test_tsql_and_bigquery_emit_alias_agree_on_short_names() -> None:
    # The fit/emit/decode logic is shared (finding 4b mixin) — identical on
    # any under-budget name; only rewrite_emitted_sql's quote anchor differs.
    a = TsqlDialect().emit_alias("orders.products.category")
    b = BigqueryDialect().emit_alias("orders.products.category")
    assert a == b == "orders___products___category"
    assert decode_alias(a) == "orders.products.category"
