"""``PERCENTILE_CONT … WITHIN GROUP`` on MySQL and SQL Server: a row-level window SELECT under a regrouping one."""

from __future__ import annotations

from decimal import Decimal

import duckdb
import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.sql.dialects.bigquery import BigqueryDialect
from slayer.sql.dialects.mysql import MysqlDialect
from slayer.sql.dialects.trino import TrinoDialect
from slayer.sql.dialects.tsql import TsqlDialect

ROWS = "(VALUES ('a', 10.0), ('a', 20.0), ('a', 30.0), ('a', 40.0), ('b', 5.0), ('b', NULL), ('b', 7.0), " \
       "('c', NULL), ('d', 9.0)) AS t(k, x)"


def _percentile_query(p: str, *, grouped: bool, where: str = "") -> str:
    keys = "t.k, " if grouped else ""
    group = " GROUP BY t.k" if grouped else ""
    return (f"SELECT {keys}PERCENTILE_CONT({p}) WITHIN GROUP (ORDER BY t.x) AS v, COUNT(*) AS n "
            f"FROM {ROWS}{where}{group}")


def _parsed(sql: str) -> Expression:
    tree = sqlglot.parse_one(sql, dialect="duckdb")
    assert isinstance(tree, Expression)
    return tree


def _rows(sql: str) -> set[tuple]:
    return {tuple(float(c) if isinstance(c, Decimal) else c for c in row) for row in duckdb.sql(sql).fetchall()}


@pytest.mark.parametrize("p", ["0", "0.25", "0.5", "0.9", "1"])
@pytest.mark.parametrize("grouped", [True, False], ids=["grouped", "scalar"])
@pytest.mark.parametrize("where", ["", " WHERE t.k = 'none'"], ids=["rows", "no rows"])
def test_mysql_interpolation_equals_percentile_cont(p: str, grouped: bool, where: str) -> None:
    """Every group, NULLs and a NULL-only group included, as a native ``PERCENTILE_CONT``."""
    query = _percentile_query(p, grouped=grouped, where=where)
    split = MysqlDialect().rewrite_statement(_parsed(query))
    assert not any(isinstance(n, exp.WithinGroup) for n in split.walk())
    assert _rows(split.sql(dialect="duckdb")) == _rows(query)


@pytest.mark.parametrize("dialect", [MysqlDialect(), TrinoDialect()], ids=["mysql", "trino"])
@pytest.mark.parametrize("p", ["0.25", "0.9"])
def test_a_descending_order_keeps_its_direction(dialect, p: str) -> None:
    query = _percentile_query(p, grouped=True).replace("ORDER BY t.x)", "ORDER BY t.x DESC)")
    split = dialect.rewrite_statement(_parsed(query))
    assert _rows(split.sql(dialect="duckdb")) == _rows(query)


@pytest.mark.parametrize("dialect", [MysqlDialect(), TrinoDialect(), BigqueryDialect()], ids=["mysql", "trino", "bigquery"])
def test_a_bare_column_projection_keeps_its_output_name(dialect) -> None:
    query = _percentile_query("0.5", grouped=True)
    split = dialect.rewrite_statement(_parsed(query))
    assert split.named_selects == ["k", "v", "n"]


@pytest.mark.parametrize("dialect", [MysqlDialect(), TrinoDialect()], ids=["mysql", "trino"])
@pytest.mark.parametrize("projection", ["k", "k AS k"], ids=["bare", "self-aliased"])
def test_an_unqualified_grouping_column_is_read_from_the_rows(dialect, projection: str) -> None:
    query = _percentile_query("0.5", grouped=True).replace("SELECT t.k", f"SELECT {projection}").replace(
        "GROUP BY t.k", "GROUP BY k")
    split = dialect.rewrite_statement(_parsed(query))
    assert split.named_selects == ["k", "v", "n"]
    assert _rows(split.sql(dialect="duckdb")) == _rows(query)


def test_sql_server_reads_a_descending_percentile_as_its_literal_complement() -> None:
    query = _percentile_query("0.25", grouped=True).replace("ORDER BY t.x)", "ORDER BY t.x DESC)")
    sql = TsqlDialect().rewrite_statement(_parsed(query)).sql(dialect="tsql")
    assert "PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY t.x) OVER (PARTITION BY t.k)" in sql


def test_bigquery_reads_a_descending_percentile_as_its_literal_complement() -> None:
    query = _percentile_query("0.1", grouped=True).replace("ORDER BY t.x)", "ORDER BY t.x DESC)")
    sql = BigqueryDialect().rewrite_statement(_parsed(query)).sql(dialect="bigquery")
    assert "PERCENTILE_CONT(t.x, 0.9) OVER (PARTITION BY t.k)" in sql


def test_sql_server_reads_a_windowed_percentile_per_group() -> None:
    query = _percentile_query("0.25", grouped=True)
    sql = TsqlDialect().rewrite_statement(_parsed(query)).sql(dialect="tsql")
    assert "PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY t.x) OVER (PARTITION BY t.k)" in sql
    assert "MAX(__slayer_os.__slayer_o" in sql
    assert sql.rstrip().endswith("GROUP BY __slayer_os.__slayer_o0")


def test_bigquery_reads_an_analytic_percentile_per_group() -> None:
    query = _percentile_query("0.25", grouped=True)
    sql = BigqueryDialect().rewrite_statement(_parsed(query)).sql(dialect="bigquery")
    assert "PERCENTILE_CONT(t.x, 0.25) OVER (PARTITION BY t.k)" in sql
    assert "WITHIN GROUP" not in sql


def test_a_select_without_a_percentile_is_unchanged() -> None:
    query = "SELECT t.k, SUM(t.x) FROM t GROUP BY t.k"
    assert MysqlDialect().rewrite_statement(_parsed(query)).sql() == sqlglot.parse_one(query).sql()


def test_trino_interpolates_exactly() -> None:
    """sqlglot would emit the approximate ``APPROX_PERCENTILE``."""
    query = _percentile_query("0.25", grouped=True)
    split = TrinoDialect().rewrite_statement(_parsed(query))
    assert "APPROX_PERCENTILE" not in split.sql(dialect="trino")
    assert _rows(split.sql(dialect="duckdb")) == _rows(query)


@pytest.mark.parametrize("group", ["1", "kk"], ids=["ordinal", "alias"])
def test_a_grouping_ordinal_or_alias_partitions_by_its_projection(group: str) -> None:
    query = (f"SELECT t.k AS kk, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.x) AS v, COUNT(*) AS n "
             f"FROM {ROWS} GROUP BY {group}")
    split = MysqlDialect().rewrite_statement(_parsed(query))
    assert _rows(split.sql(dialect="duckdb")) == _rows(query)


def test_a_star_projection_beside_a_percentile_is_refused() -> None:
    query = "SELECT *, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.x) AS v FROM t GROUP BY t.k, t.x"
    statement, dialect = _parsed(query), MysqlDialect()
    with pytest.raises(ValueError, match="cannot be split"):
        dialect.rewrite_statement(statement)
