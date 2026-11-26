"""Emission of the DISTINCT aggregates and the discrete / approximate percentiles on every Tier-1 dialect.

Spec: aggregations/distinct-and-percentiles; design.md › Emission.
"""

from __future__ import annotations

import re

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.sql.dialects import dialect_for_ds_type
from tests._engine_helpers import _engine_generate

TIER1 = ["sqlite", "duckdb", "postgres", "mysql", "clickhouse", "tsql", "trino", "snowflake", "bigquery"]

ORDERS = SlayerDataset(
    name="orders", sql_table="orders", data_source="test",
    fields=[
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="amount", datatype=DataType.FLOAT),
        DatasetField(name="qty", datatype=DataType.INTEGER),
        DatasetField(name="price", datatype=DataType.DECIMAL),
        DatasetField(name="status", datatype=DataType.STRING),
        DatasetField(name="order_date", datatype=DataType.DATE),
        DatasetField(name="created_at", datatype=DataType.DATETIME),
        DatasetField(name="is_paid", datatype=DataType.BOOLEAN),
    ],
)


async def _sql(expression: str, *, dialect: str, dimensions: tuple[str, ...] = ()) -> str:
    """Whitespace-normalised SQL of one metric over ``ORDERS``."""
    query = SlayerQuery.model_validate(
        {"population": "orders", "metrics": [{"metric": expression}], "dimensions": list(dimensions)})
    sql = await _engine_generate(query=query, dataset=ORDERS, dialect=dialect)
    return " ".join(sql.split())


def _sqlglot(dialect: str) -> str:
    return dialect_for_ds_type(dialect).sqlglot_name


def _outer_cast(sql: str, *, dialect: str) -> str | None:
    """The type the outermost projection is cast to, if it is a cast."""
    tree = sqlglot.parse_one(sql, dialect=_sqlglot(dialect))
    assert isinstance(tree, exp.Select)
    node = tree.selects[0].unalias()
    return node.to.sql(dialect=_sqlglot(dialect)) if isinstance(node, exp.Cast) else None


def _without_output_alias(sql: str, *, dialect: str) -> str:
    tree = sqlglot.parse_one(sql, dialect=_sqlglot(dialect))
    assert isinstance(tree, exp.Select)
    first = tree.selects[0]
    first.replace(first.unalias())
    return tree.sql(dialect=_sqlglot(dialect))


# --------------------------------------------------------------------------- #
# sum_distinct / avg_distinct / MIN-MAX(DISTINCT)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("dialect", TIER1)
@pytest.mark.parametrize(("agg", "sql_agg"), [("sum_distinct", "SUM"), ("avg_distinct", "AVG")])
async def test_distinct_aggregate_is_the_sql_distinct_form(dialect: str, agg: str, sql_agg: str) -> None:
    """DISTINCT sum and average: ``SUM/AVG(DISTINCT x)`` on every dialect."""
    sql = await _sql(f"{agg}(orders.qty)", dialect=dialect)
    assert f"{sql_agg}(DISTINCT ORDERS.QTY)" in sql.upper().replace("`", "").replace('"', "")


@pytest.mark.parametrize("dialect", TIER1)
async def test_sum_distinct_over_a_boolean_sums_distinct_values(dialect: str) -> None:
    """DISTINCT sum and average › Boolean input."""
    assert "SUM(DISTINCT" in (await _sql("sum_distinct(orders.is_paid)", dialect=dialect)).upper()


@pytest.mark.parametrize("dialect", TIER1)
@pytest.mark.parametrize(("ossie", "native"), [
    ("SUM(DISTINCT orders.qty)", "sum_distinct(orders.qty)"),
    ("AVG(DISTINCT orders.qty)", "avg_distinct(orders.qty)"),
    ("MIN(DISTINCT orders.qty)", "min(orders.qty)"),
    ("max(distinct orders.qty)", "max(orders.qty)"),
])
async def test_ossie_spelling_emits_the_native_sql(dialect: str, ossie: str, native: str) -> None:
    """DISTINCT sum and average › Ossie spelling equals the native spelling (same result key and SQL)."""
    assert await _sql(ossie, dialect=dialect) == await _sql(native, dialect=dialect)


# --------------------------------------------------------------------------- #
# percentile_disc
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("dialect", ["postgres", "snowflake"])
@pytest.mark.parametrize("column", ["amount", "status", "order_date"])
async def test_percentile_disc_is_native_within_group(dialect: str, column: str) -> None:
    sql = await _sql(f"percentile_disc(orders.{column}, 0.5)", dialect=dialect)
    assert f"PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY orders.{column})" in sql


@pytest.mark.parametrize("column", ["amount", "status", "order_date"])
async def test_percentile_disc_on_duckdb_is_its_discrete_quantile(column: str) -> None:
    sql = (await _sql(f"percentile_disc(orders.{column}, 0.5)", dialect="duckdb")).upper()
    assert "QUANTILE_DISC" in sql or "PERCENTILE_DISC" in sql
    assert "_CONT" not in sql


@pytest.mark.parametrize("column", ["amount", "status", "order_date"])
async def test_percentile_disc_on_sqlite_calls_the_udf(column: str) -> None:
    sql = await _sql(f"percentile_disc(orders.{column}, 0.5)", dialect="sqlite")
    assert f"PERCENTILE_DISC(orders.{column}, 0.5)" in sql


@pytest.mark.parametrize("dialect", ["mysql", "trino"])
@pytest.mark.parametrize("column", ["amount", "status", "order_date"])
async def test_percentile_disc_is_the_ordered_set_split(dialect: str, column: str) -> None:
    sql = (await _sql(f"percentile_disc(orders.{column}, 0.5)", dialect=dialect,
                      dimensions=("orders.id",))).upper()
    assert "ROW_NUMBER() OVER" in sql
    assert not any(name in sql for name in ("PERCENTILE_DISC", "PERCENTILE_CONT", "APPROX"))


@pytest.mark.parametrize("column", ["amount", "status", "order_date"])
async def test_percentile_disc_on_bigquery_is_analytic_collapsed_by_max(column: str) -> None:
    sql = await _sql(f"percentile_disc(orders.{column}, 0.5)", dialect="bigquery", dimensions=("orders.status",))
    assert f"PERCENTILE_DISC(orders.{column}, 0.5) OVER (PARTITION BY orders.status)" in sql
    assert "MAX(" in sql


async def test_percentile_disc_on_sql_server_numeric_is_native_windowed() -> None:
    sql = await _sql("percentile_disc(orders.amount, 0.5)", dialect="tsql", dimensions=("orders.status",))
    assert "PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY orders.amount) OVER (PARTITION BY orders.status)" in sql


@pytest.mark.parametrize("column", ["status", "order_date", "created_at", "is_paid"])
async def test_percentile_disc_on_sql_server_non_numeric_is_the_split(column: str) -> None:
    sql = (await _sql(f"percentile_disc(orders.{column}, 0.5)", dialect="tsql", dimensions=("orders.id",))).upper()
    assert "ROW_NUMBER() OVER" in sql
    assert "PERCENTILE_DISC" not in sql


@pytest.mark.parametrize("column", ["amount", "status", "order_date"])
async def test_percentile_disc_on_clickhouse_is_never_reservoir_sampled(column: str) -> None:
    sql = (await _sql(f"percentile_disc(orders.{column}, 0.5)", dialect="clickhouse")).lower()
    assert re.search(r"\bquantile\(", sql) is None
    assert "percentile_disc" not in sql


# --------------------------------------------------------------------------- #
# percentile_approx
# --------------------------------------------------------------------------- #
NATIVE_APPROX = {
    "duckdb": "APPROX_QUANTILE(", "snowflake": "APPROX_PERCENTILE(", "trino": "APPROX_PERCENTILE(",
    "clickhouse": "QUANTILE(0.9)(", "tsql": "APPROX_PERCENTILE_CONT(",
}


@pytest.mark.parametrize("dialect", sorted(NATIVE_APPROX))
async def test_percentile_approx_is_native(dialect: str) -> None:
    """Approximate percentile: the database's own approximate percentile."""
    assert NATIVE_APPROX[dialect] in (await _sql("percentile_approx(orders.amount, 0.9)", dialect=dialect)).upper()


@pytest.mark.parametrize("dialect", sorted(NATIVE_APPROX))
@pytest.mark.parametrize("column", ["qty", "price"])
async def test_percentile_approx_is_cast_to_the_percentile_float(dialect: str, column: str) -> None:
    """Approximate percentile › Float result over integer and decimal inputs."""
    approx = await _sql(f"percentile_approx(orders.{column}, 0.9)", dialect=dialect)
    exact = await _sql(f"percentile(orders.{column}, 0.9)", dialect=dialect)
    assert _outer_cast(approx, dialect=dialect) == _outer_cast(exact, dialect=dialect) is not None


@pytest.mark.parametrize("dialect", ["sqlite", "postgres", "mysql", "bigquery"])
async def test_percentile_approx_is_exact_without_a_native_approximation(dialect: str) -> None:
    """Approximate percentile › Exact where the database has no approximate percentile."""
    approx = await _sql("percentile_approx(orders.amount, 0.9)", dialect=dialect)
    exact = await _sql("percentile(orders.amount, 0.9)", dialect=dialect)
    assert _without_output_alias(approx, dialect=dialect) == _without_output_alias(exact, dialect=dialect)


# --------------------------------------------------------------------------- #
# ClickHouse exact percentile and median
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("expression", "p"), [("percentile(orders.amount, 0.9)", "0.9"),
                                               ("median(orders.amount)", "0.5")])
async def test_clickhouse_percentile_is_exact_and_null_guarded(expression: str, p: str) -> None:
    """Percentiles are exact on every Tier-1 database (ClickHouse)."""
    sql = (await _sql(expression, dialect="clickhouse")).lower()
    assert f"quantileexactinclusive({p})(" in sql
    assert re.search(r"\bquantile\(", sql) is None
    assert re.search(r"\bcount(if)?\(", sql) is not None


@pytest.mark.parametrize("column", ["amount", "order_date", "created_at"])
async def test_clickhouse_percentile_disc_is_null_guarded(column: str) -> None:
    """Design codex #4: the discrete percentile over numeric, date and timestamp input is guarded by a non-NULL count."""
    sql = (await _sql(f"percentile_disc(orders.{column}, 0.5)", dialect="clickhouse")).lower()
    assert re.search(r"\bquantile\(", sql) is None
    assert re.search(r"\bcount(if)?\(", sql) is not None


# --------------------------------------------------------------------------- #
# Positional p
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("dialect", TIER1)
@pytest.mark.parametrize("agg", ["percentile_disc", "percentile_approx"])
async def test_positional_p_equals_named(dialect: str, agg: str) -> None:
    """Discrete percentile › Positional p equals named."""
    positional = await _sql(f"{agg}(orders.amount, 0.9)", dialect=dialect)
    assert positional == await _sql(f"{agg}(orders.amount, p=0.9)", dialect=dialect)


async def test_clickhouse_exact_percentile_reads_a_decimal_as_a_float() -> None:
    """ClickHouse's ``quantileExactInclusive`` refuses a ``Decimal`` argument."""
    sql = (await _sql("percentile(orders.price, 0.5)", dialect="clickhouse")).lower()
    assert "quantileexactinclusive(0.5)(tofloat64(orders.price))" in sql
