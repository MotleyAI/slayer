"""Tests for the SQL generator."""

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Dimension, Measure, SlayerModel
from slayer.core.query import ColumnRef, Field, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.generator import SQLGenerator


def _generate(
    generator: SQLGenerator,
    query: SlayerQuery,
    model: SlayerModel,
) -> str:
    """Helper: enrich a query against a model, then generate SQL."""
    enriched = SlayerQueryEngine._enrich(None, query=query, model=model)
    return generator.generate(enriched=enriched)


@pytest.fixture
def orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        dimensions=[
            Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Dimension(name="status", sql="status", type=DataType.STRING),
            Dimension(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Dimension(name="customer_id", sql="customer_id", type=DataType.NUMBER),
        ],
        measures=[
            Measure(name="count", type=DataType.COUNT),
            Measure(name="revenue", sql="amount", type=DataType.SUM),
            Measure(name="avg_revenue", sql="amount", type=DataType.AVERAGE),
            Measure(name="distinct_customers", sql="customer_id", type=DataType.COUNT_DISTINCT),
        ],
    )




@pytest.fixture
def generator() -> SQLGenerator:
    return SQLGenerator(dialect="postgres")


class TestBasicQueries:
    def test_simple_count(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(model="orders", fields=[Field(formula="count")])
        sql = _generate(generator, query, orders_model)
        assert "COUNT(*)" in sql
        assert "public.orders" in sql

    def test_dimensions_only(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(model="orders", dimensions=[ColumnRef(name="status")])
        sql = _generate(generator, query, orders_model)
        assert "orders.status" in sql
        assert "GROUP BY" not in sql  # No aggregation, no GROUP BY

    def test_dimension_with_measure(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count"), Field(formula="revenue")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = _generate(generator, query, orders_model)
        assert "COUNT(*)" in sql
        assert "SUM(" in sql
        assert "GROUP BY" in sql
        assert "orders.status" in sql

    def test_limit_and_offset(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count")],
            limit=10,
            offset=20,
        )
        sql = _generate(generator, query, orders_model)
        assert "LIMIT 10" in sql
        assert "OFFSET 20" in sql

    def test_order_by(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count")],
            dimensions=[ColumnRef(name="status")],
            order=[OrderItem(column=ColumnRef(name="count", model="orders"), direction="desc")],
        )
        sql = _generate(generator, query, orders_model)
        assert "ORDER BY" in sql
        assert "DESC" in sql


class TestTimeDimensions:
    def test_time_dimension_with_granularity(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count")],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
        )
        sql = _generate(generator, query, orders_model)
        assert "DATE_TRUNC" in sql
        assert "MONTH" in sql.upper()

    def test_time_dimension_with_date_range(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count")],
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.DAY,
                    date_range=["2024-01-01", "2024-12-31"],
                ),
            ],
        )
        sql = _generate(generator, query, orders_model)
        assert "BETWEEN" in sql
        assert "2024-01-01" in sql
        assert "2024-12-31" in sql

class TestFilters:
    def test_equals_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count")],
            filters=["status == 'active'"],
        )
        sql = _generate(generator, query, orders_model)
        assert "WHERE" in sql
        assert "'active'" in sql

    def test_in_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count")],
            filters=["status in ('active', 'pending')"],
        )
        sql = _generate(generator, query, orders_model)
        assert "IN" in sql
        assert "'active'" in sql
        assert "'pending'" in sql

    def test_gt_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count")],
            filters=["customer_id > 100"],
        )
        sql = _generate(generator, query, orders_model)
        assert ">" in sql
        assert "100" in sql

    def test_contains_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count")],
            filters=["contains(status, 'act')"],
        )
        sql = _generate(generator, query, orders_model)
        assert "LIKE" in sql
        assert "%act%" in sql

    def test_not_set_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count")],
            filters=["status is None"],
        )
        sql = _generate(generator, query, orders_model)
        assert "IS NULL" in sql

    def test_composite_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count")],
            filters=["status == 'active' or customer_id > 10"],
        )
        sql = _generate(generator, query, orders_model)
        assert "OR" in sql

    def test_measure_filter_goes_to_having(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="revenue")],
            dimensions=[ColumnRef(name="status")],
            filters=["revenue > 1000"],
        )
        sql = _generate(generator, query, orders_model)
        assert "HAVING" in sql

    def test_date_range_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count")],
            filters=["between(created_at, '2024-01-01', '2024-06-30')"],
        )
        sql = _generate(generator, query, orders_model)
        assert "BETWEEN" in sql


class TestMeasureTypes:
    def test_count_distinct(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(model="orders", fields=[Field(formula="distinct_customers")])
        sql = _generate(generator, query, orders_model)
        assert "COUNT(DISTINCT" in sql

    def test_average(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(model="orders", fields=[Field(formula="avg_revenue")])
        sql = _generate(generator, query, orders_model)
        assert "AVG(" in sql

    def test_sum(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(model="orders", fields=[Field(formula="revenue")])
        sql = _generate(generator, query, orders_model)
        assert "SUM(" in sql


class TestSubquery:
    def test_model_with_sql(self, generator: SQLGenerator) -> None:
        model = SlayerModel(
            name="recent_orders",
            sql="SELECT * FROM public.orders WHERE created_at > '2024-01-01'",
            data_source="test",
            dimensions=[Dimension(name="status", sql="status", type=DataType.STRING)],
            measures=[Measure(name="count", type=DataType.COUNT)],
        )
        query = SlayerQuery(
            model="recent_orders",
            fields=[Field(formula="count")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = _generate(generator, query, model)
        assert "recent_orders" in sql
        assert "2024-01-01" in sql


class TestBareColumnNames:
    def test_bare_column_in_dimension(self) -> None:
        """Dimensions with bare column names should work."""
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            dimensions=[
                Dimension(name="status", sql="status", type=DataType.STRING),
            ],
            measures=[
                Measure(name="count", type=DataType.COUNT),
            ],
        )
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = _generate(gen, query, model)
        # Bare "status" should be qualified as orders.status
        assert "orders" in sql.lower()
        assert "status" in sql.lower()
        assert "COUNT(*)" in sql

    def test_bare_column_in_measure(self) -> None:
        """Measures with bare column names should work."""
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            measures=[
                Measure(name="total", sql="amount", type=DataType.SUM),
            ],
        )
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="total")],
        )
        sql = _generate(gen, query, model)
        assert "SUM" in sql
        assert "amount" in sql.lower()


class TestDialects:
    def test_mysql_dialect(self, orders_model: SlayerModel) -> None:
        gen = SQLGenerator(dialect="mysql")
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count")],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
        )
        sql = _generate(gen, query, orders_model)
        assert "COUNT(*)" in sql  # Basic check — dialect-specific output


class TestFields:
    def test_arithmetic_field(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Arithmetic field generates CTE + outer SELECT."""
        query = SlayerQuery(
            model="orders",
            dimensions=[ColumnRef(name="status")],
            fields=[Field(formula="count"), Field(formula="revenue"), Field(formula="revenue / count", name="aov")],
        )
        sql = _generate(generator, query, orders_model)
        assert "base" in sql.lower()
        assert "aov" in sql.lower()
        assert "COUNT(*)" in sql
        assert "SUM(" in sql

    def test_no_fields_no_cte(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Without fields, no CTE is generated."""
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count")],
        )
        sql = _generate(generator, query, orders_model)
        assert "WITH" not in sql

    def test_field_with_limit(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """LIMIT applies to the outer query, not the CTE."""
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count"), Field(formula="revenue"), Field(formula="revenue / count", name="aov")],
            limit=5,
        )
        sql = _generate(generator, query, orders_model)
        assert "LIMIT 5" in sql
        cte_end = sql.lower().index("from base")
        limit_pos = sql.upper().index("LIMIT 5")
        assert limit_pos > cte_end

    def test_cumsum(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="cumsum(revenue)", name="rev_running")],
        )
        sql = _generate(generator, query, orders_model)
        assert "SUM(" in sql
        assert "OVER" in sql
        assert "ORDER BY" in sql
        assert "rev_running" in sql.lower()

    def test_time_shift_row_based(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="time_shift(revenue, -1)", name="rev_prev")],
        )
        sql = _generate(generator, query, orders_model)
        assert "shifted_" in sql
        assert "LEFT JOIN" in sql
        assert "ROW_NUMBER()" in sql
        assert "_rn" in sql

    def test_lag(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="lag(revenue, 1)", name="rev_prev")],
        )
        sql = _generate(generator, query, orders_model)
        assert "LAG(" in sql
        assert "OVER" in sql

    def test_lead(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="lead(revenue, 1)", name="rev_next")],
        )
        sql = _generate(generator, query, orders_model)
        assert "LEAD(" in sql
        assert "OVER" in sql

    def test_change(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="change(revenue)", name="rev_change")],
        )
        sql = _generate(generator, query, orders_model)
        assert "LAG(" in sql
        assert "OVER" in sql

    def test_change_pct(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="change_pct(revenue)", name="rev_pct")],
        )
        sql = _generate(generator, query, orders_model)
        assert "LAG(" in sql
        assert "CASE" in sql

    def test_rank(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            model="orders",
            dimensions=[ColumnRef(name="status")],
            fields=[Field(formula="revenue"), Field(formula="rank(revenue)", name="rev_rank")],
        )
        sql = _generate(generator, query, orders_model)
        assert "RANK()" in sql
        assert "OVER" in sql

    def test_last(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="last(revenue)", name="latest_rev")],
        )
        sql = _generate(generator, query, orders_model)
        assert "FIRST_VALUE(" in sql
        assert "DESC" in sql

    def test_time_shift(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="time_shift(revenue, -1, 'year')", name="rev_prev_year")],
        )
        sql = _generate(generator, query, orders_model)
        assert "shifted_" in sql
        assert "LEFT JOIN" in sql
        assert "INTERVAL" in sql

    def test_transform_without_time_raises(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Transforms requiring time should fail if no time dimension available."""
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="revenue"), Field(formula="cumsum(revenue)", name="x")],
        )
        with pytest.raises(ValueError, match="requires a time dimension"):
            _generate(generator, query, orders_model)

    def test_default_time_dimension_fallback(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Model's default_time_dimension should be used when query has no time_dimensions."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="revenue"), Field(formula="cumsum(revenue)", name="x")],
        )
        sql = _generate(generator, query, orders_model)
        assert "OVER" in sql

    def test_field_plain_measure(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count")],
        )
        sql = _generate(generator, query, orders_model)
        assert "COUNT(*)" in sql

    def test_field_auto_adds_measures(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Fields referencing measures auto-add them to the base query."""
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count"), Field(formula="revenue"), Field(formula="revenue / count", name="aov")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = _generate(generator, query, orders_model)
        assert "aov" in sql.lower()
        assert "WITH" in sql

    def test_field_mixed_with_measures(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Fields can be used alongside explicit measures."""
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count"), Field(formula="revenue"), Field(formula="revenue / count", name="aov")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = _generate(generator, query, orders_model)
        assert "COUNT(*)" in sql
        assert "SUM(" in sql
        assert "aov" in sql.lower()


class TestNestedFields:
    def test_nested_transform_generates_stacked_ctes(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """change(cumsum(revenue)) should produce stacked CTEs."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[
                Field(formula="revenue"),
                Field(formula="change(cumsum(revenue))", name="delta"),
            ],
        )
        sql = _generate(generator, query, orders_model)
        # Should have base + at least one step CTE
        assert "base" in sql.lower()
        assert "step" in sql.lower()
        assert "SUM(" in sql  # cumsum
        assert "LAG(" in sql  # change
        assert "delta" in sql.lower()

    def test_mixed_arithmetic_with_transform(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """cumsum(revenue) / count should work."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[
                Field(formula="count"),
                Field(formula="revenue"),
                Field(formula="cumsum(revenue) / count", name="avg_cumsum"),
            ],
        )
        sql = _generate(generator, query, orders_model)
        assert "SUM(" in sql  # cumsum window
        assert "avg_cumsum" in sql.lower()


class TestDialectMapping:
    """Test _dialect_for_type resolves all supported datasource types."""

    @pytest.mark.parametrize("ds_type,expected", [
        ("postgres", "postgres"),
        ("postgresql", "postgres"),
        ("mysql", "mysql"),
        ("mariadb", "mysql"),
        ("clickhouse", "clickhouse"),
        ("bigquery", "bigquery"),
        ("snowflake", "snowflake"),
        ("sqlite", "sqlite"),
        ("duckdb", "duckdb"),
        ("redshift", "redshift"),
        ("trino", "trino"),
        ("presto", "presto"),
        ("athena", "presto"),
        ("databricks", "databricks"),
        ("spark", "spark"),
        ("mssql", "tsql"),
        ("sqlserver", "tsql"),
        ("tsql", "tsql"),
        ("oracle", "oracle"),
        (None, "postgres"),
        ("unknown", "postgres"),
    ])
    def test_dialect_for_type(self, ds_type: str, expected: str) -> None:
        assert SlayerQueryEngine._dialect_for_type(ds_type) == expected


class TestMultiDialectGeneration:
    """Test SQL generation across all supported dialects."""

    @pytest.fixture
    def orders_model(self) -> SlayerModel:
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            default_time_dimension="created_at",
            dimensions=[
                Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Dimension(name="status", sql="status", type=DataType.STRING),
                Dimension(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            ],
            measures=[
                Measure(name="count", type=DataType.COUNT),
                Measure(name="revenue", sql="amount", type=DataType.SUM),
            ],
        )
        return model

    ALL_DIALECTS = ["postgres", "mysql", "sqlite", "clickhouse", "bigquery",
                    "snowflake", "duckdb", "redshift", "trino", "presto",
                    "databricks", "spark", "tsql", "oracle"]

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_basic_query(self, dialect: str, orders_model: SlayerModel) -> None:
        """Basic aggregation query should generate valid SQL for every dialect."""
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count"), Field(formula="revenue")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = _generate(gen, query, orders_model)
        assert "COUNT(" in sql
        assert "SUM(" in sql

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_date_trunc(self, dialect: str, orders_model: SlayerModel) -> None:
        """DATE_TRUNC should produce valid output for every dialect."""
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            model="orders",
            fields=[Field(formula="count")],
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
        )
        sql = _generate(gen, query, orders_model)
        assert "COUNT(" in sql
        # Each dialect uses its own truncation function
        sql_upper = sql.upper()
        assert any(fn in sql_upper for fn in ["DATE_TRUNC", "STRFTIME", "TRUNC", "STR_TO_DATE"])

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_calendar_time_shift(self, dialect: str, orders_model: SlayerModel) -> None:
        """Calendar-based time_shift should produce dialect-appropriate date arithmetic."""
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="time_shift(revenue, -1, 'year')", name="rev_prev_year")],
        )
        sql = _generate(gen, query, orders_model)
        assert "shifted_" in sql
        assert "LEFT JOIN" in sql
        # Dialect-specific date arithmetic
        sql_upper = sql.upper()
        if dialect == "sqlite":
            assert "DATE(" in sql_upper
        elif dialect in ("bigquery", "clickhouse", "databricks", "spark", "tsql"):
            assert "DATE_ADD(" in sql_upper or "DATEADD(" in sql_upper
        elif dialect in ("snowflake", "redshift"):
            assert "DATEADD(" in sql_upper
        elif dialect in ("trino", "presto"):
            assert "DATE_ADD(" in sql_upper
        else:
            # Postgres, MySQL, DuckDB — INTERVAL syntax
            assert "INTERVAL" in sql_upper
