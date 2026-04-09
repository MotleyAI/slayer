"""Tests for the SQL generator."""

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Dimension, Measure, ModelJoin, SlayerModel
from slayer.core.query import ColumnRef, Field, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.generator import SQLGenerator


def _generate(
    generator: SQLGenerator,
    query: SlayerQuery,
    model: SlayerModel,
) -> str:
    """Helper: enrich a query against a model, then generate SQL."""
    from slayer.engine.enrichment import enrich_query

    enriched = enrich_query(
        query=query,
        model=model,
        resolve_dimension_via_joins=lambda **kw: None,
        resolve_cross_model_measure=lambda **kw: None,
        resolve_join_target=lambda **kw: None,
    )
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
        query = SlayerQuery(source_model="orders", fields=[Field(formula="count")])
        sql = _generate(generator, query, orders_model)
        assert "COUNT(*)" in sql
        assert "public.orders" in sql

    def test_dimensions_only(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(source_model="orders", dimensions=[ColumnRef(name="status")])
        sql = _generate(generator, query, orders_model)
        assert "orders.status" in sql
        assert "GROUP BY" not in sql  # No aggregation, no GROUP BY

    def test_dimension_with_measure(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
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
            source_model="orders",
            fields=[Field(formula="count")],
            limit=10,
            offset=20,
        )
        sql = _generate(generator, query, orders_model)
        assert "LIMIT 10" in sql
        assert "OFFSET 20" in sql

    def test_order_by(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
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
            source_model="orders",
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
            source_model="orders",
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
            source_model="orders",
            fields=[Field(formula="count")],
            filters=["status == 'active'"],
        )
        sql = _generate(generator, query, orders_model)
        assert "WHERE" in sql
        assert "'active'" in sql

    def test_in_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="count")],
            filters=["status in ('active', 'pending')"],
        )
        sql = _generate(generator, query, orders_model)
        assert "IN" in sql
        assert "'active'" in sql
        assert "'pending'" in sql

    def test_gt_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="count")],
            filters=["customer_id > 100"],
        )
        sql = _generate(generator, query, orders_model)
        assert ">" in sql
        assert "100" in sql

    def test_contains_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="count")],
            filters=["status like '%act%'"],
        )
        sql = _generate(generator, query, orders_model)
        assert "LIKE" in sql
        assert "%act%" in sql

    def test_is_null_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="count")],
            filters=["status IS NULL"],
        )
        sql = _generate(generator, query, orders_model)
        assert "IS NULL" in sql

    def test_is_not_null_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="count")],
            filters=["status IS NOT NULL"],
        )
        sql = _generate(generator, query, orders_model)
        # Python AST may produce "NOT x IS NULL" instead of "x IS NOT NULL" — both valid
        assert "IS NOT NULL" in sql or "NOT" in sql

    def test_is_null_python_compat(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Python-style 'is None' still works for backward compatibility."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="count")],
            filters=["status is None"],
        )
        sql = _generate(generator, query, orders_model)
        assert "IS NULL" in sql

    def test_sql_equals_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """SQL single = works as equality."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="count")],
            filters=["status = 'active'"],
        )
        sql = _generate(generator, query, orders_model)
        assert "= 'active'" in sql

    def test_sql_not_equals_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """SQL <> works as not-equals."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="count")],
            filters=["status <> 'cancelled'"],
        )
        sql = _generate(generator, query, orders_model)
        # sqlglot may output either != or <> depending on dialect — both valid
        assert "<> 'cancelled'" in sql or "!= 'cancelled'" in sql

    def test_equals_inside_string_literal_not_converted(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """= inside a string literal is not converted to ==."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="count")],
            filters=["status = 'x=y'"],
        )
        sql = _generate(generator, query, orders_model)
        assert "'x=y'" in sql

    def test_not_equals_inside_string_literal_not_converted(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """<> inside a string literal is not converted to !=."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="count")],
            filters=["status = 'foo<>bar'"],
        )
        sql = _generate(generator, query, orders_model)
        assert "'foo<>bar'" in sql

    def test_composite_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="count")],
            filters=["status == 'active' or customer_id > 10"],
        )
        sql = _generate(generator, query, orders_model)
        assert "OR" in sql

    def test_measure_filter_goes_to_having(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="revenue")],
            dimensions=[ColumnRef(name="status")],
            filters=["revenue > 1000"],
        )
        sql = _generate(generator, query, orders_model)
        assert "HAVING" in sql

    def test_filter_resolves_dimension_sql(self, generator: SQLGenerator) -> None:
        """Filter column names resolve through dimension sql expressions."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            dimensions=[
                Dimension(name="order_status", sql="status_col", type=DataType.STRING),
            ],
            measures=[Measure(name="count", type=DataType.COUNT)],
        )
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="count")],
            filters=["order_status == 'active'"],
        )
        sql = _generate(generator, query, model)
        assert "status_col" in sql
        assert "order_status" not in sql.split("WHERE")[1]  # dimension name not in WHERE

    def test_date_range_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="count")],
            filters=["created_at >= '2024-01-01' and created_at <= '2024-06-30'"],
        )
        sql = _generate(generator, query, orders_model)
        assert ">=" in sql
        assert "<=" in sql


class TestMeasureTypes:
    def test_count_distinct(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(source_model="orders", fields=[Field(formula="distinct_customers")])
        sql = _generate(generator, query, orders_model)
        assert "COUNT(DISTINCT" in sql

    def test_average(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(source_model="orders", fields=[Field(formula="avg_revenue")])
        sql = _generate(generator, query, orders_model)
        assert "AVG(" in sql

    def test_sum(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(source_model="orders", fields=[Field(formula="revenue")])
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
            source_model="recent_orders",
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
            source_model="orders",
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
            source_model="orders",
            fields=[Field(formula="total")],
        )
        sql = _generate(gen, query, model)
        assert "SUM" in sql
        assert "amount" in sql.lower()


class TestDialects:
    def test_mysql_dialect(self, orders_model: SlayerModel) -> None:
        gen = SQLGenerator(dialect="mysql")
        query = SlayerQuery(
            source_model="orders",
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
            source_model="orders",
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
            source_model="orders",
            fields=[Field(formula="count")],
        )
        sql = _generate(generator, query, orders_model)
        assert "WITH" not in sql

    def test_field_with_limit(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """LIMIT applies to the outer query, not the CTE."""
        query = SlayerQuery(
            source_model="orders",
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
            source_model="orders",
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
            source_model="orders",
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
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="lag(revenue, 1)", name="rev_prev")],
        )
        sql = _generate(generator, query, orders_model)
        assert "LAG(" in sql
        assert "OVER" in sql

    def test_lead(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="lead(revenue, 1)", name="rev_next")],
        )
        sql = _generate(generator, query, orders_model)
        assert "LEAD(" in sql
        assert "OVER" in sql

    def test_change(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="change(revenue)", name="rev_change")],
        )
        sql = _generate(generator, query, orders_model)
        assert "shifted_" in sql
        assert "LEFT JOIN" in sql
        assert "_rn" in sql
        # change = current - previous (self-join column expression)
        assert " - shifted_" in sql

    def test_change_pct(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="change_pct(revenue)", name="rev_pct")],
        )
        sql = _generate(generator, query, orders_model)
        assert "shifted_" in sql
        assert "LEFT JOIN" in sql
        assert "CASE" in sql

    def test_rank(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            fields=[Field(formula="revenue"), Field(formula="rank(revenue)", name="rev_rank")],
        )
        sql = _generate(generator, query, orders_model)
        assert "RANK()" in sql
        assert "OVER" in sql

    def test_last(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="last(revenue)", name="latest_rev")],
        )
        sql = _generate(generator, query, orders_model)
        assert "FIRST_VALUE(" in sql
        assert "DESC" in sql

    def test_last_measure_type(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """A measure with type=last should use ROW_NUMBER + conditional aggregate."""
        orders_model.default_time_dimension = "created_at"
        orders_model.measures.append(Measure(name="balance", sql="balance", type=DataType.LAST))
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="balance")],
        )
        sql = _generate(generator, query, orders_model)
        # ROW_NUMBER ranked subquery for latest row per group
        assert "ROW_NUMBER()" in sql
        assert "_last_rn" in sql
        assert "DESC" in sql
        # Conditional aggregate: MAX(CASE WHEN _last_rn = 1 THEN col END)
        assert "MAX(" in sql
        assert "CASE" in sql

    def test_time_shift(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="time_shift(revenue, -1, 'year')", name="rev_prev_year")],
        )
        sql = _generate(generator, query, orders_model)
        assert "shifted_" in sql
        assert "LEFT JOIN" in sql
        assert "INTERVAL" in sql

    def test_time_shift_shifted_date_range(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Calendar time_shift with date_range should shift the filter in the shifted CTE."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-03-01", "2024-03-31"],
                )
            ],
            fields=[Field(formula="revenue"), Field(formula="time_shift(revenue, -1, 'month')", name="rev_prev")],
        )
        sql = _generate(generator, query, orders_model)
        # Base CTE should have original date range
        assert "2024-03-01" in sql
        assert "2024-03-31" in sql
        # Shifted CTE should have date range shifted back by 1 month
        assert "2024-02-01" in sql
        assert "2024-02-29" in sql

    def test_time_shift_yoy_shifted_date_range(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Year-over-year time_shift should shift the date range by 1 year."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-03-01", "2024-03-31"],
                )
            ],
            fields=[Field(formula="revenue"), Field(formula="time_shift(revenue, -1, 'year')", name="rev_yoy")],
        )
        sql = _generate(generator, query, orders_model)
        # Shifted CTE should query March 2023
        assert "2023-03-01" in sql
        assert "2023-03-31" in sql

    def test_change_shifted_date_range(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Row-based change with date_range should shift the filter using query's time granularity."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-03-01", "2024-03-31"],
                )
            ],
            fields=[Field(formula="revenue"), Field(formula="change(revenue)", name="rev_change")],
        )
        sql = _generate(generator, query, orders_model)
        # change looks back 1 period — shifted CTE should query February
        assert "2024-02-01" in sql
        assert "2024-02-29" in sql

    def test_no_date_range_no_shift(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Without a date_range, shifted CTE should still be a valid base query (no date filter)."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="time_shift(revenue, -1, 'month')", name="rev_prev")],
        )
        sql = _generate(generator, query, orders_model)
        # Both base and shifted CTEs should query the source table without date filters
        assert "shifted_base_" in sql
        assert "BETWEEN" not in sql

    def test_forward_time_shift_with_date_range(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Forward time_shift(x, 1, 'month') with date_range should shift the filter forward."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                    date_range=["2024-03-01", "2024-03-31"],
                )
            ],
            fields=[Field(formula="revenue"), Field(formula="time_shift(revenue, 1, 'month')", name="rev_next")],
        )
        sql = _generate(generator, query, orders_model)
        # Shifted CTE should query April (1 month forward)
        assert "2024-04-01" in sql
        assert "2024-04-30" in sql

    def test_quarter_date_shift(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """time_shift with quarter granularity should shift the date range by 3 months."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.QUARTER,
                    date_range=["2024-07-01", "2024-09-30"],
                )
            ],
            fields=[Field(formula="revenue"), Field(formula="time_shift(revenue, -1, 'quarter')", name="prev_q")],
        )
        sql = _generate(generator, query, orders_model)
        # Q3 2024 shifted back 1 quarter = Q2 2024
        assert "2024-04-01" in sql
        assert "2024-06-30" in sql

    def test_nested_self_join_raises(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Nesting self-join transforms (e.g., change(time_shift(x))) should raise."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="change(time_shift(revenue, -1, 'year'))", name="x")],
        )
        with pytest.raises(ValueError, match="Nesting.*not supported"):
            _generate(generator, query, orders_model)

    def test_post_filter_on_computed_column(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Filters on computed columns should be applied as post-filter wrapper."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="change(revenue)", name="rev_change")],
            filters=["rev_change < 0"],
        )
        sql = _generate(generator, query, orders_model)
        # Should wrap in a post-filter SELECT
        assert "_filtered" in sql
        assert '"orders.rev_change" < 0' in sql

    def test_inline_transform_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Transform expressions in filters should be auto-extracted as hidden fields."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue")],
            filters=["last(change(revenue)) < 0"],
        )
        sql = _generate(generator, query, orders_model)
        # Should have the hidden transform columns
        assert "FIRST_VALUE" in sql  # last()
        assert "shifted_" in sql  # change() via self-join
        # Should have post-filter wrapper
        assert "_filtered" in sql
        assert "< 0" in sql

    def test_mixed_base_and_post_filters(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Base filters and post-filters should coexist correctly."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="change(revenue)", name="rev_change")],
            filters=["status == 'completed'", "rev_change > 0"],
        )
        sql = _generate(generator, query, orders_model)
        # Base filter should be in the inner WHERE
        assert "'completed'" in sql
        # Post-filter should be in the outer wrapper
        assert '"orders.rev_change" > 0' in sql
        assert "_filtered" in sql

    def test_transform_without_time_raises(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Transforms requiring time should fail if no time dimension available."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="revenue"), Field(formula="cumsum(revenue)", name="x")],
        )
        with pytest.raises(ValueError, match="requires a time dimension"):
            _generate(generator, query, orders_model)

    def test_default_time_dimension_fallback(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Model's default_time_dimension should be used when query has no time_dimensions."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="revenue"), Field(formula="cumsum(revenue)", name="x")],
        )
        sql = _generate(generator, query, orders_model)
        assert "OVER" in sql

    def test_field_plain_measure(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="count")],
        )
        sql = _generate(generator, query, orders_model)
        assert "COUNT(*)" in sql

    def test_field_auto_adds_measures(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Fields referencing measures auto-add them to the base query."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="count"), Field(formula="revenue"), Field(formula="revenue / count", name="aov")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = _generate(generator, query, orders_model)
        assert "aov" in sql.lower()
        assert "WITH" in sql

    def test_field_mixed_with_measures(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Fields can be used alongside explicit measures."""
        query = SlayerQuery(
            source_model="orders",
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
            source_model="orders",
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
        assert "shifted_" in sql  # change uses self-join
        assert "delta" in sql.lower()

    def test_mixed_arithmetic_with_transform(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """cumsum(revenue) / count should work."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
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

    @pytest.mark.parametrize(
        "ds_type,expected",
        [
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
        ],
    )
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

    ALL_DIALECTS = [
        "postgres",
        "mysql",
        "sqlite",
        "clickhouse",
        "bigquery",
        "snowflake",
        "duckdb",
        "redshift",
        "trino",
        "presto",
        "databricks",
        "spark",
        "tsql",
        "oracle",
    ]

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    def test_basic_query(self, dialect: str, orders_model: SlayerModel) -> None:
        """Basic aggregation query should generate valid SQL for every dialect."""
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
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
            source_model="orders",
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
        """Calendar-based time_shift should produce dialect-appropriate date arithmetic in shifted CTE."""
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue"), Field(formula="time_shift(revenue, -1, 'year')", name="rev_prev_year")],
        )
        sql = _generate(gen, query, orders_model)
        assert "shifted_" in sql
        assert "LEFT JOIN" in sql
        # Join should be simple equality (timestamp shift is inside the shifted CTE)
        # Dialect-specific date arithmetic should appear in the shifted CTE's SELECT/GROUP BY
        sql_upper = sql.upper()
        if dialect == "sqlite":
            assert "DATE(" in sql_upper
        else:
            assert "INTERVAL" in sql_upper


class TestPathAliasJoinInference:
    """Test that __-delimited path aliases in inline SQL are split for join inference."""

    @pytest.fixture
    def chained_model(self) -> SlayerModel:
        """Model with orders → customers → regions join chain."""
        return SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            dimensions=[
                Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Dimension(name="customer_id", sql="customer_id", type=DataType.NUMBER),
                Dimension(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                # Inline dimension referencing a path-aliased joined table
                Dimension(
                    name="is_us",
                    sql="CASE WHEN customers__regions.name = 'US' THEN 1 ELSE 0 END",
                    type=DataType.NUMBER,
                ),
            ],
            measures=[
                Measure(name="count", type=DataType.COUNT),
            ],
            joins=[
                ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
                ModelJoin(target_model="regions", join_pairs=[["customers.region_id", "id"]]),
            ],
        )

    @pytest.fixture
    def engine(self) -> SlayerQueryEngine:
        return SlayerQueryEngine(storage=None)

    def test_dimension_sql_with_path_alias_infers_joins(
        self, engine: SlayerQueryEngine, chained_model: SlayerModel
    ) -> None:
        """Inline dimension SQL like 'customers__regions.name' should infer joins for both tables."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="count")],
            dimensions=[ColumnRef(name="is_us")],
        )
        enriched = engine._enrich(query=query, model=chained_model)
        join_aliases = {alias for _, alias, _ in enriched.resolved_joins}
        assert "customers" in join_aliases
        assert "customers__regions" in join_aliases

    def test_time_dimension_sql_with_path_alias_infers_joins(self, engine: SlayerQueryEngine) -> None:
        """Inline time dimension SQL referencing path alias should also trigger join inference."""
        model = SlayerModel(
            name="events",
            sql_table="events",
            data_source="test",
            dimensions=[
                Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Dimension(name="user_id", sql="user_id", type=DataType.NUMBER),
                # Time dimension with SQL referencing a path alias
                Dimension(
                    name="user_signup_date",
                    sql="users__orgs.signup_date",
                    type=DataType.TIMESTAMP,
                ),
            ],
            measures=[
                Measure(name="count", type=DataType.COUNT),
            ],
            joins=[
                ModelJoin(target_model="users", join_pairs=[["user_id", "id"]]),
                ModelJoin(target_model="orgs", join_pairs=[["users.org_id", "id"]]),
            ],
        )
        query = SlayerQuery(
            source_model="events",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="user_signup_date"),
                    granularity=TimeGranularity.MONTH,
                )
            ],
            fields=[Field(formula="count")],
        )
        enriched = engine._enrich(query=query, model=model)
        join_aliases = {alias for _, alias, _ in enriched.resolved_joins}
        assert "users" in join_aliases
        assert "users__orgs" in join_aliases

    def test_measure_sql_with_path_alias_infers_joins(
        self, engine: SlayerQueryEngine, chained_model: SlayerModel
    ) -> None:
        """Measure SQL like 'customers__regions.population' should infer joins for both tables."""
        # Add a measure referencing a path-aliased joined table
        chained_model.measures.append(
            Measure(name="region_pop_sum", sql="customers__regions.population", type=DataType.SUM)
        )
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="region_pop_sum")],
        )
        enriched = engine._enrich(query=query, model=chained_model)
        join_aliases = {alias for _, alias, _ in enriched.resolved_joins}
        assert "customers" in join_aliases
        assert "customers__regions" in join_aliases
