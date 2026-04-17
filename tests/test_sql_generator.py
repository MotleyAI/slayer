"""Tests for the SQL generator."""

import pytest
import sqlglot

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Aggregation, AggregationParam, Dimension, Measure, ModelJoin, SlayerModel
from slayer.core.query import ColumnRef, Field, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.enriched import EnrichedMeasure, EnrichedQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.generator import SQLGenerator, _validate_agg_param_value


async def _noop_async(**kw):
    return None


async def _generate(
    generator: SQLGenerator,
    query: SlayerQuery,
    model: SlayerModel,
) -> str:
    """Helper: enrich a query against a model, then generate SQL."""
    from slayer.engine.enrichment import enrich_query

    enriched = await enrich_query(
        query=query,
        model=model,
        resolve_dimension_via_joins=_noop_async,
        resolve_cross_model_measure=_noop_async,
        resolve_join_target=_noop_async,
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
            Measure(name="revenue", sql="amount"),
            Measure(name="avg_revenue", sql="amount"),
            Measure(name="distinct_customers", sql="customer_id"),
        ],
    )


@pytest.fixture
def generator() -> SQLGenerator:
    return SQLGenerator(dialect="postgres")


class TestBasicQueries:
    async def test_numeric_literal_measure(self, generator: SQLGenerator) -> None:
        """Measures with numeric SQL expressions (e.g. dbt `expr: 1`) should generate
        SUM(1), not SUM(model."1")."""
        model = SlayerModel(
            name="policy",
            sql_table="policy",
            data_source="test",
            dimensions=[
                Dimension(name="status", type=DataType.STRING),
            ],
            measures=[
                Measure(name="num_policies", sql="1", allowed_aggregations=["sum"]),
            ],
        )
        query = SlayerQuery(source_model="policy", fields=[Field(formula="num_policies:sum")])
        sql = await _generate(generator, query, model)
        assert "SUM(1)" in sql
        assert '"1"' not in sql

    async def test_simple_count(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(source_model="orders", fields=[Field(formula="*:count")])
        sql = await _generate(generator, query, orders_model)
        assert "COUNT(*)" in sql
        assert "public.orders" in sql

    async def test_star_rejects_non_count_aggregation(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        query = SlayerQuery(source_model="orders", fields=[Field(formula="*:sum")])
        with pytest.raises(ValueError, match=r"not allowed with measure '\*'"):
            await _generate(generator, query, orders_model)

    async def test_dimensions_only(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(source_model="orders", dimensions=[ColumnRef(name="status")])
        sql = await _generate(generator, query, orders_model)
        assert "orders.status" in sql
        assert "GROUP BY" not in sql  # No aggregation, no GROUP BY

    async def test_dimension_with_measure(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count"), Field(formula="revenue:sum")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "COUNT(*)" in sql
        assert "SUM(" in sql
        assert "GROUP BY" in sql
        assert "orders.status" in sql

    async def test_limit_and_offset(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            limit=10,
            offset=20,
        )
        sql = await _generate(generator, query, orders_model)
        assert "LIMIT 10" in sql
        assert "OFFSET 20" in sql

    async def test_order_by(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            dimensions=[ColumnRef(name="status")],
            order=[OrderItem(column=ColumnRef(name="count", model="orders"), direction="desc")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "ORDER BY" in sql
        assert "DESC" in sql


class TestTimeDimensions:
    async def test_time_dimension_with_granularity(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        assert "DATE_TRUNC" in sql
        assert "MONTH" in sql.upper()

    async def test_time_dimension_with_date_range(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.DAY,
                    date_range=["2024-01-01", "2024-12-31"],
                ),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        assert "BETWEEN" in sql
        assert "2024-01-01" in sql
        assert "2024-12-31" in sql


class TestFilters:
    async def test_equals_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            filters=["status == 'active'"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "WHERE" in sql
        assert "'active'" in sql

    async def test_in_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            filters=["status in ('active', 'pending')"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "IN" in sql
        assert "'active'" in sql
        assert "'pending'" in sql

    async def test_gt_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            filters=["customer_id > 100"],
        )
        sql = await _generate(generator, query, orders_model)
        assert ">" in sql
        assert "100" in sql

    async def test_contains_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            filters=["status like '%act%'"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "LIKE" in sql
        assert "%act%" in sql

    async def test_is_null_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            filters=["status IS NULL"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "IS NULL" in sql

    async def test_is_not_null_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            filters=["status IS NOT NULL"],
        )
        sql = await _generate(generator, query, orders_model)
        # Python AST may produce "NOT x IS NULL" instead of "x IS NOT NULL" — both valid
        assert "IS NOT NULL" in sql or "NOT" in sql

    async def test_is_null_python_compat(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Python-style 'is None' still works for backward compatibility."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            filters=["status is None"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "IS NULL" in sql

    async def test_sql_equals_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """SQL single = works as equality."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            filters=["status = 'active'"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "= 'active'" in sql

    async def test_sql_not_equals_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """SQL <> works as not-equals."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            filters=["status <> 'cancelled'"],
        )
        sql = await _generate(generator, query, orders_model)
        # sqlglot may output either != or <> depending on dialect — both valid
        assert "<> 'cancelled'" in sql or "!= 'cancelled'" in sql

    async def test_equals_inside_string_literal_not_converted(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """= inside a string literal is not converted to ==."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            filters=["status = 'x=y'"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "'x=y'" in sql

    async def test_not_equals_inside_string_literal_not_converted(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """<> inside a string literal is not converted to !=."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            filters=["status = 'foo<>bar'"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "'foo<>bar'" in sql

    async def test_composite_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            filters=["status == 'active' or customer_id > 10"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "OR" in sql

    async def test_measure_filter_goes_to_having(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="revenue:sum")],
            dimensions=[ColumnRef(name="status")],
            filters=["revenue_sum > 1000"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "HAVING" in sql

    async def test_filter_resolves_dimension_sql(self, generator: SQLGenerator) -> None:
        """Filter column names resolve through dimension sql expressions."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            dimensions=[
                Dimension(name="order_status", sql="status_col", type=DataType.STRING),
            ],
            measures=[Measure(name="revenue", sql="amount")],
        )
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            filters=["order_status == 'active'"],
        )
        sql = await _generate(generator, query, model)
        assert "status_col" in sql
        assert "order_status" not in sql.split("WHERE")[1]  # dimension name not in WHERE

    async def test_date_range_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            filters=["created_at >= '2024-01-01' and created_at <= '2024-06-30'"],
        )
        sql = await _generate(generator, query, orders_model)
        assert ">=" in sql
        assert "<=" in sql


class TestMeasureTypes:
    async def test_count_distinct(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(source_model="orders", fields=[Field(formula="distinct_customers:count_distinct")])
        sql = await _generate(generator, query, orders_model)
        assert "COUNT(DISTINCT" in sql

    async def test_average(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(source_model="orders", fields=[Field(formula="avg_revenue:avg")])
        sql = await _generate(generator, query, orders_model)
        assert "AVG(" in sql

    async def test_sum(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(source_model="orders", fields=[Field(formula="revenue:sum")])
        sql = await _generate(generator, query, orders_model)
        assert "SUM(" in sql


class TestSubquery:
    async def test_model_with_sql(self, generator: SQLGenerator) -> None:
        model = SlayerModel(
            name="recent_orders",
            sql="SELECT * FROM public.orders WHERE created_at > '2024-01-01'",
            data_source="test",
            dimensions=[Dimension(name="status", sql="status", type=DataType.STRING)],
            measures=[Measure(name="revenue", sql="amount")],
        )
        query = SlayerQuery(
            source_model="recent_orders",
            fields=[Field(formula="revenue:sum")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = await _generate(generator, query, model)
        assert "recent_orders" in sql
        assert "2024-01-01" in sql


class TestBareColumnNames:
    async def test_bare_column_in_dimension(self) -> None:
        """Dimensions with bare column names should work."""
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            dimensions=[
                Dimension(name="status", sql="status", type=DataType.STRING),
            ],
            measures=[
                Measure(name="revenue", sql="amount"),
            ],
        )
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = await _generate(gen, query, model)
        # Bare "status" should be qualified as orders.status
        assert "orders" in sql.lower()
        assert "status" in sql.lower()
        assert "COUNT(*)" in sql

    async def test_bare_column_in_measure(self) -> None:
        """Measures with bare column names should work."""
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            measures=[
                Measure(name="total", sql="amount"),
            ],
        )
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="total:sum")],
        )
        sql = await _generate(gen, query, model)
        assert "SUM" in sql
        assert "amount" in sql.lower()


class TestDialects:
    async def test_mysql_dialect(self, orders_model: SlayerModel) -> None:
        gen = SQLGenerator(dialect="mysql")
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
        )
        sql = await _generate(gen, query, orders_model)
        assert "COUNT(*)" in sql  # Basic check — dialect-specific output


class TestFields:
    async def test_arithmetic_field(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Arithmetic field generates CTE + outer SELECT."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            fields=[Field(formula="*:count"), Field(formula="revenue:sum"), Field(formula="revenue:sum / *:count", name="aov")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "base" in sql.lower()
        assert "aov" in sql.lower()
        assert "COUNT(*)" in sql
        assert "SUM(" in sql

    async def test_no_fields_no_cte(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Without fields, no CTE is generated."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "WITH" not in sql

    async def test_field_with_limit(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """LIMIT applies to the outer query, not the CTE."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count"), Field(formula="revenue:sum"), Field(formula="revenue:sum / *:count", name="aov")],
            limit=5,
        )
        sql = await _generate(generator, query, orders_model)
        assert "LIMIT 5" in sql
        cte_end = sql.lower().index("from base")
        limit_pos = sql.upper().index("LIMIT 5")
        assert limit_pos > cte_end

    async def test_cumsum(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue:sum"), Field(formula="cumsum(revenue:sum)", name="rev_running")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "SUM(" in sql
        assert "OVER" in sql
        assert "ORDER BY" in sql
        assert "rev_running" in sql.lower()

    async def test_time_shift_row_based(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue:sum"), Field(formula="time_shift(revenue:sum, -1)", name="rev_prev")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "shifted_" in sql
        assert "LEFT JOIN" in sql
        assert "ROW_NUMBER()" in sql
        assert "_rn" in sql

    async def test_lag(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue:sum"), Field(formula="lag(revenue:sum, 1)", name="rev_prev")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "LAG(" in sql
        assert "OVER" in sql

    async def test_lead(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue:sum"), Field(formula="lead(revenue:sum, 1)", name="rev_next")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "LEAD(" in sql
        assert "OVER" in sql

    async def test_change(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue:sum"), Field(formula="change(revenue:sum)", name="rev_change")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "shifted_" in sql
        assert "LEFT JOIN" in sql
        assert "_rn" in sql
        # change = current - previous (self-join column expression)
        assert " - shifted_" in sql

    async def test_change_pct(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue:sum"), Field(formula="change_pct(revenue:sum)", name="rev_pct")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "shifted_" in sql
        assert "LEFT JOIN" in sql
        assert "CASE" in sql

    async def test_rank(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            fields=[Field(formula="revenue:sum"), Field(formula="rank(revenue:sum)", name="rev_rank")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "RANK()" in sql
        assert "OVER" in sql

    async def test_last(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue:sum"), Field(formula="last(revenue:sum)", name="latest_rev")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "FIRST_VALUE(" in sql
        assert "DESC" in sql

    async def test_last_measure_type(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """A measure with last aggregation should use ROW_NUMBER + conditional aggregate."""
        orders_model.default_time_dimension = "created_at"
        orders_model.measures.append(Measure(name="balance", sql="balance"))
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="balance:last")],
        )
        sql = await _generate(generator, query, orders_model)
        # ROW_NUMBER ranked subquery for latest row per group
        assert "ROW_NUMBER()" in sql
        assert "_last_rn" in sql
        assert "DESC" in sql
        # Conditional aggregate: MAX(CASE WHEN _last_rn = 1 THEN col END)
        assert "MAX(" in sql
        assert "CASE" in sql

    async def test_last_with_explicit_time_column(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """last(ordered_at) should ORDER BY the explicit time column, not the default."""
        orders_model.default_time_dimension = "created_at"
        orders_model.measures.append(Measure(name="balance", sql="balance"))
        orders_model.dimensions.append(Dimension(name="ordered_at", sql="ordered_at", type=DataType.TIMESTAMP))
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="balance:last(ordered_at)")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "ROW_NUMBER()" in sql
        assert "orders.ordered_at" in sql
        assert "DESC" in sql

    async def test_first_with_explicit_time_column(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """first(ordered_at) should ORDER BY the explicit time column ASC."""
        orders_model.default_time_dimension = "created_at"
        orders_model.measures.append(Measure(name="balance", sql="balance"))
        orders_model.dimensions.append(Dimension(name="ordered_at", sql="ordered_at", type=DataType.TIMESTAMP))
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="balance:first(ordered_at)")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "ROW_NUMBER()" in sql
        assert "orders.ordered_at" in sql
        assert "ASC" in sql

    async def test_multiple_last_different_time_columns(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Two last measures with different explicit time cols get separate ROW_NUMBER columns."""
        orders_model.default_time_dimension = "created_at"
        orders_model.measures.append(Measure(name="balance", sql="balance"))
        orders_model.dimensions.append(Dimension(name="ordered_at", sql="ordered_at", type=DataType.TIMESTAMP))
        orders_model.dimensions.append(Dimension(name="updated_at", sql="updated_at", type=DataType.TIMESTAMP))
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[
                Field(formula="revenue:last(ordered_at)"),
                Field(formula="balance:last(updated_at)"),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        # Two distinct ROW_NUMBER columns with different ORDER BY
        assert sql.count("ROW_NUMBER()") == 2
        assert "orders.ordered_at" in sql
        assert "orders.updated_at" in sql
        # One gets no suffix, the other gets _2
        assert "_last_rn " in sql or "_last_rn)" in sql
        assert "_last_rn_2" in sql
        # Each measure references its own rn column
        assert "CASE WHEN _last_rn =" in sql or "CASE WHEN _last_rn=" in sql
        assert "CASE WHEN _last_rn_2 =" in sql or "CASE WHEN _last_rn_2=" in sql

    async def test_mixed_explicit_and_default_time_columns(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """One last with explicit time, one last with default — separate ROW_NUMBER columns."""
        orders_model.default_time_dimension = "created_at"
        orders_model.measures.append(Measure(name="balance", sql="balance"))
        orders_model.dimensions.append(Dimension(name="ordered_at", sql="ordered_at", type=DataType.TIMESTAMP))
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[
                Field(formula="revenue:last"),
                Field(formula="balance:last(ordered_at)"),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        # Two distinct ROW_NUMBER columns
        assert sql.count("ROW_NUMBER()") == 2
        assert "orders.created_at" in sql
        assert "orders.ordered_at" in sql

    async def test_same_explicit_time_column_shared(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Two first/last measures with the same explicit time col share one ROW_NUMBER."""
        orders_model.default_time_dimension = "created_at"
        orders_model.measures.append(Measure(name="balance", sql="balance"))
        orders_model.dimensions.append(Dimension(name="ordered_at", sql="ordered_at", type=DataType.TIMESTAMP))
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[
                Field(formula="revenue:last(ordered_at)"),
                Field(formula="balance:first(ordered_at)"),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        # One time column = one _last_rn and one _first_rn (no suffix)
        assert "_last_rn_2" not in sql
        assert "_first_rn_2" not in sql
        assert "_last_rn" in sql
        assert "_first_rn" in sql
        assert "DESC" in sql
        assert "ASC" in sql

    async def test_time_shift(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue:sum"), Field(formula="time_shift(revenue:sum, -1, 'year')", name="rev_prev_year")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "shifted_" in sql
        assert "LEFT JOIN" in sql
        assert "INTERVAL" in sql

    async def test_time_shift_shifted_date_range(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
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
            fields=[Field(formula="revenue:sum"), Field(formula="time_shift(revenue:sum, -1, 'month')", name="rev_prev")],
        )
        sql = await _generate(generator, query, orders_model)
        # Base CTE should have original date range
        assert "2024-03-01" in sql
        assert "2024-03-31" in sql
        # Shifted CTE should have date range shifted back by 1 month
        assert "2024-02-01" in sql
        assert "2024-02-29" in sql

    async def test_time_shift_yoy_shifted_date_range(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
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
            fields=[Field(formula="revenue:sum"), Field(formula="time_shift(revenue:sum, -1, 'year')", name="rev_yoy")],
        )
        sql = await _generate(generator, query, orders_model)
        # Shifted CTE should query March 2023
        assert "2023-03-01" in sql
        assert "2023-03-31" in sql

    async def test_change_shifted_date_range(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
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
            fields=[Field(formula="revenue:sum"), Field(formula="change(revenue:sum)", name="rev_change")],
        )
        sql = await _generate(generator, query, orders_model)
        # change looks back 1 period — shifted CTE should query February
        assert "2024-02-01" in sql
        assert "2024-02-29" in sql

    async def test_no_date_range_no_shift(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Without a date_range, shifted CTE should still be a valid base query (no date filter)."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue:sum"), Field(formula="time_shift(revenue:sum, -1, 'month')", name="rev_prev")],
        )
        sql = await _generate(generator, query, orders_model)
        # Both base and shifted CTEs should query the source table without date filters
        assert "shifted_base_" in sql
        assert "BETWEEN" not in sql

    async def test_forward_time_shift_with_date_range(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
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
            fields=[Field(formula="revenue:sum"), Field(formula="time_shift(revenue:sum, 1, 'month')", name="rev_next")],
        )
        sql = await _generate(generator, query, orders_model)
        # Shifted CTE should query April (1 month forward)
        assert "2024-04-01" in sql
        assert "2024-04-30" in sql

    async def test_quarter_date_shift(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
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
            fields=[Field(formula="revenue:sum"), Field(formula="time_shift(revenue:sum, -1, 'quarter')", name="prev_q")],
        )
        sql = await _generate(generator, query, orders_model)
        # Q3 2024 shifted back 1 quarter = Q2 2024
        assert "2024-04-01" in sql
        assert "2024-06-30" in sql

    async def test_nested_self_join_raises(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Nesting self-join transforms (e.g., change(time_shift(x))) should raise."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue:sum"), Field(formula="change(time_shift(revenue:sum, -1, 'year'))", name="x")],
        )
        with pytest.raises(ValueError, match="Nesting.*not supported"):
            await _generate(generator, query, orders_model)

    async def test_post_filter_on_computed_column(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Filters on computed columns should be applied as post-filter wrapper."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue:sum"), Field(formula="change(revenue:sum)", name="rev_change")],
            filters=["rev_change < 0"],
        )
        sql = await _generate(generator, query, orders_model)
        # Should wrap in a post-filter SELECT
        assert "_filtered" in sql
        assert '"orders.rev_change" < 0' in sql

    async def test_inline_transform_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Transform expressions in filters should be auto-extracted as hidden fields."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue:sum")],
            filters=["last(change(revenue:sum)) < 0"],
        )
        sql = await _generate(generator, query, orders_model)
        # Should have the hidden transform columns
        assert "FIRST_VALUE" in sql  # last()
        assert "shifted_" in sql  # change() via self-join
        # Should have post-filter wrapper
        assert "_filtered" in sql
        assert "< 0" in sql

    async def test_mixed_base_and_post_filters(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Base filters and post-filters should coexist correctly."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue:sum"), Field(formula="change(revenue:sum)", name="rev_change")],
            filters=["status == 'completed'", "rev_change > 0"],
        )
        sql = await _generate(generator, query, orders_model)
        # Base filter should be in the inner WHERE
        assert "'completed'" in sql
        # Post-filter should be in the outer wrapper
        assert '"orders.rev_change" > 0' in sql
        assert "_filtered" in sql

    async def test_transform_without_time_raises(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Transforms requiring time should fail if no time dimension available."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="revenue:sum"), Field(formula="cumsum(revenue:sum)", name="x")],
        )
        with pytest.raises(ValueError, match="requires a time dimension"):
            await _generate(generator, query, orders_model)

    async def test_default_time_dimension_fallback(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Model's default_time_dimension should be used when query has no time_dimensions."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="revenue:sum"), Field(formula="cumsum(revenue:sum)", name="x")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "OVER" in sql

    async def test_field_plain_measure(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "COUNT(*)" in sql

    async def test_field_auto_adds_measures(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Fields referencing measures auto-add them to the base query."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count"), Field(formula="revenue:sum"), Field(formula="revenue:sum / *:count", name="aov")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "aov" in sql.lower()
        assert "WITH" in sql

    async def test_field_mixed_with_measures(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Fields can be used alongside explicit measures."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count"), Field(formula="revenue:sum"), Field(formula="revenue:sum / *:count", name="aov")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "COUNT(*)" in sql
        assert "SUM(" in sql
        assert "aov" in sql.lower()


class TestNestedFields:
    async def test_nested_transform_generates_stacked_ctes(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """change(cumsum(revenue)) should produce stacked CTEs."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[
                Field(formula="revenue:sum"),
                Field(formula="change(cumsum(revenue:sum))", name="delta"),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        # Should have base + at least one step CTE
        assert "base" in sql.lower()
        assert "step" in sql.lower()
        assert "SUM(" in sql  # cumsum
        assert "shifted_" in sql  # change uses self-join
        assert "delta" in sql.lower()

    async def test_mixed_arithmetic_with_transform(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """cumsum(revenue) / count should work."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[
                Field(formula="*:count"),
                Field(formula="revenue:sum"),
                Field(formula="cumsum(revenue:sum) / *:count", name="avg_cumsum"),
            ],
        )
        sql = await _generate(generator, query, orders_model)
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
                Measure(name="revenue", sql="amount"),
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
    async def test_basic_query(self, dialect: str, orders_model: SlayerModel) -> None:
        """Basic aggregation query should generate valid SQL for every dialect."""
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count"), Field(formula="revenue:sum")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = await _generate(gen, query, orders_model)
        assert "COUNT(" in sql
        assert "SUM(" in sql

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    async def test_date_trunc(self, dialect: str, orders_model: SlayerModel) -> None:
        """DATE_TRUNC should produce valid output for every dialect."""
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
        )
        sql = await _generate(gen, query, orders_model)
        assert "COUNT(" in sql
        # Each dialect uses its own truncation function
        sql_upper = sql.upper()
        assert any(fn in sql_upper for fn in ["DATE_TRUNC", "STRFTIME", "TRUNC", "STR_TO_DATE"])

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    async def test_calendar_time_shift(self, dialect: str, orders_model: SlayerModel) -> None:
        """Calendar-based time_shift should produce dialect-appropriate date arithmetic in shifted CTE."""
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            fields=[Field(formula="revenue:sum"), Field(formula="time_shift(revenue:sum, -1, 'year')", name="rev_prev_year")],
        )
        sql = await _generate(gen, query, orders_model)
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
    """Test that __-delimited path aliases in inline SQL cause multi-hop join inference via graph walk."""

    @pytest.fixture
    async def storage(self, tmp_path):
        from slayer.storage.yaml_storage import YAMLStorage
        s = YAMLStorage(base_dir=str(tmp_path))
        await s.save_model(SlayerModel(
            name="regions", sql_table="regions", data_source="test",
            dimensions=[
                Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Dimension(name="name", sql="name", type=DataType.STRING),
                Dimension(name="population", sql="population", type=DataType.NUMBER),
            ],
        ))
        await s.save_model(SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            dimensions=[
                Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Dimension(name="region_id", sql="region_id", type=DataType.NUMBER),
            ],
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        ))
        return s

    @pytest.fixture
    def chained_model(self) -> SlayerModel:
        """Model with orders → customers (direct) and customers → regions (on customers)."""
        return SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            dimensions=[
                Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Dimension(name="customer_id", sql="customer_id", type=DataType.NUMBER),
                Dimension(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Dimension(
                    name="is_us",
                    sql="CASE WHEN customers__regions.name = 'US' THEN 1 ELSE 0 END",
                    type=DataType.NUMBER,
                ),
            ],
            measures=[],
            joins=[
                ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ],
        )

    @pytest.fixture
    def engine(self, storage) -> SlayerQueryEngine:
        return SlayerQueryEngine(storage=storage)

    async def test_dimension_sql_with_path_alias_infers_joins(
        self, engine: SlayerQueryEngine, chained_model: SlayerModel
    ) -> None:
        """Inline dimension SQL like 'customers__regions.name' should infer joins for both tables."""
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="*:count")],
            dimensions=[ColumnRef(name="is_us")],
        )
        enriched = await engine._enrich(query=query, model=chained_model)
        join_aliases = {alias for _, alias, _ in enriched.resolved_joins}
        assert "customers" in join_aliases
        assert "customers__regions" in join_aliases

    async def test_time_dimension_sql_with_path_alias_infers_joins(self, storage) -> None:
        """Inline time dimension SQL referencing path alias should also trigger join inference."""
        await storage.save_model(SlayerModel(
            name="orgs", sql_table="orgs", data_source="test",
            dimensions=[
                Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Dimension(name="signup_date", sql="signup_date", type=DataType.TIMESTAMP),
            ],
        ))
        await storage.save_model(SlayerModel(
            name="users", sql_table="users", data_source="test",
            dimensions=[
                Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Dimension(name="org_id", sql="org_id", type=DataType.NUMBER),
            ],
            joins=[ModelJoin(target_model="orgs", join_pairs=[["org_id", "id"]])],
        ))
        model = SlayerModel(
            name="events",
            sql_table="events",
            data_source="test",
            dimensions=[
                Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Dimension(name="user_id", sql="user_id", type=DataType.NUMBER),
                Dimension(
                    name="user_signup_date",
                    sql="users__orgs.signup_date",
                    type=DataType.TIMESTAMP,
                ),
            ],
            measures=[],
            joins=[
                ModelJoin(target_model="users", join_pairs=[["user_id", "id"]]),
            ],
        )
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(
            source_model="events",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="user_signup_date"),
                    granularity=TimeGranularity.MONTH,
                )
            ],
            fields=[Field(formula="*:count")],
        )
        enriched = await engine._enrich(query=query, model=model)
        join_aliases = {alias for _, alias, _ in enriched.resolved_joins}
        assert "users" in join_aliases
        assert "users__orgs" in join_aliases

    async def test_measure_sql_with_path_alias_infers_joins(
        self, engine: SlayerQueryEngine, chained_model: SlayerModel
    ) -> None:
        """Measure SQL like 'customers__regions.population' should infer joins for both tables."""
        chained_model.measures.append(
            Measure(name="region_pop_sum", sql="customers__regions.population")
        )
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="region_pop_sum:sum")],
        )
        enriched = await engine._enrich(query=query, model=chained_model)
        join_aliases = {alias for _, alias, _ in enriched.resolved_joins}
        assert "customers" in join_aliases
        assert "customers__regions" in join_aliases


class TestAggParamSanitization:
    """Tests for SQL injection prevention in aggregation parameter values."""

    @pytest.fixture
    def agg_model(self) -> SlayerModel:
        return SlayerModel(
            name="sales",
            sql_table="public.sales",
            data_source="test",
            dimensions=[
                Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Dimension(name="region", sql="region", type=DataType.STRING),
            ],
            measures=[
                Measure(name="price", sql="price"),
                Measure(name="revenue", sql="amount"),
                Measure(name="quantity", sql="quantity"),
            ],
        )

    @pytest.fixture
    def gen(self) -> SQLGenerator:
        return SQLGenerator(dialect="postgres")

    async def test_weighted_avg_valid_column_param(self, gen: SQLGenerator, agg_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="sales",
            fields=[Field(formula="price:weighted_avg(weight=quantity)")],
        )
        sql = await _generate(gen, query, agg_model)
        assert "SUM(" in sql
        assert "NULLIF(" in sql

    async def test_percentile_valid_numeric_param(self, gen: SQLGenerator, agg_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="sales",
            fields=[Field(formula="revenue:percentile(p=0.95)")],
        )
        sql = await _generate(gen, query, agg_model)
        assert "PERCENTILE_CONT" in sql
        assert "0.95" in sql

    async def test_qualified_column_param(self, gen: SQLGenerator, agg_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="sales",
            fields=[Field(formula="price:weighted_avg(weight=sales.quantity)")],
        )
        sql = await _generate(gen, query, agg_model)
        assert "SUM(" in sql

    def test_sql_injection_semicolon_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unsafe value"):
            _validate_agg_param_value("quantity); DROP TABLE orders; --", "weight", "weighted_avg")

    def test_sql_injection_union_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unsafe value"):
            _validate_agg_param_value("1 UNION SELECT * FROM users", "weight", "weighted_avg")

    def test_sql_injection_subquery_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unsafe value"):
            _validate_agg_param_value("(SELECT password FROM users LIMIT 1)", "weight", "weighted_avg")

    def test_sql_injection_function_call_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unsafe value"):
            _validate_agg_param_value("pg_sleep(10)", "weight", "weighted_avg")

    def test_empty_param_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unsafe value"):
            _validate_agg_param_value("", "weight", "weighted_avg")

    async def test_model_level_defaults_not_validated(self, gen: SQLGenerator, agg_model: SlayerModel) -> None:
        """Model-level aggregation param defaults (trusted) bypass query-time validation."""
        agg_model.aggregations = [
            Aggregation(
                name="custom_weighted",
                formula="SUM({value} * {weight}) / NULLIF(SUM({weight}), 0)",
                params=[
                    AggregationParam(name="weight", sql="CASE WHEN quantity > 0 THEN quantity ELSE 0 END"),
                ],
            ),
        ]
        query = SlayerQuery(
            source_model="sales",
            fields=[Field(formula="price:custom_weighted")],
        )
        # Should succeed — model-level defaults are trusted
        sql = await _generate(gen, query, agg_model)
        assert "CASE WHEN" in sql
        assert "SUM(" in sql

    def test_injection_via_direct_enriched_measure(self, gen: SQLGenerator) -> None:
        """Malicious agg_kwargs on a directly constructed EnrichedMeasure are rejected."""
        enriched = EnrichedQuery(
            model_name="sales",
            sql_table="public.sales",
            measures=[
                EnrichedMeasure(
                    name="price_weighted_avg",
                    sql="price",
                    aggregation="weighted_avg",
                    alias="sales.price_weighted_avg",
                    model_name="sales",
                    agg_kwargs={"weight": "quantity); DROP TABLE orders; --"},
                )
            ],
        )
        with pytest.raises(ValueError, match="Unsafe value"):
            gen.generate(enriched=enriched)


class TestFilteredMeasures:
    """Tests for measure-level filter (CASE WHEN wrapping)."""

    async def test_filtered_sum(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.measures.append(
            Measure(name="active_revenue", sql="amount", filter="status = 'active'")
        )
        query = SlayerQuery(source_model="orders", fields=[Field(formula="active_revenue:sum")])
        sql = await _generate(generator, query, orders_model)
        assert "CASE WHEN" in sql
        assert "THEN" in sql
        assert "SUM(" in sql

    async def test_filtered_count_star(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """COUNT(*) with filter becomes COUNT(CASE WHEN filter THEN 1 END)."""
        orders_model.measures.append(
            Measure(name="active_count", sql=None, filter="status = 'active'")
        )
        query = SlayerQuery(source_model="orders", fields=[Field(formula="active_count:count")])
        sql = await _generate(generator, query, orders_model)
        assert "CASE WHEN" in sql
        assert "THEN 1" in sql
        assert "COUNT(" in sql
        # Should NOT be COUNT(*)
        assert "COUNT(*)" not in sql

    async def test_filtered_avg(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.measures.append(
            Measure(name="active_avg", sql="amount", filter="status = 'active'")
        )
        query = SlayerQuery(source_model="orders", fields=[Field(formula="active_avg:avg")])
        sql = await _generate(generator, query, orders_model)
        assert "CASE WHEN" in sql
        assert "AVG(" in sql

    async def test_unfiltered_measure_no_case(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Measures without filter should not have CASE WHEN."""
        query = SlayerQuery(source_model="orders", fields=[Field(formula="revenue:sum")])
        sql = await _generate(generator, query, orders_model)
        assert "CASE WHEN" not in sql
        assert "SUM(" in sql

    async def test_filtered_weighted_avg_filters_both_terms(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Regression for CodeRabbit #10 — weighted_avg on a filtered measure must
        filter BOTH the numerator and the denominator. Otherwise SUM({weight})
        in the denominator sums all weights regardless of filter, producing a
        wrong (under-weighted) result."""
        orders_model.dimensions.append(
            Dimension(name="quantity", sql="quantity", type=DataType.NUMBER)
        )
        orders_model.measures.append(
            Measure(name="active_revenue", sql="amount", filter="status = 'active'")
        )
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="active_revenue:weighted_avg(weight=quantity)")],
        )
        sql = await _generate(generator, query, orders_model)
        # Both the value (amount) and the weight (quantity) must be inside CASE WHEN.
        # Two SUM calls; both should reference the filter.
        assert sql.count("CASE WHEN") >= 2, f"Expected >=2 CASE WHEN, got: {sql}"
        # Denominator must NOT be a bare SUM(quantity) — that would be the bug.
        # Check that quantity appears inside a CASE WHEN context, not as a bare SUM arg.
        assert "SUM(quantity)" not in sql, (
            f"Bare SUM(quantity) leaks unfiltered weights into denominator: {sql}"
        )

    async def test_mixed_filtered_and_unfiltered(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Query with both filtered and unfiltered measures."""
        orders_model.measures.append(
            Measure(name="active_revenue", sql="amount", filter="status = 'active'")
        )
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="revenue:sum"), Field(formula="active_revenue:sum")],
        )
        sql = await _generate(generator, query, orders_model)
        # Should have one CASE WHEN (for active_revenue) and one plain SUM (for revenue)
        assert sql.count("CASE WHEN") == 1
        assert sql.count("SUM(") == 2

    async def test_filtered_last_generates_dedicated_rn(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Filtered last measure generates a dedicated ROW_NUMBER with filter in ORDER BY."""
        orders_model.default_time_dimension = "created_at"
        orders_model.measures.append(
            Measure(name="completed_balance", sql="amount", filter="status = 'completed'")
        )
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
            fields=[Field(formula="completed_balance:last")],
        )
        sql = await _generate(generator, query, orders_model)
        # Should have a dedicated filtered ROW_NUMBER column
        assert "_last_rn_f0" in sql
        # The ORDER BY should include CASE WHEN filter THEN 0 ELSE 1 END
        assert "CASE WHEN" in sql
        assert "THEN 0 ELSE 1" in sql
        # Standard ROW_NUMBER should NOT be present (no unfiltered first/last).
        # Use regex word-boundary to avoid the obvious overlap with "_last_rn_f0".
        import re as _re
        assert _re.search(r"_last_rn(?!_f)", sql) is None, (
            f"Bare _last_rn alias should not leak into SQL when only filtered "
            f"first/last is requested: {sql}"
        )

    async def test_filtered_first_generates_dedicated_rn(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Filtered first measure generates a dedicated ROW_NUMBER with filter in ORDER BY."""
        orders_model.default_time_dimension = "created_at"
        orders_model.measures.append(
            Measure(name="completed_balance", sql="amount", filter="status = 'completed'")
        )
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
            fields=[Field(formula="completed_balance:first")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "_first_rn_f0" in sql
        assert "ASC" in sql
        assert "CASE WHEN" in sql
        assert "THEN 0 ELSE 1" in sql

    async def test_unfiltered_last_unchanged(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Unfiltered last measure uses the shared ROW_NUMBER, no _rn_f columns."""
        orders_model.default_time_dimension = "created_at"
        orders_model.measures.append(Measure(name="balance", sql="amount"))
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
            fields=[Field(formula="balance:last")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "_last_rn" in sql
        assert "_last_rn_f" not in sql

    async def test_mixed_filtered_and_unfiltered_last(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Both filtered and unfiltered last measures get separate ROW_NUMBER columns."""
        orders_model.default_time_dimension = "created_at"
        orders_model.measures.append(Measure(name="balance", sql="amount"))
        orders_model.measures.append(
            Measure(name="completed_balance", sql="amount", filter="status = 'completed'")
        )
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
            fields=[
                Field(formula="balance:last"),
                Field(formula="completed_balance:last"),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        # Should have both the shared _last_rn and the filtered _last_rn_f0
        assert "_last_rn" in sql
        assert "_last_rn_f0" in sql

    async def test_filtered_last_with_cross_model_filter_carries_join(
        self, generator: SQLGenerator,
    ) -> None:
        """Regression for CodeRabbit #8 — when a filtered last measure's filter
        references a column on a JOINED model, the LEFT JOIN must be applied
        INSIDE the ranked subquery so the filter columns resolve. Previously
        _build_last_ranked_from() built the subquery from base_from only and
        the outer string-level join injection never matched the subquery wrapper."""
        from slayer.engine.enrichment import enrich_query

        customers = SlayerModel(
            name="customers",
            sql_table="public.customers",
            data_source="test",
            dimensions=[
                Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Dimension(name="status", sql="status", type=DataType.STRING),
            ],
        )
        orders = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            dimensions=[
                Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Dimension(name="customer_id", sql="customer_id", type=DataType.NUMBER),
                Dimension(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            ],
            measures=[
                Measure(
                    name="active_balance",
                    sql="amount",
                    filter="customers.status = 'active'",
                ),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
            default_time_dimension="created_at",
        )

        async def resolve_join_target(*, target_model_name, named_queries):
            if target_model_name == "customers":
                return ("public.customers", customers)
            return None

        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
            fields=[Field(formula="active_balance:last")],
        )
        enriched = await enrich_query(
            query=query,
            model=orders,
            resolve_dimension_via_joins=_noop_async,
            resolve_cross_model_measure=_noop_async,
            resolve_join_target=resolve_join_target,
        )
        sql = generator.generate(enriched=enriched)
        # The customers LEFT JOIN must be inside the ranked subquery so the
        # filter customers.status = 'active' resolves. Extract the subquery
        # by matching balanced parens after `FROM (`.
        sub_start = sql.find("FROM (") + len("FROM (")
        depth = 1
        pos = sub_start
        while pos < len(sql) and depth > 0:
            if sql[pos] == "(":
                depth += 1
            elif sql[pos] == ")":
                depth -= 1
            pos += 1
        subquery_chunk = sql[sub_start:pos]
        assert "LEFT JOIN public.customers" in subquery_chunk, (
            f"Expected LEFT JOIN inside ranked subquery; got: {sql}"
        )
        # And the outer query should NOT also add a LEFT JOIN at the top level
        # (would double-join). Count: only one occurrence of the join clause.
        assert sql.count("LEFT JOIN public.customers") == 1

    async def test_filtered_last_outer_aggregate_uses_match_flag_not_joined_filter(
        self, generator: SQLGenerator,
    ) -> None:
        """Regression for CodeRabbit B6-4 — when a filtered first/last measure's
        filter references a JOINED table (e.g. customers.status), the outer
        aggregate must reference a per-measure boolean match flag projected by
        the ranked subquery, NOT re-emit the original filter_sql. The outer FROM
        is the ranked subquery alias, which only projects model.*, _td_*,
        _rn*, and _match_*. Re-emitting `customers.status = 'active'` outside
        that subquery references a table that isn't in scope → invalid SQL."""
        from slayer.engine.enrichment import enrich_query

        customers = SlayerModel(
            name="customers",
            sql_table="public.customers",
            data_source="test",
            dimensions=[
                Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Dimension(name="status", sql="status", type=DataType.STRING),
            ],
        )
        orders = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            dimensions=[
                Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Dimension(name="customer_id", sql="customer_id", type=DataType.NUMBER),
                Dimension(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            ],
            measures=[
                Measure(
                    name="active_balance",
                    sql="amount",
                    filter="customers.status = 'active'",
                ),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
            default_time_dimension="created_at",
        )

        async def resolve_join_target(*, target_model_name, named_queries):
            if target_model_name == "customers":
                return ("public.customers", customers)
            return None

        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
            fields=[Field(formula="active_balance:last")],
        )
        enriched = await enrich_query(
            query=query,
            model=orders,
            resolve_dimension_via_joins=_noop_async,
            resolve_cross_model_measure=_noop_async,
            resolve_join_target=resolve_join_target,
        )
        sql = generator.generate(enriched=enriched)

        # Extract the OUTER part of the SQL (everything after the closing
        # paren of the ranked subquery). That section selects from the
        # subquery alias, so 'customers' is not in scope there.
        sub_start = sql.find("FROM (") + len("FROM (")
        depth = 1
        pos = sub_start
        while pos < len(sql) and depth > 0:
            if sql[pos] == "(":
                depth += 1
            elif sql[pos] == ")":
                depth -= 1
            pos += 1
        outer_part = sql[pos:]  # everything after the ) AS orders

        assert "customers." not in outer_part, (
            f"Outer aggregate references joined table 'customers' which is "
            f"not in scope outside the ranked subquery — would generate "
            f"invalid SQL. Outer part:\n{outer_part}\n\nFull SQL:\n{sql}"
        )
        # Sanity: the match flag _match_f0 should be projected by the
        # subquery and tested by the outer MAX(CASE WHEN ...)
        assert "_match_f0" in sql

    async def test_filter_with_dotted_string_literal_does_not_pull_spurious_join(
        self, generator: SQLGenerator,
    ) -> None:
        """Regression for CodeRabbit #6 — when a measure filter contains a string
        literal that happens to include a dot (e.g. "url LIKE 'foo.bar%'"), the
        join planner must NOT mistake the literal for a `customers.<col>` ref
        and pull in an unwanted LEFT JOIN. The structured filter_columns from
        ParsedFilter only lists real column references."""
        from slayer.engine.enrichment import enrich_query

        # Tracker: was resolve_join_target asked about 'foo'? If so, the regex
        # path leaked. With structured filter_columns it should never be queried.
        join_target_lookups: list = []

        async def resolve_join_target(*, target_model_name, named_queries):
            join_target_lookups.append(target_model_name)
            return None

        # Inline ModelJoin to a 'foo' model that resolve_join_target doesn't know.
        # Without the fix, the regex pattern foo.bar inside a string literal
        # would match _TABLE_COL_RE and add 'foo' to needed_tables.
        orders = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            dimensions=[
                Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Dimension(name="url", sql="url", type=DataType.STRING),
            ],
            measures=[
                Measure(
                    name="vendor_revenue",
                    sql="amount",
                    # The dot inside the literal is what would trip the regex.
                    filter="url LIKE 'foo.bar%'",
                ),
            ],
            joins=[ModelJoin(target_model="foo", join_pairs=[["id", "id"]])],
        )
        query = SlayerQuery(
            source_model="orders",
            fields=[Field(formula="vendor_revenue:sum")],
        )
        enriched = await enrich_query(
            query=query,
            model=orders,
            resolve_dimension_via_joins=_noop_async,
            resolve_cross_model_measure=_noop_async,
            resolve_join_target=resolve_join_target,
        )
        # The 'foo' join must NOT have been pulled in.
        assert "foo" not in join_target_lookups, (
            f"Spurious join planning for 'foo' triggered by dotted string "
            f"literal in filter; lookups: {join_target_lookups}"
        )
        join_aliases = {alias for _, alias, _ in enriched.resolved_joins}
        assert "foo" not in join_aliases
        # And confirm the SQL never gets a LEFT JOIN we didn't ask for.
        sql = generator.generate(enriched=enriched)
        assert "LEFT JOIN" not in sql

    async def test_two_filtered_lasts_same_source_different_filters_dont_collide(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Regression for CodeRabbit #9 — two filtered last measures backed by the
        same source measure+agg but with different filters must each get their own
        ROW_NUMBER column. Previously the map was keyed by source_measure:agg so
        the second one clobbered the first and both pointed at the same _rn alias."""
        orders_model.default_time_dimension = "created_at"
        orders_model.measures.append(
            Measure(name="active_balance", sql="amount", filter="status = 'active'")
        )
        orders_model.measures.append(
            Measure(name="completed_balance", sql="amount", filter="status = 'completed'")
        )
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
            fields=[
                Field(formula="active_balance:last"),
                Field(formula="completed_balance:last"),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        # Two distinct filtered ROW_NUMBER columns must exist in the ranked CTE.
        assert "_last_rn_f0" in sql
        assert "_last_rn_f1" in sql
        # Both filter conditions must appear inside their CASE WHEN ORDER BY.
        assert "status = 'active'" in sql
        assert "status = 'completed'" in sql

    async def test_filtered_measure_uses_source_alias_not_model_name(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Regression for CodeRabbit #7 — filter columns must be qualified with the
        source alias (model_name_str) and not the underlying model.name when the
        query's source_model string differs from model.name (e.g., named queries
        / sub-query sources)."""
        from slayer.engine.enrichment import enrich_query

        orders_model.measures.append(
            Measure(name="active_revenue", sql="amount", filter="status = 'active'")
        )
        # Underlying model loaded under a different name than the query references.
        underlying = orders_model.model_copy(update={"name": "orders_underlying"})
        query = SlayerQuery(
            source_model="orders_alias",
            fields=[Field(formula="active_revenue:sum")],
        )
        enriched = await enrich_query(
            query=query,
            model=underlying,
            resolve_dimension_via_joins=_noop_async,
            resolve_cross_model_measure=_noop_async,
            resolve_join_target=_noop_async,
        )
        measure = next(
            m for m in enriched.measures if m.source_measure_name == "active_revenue"
        )
        assert measure.filter_sql is not None
        assert "orders_alias.status" in measure.filter_sql
        assert "orders_underlying" not in measure.filter_sql

    async def test_filtered_measure_source_alias_propagates_to_generated_sql(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Regression for CodeRabbit B6-3 — extends the test above to assert
        on the *generated SQL*. Previously, even though filter_sql was resolved
        against the source alias, the EnrichedMeasure.model_name (and the
        EnrichedQuery.model_name driving the FROM clause) still used model.name.
        The generated SQL ended up with `CASE WHEN orders_alias.status THEN
        orders_underlying.amount END` from `FROM ... AS orders_underlying` —
        invalid because the source alias isn't in the FROM."""
        from slayer.engine.enrichment import enrich_query

        orders_model.measures.append(
            Measure(name="active_revenue", sql="amount", filter="status = 'active'")
        )
        underlying = orders_model.model_copy(update={"name": "orders_underlying"})
        query = SlayerQuery(
            source_model="orders_alias",
            fields=[Field(formula="active_revenue:sum")],
        )
        enriched = await enrich_query(
            query=query,
            model=underlying,
            resolve_dimension_via_joins=_noop_async,
            resolve_cross_model_measure=_noop_async,
            resolve_join_target=_noop_async,
        )
        sql = generator.generate(enriched=enriched)
        # The generated SQL must use the source alias consistently — never
        # the underlying model name. If even one site still uses model.name,
        # this assertion catches it.
        assert "orders_underlying" not in sql, (
            f"Underlying model.name leaked into generated SQL alongside source "
            f"alias 'orders_alias' — would produce invalid SQL. SQL:\n{sql}"
        )
        # Sanity: the source alias should appear at least in the FROM and the filter.
        assert "orders_alias" in sql


class TestMeasureFilterInjection:
    """End-to-end SQL-injection hardening for the ``Measure.filter`` field.

    The filter string is the only user-authored SQL fragment that gets
    interpolated into the generated query (everything else goes through
    sqlglot AST builders). These tests run the full enrichment + generation
    pipeline for each payload and verify the resulting SQL is safe across
    both standard-SQL and escape-sensitive dialects.

    Any payload the parser rejects at the ``parse_filter`` stage raises a
    ``ValueError`` — those cases assert the raise, not the output. Payloads
    the parser accepts must produce SQL in which the payload appears only
    inside a properly-closed string literal.
    """

    # ------------------------------------------------------------------
    # Rejected at parse time
    # ------------------------------------------------------------------

    async def test_drop_table_rejected(self, orders_model: SlayerModel) -> None:
        """Classic ``'; DROP TABLE ...`` payload is rejected before generation."""
        orders_model.measures.append(
            Measure(
                name="evil",
                sql="amount",
                filter="status = 'a'; DROP TABLE orders; --'",
            )
        )
        query = SlayerQuery(source_model="orders", fields=[Field(formula="evil:sum")])
        with pytest.raises(ValueError, match="Invalid filter syntax"):
            await _generate(SQLGenerator(dialect="postgres"), query, orders_model)

    async def test_union_select_rejected(self, orders_model: SlayerModel) -> None:
        """UNION SELECT payload is rejected before generation."""
        orders_model.measures.append(
            Measure(
                name="evil",
                sql="amount",
                filter="status = 'a' UNION SELECT * FROM users --'",
            )
        )
        query = SlayerQuery(source_model="orders", fields=[Field(formula="evil:sum")])
        with pytest.raises(ValueError, match="Invalid filter syntax"):
            await _generate(SQLGenerator(dialect="postgres"), query, orders_model)

    async def test_block_comment_rejected(self, orders_model: SlayerModel) -> None:
        """``/* ... */`` comment injection is rejected before generation."""
        orders_model.measures.append(
            Measure(
                name="evil",
                sql="amount",
                filter="status = 'a' /* x */ OR 1=1",
            )
        )
        query = SlayerQuery(source_model="orders", fields=[Field(formula="evil:sum")])
        with pytest.raises(ValueError, match="Invalid filter syntax"):
            await _generate(SQLGenerator(dialect="postgres"), query, orders_model)

    # ------------------------------------------------------------------
    # Accepted and neutralised in emitted SQL — tested across dialects
    # ------------------------------------------------------------------

    @pytest.mark.parametrize("dialect", ["postgres", "mysql", "sqlite", "duckdb"])
    async def test_embedded_single_quote_is_doubled(
        self, orders_model: SlayerModel, dialect: str,
    ) -> None:
        """An apostrophe in the filter value must emit as ``''`` (SQL standard).

        This holds for every dialect; none of them accept ``\\'`` as the
        canonical escape for a literal apostrophe.
        """
        orders_model.measures.append(
            Measure(
                name="irish_names",
                sql="amount",
                # Runtime value of the literal:  O'Brien
                filter="status = 'O\\'Brien'",
            )
        )
        query = SlayerQuery(
            source_model="orders", fields=[Field(formula="irish_names:sum")]
        )
        sql = await _generate(SQLGenerator(dialect=dialect), query, orders_model)
        # The emitted literal must use doubled single quotes.
        assert "'O''Brien'" in sql

    @staticmethod
    def _assert_round_trips_cleanly(sql: str, dialect: str) -> None:
        """Every emitted SQL string must tokenize + parse + round-trip in the
        target dialect. If a hostile filter manages to open an unclosed string
        literal, sqlglot's tokenizer raises ``TokenError`` — which is both the
        canonical pre-fix failure mode and a downstream DoS / error-leakage
        vector."""
        parsed = sqlglot.parse_one(sql, dialect=dialect)
        # Re-emitting must not raise either — guards against one-way tokenizer
        # tolerance that wouldn't survive a round-trip through the planner.
        _ = parsed.sql(dialect=dialect)

    @pytest.mark.parametrize("dialect", ["postgres", "mysql", "sqlite", "duckdb"])
    async def test_trailing_backslash_cannot_escape_closing_quote(
        self, orders_model: SlayerModel, dialect: str,
    ) -> None:
        """A trailing backslash in a string literal must not break out of the
        literal on escape-aware dialects (mysql, clickhouse, etc.).

        Before the fix: ``parse_filter`` emits ``'a\\'`` (one literal
        backslash inside single quotes). On MySQL that parses as "apostrophe
        escaped by the backslash, string still open", letting trailing SQL
        tokens be read as string content — triggering ``sqlglot.TokenError``
        (DoS / error-leakage vector). After the fix: the backslash is doubled
        in the emitted literal and sqlglot tokenizes without error.
        """
        orders_model.measures.append(
            Measure(
                name="evil",
                sql="amount",
                # Runtime filter string:  status = 'a\'
                filter="status = 'a\\\\'",
            )
        )
        query = SlayerQuery(source_model="orders", fields=[Field(formula="evil:sum")])
        sql = await _generate(SQLGenerator(dialect=dialect), query, orders_model)
        self._assert_round_trips_cleanly(sql, dialect)
        # Defence-in-depth: the payload ``a`` + trailing slash must be
        # confined to a single well-terminated literal. Check the literal
        # decodes to the original ``a\`` content after the dialect's own
        # unescaping — i.e. a single re-parse is idempotent.
        reparsed = sqlglot.parse_one(sql, dialect=dialect)
        rendered = reparsed.sql(dialect=dialect)
        # Round-trip stability: no additional escape inflation on the second pass.
        again = sqlglot.parse_one(rendered, dialect=dialect).sql(dialect=dialect)
        assert rendered == again, (
            f"SQL is not idempotent under re-parse on {dialect}: {rendered!r} vs {again!r}"
        )

    @pytest.mark.parametrize("dialect", ["postgres", "mysql"])
    async def test_backslash_mid_string_is_neutralised(
        self, orders_model: SlayerModel, dialect: str,
    ) -> None:
        """Backslashes mid-string also must not enable escape sequences."""
        orders_model.measures.append(
            Measure(
                name="evil",
                sql="amount",
                # Runtime filter string:  status = 'a\b'
                filter="status = 'a\\\\b'",
            )
        )
        query = SlayerQuery(source_model="orders", fields=[Field(formula="evil:sum")])
        sql = await _generate(SQLGenerator(dialect=dialect), query, orders_model)
        self._assert_round_trips_cleanly(sql, dialect)

    @pytest.mark.parametrize("dialect", ["postgres", "mysql"])
    async def test_like_pattern_backslash_is_neutralised(
        self, orders_model: SlayerModel, dialect: str,
    ) -> None:
        """The ``LIKE`` path in ``_filter_node_to_sql`` goes through a separate
        helper (``_get_string_arg``); its backslash handling must match."""
        orders_model.measures.append(
            Measure(
                name="evil",
                sql="amount",
                # Runtime filter string:  status like 'a\'
                filter="status like 'a\\\\'",
            )
        )
        query = SlayerQuery(source_model="orders", fields=[Field(formula="evil:sum")])
        sql = await _generate(SQLGenerator(dialect=dialect), query, orders_model)
        self._assert_round_trips_cleanly(sql, dialect)

    @pytest.mark.parametrize("dialect", ["postgres", "mysql"])
    async def test_adversarial_quote_break_cannot_inject(
        self, orders_model: SlayerModel, dialect: str,
    ) -> None:
        """The full attack: backslash + quote + SQL payload must either be
        rejected at parse time or confined to a string literal.

        The intent of this payload is to break out of the string in MySQL,
        then run arbitrary SQL. After the fix, this either raises at
        ``parse_filter`` (most likely) or emits a safely-terminated literal.
        """
        evil = "status = 'a\\\\' OR 1=1 --"  # Runtime: status = 'a\' OR 1=1 --
        try:
            orders_model.measures.append(Measure(name="evil", sql="amount", filter=evil))
            query = SlayerQuery(
                source_model="orders", fields=[Field(formula="evil:sum")]
            )
            sql = await _generate(SQLGenerator(dialect=dialect), query, orders_model)
        except ValueError:
            return  # parser rejected — also acceptable
        self._assert_round_trips_cleanly(sql, dialect)

    async def test_existing_filter_still_works_after_escaping(
        self, orders_model: SlayerModel,
    ) -> None:
        """Sanity: ordinary filters (no backslashes, no apostrophes) keep
        producing the same SQL shape after the escape-hardening change."""
        orders_model.measures.append(
            Measure(name="active_revenue", sql="amount", filter="status = 'active'")
        )
        query = SlayerQuery(
            source_model="orders", fields=[Field(formula="active_revenue:sum")]
        )
        sql = await _generate(SQLGenerator(dialect="postgres"), query, orders_model)
        assert "'active'" in sql
        assert "CASE WHEN" in sql
        assert "SUM(" in sql


class TestAutoMoveDimensions:
    """Test _auto_move_fields_to_dimensions preprocessing in the query engine."""

    @pytest.fixture
    def storage(self, tmp_path):
        from slayer.storage.yaml_storage import YAMLStorage

        return YAMLStorage(base_dir=str(tmp_path))

    @pytest.fixture
    async def engine_and_model(self, storage):
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            dimensions=[
                Dimension(name="status", sql="status", type=DataType.STRING),
                Dimension(name="customer_id", sql="customer_id", type=DataType.NUMBER),
            ],
            measures=[Measure(name="revenue", sql="amount")],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        customers = SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            dimensions=[
                Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Dimension(name="name", sql="name", type=DataType.STRING),
                Dimension(name="region", sql="region", type=DataType.STRING),
            ],
            measures=[],
        )
        await storage.save_model(orders)
        await storage.save_model(customers)
        engine = SlayerQueryEngine(storage=storage)
        return engine, orders

    async def test_bare_local_dimension_moved(self, engine_and_model) -> None:
        engine, model = engine_and_model
        query = SlayerQuery(source_model="orders", fields=["status", "revenue:sum"])
        result = await engine._auto_move_fields_to_dimensions(query, model, {})
        assert len(result.fields) == 1
        assert result.fields[0].formula == "revenue:sum"
        assert any(d.name == "status" for d in result.dimensions)

    async def test_cross_model_dimension_moved(self, engine_and_model) -> None:
        engine, model = engine_and_model
        query = SlayerQuery(source_model="orders", fields=["customers.name", "revenue:sum"])
        result = await engine._auto_move_fields_to_dimensions(query, model, {})
        assert len(result.fields) == 1
        assert any(d.full_name == "customers.name" for d in result.dimensions)

    async def test_colon_fields_kept(self, engine_and_model) -> None:
        engine, model = engine_and_model
        query = SlayerQuery(source_model="orders", fields=["revenue:sum", "*:count"])
        result = await engine._auto_move_fields_to_dimensions(query, model, {})
        assert len(result.fields) == 2
        assert not result.dimensions

    async def test_arithmetic_kept(self, engine_and_model) -> None:
        engine, model = engine_and_model
        query = SlayerQuery(source_model="orders", fields=["revenue:sum / *:count"])
        result = await engine._auto_move_fields_to_dimensions(query, model, {})
        assert len(result.fields) == 1

    async def test_bare_measure_name_kept(self, engine_and_model) -> None:
        engine, model = engine_and_model
        query = SlayerQuery(source_model="orders", fields=["revenue", "revenue:sum"])
        result = await engine._auto_move_fields_to_dimensions(query, model, {})
        # "revenue" is a measure, not a dimension — stays in fields
        assert len(result.fields) == 2

    async def test_unknown_bare_name_kept(self, engine_and_model) -> None:
        engine, model = engine_and_model
        query = SlayerQuery(source_model="orders", fields=["nonexistent", "revenue:sum"])
        result = await engine._auto_move_fields_to_dimensions(query, model, {})
        assert len(result.fields) == 2

    async def test_invalid_cross_model_path_kept(self, engine_and_model) -> None:
        engine, model = engine_and_model
        query = SlayerQuery(source_model="orders", fields=["customers.nonexistent", "revenue:sum"])
        result = await engine._auto_move_fields_to_dimensions(query, model, {})
        assert len(result.fields) == 2

    async def test_no_fields_noop(self, engine_and_model) -> None:
        engine, model = engine_and_model
        query = SlayerQuery(source_model="orders", dimensions=["status"])
        result = await engine._auto_move_fields_to_dimensions(query, model, {})
        assert result.fields is None

    async def test_appends_to_existing_dimensions(self, engine_and_model) -> None:
        engine, model = engine_and_model
        query = SlayerQuery(source_model="orders", fields=["customer_id", "revenue:sum"], dimensions=["status"])
        result = await engine._auto_move_fields_to_dimensions(query, model, {})
        assert len(result.fields) == 1
        dim_names = [d.name for d in result.dimensions]
        assert "status" in dim_names
        assert "customer_id" in dim_names
