"""Tests for the SQL generator."""

import re as _re
import tempfile
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sqlglot
import sqlglot.errors

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.formula import ParsedFilter
from slayer.core.models import Aggregation, AggregationParam, Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.enriched import (
    CrossModelMeasure,
    EnrichedDimension,
    EnrichedMeasure,
    EnrichedQuery,
)
from slayer.engine.enrichment import enrich_query
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.dialects import BigqueryDialect
from slayer.sql.generator import SQLGenerator, _cte_name_from_alias, _validate_agg_param_value
from slayer.storage.yaml_storage import YAMLStorage


async def _noop_async(**kw):
    return None


def _norm(s: str) -> str:
    return " ".join(s.split())


def _extract_src_body(sql: str) -> str:
    """Pull out the `_src` subquery body from a generated window-measure SQL.

    Resilient when the outer query also contains other LEFT JOIN (...) blocks
    (e.g. cross-model measure subqueries): anchors on the unique `\\n) AS _src`
    suffix and reverse-searches for the matching `LEFT JOIN (\\n` before it.
    """
    end = sql.index("\n) AS _src")
    open_token = "LEFT JOIN (\n"
    start = sql.rfind(open_token, 0, end) + len(open_token)
    return sql[start:end]


_SQLGLOT_TYPEERROR_DIALECTS = {"bigquery"}


def _assert_valid_sql(sql: str, dialect: str = "postgres"):
    """Assert generated SQL is structurally valid (parses, no nested WITH)."""
    try:
        statements = sqlglot.parse(sql, dialect=dialect)
        assert statements, f"SQL failed to parse:\n{sql}"
        assert len(statements) == 1, f"Expected 1 SQL statement, got {len(statements)}:\n{sql}"
    except TypeError as exc:
        if dialect not in _SQLGLOT_TYPEERROR_DIALECTS:
            raise AssertionError(
                f"sqlglot TypeError while validating {dialect} SQL:\n{sql}"
            ) from exc
        return  # Known sqlglot limitation for this dialect
    # No nested WITH — only one WITH keyword allowed at the start of a line
    with_lines = [line for line in sql.split("\n") if line.strip().upper().startswith("WITH ")]
    assert len(with_lines) <= 1, f"Nested WITH clauses detected:\n{sql}"


async def _generate(
    generator: SQLGenerator,
    query: SlayerQuery,
    model: SlayerModel,
) -> str:
    """Helper: enrich a query against a model, then generate SQL."""

    enriched = await enrich_query(
        query=query,
        model=model,
        resolve_dimension_via_joins=_noop_async,
        resolve_cross_model_measure=_noop_async,
        resolve_join_target=_noop_async,
    )
    sql = generator.generate(enriched=enriched)
    _assert_valid_sql(sql, dialect=generator.dialect)
    return sql


@pytest.fixture
def orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="delivery_at", sql="delivery_at", type=DataType.TIMESTAMP),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),

            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            Column(name="avg_revenue", sql="amount", type=DataType.DOUBLE),
            Column(name="distinct_customers", sql="customer_id", type=DataType.DOUBLE),
        ],
    )


@pytest.fixture
def generator() -> SQLGenerator:
    gen = SQLGenerator(dialect="postgres")
    _original = gen.generate

    def _validating_generate(enriched):
        sql = _original(enriched=enriched)
        _assert_valid_sql(sql)
        return sql

    gen.generate = _validating_generate
    return gen


class TestBasicQueries:
    async def test_numeric_literal_measure(self, generator: SQLGenerator) -> None:
        """Measures with numeric SQL expressions (e.g. dbt `expr: 1`) should generate
        SUM(1), not SUM(model."1")."""
        model = SlayerModel(
            name="policy",
            sql_table="policy",
            data_source="test",
            columns=[
                Column(name="status", type=DataType.TEXT),

                Column(name="num_policies", sql="1", allowed_aggregations=["sum"], type=DataType.DOUBLE),
            ],
        )
        query = SlayerQuery(source_model="policy", measures=[ModelMeasure(formula="num_policies:sum")])
        sql = await _generate(generator, query, model)
        # DEV-1361: a non-bare ``Column.sql`` (literal ``"1"``) is wrapped
        # in CAST when ``type`` is set, so the emission becomes
        # ``SUM(CAST(1 AS DOUBLE PRECISION))``. The original bug pinned by
        # this test — quoting ``1`` as an identifier ``"1"`` — must still
        # not happen.
        assert "SUM(CAST(1 AS" in sql or "SUM(1)" in sql
        assert '"1"' not in sql

    async def test_simple_count(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="*:count")])
        sql = await _generate(generator, query, orders_model)
        assert "COUNT(*)" in sql
        assert "public.orders" in sql

    async def test_star_rejects_non_count_aggregation(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        query = SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="*:sum")])
        with pytest.raises(ValueError, match=r"not allowed with measure '\*'"):
            await _generate(generator, query, orders_model)

    async def test_dim_only_query_deduplicates(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """A dim-only query (no measures) auto-deduplicates via GROUP BY.

        The ``GROUP BY`` must appear before ``LIMIT`` — otherwise a row
        cap can silently drop unique tuples that only surface past row N.
        """
        query = SlayerQuery(source_model="orders", dimensions=[ColumnRef(name="status")], limit=100)
        sql = await _generate(generator, query, orders_model)
        upper = sql.upper()
        assert "orders.status" in sql
        assert "GROUP BY" in upper
        assert upper.index("GROUP BY") < upper.index("LIMIT 100")

    async def test_time_dim_only_query_deduplicates(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """Time-dimension-only queries also auto-deduplicate."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        assert "GROUP BY" in sql

    async def test_dim_with_measure_emits_single_group_by(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """The dim-only path must not double-emit GROUP BY when measures aggregate."""
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="revenue:sum")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "SUM(" in sql
        assert sql.upper().count("GROUP BY") == 1

    async def test_dimension_with_measure(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="revenue:sum")],
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
            measures=[ModelMeasure(formula="*:count")],
            limit=10,
            offset=20,
        )
        sql = await _generate(generator, query, orders_model)
        assert "LIMIT 10" in sql
        assert "OFFSET 20" in sql

    async def test_order_by(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
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
            measures=[ModelMeasure(formula="*:count")],
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
            measures=[ModelMeasure(formula="*:count")],
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
            measures=[ModelMeasure(formula="*:count")],
            filters=["status == 'active'"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "WHERE" in sql
        assert "'active'" in sql

    async def test_in_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["status in ('active', 'pending')"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "IN" in sql
        assert "'active'" in sql
        assert "'pending'" in sql

    async def test_gt_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["customer_id > 100"],
        )
        sql = await _generate(generator, query, orders_model)
        assert ">" in sql
        assert "100" in sql

    async def test_contains_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["status like '%act%'"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "LIKE" in sql
        assert "%act%" in sql

    async def test_is_null_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["status IS NULL"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "IS NULL" in sql

    async def test_is_not_null_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["status IS NOT NULL"],
        )
        sql = await _generate(generator, query, orders_model)
        # Python AST may produce "NOT x IS NULL" instead of "x IS NOT NULL" — both valid
        assert "IS NOT NULL" in sql or "NOT" in sql

    async def test_is_null_python_compat(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Python-style 'is None' still works for backward compatibility."""
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["status is None"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "IS NULL" in sql

    async def test_sql_equals_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """SQL single = works as equality."""
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["status = 'active'"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "= 'active'" in sql

    async def test_sql_not_equals_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """SQL <> works as not-equals."""
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
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
            measures=[ModelMeasure(formula="*:count")],
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
            measures=[ModelMeasure(formula="*:count")],
            filters=["status = 'foo<>bar'"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "'foo<>bar'" in sql

    async def test_composite_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["status == 'active' or customer_id > 10"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "OR" in sql

    async def test_measure_filter_goes_to_having(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="revenue:sum")],
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
            columns=[
                Column(name="order_status", sql="status_col", type=DataType.TEXT),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["order_status == 'active'"],
        )
        sql = await _generate(generator, query, model)
        assert "status_col" in sql
        assert "order_status" not in sql.split("WHERE")[1]  # dimension name not in WHERE

    async def test_date_range_filter(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["created_at >= '2024-01-01' and created_at <= '2024-06-30'"],
        )
        sql = await _generate(generator, query, orders_model)
        assert ">=" in sql
        assert "<=" in sql


class TestMeasureTypes:
    async def test_count_distinct(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="distinct_customers:count_distinct")])
        sql = await _generate(generator, query, orders_model)
        assert "COUNT(DISTINCT" in sql

    async def test_average(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="avg_revenue:avg")])
        sql = await _generate(generator, query, orders_model)
        assert "AVG(" in sql

    async def test_sum(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="revenue:sum")])
        sql = await _generate(generator, query, orders_model)
        assert "SUM(" in sql


class TestSubquery:
    async def test_model_with_sql(self, generator: SQLGenerator) -> None:
        model = SlayerModel(
            name="recent_orders",
            sql="SELECT * FROM public.orders WHERE created_at > '2024-01-01'",
            data_source="test",
            columns=[Column(name="status", sql="status", type=DataType.TEXT),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        query = SlayerQuery(
            source_model="recent_orders",
            measures=[ModelMeasure(formula="revenue:sum")],
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
            columns=[
                Column(name="status", sql="status", type=DataType.TEXT),

                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
        )
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
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
            columns=[
                Column(name="total", sql="amount", type=DataType.DOUBLE),
            ],
        )
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="total:sum")],
        )
        sql = await _generate(gen, query, model)
        assert "SUM" in sql
        assert "amount" in sql.lower()


class TestFields:
    async def test_arithmetic_field(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Arithmetic field generates CTE + outer SELECT."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="revenue:sum / *:count", name="aov")],
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
            measures=[ModelMeasure(formula="*:count")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "WITH" not in sql

    async def test_field_with_limit(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """LIMIT applies to the outer query, not the CTE."""
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="revenue:sum / *:count", name="aov")],
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
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="cumsum(revenue:sum)", name="rev_running")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "SUM(" in sql
        assert "OVER" in sql
        assert "ORDER BY" in sql
        assert "rev_running" in sql.lower()

    async def test_cumsum_partitions_by_dimensions(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "cumsum(revenue:sum)", "name": "running_revenue"}],
        )
        sql = await _generate(generator=generator, query=query, model=orders_model)
        norm = _norm(sql)
        assert 'SUM("orders.revenue_sum")' in norm
        assert "OVER (" in norm
        assert 'PARTITION BY "orders.status"' in norm
        assert 'ORDER BY "orders.created_at"' in norm

    async def test_consecutive_periods_uses_reset_group_ctes(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "consecutive_periods(revenue:sum > 0)", "name": "positive_streak"}],
        )
        sql = await _generate(generator=generator, query=query, model=orders_model)
        norm = _norm(sql)
        assert "cp_reset_" in norm
        assert "cp_value_" in norm
        assert "SUM(CASE WHEN" in norm
        # Reset CTE: partition by query dim, order by query time dim.
        assert 'PARTITION BY "orders.status"' in norm
        assert 'ORDER BY "orders.created_at"' in norm
        # Value CTE: partition adds the reset-group alias.
        assert '"_cp_reset_orders__positive_streak"' in norm
        assert '"orders.positive_streak"' in norm

    async def test_consecutive_periods_no_implicit_nulls_last_sqlite(
        self, orders_model: SlayerModel,
    ) -> None:
        """Regression: sqlglot's `exp.Ordered` injects `NULLS LAST` on SQLite
        even when not requested, which would change consecutive_periods
        streak/reset semantics for any NULL time values vs. the pre-AST
        string-built `ORDER BY <t>` output. The fix is to put a bare column
        inside `exp.Order` rather than wrapping it in `exp.Ordered`.

        Caught by Codex review of PR #78. SQLite is Tier-1 in this project so
        this is a real semantic regression even though current integration
        tests don't exercise null time values.
        """
        gen = SQLGenerator(dialect="sqlite")
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "consecutive_periods(revenue:sum > 0)",
                       "name": "positive_streak"}],
        )
        sql = await _generate(generator=gen, query=query, model=orders_model)
        assert "NULLS LAST" not in sql.upper(), (
            f"sqlite consecutive_periods CTE must not emit implicit "
            f"NULLS LAST (would change streak semantics for NULL time "
            f"values).\nsql:\n{sql}"
        )

    async def test_consecutive_periods_comparison_generates_expression(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "consecutive_periods(revenue:sum > 0) >= 2", "name": "long_enough"}],
        )
        sql = await _generate(generator=generator, query=query, model=orders_model)
        norm = _norm(sql)
        assert "cp_reset_" in norm
        assert "cp_value_" in norm
        assert '>= 2' in norm
        assert '"orders.long_enough"' in norm

    async def test_windowed_sum_uses_range_join_primitive(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "revenue_90d"}],
        )
        sql = await _generate(generator=generator, query=query, model=orders_model)
        norm = _norm(sql)
        # Windowed-sum CTE name follows the measure's surfaced name; with an
        # explicit ``name="revenue_90d"`` the CTE is named after the user
        # alias (DEV-1335 — user ``name`` overrides the canonical form).
        assert "_wm_orders__revenue_90d" in norm
        assert "LEFT JOIN" in norm
        assert "_src._w_time >=" in norm
        assert "_src._w_time <" in norm
        # AST-based generation renders single-unit intervals via sqlglot's
        # per-dialect transpiler — Postgres caps the unit name.
        assert "INTERVAL '90 DAY'" in norm
        assert '_src._w_dim_0 = _base."orders.status"' in norm

    async def test_windowed_sum_preserves_other_time_dim_grain(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """With 2+ time dimensions, the windowed CTE must equality-join on every
        non-window time dim — otherwise rows from other dim values fan in."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
                TimeDimension(dimension=ColumnRef(name="delivery_at"), granularity=TimeGranularity.MONTH),
            ],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "revenue_90d"}],
        )
        sql = await _generate(generator=generator, query=query, model=orders_model)
        assert "_w_td_" in sql
        assert '_base."orders.delivery_at"' in sql

    @pytest.fixture
    async def orders_with_customers_engine(self, tmp_path):
        """Storage + engine with an orders→customers join.

        The customers model includes both name and region_id so the two
        window-CTE join-scoping regression tests below can share one fixture.
        """
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_model(SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
                Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
            ],
        ))
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        await storage.save_model(orders)
        return SlayerQueryEngine(storage=storage), orders

    async def test_windowed_sum_excludes_unrelated_joins(
        self, generator: SQLGenerator, orders_with_customers_engine,
    ) -> None:
        """The window CTE must not pull joins unrelated to the windowed measure.

        Set up a query with:
          - a windowed measure on orders' revenue (no cross-model refs), and
          - a sibling cross-model measure that DOES need the customers join.

        The customers join is required at the OUTER query level, but must NOT
        leak into the windowed measure's _src subquery — otherwise the
        customers fan-out would distort the trailing aggregation. Per
        CLAUDE.md core principle: adding a measure must not affect cardinality.
        """
        engine, orders = orders_with_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],  # local to orders — no join needed
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[
                {"formula": "revenue:sum(window='90d')", "name": "revenue_90d"},
                {"formula": "customers.id:count_distinct", "name": "n_customers"},
            ],
        )
        enriched = await engine._enrich(query=query, model=orders)
        # Sanity: the customers join *did* get resolved at the outer level.
        assert any(alias == "customers" for _, alias, *_ in enriched.resolved_joins)

        sql = generator.generate(enriched=enriched)
        src_body = _extract_src_body(sql)
        assert src_body, "Could not isolate _src subquery body"
        assert "customers" not in src_body, (
            f"_src subquery must not include the unrelated customers join.\n"
            f"src_body:\n{src_body}"
        )

    async def test_windowed_sum_keeps_joins_used_by_query_filter(
        self, generator: SQLGenerator, orders_with_customers_engine,
    ) -> None:
        """Window CTE must keep joins whose alias is referenced by a query-level
        WHERE filter, even if the windowed measure itself doesn't use them.

        Otherwise the rendered SQL has a WHERE clause referencing an alias
        whose JOIN was pruned, and the SQL becomes invalid (or silently
        changes filtering behavior).
        """
        engine, orders = orders_with_customers_engine
        # Filter on customers.region_id forces a customers join. The windowed
        # measure does not otherwise reference customers, so the join would be
        # pruned without the filter-aware logic — and then the WHERE clause
        # below would reference an undefined alias.
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "revenue_90d"}],
            filters=["customers.region_id = 5"],
        )
        enriched = await engine._enrich(query=query, model=orders)
        assert any(alias == "customers" for _, alias, *_ in enriched.resolved_joins)

        sql = generator.generate(enriched=enriched)
        src_body = _extract_src_body(sql)
        assert src_body, "Could not isolate _src subquery body"
        assert "customers" in src_body, (
            f"_src subquery must include customers join because the query-level "
            f"WHERE filter references customers.region_id.\nsrc_body:\n{src_body}"
        )

    async def test_windowed_sum_keeps_transitive_joins_for_multi_hop_filter(
        self, generator: SQLGenerator, tmp_path,
    ) -> None:
        """Multi-hop filter (e.g. customers.regions.name) must keep the
        intermediate `customers` join in the _src subquery.

        The path-aliased target_alias `customers__regions` carries a join
        condition like `customers.region_id = customers__regions.id`, so the
        prefix `customers` must also appear in the JOIN list — otherwise the
        rendered SQL references an undefined alias.
        """
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_model(SlayerModel(
            name="regions", sql_table="regions", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
            ],
        ))
        await storage.save_model(SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        ))
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        await storage.save_model(orders)
        engine = SlayerQueryEngine(storage=storage)

        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "revenue_90d"}],
            filters=["customers.regions.name = 'US'"],
        )
        enriched = await engine._enrich(query=query, model=orders)
        # Sanity: both joins resolved at outer level.
        joined_aliases = {alias for _, alias, *_ in enriched.resolved_joins}
        assert "customers" in joined_aliases
        assert "customers__regions" in joined_aliases

        sql = generator.generate(enriched=enriched)
        src_body = _extract_src_body(sql)
        assert "customers__regions" in src_body, (
            f"_src must include the multi-hop customers__regions join.\nsrc_body:\n{src_body}"
        )
        assert "customers " in src_body or "customers\n" in src_body or "customers." in src_body, (
            f"_src must also include the transitive customers join — its JOIN ON references customers.\n"
            f"src_body:\n{src_body}"
        )

    async def test_filter_on_windowed_measure_is_post_filter(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """A filter on a windowed measure must apply post-aggregation, not as
        HAVING on the base CTE. The base CTE doesn't compute the windowed
        value — applying a HAVING there would use the wrong (non-windowed)
        aggregate.

        Verify by checking the generated SQL contains a WHERE on the
        post-aggregate combined CTE (referenced via the windowed alias).
        DEV-1443: the colon-syntax filter auto-resolves to the user alias
        when the measure is renamed, so the filter references
        ``"orders.revenue_90d"`` (the user-supplied name on the windowed
        measure) and never the unrelated plain-sum alias.
        """
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "revenue_90d"}],
            filters=["revenue:sum(window='90d') > 100"],
        )
        sql = await _generate(generator=generator, query=query, model=orders_model)
        norm = _norm(sql)
        # DEV-1443: the colon-syntax filter resolves to the renamed user
        # alias. CodeRabbit nitpick: target the post-filter WHERE clause
        # specifically — the alias surfaces in the projection too, so a
        # generic ``in norm`` check could pass even on regression.
        where_clause = norm.split(" WHERE ", 1)[1] if " WHERE " in norm else ""
        assert '"orders.revenue_90d"' in where_clause, (
            f"Filter must reference the renamed windowed measure's alias in WHERE.\nsql:\n{sql}"
        )
        # The filter must NOT bind to the plain-sum alias (no window).
        assert '"orders.revenue_sum"' not in where_clause, (
            f"Filter must not bind to plain-sum alias.\nsql:\n{sql}"
        )
        # The filter must be applied OUTSIDE the base CTE (no HAVING on the
        # plain `SUM(amount)` aggregate — that would use the wrong value).
        assert "HAVING SUM" not in norm.upper(), (
            f"Windowed-measure filter must not be applied as HAVING on the base aggregate.\nsql:\n{sql}"
        )

    async def test_window_duration_full_compact_syntax(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.DAY)],
            measures=[{"formula": "revenue:avg(window='1y2m3w5d6h7min8s')", "name": "avg_window"}],
        )
        sql = await _generate(generator=generator, query=query, model=orders_model)
        norm = _norm(sql)
        assert "AVG(_src._w_value)" in norm
        # AST-based generation emits one INTERVAL per parsed (amount, unit)
        # pair, chained as repeated subtractions — sqlglot then transpiles each
        # single-unit interval per dialect (so this same compact duration
        # produces dialect-correct output on MySQL/ClickHouse/BigQuery without
        # the broken Postgres-shape multi-unit literal).
        for piece in (
            "INTERVAL '1 YEAR'",
            "INTERVAL '2 MONTH'",
            "INTERVAL '3 WEEK'",
            "INTERVAL '5 DAY'",
            "INTERVAL '6 HOUR'",
            "INTERVAL '7 MINUTE'",
            "INTERVAL '8 SECOND'",
        ):
            assert piece in norm, f"missing per-unit interval clause '{piece}'\nsql:\n{sql}"

    async def test_windowed_sum_sqlite_duration_modifiers(self, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.DAY)],
            measures=[{"formula": "revenue:sum(window='1w2d3h4min5s')", "name": "revenue_window"}],
        )
        sql = await _generate(
            generator=SQLGenerator(dialect="sqlite"),
            query=query,
            model=orders_model,
        )
        assert "DATETIME(" in sql
        assert "'-7 days'" in sql
        assert "'-2 days'" in sql
        assert "'-3 hours'" in sql
        assert "'-4 minutes'" in sql
        assert "'-5 seconds'" in sql

    async def test_time_shift_row_based(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """time_shift without explicit granularity uses the time dim's granularity (calendar-based)."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="time_shift(revenue:sum, -1)", name="rev_prev")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "shifted_" in sql
        assert "LEFT JOIN" in sql
        # Calendar-based join with INTERVAL (no more ROW_NUMBER)
        assert "INTERVAL" in sql

    async def test_lag(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="lag(revenue:sum, 1)", name="rev_prev")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "LAG(" in sql
        assert "OVER" in sql

    async def test_lead(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="lead(revenue:sum, 1)", name="rev_next")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "LEAD(" in sql
        assert "OVER" in sql

    async def test_change(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="change(revenue:sum)", name="rev_change")],
        )
        sql = await _generate(generator, query, orders_model)
        # change is desugared into time_shift + expression
        assert "shifted_" in sql
        assert "LEFT JOIN" in sql
        # Subtraction now in an expression CTE layer (not in the self-join column)
        assert "rev_change" in sql.lower()
        assert " - " in sql

    async def test_change_pct(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="change_pct(revenue:sum)", name="rev_pct")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "shifted_" in sql
        assert "LEFT JOIN" in sql
        assert "CASE" in sql

    async def test_rank(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="rank(revenue:sum)", name="rev_rank")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "RANK()" in sql
        assert "OVER" in sql

    async def test_last(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="last(revenue:sum)", name="latest_rev")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "FIRST_VALUE(" in sql
        assert "DESC" in sql

    async def test_last_measure_type(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """A measure with last aggregation should use ROW_NUMBER + conditional aggregate."""
        orders_model.default_time_dimension = "created_at"
        orders_model.columns.append(Column(name="balance", sql="balance", type=DataType.DOUBLE))
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="balance:last")],
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
        orders_model.columns.append(Column(name="balance", sql="balance", type=DataType.DOUBLE))
        orders_model.columns.append(Column(name="ordered_at", sql="ordered_at", type=DataType.TIMESTAMP))
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="balance:last(ordered_at)")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "ROW_NUMBER()" in sql
        assert "orders.ordered_at" in sql
        assert "DESC" in sql

    async def test_first_with_explicit_time_column(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """first(ordered_at) should ORDER BY the explicit time column ASC."""
        orders_model.default_time_dimension = "created_at"
        orders_model.columns.append(Column(name="balance", sql="balance", type=DataType.DOUBLE))
        orders_model.columns.append(Column(name="ordered_at", sql="ordered_at", type=DataType.TIMESTAMP))
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="balance:first(ordered_at)")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "ROW_NUMBER()" in sql
        assert "orders.ordered_at" in sql
        assert "ASC" in sql

    async def test_multiple_last_different_time_columns(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Two last measures with different explicit time cols get separate ROW_NUMBER columns."""
        orders_model.default_time_dimension = "created_at"
        orders_model.columns.append(Column(name="balance", sql="balance", type=DataType.DOUBLE))
        orders_model.columns.append(Column(name="ordered_at", sql="ordered_at", type=DataType.TIMESTAMP))
        orders_model.columns.append(Column(name="updated_at", sql="updated_at", type=DataType.TIMESTAMP))
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[
                ModelMeasure(formula="revenue:last(ordered_at)"),
                ModelMeasure(formula="balance:last(updated_at)"),
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
        orders_model.columns.append(Column(name="balance", sql="balance", type=DataType.DOUBLE))
        orders_model.columns.append(Column(name="ordered_at", sql="ordered_at", type=DataType.TIMESTAMP))
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[
                ModelMeasure(formula="revenue:last"),
                ModelMeasure(formula="balance:last(ordered_at)"),
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
        orders_model.columns.append(Column(name="balance", sql="balance", type=DataType.DOUBLE))
        orders_model.columns.append(Column(name="ordered_at", sql="ordered_at", type=DataType.TIMESTAMP))
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[
                ModelMeasure(formula="revenue:last(ordered_at)"),
                ModelMeasure(formula="balance:first(ordered_at)"),
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
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="time_shift(revenue:sum, -1, 'year')", name="rev_prev_year")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "shifted_" in sql
        assert "LEFT JOIN" in sql
        assert "INTERVAL" in sql

    async def test_time_shift_shifted_date_range(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Calendar time_shift with date_range: shifted CTE uses INTERVAL, not shifted dates."""
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
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="time_shift(revenue:sum, -1, 'month')", name="rev_prev")],
        )
        sql = await _generate(generator, query, orders_model)
        # Base CTE should have original date range
        assert "2024-03-01" in sql
        assert "2024-03-31" in sql
        # Shifted CTE uses INTERVAL to shift the time column (not shifted date strings)
        assert "INTERVAL" in sql

    async def test_time_shift_yoy_shifted_date_range(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Year-over-year time_shift uses INTERVAL '1' YEAR in the shifted CTE."""
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
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="time_shift(revenue:sum, -1, 'year')", name="rev_yoy")],
        )
        sql = await _generate(generator, query, orders_model)
        # Shifted CTE should use INTERVAL for year shift
        assert "INTERVAL" in sql
        assert "YEAR" in sql

    async def test_change_shifted_date_range(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """change() with date_range uses a hidden time_shift with INTERVAL."""
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
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="change(revenue:sum)", name="rev_change")],
        )
        sql = await _generate(generator, query, orders_model)
        # change desugars to time_shift + expression; shifted CTE uses INTERVAL
        assert "INTERVAL" in sql
        assert " - " in sql

    async def test_no_date_range_no_shift(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Without a date_range, shifted CTE should still have INTERVAL but no BETWEEN."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="time_shift(revenue:sum, -1, 'month')", name="rev_prev")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "shifted_" in sql
        assert "BETWEEN" not in sql

    async def test_forward_time_shift_with_date_range(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Forward time_shift(x, 1, 'month') with date_range should use negative INTERVAL."""
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
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="time_shift(revenue:sum, 1, 'month')", name="rev_next")],
        )
        sql = await _generate(generator, query, orders_model)
        # Forward shift uses negative INTERVAL
        assert "INTERVAL" in sql
        assert "shifted_" in sql

    async def test_quarter_date_shift(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """time_shift with quarter granularity uses INTERVAL with 3 months."""
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
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="time_shift(revenue:sum, -1, 'quarter')", name="prev_q")],
        )
        sql = await _generate(generator, query, orders_model)
        # Quarter = 3 months; shifted CTE uses INTERVAL
        assert "INTERVAL" in sql
        assert "MONTH" in sql
        assert "shifted_" in sql

    async def test_nested_self_join_raises(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Nesting self-join transforms (e.g., change(time_shift(x))) should raise."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="change(time_shift(revenue:sum, -1, 'year'))", name="x")],
        )
        with pytest.raises(ValueError, match="Nesting.*not supported"):
            await _generate(generator, query, orders_model)

    async def test_post_filter_on_computed_column(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Filters on computed columns should be applied as post-filter wrapper."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="change(revenue:sum)", name="rev_change")],
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
            measures=[ModelMeasure(formula="revenue:sum")],
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
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="change(revenue:sum)", name="rev_change")],
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
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="cumsum(revenue:sum)", name="x")],
        )
        with pytest.raises(ValueError, match="requires an unambiguous time dimension"):
            await _generate(generator, query, orders_model)

    async def test_default_time_dimension_without_explicit_time_dims_raises(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """default_time_dimension alone (no query time_dimensions) must error.

        Previously this would generate invalid SQL with an ORDER BY referencing
        a column not in the base CTE.
        """
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="cumsum(revenue:sum)", name="x")],
        )
        with pytest.raises(ValueError, match="requires an unambiguous time dimension"):
            await _generate(generator, query, orders_model)

    async def test_field_plain_measure(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "COUNT(*)" in sql

    async def test_field_auto_adds_measures(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Fields referencing measures auto-add them to the base query."""
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="revenue:sum / *:count", name="aov")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "aov" in sql.lower()
        assert "WITH" in sql

    async def test_field_mixed_with_measures(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Fields can be used alongside explicit measures."""
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="revenue:sum / *:count", name="aov")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "COUNT(*)" in sql
        assert "SUM(" in sql
        assert "aov" in sql.lower()


class TestRankFamilyTransforms:
    """rank, percent_rank, dense_rank, ntile — first-class window-function transforms.

    All four are timeless (no time_dimension required), all default to no
    PARTITION BY (rank across the entire result set), all order by the inner
    measure DESC. The ``partition_by=`` kwarg opts into per-partition ranking;
    its value must be a subset of the query's dimensions / time_dimensions.

    Pinning DEV-1353.
    """

    async def test_rank_no_partition_unchanged(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Today's behavior: rank without partition_by emits no PARTITION BY clause."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="rank(revenue:sum)", name="rev_rank")],
        )
        sql = await _generate(generator, query, orders_model)
        assert (
            'RANK() OVER (ORDER BY "orders.revenue_sum" DESC)'
            in _norm(sql)
        )

    async def test_rank_with_partition_by_single(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status"), ColumnRef(name="customer_id")],
            measures=[
                ModelMeasure(formula="revenue:sum"),
                ModelMeasure(formula="rank(revenue:sum, partition_by=status)", name="rev_rank"),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        assert (
            'RANK() OVER (PARTITION BY "orders.status" ORDER BY "orders.revenue_sum" DESC)'
            in _norm(sql)
        )

    async def test_rank_with_partition_by_list(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status"), ColumnRef(name="customer_id")],
            measures=[
                ModelMeasure(formula="revenue:sum"),
                ModelMeasure(
                    formula="rank(revenue:sum, partition_by=[status, customer_id])",
                    name="rev_rank",
                ),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        assert (
            'RANK() OVER (PARTITION BY "orders.status", "orders.customer_id" '
            'ORDER BY "orders.revenue_sum" DESC)'
            in _norm(sql)
        )

    async def test_percent_rank_default(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="revenue:sum"),
                ModelMeasure(formula="percent_rank(revenue:sum)", name="rev_pr"),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        assert (
            'PERCENT_RANK() OVER (ORDER BY "orders.revenue_sum" DESC)'
            in _norm(sql)
        )

    async def test_percent_rank_with_partition(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status"), ColumnRef(name="customer_id")],
            measures=[
                ModelMeasure(formula="revenue:sum"),
                ModelMeasure(
                    formula="percent_rank(revenue:sum, partition_by=status)", name="rev_pr"
                ),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        assert (
            'PERCENT_RANK() OVER (PARTITION BY "orders.status" '
            'ORDER BY "orders.revenue_sum" DESC)'
            in _norm(sql)
        )

    async def test_dense_rank_default(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="revenue:sum"),
                ModelMeasure(formula="dense_rank(revenue:sum)", name="rev_dr"),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        assert (
            'DENSE_RANK() OVER (ORDER BY "orders.revenue_sum" DESC)'
            in _norm(sql)
        )

    async def test_dense_rank_with_partition(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status"), ColumnRef(name="customer_id")],
            measures=[
                ModelMeasure(formula="revenue:sum"),
                ModelMeasure(
                    formula="dense_rank(revenue:sum, partition_by=status)", name="rev_dr"
                ),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        assert (
            'DENSE_RANK() OVER (PARTITION BY "orders.status" '
            'ORDER BY "orders.revenue_sum" DESC)'
            in _norm(sql)
        )

    async def test_ntile_n_4(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="revenue:sum"),
                ModelMeasure(formula="ntile(revenue:sum, n=4)", name="rev_quartile"),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        assert (
            'NTILE(4) OVER (ORDER BY "orders.revenue_sum" DESC)'
            in _norm(sql)
        )

    async def test_ntile_with_partition(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status"), ColumnRef(name="customer_id")],
            measures=[
                ModelMeasure(formula="revenue:sum"),
                ModelMeasure(
                    formula="ntile(revenue:sum, n=4, partition_by=status)",
                    name="rev_quartile",
                ),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        assert (
            'NTILE(4) OVER (PARTITION BY "orders.status" '
            'ORDER BY "orders.revenue_sum" DESC)'
            in _norm(sql)
        )

    async def test_dense_rank_in_filter_top_5_distinct(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """``dense_rank(...) <= 5`` is auto-extracted as a hidden field and post-filtered.

        Mirrors the existing ``rank(...) <= N`` pattern from DEV-1336. The window
        function must materialise inside the inner SELECT (so SQLite doesn't see
        ``WHERE DENSE_RANK() OVER (...) <= 5``) and the comparison must live in
        the outer ``_filtered`` wrapper.
        """
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customer_id")],
            measures=[ModelMeasure(formula="revenue:sum")],
            filters=["dense_rank(revenue:sum) <= 5"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "_filtered" in sql, f"expected post-filter wrapper, got:\n{sql}"
        # Split on the wrapper marker so we can pin DENSE_RANK to the inner SELECT
        # and the predicate to the outer wrapper, not just "somewhere in the SQL".
        inner_sql, outer_sql = sql.split("_filtered", 1)
        assert "DENSE_RANK()" in inner_sql, (
            f"DENSE_RANK should be materialised in the inner SELECT, got:\n{sql}"
        )
        assert "DENSE_RANK()" not in outer_sql, (
            f"DENSE_RANK should not appear in the outer wrapper, got:\n{sql}"
        )
        assert "<= 5" in _norm(outer_sql), (
            f"<= 5 predicate should live in the outer wrapper, got:\n{sql}"
        )

    async def test_rank_partition_by_time_dimension(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """partition_by= can reference a query time_dimension, not just a regular dimension.

        Pins the time-alias resolution path in _resolve_rank_partition's
        ``for td in time_dimensions`` loop — without this case, a regression
        there would silently pass.
        """
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH
                )
            ],
            measures=[
                ModelMeasure(formula="revenue:sum"),
                ModelMeasure(
                    formula="rank(revenue:sum, partition_by=created_at)", name="rev_rank"
                ),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        assert (
            'RANK() OVER (PARTITION BY "orders.created_at" '
            'ORDER BY "orders.revenue_sum" DESC)'
            in _norm(sql)
        )

    async def test_partition_by_must_be_a_query_dimension(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """partition_by referencing a column NOT in dimensions errors clearly.

        Otherwise the partition column wouldn't be in the base CTE and the
        emitted SQL would be silently invalid.
        """
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="revenue:sum"),
                ModelMeasure(
                    formula="rank(revenue:sum, partition_by=customer_id)", name="rev_rank"
                ),
            ],
        )
        with pytest.raises(ValueError) as excinfo:
            await _generate(generator, query, orders_model)
        msg = str(excinfo.value)
        assert "partition_by" in msg
        assert "customer_id" in msg
        # Contract: error lists the available dimensions so the user knows what to pick.
        assert "status" in msg


class TestTransformRequiresTimeDimension:
    """All time-ordered transforms require an explicit time_dimensions entry."""

    async def test_cumsum_without_time_dimension_raises(self, generator: SQLGenerator) -> None:
        """cumsum with only default_time_dimension (no query time_dimensions) must error."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.DATE),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
            default_time_dimension="created_at",
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="cumsum(revenue:sum)")],
            dimensions=[ColumnRef(name="status")],
            # No time_dimensions — only default_time_dimension on model
        )
        with pytest.raises(ValueError, match="requires an unambiguous time dimension"):
            await _generate(generator, query, model)

    async def test_lag_without_time_dimension_raises(self, generator: SQLGenerator) -> None:
        """lag with only default_time_dimension must error."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.DATE),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
            default_time_dimension="created_at",
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="lag(revenue:sum)")],
            dimensions=[ColumnRef(name="status")],
        )
        with pytest.raises(ValueError, match="requires an unambiguous time dimension"):
            await _generate(generator, query, model)

    async def test_consecutive_periods_without_time_dimension_raises(self, generator: SQLGenerator) -> None:
        """consecutive_periods with only default_time_dimension must error."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.DATE),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
            default_time_dimension="created_at",
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="consecutive_periods(revenue:sum > 0)")],
            dimensions=[ColumnRef(name="status")],
        )
        with pytest.raises(ValueError, match="requires an unambiguous time dimension"):
            await _generate(generator=generator, query=query, model=model)

    async def test_cumsum_with_time_dimension_works(self, generator: SQLGenerator) -> None:
        """cumsum with explicit time_dimensions should work fine."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.DATE),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="cumsum(revenue:sum)")],
            dimensions=[ColumnRef(name="status")],
            time_dimensions=[TimeDimension(dimension="created_at", granularity="month")],
        )
        sql = await _generate(generator, query, model)
        assert "SUM(" in sql
        assert "OVER" in sql


class TestNestedFields:
    async def test_nested_cumsum_of_change_generates_stacked_ctes(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """cumsum(change(revenue:sum)) should produce stacked CTEs."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[
                ModelMeasure(formula="revenue:sum"),
                ModelMeasure(formula="cumsum(change(revenue:sum))", name="delta"),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        # Should have base + stacked CTEs
        assert "base" in sql.lower()
        assert "shifted_" in sql  # change desugars to time_shift
        assert "SUM(" in sql  # cumsum window
        assert "delta" in sql.lower()

    async def test_change_of_cumsum_raises(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """change(cumsum(x)) is not supported — time_shift can't target a window function result."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[
                ModelMeasure(formula="revenue:sum"),
                ModelMeasure(formula="change(cumsum(revenue:sum))", name="delta"),
            ],
        )
        with pytest.raises(ValueError, match="not found"):
            await _generate(generator, query, orders_model)

    async def test_mixed_arithmetic_with_transform(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """cumsum(revenue) / count should work."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[
                ModelMeasure(formula="*:count"),
                ModelMeasure(formula="revenue:sum"),
                ModelMeasure(formula="cumsum(revenue:sum) / *:count", name="avg_cumsum"),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        assert "SUM(" in sql  # cumsum window
        assert "avg_cumsum" in sql.lower()

    async def test_emitted_sql_has_no_agg_placeholder(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """DEV-1341: aggregated refs nested inside non-transform calls (``nullif``)
        must be fully resolved — no ``__aggN__`` placeholder may leak through.
        """
        query = SlayerQuery(
            source_model="orders",
            measures=[
                ModelMeasure(
                    formula="*:count / nullif(revenue:max, 0)",
                    name="violation_rate",
                ),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        assert "__agg" not in sql, f"__aggN__ placeholder leaked into SQL:\n{sql}"


class TestStatAggsViaQueryEnrichment:
    """End-to-end aggregator-level checks (parser → enricher → generator).
    Confirms the new aggregations are reachable from query syntax, not
    just the internal _build_agg builder."""

    @pytest.fixture
    def sales_model(self) -> SlayerModel:
        return SlayerModel(
            name="sales",
            sql_table="public.sales",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="price", sql="price", type=DataType.DOUBLE),
                Column(name="quantity", sql="quantity", type=DataType.DOUBLE),
                Column(name="latency", sql="latency", type=DataType.DOUBLE),
            ],
        )

    @pytest.mark.parametrize(
        "formula,expected_fragment",
        [
            ("latency:stddev_samp", "STDDEV_SAMP"),
            ("latency:stddev_pop", "STDDEV_POP"),
            ("latency:var_samp", "VAR_SAMP"),
            ("latency:var_pop", "VAR_POP"),
        ],
    )
    async def test_stat_agg_via_colon_syntax(
        self,
        formula: str,
        expected_fragment: str,
        sales_model: SlayerModel,
    ) -> None:
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(
            source_model="sales",
            measures=[ModelMeasure(formula=formula)],
        )
        sql = await _generate(generator=gen, query=query, model=sales_model)
        # Pin the function-call shape: the family name immediately followed
        # by the qualified column ref in its single-arg slot. `sales.latency`
        # being inside the call is what proves enrichment+generation reached
        # the value column, not just that the alias contains the fragment.
        assert f"{expected_fragment}(sales.latency)" in sql

    async def test_corr_via_colon_syntax_with_other_kwarg(
        self, sales_model: SlayerModel,
    ) -> None:
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(
            source_model="sales",
            measures=[ModelMeasure(formula="price:corr(other=quantity)")],
        )
        sql = await _generate(generator=gen, query=query, model=sales_model)
        # Both legs flow through _resolve_sql and qualify under the LHS
        # measure's model_name.
        assert "CORR(sales.price, sales.quantity)" in sql


class TestPathAliasJoinInference:
    """Test that __-delimited path aliases in inline SQL cause multi-hop join inference via graph walk."""

    @pytest.fixture
    async def storage(self, tmp_path):
        s = YAMLStorage(base_dir=str(tmp_path))
        await s.save_model(SlayerModel(
            name="regions", sql_table="regions", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
                Column(name="population", sql="population", type=DataType.DOUBLE),
            ],
        ))
        await s.save_model(SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
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
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(
                    name="is_us",
                    sql="CASE WHEN customers__regions.name = 'US' THEN 1 ELSE 0 END",
                    type=DataType.DOUBLE,
                ),

            ],
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
            measures=[ModelMeasure(formula="*:count")],
            dimensions=[ColumnRef(name="is_us")],
        )
        enriched = await engine._enrich(query=query, model=chained_model)
        join_aliases = {alias for _, alias, *_ in enriched.resolved_joins}
        assert "customers" in join_aliases
        assert "customers__regions" in join_aliases

    async def test_time_dimension_sql_with_path_alias_infers_joins(self, storage) -> None:
        """Inline time dimension SQL referencing path alias should also trigger join inference."""
        await storage.save_model(SlayerModel(
            name="orgs", sql_table="orgs", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="signup_date", sql="signup_date", type=DataType.TIMESTAMP),
            ],
        ))
        await storage.save_model(SlayerModel(
            name="users", sql_table="users", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="org_id", sql="org_id", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="orgs", join_pairs=[["org_id", "id"]])],
        ))
        model = SlayerModel(
            name="events",
            sql_table="events",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="user_id", sql="user_id", type=DataType.DOUBLE),
                Column(
                    name="user_signup_date",
                    sql="users__orgs.signup_date",
                    type=DataType.TIMESTAMP,
                ),

            ],
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
            measures=[ModelMeasure(formula="*:count")],
        )
        enriched = await engine._enrich(query=query, model=model)
        join_aliases = {alias for _, alias, *_ in enriched.resolved_joins}
        assert "users" in join_aliases
        assert "users__orgs" in join_aliases

    async def test_measure_sql_with_path_alias_infers_joins(
        self, engine: SlayerQueryEngine, chained_model: SlayerModel
    ) -> None:
        """Measure SQL like 'customers__regions.population' should infer joins for both tables."""
        chained_model.columns.append(
            Column(name="region_pop_sum", sql="customers__regions.population", type=DataType.DOUBLE)
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="region_pop_sum:sum")],
        )
        enriched = await engine._enrich(query=query, model=chained_model)
        join_aliases = {alias for _, alias, *_ in enriched.resolved_joins}
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
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region", sql="region", type=DataType.TEXT),

                Column(name="price", sql="price", type=DataType.DOUBLE),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
                Column(name="quantity", sql="quantity", type=DataType.DOUBLE),
            ],
        )

    @pytest.fixture
    def gen(self) -> SQLGenerator:
        return SQLGenerator(dialect="postgres")

    async def test_weighted_avg_valid_column_param(self, gen: SQLGenerator, agg_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="sales",
            measures=[ModelMeasure(formula="price:weighted_avg(weight=quantity)")],
        )
        sql = await _generate(gen, query, agg_model)
        assert "SUM(" in sql
        assert "NULLIF(" in sql

    async def test_percentile_valid_numeric_param(self, gen: SQLGenerator, agg_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="sales",
            measures=[ModelMeasure(formula="revenue:percentile(p=0.95)")],
        )
        sql = await _generate(gen, query, agg_model)
        assert "PERCENTILE_CONT" in sql
        assert "0.95" in sql

    async def test_qualified_column_param(self, gen: SQLGenerator, agg_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="sales",
            measures=[ModelMeasure(formula="price:weighted_avg(weight=sales.quantity)")],
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
            measures=[ModelMeasure(formula="price:custom_weighted")],
        )
        # Should succeed — model-level defaults are trusted
        sql = await _generate(gen, query, agg_model)
        assert "CASE WHEN" in sql
        assert "SUM(" in sql

    async def test_filtered_custom_agg_does_not_case_wrap_literal_param(
        self, gen: SQLGenerator, agg_model: SlayerModel,
    ) -> None:
        """A1 (CodeRabbit major on PR #82): when `_build_formula_agg`
        substitutes params into a filtered measure's template, literal
        defaults (e.g., `scale=100`) must stay literal. Wrapping them in
        `(CASE WHEN ... THEN 100 END)` turns a constant into a row
        expression — invalid GROUP-BY shape and semantically wrong.
        Row-level params (column refs) keep getting wrapped.
        """
        agg_model.aggregations = [
            Aggregation(
                name="scaled_sum",
                formula="SUM({value}) / {scale}",
                params=[AggregationParam(name="scale", sql="100")],
            ),
        ]
        agg_model.columns.append(
            Column(
                name="active_revenue",
                sql="amount",
                filter="status = 'active'",
                type=DataType.DOUBLE,
            )
        )
        query = SlayerQuery(
            source_model="sales",
            measures=[ModelMeasure(formula="active_revenue:scaled_sum")],
        )
        sql = await _generate(generator=gen, query=query, model=agg_model)
        # The literal `100` must NOT appear inside CASE WHEN; the value
        # column SHOULD still be CASE-wrapped.
        assert "CASE WHEN status = 'active' THEN 100" not in sql
        assert "/ 100" in sql
        assert "CASE WHEN status = 'active' THEN" in sql

    async def test_filtered_weighted_avg_still_wraps_column_weight(
        self, gen: SQLGenerator, agg_model: SlayerModel,
    ) -> None:
        """Counter-test for A1: weighted_avg's `weight=quantity` IS a row-
        level reference, so the CASE-WHEN wrap still applies to it. The
        literal-vs-row-ref distinction is what matters.
        """
        agg_model.columns.append(
            Column(
                name="active_price",
                sql="price",
                filter="status = 'active'",
                type=DataType.DOUBLE,
            )
        )
        query = SlayerQuery(
            source_model="sales",
            measures=[
                ModelMeasure(formula="active_price:weighted_avg(weight=quantity)")
            ],
        )
        sql = await _generate(generator=gen, query=query, model=agg_model)
        # Both legs are row-level references → both wrapped.
        assert sql.count("CASE WHEN status = 'active'") >= 2

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
        orders_model.columns.append(
            Column(name="active_revenue", sql="amount", filter="status = 'active'", type=DataType.DOUBLE)
        )
        query = SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="active_revenue:sum")])
        sql = await _generate(generator, query, orders_model)
        assert "CASE WHEN" in sql
        assert "THEN" in sql
        assert "SUM(" in sql

    async def test_filtered_count_star(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """COUNT on a filtered column becomes COUNT(CASE WHEN filter THEN col END).

        v2 unified Column with `sql=None` defaults to the bare column name, so
        the generated SQL counts ``orders.active_count`` rows matching the filter
        rather than literal 1. Either form is correct for ``count`` aggregation.
        """
        orders_model.columns.append(
            Column(name="active_count", sql="amount", filter="status = 'active'", type=DataType.DOUBLE)
        )
        query = SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="active_count:count")])
        sql = await _generate(generator, query, orders_model)
        assert "CASE WHEN" in sql
        assert "COUNT(" in sql
        # Should NOT be COUNT(*)
        assert "COUNT(*)" not in sql

    async def test_filtered_avg(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        orders_model.columns.append(
            Column(name="active_avg", sql="amount", filter="status = 'active'", type=DataType.DOUBLE)
        )
        query = SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="active_avg:avg")])
        sql = await _generate(generator, query, orders_model)
        assert "CASE WHEN" in sql
        assert "AVG(" in sql

    async def test_unfiltered_measure_no_case(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Measures without filter should not have CASE WHEN."""
        query = SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="revenue:sum")])
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
        orders_model.columns.append(
            Column(name="quantity", sql="quantity", type=DataType.DOUBLE)
        )
        orders_model.columns.append(
            Column(name="active_revenue", sql="amount", filter="status = 'active'", type=DataType.DOUBLE)
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="active_revenue:weighted_avg(weight=quantity)")],
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
        orders_model.columns.append(
            Column(name="active_revenue", sql="amount", filter="status = 'active'", type=DataType.DOUBLE)
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="active_revenue:sum")],
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
        orders_model.columns.append(
            Column(name="completed_balance", sql="amount", filter="status = 'completed'", type=DataType.DOUBLE)
        )
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
            measures=[ModelMeasure(formula="completed_balance:last")],
        )
        sql = await _generate(generator, query, orders_model)
        # Should have a dedicated filtered ROW_NUMBER column
        assert "_last_rn_f0" in sql
        # The ORDER BY should include CASE WHEN filter THEN 0 ELSE 1 END
        assert "CASE WHEN" in sql
        assert "THEN 0 ELSE 1" in sql
        # Standard ROW_NUMBER should NOT be present (no unfiltered first/last).
        # Use regex word-boundary to avoid the obvious overlap with "_last_rn_f0".
        assert _re.search(r"_last_rn(?!_f)", sql) is None, (
            f"Bare _last_rn alias should not leak into SQL when only filtered "
            f"first/last is requested: {sql}"
        )

    async def test_filtered_first_generates_dedicated_rn(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Filtered first measure generates a dedicated ROW_NUMBER with filter in ORDER BY."""
        orders_model.default_time_dimension = "created_at"
        orders_model.columns.append(
            Column(name="completed_balance", sql="amount", filter="status = 'completed'", type=DataType.DOUBLE)
        )
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
            measures=[ModelMeasure(formula="completed_balance:first")],
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
        orders_model.columns.append(Column(name="balance", sql="amount", type=DataType.DOUBLE))
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
            measures=[ModelMeasure(formula="balance:last")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "_last_rn" in sql
        assert "_last_rn_f" not in sql

    async def test_mixed_filtered_and_unfiltered_last(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Both filtered and unfiltered last measures get separate ROW_NUMBER columns."""
        orders_model.default_time_dimension = "created_at"
        orders_model.columns.append(Column(name="balance", sql="amount", type=DataType.DOUBLE))
        orders_model.columns.append(
            Column(name="completed_balance", sql="amount", filter="status = 'completed'", type=DataType.DOUBLE)
        )
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
            measures=[
                ModelMeasure(formula="balance:last"),
                ModelMeasure(formula="completed_balance:last"),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        # Should have both the shared _last_rn and the filtered _last_rn_f0
        assert "_last_rn" in sql
        assert "_last_rn_f0" in sql

    @staticmethod
    async def _filtered_last_cross_model_sql(generator: SQLGenerator) -> str:
        """Shared setup for the two ``active_balance:last`` cross-model
        filter tests below. Builds the customers+orders models, runs
        enrichment with a stub join resolver, and returns the generated
        SQL — each caller asserts on a different facet of the output."""
        customers = SlayerModel(
            name="customers",
            sql_table="public.customers",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
            ],
        )
        orders = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),

                Column(
                    name="active_balance",
                    sql="amount",
                    filter="customers.status = 'active'", type=DataType.DOUBLE),
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
            measures=[ModelMeasure(formula="active_balance:last")],
        )
        enriched = await enrich_query(
            query=query,
            model=orders,
            resolve_dimension_via_joins=_noop_async,
            resolve_cross_model_measure=_noop_async,
            resolve_join_target=resolve_join_target,
        )
        return generator.generate(enriched=enriched)

    async def test_filtered_last_with_cross_model_filter_carries_join(
        self, generator: SQLGenerator,
    ) -> None:
        """Regression for CodeRabbit #8 — when a filtered last measure's filter
        references a column on a JOINED model, the LEFT JOIN must be applied
        INSIDE the ranked subquery so the filter columns resolve. Previously
        _build_last_ranked_from() built the subquery from base_from only and
        the outer string-level join injection never matched the subquery wrapper."""

        sql = await self._filtered_last_cross_model_sql(generator)
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
        # The cross-model filter join appears in the ranked subquery (for filter
        # resolution) and potentially in the isolated-measure CTE.
        assert "LEFT JOIN public.customers" in sql

    async def test_filtered_last_cross_model_isolates_to_cte_with_ranked_subquery(
        self, generator: SQLGenerator,
    ) -> None:
        """Regression for CodeRabbit B6-4 — when a filtered first/last measure's
        filter references a JOINED table (e.g. customers.status), the measure
        is isolated into its own CTE with a ranked subquery. The join is placed
        inside the ranked subquery so the filter resolves, and the final SELECT
        does not reference the joined table directly."""

        sql = await self._filtered_last_cross_model_sql(generator)

        # The outermost SELECT (after all CTEs) should not reference
        # 'customers.' directly — it pulls pre-computed values from CTEs.
        final_select_idx = sql.rfind("\nSELECT ")
        if final_select_idx == -1:
            final_select_idx = sql.rfind("SELECT ")
        final_select = sql[final_select_idx:]

        assert "customers." not in final_select, (
            f"Final SELECT references joined table 'customers' which is "
            f"not in scope — should use CTE column references. "
            f"Final SELECT:\n{final_select}\n\nFull SQL:\n{sql}"
        )
        # The isolated CTE should contain a ranked subquery with _last_rn
        assert "_fm_" in sql, f"Expected isolated _fm_ CTE:\n{sql}"
        assert "_last_rn" in sql, f"Expected _last_rn in isolated CTE:\n{sql}"
        # The customers JOIN should be inside the _fm_ CTE's ranked subquery
        fm_match = _re.search(r"(_fm_\w+)\s+AS\s*\(", sql)
        assert fm_match, f"No _fm_ CTE found:\n{sql}"
        fm_start = fm_match.start()
        fm_end = sql.index("\n)", fm_start)
        fm_body = sql[fm_start:fm_end]
        assert "public.customers" in fm_body, (
            f"Expected customers JOIN inside _fm_ CTE:\n{fm_body}"
        )

    async def test_filter_with_dotted_string_literal_does_not_pull_spurious_join(
        self, generator: SQLGenerator,
    ) -> None:
        """Regression for CodeRabbit #6 — when a measure filter contains a string
        literal that happens to include a dot (e.g. "url LIKE 'foo.bar%'"), the
        join planner must NOT mistake the literal for a `customers.<col>` ref
        and pull in an unwanted LEFT JOIN. The structured filter_columns from
        ParsedFilter only lists real column references."""

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
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="url", sql="url", type=DataType.TEXT),

                Column(
                    name="vendor_revenue",
                    sql="amount",
                    # The dot inside the literal is what would trip the regex.
                    filter="url LIKE 'foo.bar%'", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="foo", join_pairs=[["id", "id"]])],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="vendor_revenue:sum")],
        )
        enriched = await enrich_query(
            query=query,
            model=orders,
            resolve_dimension_via_joins=_noop_async,
            resolve_cross_model_measure=_noop_async,
            resolve_join_target=resolve_join_target,
        )
        # The 'foo' join must NOT have been pulled into the resolved joins.
        # (resolve_join_target may be called for aggregation name discovery,
        # but that should not result in an actual JOIN in the query.)
        join_aliases = {alias for _, alias, *_ in enriched.resolved_joins}
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
        orders_model.columns.append(
            Column(name="active_balance", sql="amount", filter="status = 'active'", type=DataType.DOUBLE)
        )
        orders_model.columns.append(
            Column(name="completed_balance", sql="amount", filter="status = 'completed'", type=DataType.DOUBLE)
        )
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
            measures=[
                ModelMeasure(formula="active_balance:last"),
                ModelMeasure(formula="completed_balance:last"),
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

        orders_model.columns.append(
            Column(name="active_revenue", sql="amount", filter="status = 'active'", type=DataType.DOUBLE)
        )
        # Underlying model loaded under a different name than the query references.
        underlying = orders_model.model_copy(update={"name": "orders_underlying"})
        query = SlayerQuery(
            source_model="orders_alias",
            measures=[ModelMeasure(formula="active_revenue:sum")],
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

        orders_model.columns.append(
            Column(name="active_revenue", sql="amount", filter="status = 'active'", type=DataType.DOUBLE)
        )
        underlying = orders_model.model_copy(update={"name": "orders_underlying"})
        query = SlayerQuery(
            source_model="orders_alias",
            measures=[ModelMeasure(formula="active_revenue:sum")],
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
    """End-to-end SQL-injection hardening for the ``Column.filter`` field.

    DEV-1378 finalises Mode A semantics for ``Column.filter`` /
    ``SlayerModel.filters``: the strings are pass-through SQL that flows
    into the WHERE clause and is then re-parsed by sqlglot under the
    target dialect. The user is responsible for writing valid
    dialect-aware SQL (including proper string-literal escaping —
    doubled apostrophes, no Python-style ``\\'`` escapes); sqlglot is
    the dialect-aware gate that catches malformed payloads.

    These tests verify that a hostile filter still cannot inject SQL:
    sqlglot's parser/tokenizer raises on the multi-statement,
    UNION-injection, and unbalanced-quote payloads.
    """

    # ------------------------------------------------------------------
    # Rejected at sqlglot generation time
    # ------------------------------------------------------------------

    async def test_drop_table_rejected(self, orders_model: SlayerModel) -> None:
        """Classic ``'; DROP TABLE ...`` payload is rejected by sqlglot
        when the WHERE clause is parsed under the target dialect."""
        orders_model.columns.append(
            Column(
                name="evil",
                sql="amount",
                filter="status = 'a'; DROP TABLE orders; --'",
                type=DataType.DOUBLE,
            )
        )
        query = SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="evil:sum")])
        with pytest.raises((sqlglot.errors.ParseError, sqlglot.errors.TokenError, ValueError)):
            await _generate(SQLGenerator(dialect="postgres"), query, orders_model)

    async def test_union_select_rejected(self, orders_model: SlayerModel) -> None:
        """UNION SELECT payload is rejected by sqlglot at generation time."""
        orders_model.columns.append(
            Column(
                name="evil",
                sql="amount",
                filter="status = 'a' UNION SELECT * FROM users --'",
                type=DataType.DOUBLE,
            )
        )
        query = SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="evil:sum")])
        with pytest.raises((sqlglot.errors.ParseError, sqlglot.errors.TokenError, ValueError)):
            await _generate(SQLGenerator(dialect="postgres"), query, orders_model)

    def test_block_comment_passes_through_safely(self, orders_model: SlayerModel) -> None:
        """``/* ... */`` block comments survive ``Column`` construction —
        DEV-1369's SQL-mode validator does not parse them, only checks for
        DSL constructs (aggregation colon syntax, transform calls, ``OVER``).

        End-to-end round-trip via enrichment + generation isn't validated
        here because the enrichment-side filter parser is still the DSL
        parser; threading dialect-aware sqlglot into enrichment is tracked
        separately. The current contract is: construction accepts the
        filter; generation-time SQL parsing is the dialect-specific gate.
        """
        col = Column(
            name="benign",
            sql="amount",
            filter="status = 'a' /* x */ OR 1=1",
            type=DataType.DOUBLE,
        )
        assert col.filter is not None
        assert "/*" in col.filter

    # ------------------------------------------------------------------
    # Accepted and neutralised in emitted SQL — tested across dialects
    # ------------------------------------------------------------------

    @pytest.mark.parametrize("dialect", ["postgres", "mysql", "sqlite", "duckdb"])
    async def test_embedded_single_quote_round_trips(
        self, orders_model: SlayerModel, dialect: str,
    ) -> None:
        """A SQL-escaped apostrophe (`O''Brien`) round-trips through the
        generator unchanged.

        DEV-1378: Mode A is pass-through SQL; the user writes proper
        dialect-aware escaping (doubled apostrophes), not Python-style
        backslash escapes.
        """
        orders_model.columns.append(
            Column(
                name="irish_names",
                sql="amount",
                # SQL-escaped literal (doubled single quote): O'Brien
                filter="status = 'O''Brien'", type=DataType.DOUBLE)
        )
        query = SlayerQuery(
            source_model="orders", measures=[ModelMeasure(formula="irish_names:sum")]
        )
        sql = await _generate(
            generator=SQLGenerator(dialect=dialect),
            query=query,
            model=orders_model,
        )
        # sqlglot preserves the SQL-doubled apostrophe per dialect.
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
        orders_model.columns.append(
            Column(
                name="evil",
                sql="amount",
                # Runtime filter string:  status = 'a\'
                filter="status = 'a\\\\'", type=DataType.DOUBLE)
        )
        query = SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="evil:sum")])
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
        orders_model.columns.append(
            Column(
                name="evil",
                sql="amount",
                # Runtime filter string:  status = 'a\b'
                filter="status = 'a\\\\b'", type=DataType.DOUBLE)
        )
        query = SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="evil:sum")])
        sql = await _generate(SQLGenerator(dialect=dialect), query, orders_model)
        self._assert_round_trips_cleanly(sql, dialect)

    @pytest.mark.parametrize("dialect", ["postgres", "mysql"])
    async def test_like_pattern_backslash_is_neutralised(
        self, orders_model: SlayerModel, dialect: str,
    ) -> None:
        """The ``LIKE`` path in ``_filter_node_to_sql`` goes through a separate
        helper (``_get_string_arg``); its backslash handling must match."""
        orders_model.columns.append(
            Column(
                name="evil",
                sql="amount",
                # Runtime filter string:  status like 'a\'
                filter="status like 'a\\\\'", type=DataType.DOUBLE)
        )
        query = SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="evil:sum")])
        sql = await _generate(SQLGenerator(dialect=dialect), query, orders_model)
        self._assert_round_trips_cleanly(sql, dialect)

    @pytest.mark.parametrize("dialect", ["postgres", "mysql"])
    async def test_adversarial_quote_break_cannot_inject(
        self, orders_model: SlayerModel, dialect: str,
    ) -> None:
        """A backslash-quote injection payload must either be rejected at
        sqlglot generation time or be confined to a properly-terminated
        string literal that round-trips cleanly.

        DEV-1378: Mode A pass-through. The user passing Python-style
        ``\\'`` is malformed SQL on most dialects (Postgres, SQLite,
        DuckDB don't honour backslash as escape); MySQL/ClickHouse do.
        sqlglot's tokenizer / parser is the gate.
        """
        evil = "status = 'a\\\\' OR 1=1 --"  # Runtime: status = 'a\\' OR 1=1 --
        try:
            orders_model.columns.append(Column(name="evil", sql="amount", filter=evil, type=DataType.DOUBLE))
            query = SlayerQuery(
                source_model="orders", measures=[ModelMeasure(formula="evil:sum")]
            )
            sql = await _generate(SQLGenerator(dialect=dialect), query, orders_model)
        except (ValueError, sqlglot.errors.ParseError, sqlglot.errors.TokenError):
            return  # sqlglot rejected — acceptable
        self._assert_round_trips_cleanly(sql, dialect)

    async def test_existing_filter_still_works_after_escaping(
        self, orders_model: SlayerModel,
    ) -> None:
        """Sanity: ordinary filters (no backslashes, no apostrophes) keep
        producing the same SQL shape after the escape-hardening change."""
        orders_model.columns.append(
            Column(name="active_revenue", sql="amount", filter="status = 'active'", type=DataType.DOUBLE)
        )
        query = SlayerQuery(
            source_model="orders", measures=[ModelMeasure(formula="active_revenue:sum")]
        )
        sql = await _generate(SQLGenerator(dialect="postgres"), query, orders_model)
        assert "'active'" in sql
        assert "CASE WHEN" in sql
        assert "SUM(" in sql


class TestAutoMoveDimensions:
    """Test _auto_move_fields_to_dimensions preprocessing in the query engine."""

    @pytest.fixture
    def storage(self, tmp_path):

        return YAMLStorage(base_dir=str(tmp_path))

    @pytest.fixture
    async def engine_and_model(self, storage):
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        customers = SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
                Column(name="region", sql="region", type=DataType.TEXT),

            ],
        )
        await storage.save_model(orders)
        await storage.save_model(customers)
        engine = SlayerQueryEngine(storage=storage)
        return engine, orders

    async def test_bare_local_dimension_moved(self, engine_and_model) -> None:
        engine, model = engine_and_model
        query = SlayerQuery(source_model="orders", measures=["status", "revenue:sum"])
        result = await engine._auto_move_fields_to_dimensions(query=query, model=model, named_queries={})
        assert len(result.measures) == 1
        assert result.measures[0].formula == "revenue:sum"
        assert any(d.name == "status" for d in result.dimensions)

    async def test_cross_model_dimension_moved(self, engine_and_model) -> None:
        engine, model = engine_and_model
        query = SlayerQuery(source_model="orders", measures=["customers.name", "revenue:sum"])
        result = await engine._auto_move_fields_to_dimensions(query=query, model=model, named_queries={})
        assert len(result.measures) == 1
        assert any(d.full_name == "customers.name" for d in result.dimensions)

    async def test_colon_fields_kept(self, engine_and_model) -> None:
        engine, model = engine_and_model
        query = SlayerQuery(source_model="orders", measures=["revenue:sum", "*:count"])
        result = await engine._auto_move_fields_to_dimensions(query=query, model=model, named_queries={})
        assert len(result.measures) == 2
        assert not result.dimensions

    async def test_arithmetic_kept(self, engine_and_model) -> None:
        engine, model = engine_and_model
        query = SlayerQuery(source_model="orders", measures=["revenue:sum / *:count"])
        result = await engine._auto_move_fields_to_dimensions(query=query, model=model, named_queries={})
        assert len(result.measures) == 1

    async def test_bare_named_measure_kept(self, engine_and_model) -> None:
        """A bare ref to a model-level ``ModelMeasure`` name stays in measures."""
        engine, model = engine_and_model
        # Add a model-level measure formula named "aov".
        model.measures.append(ModelMeasure(name="aov", formula="revenue:sum / *:count"))
        query = SlayerQuery(source_model="orders", measures=["aov", "revenue:sum"])
        result = await engine._auto_move_fields_to_dimensions(query=query, model=model, named_queries={})
        # "aov" is a named formula — stays in measures, not auto-moved.
        assert len(result.measures) == 2

    async def test_unknown_bare_name_kept(self, engine_and_model) -> None:
        engine, model = engine_and_model
        query = SlayerQuery(source_model="orders", measures=["nonexistent", "revenue:sum"])
        result = await engine._auto_move_fields_to_dimensions(query=query, model=model, named_queries={})
        assert len(result.measures) == 2

    async def test_invalid_cross_model_path_kept(self, engine_and_model) -> None:
        engine, model = engine_and_model
        query = SlayerQuery(source_model="orders", measures=["customers.nonexistent", "revenue:sum"])
        result = await engine._auto_move_fields_to_dimensions(query=query, model=model, named_queries={})
        assert len(result.measures) == 2

    async def test_no_fields_noop(self, engine_and_model) -> None:
        engine, model = engine_and_model
        query = SlayerQuery(source_model="orders", dimensions=["status"])
        result = await engine._auto_move_fields_to_dimensions(query=query, model=model, named_queries={})
        assert result.measures is None

    async def test_appends_to_existing_dimensions(self, engine_and_model) -> None:
        engine, model = engine_and_model
        query = SlayerQuery(source_model="orders", measures=["customer_id", "revenue:sum"], dimensions=["status"])
        result = await engine._auto_move_fields_to_dimensions(query=query, model=model, named_queries={})
        assert len(result.measures) == 1
        dim_names = [d.name for d in result.dimensions]
        assert "status" in dim_names
        assert "customer_id" in dim_names

    async def test_dotted_named_measure_not_moved_via_named_queries(self, storage) -> None:
        """A dotted ref to a ``ModelMeasure`` on a model loaded via a named query stays in measures."""
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        # customers NOT saved to storage — only available as a named query result.
        customers = SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            ],
            measures=[ModelMeasure(name="name_count", formula="id:count_distinct")],
        )
        await storage.save_model(orders)

        engine = SlayerQueryEngine(storage=storage)
        original_resolve = engine._resolve_model

        async def patched_resolve(model_name, named_queries=None, _resolving=None, **kwargs):
            if model_name == "customers":
                return customers
            return await original_resolve(
                model_name=model_name,
                named_queries=named_queries,
                _resolving=_resolving,
                **kwargs,
            )

        engine._resolve_model = patched_resolve

        query = SlayerQuery(source_model="orders", measures=["customers.name_count", "revenue:sum"])
        result = await engine._auto_move_fields_to_dimensions(query=query, model=orders, named_queries={})
        # "customers.name_count" is a named ModelMeasure — stays in measures, not auto-moved.
        assert len(result.measures) == 2, (
            f"Expected 'customers.name_count' to stay in measures, but got {len(result.measures)}: "
            f"{[f.formula for f in result.measures]}"
        )


class TestInlineSQLJoins:
    """Cross-model dimensions must emit LEFT JOINs even when source model uses inline SQL.

    Regression tests from benchmark failures: the SQL generator used string-level
    FROM marker replacement to inject LEFT JOINs, which silently failed for models
    with inline SQL (sql field) because sqlglot's pretty-printed subquery didn't
    match the raw string.
    """

    @pytest.fixture
    def inline_orders(self):
        return SlayerModel(
            name="orders_inline",
            sql="SELECT id, customer_id, amount FROM raw_orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
Column(name="amount", sql="amount", type=DataType.DOUBLE)],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )

    @pytest.fixture
    def table_orders(self):
        return SlayerModel(
            name="orders_table",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
Column(name="amount", sql="amount", type=DataType.DOUBLE)],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )

    async def test_sql_table_baseline(self, generator: SQLGenerator, table_orders) -> None:
        """Sanity check: sql_table models emit LEFT JOIN correctly."""
        query = SlayerQuery(
            source_model="orders_table",
            measures=["amount:sum"],
            dimensions=["customers.name"],
        )
        sql = await _generate(generator, query, table_orders)
        assert "LEFT JOIN" in sql
        assert "customers" in sql

    async def test_inline_sql_cross_model_dimension(self, generator: SQLGenerator, inline_orders) -> None:
        """Mirrors benchmark Q2/Q5: inline-SQL source with a cross-model dimension."""
        query = SlayerQuery(
            source_model="orders_inline",
            measures=["amount:sum"],
            dimensions=["customers.name"],
        )
        sql = await _generate(generator, query, inline_orders)
        assert "LEFT JOIN" in sql, f"LEFT JOIN missing from inline-SQL model query:\n{sql}"
        assert "customers" in sql

    async def test_inline_sql_cross_model_dim_plus_local_measure(self, generator: SQLGenerator, inline_orders) -> None:
        """Mirrors benchmark Q1: inline-SQL source with both cross-model dim and local measure."""
        query = SlayerQuery(
            source_model="orders_inline",
            measures=["amount:avg"],
            dimensions=["customers.name"],
        )
        sql = await _generate(generator, query, inline_orders)
        assert "LEFT JOIN" in sql, f"LEFT JOIN missing:\n{sql}"
        assert "AVG(" in sql.upper()


class TestSelfReferencingPaths:
    """LLMs sometimes prefix cross-model paths with the source model name.

    e.g. on source_model='orders', writing 'orders.customers.name' instead of
    'customers.name'. The leading self-reference is stripped by the deterministic
    pre-processing step SlayerQuery.strip_source_model_prefix(), so internal
    resolution methods receive already-clean references.
    """

    @pytest.fixture
    def storage(self, tmp_path):

        return YAMLStorage(base_dir=str(tmp_path))

    @pytest.fixture
    async def engine_and_models(self, storage):
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
Column(name="amount", sql="amount", type=DataType.DOUBLE)],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        customers = SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
Column(name="score", sql="score", type=DataType.DOUBLE)],
        )
        await storage.save_model(orders)
        await storage.save_model(customers)
        engine = SlayerQueryEngine(storage=storage)
        return engine, orders

    async def test_self_ref_dimension_resolved_after_strip(self, engine_and_models) -> None:
        """'orders.customers.name' is pre-stripped to 'customers.name', then resolves correctly."""
        engine, model = engine_and_models
        query = SlayerQuery(source_model="orders", dimensions=["orders.customers.name"])
        stripped = query.strip_source_model_prefix()
        # After stripping, the dimension is "customers.name"
        assert stripped.dimensions[0].model == "customers"
        assert stripped.dimensions[0].name == "name"
        # Verify the engine can resolve the stripped path
        parts = stripped.dimensions[0].model.split(".") + [stripped.dimensions[0].name]
        dim = await engine._resolve_dimension_via_joins(model=model, parts=parts)
        assert dim is not None
        assert dim.name == "name"

    async def test_self_ref_measure_resolved_after_strip(self, engine_and_models) -> None:
        """'orders.customers.score:sum' is pre-stripped to 'customers.score:sum', then resolves."""
        engine, model = engine_and_models
        query = SlayerQuery(source_model="orders", measures=["orders.customers.score:sum"])
        stripped = query.strip_source_model_prefix()
        # After stripping, the formula is "customers.score:sum"
        assert stripped.measures[0].formula == "customers.score:sum"
        # Verify the engine can resolve the stripped cross-model measure
        result = await engine._resolve_cross_model_measure(
            spec_name="customers.score",
            field_name="score",
            model=model,
            query=stripped,
            dimensions=[], time_dimensions=[],
            aggregation_name="sum",
        )
        assert result.target_model_name == "customers"

    def test_simple_self_ref_dimension_stripped(self) -> None:
        """'orders.status' on source_model=orders becomes local 'status'."""
        query = SlayerQuery(source_model="orders", dimensions=["orders.status"])
        stripped = query.strip_source_model_prefix()
        assert stripped.dimensions[0].model is None
        assert stripped.dimensions[0].name == "status"


class TestConstantSQLFilters:
    """Filters on dimensions with constant/expression SQL must not be broken by table-qualifying."""

    async def test_local_filter_on_constant_dimension(self, generator: SQLGenerator) -> None:
        """Dimension with sql='1' should produce WHERE 1 = '1', not WHERE model.1 = '1'."""
        model = SlayerModel(
            name="premium",
            sql_table="Premium",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="has_premium", sql="1", type=DataType.DOUBLE),
Column(name="amount", sql="amount", type=DataType.DOUBLE)],
        )
        query = SlayerQuery(
            source_model="premium",
            measures=[ModelMeasure(formula="amount:sum")],
            filters=["has_premium = '1'"],
        )
        sql = await _generate(generator, query, model)
        assert "premium.1" not in sql, f"Constant SQL '1' was table-qualified: {sql}"
        # The constant should appear as a bare literal in WHERE
        assert "1 = '1'" in sql or "1 = 1" in sql

    async def test_cross_model_filter_on_constant_dimension(self, generator: SQLGenerator) -> None:
        """Cross-model filter premium.has_premium where has_premium sql='1' must not produce premium.1."""

        premium_model = SlayerModel(
            name="premium",
            sql_table="Premium",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="has_premium", sql="1", type=DataType.DOUBLE),
            ],
        )
        policy_amount = SlayerModel(
            name="policy_amount",
            sql_table="Policy_Amount",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="premium_id", sql="premium_id", type=DataType.DOUBLE),
Column(name="total", sql="amount", type=DataType.DOUBLE)],
            joins=[ModelJoin(target_model="premium", join_pairs=[["premium_id", "id"]])],
        )

        async def resolve_join_target(*, target_model_name, named_queries):
            if target_model_name == "premium":
                return ("Premium", premium_model)
            return None

        query = SlayerQuery(
            source_model="policy_amount",
            measures=[ModelMeasure(formula="total:sum")],
            filters=["premium.has_premium = '1'"],
        )
        enriched = await enrich_query(
            query=query,
            model=policy_amount,
            resolve_dimension_via_joins=_noop_async,
            resolve_cross_model_measure=_noop_async,
            resolve_join_target=resolve_join_target,
        )
        sql = generator.generate(enriched=enriched)
        assert "premium.1" not in sql, f"Constant SQL '1' was table-qualified: {sql}"
        assert "1 = '1'" in sql or "1 = 1" in sql

    async def test_local_filter_on_expression_dimension(self, generator: SQLGenerator) -> None:
        """Dimension with sql='COALESCE(x, 0)' should not be table-qualified."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="safe_amount", sql="COALESCE(amount, 0)", type=DataType.DOUBLE),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="revenue:sum")],
            filters=["safe_amount > 0"],
        )
        sql = await _generate(generator, query, model)
        assert "orders.COALESCE" not in sql, f"Expression SQL was table-qualified: {sql}"
        assert "COALESCE" in sql

    async def test_cross_model_filter_on_normal_dimension(self, generator: SQLGenerator) -> None:
        """Normal column-name dimensions must still be table-qualified (regression guard)."""

        customers = SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
            ],
        )
        orders = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )

        async def resolve_join_target(*, target_model_name, named_queries):
            if target_model_name == "customers":
                return ("customers", customers)
            return None

        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="revenue:sum")],
            filters=["customers.status = 'active'"],
        )
        enriched = await enrich_query(
            query=query,
            model=orders,
            resolve_dimension_via_joins=_noop_async,
            resolve_cross_model_measure=_noop_async,
            resolve_join_target=resolve_join_target,
        )
        sql = generator.generate(enriched=enriched)
        # Normal dimension should be qualified with the table alias
        assert "customers.status" in sql


class TestDimensionAggregation:
    """Dimensions can be aggregated with colon syntax (e.g., pk:count_distinct)."""

    async def test_count_distinct_on_pk_dimension(self, generator: SQLGenerator) -> None:
        """Primary key dimension with count_distinct should produce COUNT(DISTINCT col)."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="order_id", sql="order_id", type=DataType.DOUBLE, primary_key=True),

            ],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="order_id:count_distinct")],
        )
        sql = await _generate(generator, query, model)
        assert "COUNT(DISTINCT" in sql
        assert "order_id" in sql

    async def test_count_on_dimension(self, generator: SQLGenerator) -> None:
        """count on a dimension produces COUNT(col) for non-null counting."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),

            ],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="customer_id:count")],
        )
        sql = await _generate(generator, query, model)
        assert "COUNT(" in sql
        assert "customer_id" in sql

    async def test_min_max_on_string_dimension(self, generator: SQLGenerator) -> None:
        """min/max on string dimensions is allowed."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),

            ],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="status:min")],
        )
        sql = await _generate(generator, query, model)
        assert "MIN(" in sql

    async def test_sum_on_string_dimension_rejected(self, generator: SQLGenerator) -> None:
        """sum on a string dimension must be rejected."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),

            ],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="status:sum")],
        )
        with pytest.raises(ValueError, match="not applicable to TEXT column"):
            await _generate(generator, query, model)

    async def test_sum_on_number_dimension_allowed(self, generator: SQLGenerator) -> None:
        """sum on a numeric dimension is allowed."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="quantity", sql="qty", type=DataType.DOUBLE),

            ],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="quantity:sum")],
        )
        sql = await _generate(generator, query, model)
        assert "SUM(" in sql
        assert "qty" in sql

    async def test_dimension_count_distinct_in_formula(self, generator: SQLGenerator) -> None:
        """dimension:count_distinct inside a formula should work, not just as a standalone field."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),

                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[
                ModelMeasure(formula="revenue:sum / customer_id:count_distinct", name="rev_per_customer"),
            ],
        )
        sql = await _generate(generator, query, model)
        assert "COUNT(DISTINCT" in sql
        assert "SUM(" in sql
        assert "/" in sql

    async def test_cross_model_dimension_count_distinct_in_formula(self, generator: SQLGenerator) -> None:
        """cross-model dimension:count_distinct in a formula (e.g., policies.id:count_distinct)."""

        source = SlayerModel(
            name="amounts",
            sql_table="amounts",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),

                Column(name="total", sql="amount", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="policies", join_pairs=[["policy_id", "id"]])],
        )
        target = SlayerModel(
            name="policies",
            sql_table="policies",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="policy_number", sql="policy_number", type=DataType.TEXT),

            ],
        )

        # Use a real query engine so resolve_cross_model_measure works
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_model(source)
            await storage.save_model(target)
            engine = SlayerQueryEngine(storage=storage)

            query = SlayerQuery(
                source_model="amounts",
                measures=[
                    ModelMeasure(formula="total:sum / policies.id:count_distinct", name="avg_per_policy"),
                ],
            )
            enriched = await engine._enrich(query=query, model=source, named_queries={})
            sql = generator.generate(enriched=enriched)
            assert "COUNT(DISTINCT" in sql
            assert "SUM(" in sql
            assert "/" in sql


class TestCrossModelCustomAggFuncStyle:
    """Function-style syntax with custom aggregations from joined models."""

    async def test_funcstyle_custom_agg_on_joined_model(self, generator: SQLGenerator) -> None:
        """rolling_avg(customers.score) should rewrite and generate SQL."""

        orders = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        customers = SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
Column(name="score", sql="score", type=DataType.DOUBLE)],
            aggregations=[
                Aggregation(name="rolling_avg", formula="AVG({value})"),
            ],
        )

        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_model(orders)
            await storage.save_model(customers)
            engine = SlayerQueryEngine(storage=storage)

            query = SlayerQuery(
                source_model="orders",
                measures=["rolling_avg(customers.score)"],
                dimensions=[ColumnRef(name="status")],
            )
            enriched = await engine._enrich(query=query, model=orders, named_queries={})
            sql = generator.generate(enriched=enriched)
            _assert_valid_sql(sql, dialect=generator.dialect)
            assert "AVG(" in sql


class TestReachableAggDiscoveryUnbounded:
    """Custom aggregation discovery walks the full reachable join graph.

    Regression: ``_collect_reachable_agg_names`` previously stopped after 3 hops,
    so a custom aggregation defined on a 4-hop joined model was not in
    ``custom_agg_names`` and the function-style rewrite failed.
    """

    async def test_funcstyle_custom_agg_at_four_hops(self, generator: SQLGenerator) -> None:

        a = SlayerModel(
            name="a", sql_table="a", data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),

            ],
            joins=[ModelJoin(target_model="b", join_pairs=[["b_id", "id"]])],
        )
        b = SlayerModel(
            name="b", sql_table="b", data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),

            ],
            joins=[ModelJoin(target_model="c", join_pairs=[["c_id", "id"]])],
        )
        c = SlayerModel(
            name="c", sql_table="c", data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),

            ],
            joins=[ModelJoin(target_model="d", join_pairs=[["d_id", "id"]])],
        )
        d = SlayerModel(
            name="d", sql_table="d", data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),

            ],
            joins=[ModelJoin(target_model="e", join_pairs=[["e_id", "id"]])],
        )
        e = SlayerModel(
            name="e", sql_table="e", data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
Column(name="score", sql="score", type=DataType.DOUBLE)],
            aggregations=[Aggregation(name="rolling_avg", formula="AVG({value})")],
        )

        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            for m in (a, b, c, d, e):
                await storage.save_model(m)
            engine = SlayerQueryEngine(storage=storage)

            # rolling_avg lives 4 hops away (a → b → c → d → e). The
            # function-style rewrite must still recognise it.
            query = SlayerQuery(
                source_model="a",
                measures=["rolling_avg(b.c.d.e.score)"],
            )
            enriched = await engine._enrich(query=query, model=a, named_queries={})
            sql = generator.generate(enriched=enriched)
            _assert_valid_sql(sql, dialect=generator.dialect)
            assert "AVG(" in sql

    async def test_cycle_does_not_loop(self, generator: SQLGenerator) -> None:
        """BFS terminates on a → b → a cycle (visited guard)."""

        a = SlayerModel(
            name="a", sql_table="a", data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
Column(name="amount", sql="amount", type=DataType.DOUBLE)],
            aggregations=[Aggregation(name="rolling_a", formula="AVG({value})")],
            joins=[ModelJoin(target_model="b", join_pairs=[["b_id", "id"]])],
        )
        b = SlayerModel(
            name="b", sql_table="b", data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),

            ],
            joins=[ModelJoin(target_model="a", join_pairs=[["a_id", "id"]])],
        )

        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_model(a)
            await storage.save_model(b)
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery(source_model="a", measures=["rolling_a(amount)"])
            enriched = await engine._enrich(query=query, model=a, named_queries={})
            sql = generator.generate(enriched=enriched)
            _assert_valid_sql(sql, dialect=generator.dialect)


class TestTransformAmbiguousTimeDimension:
    """Time-dependent transforms must reject ambiguous time_dimension setups.

    Regression: ``_add_transform`` only checked ``not time_dimensions`` (empty).
    With 2+ time_dimensions and no main_time_dimension/default_time_dimension,
    ``_resolve_time_alias`` returns None but the transform was built anyway.
    """

    async def test_two_time_dims_no_disambiguation_raises(self, generator: SQLGenerator) -> None:

        m = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="updated_at", sql="updated_at", type=DataType.TIMESTAMP),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_model(m)
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery(
                source_model="orders",
                measures=["cumsum(revenue:sum)"],
                time_dimensions=[
                    TimeDimension(dimension="created_at", granularity=TimeGranularity.MONTH),
                    TimeDimension(dimension="updated_at", granularity=TimeGranularity.MONTH),
                ],
            )
            with pytest.raises(ValueError, match="time"):
                await engine._enrich(query=query, model=m, named_queries={})

    async def test_two_time_dims_with_main_succeeds(self, generator: SQLGenerator) -> None:
        """Disambiguation via main_time_dimension keeps the transform working."""

        m = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="updated_at", sql="updated_at", type=DataType.TIMESTAMP),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_model(m)
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery(
                source_model="orders",
                measures=["cumsum(revenue:sum)"],
                time_dimensions=[
                    TimeDimension(dimension="created_at", granularity=TimeGranularity.MONTH),
                    TimeDimension(dimension="updated_at", granularity=TimeGranularity.MONTH),
                ],
                main_time_dimension="created_at",
            )
            enriched = await engine._enrich(query=query, model=m, named_queries={})
            sql = generator.generate(enriched=enriched)
            _assert_valid_sql(sql, dialect=generator.dialect)


class TestParameterizedAggCanonicalDistinct:
    """Distinct parameterized aggregations must produce distinct hidden aliases.

    Regression: canonical key was f"{measure}_{agg}", ignoring agg_args/agg_kwargs.
    Two ORDER BY items like revenue:last(created_at) and revenue:last(updated_at)
    collapsed to the same alias and sorted by the same value.
    """

    async def test_order_by_two_last_with_different_time_cols(
        self, generator: SQLGenerator
    ) -> None:

        m = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="updated_at", sql="updated_at", type=DataType.TIMESTAMP),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_model(m)
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery(
                source_model="orders",
                measures=["*:count"],
                dimensions=[ColumnRef(name="status")],
                order=[
                    OrderItem(column="revenue:last(created_at)", direction="desc"),
                    OrderItem(column="revenue:last(updated_at)", direction="asc"),
                ],
            )
            enriched = await engine._enrich(query=query, model=m, named_queries={})
            sql = generator.generate(enriched=enriched)
            _assert_valid_sql(sql, dialect=generator.dialect)
            # The two ORDER BY columns must reference distinct hidden aliases.
            measure_aliases = [m.alias for m in enriched.measures]
            assert len(set(measure_aliases)) == len(measure_aliases), (
                f"Expected distinct measure aliases, got: {measure_aliases}"
            )
            order_cols = [item.column.name for item in (enriched.order or [])]
            assert len(set(order_cols)) == 2, (
                f"Expected two distinct ORDER BY canonical names, got: {order_cols}"
            )

    async def test_fields_two_percentiles_with_different_p(
        self, generator: SQLGenerator
    ) -> None:

        m = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_model(m)
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery(
                source_model="orders",
                measures=[
                    ModelMeasure(formula="revenue:percentile(p=0.5)", name="p50"),
                    ModelMeasure(formula="revenue:percentile(p=0.95)", name="p95"),
                ],
                dimensions=[ColumnRef(name="status")],
            )
            enriched = await engine._enrich(query=query, model=m, named_queries={})
            sql = generator.generate(enriched=enriched)
            _assert_valid_sql(sql, dialect=generator.dialect)
            # Two distinct percentile parameterizations must produce two distinct
            # EnrichedMeasures (not collapse via the canonical-name dedup).
            assert len(enriched.measures) == 2, (
                f"Expected 2 EnrichedMeasures for distinct percentiles, got "
                f"{len(enriched.measures)}: {[em.alias for em in enriched.measures]}"
            )
            measure_aliases = [em.alias for em in enriched.measures]
            assert len(set(measure_aliases)) == 2, (
                f"Expected distinct measure aliases for distinct percentiles, got: {measure_aliases}"
            )
            # Each measure's agg_kwargs must reflect its own p value.
            kwargs_seen = {tuple(sorted(em.agg_kwargs.items())) for em in enriched.measures}
            assert kwargs_seen == {(("p", "0.5"),), (("p", "0.95"),)}, (
                f"Expected distinct agg_kwargs per measure, got: {kwargs_seen}"
            )

    async def test_unparameterized_alias_unchanged(self, generator: SQLGenerator) -> None:
        """Backwards-compat: revenue:sum still produces orders.revenue_sum (no suffix)."""

        m = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_model(m)
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery(source_model="orders", measures=["revenue:sum"])
            enriched = await engine._enrich(query=query, model=m, named_queries={})
            assert any(em.alias == "orders.revenue_sum" for em in enriched.measures)

    async def test_star_count_alias_unchanged(self, generator: SQLGenerator) -> None:
        """Backwards-compat: *:count still produces orders._count."""

        m = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_model(m)
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery(source_model="orders", measures=["*:count"])
            enriched = await engine._enrich(query=query, model=m, named_queries={})
            assert any(em.alias == "orders._count" for em in enriched.measures)


class TestMultiHopCrossModelMeasure:
    """Multi-hop cross-model measures should walk the join chain to the final model."""

    async def test_two_hop_measure(self, generator: SQLGenerator) -> None:
        """policy_coverage_detail.claim_coverage.claim_amount.total_claim_amount:sum
        should walk policy_coverage_detail → claim_coverage → claim_amount."""

        pcd = SlayerModel(
            name="policy_coverage_detail",
            sql_table="policy_coverage_detail",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="coverage_code", sql="coverage_code", type=DataType.TEXT),

            ],
            joins=[ModelJoin(target_model="claim_coverage", join_pairs=[["id", "pcd_id"]])],
        )
        claim_cov = SlayerModel(
            name="claim_coverage",
            sql_table="claim_coverage",
            data_source="test",
            columns=[
                Column(name="pcd_id", sql="pcd_id", type=DataType.DOUBLE, primary_key=True),

            ],
            joins=[ModelJoin(target_model="claim_amount", join_pairs=[["claim_id", "claim_id"]])],
        )
        claim_amt = SlayerModel(
            name="claim_amount",
            sql_table="claim_amount",
            data_source="test",
            columns=[
                Column(name="claim_id", sql="claim_id", type=DataType.DOUBLE, primary_key=True),
Column(name="total_claim_amount", sql="amount", type=DataType.DOUBLE)],
        )

        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_model(pcd)
            await storage.save_model(claim_cov)
            await storage.save_model(claim_amt)
            engine = SlayerQueryEngine(storage=storage)

            query = SlayerQuery(
                source_model="policy_coverage_detail",
                measures=[ModelMeasure(formula="claim_coverage.claim_amount.total_claim_amount:sum")],
                dimensions=[ColumnRef(name="coverage_code")],
            )
            enriched = await engine._enrich(query=query, model=pcd, named_queries={})
            sql = generator.generate(enriched=enriched)
            _assert_valid_sql(sql, dialect=generator.dialect)
            assert "SUM(" in sql
            assert "claim_amount" in sql.lower()

    async def test_three_hop_measure(self, generator: SQLGenerator) -> None:
        """a.b.c.measure:sum should walk three hops."""

        model_a = SlayerModel(
            name="a", sql_table="a_table", data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                        Column(name="status", sql="status", type=DataType.TEXT),

            ], joins=[ModelJoin(target_model="b", join_pairs=[["b_id", "id"]])],
        )
        model_b = SlayerModel(
            name="b", sql_table="b_table", data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),

            ], joins=[ModelJoin(target_model="c", join_pairs=[["c_id", "id"]])],
        )
        model_c = SlayerModel(
            name="c", sql_table="c_table", data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),

            ], joins=[ModelJoin(target_model="d", join_pairs=[["d_id", "id"]])],
        )
        model_d = SlayerModel(
            name="d", sql_table="d_table", data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
Column(name="value", sql="val", type=DataType.DOUBLE)],
        )

        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            for m in (model_a, model_b, model_c, model_d):
                await storage.save_model(m)
            engine = SlayerQueryEngine(storage=storage)

            query = SlayerQuery(
                source_model="a",
                measures=[ModelMeasure(formula="b.c.d.value:sum")],
                dimensions=[ColumnRef(name="status")],
            )
            enriched = await engine._enrich(query=query, model=model_a, named_queries={})
            sql = generator.generate(enriched=enriched)
            _assert_valid_sql(sql, dialect=generator.dialect)
            assert "SUM(" in sql

    async def test_single_hop_still_works(self, generator: SQLGenerator) -> None:
        """Existing single-hop cross-model measures must not regress."""

        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                        Column(name="status", sql="status", type=DataType.TEXT),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        customers = SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
Column(name="score", sql="score", type=DataType.DOUBLE)],
        )

        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_model(orders)
            await storage.save_model(customers)
            engine = SlayerQueryEngine(storage=storage)

            query = SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="customers.score:sum")],
                dimensions=[ColumnRef(name="status")],
            )
            enriched = await engine._enrich(query=query, model=orders, named_queries={})
            sql = generator.generate(enriched=enriched)
            _assert_valid_sql(sql, dialect=generator.dialect)
            assert "SUM(" in sql


class TestCrossModelRerootedSubquery:
    """Tests for the re-rooted subquery approach to cross-model measure CTEs.

    When a cross-model measure is used, the CTE is generated with the target
    model as FROM, allowing all of the target model's joins to be available
    for filters and dimensions. Unreachable dims/filters are dropped.
    """

    @pytest.fixture
    def _models(self):
        """Shared model definitions for re-rooting tests."""
        policy = SlayerModel(
            name="policy", sql_table="policy", data_source="test",
            columns=[
                Column(name="policy_identifier", type=DataType.DOUBLE, primary_key=True),
                Column(name="policy_number", type=DataType.TEXT),
                Column(name="status_code", type=DataType.TEXT),
            ],
            joins=[
                ModelJoin(target_model="policy_amount", join_pairs=[["policy_identifier", "policy_identifier"]], join_type="inner"),
                ModelJoin(target_model="agreement_party_role", join_pairs=[["policy_identifier", "agreement_identifier"]], join_type="inner"),
            ],
        )
        policy_amount = SlayerModel(
            name="policy_amount", sql_table="policy_amount", data_source="test",
            columns=[
                Column(name="policy_amount_identifier", type=DataType.DOUBLE, primary_key=True),
                Column(name="effective_date", type=DataType.TIMESTAMP),
Column(name="total_policy_amount", sql="policy_amount", type=DataType.DOUBLE)],
            joins=[
                ModelJoin(target_model="policy", join_pairs=[["policy_identifier", "policy_identifier"]], join_type="inner"),
                ModelJoin(target_model="premium", join_pairs=[["policy_amount_identifier", "policy_amount_identifier"]], join_type="inner"),
                ModelJoin(target_model="agreement_party_role", join_pairs=[["policy_identifier", "agreement_identifier"]], join_type="inner"),
            ],
        )
        premium = SlayerModel(
            name="premium", sql_table="premium", data_source="test",
            columns=[
                Column(name="policy_amount_identifier", type=DataType.DOUBLE, primary_key=True),
                Column(name="has_premium", sql="1", type=DataType.TEXT),
            ],
        )
        agreement_party_role = SlayerModel(
            name="agreement_party_role", sql_table="agreement_party_role", data_source="test",
            columns=[
                Column(name="agreement_identifier", type=DataType.DOUBLE, primary_key=True),
                Column(name="party_role_code", type=DataType.TEXT),
            ],
        )
        return policy, policy_amount, premium, agreement_party_role

    @asynccontextmanager
    async def _setup_engine(self, *models):
        """Yield a SlayerQueryEngine backed by a temporary YAML storage dir.

        The temp directory is cleaned up automatically on context exit.
        """

        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            for m in models:
                await storage.save_model(m)
            yield SlayerQueryEngine(storage=storage)

    async def test_rerooted_cte_includes_target_join_filters(self, generator, _models):
        """Q9-style: filters on premium and agreement_party_role are included in CTE."""
        policy, policy_amount, premium, agreement_party_role = _models
        async with self._setup_engine(policy, policy_amount, premium, agreement_party_role) as engine:
            query = SlayerQuery(
                source_model="policy",
                measures=[ModelMeasure(formula="policy_amount.total_policy_amount:sum")],
                dimensions=[ColumnRef(name="policy_number")],
                filters=[
                    "agreement_party_role.party_role_code = 'PH'",
                    "policy_amount.premium.has_premium = '1'",
                ],
            )
            enriched = await engine._enrich(query=query, model=policy, named_queries={})
            sql = generator.generate(enriched=enriched)
            _assert_valid_sql(sql)

            # CTE should FROM policy_amount (target), not FROM policy (source)
            cm_cte_start = sql.find("_cm_")
            cte_section = sql[cm_cte_start:]
            assert "FROM policy_amount" in cte_section or "FROM\n  policy_amount" in cte_section
            # CTE should JOIN premium and agreement_party_role
            assert "premium" in cte_section
            assert "agreement_party_role" in cte_section
            # CTE should include both filter conditions
            assert "party_role_code" in cte_section
            # has_premium sql='1' resolves to literal 1
            assert "1 = '1'" in cte_section or "1 = 1" in cte_section

    async def test_rerooted_cte_without_filters(self, generator, _models):
        """Cross-model measure with no filters still uses re-rooted CTE."""
        policy, policy_amount, premium, agreement_party_role = _models
        async with self._setup_engine(policy, policy_amount, premium, agreement_party_role) as engine:
            query = SlayerQuery(
                source_model="policy",
                measures=[ModelMeasure(formula="policy_amount.total_policy_amount:sum")],
                dimensions=[ColumnRef(name="policy_number")],
            )
            enriched = await engine._enrich(query=query, model=policy, named_queries={})
            sql = generator.generate(enriched=enriched)
            _assert_valid_sql(sql)

            # CTE should still FROM policy_amount (re-rooted)
            cm_cte_start = sql.find("_cm_")
            cte_section = sql[cm_cte_start:]
            assert "FROM policy_amount" in cte_section or "FROM\n  policy_amount" in cte_section

    async def test_rerooted_unreachable_dims_and_filters_dropped(self, generator):
        """Unreachable dims/filters are dropped. CTE produces scalar CROSS JOIN."""
        # orders → customers join, but customers has NO join back to orders.
        # Dimension 'status' is on orders (unreachable from customers).
        # Filter on 'warehouse' is reachable from orders but not customers.
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", type=DataType.TEXT),

            ],
            joins=[
                ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
                ModelJoin(target_model="warehouse", join_pairs=[["warehouse_id", "id"]]),
            ],
        )
        customers = SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", type=DataType.DOUBLE, primary_key=True),
Column(name="score", sql="score", type=DataType.DOUBLE)],
        )
        warehouse = SlayerModel(
            name="warehouse", sql_table="warehouse", data_source="test",
            columns=[
                Column(name="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region", type=DataType.TEXT),
            ],
        )
        async with self._setup_engine(orders, customers, warehouse) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="customers.score:avg")],
                dimensions=[ColumnRef(name="status")],
                filters=["warehouse.region = 'US'"],
            )
            enriched = await engine._enrich(query=query, model=orders, named_queries={})
            sql = generator.generate(enriched=enriched)
            _assert_valid_sql(sql)

            # CTE: FROM customers, no GROUP BY (status unreachable), no warehouse filter
            cm_cte_start = sql.find("_cm_")
            cte_section = sql[cm_cte_start:sql.find(")\nSELECT", cm_cte_start)]
            assert "FROM customers" in cte_section or "FROM\n  customers" in cte_section
            assert "warehouse" not in cte_section.lower()
            assert "status" not in cte_section.lower()
            # Combined: CROSS JOIN (no shared dims)
            assert "CROSS JOIN" in sql

    async def test_rerooted_with_time_dimension(self, generator, _models):
        """Re-rooted CTE includes time dimension when reachable from target."""
        policy, policy_amount, premium, agreement_party_role = _models
        async with self._setup_engine(policy, policy_amount, premium, agreement_party_role) as engine:
            query = SlayerQuery(
                source_model="policy",
                measures=[ModelMeasure(formula="policy_amount.total_policy_amount:sum")],
                time_dimensions=[TimeDimension(
                    dimension=ColumnRef(name="policy_amount.effective_date"),
                    granularity=TimeGranularity.MONTH,
                )],
            )
            enriched = await engine._enrich(query=query, model=policy, named_queries={})
            sql = generator.generate(enriched=enriched)
            _assert_valid_sql(sql)

            # CTE should include effective_date with DATE_TRUNC
            cm_cte_start = sql.find("_cm_")
            cte_section = sql[cm_cte_start:]
            assert "effective_date" in cte_section.lower()
            assert "GROUP BY" in cte_section

    async def test_rerooted_cross_model_in_formula(self, generator, _models):
        """Formula mixing local + cross-model measure uses re-rooted CTE."""
        policy, policy_amount, premium, agreement_party_role = _models
        # Add a local column on policy that the formula will aggregate
        policy_with_measure = policy.model_copy(update={
            "columns": list(policy.columns) + [
                Column(name="number_of_policies", sql="1", type=DataType.DOUBLE),
            ],
        })
        async with self._setup_engine(policy_with_measure, policy_amount, premium, agreement_party_role) as engine:
            query = SlayerQuery(
                source_model="policy",
                measures=[ModelMeasure(
                    formula="number_of_policies:sum / policy_amount.total_policy_amount:sum",
                    name="ratio",
                )],
                dimensions=[ColumnRef(name="policy_number")],
            )
            enriched = await engine._enrich(query=query, model=policy_with_measure, named_queries={})
            sql = generator.generate(enriched=enriched)
            _assert_valid_sql(sql)

            # Should have both _base (with SUM for local measure) and _cm_ CTE
            assert "_base" in sql
            assert "_cm_" in sql
            assert "/" in sql  # Division expression

    async def test_rerooted_local_filter_remapped_to_source(self, generator, _models):
        """Unqualified filter on source model is remapped to source.col in CTE."""
        policy, policy_amount, premium, agreement_party_role = _models
        async with self._setup_engine(policy, policy_amount, premium, agreement_party_role) as engine:
            # policy_amount has a join to policy, so status_code is reachable
            query = SlayerQuery(
                source_model="policy",
                measures=[ModelMeasure(formula="policy_amount.total_policy_amount:sum")],
                dimensions=[ColumnRef(name="policy_number")],
                filters=["status_code = 'ACTIVE'"],
            )
            enriched = await engine._enrich(query=query, model=policy, named_queries={})
            sql = generator.generate(enriched=enriched)
            _assert_valid_sql(sql)

            # CTE should include the filter, qualified with the source model alias
            cm_cte_start = sql.find("_cm_")
            cte_section = sql[cm_cte_start:]
            assert "status_code" in cte_section.lower()
            assert "'ACTIVE'" in cte_section

    async def test_rerooted_custom_agg_in_filter(self, generator):
        """Function-style custom aggregation in filter must be recognised during rerooting."""
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", type=DataType.TEXT),
Column(name="amount", sql="amount", type=DataType.DOUBLE)],
            aggregations=[Aggregation(name="custom_sum", formula="SUM({value})")],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        customers = SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", type=DataType.TEXT),
Column(name="lifetime_value", sql="lifetime_value", type=DataType.DOUBLE)],
            joins=[ModelJoin(target_model="orders", join_pairs=[["id", "customer_id"]])],
        )
        async with self._setup_engine(orders, customers) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="customers.lifetime_value:sum")],
                dimensions=[ColumnRef(name="status")],
                filters=["custom_sum(amount) > 0"],
            )
            enriched = await engine._enrich(query=query, model=orders, named_queries={})
            sql = generator.generate(enriched=enriched)
            _assert_valid_sql(sql)


class TestOrderByCustomFieldName:
    """ORDER BY must work when fields have custom names via {"formula": ..., "name": ...}."""

    async def test_order_by_custom_name(self, generator: SQLGenerator) -> None:
        """Field with custom name 'num_customers' is the surfaced alias and
        ORDER BY references it directly (DEV-1335 — user ``name`` overrides
        the canonical ``customer_id_count_distinct`` form).
        """
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="customer_id:count_distinct", name="num_customers")],
            dimensions=[ColumnRef(name="status")],
            order=[OrderItem(column=ColumnRef(name="num_customers"), direction="desc")],
        )
        sql = await _generate(generator, query, model)
        assert "ORDER BY" in sql
        order_clause = sql.split("ORDER BY", 1)[1]
        # User name surfaces as the ORDER BY column.
        assert '"orders.num_customers"' in order_clause, (
            f"user alias not used in ORDER BY: {sql}"
        )
        # Canonical form must not leak into the ORDER BY clause.
        assert '"orders.customer_id_count_distinct"' not in order_clause, (
            f"canonical alias must not leak when user supplies 'name': {sql}"
        )
        assert "COUNT(DISTINCT" in sql

    async def test_order_by_canonical_name_still_works(self, generator: SQLGenerator) -> None:
        """ORDER BY with the canonical name (customer_id_count_distinct) still works."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="customer_id:count_distinct")],
            dimensions=[ColumnRef(name="status")],
            order=[OrderItem(column=ColumnRef(name="customer_id_count_distinct"), direction="asc")],
        )
        sql = await _generate(generator, query, model)
        assert "ORDER BY" in sql
        assert "ASC" in sql

    async def test_order_by_custom_name_in_computed_query(self, generator: SQLGenerator) -> None:
        """ORDER BY with custom name must resolve correctly in computed/transform queries.

        The _apply_pagination_to_sql path (used for expressions/transforms) must
        use _resolve_order_column, not raw model.name formatting.
        """
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[
                ModelMeasure(formula="customer_id:count_distinct", name="num_customers"),
                ModelMeasure(formula="cumsum(revenue:sum)", name="running_rev"),
            ],
            dimensions=[ColumnRef(name="status")],
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            order=[OrderItem(column=ColumnRef(name="num_customers"), direction="desc")],
        )
        sql = await _generate(generator, query, model)
        assert "ORDER BY" in sql
        order_clause = sql.split("ORDER BY", 1)[1]
        # User name is the ORDER BY column (DEV-1335 — user ``name`` overrides
        # the canonical form).
        assert '"orders.num_customers"' in order_clause, (
            f"user alias must surface in ORDER BY for computed query path:\n{sql}"
        )


class TestOrderByColonSyntax:
    """ORDER BY should accept colon-aggregation syntax like fields do."""

    async def test_order_by_local_measure_colon_syntax(self, generator: SQLGenerator) -> None:
        """ORDER BY 'revenue:sum' should resolve to the correct measure alias."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="revenue:sum")],
            dimensions=[ColumnRef(name="status")],
            order=[OrderItem(column="revenue:sum", direction="desc")],
        )
        sql = await _generate(generator, query, model)
        assert "ORDER BY" in sql
        assert "DESC" in sql

    async def test_order_by_star_count_colon_syntax(self, generator: SQLGenerator) -> None:
        """ORDER BY '*:count' should resolve to the _count alias."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            dimensions=[ColumnRef(name="status")],
            order=[OrderItem(column="*:count", direction="desc")],
        )
        sql = await _generate(generator, query, model)
        assert "ORDER BY" in sql
        assert "DESC" in sql

    async def test_order_by_single_hop_cross_model_colon_syntax(self, generator: SQLGenerator) -> None:
        """ORDER BY 'customers.score:sum' on a cross-model measure."""

        orders = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        customers = SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
Column(name="score", sql="score", type=DataType.DOUBLE)],
        )

        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_model(orders)
            await storage.save_model(customers)
            engine = SlayerQueryEngine(storage=storage)

            query = SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="customers.score:sum")],
                dimensions=[ColumnRef(name="status")],
                order=[OrderItem(column="customers.score:sum", direction="desc")],
            )
            enriched = await engine._enrich(query=query, model=orders, named_queries={})
            sql = generator.generate(enriched=enriched)
            _assert_valid_sql(sql, dialect=generator.dialect)
            assert "ORDER BY" in sql
            assert "DESC" in sql

    async def test_order_by_two_hop_dimension_with_colon_measure(self, generator: SQLGenerator) -> None:
        """ORDER BY a cross-model measure alongside a two-hop dimension."""

        orders = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        customers = SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
Column(name="score", sql="score", type=DataType.DOUBLE)],
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        )
        regions = SlayerModel(
            name="regions",
            sql_table="regions",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region_name", sql="region_name", type=DataType.TEXT),

            ],
        )

        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_model(orders)
            await storage.save_model(customers)
            await storage.save_model(regions)
            engine = SlayerQueryEngine(storage=storage)

            query = SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="customers.score:sum")],
                dimensions=[ColumnRef(name="customers.regions.region_name")],
                order=[OrderItem(column="customers.score:sum", direction="asc")],
            )
            enriched = await engine._enrich(query=query, model=orders, named_queries={})
            sql = generator.generate(enriched=enriched)
            _assert_valid_sql(sql, dialect=generator.dialect)
            assert "ORDER BY" in sql
            assert "ASC" in sql
            assert "regions" in sql  # two-hop dimension join was resolved


class TestOrderByFormulaEnrichment:
    """ORDER BY formulas should be enriched as hidden fields when not in fields."""

    async def test_order_by_formula_not_in_fields(self, generator: SQLGenerator) -> None:
        """ORDER BY 'revenue:sum' creates a hidden measure when not in fields."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            dimensions=[ColumnRef(name="status")],
            order=[OrderItem(column="revenue:sum", direction="desc")],
        )
        sql = await _generate(generator, query, model)
        assert "ORDER BY" in sql
        assert "DESC" in sql
        assert "SUM(" in sql  # hidden measure was created

    async def test_order_by_parameterized_agg(self, generator: SQLGenerator) -> None:
        """ORDER BY 'revenue:last(ordered_at)' strips arglist for name matching."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="ordered_at", sql="ordered_at", type=DataType.DATE),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[
                ModelMeasure(formula="*:count"),
                ModelMeasure(formula="revenue:last(ordered_at)"),
            ],
            dimensions=[ColumnRef(name="status")],
            time_dimensions=[TimeDimension(dimension="ordered_at", granularity="month")],
            order=[OrderItem(column="revenue:last(ordered_at)", direction="desc")],
        )
        sql = await _generate(generator, query, model)
        assert "ORDER BY" in sql
        assert "DESC" in sql


class TestJoinType:
    """join_type on ModelJoin controls LEFT vs INNER in generated SQL."""

    async def test_inner_join_generated(self, generator: SQLGenerator) -> None:
        """join_type='inner' produces INNER JOIN, not LEFT JOIN."""

        customers = SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
            ],
        )
        orders = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]], join_type="inner")],
        )

        async def resolve_join_target(*, target_model_name, named_queries):
            if target_model_name == "customers":
                return ("customers", customers)
            return None

        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="revenue:sum")],
            dimensions=[ColumnRef(name="customers.name")],
        )
        enriched = await enrich_query(
            query=query,
            model=orders,
            resolve_dimension_via_joins=_noop_async,
            resolve_cross_model_measure=_noop_async,
            resolve_join_target=resolve_join_target,
        )
        sql = generator.generate(enriched=enriched)
        assert "INNER JOIN" in sql
        assert "LEFT JOIN" not in sql


class TestMeasureFilterCrossModelJoin:
    """Measure filters referencing cross-model dimensions must trigger the join."""

    async def test_measure_filter_cross_model_constant_triggers_join(self, generator: SQLGenerator) -> None:
        """Measure filter 'loss_payment.has_flag = 1' where has_flag sql='1' must JOIN to loss_payment."""

        loss_payment = SlayerModel(
            name="loss_payment",
            sql_table="Loss_Payment",
            data_source="test",
            columns=[
                Column(name="id", sql="Claim_Amount_Identifier", type=DataType.DOUBLE, primary_key=True),
                Column(name="has_flag", sql="1", type=DataType.DOUBLE),
            ],
        )
        claim_amount = SlayerModel(
            name="claim_amount",
            sql_table="Claim_Amount",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),

                Column(name="loss_amt", sql="amount", filter="loss_payment.has_flag = 1", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="loss_payment", join_pairs=[["id", "Claim_Amount_Identifier"]])],
        )

        async def resolve_join_target(*, target_model_name, named_queries):
            if target_model_name == "loss_payment":
                return ("Loss_Payment", loss_payment)
            return None

        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_amt:sum")],
        )
        enriched = await enrich_query(
            query=query,
            model=claim_amount,
            resolve_dimension_via_joins=_noop_async,
            resolve_cross_model_measure=_noop_async,
            resolve_join_target=resolve_join_target,
        )
        sql = generator.generate(enriched=enriched)
        # The JOIN to loss_payment must be present for the filter to work
        assert "Loss_Payment" in sql, f"Missing JOIN to Loss_Payment: {sql}"
        assert "JOIN" in sql

    async def test_left_join_default(self, generator: SQLGenerator) -> None:
        """Default join_type produces LEFT JOIN."""

        customers = SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
            ],
        )
        orders = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )

        async def resolve_join_target(*, target_model_name, named_queries):
            if target_model_name == "customers":
                return ("customers", customers)
            return None

        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="revenue:sum")],
            dimensions=[ColumnRef(name="customers.name")],
        )
        enriched = await enrich_query(
            query=query,
            model=orders,
            resolve_dimension_via_joins=_noop_async,
            resolve_cross_model_measure=_noop_async,
            resolve_join_target=resolve_join_target,
        )
        sql = generator.generate(enriched=enriched)
        assert "LEFT JOIN" in sql
        assert "INNER JOIN" not in sql


class TestIsolatedFilteredMeasureCTEs:
    """Cross-model-filtered measures get isolated CTEs, not CASE WHEN in the base."""

    @pytest.fixture
    def claim_amount_model(self):
        return SlayerModel(
            name="claim_amount",
            sql_table="Claim_Amount",
            data_source="test",
            columns=[
                Column(name="claim_amount_id", sql="id", type=DataType.DOUBLE, primary_key=True),

                Column(name="loss_payment_amt", sql="amount", filter="loss_payment.has_flag = 1", type=DataType.DOUBLE),
                Column(name="loss_reserve_amt", sql="amount", filter="loss_reserve.has_flag = 1", type=DataType.DOUBLE),
                Column(name="total_amount", sql="amount", type=DataType.DOUBLE),
            ],
            joins=[
                ModelJoin(target_model="loss_payment", join_pairs=[["id", "claim_amount_id"]], join_type="inner"),
                ModelJoin(target_model="loss_reserve", join_pairs=[["id", "claim_amount_id"]], join_type="inner"),
                ModelJoin(target_model="claim", join_pairs=[["claim_id", "id"]]),
            ],
        )

    @pytest.fixture
    def related_models(self):
        return {
            "loss_payment": SlayerModel(
                name="loss_payment", sql_table="Loss_Payment", data_source="test",
                columns=[
                    Column(name="claim_amount_id", sql="Claim_Amount_Identifier", type=DataType.DOUBLE, primary_key=True),
                    Column(name="has_flag", sql="1", type=DataType.DOUBLE),
                ],
            ),
            "loss_reserve": SlayerModel(
                name="loss_reserve", sql_table="Loss_Reserve", data_source="test",
                columns=[
                    Column(name="claim_amount_id", sql="Claim_Amount_Identifier", type=DataType.DOUBLE, primary_key=True),
                    Column(name="has_flag", sql="1", type=DataType.DOUBLE),
                ],
            ),
            "claim": SlayerModel(
                name="claim", sql_table="Claim", data_source="test",
                columns=[
                    Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                    Column(name="claim_number", sql="claim_number", type=DataType.TEXT),
                ],
            ),
        }

    async def _enrich(self, claim_amount_model, related_models, query):

        async def resolve_join_target(*, target_model_name, named_queries):
            m = related_models.get(target_model_name)
            if m:
                return (m.sql_table, m)
            return None

        return await enrich_query(
            query=query,
            model=claim_amount_model,
            resolve_dimension_via_joins=_noop_async,
            resolve_cross_model_measure=_noop_async,
            resolve_join_target=resolve_join_target,
        )

    async def test_two_filtered_measures_get_separate_ctes(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """Two measures with different cross-model filters → separate CTEs, not intersecting JOINs."""
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum"), ModelMeasure(formula="loss_reserve_amt:sum")],
            dimensions=[ColumnRef(name="claim.claim_number")],
        )
        enriched = await self._enrich(claim_amount_model, related_models, query)
        sql = generator.generate(enriched=enriched)

        # Each filtered measure should have its own CTE
        assert "_fm_" in sql and "loss_payment_amt" in sql
        assert "loss_reserve_amt" in sql
        # Base query should NOT have both INNER JOINs (would intersect to zero rows)
        base_section = sql.split("_fm_")[0]
        assert "Loss_Payment" not in base_section or "Loss_Reserve" not in base_section

    async def test_formula_over_isolated_measures(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """Formula referencing isolated measures evaluates in the outer query."""
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[
                ModelMeasure(formula="loss_payment_amt:sum"),
                ModelMeasure(formula="loss_reserve_amt:sum"),
                ModelMeasure(formula="loss_payment_amt:sum + loss_reserve_amt:sum", name="total_loss"),
            ],
            dimensions=[ColumnRef(name="claim.claim_number")],
        )
        enriched = await self._enrich(claim_amount_model, related_models, query)
        sql = generator.generate(enriched=enriched)

        # Formula should be evaluated (contains + operator)
        assert "+" in sql
        # Both CTE names present
        assert "_fm_" in sql and "loss_payment_amt" in sql
        assert "loss_reserve_amt" in sql

    async def test_mixed_isolated_and_local_measures(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """Unfiltered measure stays in base, filtered goes to CTE."""
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="total_amount:sum"), ModelMeasure(formula="loss_payment_amt:sum")],
            dimensions=[ColumnRef(name="claim.claim_number")],
        )
        enriched = await self._enrich(claim_amount_model, related_models, query)
        sql = generator.generate(enriched=enriched)

        # Unfiltered measure (total_amount) should be in the _base CTE
        assert "_base" in sql
        assert "total_amount_sum" in sql
        # Filtered measure in its own CTE
        assert "_fm_" in sql and "loss_payment_amt" in sql

    async def test_all_measures_isolated_produces_dimension_spine(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """When all measures are isolated, base query is just a dimension spine."""
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum")],
            dimensions=[ColumnRef(name="claim.claim_number")],
        )
        enriched = await self._enrich(claim_amount_model, related_models, query)
        sql = generator.generate(enriched=enriched)

        # Base CTE should exist with dimensions but no SUM
        assert "_base" in sql
        assert "_fm_" in sql and "loss_payment_amt" in sql
        # The base should have GROUP BY for deduplication
        base_cte = sql.split("_fm_")[0]
        assert "GROUP BY" in base_cte

    async def test_combined_uses_cross_join_when_no_dimensions(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """When no dimensions exist, measure CTEs are CROSS JOINed to base (Bug Q6)."""
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum"), ModelMeasure(formula="loss_reserve_amt:sum")],
        )
        enriched = await self._enrich(claim_amount_model, related_models, query)
        sql = generator.generate(enriched=enriched)
        # Both isolated CTEs should be present
        assert "_fm_" in sql and "loss_payment_amt" in sql
        assert "loss_reserve_amt" in sql
        # With no dimensions, CROSS JOIN is needed (not LEFT JOIN with no ON)
        assert "CROSS JOIN" in sql

    async def test_filter_join_preserved_when_skip_isolated(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """Cross-model filter joins survive skip_isolated join stripping (Bug Q9).

        When an isolated measure triggers skip_isolated, the base query strips
        non-dimension joins. But query-level filters that reference cross-model
        paths still need their joins.
        """
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum")],
            # No dimensions on claim — only the filter references the claim join
            filters=["claim.claim_number = '12345'"],
        )
        enriched = await self._enrich(claim_amount_model, related_models, query)
        sql = generator.generate(enriched=enriched)
        # The base query WHERE clause references claim.claim_number
        assert "claim_number" in sql
        # The claim join must be present in the base query for the WHERE to work
        base_section = sql.split("_fm_")[0]
        assert "Claim" in base_section and "JOIN" in base_section

    async def test_isolated_cte_qualifies_cross_model_dim_correctly(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """Isolated CTEs qualify cross-model dimensions with dim.model_name (Bug Q11).

        The dimension claim.claim_number is on the 'claim' model. The isolated
        CTE must reference claim.claim_number, not claim_amount.claim_number.
        """
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum")],
            dimensions=[ColumnRef(name="claim.claim_number")],
        )
        enriched = await self._enrich(claim_amount_model, related_models, query)
        sql = generator.generate(enriched=enriched)
        # Extract the _fm CTE body (between _fm_ name and the next CTE/combined)
        fm_match = _re.search(r"_fm_\w*loss_payment_amt\w*", sql)
        assert fm_match, f"No _fm_ CTE for loss_payment_amt in:\n{sql}"
        fm_start = fm_match.start()
        fm_body = sql[fm_start:sql.index("\n)", fm_start)]
        # The dimension should use claim.claim_number, not claim_amount.claim_number
        assert "claim.claim_number" in fm_body, f"Expected claim.claim_number in CTE:\n{fm_body}"
        assert "claim_amount.claim_number" not in fm_body, (
            f"Found wrong table qualification claim_amount.claim_number in CTE:\n{fm_body}"
        )

    def test_cm_cte_skips_filters_on_unavailable_tables(self, generator: SQLGenerator) -> None:
        """Cross-model CTE WHERE must not include filters referencing tables it doesn't join (Bug Q9)."""

        # Build an EnrichedQuery with:
        # - A cross-model measure (source=orders, target=customers)
        # - A filter on "warehouse.status = 'ACTIVE'" (table not in the CM CTE)
        enriched = EnrichedQuery(
            model_name="orders",
            sql_table="Orders",
            dimensions=[
                EnrichedDimension(name="order_id", sql="order_id", type=DataType.DOUBLE, alias="orders.order_id", model_name="orders"),
            ],
            time_dimensions=[],
            measures=[],
            cross_model_measures=[
                CrossModelMeasure(
                    name="customer_score",
                    alias="orders.customers.customer_score_sum",
                    target_model_name="customers",
                    target_model_sql_table="Customers",
                    target_model_sql=None,
                    measure=EnrichedMeasure(
                        name="score", sql="score", alias="orders.customers.customer_score_sum",
                        aggregation="sum", model_name="customers",
                    ),
                    join_pairs=[["customer_id", "id"]],
                    shared_dimensions=[
                        EnrichedDimension(name="order_id", sql="order_id", type=DataType.DOUBLE, alias="orders.order_id", model_name="orders"),
                    ],
                    shared_time_dimensions=[],
                    source_model_name="orders",
                    source_sql_table="Orders",
                    source_sql=None,
                ),
            ],
            filters=[
                ParsedFilter(sql="warehouse.status = 'ACTIVE'", columns=["warehouse.status"]),
            ],
        )
        sql = generator.generate(enriched=enriched)

        # The _cm_ CTE should NOT reference "warehouse" (it only joins orders → customers)
        cm_start = sql.index("_cm_")
        cm_end = sql.index("\n)", cm_start)
        cm_body = sql[cm_start:cm_end]
        assert "warehouse" not in cm_body.lower(), (
            f"CM CTE references unavailable table 'warehouse':\n{cm_body}"
        )
        # The base query SHOULD have the filter
        base_section = sql[:cm_start]
        assert "warehouse" in base_section.lower()

    async def test_base_not_empty_when_no_dims_all_measures_skipped(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """Base SELECT must not be empty when all measures are isolated and there are no dims (Bug Q10)."""
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum")],
            # No dimensions — base has nothing to select
        )
        enriched = await self._enrich(claim_amount_model, related_models, query)
        sql = generator.generate(enriched=enriched)
        # Must not have an empty SELECT clause
        assert "SELECT\nFROM" not in sql, f"Empty SELECT detected:\n{sql}"
        # Should still produce valid SQL with the measure CTE
        assert "_fm_" in sql and "loss_payment_amt" in sql

    async def test_having_filter_on_isolated_measure_applied_in_base(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """HAVING filter on an isolated measure is correctly applied in the base CTE."""
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum")],
            dimensions=[ColumnRef(name="claim.claim_number")],
            filters=["loss_payment_amt:sum > 1000"],
        )
        enriched = await self._enrich(claim_amount_model, related_models, query)
        sql = generator.generate(enriched=enriched)
        # HAVING must appear in the base CTE (not dropped)
        assert "HAVING" in sql, f"HAVING filter dropped:\n{sql}"
        assert "1000" in sql

    async def test_same_filtered_measure_different_aggs_separate_ctes(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """Same filtered measure with sum + avg must produce distinct CTEs, not collide."""
        loss_m = claim_amount_model.get_column("loss_payment_amt")
        loss_m.allowed_aggregations = ["sum", "avg"]

        query = SlayerQuery(
            source_model="claim_amount",
            measures=[
                ModelMeasure(formula="loss_payment_amt:sum"),
                ModelMeasure(formula="loss_payment_amt:avg"),
            ],
            dimensions=[ColumnRef(name="claim.claim_number")],
        )
        enriched = await self._enrich(claim_amount_model, related_models, query)
        sql = generator.generate(enriched=enriched)

        # Both aliases must be present in the final SQL
        assert "loss_payment_amt_sum" in sql, f"Missing loss_payment_amt_sum in:\n{sql}"
        assert "loss_payment_amt_avg" in sql, f"Missing loss_payment_amt_avg in:\n{sql}"
        # The two filtered measures must have distinct CTE names (no duplicate _fm_ CTEs)
        fm_cte_names = _re.findall(r"(_fm_\w+)\s+AS\s*\(", sql)
        assert len(fm_cte_names) == len(set(fm_cte_names)), (
            f"Duplicate _fm_ CTE names: {fm_cte_names}\n{sql}"
        )
        assert len(fm_cte_names) == 2, f"Expected 2 _fm_ CTEs, got {len(fm_cte_names)}: {fm_cte_names}"

    # --- Isolated first/last measures (Issue #40) ---

    @pytest.fixture
    def claim_amount_model_with_time(self, claim_amount_model):
        """Extend claim_amount_model with a timestamp dimension and first/last measures."""
        claim_amount_model.default_time_dimension = "created_at"
        claim_amount_model.columns.append(
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        )
        claim_amount_model.columns.append(
            Column(name="latest_payment", sql="amount", filter="loss_payment.has_flag = 1", type=DataType.DOUBLE),
        )
        return claim_amount_model

    async def test_isolated_last_no_ranked_subquery_in_base(
        self, generator: SQLGenerator, claim_amount_model_with_time, related_models,
    ) -> None:
        """Bug 1: When ALL first/last measures are isolated, base must NOT build
        a ranked subquery — it should be a plain dimension spine."""
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="latest_payment:last")],
            dimensions=[ColumnRef(name="claim.claim_number")],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
        )
        enriched = await self._enrich(claim_amount_model_with_time, related_models, query)
        sql = generator.generate(enriched=enriched)

        # Extract the _base CTE body
        assert "_base" in sql, f"Expected _base CTE in:\n{sql}"
        base_start = sql.index("_base AS")
        base_end = sql.index("\n)", base_start)
        base_body = sql[base_start:base_end]

        # Base must NOT have ROW_NUMBER — no ranked subquery needed
        assert "ROW_NUMBER" not in base_body, (
            f"Redundant ROW_NUMBER in _base when all first/last are isolated:\n{base_body}"
        )
        # Base must NOT have a subquery FROM (SELECT ...)
        assert "FROM (" not in base_body, (
            f"Redundant ranked subquery in _base:\n{base_body}"
        )

    async def test_isolated_last_cte_has_valid_ranked_subquery(
        self, generator: SQLGenerator, claim_amount_model_with_time, related_models,
    ) -> None:
        """Bug 2: The isolated CTE for a last measure must contain a ROW_NUMBER
        ranked subquery and produce valid SQL (not reference non-existent _last_rn)."""
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="latest_payment:last")],
            dimensions=[ColumnRef(name="claim.claim_number")],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
        )
        enriched = await self._enrich(claim_amount_model_with_time, related_models, query)
        sql = generator.generate(enriched=enriched)

        # The _fm_ CTE must exist and contain ROW_NUMBER
        fm_match = _re.search(r"(_fm_\w*latest_payment\w*)\s+AS\s*\(", sql)
        assert fm_match, f"No _fm_ CTE for latest_payment in:\n{sql}"
        fm_start = fm_match.start()
        fm_end = sql.index("\n)", fm_start)
        fm_body = sql[fm_start:fm_end]

        assert "ROW_NUMBER" in fm_body, (
            f"_fm_ CTE for latest_payment must contain ROW_NUMBER:\n{fm_body}"
        )
        assert "_last_rn" in fm_body, (
            f"_fm_ CTE must have _last_rn column:\n{fm_body}"
        )
        # The aggregate must use MAX(CASE WHEN _last_rn = 1 ...)
        assert "MAX(CASE WHEN" in fm_body, (
            f"_fm_ CTE must use MAX(CASE WHEN _last_rn = 1 ...):\n{fm_body}"
        )
        # Full SQL must parse as valid
        _assert_valid_sql(sql)

    async def test_mixed_isolated_and_local_first_last(
        self, generator: SQLGenerator, claim_amount_model_with_time, related_models,
    ) -> None:
        """Mixed case: one non-isolated last stays in base with ranked subquery,
        one isolated last goes to its own CTE with its own ranked subquery."""
        # total_amount has no cross-model filter → stays in base
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[
                ModelMeasure(formula="total_amount:last"),
                ModelMeasure(formula="latest_payment:last"),
            ],
            dimensions=[ColumnRef(name="claim.claim_number")],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
        )
        enriched = await self._enrich(claim_amount_model_with_time, related_models, query)
        sql = generator.generate(enriched=enriched)

        # Base query SHOULD have ROW_NUMBER (for the non-isolated total_amount:last)
        base_start = sql.index("_base AS")
        base_end = sql.index("\n)", base_start)
        base_body = sql[base_start:base_end]
        assert "ROW_NUMBER" in base_body, (
            f"Base must have ROW_NUMBER for non-isolated last measure:\n{base_body}"
        )

        # Isolated measure should get its own _fm_ CTE
        fm_match = _re.search(r"_fm_\w*latest_payment\w*", sql)
        assert fm_match, f"No _fm_ CTE for latest_payment in:\n{sql}"

        # Non-isolated measure should be in the base
        assert "total_amount" in base_body, (
            f"Non-isolated total_amount should be in base:\n{base_body}"
        )

        # Full SQL must be valid
        _assert_valid_sql(sql)

    async def test_isolated_first_with_explicit_time_column(
        self, generator: SQLGenerator, claim_amount_model_with_time, related_models,
    ) -> None:
        """Isolated first measure with explicit time_column uses correct ordering."""
        # Add a timestamp dimension and measure for the explicit time column
        claim_amount_model_with_time.columns.append(
            Column(name="updated_at", sql="updated_at", type=DataType.TIMESTAMP),
        )
        claim_amount_model_with_time.columns.append(
            Column(name="earliest_reserve", sql="amount", filter="loss_reserve.has_flag = 1", type=DataType.DOUBLE),
        )
        # Explicit time column specified at query time: first(updated_at)
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="earliest_reserve:first(updated_at)")],
            dimensions=[ColumnRef(name="claim.claim_number")],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
        )
        enriched = await self._enrich(claim_amount_model_with_time, related_models, query)
        sql = generator.generate(enriched=enriched)

        # The _fm_ CTE should use first (ASC ordering)
        fm_match = _re.search(r"(_fm_\w*earliest_reserve\w*)\s+AS\s*\(", sql)
        assert fm_match, f"No _fm_ CTE for earliest_reserve in:\n{sql}"
        fm_start = fm_match.start()
        fm_end = sql.index("\n)", fm_start)
        fm_body = sql[fm_start:fm_end]

        assert "_first_rn" in fm_body, (
            f"_fm_ CTE should use _first_rn for 'first' aggregation:\n{fm_body}"
        )
        # ASC ordering for first
        assert "ASC" in fm_body, f"Expected ASC ordering for first:\n{fm_body}"
        # Should reference the explicit time column (updated_at), not default
        assert "updated_at" in fm_body, (
            f"Expected explicit time_column 'updated_at' in _fm_ CTE:\n{fm_body}"
        )
        _assert_valid_sql(sql)

    async def test_multiple_isolated_first_last_separate_ctes(
        self, generator: SQLGenerator, claim_amount_model_with_time, related_models,
    ) -> None:
        """Two isolated first/last measures produce separate CTEs, no ROW_NUMBER in base."""
        # latest_payment already has cross-model filter; add another
        claim_amount_model_with_time.columns.append(
            Column(name="latest_reserve", sql="amount", filter="loss_reserve.has_flag = 1", type=DataType.DOUBLE),
        )
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[
                ModelMeasure(formula="latest_payment:last"),
                ModelMeasure(formula="latest_reserve:last"),
            ],
            dimensions=[ColumnRef(name="claim.claim_number")],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
        )
        enriched = await self._enrich(claim_amount_model_with_time, related_models, query)
        sql = generator.generate(enriched=enriched)

        # No ROW_NUMBER in base
        base_start = sql.index("_base AS")
        base_end = sql.index("\n)", base_start)
        base_body = sql[base_start:base_end]
        assert "ROW_NUMBER" not in base_body, (
            f"No ROW_NUMBER should be in base when all first/last are isolated:\n{base_body}"
        )

        # Two separate _fm_ CTEs
        fm_cte_names = _re.findall(r"(_fm_\w+)\s+AS\s*\(", sql)
        assert len(fm_cte_names) == 2, (
            f"Expected 2 _fm_ CTEs, got {len(fm_cte_names)}: {fm_cte_names}\n{sql}"
        )
        # Each should have ROW_NUMBER
        for fm_name in fm_cte_names:
            fm_start = sql.index(f"{fm_name} AS")
            fm_end = sql.index("\n)", fm_start)
            fm_body = sql[fm_start:fm_end]
            assert "ROW_NUMBER" in fm_body, (
                f"CTE {fm_name} must have ROW_NUMBER:\n{fm_body}"
            )

        _assert_valid_sql(sql)

    def test_same_cm_measure_different_aggs_separate_ctes(self, generator: SQLGenerator) -> None:
        """Same cross-model measure with sum + avg must produce distinct CTEs."""

        dim = EnrichedDimension(
            name="order_id", sql="order_id", type=DataType.DOUBLE,
            alias="orders.order_id", model_name="orders",
        )
        enriched = EnrichedQuery(
            model_name="orders",
            sql_table="Orders",
            columns=[dim],
            time_dimensions=[],
            measures=[],
            cross_model_measures=[
                CrossModelMeasure(
                    name="customer_revenue_sum",
                    alias="orders.customers.revenue_sum",
                    target_model_name="customers",
                    target_model_sql_table="Customers",
                    target_model_sql=None,
                    measure=EnrichedMeasure(
                        name="revenue", sql="revenue", alias="orders.customers.revenue_sum",
                        aggregation="sum", model_name="customers",
                    ),
                    join_pairs=[["customer_id", "id"]],
                    shared_dimensions=[dim],
                    shared_time_dimensions=[],
                    source_model_name="orders",
                    source_sql_table="Orders",
                    source_sql=None,
                ),
                CrossModelMeasure(
                    name="customer_revenue_avg",
                    alias="orders.customers.revenue_avg",
                    target_model_name="customers",
                    target_model_sql_table="Customers",
                    target_model_sql=None,
                    measure=EnrichedMeasure(
                        name="revenue", sql="revenue", alias="orders.customers.revenue_avg",
                        aggregation="avg", model_name="customers",
                    ),
                    join_pairs=[["customer_id", "id"]],
                    shared_dimensions=[dim],
                    shared_time_dimensions=[],
                    source_model_name="orders",
                    source_sql_table="Orders",
                    source_sql=None,
                ),
            ],
            filters=[],
        )
        sql = generator.generate(enriched=enriched)

        # Both aliases must be present
        assert "revenue_sum" in sql, f"Missing revenue_sum in:\n{sql}"
        assert "revenue_avg" in sql, f"Missing revenue_avg in:\n{sql}"
        # Two distinct CM CTE definitions
        cm_cte_names = _re.findall(r"(_cm_\w+)\s+AS\s*\(", sql)
        assert len(cm_cte_names) == 2, f"Expected 2 _cm_ CTEs, got {len(cm_cte_names)}: {cm_cte_names}\n{sql}"
        assert cm_cte_names[0] != cm_cte_names[1], f"CTE names collide: {cm_cte_names}\n{sql}"


class TestCteNameSanitization:
    """CTE names from aliases must be collision-free."""

    def test_dot_vs_underscore_no_collision(self) -> None:
        """Aliases differing only in dot/underscore placement produce distinct CTE names."""

        name_a = _cte_name_from_alias("_fm_", "a.b_c")
        name_b = _cte_name_from_alias("_fm_", "a_b.c")
        assert name_a != name_b, f"Collision: {name_a!r} == {name_b!r}"
        # a.b_c → _fm_a__b_c, a_b.c → _fm_a_b__c
        assert name_a == "_fm_a__b_c"
        assert name_b == "_fm_a_b__c"


class TestGetColumnTypesSql:
    """get_column_types must build valid SQL for expression measures."""

    async def test_expression_measure_sql_not_corrupted(self) -> None:
        """Expression measures like COALESCE(amount, 0) must not get model.name prepended."""


        storage = YAMLStorage(base_dir=tempfile.mkdtemp())
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),

                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
                Column(name="safe_amount", sql="COALESCE(amount, 0)", type=DataType.DOUBLE),
            ],
        )
        with patch.object(storage, "get_model", new_callable=AsyncMock, return_value=model):
            engine = SlayerQueryEngine(storage=storage)
            mock_ds = MagicMock()
            mock_ds.get_connection_string.return_value = "sqlite://"
            mock_ds.type = "sqlite"
            with patch.object(engine, "_resolve_datasource", new_callable=AsyncMock, return_value=mock_ds):
                captured_sql = []

                async def capture_sql(sql):
                    captured_sql.append(sql)
                    return {}

                mock_client = MagicMock()
                mock_client.get_column_types = capture_sql
                engine._sql_clients["sqlite://"] = mock_client

                await engine.get_column_types("orders")

        assert captured_sql, "get_column_types did not call client"
        sql = captured_sql[0]
        # Expression measure must NOT be corrupted: "orders.COALESCE(amount, 0)" is invalid
        assert "orders.COALESCE" not in sql, f"Expression measure corrupted:\n{sql}"
        # Bare measure should be qualified
        assert "orders.amount" in sql
        # Bare ``amount`` inside the expression now qualifies to the model's
        # alias (``orders.amount``) — derived-ref expansion (DEV-1333) makes
        # every base-column reference unambiguous.
        assert "COALESCE(orders.amount, 0)" in sql

    async def test_cross_model_measures_probed_via_engine(self) -> None:
        """Cross-model measures should be probed via the engine's enrich+generate pipeline."""


        storage = YAMLStorage(base_dir=tempfile.mkdtemp())
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),

                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
                Column(name="customer_score", sql="customers.score", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )

        # The enriched query that _enrich would produce
        mock_enriched = EnrichedQuery(
            model_name="orders", sql_table="public.orders",
            measures=[
                EnrichedMeasure(name="revenue_max", sql="amount", alias="orders.revenue_max",
                                aggregation="max", model_name="orders", source_measure_name="revenue"),
                EnrichedMeasure(name="customer_score_max", sql="customers.score",
                                alias="orders.customer_score_max", aggregation="max",
                                model_name="orders", source_measure_name="customer_score"),
            ],
        )

        with patch.object(storage, "get_model", new_callable=AsyncMock, return_value=model):
            engine = SlayerQueryEngine(storage=storage)
            mock_ds = MagicMock()
            mock_ds.get_connection_string.return_value = "sqlite://"
            mock_ds.type = "sqlite"

            with patch.object(engine, "_resolve_datasource", new_callable=AsyncMock, return_value=mock_ds), \
                 patch.object(engine, "_enrich", new_callable=AsyncMock, return_value=mock_enriched):

                async def capture_types(sql):
                    return {"orders.revenue_max": "number", "orders.customer_score_max": "number"}

                mock_client = MagicMock()
                mock_client.get_column_types = capture_types
                engine._sql_clients["sqlite://"] = mock_client

                result = await engine.get_column_types("orders")

        # Both measures should have types (cross-model included)
        assert result.get("revenue") == "number", f"Missing revenue type: {result}"
        assert result.get("customer_score") == "number", f"Missing customer_score type: {result}"

    def test_explicit_empty_allowed_aggregations_skips_probe(self) -> None:
        """An explicit empty allowed_aggregations must NOT fall back to type defaults."""

        storage = YAMLStorage(base_dir=tempfile.mkdtemp())
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
                Column(
                    name="opaque",
                    sql="amount",
                    type=DataType.DOUBLE,
                    allowed_aggregations=[],
                ),
            ],
        )
        engine = SlayerQueryEngine(storage=storage)
        probe = engine._build_type_probe_query(model)
        formulas = [m.formula for m in probe.measures]
        assert any(f and f.startswith("revenue:") for f in formulas), (
            f"Expected 'revenue' to be probed, got {formulas}"
        )
        assert not any(f and f.startswith("opaque:") for f in formulas), (
            f"Empty allowed_aggregations must skip probe, got {formulas}"
        )

# ---------------------------------------------------------------------------
# DEV-1336 — window functions in filters (single-stage)
# ---------------------------------------------------------------------------


@pytest.fixture
def planets_model() -> SlayerModel:
    """Model with a Column.sql containing a window function (top-N rank pattern)."""
    return SlayerModel(
        name="planets",
        sql_table="planets",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="mass", sql="mass", type=DataType.DOUBLE),
            Column(
                name="rn",
                sql="row_number() over (order by mass desc)",
                type=DataType.DOUBLE,
            ),
        ],
    )


class TestWindowFunctionInFilter:
    """DEV-1369 (reverses DEV-1336): a query filter that references a
    ``Column`` whose ``sql`` contains a window function (``OVER (...)``)
    no longer auto-promotes to a post-aggregation outer WHERE. The
    rank-family transforms (``rank`` / ``percent_rank`` / ``dense_rank``
    / ``ntile``) cover top-N filtering in pure DSL, so the escape hatch
    is redundant. The engine raises a clear error directing the user to
    those transforms or to a multi-stage source_queries model.

    Raw ``OVER (...)`` text in query filters or measure formulas still
    raises (preserved from DEV-1336).
    """

    async def test_filter_on_windowed_column_raises(
        self, generator: SQLGenerator, planets_model: SlayerModel,
    ) -> None:
        """Filtering on a Column whose sql contains a window function
        raises with an actionable message at enrichment time."""
        query = SlayerQuery(
            source_model="planets",
            dimensions=["name"],
            filters=["rn <= 3"],
        )
        with pytest.raises(ValueError) as excinfo:
            await _generate(generator=generator, query=query, model=planets_model)
        msg = str(excinfo.value).lower()
        assert "window function" in msg or "rank" in msg, (
            f"Expected the message to mention 'window function' and/or "
            f"'rank' suggestion. Got: {excinfo.value}"
        )

    async def test_select_only_on_windowed_column_unchanged(
        self, generator: SQLGenerator, planets_model: SlayerModel,
    ) -> None:
        """A windowed Column.sql is still legal as a *projection* — only
        as a filter target does it now error."""
        query = SlayerQuery(
            source_model="planets",
            dimensions=["name", "rn"],
        )
        sql = await _generate(generator=generator, query=query, model=planets_model)
        assert "AS _filtered" not in sql, (
            f"No post-filter wrap should be introduced when there is no "
            f"window filter.\nsql:\n{sql}"
        )


# ---------------------------------------------------------------------------
# DEV-1361: Type-aware CAST emission driven by Column.type / ModelMeasure.type.
# ---------------------------------------------------------------------------


class TestCastEmissionColumn:
    """``Column.type`` declares the result type of the column expression; the
    generator wraps non-bare ``Column.sql`` in ``CAST(... AS <type>)``. Bare
    identifiers and ``sql=None`` paths are NOT wrapped (trust DB schema +
    sqlglot). ``DataType.TEXT`` is a no-op wrapper (cosmetic).
    """

    @pytest.fixture
    def items_model_factory(self):
        def make(*, blob_type: DataType) -> SlayerModel:
            return SlayerModel(
                name="items",
                sql_table="public.items",
                data_source="test",
                columns=[
                    Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                    Column(
                        name="x",
                        sql="json_extract(blob, '$.x')",
                        type=blob_type,
                    ),
                ],
            )

        return make

    async def test_double_wraps_json_extract_postgres(self, items_model_factory) -> None:
        model = items_model_factory(blob_type=DataType.DOUBLE)
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(source_model="items", dimensions=[ColumnRef(name="x")])
        sql = await _generate(gen, query, model)
        assert "CAST(JSON_EXTRACT(" in _norm(sql).upper() or "CAST(JSON" in _norm(sql).upper()
        assert "DOUBLE" in sql.upper()

    async def test_double_wraps_json_extract_sqlite(self, items_model_factory) -> None:
        model = items_model_factory(blob_type=DataType.DOUBLE)
        gen = SQLGenerator(dialect="sqlite")
        query = SlayerQuery(source_model="items", dimensions=[ColumnRef(name="x")])
        sql = await _generate(gen, query, model)
        assert "CAST(" in sql.upper()
        assert "REAL" in sql.upper()

    async def test_int_wraps_non_bare_sql_sqlite(self) -> None:
        model = SlayerModel(
            name="items",
            sql_table="public.items",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="name_len", sql="length(name)", type=DataType.INT),
            ],
        )
        gen = SQLGenerator(dialect="sqlite")
        query = SlayerQuery(source_model="items", dimensions=[ColumnRef(name="name_len")])
        sql = await _generate(gen, query, model)
        # SQLite: CAST(... AS INTEGER) — sqlglot transpiles INT → INTEGER.
        assert "CAST(" in sql.upper()
        assert "INTEGER" in sql.upper()

    async def test_boolean_wraps_non_bare(self, items_model_factory) -> None:
        model = items_model_factory(blob_type=DataType.BOOLEAN)
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(source_model="items", dimensions=[ColumnRef(name="x")])
        sql = await _generate(gen, query, model)
        assert "CAST(" in sql.upper()
        assert "BOOLEAN" in sql.upper()

    async def test_timestamp_wraps_non_bare(self, items_model_factory) -> None:
        model = items_model_factory(blob_type=DataType.TIMESTAMP)
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(source_model="items", dimensions=[ColumnRef(name="x")])
        sql = await _generate(gen, query, model)
        assert "CAST(" in sql.upper()
        assert "TIMESTAMP" in sql.upper()

    async def test_date_wraps_non_bare(self, items_model_factory) -> None:
        model = items_model_factory(blob_type=DataType.DATE)
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(source_model="items", dimensions=[ColumnRef(name="x")])
        sql = await _generate(gen, query, model)
        assert "CAST(" in sql.upper()
        assert " DATE" in sql.upper()

    async def test_text_skips_cast(self, items_model_factory) -> None:
        """TEXT is the no-cast type — emission is unchanged from today."""
        model = items_model_factory(blob_type=DataType.TEXT)
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(source_model="items", dimensions=[ColumnRef(name="x")])
        sql = await _generate(gen, query, model)
        # JSON_EXTRACT call appears bare — no CAST wrapping it.
        assert "JSON_EXTRACT" in sql.upper()
        # No cast-to-text/varchar wrapper.
        assert "AS TEXT" not in sql.upper()
        assert "AS VARCHAR" not in sql.upper()

    async def test_bare_identifier_not_wrapped(self) -> None:
        """Bare ``sql='amount'`` (and ``sql=None``) trust the DB schema and
        sqlglot — no CAST emitted regardless of declared type."""
        model = SlayerModel(
            name="items",
            sql_table="public.items",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="qty", sql=None, type=DataType.INT),
            ],
        )
        gen = SQLGenerator(dialect="sqlite")
        query = SlayerQuery(
            source_model="items",
            dimensions=[ColumnRef(name="amount"), ColumnRef(name="qty")],
        )
        sql = await _generate(gen, query, model)
        assert "CAST(" not in sql.upper()

    async def test_idempotent_cast(self) -> None:
        """If the user pre-wrapped ``Column.sql`` in a CAST to the same target,
        the generator does NOT double-wrap."""
        model = SlayerModel(
            name="items",
            sql_table="public.items",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(
                    name="x",
                    sql="CAST(json_extract(blob, '$.x') AS DOUBLE)",
                    type=DataType.DOUBLE,
                ),
            ],
        )
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(source_model="items", dimensions=[ColumnRef(name="x")])
        sql = await _generate(gen, query, model)
        # Exactly one CAST in the projection. Any double-wrap would produce
        # CAST(CAST(... AS ...) AS ...) — assert that pattern is absent.
        assert "CAST(CAST(" not in sql.upper()


class TestCastEmissionMeasure:
    """``ModelMeasure.type`` (when set) wraps the aggregation expression in a
    final CAST. ``None`` (default) → no cast."""

    @pytest.fixture
    def orders_model(self) -> SlayerModel:
        return SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
        )

    async def test_measure_type_none_no_cast(self, orders_model) -> None:
        gen = SQLGenerator(dialect="sqlite")
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count", name="cnt")],  # type=None default
        )
        sql = await _generate(gen, query, orders_model)
        # COUNT(*) emitted bare; no outer CAST around the aggregation result.
        assert "CAST(COUNT(" not in sql.upper()

    async def test_measure_type_double_wraps_outer_count(self, orders_model) -> None:
        gen = SQLGenerator(dialect="sqlite")
        query = SlayerQuery(
            source_model="orders",
            measures=[
                ModelMeasure(formula="*:count", name="cnt", type=DataType.DOUBLE),
            ],
        )
        sql = await _generate(gen, query, orders_model)
        # SQLite: DOUBLE → REAL.
        assert "CAST(" in sql.upper()
        assert "REAL" in sql.upper()

    async def test_measure_type_double_on_ratio(self, orders_model) -> None:
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(
            source_model="orders",
            measures=[
                ModelMeasure(
                    formula="revenue:sum / *:count",
                    name="ratio",
                    type=DataType.DOUBLE,
                ),
            ],
        )
        sql = await _generate(gen, query, orders_model)
        # Outer CAST around the divided expression.
        assert "CAST(" in sql.upper()
        assert "DOUBLE" in sql.upper()


class TestCastEmissionNonBasePaths:
    """DEV-1361 follow-up: ``ModelMeasure.type`` and ``Column.type`` must wrap
    aggregation expressions in CAST across every emission path — not just the
    base ``_generate_base()`` path. Covers windowed CTEs, isolated filtered
    measure CTEs, percentile/median/stat-agg/weighted-avg builders.
    """

    @pytest.fixture
    def orders_model_for_window(self) -> SlayerModel:
        return SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
        )

    async def test_windowed_sum_with_measure_type_wraps_in_cast(
        self, orders_model_for_window: SlayerModel,
    ) -> None:
        """Windowed sum CTE was previously emitting ``SUM(_src._w_value) AS alias``
        with no CAST when the inline measure declared ``type=DataType.DOUBLE``.
        """
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(
            source_model="orders_for_window",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
            measures=[
                ModelMeasure(
                    formula="revenue:sum(window='90d')",
                    name="rev_90d",
                    type=DataType.DOUBLE,
                ),
            ],
        )
        orders_model_for_window.name = "orders_for_window"
        sql = await _generate(gen, query, orders_model_for_window)
        # The windowed CTE itself must contain the CAST around SUM(_src._w_value).
        # _wm_ prefix identifies the windowed measure CTE.
        assert "_wm_orders_for_window__rev_90d" in sql
        # CAST(SUM(...) AS DOUBLE) shape inside the windowed CTE.
        norm = _norm(sql).upper()
        assert "CAST(SUM(" in norm or "CAST (SUM(" in norm
        assert "DOUBLE" in norm

    async def test_windowed_sum_no_measure_type_skips_cast(
        self, orders_model_for_window: SlayerModel,
    ) -> None:
        """Without a declared measure type, no CAST wrapper is emitted around
        the windowed aggregation."""
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(
            source_model="orders_for_window2",
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
            measures=[
                ModelMeasure(formula="revenue:sum(window='90d')", name="rev_90d"),
            ],
        )
        orders_model_for_window.name = "orders_for_window2"
        sql = await _generate(gen, query, orders_model_for_window)
        # Windowed CTE present but no CAST around SUM(_w_value).
        assert "_wm_orders_for_window2__rev_90d" in sql
        norm = _norm(sql).upper()
        assert "CAST(SUM(_SRC._W_VALUE)" not in norm

    async def test_percentile_uses_column_type_for_inner_cast(self) -> None:
        """``_resolve_value_sql`` must propagate ``column_type`` so that
        non-bare ``Column.sql`` (e.g. ``json_extract(...)``) feeding percentile
        gets the inner pre-aggregation CAST applied."""
        model = SlayerModel(
            name="events",
            sql_table="public.events",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(
                    name="score",
                    sql="json_extract(payload, '$.score')",
                    type=DataType.DOUBLE,
                ),
            ],
        )
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(
            source_model="events",
            measures=[ModelMeasure(formula="score:percentile(p=0.5)", name="p50")],
        )
        sql = await _generate(gen, query, model)
        # Inner CAST around the json_extract — postgres uses native PERCENTILE_CONT.
        norm = _norm(sql).upper()
        assert "CAST(" in norm
        assert "DOUBLE" in norm
        assert "PERCENTILE_CONT" in norm

    async def test_weighted_avg_uses_column_type_for_inner_cast(self) -> None:
        """``weighted_avg`` goes through ``_build_formula_agg`` →
        ``_resolve_value_sql``. With ``column_type`` propagation, non-bare
        Column.sql gets CAST'd inside the formula expansion."""
        model = SlayerModel(
            name="events",
            sql_table="public.events",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(
                    name="score",
                    sql="json_extract(payload, '$.score')",
                    type=DataType.DOUBLE,
                ),
                Column(name="weight", sql="weight_col", type=DataType.DOUBLE),
            ],
        )
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(
            source_model="events",
            measures=[
                ModelMeasure(
                    formula="score:weighted_avg(weight=weight)",
                    name="wavg",
                ),
            ],
        )
        sql = await _generate(gen, query, model)
        # CAST present somewhere — column_type propagated to formula expansion.
        assert "CAST(" in sql.upper()
        assert "JSON_EXTRACT" in sql.upper()


class TestReplaceFunctionInPredicate:
    """DEV-1378: pin the ``replace(...)``-as-Command parsing trap.

    ``sqlglot.parse_one("replace(x, ',', '') = 'foo'", dialect="sqlite")``
    by default falls back to a Command (``REPLACE INTO`` statement form),
    which emits broken SQL. ``SQLGenerator._parse_predicate`` wraps the
    expression in ``SELECT 1 WHERE ...`` to dodge this. These tests pin
    that the wrap fires at every relevant predicate-emission site, so a
    regression doesn't reintroduce the trap.
    """

    @pytest.mark.parametrize("dialect", ["sqlite", "mysql"])
    async def test_replace_in_query_filter(
        self, orders_model: SlayerModel, dialect: str,
    ) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["replace(status, ',', '') = 'foo'"],
        )
        sql = await _generate(
            generator=SQLGenerator(dialect=dialect),
            query=query,
            model=orders_model,
        )
        # Function-call form, not the broken `REPLACE (x, ...)` Command form.
        assert "REPLACE(" in sql.upper() or "replace(" in sql
        assert "REPLACE (" not in sql.upper()  # space after REPLACE = Command form

    @pytest.mark.parametrize("dialect", ["sqlite", "mysql"])
    async def test_replace_in_column_filter(
        self, orders_model: SlayerModel, dialect: str,
    ) -> None:
        orders_model.columns.append(
            Column(
                name="cleaned_amt",
                sql="amount",
                filter="replace(status, ',', '') = 'foo'",
                type=DataType.DOUBLE,
            )
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="cleaned_amt:sum")],
        )
        sql = await _generate(
            generator=SQLGenerator(dialect=dialect),
            query=query,
            model=orders_model,
        )
        # Function-call form, not Command.
        assert "REPLACE(" in sql.upper() or "replace(" in sql
        assert "REPLACE (" not in sql.upper()


def _build_score_model_dev1539(*, name: str = "m", score_sql: str) -> SlayerModel:
    """DEV-1539 test helper: build a minimal model with four numeric
    columns (``a, b, c, d``) and a derived ``score`` column whose ``sql``
    is the multi-term expression under test. Used by both the local-
    branch positive test and the SQLite integration test so the model
    setup boilerplate isn't duplicated across files (Sonar
    ``new_duplicated_lines_density``).
    """
    return SlayerModel(
        name=name,
        sql_table=f"public.{name}",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="a", sql="a", type=DataType.DOUBLE),
            Column(name="b", sql="b", type=DataType.DOUBLE),
            Column(name="c", sql="c", type=DataType.DOUBLE),
            Column(name="d", sql="d", type=DataType.DOUBLE),
            Column(name="score", sql=score_sql, type=DataType.DOUBLE),
        ],
    )


def _build_backslash_risk_model_dev1539(*, name: str = "m") -> tuple[SlayerModel, str]:
    """DEV-1539 test helper: build a model with a ``risk`` column whose
    ``sql`` contains a literal double-backslash inside a string literal.
    Returns ``(model, double_backslash_literal)``. Used by both the
    WHERE-side and HAVING-side backslash-safety tests.
    """
    backslash = chr(92)
    double_bs = backslash * 2  # SQL literal `'\\'` (two backslashes)
    model = SlayerModel(
        name=name,
        sql_table=f"public.{name}",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="tag", sql="tag", type=DataType.TEXT),
            Column(
                name="risk",
                sql=f"LENGTH(REPLACE(tag, '{double_bs}', '')) + 0",
                type=DataType.DOUBLE,
            ),
        ],
    )
    return model, double_bs


async def _build_joined_customers_orders_engine_dev1539(
    *, tmp_path, customers_score_sql: str,
) -> SlayerQueryEngine:
    """DEV-1539 test helper: spin up a storage-backed engine with an
    ``orders`` model joined to a ``customers`` model whose ``score`` /
    derived column carries ``customers_score_sql``. Used by the dotted
    joined-column wrap test and the dotted-branch backslash-safety
    test so the storage + join boilerplate isn't duplicated.
    """
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_model(SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="a", sql="a", type=DataType.DOUBLE),
            Column(name="b", sql="b", type=DataType.DOUBLE),
            Column(name="tag", sql="tag", type=DataType.TEXT),
            Column(name="score", sql=customers_score_sql, type=DataType.DOUBLE),
        ],
    ))
    await storage.save_model(SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    ))
    return SlayerQueryEngine(storage=storage)


class TestFilterOuterParenWrapDev1539:
    """DEV-1539: defensive outer-paren wrapping at every site where a
    multi-term expression gets plopped into a filter context.

    Three sites:

    1. ``resolve_filter_columns`` (local + dotted joined-column branches):
       a non-bare ``Column.sql`` substituted into a filter's text must be
       wrapped in ``(...)`` so the precedence of the surrounding comparator
       is preserved by inspection, not only by SQL precedence rules.
    2. ``_compare_to_sql`` (DSL filter parser): a Compare LHS / RHS that
       is ``BinOp`` or ``BoolOp`` must be emitted with outer parens.
       Chained comparisons (``a < b < c``) are rejected — their Python
       semantics differ from SQL's left-associative comparison chaining.
    3. ``_build_where_and_having`` HAVING measure-substitution: when the
       substituted ``agg_expr`` is a Binary/Connector at the AST root,
       wrap its emitted SQL string in ``(...)`` before ``re.sub``.

    All three sites also gain ``re.sub(..., lambda _: replacement, ...)``
    in place of bare string replacement so backslashes inside inlined
    Column SQL aren't silently mutated as backref escapes.
    """

    async def test_filter_inlines_multiterm_local_column_with_outer_parens(
        self, generator: SQLGenerator,
    ) -> None:
        """A query filter on a Column whose ``sql`` is a multi-term
        arithmetic expression must surface the inlined body wrapped in
        outer parens — ``(a + b) > 7``, not ``a + b > 7``.
        """
        model = _build_score_model_dev1539(
            score_sql="a * 0.4 + b * 0.3 + c * 0.1 + d * 0.2",
        )
        query = SlayerQuery(
            source_model="m",
            measures=[ModelMeasure(formula="*:count")],
            filters=["score > 7"],
        )
        sql = await _generate(generator, query, model)
        norm = _norm(sql)
        # The LHS of `> 7` must be a parenthesised arithmetic expression.
        # NB: this test path doesn't pass a ``resolve_model`` to
        # ``enrich_query``, so derived-ref expansion (and therefore
        # column qualification) is skipped — the inlined sql_expr is
        # left textually as-written. The PAREN WRAP is independent of
        # qualification, and that's what we're pinning.
        # Single bounded unbounded quantifier (`[^)]+`) avoids the
        # multi-quantifier-backtracking pattern Sonar's S5852 flags.
        assert "WHERE" in norm
        where_clause = norm.split("WHERE", 1)[1]
        m = _re.search(r"\( a \* 0\.4[^)]+\) > 7", where_clause)
        assert m is not None, (
            f"Expected pattern `( a * 0.4 ... ) > 7` in normalised "
            f"WHERE; got: {where_clause}"
        )
        # Negative: without the wrap, the trailing arithmetic term lands
        # right next to ``> 7`` with no paren in between.
        assert "* 0.2 > 7" not in where_clause, (
            f"Pre-wrap shape `d * 0.2 > 7` should not survive the fix; "
            f"got: {where_clause}"
        )

    async def test_filter_inlines_bare_column_no_parens(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """When a Column is a bare identifier (``sql == name`` or sql is a
        single identifier), no extra parens are added around the qualified
        reference. ``WHERE orders.customer_id > 100``, not
        ``WHERE (orders.customer_id) > 100``.
        """
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["customer_id > 100"],
        )
        sql = await _generate(generator, query, orders_model)
        norm = _norm(sql)
        assert "orders.customer_id > 100" in norm
        assert "(orders.customer_id) > 100" not in norm, (
            f"Bare-identifier Column refs must not gain spurious parens; got:\n{sql}"
        )

    async def test_filter_inlines_multiterm_joined_column_with_outer_parens(
        self, generator: SQLGenerator, tmp_path,
    ) -> None:
        """The dotted joined-column branch of ``resolve_filter_columns``
        (enrichment.py around line 2789) must also wrap inlined non-bare
        Column.sql bodies in outer parens. Filter
        ``joined_model.score > 7`` where the joined column's sql is a
        multi-term arithmetic expression.
        """
        engine = await _build_joined_customers_orders_engine_dev1539(
            tmp_path=tmp_path,
            customers_score_sql="a * 0.6 + b * 0.4",
        )
        orders = await engine.storage.get_model("orders", data_source="test")
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["customers.score > 7"],
        )
        enriched = await engine._enrich(query=query, model=orders)
        sql = generator.generate(enriched=enriched)
        norm = _norm(sql)
        # The inlined cross-model expression must be wrapped before `> 7`.
        m = _re.search(
            r"\(\s*customers\.a\s*\*\s*0\.6\s*\+\s*customers\.b\s*\*\s*0\.4\s*\)\s*>\s*7",
            norm,
        )
        assert m is not None, (
            f"Expected dotted joined-column multi-term sql wrapped before `> 7`; "
            f"got normalised SQL:\n{norm}"
        )

    async def test_dsl_compare_lhs_binop_wrapped(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """A DSL filter ``a + b > 7`` must emit ``(a + b) > 7`` so the
        precedence of the multi-term arithmetic LHS is explicit by
        inspection, not only by SQL operator-precedence rules.
        """
        # Use two bare-name columns so the LHS is a Compare(BinOp(...), 7)
        # and the BinOp wrap applies at the DSL layer, not at column
        # inlining (both columns inline as bare qualified identifiers).
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["customer_id + id > 7"],
        )
        sql = await _generate(generator, query, orders_model)
        norm = _norm(sql)
        m = _re.search(
            r"\(\s*orders\.customer_id\s*\+\s*orders\.id\s*\)\s*>\s*7",
            norm,
        )
        assert m is not None, (
            f"Expected DSL Compare LHS BinOp wrapped to `(a + b) > 7`; got:\n{norm}"
        )

    async def test_dsl_compare_rhs_binop_wrapped(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """A DSL filter ``x > a + b`` must emit ``x > (a + b)``."""
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["customer_id > id + 100"],
        )
        sql = await _generate(generator, query, orders_model)
        norm = _norm(sql)
        m = _re.search(
            r"orders\.customer_id\s*>\s*\(\s*orders\.id\s*\+\s*100\s*\)",
            norm,
        )
        assert m is not None, (
            f"Expected DSL Compare RHS BinOp wrapped to `x > (a + b)`; got:\n{norm}"
        )

    async def test_dsl_compare_bare_lhs_not_wrapped(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """A DSL filter with a bare Name LHS (``x > 7``) must not gain
        spurious parens: ``WHERE orders.x > 7``, not
        ``WHERE (orders.x) > 7``.
        """
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["customer_id > 7"],
        )
        sql = await _generate(generator, query, orders_model)
        norm = _norm(sql)
        assert "orders.customer_id > 7" in norm
        assert "(orders.customer_id) > 7" not in norm

    async def test_dsl_compare_call_lhs_not_wrapped(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """A DSL filter whose LHS is a function call (``lower(status) == 'a'``)
        must not gain spurious parens around the call. Only ``BinOp`` and
        ``BoolOp`` LHSes get wrapped.
        """
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["lower(status) == 'active'"],
        )
        sql = await _generate(generator, query, orders_model)
        norm = _norm(sql)
        # LOWER(...) = 'active', not (LOWER(...)) = 'active'.
        assert _re.search(r"LOWER\([^)]*\)\s*=\s*'active'", norm, _re.IGNORECASE), (
            f"Call-LHS filter must not gain outer parens; got:\n{norm}"
        )
        assert "(LOWER(" not in norm.upper().replace("WHERE (LOWER(", ""), (
            f"Spurious parens around LOWER(...) call; got:\n{norm}"
        )

    @pytest.mark.parametrize(
        ["formula", "expected_sql"],
        [
            # IS NULL / IS NOT NULL stay as-is — `_compare_op_to_sql`
            # returns the complete operator string when the RHS is None.
            ("flag is None", "flag IS NULL"),
            ("flag is not None", "flag IS NOT NULL"),
            # Non-None IS / IS NOT: previously the `continue` in the
            # IS/IsNot branch dropped the RHS and emitted broken SQL
            # like `flag IS` / `flag IS NOT`. Fall-through must render
            # `IS <rhs>` / `IS NOT <rhs>`.
            ("flag is True", "flag IS True"),
            ("flag is not False", "flag IS NOT False"),
            # IS-non-None composed with another predicate still flows
            # through `_boolop_to_sql`'s outer wrap.
            ("flag is True and value > 0", "(flag IS True AND value > 0)"),
        ],
    )
    def test_dsl_compare_is_isnot_with_non_none_rhs(
        self, formula: str, expected_sql: str,
    ) -> None:
        """``is`` / ``is not`` against a non-None RHS used to drop the
        RHS entirely and emit the broken ``IS`` / ``IS NOT`` operator
        string. Fix: only the ``is None`` / ``is not None`` paths
        short-circuit to the complete operator; everything else falls
        through to the standard ``<op> <rhs>`` emission.
        """
        from slayer.core.formula import parse_filter

        pf = parse_filter(formula)
        assert pf.sql == expected_sql, (
            f"parse_filter({formula!r}).sql == {pf.sql!r}, expected "
            f"{expected_sql!r}"
        )

    def test_dsl_chained_compare_rejected(self) -> None:
        """Chained comparisons (``a < b < c``) have different semantics
        between Python and SQL. Python: ``(a < b) AND (b < c)``. SQL:
        ``(a < b) < c`` (a boolean re-compared to c). The DSL parser
        must reject chained comparisons with a clear, actionable error
        rather than silently emit subtly wrong SQL.
        """
        from slayer.core.formula import parse_filter

        with pytest.raises(ValueError, match=r"[Cc]hained comparison") as excinfo:
            parse_filter("a < b < c")
        # The error must point at the actionable alternative.
        assert "AND" in str(excinfo.value) or "and" in str(excinfo.value), (
            f"Chained-compare rejection should point at the `AND` rewrite; "
            f"got: {excinfo.value!r}"
        )

    async def test_dsl_compare_lhs_boolop_wrapped(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """A DSL filter whose LHS is an ``ast.BoolOp`` (rare but valid)
        — e.g. ``(a and b) > 7`` — must emit ``(a AND b) > 7``. Covers
        the BoolOp half of the Site 2 wrap rule symmetrically with the
        BinOp half.
        """
        # NB: customer_id and id are bare-identifier columns, so the
        # only wrap on emit is the DSL-level BoolOp wrap under test.
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["(customer_id and id) > 0"],
        )
        sql = await _generate(generator, query, orders_model)
        norm = _norm(sql)
        m = _re.search(
            r"\(\s*orders\.customer_id\s+AND\s+orders\.id\s*\)\s*>\s*0",
            norm,
            _re.IGNORECASE,
        )
        assert m is not None, (
            f"Expected DSL Compare LHS BoolOp wrapped to `(a AND b) > 0`; got:\n{norm}"
        )

    async def test_dsl_compare_rhs_boolop_wrapped(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """The RHS-side counterpart: ``x == (a or b)`` must emit
        ``x = (a OR b)``.
        """
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["customer_id == (id or 0)"],
        )
        sql = await _generate(generator, query, orders_model)
        norm = _norm(sql)
        m = _re.search(
            r"orders\.customer_id\s*=\s*\(\s*orders\.id\s+OR\s+0\s*\)",
            norm,
            _re.IGNORECASE,
        )
        assert m is not None, (
            f"Expected DSL Compare RHS BoolOp wrapped to `x = (a OR 0)`; got:\n{norm}"
        )

    async def test_filter_inline_preserves_backslash_in_joined_column_sql(
        self, generator: SQLGenerator, tmp_path,
    ) -> None:
        """Site 1b backslash safety: the dotted joined-column inlining
        branch at ``enrichment.py:2789`` must also use lambda-replacement
        in ``re.sub`` so backslashes inside the joined column's SQL
        aren't silently halved when substituted into the filter text.
        """
        # The joined ``customers.risk`` column must be MULTI-TERM so
        # the non-bare-identifier inlining branch fires AND contains a
        # backslash literal. We piggy-back on the shared joined-engine
        # helper by renaming the ``score`` column's body to a
        # backslash-bearing expression.
        backslash = chr(92)
        double_bs = backslash * 2
        risk_sql = f"LENGTH(REPLACE(tag, '{double_bs}', '')) + 0"
        engine = await _build_joined_customers_orders_engine_dev1539(
            tmp_path=tmp_path,
            customers_score_sql=risk_sql,
        )
        orders = await engine.storage.get_model("orders", data_source="test")
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            # The joined column is named ``score`` in the helper; that's
            # the alias under which the backslash-bearing sql lives.
            filters=["customers.score > 0"],
        )
        enriched = await engine._enrich(query=query, model=orders)
        sql = generator.generate(enriched=enriched)
        assert f"'{double_bs}'" in sql, (
            f"Site 1b (dotted-path) backslash halving regression; got:\n{sql}"
        )

    async def test_having_substitution_preserves_backslash_in_agg_sql(
        self, generator: SQLGenerator,
    ) -> None:
        """Site 3 backslash safety: HAVING measure-substitution must use
        lambda-replacement in ``re.sub`` so backslashes inside the
        aggregated value's source SQL aren't silently halved when the
        emitted ``agg_sql`` is substituted into the HAVING text.
        """
        model, double_bs = _build_backslash_risk_model_dev1539()
        query = SlayerQuery(
            source_model="m",
            dimensions=[ColumnRef(name="id")],
            measures=[ModelMeasure(formula="risk:sum")],
            filters=["risk_sum > 0"],
        )
        sql = await _generate(generator, query, model)
        assert "HAVING" in sql
        having = sql.split("HAVING", 1)[1]
        assert f"'{double_bs}'" in having, (
            f"Site 3 (HAVING) backslash halving regression; got HAVING body:\n{having}"
        )

    async def test_having_multiterm_measure_wrapped(
        self, generator: SQLGenerator,
    ) -> None:
        """When a HAVING filter references a measure whose aggregation
        expression is a multi-term form (e.g., ``SUM(x * w) / SUM(w)``
        for a ``weighted_avg`` measure), the substituted expression in
        the emitted HAVING must be wrapped in outer parens.
        """
        model = SlayerModel(
            name="sales",
            sql_table="public.sales",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region", sql="region", type=DataType.TEXT),
                Column(name="price", sql="price", type=DataType.DOUBLE),
                Column(name="quantity", sql="quantity", type=DataType.DOUBLE),
            ],
        )
        query = SlayerQuery(
            source_model="sales",
            dimensions=[ColumnRef(name="region")],
            measures=[
                ModelMeasure(
                    formula="price:weighted_avg(weight=quantity)",
                    name="wavg",
                ),
            ],
            filters=["wavg > 0"],
        )
        sql = await _generate(generator, query, model)
        norm = _norm(sql)
        assert "HAVING" in norm
        # The substituted multi-term SUM(...) / NULLIF(SUM(...)) must
        # appear wrapped in parens immediately before `> 0`. Per
        # CodeRabbit feedback: a substring check like `"(SUM(" in upper
        # and ") > 0" in upper` would pass even on the un-wrapped
        # ``SUM(...) / NULLIF(SUM(...), 0) > 0`` because the `(SUM(`
        # substring matches inside `NULLIF(SUM(`. Anchor on positional
        # checks instead — find the comparator and verify the LHS as a
        # whole is parenthesised.
        having = norm.split("HAVING", 1)[1].strip()
        gt_index = having.find(" > 0")
        assert gt_index > 0, (
            f"HAVING must end with `... > 0`; got:\n{having}"
        )
        assert having[gt_index - 1] == ")", (
            f"Expected `)` immediately before `> 0` (the outer wrap's "
            f"closer); got char {having[gt_index - 1]!r} at index "
            f"{gt_index - 1} in:\n{having}"
        )
        # The HAVING expression must START with an open paren — the
        # outer wrap. (After ``strip()`` above any pretty-print
        # indentation is gone.)
        assert having.startswith("("), (
            f"Expected HAVING multi-term LHS to start with `(`; got:\n{having}"
        )
        # And the body contains a real top-level divide between two
        # aggregate calls — not just the inner NULLIF.
        assert "SUM(" in having.upper() and "/" in having and "NULLIF" in having.upper(), (
            f"Expected HAVING body to combine SUM/NULLIF via `/`; got:\n{having}"
        )

    async def test_having_simple_measure_not_wrapped(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """A HAVING filter on a simple single-aggregation measure
        (``revenue:sum > 0``) must NOT gain spurious outer parens:
        ``HAVING SUM(...) > 0``, not ``HAVING (SUM(...)) > 0``.
        Only multi-term ``Binary``/``Connector`` ``agg_expr`` shapes
        warrant the wrap.
        """
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="revenue:sum")],
            filters=["revenue_sum > 0"],
        )
        sql = await _generate(generator, query, orders_model)
        norm = _norm(sql)
        assert "HAVING" in norm
        having = norm.split("HAVING", 1)[1]
        # Tight form: HAVING SUM(...) > 0
        assert _re.search(r"HAVING\s*SUM\([^)]*\)\s*>\s*0", "HAVING" + having, _re.IGNORECASE) or \
               _re.search(r"SUM\([^)]*\)\s*>\s*0", having, _re.IGNORECASE), (
            f"Expected `SUM(...) > 0` without outer parens in HAVING; got:\n{having}"
        )
        assert _re.search(r"\(\s*SUM\([^)]*\)\s*\)\s*>\s*0", having, _re.IGNORECASE) is None, (
            f"Single-aggregate HAVING must not gain spurious parens; got:\n{having}"
        )

    async def test_filter_inline_preserves_backslash_in_column_sql(
        self, generator: SQLGenerator,
    ) -> None:
        """``re.sub(pattern, repl, source)`` interprets backslashes in the
        replacement string as escape sequences — a literal ``\\\\`` in
        the inlined Column.sql gets silently halved to ``\\`` in the
        emitted WHERE. Using ``lambda _: repl`` for the replacement
        side-steps the bug. This test pins the fix.
        """
        model, double_bs = _build_backslash_risk_model_dev1539()
        query = SlayerQuery(
            source_model="m",
            measures=[ModelMeasure(formula="*:count")],
            filters=["risk > 0"],
        )
        # Must not raise (e.g., ``re.error: bad escape \\b``) and must
        # preserve the doubled backslash in the emitted SQL.
        sql = await _generate(generator, query, model)
        # The original SQL literal had two backslashes; after substitution
        # via lambda replacement, both must survive.
        assert f"'{double_bs}'" in sql, (
            f"Backslash halving regression: expected '{double_bs}' literal "
            f"preserved in emitted SQL; got:\n{sql}"
        )

    @pytest.mark.parametrize(
        ["formula", "expected_sql"],
        [
            # Inner low-prec child under high-prec parent — left operand.
            ("(a + b) * c > 10", "((a + b) * c) > 10"),
            # Same shape — RHS of comparator.
            ("a > (b + c) * d", "a > ((b + c) * d)"),
            # Equal-precedence right child of /, must stay wrapped.
            ("a / (b * c) > 0", "(a / (b * c)) > 0"),
            # Equal-precedence right child of -, must stay wrapped to
            # preserve right-grouping semantics.
            ("a - (b - c) > 0", "(a - (b - c)) > 0"),
            # Left-assoc, no source parens: no inner wrap needed.
            ("a - b - c > 0", "(a - b - c) > 0"),
            # Higher-precedence child under lower-precedence parent:
            # no wrap needed — `a + b * c` reads correctly bare.
            ("a + b * c > 10", "(a + b * c) > 10"),
            # User-supplied parens around left equal-precedence are
            # semantically a no-op (`(a + b) + c` == `a + b + c`) so
            # we don't emit a stray inner wrap.
            ("(a + b) + c > 0", "(a + b + c) > 0"),
            # Pow is RIGHT-associative — the equal-precedence rule
            # mirrors the others. `(a ** b) ** c` must keep its inner
            # parens; without the fix it would re-emit as
            # `a ** b ** c` which Python re-parses as `a ** (b ** c)`,
            # giving a different result.
            ("(a ** b) ** c > 0", "((a ** b) ** c) > 0"),
            # `a ** b ** c` parses RIGHT-assoc; emission must preserve
            # the grouping via explicit parens on the right operand.
            ("a ** b ** c > 0", "(a ** (b ** c)) > 0"),
        ],
    )
    def test_dsl_compare_preserves_nested_arithmetic_precedence(
        self, formula: str, expected_sql: str,
    ) -> None:
        """DEV-1539: ``_binop_to_sql`` must wrap nested child operands so
        the AST-encoded operator precedence survives serialisation.
        Without this, ``(a + b) * c > 10`` and ``a + b * c > 10`` would
        both emit as ``(a + b * c) > 10`` — semantically distinct
        inputs collapse to the same output, silently changing results.
        """
        from slayer.core.formula import parse_filter

        pf = parse_filter(formula)
        assert pf.sql == expected_sql, (
            f"parse_filter({formula!r}).sql == {pf.sql!r}, expected "
            f"{expected_sql!r}"
        )

    @pytest.mark.parametrize(
        ["body_sql", "connector"],
        [
            # sqlglot 30.4.3: ``exp.And`` is a subclass of ``exp.Func``,
            # so a pure inverse-atomic check would mis-classify this as
            # atomic and skip the wrap. The compound check must fire
            # first.
            ("archived AND deleted", "AND"),
            ("archived OR deleted", "OR"),
        ],
    )
    async def test_filter_inlines_and_or_connector_column_with_outer_parens(
        self, generator: SQLGenerator, body_sql: str, connector: str,
    ) -> None:
        """Column.sql whose root is ``a AND b`` / ``a OR b`` (sqlglot
        ``exp.And`` / ``exp.Or``) must be wrapped on inline. These
        inherit from ``exp.Func`` in sqlglot 30.4.3, so the
        inverse-atomic check at ``_filter_inline_needs_paren_wrap``
        would skip them — the compound-types check has to fire first.
        """
        model = SlayerModel(
            name="m",
            sql_table="public.m",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="archived", sql="archived", type=DataType.BOOLEAN),
                Column(name="deleted", sql="deleted", type=DataType.BOOLEAN),
                Column(name="active", sql=body_sql, type=DataType.BOOLEAN),
            ],
        )
        query = SlayerQuery(
            source_model="m",
            measures=[ModelMeasure(formula="*:count")],
            filters=["active IS NULL"],
        )
        sql = await _generate(generator, query, model)
        norm = _norm(sql)
        # Body matches `(... <connector> ...)` allowing whitespace
        # padding that sqlglot's pretty-printer injects inside the
        # parens. Single bounded `[^)]+` quantifier — S5852-safe.
        m = _re.search(
            r"\(\s*archived\s+" + connector + r"\s+deleted\s*\)",
            norm,
            _re.IGNORECASE,
        )
        assert m is not None, (
            f"{connector}-rooted Column.sql must be wrapped on inline; got:\n{norm}"
        )
        # And the wrap really protects the IS NULL precedence —
        # the char before `IS NULL` must be `)`.
        is_null_index = norm.find("IS NULL")
        assert is_null_index > 0, f"WHERE must contain IS NULL; got:\n{norm}"
        preceding = norm[:is_null_index].rstrip()
        assert preceding.endswith(")"), (
            f"Char before `IS NULL` must be `)` (outer wrap closer); "
            f"got tail {preceding[-15:]!r} in:\n{norm}"
        )

    async def test_filter_inlines_not_predicate_column_with_outer_parens(
        self, generator: SQLGenerator,
    ) -> None:
        """A Column.sql whose root is ``NOT <expr>`` (sqlglot ``exp.Not``)
        must be wrapped on inline so a surrounding predicate like
        ``IS NULL`` binds to the whole expression, not just the inner
        operand. Without the wrap, ``NOT archived IS NULL`` reads as
        ``NOT (archived IS NULL)`` — different semantics from the
        intended ``(NOT archived) IS NULL``. The original
        ``_filter_inline_needs_paren_wrap`` only matched ``Binary``/
        ``Connector`` and missed this shape.
        """
        model = SlayerModel(
            name="m",
            sql_table="public.m",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="archived", sql="archived", type=DataType.BOOLEAN),
                Column(name="active", sql="NOT archived", type=DataType.BOOLEAN),
            ],
        )
        query = SlayerQuery(
            source_model="m",
            measures=[ModelMeasure(formula="*:count")],
            filters=["active IS NULL"],
        )
        sql = await _generate(generator, query, model)
        norm = _norm(sql)
        # Must wrap: WHERE (NOT archived) IS NULL
        m = _re.search(r"\(\s*NOT\s+archived\s*\)\s+IS\s+NULL", norm, _re.IGNORECASE)
        assert m is not None, (
            f"NOT-predicate Column.sql must be wrapped on inline; got:\n{norm}"
        )

    async def test_filter_inlines_between_predicate_column_with_outer_parens(
        self, generator: SQLGenerator,
    ) -> None:
        """A Column.sql whose root is ``BETWEEN`` (sqlglot ``exp.Between``)
        must be wrapped on inline. ``exp.Between`` is NOT ``exp.Binary`` —
        the original positive check missed it.
        """
        model = SlayerModel(
            name="m",
            sql_table="public.m",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="midrange", sql="amount BETWEEN 100 AND 500", type=DataType.BOOLEAN),
            ],
        )
        query = SlayerQuery(
            source_model="m",
            measures=[ModelMeasure(formula="*:count")],
            filters=["midrange IS NULL"],
        )
        sql = await _generate(generator, query, model)
        norm = _norm(sql)
        m = _re.search(
            r"\(\s*amount\s+BETWEEN\s+100\s+AND\s+500\s*\)\s+IS\s+NULL",
            norm,
            _re.IGNORECASE,
        )
        assert m is not None, (
            f"BETWEEN-predicate Column.sql must be wrapped on inline; got:\n{norm}"
        )

    async def test_filter_inlines_in_predicate_column_with_outer_parens(
        self, generator: SQLGenerator,
    ) -> None:
        """A Column.sql whose root is ``IN`` (sqlglot ``exp.In``) must
        also be wrapped — same gap as ``Not`` / ``Between`` in the
        original positive check.
        """
        model = SlayerModel(
            name="m",
            sql_table="public.m",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="active", sql="status IN ('a', 'b', 'c')", type=DataType.BOOLEAN),
            ],
        )
        query = SlayerQuery(
            source_model="m",
            measures=[ModelMeasure(formula="*:count")],
            filters=["active IS NULL"],
        )
        sql = await _generate(generator, query, model)
        norm = _norm(sql)
        m = _re.search(
            r"\(\s*status\s+IN\s*\([^)]*\)\s*\)\s+IS\s+NULL",
            norm,
            _re.IGNORECASE,
        )
        assert m is not None, (
            f"IN-predicate Column.sql must be wrapped on inline; got:\n{norm}"
        )

    def test_dotted_path_substitution_does_not_match_longer_path(self) -> None:
        """Site 1b (dotted joined-column branch in
        ``slayer/engine/enrichment.py``) must guard against substituting
        a shorter dotted col_name (e.g. ``customers.score``) inside a
        longer dotted reference (e.g. ``customers.score.extra``).
        Without the trailing ``(?!\\.)`` lookahead — which the local and
        HAVING branches both have — a 2-hop col_name mis-substitutes as
        a prefix of a 3-hop ref, mangling the emitted SQL.
        """
        import re as _re_mod
        col_name = "customers.score"
        pattern = r"(?<!\.)(?<!\w)\b" + _re_mod.escape(col_name) + r"\b(?!\.)"
        having_sql = "customers.score > 7 AND customers.score.extra > 0"
        result = _re_mod.sub(pattern, lambda _m: "EXPANDED", having_sql)
        # Only the standalone 2-hop ref is rewritten; the 3-hop ref is
        # left intact for its own (later) substitution.
        assert result == "EXPANDED > 7 AND customers.score.extra > 0", (
            f"Post-fix dotted-path regex must skip dotted prefix matches; "
            f"got: {result!r}"
        )

    def test_having_substitution_does_not_match_dotted_continuation(
        self,
    ) -> None:
        """The HAVING-side measure substitution regex in
        ``_build_where_and_having`` must guard against matching a
        measure name when it appears as the prefix of a dotted
        continuation. Without ``(?!\\.)`` after ``\\b``, a measure named
        ``foo`` mis-substitutes inside a literal ``foo.bar`` in the
        filter SQL.

        This test constructs a case where a query measure's renamed
        alias (``rev``) is a prefix of a dotted reference inside the
        same query's HAVING filter SQL — and asserts the substitution
        does not mangle the dotted form.
        """
        # We can't easily synthesise the exact `foo.bar` literal inside
        # a HAVING string through normal DSL channels, since dotted refs
        # parse as Attribute nodes. The regression risk is real for
        # multi-stage / cross-model paths where post-DSL substitutions
        # leave dotted refs in `having_sql`. This test checks the regex
        # behaviour directly.
        import re as _re_mod
        col_name = "foo"
        agg_sql = "SUM(amount)"
        # CURRENT (buggy): no trailing `(?!\.)` guard
        pattern_with_fix = rf"(?<!\.)(?<!\w)\b{_re_mod.escape(col_name)}\b(?!\.)"
        # AFTER fix: the dotted continuation must NOT be substituted
        having_sql = "foo > 100 AND foo.bar > 5"
        result = _re_mod.sub(pattern_with_fix, lambda _: agg_sql, having_sql)
        assert result == "SUM(amount) > 100 AND foo.bar > 5", (
            f"Post-fix HAVING regex must skip dotted continuations; "
            f"got: {result!r}"
        )
        # Also verify the inverse — that the bare `foo` IS substituted.
        assert "SUM(amount)" in result


class TestBigQueryAliasMangling:
    """BigQuery rejects column names containing dots — SLayer's universal
    ``<model>.<column>`` alias convention has to be mangled to ``___`` on the
    way out (and reversed on the way back at the engine).

    These two tests assert the SQLGenerator-level behavior (full SQL output
    across dialects). Pure unit tests of BigqueryDialect.rewrite_emitted_sql
    / decode_result_keys live in tests/dialects/test_bigquery.py.
    """

    async def test_no_dotted_aliases_in_bigquery_sql(self, orders_model: SlayerModel) -> None:
        gen = SQLGenerator(dialect="bigquery")
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="revenue:sum")],
            dimensions=[ColumnRef(name="status")],
            order=[{"column": "count", "direction": "desc"}],
        )
        sql = await _generate(gen, query, orders_model)
        # Every backtick-quoted COLUMN ALIAS emitted by SLayer must NOT
        # contain a dot — that's what BigQuery rejects. Scope to ``AS `...```
        # and ORDER/GROUP BY positions so dotted table FQ refs (which use
        # backticked SEGMENTS like ``\`bigquery-public-data\`.x.y``, not a
        # single backticked-string with dots inside) don't trip the check.
        ALIAS_PATTERNS = [
            r"\bAS\s+`([^`]+)`",            # SELECT expr AS `<alias>`
            r"\bORDER\s+BY[^\n]*`([^`]+)`",  # ORDER BY `<alias>`
            r"\bGROUP\s+BY[^\n]*`([^`]+)`",  # GROUP BY `<alias>` (when sqlglot quotes it)
        ]
        found_any = False
        for pat in ALIAS_PATTERNS:
            for m in _re.findall(pat, sql, flags=_re.IGNORECASE):
                found_any = True
                assert "." not in m, (
                    f"BigQuery output rejects dotted column aliases, but found "
                    f"`{m}` in:\n{sql}"
                )
        assert found_any, f"expected at least one backticked alias in:\n{sql}"
        # Cross-check the mangled separator made it through.
        assert "___" in sql, f"expected ___ alias mangling in:\n{sql}"

    async def test_other_dialects_keep_dotted_aliases(self, orders_model: SlayerModel) -> None:
        # Mangling is bigquery-only; postgres / sqlite / etc. must keep
        # the dotted alias form (which clients and ORDER BY resolvers
        # depend on).
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = await _generate(gen, query, orders_model)
        assert '"orders._count"' in sql
        assert "___" not in sql

    async def test_wrapped_render_mode_also_mangles_aliases(
        self, orders_model: SlayerModel,
    ) -> None:
        """``render_mode="wrapped"`` (used for ``_query_as_model.inner_sql``
        and inner stages of ``source_queries``) must mangle aliases too —
        otherwise the outer stage's references to inner-CTE columns would
        mismatch what the inner CTE actually emits.

        Pins Codex HIGH #1: the consistency of multi-stage BigQuery SQL
        rests on the rewrite firing on BOTH render modes. The rewrite is
        deterministic, so as long as it fires on both, the inner emit and
        the outer reference resolve to the same ``___``-form alias.
        """
        gen = SQLGenerator(dialect="bigquery")
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="revenue:sum")],
            dimensions=[ColumnRef(name="status")],
        )
        enriched = await enrich_query(
            query=query,
            model=orders_model,
            resolve_dimension_via_joins=_noop_async,
            resolve_cross_model_measure=_noop_async,
            resolve_join_target=_noop_async,
        )
        sql = gen.generate(enriched=enriched, render_mode="wrapped")
        # Wrapped mode keeps every alias (so the outer stage can reach
        # them); all aliases must be mangled.
        for m in _re.findall(r"`([^`]+)`", sql):
            assert "." not in m, (
                f"BigQuery wrapped-mode SQL still has dotted alias `{m}`:\n{sql}"
            )
        # And the mangle separator is present (the rewrite actually fired).
        assert "___" in sql, f"wrapped-mode rewrite did not fire:\n{sql}"

    async def test_rewrite_fires_after_outer_projection_trim(
        self, orders_model: SlayerModel,
    ) -> None:
        """The BigQuery alias rewrite must fire AFTER
        ``_apply_outer_projection_trim`` (and not before). Mangling before
        the trim would let the trim's parser see already-mangled aliases
        and potentially miss public-projection columns. Pins Codex MEDIUM
        #5 — the placement contract.

        Strategy: spy on both ``SQLGenerator._apply_outer_projection_trim``
        and ``BigqueryDialect.rewrite_emitted_sql`` and assert the call
        order. Both wrappers delegate to the real implementations so the
        test verifies the production code path, not a stubbed substitute.
        """
        gen = SQLGenerator(dialect="bigquery")
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            dimensions=[ColumnRef(name="status")],
            # Filter using a windowed transform creates a hidden hoisted
            # column, so the trim has actual work to do (rather than being
            # a no-op on a trivial query).
            filters=["dense_rank(revenue:sum) <= 5"],
        )
        enriched = await enrich_query(
            query=query,
            model=orders_model,
            resolve_dimension_via_joins=_noop_async,
            resolve_cross_model_measure=_noop_async,
            resolve_join_target=_noop_async,
        )
        call_order: list[str] = []
        real_trim = SQLGenerator._apply_outer_projection_trim
        real_rewrite = BigqueryDialect.rewrite_emitted_sql

        def trim_spy(self, *, sql, enriched):
            call_order.append("trim")
            return real_trim(self, sql=sql, enriched=enriched)

        def rewrite_spy(self, sql):
            call_order.append("rewrite")
            return real_rewrite(self, sql)

        with patch.object(SQLGenerator, "_apply_outer_projection_trim", trim_spy), \
                patch.object(BigqueryDialect, "rewrite_emitted_sql", rewrite_spy):
            gen.generate(enriched=enriched, render_mode="outer")
        # Trim ran first; rewrite ran after.
        assert call_order == ["trim", "rewrite"], (
            f"_apply_outer_projection_trim must run before rewrite_emitted_sql, "
            f"got call order: {call_order}"
        )

