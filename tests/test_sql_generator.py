"""Tests for the SQL generator."""

import re as _re
import tempfile
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sqlglot
import sqlglot.errors

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Aggregation, AggregationParam, Column, DatasourceConfig, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query
from slayer.sql.generator import (
    AggRenderSpec,
    SQLGenerator,
    _validate_agg_param_value,
    _wrap_cast_for_type,
)
from slayer.sql.scope_check import assert_scope_closed
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import (
    _assert_valid_sql,
    _engine_generate,
    _extract_cte_body,
    _extract_src_body,
    _join_aliases,
    _norm,
)


def _outer_order_terms(sql: str, dialect: str = "postgres") -> list[tuple[str, str]]:
    """Return each ORDER BY term from the OUTERMOST SELECT as
    ``(expression_sql, direction)`` pairs where direction is ``"asc"`` or
    ``"desc"``. Used by tests that assert two ORDER BY terms aren't
    byte-identical AND that the direction wasn't lost in the outer wrap.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    if not isinstance(tree, sqlglot.exp.Select):
        return []
    order = tree.args.get("order")
    if order is None:
        return []
    out: list[tuple[str, str]] = []
    for ordered in order.expressions:
        direction = "desc" if ordered.args.get("desc") else "asc"
        out.append((ordered.this.sql(dialect=dialect), direction))
    return out


def _projection_rn_by_alias(sql: str, dialect: str = "postgres") -> dict[str, str]:  # NOSONAR(S3776) — sequential isinstance dispatch over outermost-SELECT projection layers (Alias / Cast / Max / Case / EQ / Column unwrap). Each layer is a structural-shape predicate whose failure short-circuits to the next projection; extracting per-layer helpers would scatter the predicate.
    """For the OUTERMOST SELECT, walk projections and return
    ``{alias: rn_column_name}`` for every projection whose body is a
    ``MAX(CASE WHEN <_first_rn|_last_rn[suffix]> = 1 THEN …)`` aggregate
    (wrapped in optional ``CAST(...)``). The alias is the ``AS …`` body;
    the rn column is the identifier referenced in the CASE WHEN.

    Lets tests cleanly assert e.g. that two projections reference
    DIFFERENT ``_last_rn{suffix}`` columns without writing a SQL regex
    that can drift across multiple aggregates.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    if not isinstance(tree, sqlglot.exp.Select):
        return {}
    out: dict[str, str] = {}
    for proj in tree.expressions:
        # Outer alias is the AS body.
        alias = proj.alias
        if not alias:
            continue
        body = proj.this if isinstance(proj, sqlglot.exp.Alias) else proj
        # Unwrap one CAST layer if present.
        if isinstance(body, sqlglot.exp.Cast):
            body = body.this
        if not isinstance(body, sqlglot.exp.Max):
            continue
        inner = body.this
        if not isinstance(inner, sqlglot.exp.Case):
            continue
        # First WHEN's condition is the rn = 1 check.
        ifs = inner.args.get("ifs") or []
        if not ifs:
            continue
        cond = ifs[0].args.get("this")
        if cond is None:
            continue
        # cond is an EQ between a column and a literal 1.
        if not isinstance(cond, sqlglot.exp.EQ):
            continue
        left = cond.args.get("this")
        if not isinstance(left, sqlglot.exp.Column):
            continue
        out[alias] = left.name
    return out


def _outer_from_node(sql: str, dialect: str = "postgres"):
    """Return the OUTERMOST SELECT's FROM source node (a sqlglot
    ``Table`` for a flat SELECT, a ``Subquery`` for an outer-wrap shape).
    sqlglot uses ``from_`` as the arg key and stores the single source at
    ``.this``, not in ``.expressions`` (which is empty for a single FROM).
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    if not isinstance(tree, sqlglot.exp.Select):
        return None
    fc = tree.args.get("from_")
    if fc is None:
        return None
    return fc.this


def _alias_bodies(sql: str, alias: str, dialect: str = "postgres") -> list[str]:
    """Rendered body of every ``<expr> AS "<alias>"`` projection in ``sql``.

    Rendered-SQL equivalent of reading an expression's SQL off the legacy
    enriched query (``{e.name: e.sql for e in enriched.expressions}``): the
    computed measure ``growth`` is emitted exactly once as an aliased
    projection, so its body is the expression the engine resolved it to.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    return [
        node.this.sql(dialect=dialect)
        for node in tree.find_all(sqlglot.exp.Alias)
        if node.alias == alias
    ]


def _hoisted_shift_aliases(sql: str, dialect: str = "postgres") -> set[str]:
    """The hidden aliases each hoisted ``time_shift`` is projected under.

    Rendered-SQL equivalent of the legacy enriched-query transform-alias set
    (``{t.alias for t in enriched.transforms}``): every shift becomes a
    ``shifted_*`` CTE that a ``sjoin_*`` CTE self-joins, projecting the
    shifted measure under the hoisted transform's alias.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    aliases: set[str] = set()
    for cte in tree.find_all(sqlglot.exp.CTE):
        if not cte.alias_or_name.startswith("sjoin_"):
            continue
        for proj in cte.this.expressions:
            if (
                isinstance(proj, sqlglot.exp.Alias)
                and isinstance(proj.this, sqlglot.exp.Column)
                and proj.this.table.startswith("shifted_")
            ):
                aliases.add(proj.alias)
    return aliases


def _outer_projection_names(sql: str, dialect: str = "postgres") -> set[str]:
    """Result keys the OUTERMOST SELECT projects — the public projection.

    Superset of the legacy enriched-query dimension/time-dimension/expression
    alias sets (user-declared dimensions AND measures, and nothing hidden), so
    asserting a hoisted transform alias is absent from it is at least as
    strict as the legacy per-set disjointness checks.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    if not isinstance(tree, sqlglot.exp.Select):
        return set()
    return {proj.alias_or_name for proj in tree.expressions}


async def _generate(
    generator: SQLGenerator,
    query: SlayerQuery,
    model: SlayerModel,
    *,
    extra_models: "list | None" = None,
) -> str:
    """Run ``query`` against ``model`` through the typed engine pipeline and
    return the emitted SQL.

    The dialect is taken from ``generator.dialect`` so the existing
    ``_generate(generator, ...)`` / ``_generate(gen, ...)`` call sites keep
    threading their per-test dialect without change. ``validate=False``
    skips the DEV-1410 cycle check for the intentionally-shaped models a
    handful of these tests construct. ``extra_models`` registers join
    targets the engine must resolve (the legacy ``_generate`` used no-op
    resolvers; the typed engine resolves joins for real).
    """
    return await _engine_generate(
        query=query, model=model, dialect=generator.dialect, validate=False,
        extra_models=extra_models,
    )


# DEV-1496 / DEV-1714 Stage 10 landed: duration-windowed measures now emit the
# `_wm_` range-join CTE and RAISE on unsupported shapes, so the former
# `_XFAIL_WINDOWED` / `_DEV1496_STAGE10` strict-xfail pins were promoted to
# plain tests (markers removed in the implementation commit).
# DEV-1531 (first/last over a cross-join derived source) landed in DEV-1709
# Stage 5; DEV-1526 (cross-model aggregate source crossing a further join)
# landed in DEV-1708 Stage 4 — both likewise promoted to plain tests.
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
    # Most tests pass this through ``_generate(generator, ...)`` purely as a
    # per-test dialect carrier (the typed engine pipeline does the SQL
    # generation + validation). A handful invoke the dialect helpers
    # (``gen._build_agg`` / ``_build_percentile`` / ``_build_stat_agg``)
    # directly with an ``AggRenderSpec``.
    return SQLGenerator(dialect="postgres")


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

    async def test_order_by_shorthand_descending_reaches_sql(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        # DEV-1575: shorthand {col: "descending"} heals + normalizes and emits DESC.
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            dimensions=[ColumnRef(name="status")],
            order=[{"status": "descending"}],
        )
        sql = await _generate(generator, query, orders_model)
        assert "ORDER BY" in sql
        assert "DESC" in sql.upper()

    async def test_order_by_ascending_synonym_not_descending(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        # DEV-1575 regression: a canonical "ASCENDING" must normalize to asc so the
        # generator's strict ``direction == "asc"`` check does NOT fall through to DESC.
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            dimensions=[ColumnRef(name="status")],
            order=[{"column": "status", "direction": "ASCENDING"}],
        )
        sql = await _generate(generator, query, orders_model)
        assert "ORDER BY" in sql
        assert "DESC" not in sql.upper()


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
        # Mode-B query filters use the ``like(value, pattern)`` scalar form;
        # it emits the SQL ``LIKE`` operator. (Native ``x LIKE y`` operator
        # syntax remains available in Mode-A model-level filters.)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["like(status, '%act%')"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "LIKE" in sql
        assert "%act%" in sql

    async def test_like_wrong_arity_raises(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        # ``like`` requires exactly (value, pattern); a single arg is rejected
        # at bind time rather than emitting broken SQL.
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["like(status)"],
        )
        with pytest.raises(ValueError, match="'like' takes exactly 2"):
            await _generate(generator, query, orders_model)

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
        """Arithmetic over aggregates is emitted inline in the SELECT (the
        typed pipeline folds ``revenue:sum / *:count`` into a single grouped
        SELECT — no CTE needed for simple aggregate arithmetic)."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="revenue:sum / *:count", name="aov")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "aov" in sql.lower()
        assert "COUNT(*)" in sql
        assert "SUM(" in sql
        # The arithmetic measure divides the two aggregates.
        assert "/ COUNT(*)" in sql

    async def test_no_fields_no_cte(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """Without fields, no CTE is generated."""
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
        )
        sql = await _generate(generator, query, orders_model)
        assert "WITH" not in sql

    async def test_field_with_limit(self, generator: SQLGenerator, orders_model: SlayerModel) -> None:
        """LIMIT applies to the final SELECT. The typed pipeline folds the
        aggregate arithmetic into one grouped SELECT, so LIMIT trails the
        single FROM clause."""
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="revenue:sum / *:count", name="aov")],
            limit=5,
        )
        sql = await _generate(generator, query, orders_model)
        assert "LIMIT 5" in sql
        # LIMIT trails the FROM (applies to the result, not an inner scope).
        assert sql.upper().index("LIMIT 5") > sql.upper().index("FROM")

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

    async def test_cumsum_over_week_sunday_time_dim(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """DEV-1572 (full enum wiring): a transform over a WEEK_SUNDAY time
        dimension must generate valid SQL — the granularity must be wired into
        the interval helpers, not crash with a KeyError on the 9th enum value.
        """
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.WEEK_SUNDAY,
            )],
            measures=[
                ModelMeasure(formula="revenue:sum"),
                ModelMeasure(formula="cumsum(revenue:sum)", name="rev_running"),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        assert "OVER" in sql
        assert "rev_running" in sql.lower()
        # The WEEK_SUNDAY time dim must compile to the Sunday-week truncation
        # (the +1-day column-side shift is unique to WEEK_SUNDAY; plain Monday
        # WEEK has no such shift), proving the granularity wasn't downgraded.
        assert "+ INTERVAL '1 DAY'" in sql.upper()

    async def test_time_shift_over_week_sunday_uses_one_week_interval(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """A time_shift over a WEEK_SUNDAY time dim (granularity derived from
        the time dim, not passed explicitly) shifts by one week — exercising
        ``build_time_offset_expr``'s ``"week_sunday"`` path. Must emit valid
        date arithmetic, not blow up on the unknown granularity string."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.WEEK_SUNDAY,
            )],
            measures=[
                ModelMeasure(formula="revenue:sum"),
                ModelMeasure(formula="time_shift(revenue:sum, -1)", name="rev_prev_week"),
            ],
        )
        sql = await _generate(generator, query, orders_model)
        assert "shifted_" in sql
        # The shift itself must use a one-WEEK interval (7 days). Target the
        # shift's INTERVAL unit specifically — ``INTERVAL '1 WEEK'`` — so the
        # assertion can't be satisfied by the ``DATE_TRUNC('WEEK', ...)`` in the
        # truncation (which uses ``INTERVAL '1 DAY'``, not a WEEK interval).
        assert "INTERVAL '1 WEEK'" in sql.upper()

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
        # Value CTE: partition adds the reset-group alias (the typed pipeline
        # uses a dotted ``orders.positive_streak`` separator, not ``__``).
        assert '"_cp_reset_orders.positive_streak"' in norm
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
        # DEV-1835: windowed measures now emit the unified ``_cm_`` producer
        # CTE named after the canonical aggregate alias (``revenue_sum_window_90d``),
        # while the surfaced output column keeps the user ``name`` (revenue_90d).
        assert "_cm_orders__revenue_sum_window_90d" in norm
        assert "LEFT JOIN" in norm
        assert "_src._w_time >=" in norm
        assert "_src._w_time <" in norm
        # AST-based generation renders single-unit intervals via sqlglot's
        # per-dialect transpiler — Postgres caps the unit name.
        assert "INTERVAL '90 DAY'" in norm
        # The inner grain comparison is NULL-SAFE: a group whose dimension is
        # NULL must receive its real windowed value, not NULL. The time-range
        # bounds above stay plain inequalities — they are a range, not grain.
        assert (
            '_src._w_dim_0 IS NOT DISTINCT FROM _base."orders.status"' in norm
        ), norm

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
        await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
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
        sql = (await engine.execute(query, dry_run=True)).sql
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
        sql = (await engine.execute(query, dry_run=True)).sql
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
        await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
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
        sql = (await engine.execute(query, dry_run=True)).sql
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

    # ------------------------------------------------------------------ #
    # DEV-1714 Stage 10 — new coverage beyond the DEV-1496 harvest pins.
    # Shape-gap tests: shapes no existing pin covers (two windowed measures
    # in one query, Column.filter CASE-wrap, filter-carrier join discovery
    # into _src, ORDER BY the windowed alias). Each asserts a window-specific
    # artifact (``_cm_``/``_src``) now emitted by the Stage-10 range-join CTE
    # (DEV-1835: unified ``_cm_`` producer-CTE naming).
    # ------------------------------------------------------------------ #

    async def test_two_windowed_measures_emit_distinct_ctes(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Two windowed measures in one query must each get their own
        collision-safe ``_cm_`` CTE and both join back to the base grain."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[
                {"formula": "revenue:sum(window='90d')", "name": "rev_90d"},
                {"formula": "revenue:avg(window='30d')", "name": "rev_30d"},
            ],
        )
        sql = await _generate(generator=generator, query=query, model=orders_model)
        norm = _norm(sql)
        assert "_cm_orders__revenue_sum_window_90d" in norm, sql
        assert "_cm_orders__revenue_avg_window_30d" in norm, sql
        # Each windowed CTE is defined exactly once (distinct, never collapsed).
        assert norm.count("_cm_orders__revenue_sum_window_90d AS (") == 1, sql
        assert norm.count("_cm_orders__revenue_avg_window_30d AS (") == 1, sql
        # Codex round 3: deterministic order — CTEs follow measure declaration
        # order (rev_90d before rev_30d), never set-iteration order.
        assert norm.index("_cm_orders__revenue_sum_window_90d AS (") < norm.index("_cm_orders__revenue_avg_window_30d AS ("), sql
        # Both CTEs join back to the base grain in the combined SELECT.
        assert "LEFT JOIN _cm_orders__revenue_sum_window_90d" in norm, sql
        assert "LEFT JOIN _cm_orders__revenue_avg_window_30d" in norm, sql
        # Both surface in the public projection.
        assert '"orders.rev_90d"' in norm, sql
        assert '"orders.rev_30d"' in norm, sql

    async def test_windowed_source_column_filter_wraps_case_in_src(
        self, generator: SQLGenerator,
    ) -> None:
        """A windowed measure whose source ``Column`` carries a ``filter=``
        must CASE-wrap the value inside ``_src`` (``_w_value``), mirroring the
        legacy filtered-aggregate shape."""
        model = SlayerModel(
            name="orders", sql_table="public.orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="paid_revenue", sql="amount", filter="status = 'paid'", type=DataType.DOUBLE),
            ],
        )
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "paid_revenue:sum(window='90d')", "name": "pr_90d"}],
        )
        sql = await _generate(generator=generator, query=query, model=model)
        assert "_cm_orders__paid_revenue_sum_window_90d" in sql, sql
        src_body = _extract_src_body(sql)
        assert "CASE" in src_body.upper(), f"_w_value must CASE-wrap the Column.filter.\nsrc:\n{src_body}"
        assert "'paid'" in src_body, src_body

    async def test_windowed_source_column_filter_crossing_join_kept_in_src(
        self, generator: SQLGenerator,
    ) -> None:
        """DEV-1714 (Codex C3): a windowed measure whose source ``Column.filter``
        crosses a join must pull that join INTO ``_src`` (predicate-carrier join
        discovery via the resolver's filter path, not just a local CASE)."""
        customers = SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
            ],
        )
        orders = SlayerModel(
            name="orders", sql_table="public.orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="flagged_revenue", sql="amount", filter="customers.region_id = 5", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "flagged_revenue:sum(window='90d')", "name": "fr_90d"}],
        )
        sql = await _generate(generator=generator, query=query, model=orders, extra_models=[customers])
        assert "_cm_" in sql, sql
        src_body = _extract_src_body(sql)
        # Assert a real JOIN was registered — not merely that the alias appears
        # inside the CASE predicate text (which would pass even if discovery
        # were broken).
        assert "customers" in _join_aliases(src_body), (
            f"_src must register the customers JOIN crossed by the Column.filter.\nsrc:\n{src_body}"
        )

    async def test_windowed_model_filter_crossing_join_kept_in_src(
        self, generator: SQLGenerator,
    ) -> None:
        """DEV-1714 (Codex C3): a model-level ``SlayerModel.filters`` entry that
        crosses a join must pull that join INTO ``_src`` (the Mode-A filter-text
        carrier path, distinct from the query-filter path already pinned)."""
        customers = SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
            ],
        )
        orders = SlayerModel(
            name="orders", sql_table="public.orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
            filters=["customers.region_id = 5"],
        )
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "rev_90d"}],
        )
        sql = await _generate(generator=generator, query=query, model=orders, extra_models=[customers])
        assert "_cm_" in sql, sql
        src_body = _extract_src_body(sql)
        # Assert a real JOIN was registered (walk the JOIN nodes), not just an
        # alias appearing in the filter predicate text.
        assert "customers" in _join_aliases(src_body), (
            f"_src must register the customers JOIN crossed by the model filter.\nsrc:\n{src_body}"
        )

    async def test_order_by_windowed_alias_resolves(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """ORDER BY the windowed measure's public alias must resolve against the
        combined SELECT (the ``_cm_`` join-back column), not the base."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "revenue_90d"}],
            order=[{"column": {"name": "revenue_90d"}, "direction": "desc"}],
        )
        sql = await _generate(generator=generator, query=query, model=orders_model)
        norm = _norm(sql)
        assert "_cm_" in norm, sql
        order_portion = norm.split("ORDER BY", 1)[1] if "ORDER BY" in norm else ""
        assert '"orders.revenue_90d"' in order_portion, (
            f"ORDER BY must reference the windowed public alias.\nsql:\n{sql}"
        )
        # The order term is the BARE combined-SELECT output column (the _cm_
        # value), never a ``_base.`` qualifier — _base excludes the windowed
        # measure, so ``_base."orders.revenue_90d"`` would dangle.
        assert '_base."orders.revenue_90d"' not in order_portion, sql
        # And the windowed measure must NOT be materialised in _base as a dead
        # plain aggregate (Codex re-review round 2).
        base_body = _extract_cte_body(sql, r"_base")
        assert '"orders.revenue_90d"' not in base_body, (
            f"windowed measure must not render as a plain aggregate in _base.\n"
            f"_base:\n{base_body}"
        )

    # ------------------------------------------------------------------ #
    # DEV-1714 Stage 10 — parity / semantic pins (F1/F4 playbook): turn the
    # spec-interview decisions into contracts.
    # ------------------------------------------------------------------ #

    async def test_windowed_row_filter_applied_inside_src(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """F4: a query WHERE-phase row filter constrains the ``_src`` scope
        (host-rooted scope inherits host row filters)."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "rev_90d"}],
            filters=["status = 'completed'"],
        )
        sql = await _generate(generator=generator, query=query, model=orders_model)
        assert "_cm_" in sql, sql
        src_body = _extract_src_body(sql)
        assert "'completed'" in src_body, (
            f"WHERE-phase row filter must be applied inside _src.\nsrc:\n{src_body}"
        )

    async def test_windowed_date_range_not_applied_inside_src(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """The typed ``date_range`` on the window time dim must be STRIPPED from
        ``_src`` — the trailing window has to reach rows before the range start
        — while still bounding the outer/base grain."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
                date_range=["2024-06-01", "2024-12-31"],
            )],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "rev_90d"}],
        )
        sql = await _generate(generator=generator, query=query, model=orders_model)
        assert "_cm_" in sql, sql
        src_body = _extract_src_body(sql)
        # The range join itself is present in _src.
        assert "_src._w_time >=" in _norm(sql), sql
        # The date_range lower bound must NOT appear inside _src (stripped)…
        assert "2024-06-01" not in src_body, (
            f"date_range must be stripped from _src.\nsrc:\n{src_body}"
        )
        # …but must still bound the host grain somewhere in the statement.
        assert "2024-06-01" in sql, sql

    async def test_windowed_explicit_time_filter_does_not_truncate_src(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """DEV-1732: an EXPLICIT relational bound on the raw window time column
        is a FRAME bound, not a population filter — so it bounds ``_base`` and is
        stripped from ``_src``, exactly like the ``date_range`` spelling of the
        same intent.

        Stage 10 pinned the opposite (legacy parity, a documented truncation);
        DEV-1732 inverted this pin. Rich coverage lives in
        ``tests/test_dev1732_frame_bound_filters.py``.
        """
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "rev_90d"}],
            filters=["created_at >= '2024-06-01'"],
        )
        sql = await _generate(generator=generator, query=query, model=orders_model)
        assert "_cm_" in sql, sql
        src_body = _extract_src_body(sql)
        assert "2024-06-01" not in src_body, (
            f"DEV-1732: an explicit raw-time-column frame bound must be stripped "
            f"from _src.\nsrc:\n{src_body}"
        )
        # …but it must still bound the host grain.
        assert "2024-06-01" in _extract_cte_body(sql, r"_base"), sql

    async def test_windowed_output_is_scope_closed(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Acceptance: the emitted ``_cm_`` statement is scope-closed — every
        alias referenced in a scope is bound in that scope's own FROM/JOINs.
        Makes the ``assert_scope_closed`` guarantee an explicit contract rather
        than only the autouse harness side effect."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "rev_90d"}],
        )
        sql = await _generate(generator=generator, query=query, model=orders_model)
        assert "_cm_" in sql, sql
        # Explicit validator pass (raises ScopeLeakError on any unbound ref).
        assert_scope_closed(sql, dialect="postgres")

    async def test_same_windowed_measure_two_aliases_both_surface(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Codex#2: the same windowed formula selected under two names interns to
        ONE slot / ONE ``_cm_`` CTE, but BOTH public aliases must surface in the
        combined projection (the CTE's single aggregate column is remapped ``AS``
        each alias) — the later alias must not be dropped."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[
                {"formula": "revenue:sum(window='90d')", "name": "rev_a"},
                {"formula": "revenue:sum(window='90d')", "name": "rev_b"},
            ],
        )
        sql = await _generate(generator=generator, query=query, model=orders_model)
        norm = _norm(sql)
        assert '"orders.rev_a"' in norm, sql
        assert '"orders.rev_b"' in norm, sql
        # One shared _cm_ CTE (same structural key), not two.
        assert norm.count("_cm_orders__revenue_sum_window_90d AS (") == 1, sql

    async def test_windowed_joined_time_dimension_registers_join_in_src(
        self, generator: SQLGenerator,
    ) -> None:
        """Codex#1: a windowed measure whose window time dimension lives on a
        JOINED model must pull that join INTO ``_src`` (registered through the
        scope), not reference an unbound alias."""
        customers = SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="signup_at", sql="signup_at", type=DataType.TIMESTAMP),
            ],
        )
        orders = SlayerModel(
            name="orders", sql_table="public.orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="customers.signup_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "rev_w"}],
        )
        sql = await _generate(generator=generator, query=query, model=orders, extra_models=[customers])
        assert "_cm_" in sql, sql
        src_body = _extract_src_body(sql)
        assert "customers" in _join_aliases(src_body), (
            f"_src must register the customers JOIN crossed by the window time "
            f"dimension.\nsrc:\n{src_body}"
        )

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
        # change_pct = (curr - prev) / NULLIF(prev, 0) — the divisor is guarded
        # against a zero prior-period value (returns NULL, not a div-by-zero).
        assert 'NULLIF("orders._time_shift_inner", 0)' in sql

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
        # ROW_NUMBER ranking inside the measure's own ``_cm_`` CTE.
        assert "ROW_NUMBER()" in sql
        assert "_ranked_rn" in sql
        assert "DESC" in sql
        # Conditional aggregate: MAX(CASE WHEN _ranked_rn = 1 THEN col END)
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
        """first(ordered_at) should ORDER BY the explicit time column ascending.

        Ascending is the ABSENCE of ``DESC`` rather than a literal ``ASC``: the
        ranking clause emits the bare column, which is what ascending means and
        what ``last`` differs from."""
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
        assert "ORDER BY orders.ordered_at)" in _norm(sql), sql

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
        norm = _norm(sql)
        # Two rankings, one per measure, each in its own CTE ordering by its own
        # time column. The rn-SUFFIX scheme (``_last_rn_2``) existed only to
        # keep two rankings apart inside ONE shared scope; one scope per
        # aggregate leaves it nothing to disambiguate.
        assert sql.count("ROW_NUMBER()") == 2
        assert "ORDER BY orders.ordered_at DESC" in norm, sql
        assert "ORDER BY orders.updated_at DESC" in norm, sql
        assert "_last_rn_2" not in sql, sql
        assert norm.count("CASE WHEN _ranked_rn = 1") == 2, sql

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
        norm = _norm(sql)
        # Two aggregates over the SAME time column are still two aggregates, so
        # two CTEs — the sharing this test was named for was an artefact of one
        # scope carrying every ranking. What they share is the ORDER BY column;
        # the DIRECTION is what makes them different aggregates.
        assert sql.count("ROW_NUMBER()") == 2
        assert "ORDER BY orders.ordered_at DESC" in norm, sql
        assert "ORDER BY orders.ordered_at)" in norm, sql
        assert "_last_rn" not in sql, sql
        assert "_first_rn" not in sql, sql

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

    async def test_multiple_time_shifts_in_arithmetic_unique_ctes(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """DEV-1692: two arithmetic-wrapped time_shifts must not share a CTE name.

        The `_t{n}` placeholder counter restarts per formula, so both measures
        used to flatten as `_t0` — emitting `shifted__t0` twice (a duplicate-CTE
        parser error) and silently aliasing the second shift onto the first.
        """
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)
            ],
            measures=[
                ModelMeasure(formula="revenue:sum - time_shift(revenue:sum, -1, 'month')", name="growth"),
                ModelMeasure(formula="revenue:sum - time_shift(revenue:sum, -2, 'month')", name="growth_2m"),
            ],
        )
        sql = await _generate(generator, query, orders_model)

        ctes = _re.findall(r'(?:WITH|,)\s*"?(\w+)"?\s+AS\s*\(', sql)
        assert len(ctes) == len(set(ctes)), f"duplicate CTE names: {ctes}"
        assert len([c for c in ctes if c.startswith("shifted_")]) == 2

    async def test_multiple_time_shifts_resolve_to_distinct_aliases(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """DEV-1692: each shift keeps its own offset — no silent alias sharing.

        Guards the corruption the duplicate name masked: both expressions
        previously resolved to `orders._t0`, so `growth_2m` would have read
        `growth`'s -1 month shift.
        """
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)
            ],
            measures=[
                ModelMeasure(formula="revenue:sum - time_shift(revenue:sum, -1, 'month')", name="growth"),
                ModelMeasure(formula="revenue:sum - time_shift(revenue:sum, -2, 'month')", name="growth_2m"),
            ],
        )
        sql = await _generate(generator, query, orders_model)

        # Both shifts are hoisted, each under its OWN alias (legacy:
        # ``len({t.alias for t in shifts}) == len(shifts)`` over the two
        # ``time_shift`` transforms).
        shift_aliases = _hoisted_shift_aliases(sql)
        assert len(shift_aliases) == 2, (
            f"expected two distinctly-aliased hoisted shifts, got "
            f"{sorted(shift_aliases)} in:\n{sql}"
        )

        # Each shift keeps its own offset (legacy: ``{t.offset} == {-1, -2}``).
        # Both are backwards shifts, so the emitted interval is ``+ 1``/``+ 2``
        # months on the join side; the sign is captured too so a flipped
        # direction can't pass. The shift applies to the TRUNCATED period
        # start (period-boundary fix, DEV-1811 audit).
        offsets = set(
            _re.findall(
                r"DATE_TRUNC\('MONTH', orders\.created_at\) ([+-]) INTERVAL '(\d+) MONTH'",
                sql,
            )
        )
        assert offsets == {("+", "1"), ("+", "2")}, (
            f"expected -1 and -2 month shifts, got {sorted(offsets)} in:\n{sql}"
        )

        # The two measures do NOT resolve to the same expression (legacy:
        # ``by_name["growth"] != by_name["growth_2m"]``).
        growth = _alias_bodies(sql, "orders.growth")
        growth_2m = _alias_bodies(sql, "orders.growth_2m")
        assert len(growth) == 1, (
            f"'growth' must be emitted exactly once; got {growth} in:\n{sql}"
        )
        assert len(growth_2m) == 1, (
            f"'growth_2m' must be emitted exactly once; got {growth_2m} in:\n{sql}"
        )
        assert growth[0] != growth_2m[0], (
            f"growth and growth_2m share one shift expression: {growth[0]!r}"
        )

    async def test_hidden_transform_name_avoids_user_measure_collision(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """DEV-1692: a hoisted transform must not land on a user measure's name.

        Hidden names are built from the owning measure's field_name, so a user
        measure literally named `_t0_growth` would otherwise claim the same
        alias as the shift hoisted out of `growth` — the self-join CTE projects
        both under that name and the shift silently resolves to the user's
        measure (valid SQL, wrong numbers).
        """
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)
            ],
            measures=[
                ModelMeasure(formula="revenue:sum - time_shift(revenue:sum, -1, 'month')", name="growth"),
                ModelMeasure(formula="revenue:sum * 2", name="_t0_growth"),
            ],
        )
        sql = await _generate(generator, query, orders_model)

        # The hoisted shift's alias must not be one of the user-declared
        # result keys (legacy: ``transform_aliases & expression_aliases``
        # empty — the outer projection is a superset of the expression
        # aliases, so this is at least as strict).
        transform_aliases = _hoisted_shift_aliases(sql)
        assert transform_aliases, f"no hoisted shift alias found in:\n{sql}"
        result_keys = _outer_projection_names(sql)
        assert {"orders.growth", "orders._t0_growth"} <= result_keys, (
            f"expected both user measures in the public projection, got "
            f"{sorted(result_keys)} in:\n{sql}"
        )
        assert not (transform_aliases & result_keys), (
            f"hoisted transform alias collides with a user-declared result "
            f"key: {sorted(transform_aliases)} in:\n{sql}"
        )

        # `growth` must reference the hoisted shift, not the user's measure.
        growth_sql = _alias_bodies(sql, "orders.growth")[0]
        assert "orders._t0_growth" not in growth_sql.replace("orders._t0_growth_2", "")

    async def test_hidden_transform_name_avoids_dimension_collision(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """DEV-1692: dimensions alias as `<model>.<name>` too, so they're reserved.

        A dimension named `_t0_growth` otherwise claims the same alias as the
        shift hoisted out of `growth`, and the measure silently computes
        `revenue - <dimension>` instead of the period difference.
        """
        orders_model.default_time_dimension = "created_at"
        orders_model.columns.append(
            Column(name="_t0_growth", sql="customer_id", type=DataType.DOUBLE)
        )
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="_t0_growth")],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)
            ],
            measures=[
                ModelMeasure(formula="revenue:sum - time_shift(revenue:sum, -1, 'month')", name="growth"),
            ],
        )
        sql = await _generate(generator, query, orders_model)

        # Legacy: ``(dimension aliases | time-dimension aliases) &
        # transform_aliases`` empty. The outer projection carries both
        # declared dimensions (``orders._t0_growth``) and the time dimension
        # (``orders.created_at``), so the disjointness check is preserved.
        dim_aliases = _outer_projection_names(sql)
        assert {"orders._t0_growth", "orders.created_at"} <= dim_aliases, (
            f"expected both dimensions in the public projection, got "
            f"{sorted(dim_aliases)} in:\n{sql}"
        )
        transform_aliases = _hoisted_shift_aliases(sql)
        assert transform_aliases, f"no hoisted shift alias found in:\n{sql}"
        assert not (dim_aliases & transform_aliases)

        # And the measure computes against the shift, not the same-named
        # dimension (the corruption the alias collision caused).
        growth_sql = _alias_bodies(sql, "orders.growth")[0]
        assert "orders._t0_growth" not in growth_sql

    async def test_dev_1692_repro_shape(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """DEV-1692: the reported four-measure period-over-period shape.

        `growth_pct` alone carries two time_shift calls, so uniquification has
        to hold within a single formula as well as across measures.
        """
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)
            ],
            measures=[
                ModelMeasure(formula="revenue:sum", name="total_revenue"),
                ModelMeasure(formula="time_shift(revenue:sum, -1, 'month')", name="prev_revenue"),
                ModelMeasure(formula="revenue:sum - time_shift(revenue:sum, -1, 'month')", name="growth"),
                ModelMeasure(
                    formula=(
                        "(revenue:sum - time_shift(revenue:sum, -1, 'month')) "
                        "/ time_shift(revenue:sum, -1, 'month')"
                    ),
                    name="growth_pct",
                ),
            ],
        )
        sql = await _generate(generator, query, orders_model)

        ctes = _re.findall(r'(?:WITH|,)\s*"?(\w+)"?\s+AS\s*\(', sql)
        assert len(ctes) == len(set(ctes)), f"duplicate CTE names: {ctes}"

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
        # Should wrap in a post-filter SELECT. The typed pipeline inlines the
        # ``change(revenue:sum)`` measure (desugared to revenue_sum minus its
        # time-shift) into the post-filter predicate rather than referencing
        # the ``rev_change`` alias.
        assert "_filtered" in sql
        assert '"orders.revenue_sum" - "orders._time_shift_inner" < 0' in sql

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
        # Post-filter should be in the outer wrapper. The computed change
        # measure is inlined into the predicate (revenue_sum minus its
        # time-shift) rather than referenced by the ``rev_change`` alias.
        assert '"orders.revenue_sum" - "orders._time_shift_inner" > 0' in sql
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
        # The referenced aggregates are materialised in the (inlined) SELECT.
        assert "COUNT(*)" in sql
        assert "SUM(" in sql

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
        # PARTITION BY column order is semantically irrelevant; the typed
        # planner emits the keys in sorted order (customer_id before status).
        #
        # Compared against the re-emitted Window node rather than the raw text:
        # the transform chain is assembled as AST (DEV-1747 D8), so an OVER
        # clause this long is line-broken by sqlglot's pretty printer, and
        # collapsing whitespace still leaves the spaces it puts inside the
        # parens. The claim here is the window's SHAPE, not its line breaks.
        window = next(
            (
                w
                for w in sqlglot.parse_one(sql, read="postgres").find_all(
                    sqlglot.exp.Window,
                )
                if isinstance(w.this, sqlglot.exp.Rank)
            ),
            None,
        )
        assert window is not None, sql
        assert window.sql(dialect="postgres") == (
            'RANK() OVER (PARTITION BY "orders.customer_id", "orders.status" '
            'ORDER BY "orders.revenue_sum" DESC)'
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

    async def test_ntile_with_n_kwarg_in_filter(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """DEV-1492: ``ntile(<measure>, n=4) <= 1`` end-to-end.

        Mirrors ``test_dense_rank_in_filter_top_5_distinct``. The fix to
        ``parse_filter_expr`` (DEV-1492) preserves the ``n=4`` kwarg through
        operator normalization, so the filter parses as a TransformCall with
        kwargs and the planner extracts it as a hidden field. The window
        function must materialise inside the inner SELECT and the comparison
        must live in the outer ``_filtered`` wrapper.
        """
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customer_id")],
            measures=[ModelMeasure(formula="revenue:sum")],
            filters=["ntile(revenue:sum, n=4) <= 1"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "_filtered" in sql, f"expected post-filter wrapper, got:\n{sql}"
        inner_sql, outer_sql = sql.split("_filtered", 1)
        assert "NTILE(4)" in inner_sql, (
            f"NTILE(4) should be materialised in the inner SELECT, got:\n{sql}"
        )
        assert "NTILE(" not in outer_sql, (
            f"NTILE should not appear in the outer wrapper, got:\n{sql}"
        )
        assert "<= 1" in _norm(outer_sql), (
            f"<= 1 predicate should live in the outer wrapper, got:\n{sql}"
        )

    async def test_rank_with_partition_by_kwarg_in_filter(
        self, generator: SQLGenerator, orders_model: SlayerModel
    ) -> None:
        """DEV-1492: ``rank(<measure>, partition_by=<col>) <= 1`` end-to-end.

        Same fix path as ntile-with-n: ``partition_by=status`` must survive
        operator normalization so the planner sees it as a kwarg, binds it to
        a column ref, and emits ``RANK() OVER (PARTITION BY ...)`` in the
        inner SELECT. The ``<= 1`` predicate must land in the outer
        ``_filtered`` wrapper.
        """
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status"), ColumnRef(name="customer_id")],
            measures=[ModelMeasure(formula="revenue:sum")],
            filters=["rank(revenue:sum, partition_by=status) <= 1"],
        )
        sql = await _generate(generator, query, orders_model)
        assert "_filtered" in sql, f"expected post-filter wrapper, got:\n{sql}"
        inner_sql, outer_sql = sql.split("_filtered", 1)
        assert (
            'RANK() OVER (PARTITION BY "orders.status" '
            'ORDER BY "orders.revenue_sum" DESC)'
            in _norm(inner_sql)
        ), f"PARTITION BY status should appear in the inner SELECT, got:\n{sql}"
        assert "RANK()" not in outer_sql, (
            f"RANK should not appear in the outer wrapper, got:\n{sql}"
        )
        assert "<= 1" in _norm(outer_sql), (
            f"<= 1 predicate should live in the outer wrapper, got:\n{sql}"
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
        with pytest.raises(ValueError, match="not supported"):
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
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),

                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
                # Second numeric column so 2-arg stat aggregates
                # (corr(other=...) / covar_*(other=...)) have a valid LHS+RHS pair.
                Column(name="quantity", sql="quantity", type=DataType.DOUBLE),
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
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="revenue:sum")],
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
            measures=[ModelMeasure(formula="*:count")],
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
        )
        sql = await _generate(gen, query, orders_model)
        assert "COUNT(" in sql
        # Each dialect uses its own truncation function
        sql_upper = sql.upper()
        assert any(fn in sql_upper for fn in ["DATE_TRUNC", "STRFTIME", "TRUNC", "STR_TO_DATE"])

    @pytest.mark.parametrize("dialect", ["postgres", "mysql", "bigquery", "duckdb", "snowflake"])
    async def test_date_trunc_casts_unknown_typed_time_dim(self, dialect: str) -> None:
        """A time-dimension whose ``sql`` is a bare literal (or any expression
        whose live type is ``unknown``) must be wrapped in ``CAST(... AS
        TIMESTAMP)`` before being passed to ``DATE_TRUNC``. Postgres has
        multiple overloads keyed on the second argument's type and rejects
        ``DATE_TRUNC('month', '2025-12-01')`` with ``AmbiguousFunctionError``.

        Bare column references stay unwrapped — their live DB type is known,
        and forcing a cast could strip ``TIMESTAMPTZ`` to ``TIMESTAMP``.
        """
        gen = SQLGenerator(dialect=dialect)
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="ts", sql="'2025-12-01'", type=DataType.TIMESTAMP),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            ],
        )
        # Bare-literal time dim — must be cast.
        sql = await _generate(
            gen,
            SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="*:count")],
                time_dimensions=[TimeDimension(dimension=ColumnRef(name="ts"), granularity=TimeGranularity.MONTH)],
            ),
            model,
        )
        # sqlglot transpiles ``TIMESTAMP`` → ``DATETIME`` on MySQL / BigQuery,
        # so we don't assert the literal target-type spelling — only that the
        # literal is wrapped in a CAST.
        assert "CAST('2025-12-01' AS" in sql, sql
        # Bare-column time dim — must NOT be cast.
        sql = await _generate(
            gen,
            SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="*:count")],
                time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            ),
            model,
        )
        # The time-dim column itself must not be CAST-wrapped — its live
        # DB type is already known. A measure-level CAST elsewhere (e.g.
        # the DEV-1361 ``CAST(COUNT(*) AS INT)`` on ``*:count``) is fine;
        # the test pins the time-dim handling only.
        assert "CAST(ORDERS.CREATED_AT" not in sql.upper(), sql
        assert "CAST(CREATED_AT" not in sql.upper(), sql

    # T-SQL excluded: it now emits DATEADD via the dialect strategy, covered by
    # tests/dialects/test_multi_dialect_generation.py::test_calendar_time_shift
    # (DEV-1716).
    # DEV-1713: BigQuery is no longer excluded — the sqlglot round-trip
    # TypeError on the calendar time_shift INTERVAL construct that the prior
    # ``_SQLGLOT_TYPEERROR_DIALECTS`` carve-out worked around no longer
    # reproduces (upstream sqlglot / DEV-1716 dialect routing), so BigQuery
    # generator output is now validated like every other dialect.
    @pytest.mark.parametrize("dialect", [d for d in ALL_DIALECTS if d != "tsql"])
    async def test_calendar_time_shift(self, dialect: str, orders_model: SlayerModel) -> None:
        """Calendar-based time_shift should produce dialect-appropriate date arithmetic in shifted CTE."""
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="revenue:sum"), ModelMeasure(formula="time_shift(revenue:sum, -1, 'year')", name="rev_prev_year")],
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

    @pytest.mark.parametrize("dialect", ["mysql", "clickhouse"])
    async def test_window_measure_multi_unit_interval_dialect_correct(
        self, dialect: str, orders_model: SlayerModel,
    ) -> None:
        """Multi-unit windows (e.g. '1y2m3d') must render as separate per-unit
        INTERVAL clauses on MySQL and ClickHouse — never as a single
        Postgres-shaped quoted multi-unit literal which neither dialect parses.

        Codex flagged this as a real correctness bug during PR #64 review:
        `_duration_interval_sql` had only two branches (SQLite + "Postgres-style"),
        and the latter emitted `INTERVAL '1 year 2 month 3 day'` for every
        non-SQLite dialect.
        """
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"),
                              granularity=TimeGranularity.DAY),
            ],
            measures=[ModelMeasure(formula="revenue:sum(window='1y2m3d')",
                                   name="rev_w")],
        )
        sql = await _generate(generator=gen, query=query, model=orders_model)
        norm = _norm(sql).upper()
        # The broken Postgres-shape multi-unit literal must NOT appear.
        assert "INTERVAL '1 YEAR 2 MONTH 3 DAY'" not in norm, (
            f"Multi-unit Postgres-shape INTERVAL literal is invalid on {dialect}.\n"
            f"sql:\n{sql}"
        )
        # Per-unit INTERVAL clauses must each be present (sqlglot transpiles
        # exp.Interval per dialect; DEV-1835's regroup desugar renders the
        # amount as a quoted literal — `INTERVAL 'N' UNIT` — on both dialects).
        for piece in ("INTERVAL '1' YEAR", "INTERVAL '2' MONTH", "INTERVAL '3' DAY"):
            assert piece in norm, (
                f"Expected dialect-correct '{piece}' in {dialect} output.\n"
                f"sql:\n{sql}"
            )

    @pytest.mark.parametrize("dialect", ["mysql", "clickhouse"])
    async def test_window_measure_single_unit_interval_dialect_correct(
        self, dialect: str, orders_model: SlayerModel,
    ) -> None:
        """Even single-unit windows must render unquoted on MySQL/ClickHouse.

        The pre-refactor code emits `INTERVAL '7 day'` for single-unit windows
        on every non-SQLite dialect, which is invalid MySQL syntax (MySQL wants
        `INTERVAL 7 DAY`). After the AST refactor, sqlglot's per-dialect
        transpiler emits the canonical form for each dialect.
        """
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"),
                              granularity=TimeGranularity.DAY),
            ],
            measures=[ModelMeasure(formula="revenue:sum(window='7d')",
                                   name="rev_w")],
        )
        sql = await _generate(generator=gen, query=query, model=orders_model)
        norm = _norm(sql).upper()
        assert "INTERVAL '7 DAY'" not in norm, (
            f"Quoted single-unit INTERVAL literal is invalid on {dialect}.\n"
            f"sql:\n{sql}"
        )
        assert "INTERVAL '7' DAY" in norm, (
            f"Expected dialect-correct 'INTERVAL '7' DAY' in {dialect} output.\n"
            f"sql:\n{sql}"
        )

    # DEV-1317: cross-dialect stat-agg generation. The exact SQL shape per
    # Tier-1 dialect is pinned in TestStatAggsPerDialect; here we just confirm
    # the generator produces parseable SQL on every supported dialect.
    # Assertions check the function-call SHAPE (qualified column refs in the
    # arg slot), not just substring fragments — substrings like "STDDEV" or
    # "CORR" pass even when the aggregate has regressed because aliases
    # such as `revenue_stddev_samp` always contain the family name.

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    @pytest.mark.parametrize(
        "formula",
        [
            "revenue:stddev_samp",
            "revenue:stddev_pop",
            "revenue:var_samp",
            "revenue:var_pop",
        ],
    )
    async def test_one_arg_stat_agg_generation(
        self,
        dialect: str,
        formula: str,
        orders_model: SlayerModel,
    ) -> None:
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula=formula)],
        )
        sql = await _generate(generator=gen, query=query, model=orders_model)
        upper = sql.upper()
        assert "SELECT" in upper
        # The aggregate must wrap the resolved value column (orders.amount,
        # since the `revenue` Column has sql="amount") in its single-arg slot.
        assert "(ORDERS.AMOUNT)" in upper, (
            f"expected single-arg call (ORDERS.AMOUNT) in SQL for {formula!r} on {dialect}:\n{sql}"
        )

    # corr / covar_samp / covar_pop: MySQL and T-SQL are excluded here. MySQL
    # emits the variance-decomposition formula (not the single-arg call this
    # test asserts) and T-SQL likewise routes through build_covar_2arg; both
    # are covered correctly in tests/dialects/test_multi_dialect_generation.py
    # (test_two_arg_stat_formula_dialects_generate_valid_sql) — DEV-1716.
    @pytest.mark.parametrize(
        "dialect", [d for d in ALL_DIALECTS if d not in ("mysql", "tsql")],
    )
    @pytest.mark.parametrize(
        "formula",
        [
            "revenue:corr(other=quantity)",
            "revenue:covar_samp(other=quantity)",
            "revenue:covar_pop(other=quantity)",
        ],
    )
    async def test_two_arg_stat_agg_generation(
        self,
        dialect: str,
        formula: str,
        orders_model: SlayerModel,
    ) -> None:
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula=formula)],
        )
        sql = await _generate(generator=gen, query=query, model=orders_model)
        upper = sql.upper()
        assert "SELECT" in upper
        # Both legs (LHS column AND `other=` kwarg) must be qualified and
        # appear in the function-call's two-arg slot in that order. This
        # asymmetry vs the 1-arg test is what distinguishes the test bodies
        # for Sonar python:S4144 and pins the new `_resolve_agg_param` +
        # `_resolve_value_sql` qualification path for 2-arg stats.
        assert "(ORDERS.AMOUNT, ORDERS.QUANTITY)" in upper, (
            f"expected two-arg call (ORDERS.AMOUNT, ORDERS.QUANTITY) in SQL for {formula!r} "
            f"on {dialect}:\n{sql}"
        )

    # DEV-1716: ``test_two_arg_stat_agg_mysql_raises`` removed — MySQL now
    # emits the variance-decomposition formula for corr/covar (not
    # NotImplementedError). Covered by tests/dialects/
    # test_multi_dialect_generation.py::test_two_arg_stat_agg_mysql_emits_formula_valid_sql.

    @pytest.mark.parametrize(
        "dialect",
        ["postgres", "sqlite", "duckdb", "mysql", "clickhouse"],
    )
    async def test_dev1501_two_last_diff_time_cols_multi_dialect(
        self, dialect: str,
    ) -> None:
        """DEV-1501 cross-dialect: ``ORDER BY revenue:last(created_at) DESC,
        revenue:last(updated_at) ASC`` must materialise two distinct
        ranked aggregates per Tier-1 dialect, with dotted quoted aliases
        rendered correctly in the outer ORDER BY (no qualified-identifier
        misparse).
        """
        m = SlayerModel(
            name="orders", sql_table="public.orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="updated_at", sql="updated_at", type=DataType.TIMESTAMP),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
        )
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=["*:count"],
            dimensions=[ColumnRef(name="status")],
            order=[
                OrderItem(column="revenue:last(created_at)", direction="desc"),
                OrderItem(column="revenue:last(updated_at)", direction="asc"),
            ],
        )
        sql = await _generate(generator=gen, query=query, model=m)
        _assert_valid_sql(sql, dialect=dialect)
        # Two rankings, one per ordered aggregate, each in its own CTE — so
        # neither the rn-suffix scheme nor any dialect-specific alias rewrite
        # is in play (no dialect rewrites the ROW_NUMBER alias).
        assert sql.count("ROW_NUMBER()") == 2, sql
        assert "_last_rn" not in sql, sql
        assert "revenue_last_created_at" in sql, (
            f"Materialised created_at alias missing on {dialect}:\n{sql}"
        )
        assert "revenue_last_updated_at" in sql, (
            f"Materialised updated_at alias missing on {dialect}:\n{sql}"
        )
        # The OUTER ORDER BY must reference each materialised alias as a
        # SINGLE dotted identifier (the dotted body lives inside one quoted
        # identifier; it must NOT decompose into a two-part name whose first
        # part is read as a relation). The qualifier is the ranked CTE that
        # OWNS the column — the same form a cross-model sort key takes, and the
        # reason a re-parse of a dotted alias cannot go wrong here. Filter to
        # Column terms because some dialects (MySQL) emit a synthetic
        # ``CASE WHEN x IS NULL`` term to emulate NULLS LAST — those are
        # NULL-ordering wrappers, not separate references.
        tree = sqlglot.parse_one(sql, dialect=dialect)
        assert isinstance(tree, sqlglot.exp.Select)
        order = tree.args.get("order")
        assert order is not None, f"No outer ORDER BY on {dialect}:\n{sql}"
        order_cols = []
        for ordered in order.expressions:
            inner = ordered.this
            if not isinstance(inner, sqlglot.exp.Column):
                continue
            # ``table`` is the qualified-prefix slot: either absent, or the
            # ranked CTE's name. Anything else means the dotted alias was split
            # and its first hop mistaken for a relation.
            tbl = inner.args.get("table")
            assert tbl is None or tbl.name.startswith("_cm_"), (
                f"{dialect}: ORDER BY decomposes the dotted alias into a "
                f"qualified two-part name (table={tbl!r}):\n{sql}"
            )
            order_cols.append(inner.name)  # the identifier body
        assert set(order_cols) == {
            "orders.revenue_last_created_at",
            "orders.revenue_last_updated_at",
        }, (
            f"{dialect}: outer ORDER BY columns wrong: {order_cols!r}\n{sql}"
        )


class TestSqliteJsonExtractInGenerator:
    """DEV-1331: ``json_extract(col, '$.path')`` in ``Column.sql`` must not be
    rewritten to ``col -> '$.path'`` on SQLite — the operator returns the
    JSON-quoted form, silently breaking equality / CASE WHEN matches.
    """

    @pytest.fixture
    def model_with_json_dim(self) -> SlayerModel:
        return SlayerModel(
            name="users",
            sql_table="users",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="payload", sql="payload", type=DataType.TEXT),
                Column(
                    name="tier",
                    sql="json_extract(payload, '$.tier')",
                    type=DataType.TEXT,
                ),
                Column(
                    name="is_gold",
                    sql=(
                        "CASE LOWER(json_extract(payload, '$.tier')) "
                        "WHEN 'gold' THEN 1 ELSE 0 END"
                    ),
                    type=DataType.DOUBLE,
                ),
            ],
        )

    async def test_sqlite_column_sql_with_json_extract_dimension(
        self, model_with_json_dim: SlayerModel,
    ) -> None:
        gen = SQLGenerator(dialect="sqlite")
        query = SlayerQuery(
            source_model="users",
            dimensions=[ColumnRef(name="tier")],
            measures=[ModelMeasure(formula="*:count")],
        )
        sql = await _generate(generator=gen, query=query, model=model_with_json_dim)
        assert "JSON_EXTRACT(" in sql, f"missing JSON_EXTRACT in:\n{sql}"
        # The lossy ``payload -> '$.tier'`` form must not appear.
        assert "payload -> '$.tier'" not in sql, sql

    async def test_sqlite_column_sql_with_json_extract_in_case_when(
        self, model_with_json_dim: SlayerModel,
    ) -> None:
        gen = SQLGenerator(dialect="sqlite")
        query = SlayerQuery(
            source_model="users",
            measures=[ModelMeasure(formula="is_gold:sum")],
        )
        sql = await _generate(generator=gen, query=query, model=model_with_json_dim)
        assert "JSON_EXTRACT(" in sql, sql
        assert "payload -> '$.tier'" not in sql, sql

    async def test_sqlite_inline_sql_subquery_with_json_extract(self) -> None:
        model = SlayerModel(
            name="users",
            sql=(
                "SELECT id, json_extract(payload, '$.tier') AS tier "
                "FROM raw_users"
            ),
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="tier", sql="tier", type=DataType.TEXT),
            ],
        )
        gen = SQLGenerator(dialect="sqlite")
        query = SlayerQuery(
            source_model="users",
            dimensions=[ColumnRef(name="tier")],
            measures=[ModelMeasure(formula="*:count")],
        )
        sql = await _generate(generator=gen, query=query, model=model)
        assert "JSON_EXTRACT(" in sql, sql
        assert "payload -> '$.tier'" not in sql, sql

    async def test_postgres_column_sql_with_json_extract_unchanged(
        self, model_with_json_dim: SlayerModel,
    ) -> None:
        """Regression guard: rewrite is SQLite-only; Postgres path is untouched.

        Postgres has no scalar-vs-JSON quoting bug for ``json_extract``;
        sqlglot transpiles it to ``JSON_EXTRACT_PATH(j, 'k')``. We just
        assert the generator produces *some* form of JSON extraction and
        does not crash.
        """
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(
            source_model="users",
            dimensions=[ColumnRef(name="tier")],
            measures=[ModelMeasure(formula="*:count")],
        )
        sql = await _generate(generator=gen, query=query, model=model_with_json_dim)
        assert "JSON_EXTRACT" in sql.upper(), sql


class TestMedianPercentilePerDialect:
    """Per-dialect SQL emission for median and percentile aggregations.

    These pin the dialect-specific output of `_build_median` and
    `_build_percentile` and assert that MySQL raises ``NotImplementedError``
    (no native function, no Python-UDF mechanism).
    """

    def _measure(
        self,
        *,
        agg: str,
        agg_kwargs: dict[str, str] | None = None,
    ) -> AggRenderSpec:
        return AggRenderSpec(
            name="amount",
            sql="amount",
            model_name="orders",
            alias=f"amount_{agg}",
            aggregation=agg,
            agg_kwargs=agg_kwargs or {},
        )

    # --- median ------------------------------------------------------------

    def test_build_median_postgres(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        inner = sqlglot.parse_one("amount", dialect="postgres")
        sql = gen._build_median(inner).sql(dialect="postgres")
        assert sql == "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount)"

    def test_build_median_sqlite_uses_udf_call(self) -> None:
        gen = SQLGenerator(dialect="sqlite")
        inner = sqlglot.parse_one("amount", dialect="sqlite")
        sql = gen._build_median(inner).sql(dialect="sqlite")
        # sqlglot rewrites MEDIAN(x) to PERCENTILE_CONT(x, 0.5) for SQLite,
        # which our percentile_cont UDF handles. SQLite UDF lookup is
        # case-insensitive.
        assert sql == "PERCENTILE_CONT(amount, 0.5)"

    def test_build_median_clickhouse_unchanged(self) -> None:
        gen = SQLGenerator(dialect="clickhouse")
        inner = sqlglot.parse_one("amount", dialect="clickhouse")
        sql = gen._build_median(inner).sql(dialect="clickhouse")
        # ClickHouse has native median(); sqlglot transpiles to its parametric form.
        assert sql == "quantile(0.5)(amount)"

    def test_build_median_duckdb(self) -> None:
        gen = SQLGenerator(dialect="duckdb")
        inner = sqlglot.parse_one("amount", dialect="duckdb")
        sql = gen._build_median(inner).sql(dialect="duckdb")
        # sqlglot translates PERCENTILE_CONT to DuckDB's QUANTILE_CONT.
        assert "QUANTILE_CONT" in sql or "PERCENTILE_CONT" in sql

    def test_build_median_mysql_raises(self) -> None:
        gen = SQLGenerator(dialect="mysql")
        inner = sqlglot.parse_one("amount", dialect="mysql")
        with pytest.raises(NotImplementedError, match="MySQL"):
            gen._build_median(inner)

    # --- percentile --------------------------------------------------------

    def test_build_percentile_postgres(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        m = self._measure(agg="percentile", agg_kwargs={"p": "0.95"})
        sql = gen._build_percentile(m).sql(dialect="postgres")
        assert sql == "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY orders.amount)"

    def test_build_percentile_sqlite(self) -> None:
        gen = SQLGenerator(dialect="sqlite")
        m = self._measure(agg="percentile", agg_kwargs={"p": "0.5"})
        sql = gen._build_percentile(m).sql(dialect="sqlite")
        assert sql == "PERCENTILE_CONT(orders.amount, 0.5)"

    def test_build_percentile_clickhouse_emits_quantile(self) -> None:
        gen = SQLGenerator(dialect="clickhouse")
        m = self._measure(agg="percentile", agg_kwargs={"p": "0.75"})
        sql = gen._build_percentile(m).sql(dialect="clickhouse")
        # ClickHouse parametric aggregate syntax.
        assert sql == "quantile(0.75)(orders.amount)"

    @pytest.mark.parametrize("p", ["0.05", "0.25", "0.5", "0.95"])
    def test_build_percentile_clickhouse_param_substitution(self, p: str) -> None:
        gen = SQLGenerator(dialect="clickhouse")
        m = self._measure(agg="percentile", agg_kwargs={"p": p})
        sql = gen._build_percentile(m).sql(dialect="clickhouse")
        assert sql == f"quantile({p})(orders.amount)"

    def test_build_percentile_duckdb(self) -> None:
        gen = SQLGenerator(dialect="duckdb")
        m = self._measure(agg="percentile", agg_kwargs={"p": "0.5"})
        sql = gen._build_percentile(m).sql(dialect="duckdb")
        # sqlglot rewrites the WITHIN GROUP form to DuckDB's QUANTILE_CONT.
        assert "QUANTILE_CONT" in sql
        # Qualified column.
        assert "orders.amount" in sql

    def test_build_percentile_mysql_raises(self) -> None:
        gen = SQLGenerator(dialect="mysql")
        m = self._measure(agg="percentile", agg_kwargs={"p": "0.5"})
        with pytest.raises(NotImplementedError, match="MySQL"):
            gen._build_percentile(m)

    def test_build_percentile_missing_p_raises(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        m = self._measure(agg="percentile", agg_kwargs={})
        with pytest.raises(ValueError, match="requires parameter 'p'"):
            gen._build_percentile(m)

    def test_build_percentile_unsafe_p_rejected(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        m = self._measure(agg="percentile", agg_kwargs={"p": "0.5); DROP TABLE x; --"})
        with pytest.raises(ValueError, match="Unsafe value"):
            gen._build_percentile(m)

    def test_build_percentile_uses_model_level_default_p(self) -> None:
        """Model-level Aggregation(name='percentile', params=[p=...]) supplies the default."""
        gen = SQLGenerator(dialect="postgres")
        agg_def = Aggregation(
            name="percentile",
            params=[AggregationParam(name="p", sql="0.9")],
        )
        m = AggRenderSpec(
            name="amount",
            sql="amount",
            model_name="orders",
            alias="amount_percentile",
            aggregation="percentile",
            agg_kwargs={},
            aggregation_def=agg_def,
        )
        sql = gen._build_percentile(m).sql(dialect="postgres")
        assert sql == "PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY orders.amount)"

    def test_build_percentile_query_kwarg_overrides_model_default(self) -> None:
        """Query-time agg_kwargs win over the model-level default."""
        gen = SQLGenerator(dialect="postgres")
        agg_def = Aggregation(
            name="percentile",
            params=[AggregationParam(name="p", sql="0.9")],
        )
        m = AggRenderSpec(
            name="amount",
            sql="amount",
            model_name="orders",
            alias="amount_percentile",
            aggregation="percentile",
            agg_kwargs={"p": "0.25"},
            aggregation_def=agg_def,
        )
        sql = gen._build_percentile(m).sql(dialect="postgres")
        assert sql == "PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY orders.amount)"

    # --- A2: percentile p must be a numeric literal in [0, 1] -----------

    def test_build_percentile_rejects_non_literal_p(self) -> None:
        """`measure:percentile(p=quantity)` must fail at SQL-generation time
        with a clear validation error, not silently emit a column reference
        in PERCENTILE_CONT(p)'s direct-arg slot. Without this guard a
        non-literal `p` flows through `_resolve_agg_param` (which is
        identifier-friendly for the column-ref kwargs like `other=`), gets
        rendered as `orders.quantity`, and fails at the database with a
        dialect-specific error.
        """
        gen = SQLGenerator(dialect="postgres")
        m = AggRenderSpec(
            name="amount", sql="amount", model_name="orders",
            alias="amount_percentile", aggregation="percentile",
            agg_kwargs={"p": "quantity"},
        )
        with pytest.raises(ValueError, match="numeric literal"):
            gen._build_percentile(m)

    def test_build_percentile_rejects_p_out_of_range(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        m = AggRenderSpec(
            name="amount", sql="amount", model_name="orders",
            alias="amount_percentile", aggregation="percentile",
            agg_kwargs={"p": "1.5"},
        )
        with pytest.raises(ValueError, match=r"\[0, 1\]"):
            gen._build_percentile(m)

    def test_build_percentile_rejects_p_negative(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        m = AggRenderSpec(
            name="amount", sql="amount", model_name="orders",
            alias="amount_percentile", aggregation="percentile",
            agg_kwargs={"p": "-0.1"},
        )
        with pytest.raises(ValueError, match=r"\[0, 1\]"):
            gen._build_percentile(m)

    def test_build_percentile_rejects_non_literal_p_via_model_default(self) -> None:
        """Model-level defaults bypass `_validate_agg_param_value` (trust
        model: model authors are trusted). The new numeric-literal check
        catches anything that's not a number even on that path — closes
        the gap where a malicious model author could put `p=pg_sleep(10)`
        as a default. Codex review #3 on PR #82.
        """
        gen = SQLGenerator(dialect="postgres")
        agg_def = Aggregation(
            name="percentile",
            params=[AggregationParam(name="p", sql="pg_sleep(10)")],
        )
        m = AggRenderSpec(
            name="amount", sql="amount", model_name="orders",
            alias="amount_percentile", aggregation="percentile",
            agg_kwargs={}, aggregation_def=agg_def,
        )
        with pytest.raises(ValueError, match="numeric literal"):
            gen._build_percentile(m)


class TestStatAggsPerDialect:
    """Per-dialect SQL emission for the new statistical aggregations
    (DEV-1317): stddev_samp, stddev_pop, var_samp, var_pop, corr,
    covar_samp, covar_pop.

    These pin the observed SQL output for each dialect — including
    sqlglot's transpilation quirks (e.g., var_samp → VARIANCE on SQLite,
    var_pop → VARIANCE_POP on SQLite/MySQL) — so the SQLite UDF
    registration knows which names to alias. The expected outputs use
    fully model-qualified column references (``orders.amount`` etc.) —
    pinning the post-refactor invariant that all three dialect-aware
    builders go through ``_resolve_sql`` for the value column AND for
    the second column on two-arg stats, matching the standard
    sum/avg/min/max path.
    """

    def _measure(
        self,
        *,
        agg: str,
        agg_kwargs: dict[str, str] | None = None,
    ) -> AggRenderSpec:
        return AggRenderSpec(
            name="amount",
            sql="amount",
            model_name="orders",
            alias=f"amount_{agg}",
            aggregation=agg,
            agg_kwargs=agg_kwargs or {},
        )

    # --- stddev_samp -------------------------------------------------------

    @pytest.mark.parametrize(
        "dialect,expected",
        [
            ("postgres", "STDDEV_SAMP(orders.amount)"),
            ("duckdb", "STDDEV_SAMP(orders.amount)"),
            ("mysql", "STDDEV_SAMP(orders.amount)"),
            ("sqlite", "STDDEV_SAMP(orders.amount)"),
        ],
    )
    def test_build_stddev_samp(self, dialect: str, expected: str) -> None:
        gen = SQLGenerator(dialect=dialect)
        m = self._measure(agg="stddev_samp")
        sql = gen._build_agg(m)[0].sql(dialect=dialect)
        assert sql == expected

    # --- stddev_pop --------------------------------------------------------

    @pytest.mark.parametrize(
        "dialect,expected",
        [
            ("postgres", "STDDEV_POP(orders.amount)"),
            ("duckdb", "STDDEV_POP(orders.amount)"),
            ("mysql", "STDDEV_POP(orders.amount)"),
            ("sqlite", "STDDEV_POP(orders.amount)"),
        ],
    )
    def test_build_stddev_pop(self, dialect: str, expected: str) -> None:
        gen = SQLGenerator(dialect=dialect)
        m = self._measure(agg="stddev_pop")
        sql = gen._build_agg(m)[0].sql(dialect=dialect)
        assert sql == expected

    # --- var_samp ----------------------------------------------------------

    @pytest.mark.parametrize(
        "dialect,expected",
        [
            ("postgres", "VAR_SAMP(orders.amount)"),
            # sqlglot rewrites VAR_SAMP → VARIANCE on SQLite/DuckDB; the
            # SQLite UDF is therefore registered under the alias `variance`
            # so generator output still resolves at runtime. MySQL is the
            # exception: sqlglot's MySQL dialect rewrites the same way, but
            # MySQL's ``VARIANCE`` is an alias for ``VAR_POP`` (population
            # variance), so the rewritten SQL would silently return the
            # wrong value. The generator emits ``VAR_SAMP`` directly on
            # MySQL via ``exp.Anonymous`` to bypass the transpile.
            ("duckdb", "VARIANCE(orders.amount)"),
            ("mysql", "VAR_SAMP(orders.amount)"),
            ("sqlite", "VARIANCE(orders.amount)"),
        ],
    )
    def test_build_var_samp(self, dialect: str, expected: str) -> None:
        gen = SQLGenerator(dialect=dialect)
        m = self._measure(agg="var_samp")
        sql = gen._build_agg(m)[0].sql(dialect=dialect)
        assert sql == expected

    # --- var_pop -----------------------------------------------------------

    @pytest.mark.parametrize(
        "dialect,expected",
        [
            ("postgres", "VAR_POP(orders.amount)"),
            ("duckdb", "VAR_POP(orders.amount)"),
            # sqlglot rewrites VAR_POP → VARIANCE_POP on SQLite (handled by
            # a registered UDF alias). MySQL gets the same buggy rewrite,
            # but ``VARIANCE_POP`` is not a real MySQL function — the
            # generator emits ``VAR_POP`` directly via ``exp.Anonymous``.
            ("mysql", "VAR_POP(orders.amount)"),
            ("sqlite", "VARIANCE_POP(orders.amount)"),
        ],
    )
    def test_build_var_pop(self, dialect: str, expected: str) -> None:
        gen = SQLGenerator(dialect=dialect)
        m = self._measure(agg="var_pop")
        sql = gen._build_agg(m)[0].sql(dialect=dialect)
        assert sql == expected

    # --- corr (2-arg via `other=` kwarg) ----------------------------------

    # corr / covar_samp / covar_pop all share the 2-arg shape and the
    # `other=` kwarg parameter; parametrize once instead of repeating.
    @pytest.mark.parametrize(
        "agg,sql_fn",
        [
            ("corr", "CORR"),
            ("covar_samp", "COVAR_SAMP"),
            ("covar_pop", "COVAR_POP"),
        ],
    )
    @pytest.mark.parametrize("dialect", ["postgres", "duckdb", "sqlite"])
    def test_build_two_arg_stat_emits_two_arg_call(
        self, dialect: str, agg: str, sql_fn: str,
    ) -> None:
        gen = SQLGenerator(dialect=dialect)
        m = self._measure(agg=agg, agg_kwargs={"other": "quantity"})
        sql = gen._build_agg(m)[0].sql(dialect=dialect)
        # Both legs go through _resolve_sql, so a bare `quantity` kwarg
        # qualifies under the LHS measure's model_name.
        assert sql == f"{sql_fn}(orders.amount, orders.quantity)"

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    def test_build_two_arg_stat_clickhouse(self, agg: str) -> None:
        gen = SQLGenerator(dialect="clickhouse")
        m = self._measure(agg=agg, agg_kwargs={"other": "quantity"})
        sql = gen._build_agg(m)[0].sql(dialect="clickhouse")
        # ClickHouse casing is its own thing; assert the call shape only.
        assert sql.lower() == f"{agg.lower()}(orders.amount, orders.quantity)"

    # DEV-1716: ``test_build_two_arg_stat_mysql_raises`` removed — MySQL now
    # emits the variance-decomposition formula (not NotImplementedError) via
    # the dialect strategy. Covered by
    # tests/dialects/test_generator_delegation.py::TestStatAggsPerDialect
    # ::test_build_two_arg_stat_mysql_emits_formula.

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    def test_build_two_arg_stat_mysql_missing_other_prioritises_param_error(
        self, agg: str,
    ) -> None:
        """When BOTH conditions hold (MySQL dialect AND missing `other=`
        kwarg), the missing-required-param error is more useful to the user
        than "MySQL not supported" — it points at the actual mistake. Codex
        review #5 on PR #82: the MySQL guard ran before `other=` resolution.
        """
        gen = SQLGenerator(dialect="mysql")
        m = self._measure(agg=agg, agg_kwargs={})
        with pytest.raises(ValueError, match=r"requires parameter 'other'"):
            gen._build_agg(m)

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    def test_build_two_arg_stat_missing_other_raises(self, agg: str) -> None:
        gen = SQLGenerator(dialect="postgres")
        m = self._measure(agg=agg, agg_kwargs={})
        with pytest.raises(ValueError, match=r"requires parameter 'other'|other="):
            gen._build_agg(m)

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    def test_build_two_arg_stat_unsafe_other_rejected(self, agg: str) -> None:
        gen = SQLGenerator(dialect="postgres")
        m = self._measure(
            agg=agg,
            agg_kwargs={"other": "quantity); DROP TABLE x; --"},
        )
        with pytest.raises(ValueError, match="Unsafe value"):
            gen._build_agg(m)

    # --- filter wrapping ---------------------------------------------------

    def test_build_stddev_samp_with_filter_wraps_value(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        m = AggRenderSpec(
            name="amount",
            sql="amount",
            model_name="orders",
            alias="amount_stddev_samp",
            aggregation="stddev_samp",
            agg_kwargs={},
            filter_sql="status = 'completed'",
        )
        sql = gen._build_agg(m)[0].sql(dialect="postgres")
        # Filter wraps the qualified column reference.
        assert "CASE WHEN status = 'completed' THEN orders.amount END" in sql
        assert "STDDEV_SAMP" in sql

    def test_build_corr_with_filter_wraps_both_columns(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        m = AggRenderSpec(
            name="amount",
            sql="amount",
            model_name="orders",
            alias="amount_corr",
            aggregation="corr",
            agg_kwargs={"other": "quantity"},
            filter_sql="status = 'completed'",
        )
        sql = gen._build_agg(m)[0].sql(dialect="postgres")
        # Both legs of corr() must be wrapped in CASE WHEN so non-matching
        # rows contribute NULL pairs (which the aggregate skips entirely).
        assert sql.count("CASE WHEN status = 'completed'") == 2
        assert "CORR(" in sql
        # Both legs are also qualified.
        assert "orders.amount" in sql
        assert "orders.quantity" in sql


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
        await s.save_datasource(DatasourceConfig(name="test", type="postgres"))
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
                    sql="CASE WHEN customers.regions.name = 'US' THEN 1 ELSE 0 END",
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
        await engine.storage.save_model(chained_model)
        sql = (await engine.execute(query, dry_run=True)).sql
        join_aliases = _join_aliases(sql)
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
                    sql="users.orgs.signup_date",
                    type=DataType.TIMESTAMP,
                ),

            ],
            joins=[
                ModelJoin(target_model="users", join_pairs=[["user_id", "id"]]),
            ],
        )
        await storage.save_model(model)
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
        sql = (await engine.execute(query, dry_run=True)).sql
        join_aliases = _join_aliases(sql)
        assert "users" in join_aliases
        assert "users__orgs" in join_aliases

    async def test_measure_sql_with_path_alias_infers_joins(
        self, engine: SlayerQueryEngine, chained_model: SlayerModel
    ) -> None:
        """DEV-1502: measure SQL like ``customers__regions.population``
        infers joins for both intermediate hops (symmetric to the
        dimension and time-dimension cases above).
        """
        chained_model.columns.append(
            Column(name="region_pop_sum", sql="customers.regions.population", type=DataType.DOUBLE)
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="region_pop_sum:sum")],
        )
        await engine.storage.save_model(chained_model)
        sql = (await engine.execute(query, dry_run=True)).sql
        join_aliases = _join_aliases(sql)
        assert "customers" in join_aliases
        assert "customers__regions" in join_aliases
        # The aggregate body must reference the path-aliased ref — not the
        # bare `region_pop_sum` (which would refer to a non-existent column
        # on `orders`). Mirrors the DEV-1494 strengthening pattern.
        assert "SUM(customers__regions.population)" in sql


class TestMeasureSourceSqlJoinInference:
    """DEV-1502 → DEV-1709 (Stage 5): an AGGREGATE slot whose source
    ``Column.sql`` contains a ``__``-delimited join-path alias (or a
    sibling derived ref whose own sql does) ISOLATES into a host-rooted
    ``_cm_*`` CTE that pulls the implied LEFT JOINs inside its own scope
    (widened Law-3 trigger, D1) — so a measure-pulled join can never
    multiply the host rows seen by sibling measures. Row-level carriers
    (dimensions / WHERE filters) keep base-pulling their joins into the
    host ``_base`` (DEV-1484 / DEV-1494 — Law-1 territory).
    """

    @pytest.fixture
    async def storage(self, tmp_path):
        """orders → customers → regions chain reused across most tests."""
        s = YAMLStorage(base_dir=str(tmp_path))
        await s.save_datasource(DatasourceConfig(name="test", type="postgres"))
        await s.save_model(SlayerModel(
            name="regions", sql_table="regions", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
                Column(name="population", sql="population", type=DataType.DOUBLE),
                Column(name="props", sql="props", type=DataType.TEXT),
                Column(name="weight", sql="weight", type=DataType.DOUBLE),
                Column(name="payment_amount", sql="payment_amount", type=DataType.DOUBLE),
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

    def _orders_model(self, *, extra_columns=None) -> SlayerModel:
        """Build the orders model with the standard customer/regions chain
        and any extra derived columns the per-test wants.
        """
        cols = [
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        ]
        if extra_columns:
            cols.extend(extra_columns)
        return SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=cols,
            joins=[
                ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ],
        )

    @pytest.fixture
    def engine(self, storage) -> SlayerQueryEngine:
        return SlayerQueryEngine(storage=storage)

    # ------------------------------------------------------------------
    # DEV-1531 (landed Stage 5 / DEV-1709): a first/last over a cross-join
    # derived source ISOLATES into a host-rooted `_cm_*` CTE (widened
    # Law-3 trigger, D1); inside that CTE the crossed value is
    # materialised as a `_val_<n>` projection in the ranked subquery
    # (Law-2) instead of leaking the path-qualified ref into the outer
    # aggregate body. The two no-op negatives stay green (local/bare
    # sources need neither isolation nor materialisation).
    # ------------------------------------------------------------------
    @staticmethod
    def _assert_ref_only_in_val(sql: str, ref: str) -> None:
        """Assert ``ref`` (a cross-table-qualified column) appears ONLY as
        the source of a materialised ``<ref> AS _val_<n>`` projection inside
        the ranked subquery — i.e. it never leaks into an outer-scope
        aggregate body / HAVING / composite where it would fail at runtime
        with ``no such column``.
        """
        norm = _norm(sql)
        first_proj = _re.search(rf"{_re.escape(ref)} AS _val_\d+", norm)
        assert first_proj is not None, (
            f"DEV-1531: expected a materialised `{ref} AS _val_<n>` "
            f"projection inside the ranked subquery:\n{norm}"
        )
        first_subquery = norm.find("FROM (")
        assert first_subquery != -1, (
            f"DEV-1531: no ranked subquery found at all in:\n{norm}"
        )
        assert first_proj.start() > first_subquery, (
            f"DEV-1531: `_val` projection of `{ref}` is not inside the ranked "
            f"subquery:\n{norm}"
        )
        stripped = _re.sub(rf"{_re.escape(ref)} AS _val_\d+", "", norm)
        assert ref not in stripped, (
            f"DEV-1531: `{ref}` leaked outside the ranked subquery's _val "
            f"projection (would fail with `no such column`):\n{norm}"
        )

    async def test_local_last_with_path_aliased_derived_source(
        self, engine: SlayerQueryEngine
    ) -> None:
        model = self._orders_model(extra_columns=[
            Column(name="region_payment", sql="customers.regions.payment_amount",
                   type=DataType.DOUBLE),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="region_payment:last(orders.created_at)")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        # The crossing source isolates. It is the RANKED route (DEV-1748): a
        # first/last isolates because it ranks, which subsumes the crossing
        # trigger this test was written for — the verdict is the same one.
        assert "_cm_" in sql, f"expected an isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "LEFT JOIN customers" in cm_body, cm_body
        assert "customers__regions" in cm_body, cm_body
        # The joins live ONLY inside the CTE — never in the host scope.
        assert sql.count("LEFT JOIN customers AS customers") == 1, sql
        self._assert_ref_only_in_val(sql, "customers__regions.payment_amount")
        assert "THEN _val" in _norm(sql), sql

    async def test_local_last_with_single_dot_derived_source(
        self, engine: SlayerQueryEngine
    ) -> None:
        await engine.storage.save_model(SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
                Column(name="balance", sql="balance", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        ))
        model = self._orders_model(extra_columns=[
            Column(name="cust_balance", sql="customers.balance", type=DataType.DOUBLE),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="cust_balance:last(orders.created_at)")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        assert "_cm_" in sql, f"expected an isolation CTE:\n{sql}"
        assert "LEFT JOIN customers" in _extract_cte_body(sql, r"_cm_\w+"), sql
        assert sql.count("LEFT JOIN customers AS customers") == 1, sql
        self._assert_ref_only_in_val(sql, "customers.balance")

    async def test_filtered_last_cross_join_value_materialized(
        self, engine: SlayerQueryEngine
    ) -> None:
        model = self._orders_model(extra_columns=[
            Column(name="region_payment", sql="customers.regions.payment_amount",
                   type=DataType.DOUBLE, filter="orders.amount > 100"),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="region_payment:last(orders.created_at)")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        assert "_cm_" in sql, f"expected an isolation CTE:\n{sql}"
        self._assert_ref_only_in_val(sql, "customers__regions.payment_amount")
        # The measure's ``Column.filter`` is a WHERE on the rows being ranked.
        # It used to be a dedicated sentinel rank column (``_last_rn_f0``) plus
        # a match flag (``_match_f0``) consulted by alias from outside, because
        # the ranking was shared with every other aggregate in the query and
        # could not simply drop rows. In its own scope it can.
        norm = _norm(sql)
        assert "_last_rn_f0" not in norm, sql
        assert "_match_f0" not in norm, sql
        assert "WHERE orders.amount > 100" in norm, sql
        assert "THEN _val" in norm, sql

    async def test_two_last_sharing_value_dedupe(
        self, engine: SlayerQueryEngine
    ) -> None:
        """Two measures over the SAME crossing sql but DISTINCT derived
        columns intern to distinct ``AggregateKey``s (identity carries the
        column name), so each isolates into its OWN host-rooted CTE with
        its own ``_val`` materialisation — per-slot isolation, no CTE
        merging (DEV-1709 interview decision; merging is DEV-1688 /
        ``may_inline`` territory). Dedupe is therefore per-CTE."""
        model = self._orders_model(extra_columns=[
            Column(name="region_payment_a", sql="customers.regions.payment_amount",
                   type=DataType.DOUBLE),
            Column(name="region_payment_b", sql="customers.regions.payment_amount",
                   type=DataType.DOUBLE),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[
                ModelMeasure(formula="region_payment_a:last(orders.created_at)"),
                ModelMeasure(formula="region_payment_b:last(orders.created_at)"),
            ],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        self._assert_ref_only_in_val(sql, "customers__regions.payment_amount")
        norm = _norm(sql)
        cm_cte_names = _re.findall(r"(_cm_\w+)\s+AS\s*\(", sql)
        assert len(cm_cte_names) == 2, (
            f"expected one isolation CTE per distinct crossing aggregate; "
            f"got {cm_cte_names}:\n{sql}"
        )
        # One _val materialisation per CTE, each consumed exactly once
        # within its own scope (sibling CTEs are self-contained statements
        # with scope-local ``_val`` numbering).
        assert norm.count(" AS _val") == 2, sql
        for cte_name in cm_cte_names:
            body = _extract_cte_body(sql, _re.escape(cte_name))
            body_norm = _norm(body)
            assert body_norm.count(" AS _val") == 1, sql
            assert body_norm.count("THEN _val") == 1, sql
        assert '"orders.region_payment_a_last_created_at"' in norm, sql
        assert '"orders.region_payment_b_last_created_at"' in norm, sql

    async def test_same_sql_different_type_no_bad_dedupe(
        self, engine: SlayerQueryEngine
    ) -> None:
        model = self._orders_model(extra_columns=[
            Column(name="region_pay_d", sql="customers.regions.payment_amount * 2",
                   type=DataType.DOUBLE),
            Column(name="region_pay_i", sql="customers.regions.payment_amount * 2",
                   type=DataType.INT),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[
                ModelMeasure(formula="region_pay_d:last(orders.created_at)"),
                ModelMeasure(formula="region_pay_i:last(orders.created_at)"),
            ],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        norm = _norm(sql)
        assert norm.count(" AS _val") == 2, (
            f"same SQL, different type must not dedupe onto one _val:\n{sql}"
        )
        assert _re.search(r"AS DOUBLE PRECISION\) AS _val_\d+", norm), sql
        assert _re.search(r"AS INT\) AS _val_\d+", norm), sql
        # The raw crossing ref must never surface in the combined outer
        # SELECT (after the last CTE closes) — only the CTE output columns.
        outer = sql[sql.rfind("\n)") + 2:]
        assert "customers__regions.payment_amount" not in outer, sql

    async def test_composite_last_cross_join_source(
        self, engine: SlayerQueryEngine
    ) -> None:
        model = self._orders_model(extra_columns=[
            Column(name="region_payment", sql="customers.regions.payment_amount",
                   type=DataType.DOUBLE),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="region_payment:last(orders.created_at) + 1")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        # Composite lowering (F3): the crossing first/last LEAF isolates;
        # the `+ 1` composite renders in the combined SELECT via the CTE's
        # projected alias.
        assert "_cm_" in sql, f"expected an isolation CTE:\n{sql}"
        self._assert_ref_only_in_val(sql, "customers__regions.payment_amount")
        outer = sql[sql.rfind("\n)") + 2:]
        assert "+ 1" in outer, (
            f"composite must render in the combined outer SELECT:\n{sql}"
        )

    async def test_having_last_cross_join_source(
        self, engine: SlayerQueryEngine
    ) -> None:
        """An aggregate-phase query filter referencing the isolated
        first/last routes to the OUTER WHERE on the combined SELECT
        (DEV-1503 rule; HAVING-into-the-CTE would surface host rows as
        NULL instead of dropping them)."""
        model = self._orders_model(extra_columns=[
            Column(name="region_payment", sql="customers.regions.payment_amount",
                   type=DataType.DOUBLE),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="region_payment:last(orders.created_at)")],
            filters=["region_payment:last(orders.created_at) > 100"],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        norm = _norm(sql)
        assert "_cm_" in sql, f"expected an isolation CTE:\n{sql}"
        self._assert_ref_only_in_val(sql, "customers__regions.payment_amount")
        assert norm.count(" AS _val") == 1, sql
        # Routed to the combined SELECT's WHERE — never HAVING.
        assert "HAVING" not in norm, sql
        outer = sql[sql.rfind("\n)") + 2:]
        assert "> 100" in outer, (
            f"aggregate filter must land in the combined outer WHERE:\n{sql}"
        )
        assert "WHERE" in outer, sql

    async def test_val_alias_avoids_source_column_collision(
        self, engine: SlayerQueryEngine
    ) -> None:
        model = self._orders_model(extra_columns=[
            Column(name="_val_0", sql="amount", type=DataType.DOUBLE),
            Column(name="region_payment", sql="customers.regions.payment_amount",
                   type=DataType.DOUBLE),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="region_payment:last(orders.created_at)")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        norm = _norm(sql)
        # The model's real `_val_0` column must not be shadowed: whatever
        # alias the allocator picks for the materialisation, it is never
        # `_val_0`, and the outer aggregate references the allocated one.
        allocated = _re.findall(r"AS (_val_\d+)", norm)
        assert allocated, f"expected a _val materialisation:\n{sql}"
        assert "_val_0" not in allocated, (
            f"allocator must skip the model's own `_val_0` column:\n{sql}"
        )
        assert f"THEN {allocated[0]}" in norm, sql
        self._assert_ref_only_in_val(sql, "customers__regions.payment_amount")

    async def test_local_only_derived_first_last_pulls_no_join(
        self, engine: SlayerQueryEngine
    ) -> None:
        """The negative control: a LOCAL-only derived source (``amount * 2``)
        crosses no join, so the ranked CTE pulls none.

        This asserted no ``_val`` materialisation at all, which was how "crosses
        no join" showed up when only a CROSSING value was materialised. Every
        value crosses the ranked scope's projection boundary now (P-B), so the
        observable claim is the one that was always the point: no JOIN."""
        model = self._orders_model(extra_columns=[
            Column(name="double_amount", sql="amount * 2", type=DataType.DOUBLE),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="double_amount:last(orders.created_at)")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        assert _join_aliases(_extract_cte_body(sql, r"_cm_\w+")) == set(), sql
        assert "orders.amount * 2" in _norm(sql), sql

    async def test_bare_source_first_last_pulls_no_join(
        self, engine: SlayerQueryEngine
    ) -> None:
        """The same control for a bare same-table source."""
        model = self._orders_model()
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:last(orders.created_at)")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        assert _join_aliases(_extract_cte_body(sql, r"_cm_\w+")) == set(), sql
        assert "orders.amount AS _val" in _norm(sql), sql
        assert "THEN _val" in _norm(sql), sql

    # ------------------------------------------------------------------
    # Core path-alias discovery
    # ------------------------------------------------------------------

    async def test_single_hop_path_alias_in_measure_sql(
        self, engine: SlayerQueryEngine
    ) -> None:
        """A single-hop ``customers.region_id`` alias in the measure source
        SQL isolates host-rooted; the ``customers`` join lives inside the
        ``_cm_*`` CTE only.
        """
        model = self._orders_model(extra_columns=[
            Column(
                name="customer_region_count",
                sql="customers.region_id",
                type=DataType.DOUBLE,
            ),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="customer_region_count:count_distinct")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        assert "_cm_" in sql, f"expected host-rooted isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "LEFT JOIN customers" in cm_body, sql
        assert "COUNT(DISTINCT customers.region_id)" in cm_body, sql
        # The join lives ONLY inside the CTE; no deeper hop is pulled.
        assert sql.count("LEFT JOIN customers AS customers") == 1, sql
        assert "customers__regions" not in _join_aliases(sql)

    async def test_composite_arithmetic_path_aliased_measure(
        self, engine: SlayerQueryEngine
    ) -> None:
        """``region_pop_sum:sum + amount:sum`` is rendered as a composite
        AGGREGATE slot (``ArithmeticKey`` wrapping two ``AggregateKey``s).
        The _visit recursion must descend through the operands so the
        path-aliased operand's joins surface.
        """
        model = self._orders_model(extra_columns=[
            Column(
                name="region_pop_sum",
                sql="customers.regions.population",
                type=DataType.DOUBLE,
            ),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="region_pop_sum:sum + amount:sum")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        # F3 composite lowering: the crossing leaf isolates into its own
        # host-rooted CTE; the local leaf stays in the base; the `+`
        # composite renders only in the combined SELECT.
        assert "_cm_" in sql, f"expected host-rooted isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "SUM(customers__regions.population)" in cm_body, sql
        assert "LEFT JOIN customers" in cm_body, sql
        assert "SUM(orders.amount)" not in cm_body, sql
        assert "SUM(orders.amount)" in sql
        assert sql.count("LEFT JOIN customers AS customers") == 1, sql
        outer = sql[sql.rfind("\n)") + 2:]
        assert "+" in outer, (
            f"composite must render in the combined outer SELECT:\n{sql}"
        )

    async def test_composite_scalar_call_path_aliased_measure(
        self, engine: SlayerQueryEngine
    ) -> None:
        """``coalesce(region_pop_sum:sum, 0)`` is a ``ScalarCallKey``
        wrapping an ``AggregateKey``. The _visit recursion descends
        through ``key.args``.
        """
        model = self._orders_model(extra_columns=[
            Column(
                name="region_pop_sum",
                sql="customers.regions.population",
                type=DataType.DOUBLE,
            ),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="coalesce(region_pop_sum:sum, 0)")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        assert "_cm_" in sql, f"expected host-rooted isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "SUM(customers__regions.population)" in cm_body, sql
        assert "LEFT JOIN customers" in cm_body, sql
        outer = sql[sql.rfind("\n)") + 2:]
        assert "COALESCE" in outer.upper(), (
            f"scalar-call composite must render in the combined outer SELECT:\n{sql}"
        )

    async def test_mode_a_function_wrapping_path_alias_in_column_sql(
        self, engine: SlayerQueryEngine
    ) -> None:
        """A Mode-A SQL function call wrapping a ``__`` ref in the column
        sql (``json_extract(customers__regions.props, '$.x')``) still
        triggers join discovery — ``_joined_paths_in_sql`` walks the
        parsed AST regardless of wrapper functions.
        """
        model = self._orders_model(extra_columns=[
            Column(
                name="region_prop_x",
                sql="json_extract(customers.regions.props, '$.x')",
                type=DataType.DOUBLE,
            ),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="region_prop_x:sum")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        assert "_cm_" in sql, f"expected host-rooted isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "LEFT JOIN customers" in cm_body, sql
        # The aggregate body wraps the JSON-extract call, qualified to
        # the path alias (the bare alias `region_prop_x` would fail).
        assert "customers__regions.props" in cm_body, sql

    async def test_sibling_derived_chain_to_path_alias(
        self, engine: SlayerQueryEngine
    ) -> None:
        """``doubled_pop`` references sibling ``pop_helper``, whose own
        sql crosses the customers→regions join. ``_expand_derived_column_sql``
        recursively inlines siblings; the new collector scans the
        recursively-expanded SQL.
        """
        model = self._orders_model(extra_columns=[
            Column(
                name="pop_helper",
                sql="customers.regions.population",
                type=DataType.DOUBLE,
            ),
            Column(
                name="doubled_pop",
                sql="pop_helper * 2",
                type=DataType.DOUBLE,
            ),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="doubled_pop:sum")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        assert "_cm_" in sql, f"expected host-rooted isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "LEFT JOIN customers" in cm_body, sql
        # Sibling recursive inlining preserves the path alias.
        assert "customers__regions.population" in cm_body, sql

    # DEV-1531 local first/last cross-join source materialisation is pinned by
    # `test_local_last_with_path_aliased_derived_source` (in the DEV-1531 harvest
    # block above, with the stronger `_assert_ref_only_in_val` assertion). The
    # earlier tracking placeholder was consolidated into it (CodeRabbit, PR #264)
    # so a single strict-xfail flips at Stage 5.

    async def test_post_transform_wrapping_path_aliased_measure(
        self, engine: SlayerQueryEngine
    ) -> None:
        """A POST-phase transform like ``cumsum(region_pop_sum:sum)`` aux-
        materialises its inner ``AggregateKey`` into ``base_render_order``.
        The new collector visits aux slots the same way it visits public
        AGGREGATE slots, so the join discovery still fires.
        """
        model = self._orders_model(extra_columns=[
            Column(
                name="region_pop_sum",
                sql="customers.regions.population",
                type=DataType.DOUBLE,
            ),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="cumsum(region_pop_sum:sum)")],
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        # The inner crossing aggregate isolates host-rooted; cumsum wraps
        # the combined output. The path-aliased ref lives ONLY inside the
        # `_cm_*` CTE body.
        assert "_cm_" in sql, f"expected host-rooted isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "customers__regions.population" in cm_body, sql
        assert "LEFT JOIN customers" in cm_body, sql
        assert sql.count("LEFT JOIN customers AS customers") == 1, sql
        assert "OVER" in sql, f"cumsum window must survive isolation:\n{sql}"

    async def test_multiple_aggregate_sources_sharing_join_dedupe(
        self, engine: SlayerQueryEngine
    ) -> None:
        """Two DISTINCT crossing aggregates isolate into two separate
        host-rooted CTEs (per-AggregateKey-slot isolation), each pulling
        its own scope-local join set — so each hop's LEFT JOIN appears
        exactly once PER CTE and never in the host scope. Cross-CTE join
        sharing is optimizer territory (DEV-1688 / ``may_inline``)."""
        model = self._orders_model(extra_columns=[
            Column(
                name="region_pop_sum",
                sql="customers.regions.population",
                type=DataType.DOUBLE,
            ),
            Column(
                name="region_weight_sum",
                sql="customers.regions.weight",
                type=DataType.DOUBLE,
            ),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[
                ModelMeasure(formula="region_pop_sum:sum"),
                ModelMeasure(formula="region_weight_sum:sum"),
            ],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        cm_cte_names = _re.findall(r"(_cm_\w+)\s+AS\s*\(", sql)
        assert len(cm_cte_names) == 2, (
            f"expected one host-rooted CTE per crossing aggregate; "
            f"got {cm_cte_names}:\n{sql}"
        )
        assert sql.count("LEFT JOIN customers AS customers") == 2, sql
        assert sql.count("LEFT JOIN regions AS customers__regions") == 2, sql
        assert "SUM(customers__regions.population)" in sql
        assert "SUM(customers__regions.weight)" in sql

    # ------------------------------------------------------------------
    # Dedup + co-existence
    # ------------------------------------------------------------------

    async def test_dim_and_measure_sharing_join_emits_join_once(
        self, engine: SlayerQueryEngine
    ) -> None:
        """A joined DIMENSION base-pulls its join into the host scope
        (Law 1); the crossing MEASURE isolates with its own scope-local
        join set (Law 3). Each scope emits its joins exactly once: the
        ``customers`` hop appears once in the host and once inside the
        CTE; the deeper ``customers__regions`` hop only inside the CTE.
        """
        model = self._orders_model(extra_columns=[
            Column(
                name="region_pop_sum",
                sql="customers.regions.population",
                type=DataType.DOUBLE,
            ),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="region_pop_sum:sum")],
            dimensions=[ColumnRef(name="customers.region_id")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        assert "_cm_" in sql, f"expected host-rooted isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert cm_body.count("LEFT JOIN customers AS customers") == 1, sql
        assert cm_body.count("LEFT JOIN regions AS customers__regions") == 1, sql
        # Host scope: exactly one customers join (the dimension's), and
        # never the deeper hop.
        host_part = sql.replace(cm_body, "")
        assert host_part.count("LEFT JOIN customers AS customers") == 1, sql
        assert host_part.count("LEFT JOIN regions AS customers__regions") == 0, sql

    async def test_filter_and_source_cross_different_joins(
        self, engine: SlayerQueryEngine
    ) -> None:
        """Filter crosses the shallow join (customers) and source crosses
        the deeper join (customers__regions): both are crossing INPUTS of
        the same aggregate, so ONE host-rooted CTE owns both joins; the
        shared customers hop is emitted exactly once inside it, and the
        host scope stays join-free.
        """
        model = self._orders_model(extra_columns=[
            Column(
                name="region_pop_sum",
                sql="customers.regions.population",
                type=DataType.DOUBLE,
                # Filter crosses only the customers hop — does NOT touch
                # customers__regions. Without DEV-1502 the regions join
                # is missing entirely.
                filter="customers.region_id IS NOT NULL",
            ),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="region_pop_sum:sum")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        assert "_cm_" in sql, f"expected host-rooted isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        # customers join is emitted exactly once (inside the CTE) even
        # though both the filter and the source discovered it.
        assert sql.count("LEFT JOIN customers AS customers") == 1, sql
        assert cm_body.count("LEFT JOIN customers AS customers") == 1, sql
        assert "customers__regions" in cm_body, sql
        # The filter CASE-WHEN references the customers hop. sqlglot
        # may canonicalise ``IS NOT NULL`` to ``NOT ... IS NULL``, so
        # check structurally rather than for an exact text form.
        assert "CASE WHEN" in cm_body, sql
        assert "customers.region_id" in cm_body, sql
        # The aggregate body references the deeper path-aliased ref.
        assert "customers__regions.population" in cm_body, sql

    async def test_no_path_alias_no_extra_join(
        self, engine: SlayerQueryEngine
    ) -> None:
        """Sanity: an aggregate over a local column with no path alias in
        its sql emits zero joins (no spurious LEFT JOINs).
        """
        model = self._orders_model()
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:sum")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        join_aliases = _join_aliases(sql)
        assert join_aliases == set(), f"unexpected joins: {join_aliases}\n{sql}"
        assert "_cm_" not in sql, f"local aggregate must not isolate:\n{sql}"

    # ------------------------------------------------------------------
    # Defensive scope skip + tracking xfails for follow-up tickets
    # ------------------------------------------------------------------

    async def test_cross_model_source_not_in_host_collector_scope(
        self, storage
    ) -> None:
        """Defensive: the new collector skips AGGREGATE slots whose
        ``source.path != ()`` (cross-model). For a cross-model aggregate
        whose target column has a LOCAL sql (no further join) the host
        base FROM must not pull a spurious deeper join.

        Confirms the cross-model path IS exercised (positive: ``_cm_``
        CTE present + the target column ref appears) AND no spurious
        ``regions`` join leaks to the host base. The *missing* deeper-
        join discovery for the cross-model case where the target column
        ITSELF crosses a further join is covered by the strict xfail
        below.
        """
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            ],
            joins=[
                ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ],
        ))
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(
            source_model="orders",
            # cross-model aggregate on a LOCAL customers column (region_id).
            measures=[ModelMeasure(formula="customers.region_id:count_distinct")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        # Positive: cross-model CTE was actually produced; the count
        # aggregate references the joined model's column inside it.
        assert "_cm_" in sql, f"expected cross-model CTE; got:\n{sql}"
        assert "customers.region_id" in sql
        # Negative: no spurious regions join leaks. The realistic leak
        # shape from a misbehaving host-side collector walking through
        # customers would be the path alias ``customers__regions``;
        # ``_join_aliases`` returns the exact alias set so a substring
        # check on bare ``regions`` would miss that. Assert both shapes.
        join_aliases = _join_aliases(sql)
        assert "customers__regions" not in join_aliases
        assert "regions" not in join_aliases

    async def test_cross_model_target_column_sql_crosses_further_join_xfail(
        self, storage
    ) -> None:
        """A cross-model aggregate ``customers_v2.deep_pop:sum`` where
        ``deep_pop`` lives on ``customers_v2`` with sql ``regions.population``
        — the deeper customers_v2→regions join should appear inside the
        ``_cm_*`` CTE body. Currently it doesn't.
        """
        # `deep_pop` on customers_v2 points at the further-joined regions.
        await storage.save_model(SlayerModel(
            name="customers_v2", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
                Column(name="deep_pop", sql="regions.population", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        ))
        await storage.save_model(SlayerModel(
            name="orders_x", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="customers_v2", join_pairs=[["customer_id", "id"]])],
        ))
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(formula="customers_v2.deep_pop:sum")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        # Extract the cross-model CTE body so the assertion ONLY pins
        # the gap inside that scope (avoids accidental promotion from
        # an unrelated host-level LEFT JOIN or formatting coincidence).
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "JOIN regions" in cm_body, (
            f"expected JOIN to regions inside the _cm_* CTE; CTE body:\n{cm_body}"
        )

    # DEV-1706 Stage 2 promoted the DEV-1527 local half (typed kwargs);
    # DEV-1709 Stage 5 widens it to ISOLATION — a crossing derived kwarg
    # now routes the whole aggregate into a host-rooted CTE. Kept the
    # ``_xfail`` name so tests/test_carrier_scope_matrix.py's harvest
    # manifest reference stays valid.
    async def test_agg_param_derived_column_path_alias_xfail(
        self, engine: SlayerQueryEngine
    ) -> None:
        """``weighted_avg(weight=region_weight)`` where ``region_weight``
        is a derived column with a path alias in its sql.
        """
        model = self._orders_model(extra_columns=[
            Column(
                name="region_weight",
                sql="customers.regions.weight",
                type=DataType.DOUBLE,
            ),
        ])
        await engine.storage.save_model(model)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:weighted_avg(weight=region_weight)")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        assert "_cm_" in sql, f"expected host-rooted isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "LEFT JOIN customers" in cm_body, sql
        # The kwarg resolves to the EXPANDED sql inside the CTE, not the
        # bare ident.
        assert "customers__regions.weight" in cm_body, sql
        # And the broken form must not appear.
        assert "orders.region_weight" not in sql


class TestDev1709WidenedIsolationShapes:
    """DEV-1709 (Stage 5) — generator shapes for the widened Law-3 trigger:
    co-occurrence with a local first/last (DEV-1702-B1), crossing explicit
    time args (D2 / DEV-1501 shape update), F4 host-filter inheritance
    into host-rooted CTEs, and the deferred implicit-time pin."""

    @staticmethod
    def _regions_model() -> SlayerModel:
        return SlayerModel(
            name="regions", sql_table="regions", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="population", sql="population", type=DataType.DOUBLE),
                Column(name="weight", sql="weight", type=DataType.DOUBLE),
            ],
        )

    @staticmethod
    def _customers_model() -> SlayerModel:
        return SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
                Column(name="signup_at", sql="signup_at", type=DataType.TIMESTAMP),
            ],
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        )

    @staticmethod
    def _orders_model(
        *, extra_columns=None, default_time_dimension=None, aggregations=None,
    ) -> SlayerModel:
        cols = [
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        ]
        cols.extend(extra_columns or [])
        return SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=cols,
            joins=[ModelJoin(
                target_model="customers", join_pairs=[["customer_id", "id"]],
            )],
            default_time_dimension=default_time_dimension,
            aggregations=aggregations or [],
        )

    async def _sql(self, query: SlayerQuery, orders: SlayerModel) -> str:
        return await _engine_generate(
            query=query, model=orders,
            extra_models=[self._customers_model(), self._regions_model()],
        )

    async def test_crossing_sum_coexists_with_local_last(self) -> None:
        """DEV-1702-B1: a regular crossing aggregate next to a local
        first/last isolates — it never renders in the ranked outer scope."""
        orders = self._orders_model(extra_columns=[
            Column(name="region_pop_sum", sql="customers.regions.population",
                   type=DataType.DOUBLE),
        ])
        query = SlayerQuery(
            source_model="orders",
            measures=[
                ModelMeasure(formula="region_pop_sum:sum"),
                ModelMeasure(formula="amount:last(orders.created_at)"),
            ],
        )
        sql = await self._sql(query, orders)
        assert "_cm_" in sql, f"expected a crossing-input isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "SUM(customers__regions.population)" in cm_body, sql
        # Each aggregate owns a scope: the crossing SUM its own ``_cm_``, the
        # first/last its own ``_cm_`` (DEV-1835 unified naming). Neither ref
        # appears in the other's, and ``_base`` — which now holds nothing at
        # all — pulls no join.
        rk_body = _extract_cte_body(sql, r"_cm_\w*amount_last\w*")
        assert "customers__regions.population" not in rk_body, sql
        assert "THEN _val" in _norm(rk_body), sql
        assert "ROW_NUMBER" not in cm_body, sql
        rest = sql.replace(cm_body, "").replace(rk_body, "")
        assert "LEFT JOIN" not in rest, (
            f"the host base must stay join-free:\n{rest}"
        )

    async def test_crossing_kwarg_coexists_with_local_last(self) -> None:
        """A crossing KWARG next to a local first/last isolates the
        parametric aggregate the same way (Codex plan-review F4 fold-in:
        kwarg expressions must never leak into the ranked outer scope)."""
        orders = self._orders_model(extra_columns=[
            Column(name="region_weight", sql="customers.regions.weight",
                   type=DataType.DOUBLE),
        ])
        query = SlayerQuery(
            source_model="orders",
            measures=[
                ModelMeasure(formula="amount:weighted_avg(weight=region_weight)"),
                ModelMeasure(formula="amount:last(orders.created_at)"),
            ],
        )
        sql = await self._sql(query, orders)
        assert "_cm_" in sql, f"expected host-rooted isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "customers__regions.weight" in cm_body, sql
        host_part = sql.replace(cm_body, "")
        assert "customers__regions.weight" not in host_part, sql
        assert "LEFT JOIN" not in host_part, (
            f"host ranked scope must stay join-free:\n{host_part}"
        )

    async def test_local_source_crossing_time_arg_isolates(self) -> None:
        """D2: ``amount:last(customers.signup_at)`` — a local source with a
        crossing explicit time arg isolates; the arg's join and ORDER BY
        ref live inside the CTE's ranked subquery (DEV-1501 shape update)."""
        orders = self._orders_model()
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:last(customers.signup_at)")],
        )
        sql = await self._sql(query, orders)
        assert "_cm_" in sql, f"expected an isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "customers.signup_at" in cm_body, sql
        assert "LEFT JOIN customers" in cm_body, sql
        host_part = sql.replace(cm_body, "")
        assert "customers.signup_at" not in host_part, sql
        assert "LEFT JOIN" not in host_part, sql

    async def test_derived_crossing_time_arg_isolates(self) -> None:
        orders = self._orders_model(extra_columns=[
            Column(name="cross_time", sql="customers.signup_at",
                   type=DataType.TIMESTAMP),
        ])
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:last(cross_time)")],
        )
        sql = await self._sql(query, orders)
        assert "_cm_" in sql, f"expected an isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "customers.signup_at" in cm_body, sql
        assert "LEFT JOIN customers" in cm_body, sql

    async def test_user_template_fragment_kwarg_renders_join_in_cte(self) -> None:
        """A crossing USER template-fragment kwarg
        (``scale='customers__regions.weight'``) isolates AND the CTE
        sub-render registers the fragment's joins — the fragment renders
        qualified, so the LEFT JOINs must live inside the CTE body
        (PR #271 Codex review: fragments triggered isolation but their
        joins were never registered in the sub-render)."""
        orders = self._orders_model(aggregations=[
            Aggregation(
                name="scaled_sum", formula="SUM({value}) / {scale}",
                params=[AggregationParam(name="scale", sql="1")],
            ),
        ])
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(
                formula="amount:scaled_sum(scale='customers.regions.weight')",
            )],
        )
        sql = await self._sql(query, orders)
        assert "_cm_" in sql, f"expected host-rooted isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "LEFT JOIN customers AS customers" in cm_body, sql
        assert "LEFT JOIN regions AS customers__regions" in cm_body, sql
        assert "customers__regions.weight" in cm_body, sql
        host_part = sql.replace(cm_body, "")
        assert "LEFT JOIN" not in host_part, sql

    async def test_model_default_fragment_kwarg_renders_join_in_cte(self) -> None:
        """Same for a crossing MODEL-DEFAULT ``AggregationParam.sql``
        fragment — no user kwarg at all."""
        orders = self._orders_model(aggregations=[
            Aggregation(
                name="wscaled_sum", formula="SUM({value} * {w})",
                params=[AggregationParam(
                    name="w", sql="customers.regions.weight",
                )],
            ),
        ])
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:wscaled_sum")],
        )
        sql = await self._sql(query, orders)
        assert "_cm_" in sql, f"expected host-rooted isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "LEFT JOIN customers AS customers" in cm_body, sql
        assert "LEFT JOIN regions AS customers__regions" in cm_body, sql
        assert "customers__regions.weight" in cm_body, sql
        host_part = sql.replace(cm_body, "")
        assert "LEFT JOIN" not in host_part, sql

    async def test_pathed_host_row_filter_inherited_into_cte(self) -> None:
        """F4 (Codex plan-review F5): a host ROW filter whose expression
        crosses a join constrains the host-rooted scope — it renders in
        BOTH the host base WHERE and the isolation CTE's scope, each with
        its own join registration."""
        orders = self._orders_model(extra_columns=[
            Column(name="region_pop_sum", sql="customers.regions.population",
                   type=DataType.DOUBLE),
        ])
        query = SlayerQuery(
            source_model="orders",
            measures=[
                ModelMeasure(formula="region_pop_sum:sum"),
                ModelMeasure(formula="amount:sum"),
            ],
            filters=["customers.region_id > 0"],
        )
        sql = await self._sql(query, orders)
        assert "_cm_" in sql, f"expected host-rooted isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "customers.region_id > 0" in cm_body, (
            f"host ROW filter must be inherited into the host-rooted CTE:\n{sql}"
        )
        host_part = sql.replace(cm_body, "")
        assert "customers.region_id > 0" in host_part, (
            f"host ROW filter must still constrain the host base:\n{sql}"
        )

    async def test_order_only_crossing_aggregate_isolates(self) -> None:
        """F3, hidden ORDER-only consumer: a crossing aggregate referenced
        ONLY by ORDER BY (colon-form ``OrderItem``) still isolates as a
        hidden slot; the ORDER BY reads the joined-back CTE column and the
        crossing ref never leaves the CTE.

        (A composite ORDER-only consumer is not constructible from the
        query surface — ``OrderItem.column`` accepts single colon-form
        refs only; composite lowering for filter-only and projected
        consumers is pinned by the sibling tests.)"""
        orders = self._orders_model(extra_columns=[
            Column(name="region_pop_sum", sql="customers.regions.population",
                   type=DataType.DOUBLE),
        ])
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:sum")],
            dimensions=[ColumnRef(name="customer_id")],
            order=[OrderItem(column="region_pop_sum:sum", direction="desc")],
        )
        sql = await self._sql(query, orders)
        assert "_cm_" in sql, f"expected host-rooted isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "customers__regions.population" in cm_body, sql
        host_part = sql.replace(cm_body, "")
        assert "customers__regions.population" not in host_part, sql
        assert "ORDER BY" in sql, sql

    async def test_filter_only_composite_with_crossing_leaf(self) -> None:
        """F3, filter-only consumer: a composite referenced only by an
        aggregate-phase filter lowers the same way — crossing leaf
        isolates, the comparison lands in the combined outer WHERE (never
        HAVING), local operand promoted into ``_base``."""
        orders = self._orders_model(extra_columns=[
            Column(name="region_pop_sum", sql="customers.regions.population",
                   type=DataType.DOUBLE),
        ])
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:sum")],
            filters=["region_pop_sum:sum + amount:sum > 100"],
        )
        sql = await self._sql(query, orders)
        assert "_cm_" in sql, f"expected host-rooted isolation CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "customers__regions.population" in cm_body, sql
        host_part = sql.replace(cm_body, "")
        assert "customers__regions.population" not in host_part, sql
        assert "HAVING" not in _norm(sql), sql
        outer = sql[sql.rfind("\n)") + 2:]
        assert "> 100" in outer, (
            f"composite aggregate filter must land in the combined outer "
            f"WHERE:\n{sql}"
        )

    async def test_ranked_isolation_survives_the_recursion_guard(self) -> None:
        """With isolation DISABLED — exactly how the recursive sub-plan is
        built — two same-sql, different-CAST-type crossing first/lasts still get
        a ranked scope EACH, and each materialises its own type-distinct value.

        This asserted they shared ONE ranked subquery, because the guard
        suppressed isolation and the host base carried every ranking. The guard
        exists to stop a CROSSING aggregate re-isolating forever inside its own
        sub-plan; a ranked aggregate is rendered, never re-planned, and inside a
        sub-plan it still needs its own row ordering — so it is deliberately not
        suppressed. The type-distinctness claim is unchanged and is what the
        two CASTs below still pin."""
        orders = self._orders_model(extra_columns=[
            Column(name="region_pay_d", sql="customers.regions.population * 2",
                   type=DataType.DOUBLE),
            Column(name="region_pay_i", sql="customers.regions.population * 2",
                   type=DataType.INT),
        ])
        bundle = ResolvedSourceBundle(
            source_model=orders,
            referenced_models=[self._customers_model(), self._regions_model()],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[
                ModelMeasure(formula="region_pay_d:last(orders.created_at)"),
                ModelMeasure(formula="region_pay_i:last(orders.created_at)"),
            ],
        )
        planned = plan_query(
            query=query, bundle=bundle, disable_host_rooted_isolation=True,
        )
        gen = SQLGenerator(dialect="postgres")
        sql = gen.generate_from_planned(planned, bundle=bundle)
        norm = _norm(sql)
        assert len(_re.findall(r"_rk_\w+\s+AS\s*\(", sql)) == 2, sql
        assert norm.count(" AS _val") == 2, (
            f"same SQL, different type must materialise two distinct _vals:"
            f"\n{sql}"
        )
        assert _re.search(r"AS DOUBLE PRECISION\) AS _val_\d+", norm), sql
        assert _re.search(r"AS INT\) AS _val_\d+", norm), sql
        for val_alias in _re.findall(r"AS (_val_\d+)", norm):
            assert f"THEN {val_alias}" in norm, sql

    async def test_a_ranked_sibling_no_longer_drags_a_kwarg_into_its_scope(
        self,
    ) -> None:
        """A crossing KWARG next to a first/last, with isolation disabled.

        This asserted the kwarg was materialised as a ``_val`` inside the RANKED
        subquery — because one first/last used to wrap the whole host base, so
        every sibling was computed over the ranked row set and any crossing ref
        it carried had to cross that scope boundary. The ranking has its own
        scope now, so the guard means exactly what it says: the crossing kwarg
        renders INLINE in ``_base``, which is legal there, and the ranked CTE
        neither sees it nor is affected by it."""
        orders = self._orders_model(extra_columns=[
            Column(name="region_weight", sql="customers.regions.weight",
                   type=DataType.DOUBLE),
        ])
        bundle = ResolvedSourceBundle(
            source_model=orders,
            referenced_models=[self._customers_model(), self._regions_model()],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[
                ModelMeasure(formula="amount:weighted_avg(weight=region_weight)"),
                ModelMeasure(formula="amount:last(orders.created_at)"),
            ],
        )
        planned = plan_query(
            query=query, bundle=bundle, disable_host_rooted_isolation=True,
        )
        gen = SQLGenerator(dialect="postgres")
        sql = gen.generate_from_planned(planned, bundle=bundle)
        base_body = _extract_cte_body(sql, r"_base")
        assert "customers__regions.weight" in base_body, sql
        assert "LEFT JOIN regions AS customers__regions" in base_body, sql
        rk_body = _extract_cte_body(sql, r"_rk_\w+")
        assert "customers__regions.weight" not in rk_body, sql
        assert "ROW_NUMBER" in rk_body, sql

    # DEV-1835 lift: local first/last now desugar to the unified ``_cm_``
    # regroup producer, so they ISOLATE universally — an implicitly-resolved
    # crossing default-time first/last is no longer the deferred DEV-1729 shape
    # (it was strict-xfail here); the isolation the test asserts now holds.
    async def test_implicit_crossing_default_time_isolates_xfail(self) -> None:
        orders = self._orders_model(
            extra_columns=[
                Column(name="cross_time", sql="customers.signup_at",
                       type=DataType.TIMESTAMP),
            ],
            default_time_dimension="cross_time",
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:last")],
        )
        sql = await self._sql(query, orders)
        assert "_cm_" in sql, f"expected host-rooted isolation CTE:\n{sql}"


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
        # ``status`` is not a declared column, so the Mode-A door qualifies it
        # against the scope root like any other bare ref (DEV-1745 W1).
        assert "CASE WHEN sales.status = 'active' THEN 100" not in sql
        assert "/ 100" in sql
        assert "CASE WHEN sales.status = 'active' THEN" in sql

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
        # Both legs are row-level references → both wrapped. (``status`` is
        # undeclared, so the door qualifies it to the root — DEV-1745 W1.)
        assert sql.count("CASE WHEN sales.status = 'active'") >= 2

    def test_injection_via_direct_agg_render_spec(self, gen: SQLGenerator) -> None:
        """Malicious agg_kwargs on a directly constructed AggRenderSpec are rejected
        at render time (the validation is wired into the dialect-helper path, not
        just the standalone ``_validate_agg_param_value``)."""
        spec = AggRenderSpec(
            sql="price",
            name="price",
            model_name="sales",
            aggregation="weighted_avg",
            alias="sales.price_weighted_avg",
            agg_kwargs={"weight": "quantity); DROP TABLE orders; --"},
        )
        with pytest.raises(ValueError, match="Unsafe value"):
            gen._build_agg(spec)


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
        """A filtered ``last`` REMOVES the non-matching rows before ranking.

        This asserted a dedicated ``_last_rn_f0`` column ranking every row with
        ``CASE WHEN <filter> THEN 0 ELSE 1 END`` first, so the non-matching ones
        sorted below the winners, plus a ``_match_f0`` flag the outer aggregate
        consulted by alias. That machinery existed because the ranking was
        shared with every other aggregate in the query and could not drop rows.
        In its own scope it simply does."""
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
        norm = _norm(sql)
        assert "WHERE orders.status = 'completed'" in norm, sql
        assert "THEN 0 ELSE 1" not in norm, sql
        assert "_match_f0" not in norm, sql
        assert _re.search(r"_(?:first|last)_rn", sql) is None, sql

    async def test_filtered_first_generates_dedicated_rn(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """The same for ``first``: the filter is a WHERE, and the ranking runs
        ascending over what survives it."""
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
        norm = _norm(sql)
        assert "WHERE orders.status = 'completed'" in norm, sql
        assert "ORDER BY orders.created_at)" in norm, sql
        assert "THEN 0 ELSE 1" not in norm, sql
        assert _re.search(r"_(?:first|last)_rn", sql) is None, sql

    async def test_unfiltered_last_unchanged(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """An unfiltered ``last`` ranks over every row — no WHERE of its own."""
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
        rk_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "ROW_NUMBER" in rk_body, sql
        assert "WHERE" not in rk_body, sql

    async def test_mixed_filtered_and_unfiltered_last(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """A filtered and an unfiltered ``last`` are two aggregates, so two
        CTEs — the filtered one carrying its predicate as a WHERE, the other
        ranking over the full row set."""
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
        names = _re.findall(r"(_cm_\w+)\s+AS\s*\(", sql)
        assert len(names) == 2, sql
        bodies = [_extract_cte_body(sql, _re.escape(n)) for n in names]
        wheres = [b for b in bodies if "WHERE" in b]
        assert len(wheres) == 1, sql
        assert "orders.status = 'completed'" in _norm(wheres[0]), sql

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

        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
            measures=[ModelMeasure(formula="active_balance:last")],
        )
        return await _engine_generate(
            query=query, model=orders, dialect=generator.dialect,
            extra_models=[customers], validate=False,
        )

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
        filter references a JOINED table (e.g. customers.status), the measure is
        isolated into a CTE of its own. The join lives inside the ranked
        subquery so the filter resolves, and the final SELECT does not reference
        the joined table directly."""

        sql = await self._filtered_last_cross_model_sql(generator)

        # The outermost SELECT (after all CTEs) should not reference 'customers.'
        # directly — it pulls pre-computed values from CTEs.
        final_select_idx = sql.rfind("\nSELECT ")
        if final_select_idx == -1:
            final_select_idx = sql.rfind("SELECT ")
        final_select = sql[final_select_idx:]

        assert "customers." not in final_select, (
            f"Final SELECT references joined table 'customers' which is "
            f"not in scope — should use CTE column references. "
            f"Final SELECT:\n{final_select}\n\nFull SQL:\n{sql}"
        )
        # The isolated CTE contains the ranking.
        assert "_cm_" in sql, f"Expected an isolated ranked CTE:\n{sql}"
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "ROW_NUMBER" in cm_body, cm_body
        # ...and the customers JOIN, so the filter resolves where it is applied.
        assert "public.customers" in cm_body, (
            f"Expected customers JOIN inside the ranked CTE:\n{cm_body}"
        )

    async def test_filter_with_dotted_string_literal_does_not_pull_spurious_join(
        self, generator: SQLGenerator,
    ) -> None:
        """Regression for CodeRabbit #6 — when a measure filter contains a string
        literal that happens to include a dot (e.g. "url LIKE 'foo.bar%'"), the
        join planner must NOT mistake the literal for a `foo.<col>` ref
        and pull in an unwanted LEFT JOIN. The structured Mode-A predicate parse
        only lists real column references."""

        # Inline ModelJoin to a 'foo' model that is never registered. A regex
        # over the filter string would match `foo.bar` inside the string
        # literal and add 'foo' to needed_tables; structured parsing must not.
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
        # The 'foo' join must NOT be pulled in by the dotted string literal.
        sql = await _generate(generator, query, orders)
        assert "foo" not in _join_aliases(sql)
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
        norm = _norm(sql)
        # Two filtered aggregates, two scopes, two predicates. The suffixed
        # sentinel columns this test was written for (``_last_rn_f0`` /
        # ``_last_rn_f1``) existed to keep two filtered rankings apart inside
        # ONE scope; there is one ranking per scope now.
        names = _re.findall(r"(_cm_\w+)\s+AS\s*\(", sql)
        assert len(names) == 2, sql
        assert _re.search(r"_(?:first|last)_rn", sql) is None, sql
        assert "WHERE orders.status = 'active'" in norm, sql
        assert "WHERE orders.status = 'completed'" in norm, sql

    # DEV-1484: the two source-alias tests that previously lived here
    # (test_filtered_measure_uses_source_alias_not_model_name and
    # test_filtered_measure_source_alias_propagates_to_generated_sql) relied on
    # a model whose ``name`` differs from ``query.source_model``. The typed
    # ``engine.execute`` resolves the source by name from storage, so the
    # relation alias always equals ``model.name`` and the divergence cannot be
    # reproduced end-to-end. Their intent — filter columns and the spec's
    # model_name qualify with the source RELATION, never the underlying
    # model.name — is preserved at the typed unit in
    # tests/test_agg_render_spec.py::TestBuilderColumnKey::
    # test_filter_qualifies_with_source_relation_not_model_name.


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


# DEV-1484: TestAutoMoveDimensions (tested the legacy
# engine._auto_move_fields_to_dimensions heuristic, which dies with the legacy
# enrichment subgraph in Stage D) was deleted. Its coverage moved to the typed
# slack-normalization layer in tests/test_slack_normalization.py: the bare-
# column / colon / arithmetic / dotted-ref / no-op / append cases live in
# TestMisplacedMeasure (`# DEV-1484 backfill` markers), and the cross-model
# dimension-in-measures end-to-end case lives in
# TestEngineWiring::test_cross_model_dimension_in_measures_groups_correctly.


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

    @pytest.fixture
    def customers(self):
        return SlayerModel(
            name="customers",
            sql_table="public.customers",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
            ],
        )

    async def test_sql_table_baseline(self, generator: SQLGenerator, table_orders, customers) -> None:
        """Sanity check: sql_table models emit LEFT JOIN correctly."""
        query = SlayerQuery(
            source_model="orders_table",
            measures=["amount:sum"],
            dimensions=["customers.name"],
        )
        sql = await _generate(generator, query, table_orders, extra_models=[customers])
        assert "LEFT JOIN" in sql
        assert "customers" in sql

    async def test_inline_sql_cross_model_dimension(self, generator: SQLGenerator, inline_orders, customers) -> None:
        """Mirrors benchmark Q2/Q5: inline-SQL source with a cross-model dimension."""
        query = SlayerQuery(
            source_model="orders_inline",
            measures=["amount:sum"],
            dimensions=["customers.name"],
        )
        sql = await _generate(generator, query, inline_orders, extra_models=[customers])
        assert "LEFT JOIN" in sql, f"LEFT JOIN missing from inline-SQL model query:\n{sql}"
        assert "customers" in sql

    async def test_inline_sql_cross_model_dim_plus_local_measure(self, generator: SQLGenerator, inline_orders, customers) -> None:
        """Mirrors benchmark Q1: inline-SQL source with both cross-model dim and local measure."""
        query = SlayerQuery(
            source_model="orders_inline",
            measures=["amount:avg"],
            dimensions=["customers.name"],
        )
        sql = await _generate(generator, query, inline_orders, extra_models=[customers])
        assert "LEFT JOIN" in sql, f"LEFT JOIN missing:\n{sql}"
        assert "AVG(" in sql.upper()


# Join target for TestSelfReferencingPaths, shared by the fixture (which saves
# it into the fixture's own store) and ``_engine_generate`` (which builds a
# fresh store per call and needs it passed as an extra model).
_CUSTOMERS_SELF_REF = SlayerModel(
    name="customers", sql_table="customers", data_source="test",
    columns=[
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name="name", sql="name", type=DataType.TEXT),
        Column(name="score", sql="score", type=DataType.DOUBLE),
    ],
)


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
        """'orders.customers.name' is pre-stripped to 'customers.name', then resolves correctly.

        DEV-1485 Stage D: was two assertions against the legacy
        ``engine._resolve_dimension_via_joins``. Now end-to-end through the
        typed pipeline, which is strictly better coverage — ``execute`` applies
        ``strip_source_model_prefix`` itself, so this exercises the real
        user-facing path rather than an internal resolver.
        """
        _engine, model = engine_and_models
        query = SlayerQuery(source_model="orders", dimensions=["orders.customers.name"])
        # The strip itself stays pinned directly — it is pure and deterministic.
        stripped = query.strip_source_model_prefix()
        assert (stripped.dimensions[0].model, stripped.dimensions[0].name) == (
            "customers", "name",
        )
        sql = await _engine_generate(
            query=query, model=model, extra_models=[_CUSTOMERS_SELF_REF],
        )
        assert "LEFT JOIN" in sql, sql
        assert "customers" in sql, sql
        # Resolved as a JOINED dimension under the dotted result key, not as a
        # bare local column that silently referenced the wrong table.
        assert "orders.customers.name" in sql, sql

    async def test_self_ref_measure_resolved_after_strip(self, engine_and_models) -> None:
        """'orders.customers.score:sum' is pre-stripped to 'customers.score:sum', then resolves.

        DEV-1485 Stage D: was an assertion against the legacy
        ``engine._resolve_cross_model_measure``; now pins the emitted
        cross-model aggregate end-to-end.
        """
        _engine, model = engine_and_models
        query = SlayerQuery(
            source_model="orders", measures=["orders.customers.score:sum"],
        )
        stripped = query.strip_source_model_prefix()
        assert stripped.measures[0].formula == "customers.score:sum"
        sql = await _engine_generate(
            query=query, model=model, extra_models=[_CUSTOMERS_SELF_REF],
        )
        assert "SUM(" in sql.upper(), sql
        assert "customers" in sql, sql

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
        # The constant should appear unqualified in WHERE. DEV-1361 wraps a
        # non-bare ``Column.sql`` (literal ``1``) in CAST when ``type`` is set.
        assert (
            "1 = '1'" in sql
            or "1 = 1" in sql
            or "CAST(1 AS DOUBLE PRECISION) = '1'" in sql
        )

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

        query = SlayerQuery(
            source_model="policy_amount",
            measures=[ModelMeasure(formula="total:sum")],
            filters=["premium.has_premium = '1'"],
        )
        sql = await _generate(generator, query, policy_amount, extra_models=[premium_model])
        assert "premium.1" not in sql, f"Constant SQL '1' was table-qualified: {sql}"
        # The constant should appear unqualified in WHERE. DEV-1361 wraps a
        # non-bare ``Column.sql`` (literal ``1``) in CAST when ``type`` is set.
        assert (
            "1 = '1'" in sql
            or "1 = 1" in sql
            or "CAST(1 AS DOUBLE PRECISION) = '1'" in sql
        )

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

        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="revenue:sum")],
            filters=["customers.status = 'active'"],
        )
        sql = await _generate(generator, query, orders, extra_models=[customers])
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
            await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
            await storage.save_model(source)
            await storage.save_model(target)
            engine = SlayerQueryEngine(storage=storage)

            query = SlayerQuery(
                source_model="amounts",
                measures=[
                    ModelMeasure(formula="total:sum / policies.id:count_distinct", name="avg_per_policy"),
                ],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
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
            await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
            await storage.save_model(orders)
            await storage.save_model(customers)
            engine = SlayerQueryEngine(storage=storage)

            query = SlayerQuery(
                source_model="orders",
                measures=["rolling_avg(customers.score)"],
                dimensions=[ColumnRef(name="status")],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
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
            await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
            for m in (a, b, c, d, e):
                await storage.save_model(m)
            engine = SlayerQueryEngine(storage=storage)

            # rolling_avg lives 4 hops away (a → b → c → d → e). The
            # function-style rewrite must still recognise it.
            query = SlayerQuery(
                source_model="a",
                measures=["rolling_avg(b.c.d.e.score)"],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
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
            await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
            await storage.save_model(a)
            await storage.save_model(b)
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery(source_model="a", measures=["rolling_a(amount)"])
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            _assert_valid_sql(sql, dialect=generator.dialect)


class TestTransformAmbiguousTimeDimension:
    """Time-dependent transforms must reject ambiguous time_dimension setups.

    Regression: ``_add_transform`` only checked ``not time_dimensions`` (empty).
    With 2+ time_dimensions and no main_time_dimension/default_time_dimension,
    ``_resolve_time_alias`` returns None but the transform was built anyway.
    """

    async def test_two_time_dims_no_disambiguation_raises(self, generator: SQLGenerator) -> None:
        m = _orders_two_ts_model()
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=["cumsum(revenue:sum)"],
                time_dimensions=[
                    TimeDimension(dimension="created_at", granularity=TimeGranularity.MONTH),
                    TimeDimension(dimension="updated_at", granularity=TimeGranularity.MONTH),
                ],
            )
            with pytest.raises(ValueError, match="time"):
                await engine.execute(query, dry_run=True)

    async def test_two_time_dims_with_main_succeeds(self, generator: SQLGenerator) -> None:
        """Disambiguation via main_time_dimension keeps the transform working."""
        m = _orders_two_ts_model()
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=["cumsum(revenue:sum)"],
                time_dimensions=[
                    TimeDimension(dimension="created_at", granularity=TimeGranularity.MONTH),
                    TimeDimension(dimension="updated_at", granularity=TimeGranularity.MONTH),
                ],
                main_time_dimension="created_at",
            )
            sql = (await engine.execute(query, dry_run=True)).sql
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
        """DEV-1501: two ORDER BY entries `revenue:last(created_at)` and
        `revenue:last(updated_at)` must produce DISTINCT ranked aggregates and
        not collapse onto one. The hidden materialised aggregates must be
        absent from the public projection (the result keys stay
        ``orders.status`` + ``orders._count``).

        The collapse it pinned was two specs sharing a bare ``_last_rn``; each
        aggregate holds its own ranking now, so the same question is asked of
        the two CTEs.
        """

        m = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="updated_at", sql="updated_at", type=DataType.TIMESTAMP),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=["*:count"],
                dimensions=[ColumnRef(name="status")],
                order=[
                    OrderItem(column="revenue:last(created_at)", direction="desc"),
                    OrderItem(column="revenue:last(updated_at)", direction="asc"),
                ],
            )
            res = await engine.execute(query, dry_run=True)
            sql = res.sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            # Two DISTINCT rankings, one per effective time column.
            assert len(_re.findall(r"_cm_\w+\s+AS\s*\(", sql)) == 2, sql
            assert (
                _re.search(r"_(?:first|last)_rn", sql) is None
            ), sql
            assert (
                _re.search(r"ORDER BY\s+orders\.created_at\s+DESC", sql)
                is not None
            ), f"created_at rank ORDER BY missing:\n{sql}"
            assert (
                _re.search(r"ORDER BY\s+orders\.updated_at\s+DESC", sql)
                is not None
            ), f"updated_at rank ORDER BY missing:\n{sql}"
            # The two outer ORDER BY EXPRESSIONS must NOT be byte-identical
            # (the bug renders them identically; the fix gives each its own
            # distinct ``_last_rn{suffix}``). Directions are independently
            # asserted to remain DESC then ASC.
            order_terms = _outer_order_terms(sql)
            assert len(order_terms) == 2, f"Expected 2 ORDER BY terms in outer:\n{sql}"
            exprs = [t[0] for t in order_terms]
            dirs = [t[1] for t in order_terms]
            assert exprs[0] != exprs[1], (
                f"Two ORDER BY expressions collapsed to identical SQL:\n{sql}"
            )
            assert dirs == ["desc", "asc"], (
                f"ORDER BY directions wrong (expected [desc, asc]): {dirs}\n{sql}"
            )
            # Hidden materialised aggregates must be TRIMMED from the public
            # projection — the result keys are dim + count only.
            assert set(res.columns) == {"orders.status", "orders._count"}, (
                f"Hidden first/last aliases leaked into result columns: "
                f"{res.columns!r}\nSQL:\n{sql}"
            )
            # Response-meta invariant: the hidden materialised aliases must not
            # appear under attributes.dimensions or attributes.measures either.
            attr_keys = (
                set(res.attributes.dimensions.keys())
                | set(res.attributes.measures.keys())
            )
            assert not any(
                "revenue_last" in k for k in attr_keys
            ), f"Hidden materialised alias surfaced in response attributes: {attr_keys!r}"

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
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=[
                    ModelMeasure(formula="revenue:percentile(p=0.5)", name="p50"),
                    ModelMeasure(formula="revenue:percentile(p=0.95)", name="p95"),
                ],
                dimensions=[ColumnRef(name="status")],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            # Two distinct percentile parameterizations must not collapse via
            # canonical-name dedup: both p values and both user aliases surface
            # distinctly in the emitted SQL.
            assert "0.5" in sql and "0.95" in sql, (
                f"Expected both percentile p values in SQL:\n{sql}"
            )
            assert "p50" in sql and "p95" in sql, (
                f"Expected both user aliases (p50, p95) in SQL:\n{sql}"
            )

    async def test_unparameterized_alias_unchanged(self, generator: SQLGenerator) -> None:
        """Backwards-compat: revenue:sum still produces orders.revenue_sum (no suffix)."""
        m = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(source_model="orders", measures=["revenue:sum"])
            sql = (await engine.execute(query, dry_run=True)).sql
            assert '"orders.revenue_sum"' in sql

    async def test_star_count_alias_unchanged(self, generator: SQLGenerator) -> None:
        """Backwards-compat: *:count still produces orders._count."""
        m = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        )
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(source_model="orders", measures=["*:count"])
            sql = (await engine.execute(query, dry_run=True)).sql
            assert '"orders._count"' in sql


@asynccontextmanager
async def _persist_and_engine(
    *models: SlayerModel,
    ds_name: str = "test",
    ds_type: str = "postgres",
):
    """Yield a ``SlayerQueryEngine`` with the given models persisted to a
    throwaway ``YAMLStorage``. Replaces the per-test ``tempfile +
    YAMLStorage + save_datasource + save_model + SlayerQueryEngine``
    boilerplate that DEV-1501 repeated 16+ times — same shape, but one
    line at the call site. ``ds_name`` / ``ds_type`` follow the
    repo-wide convention (``data_source="test"`` everywhere).
    """
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=tmp)
        await storage.save_datasource(
            DatasourceConfig(name=ds_name, type=ds_type),
        )
        for m in models:
            await storage.save_model(m)
        yield SlayerQueryEngine(storage=storage)


def _orders_with_paid_amount_model() -> SlayerModel:
    """DEV-1501 fixture-style helper — an ``orders`` model with a FILTERED
    ``paid_amount`` column (only ``status='paid'`` rows participate). Used
    by the filtered-first/last test suite (3+ tests share this exact model).
    """
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(
                name="paid_amount", sql="amount",
                filter="status = 'paid'", type=DataType.DOUBLE,
            ),
        ],
    )


def _orders_two_ts_model(*, default_td: "str | None" = None) -> SlayerModel:
    """DEV-1501 fixture-style helper — an ``orders`` model with TWO timestamp
    columns so first/last with different explicit time args can be exercised.
    ``default_td`` optionally sets ``default_time_dimension`` to test the
    "no explicit arg, fall back to default" path.
    """
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        default_time_dimension=default_td,
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="updated_at", sql="updated_at", type=DataType.TIMESTAMP),
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
        ],
    )


class TestDev1501HiddenFirstLastRender:
    """DEV-1501 — first/last aggregates reached only through ORDER BY, HAVING
    or a composite must still be materialised, must not collapse onto one
    another, and must not surface in the public projection.

    Every one of those claims survives DEV-1748; the MECHANISM they were
    written against does not. There is no rn-suffix bookkeeping to thread and
    no outer wrap to trim, because each aggregate owns a ``_cm_`` CTE: two
    aggregates cannot collapse onto one rank column when they do not share a
    rank column, and a hidden one is simply never projected from its CTE. The
    assertions below are the same questions asked of the new shape.
    """

    @staticmethod
    def _ranked_ctes(sql: str) -> "list[str]":
        # DEV-1835: local first/last desugar to the unified ``_cm_`` producer
        # CTE; a cross-model first/last still isolates via the legacy ``_rk_``
        # CTE. Match either prefix (every caller's query is ranked-only, so a
        # matched CTE is always a ranking).
        return _re.findall(r"(_(?:rk|cm)_\w+)\s+AS\s*\(", sql)

    async def test_order_by_first_and_last_different_time_cols(
        self, generator: SQLGenerator
    ) -> None:
        """ASC ``first(created_at)`` + DESC ``last(updated_at)`` produce
        distinct rank columns and distinct outer ORDER BY terms.
        """
        m = _orders_two_ts_model()
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=["*:count"],
                dimensions=[ColumnRef(name="status")],
                order=[
                    OrderItem(column="revenue:first(created_at)", direction="asc"),
                    OrderItem(column="revenue:last(updated_at)", direction="desc"),
                ],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            assert len(self._ranked_ctes(sql)) == 2, sql
            assert "orders.created_at" in sql
            assert "orders.updated_at" in sql
            terms = _outer_order_terms(sql)
            exprs = [t[0] for t in terms]
            dirs = [t[1] for t in terms]
            assert len(terms) == 2 and exprs[0] != exprs[1], (
                f"Two ORDER BY expressions collapsed:\n{sql}"
            )
            assert dirs == ["asc", "desc"], (
                f"first→ASC / last→DESC directions lost: {dirs}\n{sql}"
            )
            # The first(created_at) rank window orders created_at ascending
            # (the bare column); the last(updated_at) one orders descending.
            norm = _norm(sql)
            assert "ORDER BY orders.created_at)" in norm, sql
            assert "ORDER BY orders.updated_at DESC)" in norm, sql

    async def test_order_by_first_and_last_same_time_col(
        self, generator: SQLGenerator
    ) -> None:
        """``first(created_at)`` ASC + ``last(created_at)`` DESC rank over the
        same column in opposite directions, so they are two aggregates and get
        two CTEs. The suffix bucketing this test was named for was the old
        scheme for keeping them apart inside one shared scope.
        """
        m = _orders_two_ts_model()
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=["*:count"],
                dimensions=[ColumnRef(name="status")],
                order=[
                    OrderItem(column="revenue:first(created_at)", direction="asc"),
                    OrderItem(column="revenue:last(created_at)", direction="desc"),
                ],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            assert len(self._ranked_ctes(sql)) == 2, sql
            assert _re.search(r"_(?:first|last)_rn", sql) is None, sql
            norm = _norm(sql)
            assert "ORDER BY orders.created_at)" in norm, sql
            assert "ORDER BY orders.created_at DESC)" in norm, sql
            terms = _outer_order_terms(sql)
            exprs = [t[0] for t in terms]
            assert len(terms) == 2 and exprs[0] != exprs[1], (
                f"first/last ORDER BY expressions collapsed:\n{sql}"
            )

    async def test_order_by_last_with_explicit_time_no_default_time_dim(
        self, generator: SQLGenerator
    ) -> None:
        """Model has no ``default_time_dimension`` and the query has no
        temporal dimension; one ``ORDER BY revenue:last(created_at) DESC``
        must still build the ranked subquery ordered by ``created_at``.
        Regression for the order-by-only + no-default case.
        """
        m = _orders_two_ts_model()  # no default_td
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=["*:count"],
                dimensions=[ColumnRef(name="status")],
                order=[
                    OrderItem(column="revenue:last(created_at)", direction="desc"),
                ],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            assert len(self._ranked_ctes(sql)) == 1, sql
            assert (
                _re.search(r"ORDER BY\s+orders\.created_at\s+DESC", sql)
                is not None
            ), f"created_at rank ORDER BY missing:\n{sql}"

    async def test_order_by_last_inherits_default_time_dim(
        self, generator: SQLGenerator
    ) -> None:
        """``ORDER BY revenue:last DESC`` (no explicit time arg) inherits
        ``default_time_dimension`` for the rank ordering.
        """
        m = _orders_two_ts_model(default_td="created_at")
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=["*:count"],
                dimensions=[ColumnRef(name="status")],
                order=[OrderItem(column="revenue:last", direction="desc")],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            assert len(self._ranked_ctes(sql)) == 1, sql
            assert (
                _re.search(r"ORDER BY\s+orders\.created_at\s+DESC", sql)
                is not None
            ), f"default created_at rank ORDER BY missing:\n{sql}"

    async def test_projected_and_order_only_first_last_mixed(
        self, generator: SQLGenerator
    ) -> None:
        """A PROJECTED ``last(created_at)`` (alias ``latest_cr``) and an
        ORDER-BY-only ``last(updated_at)`` coexist — both rank columns
        exist, the projected alias surfaces in result keys, the order-only
        materialised alias is trimmed from result keys.
        """
        m = _orders_two_ts_model()
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=[
                    ModelMeasure(formula="revenue:last(created_at)", name="latest_cr"),
                ],
                dimensions=[ColumnRef(name="status")],
                order=[OrderItem(column="revenue:last(updated_at)", direction="desc")],
            )
            res = await engine.execute(query, dry_run=True)
            sql = res.sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            # One CTE per aggregate, whether or not it is projected.
            names = self._ranked_ctes(sql)
            assert len(names) == 2, sql
            # Result keys = projection only: status + projected last alias.
            assert set(res.columns) == {"orders.status", "orders.latest_cr"}, (
                f"Result columns mismatch: {res.columns!r}\nSQL:\n{sql}"
            )
            # The two aggregates must not collapse onto one another: each
            # ranks by its OWN time column, in its OWN CTE. Read off the
            # windows rather than a rank-column alias, because there is no
            # longer a shared alias space in which a collapse could hide.
            norm = _norm(sql)
            assert "ORDER BY orders.created_at DESC" in norm, sql
            assert "ORDER BY orders.updated_at DESC" in norm, sql
            # The PROJECTED one surfaces under the user's alias; the
            # order-only one is named only by the ORDER BY.
            assert '"orders.latest_cr"' in sql, sql
            order_terms = _outer_order_terms(sql)
            assert len(order_terms) == 1, order_terms
            assert "revenue_last_updated_at" in order_terms[0][0], order_terms

    async def test_filter_only_last_with_having(
        self, generator: SQLGenerator
    ) -> None:
        """A filter referencing a non-projected ``last(created_at)`` must
        materialise that aggregate and apply the predicate where its value is
        readable — the outer combined SELECT.

        It was a HAVING on the host base, because the ranked value lived there.
        It cannot be one now, and must not be: the ranked CTE is LEFT JOINed
        back, so dropping a CTE row resurrects the host row carrying NULL
        instead of removing it (the DEV-1503 rule).
        """
        m = _orders_two_ts_model(default_td="created_at")
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=["*:count"],
                dimensions=[ColumnRef(name="status")],
                filters=["revenue:last(created_at) > 100"],
            )
            res = await engine.execute(query, dry_run=True)
            sql = res.sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            names = self._ranked_ctes(sql)
            assert len(names) == 1, sql
            assert _re.search(
                r"ROW_NUMBER\(\)\s+OVER\s*\([^)]*ORDER BY\s+orders\.created_at\s+DESC", sql
            ), f"created_at rank window missing:\n{sql}"
            assert "HAVING" not in sql.upper(), sql
            tail = sql[sql.rfind("WHERE"):]
            assert tail.startswith("WHERE"), sql
            assert names[0] in tail, sql
            assert "> 100" in tail, sql
            # Hidden materialised alias must not surface in result keys.
            assert set(res.columns) == {"orders.status", "orders._count"}, (
                f"Hidden filter aggregate leaked: {res.columns!r}\nSQL:\n{sql}"
            )

    async def test_filter_only_last_two_time_cols(
        self, generator: SQLGenerator
    ) -> None:
        """A filter references TWO last() aggregates over different time
        columns — each is materialised in its own CTE and the outer predicate
        names both, so distinct time columns cannot collapse.
        """
        m = _orders_two_ts_model()
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=["*:count"],
                dimensions=[ColumnRef(name="status")],
                filters=[
                    "revenue:last(created_at) > 100 and revenue:last(updated_at) < 50"
                ],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            names = self._ranked_ctes(sql)
            assert len(names) == 2, sql
            assert "HAVING" not in sql.upper(), sql
            tail = sql[sql.rfind("WHERE"):]
            assert tail.startswith("WHERE"), sql
            for name in names:
                assert name in tail, (
                    f"outer predicate does not reference {name!r}:\n{tail}"
                )

    async def test_filter_and_order_last_different_time_cols(
        self, generator: SQLGenerator
    ) -> None:
        """A filter uses ``last(created_at)`` while ORDER BY uses
        ``last(updated_at)`` — each must resolve to its OWN materialised value.
        Combined-surface regression (Codex MED 8).
        """
        m = _orders_two_ts_model()
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=["*:count"],
                dimensions=[ColumnRef(name="status")],
                filters=["revenue:last(created_at) > 100"],
                order=[OrderItem(column="revenue:last(updated_at)", direction="desc")],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            assert len(self._ranked_ctes(sql)) == 2, sql
            assert "HAVING" not in sql.upper(), sql
            # The outer predicate names the created_at CTE and only it.
            where_at = sql.rfind("WHERE")
            where_clause = sql[where_at:sql.rfind("ORDER BY")]
            assert "revenue_last_created_at" in where_clause, sql
            assert "revenue_last_updated_at" not in where_clause, sql
            # Outer ORDER BY references the materialised alias for the
            # updated_at first/last.
            terms = _outer_order_terms(sql)
            assert len(terms) == 1, terms
            assert "revenue_last_updated_at" in terms[0][0], (
                f"Outer ORDER BY did not reference the updated_at "
                f"materialised alias:\n{terms}"
            )

    async def test_projected_two_last_different_time_cols_no_default(
        self, generator: SQLGenerator
    ) -> None:
        """Two PROJECTED ``last(created_at)`` / ``last(updated_at)`` on a model
        with NO ``default_time_dimension`` must stay DISTINCT.

        The bug this pinned was a suffix guard that skipped when
        ``default_time_col`` was ``None`` — so two specs each carrying an
        explicit arg both collapsed onto a bare ``_last_rn``. There is no
        shared rank column left for them to collapse onto; what is asserted is
        the property, on the two scopes that now hold it.
        """
        m = _orders_two_ts_model()  # no default_td
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=[
                    ModelMeasure(formula="revenue:last(created_at)", name="lc"),
                    ModelMeasure(formula="revenue:last(updated_at)", name="lu"),
                ],
                dimensions=[ColumnRef(name="status")],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            names = self._ranked_ctes(sql)
            assert len(names) == 2, sql
            norm = _norm(sql)
            # Each ranks by its OWN time column…
            assert "ORDER BY orders.created_at DESC" in norm, sql
            assert "ORDER BY orders.updated_at DESC" in norm, sql
            # …and each user alias is projected from a DIFFERENT CTE, which is
            # the collapse this test exists to catch.
            lc = _re.search(r'(_cm_\w+)\."orders\.lc"', norm)
            lu = _re.search(r'(_cm_\w+)\."orders\.lu"', norm)
            assert lc is not None, sql
            assert lu is not None, sql
            assert lc.group(1) != lu.group(1), sql


    async def test_filtered_first_last_in_having_uses_filtered_rn(
        self, generator: SQLGenerator
    ) -> None:
        """A FILTERED ``last(time_col)`` (``Column.filter`` set) referenced from
        a filter must be ranked over the MATCHING rows only.

        It used to need a dedicated rank column that sorted non-matching rows
        to the bottom (``_last_rn_f0``) plus a match flag (``_match_f0``) the
        predicate consulted, because the ranking was shared and could not drop
        rows. In its own scope the filter is simply a WHERE, applied before the
        ranking — which is the same answer by construction rather than by
        careful alias bookkeeping.
        """
        async with _persist_and_engine(_orders_with_paid_amount_model()) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=["*:count"],
                dimensions=[ColumnRef(name="status")],
                filters=["paid_amount:last(created_at) > 100"],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            names = self._ranked_ctes(sql)
            assert len(names) == 1, sql
            rk_body = _norm(_extract_cte_body(sql, _re.escape(names[0])))
            assert "WHERE orders.status = 'paid'" in rk_body, sql
            assert "_last_rn_f0" not in sql, sql
            assert "_match_f0" not in sql, sql
            # The predicate on the ranked value lands on the outer SELECT.
            assert "HAVING" not in sql.upper(), sql
            tail = sql[sql.rfind("WHERE"):]
            assert tail.startswith("WHERE"), sql
            assert names[0] in tail, sql

    async def test_filtered_first_last_in_nested_having_uses_filtered_rn(
        self, generator: SQLGenerator
    ) -> None:
        """A FILTERED ``last(time_col)`` nested inside a scalar-function call
        (``coalesce(agg, 0)``) must still resolve to the aggregate's own
        materialised value through the recursion.

        Regression for the ``_render_value_key_for_filter`` recursive call
        sites (``ScalarCallKey`` / ``BetweenKey`` / ``InKey``) that dropped
        their alias environment on the way down: the leaf then rendered against
        whatever the outer scope happened to have.
        """
        async with _persist_and_engine(_orders_with_paid_amount_model()) as engine:
            # ``coalesce(agg, 0)`` wraps the AggregateKey in a
            # ScalarCallKey — exercises the recursion branch that
            # previously dropped ``aliases_by_slot_id``.
            query = SlayerQuery(
                source_model="orders",
                measures=["*:count"],
                dimensions=[ColumnRef(name="status")],
                filters=["coalesce(paid_amount:last(created_at), 0) > 0"],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            names = self._ranked_ctes(sql)
            assert len(names) == 1, sql
            tail = sql[sql.rfind("WHERE"):]
            assert tail.startswith("WHERE"), sql
            assert "COALESCE" in tail.upper(), tail
            assert names[0] in tail, (
                f"the COALESCE wrap did not resolve its leaf to the ranked "
                f"CTE — the alias environment was dropped through the "
                f"ScalarCallKey recursion:\n{tail}"
            )

    async def test_composite_only_first_last_triggers_ranked_subquery(
        self, generator: SQLGenerator
    ) -> None:
        """A query whose ONLY first/last reference is INSIDE a composite
        aggregate (no direct first/last sibling) must still materialise each
        operand. Codex review of DEV-1501 PR #159 round 4.

        The failure it pinned was a composite render emitting
        ``MAX(CASE WHEN _last_rn = 1 …)`` against a FROM that projected no such
        column, because the ranked wrap was triggered by a scan that did not
        look inside composites. Each operand is a slot with a plan of its own
        now, so there is no trigger to miss — and the composite evaluates in
        the combined SELECT, over the joined-back columns.
        """
        async with _persist_and_engine(_orders_two_ts_model()) as engine:
            query = SlayerQuery(
                source_model="orders",
                # ONLY composite — no direct first/last sibling.
                measures=[ModelMeasure(
                    formula="revenue:last(created_at) + revenue:last(updated_at)",
                    name="diff",
                )],
                dimensions=[ColumnRef(name="status")],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            names = self._ranked_ctes(sql)
            assert len(names) == 2, (
                f"composite-only first/last did not materialise both "
                f"operands:\n{sql}"
            )
            diff_match = _re.search(
                r'(_cm_\w+)\."[^"]+" \+ (_cm_\w+)\."[^"]+" AS "orders\.diff"',
                _norm(sql),
            )
            assert diff_match is not None, (
                f"composite ``diff`` projection not found:\n{sql}"
            )
            assert diff_match.group(1) != diff_match.group(2), (
                f"Composite operands collapsed onto one CTE:\n{sql}"
            )

    async def test_composite_first_last_without_default_time_dim_raises(
        self, generator: SQLGenerator
    ) -> None:
        """A composite measure containing a first/last operand without
        explicit time arg AND no model ``default_time_dimension`` must
        raise the "first/last requires a ranking time column" error.
        Without composite-aware validation in
        ``_build_first_last_base_select``, the query bypasses the check
        and ``_build_unfiltered_rn_columns`` emits ``ORDER BY None``.
        Codex review of DEV-1501 PR #159 round 5.
        """
        # ``_orders_two_ts_model()`` has no ``default_time_dimension``.
        async with _persist_and_engine(_orders_two_ts_model()) as engine:
            query = SlayerQuery(
                source_model="orders",
                # Composite with bare ``revenue:last`` — no explicit
                # time arg, no default time dim, no temporal dim.
                measures=[ModelMeasure(
                    formula="revenue:last + 1", name="plus1",
                )],
                dimensions=[ColumnRef(name="status")],
            )
            with pytest.raises(ValueError, match="time"):
                await engine.execute(query, dry_run=True)

    async def test_composite_first_last_with_joined_time_arg_adds_join(
        self, generator: SQLGenerator
    ) -> None:
        """A composite measure containing a first/last operand with a
        JOINED explicit time arg (``revenue:last(customers.signed_up_at)
        + 1``) must pull the ``customers`` join into the FROM so the
        ranked subquery's ORDER BY can reference ``customers.signed_up_at``.
        Without composite-aware path discovery in
        ``_collect_joined_paths_for_base``, the join is missing and the
        SQL references an unjoined alias. Codex review of DEV-1501 PR
        #159 round 5.
        """
        customers = SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="signed_up_at", sql="signed_up_at", type=DataType.TIMESTAMP),
            ],
        )
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
            )],
        )
        async with _persist_and_engine(customers, orders) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(
                    formula="revenue:last(customers.signed_up_at) + 1",
                    name="plus1",
                )],
                dimensions=[ColumnRef(name="status")],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            # The ranked subquery's ORDER BY references customers.signed_up_at,
            # so the customers join MUST be in the FROM.
            assert "LEFT JOIN customers" in sql or "JOIN customers" in sql, (
                f"customers join missing — composite first/last with joined "
                f"time arg didn't pull its join:\n{sql}"
            )
            assert "customers.signed_up_at" in sql

    async def test_filtered_composite_first_last_uses_filtered_rn(
        self, generator: SQLGenerator
    ) -> None:
        """A FILTERED first/last operand inside a composite projection
        (``paid_amount:last(created_at) + 1``) must be ranked over the MATCHING
        rows.

        The bug this pinned was an alias-key mismatch: the composite synth
        minted a per-leaf alias, the ranked subquery keyed its filtered rank
        columns by a different one, the lookup missed, and the operand silently
        fell back to the unfiltered ranking. The leaf owns its scope now, so
        there is no lookup to miss. Codex review of DEV-1501 PR #159 round 6.
        """
        async with _persist_and_engine(_orders_with_paid_amount_model()) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(
                    formula="paid_amount:last(created_at) + 1",
                    name="plus1",
                )],
                dimensions=[ColumnRef(name="status")],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            names = self._ranked_ctes(sql)
            assert len(names) == 1, sql
            rk_body = _norm(_extract_cte_body(sql, _re.escape(names[0])))
            assert "WHERE orders.status = 'paid'" in rk_body, sql
            assert "_last_rn_f0" not in sql, sql
            assert "_match_f0" not in sql, sql
            assert _re.search(
                rf'{_re.escape(names[0])}\."[^"]+" \+ 1 AS "orders\.plus1"',
                _norm(sql),
            ), (
                f"the composite operand did not read the filtered aggregate's "
                f"own CTE:\n{sql}"
            )

    async def test_composite_first_last_in_projection_uses_correct_suffixes(
        self, generator: SQLGenerator
    ) -> None:
        """A COMPOSITE aggregate measure containing first/last operands with
        different explicit time columns
        (``revenue:last(created_at) + revenue:last(updated_at)``) must render
        each operand against its OWN ranking, alongside a directly projected
        first/last that shares one of those time columns.

        The bug it pinned: the composite renderer never received the rn state,
        so both operands emitted the bare ``_last_rn`` and the sum double-
        counted one column. Codex review of DEV-1501 PR #159 round 3.
        """
        async with _persist_and_engine(_orders_two_ts_model(default_td="created_at")) as engine:
            query = SlayerQuery(
                source_model="orders",
                # Direct first/last triggers _build_first_last_base_select;
                # the composite operand is the bug under test.
                measures=[
                    ModelMeasure(formula="revenue:last(created_at)", name="lc"),
                    ModelMeasure(
                        formula="revenue:last(created_at) + revenue:last(updated_at)",
                        name="diff",
                    ),
                ],
                dimensions=[ColumnRef(name="status")],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            # ``lc`` and the composite's created_at operand are the SAME
            # aggregate (one structural key, one slot, one CTE); the
            # updated_at operand is a second one.
            names = self._ranked_ctes(sql)
            assert len(names) == 2, sql
            diff_match = _re.search(
                r'(_cm_\w+)\."[^"]+" \+ (_cm_\w+)\."[^"]+" AS "orders\.diff"',
                _norm(sql),
            )
            assert diff_match is not None, (
                f"composite ``diff`` projection not found / wrong shape:\n{sql}"
            )
            left, right = diff_match.group(1), diff_match.group(2)
            assert left != right, (
                f"Composite first/last operands collapsed onto one CTE "
                f"({left!r}):\n{sql}"
            )
            lc = _re.search(r'(_cm_\w+)\."orders\.lc"', _norm(sql))
            assert lc is not None, sql
            assert lc.group(1) == left, (
                f"the projected ``lc`` and the composite's created_at operand "
                f"are one aggregate and must share one CTE:\n{sql}"
            )

    async def test_cross_model_query_with_local_first_last_having_filter(
        self, generator: SQLGenerator
    ) -> None:
        """A query carrying BOTH a cross-model aggregate AND a LOCAL first/last
        filter must emit valid SQL.

        This shape was where the ranked base wrap and the cross-model CTE path
        collided: the host base could not both be wrapped in a ranking and be
        the ``_base`` of a combined SELECT, so the query either emitted a
        HAVING referencing a dangling ``_last_rn`` or was refused outright. The
        test allowed EITHER a working render or a clear ``NotImplementedError``,
        because both beat silently invalid SQL.

        Neither aggregate lives in ``_base`` now — each has its own CTE and
        both join back on the grain — so the collision has no site left and
        only the working outcome remains. Codex review of DEV-1501 PR #159
        round 6.
        """
        customers = SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="score", sql="score", type=DataType.DOUBLE),
            ],
        )
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
            )],
        )
        async with _persist_and_engine(customers, orders) as engine:
            query = SlayerQuery(
                source_model="orders",
                # Cross-model measure + LOCAL first/last HAVING filter.
                measures=[ModelMeasure(formula="customers.score:sum", name="cs")],
                dimensions=[ColumnRef(name="status")],
                filters=["revenue:last(created_at) > 100"],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            # Three scopes, each owning one thing: ``_base`` the grain, the
            # cross-model SUM's ``_cm_`` CTE, and the ranking's own ``_cm_``
            # CTE (DEV-1835 unified naming; the ranked one carries ``_last``).
            base_sql = _extract_cte_body(sql, r"_base")
            assert "ROW_NUMBER" not in base_sql, base_sql
            assert "_cm_" in sql, sql
            assert "_cm_orders__revenue_last_created_at" in sql, sql
            assert "ROW_NUMBER" in _extract_cte_body(sql, r"_cm_\w*revenue_last\w*"), sql
            # The predicate on the ranked value is an outer WHERE, never a
            # HAVING that a LEFT JOIN back would turn into a NULL row.
            assert "HAVING" not in sql.upper(), sql
            tail = sql[sql.rfind("WHERE"):]
            assert tail.startswith("WHERE"), sql
            assert "> 100" in tail, sql

    async def test_cross_model_filtered_last_in_having(
        self, generator: SQLGenerator
    ) -> None:
        """A FILTERED cross-model ``last()`` (``Column.filter`` set on the
        joined model's column) referenced from a query filter must rank over
        the MATCHING target rows, and the predicate must be applied where that
        value is readable.

        The bug it pinned was ranked-state threading: the routed HAVING
        re-emitted the aggregate with no filtered-rank map, so it bound to the
        unfiltered ranking and picked the wrong row. The filter is a WHERE
        inside the ranked CTE now, so there is no second emission to keep in
        step. CodeRabbit review of DEV-1501 PR #159 (Group A.3).
        """
        customers = SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="signed_up_at", sql="signed_up_at", type=DataType.TIMESTAMP),
                Column(name="active", sql="active", type=DataType.BOOLEAN),
                # FILTERED column — only active customers' rows participate
                # in the ranking-and-pick.
                # ``active`` is BOOLEAN — Postgres rejects ``active = 1``
                # (no implicit bool↔int cast); use ``= TRUE`` so the
                # generated CASE WHEN is valid (CodeRabbit DEV-1501 round 5).
                Column(
                    name="active_score", sql="score",
                    filter="active = TRUE", type=DataType.DOUBLE,
                ),
            ],
        )
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
            )],
        )
        async with _persist_and_engine(customers, orders) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=["*:count"],
                dimensions=[ColumnRef(name="status")],
                filters=["customers.active_score:last(customers.signed_up_at) > 50"],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            names = self._ranked_ctes(sql)
            assert len(names) == 1, sql
            rk_body = _norm(_extract_cte_body(sql, _re.escape(names[0])))
            # The target's own rows are narrowed BEFORE the ranking.
            assert "FROM customers AS customers" in rk_body, rk_body
            assert "WHERE customers.active = TRUE" in rk_body, rk_body
            assert "_last_rn_f0" not in sql, sql
            assert "_match_f0" not in sql, sql
            assert "HAVING" not in sql.upper(), sql
            tail = sql[sql.rfind("WHERE"):]
            assert tail.startswith("WHERE"), sql
            assert names[0] in tail, sql
            assert "> 50" in tail, sql

    async def test_derived_time_arg_pulls_in_referenced_join(
        self, generator: SQLGenerator,
    ) -> None:
        """DEV-1501 (Codex round 8): a DERIVED first/last time arg whose
        ``Column.sql`` references a joined column must pull that join into
        the base FROM. Join discovery (``_resolve_agg_inputs_via_scope`` via
        ``_explicit_time_arg_of``) registers it, and the ranked plan expands
        ``net_signed_at.sql = "customers.signed_up_at"`` so the ranked
        subquery's ``ORDER BY`` emits ``customers.signed_up_at``; without
        a corresponding ``LEFT JOIN customers`` the SQL is broken.
        Previously ``_collect_joined_paths_for_base`` only walked
        ``ColumnKey`` args; ``ColumnSqlKey`` derived time args were
        invisible to join discovery.
        """
        customers = SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="signed_up_at", sql="signed_up_at", type=DataType.TIMESTAMP),
            ],
        )
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                # Derived time column whose sql crosses the customers join.
                # No other slot drags ``customers`` into the FROM, so this
                # is the ONLY signal join discovery has to add it.
                Column(
                    name="net_signed_at", sql="customers.signed_up_at",
                    type=DataType.TIMESTAMP,
                ),
            ],
            joins=[
                ModelJoin(
                    target_model="customers",
                    join_pairs=[["customer_id", "id"]],
                ),
            ],
        )
        async with _persist_and_engine(customers, orders) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=["*:count"],
                dimensions=[ColumnRef(name="status")],
                order=[
                    OrderItem(
                        column="amount:last(net_signed_at)", direction="desc",
                    ),
                ],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            # The ranked subquery's ORDER BY must reference the expanded
            # joined column.
            assert "customers.signed_up_at" in sql, (
                f"Expected expanded customers.signed_up_at ranking expression in SQL:\n{sql}"
            )
            # The customers join must be present in the base FROM. Without
            # the DEV-1501 fix, ``_collect_joined_paths_for_base`` skipped
            # the ColumnSqlKey arg and the join was missing → broken SQL.
            assert _re.search(
                r"LEFT JOIN\s+customers\b", sql, _re.IGNORECASE,
            ) is not None, (
                f"customers join missing in base FROM — derived time-col "
                f"join discovery regressed:\n{sql}"
            )


class TestDev1501BroadTriggerAndGuards:
    """DEV-1501 — broad-trigger materialise+trim behaviour and the guards
    Codex flagged: the wrap is conditional, hidden ROW order targets must
    not be materialised (cardinality preserved), composite hidden order is
    explicitly NotImplementedError, dim-only-dedup grain is preserved.
    """

    async def test_outer_wrap_only_when_hidden_present(
        self, generator: SQLGenerator
    ) -> None:
        """When no hidden materialised aggregates exist, the no-transform
        path stays flat — no outer trim wrapper. ORDER BY references the
        projected alias inline at the same SELECT level.
        """
        m = _orders_two_ts_model(default_td="created_at")
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="revenue:sum", name="rev")],
                dimensions=[ColumnRef(name="status")],
                order=[OrderItem(column="rev", direction="desc")],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            # No hidden materialised aggregates → top-level FROM is a bare
            # Table (no outer wrap Subquery wrapper).
            assert isinstance(_outer_from_node(sql), sqlglot.exp.Table), (
                f"Outer wrap added even though no hidden aggregates exist:\n{sql}"
            )

    async def test_order_by_hidden_simple_aggregate_materialized(
        self, generator: SQLGenerator
    ) -> None:
        """Broad-trigger regression: a NON-first/last hidden order aggregate
        (``ORDER BY revenue:sum DESC`` with no projected ``revenue:sum``)
        also goes through materialise+trim — outer wrap projects only the
        public columns, outer ORDER BY references the materialised
        ``"orders.revenue_sum"`` alias, and the hidden alias is absent
        from result keys and response metadata.
        """
        m = _orders_two_ts_model(default_td="created_at")
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=["*:count"],
                dimensions=[ColumnRef(name="status")],
                order=[OrderItem(column="revenue:sum", direction="desc")],
            )
            res = await engine.execute(query, dry_run=True)
            sql = res.sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            # Outer wrap present: top-level FROM is a Subquery wrapper.
            assert isinstance(_outer_from_node(sql), sqlglot.exp.Subquery), (
                f"Expected outer-wrap subquery FROM:\n{sql}"
            )
            # Public projection trimmed to dim + count.
            assert set(res.columns) == {"orders.status", "orders._count"}, (
                f"Hidden revenue_sum leaked into result columns: "
                f"{res.columns!r}\nSQL:\n{sql}"
            )
            # Outer ORDER BY references the materialised alias EXACTLY as
            # the full dotted form ``"orders.revenue_sum"`` (quoted dotted
            # alias body, not qualified ``"orders"."revenue_sum"``).
            terms = _outer_order_terms(sql)
            assert len(terms) == 1, terms
            order_expr, order_dir = terms[0]
            assert order_dir == "desc"
            assert (
                '"orders.revenue_sum"' in order_expr
                or order_expr == "orders.revenue_sum"
            ), (
                f"Outer ORDER BY does not reference the dotted materialised "
                f"alias 'orders.revenue_sum':\n{order_expr}\nSQL:\n{sql}"
            )
            # Response-meta invariant: hidden alias not in attributes.
            attr_keys = (
                set(res.attributes.dimensions.keys())
                | set(res.attributes.measures.keys())
            )
            assert not any("revenue_sum" in k for k in attr_keys), (
                f"Hidden revenue_sum surfaced in response attributes: "
                f"{attr_keys!r}"
            )

    async def test_dim_only_dedup_with_hidden_order_first_last(
        self, generator: SQLGenerator
    ) -> None:
        """Dim-only query (no measures, dim auto-dedup) with a hidden
        first/last in ORDER BY. The dim-only-dedup GROUP BY must contain
        ONLY the dimension(s) — the aggregate-only narrowing of Change 2
        prevents the hidden first/last from leaking extra GROUP BY entries.
        """
        m = _orders_two_ts_model(default_td="created_at")
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                order=[OrderItem(column="revenue:last(created_at)", direction="desc")],
            )
            res = await engine.execute(query, dry_run=True)
            sql = res.sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            assert "_cm_" in sql
            # Public projection trimmed to dim only.
            assert set(res.columns) == {"orders.status"}, (
                f"Hidden first/last alias leaked into dim-only result: "
                f"{res.columns!r}\nSQL:\n{sql}"
            )
            tree = sqlglot.parse_one(sql, dialect="postgres")
            # Every Select that has a GROUP BY must group by exactly ONE
            # expression (the status dim). The inner ranked subquery has no
            # GROUP BY; the base SELECT groups by status; the outer wrap
            # has no GROUP BY.
            group_counts = [
                len(sel.args["group"].expressions)
                for sel in tree.find_all(sqlglot.exp.Select)
                if sel.args.get("group")
            ]
            assert group_counts, f"No GROUP BY found:\n{sql}"
            assert all(c == 1 for c in group_counts), (
                f"GROUP BY contains more than the dim — extra row deps "
                f"materialised. Counts: {group_counts}\nSQL:\n{sql}"
            )

    async def test_hidden_row_order_target_max_wraps_without_widening_grain(
        self, generator: SQLGenerator
    ) -> None:
        """ORDER BY a non-projected LOCAL ROW column (e.g. ``customer_id``) in
        an aggregated query.

        History: this raised ``NotImplementedError``, then (DEV-1712 Stage 8) a
        plan-time ``ValueError``. DEV-1703 Phase 1 resolves it instead — the
        column materialises as a hidden aggregate wrap and the ORDER BY names
        that alias — ``customer_id:min`` here, since DEV-1747 D10 made the wrap
        direction-aware and this order is ASC. The invariant this test has always
        really
        been about is preserved and pinned explicitly below: the sort key must
        NEVER reach GROUP BY, because widening the grain would change both the
        row count and every other measure's value.
        """
        m = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
        )
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=["*:count"],
                dimensions=[ColumnRef(name="status")],
                order=[OrderItem(column="customer_id", direction="asc")],
            )
            resp = await engine.execute(query, dry_run=True)
            sql = resp.sql
            assert _re.search(r"MIN\(\s*orders\.customer_id\s*\)", sql), sql
            # The sort key must not widen the grain: GROUP BY stays on status.
            inner = sqlglot.parse_one(sql, dialect="postgres").find(sqlglot.exp.Group)
            assert inner is not None, sql
            group_sql = " ".join(e.sql() for e in inner.expressions)
            assert "customer_id" not in group_sql, sql
            assert "status" in group_sql, sql

    async def test_hidden_simple_aggregate_in_having(
        self, generator: SQLGenerator
    ) -> None:
        """``filters=["revenue:sum > 100"]`` with no projected
        ``revenue:sum`` (plain non-first/last). Broad materialise+trim
        applies to filter-side too: hidden ``revenue:sum`` materialised
        in base, HAVING references it (inline or by alias), absent from
        result keys and response metadata.
        """
        m = _orders_two_ts_model(default_td="created_at")
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=["*:count"],
                dimensions=[ColumnRef(name="status")],
                filters=["revenue:sum > 100"],
            )
            res = await engine.execute(query, dry_run=True)
            sql = res.sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            # Result keys must NOT carry the hidden revenue_sum alias.
            assert set(res.columns) == {"orders.status", "orders._count"}, (
                f"Hidden revenue_sum leaked into result columns: "
                f"{res.columns!r}\nSQL:\n{sql}"
            )
            attr_keys = (
                set(res.attributes.dimensions.keys())
                | set(res.attributes.measures.keys())
            )
            assert not any("revenue_sum" in k for k in attr_keys), (
                f"Hidden revenue_sum surfaced in response attributes: "
                f"{attr_keys!r}"
            )
            assert "HAVING" in sql.upper(), f"HAVING missing:\n{sql}"

    async def test_order_by_with_limit_offset_on_outer_wrap(
        self, generator: SQLGenerator
    ) -> None:
        """ORDER BY / LIMIT / OFFSET must be applied where the ordered value is
        visible — never below it, which would slice rows before the aggregate
        being sorted on exists.

        The shape has changed: a hidden order-only first/last used to be
        materialised INSIDE ``_base``, so the statement needed an outer wrap to
        sort and paginate above it. Its value lives in a joined-back CTE now,
        so the combined SELECT can see it directly and the wrap has nothing
        left to do. What must not change is the level: the pagination sits on
        the SELECT that reads the ranked column, and no scope below it carries
        any of the three.
        """
        m = _orders_two_ts_model(default_td="created_at")
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=["*:count"],
                dimensions=[ColumnRef(name="status")],
                order=[OrderItem(column="revenue:last(created_at)", direction="desc")],
                limit=5,
                offset=2,
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            tree = sqlglot.parse_one(sql, dialect="postgres")
            assert isinstance(tree, sqlglot.exp.Select)
            assert tree.args.get("order") is not None, (
                f"Outer ORDER BY missing:\n{sql}"
            )
            assert tree.args.get("limit") is not None, (
                f"Outer LIMIT missing:\n{sql}"
            )
            assert tree.args.get("offset") is not None, (
                f"Outer OFFSET missing:\n{sql}"
            )
            # ...and the ORDER BY reads the ranked CTE, so the pagination is
            # applied above the value it sorts on.
            assert "_cm_" in tree.args["order"].sql(dialect="postgres"), sql
            # No CTE body carries any of the three.
            with_node = tree.args.get("with_")
            assert with_node is not None, sql
            for cte in with_node.expressions:
                body = cte.this
                for key in ("order", "limit", "offset"):
                    assert body.args.get(key) is None, (
                        f"CTE {cte.alias_or_name!r} carries {key.upper()}:\n{sql}"
                    )

    async def test_c13_duplicate_aliases_with_outer_wrap(
        self, generator: SQLGenerator
    ) -> None:
        """DEV-1450 C13 — two declared measures with the same structural
        key but different ``name``s intern to ONE slot carrying TWO
        ``public_aliases``. Under the new outer trim wrap, BOTH aliases
        must appear in the outer projection (mirroring the transform
        path's ``outer_alias_index``).
        """
        m = _orders_two_ts_model(default_td="created_at")
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                # Same key (revenue:sum), two different names → one interned
                # slot with two public_aliases.
                measures=[
                    ModelMeasure(formula="revenue:sum", name="rev_a"),
                    ModelMeasure(formula="revenue:sum", name="rev_b"),
                ],
                dimensions=[ColumnRef(name="status")],
                # Force an outer wrap by adding a hidden order aggregate.
                order=[OrderItem(column="revenue:last(created_at)", direction="desc")],
            )
            res = await engine.execute(query, dry_run=True)
            sql = res.sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            # Both C13 aliases must be in the public projection — the outer
            # wrap must not collapse them to one.
            assert "orders.rev_a" in res.columns, (
                f"C13 alias 'orders.rev_a' missing from outer projection: "
                f"{res.columns!r}\nSQL:\n{sql}"
            )
            assert "orders.rev_b" in res.columns, (
                f"C13 alias 'orders.rev_b' missing from outer projection: "
                f"{res.columns!r}\nSQL:\n{sql}"
            )
            assert set(res.columns) == {"orders.status", "orders.rev_a", "orders.rev_b"}, (
                f"Unexpected projection: {res.columns!r}\nSQL:\n{sql}"
            )

    async def test_composite_filter_materialises_aggregate_leaves(
        self, generator: SQLGenerator
    ) -> None:
        """A composite HAVING filter (``revenue:sum - cost:sum > 0``) with
        neither operand projected: each AggregateKey leaf must be
        materialised in the base SELECT (composites recurse into operands
        per ``_iter_slot_deps``), HAVING references them (inline or by
        alias), the composite expression itself is inlined. The hidden
        leaf aliases are trimmed from result keys; no row leaves leak
        into GROUP BY.
        """
        m = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            default_time_dimension="created_at",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
                Column(name="cost", sql="cost", type=DataType.DOUBLE),
            ],
        )
        async with _persist_and_engine(m) as engine:
            query = SlayerQuery(
                source_model="orders",
                measures=["*:count"],
                dimensions=[ColumnRef(name="status")],
                filters=["revenue:sum - cost:sum > 0"],
            )
            res = await engine.execute(query, dry_run=True)
            sql = res.sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            # Result keys = projection only — hidden operand aggregates trimmed.
            assert set(res.columns) == {"orders.status", "orders._count"}, (
                f"Hidden composite operands leaked into result columns: "
                f"{res.columns!r}\nSQL:\n{sql}"
            )
            # HAVING present, references both operand aggregates (inline or by
            # materialised alias). Either SUM(orders.amount) or
            # "orders.revenue_sum" / "orders.cost_sum" — the test asserts
            # both source columns are reachable from HAVING.
            having_match = _re.search(r"HAVING(.*)$", sql, _re.DOTALL | _re.IGNORECASE)
            assert having_match is not None, f"HAVING missing:\n{sql}"
            having_sql = having_match.group(1)
            assert (
                "amount" in having_sql or "revenue_sum" in having_sql
            ), f"HAVING does not reference revenue operand:\n{having_sql}"
            assert (
                "cost" in having_sql
            ), f"HAVING does not reference cost operand:\n{having_sql}"
            # GROUP BY must NOT contain extra row-leaf columns — only status.
            tree = sqlglot.parse_one(sql, dialect="postgres")
            group_counts = [
                len(sel.args["group"].expressions)
                for sel in tree.find_all(sqlglot.exp.Select)
                if sel.args.get("group")
            ]
            assert group_counts and all(c == 1 for c in group_counts), (
                f"GROUP BY contains extras (row leaves leaked). Counts: "
                f"{group_counts}\nSQL:\n{sql}"
            )

    def test_hidden_composite_order_accepted_at_input_validation(
        self, generator: SQLGenerator
    ) -> None:
        """DEV-1733 INVERTED this contract.

        DEV-1501 pinned composite-aggregate ORDER BY as structurally
        unreachable: ``"revenue:sum - cost:sum"`` canonicalised to an invalid
        identifier and Pydantic rejected it before the planner ever saw it,
        which is why the no-transform path needed no explicit raise.

        The entry point now recognises it as a FORMULA rather than a column
        reference: the ``ColumnRef`` becomes the ``_expr_pending`` placeholder
        and ``raw_formula`` carries the original text for the planner to bind.
        A composite over declared measure ALIASES (no colon, no func-style
        call) is NOT a formula candidate and still raises here — alias
        references inside expressions are unsupported everywhere in SLayer.

        Full behaviour: ``tests/test_dev1733_order_only_transform_composite.py``.
        """
        from pydantic import ValidationError as PydanticValidationError

        item = OrderItem(column="revenue:sum - cost:sum", direction="desc")
        assert item.column.name == "_expr_pending"
        assert item.raw_formula == "revenue:sum - cost:sum"

        with pytest.raises(PydanticValidationError):
            OrderItem(column="revenue - cost", direction="desc")


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
            await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
            await storage.save_model(pcd)
            await storage.save_model(claim_cov)
            await storage.save_model(claim_amt)
            engine = SlayerQueryEngine(storage=storage)

            query = SlayerQuery(
                source_model="policy_coverage_detail",
                measures=[ModelMeasure(formula="claim_coverage.claim_amount.total_claim_amount:sum")],
                dimensions=[ColumnRef(name="coverage_code")],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
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
            await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
            for m in (model_a, model_b, model_c, model_d):
                await storage.save_model(m)
            engine = SlayerQueryEngine(storage=storage)

            query = SlayerQuery(
                source_model="a",
                measures=[ModelMeasure(formula="b.c.d.value:sum")],
                dimensions=[ColumnRef(name="status")],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
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
            await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
            await storage.save_model(orders)
            await storage.save_model(customers)
            engine = SlayerQueryEngine(storage=storage)

            query = SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="customers.score:sum")],
                dimensions=[ColumnRef(name="status")],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
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
            await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
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
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
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
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
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
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
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
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
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
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
            _assert_valid_sql(sql)

            # Should have both _base (with SUM for local measure) and _cm_ CTE
            assert "_base" in sql
            assert "_cm_" in sql
            assert "/" in sql  # Division expression

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "DEV-1445: cross-model local-filter remap to source is not yet "
            "implemented on the typed pipeline. Auto-promotes when supported."
        ),
    )
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
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
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
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
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
            await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
            await storage.save_model(orders)
            await storage.save_model(customers)
            engine = SlayerQueryEngine(storage=storage)

            query = SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="customers.score:sum")],
                dimensions=[ColumnRef(name="status")],
                order=[OrderItem(column="customers.score:sum", direction="desc")],
            )
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
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
            await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
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
            sql = (await engine.execute(query, dry_run=True)).sql
            _assert_valid_sql(sql, dialect=generator.dialect)
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

        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="revenue:sum")],
            dimensions=[ColumnRef(name="customers.name")],
        )
        sql = await _generate(generator, query, orders, extra_models=[customers])
        assert "INNER JOIN" in sql
        assert "LEFT JOIN" not in sql


class TestMeasureFilterCrossModelJoin:
    """Measure filters referencing cross-model dimensions must trigger the join."""

    async def test_measure_filter_cross_model_constant_triggers_join(self, generator: SQLGenerator) -> None:
        """Measure filter 'loss_payment.has_flag = 1' where has_flag sql='1' must JOIN to loss_payment.

        DEV-1494 / dbt placeholder-join idiom: ``has_flag sql="1"`` is a constant
        whose only purpose is to force the join (the join, not the predicate
        value, does the filtering). So the join must be KEPT, while the derived
        ref is INLINED for runnable SQL — matching the query-level path's
        ``WHERE CAST(1 AS REAL) = 1``. ``loss_payment.has_flag`` is a derived
        column, not a physical one, so it must not survive in the output.
        """

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

        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_amt:sum")],
        )
        sql = await _generate(generator, query, claim_amount, extra_models=[loss_payment])
        # The JOIN to loss_payment must be present (it is the filter mechanism).
        # Parsed-alias assertion so a stray table mention / JOIN keyword in
        # unrelated SQL can't satisfy it.
        assert "loss_payment" in _join_aliases(sql), f"Missing JOIN to loss_payment: {sql}"
        # The derived ref must be inlined — no dangling ``loss_payment.has_flag``
        # (a derived column, absent from the physical table).
        sql_no_strings = _re.sub(r"'[^']*'", "''", _re.sub(r'"[^"]*"', '""', sql))
        assert _re.search(r"\bloss_payment\.has_flag\b", sql_no_strings) is None, (
            f"derived ref 'loss_payment.has_flag' left un-inlined:\n{sql}"
        )
        # The inlined constant must live INSIDE the aggregation-time CASE-WHEN
        # wrapper (not relocated to WHERE), guarding the column-filter shape.
        assert _re.search(r"SUM\(\s*CASE WHEN .*THEN claim_amount\.amount", _norm(sql)), (
            f"column filter not rendered as SUM(CASE WHEN ... THEN col):\n{sql}"
        )

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

        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="revenue:sum")],
            dimensions=[ColumnRef(name="customers.name")],
        )
        sql = await _generate(generator, query, orders, extra_models=[customers])
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

    async def _sql(self, claim_amount_model, related_models, query):
        """Run ``query`` against ``claim_amount_model`` (with the related join
        targets registered) through the typed engine and return emitted SQL."""
        return await _engine_generate(
            query=query,
            model=claim_amount_model,
            extra_models=list(related_models.values()),
            validate=False,
        )

    async def test_two_filtered_measures_get_separate_ctes(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """Two measures with different cross-model filters → separate _cm_ CTEs, not intersecting JOINs."""
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum"), ModelMeasure(formula="loss_reserve_amt:sum")],
            dimensions=[ColumnRef(name="claim.claim_number")],
        )
        sql = await self._sql(claim_amount_model, related_models, query)

        # Each filtered measure should get its own _cm_ CTE
        assert "loss_payment_amt" in sql
        assert "loss_reserve_amt" in sql
        cm_cte_names = _re.findall(r"(_cm_\w+)\s+AS\s*\(", sql)
        assert len(cm_cte_names) >= 2, (
            f"Expected ≥ 2 _cm_ CTEs (one per filtered measure), got {len(cm_cte_names)}: {cm_cte_names}\n{sql}"
        )
        # The host _base CTE must NOT have both INNER filter-target JOINs
        # (legacy bug: intersecting to zero rows). Inspect the _base body.
        base_match = _re.search(r"_base\s+AS\s*\(", sql)
        assert base_match, f"Expected host _base CTE in:\n{sql}"
        base_start = base_match.start()
        base_body = sql[base_start:sql.index("\n)", base_start)]
        assert not ("Loss_Payment" in base_body and "Loss_Reserve" in base_body), (
            f"Host _base CTE has both INNER filter-target JOINs — would intersect:\n{base_body}"
        )

    async def test_formula_over_isolated_measures(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """Formula referencing isolated measures evaluates against the
        joined-back ``_cm_*`` columns at the combined SELECT — NOT inline
        in ``_base`` (which would pull both filter-target INNER joins back
        into the host CTE and intersect to rows present in BOTH targets,
        silently corrupting both aggregates).

        Pins Codex round 2 finding #1.
        """
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[
                ModelMeasure(formula="loss_payment_amt:sum"),
                ModelMeasure(formula="loss_reserve_amt:sum"),
                ModelMeasure(formula="loss_payment_amt:sum + loss_reserve_amt:sum", name="total_loss"),
            ],
            dimensions=[ColumnRef(name="claim.claim_number")],
        )
        sql = await self._sql(claim_amount_model, related_models, query)

        # Formula should be evaluated (contains + operator)
        assert "+" in sql
        # Both isolated measures present in the SQL
        assert "loss_payment_amt" in sql
        assert "loss_reserve_amt" in sql
        # Each isolated filtered measure gets its own _cm_ CTE.
        cm_cte_names = _re.findall(r"(_cm_\w+)\s+AS\s*\(", sql)
        assert len(cm_cte_names) >= 2, (
            f"Expected ≥ 2 _cm_ CTEs, got {len(cm_cte_names)}: {cm_cte_names}\n{sql}"
        )
        # Both filter-target INNER joins must live INSIDE the per-measure
        # ``_cm_*`` CTEs, NEVER in the host ``_base`` body. Inline
        # rendering of the composite would re-introduce them and the
        # intersection bug DEV-1503 set out to eliminate.
        base_body = _extract_cte_body(sql, r"_base")
        assert "Loss_Payment" not in base_body, (
            f"Composite over isolated aggregates leaked Loss_Payment join "
            f"into _base — DEV-1503 isolation regressed:\n{base_body}"
        )
        assert "Loss_Reserve" not in base_body, (
            f"Composite over isolated aggregates leaked Loss_Reserve join "
            f"into _base — DEV-1503 isolation regressed:\n{base_body}"
        )
        # The composite alias must appear in the OUTER combined SELECT
        # (after all CTEs), referencing both ``_cm_*`` joined-back columns.
        last_cte_close = sql.rfind("\n)")
        outer = sql[last_cte_close + 2:]
        assert "total_loss" in outer, (
            f"Composite alias 'total_loss' missing from outer combined SELECT:\n{outer}"
        )
        assert "loss_payment_amt_sum" in outer, (
            f"Outer must reference loss_payment_amt_sum CTE column:\n{outer}"
        )
        assert "loss_reserve_amt_sum" in outer, (
            f"Outer must reference loss_reserve_amt_sum CTE column:\n{outer}"
        )
        _assert_valid_sql(sql)

    async def test_mixed_isolated_and_local_measures(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """Unfiltered measure stays in host base, filtered goes to its own _cm_ CTE."""
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="total_amount:sum"), ModelMeasure(formula="loss_payment_amt:sum")],
            dimensions=[ColumnRef(name="claim.claim_number")],
        )
        sql = await self._sql(claim_amount_model, related_models, query)

        # Unfiltered measure (total_amount) should be in the host _base CTE
        assert "_base" in sql
        assert "total_amount_sum" in sql
        base_match = _re.search(r"_base\s+AS\s*\(", sql)
        assert base_match, f"Expected _base CTE in:\n{sql}"
        base_start = base_match.start()
        base_body = sql[base_start:sql.index("\n)", base_start)]
        assert "total_amount" in base_body, (
            f"unfiltered total_amount should be in host _base CTE:\n{base_body}"
        )
        # Filtered measure in its own _cm_ CTE.
        assert "_cm_" in sql and "loss_payment_amt" in sql

    async def test_all_measures_isolated_produces_dimension_spine(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """When all measures are isolated, the host _base CTE is just a dimension spine."""
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum")],
            dimensions=[ColumnRef(name="claim.claim_number")],
        )
        sql = await self._sql(claim_amount_model, related_models, query)

        # Host _base CTE exists; the filtered measure goes to its own _cm_ CTE.
        assert "_base" in sql
        assert "_cm_" in sql and "loss_payment_amt" in sql
        # Inspect the _base body: dim spine with GROUP BY, no filter-target join.
        base_match = _re.search(r"_base\s+AS\s*\(", sql)
        assert base_match, f"Expected _base CTE in:\n{sql}"
        base_start = base_match.start()
        base_body = sql[base_start:sql.index("\n)", base_start)]
        assert "GROUP BY" in base_body, (
            f"_base must group by dimensions for the spine:\n{base_body}"
        )
        # The filter-target join belongs in the _cm_ CTE, not the host _base spine.
        assert "Loss_Payment" not in base_body, (
            f"_base CTE wrongly includes the filter-target INNER join:\n{base_body}"
        )

    async def test_combined_uses_cross_join_when_no_dimensions(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """When no dimensions exist, isolated _cm_ CTEs are CROSS JOINed to base (Bug Q6)."""
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum"), ModelMeasure(formula="loss_reserve_amt:sum")],
        )
        sql = await self._sql(claim_amount_model, related_models, query)
        # Both isolated _cm_ CTEs should be present
        cm_cte_names = _re.findall(r"(_cm_\w+)\s+AS\s*\(", sql)
        assert len(cm_cte_names) >= 2, (
            f"Expected ≥ 2 _cm_ CTEs (one per filtered measure), got {len(cm_cte_names)}: {cm_cte_names}\n{sql}"
        )
        assert "loss_payment_amt" in sql
        assert "loss_reserve_amt" in sql
        # With no dimensions, CROSS JOIN is needed (not LEFT JOIN with no ON)
        assert "CROSS JOIN" in sql

    async def test_filter_join_preserved_when_skip_isolated(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """Cross-model filter joins survive measure isolation (Bug Q9).

        When a measure is isolated into its own _cm_ CTE, a query-level row
        filter that references a different cross-model path (here ``claim``)
        must still apply somewhere — and the ``claim`` join must be present
        wherever the filter lands so the ref resolves. Under DEV-1503 the
        sub-plan receives the row filter, so the join + filter both land
        inside the _cm_ CTE; the no-dim host _base becomes a placeholder
        spine (the aggregate join-back is via CROSS JOIN).
        """
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum")],
            # No dimensions on claim — only the filter references the claim join.
            filters=["claim.claim_number = '12345'"],
        )
        sql = await self._sql(claim_amount_model, related_models, query)
        # The filter literal and the dotted column ref both survive.
        assert "claim_number" in sql
        assert "12345" in sql
        # The claim join must land somewhere (legacy: in _base; new: in _cm_).
        # Either is correct as long as the filter can resolve.
        assert "Claim" in sql and "JOIN" in sql, (
            f"claim join missing entirely:\n{sql}"
        )
        _assert_valid_sql(sql)

    async def test_isolated_cte_qualifies_cross_model_dim_correctly(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """Isolated CTEs qualify cross-model dimensions with dim.model_name (Bug Q11).

        The dimension claim.claim_number is on the 'claim' model. The isolated
        _cm_ CTE must reference claim.claim_number, not claim_amount.claim_number.
        """
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum")],
            dimensions=[ColumnRef(name="claim.claim_number")],
        )
        sql = await self._sql(claim_amount_model, related_models, query)
        # Extract the _cm CTE body for the filtered measure.
        cm_match = _re.search(r"_cm_\w*loss_payment_amt\w*", sql)
        assert cm_match, f"No _cm_ CTE for loss_payment_amt in:\n{sql}"
        cm_start = cm_match.start()
        cm_body = sql[cm_start:sql.index("\n)", cm_start)]
        # The dimension should use claim.claim_number, not claim_amount.claim_number.
        assert "claim.claim_number" in cm_body, f"Expected claim.claim_number in CTE:\n{cm_body}"
        assert "claim_amount.claim_number" not in cm_body, (
            f"Found wrong table qualification claim_amount.claim_number in CTE:\n{cm_body}"
        )

    async def test_cm_cte_skips_filters_on_unavailable_tables(self, generator: SQLGenerator) -> None:
        """Cross-model CTE WHERE must not include filters referencing tables it doesn't join (Bug Q9).

        The cross-model measure ``customers.score:sum`` lands in its own
        ``_cm_`` CTE rooted at ``customers``; a query filter on
        ``warehouse.status`` (reachable from the base via the warehouse join,
        but NOT inside the customers CTE) must stay in the base, not leak into
        the ``_cm_`` CTE.
        """
        customers = SlayerModel(
            name="customers", sql_table="Customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="score", sql="score", type=DataType.DOUBLE),
            ],
        )
        warehouse = SlayerModel(
            name="warehouse", sql_table="Warehouse", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
            ],
        )
        orders = SlayerModel(
            name="orders", sql_table="Orders", data_source="test",
            columns=[
                Column(name="order_id", sql="order_id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="warehouse_id", sql="warehouse_id", type=DataType.DOUBLE),
            ],
            joins=[
                ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
                ModelJoin(target_model="warehouse", join_pairs=[["warehouse_id", "id"]]),
            ],
        )
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="order_id")],
            measures=[ModelMeasure(formula="customers.score:sum")],
            filters=["warehouse.status = 'ACTIVE'"],
        )
        sql = await _generate(generator, query, orders, extra_models=[customers, warehouse])

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
        """Base SELECT must not be empty when all measures are isolated and there are no dims (Bug Q10).

        ALSO: the placeholder ``_base`` must NOT reference the host table.
        A ``SELECT 1 AS _placeholder FROM <host>`` spine returns one row
        per host row; CROSS JOINing that to a scalar ``_cm_*`` aggregate
        duplicates the aggregate by host row count — a no-dim aggregate
        query must return one row, not N (Codex round 2 #2).
        """
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum")],
            # No dimensions — base has nothing to select
        )
        sql = await self._sql(claim_amount_model, related_models, query)
        # Must not have an empty SELECT clause
        assert "SELECT\nFROM" not in sql, f"Empty SELECT detected:\n{sql}"
        assert "SELECT FROM" not in sql, f"Empty SELECT detected:\n{sql}"
        # Should still produce valid SQL with the isolated _cm_ CTE.
        assert "_cm_" in sql and "loss_payment_amt" in sql
        # ``_base`` must NOT reference the host table — that turns the
        # one-row placeholder into N rows.
        base_body = _extract_cte_body(sql, r"_base")
        assert "Claim_Amount" not in base_body, (
            f"_base placeholder must not reference host table when empty "
            f"(would duplicate scalar aggregate by host row count):"
            f"\n{base_body}"
        )
        assert "_placeholder" in base_body, (
            f"Expected _base placeholder to use the literal 1 spine:"
            f"\n{base_body}"
        )
        _assert_valid_sql(sql)

    async def test_aggregate_filter_on_isolated_measure_applied_on_outer(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """Aggregate-phase filter referencing an isolated measure is applied on
        the outer combined SELECT (as WHERE), not inside the _cm_ CTE.

        Under isolation, the filtered aggregate lives in a _cm_ CTE that LEFT
        JOINs back to _base. The host filter ``loss_payment_amt:sum > 1000``
        must drop host rows where the aggregate fails the condition — matching
        the inline-HAVING semantic. Since the outer combined SELECT is NOT
        aggregating, the comparison renders as WHERE on the joined-back CTE
        column, not HAVING-into-the-CTE (which would surface host rows as NULL
        instead of dropping them).
        """
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum")],
            dimensions=[ColumnRef(name="claim.claim_number")],
            filters=["loss_payment_amt:sum > 1000"],
        )
        sql = await self._sql(claim_amount_model, related_models, query)
        # Filter literal must survive.
        assert "1000" in sql, f"filter literal '1000' dropped:\n{sql}"
        # A '> 1000' comparison must appear in the SQL.
        assert "> 1000" in sql, f"Expected '> 1000' comparison in SQL:\n{sql}"
        # The comparison applies on the OUTER combined SELECT (after all CTEs)
        # — locate the position of the last CTE-close ``\n)`` and assert the
        # filter literal appears after it.
        cm_match = _re.search(r"_cm_\w+\s+AS\s*\(", sql)
        assert cm_match, f"Expected _cm_ CTE in:\n{sql}"
        # Find the last ``\n)`` that closes a top-level CTE — the segment AFTER
        # it is the outer combined SELECT.
        last_cte_close = sql.rfind("\n)")
        assert last_cte_close > cm_match.start(), (
            f"Could not locate end of CTE block:\n{sql}"
        )
        outer = sql[last_cte_close + 2:]
        assert "1000" in outer, (
            f"Filter literal must appear in the outer combined SELECT "
            f"(after all CTEs), not just inside a _cm_ CTE body. Outer:\n{outer}\n\nFull SQL:\n{sql}"
        )
        # And that outer comparison must be a WHERE, not a HAVING (the outer
        # SELECT is not aggregating; HAVING without GROUP BY is invalid SQL
        # on every supported dialect).
        outer_upper = outer.upper()
        assert "WHERE" in outer_upper, (
            f"Aggregate filter on isolated measure must route as outer WHERE:\n{outer}"
        )

    async def test_same_filtered_measure_different_aggs_separate_ctes(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """Same filtered measure with sum + avg must produce distinct _cm_ CTEs, not collide."""
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
        sql = await self._sql(claim_amount_model, related_models, query)

        # Both aliases must be present in the final SQL.
        assert "loss_payment_amt_sum" in sql, f"Missing loss_payment_amt_sum in:\n{sql}"
        assert "loss_payment_amt_avg" in sql, f"Missing loss_payment_amt_avg in:\n{sql}"
        # The two filtered measures must have distinct _cm_ CTE names (no collision).
        cm_cte_names = _re.findall(r"(_cm_\w+)\s+AS\s*\(", sql)
        assert len(cm_cte_names) == len(set(cm_cte_names)), (
            f"Duplicate _cm_ CTE names: {cm_cte_names}\n{sql}"
        )
        assert len(cm_cte_names) == 2, f"Expected 2 _cm_ CTEs, got {len(cm_cte_names)}: {cm_cte_names}"

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
        """When ALL first/last measures are isolated, the host _base CTE must NOT
        build a ranked subquery — it should be a plain dimension spine. The ranked
        subquery lives inside each filtered measure's own _cm_ CTE."""
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="latest_payment:last")],
            dimensions=[ColumnRef(name="claim.claim_number")],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
        )
        sql = await self._sql(claim_amount_model_with_time, related_models, query)

        # Extract the _base CTE body (balanced-paren walker — guards against
        # nested ranked subqueries in mixed-isolation cases).
        base_body = _extract_cte_body(sql, r"_base")

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
        """The isolated CTE for a last measure must contain a ROW_NUMBER ranked
        subquery and produce valid SQL — no reference to a rank column its FROM
        never projects."""
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="latest_payment:last")],
            dimensions=[ColumnRef(name="claim.claim_number")],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
        )
        sql = await self._sql(claim_amount_model_with_time, related_models, query)

        # The _cm_ CTE for the isolated measure must exist and contain ROW_NUMBER.
        # Balanced-paren walker — the body carries a nested ranked subquery
        # whose ``\n)`` closes the inner subquery, not the CTE.
        cm_body = _extract_cte_body(sql, r"_cm_\w*latest_payment\w*")

        assert "ROW_NUMBER" in cm_body, (
            f"the ranked CTE for latest_payment must contain ROW_NUMBER:\n{cm_body}"
        )
        assert "_ranked_rn" in cm_body, (
            f"the ranked CTE must project its rank column:\n{cm_body}"
        )
        assert "MAX(CASE WHEN _ranked_rn = 1" in _norm(cm_body), (
            f"the ranked CTE must pick rank 1 by aggregation:\n{cm_body}"
        )
        # Full SQL must parse as valid
        _assert_valid_sql(sql)

    async def test_mixed_isolated_and_local_first_last(
        self, generator: SQLGenerator, claim_amount_model_with_time, related_models,
    ) -> None:
        """Mixed case: an unfiltered ``last`` and a cross-model-filtered one.

        Both used to be treated differently — the unfiltered one wrapped the
        host base in a ranking, the filtered one got a ``_cm_`` CTE of its own —
        which is exactly the asymmetry B9 removes. They are two ranked
        aggregates and get one CTE each; ``_base`` ranks nothing."""
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
        sql = await self._sql(claim_amount_model_with_time, related_models, query)

        base_body = _extract_cte_body(sql, r"_base")
        assert "ROW_NUMBER" not in base_body, (
            f"the host base must hold no ranking of its own:\n{base_body}"
        )
        assert "total_amount" not in base_body, (
            f"a ranked measure must not be computed in _base:\n{base_body}"
        )
        for pattern in (r"_cm_\w*total_amount\w*", r"_cm_\w*latest_payment\w*"):
            cm_body = _extract_cte_body(sql, pattern)
            assert "ROW_NUMBER" in cm_body, (
                f"{pattern} must carry its own ranking:\n{cm_body}"
            )
        # Only the filtered one narrows its rows.
        assert "WHERE" not in _extract_cte_body(sql, r"_cm_\w*total_amount\w*"), sql
        assert "WHERE" in _extract_cte_body(sql, r"_cm_\w*latest_payment\w*"), sql

        _assert_valid_sql(sql)

    async def test_isolated_first_with_explicit_time_column(
        self, generator: SQLGenerator, claim_amount_model_with_time, related_models,
    ) -> None:
        """Isolated first measure with explicit time_column uses correct ordering."""
        # Add a timestamp dimension and measure for the explicit time column.
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
        sql = await self._sql(claim_amount_model_with_time, related_models, query)

        # The ranked CTE orders ASCENDING for ``first`` — the bare column,
        # since ascending is the absence of DESC.
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w*earliest_reserve\w*"))

        assert _re.search(
            r"ORDER BY claim_amount\.updated_at\s*\)", cm_body,
        ), (
            f"expected ascending ranking on the explicit time column:\n{cm_body}"
        )
        _assert_valid_sql(sql)

    async def test_multiple_isolated_first_last_separate_ctes(
        self, generator: SQLGenerator, claim_amount_model_with_time, related_models,
    ) -> None:
        """Two isolated first/last measures produce separate CTEs, no ROW_NUMBER
        in base."""
        # latest_payment already has cross-model filter; add another.
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
        sql = await self._sql(claim_amount_model_with_time, related_models, query)

        # No ROW_NUMBER in host _base when all first/last are isolated.
        base_body = _extract_cte_body(sql, r"_base")
        assert "ROW_NUMBER" not in base_body, (
            f"No ROW_NUMBER should be in base when all first/last are isolated:\n{base_body}"
        )

        # Two separate CTEs for the two filtered first/last measures.
        cm_cte_names = _re.findall(r"(_cm_\w+)\s+AS\s*\(", sql)
        filtered_names = [
            n for n in cm_cte_names if "latest_payment" in n or "latest_reserve" in n
        ]
        assert len(filtered_names) == 2, (
            f"Expected 2 filtered ranked CTEs, got {len(filtered_names)}: "
            f"{filtered_names}\n{sql}"
        )
        # Each should have ROW_NUMBER. Balanced-paren walker because each
        # ``_cm_*`` body wraps a ranked subquery.
        for cm_name in filtered_names:
            cm_body = _extract_cte_body(sql, _re.escape(cm_name))
            assert "ROW_NUMBER" in cm_body, (
                f"CTE {cm_name} must have ROW_NUMBER:\n{cm_body}"
            )

        _assert_valid_sql(sql)

    async def test_same_cm_measure_different_aggs_separate_ctes(self, generator: SQLGenerator) -> None:
        """Same cross-model measure with sum + avg must produce distinct CTEs."""
        customers = SlayerModel(
            name="customers", sql_table="Customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="revenue", sql="revenue", type=DataType.DOUBLE),
            ],
        )
        orders = SlayerModel(
            name="orders", sql_table="Orders", data_source="test",
            columns=[
                Column(name="order_id", sql="order_id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="order_id")],
            measures=[
                ModelMeasure(formula="customers.revenue:sum"),
                ModelMeasure(formula="customers.revenue:avg"),
            ],
        )
        sql = await _generate(generator, query, orders, extra_models=[customers])

        # Both aliases must be present
        assert "revenue_sum" in sql, f"Missing revenue_sum in:\n{sql}"
        assert "revenue_avg" in sql, f"Missing revenue_avg in:\n{sql}"
        # Two distinct CM CTE definitions
        cm_cte_names = _re.findall(r"(_cm_\w+)\s+AS\s*\(", sql)
        assert len(cm_cte_names) == 2, f"Expected 2 _cm_ CTEs, got {len(cm_cte_names)}: {cm_cte_names}\n{sql}"
        assert cm_cte_names[0] != cm_cte_names[1], f"CTE names collide: {cm_cte_names}\n{sql}"

    # --- DEV-1503 new invariants (post-Codex review) ---

    async def test_multi_hop_derived_filter_expands_inside_cm_cte(
        self, generator: SQLGenerator,
    ) -> None:
        """A ``Column.filter`` referencing a host DERIVED column whose own
        ``Column.sql`` crosses TWO join hops (``customers.regions.code``) must
        expand inside the isolated _cm_ CTE — both join hops must appear in
        the CTE body, never in the host _base spine.

        Pins Codex review #11 (multi-hop derived filter coverage).
        """
        regions = SlayerModel(
            name="regions", sql_table="Regions", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="code", sql="code", type=DataType.TEXT),
            ],
        )
        customers = SlayerModel(
            name="customers", sql_table="Customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        )
        orders = SlayerModel(
            name="orders", sql_table="Orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                # Derived column reaching `customers__regions.code` via a
                # 2-hop walk (Mode A uses ``__`` between hops + single dot
                # before the leaf).
                Column(name="region_code", sql="customers.regions.code", type=DataType.TEXT),
                # Filtered local measure: filter references the derived column.
                Column(
                    name="eu_amount", sql="amount", filter="region_code = 'EU'",
                    type=DataType.DOUBLE,
                ),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="eu_amount:sum")],
            dimensions=[ColumnRef(name="id")],
        )
        sql = await _generate(generator, query, orders, extra_models=[customers, regions])

        # The _cm_ CTE must exist for the filtered measure — balanced-paren
        # walker so a nested subquery doesn't truncate the body.
        cm_body = _extract_cte_body(sql, r"_cm_\w*eu_amount\w*")
        # Both join hops must be inside the _cm_ CTE — never in _base.
        assert "Customers" in cm_body, (
            f"Intermediate customers join missing from _cm_ CTE:\n{cm_body}"
        )
        assert "Regions" in cm_body, (
            f"Deeper regions join missing from _cm_ CTE:\n{cm_body}"
        )
        # The host _base CTE must NOT include the filter-target joins.
        base_body = _extract_cte_body(sql, r"_base")
        assert "Customers" not in base_body, (
            f"customers join leaked into host _base:\n{base_body}"
        )
        assert "Regions" not in base_body, (
            f"regions join leaked into host _base:\n{base_body}"
        )
        _assert_valid_sql(sql)

    async def test_aggregate_filter_outer_where_with_no_dimensions(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """Aggregate filter on an isolated measure, with NO dimensions: the
        host _base CROSS JOINs the _cm_ CTE, and the outer wrapper applies the
        filter as WHERE on the joined-back aggregate column.

        Pins Codex review #10 (no-dim outer-WHERE coverage).
        """
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum")],
            filters=["loss_payment_amt:sum > 1000"],
            # No dimensions.
        )
        sql = await self._sql(claim_amount_model, related_models, query)
        # CROSS JOIN must appear (no-dim case).
        assert "CROSS JOIN" in sql, f"Expected CROSS JOIN in no-dim case:\n{sql}"
        # Filter must apply via outer WHERE > 1000 — after all CTEs.
        last_cte_close = sql.rfind("\n)")
        assert last_cte_close > 0, f"No CTE block found:\n{sql}"
        outer = sql[last_cte_close + 2:]
        assert "1000" in outer, (
            f"Filter literal '1000' must apply in the outer combined SELECT:\n{outer}"
        )
        assert "WHERE" in outer.upper(), (
            f"Outer combined SELECT must carry a WHERE for the aggregate filter:\n{outer}"
        )
        _assert_valid_sql(sql)

    async def test_mixed_operand_aggregate_filter_promotes_hidden_operand(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """A filter that references BOTH an isolated filtered-local aggregate
        AND a non-isolated local aggregate must promote the non-isolated
        operand to a hidden column in the host _base so the outer WHERE can
        evaluate it. The non-isolated operand is NOT a public measure here —
        it must stay hidden from the final projection.

        Pins Codex review #4 (mixed filters need hidden operand projection)
        and #5 (strengthen: outer WHERE references the hidden operand AND it
        does not surface in public projection).
        """
        # total_amount is a non-isolated unfiltered local measure — referenced
        # only by the filter, NOT projected as a public measure.
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum")],
            dimensions=[ColumnRef(name="claim.claim_number")],
            filters=["loss_payment_amt:sum > 1000 and total_amount:sum > 10"],
        )
        sql = await self._sql(claim_amount_model, related_models, query)
        # Filter literals must both apply.
        assert "1000" in sql
        assert "10" in sql
        # An unfiltered SUM over the host amount column must appear in _base —
        # the hidden promotion of the filter-only operand.
        base_body = _extract_cte_body(sql, r"_base")
        assert _re.search(r"SUM\(\s*claim_amount\.amount\s*\)", base_body), (
            f"Expected hidden SUM(claim_amount.amount) for filter-only operand in _base:\n{base_body}"
        )
        # The outer combined SELECT (after all CTEs) must reference BOTH the
        # isolated aggregate alias (joined-back from _cm_) AND the hidden
        # operand alias from _base — that's the outer WHERE evaluating both.
        last_cte_close = sql.rfind("\n)")
        assert last_cte_close > 0
        outer = sql[last_cte_close + 2:]
        assert "loss_payment_amt_sum" in outer, (
            f"Outer must reference the isolated aggregate alias:\n{outer}"
        )
        # The hidden operand alias must be REFERENCED in the outer WHERE...
        assert "total_amount_sum" in outer, (
            f"Outer must reference the hidden filter operand alias 'total_amount_sum':\n{outer}"
        )
        # ...but it must NOT surface in the OUTER public projection. Extract
        # the public SELECT-list (between ``SELECT`` and ``FROM``) and assert
        # the hidden alias is absent. Use sqlglot to parse robustly.
        parsed = sqlglot.parse_one(sql, dialect="postgres")
        named_aliases = {
            sel.alias_or_name for sel in parsed.find(sqlglot.exp.Select).expressions
        }
        assert "claim_amount.total_amount_sum" not in named_aliases, (
            f"Hidden operand leaked into public projection: {named_aliases}"
        )
        _assert_valid_sql(sql)

    async def test_post_phase_filter_not_routed_to_outer_combined_where(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """A POST-phase filter (transform-wrapped, ``cumsum(...) > 0``) on an
        isolated filtered-local measure stays in the existing post-transform
        wrapper — it must NOT be re-routed to the outer combined WHERE.

        Pins Codex review #5 (POST-phase non-routing).
        """
        # Need a time dimension for cumsum (which needs ordering).
        claim_amount_model.default_time_dimension = "created_at"
        claim_amount_model.columns.append(
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        )
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[
                ModelMeasure(formula="loss_payment_amt:sum"),
                ModelMeasure(formula="cumsum(loss_payment_amt:sum)", name="cum_loss"),
            ],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
            filters=["cumsum(loss_payment_amt:sum) > 0"],
        )
        sql = await self._sql(claim_amount_model, related_models, query)
        # The filter must apply — '> 0' literal present.
        assert "> 0" in sql, f"POST filter '> 0' missing:\n{sql}"
        # Cumsum (window) must be present in the SQL.
        assert "SUM" in sql.upper() and "OVER" in sql.upper(), (
            f"Expected windowed SUM ... OVER (...) for cumsum:\n{sql}"
        )
        # Layer-boundary pin: the POST predicate lives in the ``_filtered``
        # outer wrap AFTER every CTE — not inside the ``base`` CTE (the
        # combined SELECT that feeds the transform step). Routing it into
        # ``base.WHERE`` would filter rows BEFORE the cumsum window saw
        # them, silently changing the cumulative semantics.
        base_body = _extract_cte_body(sql, r"\bbase\b")
        assert "> 0" not in base_body, (
            f"POST filter '> 0' leaked into the combined ``base`` CTE — "
            f"it must stay at the post-transform ``_filtered`` wrapper:"
            f"\n{base_body}"
        )
        # And the POST predicate IS in the outer ``_filtered`` wrap.
        filtered_match = _re.search(r"\)\s*AS\s+_filtered\s*WHERE\s+([^)]+)", sql)
        assert filtered_match, (
            f"Expected ``_filtered`` outer wrap with WHERE for POST filter:\n{sql}"
        )
        assert "> 0" in filtered_match.group(1), (
            f"POST filter '> 0' must apply at the ``_filtered`` outer wrap:"
            f"\n{filtered_match.group(0)}"
        )
        _assert_valid_sql(sql)

    async def test_aggregate_and_post_filters_route_independently(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """A single query carrying BOTH an AGGREGATE-phase host filter
        (``loss_payment_amt:sum > 1000``) AND a POST-phase host filter
        (``cumsum(loss_payment_amt:sum) > 0``) routes each independently:
        the aggregate filter to the outer combined WHERE wrapper; the POST
        filter to the existing post-transform wrapper. The two predicates
        live in DIFFERENT scopes — they must not collapse into one outer
        WHERE that references the cumsum column, nor merge into one HAVING.

        Pins Codex review #1 (POST vs AGGREGATE routing in same query).
        """
        claim_amount_model.default_time_dimension = "created_at"
        claim_amount_model.columns.append(
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        )
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[
                ModelMeasure(formula="loss_payment_amt:sum"),
                ModelMeasure(formula="cumsum(loss_payment_amt:sum)", name="cum_loss"),
            ],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
            filters=[
                "loss_payment_amt:sum > 1000",
                "cumsum(loss_payment_amt:sum) > 0",
            ],
        )
        sql = await self._sql(claim_amount_model, related_models, query)
        # Both literals must survive.
        assert "> 1000" in sql, f"AGGREGATE filter '> 1000' missing:\n{sql}"
        assert "> 0" in sql, f"POST filter '> 0' missing:\n{sql}"
        # Cumsum window must be present somewhere.
        assert "OVER" in sql.upper(), f"Expected windowed SUM ... OVER (...) for cumsum:\n{sql}"
        # Layer-boundary pin: AGGREGATE in the combined ``base`` CTE
        # WHERE; POST in the outer ``_filtered`` wrap; neither leaks into
        # the other layer.
        base_body = _extract_cte_body(sql, r"\bbase\b")
        assert "> 1000" in base_body, (
            f"AGGREGATE filter '> 1000' must apply in the combined "
            f"``base`` CTE WHERE:\n{base_body}"
        )
        assert "> 0" not in base_body, (
            f"POST filter '> 0' leaked into the combined ``base`` CTE — "
            f"it must stay at the post-transform ``_filtered`` wrapper:"
            f"\n{base_body}"
        )
        filtered_match = _re.search(r"\)\s*AS\s+_filtered\s*WHERE\s+([^)]+)", sql)
        assert filtered_match, (
            f"Expected ``_filtered`` outer wrap with WHERE for POST filter:\n{sql}"
        )
        filtered_where = filtered_match.group(1)
        assert "> 0" in filtered_where, (
            f"POST filter '> 0' must apply at the ``_filtered`` outer wrap:"
            f"\n{filtered_match.group(0)}"
        )
        assert "> 1000" not in filtered_where, (
            f"AGGREGATE filter '> 1000' leaked into the ``_filtered`` "
            f"outer wrap:\n{filtered_match.group(0)}"
        )
        _assert_valid_sql(sql)

    async def test_filter_referencing_two_isolated_aggregates(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """An aggregate-phase filter referencing TWO isolated filtered-local
        aggregates (``loss_payment_amt:sum + loss_reserve_amt:sum > 100``)
        must produce two _cm_ CTEs (one per measure) and an outer WHERE
        that references BOTH joined-back aliases.

        Pins Codex review #4 (two-isolated-aggregate filter routing).
        """
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[
                ModelMeasure(formula="loss_payment_amt:sum"),
                ModelMeasure(formula="loss_reserve_amt:sum"),
            ],
            dimensions=[ColumnRef(name="claim.claim_number")],
            filters=["loss_payment_amt:sum + loss_reserve_amt:sum > 100"],
        )
        sql = await self._sql(claim_amount_model, related_models, query)
        cm_cte_names = _re.findall(r"(_cm_\w+)\s+AS\s*\(", sql)
        assert len(cm_cte_names) >= 2, (
            f"Expected ≥ 2 _cm_ CTEs (one per filtered measure); got {cm_cte_names}\n{sql}"
        )
        # Outer must reference BOTH aggregate aliases in the filter predicate.
        last_cte_close = sql.rfind("\n)")
        outer = sql[last_cte_close + 2:]
        assert "loss_payment_amt_sum" in outer, (
            f"Outer must reference loss_payment_amt_sum:\n{outer}"
        )
        assert "loss_reserve_amt_sum" in outer, (
            f"Outer must reference loss_reserve_amt_sum:\n{outer}"
        )
        assert "100" in outer, f"Filter literal '100' must apply in outer:\n{outer}"
        assert "WHERE" in outer.upper(), (
            f"Outer combined SELECT must carry a WHERE for the aggregate filter:\n{outer}"
        )
        _assert_valid_sql(sql)

    async def test_filtered_local_parametric_last_in_order_by(
        self, generator: SQLGenerator, claim_amount_model_with_time, related_models,
    ) -> None:
        """A filtered-local PARAMETRIC ``last(updated_at)`` referenced from
        ORDER BY keeps its ranking INSIDE its own CTE, and the outer ORDER BY
        resolves through that CTE's joined-back column.

        Exercises the DEV-1501 × DEV-1503 interaction: the rank column is
        scoped to the CTE, so an outer sort term naming it directly would be
        unresolvable. Pins Codex review #8 — ORDER BY half.
        """
        claim_amount_model_with_time.columns.append(
            Column(name="updated_at", sql="updated_at", type=DataType.TIMESTAMP),
        )
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[
                ModelMeasure(formula="*:count"),
                ModelMeasure(formula="latest_payment:last(updated_at)", name="latest_pmt"),
            ],
            dimensions=[ColumnRef(name="claim.claim_number")],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
            order=[
                OrderItem(column="latest_pmt", direction="desc"),
            ],
        )
        sql = await self._sql(claim_amount_model_with_time, related_models, query)

        cm_body = _extract_cte_body(sql, r"_cm_\w*latest_payment\w*")
        assert "_ranked_rn" in cm_body, (
            f"the ranked CTE must carry its own rank column:\n{cm_body}"
        )
        # The window's ORDER BY must use updated_at (the explicit time arg),
        # not the default TD (created_at).
        assert "ORDER BY claim_amount.updated_at DESC" in _norm(cm_body), (
            f"the ranked CTE must rank by the explicit time arg:\n{cm_body}"
        )
        # The outer ORDER BY references the joined-back aggregate column —
        # NOT the rank column, which is scoped to the CTE.
        assert "_ranked_rn" not in sql[sql.rfind("ORDER BY"):], sql
        terms = _outer_order_terms(sql)
        assert any("latest_pmt" in expr or "latest_payment" in expr for expr, _ in terms), (
            f"Outer ORDER BY must reference the filtered-local aggregate alias; got {terms}\nSQL:\n{sql}"
        )
        _assert_valid_sql(sql)

    async def test_filtered_local_parametric_last_in_having(
        self, generator: SQLGenerator, claim_amount_model_with_time, related_models,
    ) -> None:
        """A filtered-local PARAMETRIC ``last(updated_at)`` referenced from a
        host filter routes as an AGGREGATE-phase filter to the outer combined
        WHERE. The ranking stays inside the CTE; the outer SELECT applies the
        comparison against the joined-back column.

        Pins Codex review #8 — HAVING half.
        """
        claim_amount_model_with_time.columns.append(
            Column(name="updated_at", sql="updated_at", type=DataType.TIMESTAMP),
        )
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="latest_payment:last(updated_at)", name="latest_pmt")],
            dimensions=[ColumnRef(name="claim.claim_number")],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
            filters=["latest_pmt > 500"],
        )
        sql = await self._sql(claim_amount_model_with_time, related_models, query)

        cm_body = _extract_cte_body(sql, r"_cm_\w*latest_payment\w*")
        assert "_ranked_rn" in cm_body, (
            f"the ranked CTE must contain its rank column:\n{cm_body}"
        )
        assert "ORDER BY claim_amount.updated_at DESC" in _norm(cm_body), (
            f"the ranked CTE must rank by the explicit time arg:\n{cm_body}"
        )
        # The aggregate-phase filter routes to the OUTER combined SELECT
        # wrapper (WHERE on the joined-back column), not inside the CTE
        # and not as HAVING.
        last_cte_close = sql.rfind("\n)")
        outer = sql[last_cte_close + 2:]
        assert "500" in outer, f"Filter literal '500' must apply in outer:\n{outer}"
        assert "WHERE" in outer.upper(), (
            f"Filter on parametric last filtered-local must route to outer WHERE:\n{outer}"
        )
        _assert_valid_sql(sql)

    async def test_outer_wrapper_resolves_forward_cross_model_agg_operand(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """A mixed AGGREGATE-phase filter referencing BOTH an isolated
        filtered-local aggregate AND a forward cross-model aggregate
        must resolve BOTH operands at the outer combined SELECT — the
        filtered-local through the host-rooted ``_cm_`` CTE, the forward
        cross-model through ITS own ``_cm_`` CTE.

        Pins CodeRabbit thread 2: the outer wrapper's slot-to-CTE map must
        cover EVERY cross-model aggregate plan (not just filtered-local
        ones). Without this, the forward cross-model operand falls
        through to the ``_base`` fallback and the renderer raises
        ``NotImplementedError`` because the slot doesn't materialise in
        ``_base``.
        """
        # ``loss_payment.has_flag:sum`` is a forward cross-model aggregate
        # on the joined ``loss_payment`` target — no Column.filter, so it
        # routes as a plain forward-path ``_cm_`` plan, not filtered-local.
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[
                ModelMeasure(formula="loss_payment_amt:sum"),
                ModelMeasure(formula="loss_payment.has_flag:sum"),
            ],
            dimensions=[ColumnRef(name="claim.claim_number")],
            filters=[
                "loss_payment_amt:sum + loss_payment.has_flag:sum > 100",
            ],
        )
        sql = await self._sql(claim_amount_model, related_models, query)
        # Two _cm_ CTEs: one filtered-local (host-rooted), one forward
        # cross-model.
        cm_cte_names = _re.findall(r"(_cm_\w+)\s+AS\s*\(", sql)
        assert len(cm_cte_names) >= 2, (
            f"Expected ≥ 2 _cm_ CTEs (filtered-local + forward); got {cm_cte_names}\n{sql}"
        )
        # Outer combined SELECT must carry the filter and reference both
        # aggregate aliases.
        last_cte_close = sql.rfind("\n)")
        outer = sql[last_cte_close + 2:]
        assert "100" in outer, (
            f"Filter literal '100' must apply in outer combined SELECT:\n{outer}"
        )
        assert "WHERE" in outer.upper(), (
            f"Outer combined SELECT must carry WHERE:\n{outer}"
        )
        assert "loss_payment_amt_sum" in outer, (
            f"Outer must reference filtered-local aggregate alias:\n{outer}"
        )
        assert "has_flag_sum" in outer, (
            f"Outer must reference forward cross-model aggregate alias:\n{outer}"
        )
        _assert_valid_sql(sql)

    async def test_aggregate_filter_with_date_range_routes_correctly(
        self, generator: SQLGenerator, claim_amount_model_with_time, related_models,
    ) -> None:
        """A query carrying BOTH a date_range time dimension AND an
        AGGREGATE-phase filter on the isolated aggregate must route each
        independently: the date_range as ROW-phase WHERE (propagated into
        the sub-plan / _cm_ CTE), the aggregate filter as outer WHERE on
        the joined-back column.

        Pins the Codex review finding: ``host_filter_routings`` is
        ``[date_range_routings..., user_filter_routings...]`` in order, so
        the planner must slice user routings as ``host_filters[-N:]`` —
        not look them up by ``f"f{i}"`` against ``host_query.filters[i]``
        (off-by-one when n_date_range > 0). Without the fix, the aggregate
        filter would be classified against the leading date_range routing
        (typically ROW phase) and double-applied (sub-plan HAVING +
        outer WHERE).
        """
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum")],
            dimensions=[ColumnRef(name="claim.claim_number")],
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                    date_range=("2020-01-01", "2021-01-01"),
                ),
            ],
            filters=["loss_payment_amt:sum > 1000"],
        )
        sql = await self._sql(claim_amount_model_with_time, related_models, query)
        # The date_range literal is a ROW-phase filter — must apply where
        # the row-set forms (inside the _cm_ CTE for the isolated agg).
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "2020-01-01" in cm_body, (
            f"date_range ROW filter must apply inside the _cm_ CTE:\n{cm_body}"
        )
        # The AGGREGATE filter must apply on the OUTER combined SELECT,
        # not inside the CTE (which would surface host rows as NULL via
        # LEFT JOIN instead of dropping them).
        last_cte_close = sql.rfind("\n)")
        outer = sql[last_cte_close + 2:]
        assert "1000" in outer, (
            f"Aggregate filter '> 1000' must apply in outer combined SELECT:\n{outer}"
        )
        assert "WHERE" in outer.upper(), (
            f"Outer combined SELECT must carry a WHERE for the aggregate filter:\n{outer}"
        )
        # Double-application check: the '> 1000' literal must NOT also
        # appear inside the _cm_ CTE body (which would mean the planner
        # mis-routed the aggregate filter as ROW phase under the
        # date_range off-by-one bug).
        assert "1000" not in cm_body, (
            f"Aggregate filter '> 1000' leaked into _cm_ CTE body "
            f"(off-by-one with date_range):\n{cm_body}"
        )
        _assert_valid_sql(sql)

    async def test_order_by_projected_composite_over_isolated_resolves_at_combined(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """A query whose ORDER BY references a PROJECTED composite over
        isolated filtered-local aggregates must resolve the order alias at
        the combined SELECT level — NOT ``_base."<alias>"`` (the alias is
        no longer materialised in ``_base`` after DEV-1503 routing).

        Pins Codex round 3 #2 (ORDER BY on projected outer-routed composite).
        """
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[
                ModelMeasure(formula="loss_payment_amt:sum"),
                ModelMeasure(formula="loss_reserve_amt:sum"),
                ModelMeasure(
                    formula="loss_payment_amt:sum + loss_reserve_amt:sum",
                    name="total_loss",
                ),
            ],
            dimensions=[ColumnRef(name="claim.claim_number")],
            order=[OrderItem(column="total_loss", direction="desc")],
        )
        sql = await self._sql(claim_amount_model, related_models, query)
        # The composite must NOT render in _base (G1.1 invariant).
        base_body = _extract_cte_body(sql, r"_base")
        assert "Loss_Payment" not in base_body
        assert "Loss_Reserve" not in base_body
        # ORDER BY must use the bare combined alias, NOT _base."<alias>".
        order_match = _re.search(r"ORDER BY\s+[^\n]+", sql)
        assert order_match, f"Expected ORDER BY in:\n{sql}"
        order_clause = order_match.group(0)
        assert "total_loss" in order_clause, (
            f"ORDER BY must reference total_loss:\n{order_clause}"
        )
        assert "_base." not in order_clause, (
            f"ORDER BY must NOT reference _base alias for an outer-routed "
            f"composite (alias not materialised in _base):\n{order_clause}"
        )
        _assert_valid_sql(sql)

    async def test_outer_composite_with_multiple_user_aliases(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """The same composite formula declared under TWO user-aliases must
        surface BOTH aliases in the combined SELECT. The C13 multi-alias
        same-key consolidation collapses the two measures into ONE slot
        with ``public_aliases=[a, b]``; the renderer must cycle through
        them rather than emit ``public_aliases[0]`` twice.

        Pins CodeRabbit thread on outer-composite alias cycling.
        """
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[
                ModelMeasure(formula="loss_payment_amt:sum"),
                ModelMeasure(formula="loss_reserve_amt:sum"),
                ModelMeasure(
                    formula="loss_payment_amt:sum + loss_reserve_amt:sum",
                    name="total_loss",
                ),
                ModelMeasure(
                    formula="loss_payment_amt:sum + loss_reserve_amt:sum",
                    name="grand_total",
                ),
            ],
            dimensions=[ColumnRef(name="claim.claim_number")],
        )
        sql = await self._sql(claim_amount_model, related_models, query)
        # The isolation invariant still holds (neither join in _base).
        base_body = _extract_cte_body(sql, r"_base")
        assert "Loss_Payment" not in base_body
        assert "Loss_Reserve" not in base_body
        # Both user-declared aliases must surface in the outer SELECT.
        last_cte_close = sql.rfind("\n)")
        outer = sql[last_cte_close + 2:]
        assert "total_loss" in outer, (
            f"Outer must reference total_loss alias:\n{outer}"
        )
        assert "grand_total" in outer, (
            f"Outer must reference grand_total alias (C13 cycling):\n{outer}"
        )
        _assert_valid_sql(sql)

    async def test_local_first_last_with_routed_cross_model_filter(
        self, generator: SQLGenerator, claim_amount_model_with_time, related_models,
    ) -> None:
        """A query mixing a LOCAL first/last measure with a cross-model
        aggregate carrying a routed filter must NOT apply the routed filter
        inside the host ``_base`` ranked subquery — it belongs in the
        ``_cm_*`` CTE only.

        Pins Codex round 5 finding: ``_build_first_last_base_select`` must
        thread ``skip_filter_ids`` so a filter classified as
        ``PROPAGATE_HAVING`` on a forward cross-model plan doesn't also
        render inside the host ranked subquery's WHERE — that would
        double-apply (and, when the filter references the cross-model
        join path not in ``_base``, emit invalid SQL).
        """
        # ``total_amount`` is an unfiltered local measure; first() forces
        # _base into the ranked-subquery branch. ``loss_payment.has_flag``
        # is a forward cross-model aggregate. The filter
        # ``loss_payment.has_flag:sum > 0`` classifies as PROPAGATE_HAVING
        # on the cross-model plan and is routed there.
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[
                ModelMeasure(formula="total_amount:last"),
                ModelMeasure(formula="loss_payment.has_flag:sum"),
            ],
            dimensions=[ColumnRef(name="claim.claim_number")],
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
            filters=["loss_payment.has_flag:sum > 0"],
        )
        sql = await self._sql(
            claim_amount_model_with_time, related_models, query,
        )
        # The routed filter literal must appear in the cross-model CTE's
        # HAVING.
        cm_body = _extract_cte_body(
            sql, r"_cm_\w*has_flag\w*",
        )
        assert "> 0" in cm_body, (
            f"Routed cross-model HAVING filter missing from _cm_ CTE:\n{cm_body}"
        )
        # And it must NOT appear inside the host ``_base`` ranked
        # subquery (double-application bug).
        base_body = _extract_cte_body(sql, r"_base")
        assert "> 0" not in base_body, (
            f"Routed cross-model filter leaked into host _base ranked subquery "
            f"(skip_filter_ids not threaded into _build_first_last_base_select):"
            f"\n{base_body}"
        )
        _assert_valid_sql(sql)

    async def test_no_dim_query_with_host_row_filter_applies_in_base(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """A no-dimension query with cross-model aggregates AND a
        host-local ROW filter must apply the filter at the placeholder
        ``_base`` (via ``WHERE`` + ``LIMIT 1``). The round-2 fix that
        dropped the host ``FROM`` to avoid N-row CROSS JOIN duplication
        also bypassed ``_build_where_having_from_planned``, silently
        ignoring host-local ROW filters (Codex round 4 / CodeRabbit).

        With this fix: ``_base`` reintroduces ``FROM <host>`` when a
        non-routed ROW filter exists; ``WHERE`` applies it; ``LIMIT 1``
        keeps cardinality at 1. If the filter drops every host row, the
        combined query returns 0 rows (correct semantics) — not the
        unfiltered aggregate value (broken semantics).
        """
        # Add a host column the filter references.
        claim_amount_model.columns.append(
            Column(name="status", sql="status", type=DataType.TEXT),
        )
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum")],
            filters=["status = 'active'"],
            # No dimensions — triggers the empty_base placeholder path.
        )
        sql = await self._sql(claim_amount_model, related_models, query)
        # The filter literal MUST appear in the SQL.
        assert "'active'" in sql, (
            f"Host ROW filter literal silently dropped:\n{sql}"
        )
        # The ``_base`` placeholder must carry the filter as WHERE plus
        # LIMIT 1 (the 1-row cardinality preservation from round 2).
        base_body = _extract_cte_body(sql, r"_base")
        assert "'active'" in base_body, (
            f"Host ROW filter must apply at _base WHERE:\n{base_body}"
        )
        assert "LIMIT 1" in base_body.upper(), (
            f"_base must LIMIT 1 to preserve no-dim cardinality:\n{base_body}"
        )
        _assert_valid_sql(sql)

    async def test_no_dim_host_filter_referencing_joined_column_pulls_join(
        self, generator: SQLGenerator, claim_amount_model, related_models,
    ) -> None:
        """A no-dimension query with a host ROW filter that references a
        JOINED column (``claim.claim_number = '...'``) must pull the
        join into the placeholder ``_base`` FROM clause — the WHERE
        otherwise references an undefined alias.

        Pins Codex round 6: ``_collect_filter_join_paths`` +
        ``_build_from_and_joins`` are wired through the empty_base
        placeholder branch so joined filter aliases have their joins in
        scope.
        """
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum")],
            filters=["claim.claim_number = '12345'"],
        )
        sql = await self._sql(claim_amount_model, related_models, query)
        base_body = _extract_cte_body(sql, r"_base")
        # The Claim join MUST appear in _base alongside the filter.
        assert "Claim" in base_body, (
            f"_base must include Claim join for the WHERE alias:\n{base_body}"
        )
        # WHERE still applies + LIMIT 1 still preserves cardinality.
        assert "claim.claim_number" in base_body, (
            f"_base WHERE must reference the joined alias:\n{base_body}"
        )
        assert "12345" in base_body
        assert "LIMIT 1" in base_body.upper()
        _assert_valid_sql(sql)

    async def test_first_last_with_host_filter_not_reapplied_outside_ranked_subquery(
        self, generator: SQLGenerator, claim_amount_model_with_time, related_models,
    ) -> None:
        """A query mixing a LOCAL first/last measure and a host ROW
        filter must apply the filter only INSIDE the ranked subquery
        (pre-ranking), not again on the outer ``_base`` SELECT. The
        first/last branch returns ``where_consumed=True``; the cross-
        model orchestrator must honour that and skip the outer WHERE
        application, otherwise the filter double-applies and changes
        first/last semantics by filtering AFTER ranking.

        Pins CodeRabbit thread on ``_base_where_consumed``.
        """
        claim_amount_model_with_time.columns.append(
            Column(name="status", sql="status", type=DataType.TEXT),
        )
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[
                ModelMeasure(formula="total_amount:last"),
                ModelMeasure(formula="loss_payment.has_flag:sum"),
            ],
            dimensions=[ColumnRef(name="claim.claim_number")],
            time_dimensions=[
                TimeDimension(
                    dimension=ColumnRef(name="created_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ],
            filters=["status = 'active'"],
        )
        sql = await self._sql(
            claim_amount_model_with_time, related_models, query,
        )
        # The filter literal must appear exactly ONCE in _base — inside
        # the ranked subquery's WHERE. A second occurrence (outside the
        # subquery on the wrapping _base SELECT) means base_where was
        # re-applied even though where_consumed signalled otherwise.
        base_body = _extract_cte_body(sql, r"_base")
        assert base_body.count("'active'") == 1, (
            f"Host filter literal must appear exactly once (inside the "
            f"ranked subquery's WHERE); got {base_body.count(chr(39) + 'active' + chr(39))} occurrences:"
            f"\n{base_body}"
        )
        _assert_valid_sql(sql)

    async def test_filtered_local_in_source_queries_smoke(
        self, generator: SQLGenerator,
    ) -> None:
        """A query-backed model containing a filtered-local measure must
        compile cleanly — the host-rooted _cm_ CTE renders against the
        virtual (sql-mode) host without tripping ``_build_from_clause_from_planned``.

        Pins Codex review #12 (source_queries coverage).
        """
        loss_payment = SlayerModel(
            name="loss_payment", sql_table="Loss_Payment", data_source="test",
            columns=[
                Column(name="claim_amount_id", sql="Claim_Amount_Identifier", type=DataType.DOUBLE, primary_key=True),
                Column(name="has_flag", sql="1", type=DataType.DOUBLE),
            ],
        )
        # Auxiliary base model the subquery host references; the
        # filtered-local CTE root path must not trip on a non-sql_table
        # host shape.
        backing_raw = SlayerModel(
            name="claim_amount_raw", sql_table="Claim_Amount", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        # Use a model with `sql` (subquery) directly — simpler than source_queries
        # plumbing, but still exercises the non-sql_table host-rooted CTE path.
        host_via_subquery = SlayerModel(
            name="claim_amount", sql="SELECT * FROM Claim_Amount", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(
                    name="loss_payment_amt", sql="amount",
                    filter="loss_payment.has_flag = 1", type=DataType.DOUBLE,
                ),
            ],
            joins=[ModelJoin(
                target_model="loss_payment",
                join_pairs=[["id", "claim_amount_id"]],
                join_type="inner",
            )],
        )
        query = SlayerQuery(
            source_model="claim_amount",
            measures=[ModelMeasure(formula="loss_payment_amt:sum")],
            dimensions=[ColumnRef(name="id")],
        )
        sql = await _engine_generate(
            query=query, model=host_via_subquery,
            extra_models=[loss_payment, backing_raw],
            validate=False,
        )
        # Filtered measure isolated into its own _cm_ CTE; the subquery FROM
        # for the host renders inside it.
        assert "_cm_" in sql and "loss_payment_amt" in sql
        # Host's ``sql=...`` subquery body renders inside the _cm_ CTE — sqlglot
        # may pretty-print across multiple lines, so check whitespace-tolerantly.
        # DEV-1645 (landed early in DEV-1706 Stage 2): the mixed-case physical
        # table ``Claim_Amount`` is now quoted on emit (``FROM "Claim_Amount"``).
        sql_collapsed = _re.sub(r"\s+", " ", sql)
        assert 'SELECT * FROM "Claim_Amount"' in sql_collapsed, (
            f"host subquery FROM should render inside the _cm_ CTE:\n{sql}"
        )
        _assert_valid_sql(sql)


class TestGetColumnTypesSql:
    """get_column_types must build valid SQL for expression measures."""

    async def test_expression_measure_sql_not_corrupted(self) -> None:
        """Expression measures like COALESCE(amount, 0) must not get model.name prepended."""


        storage = YAMLStorage(base_dir=tempfile.mkdtemp())
        await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
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
            # Real attribute, not an auto-Mock: it feeds the cache key's
            # credential digest, which hashes it.
            mock_ds.credentials_json = None
            with patch.object(engine, "_resolve_datasource", new_callable=AsyncMock, return_value=mock_ds):
                captured_sql = []

                async def capture_sql(sql):
                    captured_sql.append(sql)
                    return {}

                mock_client = MagicMock()
                mock_client.get_column_types = capture_sql
                engine._sql_clients[("sqlite://", "", "")] = mock_client

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
        """Cross-model-derived columns are included in the type probe and mapped
        back to their bare column names. ``customer_score`` is a local column
        whose SQL reaches the joined ``customers`` model; the typed probe
        pipeline must still build a ``customer_score:max`` aggregate aliased
        ``orders.customer_score_max`` and surface its type under the bare name."""
        storage = YAMLStorage(base_dir=tempfile.mkdtemp())
        await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
        await storage.save_model(SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="score", sql="score", type=DataType.DOUBLE),
            ],
        ))
        await storage.save_model(SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
                Column(name="customer_score", sql="customers.score", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        ))

        engine = SlayerQueryEngine(storage=storage)
        mock_ds = MagicMock()
        mock_ds.get_connection_string.return_value = "sqlite://"
        mock_ds.type = "sqlite"

        # Real attribute, not an auto-Mock: it feeds the cache key's
        # credential digest (DEV-1755), which hashes it.
        mock_ds.credentials_json = None
        with patch.object(engine, "_resolve_datasource", new_callable=AsyncMock, return_value=mock_ds):
            async def capture_types(sql):
                return {"orders.revenue_max": "number", "orders.customer_score_max": "number"}

            mock_client = MagicMock()
            mock_client.get_column_types = capture_types
            engine._sql_clients[("sqlite://", "", "")] = mock_client

            result = await engine.get_column_types("orders")

        # Both measures should have types (cross-model-derived column included)
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


# DEV-1337: per-alias allowlists for "natively supports single-arg log10/log2"
# rendering. Outside these sets the current 2-arg LOG(base, x) form is kept.
# Mirrored in slayer/sql/generator.py — keep in sync.
_LOG10_NATIVE_DIALECTS = frozenset({
    "sqlite", "postgres", "duckdb", "mysql", "clickhouse",
    "snowflake", "bigquery", "redshift",
    "trino", "presto", "databricks", "spark", "tsql",
})
_LOG2_NATIVE_DIALECTS = frozenset({
    "sqlite", "postgres", "duckdb", "mysql", "clickhouse",
    "bigquery", "trino", "presto", "databricks", "spark",
})


class TestLogAliasPreservation:
    """DEV-1337 — user-written ``log10(x)`` / ``log2(x)`` must round-trip
    verbatim in emitted SQL on dialects that natively support those single-arg
    aliases. sqlglot's default behaviour normalises both into a generic
    ``Log(this=Literal(base), expression=arg)`` AST node and re-emits as
    ``LOG(base, x)`` for almost every dialect, which makes generated SQL
    diverge from the recipe formula text and (on dialects that lack 2-arg
    ``LOG``) can break a previously working call.
    """

    @pytest.fixture
    def log_model(self) -> SlayerModel:
        return SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                # Scalar log expressions inside Column.sql — the primary
                # path the issue surfaced through.
                Column(name="log_amount", sql="log10(amount)", type=DataType.DOUBLE),
                Column(name="log2_amount", sql="log2(amount)", type=DataType.DOUBLE),
                # Negative-control: a non-alias literal base. Must keep the
                # standard 2-arg LOG(base, x) form post-fix.
                Column(name="log3_amount", sql="log(3, amount)", type=DataType.DOUBLE),
                # ln(...) is a separate AST node (exp.Ln); the rewrite must
                # not touch it.
                Column(name="ln_amount", sql="ln(amount)", type=DataType.DOUBLE),
            ],
        )

    @pytest.mark.parametrize("dialect", TestMultiDialectGeneration.ALL_DIALECTS)
    async def test_log10_in_column_sql_is_preserved(
        self, dialect: str, log_model: SlayerModel,
    ) -> None:
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="log_amount:max")],
        )
        sql = await _generate(generator=gen, query=query, model=log_model)
        upper_no_ws = "".join(sql.upper().split())
        if dialect in _LOG10_NATIVE_DIALECTS:
            assert "LOG10(ORDERS.AMOUNT)" in upper_no_ws, (
                f"{dialect}: expected literal LOG10(amount), got:\n{sql}"
            )
            # Must not have canonicalised to either arg-order 2-arg form.
            assert "LOG(10,ORDERS.AMOUNT)" not in upper_no_ws, (
                f"{dialect}: should not canonicalise to LOG(10, amount):\n{sql}"
            )
            assert "LOG(ORDERS.AMOUNT,10)" not in upper_no_ws, (
                f"{dialect}: should not canonicalise to LOG(amount, 10):\n{sql}"
            )
        else:
            # Fallback: current 2-arg LOG behaviour is preserved on dialects
            # without native single-arg log10 (oracle).
            assert "LOG(10,ORDERS.AMOUNT)" in upper_no_ws or "LOG(ORDERS.AMOUNT,10)" in upper_no_ws, (
                f"{dialect}: expected fallback LOG(base,x) form, got:\n{sql}"
            )

    @pytest.mark.parametrize("dialect", TestMultiDialectGeneration.ALL_DIALECTS)
    async def test_log2_in_column_sql_is_preserved(
        self, dialect: str, log_model: SlayerModel,
    ) -> None:
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="log2_amount:max")],
        )
        sql = await _generate(generator=gen, query=query, model=log_model)
        upper_no_ws = "".join(sql.upper().split())
        if dialect in _LOG2_NATIVE_DIALECTS:
            assert "LOG2(ORDERS.AMOUNT)" in upper_no_ws, (
                f"{dialect}: expected literal LOG2(amount), got:\n{sql}"
            )
            assert "LOG(2,ORDERS.AMOUNT)" not in upper_no_ws, (
                f"{dialect}: should not canonicalise to LOG(2, amount):\n{sql}"
            )
            assert "LOG(ORDERS.AMOUNT,2)" not in upper_no_ws, (
                f"{dialect}: should not canonicalise to LOG(amount, 2):\n{sql}"
            )
        else:
            # Fallback for tsql / oracle / redshift / snowflake (no native LOG2).
            assert "LOG(2,ORDERS.AMOUNT)" in upper_no_ws or "LOG(ORDERS.AMOUNT,2)" in upper_no_ws, (
                f"{dialect}: expected fallback LOG(base,x) form, got:\n{sql}"
            )

    @pytest.mark.parametrize("dialect", sorted(_LOG10_NATIVE_DIALECTS))
    async def test_log10_inside_filtered_column_survives_reparse(
        self, dialect: str,
    ) -> None:
        """A filtered Column (``Column.filter="..."``) wraps the resolved
        value in ``CASE WHEN ... THEN ... END`` and re-parses through
        sqlglot. The log-alias rewrite must survive that round-trip — a
        re-parse of ``LOG10(amount)`` would otherwise canonicalise back to
        a generic ``Log`` node and re-emit as ``LOG(10, amount)``.
        """
        model = SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(
                    name="log_completed_amount",
                    sql="log10(amount)",
                    filter="status = 'completed'",
                    type=DataType.DOUBLE,
                ),
            ],
        )
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="log_completed_amount:max")],
        )
        sql = await _generate(generator=gen, query=query, model=model)
        upper_no_ws = "".join(sql.upper().split())
        assert "LOG10(ORDERS.AMOUNT)" in upper_no_ws, (
            f"{dialect}: expected literal log10(amount) inside filtered "
            f"column wrapper, got:\n{sql}"
        )
        assert "LOG(10,ORDERS.AMOUNT)" not in upper_no_ws, (
            f"{dialect}: filtered-column re-parse must not re-canonicalise "
            f"to LOG(10, amount):\n{sql}"
        )

    @pytest.mark.parametrize("dialect", sorted(_LOG10_NATIVE_DIALECTS))
    async def test_log10_in_arithmetic_measure_is_preserved(
        self, dialect: str, log_model: SlayerModel,
    ) -> None:
        """Arithmetic that mixes a log10 column-derived measure with COUNT(*).
        Pins that the rewrite survives the arithmetic enrichment path.
        """
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="log_amount:max / *:count", name="ratio")],
        )
        sql = await _generate(generator=gen, query=query, model=log_model)
        upper_no_ws = "".join(sql.upper().split())
        assert "LOG10(ORDERS.AMOUNT)" in upper_no_ws, (
            f"{dialect}: expected log10(amount) inside arithmetic measure:\n{sql}"
        )
        assert "COUNT(" in sql.upper(), f"COUNT(*) leg missing on {dialect}:\n{sql}"

    @pytest.mark.parametrize("dialect", TestMultiDialectGeneration.ALL_DIALECTS)
    async def test_log_with_non_alias_base_unchanged(
        self, dialect: str, log_model: SlayerModel,
    ) -> None:
        """Negative test: ``log(3, amount)`` (literal base ≠ 10/2) must keep
        the standard 2-arg form. The rewrite is scoped to bases 10 and 2 only.
        """
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="log3_amount:max")],
        )
        sql = await _generate(generator=gen, query=query, model=log_model)
        upper_no_ws = "".join(sql.upper().split())
        # Must NOT have invented a single-arg LOG3(...) function.
        assert "LOG3(" not in upper_no_ws, (
            f"{dialect}: must not invent LOG3() — only base 10 and 2 are aliased:\n{sql}"
        )
        # The 2-arg form must remain in some arg order.
        assert "LOG(3,ORDERS.AMOUNT)" in upper_no_ws or "LOG(ORDERS.AMOUNT,3)" in upper_no_ws, (
            f"{dialect}: expected 2-arg LOG(3, amount) preserved, got:\n{sql}"
        )

    @pytest.mark.parametrize("dialect", TestMultiDialectGeneration.ALL_DIALECTS)
    async def test_ln_unchanged(
        self, dialect: str, log_model: SlayerModel,
    ) -> None:
        """``ln(x)`` lives under a separate sqlglot AST node (``exp.Ln``);
        the rewrite must not affect it.
        """
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="ln_amount:max")],
        )
        sql = await _generate(generator=gen, query=query, model=log_model)
        # T-SQL has no LN — sqlglot transpiles to LOG(x). Every other dialect
        # keeps LN(...). We only assert the rewrite did not invent something.
        assert "LN10(" not in sql.upper()
        assert "LN2(" not in sql.upper()


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


class TestCastEmissionOpaqueType:
    """``CAST(x AS UNKNOWN)`` is not valid SQL in any dialect, so opaque types
    are skipped by ``_wrap_cast_for_type`` exactly like TEXT/None."""

    def test_unknown_returns_expression_unchanged(self) -> None:
        expr = sqlglot.parse_one("json_extract(blob, '$.x')")
        assert _wrap_cast_for_type(expr, DataType.UNKNOWN) is expr

    def test_operable_type_still_casts(self) -> None:
        expr = sqlglot.parse_one("json_extract(blob, '$.x')")
        wrapped = _wrap_cast_for_type(expr, DataType.DOUBLE)
        assert isinstance(wrapped, sqlglot.exp.Cast)

    async def test_opaque_column_rejected_as_dimension(self) -> None:
        """An opaque column has no equality operator, so grouping by it would
        emit SQL the database refuses. Reject it up front with an actionable
        message rather than letting a raw driver error surface."""
        model = SlayerModel(
            name="places",
            sql_table="public.places",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(
                    name="loc",
                    sql="coalesce(loc, home_loc)",
                    type=DataType.UNKNOWN,
                    db_type="point",
                ),
            ],
        )
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(source_model="places", dimensions=[ColumnRef(name="loc")])
        with pytest.raises(ValueError, match="cannot be used as a dimension"):
            await _generate(gen, query, model)

    async def test_opaque_column_emits_no_cast_when_projected(self) -> None:
        """The CAST guard still holds for a query that doesn't group by the
        opaque column: no ``CAST(... AS UNKNOWN)`` may reach the SQL."""
        model = SlayerModel(
            name="places",
            sql_table="public.places",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="city", sql="city", type=DataType.TEXT),
                Column(
                    name="loc",
                    sql="coalesce(loc, home_loc)",
                    type=DataType.UNKNOWN,
                    db_type="point",
                ),
            ],
        )
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(
            source_model="places",
            measures=["*:count"],
            dimensions=[ColumnRef(name="city")],
        )
        sql = await _generate(gen, query, model)
        # No opaque cast reaches the SQL (the docstring's real intent). The typed
        # pipeline still legitimately emits CAST(COUNT(*) AS INT) for the count
        # measure (DEV-1361/DEV-1484 — see TestCastEmissionMeasure), so assert the
        # absence of UNKNOWN entirely (which subsumes "no AS UNKNOWN cast") rather
        # than banning CAST( altogether.
        assert "UNKNOWN" not in sql.upper()

    async def test_opaque_column_allowed_as_raw_row_projection(self) -> None:
        """Raw-row mode (``distinct_dimension_values=False``, no measures,
        DEV-1543) emits no top-level GROUP BY, so projecting an opaque column
        as a "dimension" is legal — only grouping is refused. Regression guard:
        the opaque-dimension check must NOT fire when the query doesn't group."""
        model = SlayerModel(
            name="places",
            sql_table="public.places",
            data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="loc", type=DataType.UNKNOWN, db_type="point"),
            ],
        )
        gen = SQLGenerator(dialect="postgres")
        query = SlayerQuery(
            source_model="places",
            dimensions=[ColumnRef(name="loc")],
            distinct_dimension_values=False,
        )
        sql = await _generate(gen, query, model)
        assert "GROUP BY" not in sql.upper()
        assert "PLACES.LOC" in sql.upper()


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

    async def test_measure_type_none_count_casts_to_integer(self, orders_model) -> None:
        gen = SQLGenerator(dialect="sqlite")
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count", name="cnt")],  # type=None default
        )
        sql = await _generate(gen, query, orders_model)
        # ``*:count`` carries an implicit INT result type, so the typed
        # pipeline wraps it in CAST(... AS INTEGER) even when
        # ``ModelMeasure.type`` is unset (DEV-1484: confirmed intended).
        assert "CAST(COUNT(*) AS INTEGER)" in sql.upper()

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
        # DEV-1835: unified ``_cm_`` prefix, named after the canonical aggregate.
        assert "_cm_orders_for_window__revenue_sum_window_90d" in sql
        # CAST(SUM(...) AS DOUBLE) shape inside the windowed CTE.
        norm = _norm(sql).upper()
        assert "CAST(SUM(" in norm or "CAST (SUM(" in norm
        assert "DOUBLE" in norm

    async def test_windowed_sum_no_measure_type_casts_inferred_type(
        self, orders_model_for_window: SlayerModel,
    ) -> None:
        """DEV-1714 decision: with no EXPLICIT measure type, the windowed
        aggregation still CASTs on the type inferred from the source column
        (``revenue`` is ``DOUBLE``) — exactly as a plain aggregate does in the
        base path (DEV-1361). A windowed sum and a plain sum of the same no-type
        measure must emit identical CAST behavior; the legacy explicit-only
        distinction no longer exists on the typed pipeline."""
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
        assert "_cm_orders_for_window2__revenue_sum_window_90d" in sql
        norm = _norm(sql).upper()
        # Inferred DOUBLE type drives a CAST around SUM(_src._w_value).
        assert "CAST(SUM(" in norm or "CAST (SUM(" in norm
        assert "DOUBLE" in norm

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


class TestStringHygieneDialectTranslation:
    """DEV-1378: lowercase string-hygiene operators are pass-through to
    the emitted SQL string, then re-parsed by sqlglot under the target
    dialect at WHERE-assembly. sqlglot's per-dialect emitter chooses
    each dialect's preferred spelling. These tests pin the actual
    emitted SQL across SQLite / Postgres / MySQL / DuckDB / ClickHouse
    so a future sqlglot upgrade that changes the spelling is caught.
    """

    @pytest.mark.parametrize(
        "dialect,expected",
        [
            ("sqlite", "LOWER(orders.status) = 'active'"),
            ("postgres", "LOWER(orders.status) = 'active'"),
            ("mysql", "LOWER(orders.status) = 'active'"),
            ("duckdb", "LOWER(orders.status) = 'active'"),
            ("clickhouse", "LOWER(orders.status) = 'active'"),
        ],
    )
    async def test_lower(self, orders_model: SlayerModel, dialect: str, expected: str) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["lower(status) = 'active'"],
        )
        sql = await _generate(
            generator=SQLGenerator(dialect=dialect),
            query=query,
            model=orders_model,
        )
        assert expected in sql, f"{dialect}: {expected!r} not in {sql!r}"

    @pytest.mark.parametrize(
        "dialect,expected",
        [
            ("sqlite", "INSTR(orders.status, ',')"),
            ("postgres", "POSITION(',' IN orders.status)"),
            ("mysql", "LOCATE(',', orders.status)"),
            ("duckdb", "STRPOS(orders.status, ',')"),
            ("clickhouse", "POSITION(orders.status, ',')"),
        ],
    )
    async def test_instr_translates_per_dialect(
        self, orders_model: SlayerModel, dialect: str, expected: str,
    ) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["instr(status, ',') > 0"],
        )
        sql = await _generate(
            generator=SQLGenerator(dialect=dialect),
            query=query,
            model=orders_model,
        )
        assert expected in sql, f"{dialect}: {expected!r} not in {sql!r}"

    @pytest.mark.parametrize(
        "dialect,expected",
        [
            ("sqlite", "SUBSTRING(orders.status, 1, 5)"),
            ("postgres", "SUBSTRING(orders.status FROM 1 FOR 5)"),
            ("mysql", "SUBSTRING(orders.status, 1, 5)"),
            ("duckdb", "SUBSTRING(orders.status, 1, 5)"),
            ("clickhouse", "SUBSTRING(orders.status, 1, 5)"),
        ],
    )
    async def test_substr_translates_per_dialect(
        self, orders_model: SlayerModel, dialect: str, expected: str,
    ) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["substr(status, 1, 5) = 'abcde'"],
        )
        sql = await _generate(
            generator=SQLGenerator(dialect=dialect),
            query=query,
            model=orders_model,
        )
        assert expected in sql, f"{dialect}: {expected!r} not in {sql!r}"

    @pytest.mark.parametrize(
        "dialect,expected_substring",
        [
            # Every dialect whose sqlglot emitter prefers the operator now
            # renders ``||``: the unified ScalarCall policy builds a typed
            # ``exp.Concat`` instead of passing ``CONCAT`` through literally.
            # On Postgres this is a SEMANTIC change as well as a spelling one —
            # ``CONCAT()`` ignores NULL operands, ``||`` propagates them — and
            # it aligns filters with the projection path, which has always
            # emitted ``||`` here.
            ("sqlite", "orders.status || orders.status"),
            ("postgres", "orders.status || orders.status"),
            ("mysql", "CONCAT(orders.status, orders.status)"),
            ("duckdb", "orders.status || orders.status"),
            ("clickhouse", "CONCAT(orders.status, orders.status)"),
        ],
    )
    async def test_concat_via_pipe_pipe_translates_per_dialect(
        self, orders_model: SlayerModel, dialect: str, expected_substring: str,
    ) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["status || status = 'foo'"],
        )
        sql = await _generate(
            generator=SQLGenerator(dialect=dialect),
            query=query,
            model=orders_model,
        )
        assert expected_substring in sql, f"{dialect}: {expected_substring!r} not in {sql!r}"


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

    DEV-1703: a ``test`` datasource is registered so the two call sites can
    render through ``engine.execute(dry_run=True)`` (they used to build an
    ``EnrichedQuery`` by hand and feed the retired
    ``SQLGenerator.generate(enriched=...)`` entry point).
    """
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
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
        # The multi-term LHS of ``> 7`` must be enclosed so the comparator's
        # precedence is unambiguous. The typed pipeline qualifies the refs and
        # encloses the derived expression in its type CAST — ``CAST(m.a * 0.4
        # + ... AS DOUBLE PRECISION) > 7`` — which is precedence-safe: the whole
        # arithmetic sum binds to ``> 7``, not just the trailing term.
        # Single bounded quantifier (`[^)]+`) avoids the S5852 backtracking shape.
        assert "WHERE" in norm
        where_clause = norm.split("WHERE", 1)[1]
        m = _re.search(r"CAST\(\s*m\.a \* 0\.4[^)]+\)\s*>\s*7", where_clause)
        assert m is not None, (
            f"Expected the multi-term derived LHS enclosed before `> 7`; "
            f"got: {where_clause}"
        )
        # All four weighted terms must survive inside the enclosed LHS — the
        # single-quantifier regex alone would still match if a later term were
        # dropped (CodeRabbit). Plain substring checks keep the S5852-safe shape.
        for _term in ("m.a", "m.b", "m.c", "m.d"):
            assert _term in m.group(0), (
                f"Expected weighted term {_term} inside the enclosed LHS; "
                f"got: {m.group(0)}"
            )
        # Negative: the trailing arithmetic term must NOT land bare next to
        # ``> 7`` (the un-enclosed, precedence-broken shape).
        assert "* 0.2 > 7" not in where_clause, (
            f"Pre-wrap shape `d * 0.2 > 7` should not survive; got: {where_clause}"
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
        self, tmp_path,
    ) -> None:
        """The dotted joined-column filter branch must also wrap inlined
        non-bare Column.sql bodies in outer parens. Filter
        ``joined_model.score > 7`` where the joined column's sql is a
        multi-term arithmetic expression.

        DEV-1703: rendered through ``engine.execute(dry_run=True)``; was
        ``engine._enrich`` + ``SQLGenerator.generate(enriched=...)``.
        """
        engine = await _build_joined_customers_orders_engine_dev1539(
            tmp_path=tmp_path,
            customers_score_sql="a * 0.6 + b * 0.4",
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            filters=["customers.score > 7"],
        )
        sql = (await engine.execute(query=query, dry_run=True)).sql or ""
        norm = _norm(sql)
        # The inlined joined-column expression must be ENCLOSED before `> 7`.
        # Same shape as the local-column sibling above: the typed pipeline
        # qualifies the refs and encloses the derived expression in its type
        # CAST — ``CAST(customers.a * 0.6 + customers.b * 0.4 AS DOUBLE
        # PRECISION) > 7`` — so the whole arithmetic sum binds to ``> 7``,
        # not just the trailing term. Single bounded quantifier (`[^)]+`)
        # keeps the S5852-safe regex shape.
        assert "WHERE" in norm
        where_clause = norm.split("WHERE", 1)[1]
        m = _re.search(r"CAST\(\s*customers\.a \* 0\.6[^)]+\)\s*>\s*7", where_clause)
        assert m is not None, (
            f"Expected dotted joined-column multi-term sql enclosed before "
            f"`> 7`; got: {where_clause}"
        )
        # Both weighted terms survive inside the enclosed LHS.
        for _term in ("customers.a", "customers.b"):
            assert _term in m.group(0), (
                f"Expected weighted term {_term} inside the enclosed LHS; "
                f"got: {m.group(0)}"
            )
        # Negative: the trailing arithmetic term must NOT land bare next to
        # ``> 7`` (the un-enclosed, precedence-broken shape).
        assert "* 0.4 > 7" not in where_clause, (
            f"Pre-wrap shape `b * 0.4 > 7` should not survive; got: {where_clause}"
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
        self, tmp_path,
    ) -> None:
        """Site 1b backslash safety: the dotted joined-column inlining
        branch must also use lambda-replacement in ``re.sub`` so
        backslashes inside the joined column's SQL aren't silently halved
        when substituted into the filter text.

        DEV-1703: rendered through ``engine.execute(dry_run=True)``; was
        ``engine._enrich`` + ``SQLGenerator.generate(enriched=...)``.
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
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            # The joined column is named ``score`` in the helper; that's
            # the alias under which the backslash-bearing sql lives.
            filters=["customers.score > 0"],
        )
        sql = (await engine.execute(query=query, dry_run=True)).sql or ""
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
        # The typed pipeline qualifies the refs and encloses the connector-rooted
        # derived expression in its type CAST — ``CAST(m.archived AND m.deleted
        # AS BOOLEAN)`` — so the surrounding ``IS NULL`` binds to the whole
        # connector, not just the trailing operand. Single bounded quantifier.
        m = _re.search(
            r"CAST\(\s*m\.archived\s+" + connector + r"\s+m\.deleted\s+AS BOOLEAN\)",
            norm,
            _re.IGNORECASE,
        )
        assert m is not None, (
            f"{connector}-rooted Column.sql must be enclosed on inline; got:\n{norm}"
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
        # The typed pipeline encloses the NOT-rooted derived expression in its
        # type CAST: ``CAST(NOT m.archived AS BOOLEAN) IS NULL`` — the NOT binds
        # inside the CAST, so ``IS NULL`` applies to the whole predicate.
        m = _re.search(
            r"CAST\(\s*NOT\s+m\.archived\s+AS BOOLEAN\)\s+IS\s+NULL", norm, _re.IGNORECASE,
        )
        assert m is not None, (
            f"NOT-predicate Column.sql must be enclosed on inline; got:\n{norm}"
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
            r"CAST\(\s*m\.amount\s+BETWEEN\s+100\s+AND\s+500\s+AS BOOLEAN\)\s+IS\s+NULL",
            norm,
            _re.IGNORECASE,
        )
        assert m is not None, (
            f"BETWEEN-predicate Column.sql must be enclosed on inline; got:\n{norm}"
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
            r"CAST\(\s*m\.status\s+IN\s*\([^)]*\)\s+AS BOOLEAN\)\s+IS\s+NULL",
            norm,
            _re.IGNORECASE,
        )
        assert m is not None, (
            f"IN-predicate Column.sql must be enclosed on inline; got:\n{norm}"
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

    async def test_inner_stage_render_also_mangles_aliases(
        self, orders_model: SlayerModel,
    ) -> None:
        """Inner-stage renders (``source_queries`` stages behind a
        query-backed model) must mangle aliases too — otherwise the outer
        stage's references to inner-stage columns would mismatch what the
        inner stage actually emits.

        Pins Codex HIGH #1: the consistency of multi-stage BigQuery SQL
        rests on the rewrite firing on EVERY nesting level, not just the
        terminal one. The rewrite is deterministic, so as long as it fires
        on both, the inner emit and the outer reference resolve to the same
        ``___``-form alias.

        (Was ``test_wrapped_render_mode_also_mangles_aliases``, which drove
        the legacy ``SQLGenerator.generate(enriched=..., render_mode=
        "wrapped")`` entry point directly. The typed engine renders nested
        stages through ``generate_planned_stages``; the invariant and the
        assertions below are unchanged.)
        """
        stages_model = SlayerModel(
            name="qb_stage",
            data_source="test",
            source_queries=[
                SlayerQuery(
                    name="stg",
                    source_model="orders",
                    measures=[
                        ModelMeasure(formula="*:count"),
                        ModelMeasure(formula="revenue:sum"),
                    ],
                    dimensions=[ColumnRef(name="status")],
                ),
                SlayerQuery(
                    source_model="stg",
                    dimensions=[ColumnRef(name="status")],
                    measures=[ModelMeasure(formula="revenue_sum:sum")],
                ),
            ],
        )
        gen = SQLGenerator(dialect="bigquery")
        query = SlayerQuery(
            source_model="qb_stage",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="revenue_sum_sum:sum")],
        )
        sql = await _generate(
            gen, query, orders_model, extra_models=[stages_model],
        )
        # The inner stage keeps every alias (so the outer stage can reach
        # them); all aliases must be mangled.
        for m in _re.findall(r"`([^`]+)`", sql):
            assert "." not in m, (
                f"BigQuery inner-stage SQL still has dotted alias `{m}`:\n{sql}"
            )
        # And the mangle separator is present (the rewrite actually fired).
        assert "___" in sql, f"inner-stage rewrite did not fire:\n{sql}"
        # Inner emit and outer reference agree on the SAME ``___`` alias:
        # each appears at least twice (emitted once, referenced once).
        for alias in ("stg___status", "stg___revenue_sum_sum"):
            assert sql.count(f"`{alias}`") >= 2, (
                f"inner-stage alias `{alias}` is not referenced by the outer "
                f"stage under the same mangled name:\n{sql}"
            )

    async def test_rewrite_mangles_the_final_public_projection(
        self, orders_model: SlayerModel,
    ) -> None:
        """On BigQuery, the FINAL emitted SQL's public projection carries
        ``___``-mangled aliases — the rewrite ran on SQL whose projection
        had already been decided.

        SUBSTITUTION (DEV-1703). This test replaces
        ``test_rewrite_fires_after_outer_projection_trim``, which spied on
        ``SQLGenerator._apply_outer_projection_trim`` and
        ``BigqueryDialect.rewrite_emitted_sql`` and asserted the call order
        ``["trim", "rewrite"]`` (Codex MEDIUM #5 — mangling before the trim
        would let the trim's parser see already-mangled aliases and miss
        public-projection columns). ``_apply_outer_projection_trim`` existed
        only on the retired ``SQLGenerator.generate(enriched=...)`` entry
        point: the typed pipeline has no trim pass at all, because the
        planner BUILDS the public projection instead of trimming a wider one
        down. The ordering contract is therefore unrepresentable — there is
        no second call to order against.

        What survives is the OBSERVABLE half of that contract, pinned below:
        the mangling is applied to the finished, already-projected SQL. If
        the rewrite fired before the projection was decided, the projection
        layer emitted afterwards would carry dotted aliases and the outermost
        SELECT would come back with ``orders.status`` rather than
        ``orders___status``.

        The same query as the retired test is used — the ``dense_rank``
        filter forces a hidden hoisted column, so the public projection is a
        real narrowing rather than a no-op on a trivial query.
        """
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            dimensions=[ColumnRef(name="status")],
            # Filter using a windowed transform creates a hidden hoisted
            # column, so the public projection is a real narrowing.
            filters=["dense_rank(revenue:sum) <= 5"],
        )
        async with _persist_and_engine(orders_model, ds_type="bigquery") as engine:
            resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        # The outermost SELECT is exactly the public projection, mangled.
        outer = _outer_projection_names(sql, dialect="bigquery")
        assert outer == {"orders___status", "orders____count"}, (
            f"final projection must be the mangled public keys; got "
            f"{outer!r}\nSQL:\n{sql}"
        )
        # ...and the un-mangled dotted forms never reach the output.
        for dotted in ("orders.status", "orders._count"):
            assert dotted not in outer, (
                f"dotted alias {dotted!r} survived into the final projection — "
                f"the rewrite did not run on the projected SQL:\n{sql}"
            )
        # The hidden hoisted column was narrowed away by the projection (so
        # the projection genuinely ran) yet is itself mangled where it does
        # appear (so the rewrite covered the inner layers too).
        assert "orders____dense_rank_inner" in sql, (
            f"expected the hoisted dense_rank column, mangled, inside the "
            f"query body:\n{sql}"
        )
        assert not any(name.endswith("_dense_rank_inner") for name in outer), (
            f"hidden hoisted column must not reach the public projection; got "
            f"{outer!r}\nSQL:\n{sql}"
        )
        # No backticked identifier anywhere retains a dot.
        for ident in _re.findall(r"`([^`]+)`", sql):
            assert "." not in ident, (
                f"BigQuery output still has a dotted identifier `{ident}`:\n{sql}"
            )
        # The engine decodes the mangled keys back to the dotted result keys.
        assert resp.columns == ["orders.status", "orders._count"], resp.columns


class TestCrossModelAggregateSourceSqlJoinInference:
    """DEV-1526 harvest (Stage 1): a CROSS-MODEL aggregate whose TARGET
    column's ``Column.sql`` crosses a FURTHER join must pull the implied LEFT
    JOINs into the per-plan ``_cm_*`` CTE body. The fix lands Stage 4 (Law-1
    inside the CTE ScopeFrame); the discovery tests are pinned strict-xfail
    until then. ``test_bare_target_column_no_spurious_join_in_cte`` stays green
    (a bare cross-model aggregate already works). Chain:
    ``orders_x → customers_v2 → regions → countries``.
    """

    @pytest.fixture
    async def storage(self, tmp_path):
        s = YAMLStorage(base_dir=str(tmp_path))
        await s.save_datasource(DatasourceConfig(name="test", type="postgres"))
        await s.save_model(SlayerModel(
            name="countries", sql_table="countries", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
                Column(name="gdp", sql="gdp", type=DataType.DOUBLE),
            ],
        ))
        await s.save_model(SlayerModel(
            name="regions", sql_table="regions", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
                Column(name="population", sql="population", type=DataType.DOUBLE),
                Column(name="weight", sql="weight", type=DataType.DOUBLE),
                Column(name="props", sql="props", type=DataType.TEXT),
                Column(name="country_id", sql="country_id", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="countries", join_pairs=[["country_id", "id"]])],
        ))
        return s

    def _customers_v2(self, *, extra_columns=None, filters=None) -> SlayerModel:
        cols = [
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
            Column(name="lifetime_value", sql="lifetime_value", type=DataType.DOUBLE),
        ]
        if extra_columns:
            cols.extend(extra_columns)
        return SlayerModel(
            name="customers_v2", sql_table="customers", data_source="test",
            columns=cols,
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
            filters=filters or [],
        )

    def _orders_x(self) -> SlayerModel:
        return SlayerModel(
            name="orders_x", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            ],
            joins=[ModelJoin(target_model="customers_v2", join_pairs=[["customer_id", "id"]])],
        )

    async def _engine_with(self, storage, customers_v2) -> SlayerQueryEngine:
        await storage.save_model(customers_v2)
        await storage.save_model(self._orders_x())
        return SlayerQueryEngine(storage=storage)

    async def test_single_further_hop_source_sql(self, storage) -> None:
        engine = await self._engine_with(storage, self._customers_v2(extra_columns=[
            Column(name="deep_pop", sql="regions.population", type=DataType.DOUBLE),
        ]))
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(formula="customers_v2.deep_pop:sum")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "LEFT JOIN regions AS regions" in cm_body, cm_body
        assert "SUM(regions.population)" in cm_body, cm_body

    async def test_multi_hop_further_join_source_sql(self, storage) -> None:
        engine = await self._engine_with(storage, self._customers_v2(extra_columns=[
            Column(name="deep_gdp", sql="regions.countries.gdp", type=DataType.DOUBLE),
        ]))
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(formula="customers_v2.deep_gdp:sum")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "LEFT JOIN regions AS regions" in cm_body, cm_body
        assert "LEFT JOIN countries AS regions__countries" in cm_body, cm_body
        assert "SUM(regions__countries.gdp)" in cm_body, cm_body

    async def test_path_alias_source_sql(self, storage) -> None:
        engine = await self._engine_with(storage, self._customers_v2(extra_columns=[
            Column(name="deep_country_name", sql="regions.countries.name",
                   type=DataType.TEXT),
        ]))
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(formula="customers_v2.deep_country_name:count_distinct")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "LEFT JOIN regions AS regions" in cm_body, cm_body
        assert "LEFT JOIN countries AS regions__countries" in cm_body, cm_body
        assert "COUNT(DISTINCT regions__countries.name)" in cm_body, cm_body

    async def test_mode_a_function_wrapping_source_sql(self, storage) -> None:
        engine = await self._engine_with(storage, self._customers_v2(extra_columns=[
            Column(name="region_prop_x",
                   sql="json_extract(regions.props, '$.x')", type=DataType.DOUBLE),
        ]))
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(formula="customers_v2.region_prop_x:sum")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "LEFT JOIN regions AS regions" in cm_body, cm_body
        assert "regions.props" in cm_body, cm_body

    async def test_sibling_derived_chain_source_sql(self, storage) -> None:
        engine = await self._engine_with(storage, self._customers_v2(extra_columns=[
            Column(name="pop_helper", sql="regions.population", type=DataType.DOUBLE),
            Column(name="doubled_pop", sql="pop_helper * 2", type=DataType.DOUBLE),
        ]))
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(formula="customers_v2.doubled_pop:sum")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "LEFT JOIN regions AS regions" in cm_body, cm_body
        assert "regions.population" in cm_body, cm_body

    async def test_mixed_base_col_and_further_join_source_sql(self, storage) -> None:
        engine = await self._engine_with(storage, self._customers_v2(extra_columns=[
            Column(name="deep_score", sql="lifetime_value + regions.population",
                   type=DataType.DOUBLE),
        ]))
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(formula="customers_v2.deep_score:sum")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "LEFT JOIN regions AS regions" in cm_body, cm_body
        assert "customers_v2.lifetime_value" in cm_body, cm_body
        assert "regions.population" in cm_body, cm_body
        assert "countries" not in cm_body, cm_body

    async def test_dedup_source_and_target_model_filter_same_join(self, storage) -> None:
        # Green on my branch: the target model filter `regions.name IS NOT NULL`
        # already pulls the `regions` join via the implemented filter-discovery
        # path (DEV-1494), so the source ref resolves against it even before the
        # DEV-1526 source-SQL fix. The pure source-only cases stay xfail above.
        engine = await self._engine_with(storage, self._customers_v2(
            extra_columns=[
                Column(name="deep_pop", sql="regions.population", type=DataType.DOUBLE),
            ],
            filters=["regions.name IS NOT NULL"],
        ))
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(formula="customers_v2.deep_pop:sum")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert cm_body.count("LEFT JOIN regions AS regions") == 1, cm_body
        assert "SUM(regions.population)" in cm_body, cm_body

    async def test_filter_and_source_cross_different_depths(self, storage) -> None:
        engine = await self._engine_with(storage, self._customers_v2(
            extra_columns=[
                Column(name="deep_gdp", sql="regions.countries.gdp",
                       type=DataType.DOUBLE),
            ],
            filters=["regions.name IS NOT NULL"],
        ))
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(formula="customers_v2.deep_gdp:sum")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert cm_body.count("LEFT JOIN regions AS regions") == 1, cm_body
        assert "LEFT JOIN countries AS regions__countries" in cm_body, cm_body
        assert "SUM(regions__countries.gdp)" in cm_body, cm_body

    async def test_composite_two_operand_each_cte_pulls_join(self, storage) -> None:
        engine = await self._engine_with(storage, self._customers_v2(extra_columns=[
            Column(name="deep_pop", sql="regions.population", type=DataType.DOUBLE),
            Column(name="deep_weight", sql="regions.weight", type=DataType.DOUBLE),
        ]))
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(
                formula="customers_v2.deep_pop:sum + customers_v2.deep_weight:sum")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        pop_body = _extract_cte_body(sql, r"_cm_\w*deep_pop\w*")
        weight_body = _extract_cte_body(sql, r"_cm_\w*deep_weight\w*")
        assert pop_body.count("LEFT JOIN regions AS regions") == 1, pop_body
        assert weight_body.count("LEFT JOIN regions AS regions") == 1, weight_body
        assert "SUM(regions.population)" in pop_body, pop_body
        assert "SUM(regions.weight)" in weight_body, weight_body

    async def test_first_last_source_join_in_ranked_subquery(self, storage) -> None:
        engine = await self._engine_with(storage, self._customers_v2(extra_columns=[
            Column(name="deep_pop", sql="regions.population", type=DataType.DOUBLE),
            # The ranking column must be one of the TARGET's own — the ranked
            # CTE is rooted there and a host column is not in its scope.
            Column(name="signup_at", sql="signup_at", type=DataType.TIMESTAMP),
        ]))
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(
                formula="customers_v2.deep_pop:last(customers_v2.signup_at)")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        cm_body = _extract_cte_body(sql, r"_rk_\w+")
        assert "ROW_NUMBER()" in cm_body, cm_body
        normalized = _norm(cm_body)
        inner_start = normalized.find("FROM (")
        assert inner_start > 0, normalized
        inner_body = normalized[inner_start:]
        assert "LEFT JOIN regions AS regions" in inner_body, inner_body

    async def test_bare_target_column_no_spurious_join_in_cte(self, storage) -> None:
        """Green: a bare cross-model aggregate already works — the ``_cm_*``
        CTE has no spurious deeper join."""
        engine = await self._engine_with(storage, self._customers_v2())
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(formula="customers_v2.region_id:count_distinct")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "COUNT(DISTINCT customers_v2.region_id)" in cm_body, cm_body
        assert "regions" not in cm_body, cm_body
        assert "countries" not in cm_body, cm_body

    async def test_deeper_join_only_in_cte_not_host_base(self, storage) -> None:
        engine = await self._engine_with(storage, self._customers_v2(extra_columns=[
            Column(name="deep_pop", sql="regions.population", type=DataType.DOUBLE),
        ]))
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(formula="customers_v2.deep_pop:sum")],
        )
        sql = (await engine.execute(query, dry_run=True)).sql
        base_body = _extract_cte_body(sql, r"_base")
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "LEFT JOIN regions AS regions" in cm_body, cm_body
        assert "regions" not in base_body, base_body


class TestWindowedMeasureGuards:
    """DEV-1496 harvest (Stage 1): windowed measures must emit a ``_wm_``
    range-join CTE and RAISE loudly on shapes the primitive does not support,
    rather than silently degrade to a plain grouped aggregate. Today the typed
    pipeline drops ``window=`` silently, so every guard is pinned strict-xfail
    to Stage 10 (DEV-1714). The composite/hidden/mixed guards are DEV-1504.
    """

    @pytest.fixture
    def orders_model(self) -> SlayerModel:
        return SlayerModel(
            name="orders", sql_table="public.orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
        )

    async def test_windowed_non_sum_avg_raises(self, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "revenue:min(window='30d')", "name": "rev_w"}],
        )
        with pytest.raises(ValueError, match="only supported for sum and avg"):
            await _engine_generate(query=query, model=orders_model)

    async def test_windowed_no_time_dimension_raises(self, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[{"formula": "revenue:sum(window='30d')", "name": "rev_w"}],
        )
        with pytest.raises(ValueError, match="could not resolve its time dimension"):
            await _engine_generate(query=query, model=orders_model)

    async def test_windowed_cross_model_raises(self, tmp_path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
        await storage.save_model(SlayerModel(
            name="customers", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
        ))
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        await storage.save_model(orders)
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "customers.revenue:sum(window='30d')", "name": "rev_w"}],
        )
        with pytest.raises(NotImplementedError, match="cross-model"):
            await engine.execute(query, dry_run=True)

    async def test_windowed_with_transform_raises(self, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[
                {"formula": "revenue:sum(window='90d')", "name": "rev_w"},
                {"formula": "time_shift(revenue:sum, -1)", "name": "rev_prev"},
            ],
        )
        # DEV-1835 lift: windowed measures desugar to the regroup producer, so a
        # windowed measure coexisting with a transform sibling now renders
        # instead of raising. It must produce scope-closed SQL with the
        # ``__regroup__`` sentinel fully lowered away.
        sql = await _engine_generate(query=query, model=orders_model)
        assert_scope_closed(sql, dialect="postgres")
        assert "__regroup__" not in sql

    async def test_windowed_as_transform_input_raises(self, orders_model: SlayerModel) -> None:
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "cumsum(revenue:sum(window='90d'))", "name": "cum_w"}],
        )
        # DEV-1835 lift: a windowed aggregate feeding a transform now renders
        # instead of raising; the emitted SQL must be scope-closed and fully
        # lowered (no ``__regroup__`` sentinel).
        sql = await _engine_generate(query=query, model=orders_model)
        assert_scope_closed(sql, dialect="postgres")
        assert "__regroup__" not in sql

    async def test_windowed_in_arithmetic_raises(self, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "revenue:sum(window='90d') / 2", "name": "half_w"}],
        )
        # DEV-1835 lift: a windowed aggregate inside scalar arithmetic now
        # renders instead of raising; scope-closed and fully lowered.
        sql = await _engine_generate(query=query, model=orders_model)
        assert_scope_closed(sql, dialect="postgres")
        assert "__regroup__" not in sql

    async def test_windowed_filter_only_hidden_raises(self, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "revenue:sum", "name": "rev"}],
            filters=["revenue:sum(window='90d') > 100"],
        )
        # DEV-1835 lift: a filter-only (hidden) windowed aggregate now renders
        # instead of raising; scope-closed and fully lowered.
        sql = await _engine_generate(query=query, model=orders_model)
        assert_scope_closed(sql, dialect="postgres")
        assert "__regroup__" not in sql

    async def test_windowed_mixed_aggregate_filter_raises(self, orders_model: SlayerModel) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "rev_w"}],
            filters=["revenue:sum(window='90d') > 100 and revenue:sum > 50"],
        )
        # DEV-1835 lift: a filter mixing a windowed and a plain aggregate now
        # renders instead of raising; scope-closed and fully lowered.
        sql = await _engine_generate(query=query, model=orders_model)
        assert_scope_closed(sql, dialect="postgres")
        assert "__regroup__" not in sql

    # ------------------------------------------------------------------ #
    # DEV-1714 Stage 10 — loud-error coverage beyond the DEV-1496/1504 pins:
    # invalid durations must raise at plan time (never a silent degrade nor a
    # TypeError), the ``window`` kwarg name is reserved for sum/avg only (even
    # on custom aggregations), and guard PRECEDENCE (Codex C1) is contractual.
    # ------------------------------------------------------------------ #

    @pytest.mark.parametrize("bad", ["90x", "d90", "9z"])
    async def test_windowed_malformed_duration_raises(
        self, bad: str, orders_model: SlayerModel,
    ) -> None:
        """A malformed compact duration must raise a clear ``ValueError`` at
        plan time, not silently degrade."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": f"revenue:sum(window='{bad}')", "name": "rev_w"}],
        )
        with pytest.raises(ValueError, match=r"Use syntax like '1y2m3w5d6h7min8s'"):
            await _engine_generate(query=query, model=orders_model)

    async def test_windowed_empty_duration_raises(self, orders_model: SlayerModel) -> None:
        """An empty window duration must raise the distinct empty-duration error."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "revenue:sum(window='')", "name": "rev_w"}],
        )
        with pytest.raises(ValueError, match="cannot be empty"):
            await _engine_generate(query=query, model=orders_model)

    async def test_windowed_non_string_duration_raises(self, orders_model: SlayerModel) -> None:
        """Codex C4: a non-string ``window`` value (e.g. ``window=90``) must be
        normalized to a ``ValueError`` — never a raw ``TypeError`` from feeding a
        number to the compact-duration regex."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "revenue:sum(window=90)", "name": "rev_w"}],
        )
        with pytest.raises(ValueError):
            await _engine_generate(query=query, model=orders_model)

    async def test_custom_aggregation_with_window_raises(self) -> None:
        """The ``window`` kwarg name is reserved for sum/avg; invoking a custom
        aggregation with ``window=`` must raise G1 (legacy parity — legacy pops
        ``window`` unconditionally before dispatch)."""
        model = SlayerModel(
            name="orders", sql_table="public.orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="amount_col", sql="amount", type=DataType.DOUBLE),
            ],
            aggregations=[Aggregation(name="myagg", formula="SUM({value})")],
        )
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "amount_col:myagg(window='90d')", "name": "w"}],
        )
        with pytest.raises(ValueError, match="only supported for sum and avg"):
            await _engine_generate(query=query, model=model)

    async def test_windowed_transform_input_precedence_not_selected(
        self, orders_model: SlayerModel,
    ) -> None:
        """Codex C1: a windowed aggregate nested in a transform interns as a
        HIDDEN dependency slot, so the transform guard (G4) must win over the
        hidden-slot guard (G6) — the error says ``transform``, never
        ``selected``."""
        orders_model.default_time_dimension = "created_at"
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "cumsum(revenue:sum(window='90d'))", "name": "cum_w"}],
        )
        # DEV-1835 lift: both the transform (G4) and hidden (G6) guards are
        # gone — a windowed aggregate nested in a transform now renders instead
        # of raising, so there is no longer a precedence to disambiguate. The
        # emitted SQL must be scope-closed and fully lowered.
        sql = await _engine_generate(query=query, model=orders_model)
        assert_scope_closed(sql, dialect="postgres")
        assert "__regroup__" not in sql

    @pytest.mark.parametrize(
        "case",
        [
            "g1_non_sum_avg", "g2_no_time_dim", "g3_cross_model", "g4_transform",
            "g5_arithmetic", "g6_hidden", "g7_mixed", "g8_malformed",
        ],
    )
    def test_windowed_guards_fire_at_plan_time(self, case: str) -> None:  # NOSONAR(S3776) — flat case dispatch for a parametrized guard matrix; each branch builds one (query, bundle, exc, match) tuple, kept inline so the guard scenario reads next to its expectation.
        """Decision D-a / Codex C1+C2: every windowed guard raises in the PLANNER
        (``plan_query``) — before any SQL is rendered. Proves the guards are
        plan-time (a renderer-only guard would fail this by not raising here)."""
        created_at = Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP)
        base_cols = [
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            created_at,
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
        ]
        td = [TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)]

        def _plain() -> SlayerModel:
            return SlayerModel(
                name="orders", sql_table="public.orders", data_source="test",
                columns=[c.model_copy() for c in base_cols],
            )

        def _bundle(model: SlayerModel, referenced=None) -> ResolvedSourceBundle:
            return ResolvedSourceBundle(source_model=model, referenced_models=referenced or [])

        if case == "g1_non_sum_avg":
            q = SlayerQuery(source_model="orders", time_dimensions=td,
                            measures=[{"formula": "revenue:min(window='30d')", "name": "rev_w"}])
            bundle, exc, match = _bundle(_plain()), ValueError, "only supported for sum and avg"
        elif case == "g2_no_time_dim":
            q = SlayerQuery(source_model="orders", dimensions=[ColumnRef(name="status")],
                            measures=[{"formula": "revenue:sum(window='30d')", "name": "rev_w"}])
            bundle, exc, match = _bundle(_plain()), ValueError, "could not resolve its time dimension"
        elif case == "g3_cross_model":
            customers = SlayerModel(
                name="customers", sql_table="customers", data_source="test",
                columns=[
                    Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                    Column(name="revenue", sql="amount", type=DataType.DOUBLE),
                ],
            )
            orders = SlayerModel(
                name="orders", sql_table="orders", data_source="test",
                columns=[
                    Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                    Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                    created_at.model_copy(),
                ],
                joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
            )
            q = SlayerQuery(source_model="orders", time_dimensions=td,
                            measures=[{"formula": "customers.revenue:sum(window='30d')", "name": "rev_w"}])
            bundle, exc, match = _bundle(orders, [customers]), NotImplementedError, "cross-model"
        elif case == "g4_transform":
            model = _plain()
            model.default_time_dimension = "created_at"
            q = SlayerQuery(source_model="orders", time_dimensions=td, measures=[
                {"formula": "revenue:sum(window='90d')", "name": "rev_w"},
                {"formula": "time_shift(revenue:sum, -1)", "name": "rev_prev"},
            ])
            # DEV-1835 lift: G4 dissolved — this now plans instead of raising.
            bundle, exc, match = _bundle(model), None, None
        elif case == "g5_arithmetic":
            q = SlayerQuery(source_model="orders", time_dimensions=td,
                            measures=[{"formula": "revenue:sum(window='90d') / 2", "name": "half_w"}])
            # DEV-1835 lift: G5 dissolved — this now plans instead of raising.
            bundle, exc, match = _bundle(_plain()), None, None
        elif case == "g6_hidden":
            q = SlayerQuery(source_model="orders", time_dimensions=td,
                            measures=[{"formula": "revenue:sum", "name": "rev"}],
                            filters=["revenue:sum(window='90d') > 100"])
            # DEV-1835 lift: G6 dissolved — this now plans instead of raising.
            bundle, exc, match = _bundle(_plain()), None, None
        elif case == "g7_mixed":
            q = SlayerQuery(source_model="orders", time_dimensions=td,
                            measures=[{"formula": "revenue:sum(window='90d')", "name": "rev_w"}],
                            filters=["revenue:sum(window='90d') > 100 and revenue:sum > 50"])
            # DEV-1835 lift: G7 dissolved — this now plans instead of raising.
            bundle, exc, match = _bundle(_plain()), None, None
        else:  # g8_malformed
            q = SlayerQuery(source_model="orders", time_dimensions=td,
                            measures=[{"formula": "revenue:sum(window='90x')", "name": "rev_w"}])
            bundle, exc, match = _bundle(_plain()), ValueError, "Use syntax like"

        if exc is None:
            # DEV-1835 lift: the dissolved guards (g4–g7) now plan without error.
            planned = plan_query(query=q, bundle=bundle)
            assert planned is not None
        else:
            with pytest.raises(exc, match=match):
                plan_query(query=q, bundle=bundle)

    async def test_windowed_default_time_dimension_not_selected_raises(self) -> None:
        """CR#3: a windowed measure whose only time dimension is the model's
        ``default_time_dimension`` (never selected as a query ``time_dimensions``
        entry, so never interned as a bucket-grain slot) must raise the clean G2
        message — not crash on the required ``window_time_dimension_slot_id``."""
        model = SlayerModel(
            name="orders", sql_table="public.orders", data_source="test",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
            default_time_dimension="created_at",
        )
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "revenue:sum(window='90d')", "name": "rev_w"}],
        )
        with pytest.raises(ValueError, match="could not resolve its time dimension"):
            await _engine_generate(query=query, model=model)

    async def test_windowed_mixed_row_filter_raises(self, orders_model: SlayerModel) -> None:
        """Codex round 5: a single filter mixing a windowed measure with a ROW
        predicate (not just a plain aggregate — G7's case) is reclassified whole
        to POST, so the row part would neither filter pre-aggregation nor resolve
        against an unprojected ``_base`` column. Reject it at plan time."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[{"formula": "revenue:sum(window='90d')", "name": "rev_w"}],
            filters=["revenue:sum(window='90d') > 100 and status = 'active'"],
        )
        # DEV-1835 lift: a filter mixing a windowed aggregate with a row
        # predicate now renders instead of raising; scope-closed and fully
        # lowered.
        sql = await _engine_generate(query=query, model=orders_model)
        assert_scope_closed(sql, dialect="postgres")
        assert "__regroup__" not in sql

