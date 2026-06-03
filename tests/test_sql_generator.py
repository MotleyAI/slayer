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
from slayer.sql.generator import (
    AggRenderSpec,
    SQLGenerator,
    _cte_name_from_alias,
    _validate_agg_param_value,
)
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import _engine_generate


async def _noop_async(**kw):
    return None


def _norm(s: str) -> str:
    return " ".join(s.split())


def _join_aliases(sql: str, *, dialect: str = "postgres") -> set[str]:
    """The set of joined-table aliases in ``sql`` (the rendered-SQL
    equivalent of the legacy enriched-query join-alias set).

    Walks every ``JOIN`` node and collects the joined table's alias (or
    bare name when unaliased) — e.g. ``LEFT JOIN customers AS customers``
    yields ``customers``; ``LEFT JOIN regions AS customers__regions``
    yields ``customers__regions``.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    aliases: set[str] = set()
    for join in tree.find_all(sqlglot.exp.Join):
        target = join.this
        if isinstance(target, sqlglot.exp.Table):
            aliases.add(target.alias_or_name)
    return aliases


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


def _extract_cte_body(sql: str, cte_name_pattern: str) -> str:
    """Extract one CTE body by matching ``<cte_name> AS (`` and walking balanced
    parentheses to its closing ``)``.

    Robust against nested subqueries inside the CTE body (e.g. the ranked
    ``FROM (SELECT ... ROW_NUMBER() …) AS …`` that first/last isolated CTEs
    contain). ``cte_name_pattern`` is a regex matched against the CTE name —
    typical use: ``r"_cm_\\w*loss_payment_amt\\w*"``. Raises ``AssertionError``
    if no matching CTE is found.
    """
    name_match = _re.search(rf"({cte_name_pattern})\s+AS\s*\(", sql)
    assert name_match, f"No CTE matching {cte_name_pattern!r} in:\n{sql}"
    # Position just after the opening paren of ``<name> AS (``.
    body_start = sql.index("(", name_match.start()) + 1
    depth = 1
    i = body_start
    while i < len(sql) and depth > 0:
        ch = sql[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return sql[body_start:i]
        i += 1
    raise AssertionError(
        f"Unbalanced parens — no closing ) for CTE {name_match.group(1)!r}:\n{sql}"
    )


_SQLGLOT_TYPEERROR_DIALECTS = {"bigquery"}


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
        query, model, dialect=generator.dialect, validate=False,
        extra_models=extra_models,
    )


_XFAIL_WINDOWED = pytest.mark.xfail(
    strict=True,
    reason=(
        "DEV-1496: duration-windowed measures (sum(window='…')) are not yet "
        "implemented on the typed pipeline — the window kwarg is dropped and "
        "the measure degrades to a plain grouped aggregate. Auto-promotes when "
        "the range-join primitive is reimplemented."
    ),
)


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


class TestDialects:
    async def test_mysql_dialect(self, orders_model: SlayerModel) -> None:
        gen = SQLGenerator(dialect="mysql")
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="*:count")],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
        )
        sql = await _generate(gen, query, orders_model)
        assert "COUNT(*)" in sql  # Basic check — dialect-specific output


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

    @_XFAIL_WINDOWED
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

    @_XFAIL_WINDOWED
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

    @_XFAIL_WINDOWED
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

    @_XFAIL_WINDOWED
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

    @_XFAIL_WINDOWED
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

    @_XFAIL_WINDOWED
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

    @_XFAIL_WINDOWED
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

    @_XFAIL_WINDOWED
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
        assert (
            'RANK() OVER (PARTITION BY "orders.customer_id", "orders.status" '
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

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "DEV-1497: the typed pipeline does not validate that a rank "
            "partition_by column is a query dimension — it silently adds it "
            "to the base GROUP BY (changing result grain) instead of raising. "
            "Auto-promotes when the validation is restored."
        ),
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

    @pytest.mark.parametrize("dialect", ALL_DIALECTS)
    async def test_calendar_time_shift(self, dialect: str, orders_model: SlayerModel) -> None:
        """Calendar-based time_shift should produce dialect-appropriate date arithmetic in shifted CTE."""
        if dialect == "bigquery":
            pytest.skip(
                "sqlglot's BigQuery parser raises TypeError round-tripping the "
                "calendar time_shift INTERVAL construct (sqlglot limitation; "
                "BigQuery is Tier-2). Same carve-out as _SQLGLOT_TYPEERROR_DIALECTS."
            )
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
    @_XFAIL_WINDOWED
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
        # exp.Interval per dialect; MySQL + ClickHouse both render as
        # `INTERVAL N UNIT` for these AST nodes).
        for piece in ("INTERVAL 1 YEAR", "INTERVAL 2 MONTH", "INTERVAL 3 DAY"):
            assert piece in norm, (
                f"Expected dialect-correct '{piece}' in {dialect} output.\n"
                f"sql:\n{sql}"
            )

    @pytest.mark.parametrize("dialect", ["mysql", "clickhouse"])
    @_XFAIL_WINDOWED
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
        assert "INTERVAL 7 DAY" in norm, (
            f"Expected dialect-correct 'INTERVAL 7 DAY' in {dialect} output.\n"
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

    # corr / covar_samp / covar_pop are not supported on MySQL — the generator
    # raises NotImplementedError there, so MySQL is filtered out of the matrix.
    @pytest.mark.parametrize(
        "dialect", [d for d in ALL_DIALECTS if d != "mysql"],
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

    @pytest.mark.parametrize(
        "formula", [
            "revenue:corr(other=quantity)",
            "revenue:covar_samp(other=quantity)",
            "revenue:covar_pop(other=quantity)",
        ],
    )
    async def test_two_arg_stat_agg_mysql_raises(
        self, formula: str, orders_model: SlayerModel,
    ) -> None:
        gen = SQLGenerator(dialect="mysql")
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula=formula)],
        )
        with pytest.raises(NotImplementedError, match="MySQL"):
            await _generate(generator=gen, query=query, model=orders_model)

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
        # Two distinct rank columns per effective time column, both
        # dialect-independent (no dialect rewrites ROW_NUMBER alias).
        assert "_last_rn" in sql
        assert "_last_rn_2" in sql
        assert "revenue_last_created_at" in sql, (
            f"Materialised created_at alias missing on {dialect}:\n{sql}"
        )
        assert "revenue_last_updated_at" in sql, (
            f"Materialised updated_at alias missing on {dialect}:\n{sql}"
        )
        # The OUTER ORDER BY must reference each materialised alias as a
        # SINGLE dotted-identifier column (the dotted body lives inside
        # one quoted identifier; it must NOT decompose into a qualified
        # ``"orders"."revenue_last_created_at"`` two-part name). Filter
        # to Column terms because some dialects (MySQL) emit a synthetic
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
            # ``table`` is the qualified-prefix slot. For a single dotted
            # identifier (``"orders.revenue_last_created_at"``) it must be
            # absent / empty.
            tbl = inner.args.get("table")
            assert tbl is None or not tbl.name, (
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

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    def test_build_two_arg_stat_mysql_raises(self, agg: str) -> None:
        # MySQL has no native CORR / COVAR_SAMP / COVAR_POP and no Python-
        # UDF mechanism, so all three raise at SQL generation time.
        gen = SQLGenerator(dialect="mysql")
        m = self._measure(agg=agg, agg_kwargs={"other": "quantity"})
        with pytest.raises(NotImplementedError, match="MySQL"):
            gen._build_agg(m)

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
                    sql="users__orgs.signup_date",
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

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "DEV-1502: a measure whose source Column.sql contains a "
            "__-delimited join-path alias (customers__regions.population) "
            "does NOT trigger join inference on the typed pipeline — the "
            "aggregate emits SUM(customers__regions.population) with no LEFT "
            "JOINs, producing SQL that references an undefined alias. The "
            "dimension/time-dimension path-alias cases (above) DO infer the "
            "joins; only the measure-source path regressed. Auto-promotes "
            "when measure-source path-alias join discovery is restored."
        ),
    )
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
        await engine.storage.save_model(chained_model)
        sql = (await engine.execute(query, dry_run=True)).sql
        join_aliases = _join_aliases(sql)
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

        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH),
            ],
            measures=[ModelMeasure(formula="active_balance:last")],
        )
        return await _engine_generate(
            query, orders, dialect=generator.dialect,
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
        filter references a JOINED table (e.g. customers.status), the measure
        is isolated into its own _cm_ CTE with a ranked subquery. The join lives
        inside the ranked subquery so the filter resolves, and the final SELECT
        does not reference the joined table directly."""

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
        # The isolated _cm_ CTE should contain a ranked subquery with _last_rn.
        assert "_cm_" in sql, f"Expected isolated _cm_ CTE:\n{sql}"
        assert "_last_rn" in sql, f"Expected _last_rn in isolated CTE:\n{sql}"
        # The customers JOIN should be inside the _cm_ CTE's ranked subquery.
        # Balanced-paren walker — the body wraps a nested ranked subquery.
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "public.customers" in cm_body, (
            f"Expected customers JOIN inside _cm_ CTE:\n{cm_body}"
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
        # Two distinct filtered ROW_NUMBER columns must exist in the ranked CTE.
        assert "_last_rn_f0" in sql
        assert "_last_rn_f1" in sql
        # Both filter conditions must appear inside their CASE WHEN ORDER BY.
        assert "status = 'active'" in sql
        assert "status = 'completed'" in sql

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
        `revenue:last(updated_at)` must produce DISTINCT ranked aggregates
        (one ROW_NUMBER per effective time column) and not collapse to a
        single bare ``_last_rn``. The hidden materialised aggregates must
        be trimmed from the public projection (the result keys stay
        ``orders.status`` + ``orders._count``).
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
            # The ranked subquery must carry two DISTINCT ROW_NUMBER columns,
            # one per effective time column.
            assert "_last_rn" in sql
            assert "_last_rn_2" in sql
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
    """DEV-1501 — hidden first/last aggregates in ORDER BY / HAVING must
    trigger the ranked subquery, get their per-time-column ROW_NUMBER
    suffix threaded correctly, and be trimmed from the public projection
    by an outer wrap when materialised. See ``docs/architecture/planning.md``
    (hidden-slot materialise + outer trim).
    """

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
            assert "_first_rn" in sql
            assert "_last_rn" in sql
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
            # The first(created_at) rank window must order created_at ASC;
            # the last(updated_at) rank window must order updated_at DESC.
            assert _re.search(
                r"ROW_NUMBER\(\)\s+OVER\s*\([^)]*ORDER BY\s+orders\.created_at\s+ASC", sql
            ), f"first(created_at) rank not ASC:\n{sql}"
            assert _re.search(
                r"ROW_NUMBER\(\)\s+OVER\s*\([^)]*ORDER BY\s+orders\.updated_at\s+DESC", sql
            ), f"last(updated_at) rank not DESC:\n{sql}"

    async def test_order_by_first_and_last_same_time_col(
        self, generator: SQLGenerator
    ) -> None:
        """``first(created_at)`` ASC + ``last(created_at)`` DESC share the
        same suffix bucket (same effective time column) but produce TWO
        rank columns (one ASC for first, one DESC for last).
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
            # Same time col → single suffix bucket (``""``) carrying both
            # first and last rn columns; no ``_2`` suffix should appear.
            assert "_first_rn" in sql and "_last_rn" in sql
            assert "_last_rn_2" not in sql and "_first_rn_2" not in sql
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
            assert "_last_rn" in sql
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
            assert "_last_rn" in sql
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
            # Two distinct rank columns (one per effective time column).
            assert "_last_rn" in sql and "_last_rn_2" in sql
            # Result keys = projection only: status + projected last alias.
            assert set(res.columns) == {"orders.status", "orders.latest_cr"}, (
                f"Result columns mismatch: {res.columns!r}\nSQL:\n{sql}"
            )
            # The PROJECTED last(created_at) uses one suffix (the first
            # bucket, created_at sorted first → ``""``) and the order-only
            # last(updated_at) uses the OTHER suffix (``"_2"``). The two
            # MUST be distinct or DEV-1501 isn't actually fixed. Inspect
            # the inner (base) SELECT — the outer wrap is the trim layer,
            # and the rn-based aggregates live in the inner.
            tree = sqlglot.parse_one(sql, dialect="postgres")
            inner_from = _outer_from_node(sql)
            assert isinstance(inner_from, sqlglot.exp.Subquery), (
                f"Expected outer-wrap subquery:\n{sql}"
            )
            inner_sql = inner_from.this.sql(dialect="postgres")
            inner_rn = _projection_rn_by_alias(inner_sql)
            assert "orders.latest_cr" in inner_rn, (
                f"Projected latest_cr aggregate not found in inner SELECT:\n"
                f"{inner_sql}"
            )
            assert "orders.revenue_last_updated_at" in inner_rn, (
                f"Hidden order-only updated_at aggregate not found in inner:\n"
                f"{inner_sql}"
            )
            assert inner_rn["orders.latest_cr"] != inner_rn["orders.revenue_last_updated_at"], (
                f"Projected and order-only last() both bound to the same rank "
                f"column ({inner_rn['orders.latest_cr']!r}) — distinct "
                f"time cols collapsed.\nSQL:\n{sql}"
            )
            _ = tree  # keep tree variable for IDEs; assertion uses helper

    async def test_filter_only_last_with_having(
        self, generator: SQLGenerator
    ) -> None:
        """A HAVING filter referencing a non-projected ``last(created_at)``
        must build the ranked subquery and emit a HAVING term that resolves
        ``_last_rn`` correctly. Regression for the hidden-filter-aggregate
        path.
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
            # Ranked subquery must exist — the HAVING reference cannot dangle.
            assert "_last_rn" in sql
            assert _re.search(
                r"ROW_NUMBER\(\)\s+OVER\s*\([^)]*ORDER BY\s+orders\.created_at\s+DESC", sql
            ), f"created_at rank window missing:\n{sql}"
            # The HAVING clause must render the rank-based aggregate (not a
            # bare-aggregate fallback that would have nothing to reference).
            having_match = _re.search(r"HAVING(.*)$", sql, _re.DOTALL | _re.IGNORECASE)
            assert having_match is not None, f"HAVING missing:\n{sql}"
            having_sql = having_match.group(1)
            assert _re.search(
                r"MAX\(CASE WHEN _last_rn\s*=\s*1\s+THEN", having_sql
            ), f"HAVING does not reference the rank-based aggregate:\n{having_sql}"
            # Hidden materialised alias must not surface in result keys.
            assert set(res.columns) == {"orders.status", "orders._count"}, (
                f"Hidden filter aggregate leaked: {res.columns!r}\nSQL:\n{sql}"
            )

    async def test_filter_only_last_two_time_cols(
        self, generator: SQLGenerator
    ) -> None:
        """HAVING references TWO last() aggregates over different time
        columns — both rank columns exist, the HAVING terms resolve to
        distinct ``_last_rn{suffix}`` columns.
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
            assert "_last_rn" in sql and "_last_rn_2" in sql
            # The outer wrap fires (hidden materialised aggregates exist
            # for the filter refs), so HAVING lives in the INNER base
            # SELECT — pull the inner SQL and look at HAVING there. Both
            # rn columns must be referenced (one per filter operand) so
            # distinct time cols don't collapse.
            outer_from = _outer_from_node(sql)
            assert isinstance(outer_from, sqlglot.exp.Subquery)
            inner_sql = outer_from.this.sql(dialect="postgres")
            having_match = _re.search(
                r"HAVING(.*)$", inner_sql, _re.DOTALL | _re.IGNORECASE,
            )
            assert having_match is not None, (
                f"HAVING missing in inner:\n{inner_sql}"
            )
            having_sql = having_match.group(1)
            having_max_rns = _re.findall(
                r"MAX\(CASE WHEN (_last_rn(?:_\d+)?)\s*=\s*1\s+THEN",
                having_sql,
            )
            assert sorted(having_max_rns) == ["_last_rn", "_last_rn_2"], (
                f"HAVING aggregate rn-suffix routing wrong. Found: "
                f"{having_max_rns!r}\nHAVING:\n{having_sql}"
            )

    async def test_filter_and_order_last_different_time_cols(
        self, generator: SQLGenerator
    ) -> None:
        """HAVING uses ``last(created_at)``, ORDER BY uses
        ``last(updated_at)`` — each must resolve to its own distinct
        ``_last_rn{suffix}``. Combined-surface regression (Codex MED 8).
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
            assert "_last_rn" in sql and "_last_rn_2" in sql
            # HAVING lives in the INNER base SELECT (alongside its GROUP
            # BY); outer wrap carries ORDER BY referencing the
            # materialised alias.
            inner = _outer_from_node(sql).this  # type: ignore[union-attr]
            inner_sql = inner.sql(dialect="postgres")
            having_match = _re.search(
                r"HAVING(.*)$", inner_sql, _re.DOTALL | _re.IGNORECASE,
            )
            assert having_match is not None, f"HAVING missing in inner:\n{inner_sql}"
            having_sql = having_match.group(1)
            having_uses_first = _re.search(
                r"\b_last_rn\b", having_sql
            ) is not None
            having_uses_second = "_last_rn_2" in having_sql
            assert having_uses_first and not having_uses_second, (
                f"HAVING did not reference the first-bucket _last_rn:\n"
                f"{having_sql}"
            )
            # Outer ORDER BY references the materialised alias for the
            # updated_at first/last (the second bucket).
            terms = _outer_order_terms(sql)
            assert len(terms) == 1, terms
            assert "revenue_last_updated_at" in terms[0][0], (
                f"Outer ORDER BY did not reference the updated_at "
                f"materialised alias:\n{terms}"
            )

    async def test_projected_two_last_different_time_cols_no_default(
        self, generator: SQLGenerator
    ) -> None:
        """Two PROJECTED ``last(created_at)`` / ``last(updated_at)`` on a
        model with NO ``default_time_dimension`` must resolve to DISTINCT
        suffixes. Regression test for the ``_build_agg`` suffix-guard
        bug (currently silently collapses to ``_last_rn`` because
        ``default_time_col`` is None even though every spec carries an
        explicit time arg).
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
            # Both rank columns must exist (and they do today)…
            assert "_last_rn" in sql and "_last_rn_2" in sql
            # …but each projected aggregate must reference its OWN suffix.
            # Today both collapse to ``_last_rn`` (the suffix guard skips
            # when default_time_col is None) — Change 4 fixes this. Parse
            # the SQL and inspect each aliased projection's MAX(CASE WHEN
            # …) so the assertion can't drift across aggregates.
            rn_by_alias = _projection_rn_by_alias(sql)
            assert "orders.lc" in rn_by_alias, (
                f"lc projection missing or not rn-based:\n{sql}"
            )
            assert "orders.lu" in rn_by_alias, (
                f"lu projection missing or not rn-based:\n{sql}"
            )
            assert rn_by_alias["orders.lc"] != rn_by_alias["orders.lu"], (
                f"lc and lu reference the same rank column "
                f"({rn_by_alias['orders.lc']!r}) — distinct-time-col specs collapsed.\n"
                f"SQL:\n{sql}"
            )


    async def test_filtered_first_last_in_having_uses_filtered_rn(
        self, generator: SQLGenerator
    ) -> None:
        """A FILTERED ``last(time_col)`` (``Column.filter`` set) referenced
        from HAVING must render with the dedicated filtered rank column
        (``_last_rn_f0``) and the match-flag column (``_match_f0``) —
        NOT the unfiltered ``_last_rn``. Without this, the HAVING term
        ranks the wrong row (or references a column out of scope).
        Codex review of DEV-1501 PR #159 (Group A).
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
            # The ranked subquery must include the filtered rn column and
            # the match-flag column (filtered-first/last machinery).
            assert "_last_rn_f0" in sql, (
                f"Filtered rank column _last_rn_f0 missing:\n{sql}"
            )
            assert "_match_f0" in sql, (
                f"Filter match-flag column _match_f0 missing:\n{sql}"
            )
            # HAVING must reference the FILTERED rank column, not bare
            # ``_last_rn``. Inner SELECT carries HAVING (outer wrap holds
            # ORDER BY only; this query has no ORDER BY but the materialise
            # +trim wrap fires for the hidden filter aggregate too).
            outer_from = _outer_from_node(sql)
            inner_sql = (
                outer_from.this.sql(dialect="postgres")
                if isinstance(outer_from, sqlglot.exp.Subquery)
                else sql
            )
            having_match = _re.search(
                r"HAVING(.*)$", inner_sql, _re.DOTALL | _re.IGNORECASE,
            )
            assert having_match is not None, (
                f"HAVING missing:\n{inner_sql}"
            )
            having_sql = having_match.group(1)
            assert "_last_rn_f0" in having_sql, (
                f"HAVING does not reference the filtered _last_rn_f0; falls back to "
                f"unfiltered _last_rn (wrong row ranking). HAVING:\n{having_sql}"
            )

    async def test_filtered_first_last_in_nested_having_uses_filtered_rn(
        self, generator: SQLGenerator
    ) -> None:
        """A FILTERED ``last(time_col)`` nested inside a scalar-function
        call (``coalesce(agg, 0)``) on a HAVING expression must still
        bind to the FILTERED rank column. Regression for the
        ``_render_value_key_for_filter`` recursive call sites
        (``ScalarCallKey`` / ``BetweenKey`` / ``InKey``) that previously
        dropped ``aliases_by_slot_id`` through the recursion (Codex
        review of DEV-1501 PR #159 round 2).
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
            outer_from = _outer_from_node(sql)
            inner_sql = (
                outer_from.this.sql(dialect="postgres")
                if isinstance(outer_from, sqlglot.exp.Subquery)
                else sql
            )
            having_match = _re.search(
                r"HAVING(.*)$", inner_sql, _re.DOTALL | _re.IGNORECASE,
            )
            assert having_match is not None, (
                f"HAVING missing in inner:\n{inner_sql}"
            )
            having_sql = having_match.group(1)
            assert "_last_rn_f0" in having_sql, (
                f"Nested-HAVING (ScalarCallKey wrap) did not reference "
                f"filtered _last_rn_f0; aliases_by_slot_id dropped "
                f"through the ScalarCallKey recursion.\nHAVING:\n{having_sql}"
            )

    async def test_composite_only_first_last_triggers_ranked_subquery(
        self, generator: SQLGenerator
    ) -> None:
        """A query whose ONLY first/last reference is INSIDE a composite
        aggregate (no direct first/last sibling) must still trigger the
        ranked-subquery wrap. Without composite-aware detection in
        ``_has_first_last_aggregate``, the query takes the regular base
        path and the composite render emits ``MAX(CASE WHEN _last_rn=1
        …)`` referencing a column the bare FROM never projects. Codex
        review of DEV-1501 PR #159 round 4.
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
            # Ranked subquery must be built with BOTH time-column rn cols.
            assert "_last_rn" in sql and "_last_rn_2" in sql, (
                f"Ranked subquery not built (composite-only first/last "
                f"didn't trigger _has_first_last_aggregate):\n{sql}"
            )
            # Both composite operands must reference distinct rn columns.
            diff_match = _re.search(
                r"MAX\(CASE WHEN (_last_rn(?:_\d+)?)[\s\S]*?\+\s*"
                r"MAX\(CASE WHEN (_last_rn(?:_\d+)?)[\s\S]*?"
                r'AS "orders\.diff"',
                sql,
            )
            assert diff_match is not None, (
                f"composite ``diff`` projection not found:\n{sql}"
            )
            left_rn, right_rn = diff_match.group(1), diff_match.group(2)
            assert {left_rn, right_rn} == {"_last_rn", "_last_rn_2"}, (
                f"Composite operands collapsed: left={left_rn!r}, "
                f"right={right_rn!r}\nSQL:\n{sql}"
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
        (``paid_amount:last(created_at) + 1``) must bind to the
        FILTERED rank column (``_last_rn_f0``) and match flag — not
        bare ``_last_rn`` + raw filter. The composite synth's per-leaf
        alias must match the alias the ranked subquery's
        ``filtered_rn_map`` was keyed by. Codex review of DEV-1501 PR
        #159 round 6.
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
            assert "_last_rn_f0" in sql, (
                f"Filtered rank column _last_rn_f0 missing:\n{sql}"
            )
            assert "_match_f0" in sql, (
                f"Filter match-flag column _match_f0 missing:\n{sql}"
            )
            # The composite's ``plus1`` projection's MAX must reference
            # the FILTERED rn column — not bare ``_last_rn``.
            assert _re.search(
                r'MAX\(CASE WHEN _last_rn_f0\s*=\s*1.*?AS "orders\.plus1"',
                sql, _re.DOTALL,
            ), (
                f"Composite operand did not bind to _last_rn_f0; falls back "
                f"to bare _last_rn + raw filter (alias key mismatch).\n{sql}"
            )

    async def test_composite_first_last_in_projection_uses_correct_suffixes(
        self, generator: SQLGenerator
    ) -> None:
        """A COMPOSITE aggregate measure containing first/last operands
        with different explicit time columns
        (``revenue:last(created_at) + revenue:last(updated_at)``) must
        render each operand with its OWN ``_last_rn{suffix}``, not
        collapse to bare ``_last_rn``. Triggered when the query ALSO
        has a direct projected first/last so the first/last branch
        fires — composite renders via ``_render_aggregate_composite_expr``
        which previously didn't receive rn state. Codex review of
        DEV-1501 PR #159 round 3.
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
            # Both rn columns must exist in the ranked subquery.
            assert "_last_rn" in sql and "_last_rn_2" in sql
            # The composite ``diff`` projection must contain BOTH
            # ``_last_rn`` (created_at bucket) AND ``_last_rn_2``
            # (updated_at bucket) — not two copies of ``_last_rn``.
            diff_match = _re.search(
                r"MAX\(CASE WHEN (_last_rn(?:_\d+)?)[\s\S]*?\+\s*"
                r"MAX\(CASE WHEN (_last_rn(?:_\d+)?)[\s\S]*?"
                r'AS "orders\.diff"',
                sql,
            )
            assert diff_match is not None, (
                f"composite ``diff`` projection not found / wrong shape:\n{sql}"
            )
            left_rn, right_rn = diff_match.group(1), diff_match.group(2)
            assert {left_rn, right_rn} == {"_last_rn", "_last_rn_2"}, (
                f"Composite first/last operands collapsed: left={left_rn!r}, "
                f"right={right_rn!r} — expected one of each.\nSQL:\n{sql}"
            )

    async def test_cross_model_query_with_local_first_last_having_filter(
        self, generator: SQLGenerator
    ) -> None:
        """A query carrying BOTH a cross-model aggregate AND a LOCAL
        first/last filter in HAVING (``filters=["revenue:last(created_at)
        > 100"]``) must NOT silently emit a HAVING that references a
        dangling ``_last_rn``. Two acceptable outcomes (both are strict
        improvements over the silent-dangling-SQL bug Codex flagged):
        (a) emit valid SQL with a ranked subquery in ``_base``, or (b)
        raise ``NotImplementedError`` with the existing local-first/last
        + cross-model guard message. Codex review of DEV-1501 PR #159
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
            try:
                sql = (await engine.execute(query, dry_run=True)).sql
            except NotImplementedError as exc:
                # Acceptable outcome (b): the existing guard fires when
                # the host base would need a ranked subquery alongside a
                # cross-model aggregate. The clear error message replaces
                # what was silently dangling-``_last_rn`` SQL.
                assert "first/last" in str(exc).lower(), (
                    f"NotImplementedError raised but with unexpected message: "
                    f"{exc!r}"
                )
                return
            _assert_valid_sql(sql, dialect=generator.dialect)
            # Acceptable outcome (a): if SQL is emitted, the host
            # ``_base`` CTE must build the ranked subquery so HAVING
            # resolves.
            base_cte = _re.search(
                r"_base AS \((.*?)\), _cm_", sql, _re.DOTALL,
            )
            assert base_cte is not None, f"_base CTE not found:\n{sql}"
            base_sql = base_cte.group(1)
            assert "ROW_NUMBER" in base_sql, (
                f"Host _base CTE did NOT build the ranked subquery for the "
                f"local first/last HAVING filter — _last_rn is dangling.\n"
                f"_base:\n{base_sql}"
            )

    async def test_cross_model_filtered_last_in_having(
        self, generator: SQLGenerator
    ) -> None:
        """A FILTERED cross-model ``last()`` (``Column.filter`` set on the
        joined model's column) referenced from HAVING must, inside the
        cross-model CTE, bind the HAVING aggregate to the dedicated
        filtered rank column (``_last_rn_f0``) — same rn the CTE's SELECT
        projects. Without ranked-state threading, the routed HAVING
        re-emits ``_build_agg(synth)`` with no filtered_rn_map and binds
        to bare ``_last_rn`` (wrong row). CodeRabbit review of DEV-1501
        PR #159 (Group A.3).
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
            # The cross-model CTE must build a ranked subquery WITH the
            # FILTERED rn column (``_last_rn_f0``) and the match flag,
            # AND its HAVING must reference the FILTERED rn — not bare
            # ``_last_rn``.
            assert "_last_rn_f0" in sql, (
                f"Cross-model filtered rank column _last_rn_f0 missing:\n{sql}"
            )
            assert "_match_f0" in sql, (
                f"Cross-model filter match-flag column _match_f0 missing:\n{sql}"
            )
            # HAVING in the cross-model CTE must use the FILTERED rn-gated
            # form. Bare ``_last_rn`` in HAVING would rank against unfiltered
            # rows — wrong winner.
            having_match = _re.search(
                r"HAVING(.*?)(?:\)\s*$|\Z)", sql, _re.DOTALL | _re.IGNORECASE,
            )
            assert having_match is not None, (
                f"Cross-model HAVING missing:\n{sql}"
            )
            having_sql = having_match.group(1)
            assert "_last_rn_f0" in having_sql, (
                f"Cross-model HAVING does not reference filtered _last_rn_f0; "
                f"falls back to unfiltered _last_rn (wrong row ranking).\n"
                f"HAVING:\n{having_sql}\nSQL:\n{sql}"
            )

    async def test_derived_time_arg_pulls_in_referenced_join(
        self, generator: SQLGenerator,
    ) -> None:
        """DEV-1501 (Codex round 8): a DERIVED first/last time arg whose
        ``Column.sql`` references a joined column must pull that join into
        the base FROM. ``_resolve_explicit_time_col`` expands
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
            assert "_last_rn" in sql
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

    async def test_hidden_row_order_target_raises_nyi(
        self, generator: SQLGenerator
    ) -> None:
        """ORDER BY a non-projected ROW column (e.g. ``customer_id``) is
        not a supported shape and must raise NotImplementedError — both
        today and after Change 2. Guards against broad ``include_order=
        True`` accidentally materialising hidden row slots and silently
        changing GROUP BY grain.
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
            with pytest.raises(NotImplementedError):
                await engine.execute(query, dry_run=True)

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
        """When the outer wrap fires (hidden order/filter aggregate
        materialised), ORDER BY / LIMIT / OFFSET MUST live on the OUTER
        SELECT — the inner base SELECT must NOT carry them, else the
        pagination is applied at the wrong level and may slice rows
        before the materialised aggregate values are visible.
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
            # Inner base SELECT (inside the outer-wrap Subquery) must NOT
            # carry ORDER BY / LIMIT / OFFSET — those are owned by the
            # outer wrap.
            outer_from = _outer_from_node(sql)
            assert isinstance(outer_from, sqlglot.exp.Subquery), (
                f"Expected outer-wrap Subquery FROM:\n{sql}"
            )
            inner = outer_from.this
            assert isinstance(inner, sqlglot.exp.Select), (
                f"Outer-wrap subquery body is not a Select:\n{sql}"
            )
            assert inner.args.get("order") is None, (
                f"Inner base SELECT still carries ORDER BY:\n{sql}"
            )
            assert inner.args.get("limit") is None, (
                f"Inner base SELECT still carries LIMIT:\n{sql}"
            )
            assert inner.args.get("offset") is None, (
                f"Inner base SELECT still carries OFFSET:\n{sql}"
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

    def test_hidden_composite_order_rejected_at_input_validation(
        self, generator: SQLGenerator
    ) -> None:
        """Composite-aggregate ORDER BY (an operator over aggregates) is
        REJECTED at ``OrderItem`` input validation — the string syntax
        ``"revenue:sum - cost:sum"`` becomes an invalid identifier and
        Pydantic raises ``ValidationError`` before the query reaches the
        planner. So hidden composite order is structurally unreachable
        in the no-transform path, and Change 3 needs no explicit raise.
        """
        from pydantic import ValidationError as PydanticValidationError

        with pytest.raises(PydanticValidationError):
            OrderItem(column="revenue:sum - cost:sum", direction="desc")


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
            query,
            claim_amount_model,
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
        """The isolated _cm_ CTE for a last measure must contain a ROW_NUMBER
        ranked subquery and produce valid SQL (not reference non-existent _last_rn)."""
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
            f"_cm_ CTE for latest_payment must contain ROW_NUMBER:\n{cm_body}"
        )
        assert "_last_rn" in cm_body, (
            f"_cm_ CTE must have _last_rn column:\n{cm_body}"
        )
        # The aggregate must use MAX(CASE WHEN _last_rn = 1 ...)
        assert "MAX(CASE WHEN" in cm_body, (
            f"_cm_ CTE must use MAX(CASE WHEN _last_rn = 1 ...):\n{cm_body}"
        )
        # Full SQL must parse as valid
        _assert_valid_sql(sql)

    async def test_mixed_isolated_and_local_first_last(
        self, generator: SQLGenerator, claim_amount_model_with_time, related_models,
    ) -> None:
        """Mixed case: one non-isolated last stays in host _base with ranked subquery,
        one isolated last goes to its own _cm_ CTE with its own ranked subquery."""
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

        # Host _base SHOULD have ROW_NUMBER (for the non-isolated total_amount:last).
        base_body = _extract_cte_body(sql, r"_base")
        assert "ROW_NUMBER" in base_body, (
            f"_base must have ROW_NUMBER for non-isolated last measure:\n{base_body}"
        )

        # Isolated measure should get its own _cm_ CTE with its own ranked subquery.
        cm_body = _extract_cte_body(sql, r"_cm_\w*latest_payment\w*")
        assert "ROW_NUMBER" in cm_body, (
            f"isolated _cm_ CTE must contain its own ROW_NUMBER:\n{cm_body}"
        )

        # Non-isolated measure should be in the base.
        assert "total_amount" in base_body, (
            f"Non-isolated total_amount should be in base:\n{base_body}"
        )

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

        # The _cm_ CTE should use first (ASC ordering). Balanced-paren
        # walker — body carries the nested ranked subquery.
        cm_body = _extract_cte_body(sql, r"_cm_\w*earliest_reserve\w*")

        assert "_first_rn" in cm_body, (
            f"_cm_ CTE should use _first_rn for 'first' aggregation:\n{cm_body}"
        )
        # ASC ordering for first.
        assert "ASC" in cm_body, f"Expected ASC ordering for first:\n{cm_body}"
        # Should reference the explicit time column (updated_at), not the default TD.
        assert "updated_at" in cm_body, (
            f"Expected explicit time_column 'updated_at' in _cm_ CTE:\n{cm_body}"
        )
        _assert_valid_sql(sql)

    async def test_multiple_isolated_first_last_separate_ctes(
        self, generator: SQLGenerator, claim_amount_model_with_time, related_models,
    ) -> None:
        """Two isolated first/last measures produce separate _cm_ CTEs, no ROW_NUMBER in base."""
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

        # Two separate _cm_ CTEs for the two filtered first/last measures.
        cm_cte_names = _re.findall(r"(_cm_\w+)\s+AS\s*\(", sql)
        filtered_names = [
            n for n in cm_cte_names if "latest_payment" in n or "latest_reserve" in n
        ]
        assert len(filtered_names) == 2, (
            f"Expected 2 filtered _cm_ CTEs, got {len(filtered_names)}: {filtered_names}\n{sql}"
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
                Column(name="region_code", sql="customers__regions.code", type=DataType.TEXT),
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
        ORDER BY must materialise its ranked _last_rn INSIDE the host-rooted
        _cm_ sub-plan (where the measure is isolated), and the outer ORDER BY
        resolves through the CTE's joined-back column.

        Exercises the DEV-1501 × DEV-1503 interaction: parametric first/last
        in ORDER BY adds hidden rank materialisation; for an ISOLATED
        filtered-local measure that materialisation happens inside the _cm_
        CTE, not the host base.

        Pins Codex review #8 (parametric first/last filtered-local) — ORDER BY half.
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

        # The isolated _cm_ CTE contains the ranked _last_rn machinery (the
        # parametric last is materialised inside the sub-plan).
        cm_body = _extract_cte_body(sql, r"_cm_\w*latest_payment\w*")
        assert "_last_rn" in cm_body, (
            f"Isolated _cm_ CTE must materialise _last_rn for parametric last:\n{cm_body}"
        )
        # The window's ORDER BY must use updated_at (the explicit time arg),
        # not the default TD (created_at).
        assert "updated_at" in cm_body, (
            f"_cm_ CTE must reference the explicit time arg 'updated_at':\n{cm_body}"
        )
        # The outer ORDER BY references the joined-back aggregate alias —
        # NOT _last_rn (which is scoped to the CTE).
        terms = _outer_order_terms(sql)
        assert any("latest_pmt" in expr or "latest_payment" in expr for expr, _ in terms), (
            f"Outer ORDER BY must reference the filtered-local aggregate alias; got {terms}\nSQL:\n{sql}"
        )
        _assert_valid_sql(sql)

    async def test_filtered_local_parametric_last_in_having(
        self, generator: SQLGenerator, claim_amount_model_with_time, related_models,
    ) -> None:
        """A filtered-local PARAMETRIC ``last(updated_at)`` referenced from a
        host filter (HAVING-style) routes as an AGGREGATE-phase filter to the
        outer combined WHERE wrapper. The ranked _last_rn materialisation
        stays inside the _cm_ sub-plan; the outer wrapper applies the
        comparison against the joined-back aggregate alias.

        Pins Codex review #8 (parametric first/last filtered-local) — HAVING half.
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

        # The _cm_ CTE materialises the ranked _last_rn.
        cm_body = _extract_cte_body(sql, r"_cm_\w*latest_payment\w*")
        assert "_last_rn" in cm_body, (
            f"Isolated _cm_ CTE must contain _last_rn:\n{cm_body}"
        )
        assert "updated_at" in cm_body, (
            f"_cm_ CTE must reference explicit time arg 'updated_at':\n{cm_body}"
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
        order_match = _re.search(r"ORDER BY[^\n]+", sql)
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
            query, host_via_subquery,
            extra_models=[loss_payment, backing_raw],
            validate=False,
        )
        # Filtered measure isolated into its own _cm_ CTE; the subquery FROM
        # for the host renders inside it.
        assert "_cm_" in sql and "loss_payment_amt" in sql
        # Host's ``sql=...`` subquery body renders verbatim somewhere in the
        # SQL — sqlglot may pretty-print across multiple lines, so check
        # whitespace-tolerantly (the host SELECT body is ``SELECT * FROM
        # Claim_Amount`` regardless of formatting).
        sql_collapsed = _re.sub(r"\s+", " ", sql)
        assert "SELECT * FROM Claim_Amount" in sql_collapsed, (
            f"host subquery FROM should render inside the _cm_ CTE:\n{sql}"
        )
        _assert_valid_sql(sql)


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

        with patch.object(engine, "_resolve_datasource", new_callable=AsyncMock, return_value=mock_ds):
            async def capture_types(sql):
                return {"orders.revenue_max": "number", "orders.customer_score_max": "number"}

            mock_client = MagicMock()
            mock_client.get_column_types = capture_types
            engine._sql_clients["sqlite://"] = mock_client

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

    @_XFAIL_WINDOWED
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

    @_XFAIL_WINDOWED
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
            ("clickhouse", "SUBSTR(orders.status, 1, 5)"),
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
            # SQLite normalises CONCAT(...) → a || b at emit time.
            ("sqlite", "orders.status || orders.status"),
            ("postgres", "CONCAT(orders.status, orders.status)"),
            ("mysql", "CONCAT(orders.status, orders.status)"),
            ("duckdb", "CONCAT(orders.status, orders.status)"),
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
