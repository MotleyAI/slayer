"""DEV-1444: outermost SELECT must project exactly user-declared dims+measures.

These tests pin the contract that the rendered outermost SELECT projects
only the user-declared ``dimensions + time_dimensions + measures`` of the
final stage, in declared order — never the intermediate columns that the
engine hoists into CTEs for SQL feasibility (window functions, scalar
arithmetic over aggregates, ORDER BY aggregates, filter post-conditions).

Phase 1 lands the full suite as failing tests; Phase 2 turns them green.
"""
from __future__ import annotations

import importlib
import inspect
import re
from typing import Any

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, DatasourceConfig, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.enriched import (
    CrossModelMeasure,
    EnrichedExpression,
    EnrichedMeasure,
    EnrichedQuery,
    EnrichedTransform,
)
from slayer.engine.enrichment import enrich_query
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.generator import SQLGenerator
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Defensive late-imports for helpers introduced by DEV-1444. The test file
# must be collectable even before the implementation lands; tests that need
# these symbols fail explicitly when they're missing instead of breaking
# pytest collection.
# ---------------------------------------------------------------------------
_enriched_mod = importlib.import_module("slayer.engine.enriched")
public_projection_aliases = getattr(_enriched_mod, "public_projection_aliases", None)

_enrichment_mod = importlib.import_module("slayer.engine.enrichment")
canonical_expression_key = getattr(_enrichment_mod, "canonical_expression_key", None)


# ---------------------------------------------------------------------------
# SQL inspection helpers — implementation-agnostic. They walk the rendered
# SQL via sqlglot and answer two questions: what does the outermost SELECT
# project, and which aliases appear inside the CTE / inner layers.
# ---------------------------------------------------------------------------
def _norm(s: str) -> str:
    return " ".join(s.split())


def _outer_select_columns(sql: str, *, dialect: str = "postgres") -> list[str]:
    """Return the list of alias names projected by the OUTERMOST SELECT.

    For ``SELECT a, b FROM (...)`` the outer projection is ``[a, b]``.
    For ``WITH ... SELECT a, b FROM step1 LIMIT N`` the outer projection
    is also ``[a, b]`` — sqlglot models the WITH clause as a sibling of
    the top SELECT.
    """
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    if not isinstance(parsed, exp.Select):  # pragma: no cover — defensive
        return []
    out: list[str] = []
    for proj in parsed.expressions:
        # ``alias_or_name`` returns the alias if present, else the bare name.
        out.append(proj.alias_or_name)
    return out


def _all_aliases_in_sql(sql: str) -> list[str]:
    """Return every alias-name that appears as a quoted identifier in the SQL.

    Useful for "alias X appears in some CTE" assertions without committing
    to a particular CTE layout.
    """
    return re.findall(r'"([^"]+)"', sql)


def _outer_order_by_references(sql: str, *, dialect: str = "postgres") -> list[str]:
    """Return identifier names referenced by the outermost ORDER BY clause."""
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    if not isinstance(parsed, exp.Select):  # pragma: no cover
        return []
    order = parsed.args.get("order")
    if order is None:
        return []
    refs: list[str] = []
    for ordered in order.expressions:
        col = ordered.this
        if isinstance(col, exp.Column):
            refs.append(col.name)
        else:
            refs.append(col.sql(dialect=dialect))
    return refs


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
async def _noop(**kw: Any) -> None:  # NOSONAR(S7503) — engine API requires an awaitable callback even if this stub never needs to await
    return None


async def _enrich(query: SlayerQuery, model: SlayerModel):
    return await enrich_query(
        query=query,
        model=model,
        resolve_dimension_via_joins=_noop,
        resolve_cross_model_measure=_noop,
        resolve_join_target=_noop,
    )


async def _generate(
    generator: SQLGenerator,
    query: SlayerQuery,
    model: SlayerModel,
    *,
    render_mode: str = "outer",
):
    """Enrich + generate SQL. Passes ``render_mode`` when the generator
    accepts it (DEV-1444 introduces the param); otherwise falls back to
    the legacy signature so this file remains collectable on every commit.
    """
    enriched = await _enrich(query, model)
    sig = inspect.signature(generator.generate)
    if "render_mode" in sig.parameters:
        sql = generator.generate(enriched=enriched, render_mode=render_mode)
    else:
        sql = generator.generate(enriched=enriched)
    return sql, enriched


@pytest.fixture
def funds_model() -> SlayerModel:
    """Mirrors the ``exchange_traded_funds.funds`` shape from the DEV-1444 repros."""
    return SlayerModel(
        name="funds",
        sql_table="funds",
        data_source="test",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="geozone", sql="geozone", type=DataType.TEXT),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="expensenet", sql="expensenet", type=DataType.DOUBLE),
            Column(name="benchmarkexp", sql="benchmarkexp", type=DataType.DOUBLE),
            Column(name="average_daily_value_traded_3m", sql="adv3m", type=DataType.DOUBLE),
        ],
    )


@pytest.fixture
def orders_model() -> SlayerModel:
    """Generic orders model used by edge-case tests."""
    return SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            Column(name="quantity", sql="quantity", type=DataType.DOUBLE),
        ],
    )


@pytest.fixture
def generator() -> SQLGenerator:
    return SQLGenerator(dialect="postgres")


async def _save_test_datasource(storage: YAMLStorage) -> None:
    """DEV-1444 helper: persist a sqlite ``test`` datasource so dry-run/
    execute paths can resolve a dialect for any model whose
    ``data_source='test'``."""
    await storage.save_datasource(
        DatasourceConfig(name="test", type="sqlite", database=":memory:")
    )


@pytest.fixture
async def orders_customers_engine(tmp_path):
    """A storage + engine with orders→customers join for cross-model tests."""
    storage = YAMLStorage(base_dir=str(tmp_path))
    await _save_test_datasource(storage)
    await storage.save_model(SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="revenue", sql="lifetime_revenue", type=DataType.DOUBLE),
        ],
    ))
    orders = SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        default_time_dimension="created_at",
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


# ===========================================================================
# Group A — DEV-1444 repros (the literal three issue reproducers).
# ===========================================================================
class TestDev1444Repros:
    async def test_repro1_order_by_aggregate_not_projected(
        self, generator: SQLGenerator, funds_model: SlayerModel,
    ) -> None:
        """Repro (1): ORDER BY ``expensenet:sum`` with one declared dim and no
        declared measure must project exactly ``[funds.geozone]`` — the
        aggregate must not leak into the outer projection."""
        query = SlayerQuery(
            source_model="funds",
            dimensions=[ColumnRef(name="geozone")],
            order=[OrderItem(column="expensenet:sum", direction="desc")],
            limit=3,
        )
        sql, _ = await _generate(generator, query, funds_model)
        outer_cols = _outer_select_columns(sql)
        assert outer_cols == ["funds.geozone"], (
            f"Outer SELECT must project exactly [funds.geozone], got {outer_cols}.\n"
            f"SQL:\n{sql}"
        )
        # The ORDER BY must still take effect.
        assert "ORDER BY" in sql.upper()
        assert "DESC" in sql.upper()
        assert "LIMIT 3" in sql

    async def test_repro2_rank_of_col_agg_hides_intermediate(
        self, generator: SQLGenerator, funds_model: SlayerModel,
    ) -> None:
        """Repro (2): ``rank(expensenet:sum)`` named ``expense_rank`` must
        produce a 2-column outer SELECT (geozone, expense_rank). The hoisted
        ``expensenet_sum`` stays inside a CTE, never in the outer projection."""
        query = SlayerQuery(
            source_model="funds",
            dimensions=[ColumnRef(name="geozone")],
            measures=[ModelMeasure(formula="rank(expensenet:sum)", name="expense_rank")],
            limit=3,
        )
        sql, _ = await _generate(generator, query, funds_model)
        outer_cols = _outer_select_columns(sql)
        assert outer_cols == ["funds.geozone", "funds.expense_rank"], (
            f"Outer SELECT must project [funds.geozone, funds.expense_rank], "
            f"got {outer_cols}.\nSQL:\n{sql}"
        )
        # The intermediate must still exist somewhere inside the SQL — it's
        # needed for the RANK() OVER (...) — but not in the outer projection.
        all_aliases = _all_aliases_in_sql(sql)
        assert "funds.expensenet_sum" in all_aliases, (
            f"Intermediate funds.expensenet_sum should still appear inside a "
            f"CTE.\nSQL:\n{sql}"
        )
        assert outer_cols.count("funds.expensenet_sum") == 0

    async def test_repro3_scalar_formula_subaggs_hidden(
        self, generator: SQLGenerator, funds_model: SlayerModel,
    ) -> None:
        """Repro (3): ``expensenet:avg + benchmarkexp:avg`` named ``combined_avg``
        must produce a 2-column outer SELECT (geozone, combined_avg). Both
        hoisted sub-aggregates stay in CTEs."""
        query = SlayerQuery(
            source_model="funds",
            dimensions=[ColumnRef(name="geozone")],
            measures=[ModelMeasure(formula="expensenet:avg + benchmarkexp:avg", name="combined_avg")],
            limit=3,
        )
        sql, _ = await _generate(generator, query, funds_model)
        outer_cols = _outer_select_columns(sql)
        assert outer_cols == ["funds.geozone", "funds.combined_avg"], (
            f"Outer SELECT must project [funds.geozone, funds.combined_avg], "
            f"got {outer_cols}.\nSQL:\n{sql}"
        )
        assert "funds.expensenet_avg" not in outer_cols
        assert "funds.benchmarkexp_avg" not in outer_cols


# ===========================================================================
# Group B — Projection order.
# ===========================================================================
class TestProjectionOrder:
    async def test_dims_then_measures_in_declared_order(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Outer SELECT emits columns in declared order: dims first (in
        declared order), then measures (in declared order). Not alphabetical."""
        query = SlayerQuery(
            source_model="orders",
            # Intentionally non-alphabetical
            dimensions=[ColumnRef(name="status"), ColumnRef(name="region")],
            measures=[
                ModelMeasure(formula="revenue:sum", name="z_total"),
                ModelMeasure(formula="*:count", name="a_count"),
            ],
        )
        sql, _ = await _generate(generator, query, orders_model)
        outer_cols = _outer_select_columns(sql)
        expected = [
            "orders.status",
            "orders.region",
            "orders.z_total",
            "orders.a_count",
        ]
        assert outer_cols == expected, (
            f"Outer SELECT order must match declared order.\n"
            f"expected: {expected}\ngot:      {outer_cols}\nSQL:\n{sql}"
        )

    async def test_time_dimensions_included_in_projection(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Time dimensions are always public — they appear in the outer
        projection alongside regular dimensions, in
        ``dimensions + time_dimensions + measures`` declared order."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="*:count", name="rows")],
        )
        sql, _ = await _generate(generator, query, orders_model)
        outer_cols = _outer_select_columns(sql)
        # Exact projection: dim, then time-dim, then measure, in declared order.
        assert outer_cols == ["orders.status", "orders.created_at", "orders.rows"], (
            f"Outer SELECT must project [dim, time_dim, measure] in declared "
            f"order.\ngot: {outer_cols}\nSQL:\n{sql}"
        )


# ===========================================================================
# Group C — Window-transform argument reuse (structural equality).
# Spec rule 4: when a window transform's argument structurally equals an
# already-declared measure's canonical inner expression, reuse that
# alias inside OVER(...) and skip the duplicate hoist.
# ===========================================================================
class TestWindowArgReuse:
    # 12 of the 13 spec-listed window transforms have a single measure as
    # their inner argument — those are the ones where structural-equality
    # reuse fires when ``revenue:sum`` is independently declared. The 13th,
    # ``consecutive_periods``, takes a boolean predicate (``revenue:sum > 0``)
    # rather than a single measure; its projection-trim contract is exercised
    # by ``test_consecutive_periods_projection_trim`` further down.
    @pytest.mark.parametrize(
        "transform_formula,measure_name",
        [
            ("rank(revenue:sum)",                       "r_rank"),
            ("percent_rank(revenue:sum)",               "r_pct_rank"),
            ("dense_rank(revenue:sum)",                 "r_dense"),
            ("ntile(revenue:sum, n=3)",                 "r_ntile"),
            ("cumsum(revenue:sum)",                     "r_cumsum"),
            ("lag(revenue:sum, -1)",                    "r_lag"),
            ("lead(revenue:sum, 1)",                    "r_lead"),
            ("first(revenue:sum)",                      "r_first"),
            ("last(revenue:sum)",                       "r_last"),
            ("time_shift(revenue:sum, -1, 'year')",     "r_shift"),
            ("change(revenue:sum)",                     "r_change"),
            ("change_pct(revenue:sum)",                 "r_change_pct"),
        ],
    )
    async def test_window_transform_with_declared_inner_measure(
        self,
        generator: SQLGenerator,
        orders_model: SlayerModel,
        transform_formula: str,
        measure_name: str,
    ) -> None:
        """When the inner ``revenue:sum`` is independently declared as a
        named measure, the outer SELECT projects exactly the dim + named
        measure + transform measure, and the OVER (...) clause references
        the declared measure's alias rather than a freshly-hoisted intermediate.
        """
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                ModelMeasure(formula="revenue:sum", name="total"),
                ModelMeasure(formula=transform_formula, name=measure_name),
            ],
        )
        sql, _ = await _generate(generator, query, orders_model)
        outer_cols = _outer_select_columns(sql)
        expected = ["orders.created_at", "orders.total", f"orders.{measure_name}"]
        assert outer_cols == expected, (
            f"{transform_formula}: outer SELECT must be {expected}, got {outer_cols}.\n"
            f"SQL:\n{sql}"
        )
        # No `_inner_*` duplicate hoist of the inner measure.
        assert "_inner_" not in sql, (
            f"{transform_formula}: no _inner_* duplicate hoist expected.\nSQL:\n{sql}"
        )
        # The reused alias (orders.total) must appear inside the SQL — it
        # is the OVER clause's reference target.
        assert '"orders.total"' in sql, (
            f"{transform_formula}: declared measure alias 'orders.total' must "
            f"be referenced (typically inside OVER (...)).\nSQL:\n{sql}"
        )

    async def test_consecutive_periods_projection_trim(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """The 13th transform — ``consecutive_periods`` — takes a boolean
        predicate, not a single measure, so structural-equality reuse does
        not naturally fire. The projection-trim contract still applies: the
        outer SELECT projects only the declared dim + time-dim + ``streak``,
        and the staged ``cp_reset_*`` / ``cp_value_*`` CTE columns stay
        internal."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="consecutive_periods(revenue:sum > 0)", name="streak")],
        )
        sql, _ = await _generate(generator, query, orders_model)
        outer_cols = _outer_select_columns(sql)
        assert outer_cols == ["orders.status", "orders.created_at", "orders.streak"], (
            f"consecutive_periods: outer projection must be trimmed.\n"
            f"got: {outer_cols}\nSQL:\n{sql}"
        )
        # The internal reset/value CTE aliases must not leak.
        for col in outer_cols:
            assert "_cp_reset_" not in col
            assert "_cp_value_" not in col


# ===========================================================================
# Group D — ORDER BY policy.
# ===========================================================================
class TestOrderByPolicy:
    async def test_order_by_named_measure_uses_alias(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """When order column matches a declared measure name, ORDER BY
        renders via that measure's alias."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="revenue:sum", name="total")],
            order=[OrderItem(column=ColumnRef(name="total"), direction="desc")],
        )
        sql, _ = await _generate(generator, query, orders_model)
        outer_cols = _outer_select_columns(sql)
        assert outer_cols == ["orders.status", "orders.total"]
        # ORDER BY references the declared alias.
        norm = _norm(sql)
        assert 'ORDER BY "orders.total"' in norm or '"orders.total" DESC' in norm

    async def test_order_by_unbound_agg_hoisted_as_hidden_alias_not_projected(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Per the layering refinement, an unbound ORDER BY aggregate is hoisted
        as a hidden alias in the inner SELECT/CTE; the outer SELECT does NOT
        project it; the outer ORDER BY references the hidden alias **by name**
        (not as an inline ``SUM(...)`` expression and not as a raw column)."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            order=[OrderItem(column="revenue:sum", direction="desc")],
            limit=5,
        )
        sql, _ = await _generate(generator, query, orders_model)
        outer_cols = _outer_select_columns(sql)
        # Outer projects only the declared dim.
        assert outer_cols == ["orders.status"], (
            f"Outer SELECT must project only [orders.status], got {outer_cols}.\n"
            f"SQL:\n{sql}"
        )
        # The hidden alias must exist as a quoted identifier inside the SQL
        # (somewhere in an inner CTE / SELECT scope).
        assert "orders.revenue_sum" in _all_aliases_in_sql(sql), (
            f"Hidden alias 'orders.revenue_sum' must appear in inner scope.\n"
            f"SQL:\n{sql}"
        )
        # The outermost ORDER BY must reference that exact hidden alias by
        # name, not the inline aggregate expression.
        order_refs = _outer_order_by_references(sql)
        assert order_refs == ["orders.revenue_sum"], (
            f"Outermost ORDER BY must reference exactly the hidden alias "
            f"'orders.revenue_sum'.\nrefs: {order_refs}\nSQL:\n{sql}"
        )
        # The outermost ORDER BY clause must not contain an inline ``SUM(``.
        parsed = sqlglot.parse_one(sql, dialect="postgres")
        assert isinstance(parsed, exp.Select)
        order = parsed.args.get("order")
        if order is not None:
            order_sql = order.sql(dialect="postgres").upper()
            assert "SUM(" not in order_sql, (
                f"Outermost ORDER BY must not contain inline SUM(...).\n"
                f"order clause: {order_sql}"
            )
        assert "LIMIT 5" in sql

    async def test_order_by_raw_dim_passthrough(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """ORDER BY a declared dim alias works unchanged."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            order=[OrderItem(column=ColumnRef(name="status"), direction="asc")],
        )
        sql, _ = await _generate(generator, query, orders_model)
        outer_cols = _outer_select_columns(sql)
        assert "orders.status" in outer_cols
        assert "ORDER BY" in sql.upper()


# ===========================================================================
# Group E — Response attributes alignment.
# ===========================================================================
class TestResponseAttributesAlignment:
    async def test_attributes_have_no_entries_for_hidden_columns(
        self, funds_model: SlayerModel, tmp_path,
    ) -> None:
        """The response's attributes dict must not contain entries keyed by
        hidden / auto-extracted aliases (``funds.expensenet_sum``,
        ``funds.benchmarkexp_avg`` from repro 3). Note: the contract is NOT
        one-to-one with projection — attributes only include entries that
        have labels or formats — so we only assert hidden aliases are absent.
        """
        storage = YAMLStorage(base_dir=str(tmp_path))
        await _save_test_datasource(storage)
        await storage.save_model(funds_model)
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(
            source_model="funds",
            dimensions=[ColumnRef(name="geozone")],
            measures=[ModelMeasure(formula="expensenet:avg + benchmarkexp:avg", name="combined_avg")],
            limit=3,
        )
        resp = await engine.execute(query=query, dry_run=True)
        all_attr_keys = set(resp.attributes.dimensions) | set(resp.attributes.measures)
        for hidden in ("funds.expensenet_avg", "funds.benchmarkexp_avg"):
            assert hidden not in all_attr_keys, (
                f"Hidden alias {hidden!r} must not appear in response attributes.\n"
                f"keys: {sorted(all_attr_keys)}"
            )

    async def test_attributes_match_outer_projection_for_repros(
        self, funds_model: SlayerModel, tmp_path,
    ) -> None:
        """The attribute keys are a subset of the outer projection — never a
        superset. (One-to-one not required by the ResponseAttributes contract.)
        """
        storage = YAMLStorage(base_dir=str(tmp_path))
        await _save_test_datasource(storage)
        await storage.save_model(funds_model)
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(
            source_model="funds",
            dimensions=[ColumnRef(name="geozone")],
            measures=[ModelMeasure(formula="rank(expensenet:sum)", name="expense_rank")],
            limit=3,
        )
        resp = await engine.execute(query=query, dry_run=True)
        # resp.columns must match the outer projection (set + order).
        assert resp.columns == ["funds.geozone", "funds.expense_rank"], (
            f"dry_run resp.columns must match outer projection.\n"
            f"got: {resp.columns}"
        )
        # No hidden alias in attributes.
        all_attr_keys = set(resp.attributes.dimensions) | set(resp.attributes.measures)
        assert "funds.expensenet_sum" not in all_attr_keys

    async def test_attributes_omit_filter_extracted_hidden_aliases(
        self, orders_model: SlayerModel, tmp_path,
    ) -> None:
        """Filter expressions like ``change(revenue:sum) > 0`` auto-extract
        hidden EnrichedTransform / EnrichedExpression entries internally
        (``_ts*`` / ``_ft*`` aliases) — those MUST NOT appear as keys in
        the response attributes dict. This is the highest-risk path because
        ``query_engine.py`` currently iterates ``enriched.expressions /
        .transforms`` unconditionally when building attributes."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        await _save_test_datasource(storage)
        await storage.save_model(orders_model)
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="revenue:sum", name="total")],
            filters=["change(revenue:sum) > 0"],
        )
        resp = await engine.execute(query=query, dry_run=True)
        all_attr_keys = set(resp.attributes.dimensions) | set(resp.attributes.measures)
        # No filter-extracted ``_ts*`` / ``_ft*`` / ``_inner_*`` aliases.
        for key in all_attr_keys:
            assert "_ts" not in key, (
                f"Filter-extracted time-shift alias {key!r} leaked into attributes."
            )
            assert "_ft" not in key, (
                f"Filter-extracted hidden alias {key!r} leaked into attributes."
            )
            assert "_inner_" not in key, (
                f"Inner-only alias {key!r} leaked into attributes."
            )
            assert "change" not in key.lower(), (
                f"Hidden change(...) alias {key!r} leaked into attributes."
            )
        # The declared public columns are the only valid attribute keys.
        assert all_attr_keys.issubset({"orders.created_at", "orders.total"}), (
            f"attributes contain non-public keys: "
            f"{all_attr_keys - {'orders.created_at', 'orders.total'}}"
        )


# ===========================================================================
# Group F — Edge cases.
# ===========================================================================
class TestEdgeCases:
    async def test_dim_only_dedup_unchanged(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Dim-only queries still emit GROUP BY <all dims> and project exactly
        the declared dims."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status"), ColumnRef(name="region")],
            limit=100,
        )
        sql, _ = await _generate(generator, query, orders_model)
        outer_cols = _outer_select_columns(sql)
        assert outer_cols == ["orders.status", "orders.region"]
        assert "GROUP BY" in sql.upper()
        # GROUP BY must apply BEFORE LIMIT — pin the existing dim-only-dedup
        # invariant (otherwise the trim wrapper could move LIMIT to a layer
        # above GROUP BY and silently drop unique tuples past row N).
        upper = sql.upper()
        assert upper.index("GROUP BY") < upper.index("LIMIT 100")

    async def test_measure_only_query(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Measure-only queries project exactly the declared measures."""
        query = SlayerQuery(
            source_model="orders",
            measures=[
                ModelMeasure(formula="revenue:sum", name="total"),
                ModelMeasure(formula="*:count", name="n"),
            ],
        )
        sql, _ = await _generate(generator, query, orders_model)
        outer_cols = _outer_select_columns(sql)
        assert outer_cols == ["orders.total", "orders.n"]

    async def test_filter_on_hidden_change_field_does_not_leak(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Filters that auto-extract a hidden change/transform field still
        filter correctly, but the hidden field never appears in the outer
        projection."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="revenue:sum", name="total")],
            filters=["change(revenue:sum) > 0"],
        )
        sql, _ = await _generate(generator, query, orders_model)
        outer_cols = _outer_select_columns(sql)
        assert outer_cols == ["orders.created_at", "orders.total"], (
            f"Outer SELECT must project only [created_at, total].\n"
            f"got: {outer_cols}\nSQL:\n{sql}"
        )
        # No hidden ``change_*`` or ``_ft*`` alias leaking into the projection.
        for col in outer_cols:
            assert "change" not in col.lower()
            assert "_ft" not in col

    async def test_named_override_alias(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Explicit ``name`` on a measure → projection uses the user's name."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="revenue:sum", name="rev")],
        )
        sql, _ = await _generate(generator, query, orders_model)
        outer_cols = _outer_select_columns(sql)
        assert outer_cols == ["orders.status", "orders.rev"]
        # The canonical-form alias must NOT appear in outer projection.
        assert "orders.revenue_sum" not in outer_cols

    async def test_star_count_named_measure(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """``{"formula": "*:count", "name": "rows"}`` projects as ``orders.rows``."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count", name="rows")],
        )
        sql, _ = await _generate(generator, query, orders_model)
        outer_cols = _outer_select_columns(sql)
        assert outer_cols == ["orders.status", "orders.rows"]

    async def test_rank_inside_arithmetic_reuses_named_arg(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """A formula like ``rank(revenue:sum) + 1`` whose inner argument is
        already a declared named measure must reuse that alias and not
        re-materialize the inner aggregate."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                ModelMeasure(formula="revenue:sum", name="total"),
                ModelMeasure(formula="rank(revenue:sum) + 1", name="rank_plus_one"),
            ],
        )
        sql, _ = await _generate(generator, query, orders_model)
        outer_cols = _outer_select_columns(sql)
        assert outer_cols == ["orders.created_at", "orders.total", "orders.rank_plus_one"]
        assert "_inner_" not in sql, (
            f"No _inner_* duplicate expected when arg is a declared measure.\n"
            f"SQL:\n{sql}"
        )

    async def test_cross_model_dotted_dimension_projection(
        self, orders_customers_engine,
    ) -> None:
        """A dim referenced via a join path (``customers.revenue``) projects
        under the full dotted path, and no extra join-scaffolding columns
        leak into the outer SELECT."""
        engine, orders = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="customers.revenue")],
            measures=[ModelMeasure(formula="*:count", name="n")],
        )
        enriched = await engine._enrich(query=query, model=orders)
        sig = inspect.signature(engine._sql_generator.generate) if hasattr(engine, "_sql_generator") else None
        gen = SQLGenerator(dialect="postgres")
        if sig is not None and "render_mode" in sig.parameters:
            sql = gen.generate(enriched=enriched, render_mode="outer")
        else:
            sql = gen.generate(enriched=enriched)
        outer_cols = _outer_select_columns(sql)
        # The dim alias for the joined column is "orders.customers.revenue".
        assert "orders.customers.revenue" in outer_cols
        assert "orders.n" in outer_cols
        # Nothing else.
        assert set(outer_cols) == {"orders.customers.revenue", "orders.n"}, (
            f"Outer SELECT must contain only the declared columns.\n"
            f"got: {outer_cols}\nSQL:\n{sql}"
        )


# ===========================================================================
# Group G — Multi-stage source_queries regression guard.
# ===========================================================================
class TestMultiStageSourceQueries:
    async def test_inner_stage_keeps_full_projection(
        self, tmp_path, orders_model: SlayerModel,
    ) -> None:
        """A two-stage ``source_queries`` runtime list: the inner stage may
        project hoisted intermediates (its SELECT is not trimmed because the
        outer stage references them); only the outer stage's SELECT obeys
        the trim rule."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        await _save_test_datasource(storage)
        await storage.save_model(orders_model)
        engine = SlayerQueryEngine(storage=storage)
        inner = SlayerQuery(
            name="inner_stage",
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="rank(revenue:sum)", name="rev_rank")],
        )
        outer = SlayerQuery(
            source_model="inner_stage",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="rev_rank:max", name="top_rank")],
        )
        resp = await engine.execute(query=[inner, outer], dry_run=True)
        sql_str = resp.sql or ""
        # Outer-stage projection: trimmed to its declared dims + measures.
        assert resp.columns == ["inner_stage.status", "inner_stage.top_rank"], (
            f"Outer stage of source_queries must obey the trim rule.\n"
            f"got: {resp.columns}\nSQL:\n{sql_str}"
        )
        # Inner stage's emitted SELECT (a CTE / subquery) MUST still expose
        # ``rev_rank`` and any hoisted intermediates — otherwise the outer
        # stage's ``rev_rank:max`` reference is unresolved.
        assert "rev_rank" in sql_str, (
            f"Inner stage must still expose rev_rank for the outer stage.\n"
            f"SQL:\n{sql_str}"
        )


# ===========================================================================
# Group H — Multi-dialect smoke.
# ===========================================================================
_TIER1_DIALECTS = ["postgres", "mysql", "sqlite", "clickhouse", "duckdb"]


class TestMultiDialectProjectionTrim:
    @pytest.mark.parametrize("dialect", _TIER1_DIALECTS)
    async def test_repro2_column_count_matches_declaration(
        self, dialect: str, funds_model: SlayerModel,
    ) -> None:
        """Across every tier-1 dialect, the outer SELECT column count equals
        ``len(dimensions) + len(measures)`` for repro (2)."""
        gen = SQLGenerator(dialect=dialect)
        query = SlayerQuery(
            source_model="funds",
            dimensions=[ColumnRef(name="geozone")],
            measures=[ModelMeasure(formula="rank(expensenet:sum)", name="expense_rank")],
            limit=3,
        )
        sql, _ = await _generate(gen, query, funds_model)
        outer_cols = _outer_select_columns(sql, dialect=dialect)
        assert len(outer_cols) == 2, (
            f"{dialect}: outer SELECT must project 2 columns, got "
            f"{len(outer_cols)} ({outer_cols}).\nSQL:\n{sql}"
        )


# ===========================================================================
# Group I — Render-mode separation.
# ===========================================================================
class TestRenderModeSeparation:
    async def test_generator_accepts_render_mode_parameter(  # NOSONAR(S7503) — async-test-class consistency: sibling tests in this class do await
        self, generator: SQLGenerator, funds_model: SlayerModel,
    ) -> None:
        """``SQLGenerator.generate`` must accept a ``render_mode`` keyword
        argument with values ``"outer"`` and ``"wrapped"``."""
        sig = inspect.signature(generator.generate)
        assert "render_mode" in sig.parameters, (
            f"SQLGenerator.generate must accept render_mode parameter; "
            f"signature: {sig}"
        )

    async def test_wrapped_mode_keeps_full_projection(
        self, generator: SQLGenerator, funds_model: SlayerModel,
    ) -> None:
        """In ``render_mode='wrapped'``, the rendered SELECT exposes every
        hoisted alias (so the wrapper that embeds this SQL — e.g.
        ``_query_as_model`` or an inner stage of source_queries — can
        reference them)."""
        query = SlayerQuery(
            source_model="funds",
            dimensions=[ColumnRef(name="geozone")],
            measures=[ModelMeasure(formula="rank(expensenet:sum)", name="expense_rank")],
        )
        # Use the engine to also drive _query_as_model in wrapped mode below;
        # here we exercise the generator directly with wrapped mode.
        enriched = await _enrich(query, funds_model)
        sig = inspect.signature(generator.generate)
        if "render_mode" not in sig.parameters:
            pytest.fail("render_mode not implemented on SQLGenerator.generate")
        sql_wrapped = generator.generate(enriched=enriched, render_mode="wrapped")
        sql_outer = generator.generate(enriched=enriched, render_mode="outer")
        cols_wrapped = _outer_select_columns(sql_wrapped)
        cols_outer = _outer_select_columns(sql_outer)
        # Wrapped mode keeps the hoisted intermediate; outer mode trims it.
        assert "funds.expensenet_sum" in cols_wrapped, (
            f"wrapped mode must project hoisted intermediate.\n"
            f"got: {cols_wrapped}\nSQL:\n{sql_wrapped}"
        )
        assert "funds.expensenet_sum" not in cols_outer
        assert set(cols_outer) == {"funds.geozone", "funds.expense_rank"}

    async def test_query_as_model_wraps_with_full_projection(
        self, tmp_path, funds_model: SlayerModel,
    ) -> None:
        """When a query is wrapped as a virtual model (the actual
        ``_query_as_model`` call site at ``query_engine.py:1796``), the
        inner SQL must keep every hoisted alias — otherwise the
        virtual-model column list references columns that have been
        trimmed out of the subquery."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        await _save_test_datasource(storage)
        await storage.save_model(funds_model)
        engine = SlayerQueryEngine(storage=storage)
        # Save the repro-(2) query as a query-backed model. Internally this
        # routes through _query_as_model in wrapped mode.
        query = SlayerQuery(
            source_model="funds",
            dimensions=[ColumnRef(name="geozone")],
            measures=[ModelMeasure(formula="rank(expensenet:sum)", name="expense_rank")],
        )
        model = await engine.create_model_from_query(query=query, name="ranked_funds", save=True)
        # The model's `columns` (derived from the wrapped query's projection)
        # must include the hoisted ``expensenet_sum`` intermediate. The
        # backing_query_sql (the wrapper that downstream FROMs reference)
        # must also project that alias.
        col_names = {c.name for c in (model.columns or [])}
        assert "funds.expensenet_sum" in col_names or "expensenet_sum" in col_names or "expensenet_sum" in (model.backing_query_sql or ""), (
            f"_query_as_model wrap (wrapped mode) must keep the hoisted "
            f"``expensenet_sum`` alias accessible to downstream stages.\n"
            f"columns: {col_names}\nbacking_query_sql:\n{model.backing_query_sql}"
        )
        # And the wrapper's column for ``expense_rank`` must of course be
        # present too.
        assert "funds.expense_rank" in col_names or "expense_rank" in col_names

    async def test_runtime_list_final_stage_trimmed_inner_not(
        self, tmp_path, orders_model: SlayerModel,
    ) -> None:
        """When a runtime list of queries is passed to ``engine.execute``,
        the FINAL entry is rendered in outer mode (trim applied); inner
        entries are rendered in wrapped mode (full projection preserved)."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        await _save_test_datasource(storage)
        await storage.save_model(orders_model)
        engine = SlayerQueryEngine(storage=storage)
        inner = SlayerQuery(
            name="ranked",
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="rank(revenue:sum)", name="r")],
        )
        outer = SlayerQuery(
            source_model="ranked",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="r:max", name="top_r")],
        )
        resp = await engine.execute(query=[inner, outer], dry_run=True)
        sql_str = resp.sql or ""
        assert resp.columns == ["ranked.status", "ranked.top_r"]
        # The inner stage exposes the hoisted intermediate (revenue_sum +
        # the rank measure ``r``).
        assert "revenue_sum" in sql_str or '"orders.revenue_sum"' in sql_str
        assert '"orders.r"' in sql_str or "ranked.r" in sql_str


# ===========================================================================
# Group J — Provenance and public-projection helper.
# ===========================================================================
class TestProvenance:
    async def test_user_declared_flag_exists_on_enriched_types(  # NOSONAR(S7503) — async-test-class consistency: sibling tests in this class do await
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """``user_declared: bool`` must be present on EnrichedMeasure /
        EnrichedExpression / EnrichedTransform / CrossModelMeasure."""
        for cls in (EnrichedMeasure, EnrichedExpression, EnrichedTransform, CrossModelMeasure):
            fields = cls.model_fields if hasattr(cls, "model_fields") else cls.__fields__
            assert "user_declared" in fields, (
                f"{cls.__name__} must have a 'user_declared' field"
            )

    async def test_user_declared_true_only_for_declared_measures(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Declared measures get ``user_declared=True``; auto-extracted
        entries (from order-by or filter-extracted hidden fields) get
        ``user_declared=False``."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="revenue:sum", name="total")],
            order=[OrderItem(column="quantity:sum", direction="desc")],
            filters=["change(revenue:sum) > 0"],
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
        )
        enriched = await _enrich(query, orders_model)
        declared_aliases = {m.alias for m in enriched.measures if getattr(m, "user_declared", None) is True}
        # The declared measure ``total`` is user_declared=True.
        assert "orders.total" in declared_aliases
        # Auto-extracted hidden measures (the order-by ``quantity:sum``, the
        # filter-extracted change/time_shift inputs) must NOT be marked
        # user_declared=True.
        for m in enriched.measures:
            if m.alias != "orders.total":
                assert getattr(m, "user_declared", None) is False, (
                    f"non-declared measure {m.alias!r} must have "
                    f"user_declared=False, got {getattr(m, 'user_declared', None)!r}"
                )

    async def test_user_declared_flag_for_expression(
        self, orders_model: SlayerModel,
    ) -> None:
        """A declared scalar formula (``revenue:sum / *:count``) materializes
        as an EnrichedExpression — must be ``user_declared=True``."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="revenue:sum / *:count", name="aov")],
        )
        enriched = await _enrich(query, orders_model)
        declared = [e for e in enriched.expressions if getattr(e, "user_declared", None) is True]
        assert any(e.alias == "orders.aov" for e in declared), (
            f"EnrichedExpression 'orders.aov' must have user_declared=True.\n"
            f"expressions: {[(e.alias, getattr(e, 'user_declared', None)) for e in enriched.expressions]}"
        )

    async def test_user_declared_flag_for_transform(
        self, orders_model: SlayerModel,
    ) -> None:
        """A declared window transform (``cumsum(revenue:sum)``) materializes
        as an EnrichedTransform — must be ``user_declared=True``. The
        auto-extracted inner-arg hoist for ``revenue:sum`` (an
        EnrichedMeasure that the user did NOT declare) must be
        ``user_declared=False``."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="cumsum(revenue:sum)", name="cum")],
        )
        enriched = await _enrich(query, orders_model)
        # The declared transform.
        cum_xforms = [t for t in enriched.transforms if t.alias == "orders.cum"]
        assert cum_xforms and getattr(cum_xforms[0], "user_declared", None) is True
        # Any auto-extracted hidden measure (the inner ``revenue:sum``) must
        # be user_declared=False.
        for m in enriched.measures:
            assert getattr(m, "user_declared", None) is False, (
                f"hidden measure {m.alias!r} (inner arg to cumsum) must be "
                f"user_declared=False"
            )

    async def test_user_declared_flag_for_cross_model_measure(
        self, orders_customers_engine,
    ) -> None:
        """A declared cross-model measure (``customers.revenue:sum``)
        materializes as a CrossModelMeasure — must be ``user_declared=True``.

        Note: the engine's cross-model resolver currently ignores the user
        ``name`` override and emits its own canonical alias
        (``<src>.<hop_path>.<col>_<agg>``); the user_declared flag is the
        contract being pinned here.
        """
        engine, orders = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum", name="cust_rev")],
        )
        enriched = await engine._enrich(query=query, model=orders)
        assert enriched.cross_model_measures, "no CrossModelMeasure was created"
        cm = enriched.cross_model_measures[0]
        assert getattr(cm, "user_declared", None) is True, (
            f"declared CrossModelMeasure {cm.alias!r} must be user_declared=True"
        )

    async def test_enriched_query_has_user_projection_field(
        self, orders_model: SlayerModel,
    ) -> None:
        """``EnrichedQuery`` gains a ``user_projection`` field populated at
        enrichment time in ``dimensions + time_dimensions + measures``
        declaration order. The field exists, is populated, and drives the
        public projection helper."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status"), ColumnRef(name="region")],
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                ModelMeasure(formula="revenue:sum", name="total"),
                ModelMeasure(formula="*:count", name="n"),
            ],
            order=[OrderItem(column="quantity:sum", direction="desc")],
        )
        enriched = await _enrich(query, orders_model)
        user_projection = getattr(enriched, "user_projection", None)
        assert user_projection is not None, (
            "EnrichedQuery.user_projection field is missing"
        )
        # The list has exactly len(dims) + len(time_dims) + len(measures) entries
        # — auto-extracted entries (the order-by quantity:sum) are excluded.
        assert len(user_projection) == 5, (
            f"user_projection must have 5 entries (2 dims + 1 time_dim + 2 "
            f"measures), got {len(user_projection)}"
        )

    async def test_public_projection_aliases_helper_exists(self) -> None:  # NOSONAR(S7503) — async-test-class consistency: sibling tests in this class do await
        """The helper function ``public_projection_aliases`` must exist in
        ``slayer.engine.enriched`` (or wherever the spec puts it)."""
        assert public_projection_aliases is not None, (
            "slayer.engine.enriched.public_projection_aliases must exist"
        )
        assert callable(public_projection_aliases)

    async def test_public_projection_aliases_declaration_order(
        self, orders_model: SlayerModel,
    ) -> None:
        """The helper returns aliases in declared order (dims +
        time_dimensions + measures), omitting auto-extracted entries."""
        if public_projection_aliases is None:
            pytest.fail("public_projection_aliases not implemented")
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status"), ColumnRef(name="region")],
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                ModelMeasure(formula="revenue:sum", name="total"),
                ModelMeasure(formula="*:count", name="n"),
            ],
            order=[OrderItem(column="quantity:sum", direction="desc")],
        )
        enriched = await _enrich(query, orders_model)
        aliases = public_projection_aliases(enriched)
        assert aliases == [
            "orders.status",
            "orders.region",
            "orders.created_at",
            "orders.total",
            "orders.n",
        ], f"unexpected projection order: {aliases}"
        # The hoisted order-by aggregate alias must NOT be in the helper output.
        assert all("quantity_sum" not in a for a in aliases)

    async def test_provenance_merge_when_order_by_matches_declared(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """When the user declares ``{"formula":"revenue:sum","name":"total"}``
        AND orders by ``revenue:sum``, the entries merge: the declared alias
        (``orders.total``) is reused, no phantom ``orders.revenue_sum``."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="revenue:sum", name="total")],
            order=[OrderItem(column="revenue:sum", direction="desc")],
            limit=5,
        )
        sql, enriched = await _generate(generator, query, orders_model)
        outer_cols = _outer_select_columns(sql)
        assert outer_cols == ["orders.status", "orders.total"]
        # No duplicate phantom 'orders.revenue_sum' alias anywhere in the SQL.
        assert "orders.revenue_sum" not in _all_aliases_in_sql(sql), (
            f"Provenance merge must collapse order-by ref into declared 'total' "
            f"alias — no phantom 'orders.revenue_sum'.\nSQL:\n{sql}"
        )
        # And exactly one EnrichedMeasure for ``revenue:sum`` (matched by
        # the source measure name, not by sql — Column.sql for ``revenue``
        # is ``amount``).
        rev_measures = [
            m for m in enriched.measures
            if m.aggregation == "sum" and m.source_measure_name == "revenue"
        ]
        assert len(rev_measures) == 1, (
            f"expected one EnrichedMeasure for revenue:sum after merge, got "
            f"{len(rev_measures)}: {[m.alias for m in rev_measures]}"
        )
        # That single measure must be user_declared=True.
        assert getattr(rev_measures[0], "user_declared", None) is True


# ===========================================================================
# Group K — Dry-run / explain / expected_columns alignment.
# ===========================================================================
class TestDryRunAlignment:
    async def test_dry_run_columns_match_outer_projection(
        self, funds_model: SlayerModel, tmp_path,
    ) -> None:
        """``engine.execute(query, dry_run=True)`` returns ``response.columns``
        that matches the outer SELECT projection — never includes hoisted
        intermediates."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        await _save_test_datasource(storage)
        await storage.save_model(funds_model)
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(
            source_model="funds",
            dimensions=[ColumnRef(name="geozone")],
            measures=[ModelMeasure(formula="expensenet:avg + benchmarkexp:avg", name="combined_avg")],
            limit=3,
        )
        resp = await engine.execute(query=query, dry_run=True)
        assert resp.columns == ["funds.geozone", "funds.combined_avg"]
        # And the actual SQL's outer projection should match too.
        outer = _outer_select_columns(resp.sql or "")
        assert outer == resp.columns

    async def test_empty_result_columns_match_outer_projection(
        self, funds_model: SlayerModel, tmp_path,
    ) -> None:
        """When the query returns zero rows (or is a dry-run), the columns
        field must reflect the public alias list — no hidden aliases."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        await _save_test_datasource(storage)
        await storage.save_model(funds_model)
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery(
            source_model="funds",
            dimensions=[ColumnRef(name="geozone")],
            order=[OrderItem(column="expensenet:sum", direction="desc")],
        )
        resp = await engine.execute(query=query, dry_run=True)
        assert resp.columns == ["funds.geozone"], (
            f"dry_run resp.columns must reflect outer projection.\n"
            f"got: {resp.columns}"
        )


# ===========================================================================
# Group L — Wrapper layering: filter + order + trim interaction.
# ===========================================================================
class TestWrapperLayering:
    async def test_post_filter_plus_order_plus_trim(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Filter referencing a hidden field + ORDER BY + LIMIT must all
        apply correctly with the outer trim in effect. The spec's layering:
          1. inner CTEs / base SELECT (hoisted)
          2. optional post-filter wrapper: SELECT * FROM <inner> WHERE <hidden>
          3. outermost wrapper: SELECT <publics> FROM <step2> ORDER BY ... LIMIT N
        The outermost SELECT owns trim + ORDER + LIMIT; the post-filter wrapper
        lives one layer in.
        """
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="revenue:sum", name="total")],
            filters=["change(revenue:sum) > 0"],
            order=[OrderItem(column="revenue:sum", direction="desc")],
            limit=5,
        )
        sql, _ = await _generate(generator, query, orders_model)
        # Outer projection trimmed.
        outer_cols = _outer_select_columns(sql)
        assert outer_cols == ["orders.created_at", "orders.total"], (
            f"Outer SELECT must project only declared dim + measure.\n"
            f"got: {outer_cols}\nSQL:\n{sql}"
        )
        # AST: outermost is a Select with trim projection + order + limit.
        parsed = sqlglot.parse_one(sql, dialect="postgres")
        assert isinstance(parsed, exp.Select)
        assert parsed.args.get("order") is not None, "outer ORDER BY missing"
        assert parsed.args.get("limit") is not None, "outer LIMIT missing"
        # The outermost SELECT must NOT carry the post-filter WHERE directly
        # — the WHERE belongs to the immediate inner wrapper. (A trim
        # SELECT shouldn't filter on its own subquery columns.)
        outer_where = parsed.args.get("where")
        if outer_where is not None:
            outer_where_sql = outer_where.sql(dialect="postgres")
            # If a WHERE is present at the outer level, it must NOT be the
            # change-based filter — that belongs strictly to an inner wrapper.
            assert "change" not in outer_where_sql.lower(), (
                f"Outermost SELECT must not own the change(...) post-filter; "
                f"that belongs in the immediate inner wrapper.\n"
                f"outer WHERE: {outer_where_sql}\nSQL:\n{sql}"
            )
        # The change-based filter predicate IS applied somewhere in the SQL
        # (substring-level proof that the filter wasn't dropped).
        assert "> 0" in sql, (
            f"Filter predicate ``change(revenue:sum) > 0`` must still apply.\n"
            f"SQL:\n{sql}"
        )


# ===========================================================================
# Group M — Cross-model / isolated ORDER BY hoisted, not projected.
# ===========================================================================
class TestCrossModelOrderBy:
    async def test_order_by_cross_model_agg_hoisted_not_projected(
        self, orders_customers_engine,
    ) -> None:
        """ORDER BY a cross-model aggregate (``customers.revenue:sum``) with
        no matching declared measure: the aggregate is hoisted as a hidden
        internal CTE column, the outer ORDER BY references it via the
        hidden alias by name (NOT inline as a SUM(...) expression), and
        the outer SELECT does NOT project it."""
        engine, orders = orders_customers_engine
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            order=[OrderItem(column="customers.revenue:sum", direction="desc")],
            limit=3,
        )
        enriched = await engine._enrich(query=query, model=orders)
        gen = SQLGenerator(dialect="postgres")
        sig = inspect.signature(gen.generate)
        sql = gen.generate(enriched=enriched, render_mode="outer") if "render_mode" in sig.parameters else gen.generate(enriched=enriched)
        outer_cols = _outer_select_columns(sql)
        assert outer_cols == ["orders.status"], (
            f"Outer SELECT must project only [orders.status].\n"
            f"got: {outer_cols}\nSQL:\n{sql}"
        )
        # The hidden cross-model alias must appear in the SQL (as a CTE
        # column or as an alias in an inner SELECT). Cross-model measure
        # aliases follow the ``<source>.<target>__<col>_<agg>`` pattern;
        # accept either path-aliased or dotted-prefixed forms.
        all_aliases = _all_aliases_in_sql(sql)
        hidden_candidates = [
            a for a in all_aliases
            if "revenue_sum" in a or "revenue:sum" in a
        ]
        assert hidden_candidates, (
            f"Hidden cross-model aggregate alias must appear in the SQL.\n"
            f"aliases: {all_aliases}\nSQL:\n{sql}"
        )
        # Outermost ORDER BY references one of those hidden aliases by name,
        # not as an inline expression.
        order_refs = _outer_order_by_references(sql)
        assert any(r in hidden_candidates for r in order_refs), (
            f"Outermost ORDER BY must reference the hidden cross-model "
            f"alias by name (not inline).\n"
            f"refs: {order_refs}\nhidden candidates: {hidden_candidates}\n"
            f"SQL:\n{sql}"
        )
        # Outermost ORDER BY must not be inline ``SUM(...)``.
        parsed = sqlglot.parse_one(sql, dialect="postgres")
        if isinstance(parsed, exp.Select):
            order = parsed.args.get("order")
            if order is not None:
                assert "SUM(" not in order.sql(dialect="postgres").upper()
        assert "LIMIT 3" in sql


# ===========================================================================
# Group N — Window reuse (structural equality only; name-reuse → DEV-1447).
# ===========================================================================
class TestWindowChainReuse:
    async def test_structural_reuse_inline_subagg_collapses(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """``m1={"formula":"revenue:sum","name":"total"}`` plus
        ``m2={"formula":"rank(revenue:sum)","name":"r"}`` → ``r``'s OVER
        references ``total``'s alias (orders.total), no inner duplicate."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[
                ModelMeasure(formula="revenue:sum", name="total"),
                ModelMeasure(formula="rank(revenue:sum)", name="r"),
            ],
        )
        sql, _ = await _generate(generator, query, orders_model)
        outer_cols = _outer_select_columns(sql)
        assert outer_cols == ["orders.created_at", "orders.total", "orders.r"]
        # No `_inner_*` hoist of the inner sum.
        assert "_inner_" not in sql
        # OVER (...) DESC must reference the orders.total alias (since the
        # rank arg structurally equals the declared 'total' measure).
        # Look for an OVER clause that mentions "orders.total" near it.
        over_match = re.search(r"OVER\s*\([^)]*\)", sql)
        assert over_match is not None
        assert '"orders.total"' in over_match.group(0), (
            f"Rank OVER (...) must reference declared 'orders.total' alias, "
            f"got: {over_match.group(0)}\nSQL:\n{sql}"
        )

    async def test_nested_window_formula_stages_without_duplicate(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """``rank(rank(revenue:sum))`` named ``r2`` stages naturally as
        step1+step2 CTEs; the outer SELECT projects ``[created_at, r2]``
        only; no ``_inner_*`` duplicate hoist."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="rank(rank(revenue:sum))", name="r2")],
        )
        sql, _ = await _generate(generator, query, orders_model)
        outer_cols = _outer_select_columns(sql)
        assert outer_cols == ["orders.created_at", "orders.r2"]
        # Two RANK() OVER (...) windows must appear, stacked across CTE layers.
        rank_count = sql.upper().count("RANK()")
        assert rank_count >= 2, (
            f"nested rank(rank(...)) needs two RANK() windows, found {rank_count}.\n"
            f"SQL:\n{sql}"
        )


# ===========================================================================
# Test 17 (revised), 38, 39, 40, 41 — call-site / contract pins.
# ===========================================================================
class TestCallSitesAndContractPins:
    async def test_get_column_types_uses_outer_render_mode(
        self, orders_model: SlayerModel, tmp_path, monkeypatch,
    ) -> None:
        """``get_column_types`` (query_engine.py:923) must drive the generator
        in ``outer`` mode. Spy on ``SQLGenerator.generate`` and assert it
        was called with ``render_mode='outer'``."""
        storage = YAMLStorage(base_dir=str(tmp_path))
        await _save_test_datasource(storage)
        await storage.save_model(orders_model)
        engine = SlayerQueryEngine(storage=storage)

        # Spy on the generate calls to capture the render_mode used by
        # get_column_types. We replace the bound method on the class with
        # a wrapper that records the kwargs.
        captured: list[dict] = []
        original_generate = SQLGenerator.generate

        def _wrapper(self, *args, **kwargs):
            captured.append(dict(kwargs))
            return original_generate(self, *args, **kwargs)

        monkeypatch.setattr(SQLGenerator, "generate", _wrapper)

        await engine.get_column_types(model_name="orders")

        # Must have been called at least once, and at least one of those
        # calls must explicitly use render_mode="outer". (The probe path
        # may or may not loop; either way the contract is pinned.)
        assert captured, "SQLGenerator.generate was not called by get_column_types"
        outer_calls = [c for c in captured if c.get("render_mode") == "outer"]
        assert outer_calls, (
            f"get_column_types must call SQLGenerator.generate with "
            f"render_mode='outer'. Captured kwargs: {captured}"
        )

    async def test_outer_wrapper_owns_order_limit_offset(
        self, generator: SQLGenerator, orders_model: SlayerModel,
    ) -> None:
        """Per the layering rule, the SAME outermost SELECT carries trim +
        ORDER BY + LIMIT + OFFSET. The rendered SQL has exactly one outermost
        ORDER BY / LIMIT / OFFSET clause."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="revenue:sum", name="total")],
            order=[OrderItem(column="quantity:sum", direction="desc")],
            limit=10,
            offset=20,
        )
        sql, _ = await _generate(generator, query, orders_model)
        # Outer SELECT projects only declared dim + measure.
        outer_cols = _outer_select_columns(sql)
        assert outer_cols == ["orders.status", "orders.total"], (
            f"Outer projection wrong: {outer_cols}\nSQL:\n{sql}"
        )
        # The outermost statement is a Select whose args include order,
        # limit, and offset all together.
        parsed = sqlglot.parse_one(sql, dialect="postgres")
        assert isinstance(parsed, exp.Select)
        assert parsed.args.get("order") is not None, "outer ORDER BY missing"
        assert parsed.args.get("limit") is not None, "outer LIMIT missing"
        assert parsed.args.get("offset") is not None, "outer OFFSET missing"


# ===========================================================================
# Group canonical_expression_key (test 40 + sub-cases).
# ===========================================================================
class TestCanonicalExpressionKey:
    def test_helper_exists(self) -> None:
        """``canonical_expression_key`` must exist in
        ``slayer.engine.enrichment`` (or wherever the spec puts it)."""
        assert canonical_expression_key is not None, (
            "slayer.engine.enrichment.canonical_expression_key must exist"
        )
        assert callable(canonical_expression_key)

    def test_alias_independence(self) -> None:
        """The key for ``revenue:sum`` must be identical whether the user
        attaches a ``name`` override or not. The merge logic relies on this."""
        if canonical_expression_key is None:
            pytest.fail("canonical_expression_key not implemented")
        from slayer.core.formula import parse_formula
        k1 = canonical_expression_key(parse_formula("revenue:sum"))
        k2 = canonical_expression_key(parse_formula("revenue:sum"))
        assert k1 == k2

    def test_stability_across_repeated_parses(self) -> None:
        """Parsing the same string twice yields identical keys."""
        if canonical_expression_key is None:
            pytest.fail("canonical_expression_key not implemented")
        from slayer.core.formula import parse_formula
        k1 = canonical_expression_key(parse_formula("revenue:sum"))
        k2 = canonical_expression_key(parse_formula("revenue:sum"))
        k3 = canonical_expression_key(parse_formula("revenue:sum"))
        assert k1 == k2 == k3

    def test_different_aggregations_have_different_keys(self) -> None:
        if canonical_expression_key is None:
            pytest.fail("canonical_expression_key not implemented")
        from slayer.core.formula import parse_formula
        assert canonical_expression_key(parse_formula("revenue:sum")) != \
               canonical_expression_key(parse_formula("revenue:avg"))

    def test_cross_model_qualifier_participates(self) -> None:
        if canonical_expression_key is None:
            pytest.fail("canonical_expression_key not implemented")
        from slayer.core.formula import parse_formula
        # Plain vs cross-model — different references, different keys.
        assert canonical_expression_key(parse_formula("revenue:sum")) != \
               canonical_expression_key(parse_formula("customers.revenue:sum"))

    def test_ntile_kwargs_participate(self) -> None:
        """``ntile(x:sum, n=3)`` and ``ntile(x:sum, n=4)`` have different
        keys; ``n`` participates in the canonical form."""
        if canonical_expression_key is None:
            pytest.fail("canonical_expression_key not implemented")
        from slayer.core.formula import parse_formula
        assert canonical_expression_key(parse_formula("ntile(revenue:sum, n=3)")) != \
               canonical_expression_key(parse_formula("ntile(revenue:sum, n=4)"))

    def test_kwargs_sorted_in_key(self) -> None:
        """Reordering kwargs in a transform call produces the same key.
        ``ntile(x:sum, n=3, partition_by=status)`` and
        ``ntile(x:sum, partition_by=status, n=3)`` are semantically equal,
        so their canonical keys must be identical (kwargs sorted)."""
        if canonical_expression_key is None:
            pytest.fail("canonical_expression_key not implemented")
        from slayer.core.formula import parse_formula
        k1 = canonical_expression_key(parse_formula(
            "ntile(revenue:sum, n=3, partition_by=status)"
        ))
        k2 = canonical_expression_key(parse_formula(
            "ntile(revenue:sum, partition_by=status, n=3)"
        ))
        assert k1 == k2, (
            f"kwargs order must not affect the canonical key.\n"
            f"k1: {k1!r}\nk2: {k2!r}"
        )

    def test_repeated_parses_of_parameterized_transform(self) -> None:
        """Stability under repeated parsing of a parameterized transform
        formula. Catches non-deterministic keys (e.g., id()-based hashing
        of AST nodes)."""
        if canonical_expression_key is None:
            pytest.fail("canonical_expression_key not implemented")
        from slayer.core.formula import parse_formula
        keys = {canonical_expression_key(parse_formula("ntile(revenue:sum, n=3)")) for _ in range(5)}
        assert len(keys) == 1, (
            f"canonical key must be stable across repeated parses.\n"
            f"distinct keys: {keys}"
        )


# ===========================================================================
# Validation: dim + time_dim alias clash rejection (Codex round-3 finding 2).
# ===========================================================================
class TestDimAndTimeDimClash:
    async def test_dim_and_time_dim_resolving_to_same_alias_rejected(
        self, orders_model: SlayerModel,
    ) -> None:
        """A query that lists the same column as both a regular dimension
        and a time dimension produces ambiguous aliases — reject at
        enrichment time with a concrete ``ValueError``. Catching the
        narrow exception type prevents the test from silently masking
        unrelated failures (e.g. KeyError, AssertionError) as a "rejection".
        """
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="created_at")],
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="*:count")],
        )
        with pytest.raises(ValueError) as exc:
            await _enrich(query, orders_model)
        msg = str(exc.value).lower()
        # Surface message must clearly call out the clashing name OR a
        # duplicate/ambiguity error term — not a generic message.
        assert (
            "created_at" in str(exc.value)
            or "duplicate" in msg
            or "ambig" in msg
            or "clash" in msg
        ), f"Validation error message is too vague: {exc.value!r}"


# ===========================================================================
# Validation: duplicate user-declared canonical aggregation rejection
# (Codex review on PR #134).
# ===========================================================================
class TestDuplicateUserDeclaredCanonical:
    async def test_two_qfields_same_canonical_different_names_rejected(
        self, orders_model: SlayerModel,
    ) -> None:
        """Two user-declared measures with the same canonical aggregation
        but different ``name`` overrides would silently collapse to one
        EnrichedMeasure under the provenance-merge index, leaving the
        second name in ``user_projection`` with no backing aggregate.
        Reject at enrichment time with a concrete ``ValueError``."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="revenue:sum", name="revenue1"),
                ModelMeasure(formula="revenue:sum", name="revenue2"),
            ],
        )
        with pytest.raises(ValueError) as exc:
            await _enrich(query, orders_model)
        msg = str(exc.value).lower()
        assert "canonical" in msg or "collapse" in msg or "same canonical" in msg, (
            f"Validation message must call out the canonical-collision: {exc.value!r}"
        )

    async def test_two_qfields_same_canonical_unnamed_and_named_rejected(
        self, orders_model: SlayerModel,
    ) -> None:
        """Same shape but the first qfield is unnamed (surfaces as the
        canonical alias) and the second has a name. Still ambiguous and
        rejected."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[
                ModelMeasure(formula="revenue:sum"),
                ModelMeasure(formula="revenue:sum", name="other"),
            ],
        )
        with pytest.raises(ValueError):
            await _enrich(query, orders_model)


# ===========================================================================
# Codex review on PR #134: outer ORDER BY references after wrap path.
# ===========================================================================
class TestOuterOrderByQualifierStripping:
    async def test_combined_cte_order_by_inner_qualifier_stripped(
        self, orders_customers_engine,
    ) -> None:
        """Reproduces the bug Codex flagged on PR #134: when
        ``_assemble_combined_sql`` emits ``ORDER BY _base."x"`` AND the
        outer trim wrapper actually fires (because a hoisted hidden
        aggregate forces the wrap path), the detached order ends up at
        the outer ``SELECT public FROM (<inner>) AS _outer`` scope where
        ``_base`` is not in scope. The trim wrapper must strip such
        inner-CTE qualifiers from the detached ORDER BY so the outer
        reference uses the bare alias (which ``_outer`` exposes).
        """
        engine, _ = orders_customers_engine
        # Cross-model measure forces the combined-CTE path; an unbound
        # ORDER BY aggregate adds a hidden hoist that forces the wrap.
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="customers.revenue:sum", name="cust_rev")],
            order=[OrderItem(column="revenue:sum", direction="desc")],
            limit=5,
        )
        resp = await engine.execute(query=query, dry_run=True)
        sql = resp.sql or ""
        # Outer SQL must not have ``_base.`` in the outermost ORDER BY.
        parsed = sqlglot.parse_one(sql, dialect="postgres")
        assert isinstance(parsed, exp.Select)
        order = parsed.args.get("order")
        assert order is not None, f"no ORDER BY in outer SQL.\n{sql}"
        for col in order.find_all(exp.Column):
            assert col.args.get("table") is None, (
                f"Outermost ORDER BY must not carry inner-CTE qualifiers, "
                f"got col {col.sql()!r}.\nSQL:\n{sql}"
            )

    def test_public_projection_aliases_fallback_includes_measures(
        self,
    ) -> None:
        """When ``EnrichedQuery`` is constructed directly (bypassing
        ``enrich_query``) and ``user_projection`` is empty, the fallback
        path must still surface measures / expressions / transforms /
        cross_model_measures whose names are NOT internal-prefixed.
        Filtering by ``user_declared=False`` (the default) would drop
        them entirely."""
        # Directly construct an EnrichedQuery without calling enrich_query.
        enriched = EnrichedQuery(
            model_name="orders",
            sql_table="public.orders",
            dimensions=[],
            measures=[
                EnrichedMeasure(
                    name="revenue_sum",
                    sql="amount",
                    aggregation="sum",
                    alias="orders.revenue_sum",
                    model_name="orders",
                ),
            ],
        )
        # Fallback must include the measure even though user_declared=False.
        assert "orders.revenue_sum" in public_projection_aliases(enriched), (
            "fallback must include non-internal measures (user_declared "
            "filter only applies when user_projection is populated)."
        )

    def test_public_projection_aliases_fallback_excludes_internal_prefixes(
        self,
    ) -> None:
        """Internal-prefixed names (``_inner_*`` / ``_ft*`` / ``_ts*``)
        stay excluded in the fallback — they're auto-extracted hoists,
        never user-visible."""
        enriched = EnrichedQuery(
            model_name="orders",
            sql_table="public.orders",
            measures=[
                EnrichedMeasure(
                    name="_inner_rank_arg",
                    sql="amount",
                    aggregation="sum",
                    alias="orders._inner_rank_arg",
                    model_name="orders",
                ),
                EnrichedMeasure(
                    name="revenue_sum",
                    sql="amount",
                    aggregation="sum",
                    alias="orders.revenue_sum",
                    model_name="orders",
                ),
            ],
        )
        out = public_projection_aliases(enriched)
        assert "orders.revenue_sum" in out
        assert "orders._inner_rank_arg" not in out
