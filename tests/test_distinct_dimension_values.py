"""DEV-1543 — ``distinct_dimension_values`` flag on ``SlayerQuery``.

Default ``True`` preserves the current Cube.js-style auto-dedup behaviour
(dim-only queries emit ``GROUP BY <dim aliases>``). Setting to ``False``
emits a flat projection (``SELECT <dims/td-exprs> FROM ... WHERE ...
ORDER BY ... LIMIT ...``) — no ``GROUP BY``. Any measure reference (in
``measures``, ``filters``, or ``order``) is rejected with
``DistinctDimensionValuesError``.

Layered validation:
* Construction-time (Pydantic) — structural checks only: ``measures``
  non-empty rejected; both ``dimensions`` and ``time_dimensions`` empty
  rejected.
* Enrichment-time — authoritative: ``filters`` / ``order`` parsed with
  full model context (custom aggregations, named measures, post-variable
  substitution); any measure-reference shape rejected here.
"""

import sqlite3
import tempfile

import pytest

from slayer.core.enums import DataType, JoinType
from slayer.core.errors import DistinctDimensionValuesError, SlayerError
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import (
    ColumnRef,
    OrderItem,
    SlayerQuery,
    TimeDimension,
)
from slayer.engine.enrichment import enrich_query
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.generator import SQLGenerator
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Helpers (mirror tests/test_filter_renamed_measure.py and
# tests/test_engine_namespacing.py).
# ---------------------------------------------------------------------------


async def _noop_async(**kw):  # NOSONAR(S7503) — async callback contract
    return None


def _orders_model(
    *,
    measures: list[ModelMeasure] | None = None,
    aggregations: list[Aggregation] | None = None,
    joins: list[ModelJoin] | None = None,
) -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="test_ds",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        ],
        measures=measures or [],
        aggregations=aggregations or [],
        joins=joins or [],
    )


def _customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="test_ds",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="region", sql="region", type=DataType.TEXT),
        ],
    )


async def _generate(query: SlayerQuery, model: SlayerModel) -> str:
    enriched = await enrich_query(
        query=query,
        model=model,
        resolve_dimension_via_joins=_noop_async,
        resolve_cross_model_measure=_noop_async,
        resolve_join_target=_noop_async,
    )
    return SQLGenerator(dialect="postgres").generate(enriched=enriched)


async def _engine_with_storage() -> tuple[SlayerQueryEngine, tempfile.TemporaryDirectory]:
    """An engine backed by a YAML store seeded with one ``test_ds``
    datasource. Caller saves models then runs queries. ``tmp`` returned so
    the test can ``.cleanup()`` afterwards.
    """
    tmp = tempfile.TemporaryDirectory()
    storage = YAMLStorage(base_dir=tmp.name)
    await storage.save_datasource(
        DatasourceConfig(name="test_ds", type="sqlite", database=":memory:")
    )
    engine = SlayerQueryEngine(storage=storage)
    return engine, tmp


# ---------------------------------------------------------------------------
# Construction-time (Pydantic) — structural checks
# ---------------------------------------------------------------------------


class TestConstructionRejects:
    """Cheap structural checks. Deep filter/order parsing moves to
    enrichment, so cases that need model context live in
    ``TestEnrichmentRejects`` below."""

    def test_measures_non_empty_rejected(self) -> None:
        """Case 1: ``measures`` non-empty + flag=False → reject at construct.

        Pydantic v2 wraps ``ValueError``-subclass exceptions (which
        ``DistinctDimensionValuesError`` is) raised in
        ``model_validator(mode="after")`` into a ``ValidationError``.
        The error message bubbles up unchanged either way."""
        with pytest.raises(ValueError) as exc:
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="*:count")],
                distinct_dimension_values=False,
            )
        msg = str(exc.value)
        assert "distinct_dimension_values=False" in msg
        assert "measures" in msg.lower()

    def test_dimensions_and_time_dimensions_both_empty_rejected(self) -> None:
        """Case 2: both empty → nothing to project → reject."""
        with pytest.raises(ValueError) as exc:
            SlayerQuery(
                source_model="orders",
                distinct_dimension_values=False,
            )
        msg = str(exc.value)
        assert "dimensions" in msg.lower()
        assert "time_dimensions" in msg.lower()

    def test_default_true_preserves_existing_behaviour(self) -> None:
        """Case 9: query without the field has the same Pydantic shape."""
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
        )
        assert q.distinct_dimension_values is True

    def test_distinct_dim_values_is_value_error(self) -> None:
        """Subclasses ``ValueError`` so existing ``except ValueError`` works."""
        assert issubclass(DistinctDimensionValuesError, ValueError)
        assert issubclass(DistinctDimensionValuesError, SlayerError)


class TestConstructionAccepts:
    """Cases that build cleanly at construction time even with
    flag=False — deep validation happens at enrichment."""

    def test_dimensions_only(self) -> None:
        """Case 10: dims only, no filter, flag=False → builds."""
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            distinct_dimension_values=False,
        )
        assert q.distinct_dimension_values is False

    def test_dimensions_plus_time_dimensions(self) -> None:
        """Case 11: dims + time_dims, no measures → builds."""
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity="day"),
            ],
            distinct_dimension_values=False,
        )
        assert q.distinct_dimension_values is False

    def test_time_dimensions_only(self) -> None:
        """Case 12: time_dims only → builds (truncation is per-row, not agg)."""
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity="day"),
            ],
            distinct_dimension_values=False,
        )
        assert q.distinct_dimension_values is False

    def test_scalar_filter_no_measure_ref(self) -> None:
        """Case 13: filter that references only base columns → builds."""
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            filters=["amount > 100"],
            distinct_dimension_values=False,
        )
        assert q.distinct_dimension_values is False

    def test_order_on_plain_column(self) -> None:
        """Case 14: plain column order → builds."""
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            order=[OrderItem(column=ColumnRef(name="status"))],
            distinct_dimension_values=False,
        )
        assert q.distinct_dimension_values is False

    def test_filter_with_var_placeholder_builds(self) -> None:
        """Case 14a: ``{var}`` placeholder filter passes Pydantic; the
        post-substitution check is what matters."""
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            filters=["amount > {threshold}"],
            variables={"threshold": 100},
            distinct_dimension_values=False,
        )
        assert q.distinct_dimension_values is False

    def test_quoted_string_with_colon_passes(self) -> None:
        """Case 14b: quoted-string literal contains a colon — not a measure
        ref. Must NOT be rejected by a naive colon-substring detector
        (Pydantic-time check)."""
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            filters=["status == 'a:b'"],
            distinct_dimension_values=False,
        )
        assert q.distinct_dimension_values is False


class TestEnrichmentAccepts:
    """The companion to ``TestConstructionAccepts``: queries that survive
    full enrichment with flag=False."""

    async def test_quoted_string_with_colon_survives_enrichment(self) -> None:
        """Case 14b (full path): the colon-in-quoted-string filter MUST
        survive enrichment too — the post-substitution measure-ref check
        must not produce a false positive on literal text."""
        model = _orders_model()
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            filters=["status == 'a:b'"],
            distinct_dimension_values=False,
        )
        sql = await _generate(q, model)
        assert "GROUP BY" not in _normalise_sql(sql), sql

    async def test_filter_with_var_placeholder_resolves_to_scalar(self) -> None:
        """Case 14a (full path): ``{var}`` resolves to a scalar comparison
        and survives enrichment."""
        model = _orders_model()
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            filters=["amount > {threshold}"],
            variables={"threshold": 100},
            distinct_dimension_values=False,
        )
        sql = await _generate(q, model)
        assert "GROUP BY" not in _normalise_sql(sql), sql


# ---------------------------------------------------------------------------
# Enrichment-time (semantic) — authoritative measure-reference check
# ---------------------------------------------------------------------------


class TestEnrichmentRejects:
    """Each case: query passes Pydantic construction; then enrichment
    raises ``DistinctDimensionValuesError``."""

    async def _expect_reject(self, query: SlayerQuery, model: SlayerModel) -> str:
        with pytest.raises(DistinctDimensionValuesError) as exc:
            await _generate(query, model)
        return str(exc.value)

    async def test_filter_colon_agg(self) -> None:
        """Case 3: ``"revenue:sum > 100"`` → reject."""
        model = _orders_model()
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            filters=["amount:sum > 100"],
            distinct_dimension_values=False,
        )
        msg = await self._expect_reject(q, model)
        assert "amount:sum" in msg or "aggregation" in msg.lower()

    async def test_filter_star_count(self) -> None:
        """Case 4: ``"*:count > 0"`` → reject."""
        model = _orders_model()
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            filters=["*:count > 0"],
            distinct_dimension_values=False,
        )
        await self._expect_reject(q, model)

    async def test_filter_transform_call(self) -> None:
        """Case 5: ``"rank(amount:sum) <= 5"`` → reject."""
        model = _orders_model()
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            filters=["rank(amount:sum) <= 5"],
            distinct_dimension_values=False,
        )
        await self._expect_reject(q, model)

    async def test_filter_arithmetic_over_aggregation(self) -> None:
        """Case 6: ``"amount:sum / amount > 0"`` → reject."""
        model = _orders_model()
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            filters=["amount:sum / amount > 0"],
            distinct_dimension_values=False,
        )
        await self._expect_reject(q, model)

    async def test_order_raw_formula_contains_measure(self) -> None:
        """Case 7: ``raw_formula="amount:sum"`` on order → reject."""
        model = _orders_model()
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            order=[OrderItem(column=ColumnRef(name="status"), raw_formula="amount:sum")],
            distinct_dimension_values=False,
        )
        await self._expect_reject(q, model)

    async def test_order_on_bare_model_measure_name(self) -> None:
        """Case 15: order column resolves to a saved ``ModelMeasure``."""
        model = _orders_model(
            measures=[ModelMeasure(formula="amount:sum", name="aov")],
        )
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            order=[OrderItem(column=ColumnRef(name="aov"))],
            distinct_dimension_values=False,
        )
        await self._expect_reject(q, model)

    async def test_filter_on_bare_model_measure_name(self) -> None:
        """Case 16: filter resolves to a saved ``ModelMeasure``."""
        model = _orders_model(
            measures=[ModelMeasure(formula="amount:sum", name="aov")],
        )
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            filters=["aov > 100"],
            distinct_dimension_values=False,
        )
        await self._expect_reject(q, model)

    async def test_filter_custom_aggregation_function_style(self) -> None:
        """Case 16a: filter calls a user-defined custom aggregation."""
        model = _orders_model(
            aggregations=[
                Aggregation(
                    name="weighted_revenue",
                    formula="SUM({amount} * {weight})",
                    params=[
                        AggregationParam(name="amount", sql="amount"),
                        AggregationParam(name="weight", sql="amount"),
                    ],
                ),
            ],
        )
        # Custom aggregation via colon syntax: `amount:weighted_revenue(weight=...)`
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            filters=["amount:weighted_revenue(weight=amount) > 0"],
            distinct_dimension_values=False,
        )
        with pytest.raises(DistinctDimensionValuesError):
            await _generate(q, model)

    async def test_order_function_style_aggregate(self) -> None:
        """Case 16b: ``order=[{"column": "sum(amount)"}]`` — function-style
        aggregate via raw_formula. (Equivalent to a measure reference.)"""
        model = _orders_model()
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            order=[OrderItem(column=ColumnRef(name="sum_amount"), raw_formula="sum(amount)")],
            distinct_dimension_values=False,
        )
        with pytest.raises(DistinctDimensionValuesError):
            await _generate(q, model)

    async def test_filter_post_variable_substitution(self) -> None:
        """Case 16d: ``{var}`` substitution reveals an aggregation. The check
        MUST fire after substitution, not before — the construction-time
        check is structural only."""
        model = _orders_model()
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            filters=["{agg_filter}"],
            variables={"agg_filter": "amount:sum > 100"},
            distinct_dimension_values=False,
        )
        with pytest.raises(DistinctDimensionValuesError):
            await _generate(q, model)


# ---------------------------------------------------------------------------
# Error message content
# ---------------------------------------------------------------------------


class TestErrorMessage:
    async def test_message_names_offending_filter(self) -> None:
        """Case 29: error message points at the offending filter text."""
        model = _orders_model()
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            filters=["amount:sum > 100"],
            distinct_dimension_values=False,
        )
        with pytest.raises(DistinctDimensionValuesError) as exc:
            await _generate(q, model)
        # The offending filter text must appear in the message.
        assert "amount:sum > 100" in str(exc.value)

    async def test_message_points_at_the_fix(self) -> None:
        """Case 30: error message includes the remediation hint."""
        model = _orders_model()
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            filters=["amount:sum > 100"],
            distinct_dimension_values=False,
        )
        with pytest.raises(DistinctDimensionValuesError) as exc:
            await _generate(q, model)
        msg = str(exc.value).lower()
        # Mention either "distinct_dimension_values=True" or "remove" / "drop".
        assert (
            "distinct_dimension_values=true" in msg
            or "remove" in msg
            or "drop" in msg
        )

    async def test_message_names_offending_order(self) -> None:
        """Order-item form of case 29: error message must name the
        offending order spec, not just filters."""
        model = _orders_model()
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            order=[OrderItem(column=ColumnRef(name="status"), raw_formula="amount:sum")],
            distinct_dimension_values=False,
        )
        with pytest.raises(DistinctDimensionValuesError) as exc:
            await _generate(q, model)
        msg = str(exc.value)
        # Either the offending raw_formula text or "order" must appear.
        assert "amount:sum" in msg or "order" in msg.lower()

    def test_construction_message_names_offending_field(self) -> None:
        """Construction-time message points at ``measures`` or
        ``dimensions/time_dimensions``."""
        with pytest.raises(ValueError) as exc:
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="*:count")],
                distinct_dimension_values=False,
            )
        assert "measures" in str(exc.value).lower()


# ---------------------------------------------------------------------------
# SQL generation — no GROUP BY when flag=False, GROUP BY when True
# ---------------------------------------------------------------------------


def _normalise_sql(sql: str) -> str:
    """Collapse whitespace for substring assertions."""
    return " ".join(sql.split()).upper()


class TestSQLGeneration:
    async def test_flag_false_no_group_by_with_dimensions(self) -> None:
        """Case 17: ``dim=[a,b], flag=False`` emits no GROUP BY."""
        model = _orders_model()
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status"), ColumnRef(name="customer_id")],
            distinct_dimension_values=False,
        )
        sql = await _generate(q, model)
        assert "GROUP BY" not in _normalise_sql(sql), sql
        # Both columns are projected.
        assert "status" in sql.lower()
        assert "customer_id" in sql.lower()

    async def test_flag_false_no_group_by_with_time_dimension(self) -> None:
        """Case 18: time_dims-only emits truncation as a projected column,
        no GROUP BY."""
        model = _orders_model()
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                TimeDimension(dimension=ColumnRef(name="created_at"), granularity="day"),
            ],
            distinct_dimension_values=False,
        )
        sql = await _generate(q, model)
        assert "GROUP BY" not in _normalise_sql(sql), sql
        # The DATE_TRUNC expression should appear.
        assert "date_trunc" in sql.lower() or "datetrunc" in sql.lower()

    async def test_flag_false_emits_where_clause(self) -> None:
        """Case 19: filter survives, no GROUP BY."""
        model = _orders_model()
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            filters=["amount > 100"],
            distinct_dimension_values=False,
        )
        sql = await _generate(q, model)
        norm = _normalise_sql(sql)
        assert "WHERE" in norm
        assert "GROUP BY" not in norm

    async def test_flag_false_emits_order_and_limit(self) -> None:
        """Case 20: ORDER BY + LIMIT survive, no GROUP BY."""
        model = _orders_model()
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            order=[OrderItem(column=ColumnRef(name="status"))],
            limit=10,
            distinct_dimension_values=False,
        )
        sql = await _generate(q, model)
        norm = _normalise_sql(sql)
        assert "ORDER BY" in norm
        assert "LIMIT" in norm
        assert "GROUP BY" not in norm

    async def test_flag_true_default_keeps_dim_only_dedup(self) -> None:
        """Case 21: regression pin — default ``True`` keeps GROUP BY."""
        model = _orders_model()
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            # No explicit flag — default True.
        )
        sql = await _generate(q, model)
        assert "GROUP BY" in _normalise_sql(sql), sql


# ---------------------------------------------------------------------------
# Cross-model + multi-stage
# ---------------------------------------------------------------------------


class TestCrossModelDimensions:
    async def test_cross_model_dimensions_no_dedup(self) -> None:
        """Case 22: ``customers.region`` joins customers; with flag=False,
        no GROUP BY → raw rows surface (duplicates preserved by SQL)."""
        engine, tmp = await _engine_with_storage()
        try:
            cust = _customers_model()
            await engine.storage.save_model(cust)
            orders = _orders_model(
                joins=[
                    ModelJoin(
                        target_model="customers",
                        join_pairs=[["customer_id", "id"]],
                        join_type=JoinType.LEFT,
                    ),
                ],
            )
            await engine.storage.save_model(orders)
            q = SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="region", model="customers")],
                distinct_dimension_values=False,
            )
            resp = await engine.execute(q, dry_run=True)
            assert resp.sql is not None
            assert "GROUP BY" not in _normalise_sql(resp.sql), resp.sql
        finally:
            tmp.cleanup()


class TestMultiStageDag:
    async def test_inner_stage_raw_outer_aggregates(self) -> None:
        """Case 23: inner stage flag=False produces flat CTE; outer
        aggregates from it. Outer SQL has GROUP BY, inner CTE does not."""
        engine, tmp = await _engine_with_storage()
        try:
            orders = _orders_model()
            await engine.storage.save_model(orders)
            inner = SlayerQuery(
                source_model="orders",
                name="raw_rows",
                dimensions=[ColumnRef(name="status"), ColumnRef(name="amount")],
                distinct_dimension_values=False,
            )
            outer = SlayerQuery(
                source_model="raw_rows",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="amount:sum")],
                # Outer is default True (with aggregation, GROUP BY is mandatory).
            )
            resp = await engine.execute([inner, outer], dry_run=True)
            assert resp.sql is not None
            sql = resp.sql
            # The engine wraps inner stages as nested subqueries (not
            # WITH-CTEs), aliased ``AS <stage_name>``. Inner produces
            # raw rows; outer aggregates.
            assert "SUM(" in sql.upper()
            # Stronger check: GROUP BY appears EXACTLY once — only in the
            # outer aggregating SELECT, not in any inner raw-row stage.
            assert _normalise_sql(sql).count("GROUP BY") == 1, (
                f"GROUP BY should appear exactly once (outer aggregation), "
                f"not in inner raw-rows stages:\n{sql}"
            )
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# End-to-end execution against a real SQLite database (proves the dedup
# is actually OFF, not just absent from the SQL string).
# ---------------------------------------------------------------------------


@pytest.fixture
async def sqlite_orders_env(tmp_path):
    """Real SQLite with duplicate ``status`` values so the dedup is
    observable in the response row count."""
    db_path = tmp_path / "orders.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            amount REAL NOT NULL,
            customer_id INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    rows = [
        # 6 rows, 3 unique status values: completed (x3), pending (x2), cancelled (x1)
        (1, "completed", 100.0, 1, "2025-01-15"),
        (2, "completed", 200.0, 2, "2025-01-20"),
        (3, "pending", 50.0, 1, "2025-02-10"),
        (4, "cancelled", 75.0, 3, "2025-02-15"),
        (5, "completed", 300.0, 2, "2025-03-05"),
        (6, "pending", 25.0, 3, "2025-03-20"),
    ]
    cur.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()

    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=str(db_path))
    )
    await storage.save_model(
        SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            ],
        )
    )
    return SlayerQueryEngine(storage=storage)


class TestEndToEndExecution:
    async def test_flag_false_returns_raw_rows(self, sqlite_orders_env) -> None:
        """Case 24: row count > unique-tuple count → dedup is OFF."""
        engine = sqlite_orders_env
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            distinct_dimension_values=False,
        )
        resp = await engine.execute(q)
        # 6 rows in the source table, 3 unique status values. flag=False
        # must return 6 rows.
        assert resp.row_count == 6
        # All values should be present (with duplicates).
        statuses = [row["orders.status"] for row in resp.data]
        assert statuses.count("completed") == 3
        assert statuses.count("pending") == 2
        assert statuses.count("cancelled") == 1

    async def test_flag_true_dedupes(self, sqlite_orders_env) -> None:
        """Case 25: default flag=True returns 3 distinct tuples."""
        engine = sqlite_orders_env
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
        )
        resp = await engine.execute(q)
        assert resp.row_count == 3


# ---------------------------------------------------------------------------
# Surfaces — MCP, REST, SlayerClient
# ---------------------------------------------------------------------------


class TestMCPSurface:
    async def test_mcp_query_flag_false_returns_raw_rows(self, sqlite_orders_env) -> None:
        """Case 26: MCP ``query`` tool with flag=False returns raw rows.

        Uses ``mcp_server.call_tool(name=..., arguments=...)`` — same
        pattern as ``tests/test_mcp_server.py:_call``."""
        import json

        from slayer.mcp.server import create_mcp_server
        engine = sqlite_orders_env
        server = create_mcp_server(storage=engine.storage)
        content_blocks, _ = await server.call_tool(
            name="query",
            arguments={
                "source_model": "orders",
                "dimensions": ["status"],
                "distinct_dimension_values": False,
                "format": "json",
            },
        )
        result = content_blocks[0].text
        payload = json.loads(result)
        # MCP `query` with format=json returns a JSON array of rows.
        assert isinstance(payload, list)
        # Raw-row mode: 6 rows in the source table.
        assert len(payload) == 6

    async def test_mcp_query_nested_inner_flag_false(self, sqlite_orders_env) -> None:
        """Case 26a: ``query_nested`` with inner stage flag=False feeds
        raw rows into an outer stage that aggregates."""
        import json

        from slayer.mcp.server import create_mcp_server
        engine = sqlite_orders_env
        server = create_mcp_server(storage=engine.storage)
        content_blocks, _ = await server.call_tool(
            name="query_nested",
            arguments={
                "queries": [
                    {
                        "source_model": "orders",
                        "name": "raw_rows",
                        "dimensions": ["status", "amount"],
                        "distinct_dimension_values": False,
                    },
                    {
                        "source_model": "raw_rows",
                        "dimensions": ["status"],
                        "measures": [{"formula": "amount:sum"}],
                    },
                ],
                "format": "json",
            },
        )
        payload = json.loads(content_blocks[0].text)
        # 3 unique status values, all 6 rows aggregated.
        assert isinstance(payload, list)
        assert len(payload) == 3

    async def test_mcp_query_nested_outer_flag_false_with_measure_rejects(
        self, sqlite_orders_env,
    ) -> None:
        """Case 26b: outer stage flag=False AND outer measure ref →
        rejected. The MCP tool surfaces the error (raises or returns an
        error-shaped result — either is acceptable)."""
        from mcp.server.fastmcp.exceptions import ToolError

        from slayer.mcp.server import create_mcp_server
        engine = sqlite_orders_env
        server = create_mcp_server(storage=engine.storage)
        with pytest.raises((ToolError, DistinctDimensionValuesError, ValueError)):
            await server.call_tool(
                name="query_nested",
                arguments={
                    "queries": [
                        {
                            "source_model": "orders",
                            "name": "raw_rows",
                            "dimensions": ["status", "amount"],
                        },
                        {
                            "source_model": "raw_rows",
                            "dimensions": ["status"],
                            # Outer has flag=False but also a measure → reject.
                            "measures": [{"formula": "amount:sum"}],
                            "distinct_dimension_values": False,
                        },
                    ],
                    "format": "json",
                },
            )


class TestRESTSurface:
    def test_query_request_schema_has_flag(self) -> None:
        """OpenAPI documentation: ``QueryRequest`` must expose the field
        as an explicit Pydantic field (not just pass-through via
        ``extra="allow"``)."""
        from slayer.api.server import QueryRequest
        assert "distinct_dimension_values" in QueryRequest.model_fields

    def test_post_query_with_flag_false(self, tmp_path) -> None:
        """Case 27: REST ``POST /query`` with ``"distinct_dimension_values": false``."""
        from fastapi.testclient import TestClient

        from slayer.api.server import create_app
        from slayer.async_utils import run_sync

        storage = _seed_sqlite_storage_sync(tmp_path)
        app = create_app(storage=storage)
        client = TestClient(app)
        resp = client.post(
            "/query",
            json={
                "source_model": "orders",
                "dimensions": ["status"],
                "distinct_dimension_values": False,
            },
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["row_count"] == 6
        del run_sync  # silence unused

    def test_post_query_list_shape_per_stage_flag(self, tmp_path) -> None:
        """Case 27a: REST list shape — per-stage flag passes through."""
        from fastapi.testclient import TestClient

        from slayer.api.server import create_app
        storage = _seed_sqlite_storage_sync(tmp_path)
        app = create_app(storage=storage)
        client = TestClient(app)
        resp = client.post(
            "/query",
            json={
                "queries": [
                    {
                        "source_model": "orders",
                        "name": "raw_rows",
                        "dimensions": ["status", "amount"],
                        "distinct_dimension_values": False,
                    },
                    {
                        "source_model": "raw_rows",
                        "dimensions": ["status"],
                        "measures": [{"formula": "amount:sum"}],
                    },
                ],
            },
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        # 3 unique statuses in the source data.
        assert body["row_count"] == 3


def _seed_sqlite_storage_sync(tmp_path):
    """Sync helper for non-async tests (REST TestClient runs sync). Seeds
    the same SQLite db + storage as ``sqlite_orders_env`` but blocking."""
    from slayer.async_utils import run_sync

    db_path = tmp_path / "orders_sync.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            amount REAL NOT NULL,
            customer_id INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    rows = [
        (1, "completed", 100.0, 1, "2025-01-15"),
        (2, "completed", 200.0, 2, "2025-01-20"),
        (3, "pending", 50.0, 1, "2025-02-10"),
        (4, "cancelled", 75.0, 3, "2025-02-15"),
        (5, "completed", 300.0, 2, "2025-03-05"),
        (6, "pending", 25.0, 3, "2025-03-20"),
    ]
    cur.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()
    storage_dir = tmp_path / "storage_sync"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))
    run_sync(
        storage.save_datasource(
            DatasourceConfig(name="ds", type="sqlite", database=str(db_path))
        )
    )
    run_sync(
        storage.save_model(
            SlayerModel(
                name="orders",
                sql_table="orders",
                data_source="ds",
                columns=[
                    Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                    Column(name="status", sql="status", type=DataType.TEXT),
                    Column(name="amount", sql="amount", type=DataType.DOUBLE),
                    Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                    Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                ],
            )
        )
    )
    return storage


class TestSlayerClientBody:
    def test_build_body_includes_flag(self) -> None:
        """Case 28: ``SlayerClient._build_query_body`` includes the flag
        when the SlayerQuery sets it."""
        from slayer.client.slayer_client import SlayerClient
        q = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            distinct_dimension_values=False,
        )
        body = SlayerClient._build_query_body(q)
        assert body.get("distinct_dimension_values") is False

    def test_build_body_list_shape_per_stage(self) -> None:
        """Case 28a: list-input body preserves per-stage flag."""
        from slayer.client.slayer_client import SlayerClient
        inner = SlayerQuery(
            source_model="orders",
            name="raw_rows",
            dimensions=[ColumnRef(name="status")],
            distinct_dimension_values=False,
        )
        outer = SlayerQuery(
            source_model="raw_rows",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum")],
        )
        body = SlayerClient._build_query_body([inner, outer])
        assert "queries" in body
        assert body["queries"][0].get("distinct_dimension_values") is False
        # Outer stage default-True must round-trip the same.
        assert body["queries"][1].get("distinct_dimension_values") is not False
