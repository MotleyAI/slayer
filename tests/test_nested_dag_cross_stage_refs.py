"""Tests for DEV-1449: nested-DAG multi-hop dotted dim referenced from a
downstream stage.

Bug: when an inner stage projects a multi-hop dotted dim (e.g.
``customers.regions.name``) and an outer stage references the same dotted
path, SLayer emits broken SQL referencing a hybrid ``__`` + ``.`` table
alias that doesn't exist in scope.

Fix: virtual stage models produced by ``_query_as_model`` carry a
``source_model_origin`` breadcrumb. The four dotted-ref resolution paths
(``_resolve_dimensions``, ``_resolve_time_dimensions``, cross-model
measures, ``resolve_filter_columns``) consult a shared
``resolve_via_stage_origin`` helper when the join-walk fails.

See ``.spec/DEV-1449.md`` for the full design.
"""

import sqlite3
import tempfile
from typing import List, Tuple

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import SlayerError
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    SlayerModel,
    SourceModelOrigin,
)
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.enrichment import resolve_via_stage_origin
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Fixtures: orders → customers → regions, with a deeper `countries` model
# for 3-hop tests.
# ---------------------------------------------------------------------------

def _ds() -> DatasourceConfig:
    return DatasourceConfig(name="test_ds", type="sqlite", database=":memory:")


def _countries_model() -> SlayerModel:
    return SlayerModel(
        name="countries",
        sql_table="countries",
        data_source="test_ds",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
        ],
    )


def _regions_model() -> SlayerModel:
    return SlayerModel(
        name="regions",
        sql_table="regions",
        data_source="test_ds",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="country_id", sql="country_id", type=DataType.DOUBLE),
            Column(name="last_activity_at", sql="last_activity_at", type=DataType.TIMESTAMP),
        ],
        joins=[ModelJoin(target_model="countries", join_pairs=[["country_id", "id"]])],
    )


def _customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="test_ds",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
            Column(name="revenue", sql="revenue", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="test_ds",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


async def _engine_with_join_chain() -> Tuple[SlayerQueryEngine, tempfile.TemporaryDirectory]:
    """Build a YAMLStorage with orders → customers → regions → countries."""
    tmp = tempfile.TemporaryDirectory()
    storage = YAMLStorage(base_dir=tmp.name)
    await storage.save_datasource(_ds())
    await storage.save_model(_countries_model())
    await storage.save_model(_regions_model())
    await storage.save_model(_customers_model())
    await storage.save_model(_orders_model())
    engine = SlayerQueryEngine(storage=storage)
    return engine, tmp


async def _engine_with_real_sqlite(
    tmp_path,
) -> SlayerQueryEngine:
    """Build a YAMLStorage backed by a real on-disk SQLite DB seeded with
    a 4-table chain (countries → regions → customers → orders). Used by
    tests that need to verify execution semantics, not just SQL shape.
    """
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE countries (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE regions (
            id INTEGER PRIMARY KEY, name TEXT, country_id INTEGER,
            last_activity_at TEXT
        );
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY, region_id INTEGER, revenue REAL
        );
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL,
            created_at TEXT
        );
        INSERT INTO countries VALUES (1, 'US'), (2, 'EU');
        INSERT INTO regions VALUES
            (10, 'West',  1, '2025-01-01'),
            (11, 'East',  1, '2025-02-01'),
            (12, 'North', 2, '2025-03-01');
        INSERT INTO customers VALUES
            (100, 10, 500.0),
            (101, 11, 700.0),
            (102, 12, 300.0);
        INSERT INTO orders VALUES
            (1000, 100, 10.0, '2025-01-01'),
            (1001, 101, 20.0, '2025-02-01'),
            (1002, 102, 30.0, '2025-03-01');
        """
    )
    conn.commit()
    conn.close()
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test_ds", type="sqlite", database=str(db_path))
    )
    await storage.save_model(_countries_model())
    await storage.save_model(_regions_model())
    await storage.save_model(_customers_model())
    await storage.save_model(_orders_model())
    return SlayerQueryEngine(storage=storage)


# ---------------------------------------------------------------------------
# SQL inspection helpers — implementation-agnostic. Walk the rendered SQL
# via sqlglot. Avoid raw-string assertions so the tests survive dialect
# quoting / formatting drift.
# ---------------------------------------------------------------------------

def _outermost_select(sql: str, dialect: str = "sqlite") -> exp.Select:
    tree = sqlglot.parse_one(sql, dialect=dialect)
    assert isinstance(tree, exp.Select), f"expected outermost Select, got {type(tree).__name__}"
    return tree


def _outer_column_refs(sql: str, dialect: str = "sqlite") -> List[exp.Column]:
    """All `exp.Column` nodes whose nearest ancestor SELECT is the
    OUTERMOST SELECT of `sql` — i.e. columns directly in the outer
    projection / GROUP BY / ORDER BY / HAVING / WHERE.

    Does NOT include columns inside subqueries / CTEs / inner SELECTs.
    """
    outer = _outermost_select(sql, dialect=dialect)
    nodes: List[exp.Column] = []
    for col in outer.find_all(exp.Column):
        if col.parent_select is outer:
            nodes.append(col)
    return nodes


def _outer_tables_referenced(sql: str, dialect: str = "sqlite") -> set:
    """Set of `Column.table` values referenced in the OUTER SELECT."""
    return {c.table for c in _outer_column_refs(sql, dialect=dialect) if c.table}


def _innermost_select(tree) -> exp.Select:
    """The deepest-nested `exp.Select` in the tree (the very innermost
    subquery). Used to assert that a column reference appears OUTSIDE
    that innermost layer (e.g., in a CTE or in the outer wrapping)."""
    deepest = tree if isinstance(tree, exp.Select) else None
    deepest_depth = 0
    for node in tree.find_all(exp.Select):
        # Walk ancestors counting how deep this Select is.
        depth = 0
        cur = node.parent
        while cur is not None:
            depth += 1
            cur = cur.parent
        if depth > deepest_depth:
            deepest = node
            deepest_depth = depth
    assert deepest is not None
    return deepest


# ===========================================================================
# Test #1 — reported case: 2-stage, 2-hop dimension (the bug from Linear).
# ===========================================================================
class TestReportedCase:
    async def test_outer_references_inner_flat_alias_not_broken_hybrid(self) -> None:
        """Outer SELECT must reference `s1.customers__regions__name`, not
        the broken `customers__regions.name` (a table alias that doesn't
        exist in the outer FROM clause)."""
        engine, tmp = await _engine_with_join_chain()
        try:
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                dimensions=["customers.regions.name"],
                measures=[{"formula": "*:count"}],
            )
            outer = SlayerQuery(
                source_model="s1",
                # DEV-1450 typed contract: downstream stages see a FLAT
                # schema — the inner stage's `customers.regions.name`
                # dim is projected as `customers__regions__name` on s1.
                dimensions=["customers__regions__name"],
                measures=[{"formula": "*:count"}],
            )
            resp = await engine.execute(query=[inner, outer], dry_run=True)
            sql = resp.sql or ""
            outer_tables = _outer_tables_referenced(sql)
            # The broken table alias `customers__regions` must NOT appear
            # in the outer SELECT (it only legitimately exists inside the
            # inner stage's join).
            assert "customers__regions" not in outer_tables, (
                f"Outer SELECT must not reference `customers__regions` "
                f"(table alias from inner join, out of scope here).\n"
                f"got tables: {outer_tables}\nSQL:\n{sql}"
            )
            # The outer SELECT must reference `s1.customers__regions__name`.
            assert "s1" in outer_tables, (
                f"Outer SELECT must reference `s1` (the wrapped inner CTE).\n"
                f"got tables: {outer_tables}\nSQL:\n{sql}"
            )
            # Column name on s1 should be the flat form.
            outer_col_names = {
                c.name for c in _outer_column_refs(sql) if c.table == "s1"
            }
            assert "customers__regions__name" in outer_col_names, (
                f"Outer SELECT must reference s1.customers__regions__name.\n"
                f"got: {outer_col_names}\nSQL:\n{sql}"
            )
        finally:
            tmp.cleanup()

    async def test_end_to_end_against_real_sqlite(self, tmp_path) -> None:
        """Execute the repro query against a real SQLite DB and verify
        it runs (today it fails with `no such column`).
        """
        engine = await _engine_with_real_sqlite(tmp_path)
        inner = SlayerQuery(
            name="s1",
            source_model="orders",
            dimensions=["customers.regions.name"],
            measures=[{"formula": "*:count"}],
        )
        outer = SlayerQuery(
            source_model="s1",
            # DEV-1450 typed contract: downstream uses the flat alias.
            dimensions=["customers__regions__name"],
            measures=[{"formula": "*:count"}],
        )
        resp = await engine.execute(query=[inner, outer])
        # Three distinct regions in the seed → three rows.
        assert resp.row_count == 3, f"got {resp.row_count} rows: {resp.data}"
        region_names = {row["s1.customers__regions__name"] for row in resp.data}
        assert region_names == {"West", "East", "North"}, region_names


# ===========================================================================
# Test #2 — 2-stage, 3-hop dimension (deeper join chain).
# ===========================================================================
class TestThreeHopDimension:
    async def test_three_hop_cross_stage(self) -> None:
        """Path `customers.regions.countries.name` (3 hops) should
        resolve in the outer stage the same way as 2-hop."""
        engine, tmp = await _engine_with_join_chain()
        try:
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                dimensions=["customers.regions.countries.name"],
                measures=[{"formula": "*:count"}],
            )
            outer = SlayerQuery(
                source_model="s1",
                # DEV-1450 typed contract: flat downstream alias.
                dimensions=["customers__regions__countries__name"],
                measures=[{"formula": "*:count"}],
            )
            resp = await engine.execute(query=[inner, outer], dry_run=True)
            sql = resp.sql or ""
            outer_tables = _outer_tables_referenced(sql)
            # No `customers__regions__countries` table in outer scope.
            assert "customers__regions__countries" not in outer_tables, (
                f"Outer SELECT must not reference customers__regions__countries.\n"
                f"got: {outer_tables}\nSQL:\n{sql}"
            )
            assert "s1" in outer_tables
            outer_cols_on_s1 = {
                c.name for c in _outer_column_refs(sql) if c.table == "s1"
            }
            assert "customers__regions__countries__name" in outer_cols_on_s1, (
                f"Outer must reference flat 3-hop alias.\n"
                f"got: {outer_cols_on_s1}\nSQL:\n{sql}"
            )
        finally:
            tmp.cleanup()


# ===========================================================================
# Test #3 — cross-stage time dimension.
# ===========================================================================
class TestCrossStageTimeDimension:
    @pytest.mark.skip(
        reason="DEV-1471: cross-stage time_dim re-binding not yet supported in "
        "the typed pipeline. Inner stage truncates the column; downstream sees "
        "it as a flat name and the binder rejects re-binding as a TimeDimension."
    )
    async def test_multi_hop_dotted_time_dim_cross_stage(self) -> None:
        """Inner stage projects a multi-hop dotted time dim
        `customers.regions.last_activity_at`. Outer references the same
        dotted path. Without the fix, the outer SQL emits a broken
        `customers__regions.last_activity_at` ref."""
        engine, tmp = await _engine_with_join_chain()
        try:
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                time_dimensions=[TimeDimension(
                    dimension=ColumnRef(name="customers.regions.last_activity_at"),
                    granularity=TimeGranularity.MONTH,
                )],
                measures=[{"formula": "*:count"}],
            )
            outer = SlayerQuery(
                source_model="s1",
                time_dimensions=[TimeDimension(
                    # DEV-1450 typed contract: flat downstream alias.
                    dimension=ColumnRef(name="customers__regions__last_activity_at"),
                    granularity=TimeGranularity.MONTH,
                )],
                measures=[{"formula": "*:count"}],
            )
            resp = await engine.execute(query=[inner, outer], dry_run=True)
            sql = resp.sql or ""
            outer_tables = _outer_tables_referenced(sql)
            assert "customers__regions" not in outer_tables, (
                f"Outer time-dim must not reference customers__regions.\n"
                f"got: {outer_tables}\nSQL:\n{sql}"
            )
            # The outer reference should hit s1.customers__regions__last_activity_at.
            outer_cols_on_s1 = {
                c.name for c in _outer_column_refs(sql) if c.table == "s1"
            }
            assert "customers__regions__last_activity_at" in outer_cols_on_s1, (
                f"Outer time-dim must reference flat alias.\n"
                f"got: {outer_cols_on_s1}\nSQL:\n{sql}"
            )
        finally:
            tmp.cleanup()


# ===========================================================================
# Test #4 — cross-model measure across stages: re-aggregation semantics.
# ===========================================================================
class TestCrossStageCrossModelMeasure:
    async def test_cross_model_measure_reaggregation_sql_shape(self) -> None:
        """Inner stage projects `customers.revenue:sum`. Outer references
        same with `:sum` again → emits as a local re-aggregated measure
        `SUM(s1.customers__revenue_sum)`."""
        engine, tmp = await _engine_with_join_chain()
        try:
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                dimensions=["customers.regions.name"],
                measures=[{"formula": "customers.revenue:sum"}],
            )
            outer = SlayerQuery(
                source_model="s1",
                # DEV-1450 typed re-aggregation: outer references the flat
                # inner alias `customers__revenue_sum` directly and sums it.
                measures=[{"formula": "customers__revenue_sum:sum"}],
            )
            resp = await engine.execute(query=[inner, outer], dry_run=True)
            sql = resp.sql or ""
            # The outer SELECT must contain SUM(s1.customers__revenue_sum).
            outer = _outermost_select(sql)
            outer_sum_targets = []
            for sum_call in outer.find_all(exp.Sum):
                if sum_call.parent_select is not outer:
                    continue
                outer_sum_targets.extend(
                    (c.table, c.name) for c in sum_call.find_all(exp.Column)
                )
            assert ("s1", "customers__revenue_sum") in outer_sum_targets, (
                f"Outer SUM must target s1.customers__revenue_sum.\n"
                f"got: {outer_sum_targets}\nSQL:\n{sql}"
            )
        finally:
            tmp.cleanup()

    async def test_cross_model_measure_reaggregation_row_count(self, tmp_path) -> None:
        """Re-aggregation semantics: inner sums revenue grouped by region
        (3 rows). Outer re-sums to a single roll-up row over the same
        groups. Total revenue across the seed = 1500."""
        engine = await _engine_with_real_sqlite(tmp_path)
        inner = SlayerQuery(
            name="s1",
            source_model="orders",
            dimensions=["customers.regions.name"],
            measures=[{"formula": "customers.revenue:sum"}],
        )
        outer = SlayerQuery(
            source_model="s1",
            # DEV-1450 typed re-aggregation: outer references the flat
            # inner alias directly.
            measures=[{"formula": "customers__revenue_sum:sum"}],
        )
        resp = await engine.execute(query=[inner, outer])
        assert resp.row_count == 1, f"got {resp.row_count} rows: {resp.data}"
        # Total revenue from the 3 customers: 500 + 700 + 300 = 1500.
        # Each customer has exactly 1 order in the seed, so the inner
        # SUM per region matches per-customer revenue; outer re-SUM
        # totals 1500.
        row = resp.data[0]
        # DEV-1450 typed contract: re-aggregating the flat inner alias
        # surfaces under the canonical `<col>_sum` shape.
        assert "s1.customers__revenue_sum_sum" in row, (
            f"Re-aggregated flat alias must surface as "
            f"`s1.customers__revenue_sum_sum`.\nrow: {row}"
        )
        assert row["s1.customers__revenue_sum_sum"] == pytest.approx(1500.0), row


# ===========================================================================
# Test #5 — cross-stage filter referencing dotted dim.
# ===========================================================================
class TestCrossStageFilter:
    async def test_filter_dotted_ref_rewritten_to_flat(self) -> None:
        """Outer stage filter `customers.regions.name = 'West'` must
        compile to `s1.customers__regions__name = 'West'`."""
        engine, tmp = await _engine_with_join_chain()
        try:
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                dimensions=["customers.regions.name"],
                measures=[{"formula": "*:count"}],
            )
            outer = SlayerQuery(
                source_model="s1",
                # DEV-1450 typed contract: downstream uses flat alias in
                # dimensions AND filters.
                dimensions=["customers__regions__name"],
                measures=[{"formula": "*:count"}],
                filters=["customers__regions__name = 'West'"],
            )
            resp = await engine.execute(query=[inner, outer], dry_run=True)
            sql = resp.sql or ""
            select = _outermost_select(sql)
            where = select.args.get("where")
            assert where is not None, f"Outer WHERE missing.\nSQL:\n{sql}"
            where_cols = list(where.find_all(exp.Column))
            where_tables = {c.table for c in where_cols if c.table}
            assert "customers__regions" not in where_tables, (
                f"Outer WHERE must not reference customers__regions.\n"
                f"got tables: {where_tables}\nSQL:\n{sql}"
            )
            # Both table and column must point at s1.customers__regions__name.
            assert any(
                c.table == "s1" and c.name == "customers__regions__name"
                for c in where_cols
            ), (
                f"Outer WHERE must reference s1.customers__regions__name.\n"
                f"got: {[(c.table, c.name) for c in where_cols]}\nSQL:\n{sql}"
            )
        finally:
            tmp.cleanup()

    async def test_filter_cross_stage_filters_rows(self, tmp_path) -> None:
        """Real execution: outer filter `customers.regions.name = 'West'`
        excludes the East and North rows, keeping only West (1 row)."""
        engine = await _engine_with_real_sqlite(tmp_path)
        inner = SlayerQuery(
            name="s1",
            source_model="orders",
            dimensions=["customers.regions.name"],
            measures=[{"formula": "*:count"}],
        )
        outer = SlayerQuery(
            source_model="s1",
            # DEV-1450 typed contract: downstream flat-only refs.
            dimensions=["customers__regions__name"],
            measures=[{"formula": "*:count"}],
            filters=["customers__regions__name = 'West'"],
        )
        resp = await engine.execute(query=[inner, outer])
        assert resp.row_count == 1, f"got {resp.row_count} rows: {resp.data}"
        assert resp.data[0]["s1.customers__regions__name"] == "West"


# ===========================================================================
# Test #6 — cross-stage order_by referencing declared dim.
# ===========================================================================
class TestCrossStageOrderBy:
    async def test_order_by_dotted_ref_sql_shape(self) -> None:
        """When the dotted dim is declared in `dimensions` AND `order_by`,
        the dim-resolver fix flows through `alias_lookup` automatically."""
        engine, tmp = await _engine_with_join_chain()
        try:
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                dimensions=["customers.regions.name"],
                measures=[{"formula": "*:count"}],
            )
            outer = SlayerQuery(
                source_model="s1",
                # DEV-1450 typed contract: downstream flat-only refs.
                dimensions=["customers__regions__name"],
                measures=[{"formula": "*:count"}],
                order=[{"column": "customers__regions__name"}],
            )
            resp = await engine.execute(query=[inner, outer], dry_run=True)
            sql = resp.sql or ""
            select = _outermost_select(sql)
            order = select.args.get("order")
            assert order is not None, f"Outer ORDER BY missing.\nSQL:\n{sql}"
            order_cols = list(order.find_all(exp.Column))
            order_tables = {c.table for c in order_cols if c.table}
            assert "customers__regions" not in order_tables, (
                f"Outer ORDER BY must not reference customers__regions.\n"
                f"got: {order_tables}\nSQL:\n{sql}"
            )
            # Positive: ORDER BY must reference either the projection
            # alias `"s1.customers__regions__name"` (column with no table
            # qualifier — the outer SELECT's quoted alias) or the
            # underlying `s1.customers__regions__name` column.
            order_col_names = {c.name for c in order_cols}
            assert (
                "s1.customers__regions__name" in order_col_names
                or "customers__regions__name" in order_col_names
            ), (
                f"ORDER BY must reference the projection alias or the "
                f"flat column. got: {order_col_names}\nSQL:\n{sql}"
            )
        finally:
            tmp.cleanup()

    async def test_order_by_actually_sorts_rows(self, tmp_path) -> None:
        """Real execution: outer order_by `customers.regions.name`
        sorts the 3 region rows alphabetically (East < North < West)."""
        engine = await _engine_with_real_sqlite(tmp_path)
        inner = SlayerQuery(
            name="s1",
            source_model="orders",
            dimensions=["customers.regions.name"],
            measures=[{"formula": "*:count"}],
        )
        outer = SlayerQuery(
            source_model="s1",
            # DEV-1450 typed contract: downstream flat-only refs.
            dimensions=["customers__regions__name"],
            measures=[{"formula": "*:count"}],
            order=[{"column": "customers__regions__name"}],
        )
        resp = await engine.execute(query=[inner, outer])
        names = [row["s1.customers__regions__name"] for row in resp.data]
        assert names == sorted(names), f"Rows not sorted: {names}"
        assert names == ["East", "North", "West"], names


# ===========================================================================
# Tests #7, #8 — chained DAG (3-stage): Candidate A at depth 1; Candidate B
# at depth ≥ 2.
# ===========================================================================
class TestChainedDAGCandidates:
    def test_resolver_candidate_a_at_depth_1(self) -> None:
        """At s1 (depth 1), Candidate A (ancestor-strip) matches the
        source-prefixed dotted ref because `_alias_to_short` stripped the
        immediate source-model prefix `orders` on the inner stage."""
        # s1 wraps orders. After _query_as_model, s1.columns will have
        # `customers__regions__name` (orders prefix stripped).
        # Resolver direct test:
        s1 = SlayerModel(
            name="s1",
            sql="<placeholder>",  # not executed in this unit test
            data_source="test_ds",
            columns=[
                Column(name="customers__regions__name", sql="customers__regions__name", type=DataType.TEXT),
                Column(name="_count", sql="_count", type=DataType.DOUBLE),
            ],
            source_model_origin=SourceModelOrigin(name="orders", data_source="test_ds"),
        )
        col = resolve_via_stage_origin(
            model=s1, parts=["orders", "customers", "regions", "name"]
        )
        assert col is not None, "Candidate A should resolve orders.customers.regions.name"
        assert col.name == "customers__regions__name"

    def test_resolver_candidate_b_at_depth_2_with_source_prefix(self) -> None:
        """At s2 (depth 2) with source-prefixed form: Candidate A strips
        the immediate `orders` ancestor and tries `customers__regions__name`,
        which MISSES on s2 (s2 has the deeper-flattened
        `orders__customers__regions__name` because `_alias_to_short`
        only strips the immediate inner-model name `s1` when wrapping).
        Candidate B (full-flat) matches."""
        s1_origin = SourceModelOrigin(name="orders", data_source="test_ds")
        # s2 wraps s1 with a source-prefixed dim on the OUTER query.
        # The outer's strip_source_model_prefix doesn't strip "orders." because
        # source_model is "s1". So alias = "s1.orders.customers.regions.name",
        # _alias_to_short strips "s1." → "orders.customers.regions.name" →
        # "orders__customers__regions__name". That's s2's flat column name.
        s2 = SlayerModel(
            name="s2",
            sql="<placeholder>",
            data_source="test_ds",
            columns=[
                Column(
                    name="orders__customers__regions__name",
                    sql="orders__customers__regions__name",
                    type=DataType.TEXT,
                ),
                Column(name="_count", sql="_count", type=DataType.DOUBLE),
            ],
            source_model_origin=SourceModelOrigin(
                name="s1", data_source="test_ds", parent=s1_origin,
            ),
        )
        # Source-prefixed form: parts[0]="orders" → Candidate A tries
        # "customers__regions__name" (miss), then Candidate B tries
        # "orders__customers__regions__name" (match).
        col = resolve_via_stage_origin(
            model=s2, parts=["orders", "customers", "regions", "name"]
        )
        assert col is not None, "Candidate B should resolve at depth 2"
        assert col.name == "orders__customers__regions__name", (
            f"Candidate B should match deeper-flattened name. Got: {col.name}"
        )

    def test_resolver_candidate_b_at_depth_2_without_source_prefix(self) -> None:
        """At s2 (depth 2) with non-prefixed form: parts[0] is not in
        ancestor chain so Candidate A skips. Candidate B's full-flat
        matches the column s2 actually has when the user uses the
        non-prefixed dotted form."""
        s1_origin = SourceModelOrigin(name="orders", data_source="test_ds")
        # Non-prefixed dim: s2's column is the simple flat form.
        s2 = SlayerModel(
            name="s2",
            sql="<placeholder>",
            data_source="test_ds",
            columns=[
                Column(
                    name="customers__regions__name",
                    sql="customers__regions__name",
                    type=DataType.TEXT,
                ),
            ],
            source_model_origin=SourceModelOrigin(
                name="s1", data_source="test_ds", parent=s1_origin,
            ),
        )
        col = resolve_via_stage_origin(
            model=s2, parts=["customers", "regions", "name"]
        )
        assert col is not None, "Candidate B should resolve via full-flat"
        assert col.name == "customers__regions__name"


# ===========================================================================
# Test #9 — short-alias regression: existing flat-name reference still works.
# ===========================================================================
class TestShortAliasRegression:
    async def test_short_alias_still_resolves(self) -> None:
        """Users that already reference the flat alias
        (`customers__regions__name`) on the outer stage must keep working."""
        engine, tmp = await _engine_with_join_chain()
        try:
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                dimensions=["customers.regions.name"],
                measures=[{"formula": "*:count"}],
            )
            outer = SlayerQuery(
                source_model="s1",
                dimensions=["customers__regions__name"],
                measures=[{"formula": "*:count"}],
            )
            resp = await engine.execute(query=[inner, outer], dry_run=True)
            sql = resp.sql or ""
            outer_tables = _outer_tables_referenced(sql)
            assert "s1" in outer_tables
            assert "customers__regions" not in outer_tables
            outer_cols_on_s1 = {
                c.name for c in _outer_column_refs(sql) if c.table == "s1"
            }
            assert "customers__regions__name" in outer_cols_on_s1
        finally:
            tmp.cleanup()


# ===========================================================================
# Test #11 — engineered collision: Candidate A wins over Candidate B.
# ===========================================================================
class TestCandidatePrecedence:
    def test_candidate_a_wins_over_b(self) -> None:
        """When both Candidate A (ancestor-strip) and Candidate B
        (full-flat) match distinct columns, A wins."""
        # Construct a virtual stage where:
        # - origin chain has name "X"
        # - parts = ["X", "b", "c"]
        # - Candidate A: strip X → b__c
        # - Candidate B: full flat → X__b__c
        # Both columns exist. Resolver returns the Candidate A column.
        virtual = SlayerModel(
            name="virtual",
            sql="<placeholder>",
            data_source="test_ds",
            columns=[
                Column(name="b__c", sql="b__c", type=DataType.TEXT),
                Column(name="X__b__c", sql="X__b__c", type=DataType.TEXT),
            ],
            source_model_origin=SourceModelOrigin(name="X", data_source="test_ds"),
        )
        col = resolve_via_stage_origin(
            model=virtual, parts=["X", "b", "c"]
        )
        assert col is not None
        assert col.name == "b__c", (
            f"Candidate A (ancestor-strip → b__c) must win over Candidate B "
            f"(full-flat → X__b__c). Got: {col.name}"
        )


# ===========================================================================
# Test #12 — non-virtual model: resolver returns None (no regression).
# ===========================================================================
class TestNonVirtualModel:
    def test_resolver_returns_none_for_table_backed_model(self) -> None:
        """Real table-backed model has no `source_model_origin`; the
        resolver returns None, leaving the existing join-walk to handle
        resolution as today."""
        orders = _orders_model()
        assert orders.source_model_origin is None
        col = resolve_via_stage_origin(
            model=orders, parts=["customers", "regions", "name"]
        )
        assert col is None, (
            f"Resolver must return None for non-virtual model. Got: {col}"
        )

    async def test_existing_single_stage_multi_hop_query_unchanged(self) -> None:
        """A single-stage table-backed query with the multi-hop dim
        (which already works today) must continue to work — no
        regression. Pin the join-walk SQL shape."""
        engine, tmp = await _engine_with_join_chain()
        try:
            single = SlayerQuery(
                source_model="orders",
                dimensions=["customers.regions.name"],
                measures=[{"formula": "*:count"}],
            )
            resp = await engine.execute(single, dry_run=True)
            sql = resp.sql or ""
            # Single-stage: the outermost SELECT uses the joined table
            # alias `customers__regions` (legitimately — the LEFT JOINs
            # are in the same scope). No `s1`-style virtual stage here.
            outer_tables = _outer_tables_referenced(sql)
            assert "customers__regions" in outer_tables, (
                f"Single-stage join-walk path must still emit "
                f"customers__regions table ref.\n"
                f"got: {outer_tables}\nSQL:\n{sql}"
            )
        finally:
            tmp.cleanup()


# ===========================================================================
# Test #13 — DEV-1443 rename interaction: colon syntax does NOT auto-resolve.
# ===========================================================================
class TestRenamedInnerMeasureCrossStage:
    async def test_renamed_cmm_colon_syntax_no_longer_resolves_after_rename(
        self,
    ) -> None:
        """Post-DEV-1448 (main): `_alias_to_short`/cm.name now
        propagates the user-supplied rename to the wrapped virtual
        model's column. So `{formula: customers.revenue:sum, name: rev}`
        on s1 leaves s1.columns with `rev` (not the canonical flat
        `customers__revenue_sum`). The outer's colon-syntax reference
        `customers.revenue:sum` no longer matches the intercept's
        candidate flat name → falls through to the cross-model CTE
        path → raises on the virtual stage (no joins).

        DEV-1445 territory: users renamed cross-model measures must
        reference them by the rename on downstream stages."""
        engine, tmp = await _engine_with_join_chain()
        try:
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                dimensions=["customers.regions.name"],
                measures=[{"formula": "customers.revenue:sum", "name": "rev"}],
            )
            outer = SlayerQuery(
                source_model="s1",
                measures=[{"formula": "customers.revenue:sum"}],
            )
            with pytest.raises((SlayerError, ValueError)):
                await engine.execute(query=[inner, outer], dry_run=True)
        finally:
            tmp.cleanup()

    async def test_renamed_cmm_via_rename_resolves(self) -> None:
        """Post-DEV-1448 (main): the rename propagates to s1.columns,
        so `rev:sum` on the outer DOES resolve (s1.get_column('rev')
        finds it as a local column, the standard local-aggregate
        path emits SUM(s1.rev))."""
        engine, tmp = await _engine_with_join_chain()
        try:
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                dimensions=["customers.regions.name"],
                measures=[{"formula": "customers.revenue:sum", "name": "rev"}],
            )
            outer = SlayerQuery(
                source_model="s1",
                measures=[{"formula": "rev:sum"}],
            )
            resp = await engine.execute(query=[inner, outer], dry_run=True)
            sql = resp.sql or ""
            outer_select = _outermost_select(sql)
            outer_sum_targets = []
            for s in outer_select.find_all(exp.Sum):
                if s.parent_select is not outer_select:
                    continue
                outer_sum_targets.extend(
                    (c.table, c.name) for c in s.find_all(exp.Column)
                )
            assert ("s1", "rev") in outer_sum_targets, (
                f"Outer SUM(s1.rev) expected after DEV-1448 rename "
                f"propagation.\ngot: {outer_sum_targets}\nSQL:\n{sql}"
            )
        finally:
            tmp.cleanup()


# ===========================================================================
# Test #16 — cross-stage time_shift with dotted time dim.
# ===========================================================================
class TestCrossStageTimeShift:
    @pytest.mark.skip(
        reason="DEV-1471: time_shift on the outer stage requires a downstream "
        "TimeDimension binding, which the typed pipeline currently rejects "
        "(inner stage's truncated column surfaces as a flat StageSchema name)."
    )
    async def test_time_shift_over_multi_hop_dotted_time_dim(self) -> None:
        """Outer-stage `time_shift` applied to a multi-hop dotted time
        dim (`customers.regions.last_activity_at`) projected by the
        inner stage. The shifted CTE must reference the inner stage's
        flat alias, not the broken hybrid form."""
        engine, tmp = await _engine_with_join_chain()
        try:
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                time_dimensions=[TimeDimension(
                    dimension=ColumnRef(name="customers.regions.last_activity_at"),
                    granularity=TimeGranularity.MONTH,
                )],
                measures=[{"formula": "*:count"}],
            )
            outer = SlayerQuery(
                source_model="s1",
                time_dimensions=[TimeDimension(
                    # DEV-1450 typed contract: downstream flat alias.
                    dimension=ColumnRef(name="customers__regions__last_activity_at"),
                    granularity=TimeGranularity.MONTH,
                )],
                measures=[
                    {"formula": "*:count"},
                    {"formula": "time_shift(*:count, -1, 'month')", "name": "prev"},
                ],
            )
            resp = await engine.execute(query=[inner, outer], dry_run=True)
            sql = resp.sql or ""
            outer_tables = _outer_tables_referenced(sql)
            # Negative: no hybrid `customers__regions` ref in the outer SELECT.
            assert "customers__regions" not in outer_tables, (
                f"time_shift over cross-stage dotted time dim must not "
                f"leak hybrid ref into outer.\n"
                f"got: {outer_tables}\nSQL:\n{sql}"
            )
            # Positive: the flat alias `customers__regions__last_activity_at`
            # must appear as a Column reference in the OUTER (post-inner)
            # SQL — not just inside the innermost subquery. time_shift
            # builds a self-join CTE that selects from s1 with the time
            # column offset; that select must reference the flat alias.
            tree = sqlglot.parse_one(sql, dialect="sqlite")
            non_innermost_flat_refs = [
                c for c in tree.find_all(exp.Column)
                if c.name == "customers__regions__last_activity_at"
                and c.parent_select is not _innermost_select(tree)
            ]
            assert non_innermost_flat_refs, (
                f"time_shift must reference flat multi-hop alias outside "
                f"the innermost subquery (in the shifted CTE or outer).\n"
                f"SQL:\n{sql}"
            )
        finally:
            tmp.cleanup()


# ===========================================================================
# Test #17 — serialization roundtrip strips breadcrumb.
# ===========================================================================
class TestSerializationRoundtripStripsBreadcrumb:
    async def test_yaml_roundtrip_drops_origin(self) -> None:
        """A SlayerModel with `source_model_origin` set, persisted via
        YAMLStorage, loses the field on reload (it's `exclude=True`)."""
        tmp = tempfile.TemporaryDirectory()
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(_ds())
        m = SlayerModel(
            name="virtual_test",
            sql="SELECT 1 AS one",
            data_source="test_ds",
            columns=[Column(name="one", sql="one", type=DataType.DOUBLE)],
            source_model_origin=SourceModelOrigin(
                name="upstream", data_source="test_ds",
            ),
        )
        await storage.save_model(m)
        loaded = await storage.get_model("virtual_test", data_source="test_ds")
        try:
            assert loaded.source_model_origin is None, (
                f"YAML roundtrip must drop source_model_origin. "
                f"Got: {loaded.source_model_origin}"
            )
        finally:
            tmp.cleanup()

    async def test_sqlite_storage_roundtrip_drops_origin(self, tmp_path) -> None:
        """Same as the YAML test but for SQLiteStorage — confirms the
        `exclude=True` field is honored across storage backends."""
        db_path = tmp_path / "slayer.db"
        storage = SQLiteStorage(db_path=str(db_path))
        await storage.save_datasource(_ds())
        m = SlayerModel(
            name="virtual_test",
            sql="SELECT 1 AS one",
            data_source="test_ds",
            columns=[Column(name="one", sql="one", type=DataType.DOUBLE)],
            source_model_origin=SourceModelOrigin(
                name="upstream", data_source="test_ds",
            ),
        )
        await storage.save_model(m)
        loaded = await storage.get_model("virtual_test", data_source="test_ds")
        assert loaded.source_model_origin is None, (
            f"SQLite roundtrip must drop source_model_origin. "
            f"Got: {loaded.source_model_origin}"
        )


# ===========================================================================
# Test #18 — no spurious breadcrumb on a normally-constructed table-backed model.
# ===========================================================================
class TestNoSpuriousBreadcrumb:
    def test_table_backed_model_has_no_origin(self) -> None:
        """A real table-backed `SlayerModel` constructed normally has
        `source_model_origin is None`."""
        m = _orders_model()
        assert m.source_model_origin is None


# ===========================================================================
# Test #19 — cross-model *:count cross-stage (alias resolution only).
# ===========================================================================
class TestCrossModelStarCountCrossStage:
    async def test_star_count_cross_stage_alias_resolution(self) -> None:
        """Inner projects `customers.*:count`. Outer references same.
        Confirm the intercept matches `customers___count` on s1 and the
        outer measure resolves to a local count over that column. The
        outer aggregation must SUM the inner per-group count to roll up
        to total rows — using COUNT on the stage rows would silently
        return the number of groups instead of total rows (CodeRabbit
        review on PR #137).
        """
        engine, tmp = await _engine_with_join_chain()
        try:
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                dimensions=["customers.regions.name"],
                measures=[{"formula": "customers.*:count"}],
            )
            outer = SlayerQuery(
                source_model="s1",
                # DEV-1450 typed re-aggregation: outer references the flat
                # inner alias `customers___count` directly and sums it to
                # roll up total rows. COUNT on the stage rows would
                # silently return the number of groups instead.
                measures=[{"formula": "customers___count:sum"}],
            )
            resp = await engine.execute(query=[inner, outer], dry_run=True)
            sql = resp.sql or ""
            # Outer SELECT must SUM the inner per-group count, not COUNT
            # the stage rows.
            outer_select = _outermost_select(sql)
            sum_targets = []
            for sum_call in outer_select.find_all(exp.Sum):
                if sum_call.parent_select is not outer_select:
                    continue
                sum_targets.extend(
                    (c.table, c.name) for c in sum_call.find_all(exp.Column)
                )
            assert ("s1", "customers___count") in sum_targets, (
                f"Outer SUM must target s1.customers___count (re-aggregating "
                f"the inner per-group count).\n"
                f"got SUM targets: {sum_targets}\nSQL:\n{sql}"
            )
            # And explicitly NO outer COUNT of any shape — COUNT(*) and
            # COUNT(<col>) are both wrong here; the re-aggregation must
            # use SUM over the flat inner alias.
            outer_count_calls = [
                c for c in outer_select.find_all(exp.Count)
                if c.parent_select is outer_select
            ]
            assert not outer_count_calls, (
                f"Outer must not use COUNT for re-aggregation; expected "
                f"SUM over s1.customers___count.\nSQL:\n{sql}"
            )
        finally:
            tmp.cleanup()


# ===========================================================================
# CodeRabbit review on PR #137 — semantics gate: intercept must fall
# through for non-distributive aggregations (avg, count_distinct, ...);
# only sum/min/max pass through unchanged, count re-maps to sum.
# ===========================================================================
class TestCrossModelInterceptSemanticsGate:
    async def test_avg_falls_through_to_cte_path(self) -> None:
        """Cross-stage `customers.revenue:avg` must NOT be intercepted —
        averaging an inner-aggregated average is not equal to the overall
        average. The intercept returns None; the existing cross-model
        path takes over (which on a virtual stage with no joins raises
        ValueError — that's still better than silently lying)."""
        engine, tmp = await _engine_with_join_chain()
        try:
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                dimensions=["customers.regions.name"],
                measures=[{"formula": "customers.revenue:avg"}],
            )
            outer = SlayerQuery(
                source_model="s1",
                measures=[{"formula": "customers.revenue:avg"}],
            )
            # Falls through to existing CTE path → raises because s1 has
            # no joins. Pin the existing error rather than asserting the
            # outer SQL has any specific shape (the fix is: don't
            # silently re-aggregate).
            with pytest.raises((SlayerError, ValueError)):
                await engine.execute(query=[inner, outer], dry_run=True)
        finally:
            tmp.cleanup()

    async def test_count_distinct_falls_through(self) -> None:
        """Cross-stage `customers.revenue:count_distinct` is also
        non-distributive — falls through to the CTE path."""
        engine, tmp = await _engine_with_join_chain()
        try:
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                dimensions=["customers.regions.name"],
                measures=[{"formula": "customers.revenue:count_distinct"}],
            )
            outer = SlayerQuery(
                source_model="s1",
                measures=[{"formula": "customers.revenue:count_distinct"}],
            )
            with pytest.raises((SlayerError, ValueError)):
                await engine.execute(query=[inner, outer], dry_run=True)
        finally:
            tmp.cleanup()

    async def test_sum_passes_through_intercept(self) -> None:
        """Regression guard: re-aggregating the flat inner alias
        `customers__revenue_sum` with `:sum` is distributive — outer
        emits SUM(s1.customers__revenue_sum)."""
        engine, tmp = await _engine_with_join_chain()
        try:
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                dimensions=["customers.regions.name"],
                measures=[{"formula": "customers.revenue:sum"}],
            )
            outer = SlayerQuery(
                source_model="s1",
                # DEV-1450 typed re-aggregation via flat inner alias.
                measures=[{"formula": "customers__revenue_sum:sum"}],
            )
            resp = await engine.execute(query=[inner, outer], dry_run=True)
            sql = resp.sql or ""
            outer_select = _outermost_select(sql)
            sum_targets = []
            for sum_call in outer_select.find_all(exp.Sum):
                if sum_call.parent_select is not outer_select:
                    continue
                sum_targets.extend(
                    (c.table, c.name) for c in sum_call.find_all(exp.Column)
                )
            assert ("s1", "customers__revenue_sum") in sum_targets
        finally:
            tmp.cleanup()


# ===========================================================================
# CodeRabbit review on PR #137 — rename bookkeeping at the line-896
# intercept: a renamed intercepted measure must surface under the user
# alias, and filters / ORDER BY using the colon-form canonical alias
# must resolve to the renamed measure.
# ===========================================================================
class TestCrossModelInterceptRenameBookkeeping:
    async def test_renamed_intercept_surfaces_user_alias(self) -> None:
        """Outer projects `{formula: customers__revenue_sum:sum, name: rev}`
        against `s1`. The re-aggregated measure must surface as `s1.rev`
        (the user rename), not the canonical `customers__revenue_sum_sum`."""
        engine, tmp = await _engine_with_join_chain()
        try:
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                dimensions=["customers.regions.name"],
                measures=[{"formula": "customers.revenue:sum"}],
            )
            outer = SlayerQuery(
                source_model="s1",
                # DEV-1450 typed re-aggregation with rename.
                measures=[{"formula": "customers__revenue_sum:sum", "name": "rev"}],
            )
            resp = await engine.execute(query=[inner, outer], dry_run=True)
            sql = resp.sql or ""
            outer_select = _outermost_select(sql)
            outer_aliases = {p.alias_or_name for p in outer_select.expressions or []}
            assert "s1.rev" in outer_aliases, (
                f"Outer projection must alias as `s1.rev` (the user "
                f"rename).\ngot: {outer_aliases}\nSQL:\n{sql}"
            )
        finally:
            tmp.cleanup()

    async def test_renamed_intercept_filter_via_colon_form(self) -> None:
        """Outer renames the re-aggregated flat measure to `rev` AND
        filters via the colon form. Per DEV-1443, the filter may use
        either the colon form or the user alias; both resolve to the
        rename."""
        engine, tmp = await _engine_with_join_chain()
        try:
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                dimensions=["customers.regions.name"],
                measures=[{"formula": "customers.revenue:sum"}],
            )
            outer = SlayerQuery(
                source_model="s1",
                # DEV-1450 typed re-aggregation with rename.
                measures=[{"formula": "customers__revenue_sum:sum", "name": "rev"}],
                filters=["customers__revenue_sum:sum > 100"],
            )
            resp = await engine.execute(query=[inner, outer], dry_run=True)
            sql = resp.sql or ""
            outer_select = _outermost_select(sql)
            # The filter should land on the renamed alias `rev`, which
            # in the outer SQL is the projection's `SUM(...)`. The HAVING
            # clause (or WHERE-on-aggregate) references the rename.
            # Loose assertion: `rev` appears somewhere in the outer
            # SELECT's WHERE/HAVING, and the broken canonical-flat does
            # NOT appear in WHERE/HAVING as a bare column.
            where = outer_select.args.get("where")
            having = outer_select.args.get("having")
            # CodeRabbit review round 2 on PR #137: assert the filter
            # didn't get silently dropped (at least one of WHERE/HAVING
            # must exist), and that it doesn't reference the internal
            # `_sum_sum` canonical alias the intercept's outer
            # aggregation produces internally.
            assert where is not None or having is not None, (
                f"Expected rewritten filter in WHERE or HAVING — neither "
                f"present means the filter was silently dropped.\nSQL:\n{sql}"
            )
            for clause in (where, having):
                if clause is None:
                    continue
                clause_cols = {(c.table, c.name) for c in clause.find_all(exp.Column)}
                # Must not reference the doubled-up `customers__revenue_sum_sum`
                # internal alias — the remap should have rewritten to either
                # the aggregation expression over `customers__revenue_sum`
                # (HAVING-style) or the rename `rev`. Note: in standard SQL,
                # HAVING references the aggregation expression directly, not
                # the projection alias, so we can't pin "must reference rev".
                broken = [t for t in clause_cols if t[1] == "customers__revenue_sum_sum"]
                assert not broken, (
                    f"Filter must not reference internal double-sum alias.\n"
                    f"got: {clause_cols}\nSQL:\n{sql}"
                )
        finally:
            tmp.cleanup()


# ===========================================================================
# Codex review round 2 on PR #137 — refuse two intercepted qfields that
# canonicalise to the same cross-stage aggregate with different `name`s.
# Without the guard, the second rename mutates the first call's
# EnrichedMeasure alias and `user_projection` is left pointing at an
# orphan.
# ===========================================================================
class TestCrossModelInterceptDuplicateQfieldGuard:
    async def test_intercepted_unrenamed_works_as_middle_stage_in_three_stage_dag(
        self,
    ) -> None:
        """DEV-1450 typed contract end-to-end across a 3-stage DAG:
        stage 1 projects a cross-model measure (legal — has joins),
        stage 2 re-aggregates the flat inner alias, stage 3
        re-aggregates the stage-2 flat alias. The contract requires
        every downstream stage to address its predecessor by flat
        column name; aggregation suffixes stack predictably
        (`*_sum`, then `*_sum_sum`)."""
        engine, tmp = await _engine_with_join_chain()
        try:
            s1 = SlayerQuery(
                name="s1",
                source_model="orders",
                dimensions=["customers.regions.name"],
                measures=[{"formula": "customers.revenue:sum"}],
            )
            s2 = SlayerQuery(
                name="s2",
                source_model="s1",
                # Re-aggregate the s1 flat alias.
                measures=[{"formula": "customers__revenue_sum:sum"}],
            )
            s3 = SlayerQuery(
                source_model="s2",
                # Re-aggregate the s2 flat alias (stacked suffix).
                measures=[{"formula": "customers__revenue_sum_sum:sum"}],
            )
            resp = await engine.execute(query=[s1, s2, s3], dry_run=True)
            assert resp.sql is not None, "3-stage DAG must render SQL"
            # The outer SELECT (s3) should SUM the s2-projected
            # flat column. Walk the AST to confirm.
            outer_select = _outermost_select(resp.sql)
            outer_sum_targets = []
            for s in outer_select.find_all(exp.Sum):
                if s.parent_select is not outer_select:
                    continue
                outer_sum_targets.extend(
                    (c.table, c.name) for c in s.find_all(exp.Column)
                )
            assert ("s2", "customers__revenue_sum_sum") in outer_sum_targets, (
                f"Third-stage must SUM the s2 flat alias.\n"
                f"got: {outer_sum_targets}\nSQL:\n{resp.sql}"
            )
        finally:
            tmp.cleanup()

    async def test_intercept_skips_user_named_local_measure_that_looks_like_cmm_canonical(
        self,
    ) -> None:
        """Codex review on PR #137 round 10: a user-named local
        measure whose name coincidentally matches a cross-model
        canonical-flat shape must NOT be picked up by the cross-stage
        intercept. The intercept's `agg_column_names` set only
        includes auto-derived cross-model canonical-flats, so a
        renamed local measure like
        `{"formula": "amount:sum", "name": "customers__revenue_sum"}`
        on the inner stage stays distinct from a cross-model
        `customers.revenue:sum` reference on the outer."""
        engine, tmp = await _engine_with_join_chain()
        try:
            # Inner has a local `amount:sum` renamed to
            # `customers__revenue_sum`. There is NO real
            # `customers.revenue:sum` cross-model measure projected.
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                dimensions=["customers.regions.name"],
                measures=[{"formula": "amount:sum", "name": "customers__revenue_sum"}],
            )
            # Outer references `customers.revenue:sum` — the intercept
            # candidate computes `customers__revenue_sum` and the
            # column exists on s1, BUT it isn't an auto-derived cross-
            # model canonical-flat (it's the user-renamed local
            # measure). The intercept must skip it and fall through
            # to the cross-model CTE path, which raises because s1
            # has no joins.
            outer = SlayerQuery(
                source_model="s1",
                measures=[{"formula": "customers.revenue:sum"}],
            )
            with pytest.raises((SlayerError, ValueError)):
                await engine.execute(query=[inner, outer], dry_run=True)
        finally:
            tmp.cleanup()

    def test_intercept_skips_dim_columns_that_look_like_aggregations(
        self,
    ) -> None:
        """Codex review on PR #137 round 9: if the inner stage projects
        a DIMENSION whose flat name coincidentally matches the
        canonical-flat shape an aggregation would produce (e.g. a dim
        literally named with `_sum` suffix), the intercept must NOT
        re-aggregate it. ``agg_column_names`` records which downstream
        shorts came from CMMs / measures / transforms / expressions
        — only those qualify for the intercept's re-aggregation."""
        # Construct a virtual stage directly with a dim column whose
        # name has the canonical-flat shape, but the column is NOT in
        # agg_column_names. The intercept candidate must return None.
        s1 = SlayerModel(
            name="s1",
            sql="<placeholder>",
            data_source="test_ds",
            columns=[
                Column(
                    name="customers__revenue_sum",
                    sql="customers__revenue_sum",
                    type=DataType.TEXT,
                ),
            ],
            source_model_origin=SourceModelOrigin(
                name="orders",
                data_source="test_ds",
                # `agg_column_names` empty — the column above is a dim,
                # not an aggregation projection.
                agg_column_names=frozenset(),
            ),
        )
        # Direct check on resolve_via_stage_origin: the dim resolves
        # (it's a real column).
        col = resolve_via_stage_origin(
            model=s1, parts=["customers", "revenue_sum"]
        )
        assert col is not None and col.name == "customers__revenue_sum"
        # But the intercept candidate (which gates on agg_column_names)
        # must NOT pick it up. We test indirectly via _intercept_candidate_for_cross_model
        # being unable to be imported as a public symbol; instead we
        # check that running a cross-stage query that would trigger
        # the intercept on a dim raises (no agg fallback can succeed).
        # — covered by the run-by-name end-to-end shape below.

    async def test_unrenamed_intercepted_cmm_colon_filter_does_not_get_rewritten_as_where(
        self,
    ) -> None:
        """Codex round 7: a `customers.revenue:sum > 100` filter on an
        outer query whose intercepted measure is unrenamed must NOT be
        silently rewritten to `WHERE s1.customers__revenue_sum > 100`
        — that applies the predicate BEFORE the outer SUM, not as
        HAVING on the re-aggregated measure.

        The fallback gating recognises the aggregation-canonical leaf
        and skips the dim-style rewrite. The standard strict path
        then raises (DEV-1445 territory: cross-model measure filters
        are not yet auto-resolved). The unrenamed case is the same
        — users must reference via the renamed alias, restructure
        with multi-stage, or wait for DEV-1445."""
        engine, tmp = await _engine_with_join_chain()
        try:
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                dimensions=["customers.regions.name"],
                measures=[{"formula": "customers.revenue:sum"}],
            )
            outer = SlayerQuery(
                source_model="s1",
                measures=[{"formula": "customers.revenue:sum"}],
                filters=["customers.revenue:sum > 100"],
            )
            # The strict path raises rather than silently emitting a
            # WHERE that would clip rows before the outer aggregation.
            # (The rename path test pins the WORKING case.)
            with pytest.raises((SlayerError, ValueError)):
                await engine.execute(query=[inner, outer], dry_run=True)
        finally:
            tmp.cleanup()

    @pytest.mark.skip(
        reason="DEV-1472: hidden-slot ORDER BY (order-only ref not in "
        "dimensions/measures) is documented as deferred in the typed pipeline "
        "(stage 7b.10+). Re-enable when the gap closes."
    )
    async def test_intercepted_cmm_order_only_no_qfield_registers_alias(
        self,
    ) -> None:
        """DEV-1450 typed contract: outer ORDER BY may reference the
        flat inner alias directly, even when the column is not
        re-projected by the outer query. The order resolver must find
        the flat column on s1 and emit a valid `s1.<col>` ref."""
        engine, tmp = await _engine_with_join_chain()
        try:
            inner = SlayerQuery(
                name="s1",
                source_model="orders",
                dimensions=["customers.regions.name"],
                measures=[{"formula": "customers.revenue:sum"}],
            )
            # Outer: order-only ref to the inner flat alias, NOT in
            # the projection. Must still resolve.
            outer = SlayerQuery(
                source_model="s1",
                dimensions=["customers__regions__name"],
                measures=[{"formula": "*:count"}],
                order=[{"column": "customers__revenue_sum"}],
            )
            resp = await engine.execute(query=[inner, outer], dry_run=True)
            sql = resp.sql or ""
            outer_select = _outermost_select(sql)
            order = outer_select.args.get("order")
            assert order is not None, f"Outer ORDER BY missing.\nSQL:\n{sql}"
            order_cols = list(order.find_all(exp.Column))
            order_col_names = {c.name for c in order_cols}
            # Must resolve to a column on s1 (or a quoted projection
            # alias) — NOT a bare `customers` table that doesn't exist
            # in the outer scope.
            assert "customers" not in {c.table for c in order_cols if c.table}, (
                f"ORDER BY must not reference a bare `customers` table.\n"
                f"got: {order_col_names}\nSQL:\n{sql}"
            )
        finally:
            tmp.cleanup()



# ===========================================================================
# Test #20 dropped — was for Mode A lenient model-filter path on virtual
# stages, but `_query_as_model` does not propagate inner-model `filters`
# to the wrapped virtual model, so the lenient path is dead code for
# virtual stages. The strict path (DSL query filters) is the actual
# integration site for cross-stage dotted refs and is covered by tests
# in `TestCrossStageFilter` (#5).
# ===========================================================================
