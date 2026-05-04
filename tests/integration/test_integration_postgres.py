"""Integration tests using a real PostgreSQL database via pytest-postgresql."""

import tempfile
import uuid

import pytest

pytest.importorskip("pytest_postgresql")

import psycopg
from pytest_postgresql import factories

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from slayer.async_utils import run_sync

# Spawn a temporary Postgres process (random port)
postgresql_proc = factories.postgresql_proc(port=None)
# Function-scoped per-test connection — used by pg_cross_model_env, which has a
# test that mutates storage (create_model_from_query) and so cannot share a
# module-scoped DB safely.
postgresql = factories.postgresql("postgresql_proc")


def _create_module_db(postgresql_proc):
    """Create a fresh database on the session-scoped Postgres for one
    module-scoped fixture. Returns (open_connection, db_name). Caller is
    responsible for closing the connection and calling _drop_module_db on
    teardown."""
    info = postgresql_proc
    db_name = f"test_{uuid.uuid4().hex[:12]}"
    admin = psycopg.connect(
        host=info.host, port=info.port, user=info.user, dbname="postgres",
    )
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'CREATE DATABASE "{db_name}"')
    admin.close()
    conn = psycopg.connect(
        host=info.host, port=info.port, user=info.user, dbname=db_name,
    )
    return conn, db_name


def _drop_module_db(postgresql_proc, db_name):
    info = postgresql_proc
    admin = psycopg.connect(
        host=info.host, port=info.port, user=info.user, dbname="postgres",
    )
    admin.autocommit = True
    with admin.cursor() as cur:
        # FORCE: terminate any lingering async-engine connection pools that
        # haven't yet been GC'd. Requires Postgres 13+; pytest-postgresql ships
        # with whatever the system pg_ctl is, so this is fine in practice.
        cur.execute(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)')
    admin.close()


@pytest.fixture(scope="module")
def _pg_env_storage(postgresql_proc, tmp_path_factory):
    """Module-scoped: per-module Postgres DB with seeded customers/orders, plus
    a YAMLStorage with the orders + customers models pre-saved. Returns the
    storage instance — the per-test ``pg_env`` fixture wraps a fresh engine
    around it (engines bind to their event loop, see slayer/sql/client.py:144).
    """
    conn, db_name = _create_module_db(postgresql_proc)
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE customers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                region TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                status TEXT NOT NULL,
                amount NUMERIC(10,2) NOT NULL,
                customer_id INTEGER REFERENCES customers(id),
                created_at TIMESTAMP NOT NULL
            )
        """)
        cur.executemany(
            "INSERT INTO customers VALUES (%s, %s, %s)",
            [(1, "Acme Corp", "US"), (2, "Globex", "EU"), (3, "Initech", "US")],
        )
        cur.executemany(
            "INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
            [
                (1, "completed", 100, 1, "2024-01-15 10:00:00"),
                (2, "completed", 200, 1, "2024-01-20 11:00:00"),
                (3, "pending", 50, 2, "2024-02-10 09:00:00"),
                (4, "completed", 150, 2, "2024-02-15 14:00:00"),
                (5, "cancelled", 75, 3, "2024-03-01 08:00:00"),
                (6, "pending", 300, 3, "2024-03-10 16:00:00"),
            ],
        )
        conn.commit()

        tmpdir = str(tmp_path_factory.mktemp("pg_env"))
        storage = YAMLStorage(base_dir=tmpdir)

        info = postgresql_proc
        run_sync(storage.save_datasource(DatasourceConfig(
            name="testpg",
            type="postgres",
            host=info.host,
            port=info.port,
            database=db_name,
            username=info.user,
            password="",
        )))

        orders_model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="testpg",
            columns=[
                Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Column(name="status", sql="status", type=DataType.STRING),
                Column(name="customer_id", sql="customer_id", type=DataType.NUMBER),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),

                Column(name="total", sql="amount", type=DataType.NUMBER),
                Column(name="avg_amount", sql="amount", type=DataType.NUMBER),
            ],
        )
        customers_model = SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="testpg",
            columns=[
                Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Column(name="name", sql="name", type=DataType.STRING),
                Column(name="region", sql="region", type=DataType.STRING),

            ],
        )
        run_sync(storage.save_model(orders_model))
        run_sync(storage.save_model(customers_model))

        yield storage
    finally:
        conn.close()
        _drop_module_db(postgresql_proc, db_name)


@pytest.fixture
def pg_env(_pg_env_storage):
    """Per-test SlayerQueryEngine wrapping the module-scoped storage. The
    engine is recreated per-test because its async SQLAlchemy engine binds to
    the current event loop."""
    return SlayerQueryEngine(storage=_pg_env_storage)


@pytest.mark.integration
class TestPostgresQueries:
    async def test_count_all(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(source_model="orders", measures=[{"formula": "*:count"}])
        result = await pg_env.execute(query=query)
        assert result.row_count == 1
        assert result.data[0]["orders._count"] == 6

    async def test_sum_measure(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(source_model="orders", measures=[{"formula": "total:sum"}])
        result = await pg_env.execute(query=query)
        assert float(result.data[0]["orders.total_sum"]) == 875.0

    async def test_avg_measure(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(source_model="orders", measures=[{"formula": "avg_amount:avg"}])
        result = await pg_env.execute(query=query)
        avg = float(result.data[0]["orders.avg_amount_avg"])
        assert abs(avg - 145.83) < 0.1

    async def test_group_by_status(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "status"}],
        )
        result = await pg_env.execute(query=query)
        by_status = {r["orders.status"]: r["orders._count"] for r in result.data}
        assert by_status["completed"] == 3
        assert by_status["pending"] == 2
        assert by_status["cancelled"] == 1

    async def test_filter_equals(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            filters=["status == 'completed'"],
        )
        result = await pg_env.execute(query=query)
        assert result.data[0]["orders._count"] == 3

    async def test_filter_gt(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            filters=["amount > 100"],
        )
        result = await pg_env.execute(query=query)
        assert result.data[0]["orders._count"] == 3  # 200, 150, 300

    async def test_order_by_desc(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "status"}],
            order=[{"column": {"name": "count"}, "direction": "desc"}],
        )
        result = await pg_env.execute(query=query)
        assert result.data[0]["orders.status"] == "completed"

    async def test_limit(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "status"}],
            limit=2,
        )
        result = await pg_env.execute(query=query)
        assert result.row_count == 2

    async def test_multiple_measures(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}, {"formula": "total:sum"}],
            dimensions=[{"name": "status"}],
        )
        result = await pg_env.execute(query=query)
        completed = next(r for r in result.data if r["orders.status"] == "completed")
        assert completed["orders._count"] == 3
        assert float(completed["orders.total_sum"]) == 450.0

    async def test_time_dimension_month_granularity(self, pg_env: SlayerQueryEngine) -> None:
        """Postgres supports DATE_TRUNC — this should work unlike SQLite."""
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
        )
        result = await pg_env.execute(query=query)
        assert result.row_count == 3  # Jan, Feb, Mar

    async def test_time_dimension_with_date_range(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            time_dimensions=[{
                "dimension": {"name": "created_at"},
                "granularity": "month",
                "date_range": ["2024-01-01", "2024-02-28"],
            }],
        )
        result = await pg_env.execute(query=query)
        # Only Jan and Feb orders (4 orders)
        total = sum(r["orders._count"] for r in result.data)
        assert total == 4

    async def test_composite_filter(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            filters=["status == 'completed' or status == 'pending'"],
        )
        result = await pg_env.execute(query=query)
        assert result.data[0]["orders._count"] == 5  # 3 completed + 2 pending

    async def test_time_shift_with_date_range(self, pg_env: SlayerQueryEngine) -> None:
        """time_shift with date_range should fetch shifted data from outside the filtered range."""
        # Query only March, ask for previous month (February)
        # Seed: Jan(300), Feb(200), Mar(375)
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
                date_range=["2024-03-01", "2024-03-31"],
            )],
            measures=[
                ModelMeasure(formula="total:sum"),
                ModelMeasure(formula="time_shift(total:sum, -1, 'month')", name="prev_month"),
            ],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        )
        result = await pg_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(375.0)
        # Previous month (Feb) fetched from DB, not NULL
        assert float(result.data[0]["orders.prev_month"]) == pytest.approx(200.0)

    async def test_consecutive_periods_with_boolean_predicate(self, pg_env: SlayerQueryEngine) -> None:
        """consecutive_periods on a comparison predicate must work on Postgres.

        Postgres rejects `boolean <> integer`, so the CTE generator's numeric
        predicate `<expr> IS NOT NULL AND <expr> <> 0` cannot be used when the
        argument is already boolean. The boolean-aware path must use the
        column directly inside CASE WHEN.
        """
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[
                ModelMeasure(formula="total:sum"),
                ModelMeasure(formula="consecutive_periods(total:sum > 200)", name="positive_run"),
            ],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        )
        result = await pg_env.execute(query=query)
        # Monthly totals: Jan=300 (>200, true), Feb=200 (==200, false),
        # Mar=375 (>200, true). Trailing run lengths: 1, 0, 1.
        assert [r["orders.positive_run"] for r in result.data] == [1, 0, 1]

    async def test_change_with_date_range(self, pg_env: SlayerQueryEngine) -> None:
        """change() with date_range should fetch previous period from outside the filtered range."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
                date_range=["2024-03-01", "2024-03-31"],
            )],
            measures=[
                ModelMeasure(formula="total:sum"),
                ModelMeasure(formula="change(total:sum)", name="amount_change"),
            ],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        )
        result = await pg_env.execute(query=query)
        assert result.row_count == 1
        # Mar(375) - Feb(200) = 175
        assert float(result.data[0]["orders.amount_change"]) == pytest.approx(175.0)

    async def test_change_pct_with_date_range(self, pg_env: SlayerQueryEngine) -> None:
        """change_pct() with date_range should compute correct percentage from shifted data."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
                date_range=["2024-03-01", "2024-03-31"],
            )],
            measures=[
                ModelMeasure(formula="total:sum"),
                ModelMeasure(formula="change_pct(total:sum)", name="pct"),
            ],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        )
        result = await pg_env.execute(query=query)
        assert result.row_count == 1
        # (375 - 200) / 200 = 0.875
        assert float(result.data[0]["orders.pct"]) == pytest.approx(0.875)

    async def test_multiple_date_range_shifts(self, pg_env: SlayerQueryEngine) -> None:
        """Multiple self-join transforms with different offsets should each get correct data."""
        # Query Feb only, ask for both previous (Jan) and next (Mar)
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
                date_range=["2024-02-01", "2024-02-29"],
            )],
            measures=[
                ModelMeasure(formula="total:sum"),
                ModelMeasure(formula="time_shift(total:sum, -1, 'month')", name="prev"),
                ModelMeasure(formula="time_shift(total:sum, 1, 'month')", name="next"),
            ],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        )
        result = await pg_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(200.0)
        assert float(result.data[0]["orders.prev"]) == pytest.approx(300.0)  # Jan
        assert float(result.data[0]["orders.next"]) == pytest.approx(375.0)  # Mar


@pytest.fixture
async def pg_cross_model_env(postgresql):
    """Postgres env with orders + customers (with score) and explicit join."""
    cur = postgresql.cursor()
    cur.execute("""
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            region TEXT NOT NULL,
            score NUMERIC(5,2) NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            amount NUMERIC(10,2) NOT NULL,
            customer_id INTEGER REFERENCES customers(id),
            created_at TIMESTAMP NOT NULL
        )
    """)
    cur.executemany(
        "INSERT INTO customers VALUES (%s, %s, %s, %s)",
        [(1, "Alice", "US", 90), (2, "Bob", "EU", 60), (3, "Charlie", "US", 80)],
    )
    cur.executemany(
        "INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
        [
            (1, "completed", 100, 1, "2024-01-15 10:00:00"),
            (2, "completed", 200, 1, "2024-01-20 11:00:00"),
            (3, "pending", 50, 2, "2024-02-10 09:00:00"),
            (4, "completed", 150, 2, "2024-02-15 14:00:00"),
            (5, "completed", 300, 3, "2024-03-01 08:00:00"),
            (6, "pending", 25, 1, "2024-03-10 16:00:00"),
        ],
    )
    postgresql.commit()

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    info = postgresql.info
    await storage.save_datasource(DatasourceConfig(
        name="testpg", type="postgres",
        host=info.host, port=info.port, database=info.dbname,
        username=info.user, password="",
    ))
    from slayer.core.models import ModelJoin
    run_sync(storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="testpg",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Column(name="status", sql="status", type=DataType.STRING),
            Column(name="customer_id", sql="customer_id", type=DataType.NUMBER),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="amount", sql="amount", type=DataType.NUMBER),

            Column(name="total", sql="amount", type=DataType.NUMBER),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )))
    run_sync(storage.save_model(SlayerModel(
        name="customers", sql_table="customers", data_source="testpg",
        columns=[
            Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Column(name="name", sql="name", type=DataType.STRING),

            Column(name="avg_score", sql="score", type=DataType.NUMBER),
        ],
    )))
    return SlayerQueryEngine(storage=storage)


@pytest.mark.integration
class TestCrossModelAndMultistage:
    async def test_cross_model_measure(self, pg_cross_model_env: SlayerQueryEngine) -> None:
        """Cross-model measure: monthly order count + avg customer score."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="customers.avg_score:avg")],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        )
        result = await pg_cross_model_env.execute(query=query)
        assert result.row_count == 3
        # customers model has no join back to orders, so the time dimension is
        # unreachable from the re-rooted CTE → dropped → scalar AVG CROSS JOINed.
        # Global avg: (90 + 60 + 80) / 3 = 76.67
        global_avg = pytest.approx((90.0 + 60.0 + 80.0) / 3)
        assert float(result.data[0]["orders.customers.avg_score_avg"]) == global_avg
        assert float(result.data[1]["orders.customers.avg_score_avg"]) == global_avg
        assert float(result.data[2]["orders.customers.avg_score_avg"]) == global_avg

    async def test_query_list_named(self, pg_cross_model_env: SlayerQueryEngine) -> None:
        """Query list: named sub-query referenced by main query."""
        inner = SlayerQuery(
            name="monthly", source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="total:sum")],
        )
        outer = SlayerQuery(source_model="monthly", measures=[ModelMeasure(formula="*:count")])
        result = await pg_cross_model_env.execute(query=[inner, outer])
        assert result.data[0]["monthly._count"] == 3

    async def test_create_model_from_query(self, pg_cross_model_env: SlayerQueryEngine) -> None:
        """Save a query as a permanent model, then query it."""
        source = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="total:sum")],
        )
        saved = await pg_cross_model_env.create_model_from_query(query=source, name="pg_monthly")
        assert saved.source_queries is not None
        result = await pg_cross_model_env.execute(
            query=SlayerQuery(source_model="pg_monthly", measures=[ModelMeasure(formula="*:count")])
        )
        assert result.data[0]["pg_monthly._count"] == 3

    async def test_sql_dimension(self, pg_cross_model_env: SlayerQueryEngine) -> None:
        """SQL expression dimension via ModelExtension with Postgres."""
        from slayer.core.query import ModelExtension
        query = SlayerQuery(
            source_model=ModelExtension(
                source_name="orders",
                columns=[{"name": "tier", "sql": "CASE WHEN amount > 100 THEN 'high' ELSE 'low' END"}],
            ),
            dimensions=[ColumnRef(name="tier")],
            measures=[ModelMeasure(formula="*:count")],
        )
        result = await pg_cross_model_env.execute(query=query)
        by_tier = {r["orders.tier"]: r["orders._count"] for r in result.data}
        # high: 200, 150, 300 = 3; low: 100, 50, 25 = 3
        assert by_tier["high"] == 3
        assert by_tier["low"] == 3


@pytest.fixture(scope="module")
def pg_ingest_env(postgresql_proc):
    """Set up tables with FK relationships and ingest via rollup.

    Module-scoped: tests destructure ``(models, ds, _)`` and build their own
    ephemeral storage from the returned models — they never mutate the fixture
    state. Saves the ~0.32s/call SQLAlchemy reflection across 11 tests.
    """
    conn, db_name = _create_module_db(postgresql_proc)
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE regions (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE customers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                region_id INTEGER REFERENCES regions(id)
            )
        """)
        cur.execute("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                amount NUMERIC(10,2) NOT NULL,
                customer_id INTEGER REFERENCES customers(id)
            )
        """)
        cur.executemany("INSERT INTO regions VALUES (%s, %s)", [(1, "US"), (2, "EU")])
        cur.executemany(
            "INSERT INTO customers VALUES (%s, %s, %s)",
            [(1, "Acme", 1), (2, "Globex", 2), (3, "Initech", 1)],
        )
        cur.executemany(
            "INSERT INTO orders VALUES (%s, %s, %s)",
            [(1, 100, 1), (2, 200, 1), (3, 50, 2), (4, 150, 3)],
        )
        conn.commit()

        info = postgresql_proc
        ds = DatasourceConfig(
            name="testpg",
            type="postgres",
            host=info.host,
            port=info.port,
            database=db_name,
            username=info.user,
            password="",
        )

        models = ingest_datasource(datasource=ds, schema="public")
        yield models, ds, conn
    finally:
        conn.close()
        _drop_module_db(postgresql_proc, db_name)


@pytest.mark.integration
class TestRollupIngestion:
    def test_orders_has_own_columns_only(self, pg_ingest_env) -> None:
        """After ingestion, models only have their own columns (no flattened joined dims)."""
        models, _, _ = pg_ingest_env
        orders = next(m for m in models if m.name == "orders")

        col_names = [c.name for c in orders.columns]
        # Should have own columns only.
        assert "id" in col_names
        assert "customer_id" in col_names
        # In v2, every non-joined column appears once (numeric columns included).
        assert "amount" in col_names
        # Joined dimensions are resolved via join graph, not pre-flattened.
        assert not any("." in name for name in col_names)

    def test_orders_uses_sql_table_with_joins(self, pg_ingest_env) -> None:
        models, _, _ = pg_ingest_env
        orders = next(m for m in models if m.name == "orders")

        # Models with joins use sql_table (not baked sql) + explicit joins
        assert orders.sql_table is not None
        assert orders.sql is None
        assert len(orders.joins) > 0

    def test_regions_has_no_rollup(self, pg_ingest_env) -> None:
        models, _, _ = pg_ingest_env
        regions = next(m for m in models if m.name == "regions")

        # Regions references nothing, should keep sql_table
        assert regions.sql_table is not None
        assert regions.sql is None

    def test_orders_has_no_named_measures_after_ingest(self, pg_ingest_env) -> None:
        """v2 auto-ingest leaves model.measures (formula list) empty.

        Row-level columns live on .columns; named-formula measures are an
        opt-in library users populate themselves.
        """
        models, _, _ = pg_ingest_env
        orders = next(m for m in models if m.name == "orders")
        assert orders.measures == []
        col_names = [c.name for c in orders.columns]
        assert "amount" in col_names

    async def test_rollup_query_group_by_customer(self, pg_ingest_env) -> None:
        """Query orders grouped by rolled-up customer name."""
        models, ds, _ = pg_ingest_env

        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(ds)
        for m in models:
            await storage.save_model(m)
        engine = SlayerQueryEngine(storage=storage)

        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "customers.name"}],
        )
        result = await engine.execute(query=query)

        by_name = {r["orders.customers.name"]: r["orders._count"] for r in result.data}
        assert by_name["Acme"] == 2
        assert by_name["Globex"] == 1
        assert by_name["Initech"] == 1

    async def test_rollup_query_group_by_region(self, pg_ingest_env) -> None:
        """Query orders grouped by transitively rolled-up region name."""
        models, ds, _ = pg_ingest_env

        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(ds)
        for m in models:
            await storage.save_model(m)
        engine = SlayerQueryEngine(storage=storage)

        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}, {"formula": "amount:sum"}],
            dimensions=[{"name": "customers.regions.name"}],
        )
        result = await engine.execute(query=query)

        by_region = {r["orders.customers.regions.name"]: r for r in result.data}
        assert by_region["US"]["orders._count"] == 3  # Acme(2) + Initech(1)
        assert by_region["EU"]["orders._count"] == 1  # Globex(1)
        assert float(by_region["US"]["orders.amount_sum"]) == 450.0  # 100+200+150
        assert float(by_region["EU"]["orders.amount_sum"]) == 50.0

    async def test_dotted_dimension_single_hop(self, pg_ingest_env) -> None:
        """Dotted dimension 'customers.name' resolves to 'customers__name'."""
        models, ds, _ = pg_ingest_env

        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(ds)
        for m in models:
            await storage.save_model(m)
        engine = SlayerQueryEngine(storage=storage)

        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "customers.name"}],
        )
        result = await engine.execute(query=query)

        by_name = {r["orders.customers.name"]: r["orders._count"] for r in result.data}
        assert by_name["Acme"] == 2
        assert by_name["Globex"] == 1
        assert by_name["Initech"] == 1

    async def test_dotted_dimension_multi_hop(self, pg_ingest_env) -> None:
        """Multi-hop dotted dimension 'customers.regions.name' resolves transitively."""
        models, ds, _ = pg_ingest_env

        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(ds)
        for m in models:
            await storage.save_model(m)
        engine = SlayerQueryEngine(storage=storage)

        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "customers.regions.name"}],
        )
        result = await engine.execute(query=query)

        # Same as regions__name: US=3, EU=1
        by_region = {r["orders.customers.regions.name"]: r["orders._count"] for r in result.data}
        assert by_region["US"] == 3
        assert by_region["EU"] == 1

    async def test_selective_joins_no_joined_dims(self, pg_ingest_env) -> None:
        """Query using only source-table dimensions should not include JOINs."""
        models, ds, _ = pg_ingest_env

        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(ds)
        for m in models:
            await storage.save_model(m)
        engine = SlayerQueryEngine(storage=storage)

        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
        )
        result = await engine.execute(query=query)
        # No joined dimensions → SQL should not have LEFT JOIN
        assert "LEFT JOIN" not in result.sql
        assert result.data[0]["orders._count"] == 4

    async def test_selective_joins_single_hop(self, pg_ingest_env) -> None:
        """Query with customer dimension should JOIN customers but NOT regions."""
        models, ds, _ = pg_ingest_env

        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(ds)
        for m in models:
            await storage.save_model(m)
        engine = SlayerQueryEngine(storage=storage)

        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "customers.name"}],
        )
        result = await engine.execute(query=query)
        # Should JOIN customers but NOT regions
        assert "LEFT JOIN" in result.sql
        assert "customers" in result.sql
        assert "regions" not in result.sql

    async def test_selective_joins_transitive(self, pg_ingest_env) -> None:
        """Query with region dimension should include both customers and regions JOINs."""
        models, ds, _ = pg_ingest_env

        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(ds)
        for m in models:
            await storage.save_model(m)
        engine = SlayerQueryEngine(storage=storage)

        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "customers.regions.name"}],
        )
        result = await engine.execute(query=query)
        # Needs both customers (intermediate) and regions (target)
        assert "customers" in result.sql
        assert "regions" in result.sql

    def test_orders_has_joins_metadata(self, pg_ingest_env) -> None:
        """Ingested models should have only direct join metadata."""
        models, _, _ = pg_ingest_env
        orders = next(m for m in models if m.name == "orders")

        # orders → customers (direct FK)
        join_targets = [j.target_model for j in orders.joins]
        assert "customers" in join_targets

        # Multi-hop targets (regions) are NOT baked in — resolved at query time
        assert "regions" not in join_targets

        # Each join has at least one join pair
        for j in orders.joins:
            assert len(j.join_pairs) >= 1
            for pair in j.join_pairs:
                assert len(pair) == 2  # [source_dim, target_dim]

    def test_regions_has_no_joins(self, pg_ingest_env) -> None:
        """Models with no FK references should have empty joins."""
        models, _, _ = pg_ingest_env
        regions = next(m for m in models if m.name == "regions")
        assert regions.joins == []

    async def test_joins_serialize_to_yaml(self, pg_ingest_env) -> None:
        """Joins should survive YAML round-trip."""
        models, ds, _ = pg_ingest_env
        orders = next(m for m in models if m.name == "orders")

        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_model(orders)

        loaded = await storage.get_model("orders")
        assert len(loaded.joins) == len(orders.joins)
        for orig, loaded_j in zip(orders.joins, loaded.joins):
            assert orig.target_model == loaded_j.target_model
            assert orig.join_pairs == loaded_j.join_pairs


@pytest.mark.integration
class TestPostgresMedianPercentile:
    """Live execution of median/percentile against Postgres.

    Postgres has native ``PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)``;
    these tests pin the round-trip so the dialect-aware ``_build_percentile``
    refactor doesn't silently regress.
    """

    async def test_median(self, pg_env: SlayerQueryEngine) -> None:
        # amounts = [100, 200, 50, 150, 75, 300] -> median 125
        query = SlayerQuery(source_model="orders", measures=[{"formula": "total:median"}])
        result = await pg_env.execute(query=query)
        assert float(result.data[0]["orders.total_median"]) == pytest.approx(125.0)

    async def test_percentile_quartiles(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[
                {"formula": "total:percentile(p=0.25)"},
                {"formula": "total:percentile(p=0.75)"},
            ],
        )
        result = await pg_env.execute(query=query)
        row = result.data[0]
        assert float(row["orders.total_percentile_p_0_25"]) == pytest.approx(81.25)
        assert float(row["orders.total_percentile_p_0_75"]) == pytest.approx(187.5)

    async def test_median_grouped(self, pg_env: SlayerQueryEngine) -> None:
        # completed: [100, 150, 200] -> 150
        # pending:   [50, 300]       -> 175
        # cancelled: [75]            -> 75
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:median"}],
            dimensions=[{"name": "status"}],
        )
        result = await pg_env.execute(query=query)
        by_status = {
            r["orders.status"]: float(r["orders.total_median"]) for r in result.data
        }
        assert by_status["completed"] == pytest.approx(150)
        assert by_status["pending"] == pytest.approx(175)
        assert by_status["cancelled"] == pytest.approx(75)


@pytest.mark.integration
class TestPostgresStatAggregations:
    """DEV-1317 cross-dialect smoke: the new statistical aggregations
    (stddev_samp, stddev_pop, var_samp, var_pop, corr) must produce the
    same numeric results on Postgres native functions as the SQLite UDF
    path produces. Within rel=1e-9.
    """

    async def test_stddev_samp_native_postgres(self, pg_env: SlayerQueryEngine) -> None:
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:stddev_samp"}],
        )
        result = await pg_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_stddev_samp"]) == pytest.approx(
            statistics.stdev(amounts), rel=1e-9
        )

    async def test_stddev_pop_native_postgres(self, pg_env: SlayerQueryEngine) -> None:
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:stddev_pop"}],
        )
        result = await pg_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_stddev_pop"]) == pytest.approx(
            statistics.pstdev(amounts), rel=1e-9
        )

    async def test_var_samp_native_postgres(self, pg_env: SlayerQueryEngine) -> None:
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:var_samp"}],
        )
        result = await pg_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_var_samp"]) == pytest.approx(
            statistics.variance(amounts), rel=1e-9
        )

    async def test_var_pop_native_postgres(self, pg_env: SlayerQueryEngine) -> None:
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:var_pop"}],
        )
        result = await pg_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_var_pop"]) == pytest.approx(
            statistics.pvariance(amounts), rel=1e-9
        )

    async def test_corr_native_postgres(self, pg_env: SlayerQueryEngine) -> None:
        # CORR(amount, customer_id) — uses two existing columns.
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:corr(other=customer_id)"}],
        )
        result = await pg_env.execute(query=query)
        # Compute expected via Python's statistics.correlation.
        import statistics
        xs = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        ys = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        expected = statistics.correlation(xs, ys)
        assert float(result.data[0]["orders.total_corr_other_customer_id"]) == pytest.approx(
            expected, rel=1e-9
        )

    async def test_covar_samp_native_postgres(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:covar_samp(other=customer_id)"}],
        )
        result = await pg_env.execute(query=query)
        xs = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        ys = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        n = len(xs)
        mx, my = sum(xs) / n, sum(ys) / n
        expected = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (n - 1)
        assert float(
            result.data[0]["orders.total_covar_samp_other_customer_id"]
        ) == pytest.approx(expected, rel=1e-9)

    async def test_covar_pop_native_postgres(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:covar_pop(other=customer_id)"}],
        )
        result = await pg_env.execute(query=query)
        xs = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        ys = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        n = len(xs)
        mx, my = sum(xs) / n, sum(ys) / n
        expected = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / n
        assert float(
            result.data[0]["orders.total_covar_pop_other_customer_id"]
        ) == pytest.approx(expected, rel=1e-9)
