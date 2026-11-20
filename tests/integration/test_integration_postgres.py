"""Integration tests using a real PostgreSQL database via pytest-postgresql."""

import math as _math
import statistics
import tempfile
import uuid
from collections.abc import AsyncIterator, Iterator
from typing import Any, LiteralString, cast

import pytest

from slayer.async_utils import run_sync
from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.core.query import OrderItem, QueryDimension, QueryMetric, SlayerQuery, TimeDimension
from slayer.live_schema import ingestion
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database.client import SlayerSQLClient
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import owned_query_engine
from tests._pg_helpers import pg_query_engine
from tests._reserved_alias_probe import keyword_universe, unquoted_alias_failures
from tests.integration._consecutive_periods_calendar import (
    CALENDAR_CASES,
    assert_calendar_streak,
    seed_statements,
    streak_dataset,
)

pytest.importorskip("pytest_postgresql")

import psycopg  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above
from psycopg.rows import TupleRow  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above
from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

PgIngestEnv = tuple[list[SlayerDataset], DatasourceConfig, psycopg.Connection[TupleRow]]

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
    a YAMLStorage with the orders + customers datasets pre-saved. Returns the
    storage instance — the per-test ``pg_env`` fixture wraps a fresh engine
    around it (engines bind to their event loop, see slayer/database/client.py:144).
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
        for stmt in seed_statements():
            cur.execute(cast(LiteralString, stmt))
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

        orders_model = SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="testpg",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="status", sql="status", datatype=DataType.STRING),
                DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
                DatasetField(name="created_at", sql="created_at", datatype=DataType.DATETIME),

                DatasetField(name="total", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="avg_amount", sql="amount", datatype=DataType.FLOAT),
                # A genuine DOUBLE PRECISION expression field.
                # ``amount`` is NUMERIC, so ``round(sum(amount), 2)`` works on
                # Postgres natively; the non-bare DOUBLE-typed sql gets wrapped
                # in CAST(... AS DOUBLE PRECISION), so SUM yields double and
                # ``round(double precision, int)`` would fail without the
                # PostgresDialect numeric-cast rewrite.
                DatasetField(name="amount_d", sql="amount * 1.0", datatype=DataType.FLOAT),
            ],
        )
        customers_model = SlayerDataset(
            name="customers",
            sql_table="customers",
            data_source="testpg",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="name", sql="name", datatype=DataType.STRING),
                DatasetField(name="region", sql="region", datatype=DataType.STRING),

            ],
        )
        run_sync(storage.save_dataset(orders_model))
        run_sync(storage.save_dataset(customers_model))
        run_sync(storage.save_dataset(streak_dataset("testpg")))

        yield storage
    finally:
        conn.close()
        _drop_module_db(postgresql_proc, db_name)


@pytest.fixture
async def pg_env(_pg_env_storage) -> AsyncIterator[SlayerQueryEngine]:
    """Per-test SlayerQueryEngine wrapping the module-scoped storage. The
    engine is recreated per-test because its async SQLAlchemy engine binds to
    the current event loop."""
    async with owned_query_engine(storage=_pg_env_storage) as engine:
        yield engine


@pytest.mark.integration
class TestPostgresQueries:
    async def test_count_all(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "count(orders.*)"}]})
        result = await pg_env.execute(query=query)
        assert result.row_count == 1
        assert result.data[0]["orders._count"] == 6

    async def test_sum_measure(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(orders.total)"}]})
        result = await pg_env.execute(query=query)
        assert float(result.data[0]["orders.total_sum"]) == 875.0

    async def test_avg_measure(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "avg(orders.avg_amount)"}]})
        result = await pg_env.execute(query=query)
        avg = float(result.data[0]["orders.avg_amount_avg"])
        assert abs(avg - 145.83) < 0.1

    async def test_group_by_status(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "dimensions": ["orders.status"],
        })
        result = await pg_env.execute(query=query)
        by_status = {r["orders.status"]: r["orders._count"] for r in result.data}
        assert by_status["completed"] == 3
        assert by_status["pending"] == 2
        assert by_status["cancelled"] == 1

    async def test_filter_equals(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "filter": ["orders.status == 'completed'"],
        })
        result = await pg_env.execute(query=query)
        assert result.data[0]["orders._count"] == 3

    async def test_filter_gt(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "filter": ["orders.total > 100"],
        })
        result = await pg_env.execute(query=query)
        assert result.data[0]["orders._count"] == 3  # 200, 150, 300

    async def test_order_by_desc(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "dimensions": ["orders.status"],
            "order": [{"field": "count(orders.*)", "direction": "desc"}],
        })
        result = await pg_env.execute(query=query)
        assert result.data[0]["orders.status"] == "completed"

    async def test_limit(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "dimensions": ["orders.status"],
            "limit": 2,
        })
        result = await pg_env.execute(query=query)
        assert result.row_count == 2

    async def test_multiple_measures(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}, {"metric": "sum(orders.total)"}],
            "dimensions": ["orders.status"],
        })
        result = await pg_env.execute(query=query)
        completed = next(r for r in result.data if r["orders.status"] == "completed")
        assert completed["orders._count"] == 3
        assert float(completed["orders.total_sum"]) == 450.0

    async def test_time_dimension_month_granularity(self, pg_env: SlayerQueryEngine) -> None:
        """Postgres supports DATE_TRUNC — this should work unlike SQLite."""
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}],
        })
        result = await pg_env.execute(query=query)
        assert result.row_count == 3  # Jan, Feb, Mar

    async def test_time_dimension_with_date_range(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "time_dimensions": [{
                "field": "orders.created_at",
                "granularity": "month",
                "date_range": ["2024-01-01", "2024-02-28"],
            }],
        })
        result = await pg_env.execute(query=query)
        # Only Jan and Feb orders (4 orders)
        total = sum(r["orders._count"] for r in result.data)
        assert total == 4

    async def test_composite_filter(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "filter": ["orders.status == 'completed' or orders.status == 'pending'"],
        })
        result = await pg_env.execute(query=query)
        assert result.data[0]["orders._count"] == 5  # 3 completed + 2 pending

    async def test_time_shift_with_date_range(self, pg_env: SlayerQueryEngine) -> None:
        """time_shift with date_range should fetch shifted data from outside the filtered range."""
        # Query only March, ask for previous month (February)
        # Seed: Jan(300), Feb(200), Mar(375)
        query = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.created_at", granularity=TimeGranularity.MONTH,
                date_range=["2024-03-01", "2024-03-31"],
            )],
            metrics=[
                QueryMetric(metric="sum(orders.total)"),
                QueryMetric(metric="time_shift(sum(orders.total), -1, 'month')", alias="prev_month"),
            ],
            order=[OrderItem(field="orders.created_at", direction="asc")],
        )
        result = await pg_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(375.0)
        # Previous month (Feb) fetched from DB, not NULL
        assert float(result.data[0]["prev_month"]) == pytest.approx(200.0)

    async def test_consecutive_periods_with_boolean_predicate(self, pg_env: SlayerQueryEngine) -> None:
        """consecutive_periods on a comparison predicate must work on Postgres.

        Postgres rejects `boolean <> integer`, so the CTE generator's numeric
        predicate `<expr> IS NOT NULL AND <expr> <> 0` cannot be used when the
        argument is already boolean. The boolean-aware path must use the
        column directly inside CASE WHEN.
        """
        query = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.created_at", granularity=TimeGranularity.MONTH,
            )],
            metrics=[
                QueryMetric(metric="sum(orders.total)"),
                QueryMetric(metric="consecutive_periods(sum(orders.total) > 200)", alias="positive_run"),
            ],
            order=[OrderItem(field="orders.created_at", direction="asc")],
        )
        result = await pg_env.execute(query=query)
        # Monthly totals: Jan=300 (>200, true), Feb=200 (==200, false),
        # Mar=375 (>200, true). Trailing run lengths: 1, 0, 1.
        assert [r["positive_run"] for r in result.data] == [1, 0, 1]

    @pytest.mark.parametrize("column,granularity", CALENDAR_CASES)
    async def test_consecutive_periods_breaks_on_calendar_gap(
        self, pg_env: SlayerQueryEngine, column: str, granularity: str,
    ) -> None:
        await assert_calendar_streak(pg_env, column=column, granularity=granularity)

    async def test_change_with_date_range(self, pg_env: SlayerQueryEngine) -> None:
        """change() with date_range should fetch previous period from outside the filtered range."""
        query = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.created_at", granularity=TimeGranularity.MONTH,
                date_range=["2024-03-01", "2024-03-31"],
            )],
            metrics=[
                QueryMetric(metric="sum(orders.total)"),
                QueryMetric(metric="change(sum(orders.total))", alias="amount_change"),
            ],
            order=[OrderItem(field="orders.created_at", direction="asc")],
        )
        result = await pg_env.execute(query=query)
        assert result.row_count == 1
        # Mar(375) - Feb(200) = 175
        assert float(result.data[0]["amount_change"]) == pytest.approx(175.0)

    async def test_change_pct_with_date_range(self, pg_env: SlayerQueryEngine) -> None:
        """change_pct() with date_range should compute correct percentage from shifted data."""
        query = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.created_at", granularity=TimeGranularity.MONTH,
                date_range=["2024-03-01", "2024-03-31"],
            )],
            metrics=[
                QueryMetric(metric="sum(orders.total)"),
                QueryMetric(metric="change_pct(sum(orders.total))", alias="pct"),
            ],
            order=[OrderItem(field="orders.created_at", direction="asc")],
        )
        result = await pg_env.execute(query=query)
        assert result.row_count == 1
        # (375 - 200) / 200 = 0.875
        assert float(result.data[0]["pct"]) == pytest.approx(0.875)

    async def test_multiple_date_range_shifts(self, pg_env: SlayerQueryEngine) -> None:
        """Multiple self-join transforms with different offsets should each get correct data."""
        # Query Feb only, ask for both previous (Jan) and next (Mar)
        query = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.created_at", granularity=TimeGranularity.MONTH,
                date_range=["2024-02-01", "2024-02-29"],
            )],
            metrics=[
                QueryMetric(metric="sum(orders.total)"),
                QueryMetric(metric="time_shift(sum(orders.total), -1, 'month')", alias="prev"),
                QueryMetric(metric="time_shift(sum(orders.total), 1, 'month')", alias="next"),
            ],
            order=[OrderItem(field="orders.created_at", direction="asc")],
        )
        result = await pg_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(200.0)
        assert float(result.data[0]["prev"]) == pytest.approx(300.0)  # Jan
        assert float(result.data[0]["next"]) == pytest.approx(375.0)  # Mar


@pytest.fixture
async def pg_cross_dataset_env(postgresql):
    """Postgres env with orders + customers (with score) and explicit relationship."""
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
    run_sync(storage.save_dataset(SlayerDataset(
        name="orders", sql_table="orders", data_source="testpg",
        default_time_dimension="created_at",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="status", sql="status", datatype=DataType.STRING),
            DatasetField(name="customer_id", sql="customer_id", datatype=DataType.FLOAT),
            DatasetField(name="created_at", sql="created_at", datatype=DataType.DATETIME),
            DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),

            DatasetField(name="total", sql="amount", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
    )))
    run_sync(storage.save_dataset(SlayerDataset(
        name="customers", sql_table="customers", data_source="testpg",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="name", sql="name", datatype=DataType.STRING),

            DatasetField(name="avg_score", sql="score", datatype=DataType.FLOAT),
        ],
    )))
    async with owned_query_engine(storage=storage) as engine:
        yield engine


@pytest.mark.integration
class TestCrossModelAndMultistage:
    async def test_cross_model_measure(self, pg_cross_dataset_env: SlayerQueryEngine) -> None:
        """Cross-dataset metric: monthly order count + avg customer score."""
        query = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.created_at", granularity=TimeGranularity.MONTH,
            )],
            metrics=[QueryMetric(metric="count(orders.*)"), QueryMetric(metric="avg(customers.avg_score)")],
            order=[OrderItem(field="orders.created_at", direction="asc")],
        )
        result = await pg_cross_dataset_env.execute(query=query)
        assert result.row_count == 3
        # Associated: each month averages the distinct customers its orders reach.
        assert [float(r["customers.avg_score_avg"]) for r in result.data] == pytest.approx([90.0, 60.0, 85.0])

    async def test_query_list_named(self, pg_cross_dataset_env: SlayerQueryEngine) -> None:
        """Query list: named sub-query referenced by main query."""
        inner = SlayerQuery(
            name="monthly", population="orders",
            time_dimensions=[TimeDimension(
                field="orders.created_at", granularity=TimeGranularity.MONTH,
            )],
            metrics=[QueryMetric(metric="count(orders.*)"), QueryMetric(metric="sum(orders.total)")],
        )
        outer = SlayerQuery(population="monthly", metrics=[QueryMetric(metric="count(monthly.*)")])
        result = await pg_cross_dataset_env.execute(query=[inner, outer])
        assert result.data[0]["monthly._count"] == 3

    async def test_create_model_from_query(self, pg_cross_dataset_env: SlayerQueryEngine) -> None:
        """Save a query as a permanent dataset, then query it."""
        source = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.created_at", granularity=TimeGranularity.MONTH,
            )],
            metrics=[QueryMetric(metric="count(orders.*)"), QueryMetric(metric="sum(orders.total)")],
        )
        saved = await pg_cross_dataset_env.create_dataset_from_query(query=source, name="pg_monthly")
        assert saved.source_queries is not None
        result = await pg_cross_dataset_env.execute(
            query=SlayerQuery(population="pg_monthly", metrics=[QueryMetric(metric="count(pg_monthly.*)")])
        )
        assert result.data[0]["pg_monthly._count"] == 3

    async def test_sql_dimension(self, pg_cross_dataset_env: SlayerQueryEngine) -> None:
        """SQL expression dimension via DatasetExtension with Postgres."""
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="CASE WHEN orders.amount > 100 THEN 'high' ELSE 'low' END", alias="tier")],
            metrics=[QueryMetric(metric="count(orders.*)")],
        )
        result = await pg_cross_dataset_env.execute(query=query)
        by_tier = {r["tier"]: r["orders._count"] for r in result.data}
        # high: 200, 150, 300 = 3; low: 100, 50, 25 = 3
        assert by_tier["high"] == 3
        assert by_tier["low"] == 3


async def _ingest(ds: DatasourceConfig, **kwargs: Any) -> list[SlayerDataset]:
    client = SlayerSQLClient(datasource=ds)
    try:
        return await ingestion.ingest_datasource_async(ds, client=client, **kwargs)
    finally:
        await client.aclose()
        client.close()


@pytest.fixture(scope="module")
def pg_ingest_env(postgresql_proc) -> Iterator[PgIngestEnv]:
    """Set up tables with FK relationships and ingest via rollup.

    Module-scoped: tests destructure ``(models, ds, _)`` and build their own
    ephemeral storage from the returned datasets — they never mutate the fixture
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
        cur.execute("COMMENT ON TABLE orders IS 'All orders'")
        cur.execute("COMMENT ON COLUMN orders.amount IS 'Order amount'")
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

        models = run_sync(_ingest(ds, schema="public"))
        yield models, ds, conn
    finally:
        conn.close()
        _drop_module_db(postgresql_proc, db_name)


@pytest.fixture
async def pg_ingest_engine(pg_ingest_env: PgIngestEnv, tmp_path) -> AsyncIterator[SlayerQueryEngine]:
    """A per-test engine over the ingested datasets."""
    models, ds, _ = pg_ingest_env
    async with pg_query_engine(ds, base_dir=str(tmp_path), datasets=models) as engine:
        yield engine


@pytest.mark.integration
class TestRollupIngestion:
    def test_orders_has_own_columns_only(self, pg_ingest_env: PgIngestEnv) -> None:
        """After ingestion, datasets only have their own fields (no flattened joined dims)."""
        models, _, _ = pg_ingest_env
        orders = next(m for m in models if m.name == "orders")

        col_names = [c.name for c in orders.fields]
        # Should have own fields only.
        assert "id" in col_names
        assert "customer_id" in col_names
        # In v2, every non-joined field appears once (numeric fields included).
        assert "amount" in col_names
        # Joined dimensions are resolved via relationship graph, not pre-flattened.
        assert not any("." in name for name in col_names)

    def test_orders_uses_sql_table_with_joins(self, pg_ingest_env: PgIngestEnv) -> None:
        models, _, _ = pg_ingest_env
        orders = next(m for m in models if m.name == "orders")

        # Datasets with relationships use sql_table (not baked sql) + explicit relationships
        assert orders.sql_table is not None
        assert orders.sql is None
        assert len(orders.relationships) > 0

    def test_regions_has_no_rollup(self, pg_ingest_env: PgIngestEnv) -> None:
        models, _, _ = pg_ingest_env
        regions = next(m for m in models if m.name == "regions")

        # Regions references nothing, should keep sql_table
        assert regions.sql_table is not None
        assert regions.sql is None

    def test_orders_has_no_named_measures_after_ingest(self, pg_ingest_env: PgIngestEnv) -> None:
        """v2 auto-ingest leaves dataset.metrics (expression list) empty.

        Row-level fields live on .fields; named-expression metrics are an
        opt-in library users populate themselves.
        """
        models, _, _ = pg_ingest_env
        orders = next(m for m in models if m.name == "orders")
        assert orders.metrics == []
        col_names = [c.name for c in orders.fields]
        assert "amount" in col_names

    async def test_rollup_query_group_by_customer(self, pg_ingest_engine: SlayerQueryEngine) -> None:
        """Query orders grouped by rolled-up customer name."""
        engine = pg_ingest_engine

        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "dimensions": ["customers.name"],
        })
        result = await engine.execute(query=query)

        by_name = {r["customers.name"]: r["orders._count"] for r in result.data}
        assert by_name["Acme"] == 2
        assert by_name["Globex"] == 1
        assert by_name["Initech"] == 1

    async def test_rollup_query_group_by_region(self, pg_ingest_engine: SlayerQueryEngine) -> None:
        """Query orders grouped by transitively rolled-up region name."""
        engine = pg_ingest_engine

        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}, {"metric": "sum(orders.amount)"}],
            "dimensions": ["customers.regions.name"],
        })
        result = await engine.execute(query=query)

        by_region = {r["customers.regions.name"]: r for r in result.data}
        assert by_region["US"]["orders._count"] == 3  # Acme(2) + Initech(1)
        assert by_region["EU"]["orders._count"] == 1  # Globex(1)
        assert float(by_region["US"]["orders.amount_sum"]) == 450.0  # 100+200+150
        assert float(by_region["EU"]["orders.amount_sum"]) == 50.0

    async def test_dotted_dimension_single_hop(self, pg_ingest_engine: SlayerQueryEngine) -> None:
        """Dotted dimension 'customers.name' resolves to 'customers__name'."""
        engine = pg_ingest_engine

        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "dimensions": ["customers.name"],
        })
        result = await engine.execute(query=query)

        by_name = {r["customers.name"]: r["orders._count"] for r in result.data}
        assert by_name["Acme"] == 2
        assert by_name["Globex"] == 1
        assert by_name["Initech"] == 1

    async def test_dotted_dimension_multi_hop(self, pg_ingest_engine: SlayerQueryEngine) -> None:
        """Multi-hop dotted dimension 'customers.regions.name' resolves transitively."""
        engine = pg_ingest_engine

        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "dimensions": ["customers.regions.name"],
        })
        result = await engine.execute(query=query)

        # Same as regions__name: US=3, EU=1
        by_region = {r["customers.regions.name"]: r["orders._count"] for r in result.data}
        assert by_region["US"] == 3
        assert by_region["EU"] == 1

    async def test_selective_joins_no_joined_dims(self, pg_ingest_engine: SlayerQueryEngine) -> None:
        """Query using only source-table dimensions should not include JOINs."""
        engine = pg_ingest_engine

        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "count(orders.*)"}]})
        result = await engine.execute(query=query)
        # No joined dimensions → SQL should not have LEFT JOIN
        assert "LEFT JOIN" not in result.sql
        assert result.data[0]["orders._count"] == 4

    async def test_selective_joins_single_hop(self, pg_ingest_engine: SlayerQueryEngine) -> None:
        """Query with customer dimension should JOIN customers but NOT regions."""
        engine = pg_ingest_engine

        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "dimensions": ["customers.name"],
        })
        result = await engine.execute(query=query)
        # Should JOIN customers but NOT regions
        assert "LEFT JOIN" in result.sql
        assert "customers" in result.sql
        assert "regions" not in result.sql

    async def test_selective_joins_transitive(self, pg_ingest_engine: SlayerQueryEngine) -> None:
        """Query with region dimension should include both customers and regions JOINs."""
        engine = pg_ingest_engine

        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "dimensions": ["customers.regions.name"],
        })
        result = await engine.execute(query=query)
        # Needs both customers (intermediate) and regions (target)
        assert "customers" in result.sql
        assert "regions" in result.sql

    def test_orders_has_joins_metadata(self, pg_ingest_env: PgIngestEnv) -> None:
        """Ingested datasets should have only direct relationship metadata."""
        models, _, _ = pg_ingest_env
        orders = next(m for m in models if m.name == "orders")

        # orders → customers (direct FK)
        join_targets = [j.to for j in orders.relationships]
        assert "customers" in join_targets

        # Multi-hop targets (regions) are NOT baked in — resolved at query time
        assert "regions" not in join_targets

        # Each relationship has at least one key pair
        for j in orders.relationships:
            assert len(j.column_pairs) >= 1
            for pair in j.column_pairs:
                assert len(pair) == 2  # [source_dim, target_dim]

    def test_regions_has_no_joins(self, pg_ingest_env: PgIngestEnv) -> None:
        """Datasets with no FK references should have empty relationships."""
        models, _, _ = pg_ingest_env
        regions = next(m for m in models if m.name == "regions")
        assert regions.relationships == []

    async def test_joins_serialize_to_yaml(self, pg_ingest_env: PgIngestEnv) -> None:
        """Relationships should survive YAML round-trip."""
        models, ds, _ = pg_ingest_env
        orders = next(m for m in models if m.name == "orders")

        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_dataset(orders)

        loaded = await storage.get_dataset("orders")
        assert loaded is not None
        assert len(loaded.relationships) == len(orders.relationships)
        for orig, loaded_j in zip(orders.relationships, loaded.relationships):
            assert orig.to == loaded_j.to
            assert orig.column_pairs == loaded_j.column_pairs


@pytest.mark.integration
class TestPostgresMedianPercentile:
    """Live execution of median/percentile against Postgres.

    Postgres has native ``PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)``;
    these tests pin the round-trip so the dialect-aware ``_build_percentile``
    refactor doesn't silently regress.
    """

    async def test_median(self, pg_env: SlayerQueryEngine) -> None:
        # amounts = [100, 200, 50, 150, 75, 300] -> median 125
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "median(orders.total)"}]})
        result = await pg_env.execute(query=query)
        assert float(result.data[0]["orders.total_median"]) == pytest.approx(125.0)

    async def test_percentile_quartiles(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            population="orders",
            metrics=[
                {"metric": "percentile(orders.total, p=0.25)"},
                {"metric": "percentile(orders.total, p=0.75)"},
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
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "median(orders.total)"}],
            "dimensions": ["orders.status"],
        })
        result = await pg_env.execute(query=query)
        by_status = {
            r["orders.status"]: float(r["orders.total_median"]) for r in result.data
        }
        assert by_status["completed"] == pytest.approx(150)
        assert by_status["pending"] == pytest.approx(175)
        assert by_status["cancelled"] == pytest.approx(75)


@pytest.mark.integration
class TestPostgresStatAggregations:
    """Cross-dialect smoke: the new statistical aggregations
    (stddev_samp, stddev_pop, var_samp, var_pop, corr) must produce the
    same numeric results on Postgres native functions as the SQLite UDF
    path produces. Within rel=1e-9.
    """

    async def test_stddev_samp_native_postgres(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "stddev_samp(orders.total)"}]})
        result = await pg_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_stddev_samp"]) == pytest.approx(
            statistics.stdev(amounts), rel=1e-9
        )

    async def test_stddev_pop_native_postgres(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "stddev_pop(orders.total)"}],
        })
        result = await pg_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_stddev_pop"]) == pytest.approx(
            statistics.pstdev(amounts), rel=1e-9
        )

    async def test_var_samp_native_postgres(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "var_samp(orders.total)"}]})
        result = await pg_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_var_samp"]) == pytest.approx(
            statistics.variance(amounts), rel=1e-9
        )

    async def test_var_pop_native_postgres(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            population="orders",
            metrics=[{"metric": "var_pop(orders.total)"}],
        )
        result = await pg_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_var_pop"]) == pytest.approx(
            statistics.pvariance(amounts), rel=1e-9
        )

    async def test_corr_native_postgres(self, pg_env: SlayerQueryEngine) -> None:
        # CORR(amount, customer_id) — uses two existing fields.
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "corr(orders.total, other=orders.customer_id)"}],
        })
        result = await pg_env.execute(query=query)
        # Compute expected via Python's statistics.correlation.
        xs = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        ys = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        expected = statistics.correlation(xs, ys)
        assert float(result.data[0]["orders.total_corr_other_customer_id"]) == pytest.approx(
            expected, rel=1e-9
        )

    async def test_covar_samp_native_postgres(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "covar_samp(orders.total, other=orders.customer_id)"}],
        })
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
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "covar_pop(orders.total, other=orders.customer_id)"}],
        })
        result = await pg_env.execute(query=query)
        xs = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        ys = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        n = len(xs)
        mx, my = sum(xs) / n, sum(ys) / n
        expected = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / n
        assert float(
            result.data[0]["orders.total_covar_pop_other_customer_id"]
        ) == pytest.approx(expected, rel=1e-9)

    async def test_log10_round_trip_postgres(self, pg_env: SlayerQueryEngine) -> None:
        """A `log10(amount)` expression must execute correctly on
        Postgres (native single-arg LOG10) and the emitted SQL must contain
        `log10(...)` rather than the canonicalised `LOG(10, ...)`."""
        # Add a DatasetField.sql with log10 to the existing orders dataset. The
        # fixture's storage already has the table populated, so we just save
        # the dataset with the extra field.
        existing = await pg_env.storage.get_dataset("orders")
        assert existing is not None
        cols = list(existing.fields) + [
            DatasetField(name="log_amount", sql="log10(amount)", datatype=DataType.FLOAT),
        ]
        await pg_env.save_dataset(
            SlayerDataset(
                name=existing.name,
                sql_table=existing.sql_table,
                data_source=existing.data_source,
                fields=cols,
            )
        )

        result = await pg_env.execute(
            SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "max(orders.log_amount)"}]})
        )
        # max(amount) = 300, log10(300) ≈ 2.4771
        assert float(result.data[0]["orders.log_amount_max"]) == pytest.approx(
            _math.log10(300.0), rel=1e-9
        )

        dry = await pg_env.execute(
            SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "max(orders.log_amount)"}]}),
            dry_run=True,
        )
        assert dry.sql is not None
        sql = dry.sql.lower()
        assert "log10(" in sql, (
            f"Expected literal log10(...) in emitted SQL, got:\n{dry.sql}"
        )
        assert "log(10," not in sql.replace(" ", ""), (
            f"Emitted SQL must not canonicalise log10 to LOG(10, ...):\n{dry.sql}"
        )



# ---------------------------------------------------------------------------
# window functions in filters, Postgres parity
# ---------------------------------------------------------------------------


@pytest.fixture
async def planets_pg_env(postgresql):
    """Planets fixture (Postgres): a DatasetField.sql with `row_number() over (...)`."""
    cur = postgresql.cursor()
    cur.execute(
        """
        CREATE TABLE planets (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            mass NUMERIC(10, 4) NOT NULL
        )
        """
    )
    cur.executemany(
        "INSERT INTO planets VALUES (%s, %s, %s)",
        [
            (1, "Mercury", 0.33),
            (2, "Venus", 4.87),
            (3, "Earth", 5.97),
            (4, "Mars", 0.642),
            (5, "Jupiter", 1898.0),
            (6, "Saturn", 568.0),
            (7, "Uranus", 86.8),
            (8, "Neptune", 102.0),
        ],
    )
    postgresql.commit()

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)

    info = postgresql.info
    await storage.save_datasource(DatasourceConfig(
        name="planets_pg",
        type="postgres",
        host=info.host,
        port=info.port,
        database=info.dbname,
        username=info.user,
        password="",
    ))
    await storage.save_dataset(
        SlayerDataset(
            name="planets",
            sql_table="planets",
            data_source="planets_pg",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="name", sql="name", datatype=DataType.STRING),
                DatasetField(name="mass", sql="mass", datatype=DataType.FLOAT),
                DatasetField(
                    name="rn",
                    sql="row_number() over (order by mass desc)",
                    datatype=DataType.FLOAT,
                ),
            ],
        )
    )
    async with owned_query_engine(storage=storage) as engine:
        yield engine


@pytest.mark.integration
async def test_filter_on_windowed_column_postgres_raises(planets_pg_env):
    """Postgres parity: filtering a windowed DatasetField.sql
    raises (use rank-family transforms instead)."""
    engine = planets_pg_env
    query = SlayerQuery(
        population="planets",
        dimensions=["planets.name"],
        filter=["planets.rn <= 3"],
    )
    with pytest.raises(ValueError, match="(?i)window function|rank"):
        await engine.execute(query)

# ---------------------------------------------------------------------------
# Cross-dataset derived ``DatasetField.sql`` chaining (Postgres)
# ---------------------------------------------------------------------------


@pytest.fixture
async def pg_derived_chain_env(postgresql):
    """Postgres A→B fixture with a derived field on B referenced by A."""
    cur = postgresql.cursor()
    cur.execute("CREATE TABLE b_tbl (id INTEGER PRIMARY KEY, foo_raw NUMERIC)")
    cur.execute(
        "CREATE TABLE a_tbl (id INTEGER PRIMARY KEY, bar NUMERIC, b_id INTEGER, raw_a NUMERIC)"
    )
    cur.executemany("INSERT INTO b_tbl VALUES (%s, %s)", [(1, 200), (2, 50)])
    cur.executemany(
        "INSERT INTO a_tbl VALUES (%s, %s, %s, %s)",
        [(10, 4, 1, 100), (11, 1, 2, 5)],
    )
    postgresql.commit()

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    info = postgresql.info
    await storage.save_datasource(
        DatasourceConfig(
            name="testpg", type="postgres",
            host=info.host, port=info.port, database=info.dbname,
            username=info.user, password="",
        )
    )
    await storage.save_dataset(
        SlayerDataset(
            name="b_tbl",
            data_source="testpg",
            sql_table="b_tbl",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="foo_raw", sql="foo_raw", datatype=DataType.FLOAT),
                DatasetField(name="foo_normalized", sql="foo_raw / 100.0", datatype=DataType.FLOAT),
            ],
        )
    )
    await storage.save_dataset(
        SlayerDataset(
            name="a_tbl",
            data_source="testpg",
            sql_table="a_tbl",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="bar", sql="bar", datatype=DataType.FLOAT),
                DatasetField(name="b_id", sql="b_id", datatype=DataType.FLOAT),
                DatasetField(name="raw_a", sql="raw_a", datatype=DataType.FLOAT),
                DatasetField(
                    name="ratio_using_derived",
                    sql="a_tbl.bar / b_tbl.foo_normalized",
                    datatype=DataType.FLOAT,
                ),
            ],
            relationships=[DatasetRelationship(to="b_tbl", from_columns=["b_id"], to_columns=["id"])],
        )
    )
    async with owned_query_engine(storage=storage) as engine:
        yield engine


@pytest.mark.integration
async def test_integration_postgres_cross_model_derived_columnsql(
    pg_derived_chain_env: SlayerQueryEngine,
) -> None:
    response = await pg_derived_chain_env.execute(
        SlayerQuery(
            population="a_tbl",
            dimensions=[
                QueryDimension(field="a_tbl.id"),
                QueryDimension(field="a_tbl.ratio_using_derived"),
            ],
            order=[OrderItem(field="a_tbl.id", direction="asc")],
        )
    )
    assert response.row_count == 2
    assert float(response.data[0]["a_tbl.ratio_using_derived"]) == pytest.approx(2.0)
    assert float(response.data[1]["a_tbl.ratio_using_derived"]) == pytest.approx(2.0)


@pytest.mark.integration
class TestPostgresDev1576Heals:
    """round()/abs() execution (incl. the double-precision cast
    path) and aggregation-alias healing against a real Postgres."""

    async def test_round_two_args_over_double_executes(
        self, pg_env: SlayerQueryEngine,
    ) -> None:
        # SUM(amount_d) = 875.0 (double precision); /8.0 = 109.375 → round 2.
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "round(sum(orders.amount_d) / 8.0, 2)", "alias": "r"}],
        })
        result = await pg_env.execute(query=query)
        assert float(result.data[0]["r"]) == pytest.approx(109.38)

    async def test_round_one_arg_over_double_executes(
        self, pg_env: SlayerQueryEngine,
    ) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "round(sum(orders.amount_d) / 8.0)", "alias": "r"}],
        })
        result = await pg_env.execute(query=query)
        assert float(result.data[0]["r"]) == pytest.approx(109.0)

    async def test_abs_over_double_executes(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "abs(sum(orders.amount_d) - 1000.0)", "alias": "a"}],
        })
        result = await pg_env.execute(query=query)
        assert float(result.data[0]["a"]) == pytest.approx(125.0)

    async def test_count_distinct_alias_executes(
        self, pg_env: SlayerQueryEngine,
    ) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "countd(orders.status)"}]})
        result = await pg_env.execute(query=query)
        assert result.data[0]["orders.status_count_distinct"] == 3

    async def test_stddev_alias_executes(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "stddev(orders.total)"}],
        })
        result = await pg_env.execute(query=query)
        # Sample stddev of [100,200,50,150,75,300] (mean 145.83) ≈ 92.76.
        assert float(result.data[0]["orders.total_stddev_samp"]) == pytest.approx(
            92.76, abs=0.1
        )


# ---------------------------------------------------------------------------
# Compiler must emit valid Postgres SQL.
#   Flavor A — ORDER BY on an unprojected/renamed column.
#   Flavor B — mixed-case identifiers (DatasetField.sql, filters, JOIN ON) must be
#              double-quoted so Postgres does not fold them to lowercase.
# The datasets below deliberately write identifiers UNQUOTED (as an OTF agent
# naturally would); the fix is what makes these queries execute.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def _pg_dev1645_storage(postgresql_proc, tmp_path_factory):
    conn, db_name = _create_module_db(postgresql_proc)
    try:
        cur = conn.cursor()
        # Mixed-case column + mixed-case PK created via QUOTED DDL so their true
        # stored case is genuinely mixed (Postgres would otherwise fold to lower).
        cur.execute("""
            CREATE TABLE clusters (
                "CLSTR_PIN" INTEGER PRIMARY KEY,
                score NUMERIC(10,2) NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                "StateFlag" TEXT NOT NULL,
                acct_form TEXT NOT NULL,
                clu_ref INTEGER REFERENCES clusters("CLSTR_PIN"),
                created_at TIMESTAMP NOT NULL
            )
        """)
        cur.executemany(
            'INSERT INTO clusters ("CLSTR_PIN", score) VALUES (%s, %s)',
            [(10, 1.5), (20, 2.5)],
        )
        cur.executemany(
            "INSERT INTO accounts VALUES (%s, %s, %s, %s, %s)",
            [
                (1, "Dormant", "Bot", 10, "2024-01-15 10:00:00"),
                (2, "Active", "Human", 20, "2024-02-20 11:00:00"),
                (3, "Dormant", "Human", 10, "2024-03-01 09:00:00"),
            ],
        )
        conn.commit()

        storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp("pg_dev1645")))
        info = postgresql_proc
        run_sync(storage.save_datasource(DatasourceConfig(
            name="testpg", type="postgres", host=info.host, port=info.port,
            database=db_name, username=info.user, password="",
        )))

        clusters_model = SlayerDataset(
            name="clusters", sql_table="clusters", data_source="testpg",
            fields=[
                # PK column name is mixed-case and written UNQUOTED in the dataset.
                DatasetField(name="CLSTR_PIN", sql="CLSTR_PIN", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="score", sql="score", datatype=DataType.FLOAT),
            ],
        )
        accounts_model = SlayerDataset(
            name="accounts", sql_table="accounts", data_source="testpg",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                # Derived boolean referencing mixed-case StateFlag UNQUOTED (the
                # fake_account_23 repro). Fix must emit "StateFlag".
                DatasetField(name="is_inactive", datatype=DataType.BOOLEAN,
                       sql="CASE WHEN StateFlag = 'Dormant' THEN TRUE ELSE FALSE END"),
                DatasetField(name="acct_form", sql="acct_form", datatype=DataType.STRING),
                DatasetField(name="clu_ref", sql="clu_ref", datatype=DataType.INTEGER),
                DatasetField(name="created_at", sql="created_at", datatype=DataType.DATETIME),
            ],
            # Relationship key CLSTR_PIN written UNQUOTED (the fake_account_15 repro).
            relationships=[DatasetRelationship(to="clusters", from_columns=["clu_ref"], to_columns=["CLSTR_PIN"])],
        )
        run_sync(storage.save_dataset(clusters_model))
        run_sync(storage.save_dataset(accounts_model))
        yield storage
    finally:
        conn.close()
        _drop_module_db(postgresql_proc, db_name)


@pytest.fixture
async def pg_dev1645(_pg_dev1645_storage) -> AsyncIterator[SlayerQueryEngine]:
    async with owned_query_engine(storage=_pg_dev1645_storage) as engine:
        yield engine


@pytest.mark.integration
class TestDev1645ValidPostgres:
    async def test_flavor_b_mixed_case_column_sql_executes(
        self, pg_dev1645: SlayerQueryEngine,
    ) -> None:
        """Filter on a derived field whose SQL references mixed-case StateFlag
        (the fake_account_23 repro) — must execute, not raise UndefinedColumn."""
        query = SlayerQuery.model_validate({
            "population": "accounts",
            "metrics": [{"metric": "count(accounts.*)", "alias": "cnt"}],
            "filter": ["accounts.is_inactive == True"],
        })
        result = await pg_dev1645.execute(query=query)
        assert result.data[0]["cnt"] == 2  # two Dormant rows

    async def test_flavor_b_mixed_case_join_key_executes(
        self, pg_dev1645: SlayerQueryEngine,
    ) -> None:
        """Cross-dataset dimension across a relationship whose key is mixed-case CLSTR_PIN
        (the fake_account_15 repro) — must execute."""
        query = SlayerQuery.model_validate({
            "population": "accounts",
            "dimensions": [QueryDimension(field="clusters.score")],
            "metrics": [{"metric": "count(accounts.*)", "alias": "cnt"}],
        })
        result = await pg_dev1645.execute(query=query)
        by_score = {float(r["clusters.score"]): r["cnt"] for r in result.data}
        assert by_score[1.5] == 2
        assert by_score[2.5] == 1

    async def test_flavor_a_orderby_nonprojected_column_executes(
        self, pg_dev1645: SlayerQueryEngine,
    ) -> None:
        """Raw-row query ordering by a base field that is not in the projection
        (the mental_healths_1 / organ_transplant_13 repro) — must execute."""
        query = SlayerQuery(
            population="accounts",
            dimensions=[QueryDimension(field="accounts.acct_form")],
            distinct_dimension_values=False,
            order=[OrderItem(field="accounts.created_at", direction="desc")],
        )
        result = await pg_dev1645.execute(query=query)
        assert result.row_count == 3

    async def test_grouped_row_column_order_max_wrap_executes(
        self, pg_dev1645: SlayerQueryEngine,
    ) -> None:
        """Phase 1: a GROUPED query ordering by an unprojected LOCAL
        row field materialises a hidden ``max(created_at)`` wrap. Verified on
        real Postgres — the shape must both execute and sort correctly.

        Latest created_at per acct_form: Human 2024-03-01, Bot 2024-01-15 —
        so descending puts Human first. The grain must stay one row per
        acct_form, and the wrap must not surface in the response.
        """
        query = SlayerQuery.model_validate({
            "population": "accounts",
            "dimensions": [QueryDimension(field="accounts.acct_form")],
            "metrics": [{"metric": "count(accounts.*)", "alias": "cnt"}],
            "order": [OrderItem(field="accounts.created_at", direction="desc")],
        })
        result = await pg_dev1645.execute(query=query)
        assert [r["accounts.acct_form"] for r in result.data] == ["Human", "Bot"]
        assert result.row_count == 2
        assert all("created_at" not in c for c in result.columns), result.columns

    async def test_ungrouped_joined_column_order_executes(
        self, pg_dev1645: SlayerQueryEngine,
    ) -> None:
        """Phase 1: a RAW-ROWS query ordering by a JOINED field pulls
        the join (Law 1) and split-emits ``clusters.score``. Verified on real
        Postgres — the split reference must bind, which is exactly what the
        composite ``"clusters.score"`` form failed to do.

        scores: account 1 -> 1.5, 2 -> 2.5, 3 -> 1.5; ascending by score then
        id gives 1, 3, 2.
        """
        query = SlayerQuery(
            population="accounts",
            dimensions=[QueryDimension(field="accounts.id")],
            distinct_dimension_values=False,
            order=[
                OrderItem(field="clusters.score", direction="asc"),
                OrderItem(field="accounts.id", direction="asc"),
            ],
        )
        result = await pg_dev1645.execute(query=query)
        assert [r["accounts.id"] for r in result.data] == [1, 3, 2]
        assert all("score" not in c for c in result.columns), result.columns


# ---------------------------------------------------------------------------
# Reserved-word dataset name executes live against Postgres.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def _pg_reserved_storage(postgresql_proc, tmp_path_factory):
    """Module-scoped Postgres DB with a reserved-word ("Grant") table joined to
    "Merchant", plus a YAMLStorage with a `grant` dataset (reserved name) that
    joins to `merchant`. Exercises alias/qualifier + join_cond quoting
    end-to-end."""
    conn, db_name = _create_module_db(postgresql_proc)
    try:
        cur = conn.cursor()
        cur.execute('CREATE TABLE "Merchant" (id INTEGER PRIMARY KEY, name TEXT NOT NULL)')
        cur.execute("""
            CREATE TABLE "Grant" (
                id INTEGER PRIMARY KEY,
                namespace TEXT NOT NULL,
                amount NUMERIC(10,2) NOT NULL,
                "merchantId" INTEGER REFERENCES "Merchant"(id),
                created_at TIMESTAMP NOT NULL
            )
        """)
        cur.executemany('INSERT INTO "Merchant" VALUES (%s, %s)',
                        [(1, "Acme"), (2, "Globex")])
        cur.executemany(
            'INSERT INTO "Grant" VALUES (%s, %s, %s, %s, %s)',
            [
                (1, "org_a", 100, 1, "2024-01-15 10:00:00"),
                (2, "org_a", 200, 1, "2024-01-20 11:00:00"),
                (3, "org_b", 50, 2, "2024-02-10 09:00:00"),
            ],
        )
        # A non-reserved root (usage) joining TO the reserved `grant` dataset —
        # exercises relationship discovery + execution of a derived field that
        # references a reserved joined dataset.
        cur.execute("""
            CREATE TABLE usage (
                id INTEGER PRIMARY KEY,
                grant_id INTEGER REFERENCES "Grant"(id),
                qty INTEGER NOT NULL
            )
        """)
        cur.executemany(
            "INSERT INTO usage VALUES (%s, %s, %s)",
            [(1, 1, 5), (2, 1, 3), (3, 3, 7)],
        )
        conn.commit()

        tmpdir = str(tmp_path_factory.mktemp("pg_reserved"))
        storage = YAMLStorage(base_dir=tmpdir)
        info = postgresql_proc
        run_sync(storage.save_datasource(DatasourceConfig(
            name="testpg", type="postgres", host=info.host, port=info.port,
            database=db_name, username=info.user, password="",
        )))
        merchant = SlayerDataset(
            name="merchant", sql_table='"Merchant"', data_source="testpg",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="name", sql="name", datatype=DataType.STRING),
            ],
        )
        grant = SlayerDataset(
            name="grant", sql_table='"Grant"', data_source="testpg",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="namespace", sql="namespace", datatype=DataType.STRING),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="merchantId", sql="merchantId", datatype=DataType.INTEGER),
                DatasetField(name="created_at", sql="created_at", datatype=DataType.DATETIME),
            ],
            relationships=[DatasetRelationship(to="merchant", from_columns=["merchantId"], to_columns=["id"])],
        )
        usage = SlayerDataset(
            name="usage", sql_table="usage", data_source="testpg",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="grant_id", sql="grant_id", datatype=DataType.INTEGER),
                DatasetField(name="qty", sql="qty", datatype=DataType.INTEGER),
                # derived field referencing the reserved joined dataset `grant`
                DatasetField(name="bumped", sql="grant.amount + 1", datatype=DataType.FLOAT),
            ],
            relationships=[DatasetRelationship(to="grant", from_columns=["grant_id"], to_columns=["id"])],
        )
        run_sync(storage.save_dataset(merchant))
        run_sync(storage.save_dataset(grant))
        run_sync(storage.save_dataset(usage))
        yield storage
    finally:
        conn.close()
        _drop_module_db(postgresql_proc, db_name)


@pytest.fixture
async def pg_reserved(_pg_reserved_storage) -> AsyncIterator[SlayerQueryEngine]:
    async with owned_query_engine(storage=_pg_reserved_storage) as engine:
        yield engine


@pytest.mark.integration
class TestPostgresReservedWordModel:
    async def test_standalone_grant_dims_count_executes(self, pg_reserved: SlayerQueryEngine) -> None:
        """The exact repro: a dims + *:count query on the `grant`
        dataset must execute (previously: syntax error at or near "grant")."""
        query = SlayerQuery.model_validate({
            "population": "grant",
            "dimensions": ["grant.namespace"],
            "metrics": ["count(grant.*)"],
        })
        result = await pg_reserved.execute(query=query)
        assert result.row_count == 2  # org_a, org_b
        assert sum(r["grant._count"] for r in result.data) == 3

    async def test_join_from_grant_executes(self, pg_reserved: SlayerQueryEngine) -> None:
        """Join FROM the reserved dataset on a camelCase FK (`merchantId`)."""
        query = SlayerQuery.model_validate({
            "population": "grant",
            "dimensions": ["merchant.name"],
            "metrics": ["count(grant.*)"],
        })
        result = await pg_reserved.execute(query=query)
        by_merchant = {r["merchant.name"]: r["grant._count"] for r in result.data}
        assert by_merchant == {"Acme": 2, "Globex": 1}

    async def test_grant_measure_and_filter_executes(self, pg_reserved: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "grant",
            "dimensions": ["grant.namespace"],
            "metrics": [{"metric": "sum(grant.amount)"}],
            "filter": ["grant.amount > 60"],
        })
        result = await pg_reserved.execute(query=query)
        by_ns = {r["grant.namespace"]: float(r["grant.amount_sum"]) for r in result.data}
        assert by_ns == {"org_a": 300}

    async def test_query_backed_model_with_reserved_short_executes(
        self, pg_reserved: SlayerQueryEngine,
    ) -> None:
        """``query_engine._query_as_model`` fix: a cross-dataset metric
        renamed to a reserved word (``order``) becomes a virtual-dataset field
        exposed via ``... AS "order"`` in the wrapped SQL; it must quote and
        execute."""
        query = SlayerQuery.model_validate({
            "population": "grant",
            "dimensions": ["grant.namespace"],
            "metrics": [{"metric": "count_distinct(merchant.id)", "alias": "order"}],
        })
        await pg_reserved.create_dataset_from_query(
            query=query, name="grants_vm", save=True,
        )
        over_model = {"population": "grants_vm", "dimensions": ["grants_vm.namespace", "grants_vm.order"]}
        sql = (await pg_reserved.execute(query=over_model, dry_run=True)).sql or ""
        assert '"order"' in sql, sql
        # Executing the stored backing query (which contains ``AS "order"``)
        # must not raise a Postgres syntax error — that is the regression.
        result = await pg_reserved.execute(query="grants_vm")
        assert result.row_count == 2  # org_a, org_b
        # the reserved-short column resolves (proves AS "order" round-tripped)
        order_key = next(k for k in result.data[0] if k.endswith("order"))
        assert all(isinstance(row[order_key], int) for row in result.data)

    async def test_derived_column_referencing_reserved_join_executes(
        self, pg_reserved: SlayerQueryEngine,
    ) -> None:
        """A derived field on a non-reserved root that
        references a reserved joined dataset (`grant.amount + 1`) must both DISCOVER
        the relationship and execute — previously the join was silently dropped."""
        query = SlayerQuery.model_validate({
            "population": "usage",
            "dimensions": ["usage.bumped"],
            "metrics": ["count(usage.*)"],
        })
        result = await pg_reserved.execute(query=query)
        # grant amounts 100 (usage 1) and 50 (usage 3) → bumped 101 and 51
        by_bumped = {float(r["usage.bumped"]): r["usage._count"] for r in result.data}
        assert by_bumped == {101.0: 2, 51.0: 1}


@pytest.mark.integration
class TestPostgresIngestComments:
    def test_table_and_column_comments_imported(self, pg_ingest_env: PgIngestEnv) -> None:
        models, _, _ = pg_ingest_env
        orders = next(m for m in models if m.name == "orders")
        assert orders.description == "All orders"
        by_name = {c.name: c for c in orders.fields}
        assert by_name["amount"].description == "Order amount"
        assert by_name["id"].description is None

    def test_uncommented_table_has_no_description(self, pg_ingest_env: PgIngestEnv) -> None:
        models, _, _ = pg_ingest_env
        regions = next(m for m in models if m.name == "regions")
        assert regions.description is None


@pytest.mark.integration
def test_every_postgres_alias_failure_is_quoted(postgresql_proc) -> None:
    conn, db_name = _create_module_db(postgresql_proc)
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("CREATE TABLE kw_probe (a INTEGER)")
        cur.execute("SELECT word FROM pg_get_keywords()")
        words = keyword_universe(row[0] for row in cur.fetchall())

        def fails(sql: str) -> bool:
            try:
                cur.execute(cast(LiteralString, sql))
            except psycopg.Error:
                return True
            return False

        assert unquoted_alias_failures(
            dialect="postgres", words=words, table="kw_probe", column="a", fails=fails,
        ) == []
    finally:
        conn.close()
        _drop_module_db(postgresql_proc, db_name)
