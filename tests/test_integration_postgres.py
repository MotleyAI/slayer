"""Integration tests using a real PostgreSQL database via pytest-postgresql."""

import tempfile

import pytest

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig, Dimension, Measure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

# Spawn a temporary Postgres process (random port)
postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")


@pytest.fixture
def pg_env(postgresql):
    """Set up a full SLayer environment against the temporary Postgres."""
    # Create test tables
    cur = postgresql.cursor()
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
    postgresql.commit()

    # Set up SLayer storage
    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)

    info = postgresql.info
    storage.save_datasource(DatasourceConfig(
        name="testpg",
        type="postgres",
        host=info.host,
        port=info.port,
        database=info.dbname,
        username=info.user,
        password="",
    ))

    orders_model = SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="testpg",
        dimensions=[
            Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Dimension(name="status", sql="status", type=DataType.STRING),
            Dimension(name="customer_id", sql="customer_id", type=DataType.NUMBER),
            Dimension(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        ],
        measures=[
            Measure(name="count", type=DataType.COUNT),
            Measure(name="total", sql="amount", type=DataType.SUM),
            Measure(name="avg_amount", sql="amount", type=DataType.AVERAGE),
        ],
    )
    customers_model = SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="testpg",
        dimensions=[
            Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Dimension(name="name", sql="name", type=DataType.STRING),
            Dimension(name="region", sql="region", type=DataType.STRING),
        ],
        measures=[
            Measure(name="count", type=DataType.COUNT),
        ],
    )
    storage.save_model(orders_model)
    storage.save_model(customers_model)

    return SlayerQueryEngine(storage=storage)


@pytest.mark.integration
class TestPostgresQueries:
    def test_count_all(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(model="orders", fields=[{"formula": "count"}])
        result = pg_env.execute(query=query)
        assert result.row_count == 1
        assert result.data[0]["orders.count"] == 6

    def test_sum_measure(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(model="orders", fields=[{"formula": "total"}])
        result = pg_env.execute(query=query)
        assert float(result.data[0]["orders.total"]) == 875.0

    def test_avg_measure(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(model="orders", fields=[{"formula": "avg_amount"}])
        result = pg_env.execute(query=query)
        avg = float(result.data[0]["orders.avg_amount"])
        assert abs(avg - 145.83) < 0.1

    def test_group_by_status(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}],
            dimensions=[{"name": "status"}],
        )
        result = pg_env.execute(query=query)
        by_status = {r["orders.status"]: r["orders.count"] for r in result.data}
        assert by_status["completed"] == 3
        assert by_status["pending"] == 2
        assert by_status["cancelled"] == 1

    def test_filter_equals(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}],
            filters=["status == 'completed'"],
        )
        result = pg_env.execute(query=query)
        assert result.data[0]["orders.count"] == 3

    def test_filter_gt(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}],
            filters=["amount > 100"],
        )
        result = pg_env.execute(query=query)
        assert result.data[0]["orders.count"] == 3  # 200, 150, 300

    def test_order_by_desc(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}],
            dimensions=[{"name": "status"}],
            order=[{"column": {"name": "count"}, "direction": "desc"}],
        )
        result = pg_env.execute(query=query)
        assert result.data[0]["orders.status"] == "completed"

    def test_limit(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}],
            dimensions=[{"name": "status"}],
            limit=2,
        )
        result = pg_env.execute(query=query)
        assert result.row_count == 2

    def test_multiple_measures(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}, {"formula": "total"}],
            dimensions=[{"name": "status"}],
        )
        result = pg_env.execute(query=query)
        completed = next(r for r in result.data if r["orders.status"] == "completed")
        assert completed["orders.count"] == 3
        assert float(completed["orders.total"]) == 450.0

    def test_time_dimension_month_granularity(self, pg_env: SlayerQueryEngine) -> None:
        """Postgres supports DATE_TRUNC — this should work unlike SQLite."""
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}],
            time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
        )
        result = pg_env.execute(query=query)
        assert result.row_count == 3  # Jan, Feb, Mar

    def test_time_dimension_with_date_range(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}],
            time_dimensions=[{
                "dimension": {"name": "created_at"},
                "granularity": "month",
                "date_range": ["2024-01-01", "2024-02-28"],
            }],
        )
        result = pg_env.execute(query=query)
        # Only Jan and Feb orders (4 orders)
        total = sum(r["orders.count"] for r in result.data)
        assert total == 4

    def test_filter_with_variables(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "total"}],
            filters=["status == '{status}'"],
            variables={"status": "completed"},
        )
        result = pg_env.execute(query=query)
        assert float(result.data[0]["orders.total"]) == 450.0

    def test_composite_filter(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}],
            filters=["status == 'completed' or status == 'pending'"],
        )
        result = pg_env.execute(query=query)
        assert result.data[0]["orders.count"] == 5  # 3 completed + 2 pending


@pytest.fixture
def pg_ingest_env(postgresql):
    """Set up tables with FK relationships and ingest via rollup."""
    cur = postgresql.cursor()
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
    postgresql.commit()

    info = postgresql.info
    ds = DatasourceConfig(
        name="testpg",
        type="postgres",
        host=info.host,
        port=info.port,
        database=info.dbname,
        username=info.user,
        password="",
    )

    models = ingest_datasource(datasource=ds, schema="public")
    return models, ds, postgresql


@pytest.mark.integration
class TestRollupIngestion:
    def test_orders_has_rollup_dimensions(self, pg_ingest_env) -> None:
        models, _, _ = pg_ingest_env
        orders = next(m for m in models if m.name == "orders")

        dim_names = [d.name for d in orders.dimensions]
        # Should have own columns + rolled-up from customers and regions (transitive)
        assert "id" in dim_names
        assert "amount" in dim_names
        assert "customers__name" in dim_names
        assert "customers__id" in dim_names
        # Transitive: orders -> customers -> regions
        assert "regions__name" in dim_names
        assert "regions__id" in dim_names

    def test_orders_excludes_fk_from_rollup(self, pg_ingest_env) -> None:
        models, _, _ = pg_ingest_env
        orders = next(m for m in models if m.name == "orders")

        dim_names = [d.name for d in orders.dimensions]
        # FK columns should not be rolled up
        assert "customers__region_id" not in dim_names

    def test_orders_uses_sql_not_sql_table(self, pg_ingest_env) -> None:
        models, _, _ = pg_ingest_env
        orders = next(m for m in models if m.name == "orders")

        # Rollup model uses sql (with JOINs), not sql_table
        assert orders.sql is not None
        assert orders.sql_table is None
        assert "LEFT JOIN" in orders.sql
        assert "customers" in orders.sql
        assert "regions" in orders.sql

    def test_regions_has_no_rollup(self, pg_ingest_env) -> None:
        models, _, _ = pg_ingest_env
        regions = next(m for m in models if m.name == "regions")

        # Regions references nothing, should keep sql_table
        assert regions.sql_table is not None
        assert regions.sql is None

    def test_orders_has_count_distinct_measure(self, pg_ingest_env) -> None:
        models, _, _ = pg_ingest_env
        orders = next(m for m in models if m.name == "orders")

        measure_names = [m.name for m in orders.measures]
        assert "customers__count" in measure_names
        assert "regions__count" in measure_names

    def test_rollup_query_group_by_customer(self, pg_ingest_env) -> None:
        """Query orders grouped by rolled-up customer name."""
        models, ds, _ = pg_ingest_env

        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        storage.save_datasource(ds)
        for m in models:
            storage.save_model(m)
        engine = SlayerQueryEngine(storage=storage)

        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}],
            dimensions=[{"name": "customers__name"}],
        )
        result = engine.execute(query=query)

        by_name = {r["orders.customers__name"]: r["orders.count"] for r in result.data}
        assert by_name["Acme"] == 2
        assert by_name["Globex"] == 1
        assert by_name["Initech"] == 1

    def test_rollup_query_group_by_region(self, pg_ingest_env) -> None:
        """Query orders grouped by transitively rolled-up region name."""
        models, ds, _ = pg_ingest_env

        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        storage.save_datasource(ds)
        for m in models:
            storage.save_model(m)
        engine = SlayerQueryEngine(storage=storage)

        query = SlayerQuery(
            model="orders",
            fields=[{"formula": "count"}, {"formula": "amount_sum"}],
            dimensions=[{"name": "regions__name"}],
        )
        result = engine.execute(query=query)

        by_region = {r["orders.regions__name"]: r for r in result.data}
        assert by_region["US"]["orders.count"] == 3  # Acme(2) + Initech(1)
        assert by_region["EU"]["orders.count"] == 1  # Globex(1)
        assert float(by_region["US"]["orders.amount_sum"]) == 450.0  # 100+200+150
        assert float(by_region["EU"]["orders.amount_sum"]) == 50.0
