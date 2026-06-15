"""Integration tests using a real MySQL database via testcontainers.

DEV-1564: mirror of test_integration_postgres.py, exercising the MySQL
dialect's variance-decomposition path for corr/covar_*, the MySQL-specific
``VAR_SAMP``/``VAR_POP`` Anonymous overrides, and the NotImplementedError
emitted for median/percentile.

Skipped silently when:
- ``testcontainers[mysql]`` is not installed (importorskip)
- The Docker daemon is unreachable (autouse session fixture)

The CI workflow at .github/workflows/integration-mysql.yml asserts
``import testcontainers.mysql`` before invoking pytest so a missing extra
in CI surfaces as a workflow failure, not a silent skip.
"""

import tempfile
import uuid

import pytest

pytest.importorskip("testcontainers.mysql")

import pymysql
import sqlalchemy as sa
from testcontainers.mysql import MySqlContainer

from slayer.async_utils import run_sync
from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Docker availability
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def _docker_available_or_skip():
    """Skip the entire module if the Docker daemon isn't reachable.

    Local-only convenience — kept out of conftest.py so the gate stays
    coupled to this suite, not adjacent ones. CI runners always have
    Docker; this fixture is a no-op there.
    """
    try:
        import docker  # noqa: PLC0415
        docker.from_env().ping()
    except Exception as exc:  # pragma: no cover — exercised on local dev
        pytest.skip(f"Docker not available: {exc}")


# ---------------------------------------------------------------------------
# Container lifecycle
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def mysql_container():
    """Session-scoped MySQL 8 container. Starts once, tears down at end."""
    container = MySqlContainer(
        "mysql:8.0",
        username="slayer",
        password="slayer",  # NOSONAR(S2068) — testcontainer credentials, not real secrets
        dbname="slayer_root",
        root_password="root",  # NOSONAR(S2068) — testcontainer credentials, not real secrets
    )
    with container as c:
        yield c


def _admin_connect(mysql_container, *, dbname: str = "mysql"):
    """Open a root pymysql connection to the container."""
    host = mysql_container.get_container_host_ip()
    port = int(mysql_container.get_exposed_port(3306))
    return pymysql.connect(
        host=host,
        port=port,
        user="root",
        password="root",  # NOSONAR(S6437,S2068) — testcontainer root password, not a real credential
        database=dbname,
        autocommit=True,
    )


def _create_module_db(mysql_container) -> str:
    """Create a fresh per-module database and return its name."""
    db_name = f"test_{uuid.uuid4().hex[:12]}"
    conn = _admin_connect(mysql_container)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE `{db_name}`")
            cur.execute(
                f"GRANT ALL PRIVILEGES ON `{db_name}`.* TO 'slayer'@'%'"
            )
            cur.execute("FLUSH PRIVILEGES")
    finally:
        conn.close()
    return db_name


def _drop_module_db(mysql_container, db_name: str) -> None:
    """Dispose any cached SA engines pointing at this DB, then drop it."""
    # Dispose cached SA engines so pyodbc/pymysql pools release their
    # connections before DROP DATABASE — otherwise MySQL may keep waiting.
    for engine in engine_factory._engine_cache.values():
        engine.dispose()
    engine_factory.reset_cache()

    conn = _admin_connect(mysql_container)
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
    finally:
        conn.close()


def _ds_config(mysql_container, db_name: str) -> DatasourceConfig:
    """Build a slayer DatasourceConfig pointing at the given DB."""
    host = mysql_container.get_container_host_ip()
    port = int(mysql_container.get_exposed_port(3306))
    return DatasourceConfig(
        name="testmysql",
        type="mysql",
        host=host,
        port=port,
        database=db_name,
        username="slayer",
        password="slayer",  # NOSONAR(S2068) — testcontainer credentials, not real secrets
    )


# ---------------------------------------------------------------------------
# Base orders/customers fixtures (mirror of pg_env)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def _mysql_env_storage(mysql_container, tmp_path_factory):
    """Module-scoped: per-module MySQL DB seeded with orders + customers
    (InnoDB, with FK), plus a YAMLStorage with both models pre-saved.
    """
    db_name = _create_module_db(mysql_container)
    try:
        conn = _admin_connect(mysql_container, dbname=db_name)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE customers (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        region VARCHAR(255) NOT NULL
                    ) ENGINE=InnoDB
                """)
                cur.execute("""
                    CREATE TABLE orders (
                        id INTEGER PRIMARY KEY,
                        status VARCHAR(255) NOT NULL,
                        amount DECIMAL(10,2) NOT NULL,
                        customer_id INTEGER,
                        created_at DATETIME NOT NULL,
                        FOREIGN KEY (customer_id) REFERENCES customers(id)
                    ) ENGINE=InnoDB
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
        finally:
            conn.close()

        tmpdir = str(tmp_path_factory.mktemp("mysql_env"))
        storage = YAMLStorage(base_dir=tmpdir)
        run_sync(storage.save_datasource(_ds_config(mysql_container, db_name)))

        orders_model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="testmysql",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="total", sql="amount", type=DataType.DOUBLE),
                Column(name="avg_amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        customers_model = SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="testmysql",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
                Column(name="region", sql="region", type=DataType.TEXT),
            ],
        )
        run_sync(storage.save_model(orders_model))
        run_sync(storage.save_model(customers_model))

        yield storage
    finally:
        _drop_module_db(mysql_container, db_name)


@pytest.fixture
def mysql_env(_mysql_env_storage) -> SlayerQueryEngine:
    """Per-test SlayerQueryEngine wrapping the module-scoped storage. The
    engine is recreated per-test because its async SA engine binds to the
    current event loop (slayer/sql/client.py:148)."""
    return SlayerQueryEngine(storage=_mysql_env_storage)


@pytest.mark.integration
class TestMySQLQueries:
    async def test_count_all(self, mysql_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(source_model="orders", measures=[{"formula": "*:count"}])
        result = await mysql_env.execute(query=query)
        assert result.row_count == 1
        assert result.data[0]["orders._count"] == 6

    async def test_sum_measure(self, mysql_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(source_model="orders", measures=[{"formula": "total:sum"}])
        result = await mysql_env.execute(query=query)
        assert float(result.data[0]["orders.total_sum"]) == 875.0  # NOSONAR(S1244) — sum of integer cents, exact-representable

    async def test_avg_measure(self, mysql_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(source_model="orders", measures=[{"formula": "avg_amount:avg"}])
        result = await mysql_env.execute(query=query)
        avg = float(result.data[0]["orders.avg_amount_avg"])
        assert abs(avg - 145.83) < 0.1

    async def test_group_by_status(self, mysql_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "status"}],
        )
        result = await mysql_env.execute(query=query)
        by_status = {r["orders.status"]: r["orders._count"] for r in result.data}
        assert by_status["completed"] == 3
        assert by_status["pending"] == 2
        assert by_status["cancelled"] == 1

    async def test_filter_equals(self, mysql_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            filters=["status == 'completed'"],
        )
        result = await mysql_env.execute(query=query)
        assert result.data[0]["orders._count"] == 3

    async def test_filter_gt(self, mysql_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            filters=["total > 100"],
        )
        result = await mysql_env.execute(query=query)
        assert result.data[0]["orders._count"] == 3

    async def test_order_by_desc(self, mysql_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "status"}],
            order=[{"column": {"name": "count"}, "direction": "desc"}],
        )
        result = await mysql_env.execute(query=query)
        assert result.data[0]["orders.status"] == "completed"

    async def test_limit(self, mysql_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "status"}],
            limit=2,
        )
        result = await mysql_env.execute(query=query)
        assert result.row_count == 2

    async def test_multiple_measures(self, mysql_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}, {"formula": "total:sum"}],
            dimensions=[{"name": "status"}],
        )
        result = await mysql_env.execute(query=query)
        completed = next(r for r in result.data if r["orders.status"] == "completed")
        assert completed["orders._count"] == 3
        assert float(completed["orders.total_sum"]) == 450.0  # NOSONAR(S1244) — sum of integer cents, exact-representable

    async def test_time_dimension_month_granularity(self, mysql_env: SlayerQueryEngine) -> None:
        """MySQL supports DATE_FORMAT/DATE_TRUNC-via-sqlglot — this should
        return 3 months."""
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
        )
        result = await mysql_env.execute(query=query)
        assert result.row_count == 3

    async def test_time_dimension_with_date_range(self, mysql_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            time_dimensions=[{
                "dimension": {"name": "created_at"},
                "granularity": "month",
                "date_range": ["2024-01-01", "2024-02-28"],
            }],
        )
        result = await mysql_env.execute(query=query)
        total = sum(r["orders._count"] for r in result.data)
        assert total == 4

    async def test_composite_filter(self, mysql_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            filters=["status == 'completed' or status == 'pending'"],
        )
        result = await mysql_env.execute(query=query)
        assert result.data[0]["orders._count"] == 5

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "DEV-1571 (Bug 3): outer-wrap quote-style mismatch on MySQL — "
            "same root cause as test_change_pct_with_date_range."
        ),
    )
    async def test_time_shift_with_date_range(self, mysql_env: SlayerQueryEngine) -> None:
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
        result = await mysql_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(375.0)
        assert float(result.data[0]["orders.prev_month"]) == pytest.approx(200.0)

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "DEV-1571 (Bug 3 silent variant): MySQL treats the outer-wrap's "
            '"orders.positive_run" as a string literal, so the result row '
            "carries the literal column-name text instead of the column "
            "value. Same root cause as test_change_pct_with_date_range; "
            "fixed by routing outer-wrap SQL through sqlglot's MySQL dialect."
        ),
    )
    async def test_consecutive_periods_with_boolean_predicate(self, mysql_env: SlayerQueryEngine) -> None:
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
        result = await mysql_env.execute(query=query)
        assert [r["orders.positive_run"] for r in result.data] == [1, 0, 1]

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "DEV-1571 (Bug 3): outer-wrap quote-style mismatch on MySQL — "
            "same root cause as test_change_pct_with_date_range."
        ),
    )
    async def test_change_with_date_range(self, mysql_env: SlayerQueryEngine) -> None:
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
        result = await mysql_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.amount_change"]) == pytest.approx(175.0)

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "DEV-1571 (Bug 3): SLayer's outer-wrap on date-range time_shift "
            "queries emits ANSI double-quoted aliases while the inner CTEs "
            "use MySQL backticks. MySQL rejects double-quoted identifiers."
        ),
    )
    async def test_change_pct_with_date_range(self, mysql_env: SlayerQueryEngine) -> None:
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
        result = await mysql_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.pct"]) == pytest.approx(0.875)

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "DEV-1571 (Bug 3): outer-wrap quote-style mismatch — same root "
            "cause as test_change_pct_with_date_range."
        ),
    )
    async def test_multiple_date_range_shifts(self, mysql_env: SlayerQueryEngine) -> None:
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
        result = await mysql_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(200.0)
        assert float(result.data[0]["orders.prev"]) == pytest.approx(300.0)
        assert float(result.data[0]["orders.next"]) == pytest.approx(375.0)


# ---------------------------------------------------------------------------
# Cross-model + multistage
# ---------------------------------------------------------------------------


@pytest.fixture
def mysql_cross_model_env(mysql_container):
    """MySQL env with orders + customers (with score) and explicit join."""
    db_name = _create_module_db(mysql_container)
    conn = _admin_connect(mysql_container, dbname=db_name)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE customers (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    region VARCHAR(255) NOT NULL,
                    score DECIMAL(5,2) NOT NULL
                ) ENGINE=InnoDB
            """)
            cur.execute("""
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    status VARCHAR(255) NOT NULL,
                    amount DECIMAL(10,2) NOT NULL,
                    customer_id INTEGER,
                    created_at DATETIME NOT NULL,
                    FOREIGN KEY (customer_id) REFERENCES customers(id)
                ) ENGINE=InnoDB
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
    finally:
        conn.close()

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(mysql_container, db_name)))
    run_sync(storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="testmysql",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="total", sql="amount", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )))
    run_sync(storage.save_model(SlayerModel(
        name="customers", sql_table="customers", data_source="testmysql",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="avg_score", sql="score", type=DataType.DOUBLE),
        ],
    )))
    yield SlayerQueryEngine(storage=storage)
    _drop_module_db(mysql_container, db_name)


@pytest.mark.integration
class TestCrossModelAndMultistageMySQL:
    @pytest.mark.xfail(
        strict=True,
        reason=(
            "DEV-1571 (Bug 3): cross-model CTEs hit the same MySQL outer-wrap "
            "quote-style mismatch as the date-range time_shift path."
        ),
    )
    async def test_cross_model_measure(self, mysql_cross_model_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="customers.avg_score:avg")],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        )
        result = await mysql_cross_model_env.execute(query=query)
        assert result.row_count == 3
        global_avg = pytest.approx((90.0 + 60.0 + 80.0) / 3)
        assert float(result.data[0]["orders.customers.avg_score_avg"]) == global_avg
        assert float(result.data[1]["orders.customers.avg_score_avg"]) == global_avg
        assert float(result.data[2]["orders.customers.avg_score_avg"]) == global_avg

    async def test_query_list_named(self, mysql_cross_model_env: SlayerQueryEngine) -> None:
        inner = SlayerQuery(
            name="monthly", source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="total:sum")],
        )
        outer = SlayerQuery(source_model="monthly", measures=[ModelMeasure(formula="*:count")])
        result = await mysql_cross_model_env.execute(query=[inner, outer])
        assert result.data[0]["monthly._count"] == 3

    async def test_create_model_from_query(self, mysql_cross_model_env: SlayerQueryEngine) -> None:
        source = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="total:sum")],
        )
        saved = await mysql_cross_model_env.create_model_from_query(query=source, name="mysql_monthly")
        assert saved.source_queries is not None
        result = await mysql_cross_model_env.execute(
            query=SlayerQuery(source_model="mysql_monthly", measures=[ModelMeasure(formula="*:count")])
        )
        assert result.data[0]["mysql_monthly._count"] == 3

    async def test_sql_dimension(self, mysql_cross_model_env: SlayerQueryEngine) -> None:
        from slayer.core.query import ModelExtension
        query = SlayerQuery(
            source_model=ModelExtension(
                source_name="orders",
                columns=[{"name": "tier", "sql": "CASE WHEN amount > 100 THEN 'high' ELSE 'low' END"}],
            ),
            dimensions=[ColumnRef(name="tier")],
            measures=[ModelMeasure(formula="*:count")],
        )
        result = await mysql_cross_model_env.execute(query=query)
        by_tier = {r["orders.tier"]: r["orders._count"] for r in result.data}
        assert by_tier["high"] == 3
        assert by_tier["low"] == 3


# ---------------------------------------------------------------------------
# Rollup ingestion (MySQL 8 + InnoDB exposes FK metadata via Inspector)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mysql_ingest_env(mysql_container):
    """Set up tables with InnoDB FK relationships and ingest. If FK
    introspection returns nothing, fail the fixture with a clear reason —
    don't silently degrade. Module-scoped: tests destructure
    ``(models, ds, _)`` and build their own ephemeral storage.
    """
    db_name = _create_module_db(mysql_container)
    try:
        conn = _admin_connect(mysql_container, dbname=db_name)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE regions (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR(255) NOT NULL
                    ) ENGINE=InnoDB
                """)
                cur.execute("""
                    CREATE TABLE customers (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        region_id INTEGER,
                        FOREIGN KEY (region_id) REFERENCES regions(id)
                    ) ENGINE=InnoDB
                """)
                cur.execute("""
                    CREATE TABLE orders (
                        id INTEGER PRIMARY KEY,
                        amount DECIMAL(10,2) NOT NULL,
                        customer_id INTEGER,
                        FOREIGN KEY (customer_id) REFERENCES customers(id)
                    ) ENGINE=InnoDB
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
        finally:
            conn.close()

        ds = _ds_config(mysql_container, db_name)

        # Validate FK introspection before ingest — if MySQL's Inspector
        # doesn't surface the FK metadata we declared, the rest of the
        # rollup tests are meaningless. Fail loudly with the actual count.
        sa_engine = sa.create_engine(ds.get_connection_string())
        inspector = sa.inspect(sa_engine)
        fks_on_orders = inspector.get_foreign_keys("orders")
        sa_engine.dispose()
        assert len(fks_on_orders) >= 1, (
            f"MySQL InnoDB FK introspection returned 0 FKs on 'orders' — "
            f"rollup tests cannot validate. Inspector output: {fks_on_orders!r}"
        )

        models = ingest_datasource(datasource=ds, schema=None)
        yield models, ds, None
    finally:
        _drop_module_db(mysql_container, db_name)


@pytest.mark.integration
class TestRollupIngestionMySQL:
    def test_orders_has_own_columns_only(self, mysql_ingest_env) -> None:
        models, _, _ = mysql_ingest_env
        orders = next(m for m in models if m.name == "orders")
        col_names = [c.name for c in orders.columns]
        assert "id" in col_names
        assert "customer_id" in col_names
        assert "amount" in col_names
        assert not any("." in name for name in col_names)

    def test_orders_uses_sql_table_with_joins(self, mysql_ingest_env) -> None:
        models, _, _ = mysql_ingest_env
        orders = next(m for m in models if m.name == "orders")
        assert orders.sql_table is not None
        assert orders.sql is None
        assert len(orders.joins) > 0

    def test_regions_has_no_rollup(self, mysql_ingest_env) -> None:
        models, _, _ = mysql_ingest_env
        regions = next(m for m in models if m.name == "regions")
        assert regions.sql_table is not None
        assert regions.sql is None

    async def test_rollup_query_group_by_customer(self, mysql_ingest_env) -> None:
        models, ds, _ = mysql_ingest_env
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

    async def test_rollup_query_group_by_region(self, mysql_ingest_env) -> None:
        models, ds, _ = mysql_ingest_env
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
        assert by_region["US"]["orders._count"] == 3
        assert by_region["EU"]["orders._count"] == 1
        assert float(by_region["US"]["orders.amount_sum"]) == 450.0  # NOSONAR(S1244) — sum of integer cents, exact-representable
        assert float(by_region["EU"]["orders.amount_sum"]) == 50.0  # NOSONAR(S1244) — sum of integer cents, exact-representable

    def test_orders_has_no_named_measures_after_ingest(self, mysql_ingest_env) -> None:
        models, _, _ = mysql_ingest_env
        orders = next(m for m in models if m.name == "orders")
        assert orders.measures == []
        col_names = [c.name for c in orders.columns]
        assert "amount" in col_names

    # NOTE: ``test_dotted_dimension_single_hop`` is intentionally not ported —
    # its body would be identical to ``test_rollup_query_group_by_customer``
    # (Sonar python:S4144). The dotted single-hop path is already exercised by
    # the rollup query test; the multi-hop variant below covers transitive
    # resolution which is the part that genuinely differs.

    async def test_dotted_dimension_multi_hop(self, mysql_ingest_env) -> None:
        models, ds, _ = mysql_ingest_env
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
        by_region = {r["orders.customers.regions.name"]: r["orders._count"] for r in result.data}
        assert by_region["US"] == 3
        assert by_region["EU"] == 1

    async def test_selective_joins_no_joined_dims(self, mysql_ingest_env) -> None:
        models, ds, _ = mysql_ingest_env
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
        assert "LEFT JOIN" not in result.sql
        assert result.data[0]["orders._count"] == 4

    async def test_selective_joins_single_hop(self, mysql_ingest_env) -> None:
        models, ds, _ = mysql_ingest_env
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
        assert "LEFT JOIN" in result.sql
        assert "customers" in result.sql
        assert "regions" not in result.sql

    async def test_selective_joins_transitive(self, mysql_ingest_env) -> None:
        models, ds, _ = mysql_ingest_env
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
        assert "customers" in result.sql
        assert "regions" in result.sql

    def test_orders_has_joins_metadata(self, mysql_ingest_env) -> None:
        models, _, _ = mysql_ingest_env
        orders = next(m for m in models if m.name == "orders")
        join_targets = [j.target_model for j in orders.joins]
        assert "customers" in join_targets
        assert "regions" not in join_targets
        for j in orders.joins:
            assert len(j.join_pairs) >= 1
            for pair in j.join_pairs:
                assert len(pair) == 2

    def test_regions_has_no_joins(self, mysql_ingest_env) -> None:
        models, _, _ = mysql_ingest_env
        regions = next(m for m in models if m.name == "regions")
        assert regions.joins == []

    async def test_joins_serialize_to_yaml(self, mysql_ingest_env) -> None:
        models, _, _ = mysql_ingest_env
        orders = next(m for m in models if m.name == "orders")
        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_model(orders)
        loaded = await storage.get_model("orders")
        assert len(loaded.joins) == len(orders.joins)
        for orig, loaded_j in zip(orders.joins, loaded.joins):
            assert orig.target_model == loaded_j.target_model
            assert orig.join_pairs == loaded_j.join_pairs


# ---------------------------------------------------------------------------
# Median / percentile — MySQL raises NotImplementedError
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestMySQLMedianPercentileRaises:
    async def test_median_raises(self, mysql_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(source_model="orders", measures=[{"formula": "total:median"}])
        with pytest.raises(NotImplementedError, match="(?i)median.*mysql"):
            await mysql_env.execute(query=query)

    async def test_median_grouped_raises(self, mysql_env: SlayerQueryEngine) -> None:
        """Grouped median path goes through the same dialect hook; must also raise."""
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:median"}],
            dimensions=[{"name": "status"}],
        )
        with pytest.raises(NotImplementedError, match="(?i)median.*mysql"):
            await mysql_env.execute(query=query)

    async def test_percentile_raises(self, mysql_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:percentile(p=0.5)"}],
        )
        with pytest.raises(NotImplementedError, match="(?i)percentile.*mysql"):
            await mysql_env.execute(query=query)


# ---------------------------------------------------------------------------
# Statistical aggregations (DEV-1317 cross-dialect parity)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestMySQLStatAggregations:
    """MySQL has native STDDEV_SAMP / STDDEV_POP / VAR_SAMP / VAR_POP.

    For VAR_SAMP / VAR_POP, MysqlDialect.build_stat_agg_1arg wraps the
    aggregation in ``exp.Anonymous`` to bypass sqlglot's misleading
    transpilation (VAR_SAMP → VARIANCE on MySQL is actually VAR_POP).

    For CORR / COVAR_*, MySQL has NO native function — MysqlDialect uses
    the variance-decomposition formula. These tests pin both the numeric
    correctness AND the SQL shape via dry_run.
    """

    async def test_stddev_samp_native_mysql(self, mysql_env: SlayerQueryEngine) -> None:
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:stddev_samp"}],
        )
        result = await mysql_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_stddev_samp"]) == pytest.approx(
            statistics.stdev(amounts), rel=1e-9
        )

    async def test_stddev_pop_native_mysql(self, mysql_env: SlayerQueryEngine) -> None:
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:stddev_pop"}],
        )
        result = await mysql_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_stddev_pop"]) == pytest.approx(
            statistics.pstdev(amounts), rel=1e-9
        )

    async def test_var_samp_uses_canonical_mysql_name(self, mysql_env: SlayerQueryEngine) -> None:
        """The MysqlDialect.build_stat_agg_1arg Anonymous wrap means the
        emitted SQL contains literal ``VAR_SAMP(``, not sqlglot's default
        ``VARIANCE``. Pin via dry_run."""
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:var_samp"}],
        )
        result = await mysql_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_var_samp"]) == pytest.approx(
            statistics.variance(amounts), rel=1e-9
        )

        dry = await mysql_env.execute(query=query, dry_run=True)
        assert dry.sql is not None
        sql_lower = dry.sql.lower()
        assert "var_samp(" in sql_lower, (
            f"MySQL must emit canonical VAR_SAMP, not sqlglot's VARIANCE. Got:\n{dry.sql}"
        )

    async def test_var_pop_uses_canonical_mysql_name(self, mysql_env: SlayerQueryEngine) -> None:
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:var_pop"}],
        )
        result = await mysql_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_var_pop"]) == pytest.approx(
            statistics.pvariance(amounts), rel=1e-9
        )

        dry = await mysql_env.execute(query=query, dry_run=True)
        assert dry.sql is not None
        sql_lower = dry.sql.lower()
        assert "var_pop(" in sql_lower, (
            f"MySQL must emit canonical VAR_POP, not sqlglot's VARIANCE. Got:\n{dry.sql}"
        )

    async def test_corr_variance_decomposition_mysql(self, mysql_env: SlayerQueryEngine) -> None:
        """MySQL CORR is computed via the variance-decomposition formula
        (no native function). Pin both numeric correctness AND SQL shape."""
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:corr(other=customer_id)"}],
        )
        result = await mysql_env.execute(query=query)
        xs = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        ys = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        expected = statistics.correlation(xs, ys)
        assert float(result.data[0]["orders.total_corr_other_customer_id"]) == pytest.approx(
            expected, rel=1e-9
        )

        # Variance-decomposition shape: the SQL should reference VAR_SAMP
        # multiple times (for x, y, x+y) and STDDEV_SAMP (for normalisation).
        dry = await mysql_env.execute(query=query, dry_run=True)
        assert dry.sql is not None
        sql_lower = dry.sql.lower()
        assert sql_lower.count("var_samp(") >= 3, (
            f"MySQL CORR should use variance-decomposition with VAR_SAMP. Got:\n{dry.sql}"
        )
        assert "stddev_samp(" in sql_lower, (
            f"MySQL CORR normalisation should use STDDEV_SAMP. Got:\n{dry.sql}"
        )

    async def test_covar_samp_variance_decomposition_mysql(self, mysql_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:covar_samp(other=customer_id)"}],
        )
        result = await mysql_env.execute(query=query)
        xs = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        ys = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        n = len(xs)
        mx, my = sum(xs) / n, sum(ys) / n
        expected = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (n - 1)
        assert float(
            result.data[0]["orders.total_covar_samp_other_customer_id"]
        ) == pytest.approx(expected, rel=1e-9)

        dry = await mysql_env.execute(query=query, dry_run=True)
        assert dry.sql is not None
        sql_lower = dry.sql.lower()
        assert sql_lower.count("var_samp(") >= 3, (
            f"MySQL COVAR_SAMP should use variance-decomposition. Got:\n{dry.sql}"
        )

    async def test_covar_pop_variance_decomposition_mysql(self, mysql_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:covar_pop(other=customer_id)"}],
        )
        result = await mysql_env.execute(query=query)
        xs = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        ys = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        n = len(xs)
        mx, my = sum(xs) / n, sum(ys) / n
        expected = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / n
        assert float(
            result.data[0]["orders.total_covar_pop_other_customer_id"]
        ) == pytest.approx(expected, rel=1e-9)

        dry = await mysql_env.execute(query=query, dry_run=True)
        assert dry.sql is not None
        sql_lower = dry.sql.lower()
        assert sql_lower.count("var_pop(") >= 3, (
            f"MySQL COVAR_POP should use variance-decomposition. Got:\n{dry.sql}"
        )


# ---------------------------------------------------------------------------
# log10 round-trip (DEV-1337 — MySQL has native log10)
# ---------------------------------------------------------------------------


@pytest.fixture
def mysql_log10_env(mysql_container):
    """Dedicated function-scoped env for the log10 round-trip test so its
    Column-add doesn't leak into the module-scoped storage (would corrupt
    the shared model across sibling tests)."""
    db_name = _create_module_db(mysql_container)
    conn = _admin_connect(mysql_container, dbname=db_name)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    amount DECIMAL(10,2) NOT NULL
                ) ENGINE=InnoDB
            """)
            cur.executemany(
                "INSERT INTO orders VALUES (%s, %s)",
                [(1, 100), (2, 200), (3, 300)],
            )
    finally:
        conn.close()

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(mysql_container, db_name)))
    run_sync(storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="testmysql",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="log_amount", sql="log10(amount)", type=DataType.DOUBLE),
        ],
    )))
    yield SlayerQueryEngine(storage=storage)
    _drop_module_db(mysql_container, db_name)


@pytest.mark.integration
async def test_log10_round_trip_mysql(mysql_log10_env: SlayerQueryEngine) -> None:
    """MySQL has native LOG10 (MysqlDialect.log10_native=True). The emitted
    SQL must preserve ``log10(...)`` rather than sqlglot's canonical
    ``LOG(10, ...)`` form."""
    import math as _math

    result = await mysql_log10_env.execute(
        SlayerQuery(source_model="orders", measures=[{"formula": "log_amount:max"}])
    )
    assert float(result.data[0]["orders.log_amount_max"]) == pytest.approx(
        _math.log10(300.0), rel=1e-9
    )

    dry = await mysql_log10_env.execute(
        SlayerQuery(source_model="orders", measures=[{"formula": "log_amount:max"}]),
        dry_run=True,
    )
    assert dry.sql is not None
    sql_lower = dry.sql.lower()
    assert "log10(" in sql_lower, (
        f"Expected literal log10(...) in emitted SQL on MySQL, got:\n{dry.sql}"
    )
    assert "log(10," not in sql_lower.replace(" ", ""), (
        f"MySQL emitted SQL must not canonicalise log10 to LOG(10, ...):\n{dry.sql}"
    )


# ---------------------------------------------------------------------------
# Window-in-filter raises (DEV-1369 parity)
# ---------------------------------------------------------------------------


@pytest.fixture
def planets_mysql_env(mysql_container):
    """Planets fixture (MySQL) with a Column.sql containing ROW_NUMBER()."""
    db_name = _create_module_db(mysql_container)
    conn = _admin_connect(mysql_container, dbname=db_name)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE planets (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    mass DECIMAL(10, 4) NOT NULL
                ) ENGINE=InnoDB
            """)
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
    finally:
        conn.close()

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(mysql_container, db_name)))
    run_sync(storage.save_model(
        SlayerModel(
            name="planets",
            sql_table="planets",
            data_source="testmysql",
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
    ))
    yield SlayerQueryEngine(storage=storage)
    _drop_module_db(mysql_container, db_name)


@pytest.mark.integration
async def test_filter_on_windowed_column_mysql_raises(planets_mysql_env) -> None:
    engine = planets_mysql_env
    query = SlayerQuery(
        source_model="planets",
        dimensions=["name"],
        filters=["rn <= 3"],
    )
    with pytest.raises(ValueError, match="(?i)window function|rank"):
        await engine.execute(query)


# ---------------------------------------------------------------------------
# Cross-model derived Column.sql (DEV-1333)
# ---------------------------------------------------------------------------


@pytest.fixture
def mysql_derived_chain_env(mysql_container):
    """A→B fixture with a derived column on B referenced by A (MySQL)."""
    db_name = _create_module_db(mysql_container)
    conn = _admin_connect(mysql_container, dbname=db_name)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE b_tbl (id INTEGER PRIMARY KEY, foo_raw DECIMAL(10,2)) ENGINE=InnoDB"
            )
            cur.execute(
                "CREATE TABLE a_tbl ("
                "id INTEGER PRIMARY KEY, bar DECIMAL(10,2), b_id INTEGER, raw_a DECIMAL(10,2)"
                ") ENGINE=InnoDB"
            )
            cur.executemany("INSERT INTO b_tbl VALUES (%s, %s)", [(1, 200), (2, 50)])
            cur.executemany(
                "INSERT INTO a_tbl VALUES (%s, %s, %s, %s)",
                [(10, 4, 1, 100), (11, 1, 2, 5)],
            )
    finally:
        conn.close()

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(mysql_container, db_name)))
    run_sync(storage.save_model(
        SlayerModel(
            name="b_tbl",
            data_source="testmysql",
            sql_table="b_tbl",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="foo_raw", sql="foo_raw", type=DataType.DOUBLE),
                Column(name="foo_normalized", sql="foo_raw / 100.0", type=DataType.DOUBLE),
            ],
        )
    ))
    run_sync(storage.save_model(
        SlayerModel(
            name="a_tbl",
            data_source="testmysql",
            sql_table="a_tbl",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="bar", sql="bar", type=DataType.DOUBLE),
                Column(name="b_id", sql="b_id", type=DataType.DOUBLE),
                Column(name="raw_a", sql="raw_a", type=DataType.DOUBLE),
                Column(
                    name="ratio_using_derived",
                    sql="a_tbl.bar / b_tbl.foo_normalized",
                    type=DataType.DOUBLE,
                ),
            ],
            joins=[ModelJoin(target_model="b_tbl", join_pairs=[["b_id", "id"]])],
        )
    ))
    yield SlayerQueryEngine(storage=storage)
    _drop_module_db(mysql_container, db_name)


@pytest.mark.integration
async def test_integration_mysql_cross_model_derived_columnsql(
    mysql_derived_chain_env: SlayerQueryEngine,
) -> None:
    response = await mysql_derived_chain_env.execute(
        SlayerQuery(
            source_model="a_tbl",
            dimensions=[
                ColumnRef(name="id"),
                ColumnRef(name="ratio_using_derived"),
            ],
            order=[OrderItem(column=ColumnRef(name="id"), direction="asc")],
        )
    )
    assert response.row_count == 2
    assert float(response.data[0]["a_tbl.ratio_using_derived"]) == pytest.approx(2.0)
    assert float(response.data[1]["a_tbl.ratio_using_derived"]) == pytest.approx(2.0)
