"""Integration tests using a real SQL Server database via testcontainers.

DEV-1564: SQL Server is the biggest gap in the existing CI matrix
(no pytest suite at all before this file). Mirror of
test_integration_postgres.py focused on T-SQL specifics:

* ``DATETRUNC(unit, col)`` for time-dimension granularity (SQL Server 2022+).
* ``DATETRUNC(iso_week, col)`` for Monday-aligned week truncation,
  ``@@DATEFIRST``-independent.
* ``DATEADD(unit, n, col)`` for time-shift arithmetic — no ``INTERVAL``
  syntax in T-SQL.
* ``STDEV`` / ``STDEVP`` / ``VAR`` / ``VARP`` native (sqlglot transpiles
  via the ``exp.Anonymous`` overrides in ``TsqlDialect``).
* ``CORR`` / ``COVAR_SAMP`` / ``COVAR_POP`` via the variance-decomposition
  formula (no native function in T-SQL).
* ``MEDIAN`` / ``PERCENTILE_CONT`` as GROUP BY aggregates raise
  ``NotImplementedError`` — ``PERCENTILE_CONT`` in T-SQL is window-only.
* Native ``LOG10`` (``TsqlDialect.log10_native = True``).

The pytest job's CI step (.github/workflows/integration-sqlserver.yml)
installs ``msodbcsql18`` + ``unixodbc-dev`` on the runner so pyodbc can
load. Locally, run::

    sudo apt install unixodbc-dev msodbcsql18  # or your distro equivalent

before invoking this suite.

Teardown of the per-module DB requires special handling: SLayer caches
SQLAlchemy engines (``slayer/sql/engine_factory.py``) and pyodbc pools
those connections, so a vanilla ``DROP DATABASE`` will block. The
``_drop_module_db`` helper:

1. Disposes every cached SA engine to release pooled connections.
2. Runs ``ALTER DATABASE <db> SET SINGLE_USER WITH ROLLBACK IMMEDIATE``
   to disconnect anyone we missed.
3. ``DROP DATABASE`` the now-isolated DB.

Skipped silently when:
- ``testcontainers[mssql]`` is not installed (importorskip)
- ``pyodbc`` is not installed OR the ODBC Driver 18 isn't visible to it
- The Docker daemon is unreachable (autouse session fixture)
"""

import tempfile
import uuid

import pytest

pytest.importorskip("testcontainers.mssql")
pytest.importorskip("pyodbc")

import pyodbc
import sqlalchemy as sa
from testcontainers.mssql import SqlServerContainer

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
    try:
        import docker  # noqa: PLC0415
        docker.from_env().ping()
    except Exception as exc:  # pragma: no cover — exercised on local dev
        pytest.skip(f"Docker not available: {exc}")


@pytest.fixture(scope="session", autouse=True)
def _odbc_driver_available_or_skip():
    """Skip the suite if ``ODBC Driver 18 for SQL Server`` isn't visible to
    pyodbc. All SQLAlchemy URLs in this file hardcode that exact driver
    name — a Driver-17-only machine would skip past this gate and then fail
    at connection time, so the match must be specific.

    The CI workflow installs msodbcsql18 explicitly, so this only fires
    locally on dev machines without the driver.
    """
    if "ODBC Driver 18 for SQL Server" not in pyodbc.drivers():
        pytest.skip(
            "No 'ODBC Driver 18 for SQL Server' visible to pyodbc "
            f"(installed drivers: {pyodbc.drivers()!r}). "
            "Install `msodbcsql18` (e.g. via Microsoft's apt repo) to run."
        )


# ---------------------------------------------------------------------------
# Container lifecycle
# ---------------------------------------------------------------------------


_SA_PASSWORD = "YourStrong@Passw0rd1"  # NOSONAR(S2068) — testcontainer credentials, not real secrets


@pytest.fixture(scope="session")
def sqlserver_container():
    """Session-scoped SQL Server 2022 container. Boot is ~30s so we share
    across the entire session."""
    container = SqlServerContainer(
        "mcr.microsoft.com/mssql/server:2022-latest",
        password=_SA_PASSWORD,
    )
    with container as c:
        yield c


def _admin_url(sqlserver_container, *, database: str = "master") -> str:
    """SQLAlchemy URL for admin (master DB) connections."""
    host = sqlserver_container.get_container_host_ip()
    port = int(sqlserver_container.get_exposed_port(1433))
    # URL-encode the @ in the password.
    pw = _SA_PASSWORD.replace("@", "%40")
    return (
        f"mssql+pyodbc://sa:{pw}@{host}:{port}/{database}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&TrustServerCertificate=yes"
    )


def _create_module_db(sqlserver_container) -> str:
    db_name = f"test_{uuid.uuid4().hex[:12]}"
    engine = sa.create_engine(_admin_url(sqlserver_container), isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(sa.text(f"CREATE DATABASE [{db_name}]"))
    engine.dispose()
    return db_name


def _drop_module_db(sqlserver_container, db_name: str) -> None:
    """Tear down the per-module DB cleanly despite pyodbc engine caching."""
    # 1) Release everything pooled at the slayer engine cache.
    for engine in engine_factory._engine_cache.values():
        engine.dispose()
    engine_factory.reset_cache()

    # 2) Kick anyone else off the DB; then drop it. AUTOCOMMIT so the ALTER
    #    isolation change takes effect immediately.
    engine = sa.create_engine(_admin_url(sqlserver_container), isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as conn:
            conn.execute(sa.text(
                f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
            ))
            conn.execute(sa.text(f"DROP DATABASE [{db_name}]"))
    finally:
        engine.dispose()


def _ds_config(sqlserver_container, db_name: str) -> DatasourceConfig:
    host = sqlserver_container.get_container_host_ip()
    port = int(sqlserver_container.get_exposed_port(1433))
    return DatasourceConfig(
        name="testmssql",
        type="mssql",
        host=host,
        port=port,
        database=db_name,
        username="sa",
        password=_SA_PASSWORD,
    )


def _db_url(sqlserver_container, db_name: str) -> str:
    """SA URL for a non-cached engine targeting a specific DB."""
    host = sqlserver_container.get_container_host_ip()
    port = int(sqlserver_container.get_exposed_port(1433))
    pw = _SA_PASSWORD.replace("@", "%40")
    return (
        f"mssql+pyodbc://sa:{pw}@{host}:{port}/{db_name}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&TrustServerCertificate=yes"
    )


# ---------------------------------------------------------------------------
# Base orders/customers fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def _sqlserver_env_storage(sqlserver_container, tmp_path_factory):
    """Module-scoped: seeded SQL Server orders + customers + storage."""
    db_name = _create_module_db(sqlserver_container)
    try:
        engine = sa.create_engine(_db_url(sqlserver_container, db_name))
        try:
            with engine.begin() as conn:
                conn.execute(sa.text("""
                    CREATE TABLE customers (
                        id INTEGER PRIMARY KEY,
                        name NVARCHAR(255) NOT NULL,
                        region NVARCHAR(255) NOT NULL
                    )
                """))
                conn.execute(sa.text("""
                    CREATE TABLE orders (
                        id INTEGER PRIMARY KEY,
                        status NVARCHAR(255) NOT NULL,
                        amount DECIMAL(10,2) NOT NULL,
                        customer_id INTEGER REFERENCES customers(id),
                        created_at DATETIME2 NOT NULL
                    )
                """))
                conn.execute(sa.text(
                    "INSERT INTO customers (id, name, region) VALUES "
                    "(1, 'Acme Corp', 'US'), "
                    "(2, 'Globex', 'EU'), "
                    "(3, 'Initech', 'US')"
                ))
                conn.execute(sa.text(
                    "INSERT INTO orders (id, status, amount, customer_id, created_at) VALUES "
                    "(1, 'completed', 100, 1, '2024-01-15 10:00:00'), "
                    "(2, 'completed', 200, 1, '2024-01-20 11:00:00'), "
                    "(3, 'pending', 50, 2, '2024-02-10 09:00:00'), "
                    "(4, 'completed', 150, 2, '2024-02-15 14:00:00'), "
                    "(5, 'cancelled', 75, 3, '2024-03-01 08:00:00'), "
                    "(6, 'pending', 300, 3, '2024-03-10 16:00:00')"
                ))
        finally:
            engine.dispose()

        tmpdir = str(tmp_path_factory.mktemp("sqlserver_env"))
        storage = YAMLStorage(base_dir=tmpdir)
        run_sync(storage.save_datasource(_ds_config(sqlserver_container, db_name)))

        orders_model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="testmssql",
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
            data_source="testmssql",
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
        _drop_module_db(sqlserver_container, db_name)


@pytest.fixture
def sqlserver_env(_sqlserver_env_storage) -> SlayerQueryEngine:
    return SlayerQueryEngine(storage=_sqlserver_env_storage)


@pytest.mark.integration
class TestSQLServerQueries:
    async def test_count_all(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(source_model="orders", measures=[{"formula": "*:count"}])
        result = await sqlserver_env.execute(query=query)
        assert result.row_count == 1
        assert result.data[0]["orders._count"] == 6

    async def test_sum_measure(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(source_model="orders", measures=[{"formula": "total:sum"}])
        result = await sqlserver_env.execute(query=query)
        assert float(result.data[0]["orders.total_sum"]) == 875.0  # NOSONAR(S1244) — sum of integer cents, exact-representable

    async def test_avg_measure(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(source_model="orders", measures=[{"formula": "avg_amount:avg"}])
        result = await sqlserver_env.execute(query=query)
        avg = float(result.data[0]["orders.avg_amount_avg"])
        assert abs(avg - 145.83) < 0.1

    async def test_group_by_status(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "status"}],
        )
        result = await sqlserver_env.execute(query=query)
        by_status = {r["orders.status"]: r["orders._count"] for r in result.data}
        assert by_status["completed"] == 3
        assert by_status["pending"] == 2
        assert by_status["cancelled"] == 1

    async def test_filter_equals(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            filters=["status == 'completed'"],
        )
        result = await sqlserver_env.execute(query=query)
        assert result.data[0]["orders._count"] == 3

    async def test_filter_gt(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            filters=["total > 100"],
        )
        result = await sqlserver_env.execute(query=query)
        assert result.data[0]["orders._count"] == 3

    async def test_order_by_desc(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "status"}],
            order=[{"column": {"name": "count"}, "direction": "desc"}],
        )
        result = await sqlserver_env.execute(query=query)
        assert result.data[0]["orders.status"] == "completed"

    async def test_limit(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "status"}],
            limit=2,
        )
        result = await sqlserver_env.execute(query=query)
        assert result.row_count == 2

    async def test_multiple_measures(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}, {"formula": "total:sum"}],
            dimensions=[{"name": "status"}],
        )
        result = await sqlserver_env.execute(query=query)
        completed = next(r for r in result.data if r["orders.status"] == "completed")
        assert completed["orders._count"] == 3
        assert float(completed["orders.total_sum"]) == 450.0  # NOSONAR(S1244) — sum of integer cents, exact-representable

    async def test_time_dimension_month_granularity(self, sqlserver_env: SlayerQueryEngine) -> None:
        """SQL Server 2022 supports DATETRUNC."""
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
        )
        result = await sqlserver_env.execute(query=query)
        assert result.row_count == 3

    async def test_time_dimension_with_date_range(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            time_dimensions=[{
                "dimension": {"name": "created_at"},
                "granularity": "month",
                "date_range": ["2024-01-01", "2024-02-28"],
            }],
        )
        result = await sqlserver_env.execute(query=query)
        total = sum(r["orders._count"] for r in result.data)
        assert total == 4

    async def test_composite_filter(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            filters=["status == 'completed' or status == 'pending'"],
        )
        result = await sqlserver_env.execute(query=query)
        assert result.data[0]["orders._count"] == 5

    async def test_time_shift_with_date_range(self, sqlserver_env: SlayerQueryEngine) -> None:
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
        result = await sqlserver_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(375.0)
        assert float(result.data[0]["orders.prev_month"]) == pytest.approx(200.0)

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "Separate pre-existing T-SQL limitation (NOT one of DEV-1571's "
            "three bugs): consecutive_periods desugars the predicate as a "
            "bare boolean projection (`col > 200 AS alias`). T-SQL has no "
            "boolean scalar type and rejects `>` in a SELECT projection "
            "with `Incorrect syntax near '>'`. Fix requires wrapping the "
            "predicate as `CASE WHEN col > 200 THEN 1 ELSE 0 END` in the "
            "consecutive_periods enrichment path. Track separately — "
            "DEV-1571 only covers the CTE-hoist, alias-mangle, and "
            "outer-wrap-quote bugs."
        ),
    )
    async def test_consecutive_periods_with_boolean_predicate(self, sqlserver_env: SlayerQueryEngine) -> None:
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
        result = await sqlserver_env.execute(query=query)
        assert [r["orders.positive_run"] for r in result.data] == [1, 0, 1]

    async def test_change_with_date_range(self, sqlserver_env: SlayerQueryEngine) -> None:
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
        result = await sqlserver_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.amount_change"]) == pytest.approx(175.0)

    async def test_change_pct_with_date_range(self, sqlserver_env: SlayerQueryEngine) -> None:
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
        result = await sqlserver_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.pct"]) == pytest.approx(0.875)

    async def test_multiple_date_range_shifts(self, sqlserver_env: SlayerQueryEngine) -> None:
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
        result = await sqlserver_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(200.0)
        assert float(result.data[0]["orders.prev"]) == pytest.approx(300.0)
        assert float(result.data[0]["orders.next"]) == pytest.approx(375.0)


# ---------------------------------------------------------------------------
# Cross-model + multistage
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlserver_cross_model_env(sqlserver_container):
    """SQL Server env with orders + customers (with score) + join."""
    db_name = _create_module_db(sqlserver_container)
    engine = sa.create_engine(_db_url(sqlserver_container, db_name))
    try:
        with engine.begin() as conn:
            conn.execute(sa.text("""
                CREATE TABLE customers (
                    id INTEGER PRIMARY KEY,
                    name NVARCHAR(255) NOT NULL,
                    region NVARCHAR(255) NOT NULL,
                    score DECIMAL(5,2) NOT NULL
                )
            """))
            conn.execute(sa.text("""
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    status NVARCHAR(255) NOT NULL,
                    amount DECIMAL(10,2) NOT NULL,
                    customer_id INTEGER REFERENCES customers(id),
                    created_at DATETIME2 NOT NULL
                )
            """))
            conn.execute(sa.text(
                "INSERT INTO customers (id, name, region, score) VALUES "
                "(1, 'Alice', 'US', 90), "
                "(2, 'Bob', 'EU', 60), "
                "(3, 'Charlie', 'US', 80)"
            ))
            conn.execute(sa.text(
                "INSERT INTO orders (id, status, amount, customer_id, created_at) VALUES "
                "(1, 'completed', 100, 1, '2024-01-15 10:00:00'), "
                "(2, 'completed', 200, 1, '2024-01-20 11:00:00'), "
                "(3, 'pending', 50, 2, '2024-02-10 09:00:00'), "
                "(4, 'completed', 150, 2, '2024-02-15 14:00:00'), "
                "(5, 'completed', 300, 3, '2024-03-01 08:00:00'), "
                "(6, 'pending', 25, 1, '2024-03-10 16:00:00')"
            ))
    finally:
        engine.dispose()

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(sqlserver_container, db_name)))
    run_sync(storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="testmssql",
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
        name="customers", sql_table="customers", data_source="testmssql",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="avg_score", sql="score", type=DataType.DOUBLE),
        ],
    )))
    yield SlayerQueryEngine(storage=storage)
    _drop_module_db(sqlserver_container, db_name)


@pytest.mark.integration
class TestCrossModelAndMultistageSQLServer:
    async def test_cross_model_measure(self, sqlserver_cross_model_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="customers.avg_score:avg")],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        )
        result = await sqlserver_cross_model_env.execute(query=query)
        assert result.row_count == 3
        global_avg = pytest.approx((90.0 + 60.0 + 80.0) / 3)
        assert float(result.data[0]["orders.customers.avg_score_avg"]) == global_avg
        assert float(result.data[1]["orders.customers.avg_score_avg"]) == global_avg
        assert float(result.data[2]["orders.customers.avg_score_avg"]) == global_avg

    async def test_query_list_named(self, sqlserver_cross_model_env: SlayerQueryEngine) -> None:
        inner = SlayerQuery(
            name="monthly", source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="total:sum")],
        )
        outer = SlayerQuery(source_model="monthly", measures=[ModelMeasure(formula="*:count")])
        result = await sqlserver_cross_model_env.execute(query=[inner, outer])
        assert result.data[0]["monthly._count"] == 3

    async def test_create_model_from_query(self, sqlserver_cross_model_env: SlayerQueryEngine) -> None:
        source = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="total:sum")],
        )
        saved = await sqlserver_cross_model_env.create_model_from_query(query=source, name="ss_monthly")
        assert saved.source_queries is not None
        result = await sqlserver_cross_model_env.execute(
            query=SlayerQuery(source_model="ss_monthly", measures=[ModelMeasure(formula="*:count")])
        )
        assert result.data[0]["ss_monthly._count"] == 3

    async def test_sql_dimension(self, sqlserver_cross_model_env: SlayerQueryEngine) -> None:
        from slayer.core.query import ModelExtension
        query = SlayerQuery(
            source_model=ModelExtension(
                source_name="orders",
                columns=[{"name": "tier", "sql": "CASE WHEN amount > 100 THEN 'high' ELSE 'low' END"}],
            ),
            dimensions=[ColumnRef(name="tier")],
            measures=[ModelMeasure(formula="*:count")],
        )
        result = await sqlserver_cross_model_env.execute(query=query)
        by_tier = {r["orders.tier"]: r["orders._count"] for r in result.data}
        assert by_tier["high"] == 3
        assert by_tier["low"] == 3


# ---------------------------------------------------------------------------
# Rollup ingestion (SQL Server FK metadata IS exposed via Inspector)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sqlserver_ingest_env(sqlserver_container):
    """Set up tables with FK relationships and ingest."""
    db_name = _create_module_db(sqlserver_container)
    try:
        engine = sa.create_engine(_db_url(sqlserver_container, db_name))
        try:
            with engine.begin() as conn:
                conn.execute(sa.text("""
                    CREATE TABLE regions (
                        id INTEGER PRIMARY KEY,
                        name NVARCHAR(255) NOT NULL
                    )
                """))
                conn.execute(sa.text("""
                    CREATE TABLE customers (
                        id INTEGER PRIMARY KEY,
                        name NVARCHAR(255) NOT NULL,
                        region_id INTEGER REFERENCES regions(id)
                    )
                """))
                conn.execute(sa.text("""
                    CREATE TABLE orders (
                        id INTEGER PRIMARY KEY,
                        amount DECIMAL(10,2) NOT NULL,
                        customer_id INTEGER REFERENCES customers(id)
                    )
                """))
                conn.execute(sa.text(
                    "INSERT INTO regions (id, name) VALUES (1, 'US'), (2, 'EU')"
                ))
                conn.execute(sa.text(
                    "INSERT INTO customers (id, name, region_id) VALUES "
                    "(1, 'Acme', 1), (2, 'Globex', 2), (3, 'Initech', 1)"
                ))
                conn.execute(sa.text(
                    "INSERT INTO orders (id, amount, customer_id) VALUES "
                    "(1, 100, 1), (2, 200, 1), (3, 50, 2), (4, 150, 3)"
                ))
        finally:
            engine.dispose()

        ds = _ds_config(sqlserver_container, db_name)
        models = ingest_datasource(datasource=ds, schema=None)
        yield models, ds, None
    finally:
        _drop_module_db(sqlserver_container, db_name)


@pytest.mark.integration
class TestRollupIngestionSQLServer:
    def test_orders_has_own_columns_only(self, sqlserver_ingest_env) -> None:
        models, _, _ = sqlserver_ingest_env
        orders = next(m for m in models if m.name == "orders")
        col_names = [c.name for c in orders.columns]
        assert "id" in col_names
        assert "customer_id" in col_names
        assert "amount" in col_names
        assert not any("." in name for name in col_names)

    def test_orders_uses_sql_table_with_joins(self, sqlserver_ingest_env) -> None:
        models, _, _ = sqlserver_ingest_env
        orders = next(m for m in models if m.name == "orders")
        assert orders.sql_table is not None
        assert orders.sql is None
        assert len(orders.joins) > 0

    def test_regions_has_no_rollup(self, sqlserver_ingest_env) -> None:
        models, _, _ = sqlserver_ingest_env
        regions = next(m for m in models if m.name == "regions")
        assert regions.sql_table is not None
        assert regions.sql is None

    async def test_rollup_query_group_by_customer(self, sqlserver_ingest_env) -> None:
        models, ds, _ = sqlserver_ingest_env
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

    async def test_rollup_query_group_by_region(self, sqlserver_ingest_env) -> None:
        models, ds, _ = sqlserver_ingest_env
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

    def test_orders_has_no_named_measures_after_ingest(self, sqlserver_ingest_env) -> None:
        models, _, _ = sqlserver_ingest_env
        orders = next(m for m in models if m.name == "orders")
        assert orders.measures == []
        col_names = [c.name for c in orders.columns]
        assert "amount" in col_names

    # NOTE: ``test_dotted_dimension_single_hop`` is intentionally not ported —
    # its body would be identical to ``test_rollup_query_group_by_customer``
    # (Sonar python:S4144). Single-hop path is already covered there; the
    # multi-hop variant below tests the part that's genuinely different.

    async def test_dotted_dimension_multi_hop(self, sqlserver_ingest_env) -> None:
        models, ds, _ = sqlserver_ingest_env
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

    async def test_selective_joins_no_joined_dims(self, sqlserver_ingest_env) -> None:
        models, ds, _ = sqlserver_ingest_env
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

    async def test_selective_joins_single_hop(self, sqlserver_ingest_env) -> None:
        models, ds, _ = sqlserver_ingest_env
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

    async def test_selective_joins_transitive(self, sqlserver_ingest_env) -> None:
        models, ds, _ = sqlserver_ingest_env
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

    def test_orders_has_joins_metadata(self, sqlserver_ingest_env) -> None:
        models, _, _ = sqlserver_ingest_env
        orders = next(m for m in models if m.name == "orders")
        join_targets = [j.target_model for j in orders.joins]
        assert "customers" in join_targets
        assert "regions" not in join_targets
        for j in orders.joins:
            assert len(j.join_pairs) >= 1
            for pair in j.join_pairs:
                assert len(pair) == 2

    def test_regions_has_no_joins(self, sqlserver_ingest_env) -> None:
        models, _, _ = sqlserver_ingest_env
        regions = next(m for m in models if m.name == "regions")
        assert regions.joins == []

    async def test_joins_serialize_to_yaml(self, sqlserver_ingest_env) -> None:
        models, _, _ = sqlserver_ingest_env
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
# Median / percentile — T-SQL raises NotImplementedError
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestSQLServerMedianPercentileRaises:
    async def test_median_raises(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(source_model="orders", measures=[{"formula": "total:median"}])
        with pytest.raises(NotImplementedError, match="(?i)median.*t-sql"):
            await sqlserver_env.execute(query=query)

    async def test_median_grouped_raises(self, sqlserver_env: SlayerQueryEngine) -> None:
        """Grouped median goes through the same TsqlDialect.build_median hook."""
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:median"}],
            dimensions=[{"name": "status"}],
        )
        with pytest.raises(NotImplementedError, match="(?i)median.*t-sql"):
            await sqlserver_env.execute(query=query)

    async def test_percentile_raises(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:percentile(p=0.5)"}],
        )
        with pytest.raises(NotImplementedError, match="(?i)percentile.*t-sql"):
            await sqlserver_env.execute(query=query)


# ---------------------------------------------------------------------------
# Statistical aggregations (DEV-1317 cross-dialect parity)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestSQLServerStatAggregations:
    """T-SQL has native STDEV / STDEVP / VAR / VARP (TsqlDialect maps
    stddev_samp/_pop and var_samp/_pop via exp.Anonymous to these names).
    CORR / COVAR_* use the variance-decomposition formula with the T-SQL
    name set (VAR / VARP / STDEV)."""

    async def test_stddev_samp_native_sqlserver(self, sqlserver_env: SlayerQueryEngine) -> None:
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:stddev_samp"}],
        )
        result = await sqlserver_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_stddev_samp"]) == pytest.approx(
            statistics.stdev(amounts), rel=1e-9
        )

        # T-SQL: stddev_samp → STDEV (exp.Anonymous override).
        dry = await sqlserver_env.execute(query=query, dry_run=True)
        assert dry.sql is not None
        sql_upper = dry.sql.upper()
        assert "STDEV(" in sql_upper, (
            f"T-SQL stddev_samp must emit STDEV(. Got:\n{dry.sql}"
        )

    async def test_stddev_pop_native_sqlserver(self, sqlserver_env: SlayerQueryEngine) -> None:
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:stddev_pop"}],
        )
        result = await sqlserver_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_stddev_pop"]) == pytest.approx(
            statistics.pstdev(amounts), rel=1e-9
        )

        dry = await sqlserver_env.execute(query=query, dry_run=True)
        assert dry.sql is not None
        sql_upper = dry.sql.upper()
        assert "STDEVP(" in sql_upper, (
            f"T-SQL stddev_pop must emit STDEVP(. Got:\n{dry.sql}"
        )

    async def test_var_samp_uses_canonical_tsql_name(self, sqlserver_env: SlayerQueryEngine) -> None:
        """T-SQL: var_samp must emit ``VAR(`` not ``VAR_SAMP(``."""
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:var_samp"}],
        )
        result = await sqlserver_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_var_samp"]) == pytest.approx(
            statistics.variance(amounts), rel=1e-9
        )

        dry = await sqlserver_env.execute(query=query, dry_run=True)
        assert dry.sql is not None
        sql_upper = dry.sql.upper()
        assert "VAR(" in sql_upper and "VAR_SAMP(" not in sql_upper, (
            f"T-SQL var_samp must emit canonical VAR(, not VAR_SAMP(. Got:\n{dry.sql}"
        )

    async def test_var_pop_uses_canonical_tsql_name(self, sqlserver_env: SlayerQueryEngine) -> None:
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:var_pop"}],
        )
        result = await sqlserver_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_var_pop"]) == pytest.approx(
            statistics.pvariance(amounts), rel=1e-9
        )

        dry = await sqlserver_env.execute(query=query, dry_run=True)
        assert dry.sql is not None
        sql_upper = dry.sql.upper()
        assert "VARP(" in sql_upper, (
            f"T-SQL var_pop must emit canonical VARP(. Got:\n{dry.sql}"
        )

    async def test_corr_variance_decomposition_sqlserver(self, sqlserver_env: SlayerQueryEngine) -> None:
        """T-SQL CORR uses variance-decomposition with VAR / STDEV (not
        VAR_SAMP / STDDEV_SAMP). Pin numeric + SQL shape."""
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:corr(other=customer_id)"}],
        )
        result = await sqlserver_env.execute(query=query)
        xs = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        ys = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        expected = statistics.correlation(xs, ys)
        assert float(result.data[0]["orders.total_corr_other_customer_id"]) == pytest.approx(
            expected, rel=1e-9
        )

        dry = await sqlserver_env.execute(query=query, dry_run=True)
        assert dry.sql is not None
        sql_upper = dry.sql.upper()
        # Variance-decomposition: VAR( appears multiple times.
        assert sql_upper.count("VAR(") >= 3, (
            f"T-SQL CORR must use variance-decomposition with VAR(. Got:\n{dry.sql}"
        )
        assert "STDEV(" in sql_upper, (
            f"T-SQL CORR normalisation must use STDEV(. Got:\n{dry.sql}"
        )

    async def test_covar_samp_variance_decomposition_sqlserver(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:covar_samp(other=customer_id)"}],
        )
        result = await sqlserver_env.execute(query=query)
        xs = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        ys = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        n = len(xs)
        mx, my = sum(xs) / n, sum(ys) / n
        expected = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (n - 1)
        assert float(
            result.data[0]["orders.total_covar_samp_other_customer_id"]
        ) == pytest.approx(expected, rel=1e-9)

        # T-SQL has no native COVAR_SAMP — pin the variance-decomposition
        # shape via dry_run: VAR( appears multiple times, no COVAR_SAMP(.
        dry = await sqlserver_env.execute(query=query, dry_run=True)
        assert dry.sql is not None
        sql_upper = dry.sql.upper()
        assert sql_upper.count("VAR(") >= 3, (
            f"T-SQL COVAR_SAMP must use variance-decomposition. Got:\n{dry.sql}"
        )
        assert "COVAR_SAMP(" not in sql_upper, (
            f"T-SQL must not emit COVAR_SAMP. Got:\n{dry.sql}"
        )

    async def test_covar_pop_variance_decomposition_sqlserver(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:covar_pop(other=customer_id)"}],
        )
        result = await sqlserver_env.execute(query=query)
        xs = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        ys = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        n = len(xs)
        mx, my = sum(xs) / n, sum(ys) / n
        expected = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / n
        assert float(
            result.data[0]["orders.total_covar_pop_other_customer_id"]
        ) == pytest.approx(expected, rel=1e-9)

        dry = await sqlserver_env.execute(query=query, dry_run=True)
        assert dry.sql is not None
        sql_upper = dry.sql.upper()
        assert sql_upper.count("VARP(") >= 3, (
            f"T-SQL COVAR_POP must use variance-decomposition with VARP. Got:\n{dry.sql}"
        )
        assert "COVAR_POP(" not in sql_upper, (
            f"T-SQL must not emit COVAR_POP. Got:\n{dry.sql}"
        )


# ---------------------------------------------------------------------------
# log10 round-trip (DEV-1337 — T-SQL has native LOG10)
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlserver_log10_env(sqlserver_container):
    db_name = _create_module_db(sqlserver_container)
    engine = sa.create_engine(_db_url(sqlserver_container, db_name))
    try:
        with engine.begin() as conn:
            conn.execute(sa.text("""
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    amount DECIMAL(10,2) NOT NULL
                )
            """))
            conn.execute(sa.text(
                "INSERT INTO orders (id, amount) VALUES (1, 100), (2, 200), (3, 300)"
            ))
    finally:
        engine.dispose()

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(sqlserver_container, db_name)))
    run_sync(storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="testmssql",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="log_amount", sql="log10(amount)", type=DataType.DOUBLE),
        ],
    )))
    yield SlayerQueryEngine(storage=storage)
    _drop_module_db(sqlserver_container, db_name)


@pytest.mark.integration
async def test_log10_round_trip_sqlserver(sqlserver_log10_env: SlayerQueryEngine) -> None:
    """T-SQL has native LOG10 (TsqlDialect.log10_native = True). The emitted
    SQL must preserve ``LOG10(...)``."""
    import math as _math

    result = await sqlserver_log10_env.execute(
        SlayerQuery(source_model="orders", measures=[{"formula": "log_amount:max"}])
    )
    assert float(result.data[0]["orders.log_amount_max"]) == pytest.approx(
        _math.log10(300.0), rel=1e-9
    )

    dry = await sqlserver_log10_env.execute(
        SlayerQuery(source_model="orders", measures=[{"formula": "log_amount:max"}]),
        dry_run=True,
    )
    assert dry.sql is not None
    sql_lower = dry.sql.lower()
    assert "log10(" in sql_lower, (
        f"Expected literal log10(...) in emitted SQL on T-SQL, got:\n{dry.sql}"
    )
    assert "log(10," not in sql_lower.replace(" ", ""), (
        f"T-SQL must not canonicalise log10 to LOG(10, ...):\n{dry.sql}"
    )


# ---------------------------------------------------------------------------
# T-SQL-specific: DATETRUNC(iso_week) + DATEADD
# ---------------------------------------------------------------------------


@pytest.mark.integration
async def test_sqlserver_week_uses_iso_week(sqlserver_env: SlayerQueryEngine) -> None:
    """TsqlDialect.build_date_trunc maps granularity=week to
    ``DATETRUNC(iso_week, col)`` (Monday-aligned, @@DATEFIRST-independent).
    Pin the emitted SQL."""
    query = SlayerQuery(
        source_model="orders",
        measures=[{"formula": "*:count"}],
        time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "week"}],
    )
    dry = await sqlserver_env.execute(query=query, dry_run=True)
    assert dry.sql is not None
    sql_lower = dry.sql.lower()
    assert "datetrunc(iso_week" in sql_lower, (
        f"T-SQL week truncation must use DATETRUNC(iso_week, ...). Got:\n{dry.sql}"
    )


@pytest.mark.integration
async def test_sqlserver_time_shift_uses_dateadd(sqlserver_env: SlayerQueryEngine) -> None:
    """TsqlDialect.build_time_offset_expr emits ``DATEADD(MONTH, -1, col)``
    instead of ``col - INTERVAL 1 MONTH``. INTERVAL is not valid T-SQL."""
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="total:sum"),
            ModelMeasure(formula="time_shift(total:sum, -1, 'month')", name="prev"),
        ],
    )
    dry = await sqlserver_env.execute(query=query, dry_run=True)
    assert dry.sql is not None
    sql_upper = dry.sql.upper()
    assert "DATEADD(" in sql_upper, (
        f"T-SQL time_shift must use DATEADD. Got:\n{dry.sql}"
    )
    # No INTERVAL keyword (not valid T-SQL) — check word boundary to avoid
    # false-positives on column names containing 'interval'.
    import re as _re
    assert not _re.search(r"\bINTERVAL\b", sql_upper), (
        f"T-SQL must not emit INTERVAL keyword. Got:\n{dry.sql}"
    )


# ---------------------------------------------------------------------------
# Window-in-filter raises (DEV-1369 parity)
# ---------------------------------------------------------------------------


@pytest.fixture
def planets_sqlserver_env(sqlserver_container):
    db_name = _create_module_db(sqlserver_container)
    engine = sa.create_engine(_db_url(sqlserver_container, db_name))
    try:
        with engine.begin() as conn:
            conn.execute(sa.text("""
                CREATE TABLE planets (
                    id INTEGER PRIMARY KEY,
                    name NVARCHAR(255) NOT NULL,
                    mass DECIMAL(12, 4) NOT NULL
                )
            """))
            conn.execute(sa.text(
                "INSERT INTO planets (id, name, mass) VALUES "
                "(1, 'Mercury', 0.33), "
                "(2, 'Venus', 4.87), "
                "(3, 'Earth', 5.97), "
                "(4, 'Mars', 0.642), "
                "(5, 'Jupiter', 1898.0), "
                "(6, 'Saturn', 568.0), "
                "(7, 'Uranus', 86.8), "
                "(8, 'Neptune', 102.0)"
            ))
    finally:
        engine.dispose()

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(sqlserver_container, db_name)))
    run_sync(storage.save_model(
        SlayerModel(
            name="planets",
            sql_table="planets",
            data_source="testmssql",
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
    _drop_module_db(sqlserver_container, db_name)


@pytest.mark.integration
async def test_filter_on_windowed_column_sqlserver_raises(planets_sqlserver_env) -> None:
    engine = planets_sqlserver_env
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
def sqlserver_derived_chain_env(sqlserver_container):
    db_name = _create_module_db(sqlserver_container)
    engine = sa.create_engine(_db_url(sqlserver_container, db_name))
    try:
        with engine.begin() as conn:
            conn.execute(sa.text(
                "CREATE TABLE b_tbl (id INTEGER PRIMARY KEY, foo_raw DECIMAL(10,2))"
            ))
            conn.execute(sa.text(
                "CREATE TABLE a_tbl ("
                "id INTEGER PRIMARY KEY, bar DECIMAL(10,2), b_id INTEGER, raw_a DECIMAL(10,2)"
                ")"
            ))
            conn.execute(sa.text(
                "INSERT INTO b_tbl (id, foo_raw) VALUES (1, 200), (2, 50)"
            ))
            conn.execute(sa.text(
                "INSERT INTO a_tbl (id, bar, b_id, raw_a) VALUES "
                "(10, 4, 1, 100), (11, 1, 2, 5)"
            ))
    finally:
        engine.dispose()

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(sqlserver_container, db_name)))
    run_sync(storage.save_model(
        SlayerModel(
            name="b_tbl",
            data_source="testmssql",
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
            data_source="testmssql",
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
    _drop_module_db(sqlserver_container, db_name)


@pytest.mark.integration
async def test_integration_sqlserver_cross_model_derived_columnsql(
    sqlserver_derived_chain_env: SlayerQueryEngine,
) -> None:
    response = await sqlserver_derived_chain_env.execute(
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
