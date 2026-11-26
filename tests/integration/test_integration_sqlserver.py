"""Integration tests using a real SQL Server database via testcontainers.

SQL Server is the biggest gap in the existing CI matrix
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
* ``MEDIAN`` / ``PERCENTILE_CONT`` read a window per row (``PERCENTILE_CONT`` in
  T-SQL is window-only), regrouped.
* Native ``LOG10`` (``TsqlDialect.log10_native = True``).

The pytest job's CI step (.github/workflows/integration-sqlserver.yml)
installs ``msodbcsql18`` + ``unixodbc-dev`` on the runner so pyodbc can
load. Locally, run::

    sudo apt install unixodbc-dev msodbcsql18  # or your distro equivalent

before invoking this suite.

Teardown of the per-module DB requires special handling: SLayer caches
SQLAlchemy engines (``slayer/database/engine_factory.py``) and pyodbc pools
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

import math as _math
import re as _re
import statistics
import tempfile
from collections.abc import Iterator
import uuid
from decimal import Decimal
from typing import Any
from datetime import datetime, timezone
from typing import Optional

import pytest
import sqlalchemy as sa
import sqlalchemy.exc
from tests._dev1737_fixtures import (
    DT,
    DateCase,
    all_datasets,
    assert_case,
    check_server_scenarios,
    matrix_cases,
    matrix_query,
    oracle_part,
    parse_temporal,
    server_seed_statements,
)
from tests._engine_helpers import disposable_engine
from tests._reserved_alias_probe import keyword_universe, sa_statement_fails, unquoted_alias_failures
from tests.integration._one_request import PATHS, assert_one_request_each, run_three

from slayer.async_utils import run_sync
from slayer.core.enums import DataType, TimeGranularity
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    SlayerDataset,
)
from slayer.core.query import OrderItem, QueryDimension, QueryMetric, SlayerQuery, TimeDimension
from slayer.live_schema import ingestion
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database import engine_factory
from slayer.database.client import SlayerSQLClient
from slayer.sql.dialects import TsqlDialect
from slayer.storage.yaml_storage import YAMLStorage
from sqlglot import exp
from tests.integration._dev2015_server import (
    check_all,
    server_datasets,
    server_datasets_ts,
    server_statements,
    with_granularities,
)
from tests._dev2018_fixtures import (
    check_bucket_matrix,
    check_mixed,
    server_datasets as logical_type_datasets,
    server_statements as logical_type_statements,
    with_granularities as logical_type_granularities,
)
from tests._dev2046_fixtures import (
    FLAG_TOTALS,
    GT15_TOTALS,
    by_dim,
    dev2046_datasets,
    m as bool_metric,
    orders_q,
    seed_statements as dev2046_seed_statements,
)
from tests._cast_fixtures import check_author_casts, check_boolean_casts, check_try_cast_targets
from tests._dev2101_fixtures import (
    check_filtered_aggregations,
    check_null_ordering,
    check_server_forms,
    check_try_cast,
    dev2101_datasets,
    seed_server as dev2101_seed_server,
)
from tests._dev2120_fixtures import (
    LIB,
    REGEX_NAMES,
    check_library,
    lib_dataset,
    server_table_statements as dev2120_table_statements,
)
from tests._dev2457_fixtures import server_statements as window_statements, window_datasets
from tests._dev2457_matrix import CASE_IDS as WINDOW_CASE_IDS, CASES as WINDOW_CASES, WindowCase, check_case
from tests.integration._consecutive_periods_calendar import (
    CALENDAR_CASES,
    assert_calendar_streak,
    seed_statements,
    streak_dataset,
)

pytest.importorskip("testcontainers.mssql")
pytest.importorskip("pyodbc")

import pyodbc  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above
from testcontainers.mssql import SqlServerContainer  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

import docker  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above


# ---------------------------------------------------------------------------
# Docker availability
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def _docker_available_or_skip():
    try:
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
    with disposable_engine(_admin_url(sqlserver_container), isolation_level="AUTOCOMMIT") as engine:
        with engine.connect() as conn:
            conn.execute(sa.text(f"CREATE DATABASE [{db_name}]"))
    return db_name


def _drop_module_db(sqlserver_container, db_name: str) -> None:
    """Tear down the per-module DB cleanly despite pyodbc engine caching."""
    # 1) Release everything pooled at the slayer engine cache.
    for engine in engine_factory._engine_cache.values():
        engine.dispose()
    engine_factory.reset_cache()

    # 2) Kick anyone else off the DB; then drop it. AUTOCOMMIT so the ALTER
    #    isolation change takes effect immediately.
    with disposable_engine(_admin_url(sqlserver_container), isolation_level="AUTOCOMMIT") as engine:
        with engine.connect() as conn:
            conn.execute(sa.text(
                f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
            ))
            conn.execute(sa.text(f"DROP DATABASE [{db_name}]"))


async def _ingest(ds: DatasourceConfig, **kwargs: Any) -> list[SlayerDataset]:
    sql_client = SlayerSQLClient(datasource=ds)
    try:
        return await ingestion.ingest_datasource_async(ds, client=sql_client, **kwargs)
    finally:
        await sql_client.aclose()
        sql_client.close()


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
        with disposable_engine(_db_url(sqlserver_container, db_name)) as engine:
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
                for stmt in seed_statements(timestamp_type="DATETIME2"):
                    conn.execute(sa.text(stmt))

        tmpdir = str(tmp_path_factory.mktemp("sqlserver_env"))
        storage = YAMLStorage(base_dir=tmpdir)
        run_sync(storage.save_datasource(_ds_config(sqlserver_container, db_name)))

        orders_model = SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="testmssql",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="status", expression="status", datatype=DataType.STRING),
                DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
                DatasetField(name="created_at", expression="created_at", datatype=DataType.DATETIME),
                DatasetField(name="total", expression="amount", datatype=DataType.FLOAT),
                DatasetField(name="avg_amount", expression="amount", datatype=DataType.FLOAT),
            ],
        )
        customers_model = SlayerDataset(
            name="customers",
            sql_table="customers",
            data_source="testmssql",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="name", expression="name", datatype=DataType.STRING),
                DatasetField(name="region", expression="region", datatype=DataType.STRING),
            ],
        )
        run_sync(storage.save_dataset(orders_model))
        run_sync(storage.save_dataset(customers_model))
        run_sync(storage.save_dataset(streak_dataset("testmssql")))
        yield storage
    finally:
        _drop_module_db(sqlserver_container, db_name)


@pytest.fixture
def sqlserver_env(_sqlserver_env_storage) -> SlayerQueryEngine:
    return SlayerQueryEngine(storage=_sqlserver_env_storage)


@pytest.mark.integration
class TestSQLServerQueries:
    async def test_count_all(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "count(orders.*)"}]})
        result = await sqlserver_env.execute(query=query)
        assert result.row_count == 1
        assert result.data[0]["orders._count"] == 6

    async def test_regex_literal_extension_column(self, sqlserver_env: SlayerQueryEngine) -> None:
        """A dataset field holding a ``(?:...)`` regex literal and a ``%``
        LIKE pattern executes verbatim; text() misread ``:too`` as a bind parameter."""
        orders = await sqlserver_env.storage.get_dataset("orders")
        assert orders is not None
        await sqlserver_env.storage.save_dataset(orders.model_copy(update={"name": "orders_rx", "fields": [
            *orders.fields, DatasetField(name="rx", expression=ExpressionObject(dialects=[DialectEntry(dialect="TSQL", expression="CASE WHEN status LIKE '%pend%' OR status = '(?i)(?:too complicated|too complex)' THEN 1 ELSE 0 END")]), datatype=DataType.FLOAT),
        ]}))
        query = SlayerQuery(
            population="orders_rx",
            dimensions=[QueryDimension(field="orders_rx.rx")],
            metrics=[QueryMetric(metric="count(orders_rx.*)")],
        )
        result = await sqlserver_env.execute(query=query)
        by_rx = {int(r["orders_rx.rx"]): r["orders_rx._count"] for r in result.data}
        assert by_rx == {1: 2, 0: 4}

    async def test_sum_measure(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(orders.total)"}]})
        result = await sqlserver_env.execute(query=query)
        assert float(result.data[0]["orders.total_sum"]) == 875.0  # NOSONAR(S1244) — sum of integer cents, exact-representable

    async def test_avg_measure(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "avg(orders.avg_amount)"}]})
        result = await sqlserver_env.execute(query=query)
        avg = float(result.data[0]["orders.avg_amount_avg"])
        assert abs(avg - 145.83) < 0.1

    async def test_group_by_status(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "dimensions": ["orders.status"],
        })
        result = await sqlserver_env.execute(query=query)
        by_status = {r["orders.status"]: r["orders._count"] for r in result.data}
        assert by_status["completed"] == 3
        assert by_status["pending"] == 2
        assert by_status["cancelled"] == 1

    async def test_filter_equals(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "filter": ["orders.status == 'completed'"],
        })
        result = await sqlserver_env.execute(query=query)
        assert result.data[0]["orders._count"] == 3

    async def test_filter_gt(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "filter": ["orders.total > 100"],
        })
        result = await sqlserver_env.execute(query=query)
        assert result.data[0]["orders._count"] == 3

    async def test_order_by_desc(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "dimensions": ["orders.status"],
            "order": [{"field": "count(orders.*)", "direction": "desc"}],
        })
        result = await sqlserver_env.execute(query=query)
        assert result.data[0]["orders.status"] == "completed"

    async def test_limit(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "dimensions": ["orders.status"],
            "limit": 2,
        })
        result = await sqlserver_env.execute(query=query)
        assert result.row_count == 2

    async def test_multiple_measures(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}, {"metric": "sum(orders.total)"}],
            "dimensions": ["orders.status"],
        })
        result = await sqlserver_env.execute(query=query)
        completed = next(r for r in result.data if r["orders.status"] == "completed")
        assert completed["orders._count"] == 3
        assert float(completed["orders.total_sum"]) == 450.0  # NOSONAR(S1244) — sum of integer cents, exact-representable

    async def test_time_dimension_month_granularity(self, sqlserver_env: SlayerQueryEngine) -> None:
        """SQL Server 2022 supports DATETRUNC."""
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}],
        })
        result = await sqlserver_env.execute(query=query)
        assert result.row_count == 3

    async def test_time_dimension_with_date_range(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "time_dimensions": [{
                "field": "orders.created_at",
                "granularity": "month",
                "date_range": ["2024-01-01", "2024-02-28"],
            }],
        })
        result = await sqlserver_env.execute(query=query)
        total = sum(r["orders._count"] for r in result.data)
        assert total == 4

    async def test_composite_filter(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "filter": ["orders.status == 'completed' or orders.status == 'pending'"],
        })
        result = await sqlserver_env.execute(query=query)
        assert result.data[0]["orders._count"] == 5

    async def test_time_shift_with_date_range(self, sqlserver_env: SlayerQueryEngine) -> None:
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
        result = await sqlserver_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(375.0)
        assert float(result.data[0]["prev_month"]) == pytest.approx(200.0)

    async def test_consecutive_periods_with_boolean_predicate(self, sqlserver_env: SlayerQueryEngine) -> None:
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
        result = await sqlserver_env.execute(query=query)
        assert [r["positive_run"] for r in result.data] == [1, 0, 1]

    @pytest.mark.parametrize("column,granularity", CALENDAR_CASES)
    async def test_consecutive_periods_breaks_on_calendar_gap(
        self, sqlserver_env: SlayerQueryEngine, column: str, granularity: str,
    ) -> None:
        await assert_calendar_streak(sqlserver_env, field=column, granularity=granularity)

    async def test_change_with_date_range(self, sqlserver_env: SlayerQueryEngine) -> None:
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
        result = await sqlserver_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["amount_change"]) == pytest.approx(175.0)

    async def test_change_pct_with_date_range(self, sqlserver_env: SlayerQueryEngine) -> None:
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
        result = await sqlserver_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["pct"]) == pytest.approx(0.875)

    async def test_multiple_date_range_shifts(self, sqlserver_env: SlayerQueryEngine) -> None:
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
        result = await sqlserver_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(200.0)
        assert float(result.data[0]["prev"]) == pytest.approx(300.0)
        assert float(result.data[0]["next"]) == pytest.approx(375.0)


# ---------------------------------------------------------------------------
# Cross-dataset + multistage
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlserver_cross_dataset_env(sqlserver_container):
    """SQL Server env with orders + customers (with score) + relationship."""
    db_name = _create_module_db(sqlserver_container)
    with disposable_engine(_db_url(sqlserver_container, db_name)) as engine:
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

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(sqlserver_container, db_name)))
    run_sync(storage.save_dataset(SlayerDataset(
        name="orders", sql_table="orders", data_source="testmssql",
        default_time_dimension="created_at",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="status", expression="status", datatype=DataType.STRING),
            DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
            DatasetField(name="created_at", expression="created_at", datatype=DataType.DATETIME),
            DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
            DatasetField(name="total", expression="amount", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
    )))
    run_sync(storage.save_dataset(SlayerDataset(
        name="customers", sql_table="customers", data_source="testmssql",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="name", expression="name", datatype=DataType.STRING),
            DatasetField(name="avg_score", expression="score", datatype=DataType.FLOAT),
        ],
    )))
    yield SlayerQueryEngine(storage=storage)
    _drop_module_db(sqlserver_container, db_name)


@pytest.mark.integration
class TestCrossModelAndMultistageSQLServer:
    async def test_cross_model_measure(self, sqlserver_cross_dataset_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.created_at", granularity=TimeGranularity.MONTH,
            )],
            metrics=[QueryMetric(metric="count(orders.*)"), QueryMetric(metric="avg(customers.avg_score)")],
            order=[OrderItem(field="orders.created_at", direction="asc")],
        )
        result = await sqlserver_cross_dataset_env.execute(query=query)
        assert result.row_count == 3
        assert [float(r["customers.avg_score_avg"]) for r in result.data] == pytest.approx([90.0, 60.0, 85.0])

    async def test_query_list_named(self, sqlserver_cross_dataset_env: SlayerQueryEngine) -> None:
        inner = SlayerQuery(
            name="monthly", population="orders",
            time_dimensions=[TimeDimension(
                field="orders.created_at", granularity=TimeGranularity.MONTH,
            )],
            metrics=[QueryMetric(metric="count(orders.*)"), QueryMetric(metric="sum(orders.total)")],
        )
        outer = SlayerQuery(population="monthly", metrics=[QueryMetric(metric="count(monthly.*)")])
        result = await sqlserver_cross_dataset_env.execute(query=[inner, outer])
        assert result.data[0]["monthly._count"] == 3

    async def test_create_model_from_query(self, sqlserver_cross_dataset_env: SlayerQueryEngine) -> None:
        source = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.created_at", granularity=TimeGranularity.MONTH,
            )],
            metrics=[QueryMetric(metric="count(orders.*)"), QueryMetric(metric="sum(orders.total)")],
        )
        saved = await sqlserver_cross_dataset_env.create_dataset_from_query(query=source, name="ss_monthly")
        assert saved.source_queries is not None
        result = await sqlserver_cross_dataset_env.execute(
            query=SlayerQuery(population="ss_monthly", metrics=[QueryMetric(metric="count(ss_monthly.*)")])
        )
        assert result.data[0]["ss_monthly._count"] == 3

    async def test_sql_dimension(self, sqlserver_cross_dataset_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="CASE WHEN orders.amount > 100 THEN 'high' ELSE 'low' END", alias="tier")],
            metrics=[QueryMetric(metric="count(orders.*)")],
        )
        result = await sqlserver_cross_dataset_env.execute(query=query)
        by_tier = {r["tier"]: r["orders._count"] for r in result.data}
        assert by_tier["high"] == 3
        assert by_tier["low"] == 3


# ---------------------------------------------------------------------------
# Rollup ingestion (SQL Server FK metadata IS exposed via Inspector)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sqlserver_ingest_env(sqlserver_container) -> Iterator[tuple[list[SlayerDataset], DatasourceConfig, None]]:
    """Set up tables with FK relationships and ingest."""
    db_name = _create_module_db(sqlserver_container)
    try:
        with disposable_engine(_db_url(sqlserver_container, db_name)) as engine:
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
                conn.execute(sa.text(
                    "EXEC sp_addextendedproperty 'MS_Description', 'All orders', "
                    "'SCHEMA', 'dbo', 'TABLE', 'orders'"
                ))
                conn.execute(sa.text(
                    "EXEC sp_addextendedproperty 'MS_Description', 'Order amount', "
                    "'SCHEMA', 'dbo', 'TABLE', 'orders', 'COLUMN', 'amount'"
                ))

        ds = _ds_config(sqlserver_container, db_name)
        models = run_sync(_ingest(ds, schema=None))
        yield models, ds, None
    finally:
        _drop_module_db(sqlserver_container, db_name)


@pytest.mark.integration
class TestSQLServerIngestComments:
    def test_table_and_column_comments_imported(self, sqlserver_ingest_env: tuple[list[SlayerDataset], DatasourceConfig, None]) -> None:
        models, _, _ = sqlserver_ingest_env
        orders = next(m for m in models if m.name == "orders")
        assert orders.description == "All orders"
        by_name = {c.name: c for c in orders.fields}
        assert by_name["amount"].description == "Order amount"
        assert by_name["id"].description is None

    def test_uncommented_table_has_no_description(self, sqlserver_ingest_env: tuple[list[SlayerDataset], DatasourceConfig, None]) -> None:
        models, _, _ = sqlserver_ingest_env
        regions = next(m for m in models if m.name == "regions")
        assert regions.description is None


@pytest.mark.integration
class TestRollupIngestionSQLServer:
    def test_orders_has_own_columns_only(self, sqlserver_ingest_env: tuple[list[SlayerDataset], DatasourceConfig, None]) -> None:
        models, _, _ = sqlserver_ingest_env
        orders = next(m for m in models if m.name == "orders")
        col_names = [c.name for c in orders.fields]
        assert "id" in col_names
        assert "customer_id" in col_names
        assert "amount" in col_names
        assert not any("." in name for name in col_names)

    def test_orders_uses_sql_table_with_joins(self, sqlserver_ingest_env: tuple[list[SlayerDataset], DatasourceConfig, None]) -> None:
        models, _, _ = sqlserver_ingest_env
        orders = next(m for m in models if m.name == "orders")
        assert orders.sql_table is not None
        assert orders.sql is None
        assert len(orders.relationships) > 0

    def test_regions_has_no_rollup(self, sqlserver_ingest_env: tuple[list[SlayerDataset], DatasourceConfig, None]) -> None:
        models, _, _ = sqlserver_ingest_env
        regions = next(m for m in models if m.name == "regions")
        assert regions.sql_table is not None
        assert regions.sql is None

    async def test_rollup_query_group_by_customer(self, sqlserver_ingest_env: tuple[list[SlayerDataset], DatasourceConfig, None]) -> None:
        models, ds, _ = sqlserver_ingest_env
        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(ds)
        for m in models:
            await storage.save_dataset(m)
        engine = SlayerQueryEngine(storage=storage)
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

    async def test_rollup_query_group_by_region(self, sqlserver_ingest_env: tuple[list[SlayerDataset], DatasourceConfig, None]) -> None:
        models, ds, _ = sqlserver_ingest_env
        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(ds)
        for m in models:
            await storage.save_dataset(m)
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}, {"metric": "sum(orders.amount)"}],
            "dimensions": ["customers.regions.name"],
        })
        result = await engine.execute(query=query)
        by_region = {r["customers.regions.name"]: r for r in result.data}
        assert by_region["US"]["orders._count"] == 3
        assert by_region["EU"]["orders._count"] == 1
        assert float(by_region["US"]["orders.amount_sum"]) == 450.0  # NOSONAR(S1244) — sum of integer cents, exact-representable
        assert float(by_region["EU"]["orders.amount_sum"]) == 50.0  # NOSONAR(S1244) — sum of integer cents, exact-representable

    def test_orders_has_no_named_measures_after_ingest(self, sqlserver_ingest_env: tuple[list[SlayerDataset], DatasourceConfig, None]) -> None:
        models, _, _ = sqlserver_ingest_env
        orders = next(m for m in models if m.name == "orders")
        assert orders.metrics == []
        col_names = [c.name for c in orders.fields]
        assert "amount" in col_names

    # NOTE: ``test_dotted_dimension_single_hop`` is intentionally not ported —
    # its body would be identical to ``test_rollup_query_group_by_customer``
    # (Sonar python:S4144). Single-hop path is already covered there; the
    # multi-hop variant below tests the part that's genuinely different.

    async def test_dotted_dimension_multi_hop(self, sqlserver_ingest_env: tuple[list[SlayerDataset], DatasourceConfig, None]) -> None:
        models, ds, _ = sqlserver_ingest_env
        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(ds)
        for m in models:
            await storage.save_dataset(m)
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "dimensions": ["customers.regions.name"],
        })
        result = await engine.execute(query=query)
        by_region = {r["customers.regions.name"]: r["orders._count"] for r in result.data}
        assert by_region["US"] == 3
        assert by_region["EU"] == 1

    async def test_selective_joins_no_joined_dims(self, sqlserver_ingest_env: tuple[list[SlayerDataset], DatasourceConfig, None]) -> None:
        models, ds, _ = sqlserver_ingest_env
        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(ds)
        for m in models:
            await storage.save_dataset(m)
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "count(orders.*)"}]})
        result = await engine.execute(query=query)
        assert "LEFT JOIN" not in result.sql
        assert result.data[0]["orders._count"] == 4

    async def test_selective_joins_single_hop(self, sqlserver_ingest_env: tuple[list[SlayerDataset], DatasourceConfig, None]) -> None:
        models, ds, _ = sqlserver_ingest_env
        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(ds)
        for m in models:
            await storage.save_dataset(m)
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "dimensions": ["customers.name"],
        })
        result = await engine.execute(query=query)
        assert "LEFT JOIN" in result.sql
        assert "customers" in result.sql
        assert "regions" not in result.sql

    async def test_selective_joins_transitive(self, sqlserver_ingest_env: tuple[list[SlayerDataset], DatasourceConfig, None]) -> None:
        models, ds, _ = sqlserver_ingest_env
        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(ds)
        for m in models:
            await storage.save_dataset(m)
        engine = SlayerQueryEngine(storage=storage)
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "dimensions": ["customers.regions.name"],
        })
        result = await engine.execute(query=query)
        assert "customers" in result.sql
        assert "regions" in result.sql

    def test_orders_has_joins_metadata(self, sqlserver_ingest_env: tuple[list[SlayerDataset], DatasourceConfig, None]) -> None:
        models, _, _ = sqlserver_ingest_env
        orders = next(m for m in models if m.name == "orders")
        join_targets = [j.to for j in orders.relationships]
        assert "customers" in join_targets
        assert "regions" not in join_targets
        for j in orders.relationships:
            assert len(j.column_pairs) >= 1
            for pair in j.column_pairs:
                assert len(pair) == 2

    def test_regions_has_no_joins(self, sqlserver_ingest_env: tuple[list[SlayerDataset], DatasourceConfig, None]) -> None:
        models, _, _ = sqlserver_ingest_env
        regions = next(m for m in models if m.name == "regions")
        assert regions.relationships == []

    async def test_joins_serialize_to_yaml(self, sqlserver_ingest_env: tuple[list[SlayerDataset], DatasourceConfig, None]) -> None:
        models, _, _ = sqlserver_ingest_env
        orders = next(m for m in models if m.name == "orders")
        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_dataset(orders)
        loaded = await storage.get_dataset("orders")
        assert len(loaded.relationships) == len(orders.relationships)
        for orig, loaded_j in zip(orders.relationships, loaded.relationships):
            assert orig.to == loaded_j.to
            assert orig.column_pairs == loaded_j.column_pairs


# ---------------------------------------------------------------------------
# Exponent-form numeric text cast to DECIMAL
# ---------------------------------------------------------------------------


def _decimal_cast_sql(text: str, *, try_: bool) -> str:
    cast = TsqlDialect().build_decimal_cast(exp.Literal.string(text), exp.DataType.build("DECIMAL(10, 2)"),
                                            precision=10, scale=2, source=None, try_=try_)
    return f"SELECT {cast.sql(dialect='tsql')}"


@pytest.mark.integration
class TestSQLServerExponentText:
    """SQL Server reads ``''``, ``'+'`` and ``'-'`` as INT 0, so an exponent without digits must not reach that read."""

    @pytest.mark.parametrize("text", ["1e", "1e-", "1e+", "1e 1", "e1", "+-1e1"])
    def test_try_cast_of_malformed_exponent_text_is_null(self, sqlserver_container, text: str) -> None:
        with disposable_engine(_admin_url(sqlserver_container)) as engine, engine.connect() as conn:
            assert conn.exec_driver_sql(_decimal_cast_sql(text, try_=True)).scalar() is None

    @pytest.mark.parametrize("text", ["1e", "1e-"])
    def test_cast_of_malformed_exponent_text_fails(self, sqlserver_container, text: str) -> None:
        sql = _decimal_cast_sql(text, try_=False)
        with disposable_engine(_admin_url(sqlserver_container)) as engine, engine.connect() as conn:
            with pytest.raises(sqlalchemy.exc.DBAPIError):
                conn.exec_driver_sql(sql)

    @pytest.mark.parametrize(("text", "value"), [("1.2345e1", "12.35"), ("-1.5E-1", "-0.15"), ("12e+0", "12.00")])
    def test_exponent_text_casts_exactly(self, sqlserver_container, text: str, value: str) -> None:
        with disposable_engine(_admin_url(sqlserver_container)) as engine, engine.connect() as conn:
            assert conn.exec_driver_sql(_decimal_cast_sql(text, try_=False)).scalar() == Decimal(value)


# ---------------------------------------------------------------------------
# Median / percentile — a window per row
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestSQLServerMedianPercentile:
    async def test_median(self, sqlserver_env: SlayerQueryEngine) -> None:
        # amounts = [100, 200, 50, 150, 75, 300] -> median 125
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "median(orders.total)"}]})
        result = await sqlserver_env.execute(query=query)
        assert float(result.data[0]["orders.total_median"]) == pytest.approx(125.0)

    async def test_percentile_quartiles(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "percentile(orders.total, p=0.25)"}, {"metric": "percentile(orders.total, p=0.75)"}],
        })
        row = (await sqlserver_env.execute(query=query)).data[0]
        assert float(row["orders.total_percentile_p_0_25"]) == pytest.approx(81.25)
        assert float(row["orders.total_percentile_p_0_75"]) == pytest.approx(187.5)

    async def test_median_grouped(self, sqlserver_env: SlayerQueryEngine) -> None:
        # completed: [100, 150, 200] -> 150; pending: [50, 300] -> 175; cancelled: [75] -> 75
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "median(orders.total)"}],
            "dimensions": ["orders.status"],
        })
        result = await sqlserver_env.execute(query=query)
        by_status = {r["orders.status"]: float(r["orders.total_median"]) for r in result.data}
        assert by_status == pytest.approx({"completed": 150, "pending": 175, "cancelled": 75})


# ---------------------------------------------------------------------------
# Statistical aggregations (cross-dialect parity)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestSQLServerStatAggregations:
    """T-SQL has native STDEV / STDEVP / VAR / VARP (TsqlDialect maps
    stddev_samp/_pop and var_samp/_pop via exp.Anonymous to these names).
    CORR / COVAR_* use the variance-decomposition formula with the T-SQL
    name set (VAR / VARP / STDEV)."""

    async def test_stddev_samp_native_sqlserver(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "stddev_samp(orders.total)"}]})
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
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "stddev_pop(orders.total)"}],
        })
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
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "var_samp(orders.total)"}]})
        result = await sqlserver_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_var_samp"]) == pytest.approx(
            statistics.variance(amounts), rel=1e-9
        )

        dry = await sqlserver_env.execute(query=query, dry_run=True)
        assert dry.sql is not None
        sql_upper = dry.sql.upper()
        assert "VAR(" in sql_upper, (
            f"T-SQL var_samp must emit canonical VAR(. Got:\n{dry.sql}"
        )
        assert "VAR_SAMP(" not in sql_upper, (
            f"T-SQL var_samp must not emit VAR_SAMP(. Got:\n{dry.sql}"
        )

    async def test_var_pop_uses_canonical_tsql_name(self, sqlserver_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            population="orders",
            metrics=[{"metric": "var_pop(orders.total)"}],
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
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "corr(orders.total, other=orders.customer_id)"}],
        })
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
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "covar_samp(orders.total, other=orders.customer_id)"}],
        })
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
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "covar_pop(orders.total, other=orders.customer_id)"}],
        })
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
# log10 round-trip (T-SQL has native LOG10)
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlserver_log10_env(sqlserver_container):
    db_name = _create_module_db(sqlserver_container)
    with disposable_engine(_db_url(sqlserver_container, db_name)) as engine:
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

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(sqlserver_container, db_name)))
    run_sync(storage.save_dataset(SlayerDataset(
        name="orders", sql_table="orders", data_source="testmssql",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
            DatasetField(name="log_amount", expression=ExpressionObject(dialects=[DialectEntry(dialect="TSQL", expression="log10(amount)")]), datatype=DataType.FLOAT),
        ],
    )))
    yield SlayerQueryEngine(storage=storage)
    _drop_module_db(sqlserver_container, db_name)


@pytest.mark.integration
async def test_log10_round_trip_sqlserver(sqlserver_log10_env: SlayerQueryEngine) -> None:
    """T-SQL has native LOG10 (TsqlDialect.log10_native = True). The emitted
    SQL must preserve ``LOG10(...)``."""
    result = await sqlserver_log10_env.execute(
        SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "max(orders.log_amount)"}]})
    )
    assert float(result.data[0]["orders.log_amount_max"]) == pytest.approx(
        _math.log10(300.0), rel=1e-9
    )

    dry = await sqlserver_log10_env.execute(
        SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "max(orders.log_amount)"}]}),
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
    query = SlayerQuery.model_validate({
        "population": "orders",
        "metrics": [{"metric": "count(orders.*)"}],
        "time_dimensions": [{"field": "orders.created_at", "granularity": "week"}],
    })
    dry = await sqlserver_env.execute(query=query, dry_run=True)
    assert dry.sql is not None
    sql_lower = dry.sql.lower()
    assert "datetrunc(iso_week" in sql_lower, (
        f"T-SQL week truncation must use DATETRUNC(iso_week, ...). Got:\n{dry.sql}"
    )


@pytest.mark.integration
async def test_sqlserver_time_shift_uses_dateadd(sqlserver_env: SlayerQueryEngine) -> None:
    """TsqlDialect.build_date_add emits ``DATEADD(MONTH, -1, col)``
    instead of ``col - INTERVAL 1 MONTH``. INTERVAL is not valid T-SQL."""
    query = SlayerQuery(
        population="orders",
        time_dimensions=[TimeDimension(
            field="orders.created_at", granularity=TimeGranularity.MONTH,
        )],
        metrics=[
            QueryMetric(metric="sum(orders.total)"),
            QueryMetric(metric="time_shift(sum(orders.total), -1, 'month')", alias="prev"),
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
    assert not _re.search(r"\bINTERVAL\b", sql_upper), (
        f"T-SQL must not emit INTERVAL keyword. Got:\n{dry.sql}"
    )


# ---------------------------------------------------------------------------
# Window-in-filter raises
# ---------------------------------------------------------------------------


@pytest.fixture
def planets_sqlserver_env(sqlserver_container):
    db_name = _create_module_db(sqlserver_container)
    with disposable_engine(_db_url(sqlserver_container, db_name)) as engine:
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

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(sqlserver_container, db_name)))
    run_sync(storage.save_dataset(
        SlayerDataset(
            name="planets",
            sql_table="planets",
            data_source="testmssql",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="name", expression="name", datatype=DataType.STRING),
                DatasetField(name="mass", expression="mass", datatype=DataType.FLOAT),
                DatasetField(
                    name="rn",
                    expression=ExpressionObject(dialects=[DialectEntry(dialect="TSQL", expression="row_number() over (order by mass desc)")]),
                    datatype=DataType.FLOAT,
                ),
            ],
        )
    ))
    yield SlayerQueryEngine(storage=storage)
    _drop_module_db(sqlserver_container, db_name)


@pytest.mark.integration
async def test_filter_on_windowed_column_sqlserver(planets_sqlserver_env) -> None:
    """A filter on a windowed field applies outside the materialised window."""
    engine = planets_sqlserver_env
    query = SlayerQuery(
        population="planets",
        dimensions=["planets.name"],
        filter=["planets.rn <= 3"],
    )
    result = await engine.execute(query)
    assert {r["planets.name"] for r in result.data} == {"Jupiter", "Saturn", "Neptune"}


# ---------------------------------------------------------------------------
# Cross-dataset derived DatasetField.sql
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlserver_derived_chain_env(sqlserver_container):
    db_name = _create_module_db(sqlserver_container)
    with disposable_engine(_db_url(sqlserver_container, db_name)) as engine:
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

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(sqlserver_container, db_name)))
    run_sync(storage.save_dataset(
        SlayerDataset(
            name="b_tbl",
            data_source="testmssql",
            sql_table="b_tbl",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="foo_raw", expression="foo_raw", datatype=DataType.FLOAT),
                DatasetField(name="foo_normalized", expression=ExpressionObject(dialects=[DialectEntry(dialect="TSQL", expression="foo_raw / 100.0")]), datatype=DataType.FLOAT),
            ],
        )
    ))
    run_sync(storage.save_dataset(
        SlayerDataset(
            name="a_tbl",
            data_source="testmssql",
            sql_table="a_tbl",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="bar", expression="bar", datatype=DataType.FLOAT),
                DatasetField(name="b_id", expression="b_id", datatype=DataType.FLOAT),
                DatasetField(name="raw_a", expression="raw_a", datatype=DataType.FLOAT),
                DatasetField(
                    name="ratio_using_derived",
                    expression=ExpressionObject(dialects=[DialectEntry(dialect="TSQL", expression="a_tbl.bar / b_tbl.foo_normalized")]),
                    datatype=DataType.FLOAT,
                ),
            ],
            relationships=[DatasetRelationship(to="b_tbl", from_columns=["b_id"], to_columns=["id"])],
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


# ---------------------------------------------------------------------------
# Mode-B date functions (the shared oracle matrix + spec scenarios)
# ---------------------------------------------------------------------------

_DATE_CASES = matrix_cases()


@pytest.fixture(scope="module")
def _sqlserver_dates_storage(sqlserver_container, tmp_path_factory):
    db_name = _create_module_db(sqlserver_container)
    try:
        with disposable_engine(_db_url(sqlserver_container, db_name)) as engine:
            with engine.begin() as conn:
                for stmt in server_seed_statements("tsql", today=datetime.now(timezone.utc).date()):
                    conn.execute(sa.text(stmt))
        storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp("sqlserver_dates")))
        run_sync(storage.save_datasource(_ds_config(sqlserver_container, db_name)))
        for model in all_datasets(data_source="testmssql"):
            run_sync(storage.save_dataset(model))
        yield storage
    finally:
        _drop_module_db(sqlserver_container, db_name)


@pytest.fixture
def sqlserver_dates(_sqlserver_dates_storage) -> SlayerQueryEngine:
    return SlayerQueryEngine(storage=_sqlserver_dates_storage)


def _set_sa_language(sqlserver_container, language: str) -> None:
    """Change the login's default language (and so its DATEFIRST); drop pooled connections."""
    with disposable_engine(_admin_url(sqlserver_container), isolation_level="AUTOCOMMIT") as engine:
        with engine.connect() as conn:
            conn.execute(sa.text(f"ALTER LOGIN [sa] WITH DEFAULT_LANGUAGE = [{language}]"))
    for cached in engine_factory._engine_cache.values():
        cached.dispose()
    engine_factory.reset_cache()


@pytest.mark.integration
class TestSQLServerDateFunctions:
    @pytest.mark.parametrize("case", _DATE_CASES, ids=[c.case_id for c in _DATE_CASES])
    async def test_matrix(self, sqlserver_dates: SlayerQueryEngine, case: DateCase) -> None:
        assert_case((await sqlserver_dates.execute(matrix_query(case))).data, case)

    async def test_scenarios(self, sqlserver_dates: SlayerQueryEngine) -> None:
        await check_server_scenarios(sqlserver_dates)

    async def test_weekday_independent_of_datefirst(self, sqlserver_container, _sqlserver_dates_storage) -> None:
        case = DateCase(
            case_id="part|day_of_week|d1", expr="date_part('day_of_week', dt.d1)",
            expected={row[0]: oracle_part("day_of_week", parse_temporal(row[1])) for row in DT.rows},
        )
        try:
            for language in ("us_english", "British"):  # DATEFIRST 7, then 1
                _set_sa_language(sqlserver_container, language)
                engine = SlayerQueryEngine(storage=_sqlserver_dates_storage)
                assert_case((await engine.execute(matrix_query(case))).data, case)
        finally:
            _set_sa_language(sqlserver_container, "us_english")


@pytest.mark.integration
@pytest.mark.parametrize("fn", ["substring", "substr"])
async def test_two_arg_substring_executes(sqlserver_env: SlayerQueryEngine, fn: str) -> None:
    result = await sqlserver_env.execute(query=SlayerQuery.model_validate({
        "population": "orders", "metrics": [{"metric": "count(orders.*)", "alias": "n"}],
        "filter": [f"{fn}(orders.status, 2) == 'ending'"],
    }))
    assert result.data[0]["n"] == 2


@pytest.mark.integration
@pytest.mark.parametrize("fn", ["substring", "substr"])
async def test_two_arg_substring_keeps_trailing_spaces(sqlserver_env: SlayerQueryEngine, fn: str) -> None:
    # T-SQL '=' pads trailing spaces, so a trailing 'x' makes them count.
    async def count(filters: list[str]) -> int:
        result = await sqlserver_env.execute(query=SlayerQuery.model_validate({
            "population": "orders", "metrics": [{"metric": "count(orders.*)", "alias": "n"}], "filter": filters,
        }))
        return result.data[0]["n"]

    kept = await count([f"concat({fn}(concat(orders.status, '  '), 1), 'x') == concat(orders.status, '  x')"])
    assert kept == await count([])


@pytest.mark.integration
async def test_length_counts_trailing_spaces(sqlserver_env: SlayerQueryEngine) -> None:
    result = await sqlserver_env.execute(query=SlayerQuery.model_validate({
        "population": "orders", "metrics": [{"metric": "count(orders.*)", "alias": "n"}],
        "filter": ["length(concat(orders.status, '  ')) != length(orders.status) + 2"],
    }))
    assert result.data[0]["n"] == 0


@pytest.mark.integration
async def test_length_of_a_number(sqlserver_env: SlayerQueryEngine) -> None:
    async def count(filters: list[str]) -> int:
        result = await sqlserver_env.execute(query=SlayerQuery.model_validate({
            "population": "orders", "metrics": [{"metric": "count(orders.*)", "alias": "n"}], "filter": filters,
        }))
        return result.data[0]["n"]

    assert await count(["length(orders.id) >= 1"]) == await count([])


# ---------------------------------------------------------------------------
# Time spine and custom granularities
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def _sqlserver_spine_storage(sqlserver_container, tmp_path_factory):
    db_name = _create_module_db(sqlserver_container)
    try:
        with disposable_engine(_db_url(sqlserver_container, db_name)) as engine:
            with engine.begin() as conn:
                for stmt in server_statements("tsql"):
                    conn.execute(sa.text(stmt))
        storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp("sqlserver_spine")))
        base = _ds_config(sqlserver_container, db_name)
        for name, models in (("ms", server_datasets(data_source="ms")), ("ms_ts", server_datasets_ts(data_source="ms_ts"))):
            run_sync(storage.save_datasource(with_granularities(base, name=name)))
            for model in models:
                run_sync(storage.save_dataset(model))
        yield storage
    finally:
        _drop_module_db(sqlserver_container, db_name)


@pytest.mark.integration
class TestSQLServerTimeSpine:
    async def test_scenarios(self, _sqlserver_spine_storage) -> None:
        await check_all(SlayerQueryEngine(storage=_sqlserver_spine_storage), data_source="ms", ts_data_source="ms_ts")


# ---------------------------------------------------------------------------
# One logical bucket type; a DATE meeting a TIMESTAMP reads as midnight
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def _sqlserver_logical_types_storage(sqlserver_container, tmp_path_factory):
    db_name = _create_module_db(sqlserver_container)
    try:
        with disposable_engine(_db_url(sqlserver_container, db_name)) as engine:
            with engine.begin() as conn:
                for stmt in logical_type_statements("tsql"):
                    conn.execute(sa.text(stmt))
        storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp("sqlserver_logical_types")))
        base = _ds_config(sqlserver_container, db_name)
        run_sync(storage.save_datasource(logical_type_granularities(base, name="ms_lt")))
        for model in logical_type_datasets(data_source="ms_lt"):
            run_sync(storage.save_dataset(model))
        yield storage
    finally:
        _drop_module_db(sqlserver_container, db_name)


@pytest.mark.integration
class TestSQLServerLogicalTypes:
    async def test_bucket_matrix(self, _sqlserver_logical_types_storage) -> None:
        await check_bucket_matrix(SlayerQueryEngine(storage=_sqlserver_logical_types_storage), backend="tsql", data_source="ms_lt")

    async def test_mixed_date_timestamp(self, _sqlserver_logical_types_storage) -> None:
        await check_mixed(SlayerQueryEngine(storage=_sqlserver_logical_types_storage), backend="tsql", data_source="ms_lt")


# ---------------------------------------------------------------------------
# Boolean aggregation inputs and predicate values (BIT)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def _sqlserver_boolean_storage(sqlserver_container, tmp_path_factory):
    db_name = _create_module_db(sqlserver_container)
    try:
        with disposable_engine(_db_url(sqlserver_container, db_name)) as engine:
            with engine.begin() as conn:
                for stmt in dev2046_seed_statements("tsql"):
                    conn.execute(sa.text(stmt))
        storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp("sqlserver_boolean")))
        run_sync(storage.save_datasource(_ds_config(sqlserver_container, db_name)))
        for model in dev2046_datasets(data_source="testmssql"):
            run_sync(storage.save_dataset(model))
        yield storage
    finally:
        _drop_module_db(sqlserver_container, db_name)


@pytest.mark.integration
class TestSQLServerBooleanAggregation:
    async def test_bit_column_aggregates(self, _sqlserver_boolean_storage) -> None:
        engine = SlayerQueryEngine(storage=_sqlserver_boolean_storage)
        resp = await engine.execute(orders_q(metrics=[
            bool_metric("sum(orders.flag)", "s"), bool_metric("avg(orders.flag)", "a"), bool_metric("min(orders.flag)", "mn"), bool_metric("max(orders.flag)", "mx"),
            bool_metric("count(orders.flag)", "c"),
        ]))
        row = resp.data[0]
        assert row["s"] == FLAG_TOTALS["sum"]
        assert float(row["a"]) == pytest.approx(FLAG_TOTALS["avg"])
        assert bool(row["mn"]) is FLAG_TOTALS["min"]
        assert bool(row["mx"]) is FLAG_TOTALS["max"]
        assert row["c"] == FLAG_TOTALS["count"]

    async def test_predicate_inputs(self, _sqlserver_boolean_storage) -> None:
        engine = SlayerQueryEngine(storage=_sqlserver_boolean_storage)
        resp = await engine.execute(orders_q(metrics=[
            bool_metric("sum(orders.amount > 15)", "s"), bool_metric("count(orders.amount > 15)", "c"), bool_metric("max(orders.amount > 15)", "mx"),
        ]))
        row = resp.data[0]
        assert row["s"] == GT15_TOTALS["sum"]
        assert row["c"] == GT15_TOTALS["count"]
        assert bool(row["mx"]) is True

    async def test_avg_is_fractional(self, _sqlserver_boolean_storage) -> None:
        """T-SQL's AVG over integers truncates; ``avg`` never does."""
        engine = SlayerQueryEngine(storage=_sqlserver_boolean_storage)
        resp = await engine.execute(orders_q(dimensions=["orders.region"], metrics=[bool_metric("avg(orders.customer_id)")]))
        assert {k: float(v) for k, v in by_dim(resp, "region").items()} == pytest.approx({"east": 400 / 3, "west": 200.0})
        resp = await engine.execute(orders_q(metrics=[bool_metric("avg(orders.amount > 15)", "a")]))
        assert float(resp.data[0]["a"]) == pytest.approx(GT15_TOTALS["avg"])

    async def test_projected_boolean_measure(self, _sqlserver_boolean_storage) -> None:
        """``sum(amount) > 50`` projects as a BIT value; NULL when the sum is NULL."""
        engine = SlayerQueryEngine(storage=_sqlserver_boolean_storage)
        resp = await engine.execute(orders_q(dimensions=["orders.region"], metrics=[bool_metric("sum(orders.amount) > 50")]))
        values = by_dim(resp, "region")
        assert values["west"] is None
        assert bool(values["east"]) is True

    @pytest.mark.parametrize("having", ["max(orders.flag) = true", "sum(orders.flag) > 1", "sum(orders.amount > 15) > 1"])
    async def test_post_aggregation_filters(self, _sqlserver_boolean_storage, having: str) -> None:
        engine = SlayerQueryEngine(storage=_sqlserver_boolean_storage)
        resp = await engine.execute(orders_q(
            dimensions=["orders.region"], metrics=[bool_metric("sum(orders.flag)")], filter=["orders.amount > 5 or orders.amount is None", having],
        ))
        assert by_dim(resp, "region") == {"east": 2}


@pytest.mark.integration
def test_every_sqlserver_alias_failure_is_quoted(sqlserver_container) -> None:
    db_name = _create_module_db(sqlserver_container)
    try:
        with disposable_engine(_db_url(sqlserver_container, db_name), isolation_level="AUTOCOMMIT") as engine:
            with engine.connect() as conn:
                conn.execute(sa.text("CREATE TABLE kw_probe (a INT)"))
            assert unquoted_alias_failures(
                dialect="tsql", words=keyword_universe(), table="kw_probe", column="a",
                fails=sa_statement_fails(engine), workers=8,
            ) == []
    finally:
        _drop_module_db(sqlserver_container, db_name)


# ---------------------------------------------------------------------------
# Mode-B SQL grammar (spec: queries/expression-grammar, queries/casts, queries/positions)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def _sqlserver_dev2101_storage(sqlserver_container, tmp_path_factory):
    db_name = _create_module_db(sqlserver_container)
    try:
        with disposable_engine(_db_url(sqlserver_container, db_name)) as engine:
            raw = engine.raw_connection()
            try:
                dev2101_seed_server(raw.cursor(), dialect="tsql")
                raw.commit()
            finally:
                raw.close()
        storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp("sqlserver_dev2101")))
        run_sync(storage.save_datasource(_ds_config(sqlserver_container, db_name)))
        for model in dev2101_datasets(data_source="testmssql"):
            run_sync(storage.save_dataset(model))
        yield storage
    finally:
        _drop_module_db(sqlserver_container, db_name)


@pytest.mark.integration
class TestSQLServerModeBGrammar:
    async def test_sql_forms(self, _sqlserver_dev2101_storage) -> None:
        await check_server_forms(SlayerQueryEngine(storage=_sqlserver_dev2101_storage))

    async def test_null_ordering_over_aliased_targets(self, _sqlserver_dev2101_storage) -> None:
        await check_null_ordering(SlayerQueryEngine(storage=_sqlserver_dev2101_storage))

    async def test_try_cast(self, _sqlserver_dev2101_storage) -> None:
        await check_try_cast(SlayerQueryEngine(storage=_sqlserver_dev2101_storage))

    async def test_try_cast_integer_targets(self, _sqlserver_dev2101_storage) -> None:
        await check_try_cast_targets(SlayerQueryEngine(storage=_sqlserver_dev2101_storage), dataset="orders")

    async def test_author_casts(self, _sqlserver_dev2101_storage) -> None:
        await check_author_casts(SlayerQueryEngine(storage=_sqlserver_dev2101_storage), dataset="orders")
        await check_boolean_casts(SlayerQueryEngine(storage=_sqlserver_dev2101_storage), dataset="orders")

    async def test_filtered_aggregations(self, _sqlserver_dev2101_storage) -> None:
        await check_filtered_aggregations(SlayerQueryEngine(storage=_sqlserver_dev2101_storage))


# ---------------------------------------------------------------------------
# Portable Mode-B library (spec: queries/scalar-functions, regex, mixed-type-coercion, string-matching)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def sqlserver_2025_container():
    """SQL Server 2025, the first release with the regex functions."""
    with SqlServerContainer("mcr.microsoft.com/mssql/server:2025-latest", password=_SA_PASSWORD) as c:
        yield c


def _dev2120_storage(container, tmp_path_factory, *, label: str):
    db_name = _create_module_db(container)
    try:
        with disposable_engine(_db_url(container, db_name)) as engine:
            with engine.begin() as conn:
                for stmt in dev2120_table_statements(LIB, dialect="tsql"):
                    conn.execute(sa.text(stmt))
        storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp(f"sqlserver_dev2120_{label}")))
        run_sync(storage.save_datasource(_ds_config(container, db_name)))
        run_sync(storage.save_dataset(lib_dataset(data_source="testmssql")))
        yield storage
    finally:
        _drop_module_db(container, db_name)


@pytest.fixture(scope="module")
def _sqlserver_2022_dev2120_storage(sqlserver_container, tmp_path_factory):
    yield from _dev2120_storage(sqlserver_container, tmp_path_factory, label="2022")


@pytest.fixture(scope="module")
def _sqlserver_2025_dev2120_storage(sqlserver_2025_container, tmp_path_factory):
    yield from _dev2120_storage(sqlserver_2025_container, tmp_path_factory, label="2025")


@pytest.mark.integration
class TestSQLServerPortableLibrary:
    async def test_library_on_2022_refuses_regex(self, _sqlserver_2022_dev2120_storage) -> None:
        await check_library(SlayerQueryEngine(storage=_sqlserver_2022_dev2120_storage), gated=REGEX_NAMES,
                            refusal="2025")

    async def test_library_on_2025(self, _sqlserver_2025_dev2120_storage) -> None:
        await check_library(SlayerQueryEngine(storage=_sqlserver_2025_dev2120_storage))


# ---------------------------------------------------------------------------
# Window functions (spec: queries/window-functions › Every database runs the required windows or refuses
# with a typed error)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def _sqlserver_windows_storage(sqlserver_container, tmp_path_factory):
    db_name = _create_module_db(sqlserver_container)
    try:
        with disposable_engine(_db_url(sqlserver_container, db_name)) as engine:
            with engine.begin() as conn:
                for stmt in window_statements("tsql"):
                    conn.execute(sa.text(stmt))
        storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp("sqlserver_windows")))
        run_sync(storage.save_datasource(_ds_config(sqlserver_container, db_name)))
        for model in window_datasets(data_source="testmssql"):
            run_sync(storage.save_dataset(model))
        yield storage
    finally:
        _drop_module_db(sqlserver_container, db_name)


@pytest.fixture
def sqlserver_windows(_sqlserver_windows_storage) -> SlayerQueryEngine:
    return SlayerQueryEngine(storage=_sqlserver_windows_storage)


@pytest.mark.integration
class TestSQLServerWindows:
    @pytest.mark.parametrize("case", WINDOW_CASES, ids=WINDOW_CASE_IDS)
    async def test_the_matrix(self, sqlserver_windows: SlayerQueryEngine, case: WindowCase) -> None:
        await check_case(sqlserver_windows, case)


# ---------------------------------------------------------------------------
# JSON scalars over 4,000 characters and long split_part fields (spec: queries/json › Long scalar on SQL Server)
# ---------------------------------------------------------------------------

_LONG = "x" * 5000
_LONG_FIELD = "y" * 5000
_JDOCS: dict[int, tuple[str, str | None]] = {
    1: (f'{{"a": "{_LONG}"}}', f"{_LONG_FIELD}/tail"),
    2: ('{"a": "short"}', "a/b"),
    3: ('{"a": {"k": 1}}', None),
    4: ('{"a": [1]}', None),
    5: ('{"a": null}', None),
    6: ("{}", None),
    7: ("not json", None),
    8: ('{"b": {"c": "nested"}, "q k": "quoted", "n": 42}', None),
    9: ('"rootscalar"', None),
}
_SHORT_IDS = sorted(set(_JDOCS) - {1})


def _jdocs_storage(container, tmp_path_factory, *, label: str):
    db_name = _create_module_db(container)
    try:
        with disposable_engine(_db_url(container, db_name)) as engine:
            with engine.begin() as conn:
                conn.execute(sa.text("CREATE TABLE jdocs (id INT PRIMARY KEY, payload NVARCHAR(MAX), "
                                     "txt NVARCHAR(MAX))"))
                for rid, (payload, txt) in _JDOCS.items():
                    conn.execute(sa.text("INSERT INTO jdocs VALUES (:id, :payload, :txt)"),
                                 {"id": rid, "payload": payload, "txt": txt})
        storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp(f"sqlserver_jdocs_{label}")))
        run_sync(storage.save_datasource(_ds_config(container, db_name)))
        run_sync(storage.save_dataset(SlayerDataset(name="jdocs", sql_table="jdocs", data_source="testmssql", fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True), DatasetField(name="payload", datatype=DataType.STRING),
            DatasetField(name="txt", datatype=DataType.STRING),
        ])))
        yield storage
    finally:
        _drop_module_db(container, db_name)


@pytest.fixture(scope="module", params=["2022", "2025"])
def _jdocs(request, tmp_path_factory):
    container = request.getfixturevalue("sqlserver_container" if request.param == "2022" else "sqlserver_2025_container")
    yield from _jdocs_storage(container, tmp_path_factory, label=request.param)


def _jq(*dims: Any, ids: Optional[list[int]] = None, **body: Any) -> SlayerQuery:
    filters = [f"jdocs.id IN ({', '.join(map(str, ids))})"] if ids else []
    filters += body.pop("filter", [])
    return SlayerQuery.model_validate({"population": "jdocs", "dimensions": list(dims), "filter": filters, **body})


def _per_id(resp, name: str = "v") -> dict[int, Any]:
    return {int(r["jdocs.id"]): r[name] for r in resp.data}


def _v(expr: str) -> dict[str, str]:
    return {"field": expr, "alias": "v"}


@pytest.mark.integration
class TestSQLServerLongJsonScalars:
    @pytest.mark.parametrize("expr", ["json_value(jdocs.payload, '$.a')", "jdocs.payload ->> 'a'",
                                      "json_value(jdocs.payload, '$.a' RETURNING TEXT)"])
    async def test_long_scalar_fails_loud(self, _jdocs, expr: str) -> None:
        engine, query = SlayerQueryEngine(storage=_jdocs), _jq("jdocs.id", _v(expr))
        with pytest.raises(Exception, match="(?i)truncat"):  # noqa: B017 — SQL Server's own error (13625)
            await engine.execute(query)

    async def test_long_scalar_fails_loud_grouped(self, _jdocs) -> None:
        query = _jq(_v("json_value(jdocs.payload, '$.a')"), metrics=[{"metric": "count(jdocs.*)", "alias": "n"}])
        engine = SlayerQueryEngine(storage=_jdocs)
        with pytest.raises(Exception, match="(?i)truncat"):  # noqa: B017 — SQL Server's own error (13625)
            await engine.execute(query)

    async def test_short_missing_null_object_array_malformed(self, _jdocs) -> None:
        resp = await SlayerQueryEngine(storage=_jdocs).execute(_jq("jdocs.id", _v("json_value(jdocs.payload, '$.a')"), ids=_SHORT_IDS))
        assert _per_id(resp) == {rid: "short" if rid == 2 else None for rid in _SHORT_IDS}, resp.sql

    async def test_grouped_having_ordered(self, _jdocs) -> None:
        query = _jq(_v("json_value(jdocs.payload, '$.a')"), ids=_SHORT_IDS, metrics=[{"metric": "count(jdocs.*)", "alias": "n"}],
                    filter=["n >= 1"], order=[{"field": "v"}])
        resp = await SlayerQueryEngine(storage=_jdocs).execute(query)
        assert {r["v"]: int(r["n"]) for r in resp.data} == {"short": 1, None: len(_SHORT_IDS) - 1}, resp.sql

    @pytest.mark.parametrize(("expr", "expected"), [
        ("json_value(jdocs.payload, '$.b.c')", {8: "nested"}),
        ("json_value(jdocs.payload, '$.\"q k\"')", {8: "quoted"}),
        ("json_value(jdocs.payload, '$')", {9: "rootscalar"}),
        ("json_value(jdocs.payload, '$.n' RETURNING INT)", {8: 42}),
    ])
    async def test_paths(self, _jdocs, expr: str, expected: dict[int, Any]) -> None:
        resp = await SlayerQueryEngine(storage=_jdocs).execute(_jq("jdocs.id", _v(expr)))
        assert _per_id(resp) == {rid: expected.get(rid) for rid in _JDOCS}, resp.sql

    @pytest.mark.parametrize(("expr", "expected"), [
        ("split_part(jdocs.txt, '/', 1)", {1: _LONG_FIELD, 2: "a"}),
        ("split_part(jdocs.txt, '/', -1)", {1: "tail", 2: "b"}),
        ("split_part(jdocs.txt, '/', 2)", {1: "tail", 2: "b"}),
        ("split_part(jdocs.txt, '/', 3)", {1: "", 2: ""}),
    ])
    async def test_split_part_has_no_length_limit(self, _jdocs, expr: str, expected: dict[int, Any]) -> None:
        resp = await SlayerQueryEngine(storage=_jdocs).execute(_jq("jdocs.id", _v(expr)))
        assert _per_id(resp) == {rid: expected.get(rid) for rid in _JDOCS}, resp.sql


@pytest.mark.integration
def test_typed_probe_reports_declared_type_names(sqlserver_container) -> None:
    db_name = _create_module_db(sqlserver_container)
    try:
        with disposable_engine(_db_url(sqlserver_container, db_name)) as engine, engine.connect() as conn:
            conn.execute(sa.text("CREATE TABLE typed (i int, m decimal(10,2), t datetime2, s nvarchar(50))"))
            conn.commit()
        client = SlayerSQLClient(datasource=_ds_config(sqlserver_container, db_name))
        try:
            types = run_sync(client.get_result_column_types("SELECT i, m, t, s FROM typed"))
        finally:
            run_sync(client.aclose())
            client.close()
    finally:
        _drop_module_db(sqlserver_container, db_name)
    assert {name: (column.type, column.db_type) for name, column in types.items()} == {
        "i": (DataType.INTEGER, "int"),
        "m": (DataType.FLOAT, "decimal(10,2)"),
        "t": (DataType.DATETIME, "datetime2(7)"),
        "s": (DataType.STRING, "nvarchar(50)"),
    }


@pytest.mark.integration
async def test_money_columns_are_exact(sqlserver_container, tmp_path) -> None:
    db_name = _create_module_db(sqlserver_container)
    try:
        with disposable_engine(_db_url(sqlserver_container, db_name)) as engine, engine.begin() as conn:
            conn.execute(sa.text("CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount MONEY, small SMALLMONEY)"))
            conn.execute(sa.text("INSERT INTO ledger VALUES (1, 0.10, 0.10), (2, 0.20, 0.20)"))
        ds = _ds_config(sqlserver_container, db_name)
        (ledger,) = [m for m in await _ingest(ds, schema=None) if m.name == "ledger"]
        client = SlayerSQLClient(datasource=ds)
        try:
            probed = await client.get_result_column_types("SELECT amount, small FROM ledger")
        finally:
            await client.aclose()
            client.close()
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(ds)
        await storage.save_dataset(ledger.model_copy(update={"fields": [
            *ledger.fields,
            DatasetField(name="doubled", expression="amount * 2", datatype=DataType.FLOAT, db_type="money"),
            DatasetField(name="small_doubled", expression=ExpressionObject(dialects=[DialectEntry(dialect="TSQL", expression="small * 2")]), datatype=DataType.FLOAT, db_type="smallmoney"),
        ]}))
        engine = SlayerQueryEngine(storage=storage)
        sums = SlayerQuery.model_validate({"population": "ledger", "metrics": ["sum(ledger.amount)", "sum(ledger.small)"]})
        rows = SlayerQuery.model_validate({"population": "ledger", "dimensions": ["ledger.id", "ledger.doubled", "ledger.small_doubled"]})
        sql = [(await engine.execute(query=q, dry_run=True)).sql or "" for q in (sums, rows)]
        totals, doubled = await engine.execute(query=sums), await engine.execute(query=rows)
    finally:
        _drop_module_db(sqlserver_container, db_name)
    assert {c.name: (c.db_type or "").lower() for c in ledger.fields if c.name in ("amount", "small")} == {
        "amount": "money", "small": "smallmoney"}
    assert {name: t.lossy_db_type for name, t in probed.items()} == {"amount": "money", "small": "smallmoney"}
    assert all("CAST(" not in s.upper() for s in sql), sql
    assert totals.data == [{"ledger.amount_sum": Decimal("0.3000"), "ledger.small_sum": Decimal("0.3000")}]
    assert {r["ledger.id"]: (r["ledger.doubled"], r["ledger.small_doubled"]) for r in doubled.data} == {
        1: (Decimal("0.2000"), Decimal("0.2000")), 2: (Decimal("0.4000"), Decimal("0.4000"))}


@pytest.fixture
async def sqlserver_one_connection(_sqlserver_env_storage):
    """An engine whose client runs over a caller engine pooling exactly one connection."""
    ds = await _sqlserver_env_storage.get_datasource("testmssql")
    assert ds is not None
    with disposable_engine(ds.get_connection_string(), pool_size=1, max_overflow=0) as sa_engine:
        clients: list[SlayerSQLClient] = []

        def factory(datasource: DatasourceConfig) -> SlayerSQLClient:
            clients.append(SlayerSQLClient(datasource=datasource, sa_engine=sa_engine))
            return clients[-1]

        engine = SlayerQueryEngine(storage=_sqlserver_env_storage, client_factory=factory)
        try:
            yield engine, clients
        finally:
            await engine.aclose()
            engine.close()


_EXPLAINED = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "count(orders.*)"}]})


@pytest.mark.integration
class TestSQLServerExplain:
    async def test_explain_returns_the_plan_and_restores_the_session(self, sqlserver_one_connection) -> None:
        engine, _ = sqlserver_one_connection
        dry = await engine.execute(query=_EXPLAINED, dry_run=True)
        explained = await engine.execute(query=_EXPLAINED, explain=True)
        assert explained.sql == dry.sql
        assert explained.data
        assert all("StmtText" in row for row in explained.data)
        after = await engine.execute(query=_EXPLAINED)
        assert after.data == [{"orders._count": 6}]

    async def test_a_failing_plan_statement_raises_unchanged_and_restores_the_session(
        self, sqlserver_one_connection,
    ) -> None:
        engine, clients = sqlserver_one_connection
        await engine.execute(query=_EXPLAINED)
        statements = TsqlDialect().explain_statements("SELECT * FROM no_such_table")
        with pytest.raises(sqlalchemy.exc.ProgrammingError, match="no_such_table"):
            await clients[0].explain(statements)
        after = await engine.execute(query=_EXPLAINED)
        assert after.data == [{"orders._count": 6}]


# ---------------------------------------------------------------------------
# A warm statement costs one request
# ---------------------------------------------------------------------------


class _CountingCursor:
    """A pyodbc cursor logging every statement it sends."""

    def __init__(self, cursor: Any, log: list[str]) -> None:
        object.__setattr__(self, "_cursor", cursor)
        object.__setattr__(self, "_log", log)

    def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
        self._log.append(sql)
        return self._cursor.execute(sql, *args, **kwargs)

    def executemany(self, sql: str, *args: Any, **kwargs: Any) -> Any:
        self._log.append(sql)
        return self._cursor.executemany(sql, *args, **kwargs)

    def __iter__(self) -> Any:
        return iter(self._cursor)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(self._cursor, name, value)


class _CountingConnection:
    """A pyodbc connection logging its statements, and each commit/rollback that reaches the server."""

    def __init__(self, conn: Any, log: list[str]) -> None:
        object.__setattr__(self, "_conn", conn)
        object.__setattr__(self, "_log", log)

    def cursor(self, *args: Any, **kwargs: Any) -> _CountingCursor:
        return _CountingCursor(self._conn.cursor(*args, **kwargs), self._log)

    def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
        self._log.append(sql)
        return self._conn.execute(sql, *args, **kwargs)

    def _end(self, kind: str) -> None:
        # Under autocommit the driver ends no transaction, so nothing is sent.
        if not self._conn.autocommit:
            self._log.append(kind)
        getattr(self._conn, kind.lower())()

    def commit(self) -> None:
        self._end("COMMIT")

    def rollback(self) -> None:
        self._end("ROLLBACK")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(self._conn, name, value)


@pytest.mark.integration
@pytest.mark.parametrize("path", PATHS)
def test_warm_statement_costs_one_request(
    sqlserver_container, monkeypatch: pytest.MonkeyPatch, path: str,
) -> None:
    ds = _ds_config(sqlserver_container, "master")
    engine_factory.invalidate_engine(ds)
    sent: list[str] = []
    real_connect = pyodbc.connect
    monkeypatch.setattr(pyodbc, "connect", lambda *a, **kw: _CountingConnection(real_connect(*a, **kw), sent))
    try:
        run_three(SlayerSQLClient(datasource=ds), path=path, sql=lambda marker: f"SELECT '{marker}' AS m")
    finally:
        engine_factory.invalidate_engine(ds)
    assert_one_request_each(sent)
