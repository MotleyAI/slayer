"""Integration tests using a real ClickHouse database via testcontainers.

Mirror of test_integration_postgres.py focused on ClickHouse's
distinguishing characteristics:

* Parametric ``quantile(p)(x)`` syntax (pinned via dry_run SQL inspection).
* Native ``median(x)`` aggregate.
* Native ``stddev_*`` / ``var_*`` / ``corr`` / ``covar_*`` (sqlglot
  transpiles to the dialect-correct spellings).
* Native ``LOG10`` / ``LOG2``.
* ``DateTime`` columns must round-trip as ``DataType.DATETIME`` (auto-ingestion
  regression).

Rollup-ingestion tests are intentionally omitted: ClickHouse exposes no
foreign-key metadata via SQLAlchemy's Inspector. Cross-dataset coverage
uses hand-declared ``DatasetRelationship``s instead.

Skipped silently when:
- ``testcontainers[clickhouse]`` is not installed (importorskip)
- The Docker daemon is unreachable (autouse session fixture)
"""

import contextlib
import math as _math
import statistics
import tempfile
import uuid
from typing import Any
import warnings
from collections.abc import Generator
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest
import requests
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError

from slayer.async_utils import run_sync
from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import SlayerError
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    SlayerDataset,
)
from slayer.core.policy import JoinFilterRule, JoinFilterRuleset, SessionPolicy
from slayer.core.query import OrderItem, QueryDimension, QueryMetric, SlayerQuery, TimeDimension
from slayer.core.warnings import SlayerStatementTimeoutSkippedWarning, StatementTimeoutSkippedWarning
from slayer.live_schema import ingestion
from slayer.engine.query_engine import ClientFactory, SlayerQueryEngine
from slayer.database import engine_factory
from slayer.database.client import DelegatingSqlClient, ExecutionResult, SlayerSQLClient, SqlClient
from slayer.sql.dialects.base import ServerProfile
from slayer.storage.yaml_storage import YAMLStorage
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
from tests.integration._consecutive_periods_calendar import (
    CALENDAR_CASES,
    assert_calendar_streak,
    seed_statements,
    streak_dataset,
)

from tests._dev1737_fixtures import (
    DateCase,
    all_datasets,
    assert_case,
    check_server_scenarios,
    matrix_cases,
    matrix_query,
    server_seed_statements,
)
from tests._dev2120_fixtures import (
    DOCS_ROWS,
    LIB,
    check_library,
    check_native_json,
    contractors_dataset,
    contractors_statements,
    lib_dataset,
    server_literal,
    server_table_statements as dev2120_table_statements,
)
from tests._cast_fixtures import check_author_casts, check_boolean_casts, check_try_cast_targets
from tests._dev2137_fixtures import assignments_dataset, assignments_statements, check_lists
from tests._dev2000_fixtures import (
    DS as TZ_DS,
    MARKS as TZ_MARKS,
    NY as TZ_NY,
    ROWS as TZ_ROWS,
    amount as tz_amount,
    day as tz_day,
    instant_typing_queries,
    local as tz_local,
    marks_dataset as tz_marks_dataset,
    orders_dataset as tz_orders_dataset,
    orders_naive_instant_dataset,
    row_values,
)
from tests._engine_helpers import disposable_engine
from tests._list_mixed_numbers import check_mixed_numbers, mixed_numbers_dataset, mixed_numbers_statements
from tests._reserved_alias_probe import keyword_universe, sa_statement_fails, unquoted_alias_failures
from tests.integration import _dev2458_matrix as dev2458
from tests.integration._one_request import PATHS, assert_one_request_each, run_three

pytest.importorskip("testcontainers.clickhouse")
pytest.importorskip("clickhouse_sqlalchemy")

import docker  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above
from testcontainers.clickhouse import ClickHouseContainer  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above


# ---------------------------------------------------------------------------
# Docker availability
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def _docker_available_or_skip():
    try:
        docker.from_env().ping()
    except Exception as exc:  # pragma: no cover — exercised on local dev
        pytest.skip(f"Docker not available: {exc}")


# ---------------------------------------------------------------------------
# Container lifecycle
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def clickhouse_container():
    """Session-scoped ClickHouse 24 container."""
    # Access management lets tests create a readonly user.
    container = ClickHouseContainer("clickhouse/clickhouse-server:24-alpine").with_env(
        "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1"
    )
    with container as c:
        yield c


def _admin_url(clickhouse_container) -> str:
    """SQLAlchemy URL for admin (default DB) connections."""
    host = clickhouse_container.get_container_host_ip()
    port = int(clickhouse_container.get_exposed_port(8123))
    # testcontainers' ClickHouseContainer uses user=default / password=test
    # by default — go through the inspector helper rather than rely on the
    # specific defaults.
    user = getattr(clickhouse_container, "username", "default") or "default"
    password = getattr(clickhouse_container, "password", "") or ""
    auth = f"{user}:{password}@" if password else f"{user}@"
    return f"clickhouse+http://{auth}{host}:{port}/default"


def _ds_url_for_db(clickhouse_container, db_name: str) -> str:
    host = clickhouse_container.get_container_host_ip()
    port = int(clickhouse_container.get_exposed_port(8123))
    user = getattr(clickhouse_container, "username", "default") or "default"
    password = getattr(clickhouse_container, "password", "") or ""
    auth = f"{user}:{password}@" if password else f"{user}@"
    return f"clickhouse+http://{auth}{host}:{port}/{db_name}"


def _create_module_db(clickhouse_container) -> str:
    """Create a fresh per-module database; return its name."""
    db_name = f"test_{uuid.uuid4().hex[:12]}"
    with disposable_engine(_admin_url(clickhouse_container)) as engine:
        with engine.begin() as conn:
            conn.execute(sa.text(f"CREATE DATABASE {db_name}"))
    return db_name


def _drop_module_db(clickhouse_container, db_name: str) -> None:
    """Dispose cached SA engines, then drop the database."""
    for engine in engine_factory._engine_cache.values():
        engine.dispose()
    engine_factory.reset_cache()

    with disposable_engine(_admin_url(clickhouse_container)) as engine:
        with engine.begin() as conn:
            conn.execute(sa.text(f"DROP DATABASE IF EXISTS {db_name}"))


async def _ingest(ds: DatasourceConfig, **kwargs: Any) -> list[SlayerDataset]:
    sql_client = SlayerSQLClient(datasource=ds)
    try:
        return await ingestion.ingest_datasource_async(ds, client=sql_client, **kwargs)
    finally:
        await sql_client.aclose()
        sql_client.close()


def _ds_config(clickhouse_container, db_name: str) -> DatasourceConfig:
    host = clickhouse_container.get_container_host_ip()
    port = int(clickhouse_container.get_exposed_port(8123))
    user = getattr(clickhouse_container, "username", "default") or "default"
    password = getattr(clickhouse_container, "password", "") or ""
    return DatasourceConfig(
        name="testclickhouse",
        type="clickhouse",
        host=host,
        port=port,
        database=db_name,
        username=user,
        password=password,
    )


# ---------------------------------------------------------------------------
# Base orders/customers fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def _clickhouse_env_storage(clickhouse_container, tmp_path_factory):
    """Module-scoped: seeded ClickHouse orders + customers tables + storage."""
    db_name = _create_module_db(clickhouse_container)
    try:
        with disposable_engine(_ds_url_for_db(clickhouse_container, db_name)) as engine:
            with engine.begin() as conn:
                conn.execute(sa.text("""
                    CREATE TABLE customers (
                        id Int32,
                        name String,
                        region String
                    ) ENGINE = MergeTree() ORDER BY id
                """))
                conn.execute(sa.text("""
                    CREATE TABLE orders (
                        id Int32,
                        status String,
                        amount Float64,
                        customer_id Int32,
                        created_at DateTime
                    ) ENGINE = MergeTree() ORDER BY id
                """))
                conn.execute(sa.text("""
                    INSERT INTO customers VALUES
                        (1, 'Acme Corp', 'US'),
                        (2, 'Globex', 'EU'),
                        (3, 'Initech', 'US')
                """))
                conn.execute(sa.text("""
                    INSERT INTO orders VALUES
                        (1, 'completed', 100, 1, '2024-01-15 10:00:00'),
                        (2, 'completed', 200, 1, '2024-01-20 11:00:00'),
                        (3, 'pending', 50, 2, '2024-02-10 09:00:00'),
                        (4, 'completed', 150, 2, '2024-02-15 14:00:00'),
                        (5, 'cancelled', 75, 3, '2024-03-01 08:00:00'),
                        (6, 'pending', 300, 3, '2024-03-10 16:00:00')
                """))
                for stmt in seed_statements(
                    date_type="Date", timestamp_type="DateTime", int_type="Int32",
                    text_type="String", float_type="Float64",
                    table_options="ENGINE = MergeTree() ORDER BY id",
                ):
                    conn.execute(sa.text(stmt))

        tmpdir = str(tmp_path_factory.mktemp("clickhouse_env"))
        storage = YAMLStorage(base_dir=tmpdir)
        run_sync(storage.save_datasource(_ds_config(clickhouse_container, db_name)))
        run_sync(storage.save_dataset(SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="testclickhouse",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="status", expression="status", datatype=DataType.STRING),
                DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
                DatasetField(name="created_at", expression="created_at", datatype=DataType.DATETIME),
                DatasetField(name="total", expression="amount", datatype=DataType.FLOAT),
                DatasetField(name="avg_amount", expression="amount", datatype=DataType.FLOAT),
            ],
        )))
        run_sync(storage.save_dataset(SlayerDataset(
            name="customers",
            sql_table="customers",
            data_source="testclickhouse",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="name", expression="name", datatype=DataType.STRING),
                DatasetField(name="region", expression="region", datatype=DataType.STRING),
            ],
        )))
        run_sync(storage.save_dataset(streak_dataset("testclickhouse")))
        yield storage
    finally:
        _drop_module_db(clickhouse_container, db_name)


@pytest.fixture
def clickhouse_env(_clickhouse_env_storage) -> SlayerQueryEngine:
    """Per-test SlayerQueryEngine wrapping the module-scoped storage."""
    return SlayerQueryEngine(storage=_clickhouse_env_storage)


@pytest.mark.integration
class TestClickHouseQueries:
    async def test_try_cast_integer_targets(self, clickhouse_env: SlayerQueryEngine) -> None:
        await check_try_cast_targets(clickhouse_env, dataset="orders")

    async def test_author_casts(self, clickhouse_env: SlayerQueryEngine) -> None:
        await check_author_casts(clickhouse_env, dataset="orders")
        await check_boolean_casts(clickhouse_env, dataset="orders")

    async def test_count_all(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "count(orders.*)"}]})
        result = await clickhouse_env.execute(query=query)
        assert result.row_count == 1
        assert result.data[0]["orders._count"] == 6

    async def test_dev1933_regex_literal_extension_column(self, clickhouse_env: SlayerQueryEngine) -> None:
        """A dataset field holding a ``(?:...)`` regex literal and a ``%``
        LIKE pattern executes verbatim; text() misread ``:too`` as a bind parameter."""
        orders = await clickhouse_env.storage.get_dataset("orders")
        assert orders is not None
        await clickhouse_env.storage.save_dataset(orders.model_copy(update={"name": "orders_rx", "fields": [
            *orders.fields, DatasetField(name="rx", expression="CASE WHEN status LIKE '%pend%' "
                        "OR status = '(?i)(?:too complicated|too complex)' THEN 1 ELSE 0 END", datatype=DataType.FLOAT),
        ]}))
        query = SlayerQuery(
            population="orders_rx",
            dimensions=[QueryDimension(field="orders_rx.rx")],
            metrics=[QueryMetric(metric="count(orders_rx.*)")],
        )
        result = await clickhouse_env.execute(query=query)
        by_rx = {int(r["orders_rx.rx"]): r["orders_rx._count"] for r in result.data}
        assert by_rx == {1: 2, 0: 4}

    async def test_sum_measure(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "sum(orders.total)"}]})
        result = await clickhouse_env.execute(query=query)
        assert float(result.data[0]["orders.total_sum"]) == 875.0  # NOSONAR(S1244) — sum of integer cents, exact-representable

    async def test_string_hygiene_functions_execute(
        self, clickhouse_env: SlayerQueryEngine
    ) -> None:
        """``lower`` / ``substr`` reach ClickHouse as its character-counting ``lowerUTF8`` /
        ``substringUTF8``; ClickHouse names are case-sensitive, so this pins that they resolve live."""
        lowered = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "filter": ["lower(orders.status) = 'completed'"],
        })
        result = await clickhouse_env.execute(query=lowered)
        assert result.data[0]["orders._count"] > 0
        assert "lowerUTF8(" in (result.sql or ""), result.sql

        subs = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "filter": ["substr(orders.status, 1, 4) = 'comp'"],
        })
        result = await clickhouse_env.execute(query=subs)
        assert result.data[0]["orders._count"] > 0
        assert "substringUTF8(" in (result.sql or ""), result.sql

    async def test_trunc_executes_lowercase(
        self, clickhouse_env: SlayerQueryEngine
    ) -> None:
        """Sqlglot emits ClickHouse ``trunc`` LOWERCASE while every
        other backend uppercases it. ClickHouse function names are case-sensitive
        in general, so this pins that the lowercase spelling really resolves on a
        live server — and that ``trunc`` truncates toward zero rather than
        rounding: ``trunc(total / 40.0)`` is 2.5 -> 2 only for total = 100.
        """
        acceptance = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "filter": ["trunc(orders.total) >= 100"],
        })
        result = await clickhouse_env.execute(query=acceptance)
        assert result.data[0]["orders._count"] > 0
        assert "trunc(" in (result.sql or ""), result.sql
        assert "TRUNC(" not in (result.sql or ""), result.sql

        truncates = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "filter": ["trunc(orders.total / 40.0) == 2"],
        })
        result = await clickhouse_env.execute(query=truncates)
        assert result.data[0]["orders._count"] == 1, result.sql

    async def test_avg_measure(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "avg(orders.avg_amount)"}]})
        result = await clickhouse_env.execute(query=query)
        avg = float(result.data[0]["orders.avg_amount_avg"])
        assert abs(avg - 145.83) < 0.1

    async def test_group_by_status(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "dimensions": ["orders.status"],
        })
        result = await clickhouse_env.execute(query=query)
        by_status = {r["orders.status"]: r["orders._count"] for r in result.data}
        assert by_status["completed"] == 3
        assert by_status["pending"] == 2
        assert by_status["cancelled"] == 1

    async def test_filter_equals(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "filter": ["orders.status == 'completed'"],
        })
        result = await clickhouse_env.execute(query=query)
        assert result.data[0]["orders._count"] == 3

    async def test_filter_gt(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "filter": ["orders.total > 100"],
        })
        result = await clickhouse_env.execute(query=query)
        assert result.data[0]["orders._count"] == 3

    async def test_order_by_desc(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "dimensions": ["orders.status"],
            "order": [{"field": "count(orders.*)", "direction": "desc"}],
        })
        result = await clickhouse_env.execute(query=query)
        assert result.data[0]["orders.status"] == "completed"

    async def test_limit(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "dimensions": ["orders.status"],
            "limit": 2,
        })
        result = await clickhouse_env.execute(query=query)
        assert result.row_count == 2

    async def test_multiple_measures(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}, {"metric": "sum(orders.total)"}],
            "dimensions": ["orders.status"],
        })
        result = await clickhouse_env.execute(query=query)
        completed = next(r for r in result.data if r["orders.status"] == "completed")
        assert completed["orders._count"] == 3
        assert float(completed["orders.total_sum"]) == 450.0  # NOSONAR(S1244) — sum of integer cents, exact-representable

    async def test_time_dimension_month_granularity(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}],
        })
        result = await clickhouse_env.execute(query=query)
        assert result.row_count == 3

    async def test_time_dimension_with_date_range(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "time_dimensions": [{
                "field": "orders.created_at",
                "granularity": "month",
                "date_range": ["2024-01-01", "2024-02-28"],
            }],
        })
        result = await clickhouse_env.execute(query=query)
        total = sum(r["orders._count"] for r in result.data)
        assert total == 4

    async def test_composite_filter(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "count(orders.*)"}],
            "filter": ["orders.status == 'completed' or orders.status == 'pending'"],
        })
        result = await clickhouse_env.execute(query=query)
        assert result.data[0]["orders._count"] == 5

    async def test_time_shift_with_date_range(self, clickhouse_env: SlayerQueryEngine) -> None:
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
        result = await clickhouse_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(375.0)
        assert float(result.data[0]["prev_month"]) == pytest.approx(200.0)

    async def test_consecutive_periods_with_boolean_predicate(self, clickhouse_env: SlayerQueryEngine) -> None:
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
        result = await clickhouse_env.execute(query=query)
        assert [r["positive_run"] for r in result.data] == [1, 0, 1]

    @pytest.mark.parametrize("column,granularity", CALENDAR_CASES)
    async def test_consecutive_periods_breaks_on_calendar_gap(
        self, clickhouse_env: SlayerQueryEngine, column: str, granularity: str,
    ) -> None:
        await assert_calendar_streak(clickhouse_env, field=column, granularity=granularity)

    async def test_change_with_date_range(self, clickhouse_env: SlayerQueryEngine) -> None:
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
        result = await clickhouse_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["amount_change"]) == pytest.approx(175.0)

    async def test_change_pct_with_date_range(self, clickhouse_env: SlayerQueryEngine) -> None:
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
        result = await clickhouse_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["pct"]) == pytest.approx(0.875)

    async def test_multiple_date_range_shifts(self, clickhouse_env: SlayerQueryEngine) -> None:
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
        result = await clickhouse_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(200.0)
        assert float(result.data[0]["prev"]) == pytest.approx(300.0)
        assert float(result.data[0]["next"]) == pytest.approx(375.0)


# ---------------------------------------------------------------------------
# Cross-dataset + multistage (hand-declared relationships; no FK metadata available)
# ---------------------------------------------------------------------------


@pytest.fixture
def clickhouse_cross_dataset_env(clickhouse_container):
    """ClickHouse env with orders + customers (with score) and explicit relationship.
    No FK metadata is exposed by ClickHouse — the relationship is hand-declared."""
    db_name = _create_module_db(clickhouse_container)
    with disposable_engine(_ds_url_for_db(clickhouse_container, db_name)) as engine:
        with engine.begin() as conn:
            conn.execute(sa.text("""
                CREATE TABLE customers (
                    id Int32,
                    name String,
                    region String,
                    score Float64
                ) ENGINE = MergeTree() ORDER BY id
            """))
            conn.execute(sa.text("""
                CREATE TABLE orders (
                    id Int32,
                    status String,
                    amount Float64,
                    customer_id Int32,
                    created_at DateTime
                ) ENGINE = MergeTree() ORDER BY id
            """))
            conn.execute(sa.text("""
                INSERT INTO customers VALUES
                    (1, 'Alice', 'US', 90),
                    (2, 'Bob', 'EU', 60),
                    (3, 'Charlie', 'US', 80)
            """))
            conn.execute(sa.text("""
                INSERT INTO orders VALUES
                    (1, 'completed', 100, 1, '2024-01-15 10:00:00'),
                    (2, 'completed', 200, 1, '2024-01-20 11:00:00'),
                    (3, 'pending', 50, 2, '2024-02-10 09:00:00'),
                    (4, 'completed', 150, 2, '2024-02-15 14:00:00'),
                    (5, 'completed', 300, 3, '2024-03-01 08:00:00'),
                    (6, 'pending', 25, 1, '2024-03-10 16:00:00')
            """))

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(clickhouse_container, db_name)))
    run_sync(storage.save_dataset(SlayerDataset(
        name="orders", sql_table="orders", data_source="testclickhouse",
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
        name="customers", sql_table="customers", data_source="testclickhouse",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="name", expression="name", datatype=DataType.STRING),
            DatasetField(name="avg_score", expression="score", datatype=DataType.FLOAT),
        ],
    )))
    yield SlayerQueryEngine(storage=storage)
    _drop_module_db(clickhouse_container, db_name)


@pytest.mark.integration
class TestCrossModelAndMultistageClickHouse:
    async def test_cross_model_measure(self, clickhouse_cross_dataset_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.created_at", granularity=TimeGranularity.MONTH,
            )],
            metrics=[QueryMetric(metric="count(orders.*)"), QueryMetric(metric="avg(customers.avg_score)")],
            order=[OrderItem(field="orders.created_at", direction="asc")],
        )
        result = await clickhouse_cross_dataset_env.execute(query=query)
        assert result.row_count == 3
        assert [float(r["customers.avg_score_avg"]) for r in result.data] == pytest.approx([90.0, 60.0, 85.0])

    async def test_query_list_named(self, clickhouse_cross_dataset_env: SlayerQueryEngine) -> None:
        inner = SlayerQuery(
            name="monthly", population="orders",
            time_dimensions=[TimeDimension(
                field="orders.created_at", granularity=TimeGranularity.MONTH,
            )],
            metrics=[QueryMetric(metric="count(orders.*)"), QueryMetric(metric="sum(orders.total)")],
        )
        outer = SlayerQuery(population="monthly", metrics=[QueryMetric(metric="count(monthly.*)")])
        result = await clickhouse_cross_dataset_env.execute(query=[inner, outer])
        assert result.data[0]["monthly._count"] == 3

    async def test_create_model_from_query(self, clickhouse_cross_dataset_env: SlayerQueryEngine) -> None:
        source = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.created_at", granularity=TimeGranularity.MONTH,
            )],
            metrics=[QueryMetric(metric="count(orders.*)"), QueryMetric(metric="sum(orders.total)")],
        )
        saved = await clickhouse_cross_dataset_env.create_dataset_from_query(query=source, name="ch_monthly")
        assert saved.source_queries is not None
        result = await clickhouse_cross_dataset_env.execute(
            query=SlayerQuery(population="ch_monthly", metrics=[QueryMetric(metric="count(ch_monthly.*)")])
        )
        assert result.data[0]["ch_monthly._count"] == 3

    async def test_sql_dimension(self, clickhouse_cross_dataset_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            population="orders",
            dimensions=[QueryDimension(field="CASE WHEN orders.amount > 100 THEN 'high' ELSE 'low' END", alias="tier")],
            metrics=[QueryMetric(metric="count(orders.*)")],
        )
        result = await clickhouse_cross_dataset_env.execute(query=query)
        by_tier = {r["tier"]: r["orders._count"] for r in result.data}
        assert by_tier["high"] == 3
        assert by_tier["low"] == 3


@pytest.fixture
def clickhouse_key_union_env(clickhouse_container):
    """``kdim`` with nullable decimal / timestamp / text keys; ``facta`` reaches it, ``factb`` only by an orphan key."""
    db_name = _create_module_db(clickhouse_container)
    with disposable_engine(_ds_url_for_db(clickhouse_container, db_name)) as engine:
        with engine.begin() as conn:
            conn.execute(sa.text(
                "CREATE TABLE kdim (key_id String, k_dec Nullable(Decimal(10, 3)), k_ts Nullable(DateTime), "
                "k_txt Nullable(String)) ENGINE = MergeTree() ORDER BY key_id"))
            for fact, pk, value in (("facta", "a_id", "x"), ("factb", "b_id", "y")):
                conn.execute(sa.text(
                    f"CREATE TABLE {fact} ({pk} String, key_id String, {value} Float64) "
                    f"ENGINE = MergeTree() ORDER BY {pk}"))
            conn.execute(sa.text(
                "INSERT INTO kdim VALUES ('K1', 1.5, '2025-01-01 10:00:00', 'a'), ('K2', NULL, '2025-02-01 00:00:00', 'b')"))
            conn.execute(sa.text("INSERT INTO facta VALUES ('A1', 'K1', 1), ('A2', 'K2', 2)"))
            conn.execute(sa.text("INSERT INTO factb VALUES ('B1', 'K9', 5)"))

    storage = YAMLStorage(base_dir=tempfile.mkdtemp())
    run_sync(storage.save_datasource(_ds_config(clickhouse_container, db_name)))
    run_sync(storage.save_dataset(SlayerDataset(
        name="kdim", sql_table="kdim", data_source="testclickhouse",
        fields=[DatasetField(name="key_id", datatype=DataType.STRING, primary_key=True),
                DatasetField(name="k_dec", datatype=DataType.DECIMAL),
                DatasetField(name="k_ts", datatype=DataType.DATETIME),
                DatasetField(name="k_txt", datatype=DataType.STRING)],
    )))
    for fact, pk, value in (("facta", "a_id", "x"), ("factb", "b_id", "y")):
        run_sync(storage.save_dataset(SlayerDataset(
            name=fact, sql_table=fact, data_source="testclickhouse",
            fields=[DatasetField(name=pk, datatype=DataType.STRING, primary_key=True),
                    DatasetField(name="key_id", datatype=DataType.STRING),
                    DatasetField(name=value, datatype=DataType.FLOAT)],
            relationships=[DatasetRelationship(to="kdim", from_columns=["key_id"], to_columns=["key_id"])],
        )))
    yield SlayerQueryEngine(storage=storage)
    _drop_module_db(clickhouse_container, db_name)


@pytest.mark.integration
class TestRootlessKeyUnionClickHouse:
    async def test_key_types_agree_across_row_sources(self, clickhouse_key_union_env: SlayerQueryEngine) -> None:
        keys = ["kdim.k_dec", "kdim.k_ts", "kdim.k_txt"]
        result = await clickhouse_key_union_env.execute(query=SlayerQuery.model_validate({
            "dimensions": keys,
            "metrics": [{"metric": "sum(facta.x)", "alias": "x"}, {"metric": "sum(factb.y)", "alias": "y"}],
        }))
        assert len(result.data) == 3
        assert [r["y"] for r in result.data if all(r[k] is None for k in keys)] == [5.0]
        for k in keys:
            assert len({type(r[k]) for r in result.data if r[k] is not None}) == 1, k


@pytest.mark.integration
class TestNullableCastsClickHouse:
    async def test_casts_of_a_nullable_decimal(self, clickhouse_key_union_env: SlayerQueryEngine) -> None:
        forms = {"c": "CAST(kdim.k_dec AS INT)", "t": "TRY_CAST(kdim.k_dec AS SMALLINT)",
                 "b": "CAST(kdim.k_dec AS BOOLEAN)", "tb": "TRY_CAST(kdim.k_dec AS BOOLEAN)"}
        result = await clickhouse_key_union_env.execute(query=SlayerQuery.model_validate({
            "population": "kdim",
            "dimensions": ["kdim.key_id", *({"field": f, "alias": a} for a, f in forms.items())],
        }))
        got = {r["kdim.key_id"]: {a: (None if r[a] is None else int(r[a])) for a in forms} for r in result.data}
        assert got == {"K1": {"c": 2, "t": 2, "b": 1, "tb": 1}, "K2": dict.fromkeys(forms)}, result.sql


# ---------------------------------------------------------------------------
# Median / percentile — ClickHouse uses parametric quantile(p)(x) + median(x)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestClickHouseMedianPercentile:
    async def test_median(self, clickhouse_env: SlayerQueryEngine) -> None:
        # amounts = [100, 200, 50, 150, 75, 300] -> exact median 125
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "median(orders.total)"}]})
        result = await clickhouse_env.execute(query=query)
        assert float(result.data[0]["orders.total_median"]) == pytest.approx(125.0)

    async def test_median_emits_native_aggregate(self, clickhouse_env: SlayerQueryEngine) -> None:
        """``median`` is the exact ``quantileExactInclusive(0.5)(x)``, never ``percentile_cont``."""
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "median(orders.total)"}]})
        dry = await clickhouse_env.execute(query=query, dry_run=True)
        assert dry.sql is not None
        sql_lower = dry.sql.lower()
        assert "quantileexactinclusive(0.5)(" in sql_lower, (
            f"ClickHouse median must emit quantileExactInclusive(0.5)(x). Got:\n{dry.sql}"
        )
        assert "percentile_cont" not in sql_lower, (
            f"ClickHouse median must not fall through to PERCENTILE_CONT. Got:\n{dry.sql}"
        )

    async def test_percentile_uses_quantile_exact_inclusive(self, clickhouse_env: SlayerQueryEngine) -> None:
        """``percentile`` is the exact ``quantileExactInclusive(p)(x)``, not ``PERCENTILE_CONT``."""
        query = SlayerQuery(
            population="orders",
            metrics=[
                {"metric": "percentile(orders.total, p=0.25)"},
                {"metric": "percentile(orders.total, p=0.75)"},
            ],
        )
        dry = await clickhouse_env.execute(query=query, dry_run=True)
        assert dry.sql is not None
        sql_lower = dry.sql.lower()
        assert "quantileexactinclusive(0.25)(" in sql_lower, (
            f"ClickHouse must use quantileExactInclusive(p)(x). Got:\n{dry.sql}"
        )
        assert "quantileexactinclusive(0.75)(" in sql_lower, (
            f"ClickHouse must use quantileExactInclusive(p)(x). Got:\n{dry.sql}"
        )
        # And NOT the standard PERCENTILE_CONT (which would mean sqlglot's
        # transpilation didn't get overridden).
        assert "percentile_cont" not in sql_lower, (
            f"ClickHouse must not emit PERCENTILE_CONT. Got:\n{dry.sql}"
        )

    async def test_percentile_quartiles(self, clickhouse_env: SlayerQueryEngine) -> None:
        """ClickHouse's quantile() is an approximation (TDigest by default);
        accept loose tolerance, not exact match."""
        query = SlayerQuery(
            population="orders",
            metrics=[
                {"metric": "percentile(orders.total, p=0.25)"},
                {"metric": "percentile(orders.total, p=0.75)"},
            ],
        )
        result = await clickhouse_env.execute(query=query)
        row = result.data[0]
        # PERCENTILE_CONT(0.25) on [50,75,100,150,200,300] is 81.25;
        # PERCENTILE_CONT(0.75) is 187.5. ClickHouse approximations may
        # return the closest data point (75, 200) for tiny samples.
        assert float(row["orders.total_percentile_p_0_25"]) == pytest.approx(81.25, abs=50.0)
        assert float(row["orders.total_percentile_p_0_75"]) == pytest.approx(187.5, abs=50.0)

    async def test_median_grouped(self, clickhouse_env: SlayerQueryEngine) -> None:
        """Grouped median across status partitions:
            completed: [100, 150, 200]  -> median 150
            pending:   [50, 300]        -> median 175 (or close, due to approximation)
            cancelled: [75]             -> 75
        """
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "median(orders.total)"}],
            "dimensions": ["orders.status"],
        })
        result = await clickhouse_env.execute(query=query)
        by_status = {
            r["orders.status"]: float(r["orders.total_median"]) for r in result.data
        }
        # ClickHouse median() is an approximation; partition-level accuracy
        # is bounded but coarse on tiny samples. abs tolerance matches what
        # `test_percentile_quartiles` uses.
        assert by_status["completed"] == pytest.approx(150, abs=50.0)
        assert by_status["pending"] == pytest.approx(175, abs=125.0)
        assert by_status["cancelled"] == pytest.approx(75, abs=0.01)


@pytest.fixture(scope="module")
def _clickhouse_dev2458_storage(clickhouse_container, tmp_path_factory):
    db_name = _create_module_db(clickhouse_container)
    try:
        with disposable_engine(_ds_url_for_db(clickhouse_container, db_name)) as engine:
            with engine.begin() as conn:
                for stmt in [*dev2458.statements(dialect="clickhouse"), *dev2458.clickhouse_statements()]:
                    conn.execute(sa.text(stmt))
        storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp("clickhouse_dev2458")))
        ds = _ds_config(clickhouse_container, db_name)
        run_sync(storage.save_datasource(ds))
        for model in [*dev2458.datasets(data_source=ds.name), *dev2458.clickhouse_datasets(data_source=ds.name)]:
            run_sync(storage.save_dataset(model))
        yield storage
    finally:
        _drop_module_db(clickhouse_container, db_name)


@pytest.mark.integration
class TestClickHouseDistinctAndPercentiles:
    """Spec: aggregations/distinct-and-percentiles — exact percentiles past the reservoir, NULL never NaN."""

    @pytest.mark.parametrize("check", [
        *dev2458.COMMON_CHECKS, dev2458.check_approx_types, dev2458.check_clickhouse_large_group,
        dev2458.check_clickhouse_null_groups, dev2458.check_clickhouse_wide_integers,
    ], ids=lambda c: c.__name__)
    async def test_matrix(self, _clickhouse_dev2458_storage, check) -> None:
        await dev2458.run_check(check, storage=_clickhouse_dev2458_storage)


# ---------------------------------------------------------------------------
# Native stat aggregations (cross-dialect parity)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestClickHouseStatAggregations:
    """ClickHouse has native ``stddev_samp``/``stddev_pop``/``var_samp``/
    ``var_pop``/``corr``/``covar_samp``/``covar_pop`` (sqlglot transpiles
    to ``stddevSamp`` / ``corr`` / ``covarSamp`` / etc.). Numeric tolerance
    is tight (1e-6) because these go through ClickHouse's exact aggregate
    implementations (no UDF, no decomposition formula)."""

    async def test_stddev_samp_native_clickhouse(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "stddev_samp(orders.total)"}]})
        result = await clickhouse_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_stddev_samp"]) == pytest.approx(
            statistics.stdev(amounts), rel=1e-6
        )

    async def test_stddev_pop_native_clickhouse(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "stddev_pop(orders.total)"}],
        })
        result = await clickhouse_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_stddev_pop"]) == pytest.approx(
            statistics.pstdev(amounts), rel=1e-6
        )

    async def test_var_samp_native_clickhouse(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "var_samp(orders.total)"}]})
        result = await clickhouse_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_var_samp"]) == pytest.approx(
            statistics.variance(amounts), rel=1e-6
        )

    async def test_var_pop_native_clickhouse(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            population="orders",
            metrics=[{"metric": "var_pop(orders.total)"}],
        )
        result = await clickhouse_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_var_pop"]) == pytest.approx(
            statistics.pvariance(amounts), rel=1e-6
        )

    async def test_corr_native_clickhouse(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "corr(orders.total, other=orders.customer_id)"}],
        })
        result = await clickhouse_env.execute(query=query)
        xs = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        ys = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        expected = statistics.correlation(xs, ys)
        assert float(result.data[0]["orders.total_corr_other_customer_id"]) == pytest.approx(
            expected, rel=1e-6
        )

    async def test_covar_samp_native_clickhouse(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "covar_samp(orders.total, other=orders.customer_id)"}],
        })
        result = await clickhouse_env.execute(query=query)
        xs = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        ys = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        n = len(xs)
        mx, my = sum(xs) / n, sum(ys) / n
        expected = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (n - 1)
        assert float(
            result.data[0]["orders.total_covar_samp_other_customer_id"]
        ) == pytest.approx(expected, rel=1e-6)

    async def test_covar_pop_native_clickhouse(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "covar_pop(orders.total, other=orders.customer_id)"}],
        })
        result = await clickhouse_env.execute(query=query)
        xs = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        ys = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        n = len(xs)
        mx, my = sum(xs) / n, sum(ys) / n
        expected = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / n
        assert float(
            result.data[0]["orders.total_covar_pop_other_customer_id"]
        ) == pytest.approx(expected, rel=1e-6)


# ---------------------------------------------------------------------------
# log10 round-trip (ClickHouse has native log10)
# ---------------------------------------------------------------------------


@pytest.fixture
def clickhouse_log10_env(clickhouse_container):
    """Dedicated env for the log10 round-trip so the field-add doesn't
    leak into the shared module-scoped storage."""
    db_name = _create_module_db(clickhouse_container)
    with disposable_engine(_ds_url_for_db(clickhouse_container, db_name)) as engine:
        with engine.begin() as conn:
            conn.execute(sa.text("""
                CREATE TABLE orders (
                    id Int32,
                    amount Float64
                ) ENGINE = MergeTree() ORDER BY id
            """))
            conn.execute(sa.text(
                "INSERT INTO orders VALUES (1, 100), (2, 200), (3, 300)"
            ))

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(clickhouse_container, db_name)))
    run_sync(storage.save_dataset(SlayerDataset(
        name="orders", sql_table="orders", data_source="testclickhouse",
        fields=[
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
            DatasetField(name="log_amount", expression=ExpressionObject(dialects=[DialectEntry(dialect="CLICKHOUSE", expression="log10(amount)")]), datatype=DataType.FLOAT),
        ],
    )))
    yield SlayerQueryEngine(storage=storage)
    _drop_module_db(clickhouse_container, db_name)


@pytest.mark.integration
async def test_log10_round_trip_clickhouse(clickhouse_log10_env: SlayerQueryEngine) -> None:
    """ClickHouse has native LOG10 (ClickhouseDialect.log10_native=True).
    The emitted SQL must preserve ``log10(...)``."""
    result = await clickhouse_log10_env.execute(
        SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "max(orders.log_amount)"}]})
    )
    assert float(result.data[0]["orders.log_amount_max"]) == pytest.approx(
        _math.log10(300.0), rel=1e-9
    )

    dry = await clickhouse_log10_env.execute(
        SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "max(orders.log_amount)"}]}),
        dry_run=True,
    )
    assert dry.sql is not None
    sql_lower = dry.sql.lower()
    assert "log10(" in sql_lower, (
        f"Expected literal log10(...) in emitted SQL on ClickHouse, got:\n{dry.sql}"
    )
    assert "log(10," not in sql_lower.replace(" ", ""), (
        f"ClickHouse emitted SQL must not canonicalise log10 to LOG(10, ...):\n{dry.sql}"
    )


# ---------------------------------------------------------------------------
# Window-in-filter raises (parity)
# ---------------------------------------------------------------------------


@pytest.fixture
def planets_clickhouse_env(clickhouse_container):
    db_name = _create_module_db(clickhouse_container)
    with disposable_engine(_ds_url_for_db(clickhouse_container, db_name)) as engine:
        with engine.begin() as conn:
            conn.execute(sa.text("""
                CREATE TABLE planets (
                    id Int32,
                    name String,
                    mass Float64
                ) ENGINE = MergeTree() ORDER BY id
            """))
            conn.execute(sa.text("""
                INSERT INTO planets VALUES
                    (1, 'Mercury', 0.33),
                    (2, 'Venus', 4.87),
                    (3, 'Earth', 5.97),
                    (4, 'Mars', 0.642),
                    (5, 'Jupiter', 1898.0),
                    (6, 'Saturn', 568.0),
                    (7, 'Uranus', 86.8),
                    (8, 'Neptune', 102.0)
            """))

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(clickhouse_container, db_name)))
    run_sync(storage.save_dataset(
        SlayerDataset(
            name="planets",
            sql_table="planets",
            data_source="testclickhouse",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="name", expression="name", datatype=DataType.STRING),
                DatasetField(name="mass", expression="mass", datatype=DataType.FLOAT),
                DatasetField(
                    name="rn",
                    expression=ExpressionObject(dialects=[DialectEntry(dialect="CLICKHOUSE", expression="row_number() over (order by mass desc)")]),
                    datatype=DataType.FLOAT,
                ),
            ],
        )
    ))
    yield SlayerQueryEngine(storage=storage)
    _drop_module_db(clickhouse_container, db_name)


@pytest.mark.integration
async def test_filter_on_windowed_column_clickhouse(planets_clickhouse_env) -> None:
    """A filter on a windowed field applies outside the materialised window."""
    engine = planets_clickhouse_env
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
def clickhouse_derived_chain_env(clickhouse_container):
    db_name = _create_module_db(clickhouse_container)
    with disposable_engine(_ds_url_for_db(clickhouse_container, db_name)) as engine:
        with engine.begin() as conn:
            conn.execute(sa.text(
                "CREATE TABLE b_tbl (id Int32, foo_raw Float64) "
                "ENGINE = MergeTree() ORDER BY id"
            ))
            conn.execute(sa.text(
                "CREATE TABLE a_tbl (id Int32, bar Float64, b_id Int32, raw_a Float64) "
                "ENGINE = MergeTree() ORDER BY id"
            ))
            conn.execute(sa.text("INSERT INTO b_tbl VALUES (1, 200), (2, 50)"))
            conn.execute(sa.text(
                "INSERT INTO a_tbl VALUES (10, 4, 1, 100), (11, 1, 2, 5)"
            ))

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(clickhouse_container, db_name)))
    run_sync(storage.save_dataset(
        SlayerDataset(
            name="b_tbl",
            data_source="testclickhouse",
            sql_table="b_tbl",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="foo_raw", expression="foo_raw", datatype=DataType.FLOAT),
                DatasetField(name="foo_normalized", expression="foo_raw / 100.0", datatype=DataType.FLOAT),
            ],
        )
    ))
    run_sync(storage.save_dataset(
        SlayerDataset(
            name="a_tbl",
            data_source="testclickhouse",
            sql_table="a_tbl",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="bar", expression="bar", datatype=DataType.FLOAT),
                DatasetField(name="b_id", expression="b_id", datatype=DataType.FLOAT),
                DatasetField(name="raw_a", expression="raw_a", datatype=DataType.FLOAT),
                DatasetField(
                    name="ratio_using_derived",
                    expression=ExpressionObject(dialects=[DialectEntry(dialect="CLICKHOUSE", expression="a_tbl.bar / b_tbl.foo_normalized")]),
                    datatype=DataType.FLOAT,
                ),
            ],
            relationships=[DatasetRelationship(to="b_tbl", from_columns=["b_id"], to_columns=["id"])],
        )
    ))
    yield SlayerQueryEngine(storage=storage)
    _drop_module_db(clickhouse_container, db_name)


@pytest.mark.integration
async def test_integration_clickhouse_cross_model_derived_columnsql(
    clickhouse_derived_chain_env: SlayerQueryEngine,
) -> None:
    response = await clickhouse_derived_chain_env.execute(
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
# Dialect-specific: DateTime column type round-trip
# ---------------------------------------------------------------------------


@pytest.fixture
def clickhouse_ingest_for_types_env(clickhouse_container):
    """Seed a ClickHouse table with each scalar type we care about,
    then auto-ingest. The test asserts the inferred DataType vocabulary."""
    db_name = _create_module_db(clickhouse_container)
    with disposable_engine(_ds_url_for_db(clickhouse_container, db_name)) as engine:
        with engine.begin() as conn:
            conn.execute(sa.text("""
                CREATE TABLE orders (
                    id Int32,
                    customer_id Int32,
                    quantity Int32,
                    amount Float64 COMMENT 'Order amount',
                    status String,
                    created_at DateTime
                ) ENGINE = MergeTree() ORDER BY id COMMENT 'All orders'
            """))
            conn.execute(sa.text("""
                INSERT INTO orders VALUES
                    (1, 1, 5, 100.0, 'completed', '2024-01-01 00:00:00')
            """))
    ds = _ds_config(clickhouse_container, db_name)
    yield ds
    _drop_module_db(clickhouse_container, db_name)


@pytest.mark.integration
def test_clickhouse_datetime_typed_correctly(clickhouse_ingest_for_types_env) -> None:
    """Regression: auto-ingesting a ClickHouse DateTime
    column must produce ``DataType.DATETIME``, not STRING. Int32 / Float64
    must map to INT / DOUBLE."""
    ds = clickhouse_ingest_for_types_env
    models = run_sync(_ingest(ds, schema=None))
    orders = next(m for m in models if m.name == "orders")
    by_name = {c.name: c for c in orders.fields}

    assert by_name["id"].datatype == DataType.INTEGER
    assert by_name["customer_id"].datatype == DataType.INTEGER
    assert by_name["quantity"].datatype == DataType.INTEGER
    assert by_name["amount"].datatype == DataType.FLOAT
    assert by_name["status"].datatype == DataType.STRING
    assert by_name["created_at"].datatype == DataType.DATETIME, (
        f"ClickHouse DateTime must map to TIMESTAMP, got "
        f"{by_name['created_at'].datatype!r}"
    )


@pytest.mark.integration
def test_clickhouse_comments_imported(clickhouse_ingest_for_types_env) -> None:
    ds = clickhouse_ingest_for_types_env
    models = run_sync(_ingest(ds, schema=None))
    orders = next(m for m in models if m.name == "orders")
    assert orders.description == "All orders"
    by_name = {c.name: c for c in orders.fields}
    assert by_name["amount"].description == "Order amount"
    assert by_name["status"].description is None


# ---------------------------------------------------------------------------
# Dialect-aware Mode-A {var} escaping: ClickHouse's C-style literals break naive
# '' quote-doubling on a backslash-bearing value; escaping must round-trip.
# ---------------------------------------------------------------------------

# Distinct amounts per tricky status so a correct match is provable via the sum.
_CH_ESC_ROWS = [
    {"id": 1, "status": "a\\'b", "amount": 10.0},       # backslash + single quote
    {"id": 2, "status": "plain", "amount": 20.0},
    {"id": 3, "status": 'say "hi"', "amount": 30.0},    # embedded double quote
    {"id": 4, "status": "back\\slash", "amount": 40.0}, # lone backslash
]


@pytest.fixture(scope="module")
def _clickhouse_esc_storage(clickhouse_container, tmp_path_factory):
    """Per-module ClickHouse DB with an ``esc`` table of tricky-status rows +
    a dataset whose Mode-A filter carries a ``{v}`` placeholder."""
    db_name = _create_module_db(clickhouse_container)
    try:
        with disposable_engine(_ds_url_for_db(clickhouse_container, db_name)) as engine:
            with engine.begin() as conn:
                conn.execute(sa.text("""
                    CREATE TABLE esc (
                        id Int32,
                        status String,
                        amount Float64
                    ) ENGINE = MergeTree() ORDER BY id
                """))
                # Core Table insert → the driver binds each value as a
                # parameter, storing it literally regardless of SLayer's escaping.
                esc = sa.Table(
                    "esc",
                    sa.MetaData(),
                    sa.Column("id", sa.Integer),
                    sa.Column("status", sa.String),
                    sa.Column("amount", sa.Float),
                )
                conn.execute(esc.insert(), _CH_ESC_ROWS)

        tmpdir = str(tmp_path_factory.mktemp("clickhouse_esc"))
        storage = YAMLStorage(base_dir=tmpdir)
        run_sync(storage.save_datasource(_ds_config(clickhouse_container, db_name)))
        run_sync(storage.save_dataset(SlayerDataset(
            name="esc",
            sql_table="esc",
            data_source="testclickhouse",
            filters=["status = '{v}'"],
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="status", expression="status", datatype=DataType.STRING),
                DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
            ],
        )))
        yield storage
    finally:
        _drop_module_db(clickhouse_container, db_name)


@pytest.fixture
def clickhouse_esc_env(_clickhouse_esc_storage) -> SlayerQueryEngine:
    return SlayerQueryEngine(storage=_clickhouse_esc_storage)


@pytest.mark.integration
class TestClickHouseModeAEscaping:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("a\\'b", 10.0),
            ('say "hi"', 30.0),
            ("back\\slash", 40.0),
        ],
    )
    async def test_backslash_value_matches_only_its_row(
        self, clickhouse_esc_env: SlayerQueryEngine, value: str, expected: float
    ) -> None:
        resp = await clickhouse_esc_env.execute(SlayerQuery.model_validate({
            "population": "esc",
            "metrics": [{"metric": "sum(esc.amount)"}],
            "variables": {"v": value},
        }))
        assert resp.row_count == 1
        assert float(resp.data[0]["esc.amount_sum"]) == expected

    async def test_breakout_attempt_matches_nothing(
        self, clickhouse_esc_env: SlayerQueryEngine
    ) -> None:
        # Assert on the COUNT of matching rows (unambiguous) rather than a zero
        # aggregate — a breakout that matched the whole table would give a
        # non-zero count, and a zero SUM would not distinguish the cases.
        resp = await clickhouse_esc_env.execute(SlayerQuery.model_validate({
            "population": "esc",
            "metrics": [{"metric": "count(esc.*)"}],
            "variables": {"v": "x\\' OR '1'='1"},
        }))
        assert int(resp.data[0]["esc._count"]) == 0


# ---------------------------------------------------------------------------
# Decimal native-type preservation
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def _clickhouse_decimal_storage(clickhouse_container, tmp_path_factory):
    """Module-scoped: ingested table with wrapped and short decimal columns."""
    db_name = _create_module_db(clickhouse_container)
    try:
        with disposable_engine(_ds_url_for_db(clickhouse_container, db_name)) as engine:
            with engine.begin() as conn:
                conn.execute(sa.text("""
                    CREATE TABLE decimal_orders (
                        id Int32,
                        amount Nullable(Decimal(18, 2)),
                        short_amount Decimal64(4)
                    ) ENGINE = MergeTree() ORDER BY id
                """))
                conn.execute(sa.text("""
                    INSERT INTO decimal_orders VALUES
                        (1, '90071992547409.91', '900719925474.0992'),
                        (2, '0.02', '0.0001')
                """))

        datasource = _ds_config(clickhouse_container, db_name)
        model = next(
            m for m in run_sync(_ingest(datasource))
            if m.name == "decimal_orders"
        )
        amount = model.get_field("amount")
        short_amount = model.get_field("short_amount")
        assert amount is not None
        assert short_amount is not None
        # Bare inner type retained — wrapper text must not defeat preservation.
        assert amount.db_type == "Decimal(18, 2)"
        assert short_amount.db_type is not None
        assert "Decimal" in short_amount.db_type

        tmpdir = str(tmp_path_factory.mktemp("clickhouse_decimal"))
        storage = YAMLStorage(base_dir=tmpdir)
        run_sync(storage.save_datasource(datasource))
        run_sync(storage.save_dataset(model))
        yield storage
    finally:
        _drop_module_db(clickhouse_container, db_name)


@pytest.fixture
def clickhouse_decimal_env(_clickhouse_decimal_storage) -> SlayerQueryEngine:
    return SlayerQueryEngine(storage=_clickhouse_decimal_storage)


@pytest.mark.integration
class TestClickHouseDecimalPreservation:
    @pytest.mark.parametrize(
        "measure,result_key,expected",
        [
            (
                "sum(decimal_orders.amount)",
                "decimal_orders.amount_sum",
                Decimal("90071992547409.93"),
            ),
            (
                "sum(decimal_orders.short_amount)",
                "decimal_orders.short_amount_sum",
                Decimal("900719925474.0993"),
            ),
        ],
    )
    async def test_inferred_decimal_sum_preserves_exact_value(
        self, clickhouse_decimal_env: SlayerQueryEngine, measure, result_key, expected
    ) -> None:
        query = SlayerQuery.model_validate({"population": "decimal_orders", "metrics": [{"metric": measure}]})
        result = await clickhouse_decimal_env.execute(query=query)
        sql = (await clickhouse_decimal_env.execute(query=query, dry_run=True)).sql

        assert " AS DOUBLE" not in sql, sql
        assert "Float64" not in sql, sql
        value = result.data[0][result_key]
        assert isinstance(value, Decimal)
        assert value == expected


# ---------------------------------------------------------------------------
# An inferred timestamp keeps its native precision (no cast)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def _clickhouse_nanos_storage(clickhouse_container, tmp_path_factory):
    """Two DateTime64(9) values inside one microsecond."""
    db_name = _create_module_db(clickhouse_container)
    try:
        with disposable_engine(_ds_url_for_db(clickhouse_container, db_name)) as engine:
            with engine.begin() as conn:
                conn.execute(sa.text(
                    "CREATE TABLE nanos (id Int32, at DateTime64(9)) ENGINE = MergeTree() ORDER BY id"
                ))
                conn.execute(sa.text(
                    "INSERT INTO nanos VALUES (1, '2024-03-01 10:00:00.123456100'), (2, '2024-03-01 10:00:00.123456900')"
                ))
        datasource = _ds_config(clickhouse_container, db_name)
        storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp("clickhouse_nanos")))
        run_sync(storage.save_datasource(datasource))
        run_sync(storage.save_dataset(SlayerDataset(
            name="nanos", sql_table="nanos", data_source=datasource.name,
            fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                    DatasetField(name="at", datatype=DataType.DATETIME)],
        )))
        yield storage
    finally:
        _drop_module_db(clickhouse_container, db_name)


@pytest.mark.integration
class TestClickHouseInferredTimestampPrecision:
    async def test_a_computed_timestamp_keeps_values_within_one_microsecond_apart(
        self, _clickhouse_nanos_storage,
    ) -> None:
        engine = SlayerQueryEngine(storage=_clickhouse_nanos_storage)
        # The driver cannot read a nanosecond value back, so a later stage counts the groups.
        groups = {"name": "g", "population": "nanos",
                  "dimensions": [{"field": "date_add(nanos.at, 1, 'hour')", "alias": "v"}],
                  "metrics": [{"metric": "count(nanos.*)", "alias": "n"}]}
        result = await engine.execute([groups, {"population": "g", "metrics": [{"metric": "count(g.*)", "alias": "k"}]}])
        (row,) = result.data
        assert [int(v) for k, v in row.items() if k == "k" or k.endswith(".k")] == [2], result.sql

# ---------------------------------------------------------------------------


def _admin_rows(clickhouse_container, sql: str) -> list[tuple]:
    with disposable_engine(_admin_url(clickhouse_container)) as engine:
        with engine.connect() as conn:
            conn.exec_driver_sql("SYSTEM FLUSH LOGS")
            rows = conn.exec_driver_sql(sql, execution_options={"no_parameters": True}).fetchall()
            return [tuple(r) for r in rows]


def _readonly_level_datasource(clickhouse_container, level: int):
    """A datasource whose user has ``readonly = <level>``."""
    user = f"ro{level}_{uuid.uuid4().hex[:8]}"
    with disposable_engine(_admin_url(clickhouse_container)) as engine:
        with engine.begin() as conn:
            conn.execute(sa.text(
                f"CREATE USER {user} IDENTIFIED WITH plaintext_password BY 'pw' "
                f"SETTINGS readonly = {level}"
            ))
            conn.execute(sa.text(f"GRANT SELECT ON *.* TO {user}"))
    yield DatasourceConfig(
        name="readonly_clickhouse",
        type="clickhouse",
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
        database="default",
        username=user,
        password="pw",
    )
    with disposable_engine(_admin_url(clickhouse_container)) as engine:
        with engine.begin() as conn:
            conn.execute(sa.text(f"DROP USER IF EXISTS {user}"))


@pytest.fixture
def clickhouse_readonly_datasource(clickhouse_container):
    """A datasource whose user has ``readonly = 1`` and may change no settings."""
    yield from _readonly_level_datasource(clickhouse_container, 1)


@pytest.fixture
def clickhouse_readonly2_datasource(clickhouse_container):
    """A datasource whose user has ``readonly = 2``: read-only, but may change settings."""
    yield from _readonly_level_datasource(clickhouse_container, 2)


def _pooled_max_execution_time(client: SlayerSQLClient) -> str:
    with client._get_sync_engine_for_client().connect() as conn:
        return str(conn.exec_driver_sql("SELECT getSetting('max_execution_time')").scalar())


@pytest.mark.integration
class TestClickHouseStatementTimeout:
    async def test_timeout_stops_long_query(self, clickhouse_container) -> None:
        client = SlayerSQLClient(datasource=_ds_config(clickhouse_container, "default"))
        with pytest.raises(Exception, match="TIMEOUT_EXCEEDED"):
            await client.execute(sql="SELECT sleep(3)", timeout_seconds=1)

    async def test_statement_reaches_server_byte_identical(self, clickhouse_container) -> None:
        marker = f"slayer_{uuid.uuid4().hex[:12]}"
        sql = (
            "SELECT toStartOfMonth(d) AS m, x::Int32 AS y "
            f"FROM (SELECT toDate('2024-03-15') AS d, '7' AS x) -- {marker}"
        )
        client = SlayerSQLClient(datasource=_ds_config(clickhouse_container, "default"))
        rows = (await client.execute(sql=sql, timeout_seconds=30)).rows
        assert [(str(r["m"]), int(r["y"])) for r in rows] == [("2024-03-01", 7)]
        logged = _admin_rows(
            clickhouse_container,
            "SELECT query FROM system.query_log WHERE type = 'QueryFinish' "
            f"AND query LIKE '%{marker}%' AND query NOT LIKE '%system.query_log%'",
        )
        assert logged == [(sql,)]

    async def test_sql_own_setting_wins(self, clickhouse_container) -> None:
        client = SlayerSQLClient(datasource=_ds_config(clickhouse_container, "default"))
        result = await client.execute(
            sql="SELECT sleep(2) AS s SETTINGS max_execution_time = 5", timeout_seconds=1,
        )
        assert len(result.rows) == 1

    def test_setting_restored_on_pooled_connection(self, clickhouse_container) -> None:
        client = SlayerSQLClient(datasource=_ds_config(clickhouse_container, "default"))
        prior = _pooled_max_execution_time(client)
        client.execute_sync("SELECT 1", timeout_seconds=1)
        assert _pooled_max_execution_time(client) == prior

    def test_setting_restored_after_timed_query_raised(self, clickhouse_container) -> None:
        client = SlayerSQLClient(datasource=_ds_config(clickhouse_container, "default"))
        prior = _pooled_max_execution_time(client)
        with pytest.raises(Exception, match="TIMEOUT_EXCEEDED"):
            client.execute_sync("SELECT sleep(3)", timeout_seconds=1)
        assert _pooled_max_execution_time(client) == prior

    async def test_readonly_user_runs_queries(self, clickhouse_readonly_datasource) -> None:
        client = SlayerSQLClient(datasource=clickhouse_readonly_datasource)
        for _ in range(2):  # second call takes the cached no-setting path
            rows = (await client.execute(sql="SELECT 1 AS x")).rows
            assert [int(r["x"]) for r in rows] == [1]

    async def test_readonly_user_warned_and_checked_once(
        self, clickhouse_container, clickhouse_readonly_datasource,
    ) -> None:
        client = SlayerSQLClient(datasource=clickhouse_readonly_datasource)
        expected = StatementTimeoutSkippedWarning(
            datasource="readonly_clickhouse", timeout_seconds=120, reason="readonly_user",
        )
        for _ in range(2):
            with pytest.warns(SlayerStatementTimeoutSkippedWarning, match="readonly = 2"):
                result = await client.execute(sql="SELECT 1 AS x")
            assert result.warnings == [expected]
        (checks,) = _admin_rows(
            clickhouse_container,
            "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' "
            f"AND user = '{clickhouse_readonly_datasource.username}' "
            "AND query LIKE '%getSetting(%readonly%'",
        )
        assert checks == (1,)

    async def test_readonly_user_own_setting_fails_without_retry(
        self, clickhouse_container, clickhouse_readonly_datasource,
    ) -> None:
        client = SlayerSQLClient(datasource=clickhouse_readonly_datasource)
        marker = f"slayer_{uuid.uuid4().hex[:12]}"
        with pytest.raises(Exception, match="READONLY"):
            await client.execute(
                sql=f"SELECT number FROM system.numbers LIMIT 1 SETTINGS max_execution_time = 5 -- {marker}",
            )
        (attempts,) = _admin_rows(
            clickhouse_container,
            "SELECT count() FROM system.query_log WHERE type != 'QueryStart' "
            f"AND user = '{clickhouse_readonly_datasource.username}' AND query LIKE '%{marker}%'",
        )
        assert attempts == (1,)

    async def test_readonly_2_user_gets_timeout(self, clickhouse_readonly2_datasource) -> None:
        client = SlayerSQLClient(datasource=clickhouse_readonly2_datasource)
        with pytest.raises(Exception, match="TIMEOUT_EXCEEDED"):
            await client.execute(sql="SELECT sleep(3)", timeout_seconds=1)

    async def test_readonly_2_user_not_warned(self, clickhouse_readonly2_datasource) -> None:
        client = SlayerSQLClient(datasource=clickhouse_readonly2_datasource)
        with warnings.catch_warnings():
            warnings.simplefilter("error", SlayerStatementTimeoutSkippedWarning)
            result = await client.execute(sql="SELECT 1 AS x")
        assert result.warnings == []


# ---------------------------------------------------------------------------
# Join-based session policy + semi-join gate for readonly users
# ---------------------------------------------------------------------------

_CORRELATED_SETTING = "allow_experimental_correlated_subqueries"

_RLS_DDL = [
    "CREATE TABLE customers (id Nullable(Int32), org String) ENGINE = MergeTree() ORDER BY tuple()",
    "CREATE TABLE orders (id Int32, customer_id Nullable(Int32)) ENGINE = MergeTree() ORDER BY id",
    "CREATE TABLE line_items (id Int32, order_id Nullable(Int32)) ENGINE = MergeTree() ORDER BY id",
    "INSERT INTO customers VALUES (1, 'A'), (2, 'B'), (NULL, 'A')",
    "INSERT INTO orders VALUES (10, 1), (11, 2), (12, NULL), (13, 1)",
    "INSERT INTO line_items VALUES (100, 10), (101, 11), (102, 12), (103, NULL), (104, 13)",
    "CREATE TABLE sj_customers (id Int32, org String, spend Float64) ENGINE = MergeTree() ORDER BY id",
    "CREATE TABLE sj_orders (id Int32, customer_id Int32, channel String, amount Float64) "
    "ENGINE = MergeTree() ORDER BY id",
    "INSERT INTO sj_customers VALUES (1, 'A', 100), (2, 'B', 50), (3, 'A', 30)",
    "INSERT INTO sj_orders VALUES (10, 1, 'app', 5), (11, 1, 'web', 6), (12, 2, 'web', 7), (13, 3, 'web', 8)",
]


def _distributed_ddl(db_name: str) -> list[str]:
    return [
        f"CREATE TABLE customers_dist AS customers ENGINE = Distributed('default', '{db_name}', 'customers')",
        f"CREATE TABLE orders_dist AS orders ENGINE = Distributed('default', '{db_name}', 'orders')",
    ]


def _seed_rls_db(container, *, distributed: bool) -> str:
    if distributed:
        assert _admin_rows(container, "SELECT count() FROM system.clusters WHERE cluster = 'default'") != [(0,)], (
            "image lacks the stock 'default' cluster the Distributed tables need"
        )
    db_name = _create_module_db(container)
    with disposable_engine(_ds_url_for_db(container, db_name)) as engine:
        with engine.begin() as conn:
            for statement in [*_RLS_DDL, *(_distributed_ddl(db_name) if distributed else [])]:
                conn.exec_driver_sql(statement)
    return db_name


def _rls_datasets() -> list[SlayerDataset]:
    def _ids(name: str, key: str | None = None) -> SlayerDataset:
        columns = [DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True)]
        if key:
            columns.append(DatasetField(name=key, datatype=DataType.INTEGER))
        return SlayerDataset(name=name, sql_table=name, data_source="rls_ch", fields=columns)

    return [
        _ids("customers"), _ids("orders", "customer_id"), _ids("line_items", "order_id"),
        _ids("customers_dist"), _ids("orders_dist", "customer_id"), _ids("line_items_dist", "order_id"),
        SlayerDataset(name="sj_customers", sql_table="sj_customers", data_source="rls_ch", fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="org", datatype=DataType.STRING),
            DatasetField(name="spend", datatype=DataType.FLOAT),
        ]),
        SlayerDataset(
            name="sj_orders", sql_table="sj_orders", data_source="rls_ch",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="customer_id", datatype=DataType.INTEGER),
                DatasetField(name="channel", datatype=DataType.STRING),
                DatasetField(name="amount", datatype=DataType.FLOAT),
            ],
            relationships=[DatasetRelationship(to="sj_customers", from_columns=["customer_id"], to_columns=["id"])],
        ),
    ]


@contextlib.contextmanager
def _user_datasource(
    container, db_name: str, *, settings: str, extra_dbs: tuple[str, ...] = (),
) -> Generator[DatasourceConfig]:
    user = f"rls_{uuid.uuid4().hex[:8]}"
    with disposable_engine(_admin_url(container)) as engine:
        with engine.begin() as conn:
            conn.exec_driver_sql(f"CREATE USER {user} IDENTIFIED WITH plaintext_password BY 'pw' SETTINGS {settings}")
            for db in (db_name, *extra_dbs):
                conn.exec_driver_sql(f"GRANT SELECT ON {db}.* TO {user}")
    try:
        yield DatasourceConfig(
            name="rls_ch", type="clickhouse",
            host=container.get_container_host_ip(), port=int(container.get_exposed_port(8123)),
            database=db_name, username=user, password="pw",
        )
    finally:
        engine_factory.reset_cache()
        with disposable_engine(_admin_url(container)) as engine:
            with engine.begin() as conn:
                conn.exec_driver_sql(f"DROP USER IF EXISTS {user}")


def _rls_engine(
    datasource: DatasourceConfig, *, policy: SessionPolicy | None = None, client_factory: ClientFactory | None = None,
) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=tempfile.mkdtemp(prefix="rls_ch_"))
    run_sync(storage.save_datasource(datasource))
    for model in _rls_datasets():
        run_sync(storage.save_dataset(model))
    return SlayerQueryEngine(storage=storage, policy=policy, client_factory=client_factory)


class _RecordingGateway(DelegatingSqlClient):
    """A factory client over the default one, recording what reaches it."""

    def __init__(self, inner: SqlClient) -> None:
        super().__init__(inner)
        self.profile_requests = 0
        self.statements: list[str] = []

    async def server_profile(self) -> ServerProfile:
        self.profile_requests += 1
        return await self.inner.server_profile()

    async def execute(self, sql: str, *args: Any, **kwargs: Any) -> ExecutionResult:
        self.statements.append(sql)
        return await self.inner.execute(sql, *args, **kwargs)


_TENANT_POLICY = SessionPolicy(ruleset=JoinFilterRuleset(
    table="customers", column="org", value="A",
    joins=(
        JoinFilterRule(target_table="orders", join_path=("orders.customer_id = customers.id",)),
        JoinFilterRule(
            target_table="line_items",
            join_path=("line_items.order_id = orders.id", "orders.customer_id = customers.id"),
        ),
    ),
))

_DIST_POLICY = SessionPolicy(ruleset=JoinFilterRuleset(
    table="customers_dist", column="org", value="A",
    joins=(
        JoinFilterRule(target_table="orders_dist", join_path=("orders_dist.customer_id = customers_dist.id",)),
        JoinFilterRule(
            target_table="line_items_dist",
            join_path=("line_items_dist.order_id = orders_dist.id", "orders_dist.customer_id = customers_dist.id"),
        ),
    ),
))

# Each order / line item sits on the other shard from its customer / order.
_SHARD_ROWS = {
    "customers": ["(1, 'A')", "(2, 'B'), (NULL, 'A')"],
    "orders": ["(13, 1)", "(10, 1), (11, 2), (12, NULL)"],
    "line_items": ["(100, 10), (103, NULL)", "(101, 11), (102, 12), (104, 13)"],
}

_SEMI_JOIN_QUERY = SlayerQuery.model_validate({
    "population": "sj_orders", "dimensions": ["sj_customers.org"],
    "metrics": [{"metric": "sum(sj_orders.amount)", "alias": "m"}, {"metric": "sum(sj_customers.spend)", "alias": "cm"}],
    "filter": ["sj_orders.channel = 'app'"],
})
_SEMI_JOIN_ROWS = [{"sj_customers.org": "A", "m": 5.0, "cm": 100.0}]


async def _ids(engine: SlayerQueryEngine, dataset: str) -> list[int]:
    resp = await engine.execute(SlayerQuery.model_validate({"population": dataset, "dimensions": [f"{dataset}.id"]}))
    return sorted(int(row[f"{dataset}.id"]) for row in resp.data)


async def _assert_semi_join_runs_with_setting(engine: SlayerQueryEngine) -> None:
    dry = await engine.execute(_SEMI_JOIN_QUERY, dry_run=True)
    assert dry.sql is not None
    assert f"{_CORRELATED_SETTING} = 1" in dry.sql
    assert (await engine.execute(_SEMI_JOIN_QUERY)).data == _SEMI_JOIN_ROWS


@pytest.fixture(scope="module")
def rls_sharded_db(clickhouse_container):
    """Distributed tables over a two-shard cluster (one server, a database per shard)."""
    uid = uuid.uuid4().hex[:8]
    cluster, shards = f"rls_{uid}", tuple(f"rls_{uid}_s{i}" for i in (1, 2))
    replicas = "".join(
        f"<shard><replica><host>127.0.0.{i}</host><port>9000</port><user>{clickhouse_container.username}</user>"
        f"<password>{clickhouse_container.password}</password><default_database>{db}</default_database></replica></shard>"
        for i, db in enumerate(shards, start=1)
    )
    config = f"/etc/clickhouse-server/config.d/{cluster}.xml"
    xml = f"<clickhouse><remote_servers><{cluster}>{replicas}</{cluster}></remote_servers></clickhouse>"
    assert clickhouse_container.exec(["sh", "-c", f"cat > {config} <<'EOF'\n{xml}\nEOF"]).exit_code == 0
    db_name = _create_module_db(clickhouse_container)
    with disposable_engine(_admin_url(clickhouse_container)) as engine:
        with engine.begin() as conn:
            conn.exec_driver_sql("SYSTEM RELOAD CONFIG")
            for i, shard in enumerate(shards):
                conn.exec_driver_sql(f"CREATE DATABASE {shard}")
                for create in _RLS_DDL[:3]:
                    conn.exec_driver_sql(create.replace("CREATE TABLE ", f"CREATE TABLE {shard}.", 1))
                for table, rows in _SHARD_ROWS.items():
                    conn.exec_driver_sql(f"INSERT INTO {shard}.{table} VALUES {rows[i]}")
            for table in _SHARD_ROWS:
                conn.exec_driver_sql(
                    f"CREATE TABLE {db_name}.{table}_dist AS {shards[0]}.{table} "
                    f"ENGINE = Distributed('{cluster}', '', '{table}')"
                )
    yield db_name, shards
    _drop_module_db(clickhouse_container, db_name)
    with disposable_engine(_admin_url(clickhouse_container)) as engine:
        with engine.begin() as conn:
            for shard in shards:
                conn.exec_driver_sql(f"DROP DATABASE IF EXISTS {shard}")
    clickhouse_container.exec(["rm", "-f", config])


@pytest.fixture(scope="module")
def rls_db(clickhouse_container):
    db_name = _seed_rls_db(clickhouse_container, distributed=True)
    yield db_name
    _drop_module_db(clickhouse_container, db_name)


@pytest.mark.integration
class TestJoinPolicyReadonly:
    async def test_single_and_multi_hop_scope(self, clickhouse_container, rls_db) -> None:
        with _user_datasource(clickhouse_container, rls_db, settings="readonly = 1") as ds:
            engine = _rls_engine(ds, policy=_TENANT_POLICY)
            assert await _ids(engine, "orders") == [10, 13]
            assert await _ids(engine, "line_items") == [100, 104]

    async def test_no_setting_reaches_the_server(self, clickhouse_container, rls_db) -> None:
        with _user_datasource(clickhouse_container, rls_db, settings="readonly = 1") as ds:
            engine = _rls_engine(ds, policy=_TENANT_POLICY)
            dry = await engine.execute(SlayerQuery.model_validate({
                "population": "line_items",
                "dimensions": ["line_items.id"],
            }), dry_run=True)
        assert dry.sql is not None
        assert _CORRELATED_SETTING not in dry.sql
        assert "EXISTS" not in dry.sql.upper()

    async def test_null_keys_never_admit_a_row(self, clickhouse_container, rls_db) -> None:
        """Scenario: NULL keys never admit a row (``transform_null_in = 1``)."""
        settings = "readonly = 1, transform_null_in = 1"
        with _user_datasource(clickhouse_container, rls_db, settings=settings) as ds:
            engine = _rls_engine(ds, policy=_TENANT_POLICY)
            assert await _ids(engine, "orders") == [10, 13]
            assert await _ids(engine, "line_items") == [100, 104]

    async def test_distributed_tables(self, clickhouse_container, rls_db) -> None:
        with _user_datasource(clickhouse_container, rls_db, settings="readonly = 1") as ds:
            engine = _rls_engine(ds, policy=_DIST_POLICY)
            assert await _ids(engine, "orders_dist") == [10, 13]

    @pytest.mark.parametrize("product_mode", ["deny", "local"])
    async def test_sharded_distributed_tables(self, clickhouse_container, rls_sharded_db, product_mode) -> None:
        """Scenario: Distributed tables — rows and their join partners on different shards."""
        db_name, shards = rls_sharded_db
        settings = f"readonly = 1, transform_null_in = 1, distributed_product_mode = '{product_mode}'"
        with _user_datasource(clickhouse_container, db_name, settings=settings, extra_dbs=shards) as ds:
            engine = _rls_engine(ds, policy=_DIST_POLICY)
            assert await _ids(engine, "orders_dist") == [10, 13]
            assert await _ids(engine, "line_items_dist") == [100, 104]


@pytest.fixture(scope="module", params=["25.4", "25.8"])
def versioned_clickhouse(request):
    """A ClickHouse server of the given version with the RLS / semi-join dataset seeded."""
    container = ClickHouseContainer(f"clickhouse/clickhouse-server:{request.param}").with_env(
        "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1"
    )
    with container as c:
        db_name = _seed_rls_db(c, distributed=False)
        yield request.param, c, db_name
        _drop_module_db(c, db_name)


def _user_queries_touching(container, *, user: str, table: str) -> int:
    (count,) = _admin_rows(
        container,
        "SELECT count() FROM system.query_log WHERE type != 'QueryStart' "
        f"AND user = '{user}' AND query LIKE '%{table}%'",
    )
    return int(count[0])


@pytest.mark.integration
class TestVersionedReadonly:
    async def test_readonly_join_policy_scopes(self, versioned_clickhouse) -> None:
        """Scenario: Readonly user on a server with correlated subqueries off."""
        _, container, db_name = versioned_clickhouse
        with _user_datasource(container, db_name, settings="readonly = 1") as ds:
            engine = _rls_engine(ds, policy=_TENANT_POLICY)
            assert await _ids(engine, "orders") == [10, 13]
            assert await _ids(engine, "line_items") == [100, 104]

    async def test_readonly_semi_join(self, versioned_clickhouse) -> None:
        version, container, db_name = versioned_clickhouse
        with _user_datasource(container, db_name, settings="readonly = 1") as ds:
            engine = _rls_engine(ds)
            if version == "25.4":
                with pytest.raises(SlayerError) as ei:
                    await engine.execute(_SEMI_JOIN_QUERY)
                assert _CORRELATED_SETTING in str(ei.value)
                assert "channel" in str(ei.value)
                assert _user_queries_touching(container, user=ds.username or "", table="sj_orders") == 0
            else:
                await _assert_semi_join_runs_with_setting(engine)

    async def test_readonly_2_semi_join_runs(self, versioned_clickhouse) -> None:
        _, container, db_name = versioned_clickhouse
        with _user_datasource(container, db_name, settings="readonly = 2") as ds:
            await _assert_semi_join_runs_with_setting(_rls_engine(ds))

    async def test_semi_join_and_profile_reach_the_factory_client(self, versioned_clickhouse) -> None:
        """Scenario: Engine-internal statements reach the factory client (ClickHouse server profile)."""
        _, container, db_name = versioned_clickhouse
        built: list[_RecordingGateway] = []

        def factory(datasource: DatasourceConfig) -> _RecordingGateway:
            built.append(_RecordingGateway(SlayerSQLClient(datasource=datasource)))
            return built[-1]

        with _user_datasource(container, db_name, settings="readonly = 2") as ds:
            engine = _rls_engine(ds, client_factory=factory)
            try:
                assert (await engine.execute(_SEMI_JOIN_QUERY)).data == _SEMI_JOIN_ROWS
            finally:
                await engine.aclose()
                engine.close()
        assert len(built) == 1
        assert built[0].profile_requests >= 1
        assert any("EXISTS" in sql.upper() for sql in built[0].statements), built[0].statements

    async def test_setting_enabled_in_profile_runs(self, versioned_clickhouse) -> None:
        _, container, db_name = versioned_clickhouse
        settings = f"readonly = 1, {_CORRELATED_SETTING} = 1"
        with _user_datasource(container, db_name, settings=settings) as ds:
            await _assert_semi_join_runs_with_setting(_rls_engine(ds))


# ---------------------------------------------------------------------------
# Mode-B date functions (the shared oracle matrix + spec scenarios)
# ---------------------------------------------------------------------------

_DATE_CASES = matrix_cases()


@pytest.fixture(scope="module")
def _clickhouse_dates_storage(clickhouse_container, tmp_path_factory):
    db_name = _create_module_db(clickhouse_container)
    try:
        with disposable_engine(_ds_url_for_db(clickhouse_container, db_name)) as engine:
            with engine.begin() as conn:
                for stmt in server_seed_statements("clickhouse", today=datetime.now(timezone.utc).date()):
                    conn.execute(sa.text(stmt))
        storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp("clickhouse_dates")))
        run_sync(storage.save_datasource(_ds_config(clickhouse_container, db_name)))
        for model in all_datasets(data_source="testclickhouse"):
            run_sync(storage.save_dataset(model))
        yield storage
    finally:
        _drop_module_db(clickhouse_container, db_name)


@pytest.fixture
def clickhouse_dates(_clickhouse_dates_storage) -> SlayerQueryEngine:
    return SlayerQueryEngine(storage=_clickhouse_dates_storage)


@pytest.mark.integration
class TestClickHouseDateFunctions:
    @pytest.mark.parametrize("case", _DATE_CASES, ids=[c.case_id for c in _DATE_CASES])
    async def test_matrix(self, clickhouse_dates: SlayerQueryEngine, case: DateCase) -> None:
        assert_case((await clickhouse_dates.execute(matrix_query(case))).data, case)

    async def test_scenarios(self, clickhouse_dates: SlayerQueryEngine) -> None:
        await check_server_scenarios(clickhouse_dates)


# ---------------------------------------------------------------------------
# Time spine and custom granularities
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def _clickhouse_spine_storage(clickhouse_container, tmp_path_factory):
    db_name = _create_module_db(clickhouse_container)
    try:
        with disposable_engine(_ds_url_for_db(clickhouse_container, db_name)) as engine:
            with engine.begin() as conn:
                for stmt in server_statements("clickhouse"):
                    conn.execute(sa.text(stmt))
        storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp("clickhouse_spine")))
        base = _ds_config(clickhouse_container, db_name)
        for name, models in (("ch", server_datasets(data_source="ch")), ("ch_ts", server_datasets_ts(data_source="ch_ts"))):
            run_sync(storage.save_datasource(with_granularities(base, name=name)))
            for model in models:
                run_sync(storage.save_dataset(model))
        yield storage
    finally:
        _drop_module_db(clickhouse_container, db_name)


@pytest.mark.integration
class TestClickHouseTimeSpine:
    async def test_scenarios(self, _clickhouse_spine_storage) -> None:
        await check_all(SlayerQueryEngine(storage=_clickhouse_spine_storage), data_source="ch", ts_data_source="ch_ts")


# ---------------------------------------------------------------------------
# One logical bucket type; a DATE meeting a TIMESTAMP reads as midnight
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def _clickhouse_logical_types_storage(clickhouse_container, tmp_path_factory):
    db_name = _create_module_db(clickhouse_container)
    try:
        with disposable_engine(_ds_url_for_db(clickhouse_container, db_name)) as engine:
            with engine.begin() as conn:
                for stmt in logical_type_statements("clickhouse"):
                    conn.execute(sa.text(stmt))
        storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp("clickhouse_logical_types")))
        base = _ds_config(clickhouse_container, db_name)
        run_sync(storage.save_datasource(logical_type_granularities(base, name="ch_lt")))
        for model in logical_type_datasets(data_source="ch_lt"):
            run_sync(storage.save_dataset(model))
        yield storage
    finally:
        _drop_module_db(clickhouse_container, db_name)


@pytest.mark.integration
class TestClickHouseLogicalTypes:
    async def test_bucket_matrix(self, _clickhouse_logical_types_storage) -> None:
        await check_bucket_matrix(SlayerQueryEngine(storage=_clickhouse_logical_types_storage), backend="clickhouse", data_source="ch_lt")

    async def test_mixed_date_timestamp(self, _clickhouse_logical_types_storage) -> None:
        await check_mixed(SlayerQueryEngine(storage=_clickhouse_logical_types_storage), backend="clickhouse", data_source="ch_lt")


@pytest.mark.integration
def test_every_clickhouse_alias_failure_is_quoted(clickhouse_container) -> None:
    db_name = _create_module_db(clickhouse_container)
    try:
        with disposable_engine(_ds_url_for_db(clickhouse_container, db_name)) as engine:
            with engine.begin() as conn:
                conn.execute(sa.text("CREATE TABLE kw_probe (a Int32) ENGINE = Memory"))
            try:
                with engine.connect() as conn:
                    own = [r[0] for r in conn.execute(sa.text("SELECT keyword FROM system.keywords"))]
            except DBAPIError:
                own = []  # system.keywords is newer than some server versions
            assert unquoted_alias_failures(
                dialect="clickhouse", words=keyword_universe(own), table="kw_probe", column="a",
                fails=sa_statement_fails(engine), workers=8,
            ) == []
    finally:
        _drop_module_db(clickhouse_container, db_name)


# ---------------------------------------------------------------------------
# Portable Mode-B library (spec: queries/scalar-functions, regex, json, mixed-type-coercion, string-matching)
# ---------------------------------------------------------------------------

def _http(clickhouse_container, db_name: str, sql: str, **settings: str) -> None:
    """Run one statement over HTTP, where per-query server settings ride as URL parameters."""
    host = clickhouse_container.get_container_host_ip()
    port = int(clickhouse_container.get_exposed_port(8123))
    user = getattr(clickhouse_container, "username", "default") or "default"
    password = getattr(clickhouse_container, "password", "") or ""
    resp = requests.post(f"http://{host}:{port}/", params={"database": db_name, **settings}, data=sql.encode(),
                         auth=(user, password), timeout=60)
    assert resp.ok, resp.text


@pytest.fixture(scope="module")
def _clickhouse_dev2120_storage(clickhouse_container, tmp_path_factory):
    db_name = _create_module_db(clickhouse_container)
    try:
        with disposable_engine(_ds_url_for_db(clickhouse_container, db_name)) as engine:
            with engine.begin() as conn:
                for stmt in dev2120_table_statements(LIB, dialect="clickhouse"):
                    conn.execute(sa.text(stmt))
        _http(clickhouse_container, db_name, "CREATE TABLE docs (id Int32, payload JSON) ENGINE = MergeTree ORDER BY id",
              allow_experimental_json_type="1")
        rows = ", ".join(f"({rid}, {server_literal(doc or '{}', kind='TEXT', dialect='clickhouse')})"
                         for rid, doc in DOCS_ROWS)
        _http(clickhouse_container, db_name, f"INSERT INTO docs VALUES {rows}", allow_experimental_json_type="1")
        ds = _ds_config(clickhouse_container, db_name)
        storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp("clickhouse_dev2120")))
        run_sync(storage.save_datasource(ds))
        run_sync(storage.save_dataset(lib_dataset(data_source="testclickhouse")))
        for model in run_sync(_ingest(ds, include_tables=["docs"])):
            run_sync(storage.save_dataset(model))
        yield storage
    finally:
        _drop_module_db(clickhouse_container, db_name)


@pytest.mark.integration
class TestClickHousePortableLibrary:
    async def test_library(self, _clickhouse_dev2120_storage) -> None:
        await check_library(SlayerQueryEngine(storage=_clickhouse_dev2120_storage))

    async def test_native_json_column(self, _clickhouse_dev2120_storage) -> None:
        await check_native_json(SlayerQueryEngine(storage=_clickhouse_dev2120_storage))


# ---------------------------------------------------------------------------
# List fields (spec: models/list-columns, queries/list-functions, queries/list-children, aggregations/collect)
# ---------------------------------------------------------------------------


_TAGGED = ("CREATE TABLE tagged (id Int32, tags Array(LowCardinality(Nullable(String)))) "
           "ENGINE = MergeTree ORDER BY id")


@contextlib.contextmanager
def _seeded_lists(container, base_dir: str):
    db_name = _create_module_db(container)
    try:
        with disposable_engine(_ds_url_for_db(container, db_name)) as engine:
            with engine.begin() as conn:
                for stmt in [*contractors_statements(dialect="clickhouse"),
                             *assignments_statements(dialect="clickhouse"),
                             *mixed_numbers_statements(dialect="clickhouse"), _TAGGED]:
                    conn.execute(sa.text(stmt))
        ds = _ds_config(container, db_name)
        storage = YAMLStorage(base_dir=base_dir)
        run_sync(storage.save_datasource(ds))
        run_sync(storage.save_dataset(contractors_dataset(data_source=ds.name)))
        run_sync(storage.save_dataset(assignments_dataset(data_source=ds.name)))
        run_sync(storage.save_dataset(mixed_numbers_dataset(data_source=ds.name)))
        yield storage, ds
    finally:
        _drop_module_db(container, db_name)


@pytest.fixture(scope="module")
def _clickhouse_lists(clickhouse_container, tmp_path_factory):
    with _seeded_lists(clickhouse_container, str(tmp_path_factory.mktemp("clickhouse_lists"))) as seeded:
        yield seeded


@pytest.fixture(scope="module")
def _versioned_clickhouse_lists(versioned_clickhouse, tmp_path_factory):
    _, container, _ = versioned_clickhouse
    with _seeded_lists(container, str(tmp_path_factory.mktemp("clickhouse_lists_versioned"))) as seeded:
        yield seeded


@pytest.mark.integration
class TestClickHouseLists:
    async def test_lists(self, _clickhouse_lists) -> None:
        # Below 25.4 a filter across a to-many hop is refused, a list reached through a to-one relationship included.
        await check_lists(SlayerQueryEngine(storage=_clickhouse_lists[0]), dialect="clickhouse",
                          refused={"through-to-one": "25.4", "elements-filtered-across-another-to-many": "25.4"})

    async def test_lists_on_a_correlating_server(self, _versioned_clickhouse_lists, versioned_clickhouse) -> None:
        # Before 25.8 a correlated EXISTS beside a list's ARRAY JOIN is refused.
        refused = {"elements-filtered-across-another-to-many": "25.8"} if versioned_clickhouse[0] == "25.4" else None
        await check_lists(SlayerQueryEngine(storage=_versioned_clickhouse_lists[0]), dialect="clickhouse",
                          refused=refused)

    async def test_mixed_numeric_lists(self, _clickhouse_lists) -> None:
        await check_mixed_numbers(SlayerQueryEngine(storage=_clickhouse_lists[0]))

    def test_wrapped_element_ingested(self, _clickhouse_lists) -> None:
        (model,) = run_sync(_ingest(_clickhouse_lists[1], include_tables=["tagged"]))
        tags = model.get_field("tags")
        assert tags is not None
        assert (tags.datatype, tags.element_type) == (DataType.ARRAY, DataType.STRING)


# ---------------------------------------------------------------------------
# A warm statement costs one request
# ---------------------------------------------------------------------------


def _query_log_since(clickhouse_container, since: str) -> list[str]:
    """Every client query the server started since ``since``, in order."""
    with disposable_engine(_admin_url(clickhouse_container)) as engine, engine.connect() as conn:
        conn.exec_driver_sql("SYSTEM FLUSH LOGS")
        rows = conn.exec_driver_sql(
            "SELECT query FROM system.query_log WHERE type = 'QueryStart' AND is_initial_query"
            f" AND event_time_microseconds >= toDateTime64('{since}', 6) ORDER BY event_time_microseconds",
        ).fetchall()
    return [query for (query,) in rows]


@pytest.mark.integration
@pytest.mark.parametrize("path", PATHS)
def test_warm_statement_costs_one_request(clickhouse_container, path: str) -> None:
    with disposable_engine(_admin_url(clickhouse_container)) as engine, engine.connect() as conn:
        since = conn.exec_driver_sql("SELECT toString(now64(6))").scalar()
    run_three(
        SlayerSQLClient(datasource=_ds_config(clickhouse_container, "default")), path=path,
        sql=lambda marker: f"SELECT '{marker}' AS m",
    )
    assert_one_request_each(_query_log_since(clickhouse_container, since))


# ---------------------------------------------------------------------------
# Read-only requests for an unrestricted user (readonly = 0)
# ---------------------------------------------------------------------------


@pytest.fixture
def clickhouse_unrestricted_datasource(clickhouse_container):
    """A ``readonly = 0`` user who may write to a fresh database holding ``t``."""
    db_name = _create_module_db(clickhouse_container)
    user = f"rw_{uuid.uuid4().hex[:8]}"
    with disposable_engine(_admin_url(clickhouse_container)) as engine:
        with engine.begin() as conn:
            conn.execute(sa.text(f"CREATE TABLE {db_name}.t (x Int32) ENGINE = MergeTree ORDER BY x"))
            conn.execute(sa.text(f"CREATE USER {user} IDENTIFIED WITH plaintext_password BY 'pw' SETTINGS readonly = 0"))
            conn.execute(sa.text(f"GRANT SELECT, INSERT ON {db_name}.* TO {user}"))
    yield DatasourceConfig(
        name="unrestricted_clickhouse", type="clickhouse",
        host=clickhouse_container.get_container_host_ip(),
        port=int(clickhouse_container.get_exposed_port(8123)),
        database=db_name, username=user, password="pw",
    )
    _drop_module_db(clickhouse_container, db_name)
    with disposable_engine(_admin_url(clickhouse_container)) as engine:
        with engine.begin() as conn:
            conn.execute(sa.text(f"DROP USER IF EXISTS {user}"))


def _insert_through_engine(ds: DatasourceConfig, *, after_reconnect: bool = False) -> None:
    engine = engine_factory.get_engine(ds)
    if after_reconnect:
        with engine.connect() as conn:
            conn.invalidate()
    with engine.connect() as conn:
        conn.exec_driver_sql("INSERT INTO t VALUES (1)")


@pytest.mark.integration
class TestClickHouseReadOnly:
    async def test_unrestricted_user_is_made_read_only(
        self, clickhouse_container, clickhouse_unrestricted_datasource,
    ) -> None:
        ds = clickhouse_unrestricted_datasource
        engine_factory.invalidate_engine(ds)
        client = SlayerSQLClient(datasource=ds)
        try:
            with pytest.raises(Exception, match="TIMEOUT_EXCEEDED"):
                await client.execute(sql="SELECT sleep(3)", timeout_seconds=1)
            assert (await client.execute(sql="SELECT 1 AS x")).rows == [{"x": 1}]
            with pytest.raises(Exception, match="READONLY"):
                _insert_through_engine(ds)
            with pytest.raises(Exception, match="READONLY"):
                _insert_through_engine(ds, after_reconnect=True)
        finally:
            await client.aclose()
            client.close()
            engine_factory.invalidate_engine(ds)
        (checks,) = _admin_rows(
            clickhouse_container,
            "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' "
            f"AND user = '{ds.username}' AND query LIKE '%getSetting(%readonly%'",
        )
        sent = _admin_rows(
            clickhouse_container,
            "SELECT DISTINCT Settings['readonly'] FROM system.query_log WHERE type = 'QueryFinish' "
            f"AND user = '{ds.username}' AND query LIKE 'SELECT 1 AS x%'",
        )
        assert checks == (1,)
        assert sent == [("2",)]

    async def test_a_metadata_read_on_a_fresh_engine_is_read_only(
        self, clickhouse_container, clickhouse_unrestricted_datasource,
    ) -> None:
        ds = clickhouse_unrestricted_datasource
        engine_factory.invalidate_engine(ds)
        client = SlayerSQLClient(datasource=ds)
        try:
            listing = await client.list_objects(ds.database)
            with pytest.raises(Exception, match="READONLY"):
                _insert_through_engine(ds)
        finally:
            await client.aclose()
            client.close()
            engine_factory.invalidate_engine(ds)
        logged = _admin_rows(
            clickhouse_container,
            "SELECT query LIKE '%getSetting(%readonly%' AS probe, Settings['readonly'] FROM system.query_log "
            f"WHERE type = 'QueryFinish' AND user = '{ds.username}'",
        )
        assert "t" in listing.tables
        assert {readonly for probe, readonly in logged if probe} == {"0"}  # the user's own level, not SLayer's 2
        assert {readonly for probe, readonly in logged if not probe} == {"2"}

    @pytest.mark.parametrize("level", [1, 2])
    async def test_restricted_users_are_sent_no_readonly_setting(self, clickhouse_container, level: int) -> None:
        # Such a user may not change ``readonly``: a sent setting would fail these requests.
        with contextlib.contextmanager(_readonly_level_datasource)(clickhouse_container, level) as ds:
            engine_factory.invalidate_engine(ds)
            client = SlayerSQLClient(datasource=ds)
            try:
                listing = await client.list_objects("system")
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", SlayerStatementTimeoutSkippedWarning)
                    rows = (await client.execute(sql="SELECT 1 AS x")).rows
            finally:
                await client.aclose()
                client.close()
                engine_factory.invalidate_engine(ds)
        assert "one" in listing.tables
        assert rows == [{"x": 1}]


# ---------------------------------------------------------------------------
# Time zones (spec: queries/time-zones › Zone conversion renders on every Tier-1 dialect)
# ---------------------------------------------------------------------------

_TZ_LITERAL = f"DateTime64(6, '{TZ_NY}')"


def _ch_value(value: Any) -> str:
    return repr(value) if isinstance(value, float) else f"'{value}'"


@pytest.fixture
def clickhouse_tz_env(clickhouse_container, tmp_path) -> Generator[SlayerQueryEngine]:
    db_name = _create_module_db(clickhouse_container)
    rows = ", ".join(
        "(" + ", ".join([str(i), _ch_value(a), _ch_value(ts), _ch_value(ts), _ch_value(slot), _ch_value(d),
                         _ch_value(ts)]) + ")"
        for i, (a, ts, slot, d) in TZ_ROWS.items()
    )
    marks = ", ".join(f"('{at}', '{label}')" for at, label in TZ_MARKS.items())
    try:
        with disposable_engine(_ds_url_for_db(clickhouse_container, db_name)) as engine:
            with engine.begin() as conn:
                conn.execute(sa.text(
                    "CREATE TABLE orders (id Int32, amount Float64, ordered_at DateTime64(6), "
                    "paid_at DateTime64(6, 'UTC'), local_slot DateTime64(6), order_day Date, "
                    "created_raw DateTime64(6)) ENGINE = MergeTree() ORDER BY id"
                ))
                conn.execute(sa.text(f"INSERT INTO orders VALUES {rows}"))
                conn.execute(sa.text(
                    "CREATE TABLE marks (marked_at DateTime64(6), label String) ENGINE = MergeTree() ORDER BY marked_at"
                ))
                conn.execute(sa.text(f"INSERT INTO marks VALUES {marks}"))
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        run_sync(storage.save_datasource(DatasourceConfig.model_validate({
            **_ds_config(clickhouse_container, db_name).model_dump(), "name": TZ_DS, "time_zone": "UTC",
        })))
        run_sync(storage.save_dataset(tz_orders_dataset()))
        run_sync(storage.save_dataset(orders_naive_instant_dataset()))
        run_sync(storage.save_dataset(tz_marks_dataset()))
        yield SlayerQueryEngine(storage=storage)
    finally:
        _drop_module_db(clickhouse_container, db_name)


def _wall(value: Any) -> datetime:
    """A ``DateTime64`` value's wall clock in its own zone, whether the driver returns it zoned or naive."""
    return value.replace(tzinfo=None) if isinstance(value, datetime) else datetime.fromisoformat(str(value))


def _wall_day(value: Any):
    """A ``DateTime64(6, Q)`` bucket's Q wall-clock day, whether the driver returns it zoned or naive."""
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.replace(tzinfo=None).date()
    return tz_day(value)


@pytest.mark.integration
class TestClickHouseTimeZones:
    @pytest.mark.parametrize("field", ["ordered_at", "paid_at"])
    async def test_local_day(self, clickhouse_tz_env: SlayerQueryEngine, field: str) -> None:
        resp = await clickhouse_tz_env.execute(SlayerQuery.model_validate({
            "population": "orders", "time_zone": TZ_NY, "metrics": [{"metric": "sum(orders.amount)", "alias": "s"}],
            "time_dimensions": [{"field": f"orders.{field}", "granularity": "day"}],
        }))
        got = {_wall_day(r[f"orders.{field}"]): float(r["s"]) for r in resp.data}
        assert got == {tz_local(i, TZ_NY).date(): tz_amount(i) for i in TZ_ROWS}, resp.sql

    async def test_a_filter_literal_is_zoned(self, clickhouse_tz_env: SlayerQueryEngine) -> None:
        resp = await clickhouse_tz_env.execute(SlayerQuery.model_validate({
            "population": "orders", "time_zone": TZ_NY, "dimensions": ["orders.id"],
            "filter": ["orders.ordered_at in '2024-12-31'"],
        }))
        assert {int(r["orders.id"]) for r in resp.data} == {1}, resp.sql
        assert resp.sql is not None
        assert _TZ_LITERAL in resp.sql, resp.sql

    async def test_a_date_range_is_zoned(self, clickhouse_tz_env: SlayerQueryEngine) -> None:
        resp = await clickhouse_tz_env.execute(SlayerQuery.model_validate({
            "population": "orders", "time_zone": TZ_NY, "metrics": [{"metric": "sum(orders.amount)", "alias": "s"}],
            "time_dimensions": [{"field": "orders.ordered_at", "granularity": "day",
                                 "date_range": ["2025-01-01", "2025-01-31"]}],
        }))
        got = {_wall_day(r["orders.ordered_at"]): float(r["s"]) for r in resp.data}
        assert got == {tz_local(2, TZ_NY).date(): 20.0, tz_local(3, TZ_NY).date(): 30.0}, resp.sql
        assert resp.sql is not None
        assert _TZ_LITERAL in resp.sql, resp.sql

    async def test_a_converted_value_meets_a_civil_one_as_wall_clocks(self, clickhouse_tz_env: SlayerQueryEngine) -> None:
        resp = await clickhouse_tz_env.execute(SlayerQuery.model_validate({
            "population": "orders", "time_zone": TZ_NY,
            "dimensions": ["orders.id", {"field": "greatest(orders.ordered_at, orders.local_slot)", "alias": "v"}],
            "filter": ["orders.ordered_at < orders.local_slot"],
        }))
        got = {int(r["orders.id"]): _wall(r["v"]) for r in resp.data}
        slots = {i: datetime.fromisoformat(TZ_ROWS[i][2]) for i in TZ_ROWS}
        assert got == {i: slots[i] for i in TZ_ROWS if tz_local(i, TZ_NY) < slots[i]}, resp.sql

    async def test_a_civil_value_meets_one_instant_of_a_repeated_hour(
        self, clickhouse_tz_env: SlayerQueryEngine, clickhouse_container,
    ) -> None:
        """Documented: in New York's repeated 01:30 a civil 01:30 equals one of the two instants reading as it."""
        ds = await clickhouse_tz_env.storage.get_datasource(TZ_DS)
        assert ds is not None
        assert ds.database is not None
        with disposable_engine(_ds_url_for_db(clickhouse_container, ds.database)) as engine:
            with engine.begin() as conn:
                conn.execute(sa.text(
                    "INSERT INTO orders VALUES "
                    "(5, 1.0, '2024-11-03 05:30:00', '2024-11-03 05:30:00', '2024-11-03 01:30:00', '2024-11-03', "
                    "'2024-11-03 05:30:00'), "
                    "(6, 1.0, '2024-11-03 06:30:00', '2024-11-03 06:30:00', '2024-11-03 01:30:00', '2024-11-03', "
                    "'2024-11-03 06:30:00')"))
        resp = await clickhouse_tz_env.execute(SlayerQuery.model_validate({
            "population": "orders", "time_zone": TZ_NY, "dimensions": ["orders.id"],
            "filter": ["orders.ordered_at = orders.local_slot"],
        }))
        assert len({int(r["orders.id"]) for r in resp.data} & {5, 6}) == 1, resp.sql

    async def test_derived_timestamps_keep_the_query_zone_wall_clock(self, clickhouse_tz_env: SlayerQueryEngine) -> None:
        later = [DatasetField.model_validate({"name": f"{src}_later", "expression": f"{src} + INTERVAL 1 HOUR",
                                              "datatype": datatype})
                 for src, datatype in (("ordered_at", "DateTime"), ("paid_at", "DateTimeTz"))]
        await clickhouse_tz_env.storage.save_dataset(tz_orders_dataset(extra_fields=later))
        resp = await clickhouse_tz_env.execute(SlayerQuery.model_validate({
            "population": "orders", "time_zone": TZ_NY,
            "dimensions": ["orders.id", "orders.ordered_at_later", "orders.paid_at_later"],
        }))
        # Hours added across a DST transition differ by dialect (elapsed here, wall clock on DuckDB): away from one.
        expected = {i: tz_local(i, TZ_NY) + timedelta(hours=1) for i in TZ_ROWS
                    if (tz_local(i, TZ_NY) + timedelta(hours=1)).date() != date(2025, 3, 9)}
        for name in ("ordered_at_later", "paid_at_later"):
            got = {int(r["orders.id"]): _wall(r[f"orders.{name}"]) for r in resp.data}
            assert {i: v for i, v in got.items() if i in expected} == expected, resp.sql

    async def test_instant_typing_changes_nothing_without_a_zone(self, clickhouse_tz_env: SlayerQueryEngine) -> None:
        for zoned, naive in zip(instant_typing_queries("orders"), instant_typing_queries("orders_naive")):
            got, want = await clickhouse_tz_env.execute(zoned), await clickhouse_tz_env.execute(naive)
            assert row_values(got) == row_values(want), got.sql


# ---------------------------------------------------------------------------
# Time of day (spec: queries/time-of-day › ClickHouse answers from server 25.6)
# ---------------------------------------------------------------------------

_TIME_SETTING = "enable_time_time64_type"
_TIME_CONSTRUCTS = ["CAST('10:30:00' AS TIME)", "'10:30:00'::TIME", "TRY_CAST('10:30:00' AS TIME)", "TIME '10:30:00'"]


def _shifts_engine(datasource: DatasourceConfig) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=tempfile.mkdtemp(prefix="tod_ch_"))
    run_sync(storage.save_datasource(datasource))
    for name in ("shifts", "long_shifts"):
        run_sync(storage.save_dataset(SlayerDataset(name=name, sql_table=name, data_source=datasource.name, fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="starts_at", datatype=DataType.TIME),
            DatasetField(name="label", datatype=DataType.STRING)]), _validate=False))
    return SlayerQueryEngine(storage=storage)


@pytest.fixture(scope="module")
def clickhouse_shifts(versioned_clickhouse):
    """``shifts`` and ``long_shifts`` (holding ``25:00:00``): ``Time`` columns from 25.6, text on 25.4."""
    version, container, db_name = versioned_clickhouse
    settings = {} if version == "25.4" else {_TIME_SETTING: "1"}
    column = "String" if version == "25.4" else "Time"
    for table, rows in (("shifts", "(1, '09:30:00', 'a'), (2, '17:00:00', 'b')"), ("long_shifts", "(1, '25:00:00', 'x')")):
        _http(container, db_name,
              f"CREATE TABLE {table} (id Int32, starts_at {column}, label String) ENGINE = MergeTree ORDER BY id", **settings)
        _http(container, db_name, f"INSERT INTO {table} VALUES {rows}", **settings)
    return version, container, db_name


def _shift_query(**fields: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"population": "shifts", **fields})


def _from_25_6(version: str) -> None:
    if version == "25.4":
        pytest.skip("no time-of-day type before server 25.6")


@pytest.mark.integration
class TestVersionedTimeOfDay:
    @pytest.mark.parametrize("expression", _TIME_CONSTRUCTS)
    async def test_a_construct_runs_from_25_6_and_is_refused_below(self, clickhouse_shifts, expression: str) -> None:
        version, container, db_name = clickhouse_shifts
        query = _shift_query(dimensions=[{"field": expression, "alias": "v"}], filter=["shifts.id = 1"])
        with _user_datasource(container, db_name, settings="readonly = 0") as ds:
            engine = _shifts_engine(ds)
            if version == "25.4":
                with pytest.raises(SlayerError) as ei:
                    await engine.execute(query)
                assert "25.6" in str(ei.value)
                assert _user_queries_touching(container, user=ds.username or "", table="shifts") == 0
                return
            resp = await engine.execute(query)
        assert resp.data == [{"v": time(10, 30)}], resp.sql
        assert _TIME_SETTING in (resp.sql or ""), resp.sql

    async def test_the_clock(self, clickhouse_shifts) -> None:
        version, container, db_name = clickhouse_shifts
        _from_25_6(version)
        with _user_datasource(container, db_name, settings="readonly = 0") as ds:
            resp = await _shifts_engine(ds).execute(_shift_query(
                dimensions=[{"field": "CURRENT_TIME", "alias": "ct"}, {"field": "current_time()", "alias": "fn"}]))
        [row] = resp.data
        assert type(row["ct"]) is time, row
        assert row["ct"] == row["fn"], row

    async def test_a_time_column(self, clickhouse_shifts) -> None:
        version, container, db_name = clickhouse_shifts
        _from_25_6(version)
        with _user_datasource(container, db_name, settings="readonly = 0") as ds:
            engine = _shifts_engine(ds)
            rows = await engine.execute(_shift_query(dimensions=["shifts.id", "shifts.starts_at", {
                "field": "date_part('hour', shifts.starts_at)", "alias": "h"}]))
            ends = await engine.execute(_shift_query(metrics=[{"metric": "min(shifts.starts_at)", "alias": "lo"},
                                                               {"metric": "max(shifts.starts_at)", "alias": "hi"}]))
            late = await engine.execute(_shift_query(dimensions=["shifts.id"], filter=["shifts.starts_at >= '12:00'"]))
        assert {int(r["shifts.id"]): (r["shifts.starts_at"], int(r["h"])) for r in rows.data} == {
            1: (time(9, 30), 9), 2: (time(17, 0), 17)}
        assert ends.data == [{"lo": time(9, 30), "hi": time(17, 0)}], ends.sql
        assert [int(r["shifts.id"]) for r in late.data] == [2]

    @pytest.mark.parametrize("text", ["bogus", "25:00:00", "-01:00:00"])
    async def test_try_cast_of_text_naming_no_time_is_null(self, clickhouse_shifts, text: str) -> None:
        version, container, db_name = clickhouse_shifts
        _from_25_6(version)
        with _user_datasource(container, db_name, settings="readonly = 0") as ds:
            resp = await _shifts_engine(ds).execute(_shift_query(
                dimensions=[{"field": f"TRY_CAST('{text}' AS TIME)", "alias": "v"}], filter=["shifts.id = 1"]))
        assert resp.data == [{"v": None}], resp.sql

    async def test_a_stored_duration_fails_the_query(self, clickhouse_shifts) -> None:
        version, container, db_name = clickhouse_shifts
        _from_25_6(version)
        query = SlayerQuery.model_validate({"population": "long_shifts", "dimensions": ["long_shifts.starts_at"]})
        with _user_datasource(container, db_name, settings="readonly = 0") as ds:
            engine = _shifts_engine(ds)
            with pytest.raises(SlayerError) as ei:
                await engine.execute(query)
        assert "long_shifts.starts_at" in str(ei.value)
        assert "25:00:00" in str(ei.value)

    async def test_a_readonly_user_is_refused_naming_the_setting(self, clickhouse_shifts) -> None:
        version, container, db_name = clickhouse_shifts
        _from_25_6(version)
        query = _shift_query(dimensions=["shifts.starts_at"])
        with _user_datasource(container, db_name, settings="readonly = 1") as ds:
            engine = _shifts_engine(ds)
            with pytest.raises(SlayerError) as ei:
                await engine.execute(query)
            assert _TIME_SETTING in str(ei.value)
            assert _user_queries_touching(container, user=ds.username or "", table="shifts") == 0

    @pytest.mark.parametrize("settings", ["readonly = 2", f"readonly = 1, {_TIME_SETTING} = 1"])
    async def test_a_user_who_may_run_it_runs_it(self, clickhouse_shifts, settings: str) -> None:
        version, container, db_name = clickhouse_shifts
        _from_25_6(version)
        with _user_datasource(container, db_name, settings=settings) as ds:
            resp = await _shifts_engine(ds).execute(_shift_query(dimensions=["shifts.starts_at"],
                                                                 order=[{"field": "shifts.starts_at"}]))
        assert [r["shifts.starts_at"] for r in resp.data] == [time(9, 30), time(17, 0)]

    async def test_a_query_without_a_time_of_day_is_unaffected(self, clickhouse_shifts) -> None:
        _, container, db_name = clickhouse_shifts
        with _user_datasource(container, db_name, settings="readonly = 1") as ds:
            resp = await _shifts_engine(ds).execute(_shift_query(dimensions=["shifts.label"],
                                                                 order=[{"field": "shifts.label"}]))
        assert [r["shifts.label"] for r in resp.data] == ["a", "b"]
        assert _TIME_SETTING not in (resp.sql or "")

    async def test_time_columns_ingest_as_time(self, clickhouse_shifts) -> None:
        version, container, db_name = clickhouse_shifts
        _from_25_6(version)
        _http(container, db_name, "CREATE TABLE IF NOT EXISTS clocks (id Int32, t Time, t6 Time64(6), n Nullable(Time)) "
                                  "ENGINE = MergeTree ORDER BY id", **{_TIME_SETTING: "1"})
        datasets = await _ingest(_ds_config(container, db_name), include_tables=["clocks"])
        assert {f.name: f.datatype for f in datasets[0].fields} == {
            "id": DataType.INTEGER, "t": DataType.TIME, "t6": DataType.TIME, "n": DataType.TIME}
