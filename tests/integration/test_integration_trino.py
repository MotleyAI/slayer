"""Live Trino suite: a ``trinodb/trino`` container, each module seeding its own ``memory`` schema.

The federation tests run a second Trino with live Postgres and MySQL catalogs.

The ``memory`` connector has no primary or foreign keys, so joins are hand-declared.
Skipped when ``testcontainers[trino]`` or the Trino driver is missing, or Docker is unreachable.
"""

import contextlib
import json
import math
import re
import statistics
import time
import uuid
from collections.abc import Generator, Iterable
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from urllib.parse import quote

import pytest

from slayer.async_utils import run_sync
from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, DatasourceConfig, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, ModelExtension, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.ingestion import ingest_datasource, ingest_datasource_idempotent
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.sql.client import SlayerSQLClient, get_column_types_sync
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.dialects import trino as trino_dialect_module
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1737_fixtures import (
    DateCase,
    all_models,
    assert_case,
    assert_value,
    check_server_scenarios,
    matrix_cases,
    matrix_query,
    server_seed_statements,
    sql_literal,
)
from tests._engine_helpers import disposable_engine
from tests._reserved_alias_probe import keyword_universe, sa_statement_fails, unquoted_alias_failures
from tests.integration._consecutive_periods_calendar import (
    CALENDAR_CASES,
    assert_calendar_streak,
    seed_statements,
    streak_model,
)
from tests.integration._dev2015_server import check_all, server_models, server_models_ts, server_statements, with_granularities

pytest.importorskip("testcontainers.trino")
pytest.importorskip("trino.sqlalchemy")

import docker  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above
from testcontainers.core.network import Network  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above
from testcontainers.mysql import MySqlContainer  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above
from testcontainers.postgres import PostgresContainer  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above
from testcontainers.trino import TrinoContainer  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

_IMAGE = "trinodb/trino:483"
_CATALOG = "memory"
_DS = "testtrino"
_AMOUNTS = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
_CUSTOMER_IDS = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]


# ---------------------------------------------------------------------------
# Container, schemas, datasources
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def _docker_available_or_skip():
    try:
        docker.from_env().ping()
    except Exception as exc:  # pragma: no cover — exercised on local dev
        pytest.skip(f"Docker not available: {exc}")


@pytest.fixture(scope="session")
def trino_container():
    with TrinoContainer(_IMAGE, container_start_timeout=180) as container:
        yield container


def _host_port(container) -> tuple[str, int]:
    return container.get_container_host_ip(), int(container.get_exposed_port(8080))


def _url(container, schema: str | None = None, *, catalog: str = _CATALOG) -> str:
    host, port = _host_port(container)
    path = f"{catalog}/{schema}" if schema else catalog
    return f"trino://{container.user}@{host}:{port}/{path}"


def _run(
    container, statements: Iterable[str], *, schema: str | None = None, catalog: str = _CATALOG,
) -> list[list[Any]]:
    """Run ``statements`` in order on a raw driver cursor; the last one's rows."""
    rows: list[list[Any]] = []
    with disposable_engine(_url(container, schema, catalog=catalog)) as engine:
        raw = engine.raw_connection()
        try:
            cursor = raw.cursor()
            for statement in statements:
                cursor.execute(statement)
                rows = list(cursor.fetchall())
        finally:
            raw.close()
    return rows


def _drop_schema(container, schema: str) -> None:
    engine_factory.reset_cache()
    tables = _run(container, [f"SHOW TABLES FROM {_CATALOG}.{schema}"])
    _run(container, [
        *(f"DROP TABLE {_CATALOG}.{schema}.{table}" for (table,) in tables),
        f"DROP SCHEMA {_CATALOG}.{schema}",
    ])


@contextlib.contextmanager
def _seeded_schema(container, statements: Iterable[str]) -> Generator[str]:
    schema = f"test_{uuid.uuid4().hex[:12]}"
    _run(container, [f"CREATE SCHEMA {_CATALOG}.{schema}"])
    try:
        _run(container, statements, schema=schema)
        yield schema
    finally:
        _drop_schema(container, schema)


def _ds_config(container, schema: str, *, name: str = _DS, catalog: str = _CATALOG) -> DatasourceConfig:
    host, port = _host_port(container)
    return DatasourceConfig(
        name=name, type="trino", host=host, port=port,
        database=f"{catalog}/{schema}", username=container.user,
    )


def _storage(base_dir: str, datasource: DatasourceConfig, models: Iterable[SlayerModel]) -> YAMLStorage:
    storage = YAMLStorage(base_dir=base_dir)
    run_sync(storage.save_datasource(datasource))
    for model in models:
        run_sync(storage.save_model(model))
    return storage


def _orders_rows(rows: Iterable[tuple[int, str, float, int, str]]) -> str:
    return ", ".join(
        f"({i}, '{status}', {amount}, {customer}, TIMESTAMP '{ts}', DATE '{ts[:10]}')"
        for i, status, amount, customer, ts in rows
    )


# ---------------------------------------------------------------------------
# Base orders / customers
# ---------------------------------------------------------------------------

_BASE_ORDERS = [
    (1, "completed", 100.0, 1, "2024-01-15 10:00:00"),
    (2, "completed", 200.0, 1, "2024-01-20 11:00:00"),
    (3, "pending", 50.0, 2, "2024-02-10 09:00:00"),
    (4, "completed", 150.0, 2, "2024-02-15 14:00:00"),
    (5, "cancelled", 75.0, 3, "2024-03-01 08:00:00"),
    (6, "pending", 300.0, 3, "2024-03-10 16:00:00"),
]
_PCT_AMOUNTS = [100.0, 200.0, 50.0, 150.0, 300.0, 25.0, 10.01]

_BASE_SEED = [
    "CREATE TABLE customers (id INTEGER, name VARCHAR, region VARCHAR)",
    "CREATE TABLE orders (id INTEGER, status VARCHAR, amount DOUBLE, customer_id INTEGER, "
    "created_at TIMESTAMP(6), created_on DATE)",
    "INSERT INTO customers VALUES (1, 'Acme Corp', 'US'), (2, 'Globex', 'EU'), (3, 'Initech', 'US')",
    f"INSERT INTO orders VALUES {_orders_rows(_BASE_ORDERS)}",
    "CREATE TABLE pct_amounts (id INTEGER, amount DOUBLE)",
    "INSERT INTO pct_amounts VALUES " + ", ".join(f"({i}, {a})" for i, a in enumerate(_PCT_AMOUNTS, start=1)),
    *seed_statements(
        timestamp_type="TIMESTAMP(6)", text_type="VARCHAR", float_type="DOUBLE", typed_temporals=True,
    ),
]


def _base_models() -> list[SlayerModel]:
    return [
        SlayerModel(
            name="orders", sql_table="orders", data_source=_DS,
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="created_on", sql="created_on", type=DataType.DATE),
                Column(name="total", sql="amount", type=DataType.DOUBLE),
                Column(name="avg_amount", sql="amount", type=DataType.DOUBLE),
            ],
        ),
        SlayerModel(
            name="customers", sql_table="customers", data_source=_DS,
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
                Column(name="region", sql="region", type=DataType.TEXT),
            ],
        ),
        SlayerModel(
            name="pct_amounts", sql_table="pct_amounts", data_source=_DS,
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        ),
        streak_model(_DS),
    ]


@pytest.fixture(scope="module")
def _trino_base(trino_container, tmp_path_factory):
    with _seeded_schema(trino_container, _BASE_SEED) as schema:
        storage = _storage(
            str(tmp_path_factory.mktemp("trino_env")), _ds_config(trino_container, schema), _base_models(),
        )
        yield storage, schema


@pytest.fixture
def trino_env(_trino_base) -> SlayerQueryEngine:
    return SlayerQueryEngine(storage=_trino_base[0])


@pytest.fixture
def trino_direct(trino_container, _trino_base):
    """Run raw SQL in the base schema."""
    return lambda sql: _run(trino_container, [sql], schema=_trino_base[1])


async def _count_by(engine: SlayerQueryEngine, *, dimension: str, **query: Any) -> dict[Any, int]:
    resp = await engine.execute(SlayerQuery.model_validate({
        "source_model": "orders", "measures": [{"formula": "*:count", "name": "n"}],
        "dimensions": [dimension], **query,
    }))
    return {r[f"orders.{dimension}"]: int(r["orders.n"]) for r in resp.data}


async def _week_buckets(engine: SlayerQueryEngine, *, column: str, granularity: str) -> dict[str, int]:
    resp = await engine.execute(SlayerQuery.model_validate({
        "source_model": "orders", "measures": [{"formula": "*:count", "name": "n"}],
        "time_dimensions": [{"dimension": column, "granularity": granularity}],
    }))
    return {str(r[f"orders.{column}"])[:10]: int(r["orders.n"]) for r in resp.data}


@pytest.mark.integration
class TestTrinoQueries:
    async def test_count_all(self, trino_env: SlayerQueryEngine) -> None:
        result = await trino_env.execute(query=SlayerQuery.model_validate({"source_model": "orders", "measures": [{"formula": "*:count"}]}))
        assert result.row_count == 1
        assert result.data[0]["orders._count"] == 6

    async def test_regex_literal_extension_column(self, trino_env: SlayerQueryEngine) -> None:
        """A ``(?:...)`` regex literal and a ``%`` LIKE pattern execute verbatim."""
        query = SlayerQuery(
            source_model=ModelExtension(
                source_name="orders",
                columns=[Column(
                    name="rx",
                    sql="CASE WHEN status LIKE '%pend%' "
                        "OR status = '(?i)(?:too complicated|too complex)' THEN 1 ELSE 0 END",
                    type=DataType.DOUBLE,
                )],
            ),
            dimensions=[ColumnRef(name="rx")],
            measures=[ModelMeasure(formula="*:count")],
        )
        result = await trino_env.execute(query=query)
        assert {int(r["orders.rx"]): r["orders._count"] for r in result.data} == {1: 2, 0: 4}

    async def test_sum_measure(self, trino_env: SlayerQueryEngine) -> None:
        result = await trino_env.execute(query=SlayerQuery.model_validate({"source_model": "orders", "measures": [{"formula": "total:sum"}]}))
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(875.0)

    async def test_functional_aggregation_parity(self, trino_env: SlayerQueryEngine) -> None:
        colon = await trino_env.execute(query=SlayerQuery.model_validate({"source_model": "orders", "measures": [{"formula": "total:sum"}]}))
        func = await trino_env.execute(query=SlayerQuery.model_validate({"source_model": "orders", "measures": [{"formula": "sum(total)"}]}))
        assert float(func.data[0]["orders.total_sum"]) == float(colon.data[0]["orders.total_sum"])

    async def test_string_hygiene_functions_execute(self, trino_env: SlayerQueryEngine) -> None:
        for filt in ("lower(status) = 'completed'", "substr(status, 1, 4) = 'comp'"):
            result = await trino_env.execute(query=SlayerQuery.model_validate({
                "source_model": "orders", "measures": [{"formula": "*:count"}], "filters": [filt],
            }))
            assert result.data[0]["orders._count"] == 3, result.sql

    async def test_trunc_truncates_toward_zero(self, trino_env: SlayerQueryEngine) -> None:
        for filt, expected in (("trunc(total) >= 100", 4), ("trunc(total / 40.0) == 2", 1)):
            result = await trino_env.execute(query=SlayerQuery.model_validate({
                "source_model": "orders", "measures": [{"formula": "*:count"}], "filters": [filt],
            }))
            assert result.data[0]["orders._count"] == expected, result.sql

    async def test_avg_measure(self, trino_env: SlayerQueryEngine) -> None:
        result = await trino_env.execute(query=SlayerQuery.model_validate({"source_model": "orders", "measures": [{"formula": "avg_amount:avg"}]}))
        assert float(result.data[0]["orders.avg_amount_avg"]) == pytest.approx(875.0 / 6)

    async def test_group_by_status(self, trino_env: SlayerQueryEngine) -> None:
        assert await _count_by(trino_env, dimension="status") == {"completed": 3, "pending": 2, "cancelled": 1}

    async def test_filter_equals(self, trino_env: SlayerQueryEngine) -> None:
        result = await trino_env.execute(query=SlayerQuery.model_validate({
            "source_model": "orders", "measures": [{"formula": "*:count"}], "filters": ["status == 'completed'"],
        }))
        assert result.data[0]["orders._count"] == 3

    async def test_filter_gt(self, trino_env: SlayerQueryEngine) -> None:
        result = await trino_env.execute(query=SlayerQuery.model_validate({
            "source_model": "orders", "measures": [{"formula": "*:count"}], "filters": ["total > 100"],
        }))
        assert result.data[0]["orders._count"] == 3

    async def test_composite_filter(self, trino_env: SlayerQueryEngine) -> None:
        result = await trino_env.execute(query=SlayerQuery.model_validate({
            "source_model": "orders", "measures": [{"formula": "*:count"}],
            "filters": ["status == 'completed' or status == 'pending'"],
        }))
        assert result.data[0]["orders._count"] == 5

    async def test_order_by_desc(self, trino_env: SlayerQueryEngine) -> None:
        result = await trino_env.execute(query=SlayerQuery.model_validate({
            "source_model": "orders", "measures": [{"formula": "*:count"}], "dimensions": [{"name": "status"}],
            "order": [{"column": {"name": "count"}, "direction": "desc"}],
        }))
        assert result.data[0]["orders.status"] == "completed"

    async def test_limit(self, trino_env: SlayerQueryEngine) -> None:
        result = await trino_env.execute(query=SlayerQuery.model_validate({
            "source_model": "orders", "measures": [{"formula": "*:count"}], "dimensions": [{"name": "status"}], "limit": 2,
        }))
        assert result.row_count == 2

    async def test_limit_offset(self, trino_env: SlayerQueryEngine) -> None:
        result = await trino_env.execute(query=SlayerQuery.model_validate({
            "source_model": "orders", "measures": [{"formula": "*:count"}], "dimensions": [{"name": "status"}],
            "order": [{"column": {"name": "status"}, "direction": "asc"}], "limit": 2, "offset": 1,
        }))
        assert [r["orders.status"] for r in result.data] == ["completed", "pending"]

    async def test_multiple_measures(self, trino_env: SlayerQueryEngine) -> None:
        result = await trino_env.execute(query=SlayerQuery.model_validate({
            "source_model": "orders", "measures": [{"formula": "*:count"}, {"formula": "total:sum"}],
            "dimensions": [{"name": "status"}],
        }))
        completed = next(r for r in result.data if r["orders.status"] == "completed")
        assert completed["orders._count"] == 3
        assert float(completed["orders.total_sum"]) == pytest.approx(450.0)

    @pytest.mark.parametrize("column", ["created_at", "created_on"])
    async def test_month_granularity(self, trino_env: SlayerQueryEngine, column: str) -> None:
        assert await _week_buckets(trino_env, column=column, granularity="month") == {
            "2024-01-01": 2, "2024-02-01": 2, "2024-03-01": 2,
        }

    @pytest.mark.parametrize("column", ["created_at", "created_on"])
    async def test_week_granularity(self, trino_env: SlayerQueryEngine, column: str) -> None:
        assert await _week_buckets(trino_env, column=column, granularity="week") == {
            "2024-01-15": 2, "2024-02-05": 1, "2024-02-12": 1, "2024-02-26": 1, "2024-03-04": 1,
        }

    @pytest.mark.parametrize("column", ["created_at", "created_on"])
    async def test_week_sunday_granularity(self, trino_env: SlayerQueryEngine, column: str) -> None:
        assert await _week_buckets(trino_env, column=column, granularity="week_sunday") == {
            "2024-01-14": 2, "2024-02-04": 1, "2024-02-11": 1, "2024-02-25": 1, "2024-03-10": 1,
        }

    async def test_time_dimension_with_date_range(self, trino_env: SlayerQueryEngine) -> None:
        result = await trino_env.execute(query=SlayerQuery.model_validate({
            "source_model": "orders", "measures": [{"formula": "*:count"}],
            "time_dimensions": [{
                "dimension": {"name": "created_at"}, "granularity": "month",
                "date_range": ["2024-01-01", "2024-02-28"],
            }],
        }))
        assert sum(r["orders._count"] for r in result.data) == 4

    async def test_date_range_on_date_column(self, trino_env: SlayerQueryEngine) -> None:
        result = await trino_env.execute(query=SlayerQuery.model_validate({
            "source_model": "orders", "measures": [{"formula": "*:count"}],
            "time_dimensions": [{
                "dimension": {"name": "created_on"}, "granularity": "day",
                "date_range": ["2024-02-10", "2024-03-01"],
            }],
        }))
        assert sum(r["orders._count"] for r in result.data) == 3

    async def test_time_shift_with_date_range(self, trino_env: SlayerQueryEngine) -> None:
        result = await trino_env.execute(query=SlayerQuery(
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
        ))
        assert result.row_count == 1
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(375.0)
        assert float(result.data[0]["orders.prev_month"]) == pytest.approx(200.0)

    async def test_consecutive_periods_with_boolean_predicate(self, trino_env: SlayerQueryEngine) -> None:
        result = await trino_env.execute(query=SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[
                ModelMeasure(formula="total:sum"),
                ModelMeasure(formula="consecutive_periods(total:sum > 200)", name="positive_run"),
            ],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        ))
        assert [r["orders.positive_run"] for r in result.data] == [1, 0, 1]

    @pytest.mark.parametrize("column,granularity", CALENDAR_CASES)
    async def test_consecutive_periods_breaks_on_calendar_gap(
        self, trino_env: SlayerQueryEngine, column: str, granularity: str,
    ) -> None:
        await assert_calendar_streak(trino_env, column=column, granularity=granularity)

    async def test_change_with_date_range(self, trino_env: SlayerQueryEngine) -> None:
        result = await trino_env.execute(query=SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
                date_range=["2024-03-01", "2024-03-31"],
            )],
            measures=[ModelMeasure(formula="total:sum"), ModelMeasure(formula="change(total:sum)", name="amount_change")],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        ))
        assert result.row_count == 1
        assert float(result.data[0]["orders.amount_change"]) == pytest.approx(175.0)

    async def test_change_pct_with_date_range(self, trino_env: SlayerQueryEngine) -> None:
        result = await trino_env.execute(query=SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
                date_range=["2024-03-01", "2024-03-31"],
            )],
            measures=[ModelMeasure(formula="total:sum"), ModelMeasure(formula="change_pct(total:sum)", name="pct")],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        ))
        assert result.row_count == 1
        assert float(result.data[0]["orders.pct"]) == pytest.approx(0.875)

    async def test_multiple_date_range_shifts(self, trino_env: SlayerQueryEngine) -> None:
        result = await trino_env.execute(query=SlayerQuery(
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
        ))
        assert result.row_count == 1
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(200.0)
        assert float(result.data[0]["orders.prev"]) == pytest.approx(300.0)
        assert float(result.data[0]["orders.next"]) == pytest.approx(375.0)


@pytest.mark.integration
class TestTrinoInvariants:
    async def test_sum_of_grouped_counts_equals_total(self, trino_env: SlayerQueryEngine) -> None:
        total = await trino_env.execute(query=SlayerQuery.model_validate({"source_model": "orders", "measures": [{"formula": "*:count"}]}))
        grouped = await _count_by(trino_env, dimension="status")
        assert sum(grouped.values()) == total.data[0]["orders._count"] == 6

    async def test_adding_a_measure_keeps_rows(self, trino_env: SlayerQueryEngine) -> None:
        def query(*measures: str) -> SlayerQuery:
            return SlayerQuery.model_validate({
                "source_model": "orders", "dimensions": ["status"],
                "measures": [{"formula": f} for f in measures],
            })

        narrow = await trino_env.execute(query=query("*:count"))
        wide = await trino_env.execute(query=query("*:count", "total:sum", "total:median"))
        assert wide.row_count == narrow.row_count == 3
        assert {r["orders.status"]: r["orders._count"] for r in wide.data} == {
            r["orders.status"]: r["orders._count"] for r in narrow.data
        }


# ---------------------------------------------------------------------------
# Median / percentile: Trino's APPROX_PERCENTILE
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestTrinoMedianPercentile:
    async def test_median_and_percentiles_equal_trinos_own(self, trino_env: SlayerQueryEngine, trino_direct) -> None:
        result = await trino_env.execute(query=SlayerQuery.model_validate({
            "source_model": "orders",
            "measures": [
                {"formula": "total:median", "name": "p50"},
                {"formula": "total:percentile(p=0.9)", "name": "p90"},
                {"formula": "total:percentile(p=0.25)", "name": "p25"},
            ],
        }))
        ((p50, p90, p25),) = trino_direct(
            "SELECT approx_percentile(amount, 0.5), approx_percentile(amount, 0.9), "
            "approx_percentile(amount, 0.25) FROM orders"
        )
        row = result.data[0]
        assert (float(row["orders.p50"]), float(row["orders.p90"]), float(row["orders.p25"])) == (p50, p90, p25)

    async def test_grouped_median_equals_trinos_own(self, trino_env: SlayerQueryEngine, trino_direct) -> None:
        result = await trino_env.execute(query=SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["status"],
            "measures": [{"formula": "total:median", "name": "p50"}],
        }))
        expected = dict(trino_direct("SELECT status, approx_percentile(amount, 0.5) FROM orders GROUP BY status"))
        assert {r["orders.status"]: float(r["orders.p50"]) for r in result.data} == expected

    async def test_p90_is_approximate_not_interpolated(self, trino_env: SlayerQueryEngine) -> None:
        result = await trino_env.execute(query=SlayerQuery.model_validate({
            "source_model": "pct_amounts", "measures": [{"formula": "amount:percentile(p=0.9)", "name": "p90"}],
        }))
        assert float(result.data[0]["pct_amounts.p90"]) == pytest.approx(300.0)

    async def test_emits_approx_percentile(self, trino_env: SlayerQueryEngine) -> None:
        dry = await trino_env.execute(
            query=SlayerQuery.model_validate({"source_model": "orders", "measures": [{"formula": "total:median"}]}), dry_run=True,
        )
        assert dry.sql is not None
        assert "APPROX_PERCENTILE(" in dry.sql.upper()
        assert "PERCENTILE_CONT" not in dry.sql.upper()


# ---------------------------------------------------------------------------
# Native statistics and approximate distinct
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestTrinoStatAggregations:
    @pytest.mark.parametrize("formula,expected", [
        ("total:stddev_samp", statistics.stdev(_AMOUNTS)),
        ("total:stddev_pop", statistics.pstdev(_AMOUNTS)),
        ("total:var_samp", statistics.variance(_AMOUNTS)),
        ("total:var_pop", statistics.pvariance(_AMOUNTS)),
        ("total:corr(other=customer_id)", statistics.correlation(_AMOUNTS, _CUSTOMER_IDS)),
        ("total:covar_samp(other=customer_id)", statistics.covariance(_AMOUNTS, _CUSTOMER_IDS)),
        (
            "total:covar_pop(other=customer_id)",
            statistics.covariance(_AMOUNTS, _CUSTOMER_IDS) * (len(_AMOUNTS) - 1) / len(_AMOUNTS),
        ),
    ])
    async def test_native_statistic(self, trino_env: SlayerQueryEngine, formula: str, expected: float) -> None:
        result = await trino_env.execute(query=SlayerQuery.model_validate({
            "source_model": "orders", "measures": [{"formula": formula, "name": "v"}],
        }))
        assert float(result.data[0]["orders.v"]) == pytest.approx(expected, rel=1e-6)

    async def test_approx_distinct(self, trino_env: SlayerQueryEngine) -> None:
        query = SlayerQuery.model_validate({
            "source_model": "orders", "measures": [{"formula": "customer_id:count_distinct_approx", "name": "v"}],
        })
        result = await trino_env.execute(query=query)
        assert int(result.data[0]["orders.v"]) == 3
        assert "APPROX_DISTINCT(" in (result.sql or "").upper()


# ---------------------------------------------------------------------------
# Cross-model + multistage (hand-declared joins)
# ---------------------------------------------------------------------------

_CROSS_ORDERS = [
    (1, "completed", 100.0, 1, "2024-01-15 10:00:00"),
    (2, "completed", 200.0, 1, "2024-01-20 11:00:00"),
    (3, "pending", 50.0, 2, "2024-02-10 09:00:00"),
    (4, "completed", 150.0, 2, "2024-02-15 14:00:00"),
    (5, "completed", 300.0, 3, "2024-03-01 08:00:00"),
    (6, "pending", 25.0, 1, "2024-03-10 16:00:00"),
]


@pytest.fixture
def trino_cross_model_env(trino_container, tmp_path):
    seed = [
        "CREATE TABLE customers (id INTEGER, name VARCHAR, region VARCHAR, score DOUBLE)",
        "CREATE TABLE orders (id INTEGER, status VARCHAR, amount DOUBLE, customer_id INTEGER, "
        "created_at TIMESTAMP(6), created_on DATE)",
        "INSERT INTO customers VALUES (1, 'Alice', 'US', 90.0), (2, 'Bob', 'EU', 60.0), (3, 'Charlie', 'US', 80.0)",
        f"INSERT INTO orders VALUES {_orders_rows(_CROSS_ORDERS)}",
    ]
    with _seeded_schema(trino_container, seed) as schema:
        storage = _storage(str(tmp_path), _ds_config(trino_container, schema), [
            SlayerModel(
                name="orders", sql_table="orders", data_source=_DS, default_time_dimension="created_at",
                columns=[
                    Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                    Column(name="status", sql="status", type=DataType.TEXT),
                    Column(name="customer_id", sql="customer_id", type=DataType.INT),
                    Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                    Column(name="amount", sql="amount", type=DataType.DOUBLE),
                    Column(name="total", sql="amount", type=DataType.DOUBLE),
                ],
                joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
            ),
            SlayerModel(
                name="customers", sql_table="customers", data_source=_DS,
                columns=[
                    Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                    Column(name="name", sql="name", type=DataType.TEXT),
                    Column(name="region", sql="region", type=DataType.TEXT),
                    Column(name="avg_score", sql="score", type=DataType.DOUBLE),
                ],
            ),
        ])
        yield SlayerQueryEngine(storage=storage)


@pytest.mark.integration
class TestCrossModelAndMultistageTrino:
    async def test_cross_model_measure(self, trino_cross_model_env: SlayerQueryEngine) -> None:
        result = await trino_cross_model_env.execute(query=SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="customers.avg_score:avg")],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        ))
        assert result.row_count == 3
        global_avg = pytest.approx((90.0 + 60.0 + 80.0) / 3)
        assert [float(r["orders.customers.avg_score_avg"]) for r in result.data] == [global_avg] * 3

    async def test_query_list_named(self, trino_cross_model_env: SlayerQueryEngine) -> None:
        inner = SlayerQuery(
            name="monthly", source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="total:sum")],
        )
        outer = SlayerQuery(source_model="monthly", measures=[ModelMeasure(formula="*:count")])
        result = await trino_cross_model_env.execute(query=[inner, outer])
        assert result.data[0]["monthly._count"] == 3

    async def test_create_model_from_query(self, trino_cross_model_env: SlayerQueryEngine) -> None:
        source = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="total:sum")],
        )
        saved = await trino_cross_model_env.create_model_from_query(query=source, name="trino_monthly")
        assert saved.source_queries is not None
        result = await trino_cross_model_env.execute(
            query=SlayerQuery(source_model="trino_monthly", measures=[ModelMeasure(formula="*:count")])
        )
        assert result.data[0]["trino_monthly._count"] == 3

    async def test_sql_dimension(self, trino_cross_model_env: SlayerQueryEngine) -> None:
        result = await trino_cross_model_env.execute(query=SlayerQuery(
            source_model=ModelExtension.model_validate({
                "source_name": "orders",
                "columns": [{"name": "tier", "sql": "CASE WHEN amount > 100 THEN 'high' ELSE 'low' END"}],
            }),
            dimensions=[ColumnRef(name="tier")],
            measures=[ModelMeasure(formula="*:count")],
        ))
        assert {r["orders.tier"]: r["orders._count"] for r in result.data} == {"high": 3, "low": 3}

    async def test_grouped_counts_across_join_sum_to_total(self, trino_cross_model_env: SlayerQueryEngine) -> None:
        result = await trino_cross_model_env.execute(query=SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["customers.region"],
            "measures": [{"formula": "*:count", "name": "n"}],
        }))
        assert {r["orders.customers.region"]: int(r["orders.n"]) for r in result.data} == {"US": 4, "EU": 2}

    async def test_joined_measure_keeps_rows(self, trino_cross_model_env: SlayerQueryEngine) -> None:
        def query(*measures: str) -> SlayerQuery:
            return SlayerQuery.model_validate({
                "source_model": "orders", "dimensions": ["status"],
                "measures": [{"formula": f} for f in measures],
            })

        narrow = await trino_cross_model_env.execute(query=query("*:count"))
        wide = await trino_cross_model_env.execute(query=query("*:count", "customers.avg_score:avg"))
        assert wide.row_count == narrow.row_count == 2
        assert {r["orders.status"]: r["orders._count"] for r in wide.data} == {"completed": 4, "pending": 2}


# ---------------------------------------------------------------------------
# Federation: one Trino datasource joining Postgres and MySQL catalogs
# ---------------------------------------------------------------------------

_FED_CATALOGS = {
    "postgresql": "connector.name=postgresql\nconnection-url=jdbc:postgresql://pg:5432/fed\n"
                  "connection-user=slayer\nconnection-password=slayer\n",
    "mysql": "connector.name=mysql\nconnection-url=jdbc:mysql://mysql:3306\n"
             "connection-user=root\nconnection-password=root\n",
}
_FED_SEED = [
    "CREATE TABLE postgresql.public.customers (id INTEGER, name VARCHAR, region VARCHAR)",
    "INSERT INTO postgresql.public.customers VALUES (1, 'Acme Corp', 'US'), (2, 'Globex', 'EU'), (3, 'Initech', 'US')",
    "CREATE TABLE mysql.shop.orders (id INTEGER, status VARCHAR, amount DOUBLE, customer_id INTEGER)",
    "INSERT INTO mysql.shop.orders VALUES "
    + ", ".join(f"({i}, '{status}', {amount}, {customer})" for i, status, amount, customer, _ in _BASE_ORDERS),
]


@pytest.fixture(scope="module")
def trino_federation_env(tmp_path_factory):
    """A Trino whose ``postgresql`` / ``mysql`` catalogs are live containers, as one SLayer datasource."""
    catalog_dir = tmp_path_factory.mktemp("trino_catalogs")
    trino = TrinoContainer(_IMAGE, container_start_timeout=180)
    for name, properties in _FED_CATALOGS.items():
        path = catalog_dir / f"{name}.properties"
        path.write_text(properties)
        trino.with_volume_mapping(str(path), f"/etc/trino/catalog/{name}.properties")
    with Network() as network:
        postgres = PostgresContainer(
            "postgres:16-alpine", username="slayer", dbname="fed",
            password="slayer",  # NOSONAR(S2068) — testcontainer credentials, not real secrets
        ).with_network(network).with_network_aliases("pg")
        mysql = MySqlContainer(
            "mysql:8.0", dbname="shop",
            root_password="root",  # NOSONAR(S2068) — testcontainer credentials, not real secrets
        ).with_network(network).with_network_aliases("mysql")
        with postgres, mysql, trino.with_network(network):
            _run(trino, _FED_SEED, catalog="postgresql")
            yield SlayerQueryEngine(storage=_storage(
                str(tmp_path_factory.mktemp("trino_fed")),
                _ds_config(trino, "public", name="fed", catalog="postgresql"),
                [
                    SlayerModel(
                        name="orders", sql_table="mysql.shop.orders", data_source="fed",
                        columns=[
                            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                            Column(name="status", sql="status", type=DataType.TEXT),
                            Column(name="customer_id", sql="customer_id", type=DataType.INT),
                            Column(name="amount", sql="amount", type=DataType.DOUBLE),
                        ],
                        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
                    ),
                    SlayerModel(
                        name="customers", sql_table="postgresql.public.customers", data_source="fed",
                        columns=[
                            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                            Column(name="region", sql="region", type=DataType.TEXT),
                        ],
                    ),
                ],
            ))
            engine_factory.reset_cache()


@pytest.mark.integration
class TestTrinoFederation:
    async def test_join_across_catalogs(self, trino_federation_env: SlayerQueryEngine) -> None:
        result = await trino_federation_env.execute(SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["customers.region"],
            "measures": [{"formula": "*:count", "name": "n"}, {"formula": "amount:sum", "name": "revenue"}],
        }))
        assert {
            r["orders.customers.region"]: (int(r["orders.n"]), float(r["orders.revenue"])) for r in result.data
        } == {"US": (4, 675.0), "EU": (2, 200.0)}

    async def test_grouped_counts_sum_to_total(self, trino_federation_env: SlayerQueryEngine) -> None:
        grouped = await trino_federation_env.execute(SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["customers.region", "status"],
            "measures": [{"formula": "*:count", "name": "n"}],
        }))
        total = await trino_federation_env.execute(SlayerQuery.model_validate({
            "source_model": "orders", "measures": [{"formula": "*:count", "name": "n"}],
        }))
        assert sum(int(r["orders.n"]) for r in grouped.data) == int(total.data[0]["orders.n"]) == len(_BASE_ORDERS)


@pytest.fixture
def trino_derived_chain_env(trino_container, tmp_path):
    seed = [
        "CREATE TABLE b_tbl (id INTEGER, foo_raw DOUBLE)",
        "CREATE TABLE a_tbl (id INTEGER, bar DOUBLE, b_id INTEGER, raw_a DOUBLE)",
        "INSERT INTO b_tbl VALUES (1, 200.0), (2, 50.0)",
        "INSERT INTO a_tbl VALUES (10, 4.0, 1, 100.0), (11, 1.0, 2, 5.0)",
    ]
    with _seeded_schema(trino_container, seed) as schema:
        storage = _storage(str(tmp_path), _ds_config(trino_container, schema), [
            SlayerModel(
                name="b_tbl", data_source=_DS, sql_table="b_tbl",
                columns=[
                    Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                    Column(name="foo_raw", sql="foo_raw", type=DataType.DOUBLE),
                    Column(name="foo_normalized", sql="foo_raw / 100.0", type=DataType.DOUBLE),
                ],
            ),
            SlayerModel(
                name="a_tbl", data_source=_DS, sql_table="a_tbl",
                columns=[
                    Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                    Column(name="bar", sql="bar", type=DataType.DOUBLE),
                    Column(name="b_id", sql="b_id", type=DataType.INT),
                    Column(name="raw_a", sql="raw_a", type=DataType.DOUBLE),
                    Column(name="ratio_using_derived", sql="a_tbl.bar / b_tbl.foo_normalized", type=DataType.DOUBLE),
                ],
                joins=[ModelJoin(target_model="b_tbl", join_pairs=[["b_id", "id"]])],
            ),
        ])
        yield SlayerQueryEngine(storage=storage)


@pytest.mark.integration
async def test_cross_model_derived_columnsql(trino_derived_chain_env: SlayerQueryEngine) -> None:
    response = await trino_derived_chain_env.execute(SlayerQuery(
        source_model="a_tbl",
        dimensions=[ColumnRef(name="id"), ColumnRef(name="ratio_using_derived")],
        order=[OrderItem(column=ColumnRef(name="id"), direction="asc")],
    ))
    assert [float(r["a_tbl.ratio_using_derived"]) for r in response.data] == [pytest.approx(2.0)] * 2


# ---------------------------------------------------------------------------
# log10 / log2, windowed-column filter
# ---------------------------------------------------------------------------


@pytest.fixture
def trino_log_env(trino_container, tmp_path):
    seed = ["CREATE TABLE orders (id INTEGER, amount DOUBLE)", "INSERT INTO orders VALUES (1, 100.0), (2, 256.0), (3, 300.0)"]
    with _seeded_schema(trino_container, seed) as schema:
        storage = _storage(str(tmp_path), _ds_config(trino_container, schema), [SlayerModel(
            name="orders", sql_table="orders", data_source=_DS,
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="log_amount", sql="log10(amount)", type=DataType.DOUBLE),
                Column(name="log2_amount", sql="log2(amount)", type=DataType.DOUBLE),
            ],
        )])
        yield SlayerQueryEngine(storage=storage)


@pytest.mark.integration
async def test_log10_log2_round_trip(trino_log_env: SlayerQueryEngine) -> None:
    query = SlayerQuery.model_validate({
        "source_model": "orders",
        "measures": [{"formula": "log_amount:max", "name": "l10"}, {"formula": "log2_amount:max", "name": "l2"}],
    })
    result = await trino_log_env.execute(query)
    assert float(result.data[0]["orders.l10"]) == pytest.approx(math.log10(300.0), rel=1e-9)
    assert float(result.data[0]["orders.l2"]) == pytest.approx(math.log2(300.0), rel=1e-9)
    sql = (result.sql or "").lower().replace(" ", "")
    assert "log10(" in sql, result.sql
    assert "log2(" in sql, result.sql
    assert "log(10," not in sql, result.sql
    assert "log(2," not in sql, result.sql


@pytest.fixture
def trino_planets_env(trino_container, tmp_path):
    seed = [
        "CREATE TABLE planets (id INTEGER, name VARCHAR, mass DOUBLE)",
        "INSERT INTO planets VALUES (1, 'Mercury', 0.33), (2, 'Venus', 4.87), (3, 'Earth', 5.97), "
        "(4, 'Mars', 0.642), (5, 'Jupiter', 1898.0), (6, 'Saturn', 568.0)",
    ]
    with _seeded_schema(trino_container, seed) as schema:
        storage = _storage(str(tmp_path), _ds_config(trino_container, schema), [SlayerModel(
            name="planets", sql_table="planets", data_source=_DS,
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
                Column(name="mass", sql="mass", type=DataType.DOUBLE),
                Column(name="rn", sql="row_number() over (order by mass desc)", type=DataType.DOUBLE),
            ],
        )])
        yield SlayerQueryEngine(storage=storage)


@pytest.mark.integration
async def test_filter_on_windowed_column_raises(trino_planets_env: SlayerQueryEngine) -> None:
    query = SlayerQuery.model_validate({"source_model": "planets", "dimensions": ["name"], "filters": ["rn <= 3"]})
    with pytest.raises(ValueError, match="(?i)window function|rank"):
        await trino_planets_env.execute(query)


# ---------------------------------------------------------------------------
# Mode-A {var} escaping: Trino string literals take '' doubling, backslash is literal
# ---------------------------------------------------------------------------

_ESC_ROWS = [(1, "a\\'b", 10.0), (2, "plain", 20.0), (3, 'say "hi"', 30.0), (4, "back\\slash", 40.0)]


@pytest.fixture(scope="module")
def _trino_esc_storage(trino_container, tmp_path_factory):
    seed = [
        "CREATE TABLE esc (id INTEGER, status VARCHAR, amount DOUBLE)",
        "INSERT INTO esc VALUES " + ", ".join(f"({i}, {sql_literal(s)}, {a})" for i, s, a in _ESC_ROWS),
    ]
    with _seeded_schema(trino_container, seed) as schema:
        yield _storage(str(tmp_path_factory.mktemp("trino_esc")), _ds_config(trino_container, schema), [SlayerModel(
            name="esc", sql_table="esc", data_source=_DS, filters=["status = '{v}'"],
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )])


@pytest.mark.integration
class TestTrinoModeAEscaping:
    @pytest.mark.parametrize("value,expected", [("a\\'b", 10.0), ('say "hi"', 30.0), ("back\\slash", 40.0)])
    async def test_value_matches_only_its_row(self, _trino_esc_storage, value: str, expected: float) -> None:
        resp = await SlayerQueryEngine(storage=_trino_esc_storage).execute(SlayerQuery.model_validate({
            "source_model": "esc", "measures": [{"formula": "amount:sum"}], "variables": {"v": value},
        }))
        assert resp.row_count == 1
        assert float(resp.data[0]["esc.amount_sum"]) == expected

    async def test_breakout_attempt_matches_nothing(self, _trino_esc_storage) -> None:
        resp = await SlayerQueryEngine(storage=_trino_esc_storage).execute(SlayerQuery.model_validate({
            "source_model": "esc", "measures": [{"formula": "*:count"}], "variables": {"v": "x' OR '1'='1"},
        }))
        assert int(resp.data[0]["esc._count"]) == 0


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

_INGEST_SEED = [
    "CREATE TABLE customers (id INTEGER, name VARCHAR)",
    "CREATE TABLE orders (id INTEGER, customer_id BIGINT, quantity INTEGER, amount DOUBLE, "
    "price DECIMAL(10, 2), status VARCHAR, created_at TIMESTAMP(6), "
    "created_tz TIMESTAMP(6) WITH TIME ZONE, order_date DATE, is_paid BOOLEAN)",
    "COMMENT ON TABLE orders IS 'All orders'",
    "COMMENT ON COLUMN orders.amount IS 'Order amount'",
    "INSERT INTO customers VALUES (1, 'Acme')",
    "INSERT INTO orders VALUES (1, 1, 5, 100.0, 9.99, 'completed', TIMESTAMP '2024-01-01 10:00:00', "
    "TIMESTAMP '2024-01-01 10:00:00 UTC', DATE '2024-01-01', TRUE)",
]


@pytest.fixture(scope="module")
def trino_ingest_ds(trino_container):
    with _seeded_schema(trino_container, _INGEST_SEED) as schema:
        yield _ds_config(trino_container, schema)


def _ingested(ds: DatasourceConfig, name: str) -> SlayerModel:
    return next(m for m in ingest_datasource(datasource=ds) if m.name == name)


async def _stored_dumps(storage: YAMLStorage, *names: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for name in names:
        model = await storage.get_model(name)
        assert model is not None, name
        out[name] = model.model_dump()
    return out


@pytest.mark.integration
class TestTrinoIngestion:
    def test_column_types(self, trino_ingest_ds) -> None:
        columns = {c.name: c for c in _ingested(trino_ingest_ds, "orders").columns}
        assert {name: c.type for name, c in columns.items()} == {
            "id": DataType.INT, "customer_id": DataType.INT, "quantity": DataType.INT,
            "amount": DataType.DOUBLE, "price": DataType.DOUBLE, "status": DataType.TEXT,
            "created_at": DataType.TIMESTAMP, "created_tz": DataType.TIMESTAMP,
            "order_date": DataType.DATE, "is_paid": DataType.BOOLEAN,
        }
        assert "DECIMAL" in (columns["price"].db_type or "").upper()

    def test_comments_imported(self, trino_ingest_ds) -> None:
        orders = _ingested(trino_ingest_ds, "orders")
        assert orders.description == "All orders"
        by_name = {c.name: c for c in orders.columns}
        assert by_name["amount"].description == "Order amount"
        assert by_name["status"].description is None
        assert _ingested(trino_ingest_ds, "customers").description is None

    def test_no_keys_or_joins_discovered(self, trino_ingest_ds) -> None:
        orders = _ingested(trino_ingest_ds, "orders")
        assert [c.name for c in orders.columns if c.primary_key] == []
        assert orders.joins == []

    async def test_reingest_is_idempotent(self, trino_ingest_ds, tmp_path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(trino_ingest_ds)
        await ingest_datasource_idempotent(datasource=trino_ingest_ds, storage=storage)
        first = await _stored_dumps(storage, "orders", "customers")
        await ingest_datasource_idempotent(datasource=trino_ingest_ds, storage=storage)
        assert await _stored_dumps(storage, "orders", "customers") == first


# ---------------------------------------------------------------------------
# Decimal native-type preservation
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def _trino_decimal_storage(trino_container, tmp_path_factory):
    seed = [
        "CREATE TABLE decimal_orders (id INTEGER, amount DECIMAL(18, 2), short_amount DECIMAL(16, 4))",
        "INSERT INTO decimal_orders VALUES (1, DECIMAL '90071992547409.91', DECIMAL '900719925474.0992'), "
        "(2, DECIMAL '0.02', DECIMAL '0.0001')",
    ]
    with _seeded_schema(trino_container, seed) as schema:
        datasource = _ds_config(trino_container, schema)
        yield _storage(
            str(tmp_path_factory.mktemp("trino_decimal")), datasource, [_ingested(datasource, "decimal_orders")],
        )


@pytest.mark.integration
@pytest.mark.parametrize("measure,key,expected", [
    ("amount:sum", "decimal_orders.amount_sum", Decimal("90071992547409.93")),
    ("short_amount:sum", "decimal_orders.short_amount_sum", Decimal("900719925474.0993")),
])
async def test_decimal_sum_preserves_exact_value(_trino_decimal_storage, measure: str, key: str, expected: Decimal) -> None:
    engine = SlayerQueryEngine(storage=_trino_decimal_storage)
    query = SlayerQuery.model_validate({"source_model": "decimal_orders", "measures": [{"formula": measure}]})
    result = await engine.execute(query=query)
    assert " AS DOUBLE" not in (result.sql or "").upper(), result.sql
    assert isinstance(result.data[0][key], Decimal)
    assert result.data[0][key] == expected


# ---------------------------------------------------------------------------
# Per-statement timeout: query_max_run_time on the driver connection
# ---------------------------------------------------------------------------

# Runs for many seconds, bounded so a missing timeout fails rather than hangs.
_SLOW_SQL = (
    "SELECT count(*) FROM UNNEST(sequence(1, 10000)) a(x) CROSS JOIN UNNEST(sequence(1, 10000)) b(y) "
    "CROSS JOIN UNNEST(sequence(1, 100)) c(z) WHERE x * y * z % 7 = 3"
)
_SHOW_RUN_TIME = "SHOW SESSION LIKE 'query_max_run_time'"
_DURATION_UNITS = {"ns": 1e-9, "us": 1e-6, "ms": 1e-3, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}


def _seconds(text: str) -> float:
    match = re.fullmatch(r"\s*([\d.]+)\s*([a-z]+)\s*", text)
    assert match, text
    return float(match[1]) * _DURATION_UNITS[match[2]]


def _run_time_row(row: Any) -> dict[str, str]:
    """``SHOW SESSION`` row as ``{"value": ..., "default": ...}``."""
    items = row.items() if isinstance(row, dict) else zip(("name", "value", "default"), row)
    return {str(k).lower(): str(v) for k, v in items}


def _pooled_run_time(client: SlayerSQLClient) -> dict[str, str]:
    with client._get_sync_engine_for_client().connect() as conn:
        return _run_time_row(tuple(conn.exec_driver_sql(_SHOW_RUN_TIME).one()))


def _configured_ds(container) -> DatasourceConfig:
    props = quote(json.dumps({"query_max_run_time": "30m"}))
    return DatasourceConfig(
        name="trino_configured", type="trino",
        connection_string=f"{_url(container, 'default')}?session_properties={props}",
    )


def _spy_run_time(monkeypatch: pytest.MonkeyPatch) -> list[str | None]:
    """Record ``query_max_run_time`` on the driver connection right after the dialect sets it."""
    seen: list[str | None] = []
    dialect_cls = type(dialect_for_ds_type("trino"))
    real_set = dialect_cls.set_connection_timeout

    def spy(self, dbapi_connection: Any, timeout_seconds: int) -> object:
        prior = real_set(self, dbapi_connection, timeout_seconds)
        seen.append(dbapi_connection._client_session.properties.get("query_max_run_time"))
        return prior

    monkeypatch.setattr(dialect_cls, "set_connection_timeout", spy)
    return seen


def _set_session_count(container) -> int:
    ((count,),) = _run(container, [
        "SELECT count(*) FROM system.runtime.queries WHERE upper(trim(query)) LIKE 'SET SESSION%'",
    ])
    return int(count)


@pytest.mark.integration
@pytest.mark.timeout(240)
class TestTrinoStatementTimeout:
    async def test_session_property_in_force_during_query(self, trino_container) -> None:
        client = SlayerSQLClient(datasource=_ds_config(trino_container, "default"))
        row = _run_time_row((await client.execute(sql=_SHOW_RUN_TIME, timeout_seconds=120)).rows[0])
        assert _seconds(row["value"]) == 120

    async def test_timeout_stops_long_query(self, trino_container) -> None:
        client = SlayerSQLClient(datasource=_ds_config(trino_container, "default"))
        started = time.monotonic()
        with pytest.raises(Exception, match="EXCEEDED_TIME_LIMIT"):
            await client.execute(sql=_SLOW_SQL, timeout_seconds=1)
        assert time.monotonic() - started < 30

    def test_timeout_stops_long_query_sync(self, trino_container) -> None:
        client = SlayerSQLClient(datasource=_ds_config(trino_container, "default"))
        started = time.monotonic()
        with pytest.raises(Exception, match="EXCEEDED_TIME_LIMIT"):
            client.execute_sync(_SLOW_SQL, timeout_seconds=1)
        assert time.monotonic() - started < 30

    def test_setting_restored_on_pooled_connection(self, trino_container) -> None:
        client = SlayerSQLClient(datasource=_ds_config(trino_container, "default"))
        prior = _pooled_run_time(client)
        assert prior["value"] == prior["default"]
        client.execute_sync("SELECT 1", timeout_seconds=1)
        assert _pooled_run_time(client) == prior

    def test_setting_restored_after_timed_query_raised(self, trino_container) -> None:
        client = SlayerSQLClient(datasource=_ds_config(trino_container, "default"))
        prior = _pooled_run_time(client)
        with pytest.raises(Exception, match="EXCEEDED_TIME_LIMIT"):
            client.execute_sync(_SLOW_SQL, timeout_seconds=1)
        assert _pooled_run_time(client) == prior

    def test_configured_value_restored(self, trino_container) -> None:
        client = SlayerSQLClient(datasource=_configured_ds(trino_container))
        prior = _pooled_run_time(client)
        assert _seconds(prior["value"]) == 1800
        with pytest.raises(Exception, match="EXCEEDED_TIME_LIMIT"):
            client.execute_sync(_SLOW_SQL, timeout_seconds=1)
        assert _pooled_run_time(client) == prior

    async def test_no_set_session_sent(self, trino_container) -> None:
        client = SlayerSQLClient(datasource=_ds_config(trino_container, "default"))
        before = _set_session_count(trino_container)
        await client.execute(sql="SELECT 1", timeout_seconds=30)
        client.execute_sync("SELECT 2", timeout_seconds=30)
        assert _set_session_count(trino_container) == before

    async def test_type_probe_is_timed(self, trino_container, monkeypatch) -> None:
        seen = _spy_run_time(monkeypatch)
        client = SlayerSQLClient(datasource=_ds_config(trino_container, "default"))
        prior = _pooled_run_time(client)
        assert await client.get_column_types("SELECT 1 AS x") == {"x": "number"}
        assert seen == ["60s"]
        assert _pooled_run_time(client) == prior

    def test_sync_type_probe_is_timed(self, trino_container, monkeypatch) -> None:
        seen = _spy_run_time(monkeypatch)
        client = SlayerSQLClient(datasource=_ds_config(trino_container, "default"))
        prior = _pooled_run_time(client)
        types = get_column_types_sync(
            "SELECT 1 AS x", engine=client._get_sync_engine_for_client(), db_type="trino", datasource_name=_DS,
        )
        assert types == {"x": "number"}
        assert seen == ["60s"]
        assert _pooled_run_time(client) == prior


# ---------------------------------------------------------------------------
# Mode-B date functions (the shared oracle matrix + spec scenarios)
# ---------------------------------------------------------------------------

_DATE_CASES = matrix_cases()
assert len(_DATE_CASES) == 116


@pytest.fixture(scope="module")
def _trino_dates_storage(trino_container, tmp_path_factory):
    seed = server_seed_statements("trino", today=datetime.now(timezone.utc).date())
    with _seeded_schema(trino_container, seed) as schema:
        yield _storage(
            str(tmp_path_factory.mktemp("trino_dates")), _ds_config(trino_container, schema),
            all_models(data_source=_DS),
        )


@pytest.fixture
def trino_dates(_trino_dates_storage) -> SlayerQueryEngine:
    return SlayerQueryEngine(storage=_trino_dates_storage)


async def _order_value(engine: SlayerQueryEngine, expr: str, *, order_id: int) -> Any:
    resp = await engine.execute(SlayerQuery.model_validate({
        "source_model": "orders", "dimensions": ["id", {"expression": expr, "name": "v"}],
    }))
    return {int(r["orders.id"]): r["orders.v"] for r in resp.data}[order_id]


@pytest.mark.integration
class TestTrinoDateFunctions:
    @pytest.mark.parametrize("case", _DATE_CASES, ids=[c.case_id for c in _DATE_CASES])
    async def test_matrix(self, trino_dates: SlayerQueryEngine, case: DateCase) -> None:
        assert_case((await trino_dates.execute(matrix_query(case))).data, case)

    async def test_scenarios(self, trino_dates: SlayerQueryEngine) -> None:
        await check_server_scenarios(trino_dates)

    @pytest.mark.parametrize("count,unit,expected", [
        (2, "hour", datetime(2024, 3, 1, 2, 0)),
        (-90, "minute", datetime(2024, 2, 29, 22, 30)),
        (30, "second", datetime(2024, 3, 1, 0, 0, 30)),
    ])
    async def test_sub_day_offset_of_a_date(
        self, trino_dates: SlayerQueryEngine, count: int, unit: str, expected: datetime,
    ) -> None:
        expr = f"date_add(order_date, {count}, '{unit}')"
        assert_value(await _order_value(trino_dates, expr, order_id=1), expected, where=expr)

    async def test_iso_calendar_parts(self, trino_dates: SlayerQueryEngine) -> None:
        """Order 4 is 2024-12-30, a Monday in ISO week 1 of 2025."""
        assert await _order_value(trino_dates, "date_part('iso_year', order_date)", order_id=4) == 2025
        assert await _order_value(trino_dates, "date_part('day_of_week', order_date)", order_id=4) == 1

    async def test_hourly_time_dimension_keeps_the_hour(self, trino_dates: SlayerQueryEngine) -> None:
        resp = await trino_dates.execute(SlayerQuery.model_validate({
            "source_model": "dt", "dimensions": ["id"],
            "time_dimensions": [{"dimension": "t1", "granularity": "hour"}],
        }))
        buckets = {int(r["dt.id"]): r["dt.t1"] for r in resp.data}
        assert str(buckets[1])[:19] == "2024-01-31 23:00:00"
        assert str(buckets[4])[:19] == "2024-06-02 10:00:00"


# ---------------------------------------------------------------------------
# Time spine and custom granularities
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def _trino_spine_storage(trino_container, tmp_path_factory):
    with _seeded_schema(trino_container, server_statements("trino")) as schema:
        storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp("trino_spine")))
        base = _ds_config(trino_container, schema)
        for name, models in (("tr", server_models(data_source="tr")), ("tr_ts", server_models_ts(data_source="tr_ts"))):
            run_sync(storage.save_datasource(with_granularities(base, name=name)))
            for model in models:
                run_sync(storage.save_model(model))
        yield storage


@pytest.mark.integration
class TestTrinoTimeSpine:
    async def test_scenarios(self, _trino_spine_storage) -> None:
        await check_all(SlayerQueryEngine(storage=_trino_spine_storage), data_source="tr", ts_data_source="tr_ts")

    @pytest.mark.parametrize("size", [1, 10_000, 25_001])
    def test_integer_sequence_spans_the_sequence_cap(self, trino_container, size: int) -> None:
        sequence = dialect_for_ds_type("trino").build_integer_sequence(size=size).sql(dialect="trino")
        rows = _run(trino_container, [f"SELECT count(*), count(DISTINCT i), min(i), max(i) FROM ({sequence}) AS s"])
        assert rows == [[size, size, 0, size - 1]]

    @pytest.mark.parametrize("size", [101, 1_000, 1_234])
    def test_integer_sequence_digits_cover_the_range(self, trino_container, monkeypatch, size: int) -> None:
        monkeypatch.setattr(trino_dialect_module, "_SEQUENCE_MAX", 10)  # 3+ digit factors at small sizes
        self.test_integer_sequence_spans_the_sequence_cap(trino_container, size)


@pytest.mark.integration
def test_every_trino_alias_failure_is_quoted(trino_container) -> None:
    with _seeded_schema(trino_container, ["CREATE TABLE kw_probe (a INTEGER)"]) as schema:
        with disposable_engine(_url(trino_container, schema)) as engine:
            assert unquoted_alias_failures(
                dialect="trino", words=keyword_universe(), table="kw_probe", column="a",
                fails=sa_statement_fails(engine), workers=8,
            ) == []
