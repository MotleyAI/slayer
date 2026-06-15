"""Integration tests using a real ClickHouse database via testcontainers.

DEV-1564: mirror of test_integration_postgres.py focused on ClickHouse's
distinguishing characteristics:

* Parametric ``quantile(p)(x)`` syntax (pinned via dry_run SQL inspection).
* Native ``median(x)`` aggregate.
* Native ``stddev_*`` / ``var_*`` / ``corr`` / ``covar_*`` (sqlglot
  transpiles to the dialect-correct spellings).
* Native ``LOG10`` / ``LOG2``.
* ``DateTime`` columns must round-trip as ``DataType.TIMESTAMP`` (issue #62
  regression — auto-ingestion).

Rollup-ingestion tests are intentionally omitted: ClickHouse exposes no
foreign-key metadata via SQLAlchemy's Inspector. Cross-model coverage
uses hand-declared ``ModelJoin``s instead.

Skipped silently when:
- ``testcontainers[clickhouse]`` is not installed (importorskip)
- The Docker daemon is unreachable (autouse session fixture)
"""

import tempfile
import uuid

import pytest

pytest.importorskip("testcontainers.clickhouse")
pytest.importorskip("clickhouse_sqlalchemy")

import sqlalchemy as sa
from testcontainers.clickhouse import ClickHouseContainer

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


# ---------------------------------------------------------------------------
# Container lifecycle
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def clickhouse_container():
    """Session-scoped ClickHouse 24 container."""
    container = ClickHouseContainer("clickhouse/clickhouse-server:24-alpine")
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
    engine = sa.create_engine(_admin_url(clickhouse_container))
    with engine.begin() as conn:
        conn.execute(sa.text(f"CREATE DATABASE {db_name}"))
    engine.dispose()
    return db_name


def _drop_module_db(clickhouse_container, db_name: str) -> None:
    """Dispose cached SA engines, then drop the database."""
    for engine in engine_factory._engine_cache.values():
        engine.dispose()
    engine_factory.reset_cache()

    engine = sa.create_engine(_admin_url(clickhouse_container))
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP DATABASE IF EXISTS {db_name}"))
    engine.dispose()


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
        engine = sa.create_engine(_ds_url_for_db(clickhouse_container, db_name))
        try:
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
        finally:
            engine.dispose()

        tmpdir = str(tmp_path_factory.mktemp("clickhouse_env"))
        storage = YAMLStorage(base_dir=tmpdir)
        run_sync(storage.save_datasource(_ds_config(clickhouse_container, db_name)))
        run_sync(storage.save_model(SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="testclickhouse",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
                Column(name="total", sql="amount", type=DataType.DOUBLE),
                Column(name="avg_amount", sql="amount", type=DataType.DOUBLE),
            ],
        )))
        run_sync(storage.save_model(SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="testclickhouse",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="name", sql="name", type=DataType.TEXT),
                Column(name="region", sql="region", type=DataType.TEXT),
            ],
        )))
        yield storage
    finally:
        _drop_module_db(clickhouse_container, db_name)


@pytest.fixture
def clickhouse_env(_clickhouse_env_storage) -> SlayerQueryEngine:
    """Per-test SlayerQueryEngine wrapping the module-scoped storage."""
    return SlayerQueryEngine(storage=_clickhouse_env_storage)


@pytest.mark.integration
class TestClickHouseQueries:
    async def test_count_all(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(source_model="orders", measures=[{"formula": "*:count"}])
        result = await clickhouse_env.execute(query=query)
        assert result.row_count == 1
        assert result.data[0]["orders._count"] == 6

    async def test_sum_measure(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(source_model="orders", measures=[{"formula": "total:sum"}])
        result = await clickhouse_env.execute(query=query)
        assert float(result.data[0]["orders.total_sum"]) == 875.0  # NOSONAR(S1244) — sum of integer cents, exact-representable

    async def test_avg_measure(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(source_model="orders", measures=[{"formula": "avg_amount:avg"}])
        result = await clickhouse_env.execute(query=query)
        avg = float(result.data[0]["orders.avg_amount_avg"])
        assert abs(avg - 145.83) < 0.1

    async def test_group_by_status(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "status"}],
        )
        result = await clickhouse_env.execute(query=query)
        by_status = {r["orders.status"]: r["orders._count"] for r in result.data}
        assert by_status["completed"] == 3
        assert by_status["pending"] == 2
        assert by_status["cancelled"] == 1

    async def test_filter_equals(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            filters=["status == 'completed'"],
        )
        result = await clickhouse_env.execute(query=query)
        assert result.data[0]["orders._count"] == 3

    async def test_filter_gt(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            filters=["total > 100"],
        )
        result = await clickhouse_env.execute(query=query)
        assert result.data[0]["orders._count"] == 3

    async def test_order_by_desc(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "status"}],
            order=[{"column": {"name": "count"}, "direction": "desc"}],
        )
        result = await clickhouse_env.execute(query=query)
        assert result.data[0]["orders.status"] == "completed"

    async def test_limit(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "status"}],
            limit=2,
        )
        result = await clickhouse_env.execute(query=query)
        assert result.row_count == 2

    async def test_multiple_measures(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}, {"formula": "total:sum"}],
            dimensions=[{"name": "status"}],
        )
        result = await clickhouse_env.execute(query=query)
        completed = next(r for r in result.data if r["orders.status"] == "completed")
        assert completed["orders._count"] == 3
        assert float(completed["orders.total_sum"]) == 450.0  # NOSONAR(S1244) — sum of integer cents, exact-representable

    async def test_time_dimension_month_granularity(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
        )
        result = await clickhouse_env.execute(query=query)
        assert result.row_count == 3

    async def test_time_dimension_with_date_range(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            time_dimensions=[{
                "dimension": {"name": "created_at"},
                "granularity": "month",
                "date_range": ["2024-01-01", "2024-02-28"],
            }],
        )
        result = await clickhouse_env.execute(query=query)
        total = sum(r["orders._count"] for r in result.data)
        assert total == 4

    async def test_composite_filter(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            filters=["status == 'completed' or status == 'pending'"],
        )
        result = await clickhouse_env.execute(query=query)
        assert result.data[0]["orders._count"] == 5

    async def test_time_shift_with_date_range(self, clickhouse_env: SlayerQueryEngine) -> None:
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
        result = await clickhouse_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(375.0)
        assert float(result.data[0]["orders.prev_month"]) == pytest.approx(200.0)

    async def test_consecutive_periods_with_boolean_predicate(self, clickhouse_env: SlayerQueryEngine) -> None:
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
        result = await clickhouse_env.execute(query=query)
        assert [r["orders.positive_run"] for r in result.data] == [1, 0, 1]

    async def test_change_with_date_range(self, clickhouse_env: SlayerQueryEngine) -> None:
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
        result = await clickhouse_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.amount_change"]) == pytest.approx(175.0)

    async def test_change_pct_with_date_range(self, clickhouse_env: SlayerQueryEngine) -> None:
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
        result = await clickhouse_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.pct"]) == pytest.approx(0.875)

    async def test_multiple_date_range_shifts(self, clickhouse_env: SlayerQueryEngine) -> None:
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
        result = await clickhouse_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(200.0)
        assert float(result.data[0]["orders.prev"]) == pytest.approx(300.0)
        assert float(result.data[0]["orders.next"]) == pytest.approx(375.0)


# ---------------------------------------------------------------------------
# Cross-model + multistage (hand-declared joins; no FK metadata available)
# ---------------------------------------------------------------------------


@pytest.fixture
def clickhouse_cross_model_env(clickhouse_container):
    """ClickHouse env with orders + customers (with score) and explicit join.
    No FK metadata is exposed by ClickHouse — the join is hand-declared."""
    db_name = _create_module_db(clickhouse_container)
    engine = sa.create_engine(_ds_url_for_db(clickhouse_container, db_name))
    try:
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
    finally:
        engine.dispose()

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(clickhouse_container, db_name)))
    run_sync(storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="testclickhouse",
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
        name="customers", sql_table="customers", data_source="testclickhouse",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="avg_score", sql="score", type=DataType.DOUBLE),
        ],
    )))
    yield SlayerQueryEngine(storage=storage)
    _drop_module_db(clickhouse_container, db_name)


@pytest.mark.integration
class TestCrossModelAndMultistageClickHouse:
    async def test_cross_model_measure(self, clickhouse_cross_model_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="customers.avg_score:avg")],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        )
        result = await clickhouse_cross_model_env.execute(query=query)
        assert result.row_count == 3
        global_avg = pytest.approx((90.0 + 60.0 + 80.0) / 3)
        assert float(result.data[0]["orders.customers.avg_score_avg"]) == global_avg
        assert float(result.data[1]["orders.customers.avg_score_avg"]) == global_avg
        assert float(result.data[2]["orders.customers.avg_score_avg"]) == global_avg

    async def test_query_list_named(self, clickhouse_cross_model_env: SlayerQueryEngine) -> None:
        inner = SlayerQuery(
            name="monthly", source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="total:sum")],
        )
        outer = SlayerQuery(source_model="monthly", measures=[ModelMeasure(formula="*:count")])
        result = await clickhouse_cross_model_env.execute(query=[inner, outer])
        assert result.data[0]["monthly._count"] == 3

    async def test_create_model_from_query(self, clickhouse_cross_model_env: SlayerQueryEngine) -> None:
        source = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="total:sum")],
        )
        saved = await clickhouse_cross_model_env.create_model_from_query(query=source, name="ch_monthly")
        assert saved.source_queries is not None
        result = await clickhouse_cross_model_env.execute(
            query=SlayerQuery(source_model="ch_monthly", measures=[ModelMeasure(formula="*:count")])
        )
        assert result.data[0]["ch_monthly._count"] == 3

    async def test_sql_dimension(self, clickhouse_cross_model_env: SlayerQueryEngine) -> None:
        from slayer.core.query import ModelExtension
        query = SlayerQuery(
            source_model=ModelExtension(
                source_name="orders",
                columns=[{"name": "tier", "sql": "CASE WHEN amount > 100 THEN 'high' ELSE 'low' END"}],
            ),
            dimensions=[ColumnRef(name="tier")],
            measures=[ModelMeasure(formula="*:count")],
        )
        result = await clickhouse_cross_model_env.execute(query=query)
        by_tier = {r["orders.tier"]: r["orders._count"] for r in result.data}
        assert by_tier["high"] == 3
        assert by_tier["low"] == 3


# ---------------------------------------------------------------------------
# Median / percentile — ClickHouse uses parametric quantile(p)(x) + median(x)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestClickHouseMedianPercentile:
    async def test_median(self, clickhouse_env: SlayerQueryEngine) -> None:
        # amounts = [100, 200, 50, 150, 75, 300]; native ClickHouse median()
        # is a quantile(0.5) approximation — accept a tolerance, not equality.
        query = SlayerQuery(source_model="orders", measures=[{"formula": "total:median"}])
        result = await clickhouse_env.execute(query=query)
        assert float(result.data[0]["orders.total_median"]) == pytest.approx(125.0, abs=50.0)

    async def test_median_emits_native_aggregate(self, clickhouse_env: SlayerQueryEngine) -> None:
        """ClickhouseDialect.build_median routes through sqlglot's ClickHouse
        dialect which emits ``quantile(0.5)(x)`` natively. Pin the SQL shape
        via dry_run so a regression to ``percentile_cont`` is caught."""
        query = SlayerQuery(source_model="orders", measures=[{"formula": "total:median"}])
        dry = await clickhouse_env.execute(query=query, dry_run=True)
        assert dry.sql is not None
        sql_lower = dry.sql.lower()
        assert "quantile(0.5)(" in sql_lower or "median(" in sql_lower, (
            f"ClickHouse median must emit native quantile(0.5)(x) or median(x). Got:\n{dry.sql}"
        )
        assert "percentile_cont" not in sql_lower, (
            f"ClickHouse median must not fall through to PERCENTILE_CONT. Got:\n{dry.sql}"
        )

    async def test_percentile_uses_parametric_quantile(self, clickhouse_env: SlayerQueryEngine) -> None:
        """ClickhouseDialect.build_percentile emits the parametric
        ``quantile(p)(x)`` form, not ``PERCENTILE_CONT``. Pin via dry_run."""
        query = SlayerQuery(
            source_model="orders",
            measures=[
                {"formula": "total:percentile(p=0.25)"},
                {"formula": "total:percentile(p=0.75)"},
            ],
        )
        dry = await clickhouse_env.execute(query=query, dry_run=True)
        assert dry.sql is not None
        sql_lower = dry.sql.lower()
        # Pattern: quantile(<p>)(<col>)
        assert "quantile(0.25)(" in sql_lower, (
            f"ClickHouse must use parametric quantile(p)(x) syntax. Got:\n{dry.sql}"
        )
        assert "quantile(0.75)(" in sql_lower, (
            f"ClickHouse must use parametric quantile(p)(x) syntax. Got:\n{dry.sql}"
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
            source_model="orders",
            measures=[
                {"formula": "total:percentile(p=0.25)"},
                {"formula": "total:percentile(p=0.75)"},
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
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:median"}],
            dimensions=[{"name": "status"}],
        )
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


# ---------------------------------------------------------------------------
# Native stat aggregations (DEV-1317 cross-dialect parity)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestClickHouseStatAggregations:
    """ClickHouse has native ``stddev_samp``/``stddev_pop``/``var_samp``/
    ``var_pop``/``corr``/``covar_samp``/``covar_pop`` (sqlglot transpiles
    to ``stddevSamp`` / ``corr`` / ``covarSamp`` / etc.). Numeric tolerance
    is tight (1e-6) because these go through ClickHouse's exact aggregate
    implementations (no UDF, no decomposition formula)."""

    async def test_stddev_samp_native_clickhouse(self, clickhouse_env: SlayerQueryEngine) -> None:
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:stddev_samp"}],
        )
        result = await clickhouse_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_stddev_samp"]) == pytest.approx(
            statistics.stdev(amounts), rel=1e-6
        )

    async def test_stddev_pop_native_clickhouse(self, clickhouse_env: SlayerQueryEngine) -> None:
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:stddev_pop"}],
        )
        result = await clickhouse_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_stddev_pop"]) == pytest.approx(
            statistics.pstdev(amounts), rel=1e-6
        )

    async def test_var_samp_native_clickhouse(self, clickhouse_env: SlayerQueryEngine) -> None:
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:var_samp"}],
        )
        result = await clickhouse_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_var_samp"]) == pytest.approx(
            statistics.variance(amounts), rel=1e-6
        )

    async def test_var_pop_native_clickhouse(self, clickhouse_env: SlayerQueryEngine) -> None:
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:var_pop"}],
        )
        result = await clickhouse_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_var_pop"]) == pytest.approx(
            statistics.pvariance(amounts), rel=1e-6
        )

    async def test_corr_native_clickhouse(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:corr(other=customer_id)"}],
        )
        result = await clickhouse_env.execute(query=query)
        import statistics
        xs = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        ys = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        expected = statistics.correlation(xs, ys)
        assert float(result.data[0]["orders.total_corr_other_customer_id"]) == pytest.approx(
            expected, rel=1e-6
        )

    async def test_covar_samp_native_clickhouse(self, clickhouse_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:covar_samp(other=customer_id)"}],
        )
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
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:covar_pop(other=customer_id)"}],
        )
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
# log10 round-trip (DEV-1337 — ClickHouse has native log10)
# ---------------------------------------------------------------------------


@pytest.fixture
def clickhouse_log10_env(clickhouse_container):
    """Dedicated env for the log10 round-trip so the Column-add doesn't
    leak into the shared module-scoped storage."""
    db_name = _create_module_db(clickhouse_container)
    engine = sa.create_engine(_ds_url_for_db(clickhouse_container, db_name))
    try:
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
    finally:
        engine.dispose()

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(clickhouse_container, db_name)))
    run_sync(storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="testclickhouse",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="log_amount", sql="log10(amount)", type=DataType.DOUBLE),
        ],
    )))
    yield SlayerQueryEngine(storage=storage)
    _drop_module_db(clickhouse_container, db_name)


@pytest.mark.integration
async def test_log10_round_trip_clickhouse(clickhouse_log10_env: SlayerQueryEngine) -> None:
    """ClickHouse has native LOG10 (ClickhouseDialect.log10_native=True).
    The emitted SQL must preserve ``log10(...)``."""
    import math as _math

    result = await clickhouse_log10_env.execute(
        SlayerQuery(source_model="orders", measures=[{"formula": "log_amount:max"}])
    )
    assert float(result.data[0]["orders.log_amount_max"]) == pytest.approx(
        _math.log10(300.0), rel=1e-9
    )

    dry = await clickhouse_log10_env.execute(
        SlayerQuery(source_model="orders", measures=[{"formula": "log_amount:max"}]),
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
# Window-in-filter raises (DEV-1369 parity)
# ---------------------------------------------------------------------------


@pytest.fixture
def planets_clickhouse_env(clickhouse_container):
    db_name = _create_module_db(clickhouse_container)
    engine = sa.create_engine(_ds_url_for_db(clickhouse_container, db_name))
    try:
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
    finally:
        engine.dispose()

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(clickhouse_container, db_name)))
    run_sync(storage.save_model(
        SlayerModel(
            name="planets",
            sql_table="planets",
            data_source="testclickhouse",
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
    _drop_module_db(clickhouse_container, db_name)


@pytest.mark.integration
async def test_filter_on_windowed_column_clickhouse_raises(planets_clickhouse_env) -> None:
    engine = planets_clickhouse_env
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
def clickhouse_derived_chain_env(clickhouse_container):
    db_name = _create_module_db(clickhouse_container)
    engine = sa.create_engine(_ds_url_for_db(clickhouse_container, db_name))
    try:
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
    finally:
        engine.dispose()

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)
    run_sync(storage.save_datasource(_ds_config(clickhouse_container, db_name)))
    run_sync(storage.save_model(
        SlayerModel(
            name="b_tbl",
            data_source="testclickhouse",
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
            data_source="testclickhouse",
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
    _drop_module_db(clickhouse_container, db_name)


@pytest.mark.integration
async def test_integration_clickhouse_cross_model_derived_columnsql(
    clickhouse_derived_chain_env: SlayerQueryEngine,
) -> None:
    response = await clickhouse_derived_chain_env.execute(
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


# ---------------------------------------------------------------------------
# Dialect-specific: DateTime column type round-trip (issue #62 regression)
# ---------------------------------------------------------------------------


@pytest.fixture
def clickhouse_ingest_for_types_env(clickhouse_container):
    """Seed a ClickHouse table with each scalar type we care about,
    then auto-ingest. The test asserts the inferred DataType vocabulary."""
    db_name = _create_module_db(clickhouse_container)
    engine = sa.create_engine(_ds_url_for_db(clickhouse_container, db_name))
    try:
        with engine.begin() as conn:
            conn.execute(sa.text("""
                CREATE TABLE orders (
                    id Int32,
                    customer_id Int32,
                    quantity Int32,
                    amount Float64,
                    status String,
                    created_at DateTime
                ) ENGINE = MergeTree() ORDER BY id
            """))
            conn.execute(sa.text("""
                INSERT INTO orders VALUES
                    (1, 1, 5, 100.0, 'completed', '2024-01-01 00:00:00')
            """))
    finally:
        engine.dispose()
    ds = _ds_config(clickhouse_container, db_name)
    yield ds
    _drop_module_db(clickhouse_container, db_name)


@pytest.mark.integration
def test_clickhouse_datetime_typed_correctly(clickhouse_ingest_for_types_env) -> None:
    """Regression for issue #62: auto-ingesting a ClickHouse DateTime
    column must produce ``DataType.TIMESTAMP``, not STRING. Int32 / Float64
    must map to INT / DOUBLE."""
    ds = clickhouse_ingest_for_types_env
    models = ingest_datasource(datasource=ds, schema=None)
    orders = next(m for m in models if m.name == "orders")
    by_name = {c.name: c for c in orders.columns}

    assert by_name["id"].type == DataType.INT
    assert by_name["customer_id"].type == DataType.INT
    assert by_name["quantity"].type == DataType.INT
    assert by_name["amount"].type == DataType.DOUBLE
    assert by_name["status"].type == DataType.TEXT
    assert by_name["created_at"].type == DataType.TIMESTAMP, (
        f"ClickHouse DateTime must map to TIMESTAMP, got "
        f"{by_name['created_at'].type!r}"
    )
