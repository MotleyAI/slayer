"""Live integration tests for Snowflake.

Skip-by-default: requires ``snowflake-connector-python``, ``snowflake-sqlalchemy``,
and a ``~/.snowflake/connections.toml`` profile named ``slayer_test`` (override
with ``$SLAYER_SNOWFLAKE_CONNECTION``).

The session-scoped fixture creates a uniquely-named transient schema, seeds the
standard orders / customers / products / regions tables (with FK ``REFERENCES``
clauses — Snowflake stores them informationally, the Inspector returns them,
auto-ingestion discovers joins), runs the full test matrix, and drops the
schema on teardown. A ``SELECT 1`` warm-up runs once at fixture entry to
absorb warehouse-suspend latency.
"""

import asyncio
import os
import pathlib
import tomllib
import uuid
from typing import Any, NoReturn, cast

import pytest
import sqlalchemy as sa
import sqlglot
from sqlalchemy import event
from sqlalchemy.pool import QueuePool

from slayer.async_utils import run_sync
from slayer.core.enums import DataType, JoinCardinality, TimeGranularity
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.live_schema import ingestion
from slayer.live_schema.ingestion import ingest_datasource_idempotent
from slayer.live_schema.schema_drift import IdempotentIngestResult
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.importers.column_types import LiveDatasource, LiveSource
from slayer.database import client, engine_factory
from slayer.database.client import ResultColumnType
from slayer.sql.dialects import SnowflakeDialect
from slayer.sql.dialects.reflection import TableLocation
from slayer.storage.yaml_storage import YAMLStorage
from tests._cast_fixtures import check_try_cast_targets
from tests._dev2120_fixtures import (
    DOCS_ROWS,
    LIB,
    check_library,
    check_native_json,
    contractors_model,
    contractors_statements,
    lib_model,
    server_literal,
    server_table_statements as dev2120_table_statements,
)
from tests._dev2137_fixtures import assignments_model, assignments_statements, check_lists
from tests._list_mixed_numbers import check_mixed_numbers, mixed_numbers_model, mixed_numbers_statements
from tests.integration._one_request import MARKERS, PATHS, assert_one_request_each, run_three
from tests._reserved_alias_probe import (
    SNOWFLAKE_KEYWORDS,
    keyword_universe,
    sa_statement_fails,
    unquoted_alias_failures,
)

# Skip the entire module if the snowflake extras aren't installed.
pytest.importorskip("snowflake.connector")
pytest.importorskip("snowflake.sqlalchemy")

import snowflake.connector  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above
import snowflake.connector.errors as sf_errors  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above
from snowflake.connector.errors import ProgrammingError  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

_TOML_PATH = pathlib.Path("~/.snowflake/connections.toml").expanduser()
_CONNECTION_NAME = os.environ.get("SLAYER_SNOWFLAKE_CONNECTION", "slayer_test")
# Set by CI when the secret is present: an unreachable Snowflake fails instead of skipping.
_REQUIRED = os.environ.get("SLAYER_SNOWFLAKE_REQUIRED") == "1"

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _TOML_PATH.is_file() and not _REQUIRED,
        reason=f"{_TOML_PATH} not present; skipping Snowflake live tests",
    ),
]


def _load_profile() -> dict:
    """Read the named profile from connections.toml. Returns {} if the
    profile is absent; tests using this surface their own skip reason."""
    if not _TOML_PATH.is_file():
        return {}
    with open(_TOML_PATH, "rb") as f:
        data = tomllib.load(f)
    profile = data.get(_CONNECTION_NAME)
    if not isinstance(profile, dict):
        return {}
    return profile


def _unavailable(reason: str) -> NoReturn:
    if _REQUIRED:
        pytest.fail(reason)
    pytest.skip(reason)


def _profile_or_skip() -> dict:
    profile = _load_profile()
    if not profile:
        _unavailable(
            f"Snowflake profile '{_CONNECTION_NAME}' not found in {_TOML_PATH}; "
            f"set $SLAYER_SNOWFLAKE_CONNECTION or add a [{_CONNECTION_NAME}] profile."
        )
    return profile


@pytest.fixture(scope="session")
def sf_transient_schema():
    """Session-scoped: a uniquely-named transient schema created on the
    connection_name profile's default database. Returns the schema name.
    Teardown drops the schema.

    Transient schemas have no Fail-safe (cheap), and per-test isolation isn't
    needed because the integration suite is sequential. A session-scoped
    schema saves N*setup cost across the suite.

    Teardown safety: ``cur`` and ``schema_name`` are bound BEFORE entering
    the try block so the finally clause can rely on them existing even when
    early setup fails (e.g., cursor creation, USE WAREHOUSE permission denied).
    """
    profile = _profile_or_skip()
    try:
        conn = snowflake.connector.connect(connection_name=_CONNECTION_NAME)
    except Exception as exc:
        _unavailable(f"Could not connect to Snowflake via '{_CONNECTION_NAME}': {exc}")
        return  # for type checkers
    schema_name = f"SLAYER_TEST_{uuid.uuid4().hex[:12].upper()}"
    db_name = profile.get("database")
    warehouse = profile.get("warehouse")
    cur = conn.cursor()  # bind before any operation so the finally has it
    schema_created = False
    try:
        # Warehouse warm-up before any timeout-sensitive assertions.
        if warehouse:
            cur.execute(f"USE WAREHOUSE {warehouse}")
        cur.execute("SELECT 1")
        cur.fetchall()
        if db_name:
            cur.execute(f"USE DATABASE {db_name}")
        cur.execute(f"CREATE TRANSIENT SCHEMA {schema_name}")
        schema_created = True
        cur.execute(f"USE SCHEMA {schema_name}")
        # Seed schema — Snowflake-flavored types.
        cur.execute("""
            CREATE TABLE regions (
                id NUMBER(38,0) PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE customers (
                id NUMBER(38,0) PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                region_id NUMBER(38,0) REFERENCES regions(id)
            )
        """)
        cur.execute("""
            CREATE TABLE products (
                id NUMBER(38,0) PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price NUMBER(10,2) NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE orders (
                id NUMBER(38,0) PRIMARY KEY,
                customer_id NUMBER(38,0) REFERENCES customers(id),
                product_id NUMBER(38,0) REFERENCES products(id),
                quantity NUMBER(38,0) NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMP_NTZ NOT NULL
            )
        """)
        cur.execute("COMMENT ON TABLE orders IS 'All orders'")
        cur.execute("COMMENT ON COLUMN orders.quantity IS 'Units ordered'")
        cur.executemany(
            "INSERT INTO regions VALUES (%s, %s)",
            [(1, "US"), (2, "EU"), (3, "APAC")],
        )
        cur.executemany(
            "INSERT INTO customers VALUES (%s, %s, %s, %s)",
            [
                (1, "Acme", "acme@example.com", 1),
                (2, "Globex", "globex@example.com", 2),
                (3, "Initech", "initech@example.com", 1),
            ],
        )
        cur.executemany(
            "INSERT INTO products VALUES (%s, %s, %s, %s)",
            [
                (1, "Widget", "tools", 9.99),
                (2, "Gadget", "tools", 19.99),
                (3, "Doohickey", "novelty", 4.99),
            ],
        )
        cur.executemany(
            "INSERT INTO orders VALUES (%s, %s, %s, %s, %s, %s)",
            [
                (1, 1, 1, 2, "completed", "2024-01-15 10:00:00"),
                (2, 1, 2, 1, "completed", "2024-01-20 11:00:00"),
                (3, 2, 3, 5, "pending", "2024-02-10 09:00:00"),
                (4, 2, 1, 3, "completed", "2024-02-15 14:00:00"),
                (5, 3, 2, 1, "cancelled", "2024-03-01 08:00:00"),
                (6, 3, 3, 4, "pending", "2024-03-10 16:00:00"),
            ],
        )
        conn.commit()
        yield schema_name
    finally:
        # Best-effort teardown. cur and schema_name are bound at top of try.
        if schema_created:
            try:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
            except Exception:
                # Don't mask the original error if setup failed mid-way.
                pass
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


@pytest.fixture
def sf_datasource(sf_transient_schema: str) -> DatasourceConfig:
    """A DatasourceConfig pointing at the transient schema, via
    ``connection_name``. The ``USE SCHEMA`` event listener pins each
    SA connection to the per-session transient schema."""
    return DatasourceConfig(
        name="sf_test",
        type="snowflake",
        connection_name=_CONNECTION_NAME,
        schema_name=sf_transient_schema,
    )


async def _ingest(ds: DatasourceConfig, **kwargs: Any) -> list[SlayerModel]:
    sql_client = client.SlayerSQLClient(datasource=ds)
    try:
        return await ingestion.ingest_datasource_async(ds, client=sql_client, **kwargs)
    finally:
        await sql_client.aclose()
        sql_client.close()


def _ingest_idempotent(*, datasource: DatasourceConfig, storage: YAMLStorage,
                       **scope: Any) -> IdempotentIngestResult:
    engine = SlayerQueryEngine(storage=storage)
    try:
        return engine.run_sync(ingest_datasource_idempotent(datasource=datasource, storage=storage,
                                                            client=engine.client_for(datasource), **scope))
    finally:
        engine.close()


@pytest.fixture
def sf_engine(sf_datasource: DatasourceConfig, sf_transient_schema):
    """An ``sa.Engine`` for the transient schema.

    Per the M8 plan addition, ``engine_factory.get_engine`` itself registers
    a ``connect`` event listener that applies USE WAREHOUSE / USE ROLE /
    USE DATABASE / USE SCHEMA when those fields are set on the
    DatasourceConfig. So the fixture only needs to call the factory —
    schema/warehouse/role overrides are wired in production code, not in
    the test harness. (This is what makes ``SlayerQueryEngine`` execution
    paths land in the right session state too.)
    """
    yield engine_factory.get_engine(sf_datasource)


@pytest.fixture
def sf_storage_with_models(sf_datasource: DatasourceConfig, tmp_path):
    """YAMLStorage with the four standard models pre-saved.
    Caller adds joins where needed."""
    storage = YAMLStorage(base_dir=str(tmp_path))
    run_sync(storage.save_datasource(sf_datasource))

    orders = SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="sf_test",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.INT),
            Column(name="product_id", sql="product_id", type=DataType.INT),
            Column(name="quantity", sql="quantity", type=DataType.INT),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ModelJoin(target_model="products", join_pairs=[["product_id", "id"]]),
        ],
    )
    customers = SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="sf_test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="email", sql="email", type=DataType.TEXT),
            Column(name="region_id", sql="region_id", type=DataType.INT),
        ],
        joins=[
            ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]]),
        ],
    )
    products = SlayerModel(
        name="products",
        sql_table="products",
        data_source="sf_test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="category", sql="category", type=DataType.TEXT),
            Column(name="price", sql="price", type=DataType.DOUBLE),
        ],
    )
    regions = SlayerModel(
        name="regions",
        sql_table="regions",
        data_source="sf_test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
        ],
    )
    for m in (orders, customers, products, regions):
        run_sync(storage.save_model(m))
    yield storage


# ---------------------------------------------------------------------------
# Smoke tests
# ---------------------------------------------------------------------------


def test_basic_query(sf_storage_with_models) -> None:
    """SELECT sum(quantity) FROM orders — minimum smoke for the engine path."""
    engine = SlayerQueryEngine(storage=sf_storage_with_models)
    result = run_sync(engine.execute(SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="sum(quantity)")],
    )))
    rows = result.data
    assert len(rows) == 1
    # 6 orders, quantities 2+1+5+3+1+4 = 16
    val = next(iter(rows[0].values()))
    assert int(val) == 16


def test_try_cast_integer_targets(sf_storage_with_models) -> None:
    run_sync(check_try_cast_targets(SlayerQueryEngine(storage=sf_storage_with_models), model="orders"))


def test_functional_aggregation(sf_storage_with_models) -> None:
    engine = SlayerQueryEngine(storage=sf_storage_with_models)
    func = run_sync(engine.execute(SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="sum(quantity)")],
    )))
    assert int(next(iter(func.data[0].values()))) == 16


def test_query_with_dimension(sf_storage_with_models) -> None:
    """Group-by status; assert 3 status groups."""
    engine = SlayerQueryEngine(storage=sf_storage_with_models)
    result = run_sync(engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[ModelMeasure(formula="count(*)")],
    )))
    rows = result.data
    statuses = {row.get("orders.status") for row in rows}
    assert statuses == {"completed", "pending", "cancelled"}


def test_dev1933_regex_literal_model_column(sf_storage_with_models) -> None:
    """A model column holding a ``(?:...)`` regex literal and a ``%``
    LIKE pattern executes verbatim; text() misread ``:too`` as a bind parameter."""
    engine = SlayerQueryEngine(storage=sf_storage_with_models)
    orders = run_sync(sf_storage_with_models.get_model("orders"))
    assert orders is not None
    run_sync(sf_storage_with_models.save_model(orders.model_copy(update={"name": "orders_rx", "columns": [
        *orders.columns,
        Column(name="rx", sql="CASE WHEN status LIKE '%pend%' "
                              "OR status = '(?i)(?:too complicated|too complex)' THEN 1 ELSE 0 END",
               type=DataType.DOUBLE),
    ]})))
    result = run_sync(engine.execute(SlayerQuery(
        source_model="orders_rx",
        dimensions=[ColumnRef(name="rx")],
        measures=[ModelMeasure(formula="count(*)")],
    )))
    by_rx = {int(r["orders_rx.rx"]): r["orders_rx._count"] for r in result.data}
    assert by_rx == {1: 2, 0: 4}


def test_rollup_join_via_explicit_joins(sf_storage_with_models) -> None:
    """Cross-model measure: orders.customers.regions.name (multi-hop join).

    Seed data: customers 1+3 live in US, customer 2 lives in EU, region 3
    (APAC) has no customers — and the LEFT JOIN chain starts at orders, so
    APAC never surfaces. Exercising orders → customers → regions correctly
    yields exactly {US, EU}."""
    engine = SlayerQueryEngine(storage=sf_storage_with_models)
    result = run_sync(engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="customers.regions.name")],
        measures=[ModelMeasure(formula="sum(quantity)")],
    )))
    rows = result.data
    region_names = {row.get("orders.customers.regions.name") for row in rows}
    assert region_names == {"US", "EU"}


# ---------------------------------------------------------------------------
# Aggregation matrix
# ---------------------------------------------------------------------------


def test_median_percentile_native(sf_storage_with_models) -> None:
    """Snowflake has native MEDIAN and PERCENTILE_CONT WITHIN GROUP."""
    engine = SlayerQueryEngine(storage=sf_storage_with_models)
    result = run_sync(engine.execute(SlayerQuery(
        source_model="orders",
        measures=[
            ModelMeasure(formula="median(quantity)"),
            ModelMeasure(formula="percentile(quantity, p=0.5)"),
        ],
    )))
    rows = result.data
    assert len(rows) == 1
    vals = list(rows[0].values())
    # Both should produce a numeric median for the quantities [2,1,5,3,1,4] → 2.5
    for v in vals:
        assert v is not None
        assert float(v) == pytest.approx(2.5, abs=0.01)


def test_stddev_var_native(sf_storage_with_models) -> None:
    """Snowflake has native STDDEV_SAMP / VAR_SAMP."""
    engine = SlayerQueryEngine(storage=sf_storage_with_models)
    result = run_sync(engine.execute(SlayerQuery(
        source_model="orders",
        measures=[
            ModelMeasure(formula="stddev_samp(quantity)"),
            ModelMeasure(formula="var_samp(quantity)"),
        ],
    )))
    rows = result.data
    assert len(rows) == 1
    for v in rows[0].values():
        assert v is not None
        assert float(v) > 0


def test_corr_covar_native(sf_storage_with_models) -> None:
    """Snowflake has native CORR / COVAR_SAMP / COVAR_POP."""
    engine = SlayerQueryEngine(storage=sf_storage_with_models)
    result = run_sync(engine.execute(SlayerQuery(
        source_model="orders",
        measures=[
            ModelMeasure(formula="corr(quantity, other=customer_id)"),
            ModelMeasure(formula="covar_samp(quantity, other=customer_id)"),
        ],
    )))
    rows = result.data
    assert len(rows) == 1
    # Both should produce a numeric result (not NaN, not NULL).
    for v in rows[0].values():
        assert v is not None


def test_date_trunc_quarter(sf_storage_with_models) -> None:
    """Snowflake native DATE_TRUNC('QUARTER', ts) via sqlglot."""
    engine = SlayerQueryEngine(storage=sf_storage_with_models)
    result = run_sync(engine.execute(SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension="created_at",
            granularity=TimeGranularity.QUARTER,
        )],
        measures=[ModelMeasure(formula="sum(quantity)")],
    )))
    rows = result.data
    # All orders are in Q1 2024 → exactly one row.
    assert len(rows) == 1


def test_time_shift_dateadd(sf_storage_with_models) -> None:
    """time_shift uses INTERVAL → sqlglot transpiles to DATEADD on Snowflake."""
    engine = SlayerQueryEngine(storage=sf_storage_with_models)
    result = run_sync(engine.execute(SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension="created_at",
            granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="sum(quantity)"),
            ModelMeasure(formula="time_shift(sum(quantity), -1, 'month')", name="prev_month"),
        ],
    )))
    rows = result.data
    assert len(rows) >= 1  # Three months in the dataset


def test_rank_transforms(sf_storage_with_models) -> None:
    """RANK / DENSE_RANK / PERCENT_RANK / NTILE — standard SQL, no Snowflake
    branch needed."""
    engine = SlayerQueryEngine(storage=sf_storage_with_models)
    result = run_sync(engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[
            ModelMeasure(formula="sum(quantity)"),
            ModelMeasure(formula="rank(sum(quantity), direction='desc')", name="qty_rank"),
            ModelMeasure(formula="dense_rank(sum(quantity), direction='desc')", name="qty_dense_rank"),
            ModelMeasure(formula="ntile(sum(quantity), n=2)", name="qty_bucket"),
        ],
    )))
    rows = result.data
    # Three status groups → three rows with rank values 1..3
    ranks = {int(row.get("orders.qty_rank")) for row in rows}
    assert ranks == {1, 2, 3}


# ---------------------------------------------------------------------------
# Connection-name path (creator=) vs inline path
# ---------------------------------------------------------------------------


def test_connection_name_path_executes_query(sf_engine, sf_transient_schema) -> None:
    """End-to-end via the ``creator=`` engine bridge: build engine via
    DatasourceConfig.connection_name → execute a SELECT → expect a row."""
    with sf_engine.connect() as conn:
        result = conn.execute(sa.text("SELECT COUNT(*) FROM orders"))
        row = result.fetchone()
        assert row[0] == 6


def test_inline_credentials_path_executes_query(sf_transient_schema) -> None:
    """Read profile fields directly from the TOML, build an inline-form
    DatasourceConfig (no connection_name), open an engine through the
    factory, and execute. Pins the snowflake-sqlalchemy URL form."""
    profile = _profile_or_skip()
    if "password" not in profile:
        pytest.skip("Inline path requires a password in the TOML profile")

    ds = DatasourceConfig(
        name="sf_inline",
        type="snowflake",
        host=profile["account"],
        username=profile["user"],
        password=profile["password"],
        database=profile.get("database"),
        schema_name=sf_transient_schema,
        warehouse=profile.get("warehouse"),
        role=profile.get("role"),
    )
    engine = engine_factory.get_engine(ds)
    with engine.connect() as conn:
        # Inline path may not set the schema if profile lacks one; explicitly USE it.
        conn.execute(sa.text(f"USE SCHEMA {sf_transient_schema}"))
        result = conn.execute(sa.text("SELECT COUNT(*) FROM orders"))
        row = result.fetchone()
        assert row[0] == 6


# ---------------------------------------------------------------------------
# Statement timeout
# ---------------------------------------------------------------------------


def test_statement_timeout_aborts_long_query(sf_engine) -> None:
    """With ``timeout_seconds=1`` and an explicit ``CALL
    SYSTEM$WAIT(5, 'SECONDS')``, Snowflake should raise a
    statement-cancellation error (SQLSTATE 57014 in Snowflake's mapping,
    surfaced as ProgrammingError with errno 604 / 'SQL execution canceled'
    or 'Statement reached its statement or warehouse timeout').

    Assert on the specific error code/message — a bare "any DBAPI error"
    would silently pass for unrelated failures (permission denied, syntax
    error, etc.). If Snowflake doesn't honor STATEMENT_TIMEOUT for the
    chosen wait function on this account/version, fail loudly so we can
    pick a different probe rather than masking a real timeout regression.

    Uses 5 seconds (not 10) to bound suite latency.
    """
    with pytest.raises((sf_errors.ProgrammingError, sa.exc.DBAPIError)) as exc_info:
        client._execute_sql_sync(
            sql="CALL SYSTEM$WAIT(5, 'SECONDS')",
            db_type="snowflake",
            timeout_seconds=1,
            engine=sf_engine,
        )
    msg = str(exc_info.value).lower()
    # Snowflake timeout error vocabulary — match any of the standard markers.
    expected_signals = (
        "statement reached its statement or warehouse timeout",
        "sql execution canceled",
        "statement_timeout_in_seconds",
        "query was canceled",  # alternate phrasing on some account versions
    )
    assert any(sig in msg for sig in expected_signals), (
        f"Snowflake timeout error did not match expected signals.\n"
        f"Expected one of: {expected_signals}\nGot: {msg}"
    )


# ---------------------------------------------------------------------------
# Column-type round-trip
# ---------------------------------------------------------------------------


def test_column_types_round_trip(sf_engine) -> None:
    """A SELECT against each Snowflake type code must round-trip through
    the type-probe door and produce the expected SLayer types."""
    sql = """
        SELECT
            CAST(1 AS NUMBER) AS as_number,
            CAST(1.5 AS DOUBLE) AS as_double,
            CAST('foo' AS VARCHAR) AS as_text,
            CAST(TRUE AS BOOLEAN) AS as_bool,
            CAST('2024-01-01' AS DATE) AS as_date,
            CAST('2024-01-01 10:00' AS TIMESTAMP_NTZ) AS as_ts
    """
    types = client.get_result_column_types_sync(
        client.render_type_probe(sqlglot.parse_one(sql, dialect="snowflake"), dialect=SnowflakeDialect()),
        db_type="snowflake",
        engine=sf_engine,
    )
    assert types["as_number"] == ResultColumnType(type=DataType.INT, db_type="NUMBER(38,0)")
    assert types["as_double"].type == DataType.DOUBLE
    assert types["as_text"].type == DataType.TEXT
    assert types["as_bool"].type == DataType.BOOLEAN
    assert types["as_date"].type == DataType.DATE
    assert types["as_ts"].type == DataType.TIMESTAMP


def test_session_overrides_apply_to_metadata_reads(sf_transient_schema) -> None:
    profile = _profile_or_skip()
    if not all(profile.get(k) for k in ("role", "warehouse", "database")):
        pytest.skip("profile lacks role / warehouse / database to override")
    ds = DatasourceConfig(
        name="sf_overrides", type="snowflake", connection_name=_CONNECTION_NAME,
        role=profile["role"], warehouse=profile["warehouse"], database=profile["database"],
        schema_name=sf_transient_schema,
    )
    states: dict[int, tuple] = {}

    def _record(dbapi_conn, _conn_record, _proxy) -> None:  # noqa: ANN001
        cur = dbapi_conn.cursor()
        try:
            cur.execute("SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
            states[id(dbapi_conn)] = tuple(cur.fetchone())
        finally:
            cur.close()

    sa_engine = engine_factory.get_engine(ds)
    event.listen(sa_engine, "checkout", _record)
    sql_client = client.SlayerSQLClient(datasource=ds)
    locations = [TableLocation(name=n) for n in ("orders", "customers", "products", "regions")]

    async def _reads() -> None:
        await asyncio.gather(
            sql_client.list_objects(None),
            *(sql_client.reflect_columns(loc) for loc in locations),
            *(sql_client.reflect_table_keys(loc) for loc in locations),
        )

    try:
        # A held connection forces the client's reads onto further pooled connections.
        with sa_engine.connect():
            run_sync(_reads())
    finally:
        sql_client.close()
        engine_factory.invalidate_engine(ds)
    expected = tuple(v.upper() for v in (profile["role"], profile["warehouse"], profile["database"], sf_transient_schema))
    assert len(states) >= 2
    assert set(states.values()) == {expected}


# ---------------------------------------------------------------------------
# Auto-ingestion + FK discovery
# ---------------------------------------------------------------------------


def test_auto_ingest_discovers_joins(sf_datasource, sf_transient_schema) -> None:
    """Snowflake stores FK constraints declaratively and exposes them via
    Inspector. Auto-ingestion should pick them up and produce models with
    ``joins:`` entries. Pins the corrected behavior post-spec v2: Snowflake
    is NOT in the no-FK list.

    Coverage:
      - orders → customers (FK on customer_id)
      - orders → products (FK on product_id)
      - customers → regions (second-hop FK on region_id)
      - source/target column names on each join
    """
    schema_to_introspect = sf_transient_schema
    models = run_sync(_ingest(sf_datasource, schema=schema_to_introspect))
    by_name = {m.name.lower(): m for m in models}
    orders = by_name.get("orders")
    customers = by_name.get("customers")
    assert orders is not None, f"orders model not ingested. Got: {list(by_name)}"
    assert customers is not None, f"customers model not ingested. Got: {list(by_name)}"

    # orders → customers / products. ModelJoin stores join_pairs as
    # [[source_col, target_col], ...]; flatten the first pair for the
    # standard single-column FK case.
    def _first_pair(j) -> tuple[str, str]:
        pair = j.join_pairs[0]
        return (pair[0].lower(), pair[1].lower())

    orders_joins = {
        j.target_model.lower(): _first_pair(j)
        for j in (orders.joins or [])
    }
    assert "customers" in orders_joins, f"orders→customers FK not discovered: {orders_joins}"
    assert "products" in orders_joins, f"orders→products FK not discovered: {orders_joins}"
    # Source/target columns are lowercase regardless of Snowflake's
    # uppercase storage (snowflake-sqlalchemy Inspector returns lowercase).
    assert orders_joins["customers"] == ("customer_id", "id")
    assert orders_joins["products"] == ("product_id", "id")

    # customers → regions (the second hop required for the
    # ``orders.customers.regions.name`` multi-hop path in the query tests).
    customers_joins = {
        j.target_model.lower(): _first_pair(j)
        for j in (customers.joins or [])
    }
    assert "regions" in customers_joins, f"customers→regions FK not discovered: {customers_joins}"
    assert customers_joins["regions"] == ("region_id", "id")


def test_auto_ingest_imports_comments(sf_datasource, sf_transient_schema) -> None:
    """Snowflake table/column comments must land on model/column descriptions."""
    models = run_sync(_ingest(sf_datasource, schema=sf_transient_schema))
    by_name = {m.name.lower(): m for m in models}
    orders = by_name["orders"]
    assert orders.description == "All orders"
    cols = {c.name.lower(): c for c in orders.columns}
    assert cols["quantity"].description == "Units ordered"
    assert cols["status"].description is None
    assert by_name["regions"].description is None


# ---------------------------------------------------------------------------
# EXPLAIN
# ---------------------------------------------------------------------------


def test_explain_query_uses_explain_using_json(sf_storage_with_models) -> None:
    """``engine.execute(..., explain=True)`` should call ``EXPLAIN USING JSON``
    on Snowflake (the prefix from _explain_prefix_map). Asserts the call
    succeeds and returns a non-empty plan."""
    engine = SlayerQueryEngine(storage=sf_storage_with_models)
    result = run_sync(engine.execute(
        query=SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="sum(quantity)")],
        ),
        explain=True,
    ))
    rows = result.data
    # Snowflake EXPLAIN USING JSON returns a single row with a JSON plan.
    assert len(rows) >= 1


# ---------------------------------------------------------------------------
# Identifier casing
# ---------------------------------------------------------------------------


def test_lowercase_identifier_resolves_to_uppercase_storage(sf_engine) -> None:
    """Snowflake stores unquoted ``orders`` as ``ORDERS`` but resolves
    unquoted references case-insensitively. sqlglot emits lowercase
    unquoted identifiers for the snowflake dialect, which therefore
    resolves correctly."""
    with sf_engine.connect() as conn:
        result = conn.execute(sa.text("SELECT COUNT(*) FROM orders"))
        assert result.fetchone()[0] == 6


def test_uppercase_identifier_also_resolves(sf_engine) -> None:
    """Uppercase unquoted ``ORDERS`` must resolve identically — Snowflake's
    storage case is uppercase but the resolution is case-insensitive for
    unquoted refs."""
    with sf_engine.connect() as conn:
        result = conn.execute(sa.text("SELECT COUNT(*) FROM ORDERS"))
        assert result.fetchone()[0] == 6


def test_quoted_uppercase_identifier_resolves(sf_engine) -> None:
    '''``"ORDERS"`` (quoted, uppercase) resolves directly against the
    uppercased storage and works. Useful gotcha for users who manually
    quote identifiers.'''
    with sf_engine.connect() as conn:
        result = conn.execute(sa.text('SELECT COUNT(*) FROM "ORDERS"'))
        assert result.fetchone()[0] == 6


def test_quoted_lowercase_identifier_fails(sf_engine) -> None:
    '''``"orders"`` (quoted, lowercase) FAILS — quoted identifiers are
    case-sensitive on Snowflake, and the stored name is uppercase. Pins
    the documented identifier-casing caveat: users writing
    ``"Revenue"`` in Column.sql on Snowflake must match storage exactly.
    '''
    with sf_engine.connect() as conn:
        stmt = sa.text('SELECT COUNT(*) FROM "orders"')
        with pytest.raises((ProgrammingError, sa.exc.DBAPIError)):
            conn.execute(stmt)


def test_inspector_returns_lowercase_column_names(sf_engine, sf_transient_schema) -> None:
    """``snowflake-sqlalchemy`` Inspector lowercases by default. SLayer's
    ingestion relies on this for round-tripping ``Column.name`` with the
    SQL the generator emits. Pass the schema explicitly — the Inspector
    doesn't pick up the per-connection ``USE SCHEMA`` from the listener."""
    inspector = sa.inspect(sf_engine)
    cols = inspector.get_columns("orders", schema=sf_transient_schema)
    names = {c["name"] for c in cols}
    assert "id" in names
    assert "customer_id" in names
    assert "created_at" in names


def test_inspector_returns_lowercase_fk_metadata(sf_engine, sf_transient_schema) -> None:
    """FK introspection casing — referred_table and referred_columns are
    lowercase, matching get_table_names / get_columns output, so the FK
    resolution in ingestion produces models whose join targets match
    other models' names by string equality."""
    inspector = sa.inspect(sf_engine)
    fks = inspector.get_foreign_keys("orders", schema=sf_transient_schema)
    by_referred = {fk["referred_table"]: fk for fk in fks}
    assert "customers" in by_referred, f"FK to customers missing: {by_referred}"
    assert "products" in by_referred, f"FK to products missing: {by_referred}"
    assert by_referred["customers"]["constrained_columns"] == ["customer_id"]
    assert by_referred["customers"]["referred_columns"] == ["id"]


def test_query_result_keys_use_lowercase(sf_storage_with_models) -> None:
    """Result-column keys returned by SlayerQueryEngine use the model's
    lowercase column names (not Snowflake's uppercase storage form).
    Pins the user-facing API: `result.rows[0]["orders.status"]` works,
    `result.rows[0]["ORDERS.STATUS"]` would silently miss."""
    engine = SlayerQueryEngine(storage=sf_storage_with_models)
    result = run_sync(engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[ModelMeasure(formula="count(*)")],
    )))
    rows = result.data
    assert rows
    first_row = rows[0]
    keys = set(first_row.keys())
    # Lowercase form is the canonical key.
    assert "orders.status" in keys, f"Expected lowercase 'orders.status' in row keys, got: {keys}"


# ---------------------------------------------------------------------------
# Mixed-case identifiers must be double-quoted for Snowflake.
#
# Snowflake folds UNQUOTED identifiers to UPPERCASE. A column created via
# QUOTED DDL (``"StateFlag"``) is stored with genuinely mixed case, so an
# UNQUOTED reference (which folds to ``STATEFLAG``) fails to resolve — exactly
# the upper-folding mirror of the Postgres bug. The fix quotes it, so the
# model (which writes ``StateFlag`` unquoted, as an OTF agent would) executes.
# ---------------------------------------------------------------------------

@pytest.fixture
def sf_dev1645_storage(sf_transient_schema: str, sf_datasource: DatasourceConfig, tmp_path):
    conn = snowflake.connector.connect(connection_name=_CONNECTION_NAME)
    cur = conn.cursor()
    try:
        cur.execute(f"USE SCHEMA {sf_transient_schema}")
        # QUOTED mixed-case column => genuinely stored as "StateFlag".
        cur.execute('CREATE TABLE dev1645_accounts (id INT, "StateFlag" TEXT)')
        cur.executemany(
            'INSERT INTO dev1645_accounts (id, "StateFlag") VALUES (%s, %s)',
            [(1, "Dormant"), (2, "Active"), (3, "Dormant")],
        )
        conn.commit()

        storage = YAMLStorage(base_dir=str(tmp_path))
        run_sync(storage.save_datasource(sf_datasource))
        model = SlayerModel(
            name="dev1645_accounts",
            sql_table="dev1645_accounts",
            data_source="sf_test",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                # Written UNQUOTED — the fix must emit "StateFlag".
                Column(name="is_inactive", type=DataType.BOOLEAN,
                       sql="CASE WHEN StateFlag = 'Dormant' THEN TRUE ELSE FALSE END"),
            ],
        )
        run_sync(storage.save_model(model))
        yield storage
    finally:
        try:
            cur.execute("DROP TABLE IF EXISTS dev1645_accounts")
            conn.commit()
        finally:
            cur.close()
            conn.close()


def test_dev1645_mixed_case_column_executes(sf_dev1645_storage) -> None:
    """Filter on a derived column referencing UNQUOTED mixed-case StateFlag —
    must execute on Snowflake (upper-folding), proving universal quoting is
    correct there too."""
    engine = SlayerQueryEngine(storage=sf_dev1645_storage)
    result = run_sync(engine.execute(SlayerQuery(
        source_model="dev1645_accounts",
        measures=[ModelMeasure(formula="count(*)", name="cnt")],
        filters=["is_inactive == True"],
    )))
    assert result.data[0]["dev1645_accounts.cnt"] == 2


def test_every_snowflake_alias_failure_is_quoted(sf_engine) -> None:
    assert unquoted_alias_failures(
        dialect="snowflake", words=keyword_universe(SNOWFLAKE_KEYWORDS), table="regions", column="id",
        fails=sa_statement_fails(sf_engine), workers=cast(QueuePool, sf_engine.pool).size(),
    ) == []


# ---------------------------------------------------------------------------
# Portable Mode-B library (spec: queries/scalar-functions, regex, json, mixed-type-coercion, string-matching)
# ---------------------------------------------------------------------------


@pytest.fixture
def sf_dev2120_storage(sf_transient_schema: str, sf_datasource: DatasourceConfig, tmp_path):
    conn = snowflake.connector.connect(connection_name=_CONNECTION_NAME)
    cur = conn.cursor()
    try:
        cur.execute(f"USE SCHEMA {sf_transient_schema}")
        for stmt in dev2120_table_statements(LIB, dialect="snowflake"):
            cur.execute(stmt)
        cur.execute("CREATE TABLE docs (id NUMBER(38,0), payload VARIANT)")
        rows = ", ".join(f"({rid}, {server_literal(doc, kind='TEXT', dialect='snowflake')})" for rid, doc in DOCS_ROWS)
        cur.execute(f"INSERT INTO docs SELECT column1, PARSE_JSON(column2) FROM VALUES {rows}")
        conn.commit()
        storage = YAMLStorage(base_dir=str(tmp_path))
        run_sync(storage.save_datasource(sf_datasource))
        base = lib_model(data_source="sf_test")
        run_sync(storage.save_model(base.model_copy(update={
            "columns": [c.model_copy(update={"sql": c.name}) for c in base.columns]})))
        for model in run_sync(_ingest(sf_datasource, include_tables=["docs"])):
            run_sync(storage.save_model(model))
        yield storage
    finally:
        try:
            cur.execute("DROP TABLE IF EXISTS lib")
            cur.execute("DROP TABLE IF EXISTS docs")
            conn.commit()
        finally:
            cur.close()
            conn.close()


def test_dev2120_library(sf_dev2120_storage) -> None:
    run_sync(check_library(SlayerQueryEngine(storage=sf_dev2120_storage)))


def test_dev2120_variant_column(sf_dev2120_storage) -> None:
    run_sync(check_native_json(SlayerQueryEngine(storage=sf_dev2120_storage)))


# ---------------------------------------------------------------------------
# List columns: untyped ARRAY element inference (spec: models/list-columns, queries/list-functions,
# queries/list-children, aggregations/collect)
# ---------------------------------------------------------------------------

_INFERRED = (
    "CREATE TABLE inferred (id NUMBER(38,0), ints ARRAY, mixed ARRAY, objs ARRAY, empty ARRAY)",
    "INSERT INTO inferred SELECT 1, ARRAY_CONSTRUCT(1, 2), ARRAY_CONSTRUCT('a', 1), "
    "ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('k', 1)), ARRAY_CONSTRUCT()",
    "CREATE TABLE structured (id NUMBER(38,0), typed ARRAY(VARCHAR))",
    "INSERT INTO structured SELECT 1, ['go', 'sql']::ARRAY(VARCHAR) UNION ALL SELECT 2, ['sql']::ARRAY(VARCHAR)",
)


@pytest.fixture(scope="module")
def _sf_list_tables(sf_transient_schema: str):
    """The list tables, seeded once per module; tests only read them."""
    conn = snowflake.connector.connect(connection_name=_CONNECTION_NAME)
    cur = conn.cursor()
    try:
        cur.execute(f"USE SCHEMA {sf_transient_schema}")
        for stmt in [*contractors_statements(dialect="snowflake"), *assignments_statements(dialect="snowflake"),
                     *mixed_numbers_statements(dialect="snowflake"), *_INFERRED]:
            cur.execute(stmt)
        conn.commit()
        yield cur
    finally:
        try:
            for table in ("contractors", "assignments", "mixed", "inferred", "structured"):
                cur.execute(f"DROP TABLE IF EXISTS {table}")
            conn.commit()
        finally:
            cur.close()
            conn.close()


@pytest.fixture
def sf_lists(_sf_list_tables, sf_datasource: DatasourceConfig, tmp_path):
    storage = YAMLStorage(base_dir=str(tmp_path))
    run_sync(storage.save_datasource(sf_datasource))
    for base in (contractors_model(data_source="sf_test"), assignments_model(data_source="sf_test")):
        run_sync(storage.save_model(base.model_copy(update={
            "columns": [c.model_copy(update={"sql": c.name}) for c in base.columns]})))
    run_sync(storage.save_model(mixed_numbers_model(data_source="sf_test", explicit_sql=True)))
    return storage, _sf_list_tables


def _elements(datasource: DatasourceConfig, table: str) -> dict[str, tuple[DataType, DataType | None]]:
    (model,) = run_sync(_ingest(datasource, include_tables=[table]))
    return {c.name: (c.type, c.element_type) for c in model.columns if c.name != "id"}


def test_lists(sf_lists) -> None:
    run_sync(check_lists(SlayerQueryEngine(storage=sf_lists[0]), dialect="snowflake"))


def test_mixed_numeric_lists(sf_lists) -> None:
    run_sync(check_mixed_numbers(SlayerQueryEngine(storage=sf_lists[0])))


def test_untyped_array_elements_inferred(sf_lists, sf_datasource: DatasourceConfig) -> None:
    assert _elements(sf_datasource, "inferred") == {
        "ints": (DataType.ARRAY, DataType.INT), "mixed": (DataType.ARRAY, DataType.TEXT),
        "objs": (DataType.ARRAY, DataType.UNKNOWN), "empty": (DataType.ARRAY, DataType.UNKNOWN)}
    assert _elements(sf_datasource, "contractors")["scores"] == (DataType.ARRAY, DataType.INT)


def test_structured_array_needs_no_inference_query(sf_lists, sf_datasource: DatasourceConfig) -> None:
    statements: list[str] = []

    def record(conn, cursor, statement, *args) -> None:  # noqa: ARG001 — SQLAlchemy listener signature
        statements.append(statement)

    event.listen(sa.engine.Engine, "before_cursor_execute", record)
    try:
        assert _elements(sf_datasource, "structured") == {"typed": (DataType.ARRAY, DataType.TEXT)}
    finally:
        event.remove(sa.engine.Engine, "before_cursor_execute", record)
    assert not [s for s in statements if "FLATTEN" in s.upper()], statements


@pytest.mark.parametrize(("filters", "kept"), [
    (["contains(typed, 'go')"], {1}),
    (["contains_any(typed, ['go', 'rust'])"], {1}),
    (["contains_all(typed, ['go', 'sql'])"], {1}),
    (["length(typed) = 1"], {2}),
    (["typed.value = 'sql'"], {1, 2}),
])
def test_list_functions_on_a_structured_array(sf_lists, sf_datasource: DatasourceConfig, filters, kept) -> None:
    storage, _ = sf_lists
    (model,) = run_sync(_ingest(sf_datasource, include_tables=["structured"]))
    run_sync(storage.save_model(model))
    resp = run_sync(SlayerQueryEngine(storage=storage).execute(
        {"source_model": "structured", "dimensions": ["id"], "filters": filters}))
    assert {int(r["structured.id"]) for r in resp.data} == kept, resp.sql


def test_reingest_reinfers_only_unknown_elements(sf_lists, sf_datasource: DatasourceConfig) -> None:
    storage, cur = sf_lists
    cur.execute("CREATE OR REPLACE TABLE reingested CLONE inferred")  # a private copy: the seeded tables stay read-only
    try:
        for model in run_sync(_ingest(sf_datasource, include_tables=["reingested"])):
            run_sync(storage.save_model(model))
        cur.execute("INSERT INTO reingested SELECT 2, ARRAY_CONSTRUCT('x'), NULL, NULL, ARRAY_CONSTRUCT(7)")
        _ingest_idempotent(datasource=sf_datasource, storage=storage, include_tables=["reingested"])
        model = run_sync(storage.get_model("reingested", data_source="sf_test"))
    finally:
        cur.execute("DROP TABLE IF EXISTS reingested")
    typed = {c.name: c.element_type for c in model.columns if c.name in ("ints", "empty")}
    assert typed == {"ints": DataType.INT, "empty": DataType.INT}


# ---------------------------------------------------------------------------
# A warm statement costs one request
# ---------------------------------------------------------------------------


def _session_history(session_id: str) -> list[str]:
    """The statements one Snowflake session sent, in order."""
    conn = snowflake.connector.connect(connection_name=_CONNECTION_NAME)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT query_text FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION("
            "SESSION_ID => %s, RESULT_LIMIT => 1000)) ORDER BY start_time",
            (int(session_id),),
        )
        return [text for (text,) in cur.fetchall()]
    finally:
        conn.close()


def _first_value(rows: list[dict[str, Any]]) -> Any:
    return next(iter(rows[0].values()))


def _overrides_datasource(schema: str) -> DatasourceConfig:
    profile = _profile_or_skip()
    if not all(profile.get(k) for k in ("role", "warehouse", "database")):
        pytest.skip("profile lacks role / warehouse / database to override")
    return DatasourceConfig(
        name="sf_overrides", type="snowflake", connection_name=_CONNECTION_NAME,
        role=profile["role"], warehouse=profile["warehouse"], database=profile["database"], schema_name=schema,
    )


@pytest.mark.parametrize("path", PATHS)
def test_warm_statement_costs_one_request(sf_datasource: DatasourceConfig, path: str) -> None:
    engine_factory.invalidate_engine(sf_datasource)
    try:
        rows = run_three(
            client.SlayerSQLClient(datasource=sf_datasource), path=path,
            sql=lambda marker: f"SELECT CURRENT_SESSION() AS sid, '{marker}' AS m",
        )
    finally:
        engine_factory.invalidate_engine(sf_datasource)
    assert_one_request_each(_session_history(_first_value(rows[0])))


def test_session_overrides_are_set_once_per_connection(sf_transient_schema: str) -> None:
    ds = _overrides_datasource(sf_transient_schema)
    engine_factory.invalidate_engine(ds)
    try:
        rows = run_three(
            client.SlayerSQLClient(datasource=ds), path="sync",
            sql=lambda marker: (
                "SELECT CURRENT_SESSION() AS sid, CURRENT_ROLE() AS r, CURRENT_WAREHOUSE() AS w,"
                f" CURRENT_DATABASE() AS d, CURRENT_SCHEMA() AS s, '{marker}' AS m"
            ),
        )
    finally:
        engine_factory.invalidate_engine(ds)
    states = [tuple(r[0].values()) for r in rows]
    assert {state[0] for state in states} == {states[0][0]}, "statements ran on more than one connection"
    expected = tuple(v.upper() for v in (ds.role, ds.warehouse, ds.database, ds.schema_name))  # type: ignore[union-attr]
    assert [state[1:5] for state in states] == [expected] * 3
    history = _session_history(states[0][0])
    first = next(i for i, text in enumerate(history) if MARKERS[0] in text)
    uses = [text.upper() for text in history if text.lstrip().upper().startswith("USE ")]
    assert [u.split()[1] for u in uses] == ["ROLE", "WAREHOUSE", "DATABASE", "SCHEMA"], history
    assert all(text.upper() not in uses for text in history[first:]), history


def test_use_statement_is_refused(sf_transient_schema: str) -> None:
    ds = _overrides_datasource(sf_transient_schema)
    engine_factory.invalidate_engine(ds)
    sql_client = client.SlayerSQLClient(datasource=ds)
    try:
        session_id = _first_value(sql_client.execute_sync("SELECT CURRENT_SESSION() AS sid").rows)
        with pytest.raises(client.SessionStatementRefused, match="USE"):
            sql_client.execute_sync("USE WAREHOUSE other_wh")
        warehouse = _first_value(sql_client.execute_sync("SELECT CURRENT_WAREHOUSE() AS w").rows)
    finally:
        sql_client.close()
        engine_factory.invalidate_engine(ds)
    assert warehouse == ds.warehouse.upper()  # type: ignore[union-attr]
    assert not any("OTHER_WH" in text.upper() for text in _session_history(session_id))


def test_quoted_lowercase_table_ingests_types_and_queries(sf_datasource, sf_engine, sf_transient_schema,
                                                         tmp_path) -> None:
    '''A quoted lower-case ``"lower_items"`` is not ``LOWER_ITEMS``: ingestion and import typing name it exactly.'''
    with sf_engine.connect() as conn:
        conn.execute(sa.text('CREATE TABLE "lower_items" (qty NUMBER(38,0), "tags" ARRAY)'))
        conn.execute(sa.text('INSERT INTO "lower_items" SELECT 7, ARRAY_CONSTRUCT(1, 2)'))
        conn.commit()
    try:
        live = LiveDatasource(offline_reason=None, data_source="sf_test", db_type="snowflake", dialect="snowflake")
        sql_table = f'{sf_transient_schema}."lower_items"'
        caller = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(tmp_path / "caller")),
                                   client_factory=lambda ds: client.SlayerSQLClient(datasource=ds, sa_engine=sf_engine))
        try:
            caller.run_sync(live.collect(client=caller.client_for(sf_datasource),
                                         convert=lambda: live.source(sql_table=sql_table)))
        finally:
            caller.close()
        source = live.source(sql_table=sql_table)
        assert isinstance(source, LiveSource)
        assert source.column("qty") == (DataType.INT, None)
        storage = YAMLStorage(base_dir=str(tmp_path))
        run_sync(storage.save_datasource(sf_datasource))
        _ingest_idempotent(datasource=sf_datasource, storage=storage, schema=sf_transient_schema,
                           include_tables=["lower_items"])
        model = run_sync(storage.get_model("lower_items"))
        assert model is not None
        assert model.sql_table == f'{sf_transient_schema}."lower_items"'
        tags = model.get_column("tags")
        assert tags is not None
        assert tags.element_type is DataType.INT
        assert tags.sql == '"tags"'
        engine = SlayerQueryEngine(storage=storage)
        try:
            rows = run_sync(engine.execute({"source_model": "lower_items", "dimensions": ["qty"]})).data
        finally:
            engine.close()
        assert [r["lower_items.qty"] for r in rows] == [7]
    finally:
        with sf_engine.connect() as conn:
            conn.execute(sa.text('DROP TABLE IF EXISTS "lower_items"'))
            conn.commit()


def test_join_cardinality_profiles_snowflake_keys(sf_storage_with_models) -> None:
    '''Profiling reads ``orders``/``customer_id`` as Snowflake folds them (``ORDERS``/``CUSTOMER_ID``).'''
    engine = SlayerQueryEngine(storage=sf_storage_with_models)
    try:
        report = run_sync(engine.detect_join_cardinality(data_source="sf_test", model="orders"))
    finally:
        engine.close()
    (finding,) = [f for f in report.findings if f.target_model == "customers"]
    assert finding.detected is JoinCardinality.MANY_TO_ONE
