"""Live integration tests for Snowflake (DEV-1551).

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

import os
import pathlib
import tomllib
import uuid

import pytest

# Skip the entire module if the snowflake extras aren't installed.
pytest.importorskip("snowflake.connector")
pytest.importorskip("snowflake.sqlalchemy")

import snowflake.connector  # noqa: E402
import sqlalchemy as sa  # noqa: E402

from slayer.async_utils import run_sync  # noqa: E402
from slayer.core.enums import DataType, TimeGranularity  # noqa: E402
from slayer.core.models import (  # noqa: E402
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension  # noqa: E402
from slayer.engine.ingestion import ingest_datasource  # noqa: E402
from slayer.engine.query_engine import SlayerQueryEngine  # noqa: E402
from slayer.storage.yaml_storage import YAMLStorage  # noqa: E402

_TOML_PATH = pathlib.Path("~/.snowflake/connections.toml").expanduser()
_CONNECTION_NAME = os.environ.get("SLAYER_SNOWFLAKE_CONNECTION", "slayer_test")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _TOML_PATH.is_file(),
        reason=f"{_TOML_PATH} not present; skipping Snowflake live tests",
    ),
]


def _load_profile() -> dict:
    """Read the named profile from connections.toml. Returns {} if the
    profile is absent; tests using this surface their own skip reason."""
    with open(_TOML_PATH, "rb") as f:
        data = tomllib.load(f)
    profile = data.get(_CONNECTION_NAME)
    if not isinstance(profile, dict):
        return {}
    return profile


def _profile_or_skip() -> dict:
    profile = _load_profile()
    if not profile:
        pytest.skip(
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
        pytest.skip(f"Could not connect to Snowflake via '{_CONNECTION_NAME}': {exc}")
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
    from slayer.sql import engine_factory
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
        measures=[ModelMeasure(formula="quantity:sum")],
    )))
    rows = result.data
    assert len(rows) == 1
    # 6 orders, quantities 2+1+5+3+1+4 = 16
    val = next(iter(rows[0].values()))
    assert int(val) == 16


def test_query_with_dimension(sf_storage_with_models) -> None:
    """Group-by status; assert 3 status groups."""
    engine = SlayerQueryEngine(storage=sf_storage_with_models)
    result = run_sync(engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[ModelMeasure(formula="*:count")],
    )))
    rows = result.data
    statuses = {row.get("orders.status") for row in rows}
    assert statuses == {"completed", "pending", "cancelled"}


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
        measures=[ModelMeasure(formula="quantity:sum")],
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
            ModelMeasure(formula="quantity:median"),
            ModelMeasure(formula="quantity:percentile(p=0.5)"),
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
            ModelMeasure(formula="quantity:stddev_samp"),
            ModelMeasure(formula="quantity:var_samp"),
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
            ModelMeasure(formula="quantity:corr(other=customer_id)"),
            ModelMeasure(formula="quantity:covar_samp(other=customer_id)"),
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
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.QUARTER,
        )],
        measures=[ModelMeasure(formula="quantity:sum")],
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
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="quantity:sum"),
            ModelMeasure(formula="time_shift(quantity:sum, -1, 'month')", name="prev_month"),
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
            ModelMeasure(formula="quantity:sum"),
            ModelMeasure(formula="rank(quantity:sum)", name="qty_rank"),
            ModelMeasure(formula="dense_rank(quantity:sum)", name="qty_dense_rank"),
            ModelMeasure(formula="ntile(quantity:sum, n=2)", name="qty_bucket"),
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
    from slayer.sql import engine_factory

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
    import snowflake.connector.errors as sf_errors

    from slayer.sql import client

    connection_string = "snowflake://?connection_name=" + _CONNECTION_NAME
    with pytest.raises((sf_errors.ProgrammingError, sa.exc.DBAPIError)) as exc_info:
        client._execute_sql_sync(
            sql="CALL SYSTEM$WAIT(5, 'SECONDS')",
            connection_string=connection_string,
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
    ``_get_column_types_sync`` and produce the expected SLayer categories."""
    from slayer.sql import client

    connection_string = "snowflake://?connection_name=" + _CONNECTION_NAME
    sql = """
        SELECT
            CAST(1 AS NUMBER) AS as_number,
            CAST(1.5 AS DOUBLE) AS as_double,
            CAST('foo' AS VARCHAR) AS as_text,
            CAST(TRUE AS BOOLEAN) AS as_bool,
            CAST('2024-01-01' AS DATE) AS as_date,
            CAST('2024-01-01 10:00' AS TIMESTAMP_NTZ) AS as_ts
    """
    types = client._get_column_types_sync(
        sql=sql,
        connection_string=connection_string,
        db_type="snowflake",
        engine=sf_engine,
    )
    assert types["as_number"] == "number"
    assert types["as_double"] == "number"
    assert types["as_text"] == "string"
    assert types["as_bool"] == "boolean"
    assert types["as_date"] == "time"
    assert types["as_ts"] == "time"


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
    models = ingest_datasource(datasource=sf_datasource, schema=schema_to_introspect)
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
            measures=[ModelMeasure(formula="quantity:sum")],
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
    from snowflake.connector.errors import ProgrammingError
    with sf_engine.connect() as conn:
        with pytest.raises((ProgrammingError, sa.exc.DBAPIError)):
            conn.execute(sa.text('SELECT COUNT(*) FROM "orders"'))


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
        measures=[ModelMeasure(formula="*:count")],
    )))
    rows = result.data
    assert rows
    first_row = rows[0]
    keys = set(first_row.keys())
    # Lowercase form is the canonical key.
    assert "orders.status" in keys, f"Expected lowercase 'orders.status' in row keys, got: {keys}"
