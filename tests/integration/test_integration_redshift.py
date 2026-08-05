"""Live integration tests for Redshift.

Skip-by-default: requires ``sqlalchemy-redshift`` + ``redshift-connector``
(the ``motley-slayer[redshift]`` extra) and a real Redshift cluster/serverless
endpoint reachable via env vars:

    REDSHIFT_HOST      (required — cluster/serverless endpoint, no port)
    REDSHIFT_PORT       (optional, default 5439)
    REDSHIFT_DATABASE   (required)
    REDSHIFT_USER       (required)
    REDSHIFT_PASSWORD   (required)

Redshift has no free local Docker image (unlike Postgres/MySQL/ClickHouse/
SQL Server) and no shared local-profile convention (unlike Snowflake's
``~/.snowflake/connections.toml``), so — like BigQuery — this suite is
env-var-gated rather than file-gated, and skips cleanly in any environment
without those set (including plain `pytest` runs in this repo today).

Unlike the Snowflake suite, this does NOT assert on auto-ingestion's FK-join
discovery: Redshift allows declaring (unenforced) FOREIGN KEY constraints,
but whether SQLAlchemy's Inspector surfaces them through
``sqlalchemy-redshift`` for auto-ingestion to pick up is unverified absent a
live cluster. Models below define ``joins=`` explicitly instead, which
exercises the query/rollup path regardless of that answer.
"""

import os
import uuid

import pytest

# Skip the entire module if the redshift extras aren't installed.
pytest.importorskip("redshift_connector")
pytest.importorskip("sqlalchemy_redshift")

import redshift_connector  # noqa: E402

from slayer.async_utils import run_sync  # noqa: E402
from slayer.core.enums import DataType  # noqa: E402
from slayer.core.models import (  # noqa: E402
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery  # noqa: E402
from slayer.engine.query_engine import SlayerQueryEngine  # noqa: E402
from slayer.storage.yaml_storage import YAMLStorage  # noqa: E402

_HOST = os.environ.get("REDSHIFT_HOST")
_PORT = int(os.environ.get("REDSHIFT_PORT", "5439"))
_DATABASE = os.environ.get("REDSHIFT_DATABASE")
_USER = os.environ.get("REDSHIFT_USER")
_PASSWORD = os.environ.get("REDSHIFT_PASSWORD")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not (_HOST and _DATABASE and _USER and _PASSWORD),
        reason=(
            "REDSHIFT_HOST/REDSHIFT_DATABASE/REDSHIFT_USER/REDSHIFT_PASSWORD not all set; skipping Redshift live tests"
        ),
    ),
]


@pytest.fixture(scope="module")
def rs_transient_schema():
    """Module-scoped: a uniquely-named schema created for this test run.
    Returns the schema name. Teardown drops it with CASCADE.

    Teardown safety: ``cur`` and ``schema_name`` are bound BEFORE entering
    the try block so the finally clause can rely on them existing even when
    early setup fails.
    """
    conn = redshift_connector.connect(
        host=_HOST,
        port=_PORT,
        database=_DATABASE,
        user=_USER,
        password=_PASSWORD,
    )
    schema_name = f"slayer_test_{uuid.uuid4().hex[:12]}"
    cur = conn.cursor()
    schema_created = False
    try:
        cur.execute(f"CREATE SCHEMA {schema_name}")
        schema_created = True
        cur.execute(f"SET search_path TO {schema_name}")
        cur.execute("""
            CREATE TABLE regions (
                id BIGINT PRIMARY KEY,
                name VARCHAR(64) NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE customers (
                id BIGINT PRIMARY KEY,
                name VARCHAR(128) NOT NULL,
                email VARCHAR(256) NOT NULL,
                region_id BIGINT
            )
        """)
        cur.execute("""
            CREATE TABLE products (
                id BIGINT PRIMARY KEY,
                name VARCHAR(128) NOT NULL,
                category VARCHAR(64) NOT NULL,
                price DECIMAL(10, 2) NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE orders (
                id BIGINT PRIMARY KEY,
                customer_id BIGINT,
                product_id BIGINT,
                quantity BIGINT NOT NULL,
                status VARCHAR(32) NOT NULL,
                created_at TIMESTAMP NOT NULL
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
        if schema_created:
            try:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
                conn.commit()
            except Exception:
                # Don't mask the original error if setup failed mid-way.
                pass
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


@pytest.fixture
def rs_datasource(rs_transient_schema: str) -> DatasourceConfig:
    return DatasourceConfig(
        name="rs_test",
        type="redshift",
        host=_HOST,
        port=_PORT,
        database=_DATABASE,
        username=_USER,
        password=_PASSWORD,
        schema_name=rs_transient_schema,
    )


@pytest.fixture
def rs_storage_with_models(rs_datasource: DatasourceConfig, tmp_path):
    """YAMLStorage with the four standard models pre-saved, joins declared
    explicitly (see module docstring)."""
    storage = YAMLStorage(base_dir=str(tmp_path))
    run_sync(storage.save_datasource(rs_datasource))

    orders = SlayerModel(
        name="orders",
        sql_table=f"{rs_datasource.schema_name}.orders",
        data_source="rs_test",
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
        sql_table=f"{rs_datasource.schema_name}.customers",
        data_source="rs_test",
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
        sql_table=f"{rs_datasource.schema_name}.products",
        data_source="rs_test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="category", sql="category", type=DataType.TEXT),
            Column(name="price", sql="price", type=DataType.DOUBLE),
        ],
    )
    regions = SlayerModel(
        name="regions",
        sql_table=f"{rs_datasource.schema_name}.regions",
        data_source="rs_test",
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


def test_basic_query(rs_storage_with_models) -> None:
    """SELECT sum(quantity) FROM orders — minimum smoke for the engine path,
    exercising the redshift-connector driver end to end."""
    engine = SlayerQueryEngine(storage=rs_storage_with_models)
    result = run_sync(
        engine.execute(
            SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="quantity:sum")],
            )
        )
    )
    rows = result.data
    assert len(rows) == 1
    # 6 orders, quantities 2+1+5+3+1+4 = 16
    val = next(iter(rows[0].values()))
    assert int(val) == 16


def test_query_with_dimension(rs_storage_with_models) -> None:
    """Group-by status; assert 3 status groups."""
    engine = SlayerQueryEngine(storage=rs_storage_with_models)
    result = run_sync(
        engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="*:count")],
            )
        )
    )
    rows = result.data
    statuses = {row.get("orders.status") for row in rows}
    assert statuses == {"completed", "pending", "cancelled"}


def test_rollup_join_via_explicit_joins(rs_storage_with_models) -> None:
    """Cross-model measure: orders.customers.regions.name (multi-hop join).

    Seed data: customers 1+3 live in US, customer 2 lives in EU, region 3
    (APAC) has no customers — and the LEFT JOIN chain starts at orders, so
    APAC never surfaces. Exercising orders -> customers -> regions correctly
    yields exactly {US, EU}."""
    engine = SlayerQueryEngine(storage=rs_storage_with_models)
    result = run_sync(
        engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="customers.regions.name")],
                measures=[ModelMeasure(formula="quantity:sum")],
            )
        )
    )
    rows = result.data
    region_names = {row.get("orders.customers.regions.name") for row in rows}
    assert region_names == {"US", "EU"}


def test_approx_count_distinct_uses_approximate_keyword(rs_storage_with_models) -> None:
    """RedshiftDialect.build_approx_count_distinct emits the keyword-prefix
    form ``APPROXIMATE COUNT(DISTINCT x)`` rather than a native aggregate
    function name — this is the one Tier-2 SQL-shape quirk this dialect
    encodes (slayer/sql/dialects/_tier2.py). 3 distinct customer_ids across
    6 orders."""
    engine = SlayerQueryEngine(storage=rs_storage_with_models)
    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="customer_id:count_distinct_approx")],
    )
    dry_run_result = run_sync(engine.execute(query, dry_run=True))
    assert "APPROXIMATE COUNT(DISTINCT" in dry_run_result.sql

    result = run_sync(engine.execute(query))
    rows = result.data
    assert len(rows) == 1
    val = next(iter(rows[0].values()))
    assert int(val) == 3
