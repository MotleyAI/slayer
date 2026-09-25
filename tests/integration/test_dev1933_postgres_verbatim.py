"""Rendered SQL reaches Postgres verbatim on real asyncpg + psycopg2 (DEV-1933).

The bind hazard (``:too`` from a regex ``(?:...)`` group) and the pyformat ``%``
hazard only surface on a real driver: asyncpg runs the async execute + type-probe
doors, the sync psycopg driver runs the sync door, and the engine runs the reported
``REGEXP_REPLACE`` shape end to end. Through ``text()`` each raised
``A value is required for bind parameter 'too'``.
"""

import uuid

import pytest

from slayer.async_utils import run_sync
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, ModelExtension, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.client import SlayerSQLClient
from slayer.storage.yaml_storage import YAMLStorage

pytest.importorskip("pytest_postgresql")

import psycopg  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above
from psycopg import sql as pg_sql  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above
from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)

_REGEX_LITERAL = "(?i)(?:too complicated|too complex)"

# One statement carrying both hazards: ``:too`` (regex group) and ``%`` (LIKE).
_HAZARD_SQL = (
    "SELECT status FROM orders "
    "WHERE status ~ '(?i)(?:too complicated|too complex)' "
    "OR status LIKE '%pend%' "
    "ORDER BY id"
)
_PROBE_SQL = "SELECT '(?i)(?:too complicated|too complex)'::text AS pat, 'a%b' AS pct"


def _create_db(info) -> str:
    db_name = f"test_{uuid.uuid4().hex[:12]}"
    admin = psycopg.connect(host=info.host, port=info.port, user=info.user, dbname="postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(pg_sql.SQL('CREATE DATABASE {}').format(pg_sql.Identifier(db_name)))
    admin.close()
    conn = psycopg.connect(host=info.host, port=info.port, user=info.user, dbname=db_name)
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT NOT NULL)")
        cur.executemany(
            "INSERT INTO orders VALUES (%s, %s)",
            [(1, "completed"), (2, "pending"), (3, "pending"), (4, "cancelled")],
        )
    conn.commit()
    conn.close()
    return db_name


def _drop_db(info, db_name: str) -> None:
    admin = psycopg.connect(host=info.host, port=info.port, user=info.user, dbname="postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(
            pg_sql.SQL('DROP DATABASE IF EXISTS {} WITH (FORCE)').format(pg_sql.Identifier(db_name))
        )
    admin.close()


@pytest.fixture(scope="module")
def _pg_datasource(postgresql_proc):
    info = postgresql_proc
    db_name = _create_db(info)
    try:
        yield DatasourceConfig(
            name="testpg", type="postgres", host=info.host, port=info.port,
            database=db_name, username=info.user, password="",
        )
    finally:
        _drop_db(info, db_name)


@pytest.fixture
def pg_client(_pg_datasource) -> SlayerSQLClient:
    return SlayerSQLClient(datasource=_pg_datasource)


@pytest.fixture
def pg_env(_pg_datasource, tmp_path) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=str(tmp_path))
    run_sync(storage.save_datasource(_pg_datasource))
    run_sync(storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="testpg",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
        ],
    )))
    return SlayerQueryEngine(storage=storage)


@pytest.mark.integration
class TestPostgresVerbatimExecution:

    async def test_execute_async_regex_and_percent(self, pg_client: SlayerSQLClient) -> None:
        rows = (await pg_client.execute(_HAZARD_SQL)).rows
        assert [r["status"] for r in rows] == ["pending", "pending"]

    async def test_get_column_types_async_regex(self, pg_client: SlayerSQLClient) -> None:
        types = await pg_client.get_column_types(_PROBE_SQL)
        assert types == {"pat": "string", "pct": "string"}

    def test_execute_sync_regex_and_percent(self, pg_client: SlayerSQLClient) -> None:
        rows = pg_client.execute_sync(_HAZARD_SQL).rows
        assert [r["status"] for r in rows] == ["pending", "pending"]

    async def test_regexp_replace_extension_end_to_end(self, pg_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model=ModelExtension(
                source_name="orders",
                columns=[Column(
                    name="rx",
                    sql=f"REGEXP_REPLACE(status, '{_REGEX_LITERAL}', 'X')",
                    type=DataType.TEXT,
                )],
            ),
            dimensions=[ColumnRef(name="rx")],
            measures=[ModelMeasure(formula="*:count")],
        )
        result = await pg_env.execute(query=query)
        # No status matches the pattern, so REGEXP_REPLACE returns each unchanged.
        by_rx = {r["orders.rx"]: r["orders._count"] for r in result.data}
        assert by_rx == {"completed": 1, "pending": 2, "cancelled": 1}
