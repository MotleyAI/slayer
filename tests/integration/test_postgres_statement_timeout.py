"""Postgres statement timeout on real asyncpg + psycopg: in force, and the probe keeps its read-only guard."""

import uuid

import pytest
import sqlglot

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig
from slayer.database.client import (
    ResultColumnType,
    SlayerSQLClient,
    TypeProbe,
    get_result_column_types_sync,
    render_type_probe,
)
from slayer.sql.dialects import PostgresDialect

factories = pytest.importorskip("pytest_postgresql.factories")
psycopg = pytest.importorskip("psycopg")
pg_sql = pytest.importorskip("psycopg.sql")

postgresql_proc = factories.postgresql_proc(port=None)

_TIMEOUT_SQL = "SELECT current_setting('statement_timeout') AS t"


def _probe(sql: str) -> TypeProbe:
    return render_type_probe(sqlglot.parse_one(sql, dialect="postgres"), dialect=PostgresDialect())


def _types(types: dict[str, ResultColumnType]) -> dict[str, DataType]:
    return {name: column.type for name, column in types.items()}


def _admin(info, dbname: str):
    conn = psycopg.connect(host=info.host, port=info.port, user=info.user, dbname=dbname)
    conn.autocommit = True
    return conn


@pytest.fixture(scope="module")
def _pg_datasource(postgresql_proc):
    info = postgresql_proc
    db_name = f"test_{uuid.uuid4().hex[:12]}"
    with _admin(info, "postgres") as admin, admin.cursor() as cur:
        cur.execute(pg_sql.SQL("CREATE DATABASE {}").format(pg_sql.Identifier(db_name)))
    with _admin(info, db_name) as conn, conn.cursor() as cur:
        cur.execute("CREATE SEQUENCE s")
        # IMMUTABLE lets the planner fold it, so the write runs even under the probe's LIMIT 0.
        cur.execute("CREATE FUNCTION bump() RETURNS bigint IMMUTABLE LANGUAGE sql AS 'SELECT nextval(''s'')'")
    try:
        yield DatasourceConfig(
            name="testpg", type="postgres", host=info.host, port=info.port,
            database=db_name, username=info.user, password="",
        )
    finally:
        with _admin(info, "postgres") as admin, admin.cursor() as cur:
            cur.execute(pg_sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(pg_sql.Identifier(db_name)))


@pytest.fixture
def pg_client(_pg_datasource) -> SlayerSQLClient:
    return SlayerSQLClient(datasource=_pg_datasource)


@pytest.mark.integration
class TestPostgresStatementTimeout:
    async def test_async_timeout_in_force(self, pg_client: SlayerSQLClient) -> None:
        result = await pg_client.execute(_TIMEOUT_SQL, timeout_seconds=7)
        assert result.rows == [{"t": "7s"}]
        assert result.warnings == []

    def test_sync_timeout_in_force(self, pg_client: SlayerSQLClient) -> None:
        result = pg_client.execute_sync(_TIMEOUT_SQL, timeout_seconds=7)
        assert result.rows == [{"t": "7s"}]
        assert result.warnings == []

    async def test_async_probe_infers_types(self, pg_client: SlayerSQLClient) -> None:
        types = await pg_client.get_result_column_types(_probe("SELECT 1 AS n, 'a' AS s"))
        assert _types(types) == {"n": DataType.INT, "s": DataType.TEXT}

    def test_sync_probe_infers_types(self, pg_client: SlayerSQLClient) -> None:
        types = get_result_column_types_sync(
            _probe("SELECT 1 AS n, 'a' AS s"), engine=pg_client._get_sync_engine_for_client(), db_type="postgres",
        )
        assert _types(types) == {"n": DataType.INT, "s": DataType.TEXT}

    async def test_async_probe_keeps_read_only_guard(self, pg_client: SlayerSQLClient) -> None:
        probe = _probe("SELECT bump() AS n")
        with pytest.raises(Exception, match="read-only transaction"):
            await pg_client.get_result_column_types(probe)

    def test_sync_probe_keeps_read_only_guard(self, pg_client: SlayerSQLClient) -> None:
        engine = pg_client._get_sync_engine_for_client()
        probe = _probe("SELECT bump() AS n")
        with pytest.raises(Exception, match="read-only transaction"):
            get_result_column_types_sync(probe, engine=engine, db_type="postgres")
