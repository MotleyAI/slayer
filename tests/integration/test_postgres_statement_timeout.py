"""Postgres statement timeout on real asyncpg + psycopg: transaction-local, and the probe keeps its read-only guard."""

import uuid

import pytest

from slayer.core.models import DatasourceConfig
from slayer.sql.client import SlayerSQLClient, get_column_types_sync

factories = pytest.importorskip("pytest_postgresql.factories")
psycopg = pytest.importorskip("psycopg")
pg_sql = pytest.importorskip("psycopg.sql")

postgresql_proc = factories.postgresql_proc(port=None)

_TIMEOUT_SQL = "SELECT current_setting('statement_timeout') AS t"


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

    def test_timeout_is_transaction_local(self, pg_client: SlayerSQLClient) -> None:
        pg_client.execute_sync(_TIMEOUT_SQL, timeout_seconds=7)
        with pg_client._get_sync_engine_for_client().connect() as conn:
            assert conn.exec_driver_sql("SHOW statement_timeout").scalar() == "0"

    async def test_async_probe_infers_types(self, pg_client: SlayerSQLClient) -> None:
        assert await pg_client.get_column_types("SELECT 1 AS n, 'a' AS s") == {"n": "number", "s": "string"}

    def test_sync_probe_infers_types(self, pg_client: SlayerSQLClient) -> None:
        types = get_column_types_sync(
            "SELECT 1 AS n, 'a' AS s", engine=pg_client._get_sync_engine_for_client(), db_type="postgres",
        )
        assert types == {"n": "number", "s": "string"}

    async def test_async_probe_keeps_read_only_guard(self, pg_client: SlayerSQLClient) -> None:
        with pytest.raises(Exception, match="read-only transaction"):
            await pg_client.get_column_types("SELECT bump() AS n")

    def test_sync_probe_keeps_read_only_guard(self, pg_client: SlayerSQLClient) -> None:
        engine = pg_client._get_sync_engine_for_client()
        with pytest.raises(Exception, match="read-only transaction"):
            get_column_types_sync("SELECT bump() AS n", engine=engine, db_type="postgres")
