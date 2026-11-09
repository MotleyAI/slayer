"""Postgres session timeout, probe guard and transaction_pooler on a live server (spec: sql/execution)."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Generator
from typing import Any, TypeVar

import psycopg
import pytest
import sqlalchemy.exc
from pytest_postgresql import factories
from sqlalchemy.engine.url import make_url

from slayer.core.models import DatasourceConfig
from slayer.sql import engine_factory
from slayer.sql.client import SlayerSQLClient
from tests._pg_helpers import pg_database
from tests._pg_statement_log import LOGGED_POSTGRES_OPTIONS, ServerLog

pytestmark = pytest.mark.filterwarnings("ignore::FutureWarning")

postgresql_logged = factories.postgresql_proc(port=None, postgres_options=LOGGED_POSTGRES_OPTIONS)

_T = TypeVar("_T")

_SCHEMES = {"psycopg": "postgresql+psycopg", "psycopg2": "postgresql+psycopg2"}

_STATE_SQL = (
    "SELECT current_setting('statement_timeout') AS t, current_setting('transaction_read_only') AS ro, "
    "pg_backend_pid() AS p"
)

_SEED = [
    "CREATE TABLE t (x INTEGER)",
    "CREATE SEQUENCE s",
    # IMMUTABLE lets the planner fold it, so the write runs even under the probe's bound.
    "CREATE FUNCTION bump() RETURNS bigint IMMUTABLE LANGUAGE sql AS 'SELECT nextval(''s'')'",
    # Folded at plan time: the probe's backend ends itself, so its rollback fails.
    "CREATE FUNCTION die() RETURNS boolean IMMUTABLE LANGUAGE sql AS 'SELECT pg_terminate_backend(pg_backend_pid())'",
]


@contextlib.contextmanager
def _logged(
    info: Any, *, driver: str, ds_type: str | None = "postgres", **extra: Any,
) -> Generator[tuple[DatasourceConfig, ServerLog]]:
    with pg_database(info, statements=_SEED, name=f"sess_{driver}") as plain:
        ds = DatasourceConfig.model_validate({
            "name": plain.name, "type": ds_type,
            "connection_string": f"{_SCHEMES[driver]}://{plain.username}@{plain.host}:{plain.port}/{plain.database}",
            **extra,
        })
        assert plain.database is not None
        try:
            yield ds, ServerLog(logfile=info.logfile, database=plain.database)
        finally:
            engine_factory.invalidate_engine(ds)


def _in_one_loop(client: SlayerSQLClient, work: Callable[[], Awaitable[_T]]) -> _T:
    async def _go() -> _T:
        try:
            return await work()
        finally:
            await client.aclose()

    try:
        return asyncio.run(_go())
    finally:
        client.close()


def _admin(ds: DatasourceConfig, sql: str) -> list[tuple[Any, ...]]:
    url = make_url(ds.get_connection_string())
    with psycopg.connect(host=url.host, port=url.port, user=url.username, dbname=url.database, autocommit=True) as c:
        cur = c.execute(sql.encode())
        return cur.fetchall() if cur.description else []


@pytest.fixture(params=sorted(_SCHEMES))
def driver(request: pytest.FixtureRequest) -> str:
    return request.param


# ---------------------------------------------------------------------------
# Session-level timeout, once per connection
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ds_type", ["postgres", None, "totally_unknown"])
def test_session_timeout_is_set_once(postgresql_logged: Any, driver: str, ds_type: str | None) -> None:
    with _logged(postgresql_logged, driver=driver, ds_type=ds_type) as (ds, log):
        client = SlayerSQLClient(ds)

        async def _work() -> list[Any]:
            return [(await client.execute(_STATE_SQL)).rows for _ in range(3)]

        rows = _in_one_loop(client, _work)
        sent = [e.sql for e in log.statements(until="pg_backend_pid") if "statement_timeout = " in e.sql]
    assert [r[0]["t"] for r in rows] == ["2min"] * 3
    assert sent == ["SET statement_timeout = 120000"]


def test_replaced_connection_sets_its_timeout_afresh(postgresql_logged: Any) -> None:
    with _logged(postgresql_logged, driver="psycopg2") as (ds, log):
        client = SlayerSQLClient(ds)
        try:
            first = client.execute_sync(_STATE_SQL).rows[0]
            with engine_factory.get_engine(ds).connect() as conn:
                conn.invalidate()
            log.mark()
            second = client.execute_sync(_STATE_SQL).rows[0]
        finally:
            client.close()
        new_pid = [e for e in log.statements(until="pg_backend_pid") if e.pid == second["p"]]
    assert second["p"] != first["p"]
    assert [e.sql for e in new_pid if "statement_timeout = " in e.sql] == ["SET statement_timeout = 120000"]
    assert second["t"] == "2min"


def test_a_killed_connection_is_replaced_with_its_timeout(postgresql_logged: Any, driver: str) -> None:
    with _logged(postgresql_logged, driver=driver) as (ds, _log):
        client = SlayerSQLClient(ds)

        async def _work() -> tuple[Any, Any]:
            first = (await client.execute(_STATE_SQL)).rows[0]
            _admin(ds, f"SELECT pg_terminate_backend({int(first['p'])})")
            return first, (await client.execute(_STATE_SQL)).rows[0]

        first, second = _in_one_loop(client, _work)
    assert second["p"] != first["p"]
    assert second["t"] == "2min"


# ---------------------------------------------------------------------------
# The type probe: timeout first, then a real read-only transaction, rolled back in finally
# ---------------------------------------------------------------------------


def test_probe_sets_the_timeout_before_its_read_only_transaction(postgresql_logged: Any, driver: str) -> None:
    with _logged(postgresql_logged, driver=driver) as (ds, log):
        client = SlayerSQLClient(ds)

        async def _work() -> None:
            await client.execute("SELECT 'warm' AS m")
            log.mark()
            await client.get_result_column_types("SELECT 1 AS probe_marker")
            await client.execute("SELECT 'after' AS m")

        _in_one_loop(client, _work)
        sent = [e.sql for e in log.statements(until="'after'")]
    probe_at = next(i for i, s in enumerate(sent) if "probe_marker" in s)
    assert sent[:3] == ["SET statement_timeout = 60000", "BEGIN", "SET TRANSACTION READ ONLY"], sent
    assert sent[probe_at + 1].rstrip(";").upper() == "ROLLBACK", sent
    assert sent[probe_at + 2:] == ["SET statement_timeout = 120000", "SELECT 'after' AS m"], sent


@pytest.mark.parametrize("ds_type", ["postgres", None])
def test_probe_keeps_its_read_only_guard(postgresql_logged: Any, driver: str, ds_type: str | None) -> None:
    with _logged(postgresql_logged, driver=driver, ds_type=ds_type) as (ds, _log):
        client = SlayerSQLClient(ds)
        def _probe() -> Awaitable[Any]:
            return client.get_result_column_types("SELECT bump() AS n")

        with pytest.raises(Exception, match="read-only transaction"):
            _in_one_loop(client, _probe)


def test_a_failed_probe_leaves_no_transaction_open(postgresql_logged: Any, driver: str) -> None:
    with _logged(postgresql_logged, driver=driver) as (ds, _log):
        client = SlayerSQLClient(ds)

        async def _work() -> tuple[Any, Any]:
            before = (await client.execute(_STATE_SQL)).rows[0]
            with pytest.raises(Exception, match="missing_table"):
                await client.get_result_column_types("SELECT * FROM missing_table")
            after = (await client.execute(_STATE_SQL)).rows[0]
            await client.execute("INSERT INTO t VALUES (1)")
            return before, after

        before, after = _in_one_loop(client, _work)
        committed = _admin(ds, "SELECT count(*) FROM t")
    assert after["p"] == before["p"]
    assert (after["t"], after["ro"]) == ("2min", "off")
    assert committed == [(1,)]


def test_a_probe_whose_rollback_fails_leaves_a_usable_pool(postgresql_logged: Any, driver: str) -> None:
    with _logged(postgresql_logged, driver=driver) as (ds, _log):
        client = SlayerSQLClient(ds)

        async def _work() -> tuple[Any, Any]:
            before = (await client.execute(_STATE_SQL)).rows[0]
            with pytest.raises(sqlalchemy.exc.OperationalError):  # the connection dies mid-probe
                await client.get_result_column_types("SELECT die() AS n")
            return before, (await client.execute(_STATE_SQL)).rows[0]

        before, after = _in_one_loop(client, _work)
    assert after["p"] != before["p"]
    assert (after["t"], after["ro"]) == ("2min", "off")


# ---------------------------------------------------------------------------
# transaction_pooler keeps the timeout transaction-local
# ---------------------------------------------------------------------------


def test_pooler_timeout_is_transaction_local(postgresql_logged: Any, driver: str) -> None:
    with _logged(postgresql_logged, driver=driver, transaction_pooler=True) as (ds, log):
        client = SlayerSQLClient(ds)

        async def _work() -> list[Any]:
            return [(await client.execute(_STATE_SQL)).rows[0] for _ in range(2)]

        rows = _in_one_loop(client, _work)
        sent = [e.sql for e in log.statements(until="pg_backend_pid") if "statement_timeout = " in e.sql]
    assert [r["t"] for r in rows] == ["2min", "2min"]
    assert sent == ["SET LOCAL statement_timeout = 120000"] * 2


def test_pooler_probe_is_transaction_local_and_read_only(postgresql_logged: Any, driver: str) -> None:
    with _logged(postgresql_logged, driver=driver, transaction_pooler=True) as (ds, log):
        client = SlayerSQLClient(ds)

        async def _work() -> Any:
            await client.get_result_column_types("SELECT 1 AS probe_marker")
            with pytest.raises(Exception, match="read-only transaction"):
                await client.get_result_column_types("SELECT bump() AS n")
            return (await client.execute(_STATE_SQL)).rows[0]

        after = _in_one_loop(client, _work)
        sent = [e.sql for e in log.statements(until="pg_backend_pid") if "statement_timeout = " in e.sql]
    assert sent == ["SET LOCAL statement_timeout = 60000"] * 2 + ["SET LOCAL statement_timeout = 120000"]
    assert (after["t"], after["ro"]) == ("2min", "off")


def test_pooler_leaves_the_session_timeout_unchanged(postgresql_logged: Any) -> None:
    with _logged(postgresql_logged, driver="psycopg2", transaction_pooler=True) as (ds, _log):
        client = SlayerSQLClient(ds)
        try:
            used = client.execute_sync(_STATE_SQL).rows[0]
            with engine_factory.get_engine(ds).connect() as conn:
                session, pid = conn.exec_driver_sql(
                    "SELECT current_setting('statement_timeout'), pg_backend_pid()",
                ).one()
        finally:
            client.close()
    assert pid == used["p"]
    assert (used["t"], session) == ("2min", "0")
