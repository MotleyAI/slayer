"""The engine runs ``explain=True`` through the client's explain operation (specs: sql/client-contract, sql/execution)."""

from __future__ import annotations

import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import duckdb
import pytest
from pytest_postgresql import factories
from sqlalchemy.exc import SQLAlchemyError

from slayer.core.enums import DataType
from slayer.core.errors import ExplainUnsupportedError, SchemaDriftError
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database import client as client_module
from slayer.database import engine_factory
from slayer.database.client import DelegatingSqlClient, ExecutionResult, SlayerSQLClient, SqlClient
from slayer.sql.dialects.base import ExplainStatements
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import owned_query_engine
from tests._pg_helpers import pg_database

postgresql_proc = factories.postgresql_proc(port=None)

_ORDERS_DDL = "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount DOUBLE PRECISION)"
_ORDERS_ROWS = "INSERT INTO orders VALUES (1, 10.0), (2, 20.0)"
_QUERY = {"source_model": "orders", "measures": [{"formula": "sum(amount)", "name": "total"}]}
_PLAN = [{"StmtText": "plan", "NodeId": 1}]


def _orders_model(data_source: str) -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source=data_source,
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
        ],
    )


async def _storage(base: str, datasource: DatasourceConfig) -> YAMLStorage:
    storage = YAMLStorage(base_dir=base)
    await storage.save_datasource(datasource)
    await storage.save_model(_orders_model(datasource.name), _validate=False)
    return storage


class Recorder(DelegatingSqlClient):
    """Records statement execution and explain; answers explain with ``_PLAN`` when ``answer`` is set."""

    def __init__(self, inner: SqlClient | None, *, answer: bool = False) -> None:
        super().__init__(inner)  # pyright: ignore[reportArgumentType]
        self.answer = answer
        self.executed: list[str] = []
        self.explained: list[ExplainStatements] = []

    async def execute(self, sql: str) -> ExecutionResult:
        self.executed.append(sql)
        return await self.inner.execute(sql)

    async def explain(self, statements: ExplainStatements) -> ExecutionResult:
        self.explained.append(statements)
        if self.answer:
            return ExecutionResult(rows=_PLAN)
        return await self.inner.explain(statements)


class Untouchable:
    """A client that must receive no call."""

    def __init__(self, datasource: DatasourceConfig) -> None:
        self.datasource = datasource
        self.called: list[str] = []

    def __getattr__(self, name: str) -> Any:
        async def _call(*_args: Any, **_kwargs: Any) -> Any:
            self.called.append(name)
            raise AssertionError(f"{name} reached the client")

        return _call

    def close(self) -> None:
        return None


@pytest.fixture
def sqlite_ds(tmp_path: Path) -> Iterator[DatasourceConfig]:
    path = str(tmp_path / "orders.db")
    with transaction(path) as cur:
        cur.execute(_ORDERS_DDL.replace("DOUBLE PRECISION", "REAL"))
        cur.execute(_ORDERS_ROWS)
    ds = DatasourceConfig(name="lite", type="sqlite", database=path)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


@pytest.fixture
def duckdb_ds(tmp_path: Path) -> Iterator[DatasourceConfig]:
    path = str(tmp_path / "orders.duckdb")
    con = duckdb.connect(path)
    try:
        con.execute(_ORDERS_DDL.replace("DOUBLE PRECISION", "DOUBLE"))
        con.execute(_ORDERS_ROWS)
    finally:
        con.close()
    ds = DatasourceConfig(name="duck", type="duckdb", database=path)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


@pytest.fixture
def postgres_ds(postgresql_proc: Any) -> Iterator[DatasourceConfig]:
    with pg_database(postgresql_proc, statements=[_ORDERS_DDL, _ORDERS_ROWS], name="explain_pg") as ds:
        yield ds


@pytest.fixture
def door_spy(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Every statement either verbatim door hands the driver."""
    sent: list[str] = []
    sync_door, async_door = client_module._exec_verbatim, client_module._exec_verbatim_async

    def _sync(conn: Any, sql: str) -> Any:
        sent.append(sql)
        return sync_door(conn, sql)

    async def _async(conn: Any, sql: str) -> Any:
        sent.append(sql)
        return await async_door(conn, sql)

    monkeypatch.setattr(client_module, "_exec_verbatim", _sync)
    monkeypatch.setattr(client_module, "_exec_verbatim_async", _async)
    return sent


class TestSingleStatementDialectsKeepTheirExplain:
    @pytest.mark.parametrize(("fixture", "prefix"), [
        ("sqlite_ds", "EXPLAIN QUERY PLAN"), ("duckdb_ds", "EXPLAIN ANALYZE"), ("postgres_ds", "EXPLAIN ANALYZE"),
    ], ids=["sqlite", "duckdb", "postgres"])
    async def test_one_statement_reaches_the_database(
        self, request: pytest.FixtureRequest, tmp_path: Path, door_spy: list[str], fixture: str, prefix: str,
    ) -> None:
        ds: DatasourceConfig = request.getfixturevalue(fixture)
        async with owned_query_engine(storage=await _storage(str(tmp_path / "store"), ds)) as engine:
            response = await engine.execute(_QUERY, explain=True)
        assert response.sql
        assert [sql for sql in door_spy if response.sql in sql] == [f"{prefix} {response.sql}"]
        assert response.data


class TestTheEngineCallsExplain:
    async def test_explain_reaches_the_explain_operation_never_execute(
        self, tmp_path: Path, sqlite_ds: DatasourceConfig,
    ) -> None:
        recorders: list[Recorder] = []

        def factory(ds: DatasourceConfig) -> Recorder:
            recorders.append(Recorder(SlayerSQLClient(datasource=ds)))
            return recorders[-1]

        storage = await _storage(str(tmp_path / "store"), sqlite_ds)
        async with owned_query_engine(storage=storage, client_factory=factory) as engine:
            dry = await engine.execute(_QUERY, dry_run=True)
            response = await engine.execute(_QUERY, explain=True)
        assert response.sql == dry.sql
        assert recorders[0].explained == [ExplainStatements(statement=f"EXPLAIN QUERY PLAN {dry.sql}")]
        assert recorders[0].executed == []
        assert response.data

    async def test_sql_server_explains_with_showplan_switches(self) -> None:
        recorders: list[Recorder] = []

        def factory(_ds: DatasourceConfig) -> Recorder:
            recorders.append(Recorder(None, answer=True))
            return recorders[-1]

        with tempfile.TemporaryDirectory() as tmp:
            ds = DatasourceConfig(name="mssql", type="mssql", database="d")
            engine = SlayerQueryEngine(storage=await _storage(tmp, ds), client_factory=factory)
            dry = await engine.execute(_QUERY, dry_run=True)
            response = await engine.execute(_QUERY, explain=True)
            engine.close()
        assert dry.sql is not None
        assert recorders[0].explained == [ExplainStatements(
            statement=dry.sql, session_on="SET SHOWPLAN_ALL ON", session_off="SET SHOWPLAN_ALL OFF",
        )]
        assert recorders[0].executed == []
        assert response.data == _PLAN
        assert response.sql == dry.sql

    async def test_bigquery_refuses_before_any_client_call(self) -> None:
        clients: list[Untouchable] = []

        def factory(ds: DatasourceConfig) -> Untouchable:
            clients.append(Untouchable(ds))
            return clients[-1]

        with tempfile.TemporaryDirectory() as tmp:
            ds = DatasourceConfig(name="bq", type="bigquery", connection_string="bigquery://proj/dset")
            engine = SlayerQueryEngine(storage=await _storage(tmp, ds), client_factory=factory)  # pyright: ignore[reportArgumentType]
            with pytest.raises(ExplainUnsupportedError) as excinfo:
                await engine.execute(_QUERY, explain=True)
            engine.close()
        assert isinstance(excinfo.value, ValueError)
        assert all(client.called == [] for client in clients)


    async def test_a_drifted_explain_raises_schema_drift(self, tmp_path: Path, sqlite_ds: DatasourceConfig) -> None:
        recorders: list[Recorder] = []

        def factory(ds: DatasourceConfig) -> Recorder:
            recorders.append(Recorder(SlayerSQLClient(datasource=ds)))
            return recorders[-1]

        storage = await _storage(str(tmp_path / "store"), sqlite_ds)
        async with owned_query_engine(storage=storage, client_factory=factory) as engine:
            with transaction(sqlite_ds.database or "") as cur:
                cur.execute("DROP TABLE orders")
            with pytest.raises(SchemaDriftError) as excinfo:
                await engine.execute(_QUERY, explain=True)
        assert "orders" in excinfo.value.models
        assert isinstance(excinfo.value.__cause__, SQLAlchemyError)
        assert len(recorders[0].explained) == 1
