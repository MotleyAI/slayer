"""Routing harness: a recording gateway client, a counting factory, and a guard that refuses direct access.

The gateway answers from a private backend datasource (same database, distinct name); the guard
lets only backend datasources reach the engine factory and the inspector, so any SLayer code
path that bypasses the engine's client is refused and recorded.
"""

from __future__ import annotations

import asyncio
import sqlite3
import sqlite3.dbapi2 as sqlite_dbapi2
import threading
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from typing import Any

import duckdb
import psycopg
import pytest
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_asyncio
from pydantic import BaseModel, ConfigDict, Field

import slayer.database.client as client_module
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.database import engine_factory
from slayer.database.client import SlayerSQLClient
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage

BACKEND_SUFFIX = "__gateway_backend"
DS_NAME = "shop"

# Every client operation of the contract (statements, metadata, lifecycle).
STATEMENT_OPS = ("execute", "explain", "get_result_column_types", "server_profile")
METADATA_OPS = (
    "ping", "list_schemas", "default_schema", "current_catalog", "list_objects",
    "reflect_columns", "reflect_table_keys", "table_comment", "schema_description",
)

SHOP_DDL = (
    "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, org TEXT NOT NULL)",
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers(id), "
    "amount REAL, quantity INTEGER, status TEXT, org TEXT)",
    "CREATE VIEW paid_orders AS SELECT id, amount FROM orders WHERE status = 'paid'",
    "INSERT INTO customers VALUES (1, 'Ann', 'acme'), (2, 'Bob', 'acme'), (3, 'Cid', 'other')",
    "INSERT INTO orders VALUES (1, 1, 10.5, 1, 'paid', 'acme'), (2, 1, 20.0, 2, 'new', 'acme'), "
    "(3, 2, 30.0, 3, 'paid', 'acme'), (4, 3, 40.0, 4, 'paid', 'other')",
)


def seed_shop(db_path: str) -> None:
    with transaction(db_path) as cur:
        for statement in SHOP_DDL:
            cur.execute(statement)


def shop_datasource(db_path: str, *, name: str = DS_NAME) -> DatasourceConfig:
    return DatasourceConfig(name=name, type="sqlite", database=db_path)


def backend_of(datasource: DatasourceConfig) -> DatasourceConfig:
    """The gateway's private datasource over the same database."""
    return datasource.model_copy(update={"name": f"{datasource.name}{BACKEND_SUFFIX}"})


def orders_model(*, data_source: str = DS_NAME) -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source=data_source,
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.INT),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="org", sql="org", type=DataType.TEXT),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


def customers_model(*, data_source: str = DS_NAME) -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source=data_source,
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="org", sql="org", type=DataType.TEXT),
        ],
    )


def paid_sql_model(*, data_source: str = DS_NAME) -> SlayerModel:
    return SlayerModel(
        name="paid", sql="SELECT id, amount FROM orders WHERE status = 'paid'", data_source=data_source,
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
        ],
    )


COUNT_ORDERS_MEM = {"source_model": "t", "measures": [{"formula": "*:count", "name": "n"}]}


def mem_count_model() -> SlayerModel:
    """Model ``t`` over table ``t`` of the in-memory datasource ``mem``."""
    return SlayerModel(
        name="t", sql_table="t", data_source="mem",
        columns=[Column(name="v", sql="v", type=DataType.INT)],
    )


async def shop_storage(
    base: str, *, datasource: DatasourceConfig, models: Sequence[SlayerModel] = (),
) -> YAMLStorage:
    storage = YAMLStorage(base_dir=base)
    await storage.save_datasource(datasource)
    for model in models:
        await storage.save_model(model, _validate=False)
    return storage


class ClientCall(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    op: str
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = Field(default_factory=dict)


class RecordingClient:
    """Implements the client contract by forwarding to ``inner``; extends no SLayer class."""

    def __init__(self, *, inner: Any, datasource: DatasourceConfig, delay: float = 0.0) -> None:
        self.inner = inner
        self.datasource = datasource
        self.calls: list[ClientCall] = []
        self.acloses = 0
        self.closes = 0
        self._delay = delay

    async def _forward(self, op: str, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(ClientCall(op=op, args=args, kwargs=kwargs))
        if self._delay:
            await asyncio.sleep(self._delay)
        return await getattr(self.inner, op)(*args, **kwargs)

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        return await self._forward("execute", *args, **kwargs)

    async def explain(self, *args: Any, **kwargs: Any) -> Any:
        return await self._forward("explain", *args, **kwargs)

    async def get_result_column_types(self, *args: Any, **kwargs: Any) -> Any:
        return await self._forward("get_result_column_types", *args, **kwargs)

    async def server_profile(self, *args: Any, **kwargs: Any) -> Any:
        return await self._forward("server_profile", *args, **kwargs)

    async def ping(self, *args: Any, **kwargs: Any) -> Any:
        return await self._forward("ping", *args, **kwargs)

    async def list_schemas(self, *args: Any, **kwargs: Any) -> Any:
        return await self._forward("list_schemas", *args, **kwargs)

    async def default_schema(self, *args: Any, **kwargs: Any) -> Any:
        return await self._forward("default_schema", *args, **kwargs)

    async def current_catalog(self, *args: Any, **kwargs: Any) -> Any:
        return await self._forward("current_catalog", *args, **kwargs)

    async def list_objects(self, *args: Any, **kwargs: Any) -> Any:
        return await self._forward("list_objects", *args, **kwargs)

    async def reflect_columns(self, *args: Any, **kwargs: Any) -> Any:
        return await self._forward("reflect_columns", *args, **kwargs)

    async def reflect_table_keys(self, *args: Any, **kwargs: Any) -> Any:
        return await self._forward("reflect_table_keys", *args, **kwargs)

    async def table_comment(self, *args: Any, **kwargs: Any) -> Any:
        return await self._forward("table_comment", *args, **kwargs)

    async def schema_description(self, *args: Any, **kwargs: Any) -> Any:
        return await self._forward("schema_description", *args, **kwargs)

    async def aclose(self) -> None:
        self.acloses += 1
        await self.inner.aclose()

    def close(self) -> None:
        self.closes += 1
        self.inner.close()

    def ops(self) -> list[str]:
        return [c.op for c in self.calls]

    def statements(self) -> list[str]:
        """SQL text of every ``execute`` / ``explain`` / ``get_result_column_types`` call, in order."""
        out = []
        for call in self.calls:
            if call.op == "explain":
                explained = call.args[0] if call.args else call.kwargs["statements"]
                out.extend(s for s in (explained.session_on, explained.statement, explained.session_off) if s)
            elif call.op in ("execute", "get_result_column_types"):
                out.append(call.args[0] if call.args else call.kwargs["sql"])
        return out


class GatewayFactory:
    """``client_factory`` building a ``RecordingClient`` over a default client of the backend datasource."""

    def __init__(self, *, delay: float = 0.0) -> None:
        self.built: list[DatasourceConfig] = []
        self.clients: list[RecordingClient] = []
        self._delay = delay

    def __call__(self, datasource: DatasourceConfig) -> RecordingClient:
        self.built.append(datasource)
        client = RecordingClient(
            inner=SlayerSQLClient(datasource=backend_of(datasource)), datasource=datasource, delay=self._delay,
        )
        self.clients.append(client)
        return client

    def all_calls(self) -> list[ClientCall]:
        return [call for client in self.clients for call in client.calls]

    def all_statements(self) -> list[str]:
        return [sql for client in self.clients for sql in client.statements()]


class DirectAccessAttempted(Exception):
    pass


class DirectAccessGuard(BaseModel):
    """Every refused direct-access attempt, by the entry point it hit."""

    attempts: list[str] = Field(default_factory=list)


def _refusing(guard: DirectAccessGuard, name: str) -> Callable[..., Any]:
    def _refuse(*_args: Any, **_kwargs: Any) -> Any:
        guard.attempts.append(name)
        raise DirectAccessAttempted(name)

    return _refuse


_DRIVER_CONNECTS = (
    (sqlite3, "sqlite3.connect"), (sqlite_dbapi2, "sqlite3.connect"),
    (duckdb, "duckdb.connect"), (psycopg, "psycopg.connect"),
)


@contextmanager
def no_direct_access(*, backends: Sequence[DatasourceConfig] = ()) -> Generator[DirectAccessGuard]:
    """Refuse engine creation, inspection, engine building and driver connects, except for ``backends``."""
    guard = DirectAccessGuard()
    allowed_names = {ds.name for ds in backends}
    allowed_engines = [engine_factory.get_engine(ds) for ds in backends]  # warmed before the guard
    real_get_engine = engine_factory.get_engine
    real_inspect = sa.inspect
    # Driver connects pass only while an allowed engine's pool opens a connection.
    opening = threading.local()

    def _get_engine(datasource: DatasourceConfig, *args: Any, **kwargs: Any) -> sa.Engine:
        if datasource.name in allowed_names:
            return real_get_engine(datasource, *args, **kwargs)
        guard.attempts.append(f"get_engine({datasource.name})")
        raise DirectAccessAttempted(f"get_engine({datasource.name})")

    def _inspect(subject: Any, *args: Any, **kwargs: Any) -> Any:
        target = getattr(subject, "engine", subject)
        if any(target is engine for engine in allowed_engines):
            return real_inspect(subject, *args, **kwargs)
        guard.attempts.append("sa.inspect")
        raise DirectAccessAttempted("sa.inspect")

    def _allowed_dialect_connect(real: Callable[..., Any]) -> Callable[..., Any]:
        def _connect(*args: Any, **kwargs: Any) -> Any:
            opening.active = True
            try:
                return real(*args, **kwargs)
            finally:
                opening.active = False

        return _connect

    def _driver_connect(real: Callable[..., Any], name: str) -> Callable[..., Any]:
        def _connect(*args: Any, **kwargs: Any) -> Any:
            if getattr(opening, "active", False):
                return real(*args, **kwargs)
            guard.attempts.append(name)
            raise DirectAccessAttempted(name)

        return _connect

    try:
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(engine_factory, "get_engine", _get_engine)
            mp.setattr(engine_factory, "build_in_memory_sqlite_engine", _refusing(guard, "in-memory engine"))
            mp.setattr(sa, "inspect", _inspect)
            mp.setattr(sa, "create_engine", _refusing(guard, "create_engine"))
            mp.setattr(sa_asyncio, "create_async_engine", _refusing(guard, "create_async_engine"))
            mp.setattr(client_module, "create_async_engine", _refusing(guard, "create_async_engine"))
            for engine in allowed_engines:
                mp.setattr(engine.dialect, "connect", _allowed_dialect_connect(engine.dialect.connect))
            for module, name in _DRIVER_CONNECTS:
                mp.setattr(module, "connect", _driver_connect(module.connect, name))
            yield guard
    finally:
        for ds in backends:
            engine_factory.invalidate_engine(ds)
