"""Each Postgres driver a URL names serves every client operation (spec: database/execution, database/client-contract)."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
import sqlalchemy as sa
from pytest_postgresql import factories
from sqlalchemy.engine.url import make_url

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig
from slayer.database import client as client_module
from slayer.database import engine_factory
from slayer.database.client import ResultColumnType, SlayerSQLClient
from slayer.sql.dialects.base import ServerProfile
from slayer.sql.dialects.reflection import ReflectedForeignKey, TableKeys, TableLocation
from tests._pg_helpers import pg_database

pytestmark = pytest.mark.filterwarnings("ignore::FutureWarning")

postgresql_proc = factories.postgresql_proc(port=None)

_SCHEMA = [
    "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)",
    "COMMENT ON TABLE customers IS 'People who buy'",
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers(id), amount NUMERIC(12,3))",
    "INSERT INTO customers VALUES (1, 'ada')",
    "INSERT INTO orders VALUES (1, 1, 2.5)",
]

# URL scheme and query → the driver every connection must use.
_URLS = {
    "bare": ("postgresql", "sslmode=disable&connect_timeout=5", "psycopg"),
    "psycopg": ("postgresql+psycopg", "sslmode=disable", "psycopg"),
    "psycopg2": ("postgresql+psycopg2", "sslmode=disable", "psycopg2"),
    "asyncpg": ("postgresql+asyncpg", "ssl=disable", "asyncpg"),
    "pg8000": ("postgresql+pg8000", "", "pg8000"),
}


_SYNC_ONLY = frozenset({"psycopg2", "pg8000"})


class _Built:
    """Every engine the client builds or takes from the factory."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.async_urls: list[sa.URL] = []
        self.factory_urls: list[sa.URL] = []
        real_async = client_module.create_async_engine
        real_factory = engine_factory.get_engine

        def _async(url: Any, **kwargs: Any) -> Any:
            engine = real_async(url, **kwargs)
            self.async_urls.append(engine.url)
            return engine

        def _factory(datasource: DatasourceConfig) -> sa.Engine:
            engine = real_factory(datasource)
            self.factory_urls.append(engine.url)
            return engine

        monkeypatch.setattr(client_module, "create_async_engine", _async)
        monkeypatch.setattr(engine_factory, "get_engine", _factory)


@pytest.fixture(params=sorted(_URLS))
def named(request: pytest.FixtureRequest, postgresql_proc: Any) -> Iterator[tuple[DatasourceConfig, str]]:
    scheme, query, driver = _URLS[request.param]
    with pg_database(postgresql_proc, statements=_SCHEMA, name=f"pg_{request.param}") as plain:
        ds = DatasourceConfig(
            name=plain.name, type="postgres",
            connection_string=f"{scheme}://{plain.username}@{plain.host}:{plain.port}/{plain.database}"
            + (f"?{query}" if query else ""),
        )
        try:
            yield ds, driver
        finally:
            engine_factory.invalidate_engine(ds)


async def _run_every_operation(client: SlayerSQLClient) -> dict[str, Any]:
    await client.ping()
    return {
        "tables": (await client.list_objects(None)).tables,
        "columns": [c.name for c in await client.reflect_columns(TableLocation(name="orders"))],
        "keys": await client.reflect_table_keys(TableLocation(name="orders")),
        "comment": await client.table_comment(TableLocation(name="customers")),
        "profile": await client.server_profile(),
        "types": await client.get_result_column_types("SELECT id, name FROM customers"),
        "rows": (await client.execute("SELECT name FROM customers")).rows,
    }


async def test_every_operation_succeeds(named: tuple[DatasourceConfig, str]) -> None:
    ds, _ = named
    client = SlayerSQLClient(ds)
    try:
        seen = await _run_every_operation(client)
    finally:
        await client.aclose()
        client.close()
    assert {"customers", "orders"} <= set(seen["tables"])
    assert seen["columns"] == ["id", "customer_id", "amount"]
    assert seen["keys"] == TableKeys(
        primary_key=["id"],
        foreign_keys=[ReflectedForeignKey(columns=["customer_id"], referred_table="customers", referred_columns=["id"])],
    )
    assert seen["comment"] == "People who buy"
    assert seen["profile"].version is not None
    assert seen["profile"].model_copy(update={"version": None}) == ServerProfile()
    assert seen["types"] == {
        "id": ResultColumnType(type=DataType.INT, db_type="integer"),
        "name": ResultColumnType(type=DataType.TEXT, db_type="text"),
    }
    assert seen["rows"] == [{"name": "ada"}]


async def test_every_connection_uses_the_named_driver_and_query(
    monkeypatch: pytest.MonkeyPatch, named: tuple[DatasourceConfig, str],
) -> None:
    ds, driver = named
    built = _Built(monkeypatch)
    client = SlayerSQLClient(ds)
    try:
        await _run_every_operation(client)
    finally:
        await client.aclose()
        client.close()
    urls = built.async_urls + built.factory_urls
    assert urls
    assert {url.drivername for url in urls} == {f"postgresql+{driver}"}
    assert all(url.query == make_url(ds.connection_string or "").query for url in urls)
    if driver in _SYNC_ONLY:
        assert built.async_urls == []
    else:
        assert built.factory_urls == []


_NUMERIC_PROBE = "SELECT CAST(5 AS NUMERIC(12,0)) AS n0, CAST(2.5 AS NUMERIC(12,3)) AS n3 FROM orders"


@pytest.mark.parametrize("named", ["asyncpg", "bare", "psycopg", "psycopg2"], indirect=True)
async def test_exact_numerics_are_typed_by_what_the_driver_reports(named: tuple[DatasourceConfig, str]) -> None:
    ds, driver = named
    client = SlayerSQLClient(ds)
    try:
        types = await client.get_result_column_types(_NUMERIC_PROBE)
    finally:
        await client.aclose()
        client.close()
    if driver == "asyncpg":
        assert types == {
            "n0": ResultColumnType(type=DataType.DOUBLE, db_type="numeric"),
            "n3": ResultColumnType(type=DataType.DOUBLE, db_type="numeric"),
        }
    else:
        assert types == {
            "n0": ResultColumnType(type=DataType.INT, db_type="numeric(12,0)"),
            "n3": ResultColumnType(type=DataType.DOUBLE, db_type="numeric(12,3)"),
        }
