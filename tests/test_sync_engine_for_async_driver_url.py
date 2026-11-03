"""An async-only driver URL still gets a synchronous engine (spec: sql/execution)."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from pytest_postgresql import factories

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig
from slayer.sql import engine_factory
from slayer.sql.client import ResultColumnType, SlayerSQLClient
from slayer.sql.dialects.reflection import TableLocation
from tests._pg_helpers import pg_database

postgresql_proc = factories.postgresql_proc(port=None)


@pytest.mark.parametrize(("ds_type", "url", "sync_url"), [
    ("postgres", "postgresql+asyncpg://u:p@h:5432/d?sslmode=require", "postgresql+psycopg2://u:p@h:5432/d?sslmode=require"),
    ("postgresql", "postgresql+asyncpg://u@h/d", "postgresql+psycopg2://u@h/d"),
    ("mysql", "mysql+aiomysql://u@h/d", "mysql+pymysql://u@h/d"),
    ("mariadb", "mariadb+aiomysql://u@h/d", "mariadb+pymysql://u@h/d"),
])
def test_sync_engine_uses_the_default_sync_driver(ds_type: str, url: str, sync_url: str) -> None:
    ds = DatasourceConfig(name="async_url", type=ds_type, connection_string=url)
    try:
        engine = engine_factory.get_engine(ds)
        assert engine.url.render_as_string(hide_password=False) == sync_url
    finally:
        engine_factory.invalidate_engine(ds)


@pytest.fixture
def asyncpg_ds(postgresql_proc: Any) -> Iterator[DatasourceConfig]:
    with pg_database(postgresql_proc, statements=[
        "CREATE TABLE t (id INTEGER PRIMARY KEY, amount NUMERIC(12,0))",
        "INSERT INTO t VALUES (1, 5)",
    ], name="asyncpg_url") as plain:
        ds = DatasourceConfig(
            name="asyncpg_url", type="postgres",
            connection_string=f"postgresql+asyncpg://{plain.username}@{plain.host}:{plain.port}/{plain.database}",
        )
        try:
            yield ds
        finally:
            engine_factory.invalidate_engine(ds)


async def test_metadata_and_probes_work_over_an_asyncpg_url(asyncpg_ds: DatasourceConfig) -> None:
    client = SlayerSQLClient(datasource=asyncpg_ds)
    try:
        await client.ping()
        columns = await client.reflect_columns(TableLocation(name="t"))
        types = await client.get_result_column_types("SELECT id, amount FROM t")
        rows = (await client.execute("SELECT amount FROM t")).rows
        async_engine = client._get_async_engine()
        assert async_engine is not None and async_engine.url.drivername == "postgresql+asyncpg"
    finally:
        await client.aclose()
        client.close()
    assert [c.name for c in columns] == ["id", "amount"]
    assert types == {
        "id": ResultColumnType(type=DataType.INT, db_type="integer"),
        "amount": ResultColumnType(type=DataType.INT, db_type="numeric(12,0)"),
    }
    assert rows == [{"amount": 5}]
