"""A throwaway database per test on the ``pytest-postgresql`` server, and a query engine owned over one."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import AsyncGenerator, Generator, Sequence
from typing import Any, LiteralString, cast

import psycopg
from psycopg import sql as pg_sql

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.database import engine_factory
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import owned_query_engine


@contextlib.contextmanager
def pg_database(info: Any, *, statements: Sequence[str], name: str = "pg") -> Generator[DatasourceConfig]:
    """A fresh database loaded with ``statements``, as a postgres datasource; dropped afterwards."""
    db_name = f"{name}_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(host=info.host, port=info.port, user=info.user, dbname="postgres", autocommit=True) as admin:
        admin.execute(pg_sql.SQL("CREATE DATABASE {}").format(pg_sql.Identifier(db_name)))
    with psycopg.connect(host=info.host, port=info.port, user=info.user, dbname=db_name) as conn:
        for statement in statements:
            conn.execute(cast(LiteralString, statement))
        conn.commit()
    ds = DatasourceConfig(
        name=name, type="postgres", host=info.host, port=info.port,
        database=db_name, username=info.user, password="",
    )
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)
        with psycopg.connect(host=info.host, port=info.port, user=info.user, dbname="postgres", autocommit=True) as admin:
            admin.execute(pg_sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(pg_sql.Identifier(db_name)))


def client_datasource(info: Any, *, name: str = "pg") -> DatasourceConfig:
    """A pytest-postgresql client fixture's database (its connection's ``info``) as a postgres datasource."""
    return DatasourceConfig(
        name=name, type="postgres", host=info.host, port=info.port,
        database=info.dbname, username=info.user, password="",
    )


@contextlib.asynccontextmanager
async def pg_query_engine(
    ds: DatasourceConfig, *, base_dir: str, models: Sequence[SlayerModel] = (), validate: bool = True,
    **engine_kwargs: Any,
) -> AsyncGenerator[SlayerQueryEngine]:
    """``owned_query_engine`` over a fresh ``YAMLStorage`` holding ``ds`` and ``models``."""
    storage = YAMLStorage(base_dir=base_dir)
    async with owned_query_engine(storage=storage, datasources=[ds], **engine_kwargs) as engine:
        for model in models:
            await storage.save_model(model, _validate=validate)
        yield engine
