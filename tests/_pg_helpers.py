"""A throwaway database per test on the ``pytest-postgresql`` server."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Generator, Sequence
from typing import Any, LiteralString, cast

import psycopg
from psycopg import sql as pg_sql

from slayer.core.models import DatasourceConfig
from slayer.sql import engine_factory


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
