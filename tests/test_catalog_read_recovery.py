"""A failed catalog read never poisons the next read on the same connection (Postgres aborts the transaction)."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, NoReturn

import pytest
import sqlalchemy as sa
import sqlalchemy.exc
from pytest_postgresql import factories

from slayer.sql.dialects.base import SqlDialect
from tests._engine_helpers import disposable_engine
from tests._pg_helpers import pg_database

postgresql_proc = factories.postgresql_proc(port=None)


class _PoisoningInspector:
    """Each read fails a statement on ``conn`` first, aborting its transaction, then raises."""

    default_schema_name: str | None = None

    def __init__(self, conn: sa.Connection) -> None:
        self._conn = conn

    def _fail(self, *_args: Any, **_kwargs: Any) -> NoReturn:
        try:
            self._conn.exec_driver_sql("SELECT * FROM no_such_table")
        except sqlalchemy.exc.DBAPIError:
            pass
        raise RuntimeError("inspector read failed")

    get_columns = get_pk_constraint = get_foreign_keys = get_unique_constraints = get_indexes = _fail


@pytest.fixture
def conn(postgresql_proc: Any) -> Iterator[sa.Connection]:
    statements = ["CREATE TABLE orders (id INTEGER PRIMARY KEY, amount DOUBLE PRECISION)"]
    with (
        pg_database(postgresql_proc, statements=statements, name="recovery") as ds,
        disposable_engine(ds.get_connection_string()) as engine,
        engine.connect() as connection,
    ):
        yield connection


def test_columns_fall_back_after_a_failed_inspector_read(conn: sa.Connection) -> None:
    cols = SqlDialect().reflect_columns(
        connection=conn, inspector=_PoisoningInspector(conn), table="orders", schema="public", catalog=None,
    )
    assert [c.name for c in cols] == ["id", "amount"]


def test_primary_key_falls_back_after_a_failed_inspector_read(conn: sa.Connection) -> None:
    keys = SqlDialect().reflect_table_keys(
        connection=conn, inspector=_PoisoningInspector(conn), table="orders", schema="public", catalog=None,
    )
    assert keys.primary_key == ["id"]


def test_the_connection_serves_reads_after_a_failed_one(conn: sa.Connection) -> None:
    SqlDialect().reflect_table_keys(
        connection=conn, inspector=_PoisoningInspector(conn), table="orders", schema="public", catalog=None,
    )
    assert conn.exec_driver_sql("SELECT count(*) FROM orders").scalar() == 0
