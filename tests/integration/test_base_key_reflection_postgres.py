"""Base ``reflect_table_keys`` keeps today's key reading on a real Postgres, hook-level and through ingestion."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
import sqlalchemy as sa
from pytest_postgresql import factories

from slayer.core.enums import JoinCardinality
from slayer.core.models import DatasourceConfig
from slayer.engine.ingestion import ingest_datasource_report
from slayer.sql.dialects import PostgresDialect, ReflectedForeignKey, TableKeys
from tests._engine_helpers import disposable_engine

pytestmark = pytest.mark.integration

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

_DDL = (
    "CREATE SCHEMA shop",
    "CREATE TABLE shop.customers (id INTEGER PRIMARY KEY, email TEXT UNIQUE, a INTEGER, b INTEGER, "
    "code TEXT, flag INTEGER, UNIQUE (a, b))",
    "CREATE UNIQUE INDEX ux_code ON shop.customers (code)",
    "CREATE UNIQUE INDEX ux_partial ON shop.customers (flag) WHERE flag > 0",
    "CREATE UNIQUE INDEX ux_expr ON shop.customers (lower(code))",
    "CREATE TABLE shop.orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES shop.customers(id), "
    "a INTEGER, b INTEGER, FOREIGN KEY (a, b) REFERENCES shop.customers(a, b))",
    "CREATE TABLE shop.lines (order_id INTEGER, n INTEGER, PRIMARY KEY (order_id, n))",
)


@pytest.fixture
def pg_ds(postgresql) -> DatasourceConfig:
    cur = postgresql.cursor()
    for stmt in _DDL:
        cur.execute(stmt)
    postgresql.commit()
    info = postgresql.info
    return DatasourceConfig(
        name="pg", type="postgres", host=info.host, port=info.port,
        database=info.dbname, username=info.user, password="",
    )


@pytest.fixture
def pg_engine(postgresql, pg_ds) -> Iterator[sa.Engine]:
    info = postgresql.info
    with disposable_engine(
        f"postgresql+psycopg2://{info.user}@{info.host}:{info.port}/{info.dbname}"
    ) as engine:
        yield engine


def _keys(engine: sa.Engine, table: str) -> TableKeys:
    return PostgresDialect().reflect_table_keys(
        sa_engine=engine, inspector=sa.inspect(engine), table=table, schema="shop", catalog=None,
    )


def test_pk_unique_and_whole_table_unique_indexes(pg_engine) -> None:
    keys = _keys(pg_engine, "customers")
    assert keys.primary_key == ["id"]
    assert {tuple(s) for s in keys.unique_key_sets} == {("email",), ("a", "b"), ("code",)}


def test_composite_pk(pg_engine) -> None:
    assert _keys(pg_engine, "lines").primary_key == ["order_id", "n"]


def test_foreign_keys(pg_engine) -> None:
    fks = sorted(_keys(pg_engine, "orders").foreign_keys, key=lambda fk: fk.columns)
    assert fks == [
        ReflectedForeignKey(
            columns=["a", "b"], referred_schema="shop",
            referred_table="customers", referred_columns=["a", "b"],
        ),
        ReflectedForeignKey(
            columns=["customer_id"], referred_schema="shop",
            referred_table="customers", referred_columns=["id"],
        ),
    ]


def test_ingestion_flags_and_joins(pg_ds) -> None:
    report = ingest_datasource_report(datasource=pg_ds, schemas=["shop"])
    models = {m.name: m for m in report.models}
    customers, orders = models["customers"], models["orders"]
    assert [c.name for c in customers.columns if c.primary_key] == ["id"]
    assert {c.name for c in customers.columns if c.unique} == {"email", "code"}
    assert {tuple(map(tuple, j.join_pairs)): j.cardinality for j in orders.joins} == {
        (("customer_id", "id"),): JoinCardinality.MANY_TO_ONE,
        (("a", "a"), ("b", "b")): JoinCardinality.MANY_TO_ONE,
    }
