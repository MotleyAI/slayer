"""Tier-1 integration coverage of a reverse-hop query.

Customers-rooted query over the only stored edge orders → customers, on
SQLite, DuckDB, and Postgres.
"""

from __future__ import annotations

import importlib.util
import tempfile

import pytest

from slayer.core.models import DatasourceConfig
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1853_fixtures import (
    _CHAIN_CUSTOMERS_ROWS,
    _CHAIN_ORDERS_ROWS,
    _REGIONS_ROWS,
    CHAIN_REVERSE_DIMS_LEFT,
    chain_engine,
    chain_models,
    rows_set,
)
from tests._pg_helpers import client_datasource, pg_query_engine

_HAS_PG = importlib.util.find_spec("pytest_postgresql") is not None
if _HAS_PG:
    from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, guarded

    postgresql_proc = factories.postgresql_proc(port=None)
    postgresql = factories.postgresql("postgresql_proc")

_REVERSE_QUERY = SlayerQuery(
    source_model="customers", dimensions=["name", "orders.status"])
_KEYS = ("customers.name", "customers.orders.status")


async def test_reverse_hop_sqlite() -> None:
    with tempfile.TemporaryDirectory() as d:
        engine = await chain_engine(d)
        resp = await engine.execute(_REVERSE_QUERY)
        assert rows_set(resp, *_KEYS) == CHAIN_REVERSE_DIMS_LEFT


async def test_reverse_hop_duckdb() -> None:
    duckdb = pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as d:
        db_path = f"{d}/data.duckdb"
        con = duckdb.connect(db_path)
        con.execute("CREATE TABLE regions (id INTEGER, name VARCHAR, pop DOUBLE)")
        con.executemany("INSERT INTO regions VALUES (?,?,?)", _REGIONS_ROWS)
        con.execute(
            "CREATE TABLE customers (id INTEGER, region_id INTEGER, "
            "name VARCHAR, tier VARCHAR, spend DOUBLE, signup_at TIMESTAMP)")
        con.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?)",
                        _CHAIN_CUSTOMERS_ROWS)
        con.execute(
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, "
            "status VARCHAR, amount DOUBLE, ordered_at TIMESTAMP)")
        con.executemany("INSERT INTO orders VALUES (?,?,?,?,?)",
                        _CHAIN_ORDERS_ROWS)
        con.close()
        storage = YAMLStorage(base_dir=f"{d}/store")
        await storage.save_datasource(DatasourceConfig(
            name="test", type="duckdb", database=db_path))
        for model in chain_models():
            await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)
        resp = await engine.execute(_REVERSE_QUERY)
        assert rows_set(resp, *_KEYS) == CHAIN_REVERSE_DIMS_LEFT


@pytest.mark.integration
@pytest.mark.skipif(not _HAS_PG, reason="pytest_postgresql not installed")
async def test_reverse_hop_postgres(postgresql, tmp_path) -> None:
    cur = postgresql.cursor()
    cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT, "
                "pop DOUBLE PRECISION)")
    cur.executemany("INSERT INTO regions VALUES (%s,%s,%s)", _REGIONS_ROWS)
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
        "name TEXT, tier TEXT, spend DOUBLE PRECISION, signup_at TIMESTAMP)")
    cur.executemany("INSERT INTO customers VALUES (%s,%s,%s,%s,%s,%s)",
                    _CHAIN_CUSTOMERS_ROWS)
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
        "status TEXT, amount DOUBLE PRECISION, ordered_at TIMESTAMP)")
    cur.executemany("INSERT INTO orders VALUES (%s,%s,%s,%s,%s)",
                    _CHAIN_ORDERS_ROWS)
    postgresql.commit()
    async with pg_query_engine(client_datasource(postgresql.info, name="test"), base_dir=str(tmp_path / "store"),
                               models=chain_models()) as engine:
        resp = await engine.execute(_REVERSE_QUERY)
    assert rows_set(resp, *_KEYS) == CHAIN_REVERSE_DIMS_LEFT
