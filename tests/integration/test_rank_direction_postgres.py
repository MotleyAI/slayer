"""Rank-family NULL inputs executed on a real Postgres."""

import pytest
from pytest_postgresql import factories

import tests.test_rank_direction as base
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1847_fixtures import (
    _CORDERS_ROWS,
    _CUSTOMERS_ROWS,
    _REGIONS_ROWS,
    _SALES_ROWS_WIDE,
    dev1847_models,
)

pytestmark = pytest.mark.integration

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

_DDL = (
    "CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, city TEXT, product TEXT, "
    "amount DOUBLE PRECISION, quantity DOUBLE PRECISION, unit_price DOUBLE PRECISION)",
    "CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT)",
    "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER)",
    "CREATE TABLE corders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount DOUBLE PRECISION)",
)
_INSERTS = (
    ("INSERT INTO sales VALUES (%s, %s, %s, %s, %s, %s, %s)", _SALES_ROWS_WIDE),
    ("INSERT INTO regions VALUES (%s, %s)", _REGIONS_ROWS),
    ("INSERT INTO customers VALUES (%s, %s)", _CUSTOMERS_ROWS),
    ("INSERT INTO corders VALUES (%s, %s, %s)", _CORDERS_ROWS),
)


class TestPostgresNullInputs(base.TestNullInputs):
    @pytest.fixture
    async def engine(self, postgresql, tmp_path):
        cur = postgresql.cursor()
        for stmt in _DDL:
            cur.execute(stmt)
        for stmt, rows in _INSERTS:
            cur.executemany(stmt, rows)
        postgresql.commit()
        info = postgresql.info
        ds = DatasourceConfig(name="test", type="postgres", host=info.host, port=info.port,
                              database=info.dbname, username=info.user, password="")
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_datasource(ds)
        for model in dev1847_models():
            await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)
        try:
            yield engine
        finally:
            await engine.aclose()
            engine.close()
            engine_factory.invalidate_engine(ds)
