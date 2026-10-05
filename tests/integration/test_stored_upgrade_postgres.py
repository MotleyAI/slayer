"""0.10.x stored-upgrade repairs executed on a real Postgres."""

import pytest
import sqlglot
from pytest_postgresql import factories
from sqlglot import exp

from slayer.core.models import DatasourceConfig
from slayer.sql import engine_factory
from tests._stored_upgrade_fixtures import (
    CUSTOMERS_ROWS,
    ORDERS_ROWS,
    AMOUNT_BY_REGION,
    DS,
    by_dimension,
    customers_v10,
    measure_total,
    orders_v10,
    query_backed_v10,
    raw_store,
    rev_query,
    run,
    year_td,
)

pytestmark = pytest.mark.integration

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

_DDL = (
    "CREATE TABLE customers (id INTEGER, name TEXT, region TEXT, signed_up_at TIMESTAMP)",
    "CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, status TEXT, "
    "ordered_at TIMESTAMP, amount DOUBLE PRECISION)",
)
REGION_QUERY = {"source_model": "orders", "dimensions": ["customers.region"], "measures": ["sum(amount)"]}


@pytest.fixture
def shop_ds(postgresql):
    cur = postgresql.cursor()
    for stmt in _DDL:
        cur.execute(stmt)
    cur.executemany("INSERT INTO customers VALUES (%s, %s, %s, %s)", CUSTOMERS_ROWS)
    cur.executemany("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", ORDERS_ROWS)
    postgresql.commit()
    info = postgresql.info
    ds = DatasourceConfig(name=DS, type="postgres", host=info.host, port=info.port,
                          database=info.dbname, username=info.user, password="")
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


@pytest.mark.parametrize("side", ["source", "target"])
async def test_undeclared_join_key_joins_without_casts(tmp_path, shop_ds, side):
    models = ([orders_v10(declare_fk=False), customers_v10()] if side == "source"
              else [orders_v10(), customers_v10(declare_pk=False)])
    storage = await raw_store(backend="yaml", base=str(tmp_path), datasource=shop_ds, models=models)
    resp = await run(storage, REGION_QUERY)
    assert by_dimension(resp, dim_suffix="region") == AMOUNT_BY_REGION
    dry = await run(storage, REGION_QUERY, dry_run=True)
    assert dry.sql is not None
    [join] = list(sqlglot.parse_one(dry.sql, read="postgres").find_all(exp.Join))
    on = join.args["on"]
    assert not list(on.find_all(exp.Cast, exp.TryCast)), on.sql(dialect="postgres")


@pytest.mark.parametrize(("date_range", "expected"), [
    (["2024-01-01T00:00:00Z", "2024-12-31T23:59:59Z"], 150.0),
    (["2024-01-01T02:00:00+02:00", "2024-12-31T23:59:59+02:00"], 100.0),
])
async def test_offset_date_range_returns_the_0_10_2_rows(tmp_path, shop_ds, date_range, expected):
    qb = query_backed_v10(name="rev_range", stages=[rev_query(time_dimensions=[year_td(date_range)])])
    storage = await raw_store(backend="yaml", base=str(tmp_path), datasource=shop_ds,
                              models=[orders_v10(), customers_v10(), qb])
    resp = await run(storage, {"source_model": "rev_range", "measures": ["sum(rev)"]})
    assert measure_total(resp, measure="rev_sum") == expected
