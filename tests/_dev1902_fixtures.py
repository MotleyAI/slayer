"""Renamed-key join graph: ``orders → customers → regions``, every key column's
logical ``name`` differing from its physical ``sql`` spelling.

Physical tables::

    regions   (region_pk, name)
    customers (customer_pk, name, region_fk, tier, credit)
    orders    (order_pk, cust_fk, amount, status)
"""

from __future__ import annotations

from typing import AsyncIterator, List

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import seeded_exec_engine

_REGIONS_ROWS = [(1, "North"), (2, "South")]
# (customer_pk, name, region_fk, tier, credit)
_CUSTOMERS_ROWS = [
    (1, "Ann", 1, "gold", 100.0),
    (2, "Bob", 1, "silver", 200.0),
    (3, "Cid", 2, "gold", 300.0),
]
# (order_pk, cust_fk, amount, status)
_ORDERS_ROWS = [
    (1, 1, 10.0, "ok"),
    (2, 1, 20.0, "new"),
    (3, 2, 30.0, "ok"),
    (4, 3, 40.0, "ok"),
    (5, 3, 50.0, "new"),
]

#: sum(orders.amount) by customers.name.
AMOUNT_BY_CUSTOMER = {"Ann": 30.0, "Bob": 30.0, "Cid": 90.0}
#: sum(orders.amount) by customers.tier.
AMOUNT_BY_TIER = {"gold": 120.0, "silver": 30.0}
#: sum(orders.amount) by customers.regions.name.
AMOUNT_BY_REGION = {"North": 60.0, "South": 90.0}
#: associate: sum(customers.credit) per orders.status over distinct customers.
ASSOC_CREDIT_BY_STATUS = {"ok": 600.0, "new": 400.0}
#: sum(credit) over customers with a 'new' order (Ann, Cid).
CREDIT_WITH_NEW_ORDER = 400.0


def regions_model() -> SlayerModel:
    return SlayerModel(
        name="regions", data_source="test", sql_table="regions",
        columns=[
            Column(name="id", sql="region_pk", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )


def customers_model(*, rename_pk: bool = True, pk_sql: str = "customer_pk") -> SlayerModel:
    """``rename_pk=False`` spells the PK by its physical name (``customer_pk``)."""
    pk = (Column(name="id", sql=pk_sql, type=DataType.INT, primary_key=True)
          if rename_pk else
          Column(name="customer_pk", type=DataType.INT, primary_key=True))
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            pk,
            Column(name="name", type=DataType.TEXT),
            Column(name="region_id", sql="region_fk", type=DataType.INT),
            Column(name="tier", type=DataType.TEXT),
            Column(name="credit", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


def orders_model(*, rename_fk: bool = True, target_key: str = "id") -> SlayerModel:
    """``rename_fk=False`` spells the FK by its physical name (``cust_fk``)."""
    fk = (Column(name="customer_id", sql="cust_fk", type=DataType.INT)
          if rename_fk else Column(name="cust_fk", type=DataType.INT))
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", sql="order_pk", type=DataType.INT, primary_key=True),
            fk,
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
        ],
        joins=[ModelJoin(target_model="customers",
                         join_pairs=[[fk.name, target_key]])],
    )


def renamed_graph(
    *, rename_fk: bool = True, rename_pk: bool = True, pk_sql: str = "customer_pk",
) -> List[SlayerModel]:
    """``orders`` first (the default query root)."""
    return [
        orders_model(rename_fk=rename_fk,
                     target_key="id" if rename_pk else "customer_pk"),
        customers_model(rename_pk=rename_pk, pk_sql=pk_sql),
        regions_model(),
    ]


def _seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as cur:
        cur.execute("CREATE TABLE regions (region_pk INTEGER PRIMARY KEY, name TEXT)")
        cur.executemany("INSERT INTO regions VALUES (?,?)", _REGIONS_ROWS)
        cur.execute(
            "CREATE TABLE customers (customer_pk INTEGER PRIMARY KEY, name TEXT, "
            "region_fk INTEGER, tier TEXT, credit REAL)")
        cur.executemany("INSERT INTO customers VALUES (?,?,?,?,?)", _CUSTOMERS_ROWS)
        cur.execute(
            "CREATE TABLE orders (order_pk INTEGER PRIMARY KEY, cust_fk INTEGER, "
            "amount REAL, status TEXT)")
        cur.executemany("INSERT INTO orders VALUES (?,?,?,?)", _ORDERS_ROWS)


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    try:
        con.execute("CREATE TABLE regions (region_pk INTEGER, name VARCHAR)")
        con.executemany("INSERT INTO regions VALUES (?,?)", _REGIONS_ROWS)
        con.execute(
            "CREATE TABLE customers (customer_pk INTEGER, name VARCHAR, "
            "region_fk INTEGER, tier VARCHAR, credit DOUBLE)")
        con.executemany("INSERT INTO customers VALUES (?,?,?,?,?)", _CUSTOMERS_ROWS)
        con.execute(
            "CREATE TABLE orders (order_pk INTEGER, cust_fk INTEGER, "
            "amount DOUBLE, status VARCHAR)")
        con.executemany("INSERT INTO orders VALUES (?,?,?,?)", _ORDERS_ROWS)
    finally:
        con.close()


async def make_exec_engine(
    request, *, models: List[SlayerModel],
) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(dialect=dialect, seed=seed, models=models) as (engine, _db):
        yield engine


def cells(resp, *, dim_suffix: str, measure: str) -> dict:
    """``{dimension value: float(measure)}``; both columns found by suffix."""
    row = resp.data[0]
    dim = next(k for k in row if k.endswith(dim_suffix))
    val = next(k for k in row if k == measure or k.endswith(f".{measure}"))
    return {r[dim]: float(r[val]) for r in resp.data}


def warnings_of(resp, kind: str) -> list:
    return [w for w in (resp.warnings or []) if getattr(w, "kind", None) == kind]
