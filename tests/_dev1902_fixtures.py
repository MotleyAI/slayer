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
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
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


def regions_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="regions", data_source="test", sql_table="regions",
        fields=[
            DatasetField(name="id", sql="region_pk", datatype=DataType.INT, primary_key=True),
            DatasetField(name="name", datatype=DataType.TEXT),
        ],
    )


def customers_dataset(*, rename_pk: bool = True, pk_sql: str = "customer_pk") -> SlayerDataset:
    """``rename_pk=False`` spells the PK by its physical name (``customer_pk``)."""
    pk = (DatasetField(name="id", sql=pk_sql, datatype=DataType.INT, primary_key=True)
          if rename_pk else
          DatasetField(name="customer_pk", datatype=DataType.INT, primary_key=True))
    return SlayerDataset(
        name="customers", data_source="test", sql_table="customers",
        fields=[
            pk,
            DatasetField(name="name", datatype=DataType.TEXT),
            DatasetField(name="region_id", sql="region_fk", datatype=DataType.INT),
            DatasetField(name="tier", datatype=DataType.TEXT),
            DatasetField(name="credit", datatype=DataType.DOUBLE),
        ],
        relationships=[DatasetRelationship(to="regions", join_pairs=[["region_id", "id"]])],
    )


def orders_dataset(*, rename_fk: bool = True, target_key: str = "id") -> SlayerDataset:
    """``rename_fk=False`` spells the FK by its physical name (``cust_fk``)."""
    fk = (DatasetField(name="customer_id", sql="cust_fk", datatype=DataType.INT)
          if rename_fk else DatasetField(name="cust_fk", datatype=DataType.INT))
    return SlayerDataset(
        name="orders", data_source="test", sql_table="orders",
        fields=[
            DatasetField(name="id", sql="order_pk", datatype=DataType.INT, primary_key=True),
            fk,
            DatasetField(name="amount", datatype=DataType.DOUBLE),
            DatasetField(name="status", datatype=DataType.TEXT),
        ],
        relationships=[DatasetRelationship(to="customers",
                         join_pairs=[[fk.name, target_key]])],
    )


def renamed_graph(
    *, rename_fk: bool = True, rename_pk: bool = True, pk_sql: str = "customer_pk",
) -> List[SlayerDataset]:
    """``orders`` first (the default query root)."""
    return [
        orders_dataset(rename_fk=rename_fk,
                     target_key="id" if rename_pk else "customer_pk"),
        customers_dataset(rename_pk=rename_pk, pk_sql=pk_sql),
        regions_dataset(),
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
    request, *, datasets: List[SlayerDataset],
) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(dialect=dialect, seed=seed, datasets=datasets) as (engine, _db):
        yield engine


def cells(resp, *, dim_suffix: str, metric: str) -> dict:
    """``{dimension value: float(measure)}``; both columns found by suffix."""
    row = resp.data[0]
    dim = next(k for k in row if k.endswith(dim_suffix))
    val = next(k for k in row if k == metric or k.endswith(f".{metric}"))
    return {r[dim]: float(r[val]) for r in resp.data}


def warnings_of(resp, kind: str) -> list:
    return [w for w in (resp.warnings or []) if getattr(w, "kind", None) == kind]
