"""Shared fixtures for DEV-1853 — bidirectional join traversal.

Chain graph (datasource ``test``): regions ← customers ← orders, with the
edge between orders and customers declared on either side per builder flag.

Chain dataset (hand-computed; every executed expectation derives from here)
---------------------------------------------------------------------------
regions:   1 North 100 | 2 South 200
customers (id, region_id, name, tier, spend, signup_at):
   1  1  Alice gold   100  2024-01-05
   2  1  Bob   silver 150  2024-02-10
   3  2  Cara  gold    60  2024-03-15    (no orders — LEFT/INNER discriminator)
orders (id, customer_id, status, amount, ordered_at):
   1  1     ok   10  2024-01-10
   2  1     new  20  2024-01-20
   3  2     ok   30  2024-02-10
   4  NULL  new  40  2024-03-01          (orphan order)

Parallel graph: orders carries billing/shipping FKs onto customers
(regions/customers rows as above).
orders (id, billing_customer_id, shipping_customer_id, status, amount, ordered_at):
   1  1  2  ok   10  2024-01-10
   2  1  1  new  20  2024-01-20
   3  2  3  ok   30  2024-02-10
"""

from __future__ import annotations

import os
import sqlite3
from typing import Literal

from slayer.core.enums import DataType, JoinCardinality, JoinType, invert_cardinality
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine

from tests._engine_helpers import make_seeded_sqlite_engine


# --------------------------------------------------------------------------- #
# Chain graph builders.
# --------------------------------------------------------------------------- #
def regions_model() -> SlayerModel:
    return SlayerModel(
        name="regions", data_source="test", sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="pop", type=DataType.DOUBLE),
        ],
    )


def chain_models(
    *,
    join_type: JoinType = JoinType.LEFT,
    cardinality: JoinCardinality | None = JoinCardinality.MANY_TO_ONE,
    declare: Literal["forward", "reverse"] = "forward",
    customers_id_pk: bool = True,
) -> list[SlayerModel]:
    """regions/customers/orders with ONE orders↔customers edge.

    ``cardinality`` is read in the orders→customers orientation regardless of
    which side declares; ``declare="reverse"`` stores the edge on customers
    with swapped pairs and inverted cardinality.
    """
    customer_joins = [
        ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]]),
    ]
    order_joins = []
    if declare == "forward":
        order_joins.append(ModelJoin(
            target_model="customers", join_pairs=[["customer_id", "id"]],
            join_type=join_type, cardinality=cardinality,
        ))
    else:
        customer_joins.append(ModelJoin(
            target_model="orders", join_pairs=[["id", "customer_id"]],
            join_type=join_type, cardinality=invert_cardinality(cardinality),
        ))
    customers = SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=customers_id_pk),
            Column(name="region_id", type=DataType.INT),
            Column(name="name", type=DataType.TEXT),
            Column(name="tier", type=DataType.TEXT),
            Column(name="spend", type=DataType.DOUBLE),
            Column(name="signup_at", type=DataType.TIMESTAMP),
        ],
        joins=customer_joins,
    )
    orders = SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
        ],
        joins=order_joins,
    )
    return [orders, customers, regions_model()]


_REGIONS_ROWS = [(1, "North", 100.0), (2, "South", 200.0)]
_CHAIN_CUSTOMERS_ROWS = [
    (1, 1, "Alice", "gold", 100.0, "2024-01-05"),
    (2, 1, "Bob", "silver", 150.0, "2024-02-10"),
    (3, 2, "Cara", "gold", 60.0, "2024-03-15"),
]
_CHAIN_ORDERS_ROWS = [
    (1, 1, "ok", 10.0, "2024-01-10"),
    (2, 1, "new", 20.0, "2024-01-20"),
    (3, 2, "ok", 30.0, "2024-02-10"),
    (4, None, "new", 40.0, "2024-03-01"),
]


def seed_chain(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT, pop REAL)")
    cur.executemany("INSERT INTO regions VALUES (?,?,?)", _REGIONS_ROWS)
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
        "name TEXT, tier TEXT, spend REAL, signup_at TEXT)"
    )
    cur.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?)",
                    _CHAIN_CUSTOMERS_ROWS)
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
        "status TEXT, amount REAL, ordered_at TEXT)"
    )
    cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", _CHAIN_ORDERS_ROWS)
    con.commit()
    con.close()


# --------------------------------------------------------------------------- #
# Parallel-edge graph builders.
# --------------------------------------------------------------------------- #
def parallel_models(*, named: bool) -> list[SlayerModel]:
    """orders with billing + shipping edges onto customers (+ regions chain)."""
    order_columns = [
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="billing_customer_id", type=DataType.INT),
        Column(name="shipping_customer_id", type=DataType.INT),
        Column(name="status", type=DataType.TEXT),
        Column(name="amount", type=DataType.DOUBLE),
        Column(name="ordered_at", type=DataType.TIMESTAMP),
    ]
    if named:
        # Mode-A probe over a named edge (planned only when referenced).
        order_columns.append(
            Column(name="bill_tier", type=DataType.TEXT,
                   sql="billing_customer.tier"))
    orders = SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=order_columns,
        joins=[
            ModelJoin(
                target_model="customers",
                join_pairs=[["billing_customer_id", "id"]],
                cardinality=JoinCardinality.MANY_TO_ONE,
                name="billing_customer" if named else None,
            ),
            ModelJoin(
                target_model="customers",
                join_pairs=[["shipping_customer_id", "id"]],
                cardinality=JoinCardinality.MANY_TO_ONE,
                name="shipping_customer" if named else None,
            ),
        ],
    )
    customers = SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="name", type=DataType.TEXT),
            Column(name="tier", type=DataType.TEXT),
            # Labels feed response metadata — the D5 terminal-model probe.
            Column(name="spend", type=DataType.DOUBLE, label="Customer spend"),
            Column(name="signup_at", type=DataType.TIMESTAMP,
                   label="Signup date"),
        ],
        joins=[ModelJoin(target_model="regions",
                         join_pairs=[["region_id", "id"]])],
    )
    return [orders, customers, regions_model()]


_PARALLEL_ORDERS_ROWS = [
    (1, 1, 2, "ok", 10.0, "2024-01-10"),
    (2, 1, 1, "new", 20.0, "2024-01-20"),
    (3, 2, 3, "ok", 30.0, "2024-02-10"),
]


def seed_parallel(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT, pop REAL)")
    cur.executemany("INSERT INTO regions VALUES (?,?,?)", _REGIONS_ROWS)
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
        "name TEXT, tier TEXT, spend REAL, signup_at TEXT)"
    )
    cur.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?)",
                    _CHAIN_CUSTOMERS_ROWS)
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, "
        "billing_customer_id INTEGER, shipping_customer_id INTEGER, "
        "status TEXT, amount REAL, ordered_at TEXT)"
    )
    cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?)",
                    _PARALLEL_ORDERS_ROWS)
    con.commit()
    con.close()


# --------------------------------------------------------------------------- #
# Engines.
# --------------------------------------------------------------------------- #
async def chain_engine(tmpdir: str, **model_kwargs) -> SlayerQueryEngine:
    db_path = os.path.join(tmpdir, "data.db")
    seed_chain(db_path)
    return await make_seeded_sqlite_engine(
        base_dir=os.path.join(tmpdir, "store"), db_path=db_path,
        models=chain_models(**model_kwargs),
    )


async def parallel_engine(tmpdir: str, *, named: bool) -> SlayerQueryEngine:
    db_path = os.path.join(tmpdir, "data.db")
    seed_parallel(db_path)
    return await make_seeded_sqlite_engine(
        base_dir=os.path.join(tmpdir, "store"), db_path=db_path,
        models=parallel_models(named=named),
    )


def rows_set(resp, *keys) -> set[tuple]:
    """Rows as a set of value tuples for the given result-column keys."""
    return {tuple(r[k] for k in keys) for r in resp.data}


# --------------------------------------------------------------------------- #
# Oracles (see module docstring).
# --------------------------------------------------------------------------- #
CHAIN_REVERSE_DIMS_LEFT = {
    ("Alice", "ok"), ("Alice", "new"), ("Bob", "ok"), ("Cara", None),
}
CHAIN_REVERSE_DIMS_INNER = {("Alice", "ok"), ("Alice", "new"), ("Bob", "ok")}
CHAIN_AMOUNT_BY_NAME = {"Alice": 30.0, "Bob": 30.0, "Cara": None}
CHAIN_AMOUNT_BY_NAME_WITH_ORPHAN = {
    "Alice": 30.0, "Bob": 30.0, None: 40.0,
}
# Total-grain aggregate covers all orders, orphan included (mirror parity).
CHAIN_REVERSE_AGG_TOTAL = 100.0
# Local measure grouped by a reverse fan-out dim. Default broadcasts the
# distinct-customer total (Alice 100 + Bob 150 + Cara 60); associate dedups
# each customer once per status it has (overlapping, so not additive).
CHAIN_SPEND_BY_REVERSE_STATUS = {"ok": 250.0, "new": 100.0, None: 60.0}
CHAIN_SPEND_BROADCAST_TOTAL = 310.0
CHAIN_AMOUNT_BY_TIER_EXACT = {"gold": 30.0, "silver": 30.0}
CHAIN_UNPROVEN_BROADCAST_TOTAL = 100.0  # amount total, repeated per group
CHAIN_POP_BROADCAST_TOTAL = 300.0  # pop total, repeated per group
CHAIN_PUSHDOWN_OK_SPEND = 250.0  # Alice 100 + Bob 150; Cara has no ok order
CHAIN_MULTIHOP = {("North", "ok"), ("North", "new"), ("South", None)}

PARALLEL_BILLING_NAMES = {("Alice",), ("Bob",)}
PARALLEL_SHIPPING_NAMES = {("Alice",), ("Bob",), ("Cara",)}
PARALLEL_BILLING_AMOUNTS = {"Alice": 30.0, "Bob": 30.0, "Cara": None}
PARALLEL_BILLING_MONTH_AMOUNTS = {"2024-01": 30.0, "2024-02": 30.0}
PARALLEL_BILL_TIERS = {("gold",), ("silver",)}
# Target-rooted producer covers the whole customers population per group.
PARALLEL_REROOT_BY_TIER = {"gold": 160.0, "silver": 150.0}
PARALLEL_LAST_SPEND_BY_SIGNUP = 60.0  # Cara signed up last
PARALLEL_PUSHDOWN_BILLING_OK_POP = 100.0  # North once; South (Cara) excluded


__all__ = [
    "regions_model", "chain_models", "parallel_models",
    "seed_chain", "seed_parallel", "chain_engine", "parallel_engine",
    "rows_set",
    "CHAIN_REVERSE_DIMS_LEFT", "CHAIN_REVERSE_DIMS_INNER",
    "CHAIN_AMOUNT_BY_NAME", "CHAIN_AMOUNT_BY_NAME_WITH_ORPHAN",
    "CHAIN_REVERSE_AGG_TOTAL", "CHAIN_SPEND_BY_REVERSE_STATUS",
    "CHAIN_SPEND_BROADCAST_TOTAL",
    "CHAIN_AMOUNT_BY_TIER_EXACT", "CHAIN_UNPROVEN_BROADCAST_TOTAL",
    "CHAIN_POP_BROADCAST_TOTAL",
    "CHAIN_PUSHDOWN_OK_SPEND", "CHAIN_MULTIHOP",
    "PARALLEL_BILLING_NAMES", "PARALLEL_SHIPPING_NAMES",
    "PARALLEL_BILLING_AMOUNTS", "PARALLEL_BILLING_MONTH_AMOUNTS",
    "PARALLEL_BILL_TIERS", "PARALLEL_REROOT_BY_TIER",
    "PARALLEL_LAST_SPEND_BY_SIGNUP", "PARALLEL_PUSHDOWN_BILLING_OK_POP",
]
