"""Shared fixtures for DEV-1945 — the first/last ranking key is a closure-judged input.

Underscore-prefixed so pytest skips collection. ``orders → line_items`` (``id =
order_id``) and ``line_items → shipments`` (``id = line_item_id``) are undeclared
hops whose only proof is the reverse orientation (unproven forward);
``orders → customers`` is proven (``customers.id`` is a primary key).

orders (id, customer_id, amount, created_at):  1 c1 10.0 2024-01-01 | 2 c2 30.0 2024-01-02
customers (id, signup_at):                     1 2023-06-01 | 2 2023-07-01
line_items (id, order_id, qty, created_at):    1 o1 1.0 2024-01-05 | 2 o1 2.0 2024-01-03 |
                                               3 o2 3.0 2024-01-04
shipments (id, line_item_id, shipped_at):      1 li1 2024-01-06 | 2 li1 2024-01-07 | 3 li3 2024-01-08

Ranking ``amount:last`` by ``created_at`` or by ``cust_signup`` picks order 2 (30.0);
ranking over the fanned ``li_ts`` join would pick order 1 (its line item dated
2024-01-05 is newest) — the defect the refusals guard against.
"""

from __future__ import annotations

import os
from typing import List, Optional

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.storage.sqlite_conn import transaction

from tests._engine_helpers import make_seeded_sqlite_engine

# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
UNPARSEABLE_SQL = ")((( bad"


def orders_model(*, default: Optional[str] = None) -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        default_time_dimension=default,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
            # Derived across the unproven hop.
            Column(name="li_ts", type=DataType.TIMESTAMP, sql="line_items.created_at"),
            # Local value, filter across the unproven hop.
            Column(
                name="li_flag_ts", type=DataType.TIMESTAMP, sql="created_at",
                filter="line_items.qty > 0",
            ),
            Column(name="bad_ts", type=DataType.TIMESTAMP, sql=UNPARSEABLE_SQL),
            # Derived across the proven hop.
            Column(name="cust_signup", type=DataType.TIMESTAMP, sql="customers.signup_at"),
        ],
        joins=[
            ModelJoin(target_model="line_items", join_pairs=[["id", "order_id"]]),
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
    )


def line_items_model(*, default: Optional[str] = None) -> SlayerModel:
    return SlayerModel(
        name="line_items", sql_table="line_items", data_source="test",
        default_time_dimension=default,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="order_id", type=DataType.INT),
            Column(name="qty", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="sh_ts", type=DataType.TIMESTAMP, sql="shipments.shipped_at"),
        ],
        joins=[
            ModelJoin(target_model="shipments", join_pairs=[["id", "line_item_id"]]),
        ],
    )


def shipments_model() -> SlayerModel:
    return SlayerModel(
        name="shipments", sql_table="shipments", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="line_item_id", type=DataType.INT),
            Column(name="shipped_at", type=DataType.TIMESTAMP),
        ],
    )


def customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="signup_at", type=DataType.TIMESTAMP),
        ],
    )


def dev1945_models(
    *, orders_default: Optional[str] = None, line_items_default: Optional[str] = None,
) -> List[SlayerModel]:
    """Host first; each model's ``default_time_dimension`` is set per case."""
    return [
        orders_model(default=orders_default),
        line_items_model(default=line_items_default),
        shipments_model(),
        customers_model(),
    ]


def dev1945_bundle(models: List[SlayerModel]) -> ResolvedSourceBundle:
    return ResolvedSourceBundle(dialect="postgres", source_model=models[0], referenced_models=models)


# --------------------------------------------------------------------------- #
# Seed + engine
# --------------------------------------------------------------------------- #
_ORDERS_ROWS = [(1, 1, 10.0, "2024-01-01"), (2, 2, 30.0, "2024-01-02")]
_CUSTOMERS_ROWS = [(1, "2023-06-01"), (2, "2023-07-01")]
_LINE_ITEMS_ROWS = [
    (1, 1, 1.0, "2024-01-05"), (2, 1, 2.0, "2024-01-03"), (3, 2, 3.0, "2024-01-04"),
]
_SHIPMENTS_ROWS = [(1, 1, "2024-01-06"), (2, 1, "2024-01-07"), (3, 3, "2024-01-08")]


def seed_dev1945_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        con.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
            "amount REAL, created_at TEXT)"
        )
        con.executemany("INSERT INTO orders VALUES (?,?,?,?)", _ORDERS_ROWS)
        con.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, signup_at TEXT)")
        con.executemany("INSERT INTO customers VALUES (?,?)", _CUSTOMERS_ROWS)
        con.execute(
            "CREATE TABLE line_items (id INTEGER PRIMARY KEY, order_id INTEGER, "
            "qty REAL, created_at TEXT)"
        )
        con.executemany("INSERT INTO line_items VALUES (?,?,?,?)", _LINE_ITEMS_ROWS)
        con.execute(
            "CREATE TABLE shipments (id INTEGER PRIMARY KEY, line_item_id INTEGER, "
            "shipped_at TEXT)"
        )
        con.executemany("INSERT INTO shipments VALUES (?,?,?)", _SHIPMENTS_ROWS)


async def make_engine(
    base_dir: str, *, orders_default: Optional[str] = None,
    line_items_default: Optional[str] = None,
) -> SlayerQueryEngine:
    """Seeded SQLite engine over the graph with the given model defaults."""
    db_path = os.path.join(base_dir, "dev1945.db")
    seed_dev1945_sqlite(db_path)
    return await make_seeded_sqlite_engine(
        base_dir=base_dir, db_path=db_path,
        models=dev1945_models(
            orders_default=orders_default, line_items_default=line_items_default,
        ),
    )


# --------------------------------------------------------------------------- #
# Query shorthands + oracles
# --------------------------------------------------------------------------- #
def orders_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def month_td(column: str = "created_at") -> List[TimeDimension]:
    return [TimeDimension(
        dimension=ColumnRef(name=column), granularity=TimeGranularity.MONTH,
    )]


def measure(formula: str, name: str = "l") -> dict:
    return {"formula": formula, "name": name}


#: ``amount:last`` by ``created_at`` and by ``cust_signup`` both pick order 2.
LAST_AMOUNT = 30.0
#: ``amount:first`` by ``created_at`` picks order 1.
FIRST_AMOUNT = 10.0
#: The only month bucket; its trailing 30d window holds both orders.
JAN = "2024-01"


def month_key(value) -> str:
    """Stable per-month key across SQLite text and DuckDB timestamp values."""
    return str(value)[:7]


__all__ = [
    "orders_model", "line_items_model", "shipments_model",
    "customers_model", "dev1945_models", "dev1945_bundle", "seed_dev1945_sqlite",
    "make_engine", "orders_q", "month_td", "measure", "month_key",
    "LAST_AMOUNT", "FIRST_AMOUNT", "JAN",
]
