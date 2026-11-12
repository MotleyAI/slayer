"""A small shop for query-scoped extensions, edge merge and query-declared INNER joins.

Physical tables::

    countries  (id, name)
    regions    (id, name, country_id, manager_id)
    customers  (id, name, tier, credit, region_id, home_address_id)
    vip_list   (customer_id, note, active)
    addresses  (id, city, customer_id, region_id)
    orders     (id, customer_id, status, amount, billing_address_id, shipping_address_id, referrer_id, ship_region_id)
    vip_orders (order_id)
    events     (id, user_id)
    users      (id, name)
    parts      (id, pa, pb)
    part_keys / part_keys_b (ka, kb)
    shipments  (id, address_id)

Stored edges: ``orders → customers`` (LEFT, many_to_one), ``orders → addresses`` as ``billing_address`` and
``shipping_address``, ``part_keys → parts`` on ``(ka, kb) = (pa, pb)``, and ``shipments → addresses`` twice on one
key pair (``origin``, ``dest``). Variants add more.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Iterable
from contextlib import asynccontextmanager
from typing import Any

import pytest

from slayer.core.enums import DataType, JoinCardinality, JoinType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import seeded_exec_engine

DS = "test"
DIALECTS = ["sqlite", "duckdb"]

_TABLES: dict[str, tuple[str, list[tuple]]] = {
    "countries": ("id INTEGER, name TEXT", [(1, "Norland"), (2, "Southia")]),
    "regions": ("id INTEGER, name TEXT, country_id INTEGER, manager_id INTEGER", [
        (1, "North", 1, 2), (2, "South", 2, 1), (3, "East", None, None),
    ]),
    "customers": ("id INTEGER, name TEXT, tier TEXT, credit DOUBLE, region_id INTEGER, home_address_id INTEGER", [
        (1, "Ann", "gold", 100.0, 1, 11),
        (2, "Bob", "silver", 50.0, 2, 12),
        (3, "Cid", "gold", 30.0, None, None),
        (4, "Dee", "bronze", 20.0, 9, None),
        (5, "Eve", "silver", 10.0, 3, 13),
    ]),
    "vip_list": ("customer_id INTEGER, note TEXT, active INTEGER", [
        (1, "a", 1), (1, "b", 1), (2, "c", 0), (5, "d", 1),
    ]),
    "addresses": ("id INTEGER, city TEXT, customer_id INTEGER, region_id INTEGER", [
        (11, "Oslo", 1, 1), (12, "Rome", 2, 2), (13, "Kiev", 5, 3), (14, "Lima", 2, None),
    ]),
    "orders": (
        "id INTEGER, customer_id INTEGER, status TEXT, amount DOUBLE, billing_address_id INTEGER, "
        "shipping_address_id INTEGER, referrer_id INTEGER, ship_region_id INTEGER",
        [
            (1, 1, "ok", 10.0, 11, 12, 2, 1),
            (2, 1, "new", 20.0, 11, 11, None, 2),
            (3, 2, "ok", 30.0, 12, 14, 1, None),
            (4, 3, "ok", 5.0, 13, 13, None, 1),
            (5, 4, "new", 40.0, 14, 12, 3, 3),
            (6, 5, "ok", 15.0, 13, 11, None, 2),
            (7, None, "ok", 7.0, 11, None, None, None),
        ],
    ),
    "vip_orders": ("order_id INTEGER", [(1,), (3,), (3,)]),
    "events": ("id INTEGER, user_id INTEGER", [(1, 1), (2, 2)]),
    "users": ("id INTEGER, name TEXT", [(1, "u1")]),
    "parts": ("id INTEGER, pa INTEGER, pb INTEGER", [(1, 1, 1), (2, 1, None), (3, None, 2), (4, 2, 2)]),
    "part_keys": ("ka INTEGER, kb INTEGER", [(1, 1), (1, None), (None, 2)]),
    "part_keys_b": ("ka INTEGER, kb INTEGER", [(1, 1), (1, None), (None, 2)]),
    "shipments": ("id INTEGER, address_id INTEGER", [(1, 11), (2, 12)]),
}

#: Customers with a vip_list row; with the ``active = 1`` model filter, Bob drops.
VIP_CUSTOMERS = {"Ann", "Bob", "Eve"}
ACTIVE_VIP_CUSTOMERS = {"Ann", "Eve"}
ALL_CUSTOMERS = {"Ann", "Bob", "Cid", "Dee", "Eve"}
#: Orders whose customer has a matching region.
ORDERS_WITH_REGIONED_CUSTOMER = {1, 2, 3, 6}
#: Orders with a customer (the orphan order 7 has none).
ORDERS_WITH_CUSTOMER = {1, 2, 3, 4, 5, 6}
#: Orders whose own customer is on the vip list.
ORDERS_OF_VIP_CUSTOMERS = {1, 2, 3, 6}
#: sum(orders.amount) by customer tier.
AMOUNT_BY_TIER = {"gold": 35.0, "silver": 45.0, "bronze": 40.0, None: 7.0}
#: Customers whose orders total more than 25.
BIG_SPENDERS = {"Ann", "Bob", "Dee"}
TOTAL_AMOUNT = 127.0
TOTAL_CREDIT = 210.0


def _seed(db_path: str, *, dialect: str) -> None:
    if dialect == "duckdb":
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(db_path)
        try:
            for table, (ddl, rows) in _TABLES.items():
                con.execute(f"CREATE TABLE {table} ({ddl})")
                marks = ",".join("?" * len(rows[0]))
                con.executemany(f"INSERT INTO {table} VALUES ({marks})", rows)
        finally:
            con.close()
        return
    with transaction(db_path) as cur:
        for table, (ddl, rows) in _TABLES.items():
            cur.execute(f"CREATE TABLE {table} ({ddl})")
            marks = ",".join("?" * len(rows[0]))
            cur.executemany(f"INSERT INTO {table} VALUES ({marks})", rows)


def _cols(*spec: tuple[str, DataType], pk: str | None = "id") -> list[Column]:
    return [Column(name=n, type=t, primary_key=n == pk) for n, t in spec]


_INT, _TEXT, _DOUBLE = DataType.INT, DataType.TEXT, DataType.DOUBLE


def countries() -> SlayerModel:
    return SlayerModel(name="countries", data_source=DS, sql_table="countries",
                       columns=_cols(("id", _INT), ("name", _TEXT)))


def regions() -> SlayerModel:
    return SlayerModel(name="regions", data_source=DS, sql_table="regions", columns=_cols(
        ("id", _INT), ("name", _TEXT), ("country_id", _INT), ("manager_id", _INT)))


def customers(*joins: ModelJoin) -> SlayerModel:
    return SlayerModel(name="customers", data_source=DS, sql_table="customers", joins=list(joins), columns=_cols(
        ("id", _INT), ("name", _TEXT), ("tier", _TEXT), ("credit", _DOUBLE), ("region_id", _INT),
        ("home_address_id", _INT)))


def vip_list(*, filters: list[str] | None = None) -> SlayerModel:
    return SlayerModel(name="vip_list", data_source=DS, sql_table="vip_list", filters=filters or [],
                       columns=_cols(("customer_id", _INT), ("note", _TEXT), ("active", _INT), pk=None))


def addresses(*joins: ModelJoin) -> SlayerModel:
    return SlayerModel(name="addresses", data_source=DS, sql_table="addresses", joins=list(joins), columns=_cols(
        ("id", _INT), ("city", _TEXT), ("customer_id", _INT), ("region_id", _INT)))


CUSTOMER_PAIRS = [["customer_id", "id"]]
BILLING_PAIRS = [["billing_address_id", "id"]]
SHIPPING_PAIRS = [["shipping_address_id", "id"]]


def orders() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source=DS, sql_table="orders",
        columns=_cols(
            ("id", _INT), ("customer_id", _INT), ("status", _TEXT), ("amount", _DOUBLE),
            ("billing_address_id", _INT), ("shipping_address_id", _INT), ("referrer_id", _INT),
            ("ship_region_id", _INT)),
        joins=[
            ModelJoin(target_model="customers", join_pairs=CUSTOMER_PAIRS, join_type=JoinType.LEFT,
                      cardinality=JoinCardinality.MANY_TO_ONE),
            ModelJoin(target_model="addresses", name="billing_address", join_pairs=BILLING_PAIRS,
                      cardinality=JoinCardinality.MANY_TO_ONE),
            ModelJoin(target_model="addresses", name="shipping_address", join_pairs=SHIPPING_PAIRS,
                      cardinality=JoinCardinality.MANY_TO_ONE),
        ],
    )


def _plain(name: str, *spec: tuple[str, DataType], pk: str | None = None, joins: Iterable[ModelJoin] = ()) -> SlayerModel:
    return SlayerModel(name=name, data_source=DS, sql_table=name, columns=_cols(*spec, pk=pk), joins=list(joins))


PART_PAIRS = [["pa", "ka"], ["pb", "kb"]]


def base_models() -> list[SlayerModel]:
    return [
        countries(), regions(), customers(), vip_list(), addresses(), orders(),
        _plain("vip_orders", ("order_id", _INT)),
        _plain("events", ("id", _INT), ("user_id", _INT), pk="id"),
        _plain("users", ("id", _INT), ("name", _TEXT), pk="id"),
        _plain("parts", ("id", _INT), ("pa", _INT), ("pb", _INT), pk="id"),
        _plain("part_keys", ("ka", _INT), ("kb", _INT),
               joins=[ModelJoin(target_model="parts", join_pairs=[["ka", "pa"], ["kb", "pb"]])]),
        _plain("part_keys_b", ("ka", _INT), ("kb", _INT)),
        _plain("shipments", ("id", _INT), ("address_id", _INT), pk="id", joins=[
            ModelJoin(target_model="addresses", name="origin", join_pairs=[["address_id", "id"]]),
            ModelJoin(target_model="addresses", name="dest", join_pairs=[["address_id", "id"]]),
        ]),
    ]


def with_models(*replacements: SlayerModel) -> list[SlayerModel]:
    """``base_models()`` with same-named models replaced."""
    by_name = {m.name: m for m in replacements}
    return [by_name.pop(m.name, m) for m in base_models()] + list(by_name.values())


def respelled_model(models: list[SlayerModel], *, name: str, spelling: str) -> list[SlayerModel]:
    """``models`` with model ``name`` saved as ``spelling``, every stored join to it retargeted."""
    def one(m: SlayerModel) -> SlayerModel:
        joins = [j.model_copy(update={"target_model": spelling}) if j.target_model == name else j for j in m.joins]
        return m.model_copy(update={"name": spelling if m.name == name else m.name, "joins": joins})
    return [one(m) for m in models]


@asynccontextmanager
async def shop(dialect: str, models: list[SlayerModel] | None = None) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect=dialect, seed=lambda path: _seed(path, dialect=dialect),
        models=models if models is not None else base_models(),
    ) as (engine, _db):
        yield engine


def q(**fields: Any) -> SlayerQuery:
    return SlayerQuery.model_validate(fields)


def ext(source_name: str, **fields: Any) -> dict:
    return {"source_name": source_name, **fields}


def join(target: str, pairs: list[list[str]], **fields: Any) -> dict:
    return {"target_model": target, "join_pairs": pairs, **fields}


async def run(engine: SlayerQueryEngine, query: Any) -> SlayerResponse:
    return await engine.execute(query)


def values(resp: SlayerResponse, key: str) -> set:
    """The set of ``key``'s values over every row."""
    return {row[key] for row in resp.data}


def rows(resp: SlayerResponse, *keys: str) -> list[tuple]:
    """Rows projected onto ``keys``, sorted (``None`` first)."""
    return sorted((tuple(row[k] for k in keys) for row in resp.data), key=repr)


def by_key(resp: SlayerResponse, key: str, value: str) -> dict:
    return {row[key]: (None if row[value] is None else float(row[value])) for row in resp.data}
