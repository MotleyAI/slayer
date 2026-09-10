"""Shared fixtures for DEV-1866 — optional query population, dimension-determined default.

Underscore-prefixed so pytest skips collection here. Provides several small
topologies, each isolated in its own datasource so a rootless query's anchor
names scope to exactly one datasource:

* ``ds_chain``  — orders → customers → regions, all edges declared many-to-one.
  ``region`` is a plain column on ``customers`` (the canonical "coarse side").
  Unique-minimal determinations; the only topology seeded with real rows.
* ``ds_pair``   — ``prof`` ↔ ``acct`` one-to-one; two viable candidates ⇒ tie.
* ``ds_fan``    — ``basket`` with two child tables (``basket_items``,
  ``basket_pays``); sibling dims have no to-one determiner ⇒ no viable candidate.
* ``ds_ambig``  — ``shipment`` with two unnamed edges to ``depot`` ⇒ ambiguous hop.
* ``ds_widgets_a`` / ``ds_widgets_b`` — a ``widgets`` model in each, for the
  datasource-scoping (ambiguous / zero-candidate) cases.

Chain dataset (hand-computed; every executed expectation derives from here)
--------------------------------------------------------------------------
regions: 1 North | 2 South | 3 West
customers (id, region, region_id, tier, name):
   1 North 1 gold   Alice
   2 North 1 silver Bob
   3 South 2 gold   Cara
   4 West  3 bronze Dan     (no orders — NULL-attach discriminator)
orders (id, customer_id, status, amount):
   1 1 ok     10
   2 1 cancel  5
   3 2 ok     20
   4 3 ok     30

amount:sum by customers.region:  North=35 (10+5+20)  South=30  West=NULL
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

DS_CHAIN = "ds_chain"
DS_PAIR = "ds_pair"
DS_FAN = "ds_fan"
DS_AMBIG = "ds_ambig"
DS_UNKNOWN = "ds_unknown"
DS_KEYCOV = "ds_keycov"
DS_WIDGETS_A = "ds_widgets_a"
DS_WIDGETS_B = "ds_widgets_b"
DS_NAMED = "ds_named"


# --------------------------------------------------------------------------- #
# Model builders.
# --------------------------------------------------------------------------- #
def chain_models() -> list[SlayerModel]:
    """orders → customers → regions; region is a column on customers."""
    regions = SlayerModel(
        name="regions", data_source=DS_CHAIN, sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )
    customers = SlayerModel(
        name="customers", data_source=DS_CHAIN, sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region", type=DataType.TEXT),
            Column(name="region_id", type=DataType.INT),
            Column(name="tier", type=DataType.TEXT),
            Column(name="name", type=DataType.TEXT),
        ],
        joins=[
            ModelJoin(
                target_model="regions", join_pairs=[["region_id", "id"]],
                cardinality=JoinCardinality.MANY_TO_ONE,
            ),
        ],
    )
    orders = SlayerModel(
        name="orders", data_source=DS_CHAIN, sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="ordered_at", type=DataType.TEXT),
        ],
        measures=[
            ModelMeasure(formula="amount:sum", name="revenue"),
            ModelMeasure(formula="amount:avg", name="avg_amt"),
        ],
        joins=[
            ModelJoin(
                target_model="customers", join_pairs=[["customer_id", "id"]],
                cardinality=JoinCardinality.MANY_TO_ONE,
            ),
        ],
    )
    return [orders, customers, regions]


def chain_models_by_name() -> dict[str, SlayerModel]:
    return {m.name: m for m in chain_models()}


def pair_models() -> list[SlayerModel]:
    """prof ↔ acct one-to-one — both sides to-one, so two viable candidates tie."""
    acct = SlayerModel(
        name="acct", data_source=DS_PAIR, sql_table="acct",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="email", type=DataType.TEXT),
        ],
    )
    prof = SlayerModel(
        name="prof", data_source=DS_PAIR, sql_table="prof",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="acct_id", type=DataType.INT, unique=True),
            Column(name="bio", type=DataType.TEXT),
        ],
        joins=[
            ModelJoin(
                target_model="acct", join_pairs=[["acct_id", "id"]],
                cardinality=JoinCardinality.ONE_TO_ONE,
            ),
        ],
    )
    return [acct, prof]


def fan_models() -> list[SlayerModel]:
    """basket with two children — sibling dims have no to-one determiner."""
    basket = SlayerModel(
        name="basket", data_source=DS_FAN, sql_table="basket",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
    )
    items = SlayerModel(
        name="basket_items", data_source=DS_FAN, sql_table="basket_items",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="basket_id", type=DataType.INT),
            Column(name="sku", type=DataType.TEXT),
        ],
        joins=[
            ModelJoin(
                target_model="basket", join_pairs=[["basket_id", "id"]],
                cardinality=JoinCardinality.MANY_TO_ONE,
            ),
        ],
    )
    pays = SlayerModel(
        name="basket_pays", data_source=DS_FAN, sql_table="basket_pays",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="basket_id", type=DataType.INT),
            Column(name="method", type=DataType.TEXT),
        ],
        joins=[
            ModelJoin(
                target_model="basket", join_pairs=[["basket_id", "id"]],
                cardinality=JoinCardinality.MANY_TO_ONE,
            ),
        ],
    )
    return [basket, items, pays]


def ambig_models() -> list[SlayerModel]:
    """shipment with two unnamed edges to depot — any depot hop is ambiguous."""
    depot = SlayerModel(
        name="depot", data_source=DS_AMBIG, sql_table="depot",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )
    shipment = SlayerModel(
        name="shipment", data_source=DS_AMBIG, sql_table="shipment",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="origin_depot_id", type=DataType.INT),
            Column(name="dest_depot_id", type=DataType.INT),
            Column(name="weight", type=DataType.DOUBLE),
        ],
        joins=[
            ModelJoin(
                target_model="depot", join_pairs=[["origin_depot_id", "id"]],
                cardinality=JoinCardinality.MANY_TO_ONE,
            ),
            ModelJoin(
                target_model="depot", join_pairs=[["dest_depot_id", "id"]],
                cardinality=JoinCardinality.MANY_TO_ONE,
            ),
        ],
    )
    return [depot, shipment]


def unknown_models() -> list[SlayerModel]:
    """hits → sess over an undeclared-cardinality hop into a keyless target.

    ``sess`` has no primary key or unique column, so the hop is neither declared
    to-one nor unique-key covered — unknown cardinality, which does not count as
    determination in either direction.
    """
    sess = SlayerModel(
        name="sess", data_source=DS_UNKNOWN, sql_table="sess",
        columns=[
            Column(name="sess_key", type=DataType.INT),
            Column(name="token", type=DataType.TEXT),
        ],
    )
    hits = SlayerModel(
        name="hits", data_source=DS_UNKNOWN, sql_table="hits",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="sess_ref", type=DataType.INT),
            Column(name="page", type=DataType.TEXT),
        ],
        joins=[
            ModelJoin(
                target_model="sess", join_pairs=[["sess_ref", "sess_key"]],
                cardinality=None,
            ),
        ],
    )
    return [sess, hits]


def keycov_models() -> list[SlayerModel]:
    """events → days over an UNDECLARED-cardinality hop into a unique key.

    ``events.day_id → days.id`` declares no cardinality, but ``days.id`` is the
    primary key, so the hop is provably to-one by unique-key coverage — it counts
    as determination even though cardinality is unset.
    """
    days = SlayerModel(
        name="days", data_source=DS_KEYCOV, sql_table="days",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="label", type=DataType.TEXT),
        ],
    )
    events = SlayerModel(
        name="events", data_source=DS_KEYCOV, sql_table="events",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="day_id", type=DataType.INT),
            Column(name="kind", type=DataType.TEXT),
        ],
        joins=[
            ModelJoin(
                target_model="days", join_pairs=[["day_id", "id"]],
                cardinality=None,
            ),
        ],
    )
    return [days, events]


def named_models() -> list[SlayerModel]:
    """tickets → agents over a *named* join ("reporter"); agents owns a saved measure,
    and column ``score`` deliberately shares a name with a custom aggregation."""
    agents = SlayerModel(
        name="agents", data_source=DS_NAMED, sql_table="agents",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="score", type=DataType.INT),
        ],
        measures=[ModelMeasure(formula="id:count", name="handled")],
        aggregations=[Aggregation(name="score", formula="MAX({score})")],
    )
    tickets = SlayerModel(
        name="tickets", data_source=DS_NAMED, sql_table="tickets",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="subject", type=DataType.TEXT),
            Column(name="agent_id", type=DataType.INT),
        ],
        joins=[
            ModelJoin(
                name="reporter", target_model="agents",
                join_pairs=[["agent_id", "id"]],
                cardinality=JoinCardinality.MANY_TO_ONE,
            ),
        ],
    )
    return [agents, tickets]


def named_models_by_name() -> dict[str, SlayerModel]:
    return {m.name: m for m in named_models()}


def widgets_model(data_source: str) -> SlayerModel:
    return SlayerModel(
        name="widgets", data_source=data_source, sql_table="widgets",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="x", type=DataType.TEXT),
        ],
    )


# --------------------------------------------------------------------------- #
# Storage for metadata-only inference tests (no rows executed).
# --------------------------------------------------------------------------- #
_ALL_TOPOLOGIES: list[tuple[str, list[SlayerModel]]] = [
    (DS_CHAIN, chain_models()),
    (DS_PAIR, pair_models()),
    (DS_FAN, fan_models()),
    (DS_AMBIG, ambig_models()),
    (DS_UNKNOWN, unknown_models()),
    (DS_KEYCOV, keycov_models()),
    (DS_WIDGETS_A, [widgets_model(DS_WIDGETS_A)]),
    (DS_WIDGETS_B, [widgets_model(DS_WIDGETS_B)]),
    (DS_NAMED, named_models()),
]


async def make_inference_storage() -> YAMLStorage:
    """A YAMLStorage with every topology's models saved (no DB, no rows).

    Models are saved with ``_validate=False`` because the datasources point at
    :memory: SQLite files with no tables — inference is metadata-only.
    """
    d = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    for ds, models in _ALL_TOPOLOGIES:
        await storage.save_datasource(
            DatasourceConfig(name=ds, type="sqlite", database=":memory:"),
        )
        for model in models:
            await storage.save_model(model, _validate=False)
    return storage


async def make_inference_engine() -> SlayerQueryEngine:
    return SlayerQueryEngine(storage=await make_inference_storage())


# --------------------------------------------------------------------------- #
# Seeded chain engine (SQLite + DuckDB) for executed-value tests.
# --------------------------------------------------------------------------- #
_REGIONS_ROWS = [(1, "North"), (2, "South"), (3, "West")]
_CUSTOMERS_ROWS = [
    (1, "North", 1, "gold", "Alice"),
    (2, "North", 1, "silver", "Bob"),
    (3, "South", 2, "gold", "Cara"),
    (4, "West", 3, "bronze", "Dan"),
]
_ORDERS_ROWS = [
    (1, 1, "ok", 10.0, "2024-01-10"),
    (2, 1, "cancel", 5.0, "2024-01-20"),
    (3, 2, "ok", 20.0, "2024-02-10"),
    (4, 3, "ok", 30.0, "2024-03-01"),
]

# amount:sum by customers.region (NULL where a region has no orders).
CHAIN_REVENUE_BY_REGION = {"North": 35.0, "South": 30.0, "West": None}


def _seed_chain_sqlite(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT)")
    cur.executemany("INSERT INTO regions VALUES (?,?)", _REGIONS_ROWS)
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT, "
        "region_id INTEGER, tier TEXT, name TEXT)"
    )
    cur.executemany("INSERT INTO customers VALUES (?,?,?,?,?)", _CUSTOMERS_ROWS)
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
        "status TEXT, amount REAL, ordered_at TEXT)"
    )
    cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", _ORDERS_ROWS)
    con.commit()
    con.close()


def _seed_chain_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE regions (id INTEGER, name VARCHAR)")
    con.executemany("INSERT INTO regions VALUES (?,?)", _REGIONS_ROWS)
    con.execute(
        "CREATE TABLE customers (id INTEGER, region VARCHAR, region_id INTEGER, "
        "tier VARCHAR, name VARCHAR)"
    )
    con.executemany("INSERT INTO customers VALUES (?,?,?,?,?)", _CUSTOMERS_ROWS)
    con.execute(
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, status VARCHAR, "
        "amount DOUBLE, ordered_at VARCHAR)"
    )
    con.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", _ORDERS_ROWS)
    con.close()


async def seed_chain_storage(base_dir: str) -> tuple[YAMLStorage, str]:
    """(storage, db_path) over a seeded SQLite chain rooted at ``base_dir``.

    Returns the db_path so callers can mutate rows (e.g. to drive cache refresh).
    """
    db_path = os.path.join(base_dir, "data.sqlite")
    _seed_chain_sqlite(db_path)
    storage = YAMLStorage(base_dir=os.path.join(base_dir, "store"))
    await storage.save_datasource(
        DatasourceConfig(name=DS_CHAIN, type="sqlite", database=db_path),
    )
    for model in chain_models():
        await storage.save_model(model)
    return storage, db_path


async def make_chain_sqlite_storage() -> YAMLStorage:
    """Persistent (mkdtemp) YAMLStorage over a seeded SQLite chain.

    Used by the REST/MCP surface tests, which build a server from a storage and
    need the datasource file to outlive the call.
    """
    storage, _ = await seed_chain_storage(tempfile.mkdtemp())
    return storage


async def make_chain_exec_engine(dialect: str) -> AsyncIterator[SlayerQueryEngine]:
    """Seeded, executing chain engine — one backend per call."""
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, f"data.{dialect}")
        if dialect == "sqlite":
            _seed_chain_sqlite(db_path)
        else:
            _seed_chain_duckdb(db_path)
        storage = YAMLStorage(base_dir=os.path.join(d, "store"))
        await storage.save_datasource(
            DatasourceConfig(name=DS_CHAIN, type=dialect, database=db_path),
        )
        for model in chain_models():
            await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)
        try:
            yield engine
        finally:
            await engine.aclose()


# --------------------------------------------------------------------------- #
# Row helpers.
# --------------------------------------------------------------------------- #
def rows_by(resp, *, key: str, value: str) -> dict:
    """{row[key]: row[value]} over a response's data rows."""
    return {r[key]: r[value] for r in resp.data}


def key_set(resp, *keys: str) -> set:
    """Set of value tuples over the given column keys (dimension row set)."""
    return {tuple(r[k] for k in keys) for r in resp.data}
