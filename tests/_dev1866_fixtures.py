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
from slayer.storage.sqlite_conn import transaction
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import (
    Aggregation,
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import seeded_exec_engine

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
def chain_datasets() -> list[SlayerDataset]:
    """orders → customers → regions; region is a column on customers."""
    regions = SlayerDataset(
        name="regions", data_source=DS_CHAIN, sql_table="regions",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="name", datatype=DataType.TEXT),
        ],
    )
    customers = SlayerDataset(
        name="customers", data_source=DS_CHAIN, sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="region", datatype=DataType.TEXT),
            DatasetField(name="region_id", datatype=DataType.INT),
            DatasetField(name="tier", datatype=DataType.TEXT),
            DatasetField(name="name", datatype=DataType.TEXT),
        ],
        relationships=[
            DatasetRelationship(
                to="regions", join_pairs=[["region_id", "id"]],
                cardinality=RelationshipCardinality.MANY_TO_ONE,
            ),
        ],
    )
    orders = SlayerDataset(
        name="orders", data_source=DS_CHAIN, sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INT),
            DatasetField(name="status", datatype=DataType.TEXT),
            DatasetField(name="amount", datatype=DataType.DOUBLE),
            DatasetField(name="ordered_at", datatype=DataType.TEXT),
        ],
        metrics=[
            DatasetMetric(expression="sum(amount)", name="revenue"),
            DatasetMetric(expression="avg(amount)", name="avg_amt"),
        ],
        relationships=[
            DatasetRelationship(
                to="customers", join_pairs=[["customer_id", "id"]],
                cardinality=RelationshipCardinality.MANY_TO_ONE,
            ),
        ],
    )
    return [orders, customers, regions]


def chain_datasets_by_name() -> dict[str, SlayerDataset]:
    return {m.name: m for m in chain_datasets()}


def pair_datasets() -> list[SlayerDataset]:
    """prof ↔ acct one-to-one — both sides to-one, so two viable candidates tie."""
    acct = SlayerDataset(
        name="acct", data_source=DS_PAIR, sql_table="acct",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="email", datatype=DataType.TEXT),
        ],
    )
    prof = SlayerDataset(
        name="prof", data_source=DS_PAIR, sql_table="prof",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="acct_id", datatype=DataType.INT, unique=True),
            DatasetField(name="bio", datatype=DataType.TEXT),
        ],
        relationships=[
            DatasetRelationship(
                to="acct", join_pairs=[["acct_id", "id"]],
                cardinality=RelationshipCardinality.ONE_TO_ONE,
            ),
        ],
    )
    return [acct, prof]


def fan_datasets() -> list[SlayerDataset]:
    """basket with two children — sibling dims have no to-one determiner."""
    basket = SlayerDataset(
        name="basket", data_source=DS_FAN, sql_table="basket",
        fields=[DatasetField(name="id", datatype=DataType.INT, primary_key=True)],
    )
    items = SlayerDataset(
        name="basket_items", data_source=DS_FAN, sql_table="basket_items",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="basket_id", datatype=DataType.INT),
            DatasetField(name="sku", datatype=DataType.TEXT),
        ],
        relationships=[
            DatasetRelationship(
                to="basket", join_pairs=[["basket_id", "id"]],
                cardinality=RelationshipCardinality.MANY_TO_ONE,
            ),
        ],
    )
    pays = SlayerDataset(
        name="basket_pays", data_source=DS_FAN, sql_table="basket_pays",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="basket_id", datatype=DataType.INT),
            DatasetField(name="method", datatype=DataType.TEXT),
        ],
        relationships=[
            DatasetRelationship(
                to="basket", join_pairs=[["basket_id", "id"]],
                cardinality=RelationshipCardinality.MANY_TO_ONE,
            ),
        ],
    )
    return [basket, items, pays]


def ambig_datasets() -> list[SlayerDataset]:
    """shipment with two unnamed edges to depot — any depot hop is ambiguous."""
    depot = SlayerDataset(
        name="depot", data_source=DS_AMBIG, sql_table="depot",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="name", datatype=DataType.TEXT),
        ],
    )
    shipment = SlayerDataset(
        name="shipment", data_source=DS_AMBIG, sql_table="shipment",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="origin_depot_id", datatype=DataType.INT),
            DatasetField(name="dest_depot_id", datatype=DataType.INT),
            DatasetField(name="weight", datatype=DataType.DOUBLE),
        ],
        relationships=[
            DatasetRelationship(
                to="depot", join_pairs=[["origin_depot_id", "id"]],
                cardinality=RelationshipCardinality.MANY_TO_ONE,
            ),
            DatasetRelationship(
                to="depot", join_pairs=[["dest_depot_id", "id"]],
                cardinality=RelationshipCardinality.MANY_TO_ONE,
            ),
        ],
    )
    return [depot, shipment]


def unknown_datasets() -> list[SlayerDataset]:
    """hits → sess over an undeclared-cardinality hop into a keyless target.

    ``sess`` has no primary key or unique column, so the hop is neither declared
    to-one nor unique-key covered — unknown cardinality, which does not count as
    determination in either direction.
    """
    sess = SlayerDataset(
        name="sess", data_source=DS_UNKNOWN, sql_table="sess",
        fields=[
            DatasetField(name="sess_key", datatype=DataType.INT),
            DatasetField(name="token", datatype=DataType.TEXT),
        ],
    )
    hits = SlayerDataset(
        name="hits", data_source=DS_UNKNOWN, sql_table="hits",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="sess_ref", datatype=DataType.INT),
            DatasetField(name="page", datatype=DataType.TEXT),
        ],
        relationships=[
            DatasetRelationship(
                to="sess", join_pairs=[["sess_ref", "sess_key"]],
                cardinality=None,
            ),
        ],
    )
    return [sess, hits]


def keycov_datasets() -> list[SlayerDataset]:
    """events → days over an UNDECLARED-cardinality hop into a unique key.

    ``events.day_id → days.id`` declares no cardinality, but ``days.id`` is the
    primary key, so the hop is provably to-one by unique-key coverage — it counts
    as determination even though cardinality is unset.
    """
    days = SlayerDataset(
        name="days", data_source=DS_KEYCOV, sql_table="days",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="label", datatype=DataType.TEXT),
        ],
    )
    events = SlayerDataset(
        name="events", data_source=DS_KEYCOV, sql_table="events",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="day_id", datatype=DataType.INT),
            DatasetField(name="kind", datatype=DataType.TEXT),
        ],
        relationships=[
            DatasetRelationship(
                to="days", join_pairs=[["day_id", "id"]],
                cardinality=None,
            ),
        ],
    )
    return [days, events]


def named_datasets() -> list[SlayerDataset]:
    """tickets → agents over a *named* join ("reporter"); agents owns a saved measure,
    and column ``score`` deliberately shares a name with a custom aggregation."""
    agents = SlayerDataset(
        name="agents", data_source=DS_NAMED, sql_table="agents",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="name", datatype=DataType.TEXT),
            DatasetField(name="score", datatype=DataType.INT),
        ],
        metrics=[DatasetMetric(expression="count(id)", name="handled")],
        aggregations=[Aggregation(name="score", expression="MAX({score})")],
    )
    tickets = SlayerDataset(
        name="tickets", data_source=DS_NAMED, sql_table="tickets",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="subject", datatype=DataType.TEXT),
            DatasetField(name="agent_id", datatype=DataType.INT),
        ],
        relationships=[
            DatasetRelationship(
                name="reporter", to="agents",
                join_pairs=[["agent_id", "id"]],
                cardinality=RelationshipCardinality.MANY_TO_ONE,
            ),
        ],
    )
    return [agents, tickets]


def named_datasets_by_name() -> dict[str, SlayerDataset]:
    return {m.name: m for m in named_datasets()}


def widgets_dataset(data_source: str) -> SlayerDataset:
    return SlayerDataset(
        name="widgets", data_source=data_source, sql_table="widgets",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="x", datatype=DataType.TEXT),
        ],
    )


# --------------------------------------------------------------------------- #
# Storage for metadata-only inference tests (no rows executed).
# --------------------------------------------------------------------------- #
_ALL_TOPOLOGIES: list[tuple[str, list[SlayerDataset]]] = [
    (DS_CHAIN, chain_datasets()),
    (DS_PAIR, pair_datasets()),
    (DS_FAN, fan_datasets()),
    (DS_AMBIG, ambig_datasets()),
    (DS_UNKNOWN, unknown_datasets()),
    (DS_KEYCOV, keycov_datasets()),
    (DS_WIDGETS_A, [widgets_dataset(DS_WIDGETS_A)]),
    (DS_WIDGETS_B, [widgets_dataset(DS_WIDGETS_B)]),
    (DS_NAMED, named_datasets()),
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
            await storage.save_dataset(model, _validate=False)
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
    with transaction(db_path) as con:
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
    for model in chain_datasets():
        await storage.save_dataset(model)
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
    seed = _seed_chain_duckdb if dialect == "duckdb" else _seed_chain_sqlite
    async with seeded_exec_engine(
        dialect=dialect, seed=seed, datasets=chain_datasets(),
        datasource=DS_CHAIN, validate=True,
    ) as (engine, _db):
        yield engine


# --------------------------------------------------------------------------- #
# Row helpers.
# --------------------------------------------------------------------------- #
def rows_by(resp, *, key: str, value: str) -> dict:
    """{row[key]: row[value]} over a response's data rows."""
    return {r[key]: r[value] for r in resp.data}


def key_set(resp, *keys: str) -> set:
    """Set of value tuples over the given column keys (dimension row set)."""
    return {tuple(r[k] for k in keys) for r in resp.data}
