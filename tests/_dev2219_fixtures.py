"""A small shop for stage edge inheritance on pass-through key fields.

Physical tables::

    regions   (id, name)
    customers (id, name, tier, region, region_id)
    orders    (id, customer_id, status, amount, ordered_at, region_id)
    calendar  (day, is_holiday)
    invoices  (id, billing_customer_id, shipping_customer_id, total)
    events    (id, customer_id)
    quotas    (customer_id, status, quota)

Stored edges: ``customers → regions`` (many_to_one), ``orders → customers`` (unnamed, LEFT, many_to_one),
``orders → calendar`` on ``ordered_at = day``, ``invoices → customers`` as ``billing`` and ``shipping``,
``events → customers``. ``orders`` has no stored edge to ``regions`` or ``quotas``; variants add them.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import pytest

from slayer.core.enums import DataType, JoinType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import seeded_exec_engine

DS = "test"
DIALECTS = ["sqlite", "duckdb"]

_TABLES: dict[str, tuple[str, list[tuple]]] = {
    "regions": ("id INTEGER, name TEXT", [(1, "North"), (2, "South"), (3, "East")]),
    "customers": ("id INTEGER, name TEXT, tier TEXT, region TEXT, region_id INTEGER", [
        (1, "Ann", "gold", "North", 1),
        (2, "Bob", "silver", "South", 2),
        (3, "Cid", "gold", "North", 1),
        (4, "Dee", "bronze", None, None),
        (5, "Eve", "silver", "East", 3),
    ]),
    "orders": ("id INTEGER, customer_id INTEGER, status TEXT, amount DOUBLE, ordered_at TIMESTAMP, region_id INTEGER", [
        (1, 1, "ok", 10.0, "2024-01-10", 1),
        (2, 1, "new", 20.0, "2024-01-20", 2),
        (3, 2, "ok", 30.0, "2024-02-10", 2),
        (4, 3, "ok", 5.0, "2024-02-15", 1),
        (5, 4, "new", 40.0, "2024-03-10", 3),
        (6, 5, "ok", 15.0, "2024-03-12", None),
        (7, None, "ok", 7.0, "2024-03-20", 1),
    ]),
    "calendar": ("day TIMESTAMP, is_holiday INTEGER", [
        ("2024-01-10", 1), ("2024-01-20", 0), ("2024-02-10", 0), ("2024-02-15", 1),
        ("2024-03-10", 0), ("2024-03-12", 0), ("2024-03-20", 1),
    ]),
    "invoices": ("id INTEGER, billing_customer_id INTEGER, shipping_customer_id INTEGER, total DOUBLE", [
        (1, 1, 2, 100.0), (2, 2, 2, 50.0), (3, 3, 1, 25.0), (4, 5, 4, 10.0),
    ]),
    "events": ("id INTEGER, customer_id INTEGER", [(1, 1), (2, 1), (3, 2), (4, 5)]),
    "quotas": ("customer_id INTEGER, status TEXT, quota INTEGER", [
        (1, "ok", 5), (1, "new", 6), (2, "ok", 7), (3, "ok", 8), (4, "new", 9), (5, "ok", 10),
    ]),
}

#: sum(orders.amount) per customer id; the orphan order is None.
REVENUE_BY_CUSTOMER = {1: 30.0, 2: 30.0, 3: 5.0, 4: 40.0, 5: 15.0, None: 7.0}
#: REVENUE_BY_CUSTOMER by the customer's name, customers-rooted (the orphan order has no customer).
REVENUE_BY_NAME = {"Ann": 30.0, "Bob": 30.0, "Cid": 5.0, "Dee": 40.0, "Eve": 15.0}
#: sum(orders.amount) by customers.region, rooted anywhere (Dee and the orphan land in None: Ossie S1).
REVENUE_BY_REGION = {"North": 35.0, "South": 30.0, "East": 15.0, None: 47.0}
#: sum(orders.amount) by the order's own regions.name.
REVENUE_BY_ORDER_REGION = {"North": 22.0, "South": 50.0, "East": 40.0, None: 15.0}
#: Per orders.region_id, regions.name joined on that region's order count (3, 2, 1, 1) instead of its id.
ZONE_BY_COUNT = {1: "East", 2: "South", 3: "North", None: "North"}
#: sum(invoices.total) by the billing customer's name.
INVOICE_TOTAL_BY_BILLING = {"Ann": 100.0, "Bob": 50.0, "Cid": 25.0, "Eve": 10.0}
#: count(events) — every customer sees the same total.
EVENT_COUNT = 4

_INT, _TEXT, _DOUBLE, _TS = DataType.INTEGER, DataType.STRING, DataType.FLOAT, DataType.DATETIME


def _seed(db_path: str, *, dialect: str) -> None:
    if dialect == "duckdb":
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(db_path)
        try:
            for table, (ddl, data) in _TABLES.items():
                con.execute(f"CREATE TABLE {table} ({ddl})")
                marks = ",".join("?" * len(data[0]))
                con.executemany(f"INSERT INTO {table} VALUES ({marks})", data)
        finally:
            con.close()
        return
    with transaction(db_path) as cur:
        for table, (ddl, data) in _TABLES.items():
            cur.execute(f"CREATE TABLE {table} ({ddl.replace('TIMESTAMP', 'TEXT')})")
            marks = ",".join("?" * len(data[0]))
            cur.executemany(f"INSERT INTO {table} VALUES ({marks})", data)


def _fields(*spec: tuple[str, DataType], pk: str | None = "id") -> list[DatasetField]:
    return [DatasetField(name=n, datatype=t, primary_key=n == pk) for n, t in spec]


def rel(target: str, pairs: list[tuple[str, str]], **kw: Any) -> DatasetRelationship:
    return DatasetRelationship(to=target, from_columns=[a for a, _ in pairs], to_columns=[b for _, b in pairs], **kw)


def regions() -> SlayerDataset:
    return SlayerDataset(name="regions", data_source=DS, sql_table="regions", fields=_fields(("id", _INT), ("name", _TEXT)))


def customers(*, filters: list[str] | None = None, to_regions: bool = True) -> SlayerDataset:
    return SlayerDataset(
        name="customers", data_source=DS, sql_table="customers", filters=filters or [],
        fields=_fields(("id", _INT), ("name", _TEXT), ("tier", _TEXT), ("region", _TEXT), ("region_id", _INT)),
        relationships=[rel("regions", [("region_id", "id")], cardinality=RelationshipCardinality.MANY_TO_ONE)]
        if to_regions else [],
    )


def orders(*, filters: list[str] | None = None, customer_edge_name: str | None = None,
           ship_region: bool = False, to_quotas: bool = False) -> SlayerDataset:
    edges = [
        rel("customers", [("customer_id", "id")], name=customer_edge_name, join_type=JoinType.LEFT,
            cardinality=RelationshipCardinality.MANY_TO_ONE),
        rel("calendar", [("ordered_at", "day")]),
    ]
    if ship_region:
        edges.append(rel("regions", [("region_id", "id")], name="ship_region",
                         cardinality=RelationshipCardinality.MANY_TO_ONE))
    if to_quotas:
        edges.append(rel("quotas", [("customer_id", "customer_id"), ("status", "status")]))
    return SlayerDataset(
        name="orders", data_source=DS, sql_table="orders", filters=filters or [],
        default_time_dimension="ordered_at",
        fields=_fields(("id", _INT), ("customer_id", _INT), ("status", _TEXT), ("amount", _DOUBLE),
                       ("ordered_at", _TS), ("region_id", _INT)),
        relationships=edges,
    )


def calendar() -> SlayerDataset:
    return SlayerDataset(name="calendar", data_source=DS, sql_table="calendar",
                         fields=_fields(("day", _TS), ("is_holiday", _INT), pk="day"))


def invoices() -> SlayerDataset:
    return SlayerDataset(
        name="invoices", data_source=DS, sql_table="invoices",
        fields=_fields(("id", _INT), ("billing_customer_id", _INT), ("shipping_customer_id", _INT), ("total", _DOUBLE)),
        relationships=[
            rel("customers", [("billing_customer_id", "id")], name="billing"),
            rel("customers", [("shipping_customer_id", "id")], name="shipping"),
        ],
    )


def events() -> SlayerDataset:
    return SlayerDataset(name="events", data_source=DS, sql_table="events",
                         fields=_fields(("id", _INT), ("customer_id", _INT)),
                         relationships=[rel("customers", [("customer_id", "id")])])


def quotas() -> SlayerDataset:
    return SlayerDataset(name="quotas", data_source=DS, sql_table="quotas",
                         fields=_fields(("customer_id", _INT), ("status", _TEXT), ("quota", _INT), pk=None))


def base_datasets() -> list[SlayerDataset]:
    return [regions(), customers(), orders(), calendar(), invoices(), events(), quotas()]


def with_datasets(*replacements: SlayerDataset) -> list[SlayerDataset]:
    """``base_datasets()`` with same-named datasets replaced."""
    by_name = {d.name: d for d in replacements}
    return [by_name.pop(d.name, d) for d in base_datasets()] + list(by_name.values())


@asynccontextmanager
async def shop(dialect: str, datasets: list[SlayerDataset] | None = None) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect=dialect, seed=lambda path: _seed(path, dialect=dialect),
        datasets=datasets if datasets is not None else base_datasets(),
    ) as (engine, _db):
        yield engine


def q(**fields: Any) -> SlayerQuery:
    return SlayerQuery.model_validate(fields)


def m(expr: str, alias: str) -> dict:
    return {"metric": expr, "alias": alias}


def ext(dataset: str, *relationships: dict, **fields: Any) -> dict:
    return {"dataset": dataset, "relationships": list(relationships), **fields}


def relationship_to(target: str, pairs: list[tuple[str, str]], **fields: Any) -> dict:
    return {"to": target, "from_columns": [a for a, _ in pairs], "to_columns": [b for _, b in pairs], **fields}


def per_customer(name: str = "per_customer", **kw: Any) -> SlayerQuery:
    """``orders`` grouped by ``customer_id``, ``revenue = sum(amount)``."""
    kw.setdefault("dimensions", ["orders.customer_id"])
    kw.setdefault("metrics", [m("sum(orders.amount)", "revenue")])
    return q(name=name, population="orders", **kw)


def over(population: str, *, dimensions: list, metric: str | None = None, alias: str = "total",
         **kw: Any) -> SlayerQuery:
    """The root stage reading ``population``; ``metric`` defaults to ``sum(<population>.revenue)``."""
    return q(population=population, dimensions=dimensions, metrics=[m(metric or f"sum({population}.revenue)", alias)],
             **kw)


def column(resp: SlayerResponse, suffix: str) -> str:
    """The one result key equal to ``suffix`` or ending in ``.suffix``."""
    keys = list(resp.data[0]) if resp.data else list(resp.columns or [])
    hits = [k for k in keys if k == suffix or k.endswith(f".{suffix}")]
    assert len(hits) == 1, (suffix, keys)
    return hits[0]


def by(resp: SlayerResponse, *, key: str, value: str) -> dict:
    """``{row[key]: float(row[value])}``, both columns matched by suffix."""
    k, v = column(resp, key), column(resp, value)
    return {row[k]: (None if row[v] is None else float(row[v])) for row in resp.data}


def kinds(resp: SlayerResponse) -> set[str]:
    return {w.kind for w in resp.warnings}


async def outcome(engine: SlayerQueryEngine, query: Any) -> tuple:
    """``("ok", sorted rows)`` or ``("error", type, message)`` — for comparing two runs."""
    try:
        resp = await engine.execute(query)
    except Exception as exc:  # noqa: BLE001 — compared, never swallowed
        return ("error", type(exc).__name__, str(exc))
    return ("ok", tuple(sorted((tuple(sorted(r.items(), key=repr)) for r in resp.data), key=repr)))
