"""Shared fixtures for query-local stage names and query-backed splicing.

Graph (datasource ``test``): ``orders`` → ``customers`` / ``clients`` (``clients`` reads the
physical table ``acct``) → ``cust_rev`` (query-backed). No orphan order, so every join is total.
"""

from __future__ import annotations

import re
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator
from typing import Any, Dict, List, Optional, Union

import pytest

from slayer.core.enums import DataType
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import DatasetExtension, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import seeded_exec_engine

# (id, tier, spend)
CUSTOMERS_ROWS = [
    (1, "gold", 100.0),
    (2, "silver", 150.0),
    (3, "gold", 60.0),
    (4, "bronze", 40.0),
    (5, "silver", 80.0),
]
# (id, customer_id, status, amount, ordered_at)
ORDERS_ROWS = [
    (1, 1, "ok", 10.0, "2024-01-10"),
    (2, 1, "new", 20.0, "2024-01-20"),
    (3, 2, "ok", 30.0, "2024-02-10"),
    (4, 2, "new", 25.0, "2024-02-20"),
    (5, 3, "ok", 5.0, "2024-03-05"),
    (6, 4, "new", 40.0, "2024-03-10"),
    (7, 5, "ok", 15.0, "2024-04-02"),
]

AMOUNT_BY_TIER = {"gold": 35.0, "silver": 70.0, "bronze": 40.0}
AMOUNT_BY_STATUS = {"ok": 60.0, "new": 85.0}
SPEND_BY_TIER = {"gold": 160.0, "silver": 230.0, "bronze": 40.0}
#: cust_rev: per customer (rev = sum, top = max of per-(customer, status) sums).
CUST_REV = {1: 30.0, 2: 55.0, 3: 5.0, 4: 40.0, 5: 15.0}
CUST_TOP = {1: 20.0, 2: 30.0, 3: 5.0, 4: 40.0, 5: 15.0}
#: ok_rev: per customer, ``ok`` orders only (customer 4 has none).
OK_REV = {1: 10.0, 2: 30.0, 3: 5.0, 5: 15.0}
#: Latest monthly amount per status (ok: April, new: March).
LAST_MONTHLY_BY_STATUS = {"ok": 15.0, "new": 40.0}

STATUS_LABEL = "Status!"
AMOUNT_DESCRIPTION = "Order amount"


def query(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate(kw)


def m(expression: str, name: str) -> Dict[str, str]:
    return {"metric": expression, "alias": name}


def ext_m(expression: str, name: str) -> Dict[str, str]:
    return {"name": name, "expression": expression}


# --------------------------------------------------------------------------- #
# Storage models.
# --------------------------------------------------------------------------- #
def customers_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="customers", data_source="test", sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="tier", datatype=DataType.STRING),
            DatasetField(name="spend", datatype=DataType.FLOAT),
        ],
    )


def clients_dataset() -> SlayerDataset:
    """Named unlike its physical table ``acct``; joins the query-backed ``cust_rev``."""
    return SlayerDataset(
        name="clients", data_source="test", sql_table="acct",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="tier", datatype=DataType.STRING),
            DatasetField(name="spend", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="cust_rev", from_columns=["id"], to_columns=["customer_id"])],
    )


def orders_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="orders", data_source="test", sql_table="orders",
        default_time_dimension="ordered_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="status", datatype=DataType.STRING, label=STATUS_LABEL),
            DatasetField(name="amount", datatype=DataType.FLOAT, description=AMOUNT_DESCRIPTION,
                   format=NumberFormat(type=NumberFormatType.CURRENCY)),
            DatasetField(name="ordered_at", datatype=DataType.DATETIME),
        ],
        relationships=[
            DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
            DatasetRelationship(to="clients", from_columns=["customer_id"], to_columns=["id"]),
        ],
    )


def orders_v_dataset(*, default: Optional[str]) -> SlayerDataset:
    """``orders`` carrying a ``th`` default (the source-model variable layer)."""
    return SlayerDataset(
        name="orders_v", data_source="test", sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="amount", datatype=DataType.FLOAT),
        ],
        query_variables={"th": default} if default is not None else {},
    )


def _qb(name: str, *stages: SlayerQuery, **kw: Any) -> SlayerDataset:
    return SlayerDataset(name=name, data_source="test", source_queries=list(stages), **kw)


def rev_by_status_dataset() -> SlayerDataset:
    return _qb("rev_by_status", query(
        population="orders", dimensions=["orders.status"], metrics=[m("sum(orders.amount)", "rev")]))


def status_share_dataset() -> SlayerDataset:
    """Query-backed over the query-backed ``rev_by_status``."""
    return _qb("status_share", query(
        population="rev_by_status", dimensions=["rev_by_status.status"], metrics=[m("sum(rev_by_status.rev)", "rr")]))


def cust_rev_dataset() -> SlayerDataset:
    """Multi-stage with a private stage ``x``; grain ``{customer_id}``."""
    return _qb(
        "cust_rev",
        query(name="x", population="orders", dimensions=["orders.customer_id", "orders.status"],
              metrics=[m("sum(orders.amount)", "amt")]),
        query(population="x", dimensions=["x.customer_id"],
              metrics=[m("sum(x.amt)", "rev"), m("max(x.amt)", "top")]),
    )


def ok_rev_dataset() -> SlayerDataset:
    """Multi-stage, also with a private stage ``x``."""
    return _qb(
        "ok_rev",
        query(name="x", population="orders", dimensions=["orders.customer_id", "orders.status"],
              metrics=[m("sum(orders.amount)", "amt")], filter=["orders.status = 'ok'"]),
        query(population="x", dimensions=["x.customer_id"], metrics=[m("sum(x.amt)", "ok_rev")]),
    )


def qb_shadow_dataset() -> SlayerDataset:
    """Private stage named ``customers``; the final stage reaches model ``customers`` via a stored join."""
    return _qb(
        "qb_shadow",
        query(name="customers", population="orders", dimensions=["orders.status"],
              metrics=[m("sum(orders.amount)", "a")]),
        query(population="orders", dimensions=["orders.customers.tier"],
              metrics=[m("sum(orders.amount)", "amt")]),
    )


def monthly_dataset() -> SlayerDataset:
    return _qb("monthly", query(
        population="orders", dimensions=["orders.status"],
        time_dimensions=[{"field": "orders.ordered_at", "granularity": "month"}],
        metrics=[m("sum(orders.amount)", "rev")]))


def bcast_qb_dataset() -> SlayerDataset:
    """Private stage ``bstage`` broadcasts ``sum(customers.spend)`` across ``status``."""
    return _qb(
        "bcast_qb",
        query(name="bstage", population="orders", dimensions=["orders.status"], to_many_handling="broadcast",
              metrics=[m("sum(orders.amount)", "amt"), m("sum(customers.spend)", "cs")]),
        query(population="bstage", dimensions=["bstage.status"], metrics=[m("sum(bstage.amt)", "amt2")]),
    )


def jt_qb_dataset() -> SlayerDataset:
    """``a`` depends on ``b`` only through a join target and is supplied first."""
    return _qb(
        "jt_qb",
        query(name="a",
              population="customers",
              extensions=[{"dataset": "customers",
                           "relationships": [{"to": "b", "from_columns": ["id"], "to_columns": ["customer_id"]}]}],
              dimensions=["customers.tier"], metrics=[m("sum(b.rev)", "ab")]),
        query(name="b", population="orders", dimensions=["orders.customer_id"],
              metrics=[m("sum(orders.amount)", "rev")]),
        query(population="a", dimensions=["a.tier"], metrics=[m("max(a.ab)", "v")]),
    )


def _sql_dataset(name: str, sql: str) -> SlayerDataset:
    return SlayerDataset(
        name=name, data_source="test", sql=sql,
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="amount", datatype=DataType.FLOAT),
        ],
    )


#: sql-backed models whose SQL carries ``WITH``: name → (sql, per-customer amount sums).
EMBEDDED_WITH: Dict[str, "tuple[str, Dict[int, float]]"] = {
    "with_t": (
        "WITH t AS (SELECT id, customer_id, amount FROM orders) "
        "SELECT id, customer_id, amount FROM t",
        CUST_REV,
    ),
    "with_shadow": (
        "WITH t AS (SELECT id, customer_id, amount FROM orders WHERE status = 'new') "
        "SELECT id, customer_id, amount FROM ("
        "WITH t AS (SELECT id, customer_id, amount FROM orders WHERE status = 'ok') "
        "SELECT id, customer_id, amount FROM t) AS okrows "
        "UNION ALL SELECT id, customer_id, amount FROM t",
        CUST_REV,
    ),
    "with_alias": (
        "WITH t AS (SELECT id FROM orders WHERE status = 'ok') "
        "SELECT t.id, t.customer_id, t.amount FROM orders AS t "
        "WHERE t.id IN (SELECT id FROM t)",
        OK_REV,
    ),
    "with_recursive": (
        "WITH RECURSIVE n(k) AS (SELECT 1 UNION ALL SELECT k + 1 FROM n WHERE k < 3) "
        "SELECT o.id, o.customer_id, o.amount FROM orders AS o JOIN n ON o.id = n.k",
        {1: 30.0, 2: 30.0},
    ),
    "with_quoted": (
        'WITH "my t" AS (SELECT id, customer_id, amount FROM orders) '
        'SELECT id, customer_id, amount FROM "my t"',
        CUST_REV,
    ),
}


def embedded_with_datasets() -> List[SlayerDataset]:
    return [_sql_dataset(name, sql) for name, (sql, _) in EMBEDDED_WITH.items()]


def query_backed_datasets() -> List[SlayerDataset]:
    return [
        rev_by_status_dataset(), status_share_dataset(), cust_rev_dataset(), ok_rev_dataset(),
        qb_shadow_dataset(), monthly_dataset(), bcast_qb_dataset(), jt_qb_dataset(),
    ]


def dev1966_datasets() -> List[SlayerDataset]:
    return [
        orders_dataset(), customers_dataset(), clients_dataset(),
        *query_backed_datasets(), *embedded_with_datasets(),
    ]


# --------------------------------------------------------------------------- #
# Engines.
# --------------------------------------------------------------------------- #
def seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        cur = con.cursor()
        for table in ("customers", "acct"):
            cur.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, tier TEXT, spend REAL)")
            cur.executemany(f"INSERT INTO {table} VALUES (?,?,?)", CUSTOMERS_ROWS)
        cur.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
            "status TEXT, amount REAL, ordered_at TEXT)")
        cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", ORDERS_ROWS)


def seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    try:
        for table in ("customers", "acct"):
            con.execute(f"CREATE TABLE {table} (id INTEGER, tier VARCHAR, spend DOUBLE)")
            con.executemany(f"INSERT INTO {table} VALUES (?,?,?)", CUSTOMERS_ROWS)
        con.execute(
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, status VARCHAR, "
            "amount DOUBLE, ordered_at TIMESTAMP)")
        con.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", ORDERS_ROWS)
    finally:
        con.close()


@asynccontextmanager
async def dev1966_engine(
    dialect: str, *, datasets: Optional[List[SlayerDataset]] = None,
) -> AsyncGenerator[SlayerQueryEngine]:
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = seed_duckdb if dialect == "duckdb" else seed_sqlite
    async with seeded_exec_engine(
        dialect=dialect, seed=seed, datasets=dev1966_datasets() if datasets is None else datasets,
    ) as (engine, _):
        yield engine


# --------------------------------------------------------------------------- #
# Helpers.
# --------------------------------------------------------------------------- #
def explicit_splice(
    dataset: SlayerDataset, *, rename: Optional[Dict[str, str]] = None,
) -> List[SlayerQuery]:
    """``model``'s stages as named user stages: private stages renamed per ``rename``, the final one
    named after the model."""
    rename = rename or {}
    stages: List[SlayerQuery] = list(dataset.source_queries or [])

    def respell(spec: Union[str, DatasetExtension, SlayerDataset, None]) -> Union[str, DatasetExtension, SlayerDataset, None]:
        if isinstance(spec, str):
            return rename.get(spec, spec)
        if spec is None or isinstance(spec, SlayerDataset):
            return spec
        joins = [j.model_copy(update={"to": rename.get(j.to, j.to)})
                 for j in spec.relationships or []]
        return spec.model_copy(update={"dataset": rename.get(spec.dataset, spec.dataset),
                                       "relationships": joins})

    def anchor(text: str) -> str:
        for old, new in rename.items():
            text = re.sub(rf"(?<![\w.\"]){re.escape(old)}\.", f"{new}.", text)
        return text

    out = []
    for i, stage in enumerate(stages):
        name = dataset.name if i == len(stages) - 1 else respell(stage.name)
        extensions = [respell(e) for e in stage.extensions] if stage.extensions else stage.extensions
        update: Dict[str, Any] = {"name": name, "population": respell(stage.population), "extensions": extensions}
        if rename and stage.dimensions:
            update["dimensions"] = [d.model_copy(update={"field": anchor(d.field)}) for d in stage.dimensions]
        if rename and stage.metrics:
            update["metrics"] = [mt.model_copy(update={"metric": anchor(mt.metric)}) for mt in stage.metrics]
        if rename and isinstance(stage.filter, list):
            update["filter"] = [anchor(f) for f in stage.filter]
        out.append(stage.model_copy(update=update))
    return out


def sorted_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(rows, key=lambda r: [(v is None, str(v)) for v in r.values()])


def rows_by(rows: List[Dict[str, Any]], *, key: str, value: str) -> Dict[Any, Any]:
    return {r[key]: r[value] for r in rows}
