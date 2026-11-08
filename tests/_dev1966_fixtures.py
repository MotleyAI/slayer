"""Shared fixtures for query-local stage names and query-backed splicing.

Graph (datasource ``test``): ``orders`` → ``customers`` / ``clients`` (``clients`` reads the
physical table ``acct``) → ``cust_rev`` (query-backed). No orphan order, so every join is total.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator
from typing import Any, Dict, List, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
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


def m(formula: str, name: str) -> Dict[str, str]:
    return {"formula": formula, "name": name}


# --------------------------------------------------------------------------- #
# Storage models.
# --------------------------------------------------------------------------- #
def customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="tier", type=DataType.TEXT),
            Column(name="spend", type=DataType.DOUBLE),
        ],
    )


def clients_model() -> SlayerModel:
    """Named unlike its physical table ``acct``; joins the query-backed ``cust_rev``."""
    return SlayerModel(
        name="clients", data_source="test", sql_table="acct",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="tier", type=DataType.TEXT),
            Column(name="spend", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="cust_rev", join_pairs=[["id", "customer_id"]])],
    )


def orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        default_time_dimension="ordered_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT, label=STATUS_LABEL),
            Column(name="amount", type=DataType.DOUBLE, description=AMOUNT_DESCRIPTION,
                   format=NumberFormat(type=NumberFormatType.CURRENCY)),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ModelJoin(target_model="clients", join_pairs=[["customer_id", "id"]]),
        ],
    )


def orders_v_model(*, default: Optional[str]) -> SlayerModel:
    """``orders`` carrying a ``th`` default (the source-model variable layer)."""
    return SlayerModel(
        name="orders_v", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
        ],
        query_variables={"th": default} if default is not None else {},
    )


def _qb(name: str, *stages: SlayerQuery, **kw: Any) -> SlayerModel:
    return SlayerModel(name=name, data_source="test", source_queries=list(stages), **kw)


def rev_by_status_model() -> SlayerModel:
    return _qb("rev_by_status", query(
        source_model="orders", dimensions=["status"], measures=[m("amount:sum", "rev")]))


def status_share_model() -> SlayerModel:
    """Query-backed over the query-backed ``rev_by_status``."""
    return _qb("status_share", query(
        source_model="rev_by_status", dimensions=["status"], measures=[m("rev:sum", "rr")]))


def cust_rev_model() -> SlayerModel:
    """Multi-stage with a private stage ``x``; grain ``{customer_id}``."""
    return _qb(
        "cust_rev",
        query(name="x", source_model="orders", dimensions=["customer_id", "status"],
              measures=[m("amount:sum", "amt")]),
        query(source_model="x", dimensions=["customer_id"],
              measures=[m("amt:sum", "rev"), m("amt:max", "top")]),
    )


def ok_rev_model() -> SlayerModel:
    """Multi-stage, also with a private stage ``x``."""
    return _qb(
        "ok_rev",
        query(name="x", source_model="orders", dimensions=["customer_id", "status"],
              measures=[m("amount:sum", "amt")], filters=["status = 'ok'"]),
        query(source_model="x", dimensions=["customer_id"], measures=[m("amt:sum", "ok_rev")]),
    )


def qb_shadow_model() -> SlayerModel:
    """Private stage named ``customers``; the final stage reaches model ``customers`` via a stored join."""
    return _qb(
        "qb_shadow",
        query(name="customers", source_model="orders", dimensions=["status"],
              measures=[m("amount:sum", "a")]),
        query(source_model="orders", dimensions=["customers.tier"],
              measures=[m("amount:sum", "amt")]),
    )


def monthly_model() -> SlayerModel:
    return _qb("monthly", query(
        source_model="orders", dimensions=["status"],
        time_dimensions=[{"dimension": "ordered_at", "granularity": "month"}],
        measures=[m("amount:sum", "rev")]))


def bcast_qb_model() -> SlayerModel:
    """Private stage ``bstage`` broadcasts ``customers.spend:sum`` across ``status``."""
    return _qb(
        "bcast_qb",
        query(name="bstage", source_model="orders", dimensions=["status"],
              measures=[m("amount:sum", "amt"), m("customers.spend:sum", "cs")]),
        query(source_model="bstage", dimensions=["status"], measures=[m("amt:sum", "amt2")]),
    )


def jt_qb_model() -> SlayerModel:
    """``a`` depends on ``b`` only through a join target and is supplied first."""
    return _qb(
        "jt_qb",
        query(name="a",
              source_model={"source_name": "customers",
                            "joins": [{"target_model": "b", "join_pairs": [["id", "customer_id"]]}]},
              dimensions=["tier"], measures=[m("b.rev:sum", "ab")]),
        query(name="b", source_model="orders", dimensions=["customer_id"],
              measures=[m("amount:sum", "rev")]),
        query(source_model="a", dimensions=["tier"], measures=[m("ab:max", "v")]),
    )


def _sql_model(name: str, sql: str) -> SlayerModel:
    return SlayerModel(
        name=name, data_source="test", sql=sql,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
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


def embedded_with_models() -> List[SlayerModel]:
    return [_sql_model(name, sql) for name, (sql, _) in EMBEDDED_WITH.items()]


def query_backed_models() -> List[SlayerModel]:
    return [
        rev_by_status_model(), status_share_model(), cust_rev_model(), ok_rev_model(),
        qb_shadow_model(), monthly_model(), bcast_qb_model(), jt_qb_model(),
    ]


def dev1966_models() -> List[SlayerModel]:
    return [
        orders_model(), customers_model(), clients_model(),
        *query_backed_models(), *embedded_with_models(),
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
    dialect: str, *, models: Optional[List[SlayerModel]] = None,
) -> AsyncGenerator[SlayerQueryEngine]:
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = seed_duckdb if dialect == "duckdb" else seed_sqlite
    async with seeded_exec_engine(
        dialect=dialect, seed=seed, models=dev1966_models() if models is None else models,
    ) as (engine, _):
        yield engine


# --------------------------------------------------------------------------- #
# Helpers.
# --------------------------------------------------------------------------- #
def explicit_splice(
    model: SlayerModel, *, rename: Optional[Dict[str, str]] = None,
) -> List[SlayerQuery]:
    """``model``'s stages as named user stages: private stages renamed per ``rename``, the final one
    named after the model."""
    rename = rename or {}
    stages = list(model.source_queries or [])

    def respell(spec: Any) -> Any:
        if isinstance(spec, str):
            return rename.get(spec, spec)
        if spec is None or isinstance(spec, SlayerModel):
            return spec
        joins = [j.model_copy(update={"target_model": rename.get(j.target_model, j.target_model)})
                 for j in spec.joins or []]
        return spec.model_copy(update={"source_name": rename.get(spec.source_name, spec.source_name),
                                       "joins": joins})

    out = []
    for i, stage in enumerate(stages):
        name = model.name if i == len(stages) - 1 else rename.get(stage.name, stage.name)
        out.append(stage.model_copy(update={"name": name, "source_model": respell(stage.source_model)}))
    return out


def sorted_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(rows, key=lambda r: [(v is None, str(v)) for v in r.values()])


def rows_by(rows: List[Dict[str, Any]], *, key: str, value: str) -> Dict[Any, Any]:
    return {r[key]: r[value] for r in rows}
