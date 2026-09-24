"""Shared fixtures for multi-stage sibling-edge tests: a tiny orders → customers graph."""

from __future__ import annotations

from typing import AsyncIterator, List

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import seeded_exec_engine

# (id, tier, spend)
_CUSTOMERS_ROWS = [
    (1, "gold", 100.0),
    (2, "silver", 150.0),
    (3, "gold", 60.0),
    (4, "bronze", 40.0),
    (5, "silver", 80.0),
]
# (id, customer_id, status, amount, ordered_at)
_ORDERS_ROWS = [
    (1, 1, "ok", 10.0, "2024-01-10"),
    (2, 1, "new", 20.0, "2024-01-20"),
    (3, 2, "ok", 30.0, "2024-02-10"),
    (4, 2, "new", 25.0, "2024-02-20"),
    (5, 3, "ok", 5.0, "2024-03-05"),
    (6, 4, "new", 40.0, "2024-03-10"),
    (7, 5, "ok", 15.0, "2024-04-02"),
    (8, None, "ok", 7.0, "2024-03-12"),
]

#: sum(orders.amount) by the ordering customer's tier; the orphan order is NULL.
AMOUNT_BY_TIER = {"gold": 35.0, "silver": 70.0, "bronze": 40.0, None: 7.0}
#: sum(orders.amount) by status.
AMOUNT_BY_STATUS = {"ok": 67.0, "new": 85.0}
#: AMOUNT_BY_TIER over customers only (inner-joined through a per-customer stage).
CUSTOMER_AMOUNT_BY_TIER = {"gold": 35.0, "silver": 70.0, "bronze": 40.0}
#: sum(customers.spend) by tier.
SPEND_BY_TIER = {"gold": 160.0, "silver": 230.0, "bronze": 40.0}
#: sum(orders.amount) by status over customers whose spend > 50 (customer 4 and the orphan drop).
BIG_SPENDER_AMOUNT_BY_STATUS = {"ok": 60.0, "new": 45.0}


def customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="tier", type=DataType.TEXT),
            Column(name="spend", type=DataType.DOUBLE),
        ],
    )


def orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        default_time_dimension="ordered_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


def dev1948_models() -> List[SlayerModel]:
    return [orders_model(), customers_model()]


def _seed_sqlite(db_path: str) -> None:
    with transaction(db_path) as con:
        cur = con.cursor()
        cur.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, tier TEXT, spend REAL)")
        cur.executemany("INSERT INTO customers VALUES (?,?,?)", _CUSTOMERS_ROWS)
        cur.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
            "status TEXT, amount REAL, ordered_at TEXT)")
        cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", _ORDERS_ROWS)


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    try:
        con.execute("CREATE TABLE customers (id INTEGER, tier VARCHAR, spend DOUBLE)")
        con.executemany("INSERT INTO customers VALUES (?,?,?)", _CUSTOMERS_ROWS)
        con.execute(
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, status VARCHAR, "
            "amount DOUBLE, ordered_at TIMESTAMP)")
        con.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", _ORDERS_ROWS)
    finally:
        con.close()


async def make_dev1948_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(
        dialect=dialect, seed=seed, models=dev1948_models(),
    ) as (engine, _db):
        yield engine


# --------------------------------------------------------------------------- #
# The chain list: x (customers) ← c (x) ← b (orders joined to c) ← root (b).
# --------------------------------------------------------------------------- #
def stage_x() -> SlayerQuery:
    return SlayerQuery(
        name="x", source_model="customers", dimensions=["id", "tier"],
        measures=[{"formula": "spend:sum", "name": "total_spend"}],
    )


def stage_c() -> SlayerQuery:
    return SlayerQuery(
        name="c", source_model="x", dimensions=["id", "tier"],
        measures=[{"formula": "total_spend:sum", "name": "cspend"}],
    )


def orders_joined(*targets: str) -> ModelExtension:
    """``orders`` extended with a ``customer_id = <target>.id`` join per target."""
    return ModelExtension.model_validate({
        "source_name": "orders",
        "joins": [{"target_model": t, "join_pairs": [["customer_id", "id"]]} for t in targets],
    })


def stage_b() -> SlayerQuery:
    return SlayerQuery(
        name="b", source_model=orders_joined("c"), dimensions=["c.tier"],
        measures=[{"formula": "amount:sum", "name": "amt"}],
    )


def root_over_b() -> SlayerQuery:
    return SlayerQuery(
        source_model="b", dimensions=["c__tier"],
        measures=[{"formula": "amt:sum", "name": "total"}],
    )


def chain_list() -> List[SlayerQuery]:
    return [stage_x(), stage_c(), stage_b(), root_over_b()]


def inline_orders(*, joins: List[ModelJoin]) -> SlayerModel:
    """An inline model over the ``orders`` table carrying ``joins``."""
    return SlayerModel(
        name="orders_inline", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
        ],
        joins=joins,
    )


def inline_join_list() -> List[SlayerQuery]:
    """``b`` reaches sibling ``c`` through an inline model's own joins."""
    b = SlayerQuery(
        name="b",
        source_model=inline_orders(
            joins=[ModelJoin(target_model="c", join_pairs=[["customer_id", "id"]])]),
        dimensions=["c.tier"],
        measures=[{"formula": "amount:sum", "name": "amt"}],
    )
    return [stage_x(), stage_c(), b, root_over_b()]


def stage_o() -> SlayerQuery:
    """Per-customer order totals."""
    return SlayerQuery(
        name="o", source_model="orders", dimensions=["customer_id"],
        measures=[{"formula": "amount:sum", "name": "amt_c"}],
    )


def three_reads_list() -> List[SlayerQuery]:
    """``p`` sources sibling ``c`` and joins siblings ``x`` and ``o``."""
    p = SlayerQuery(
        name="p",
        source_model=ModelExtension.model_validate({
            "source_name": "c",
            "joins": [
                {"target_model": "x", "join_pairs": [["id", "id"]]},
                {"target_model": "o", "join_pairs": [["id", "customer_id"]]},
            ],
        }),
        dimensions=["id", "x.tier", "o.amt_c"],
    )
    root = SlayerQuery(
        source_model="p", dimensions=["x__tier"],
        measures=[{"formula": "o__amt_c:sum", "name": "total"}],
    )
    return [stage_x(), stage_c(), stage_o(), p, root]


def semi_join_list() -> List[SlayerQuery]:
    """``b`` filters orders by association with sibling ``c`` (an ``EXISTS`` hop)."""
    b = SlayerQuery(
        name="b", source_model=orders_joined("c"), dimensions=["status"],
        filters=["c.cspend > 50"],
        measures=[{"formula": "amount:sum", "name": "amt"}],
    )
    root = SlayerQuery(
        source_model="b", dimensions=["status"],
        measures=[{"formula": "amt:sum", "name": "total"}],
    )
    return [stage_x(), stage_c(), b, root]


def producer_at_sibling_list() -> List[SlayerQuery]:
    """``s`` sources sibling ``c`` and carries a regroup producer over it."""
    s = SlayerQuery(
        name="s", source_model="c", dimensions=["id", "tier"],
        measures=[{"formula": "cspend:sum(partition_by=[tier])", "name": "tier_total"}],
    )
    root = SlayerQuery(
        source_model="s", dimensions=["tier"],
        measures=[{"formula": "tier_total:max", "name": "total"}],
    )
    return [stage_x(), stage_c(), s, root]


def root_producer_list() -> List[SlayerQuery]:
    """The root itself carries a regroup producer over sibling ``c``."""
    root = SlayerQuery(
        source_model="c", dimensions=["id", "tier"],
        measures=[{"formula": "cspend:sum(partition_by=[tier])", "name": "tier_total"}],
    )
    return [stage_x(), stage_c(), root]


def shared_producer_list() -> List[SlayerQuery]:
    """``s1`` and ``s2`` carry one structurally-equal producer; ``s2`` reuses ``s1``'s CTE."""
    def stage(name: str) -> SlayerQuery:
        return SlayerQuery(
            name=name, source_model="orders", dimensions=["status", "customer_id"],
            measures=[{"formula": "amount:sum(partition_by=[status])", "name": "st"}],
        )
    root = SlayerQuery(
        source_model="s2", dimensions=["status"],
        measures=[{"formula": "st:max", "name": "total"}],
    )
    return [stage("s1"), stage("s2"), root]


def pop_check_list() -> List[SlayerQuery]:
    """``s1`` renders producer P, ``s2`` producer Q, ``s3`` reuses both."""
    p = {"formula": "amount:sum(partition_by=[status])", "name": "st"}
    q = {"formula": "amount:sum(partition_by=[customer_id])", "name": "ct"}

    def stage(name: str, measures: list) -> SlayerQuery:
        return SlayerQuery(
            name=name, source_model="orders", dimensions=["status", "customer_id"],
            measures=measures,
        )
    root = SlayerQuery(
        source_model="s3", dimensions=["status"],
        measures=[{"formula": "st:max", "name": "total"}],
    )
    return [stage("s1", [p]), stage("s2", [q]), stage("s3", [p, q]), root]


def stored_join_collision_list() -> List[SlayerQuery]:
    """A sibling named ``customers`` beside ``orders``' stored join to model ``customers``."""
    sibling = SlayerQuery(
        name="customers", source_model="orders", dimensions=["status"],
        measures=[{"formula": "amount:sum", "name": "a"}],
    )
    b = SlayerQuery(
        name="b", source_model="orders", dimensions=["customers.tier"],
        measures=[{"formula": "amount:sum", "name": "amt"}],
    )
    root = SlayerQuery(
        source_model="b", dimensions=["customers__tier"],
        measures=[{"formula": "amt:sum", "name": "total"}],
    )
    return [sibling, b, root]


def nested_stage_n() -> SlayerQuery:
    """``n`` reads sibling ``c`` two ``source_queries`` levels down."""
    inner = SlayerModel(name="inner", source_queries=[SlayerQuery(
        source_model="c", dimensions=["tier"],
        measures=[{"formula": "cspend:sum", "name": "s"}])])
    outer = SlayerModel(name="outer", source_queries=[SlayerQuery(
        source_model=inner, dimensions=["tier"],
        measures=[{"formula": "s:sum", "name": "s2"}])])
    return SlayerQuery(
        name="n", source_model=outer, dimensions=["tier"],
        measures=[{"formula": "s2:sum", "name": "s3"}],
    )


def nested_list() -> List[SlayerQuery]:
    root = SlayerQuery(
        source_model="n", dimensions=["tier"],
        measures=[{"formula": "s3:sum", "name": "total"}],
    )
    return [stage_x(), stage_c(), nested_stage_n(), root]


def rows_by(rows: list, *, key: str, value: str) -> dict:
    return {r[key]: r[value] for r in rows}
