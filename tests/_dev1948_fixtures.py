"""Shared fixtures for multi-stage sibling-edge tests: a tiny orders → customers graph."""

from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import DatasetExtension, SlayerQuery
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


def _query(**kw) -> SlayerQuery:
    return SlayerQuery.model_validate(kw)


def customers_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="customers", data_source="test", sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="tier", datatype=DataType.STRING),
            DatasetField(name="spend", datatype=DataType.FLOAT),
        ],
    )


def orders_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="orders", data_source="test", sql_table="orders",
        default_time_dimension="ordered_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="ordered_at", datatype=DataType.DATETIME),
        ],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
    )


def dev1948_datasets() -> List[SlayerDataset]:
    return [orders_dataset(), customers_dataset()]


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
        dialect=dialect, seed=seed, datasets=dev1948_datasets(),
    ) as (engine, _db):
        yield engine


# --------------------------------------------------------------------------- #
# The chain list: x (customers) ← c (x) ← b (orders joined to c) ← root (b).
# --------------------------------------------------------------------------- #
def stage_x() -> SlayerQuery:
    return _query(
        name="x", population="customers", dimensions=["id", "tier"],
        metrics=[{"metric": "sum(spend)", "alias": "total_spend"}],
    )


def stage_c() -> SlayerQuery:
    """Grain ``{id}`` so a join on ``id`` is provably to-one; ``tr`` carries the tier."""
    return _query(
        name="c", population="x", dimensions=["id"],
        metrics=[{"metric": "sum(total_spend)", "alias": "cspend"},
                  {"metric": "max(tier)", "alias": "tr"}],
    )


def orders_joined(*targets: str) -> Dict[str, Any]:
    """Query kwargs: ``orders`` extended with a ``customer_id = <target>.id`` join per target."""
    return {"population": "orders", "extensions": [DatasetExtension.model_validate({
        "dataset": "orders",
        "relationships": [{"to": t, "from_columns": ["customer_id"], "to_columns": ["id"]} for t in targets],
    })]}


def stage_b() -> SlayerQuery:
    return _query(
        name="b", **orders_joined("c"), dimensions=["c.tr"],
        metrics=[{"metric": "sum(amount)", "alias": "amt"}],
    )


def root_over_b() -> SlayerQuery:
    return _query(
        population="b", dimensions=["c__tr"],
        metrics=[{"metric": "sum(amt)", "alias": "total"}],
    )


def chain_list() -> List[SlayerQuery]:
    return [stage_x(), stage_c(), stage_b(), root_over_b()]


def inline_orders(*, joins: List[DatasetRelationship]) -> SlayerDataset:
    """An inline model over the ``orders`` table carrying ``joins``."""
    return SlayerDataset(
        name="orders_inline", data_source="test", sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="amount", datatype=DataType.FLOAT),
        ],
        relationships=joins,
    )


def inline_join_list() -> List[SlayerQuery]:
    """``b`` reaches sibling ``c`` through an inline model's own joins."""
    b = _query(
        name="b",
        population=inline_orders(
            joins=[DatasetRelationship(to="c", from_columns=["customer_id"], to_columns=["id"])]),
        dimensions=["c.tr"],
        metrics=[{"metric": "sum(amount)", "alias": "amt"}],
    )
    return [stage_x(), stage_c(), b, root_over_b()]


def stage_o() -> SlayerQuery:
    """Per-customer order totals."""
    return _query(
        name="o", population="orders", dimensions=["customer_id"],
        metrics=[{"metric": "sum(amount)", "alias": "amt_c"}],
    )


def three_reads_list() -> List[SlayerQuery]:
    """``p`` sources sibling ``c`` and joins siblings ``x`` and ``o``."""
    p = _query(
        name="p",
        population="c",
        extensions=[DatasetExtension.model_validate({
            "dataset": "c",
            "relationships": [
                {"to": "x", "from_columns": ["id"], "to_columns": ["id"]},
                {"to": "o", "from_columns": ["id"], "to_columns": ["customer_id"]},
            ],
        })],
        dimensions=["id", "x.tier", "o.amt_c"],
    )
    root = _query(
        population="p", dimensions=["x__tier"],
        metrics=[{"metric": "sum(o__amt_c)", "alias": "total"}],
    )
    return [stage_x(), stage_c(), stage_o(), p, root]


def semi_join_list() -> List[SlayerQuery]:
    """``b`` filters orders by association with sibling ``c`` (an ``EXISTS`` hop)."""
    b = _query(
        name="b", **orders_joined("c"), dimensions=["status"],
        filter=["c.cspend > 50"],
        metrics=[{"metric": "sum(amount)", "alias": "amt"}],
    )
    root = _query(
        population="b", dimensions=["status"],
        metrics=[{"metric": "sum(amt)", "alias": "total"}],
    )
    return [stage_x(), stage_c(), b, root]


def producer_at_sibling_list() -> List[SlayerQuery]:
    """``s`` sources sibling ``c`` and carries a regroup producer over it."""
    s = _query(
        name="s", population="c", dimensions=["id", "tr"],
        metrics=[{"metric": "sum(cspend, partition_by=[tr])", "alias": "tier_total"}],
    )
    root = _query(
        population="s", dimensions=["tr"],
        metrics=[{"metric": "max(tier_total)", "alias": "total"}],
    )
    return [stage_x(), stage_c(), s, root]


def root_producer_list() -> List[SlayerQuery]:
    """The root itself carries a regroup producer over sibling ``c``."""
    root = _query(
        population="c", dimensions=["id", "tr"],
        metrics=[{"metric": "sum(cspend, partition_by=[tr])", "alias": "tier_total"}],
    )
    return [stage_x(), stage_c(), root]


def shared_producer_list() -> List[SlayerQuery]:
    """``s1`` and ``s2`` carry one structurally-equal producer; ``s2`` reuses ``s1``'s CTE."""
    def stage(name: str) -> SlayerQuery:
        return _query(
            name=name, population="orders", dimensions=["status", "customer_id"],
            metrics=[{"metric": "sum(amount, partition_by=[status])", "alias": "st"}],
        )
    root = _query(
        population="s2", dimensions=["status"],
        metrics=[{"metric": "max(st)", "alias": "total"}],
    )
    return [stage("s1"), stage("s2"), root]


def pop_check_list() -> List[SlayerQuery]:
    """``s1`` renders producer P, ``s2`` producer Q, ``s3`` reuses both."""
    p = {"expression": "sum(amount, partition_by=[status])", "name": "st"}
    q = {"expression": "sum(amount, partition_by=[customer_id])", "name": "ct"}

    def stage(name: str, measures: list) -> SlayerQuery:
        return _query(
            name=name, population="orders", dimensions=["status", "customer_id"],
            metrics=measures,
        )
    root = _query(
        population="s3", dimensions=["status"],
        metrics=[{"metric": "max(st)", "alias": "total"}],
    )
    return [stage("s1", [p]), stage("s2", [q]), stage("s3", [p, q]), root]


def stored_join_collision_list() -> List[SlayerQuery]:
    """A sibling named ``customers`` beside ``orders``' stored join to model ``customers``."""
    sibling = _query(
        name="customers", population="orders", dimensions=["status"],
        metrics=[{"metric": "sum(amount)", "alias": "a"}],
    )
    b = _query(
        name="b", population="orders", dimensions=["customers.tier"],
        metrics=[{"metric": "sum(amount)", "alias": "amt"}],
    )
    root = _query(
        population="b", dimensions=["customers__tier"],
        metrics=[{"metric": "sum(amt)", "alias": "total"}],
    )
    return [sibling, b, root]


def rows_by(rows: list, *, key: str, value: str) -> dict:
    return {r[key]: r[value] for r in rows}
