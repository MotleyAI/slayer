"""Shared fixtures for model filters as filtered datasets: the ``orders`` / ``returns`` / ``shipments`` tables and their models.

orders    (id, status, amount, ordered_at): 1 done 100 2024-01-10 | 2 done 200 2024-02-10 | 3 done 300 2024-03-10
                                            | 4 open 400 2024-02-20
returns   (id, order_id, reason):           10 1 damaged | 11 1 late | 12 2 damaged | 13 4 damaged
shipments (id, order_ref, carrier):         20 1 ups | 21 1 ups | 22 2 dhl | 23 3 ups  (order_ref not unique)
carriers  (id, name):                       1 ups | 2 dhl  (related to nothing)
"""

from __future__ import annotations

import contextlib
import os
from collections.abc import AsyncGenerator, Sequence
from typing import Any

import pytest

from slayer.core.enums import DataType, JoinType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.plan import plan_query
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.generator import SQLGenerator
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import seeded_exec_engine
from tests._pg_helpers import pg_database, pg_query_engine

DDL = (
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, status VARCHAR(10), amount INTEGER, ordered_at DATE)",
    "INSERT INTO orders VALUES (1, 'done', 100, '2024-01-10'), (2, 'done', 200, '2024-02-10'), "
    "(3, 'done', 300, '2024-03-10'), (4, 'open', 400, '2024-02-20')",
    "CREATE TABLE returns (id INTEGER PRIMARY KEY, order_id INTEGER, reason VARCHAR(10))",
    "INSERT INTO returns VALUES (10, 1, 'damaged'), (11, 1, 'late'), (12, 2, 'damaged'), (13, 4, 'damaged')",
    "CREATE TABLE shipments (id INTEGER PRIMARY KEY, order_ref INTEGER, carrier VARCHAR(10))",
    "INSERT INTO shipments VALUES (20, 1, 'ups'), (21, 1, 'ups'), (22, 2, 'dhl'), (23, 3, 'ups')",
    "CREATE TABLE carriers (id INTEGER PRIMARY KEY, name VARCHAR(10))",
    "INSERT INTO carriers VALUES (1, 'ups'), (2, 'dhl')",
)

BACKENDS = ("sqlite", "duckdb", "postgres")


def seed(dialect: str, db_path: str, statements: Sequence[str] = DDL) -> None:
    if dialect == "sqlite":
        with transaction(db_path) as con:
            for statement in statements:
                con.execute(statement)
        return
    duckdb = pytest.importorskip("duckdb")
    with contextlib.closing(duckdb.connect(db_path)) as con:
        for statement in statements:
            con.execute(statement)


def _id(**kw: Any) -> Column:
    return Column(name="id", type=DataType.INT, primary_key=True, **kw)


def _order_columns() -> list[Column]:
    return [
        _id(), Column(name="status", type=DataType.TEXT), Column(name="amount", type=DataType.INT),
        Column(name="ordered_at", type=DataType.DATE),
    ]


def _returns(name: str, *, joins: list[ModelJoin], extra: Sequence[Column] = (), **kw: Any) -> SlayerModel:
    return SlayerModel(
        name=name, data_source="test", sql_table="returns",
        columns=[_id(), Column(name="order_id", type=DataType.INT), Column(name="reason", type=DataType.TEXT), *extra],
        joins=joins, **kw,
    )


def _to(target: str, *, pairs: tuple[str, str] = ("order_id", "id"), **kw: Any) -> ModelJoin:
    return ModelJoin(target_model=target, join_pairs=[list(pairs)], **kw)


def _orders(name: str, **kw: Any) -> SlayerModel:
    return SlayerModel(name=name, data_source="test", sql_table="orders", columns=_order_columns(), **kw)


def table_models() -> list[SlayerModel]:
    """Every table- and SQL-backed model of the fixture."""
    to_shipments = [_to("shipments", pairs=("id", "order_ref"))]
    return [
        _orders("orders"),
        _orders("done_orders", filters=["status = 'done'"]),
        _orders("ups_orders", joins=to_shipments, filters=["shipments.carrier = 'ups'"]),
        _orders("ups_or_big_orders", joins=to_shipments, filters=["shipments.carrier = 'ups' or amount > 350"]),
        _orders("split_entry_orders", joins=to_shipments, filters=["shipments.id = 20", "shipments.id = 21"]),
        _orders("joint_entry_orders", joins=to_shipments, filters=["shipments.id = 20 and shipments.id = 21"]),
        SlayerModel(name="shipments", data_source="test", sql_table="shipments", columns=[
            _id(), Column(name="order_ref", type=DataType.INT), Column(name="carrier", type=DataType.TEXT),
        ]),
        SlayerModel(name="carriers", data_source="test", sql_table="carriers", columns=[
            _id(), Column(name="name", type=DataType.TEXT),
        ]),
        _returns("returns", joins=[_to("done_orders"), _to("ups_orders")],
                 extra=[Column(name="done_status", type=DataType.TEXT, sql="done_orders.status")]),
        _returns("late_returns", joins=[_to("orders")], filters=["reason = 'late'"]),
        _returns("returns_of_done", joins=[_to("done_orders")], filters=["done_orders.id is not null"]),
        _returns("returns_inner", joins=[_to("done_orders", join_type=JoinType.INNER)]),
        SlayerModel(name="done_orders_sql", data_source="test", sql="SELECT id, status FROM orders",
                    columns=[_id(), Column(name="status", type=DataType.TEXT)], filters=["status = 'done'"]),
        _returns("returns_sql", joins=[_to("done_orders_sql")]),
        _returns("returns_qb", joins=[_to("done_orders_qb")]),
    ]


def query_backed_model() -> SlayerModel:
    """``done_orders_qb``: a query-backed model over ``orders``, filtered to done orders."""
    return SlayerModel(
        name="done_orders_qb", data_source="test", filters=["status = 'done'"],
        source_queries=[SlayerQuery.model_validate({"source_model": "orders", "dimensions": ["id", "status"]})],
    )


@contextlib.asynccontextmanager
async def filtered_engine(backend: str, request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    """An engine over the fixture tables on ``backend`` holding every fixture model."""
    if backend == "postgres":
        info = request.getfixturevalue("postgresql_proc")
        base_dir = str(request.getfixturevalue("tmp_path") / "store")
        with pg_database(info, statements=DDL, name="test") as ds:
            async with pg_query_engine(ds, base_dir=base_dir, models=table_models(), validate=False) as engine:
                await engine.save_model(query_backed_model())
                yield engine
        return
    if backend == "duckdb":
        pytest.importorskip("duckdb")
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed(backend, p), models=table_models(),
    ) as (engine, _db):
        await engine.save_model(query_backed_model())
        yield engine


def seeded_file(backend: str, tmp_dir: str) -> str:
    """A seeded sqlite/duckdb file for raw-SQL execution."""
    path = os.path.join(tmp_dir, f"raw.{backend}")
    seed(backend, path)
    return path


def bundle(source: str, *, dialect: str, models: Sequence[SlayerModel] | None = None) -> ResolvedSourceBundle:
    by_name = {m.name: m for m in (models if models is not None else table_models())}
    return ResolvedSourceBundle(
        dialect=dialect, source_model=by_name[source],
        referenced_models=[m for n, m in by_name.items() if n != source],
    )


def render(query: dict[str, Any], *, dialect: str, force_association: bool = False) -> str:
    """The SQL ``query`` renders to on ``dialect`` straight from the planner (no engine-level gating);
    ``force_association`` renders every filter read as its association, never inlined."""
    q = SlayerQuery.model_validate(query)
    assert isinstance(q.source_model, str)
    b = bundle(q.source_model, dialect=dialect)
    gen = SQLGenerator(dialect=dialect, force_association_filters=True) if force_association else SQLGenerator(dialect=dialect)
    return gen.generate_from_planned(plan_query(query=q, bundle=b), bundle=b)


def ids(resp: SlayerResponse, source: str) -> set[int]:
    return {int(row[f"{source}.id"]) for row in resp.data}


def by_id(resp: SlayerResponse, source: str, column: str) -> dict[int, Any]:
    got = {int(row[f"{source}.id"]): row[f"{source}.{column}"] for row in resp.data}
    assert len(got) == len(resp.data), f"duplicate {source}.id rows: {resp.data}"
    return got
