"""Shared fixtures for dataset filters as filtered datasets: the ``orders`` / ``returns`` / ``shipments`` tables and their datasets.

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
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
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


def _id(**kw: Any) -> DatasetField:
    return DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True, **kw)


def _order_fields() -> list[DatasetField]:
    return [
        _id(), DatasetField(name="status", datatype=DataType.STRING), DatasetField(name="amount", datatype=DataType.INTEGER),
        DatasetField(name="ordered_at", datatype=DataType.DATE),
    ]


def _returns(name: str, *, relationships: list[DatasetRelationship], extra: Sequence[DatasetField] = (), **kw: Any) -> SlayerDataset:
    return SlayerDataset(
        name=name, data_source="test", sql_table="returns",
        fields=[_id(), DatasetField(name="order_id", datatype=DataType.INTEGER), DatasetField(name="reason", datatype=DataType.STRING), *extra],
        relationships=relationships, **kw,
    )


def _to(target: str, *, pairs: tuple[str, str] = ("order_id", "id"), **kw: Any) -> DatasetRelationship:
    return DatasetRelationship(to=target, from_columns=[pairs[0]], to_columns=[pairs[1]], **kw)


def _orders(name: str, **kw: Any) -> SlayerDataset:
    return SlayerDataset(name=name, data_source="test", sql_table="orders", fields=_order_fields(), **kw)


def table_datasets() -> list[SlayerDataset]:
    """Every table- and SQL-backed dataset of the fixture."""
    to_shipments = [_to("shipments", pairs=("id", "order_ref"))]
    return [
        _orders("orders"),
        _orders("done_orders", filters=["status = 'done'"]),
        _orders("ups_orders", relationships=to_shipments, filters=["shipments.carrier = 'ups'"]),
        _orders("ups_or_big_orders", relationships=to_shipments, filters=["shipments.carrier = 'ups' or amount > 350"]),
        _orders("split_entry_orders", relationships=to_shipments, filters=["shipments.id = 20", "shipments.id = 21"]),
        _orders("joint_entry_orders", relationships=to_shipments, filters=["shipments.id = 20 and shipments.id = 21"]),
        SlayerDataset(name="shipments", data_source="test", sql_table="shipments", fields=[
            _id(), DatasetField(name="order_ref", datatype=DataType.INTEGER), DatasetField(name="carrier", datatype=DataType.STRING),
        ]),
        SlayerDataset(name="carriers", data_source="test", sql_table="carriers", fields=[
            _id(), DatasetField(name="name", datatype=DataType.STRING),
        ]),
        _returns("returns", relationships=[_to("done_orders"), _to("ups_orders")],
                 extra=[DatasetField(name="done_status", datatype=DataType.STRING, sql="done_orders.status")]),
        _returns("late_returns", relationships=[_to("orders")], filters=["reason = 'late'"]),
        _returns("returns_of_done", relationships=[_to("done_orders")], filters=["done_orders.id is not null"]),
        _returns("returns_inner", relationships=[_to("done_orders", join_type=JoinType.INNER)]),
        SlayerDataset(name="done_orders_sql", data_source="test", sql="SELECT id, status FROM orders",
                    fields=[_id(), DatasetField(name="status", datatype=DataType.STRING)], filters=["status = 'done'"]),
        _returns("returns_sql", relationships=[_to("done_orders_sql")]),
        _returns("returns_qb", relationships=[_to("done_orders_qb")]),
    ]


def query_backed_dataset() -> SlayerDataset:
    """``done_orders_qb``: a query-backed dataset over ``orders``, filtered to done orders."""
    return SlayerDataset(
        name="done_orders_qb", data_source="test", filters=["status = 'done'"],
        source_queries=[SlayerQuery.model_validate({"population": "orders", "dimensions": ["orders.id", "orders.status"]})],
    )


@contextlib.asynccontextmanager
async def filtered_engine(backend: str, request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    """An engine over the fixture tables on ``backend`` holding every fixture dataset."""
    if backend == "postgres":
        info = request.getfixturevalue("postgresql_proc")
        base_dir = str(request.getfixturevalue("tmp_path") / "store")
        with pg_database(info, statements=DDL, name="test") as ds:
            async with pg_query_engine(ds, base_dir=base_dir, datasets=table_datasets(), validate=False) as engine:
                await engine.save_dataset(query_backed_dataset())
                yield engine
        return
    if backend == "duckdb":
        pytest.importorskip("duckdb")
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed(backend, p), datasets=table_datasets(),
    ) as (engine, _db):
        await engine.save_dataset(query_backed_dataset())
        yield engine


def seeded_file(backend: str, tmp_dir: str) -> str:
    """A seeded sqlite/duckdb file for raw-SQL execution."""
    path = os.path.join(tmp_dir, f"raw.{backend}")
    seed(backend, path)
    return path


def bundle(source: str, *, dialect: str, datasets: Sequence[SlayerDataset] | None = None) -> ResolvedSourceBundle:
    by_name = {m.name: m for m in (datasets if datasets is not None else table_datasets())}
    return ResolvedSourceBundle(
        dialect=dialect, population=by_name[source],
        referenced_datasets=[m for n, m in by_name.items() if n != source],
    )


def render(query: dict[str, Any], *, dialect: str, force_association: bool = False) -> str:
    """The SQL ``query`` renders to on ``dialect`` straight from the planner (no engine-level gating);
    ``force_association`` renders every filter read as its association, never inlined."""
    q = SlayerQuery.model_validate(query)
    assert isinstance(q.population, str)
    b = bundle(q.population, dialect=dialect)
    gen = SQLGenerator(dialect=dialect, force_association_filters=True) if force_association else SQLGenerator(dialect=dialect)
    return gen.generate_from_planned(plan_query(query=q, bundle=b), bundle=b)


def ids(resp: SlayerResponse, source: str) -> set[int]:
    return {int(row[f"{source}.id"]) for row in resp.data}


def by_id(resp: SlayerResponse, source: str, column: str) -> dict[int, Any]:
    key = column if "." in column else f"{source}.{column}"
    got = {int(row[f"{source}.id"]): row[key] for row in resp.data}
    assert len(got) == len(resp.data), f"duplicate {source}.id rows: {resp.data}"
    return got
