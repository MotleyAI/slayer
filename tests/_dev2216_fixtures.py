"""Ossie Appendix B.1 data over a keyless ``customers`` (no primary or unique key) and an
``orders → customers`` join with no declared cardinality, seeded into SQLite, DuckDB or Postgres.

customers (id, region, spend):  C1 EAST 10 | C2 EAST 20 | C3 WEST 30 | C4 NORTH 40 (no orders)
orders (id, customer_id, status, amount):
    O1 C1 completed 100 | O2 C1 completed 200 | O3 C2 pending 50 | O4 C3 completed 300
``duplicate=True`` stores C3's row twice (two identical physical rows).
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import Any

import pytest

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import owned_query_engine, seeded_exec_engine
from tests._pg_helpers import pg_database

_CUSTOMERS = [(1, "EAST", 10.0), (2, "EAST", 20.0), (3, "WEST", 30.0), (4, "NORTH", 40.0)]
_ORDERS = [
    (1, 1, "completed", 100.0), (2, 1, "completed", 200.0),
    (3, 2, "pending", 50.0), (4, 3, "completed", 300.0),
]

#: sum(orders.amount) by customers.region — association (Ossie's numbers) and broadcast.
AMOUNT_BY_REGION = {"EAST": 350.0, "WEST": 300.0, "NORTH": None}
AMOUNT_BY_REGION_ROOTED = {"EAST": 350.0, "WEST": 300.0}
AMOUNT_TOTAL = 650.0
#: orders-rooted, by status, over the distinct customers reaching each status.
SPEND_BY_STATUS = {"completed": 40.0, "pending": 20.0}
WSUM_BY_STATUS = {"completed": 10.0 ** 2 + 30.0 ** 2, "pending": 20.0 ** 2}
WAVG_BY_STATUS = {"completed": (10.0 ** 2 + 30.0 ** 2) / 40.0, "pending": 20.0}
#: count(customers.*) by status with C3 stored twice: C1 once (two orders), C3 twice.
DUP_COUNT_BY_STATUS = {"completed": 3, "pending": 1}
#: customers-rooted sum(spend) by orders.status under ``orders.amount >= 50``: C1 once despite O1 and O2.
FILTERED_SPEND_BY_STATUS = {"completed": 40.0, "pending": 20.0}


def customers_model(*, keyed: bool = False) -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=keyed),
            Column(name="region", type=DataType.TEXT),
            Column(name="spend", type=DataType.DOUBLE),
        ],
        aggregations=[
            Aggregation(name="wsum", formula="SUM({value} * {weight})",
                        params=[AggregationParam(name="weight", sql="spend")]),
        ],
    )


def orders_model(*, cardinality: JoinCardinality | None = None) -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]],
                         cardinality=cardinality)],
    )


def ossie_models(*, keyed: bool = False) -> list[SlayerModel]:
    return [orders_model(), customers_model(keyed=keyed)]


def statements(*, duplicate: bool = False) -> list[str]:
    customers = [*_CUSTOMERS, _CUSTOMERS[2]] if duplicate else _CUSTOMERS
    return [
        "CREATE TABLE customers (id INTEGER, region VARCHAR(16), spend DOUBLE PRECISION)",
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, status VARCHAR(16), amount DOUBLE PRECISION)",
        *(f"INSERT INTO customers VALUES ({i}, '{r}', {s})" for i, r, s in customers),
        *(f"INSERT INTO orders VALUES ({i}, {c}, '{st}', {a})" for i, c, st, a in _ORDERS),
    ]


def _seed(dialect: str, stmts: Sequence[str]):
    def seed(db_path: str) -> None:
        if dialect == "duckdb":
            con = pytest.importorskip("duckdb").connect(db_path)
            try:
                for stmt in stmts:
                    con.execute(stmt)
            finally:
                con.close()
            return
        with transaction(db_path) as con:
            for stmt in stmts:
                con.execute(stmt)
    return seed


@asynccontextmanager
async def ossie_engine(
    dialect: str, *, request: pytest.FixtureRequest | None = None,
    models: list[SlayerModel] | None = None, duplicate: bool = False,
) -> AsyncGenerator[SlayerQueryEngine]:
    """An engine over the Ossie data on ``dialect`` (``postgres`` needs ``request`` for ``postgresql_proc``)."""
    models = models if models is not None else ossie_models()
    stmts = statements(duplicate=duplicate)
    if dialect == "postgres":
        assert request is not None
        info = request.getfixturevalue("postgresql_proc")
        with pg_database(info, statements=stmts, name="test") as ds:
            storage = YAMLStorage(base_dir=str(request.getfixturevalue("tmp_path") / "store"))
            async with owned_query_engine(storage=storage, datasources=[ds]) as engine:
                for model in models:
                    await storage.save_model(model, _validate=False)
                yield engine
        return
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    async with seeded_exec_engine(dialect=dialect, seed=_seed(dialect, stmts), models=models) as (engine, _):
        yield engine


def query(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate(kw)


def measure(formula: str, name: str) -> ModelMeasure:
    return ModelMeasure(formula=formula, name=name)


def cells(resp: SlayerResponse, dim: str, value: str) -> dict:
    """``{dimension value: measure value}`` keyed on the result column ending in ``dim``."""
    out: dict = {}
    for row in resp.data:
        d = next(k for k in row if k.endswith(dim))
        v = next(k for k in row if k.endswith(value))
        out[row[d]] = None if row[v] is None else float(row[v])
    return out


def kinds(resp: SlayerResponse, kind: str) -> list:
    return [w for w in resp.warnings or [] if getattr(w, "kind", None) == kind]
