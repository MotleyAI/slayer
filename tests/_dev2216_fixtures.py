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

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    DatasetField,
    DatasetRelationship,
    SlayerDataset,
)
from slayer.core.query import QueryMetric, SlayerQuery
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


def customers_dataset(*, keyed: bool = False, unique: bool = False) -> SlayerDataset:
    """``keyed`` declares ``id`` the primary key; ``unique`` declares it a standalone unique column."""
    return SlayerDataset(
        name="customers", data_source="test", sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=keyed, unique=unique),
            DatasetField(name="region", datatype=DataType.STRING),
            DatasetField(name="spend", datatype=DataType.FLOAT),
        ],
        aggregations=[
            Aggregation(name="wsum", expression="SUM({value} * {weight})",
                        params=[AggregationParam(name="weight", sql="spend")]),
        ],
    )


def orders_dataset(*, cardinality: RelationshipCardinality | None = None) -> SlayerDataset:
    return SlayerDataset(
        name="orders", data_source="test", sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="amount", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"],
                         cardinality=cardinality)],
    )


def ossie_datasets(*, keyed: bool = False, unique: bool = False) -> list[SlayerDataset]:
    return [orders_dataset(), customers_dataset(keyed=keyed, unique=unique)]


def statements(*, duplicate: bool = False) -> list[str]:
    customers = [*_CUSTOMERS, _CUSTOMERS[2]] if duplicate else _CUSTOMERS
    return [
        "CREATE TABLE customers (id INTEGER, region VARCHAR(16), spend DOUBLE PRECISION)",
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, status VARCHAR(16), amount DOUBLE PRECISION)",
        *(f"INSERT INTO customers VALUES ({i}, '{r}', {s})" for i, r, s in customers),
        *(f"INSERT INTO orders VALUES ({i}, {c}, '{st}', {a})" for i, c, st, a in _ORDERS),
    ]


def seed_with(dialect: str, stmts: Sequence[str]):
    """A ``seeded_exec_engine`` seed running ``stmts`` on ``dialect``."""
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
    datasets: list[SlayerDataset] | None = None, duplicate: bool = False,
) -> AsyncGenerator[SlayerQueryEngine]:
    """An engine over the Ossie data on ``dialect`` (``postgres`` needs ``request`` for ``postgresql_proc``)."""
    datasets = datasets if datasets is not None else ossie_datasets()
    stmts = statements(duplicate=duplicate)
    if dialect == "postgres":
        assert request is not None
        info = request.getfixturevalue("postgresql_proc")
        with pg_database(info, statements=stmts, name="test") as ds:
            storage = YAMLStorage(base_dir=str(request.getfixturevalue("tmp_path") / "store"))
            async with owned_query_engine(storage=storage, datasources=[ds]) as engine:
                for model in datasets:
                    await storage.save_dataset(model, _validate=False)
                yield engine
        return
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    async with seeded_exec_engine(dialect=dialect, seed=seed_with(dialect, stmts), datasets=datasets) as (engine, _):
        yield engine


def query(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate(kw)


def metric(expression: str, name: str) -> QueryMetric:
    return QueryMetric(metric=expression, alias=name)


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
