"""DISTINCT aggregates and discrete / approximate percentiles over SQLite and DuckDB.

orders (id, status, region, qty, amount, is_paid, order_date):
   1  paid     EU    1     10    true   2024-01-05
   2  paid     EU    2     20    false  2024-02-05
   3  pending  US    2     30    true   2024-03-05
   4  pending  EU    1     40    false  2024-04-05
   5  shipped  US    3     60    true   2024-05-05
   6  void     NULL  NULL  NULL  NULL   2024-06-05
order_items (id, order_id, product): (1, 1, A) (2, 1, B) (3, 2, A) (4, 3, B) (5, 3, B) (6, 5, A)
points (id, x): 5, 5, 7, 9
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, Iterable
from typing import Any, Optional

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import Aggregation, DatasetField, DatasetMetric, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.bind_inputs import bind_query_inputs
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle
from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._engine_helpers import seeded_exec_engine

BACKENDS = ("sqlite", "duckdb")
NEW_BUILTINS = ("sum_distinct", "avg_distinct", "percentile_disc", "percentile_approx")

ORDERS = TableSpec(
    name="orders",
    columns=[("id", "INT"), ("status", "TEXT"), ("region", "TEXT"), ("qty", "INT"), ("amount", "DOUBLE"),
             ("is_paid", "BOOLEAN"), ("order_date", "DATE")],
    rows=[
        (1, "paid", "EU", 1, 10.0, True, "2024-01-05"),
        (2, "paid", "EU", 2, 20.0, False, "2024-02-05"),
        (3, "pending", "US", 2, 30.0, True, "2024-03-05"),
        (4, "pending", "EU", 1, 40.0, False, "2024-04-05"),
        (5, "shipped", "US", 3, 60.0, True, "2024-05-05"),
        (6, "void", None, None, None, None, "2024-06-05"),
    ],
)
ORDER_ITEMS = TableSpec(
    name="order_items", columns=[("id", "INT"), ("order_id", "INT"), ("product", "TEXT")],
    rows=[(1, 1, "A"), (2, 1, "B"), (3, 2, "A"), (4, 3, "B"), (5, 3, "B"), (6, 5, "A")],
)
POINTS = TableSpec(name="points", columns=[("id", "INT"), ("x", "INT")], rows=[(1, 5), (2, 5), (3, 7), (4, 9)])
TABLES = (ORDERS, ORDER_ITEMS, POINTS)


def orders_dataset(*, aggregations: Iterable[Aggregation] = (), metrics: Iterable[DatasetMetric] = ()) -> SlayerDataset:
    return SlayerDataset(
        name="orders", sql_table="orders", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="region", datatype=DataType.STRING),
            DatasetField(name="qty", datatype=DataType.INTEGER),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="is_paid", datatype=DataType.BOOLEAN),
            DatasetField(name="order_date", datatype=DataType.DATE),
        ],
        relationships=[DatasetRelationship(to="order_items", from_columns=["id"], to_columns=["order_id"],
                                           cardinality=RelationshipCardinality.ONE_TO_MANY)],
        aggregations=list(aggregations), metrics=list(metrics),
    )


def order_items_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="order_items", sql_table="order_items", data_source="test",
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="order_id", datatype=DataType.INTEGER),
                DatasetField(name="product", datatype=DataType.STRING)],
    )


def points_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="points", sql_table="points", data_source="test",
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="x", datatype=DataType.INTEGER)],
    )


def datasets(*, orders: Optional[SlayerDataset] = None) -> list[SlayerDataset]:
    return [orders or orders_dataset(), order_items_dataset(), points_dataset()]


@contextlib.asynccontextmanager
async def engine_for(backend: str, *, orders: Optional[SlayerDataset] = None) -> AsyncGenerator[SlayerQueryEngine]:
    """Executing engine over the seed; datasets saved unvalidated."""
    async with seeded_exec_engine(dialect=backend, seed=lambda p: seed_backend(backend, p, TABLES),
                                  datasets=datasets(orders=orders)) as (engine, _db):
        yield engine


def q(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"population": "orders", **kw})


def m(expression: str, name: str = "v") -> dict[str, str]:
    return {"metric": expression, "alias": name}


def num(value: Any) -> Optional[float]:
    return None if value is None else float(value)


def plan_type(expression: str, *, dataset: Optional[SlayerDataset] = None) -> Optional[DataType]:
    """The query plan's type of ``expression`` as the one metric of a query over ``dataset``."""
    model = dataset or orders_dataset()
    query = SlayerQuery.model_validate({"population": model.name, "metrics": [{"metric": expression, "alias": "x"}]})
    prebound = bind_query_inputs(query=query, bundle=ResolvedSourceBundle(dialect="postgres", population=model))
    (declared,) = [dm for dm in prebound.declared_metrics if dm.public_name == "x"]
    return declared.datatype
