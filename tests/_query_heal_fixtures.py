"""Seed, models and equivalence helpers for the clear-intent query heals (datasource ``test``).

orders (id, cust, region, status, amount, created_at, shipped_at):
  1  1  US    paid      10  2024-01-01 Mon  2024-01-03
  2  1  US    paid      20  2024-01-07 Sun  NULL
  3  2  EU    paid      30  2024-02-14 Wed  2024-02-20
  4  2  EU    refunded  40  2024-02-29 Thu  2024-03-02
  5  3  US    paid      50  2024-03-03 Sun  2024-03-05
  6  3  EU    paid      60  2024-03-16 Sat  NULL
  7  1  NULL  paid      70  2024-12-30 Mon  2025-01-02   (ISO year 2025, ISO week 1)
order_items: orders 1-5 and 7 have items, order 6 has none.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, Iterable, Sequence
from typing import Any, Optional

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import Aggregation, DatasetField, DatasetRelationship, DatasetMetric, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._engine_helpers import seeded_exec_engine

BACKENDS = ("sqlite", "duckdb")

CUSTOMERS = TableSpec(
    name="customers",
    columns=[("id", "INT"), ("name", "TEXT"), ("credit", "DOUBLE"), ("signup_at", "TIMESTAMP")],
    rows=[
        (1, "Ann", 100.0, "2023-11-02 09:00:00"),
        (2, "Bob", 50.0, "2024-01-15 12:00:00"),
        (3, "Cy", 25.0, "2024-02-20 08:00:00"),
    ],
)
ORDERS = TableSpec(
    name="orders",
    columns=[
        ("id", "INT"), ("customer_id", "INT"), ("region", "TEXT"), ("status", "TEXT"),
        ("amount", "DOUBLE"), ("created_at", "TIMESTAMP"), ("shipped_at", "TIMESTAMP"),
    ],
    rows=[
        (1, 1, "US", "paid", 10.0, "2024-01-01 10:00:00", "2024-01-03 10:00:00"),
        (2, 1, "US", "paid", 20.0, "2024-01-07 11:00:00", None),
        (3, 2, "EU", "paid", 30.0, "2024-02-14 12:00:00", "2024-02-20 12:00:00"),
        (4, 2, "EU", "refunded", 40.0, "2024-02-29 13:00:00", "2024-03-02 13:00:00"),
        (5, 3, "US", "paid", 50.0, "2024-03-03 14:00:00", "2024-03-05 14:00:00"),
        (6, 3, "EU", "paid", 60.0, "2024-03-16 15:00:00", None),
        (7, 1, None, "paid", 70.0, "2024-12-30 16:00:00", "2025-01-02 16:00:00"),
    ],
)
ORDER_ITEMS = TableSpec(
    name="order_items",
    columns=[
        ("id", "INT"), ("order_id", "INT"), ("product_id", "INT"), ("price", "DOUBLE"),
        ("qty", "DOUBLE"), ("created_at", "TIMESTAMP"),
    ],
    rows=[
        (1, 1, 1, 5.0, 1.0, "2024-01-01 10:00:00"),
        (2, 1, 2, 2.5, 2.0, "2024-01-01 10:05:00"),
        (3, 2, 1, 20.0, 1.0, "2024-01-07 11:00:00"),
        (4, 3, 2, 10.0, 3.0, "2024-02-14 12:00:00"),
        (5, 4, 3, 40.0, 1.0, "2024-02-29 13:00:00"),
        (6, 5, 1, 25.0, 2.0, "2024-03-03 14:00:00"),
        (7, 7, 3, 35.0, 2.0, "2024-12-31 09:00:00"),
    ],
)
PRODUCTS = TableSpec(
    name="products",
    columns=[("id", "INT"), ("category", "TEXT"), ("list_price", "DOUBLE")],
    rows=[(1, "toys", 5.0), (2, "books", 2.5), (3, "games", 40.0)],
)
TABLES = [CUSTOMERS, ORDERS, ORDER_ITEMS, PRODUCTS]

FISCAL_YEAR = {"name": "fiscal_year", "base": "year", "origin": "2000-04-01"}
DS_FIELDS = {"granularities": [FISCAL_YEAR]}


def customers_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="customers", sql_table="customers", data_source="test", default_time_dimension="signup_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="name", datatype=DataType.STRING),
            DatasetField(name="credit", datatype=DataType.FLOAT),
            DatasetField(name="signup_at", datatype=DataType.DATETIME),
        ],
    )


def orders_dataset(
    *, metrics: Sequence[DatasetMetric] = (), aggregations: Sequence[Aggregation] = (),
) -> SlayerDataset:
    return SlayerDataset(
        name="orders", sql_table="orders", data_source="test", default_time_dimension="created_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="region", datatype=DataType.STRING),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="created_at", datatype=DataType.DATETIME),
            DatasetField(name="shipped_at", datatype=DataType.DATETIME),
        ],
        metrics=list(metrics),
        aggregations=list(aggregations),
        relationships=[
            DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
            DatasetRelationship(
                to="order_items", from_columns=["id"], to_columns=["order_id"],
                cardinality=RelationshipCardinality.ONE_TO_MANY,
            ),
        ],
    )


def order_items_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="order_items", sql_table="order_items", data_source="test", default_time_dimension="created_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="order_id", datatype=DataType.INTEGER),
            DatasetField(name="product_id", datatype=DataType.INTEGER),
            DatasetField(name="price", datatype=DataType.FLOAT),
            DatasetField(name="qty", datatype=DataType.FLOAT),
            DatasetField(name="created_at", datatype=DataType.DATETIME),
        ],
        relationships=[DatasetRelationship(to="products", from_columns=["product_id"], to_columns=["id"])],
    )


def products_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="products", sql_table="products", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="category", datatype=DataType.STRING),
            DatasetField(name="list_price", datatype=DataType.FLOAT),
        ],
    )


def heal_datasets(*, orders: Optional[SlayerDataset] = None) -> list[SlayerDataset]:
    return [orders or orders_dataset(), customers_dataset(), order_items_dataset(), products_dataset()]


@contextlib.asynccontextmanager
async def heal_engine(
    backend: str, *, datasets: Optional[Iterable[SlayerDataset]] = None,
) -> AsyncGenerator[SlayerQueryEngine]:
    """Executing engine over the heal seed, with the ``fiscal_year`` custom granularity declared."""
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, TABLES),
        datasets=list(datasets) if datasets is not None else heal_datasets(), datasource_fields=DS_FIELDS,
    ) as (engine, _):
        yield engine


def table(resp: SlayerResponse, *, ordered: bool) -> list[tuple]:
    """Rows as tuples in column order; sorted unless the query's order is under test."""
    rows = [tuple(str(r[c]) for c in resp.columns) for r in resp.data]
    return rows if ordered else sorted(rows)


async def assert_equivalent(
    engine: SlayerQueryEngine, healed: Any, canonical: Any, *, ordered: bool = False, deprecated: bool = False,
    **execute_kw: Any,
) -> SlayerResponse:
    """``healed`` returns the same columns, rows, SQL and warnings as ``canonical`` (plus deprecations when
    ``deprecated``: a 1.x form)."""
    expected = await engine.execute(canonical, **execute_kw)
    actual = await engine.execute(healed, **execute_kw)
    assert actual.columns == expected.columns
    assert table(actual, ordered=ordered) == table(expected, ordered=ordered)
    assert actual.sql == expected.sql
    kept = [w for w in actual.warnings if not (deprecated and w.kind == "deprecated_input")]
    assert [w.human_message() for w in kept] == [w.human_message() for w in expected.warnings]
    return actual
