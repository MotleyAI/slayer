"""Shared fixtures for Mode B window functions (spec: queries/window-functions).

orders (id, region, amount, ordered_at, customer_id):
  1 EU 10 2024-01-01 1 | 2 EU 20 2024-01-02 1 | 3 EU 30 2024-01-03 2 | 4 US 40 2024-01-01 2
  5 US 50 2024-01-02 3 | 6 US 60 2024-01-03 3 | 7 AS 70 2024-01-01 1
customers (id, tier): 1 gold | 2 silver | 3 gold | 4 silver
scores (id, score, at): 1 10 2024-01-01 | 2 NULL 2024-01-02 | 3 5 NULL  (``at`` is column ``scored_at``)
ties (id, a, b): 1 NULL NULL | 2 NULL NULL | 3 1 NULL | 4 1 2
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, Sequence
from typing import Any

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from tests._dev1737_fixtures import TableSpec, seed_backend, server_table_statements
from tests._engine_helpers import seeded_exec_engine

BACKENDS = ("sqlite", "duckdb")

ORDERS = TableSpec(
    name="orders",
    columns=[("id", "INT"), ("region", "TEXT"), ("amount", "INT"), ("ordered_at", "DATE"), ("customer_id", "INT")],
    rows=[(1, "EU", 10, "2024-01-01", 1), (2, "EU", 20, "2024-01-02", 1), (3, "EU", 30, "2024-01-03", 2),
          (4, "US", 40, "2024-01-01", 2), (5, "US", 50, "2024-01-02", 3), (6, "US", 60, "2024-01-03", 3),
          (7, "AS", 70, "2024-01-01", 1)],
)
CUSTOMERS = TableSpec(name="customers", columns=[("id", "INT"), ("tier", "TEXT")],
                      rows=[(1, "gold"), (2, "silver"), (3, "gold"), (4, "silver")])
SCORES = TableSpec(name="scores", columns=[("id", "INT"), ("score", "INT"), ("scored_at", "DATE")],
                   rows=[(1, 10, "2024-01-01"), (2, None, "2024-01-02"), (3, 5, None)])
TIES = TableSpec(name="ties", columns=[("id", "INT"), ("a", "INT"), ("b", "INT")],
                 rows=[(1, None, None), (2, None, None), (3, 1, None), (4, 1, 2)])
TABLES = (ORDERS, CUSTOMERS, SCORES, TIES)

#: The spec's windowed column of ``orders``, as a field writes it and as a query writes it.
RN = "ROW_NUMBER() OVER (PARTITION BY region ORDER BY ordered_at)"
RN_INLINE = "ROW_NUMBER() OVER (PARTITION BY orders.region ORDER BY orders.ordered_at)"
RN_OF_ID = {1: 1, 2: 2, 3: 3, 4: 1, 5: 2, 6: 3, 7: 1}
AMOUNT_OF_ID = {i: i * 10 for i in range(1, 8)}
DAYS = ("2024-01-01", "2024-01-02", "2024-01-03")


def col(name: str, datatype: DataType = DataType.INTEGER, **kw: Any) -> DatasetField:
    return DatasetField(name=name, datatype=datatype, **kw)


def orders_dataset(*, name: str = "orders", data_source: str = "test", fields: Sequence[DatasetField] = (),
                   **kw: Any) -> SlayerDataset:
    kw.setdefault("sql_table", "orders")
    kw.setdefault("relationships", [DatasetRelationship(
        to="customers", from_columns=["customer_id"], to_columns=["id"],
        cardinality=RelationshipCardinality.MANY_TO_ONE)])
    return SlayerDataset(name=name, data_source=data_source, fields=[
        col("id", primary_key=True), col("region", DataType.STRING), col("amount"),
        col("ordered_at", DataType.DATE), col("customer_id"), *fields], **kw)


def customers_dataset(*, data_source: str = "test", fields: Sequence[DatasetField] = ()) -> SlayerDataset:
    return SlayerDataset(name="customers", data_source=data_source, sql_table="customers",
                         fields=[col("id", primary_key=True), col("tier", DataType.STRING), *fields])


def scores_dataset(*, data_source: str = "test", fields: Sequence[DatasetField] = ()) -> SlayerDataset:
    return SlayerDataset(name="scores", data_source=data_source, sql_table="scores", fields=[
        col("id"), col("score"), col("at", DataType.DATE, expression="scored_at"), *fields])


def ties_dataset(*, data_source: str = "test") -> SlayerDataset:
    return SlayerDataset(name="ties", data_source=data_source, sql_table="ties",
                         fields=[col("id", primary_key=True), col("a"), col("b")])


def window_datasets(*, data_source: str = "test") -> list[SlayerDataset]:
    return [orders_dataset(data_source=data_source), customers_dataset(data_source=data_source),
            scores_dataset(data_source=data_source), ties_dataset(data_source=data_source)]


def server_statements(backend: str) -> list[str]:
    """Every statement seeding the four tables on a server backend."""
    return [s for table in TABLES for s in server_table_statements(table, backend=backend)]


@contextlib.asynccontextmanager
async def window_engine(backend: str, *, datasets: Sequence[SlayerDataset] | None = None,
                        tables: Sequence[TableSpec] = TABLES, **kw: Any) -> AsyncGenerator[SlayerQueryEngine]:
    """An executing SQLite or DuckDB engine over the seed; datasets are saved unvalidated."""
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, tables),
        datasets=list(window_datasets() if datasets is None else datasets), **kw,
    ) as (engine, _):
        yield engine


async def run(engine: SlayerQueryEngine, body: dict[str, Any], **kw: Any) -> SlayerResponse:
    return await engine.execute(SlayerQuery.model_validate(body), **kw)


def raw(population: str, *expressions: str, key: str = "id", **extra: Any) -> dict[str, Any]:
    """Raw rows of ``population``: its ``key`` plus each expression, aliased ``v0``, ``v1``, …"""
    return {"population": population, "distinct_dimension_values": False,
            "dimensions": [{"field": f"{population}.{key}", "alias": "id"},
                           *({"field": e, "alias": f"v{i}"} for i, e in enumerate(expressions))], **extra}


def grouped(population: str, dimension: str, metric: str, **extra: Any) -> dict[str, Any]:
    return {"population": population, "dimensions": [{"field": dimension, "alias": "d"}],
            "metrics": [{"metric": metric, "alias": "m"}], **extra}


def by(resp: SlayerResponse, key: str = "id", value: str = "v0") -> dict[Any, Any]:
    got = {row[key]: row[value] for row in resp.data}
    assert len(got) == len(resp.data), f"duplicate {key} rows: {resp.data}"
    return got


def ints(values: dict[Any, Any]) -> dict[Any, Any]:
    """Keys and values as ints, NULL kept."""
    return {(k if k is None or isinstance(k, str) else int(k)): (None if v is None else int(v))
            for k, v in values.items()}


def floats(values: dict[Any, Any]) -> dict[Any, Any]:
    return {(k if k is None or isinstance(k, str) else int(k)): (None if v is None else round(float(v), 6))
            for k, v in values.items()}


def day(value: Any) -> str | None:
    return None if value is None else str(value)[:10]


async def column(engine: SlayerQueryEngine, population: str, expression: str, **extra: Any) -> dict[int, Any]:
    """``expression`` per row id of ``population``, as a raw-row query returns it."""
    return ints_keys(by(await run(engine, raw(population, expression, **extra))))


def ints_keys(values: dict[Any, Any]) -> dict[int, Any]:
    return {int(k): v for k, v in values.items()}
