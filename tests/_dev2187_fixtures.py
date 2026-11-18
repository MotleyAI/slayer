"""Seed, models and comparison helpers for the root-qualifier and one-reader tests."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator
from typing import Any, Optional

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, DatasetMetric, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests._dev1737_fixtures import TableSpec, exec_engine

BACKENDS = ("sqlite", "duckdb")

ORDERS = TableSpec(
    name="orders",
    columns=[
        ("id", "INT"), ("customer_id", "INT"), ("status", "TEXT"), ("note", "TEXT"),
        ("amount", "DOUBLE"), ("created_at", "DATE"), ("shipped_at", "DATE"),
    ],
    rows=[
        (1, 1, "paid", "see orders.x", 10.0, "2025-01-05", "2025-01-07"),
        (2, 1, "open", "see x", 20.0, "2025-01-20", None),
        (3, 2, "paid", "time_spine.x", 30.0, "2025-02-03", "2025-02-05"),
        (4, 2, "paid", "a", 40.0, "2025-03-09", "2025-03-10"),
    ],
)
CUSTOMERS = TableSpec(
    name="customers", columns=[("id", "INT"), ("name", "TEXT"), ("score", "DOUBLE")],
    rows=[(1, "ann", 1.0), (2, "bob", 2.0)],
)


def orders_columns() -> list[DatasetField]:
    return [
        DatasetField(name="id", datatype=DataType.INT, primary_key=True),
        DatasetField(name="customer_id", datatype=DataType.INT),
        DatasetField(name="status", datatype=DataType.TEXT),
        DatasetField(name="note", datatype=DataType.TEXT),
        DatasetField(name="amount", datatype=DataType.DOUBLE),
        DatasetField(name="created_at", datatype=DataType.DATE),
        DatasetField(name="shipped_at", datatype=DataType.DATE),
    ]


def orders_dataset(*, edge_name: Optional[str] = None) -> SlayerDataset:
    """``orders`` → ``customers`` (the edge optionally named), saved measure ``revenue``."""
    return SlayerDataset(
        name="orders", sql_table="orders", data_source="test", default_time_dimension="created_at",
        fields=orders_columns(),
        relationships=[DatasetRelationship(to="customers", name=edge_name, join_pairs=[["customer_id", "id"]])],
        metrics=[DatasetMetric(name="revenue", expression="sum(amount)")],
    )


def customers_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="customers", sql_table="customers", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="name", datatype=DataType.TEXT),
            DatasetField(name="score", datatype=DataType.DOUBLE),
        ],
        metrics=[DatasetMetric(name="aov", expression="avg(score)")],
    )


def qb_dataset() -> SlayerDataset:
    """Query-backed: ``tot`` per ``status``."""
    return SlayerDataset(name="qb", data_source="test", source_queries=[SlayerQuery.model_validate({
        "population": "orders", "dimensions": ["status"], "metrics": [{"expression": "sum(amount)", "name": "tot"}],
    })])


@contextlib.asynccontextmanager
async def orders_engine(backend: str, *, edge_name: Optional[str] = None) -> AsyncGenerator[SlayerQueryEngine]:
    async with exec_engine(
        backend, tables=[ORDERS, CUSTOMERS],
        datasets=[orders_dataset(edge_name=edge_name), customers_dataset(), qb_dataset()],
    ) as engine:
        yield engine


def canonical_rows(resp: SlayerResponse) -> list[tuple]:
    return sorted((tuple(str(r[c]) for c in resp.columns) for r in resp.data))


def warning_texts(resp: SlayerResponse) -> list[str]:
    return [w.human_message() for w in resp.warnings]


async def assert_same(engine: SlayerQueryEngine, left: Any, right: Any) -> SlayerResponse:
    """``left`` and ``right`` give one SQL, one key list, one row set and one warning list."""
    a = await engine.execute(left)
    b = await engine.execute(right)
    assert a.sql == b.sql
    assert a.columns == b.columns
    assert canonical_rows(a) == canonical_rows(b)
    assert warning_texts(a) == warning_texts(b)
    return a
