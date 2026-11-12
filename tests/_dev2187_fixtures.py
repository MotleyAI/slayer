"""Seed, models and comparison helpers for the root-qualifier and one-reader tests."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator
from typing import Any, Optional

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
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


def orders_columns() -> list[Column]:
    return [
        Column(name="id", type=DataType.INT, primary_key=True),
        Column(name="customer_id", type=DataType.INT),
        Column(name="status", type=DataType.TEXT),
        Column(name="note", type=DataType.TEXT),
        Column(name="amount", type=DataType.DOUBLE),
        Column(name="created_at", type=DataType.DATE),
        Column(name="shipped_at", type=DataType.DATE),
    ]


def orders_model(*, edge_name: Optional[str] = None) -> SlayerModel:
    """``orders`` → ``customers`` (the edge optionally named), saved measure ``revenue``."""
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test", default_time_dimension="created_at",
        columns=orders_columns(),
        joins=[ModelJoin(target_model="customers", name=edge_name, join_pairs=[["customer_id", "id"]])],
        measures=[ModelMeasure(name="revenue", formula="sum(amount)")],
    )


def customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="score", type=DataType.DOUBLE),
        ],
        measures=[ModelMeasure(name="aov", formula="avg(score)")],
    )


def qb_model() -> SlayerModel:
    """Query-backed: ``tot`` per ``status``."""
    return SlayerModel(name="qb", data_source="test", source_queries=[SlayerQuery.model_validate({
        "source_model": "orders", "dimensions": ["status"], "measures": [{"formula": "sum(amount)", "name": "tot"}],
    })])


@contextlib.asynccontextmanager
async def orders_engine(backend: str, *, edge_name: Optional[str] = None) -> AsyncGenerator[SlayerQueryEngine]:
    async with exec_engine(
        backend, tables=[ORDERS, CUSTOMERS],
        models=[orders_model(edge_name=edge_name), customers_model(), qb_model()],
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
