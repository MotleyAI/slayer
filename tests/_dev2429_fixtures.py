"""The moved-frame seed (queries/transforms, "time_shift evaluates its operand in the moved frame").

North ``sum(amount)`` by month: 2023-12 20, 2024-01 30 (dated 01-10), 2024-02 40, 2024-04 30, 2024-05 5,
2024-06 7, no March. East carries the 2024/2025 rows of the year-over-year scenario. Each row ships two
days after it is ordered.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator
from typing import Any, Optional

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._engine_helpers import _engine_generate, seeded_exec_engine

BACKENDS = ("sqlite", "duckdb")
FEB_MAY = ["2024-02-01", "2024-05-31"]
JAN_APR = ["2024-01-01", "2024-04-30"]

AMOUNT = "sum(orders.amount)"
#: Equal to ``AMOUNT`` in every cell.
REWRITE = "sum(orders.amount) + 0 * cumsum(sum(orders.amount))"

CUSTOMERS = TableSpec(name="customers", columns=[("id", "INT"), ("spend", "DOUBLE")],
                      rows=[(1, 100.0), (2, 50.0)])
ORDERS = TableSpec(
    name="orders",
    columns=[("id", "INT"), ("region", "TEXT"), ("customer_id", "INT"), ("ordered_at", "DATE"),
             ("shipped_at", "DATE"), ("amount", "DOUBLE")],
    rows=[
        (1, "North", 1, "2023-12-15", "2023-12-18", 20.0),
        (2, "North", 1, "2024-01-10", "2024-01-12", 30.0),
        (3, "North", 2, "2024-02-12", "2024-02-14", 40.0),
        (4, "North", 1, "2024-04-08", "2024-04-10", 30.0),
        (5, "North", 2, "2024-05-20", "2024-05-22", 5.0),
        (6, "North", 1, "2024-06-03", "2024-06-05", 7.0),
        (7, "East", 2, "2024-02-05", "2024-02-07", 3.0),
        (8, "East", 2, "2024-03-14", "2024-03-16", 10.0),
        (9, "East", 1, "2025-02-11", "2025-02-13", 6.0),
        (10, "East", 1, "2025-03-09", "2025-03-11", 8.0),
    ],
)


def datasets() -> list[SlayerDataset]:
    return [
        SlayerDataset(
            name="orders", sql_table="orders", data_source="test", default_time_dimension="ordered_at",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="region", datatype=DataType.STRING),
                DatasetField(name="customer_id", datatype=DataType.INTEGER),
                DatasetField(name="ordered_at", datatype=DataType.DATE),
                DatasetField(name="shipped_at", datatype=DataType.DATE),
                DatasetField(name="amount", datatype=DataType.FLOAT),
            ],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        ),
        SlayerDataset(
            name="customers", sql_table="customers", data_source="test",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="spend", datatype=DataType.FLOAT),
            ],
        ),
    ]


@contextlib.asynccontextmanager
async def moved_frame_engine(backend: str) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, [CUSTOMERS, ORDERS]), datasets=datasets(),
    ) as (engine, _db):
        yield engine


def query(*metrics: str, date_range: Any = FEB_MAY, spine: bool = False, granularity: str = "month",
          extra_time_dimensions: tuple[dict[str, Any], ...] = (), **extra: Any) -> SlayerQuery:
    """``metrics`` aliased ``m0``, ``m1``, … by ``orders.region`` and the month (or spine month)."""
    td: dict[str, Any] = {"field": "time_spine.timestamp" if spine else "orders.ordered_at",
                          "granularity": granularity}
    if date_range is not None:
        td["date_range"] = date_range
    if extra_time_dimensions:
        extra["main_time_dimension"] = td["field"]
    return SlayerQuery.model_validate({
        "population": "orders", "dimensions": ["orders.region"], "time_dimensions": [td, *extra_time_dimensions],
        "metrics": [{"metric": m, "alias": f"m{i}"} for i, m in enumerate(metrics)], **extra,
    })


def _key(row: dict[str, Any], tail: str) -> str:
    hits = [k for k in row if k == tail or k.endswith(f".{tail}")]
    assert len(hits) == 1, (tail, list(row))
    return hits[0]


def _month(row: dict[str, Any]) -> str:
    key = next(k for k in row if k.endswith((".ordered_at", "time_spine.timestamp")))
    return str(row[key])[:7]


def num(v: Any) -> Optional[float]:
    return None if v is None else float(v)


def cells(resp: SlayerResponse, alias: str = "m0", *, region: str = "North") -> dict[str, Optional[float]]:
    """``{YYYY-MM: value}`` of ``alias`` for ``region``'s rows; asserts one row per month."""
    out: dict[str, Optional[float]] = {}
    for row in resp.data:
        if row[_key(row, "region")] != region:
            continue
        month = _month(row)
        assert month not in out, month
        out[month] = num(row[_key(row, alias)])
    return out


def months(resp: SlayerResponse, *, region: str = "North") -> list[str]:
    """``region``'s months in result order."""
    return [_month(r) for r in resp.data if r[_key(r, "region")] == region]


async def generate(q: SlayerQuery, *, dialect: str) -> str:
    models = datasets()
    return await _engine_generate(query=q, dataset=models[0], extra_datasets=models[1:], dialect=dialect,
                                  validate=False)
