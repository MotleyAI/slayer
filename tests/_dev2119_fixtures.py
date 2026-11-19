"""Shared fixtures for row-level expressions in every field position.

Underscore-prefixed so pytest skips collection. ``orders → customers`` is proven
many-to-one (``customers.id`` is a primary key); ``accounts`` (the customers table)
→ ``orders`` is declared one-to-many; ``orders → regions`` (``region = code``, ``code``
not a key) is unproven.

orders (id cust region city product status amount quantity price created_at shipped_at refunded_at created_txt):
   1  1  EU  Paris   A  paid     10  1  2  2025-01-05 10:00  2025-01-07 09:00  NULL              '2025-01-05'
   2  1  EU  Berlin  B  paid     20  2  3  2025-01-20 11:00  2025-02-02 08:00  2025-02-15 10:00  '2025-01-20'
   3  2  US  NYC     A  pending  30  3  4  2025-02-10 12:00  NULL              NULL              '2025-02-10'
   4  2  US  paris   B  paid     40  2  5  2025-02-25 09:00  2025-03-03 10:00  2025-03-10 09:00  '2025-02-25'
   5  3  EU  berlin  A  void     50  4  1  2025-03-15 08:00  2025-04-01 12:00  NULL              '2025-03-15'
   6  1  US  NYC     B  paid     60  1  6  2024-12-28 15:00  2025-03-20 10:00  NULL              '2024-12-28'
customers (id name region tier signup_ts updated_at discount spend):
   1 A North gold   2024-11-15 09:00  2025-01-10  0.1   100
   2 B South silver NULL              2025-02-20  NULL  200
   3 C North gold   2025-03-20 10:00  NULL        0.5   300
accounts: the customers rows (id signup_ts updated_at spend); ``orders`` cannot reach it.
balances (id customer_id balance updated_at created_at):
   1 1 100 2025-01-01  2025-04-01 | 2 1 150 NULL        2025-03-01 | 3 1 120 2025-02-01  2025-01-15
   4 2  50 2025-01-15  2025-01-10 | 5 2  70 NULL        2025-01-20 | 6 2  80 2025-01-18  2025-01-05
regions (id code manager): 1 EU Ann | 2 US Bob | 3 EU Cy

``EFF`` = ``coalesce(shipped_at, created_at)``; the derived field ``orders.eff`` holds it.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, Iterable
from datetime import date, datetime
from typing import Any, Optional

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import QueryMetric, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._engine_helpers import _engine_generate, seeded_exec_engine
from tests._time_points_fixtures import PinnedClock

BACKENDS = ["sqlite", "duckdb"]
TIER1 = ["sqlite", "postgres", "duckdb", "mysql", "clickhouse", "tsql", "trino", "snowflake", "bigquery"]
NOW = datetime(2026, 9, 29, 12, 0, 0)

EFF = "coalesce(shipped_at, created_at)"
EFF_NAME = "coalesce_shipped_at_created_at"

# --------------------------------------------------------------------------- #
# Tables
# --------------------------------------------------------------------------- #
ORDERS = TableSpec(
    name="orders",
    columns=[
        ("id", "INT"), ("customer_id", "INT"), ("region", "TEXT"), ("city", "TEXT"), ("product", "TEXT"),
        ("status", "TEXT"), ("amount", "DOUBLE"), ("quantity", "INT"), ("price", "DOUBLE"),
        ("created_at", "TIMESTAMP"), ("shipped_at", "TIMESTAMP"), ("refunded_at", "TIMESTAMP"),
        ("created_txt", "TEXT"),
    ],
    rows=[
        (1, 1, "EU", "Paris", "A", "paid", 10.0, 1, 2.0, "2025-01-05 10:00:00", "2025-01-07 09:00:00", None,
         "2025-01-05"),
        (2, 1, "EU", "Berlin", "B", "paid", 20.0, 2, 3.0, "2025-01-20 11:00:00", "2025-02-02 08:00:00",
         "2025-02-15 10:00:00", "2025-01-20"),
        (3, 2, "US", "NYC", "A", "pending", 30.0, 3, 4.0, "2025-02-10 12:00:00", None, None, "2025-02-10"),
        (4, 2, "US", "paris", "B", "paid", 40.0, 2, 5.0, "2025-02-25 09:00:00", "2025-03-03 10:00:00",
         "2025-03-10 09:00:00", "2025-02-25"),
        (5, 3, "EU", "berlin", "A", "void", 50.0, 4, 1.0, "2025-03-15 08:00:00", "2025-04-01 12:00:00", None,
         "2025-03-15"),
        (6, 1, "US", "NYC", "B", "paid", 60.0, 1, 6.0, "2024-12-28 15:00:00", "2025-03-20 10:00:00", None,
         "2024-12-28"),
    ],
)
CUSTOMERS = TableSpec(
    name="customers",
    columns=[
        ("id", "INT"), ("name", "TEXT"), ("region", "TEXT"), ("tier", "TEXT"), ("signup_ts", "TIMESTAMP"),
        ("updated_at", "TIMESTAMP"), ("discount", "DOUBLE"), ("spend", "DOUBLE"),
    ],
    rows=[
        (1, "A", "North", "gold", "2024-11-15 09:00:00", "2025-01-10 00:00:00", 0.1, 100.0),
        (2, "B", "South", "silver", None, "2025-02-20 00:00:00", None, 200.0),
        (3, "C", "North", "gold", "2025-03-20 10:00:00", None, 0.5, 300.0),
    ],
)
BALANCES = TableSpec(
    name="balances",
    columns=[("id", "INT"), ("customer_id", "INT"), ("balance", "DOUBLE"), ("updated_at", "TIMESTAMP"),
             ("created_at", "TIMESTAMP")],
    rows=[
        (1, 1, 100.0, "2025-01-01 00:00:00", "2025-04-01 00:00:00"),
        (2, 1, 150.0, None, "2025-03-01 00:00:00"),
        (3, 1, 120.0, "2025-02-01 00:00:00", "2025-01-15 00:00:00"),
        (4, 2, 50.0, "2025-01-15 00:00:00", "2025-01-10 00:00:00"),
        (5, 2, 70.0, None, "2025-01-20 00:00:00"),
        (6, 2, 80.0, "2025-01-18 00:00:00", "2025-01-05 00:00:00"),
    ],
)
REGIONS = TableSpec(
    name="regions",
    columns=[("id", "INT"), ("code", "TEXT"), ("manager", "TEXT")],
    rows=[(1, "EU", "Ann"), (2, "US", "Bob"), (3, "EU", "Cy")],
)
TABLES = [ORDERS, CUSTOMERS, BALANCES, REGIONS]

# --------------------------------------------------------------------------- #
# Oracles
# --------------------------------------------------------------------------- #
#: Per-order ``EFF`` (the coalesced timestamp) and ``amount``.
EFF_BY_ID = {
    1: "2025-01-07 09:00:00", 2: "2025-02-02 08:00:00", 3: "2025-02-10 12:00:00",
    4: "2025-03-03 10:00:00", 5: "2025-04-01 12:00:00", 6: "2025-03-20 10:00:00",
}
AMOUNT_BY_ID = {1: 10.0, 2: 20.0, 3: 30.0, 4: 40.0, 5: 50.0, 6: 60.0}
#: ``count(*)`` / ``sum(amount)`` per month of ``EFF``.
EFF_MONTH_COUNT = {"2025-01": 1, "2025-02": 2, "2025-03": 2, "2025-04": 1}
EFF_MONTH_SUM = {"2025-01": 10.0, "2025-02": 50.0, "2025-03": 100.0, "2025-04": 50.0}
#: ``count(*)`` per month of ``created_at`` (= of ``CAST(created_txt AS DATE)``).
CREATED_MONTH_COUNT = {"2024-12": 1, "2025-01": 2, "2025-02": 2, "2025-03": 1}


# --------------------------------------------------------------------------- #
# Datasets
# --------------------------------------------------------------------------- #
def orders_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="orders", sql_table="orders", data_source="test", default_time_dimension="created_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="region", datatype=DataType.STRING),
            DatasetField(name="city", datatype=DataType.STRING),
            DatasetField(name="product", datatype=DataType.STRING),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="quantity", datatype=DataType.INTEGER),
            DatasetField(name="price", datatype=DataType.FLOAT),
            DatasetField(name="created_at", datatype=DataType.DATETIME),
            DatasetField(name="shipped_at", datatype=DataType.DATETIME),
            DatasetField(name="refunded_at", datatype=DataType.DATETIME),
            DatasetField(name="created_txt", datatype=DataType.STRING),
            DatasetField(name="eff", datatype=DataType.DATETIME, sql="COALESCE(shipped_at, created_at)"),
        ],
        relationships=[
            DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
            DatasetRelationship(to="regions", from_columns=["region"], to_columns=["code"]),
        ],
    )


def customers_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="customers", sql_table="customers", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="name", datatype=DataType.STRING),
            DatasetField(name="region", datatype=DataType.STRING),
            DatasetField(name="tier", datatype=DataType.STRING),
            DatasetField(name="signup_ts", datatype=DataType.DATETIME),
            DatasetField(name="updated_at", datatype=DataType.DATETIME),
            DatasetField(name="discount", datatype=DataType.FLOAT),
            DatasetField(name="spend", datatype=DataType.FLOAT),
        ],
    )


def accounts_dataset() -> SlayerDataset:
    """The ``customers`` table with a declared one-to-many hop to ``orders``."""
    return SlayerDataset(
        name="accounts", sql_table="customers", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="signup_ts", datatype=DataType.DATETIME),
            DatasetField(name="updated_at", datatype=DataType.DATETIME),
            DatasetField(name="spend", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="orders", from_columns=["id"], to_columns=["customer_id"],
                         cardinality=RelationshipCardinality.ONE_TO_MANY)],
    )


def balances_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="balances", sql_table="balances", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="balance", datatype=DataType.FLOAT),
            DatasetField(name="updated_at", datatype=DataType.DATETIME),
            DatasetField(name="created_at", datatype=DataType.DATETIME),
        ],
    )


def regions_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="regions", sql_table="regions", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="code", datatype=DataType.STRING),
            DatasetField(name="manager", datatype=DataType.STRING),
        ],
    )


def monthly_dataset() -> SlayerDataset:
    """Query-backed: ``created_at`` cached at ``month``, ``refunded_at`` unbucketed."""
    return SlayerDataset(
        name="monthly", data_source="test",
        source_queries=[SlayerQuery.model_validate({
            "population": "orders", "dimensions": ["refunded_at"],
            "time_dimensions": [{"field": "created_at", "granularity": "month"}],
            "metrics": [{"metric": "sum(amount)", "alias": "rev"}],
        })],
    )


def dev2119_datasets() -> list[SlayerDataset]:
    return [orders_dataset(), customers_dataset(), accounts_dataset(), balances_dataset(), regions_dataset(), monthly_dataset()]


# --------------------------------------------------------------------------- #
# Engines
# --------------------------------------------------------------------------- #
@contextlib.asynccontextmanager
async def engine_for(
    backend: str, *, datasets: Optional[list[SlayerDataset]] = None, clock: Optional[PinnedClock] = None,
    datasource_fields: Optional[dict[str, Any]] = None,
) -> AsyncGenerator[SlayerQueryEngine]:
    """An executing engine over the seeded dataset on ``sqlite`` or ``duckdb``."""
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, TABLES),
        datasets=dev2119_datasets() if datasets is None else datasets,
        clock=clock if clock is not None else PinnedClock(NOW), datasource_fields=datasource_fields,
    ) as (engine, _):
        yield engine


async def emit(query: SlayerQuery | list[SlayerQuery | dict], *, dialect: str,
               datasets: Optional[Iterable[SlayerDataset]] = None) -> str:
    """Dry-run SQL over the dev2119 datasets on ``dialect`` (asserts it parses there)."""
    first, *rest = list(datasets) if datasets is not None else dev2119_datasets()
    return await _engine_generate(query=query, dataset=first, extra_datasets=rest, dialect=dialect)


# --------------------------------------------------------------------------- #
# Queries and readers
# --------------------------------------------------------------------------- #
def q(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"population": "orders", **kw})


def m(expression: str, name: str = "v") -> QueryMetric:
    return QueryMetric(metric=expression, alias=name)


def td(dimension: str, granularity: str = "month", **extra: Any) -> dict[str, Any]:
    return {"field": dimension, "granularity": granularity, **extra}


def num(value: Any) -> Optional[float]:
    return None if value is None else float(value)


def bucket(value: Any, *, width: int = 7) -> Optional[str]:
    """A bucket cell as ISO text cut to ``width`` (7 → ``YYYY-MM``, 4 → a year, 10 → a day)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        text = value.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(value, date):
        text = value.isoformat()
    else:
        text = str(value).replace("T", " ")
    return text[:width]


def column(resp: SlayerResponse, name: str) -> str:
    """The unique result key equal to ``name`` or ending in ``.name``."""
    hits = [k for k in resp.columns if k == name or k.endswith(f".{name}")]
    assert len(hits) == 1, (name, resp.columns)
    return hits[0]


def by_bucket(resp: SlayerResponse, key: str, value: str = "v", *, width: int = 7,
              by: Iterable[str] = ()) -> dict[Any, Optional[float]]:
    """``{bucket (or (by..., bucket)): value}``; asserts one row per key."""
    by = list(by)
    out: dict[Any, Optional[float]] = {}
    for row in resp.data:
        b = bucket(row[key], width=width)
        k = (*(row[d] for d in by), b) if by else b
        assert k not in out, (k, resp.data)
        out[k] = num(row[value])
    return out


def by_dims(resp: SlayerResponse, keys: Iterable[str], value: str = "orders.v") -> dict[tuple, Optional[float]]:
    keys = list(keys)
    out = {tuple(r[k] for k in keys): num(r[value]) for r in resp.data}
    assert len(out) == len(resp.data), resp.data
    return out
