"""Shared fixtures for row-level expressions in every column position.

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

``EFF`` = ``coalesce(shipped_at, created_at)``; the derived column ``orders.eff`` holds it.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, Iterable
from datetime import date, datetime
from typing import Any, Optional

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
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
# Models
# --------------------------------------------------------------------------- #
def orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test", default_time_dimension="created_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="region", type=DataType.TEXT),
            Column(name="city", type=DataType.TEXT),
            Column(name="product", type=DataType.TEXT),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="quantity", type=DataType.INT),
            Column(name="price", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="shipped_at", type=DataType.TIMESTAMP),
            Column(name="refunded_at", type=DataType.TIMESTAMP),
            Column(name="created_txt", type=DataType.TEXT),
            Column(name="eff", type=DataType.TIMESTAMP, sql="COALESCE(shipped_at, created_at)"),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ModelJoin(target_model="regions", join_pairs=[["region", "code"]]),
        ],
    )


def customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="region", type=DataType.TEXT),
            Column(name="tier", type=DataType.TEXT),
            Column(name="signup_ts", type=DataType.TIMESTAMP),
            Column(name="updated_at", type=DataType.TIMESTAMP),
            Column(name="discount", type=DataType.DOUBLE),
            Column(name="spend", type=DataType.DOUBLE),
        ],
    )


def accounts_model() -> SlayerModel:
    """The ``customers`` table with a declared one-to-many hop to ``orders``."""
    return SlayerModel(
        name="accounts", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="signup_ts", type=DataType.TIMESTAMP),
            Column(name="updated_at", type=DataType.TIMESTAMP),
            Column(name="spend", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="orders", join_pairs=[["id", "customer_id"]],
                         cardinality=JoinCardinality.ONE_TO_MANY)],
    )


def balances_model() -> SlayerModel:
    return SlayerModel(
        name="balances", sql_table="balances", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="balance", type=DataType.DOUBLE),
            Column(name="updated_at", type=DataType.TIMESTAMP),
            Column(name="created_at", type=DataType.TIMESTAMP),
        ],
    )


def regions_model() -> SlayerModel:
    return SlayerModel(
        name="regions", sql_table="regions", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="code", type=DataType.TEXT),
            Column(name="manager", type=DataType.TEXT),
        ],
    )


def monthly_model() -> SlayerModel:
    """Query-backed: ``created_at`` cached at ``month``, ``refunded_at`` unbucketed."""
    return SlayerModel(
        name="monthly", data_source="test",
        source_queries=[SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["refunded_at"],
            "time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
            "measures": [{"formula": "sum(amount)", "name": "rev"}],
        })],
    )


def dev2119_models() -> list[SlayerModel]:
    return [orders_model(), customers_model(), accounts_model(), balances_model(), regions_model(), monthly_model()]


# --------------------------------------------------------------------------- #
# Engines
# --------------------------------------------------------------------------- #
@contextlib.asynccontextmanager
async def engine_for(
    backend: str, *, models: Optional[list[SlayerModel]] = None, clock: Optional[PinnedClock] = None,
    datasource_fields: Optional[dict[str, Any]] = None,
) -> AsyncGenerator[SlayerQueryEngine]:
    """An executing engine over the seeded dataset on ``sqlite`` or ``duckdb``."""
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, TABLES),
        models=dev2119_models() if models is None else models,
        clock=clock if clock is not None else PinnedClock(NOW), datasource_fields=datasource_fields,
    ) as (engine, _):
        yield engine


async def emit(query: SlayerQuery | list[SlayerQuery | dict], *, dialect: str,
               models: Optional[Iterable[SlayerModel]] = None) -> str:
    """Dry-run SQL over the dev2119 models on ``dialect`` (asserts it parses there)."""
    first, *rest = list(models) if models is not None else dev2119_models()
    return await _engine_generate(query=query, model=first, extra_models=rest, dialect=dialect)


# --------------------------------------------------------------------------- #
# Queries and readers
# --------------------------------------------------------------------------- #
def q(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "orders", **kw})


def m(formula: str, name: str = "v") -> ModelMeasure:
    return ModelMeasure(formula=formula, name=name)


def td(dimension: str, granularity: str = "month", **extra: Any) -> dict[str, Any]:
    return {"dimension": dimension, "granularity": granularity, **extra}


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
