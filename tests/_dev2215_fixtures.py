"""Shared fixtures for the Ossie Layer 1 alignment (date-part extraction, identifier case, ``agg``, ``=`` in calls,
``first``/``last`` NULL keys, deprecated spellings).

orders (id, customer_id, region, status, amount, qty, weight, created_at, shipped_at, order_date, note):
   1  1  EU    paid     10    1  1  2024-03-05 08:00:00  2024-03-06 08:00:00  2024-03-05  a
   2  2  EU    paid     20    2  2  2024-04-10 09:30:00  2024-04-20 09:30:00  2024-04-10  b
   3  3  US    pending  30    1  1  2024-12-30 10:15:42  NULL                 2024-12-30  c
   4  4  US    paid     40    3  0  2025-03-17 12:00:00  2025-03-18 12:00:00  2025-03-17  d
   5  2  NULL  void     NULL  2  1  2025-03-28 23:00:00  2025-04-05 23:00:00  2025-03-28  NULL
   6  1  EU    paid     60    1  2  2025-06-02 07:45:00  2025-06-03 07:45:00  2025-06-02  f
customers (id, name, region, status, updated_at, score): 1 A North gold 2024-05-01 1 | 2 B North silver NULL 2 |
   3 C South bronze 2024-02-01 3 | 4 D South tin NULL 4
events (id, customer_id, updated_at, status): customer 1 → (2024-01-01 a), (NULL b), (2024-03-01 c);
   customer 2 → (NULL x), (NULL y); customer 3 → (2024-01-01 p), (2024-02-01 NULL)
ledger (id, rev_a, rev_b): (1, 1, 10), (2, 2, 20) — model columns ``Revenue`` (rev_a) and ``revenue`` (rev_b).

Row 3's ``created_at`` extracts to year 2024, quarter 4, month 12, ISO week 1, day 30, hour 10, minute 15, second 42.
"""

from __future__ import annotations

import contextlib
import json
import warnings
from collections.abc import AsyncGenerator, Iterable, Iterator
from datetime import datetime
from typing import Any, Optional

from slayer.core.enums import DataType
from slayer.core.models import Aggregation, AggregationParam, Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests._dev1737_fixtures import TableSpec, seed_backend, server_table_statements
from tests._engine_helpers import seeded_exec_engine
from tests._time_points_fixtures import PinnedClock

BACKENDS = ("sqlite", "duckdb")
TIER1 = ("sqlite", "postgres", "duckdb", "mysql", "clickhouse", "tsql", "trino", "snowflake", "bigquery")
NOW = datetime(2026, 9, 29, 12, 0, 0)
GRANULARITIES: list[dict[str, Any]] = [{"name": "fiscal_year", "base": "year", "origin": "2000-04-01"}]
DS_FIELDS: dict[str, Any] = {"granularities": GRANULARITIES}
BUILT_IN_UNITS = ("second", "minute", "hour", "day", "week", "week_sunday", "month", "quarter", "year")
#: The eight extraction names and their ``date_part`` part.
EXTRACTIONS = ("year", "quarter", "month", "week", "day", "hour", "minute", "second")
#: Row 3's extraction per part.
ROW3_PARTS = {"year": 2024, "quarter": 4, "month": 12, "week": 1, "day": 30, "hour": 10, "minute": 15, "second": 42}

ORDERS = TableSpec(
    name="orders",
    columns=[
        ("id", "INT"), ("customer_id", "INT"), ("region", "TEXT"), ("status", "TEXT"), ("amount", "DOUBLE"),
        ("qty", "INT"), ("weight", "DOUBLE"), ("created_at", "TIMESTAMP"), ("shipped_at", "TIMESTAMP"),
        ("order_date", "DATE"), ("note", "TEXT"),
    ],
    rows=[
        (1, 1, "EU", "paid", 10.0, 1, 1.0, "2024-03-05 08:00:00", "2024-03-06 08:00:00", "2024-03-05", "a"),
        (2, 2, "EU", "paid", 20.0, 2, 2.0, "2024-04-10 09:30:00", "2024-04-20 09:30:00", "2024-04-10", "b"),
        (3, 3, "US", "pending", 30.0, 1, 1.0, "2024-12-30 10:15:42", None, "2024-12-30", "c"),
        (4, 4, "US", "paid", 40.0, 3, 0.0, "2025-03-17 12:00:00", "2025-03-18 12:00:00", "2025-03-17", "d"),
        (5, 2, None, "void", None, 2, 1.0, "2025-03-28 23:00:00", "2025-04-05 23:00:00", "2025-03-28", None),
        (6, 1, "EU", "paid", 60.0, 1, 2.0, "2025-06-02 07:45:00", "2025-06-03 07:45:00", "2025-06-02", "f"),
    ],
)
CUSTOMERS = TableSpec(
    name="customers",
    columns=[("id", "INT"), ("name", "TEXT"), ("region", "TEXT"), ("status", "TEXT"), ("updated_at", "TIMESTAMP"),
             ("score", "DOUBLE")],
    rows=[
        (1, "A", "North", "gold", "2024-05-01 00:00:00", 1.0),
        (2, "B", "North", "silver", None, 2.0),
        (3, "C", "South", "bronze", "2024-02-01 00:00:00", 3.0),
        (4, "D", "South", "tin", None, 4.0),
    ],
)
EVENTS = TableSpec(
    name="events",
    columns=[("id", "INT"), ("customer_id", "INT"), ("updated_at", "TIMESTAMP"), ("status", "TEXT")],
    rows=[
        (1, 1, "2024-01-01 00:00:00", "a"), (2, 1, None, "b"), (3, 1, "2024-03-01 00:00:00", "c"),
        (4, 2, None, "x"), (5, 2, None, "y"),
        (6, 3, "2024-01-01 00:00:00", "p"), (7, 3, "2024-02-01 00:00:00", None),
    ],
)
LEDGER = TableSpec(
    name="ledger", columns=[("id", "INT"), ("rev_a", "DOUBLE"), ("rev_b", "DOUBLE")],
    rows=[(1, 1.0, 10.0), (2, 2.0, 20.0)],
)
TABLES = (ORDERS, CUSTOMERS, EVENTS, LEDGER)


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
def trimmed() -> Aggregation:
    return Aggregation(name="trimmed", formula="AVG(CASE WHEN {value} > {cutoff} THEN {value} END)",
                       params=[AggregationParam(name="cutoff", sql="0")])


def share() -> Aggregation:
    return Aggregation(name="share", formula="SUM({value}) * 1.0 / COUNT(*)")


def orders_model(*, data_source: str = "test", aggregations: Iterable[Aggregation] = ()) -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source=data_source, default_time_dimension="created_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="region", type=DataType.TEXT),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="qty", type=DataType.INT),
            Column(name="weight", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="shipped_at", type=DataType.TIMESTAMP),
            Column(name="order_date", type=DataType.DATE),
            Column(name="note", type=DataType.TEXT),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        aggregations=[trimmed(), share(), *aggregations],
    )


def customers_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source=data_source,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="region", type=DataType.TEXT),
            Column(name="status", type=DataType.TEXT),
            Column(name="updated_at", type=DataType.TIMESTAMP),
            Column(name="score", type=DataType.DOUBLE),
        ],
    )


def events_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="events", sql_table="events", data_source=data_source,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="updated_at", type=DataType.TIMESTAMP),
            Column(name="status", type=DataType.TEXT),
        ],
    )


def ledger_model(*, data_source: str = "test") -> SlayerModel:
    """Grandfathered case collision: ``Revenue`` and ``revenue`` (save with ``_validate=False``)."""
    return SlayerModel(
        name="ledger", sql_table="ledger", data_source=data_source,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="Revenue", sql="rev_a", type=DataType.DOUBLE),
            Column(name="revenue", sql="rev_b", type=DataType.DOUBLE),
        ],
    )


def dev2215_models(*, data_source: str = "test", aggregations: Iterable[Aggregation] = ()) -> list[SlayerModel]:
    return [orders_model(data_source=data_source, aggregations=aggregations), customers_model(data_source=data_source),
            events_model(data_source=data_source), ledger_model(data_source=data_source)]


# --------------------------------------------------------------------------- #
# Engines
# --------------------------------------------------------------------------- #
@contextlib.asynccontextmanager
async def dev2215_engine(
    backend: str, *, models: Optional[list[SlayerModel]] = None, clock: Optional[PinnedClock] = None,
    datasource_fields: Optional[dict[str, Any]] = None,
) -> AsyncGenerator[SlayerQueryEngine]:
    """Executing engine over the seed; the datasource defines ``fiscal_year``; models are saved unvalidated."""
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, TABLES),
        models=dev2215_models() if models is None else models,
        clock=clock if clock is not None else PinnedClock(NOW),
        datasource_fields=DS_FIELDS if datasource_fields is None else datasource_fields,
    ) as (engine, _db):
        yield engine


def postgres_statements() -> list[str]:
    out: list[str] = []
    for table in TABLES:
        out += server_table_statements(table, backend="postgres")
    return out


# --------------------------------------------------------------------------- #
# Queries and readers
# --------------------------------------------------------------------------- #
def q(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "orders", **kw})


def m(formula: str, name: str = "v") -> dict[str, str]:
    return {"formula": formula, "name": name}


FIRST_LAST = [m("first(status, updated_at)", "f"), m("last(status, updated_at)", "l")]
#: customer_id → (first, last) over the events seed.
FIRST_LAST_BY_CUSTOMER = {1: ("a", "c"), 2: (None, None), 3: ("p", None)}


def events_query() -> SlayerQuery:
    return q(source_model="events", dimensions=["customer_id"], measures=FIRST_LAST)


async def check_events(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(events_query())
    got = {int(r["events.customer_id"]): (r["events.f"], r["events.l"]) for r in resp.data}
    assert got == FIRST_LAST_BY_CUSTOMER


def single(resp: SlayerResponse, name: str = "v", *, model: str = "orders") -> Any:
    assert len(resp.data) == 1, resp.data
    return resp.data[0][f"{model}.{name}"]


def ids(resp: SlayerResponse, *, model: str = "orders") -> set[int]:
    return {int(r[f"{model}.id"]) for r in resp.data}


def num(value: Any) -> Optional[float]:
    return None if value is None else float(value)


def day(value: Any) -> Optional[str]:
    """A date / timestamp cell as ``YYYY-MM-DD`` across SQLite text and DuckDB values."""
    return None if value is None else str(value)[:10]


def by_key(resp: SlayerResponse, key: str, value: str = "v", *, model: str = "orders") -> dict[Any, Any]:
    out = {r[f"{model}.{key}"]: r[f"{model}.{value}"] for r in resp.data}
    assert len(out) == len(resp.data), f"duplicate result rows for one {key}"
    return out


def count_by(resp: SlayerResponse, key: str, *, value: str = "n", model: str = "orders") -> dict[Any, int]:
    """``{key cell: count}`` with integer keys where the cell is numeric."""
    return {(int(k) if isinstance(k, (int, float)) else k): int(v)
            for k, v in by_key(resp, key, value, model=model).items()}


def sorted_rows(resp: SlayerResponse) -> list[str]:
    """Rows as order-insensitive comparable text (DuckDB GROUP BY order is arbitrary)."""
    return sorted(json.dumps(r, default=str, sort_keys=True) for r in resp.data)


def payload_text(warning: Any) -> str:
    """One structured response warning as searchable text."""
    dump = warning.model_dump(mode="json") if hasattr(warning, "model_dump") else warning
    return json.dumps(dump, default=str)


def warnings_mentioning(resp: SlayerResponse, *needles: str) -> list[Any]:
    """Structured response warnings whose payload mentions every needle."""
    return [w for w in resp.warnings or () if all(n in payload_text(w) for n in needles)]


@contextlib.contextmanager
def recorded_warnings() -> Iterator[list[warnings.WarningMessage]]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        yield caught


def future_warnings(caught: Iterable[warnings.WarningMessage], *needles: str) -> list[str]:
    """``FutureWarning`` messages mentioning every needle."""
    return [str(w.message) for w in caught
            if issubclass(w.category, FutureWarning) and all(n in str(w.message) for n in needles)]


def messages(caught: Iterable[warnings.WarningMessage], *needles: str) -> list[str]:
    """Any warning messages mentioning every needle."""
    return [str(w.message) for w in caught if all(n in str(w.message) for n in needles)]
