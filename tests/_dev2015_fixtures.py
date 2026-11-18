"""Shared fixtures for the time spine and custom granularities: seeds, models, engines, row helpers."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, Iterable
from datetime import date, datetime
from typing import Any, Optional

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.core.warnings import BroadcastGrainWarningPayload
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._engine_helpers import seeded_exec_engine
from tests._time_points_fixtures import PinnedClock

BACKENDS = ("sqlite", "duckdb")

NOW = datetime(2026, 9, 29, 12, 0, 0)
JAN_JUN = ["2025-01-01", "2025-06-30"]
JAN_MAR = ["2025-01-01", "2025-03-31"]
MONTHS = ["2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-06"]

# ---------------------------------------------------------------------------
# Spine seed (queries/time-spine scenarios)
# ---------------------------------------------------------------------------

CUSTOMERS = TableSpec(
    name="customers",
    columns=[("id", "INT"), ("region", "TEXT"), ("signed_up_at", "DATE")],
    rows=[(1, "N", "2024-11-15"), (2, "S", "2025-01-05"), (3, "E", "2025-04-20")],
)
ORDERS_ROWS: list[tuple] = [
    (1, 1, "2025-01-10", 100.0), (2, 2, "2025-01-20", 50.0), (3, 1, "2025-02-05", 70.0),
]
RETURNS = TableSpec(
    name="returns",
    columns=[("id", "INT"), ("customer_id", "INT"), ("return_date", "DATE"), ("amount", "DOUBLE")],
    rows=[(1, 1, "2025-02-10", 20.0), (2, 2, "2025-03-03", 10.0), (3, 1, "2025-05-03", 5.0)],
)
ORDER_ITEMS = TableSpec(
    name="order_items",
    columns=[("id", "INT"), ("order_id", "INT"), ("qty", "DOUBLE")],
    rows=[(1, 1, 2.0), (2, 1, 3.0), (3, 2, 1.0), (4, 3, 4.0)],
)
SHIPMENTS = TableSpec(
    name="shipments",
    columns=[
        ("id", "INT"), ("order_id", "INT"), ("return_id", "INT"), ("weight", "DOUBLE"),
        ("created_at", "DATE"), ("delivered_at", "DATE"),
    ],
    rows=[(1, 1, 1, 3.0, "2025-01-11", "2025-01-14"), (2, 3, 2, 4.0, "2025-02-06", "2025-02-09")],
)


def orders_table(rows: Optional[list[tuple]] = None, *, date_type: str = "DATE") -> TableSpec:
    return TableSpec(
        name="orders",
        columns=[("id", "INT"), ("customer_id", "INT"), ("order_date", date_type), ("amount", "DOUBLE")],
        rows=ORDERS_ROWS if rows is None else rows,
    )


def calendar_table(start: date = date(2025, 1, 1), days: int = 181) -> TableSpec:
    return TableSpec(
        name="calendar", columns=[("date", "DATE")],
        rows=[(date.fromordinal(start.toordinal() + i).isoformat(),) for i in range(days)],
    )


def spine_tables(*, orders_rows: Optional[list[tuple]] = None, date_type: str = "DATE") -> list[TableSpec]:
    return [
        CUSTOMERS, orders_table(orders_rows, date_type=date_type), RETURNS, ORDER_ITEMS, SHIPMENTS,
        calendar_table(),
    ]


def _to_customers() -> DatasetRelationship:
    return DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])


def customers_dataset(*, signed_up_at: bool = False) -> SlayerDataset:
    columns = [
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="region", datatype=DataType.STRING),
    ]
    if signed_up_at:
        columns.append(DatasetField(name="signed_up_at", datatype=DataType.DATE))
    return SlayerDataset(name="customers", sql_table="customers", data_source="test", fields=columns)


def orders_dataset(*, date_type: DataType = DataType.DATE, extra_joins: Iterable[DatasetRelationship] = (),
                 filters: Optional[list[str]] = None) -> SlayerDataset:
    return SlayerDataset(
        name="orders", sql_table="orders", data_source="test", default_time_dimension="order_date",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="order_date", datatype=date_type),
            DatasetField(name="amount", datatype=DataType.FLOAT),
        ],
        relationships=[_to_customers(), *extra_joins],
        filters=filters or [],
    )


def returns_dataset(*, default_time_dimension: Optional[str] = None,
                  extra_joins: Iterable[DatasetRelationship] = ()) -> SlayerDataset:
    """No declared default: ``return_date`` is the sole temporal column."""
    return SlayerDataset(
        name="returns", sql_table="returns", data_source="test",
        default_time_dimension=default_time_dimension,
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="return_date", datatype=DataType.DATE),
            DatasetField(name="amount", datatype=DataType.FLOAT),
        ],
        relationships=[_to_customers(), *extra_joins],
    )


def order_items_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="order_items", sql_table="order_items", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="order_id", datatype=DataType.INTEGER),
            DatasetField(name="qty", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="orders", from_columns=["order_id"], to_columns=["id"])],
    )


def shipments_two_dates_dataset() -> SlayerDataset:
    """Two temporal columns and no declared default: no axis."""
    return SlayerDataset(
        name="shipments", sql_table="shipments", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="weight", datatype=DataType.FLOAT),
            DatasetField(name="created_at", datatype=DataType.DATE),
            DatasetField(name="delivered_at", datatype=DataType.DATE),
        ],
    )


def shipments_two_parents_dataset() -> SlayerDataset:
    """No temporal column; joins both facts, so two equal-length spine routes."""
    return SlayerDataset(
        name="shipments", sql_table="shipments", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="order_id", datatype=DataType.INTEGER),
            DatasetField(name="return_id", datatype=DataType.INTEGER),
            DatasetField(name="weight", datatype=DataType.FLOAT),
        ],
        relationships=[
            DatasetRelationship(to="orders", from_columns=["order_id"], to_columns=["id"]),
            DatasetRelationship(to="returns", from_columns=["return_id"], to_columns=["id"]),
        ],
    )


def calendar_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="calendar", sql_table="calendar", data_source="test",
        fields=[DatasetField(name="date", datatype=DataType.DATE, primary_key=True)],
    )


def _to_calendar(column: str) -> DatasetRelationship:
    return DatasetRelationship(to="calendar", from_columns=[column], to_columns=["date"])


def monthly_dataset() -> SlayerDataset:
    """Query-backed monthly revenue; its axis is the ``month``-bucketed ``order_date``."""
    return SlayerDataset(
        name="monthly", data_source="test",
        source_queries=[SlayerQuery.model_validate({
            "population": "orders",
            "time_dimensions": [{"field": "order_date", "granularity": "month"}],
            "metrics": [{"metric": "sum(amount)", "alias": "rev"}],
        })],
    )


def spine_datasets(*, signed_up_at: bool = False, shipments: Optional[str] = None,
                 date_type: DataType = DataType.DATE, returns_default: Optional[str] = None,
                 orders_filters: Optional[list[str]] = None) -> list[SlayerDataset]:
    """``customers``, ``orders``, ``returns``, ``order_items``, ``monthly`` and an optional ``shipments``."""
    models = [
        customers_dataset(signed_up_at=signed_up_at),
        orders_dataset(date_type=date_type, filters=orders_filters),
        returns_dataset(default_time_dimension=returns_default),
        order_items_dataset(),
        monthly_dataset(),
    ]
    if shipments == "two_dates":
        models.append(shipments_two_dates_dataset())
    elif shipments == "two_parents":
        models.append(shipments_two_parents_dataset())
    return models


def calendar_datasets() -> list[SlayerDataset]:
    """The hand-made calendar pattern: each fact joins ``calendar`` by its date."""
    return [
        customers_dataset(),
        orders_dataset(extra_joins=[_to_calendar("order_date")]),
        returns_dataset(extra_joins=[_to_calendar("return_date")]),
        calendar_dataset(),
    ]


# ---------------------------------------------------------------------------
# Custom-granularity seed (queries/custom-granularities scenarios)
# ---------------------------------------------------------------------------

GRANULARITIES: list[dict[str, Any]] = [
    {"name": "fiscal_year", "base": "year", "origin": "2000-04-01"},
    {"name": "billing_month", "base": "month", "origin": "2000-01-15"},
    {"name": "quarter_hour", "base": "minute", "multiple": 15},
    {"name": "sprint", "base": "week", "multiple": 2, "origin": "2025-01-06"},
]
GRANULARITY_NAMES = ("fiscal_year", "billing_month", "quarter_hour", "sprint")
BUILT_IN_GRANULARITIES = ("second", "minute", "hour", "day", "week", "week_sunday", "month", "quarter", "year")
DS_GRANULARITIES = {"granularities": GRANULARITIES}

CG_ORDERS = TableSpec(
    name="orders",
    columns=[("id", "INT"), ("order_date", "DATE"), ("amount", "DOUBLE")],
    rows=[(1, "2024-03-15", 10.0), (2, "2024-04-02", 20.0), (3, "2025-03-31", 30.0), (4, "2025-04-01", 40.0)],
)
# (id, series, ts, amount)
EVENTS = TableSpec(
    name="events",
    columns=[("id", "INT"), ("series", "TEXT"), ("ts", "TIMESTAMP"), ("d", "DATE"), ("amount", "DOUBLE")],
    rows=[
        (1, "bill", "2025-03-10 00:00:00", "2025-03-10", 1.0),
        (2, "bill", "2025-03-15 00:00:00", "2025-03-15", 1.0),
        (3, "sprint", "2024-12-30 00:00:00", "2024-12-30", 1.0),
        (4, "sprint", "2025-01-19 00:00:00", "2025-01-19", 1.0),
        (5, "sprint", "2025-01-20 00:00:00", "2025-01-20", 1.0),
        (6, "qh", "2025-06-02 10:07:00", "2025-06-02", 1.0),
        (7, "qh", "2025-06-02 10:14:00", "2025-06-02", 2.0),
        (8, "qh", "2025-06-02 10:15:00", "2025-06-02", 4.0),
        (9, "qh", "2025-06-02 10:44:00", "2025-06-02", 8.0),
        # sprints starting 2025-01-06, 2025-01-20 and 2025-02-17 (2025-02-03 empty)
        (10, "streak", "2025-01-07 09:00:00", "2025-01-07", 1.0),
        (11, "streak", "2025-01-21 09:00:00", "2025-01-21", 1.0),
        (12, "streak", "2025-02-18 09:00:00", "2025-02-18", 1.0),
    ],
)


def cg_orders_dataset(*, order_date_granularity: Optional[str] = None) -> SlayerDataset:
    column: dict[str, Any] = {"name": "order_date", "datatype": "Date"}
    if order_date_granularity is not None:
        column["granularity"] = order_date_granularity
    return SlayerDataset.model_validate({
        "name": "orders", "sql_table": "orders", "data_source": "test", "default_time_dimension": "order_date",
        "fields": [
            {"name": "id", "datatype": "Integer", "primary_key": True},
            column,
            {"name": "amount", "datatype": "Float"},
        ],
    })


def events_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="events", sql_table="events", data_source="test", default_time_dimension="ts",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="series", datatype=DataType.STRING),
            DatasetField(name="ts", datatype=DataType.DATETIME),
            DatasetField(name="d", datatype=DataType.DATE),
            DatasetField(name="amount", datatype=DataType.FLOAT),
        ],
    )


def bucketed_dataset(*, name: str, source: str, column: str, granularity: str) -> SlayerDataset:
    """A query-backed model whose ``column`` is bucketed at ``granularity``."""
    return SlayerDataset.model_validate({
        "name": name, "data_source": "test",
        "source_queries": [{
            "population": source,
            "time_dimensions": [{"field": column, "granularity": granularity}],
            "metrics": [{"metric": "sum(amount)", "alias": "rev"}],
        }],
    })


# ---------------------------------------------------------------------------
# Engines
# ---------------------------------------------------------------------------

@contextlib.asynccontextmanager
async def spine_engine(
    backend: str, *, datasets: Optional[list[SlayerDataset]] = None, tables: Optional[list[TableSpec]] = None,
    clock: Optional[PinnedClock] = None, datasource_fields: Optional[dict[str, Any]] = None,
) -> AsyncGenerator[SlayerQueryEngine]:
    """Executing engine over the spine seed (the custom granularities are declared too)."""
    seeded = spine_tables() if tables is None else tables
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, seeded),
        datasets=spine_datasets() if datasets is None else datasets,
        clock=clock if clock is not None else PinnedClock(NOW),
        datasource_fields=DS_GRANULARITIES if datasource_fields is None else datasource_fields,
    ) as (engine, _db):
        yield engine


@contextlib.asynccontextmanager
async def cg_engine(
    backend: str, *, datasets: Optional[list[SlayerDataset]] = None, clock: Optional[PinnedClock] = None,
    datasource_fields: Optional[dict[str, Any]] = None,
) -> AsyncGenerator[SlayerQueryEngine]:
    """Executing engine over the custom-granularity seed, the datasource defining the four granularities."""
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, [CG_ORDERS, EVENTS]),
        datasets=[cg_orders_dataset(), events_dataset()] if datasets is None else datasets,
        clock=clock if clock is not None else PinnedClock(NOW),
        datasource_fields=DS_GRANULARITIES if datasource_fields is None else datasource_fields,
    ) as (engine, _db):
        yield engine


# ---------------------------------------------------------------------------
# Queries and rows
# ---------------------------------------------------------------------------

def spine_td(*, granularity: str = "month", date_range: Any = JAN_JUN) -> dict[str, Any]:
    td: dict[str, Any] = {"field": "time_spine.timestamp", "granularity": granularity}
    if date_range is not None:
        td["date_range"] = date_range
    return td


def m(expression: str, name: str) -> dict[str, str]:
    return {"expression": expression, "name": name}


TWO_FACTS = [m("sum(orders.amount)", "o"), m("sum(returns.amount)", "r"), m("count(orders.id)", "n")]
TWO_FACT_ROWS = {
    "2025-01": (150.0, None, 2.0), "2025-02": (70.0, 20.0, 1.0), "2025-03": (None, 10.0, 0.0),
    "2025-04": (None, None, 0.0), "2025-05": (None, 5.0, 0.0), "2025-06": (None, None, 0.0),
}


def spine_query(*, metrics: list[Any], date_range: Any = JAN_JUN, granularity: str = "month",
                **extra: Any) -> SlayerQuery:
    tds = extra.pop("time_dimensions", None) or [spine_td(granularity=granularity, date_range=date_range)]
    return SlayerQuery.model_validate({"metrics": metrics, "time_dimensions": tds, **extra})


def key_of(row: dict[str, Any], name: str) -> str:
    """The unique result key naming ``name`` (a bare name or its dotted tail)."""
    hits = [k for k in row if k == name or k.endswith(f".{name}")]
    assert len(hits) == 1, (name, list(row))
    return hits[0]


def value(row: dict[str, Any], name: str) -> Any:
    return row[key_of(row, name)]


def num(v: Any) -> Optional[float]:
    return None if v is None else float(v)


def bucket_key(v: Any, *, width: int = 7) -> str:
    """A bucket value as ISO text, truncated to ``width`` (7 → ``YYYY-MM``, 10 → a day, 16 → a minute)."""
    if isinstance(v, datetime):
        text = v.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(v, date):
        text = f"{v.isoformat()} 00:00:00"
    else:
        text = str(v).replace("T", " ")
    return text[:width]


def by_bucket(resp: SlayerResponse, names: Iterable[str], *, bucket: str = "time_spine.timestamp",
              width: int = 7, by: Iterable[str] = ()) -> dict[Any, tuple]:
    """``{bucket (or (by..., bucket)): (values of names...)}``; asserts buckets are unique."""
    names, by = list(names), list(by)
    out: dict[Any, tuple] = {}
    for row in resp.data:
        b = bucket_key(row[bucket], width=width)
        k = (*(value(row, d) for d in by), b) if by else b
        assert k not in out, k
        out[k] = tuple(num(value(row, n)) if not isinstance(value(row, n), str) else value(row, n) for n in names)
    return out


def broadcast_warnings(resp: SlayerResponse) -> list[BroadcastGrainWarningPayload]:
    return [w for w in (resp.warnings or []) if isinstance(w, BroadcastGrainWarningPayload)]
