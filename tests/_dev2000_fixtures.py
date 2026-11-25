"""Time-zone scenario data (spec: queries/time-zones), seeded on SQLite and DuckDB, with a zoneinfo oracle."""

from __future__ import annotations

import contextlib
import os
import tempfile
from collections.abc import AsyncGenerator, Callable, Iterable, Mapping
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

import pytest
from pydantic import BaseModel

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, DatasourceConfig, SlayerDataset
from slayer.core.policy import SessionPolicy
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import owned_query_engine

BACKENDS = ("sqlite", "duckdb")
NY = "America/New_York"
TOKYO = "Asia/Tokyo"
ZONES = (NY, TOKYO)
DS = "shop"
UNDECLARED = "time_zone_undeclared"
FISCAL_WEEK = {"name": "fiscal_week", "base": "week", "origin": "2024-12-30 00:00:00"}

# id → (amount, ordered_at UTC wall clock, local_slot civil, order_day); paid_at is ordered_at as a UTC instant.
ROWS: dict[int, tuple[float, str, str, str]] = {
    1: (10.0, "2025-01-01 03:30:00", "2025-01-01 09:00:00", "2025-01-01"),
    2: (20.0, "2025-01-01 06:00:00", "2025-01-01 09:00:00", "2025-01-01"),
    3: (30.0, "2025-01-31 23:30:00", "2025-01-31 09:00:00", "2025-01-31"),
    4: (40.0, "2025-03-09 06:30:00", "2025-03-09 09:00:00", "2025-03-09"),
}
# marks(marked_at, label): a timestamp-keyed join target matching rows 1 and 3.
MARKS = {"2025-01-01 03:30:00": "first", "2025-01-31 23:30:00": "month_end"}
DST_NAIVE = {1: "2025-03-09 02:30:00", 2: "2024-11-03 01:30:00"}  # America/New_York wall clock
DST_INSTANTS = {1: "2024-11-03 05:30:00", 2: "2024-11-03 06:30:00"}  # UTC instants, both 01:30 in New York
HOURLY_START = datetime(2024, 11, 2)
HOURLY_COUNT = 73  # 2024-11-02 00:00Z … 2024-11-05 00:00Z


def utc(i: int) -> datetime:
    return datetime.fromisoformat(ROWS[i][1])


def amount(i: int) -> float:
    return ROWS[i][0]


def local(i: int, zone: str) -> datetime:
    """Row ``i``'s ``ordered_at`` as ``zone`` wall clock (naive)."""
    return to_zone(utc(i), zone)


def to_zone(value: datetime, zone: str, *, source: str = "UTC") -> datetime:
    """Naive ``value`` read in ``source``, as naive ``zone`` wall clock (Postgres DST rule)."""
    return from_zone(value, source).astimezone(ZoneInfo(zone)).replace(tzinfo=None)


def from_zone(value: datetime, zone: str) -> datetime:
    """Naive ``value`` in ``zone`` as an aware instant: a skipped time takes the pre-transition offset, a repeated
    time its later occurrence."""
    tz = ZoneInfo(zone)
    early, late = value.replace(tzinfo=tz, fold=0), value.replace(tzinfo=tz, fold=1)
    if early.utcoffset() == late.utcoffset():
        return early
    if early.astimezone(timezone.utc).astimezone(tz).replace(tzinfo=None) != value:
        return early  # skipped: fold=0 carries the offset before the transition
    return late


def naive(value: Any) -> datetime:
    """A backend timestamp (ISO text on SQLite) as a naive datetime; fails on a zoned value."""
    if isinstance(value, datetime):
        assert value.tzinfo is None, f"expected a naive timestamp, got {value!r}"
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    assert isinstance(value, str), f"not a timestamp: {value!r}"
    text = value.replace("T", " ")
    parsed = datetime.fromisoformat(text) if len(text) > 10 else datetime.fromisoformat(f"{text} 00:00:00")
    assert parsed.tzinfo is None, f"expected a naive timestamp, got {value!r}"
    return parsed


def day(value: Any) -> date:
    return naive(value).date()


class PinnedClock:
    """Returns ``now`` (aware or naive) and counts calls."""

    def __init__(self, now: datetime) -> None:
        self.now = now
        self.calls = 0

    def __call__(self) -> datetime:
        self.calls += 1
        return self.now


def instant(text: str) -> datetime:
    """A UTC instant from ``YYYY-MM-DD HH:MM:SS``."""
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

class Table(BaseModel):
    name: str
    columns: list[tuple[str, str]]  # (name, kind): INT, DOUBLE, TEXT, DATE, TIMESTAMP, TIMESTAMPTZ
    rows: list[tuple]


_TYPES = {
    "sqlite": {"INT": "INTEGER", "DOUBLE": "REAL", "TEXT": "TEXT", "DATE": "TEXT", "TIMESTAMP": "TEXT",
               "TIMESTAMPTZ": "TEXT"},
    "duckdb": {"INT": "INTEGER", "DOUBLE": "DOUBLE", "TEXT": "VARCHAR", "DATE": "DATE", "TIMESTAMP": "TIMESTAMP",
               "TIMESTAMPTZ": "TIMESTAMPTZ"},
}


def _instant_text(text: Optional[str]) -> Optional[str]:
    return None if text is None else f"{text}+00:00"


def orders_table(*, ordered_at: Optional[Mapping[int, str]] = None) -> Table:
    """``orders``; ``ordered_at`` overrides that column's stored values (e.g. New York wall clock)."""
    stored = {i: r[1] for i, r in ROWS.items()} if ordered_at is None else dict(ordered_at)
    return Table(
        name="orders",
        columns=[("id", "INT"), ("amount", "DOUBLE"), ("ordered_at", "TIMESTAMP"), ("paid_at", "TIMESTAMPTZ"),
                 ("local_slot", "TIMESTAMP"), ("order_day", "DATE"), ("created_raw", "TIMESTAMP")],
        rows=[(i, a, stored[i], _instant_text(ts), slot, d, ts) for i, (a, ts, slot, d) in ROWS.items()],
    )


def marks_table() -> Table:
    return Table(name="marks", columns=[("marked_at", "TIMESTAMP"), ("label", "TEXT")], rows=list(MARKS.items()))


def dst_table() -> Table:
    return Table(
        name="dst", columns=[("id", "INT"), ("local_at", "TIMESTAMP"), ("instant_at", "TIMESTAMPTZ")],
        rows=[(i, DST_NAIVE[i], _instant_text(DST_INSTANTS[i])) for i in DST_NAIVE],
    )


def hourly_table() -> Table:
    stamps = [(HOURLY_START + timedelta(hours=h)).strftime("%Y-%m-%d %H:%M:%S") for h in range(HOURLY_COUNT)]
    return Table(name="hourly", columns=[("id", "INT"), ("ts", "TIMESTAMPTZ")],
                 rows=[(i, _instant_text(s)) for i, s in enumerate(stamps)])


def default_tables() -> list[Table]:
    return [orders_table(), marks_table(), dst_table(), hourly_table()]


def seed(backend: str, db_path: str, tables: Iterable[Table]) -> None:
    types = _TYPES[backend]

    def fill(con: Any) -> None:
        for t in tables:
            con.execute(f"CREATE TABLE {t.name} ({', '.join(f'{c} {types[k]}' for c, k in t.columns)})")
            if t.rows:
                con.executemany(f"INSERT INTO {t.name} VALUES ({', '.join('?' * len(t.columns))})", t.rows)

    if backend == "sqlite":
        with transaction(db_path) as con:
            fill(con)
        return
    duckdb = pytest.importorskip("duckdb")
    with contextlib.closing(duckdb.connect(db_path)) as con:
        fill(con)


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------

def field(name: str, datatype: DataType, **extra: Any) -> DatasetField:
    return DatasetField.model_validate({"name": name, "datatype": datatype, **extra})


def orders_fields(*, field_zones: Optional[Mapping[str, str]] = None) -> list[DatasetField]:
    """``orders`` fields; ``local_slot`` is ``civil`` unless ``field_zones`` says otherwise (``{}`` keeps it undeclared)."""
    zones = {"local_slot": "civil"} if field_zones is None else dict(field_zones)

    def zoned(name: str, datatype: DataType) -> DatasetField:
        return field(name, datatype, **({"time_zone": zones[name]} if name in zones else {}))

    return [
        field("id", DataType.INTEGER, primary_key=True),
        field("amount", DataType.FLOAT),
        zoned("ordered_at", DataType.DATETIME),
        field("paid_at", DataType.DATETIME_TZ),
        zoned("local_slot", DataType.DATETIME),
        field("order_day", DataType.DATE),
    ]


def orders_dataset(
    *, field_zones: Optional[Mapping[str, str]] = None, extra_fields: Iterable[DatasetField] = (),
    metrics: Iterable[DatasetMetric] = (), filters: Iterable[str] = (),
) -> SlayerDataset:
    return SlayerDataset(
        name="orders", sql_table="orders", data_source=DS, default_time_dimension="ordered_at",
        fields=[*orders_fields(field_zones=field_zones), *extra_fields], metrics=list(metrics), filters=list(filters),
        relationships=[DatasetRelationship(to="marks", from_columns=["ordered_at"], to_columns=["marked_at"])],
    )


def orders_naive_instant_dataset() -> SlayerDataset:
    """``orders`` as ``orders_naive``, its instant ``paid_at`` typed ``DateTime``."""
    base = orders_dataset()
    return base.model_copy(update={"name": "orders_naive", "fields": [
        f.model_copy(update={"datatype": DataType.DATETIME}) if f.name == "paid_at" else f for f in base.fields]})


def instant_typing_queries(dataset: str) -> list[SlayerQuery]:
    """Zone-less queries filtering, casting, bucketing and shifting ``paid_at`` of ``dataset``."""
    paid = f"{dataset}.paid_at"
    return [SlayerQuery.model_validate(body) for body in (
        {"population": dataset, "dimensions": [f"{dataset}.id"], "filter": [f"{paid} >= '2025-01-01 05:00:00'"]},
        {"population": dataset, "dimensions": [f"{dataset}.id", {"field": f"CAST({paid} AS DATE)", "alias": "v"}]},
        {"population": dataset, "time_dimensions": [{"field": paid, "granularity": "month"}],
         "metrics": [{"metric": f"count({dataset}.*)", "alias": "n"}]},
        {"population": dataset, "dimensions": [f"{dataset}.id", {"field": f"date_add({paid}, 1, 'hour')", "alias": "v"}]},
    )]


def row_values(resp: SlayerResponse) -> list[tuple]:
    """``resp``'s rows as value tuples in column order, sorted."""
    return sorted((tuple(repr(r[c]) for c in resp.columns) for r in resp.data), key=repr)


def marks_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="marks", sql_table="marks", data_source=DS,
        fields=[field("marked_at", DataType.DATETIME, primary_key=True), field("label", DataType.STRING)],
    )


def dst_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="dst", sql_table="dst", data_source=DS,
        fields=[field("id", DataType.INTEGER, primary_key=True),
                field("local_at", DataType.DATETIME, time_zone=NY), field("instant_at", DataType.DATETIME_TZ)],
    )


def hourly_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="hourly", sql_table="hourly", data_source=DS, default_time_dimension="ts",
        fields=[field("id", DataType.INTEGER, primary_key=True), field("ts", DataType.DATETIME_TZ)],
    )


def default_datasets(orders: Optional[SlayerDataset] = None) -> list[SlayerDataset]:
    return [orders if orders is not None else orders_dataset(), marks_dataset(), dst_dataset(), hourly_dataset()]


@contextlib.asynccontextmanager
async def tz_engine(
    backend: str,
    *,
    datasets: Optional[list[SlayerDataset]] = None,
    tables: Optional[list[Table]] = None,
    ds_zone: Optional[str] = "UTC",
    clock: Optional[Callable[[], datetime]] = None,
    datasource_fields: Optional[Mapping[str, Any]] = None,
    policy: Optional[SessionPolicy] = None,
) -> AsyncGenerator[SlayerQueryEngine]:
    """An executing engine over the scenario tables; datasource ``shop`` has ``time_zone: ds_zone``."""
    seeded = tables if tables is not None else default_tables()
    fields: dict[str, Any] = {"granularities": [FISCAL_WEEK], **(datasource_fields or {})}
    if ds_zone is not None:
        fields["time_zone"] = ds_zone
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, f"seed.{'duckdb' if backend == 'duckdb' else 'db'}")
        seed(backend, db_path, seeded)
        storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        ds = DatasourceConfig.model_validate({"name": DS, "type": backend, "database": db_path, **fields})
        async with owned_query_engine(
            storage=storage, datasources=[ds], policy=policy,
            clock=clock if clock is not None else PinnedClock(instant("2025-06-01 12:00:00")),
        ) as engine:
            for dataset in datasets or default_datasets():
                await storage.save_dataset(dataset, _validate=False)
            yield engine


# ---------------------------------------------------------------------------
# Queries and results
# ---------------------------------------------------------------------------

def q(**fields: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"population": "orders", **fields})


def m(expression: str, alias: str) -> dict[str, str]:
    return {"metric": expression, "alias": alias}


def d(expression: str, alias: str) -> dict[str, str]:
    return {"field": expression, "alias": alias}


def column(resp: SlayerResponse, key: str) -> list[Any]:
    return [r[key] for r in resp.data]


def by_id(resp: SlayerResponse, key: str, *, id_key: str = "orders.id") -> dict[int, Any]:
    return {int(r[id_key]): r[key] for r in resp.data}


def pairs(resp: SlayerResponse, key: str, value: str, *, conv: Callable[[Any], Any] = naive) -> dict[Any, float]:
    """``{conv(row[key]): float(row[value])}``."""
    return {conv(r[key]): float(r[value]) for r in resp.data}


def warnings_of(resp: SlayerResponse, kind: str = UNDECLARED) -> list[Any]:
    return [w for w in resp.warnings if w.kind == kind]
