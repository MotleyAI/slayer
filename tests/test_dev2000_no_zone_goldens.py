"""A query without a zone emits exactly the SQL and returns exactly the values it did before time zones existed,
whatever source zones are declared and however instant fields are typed (spec: queries/time-zones).

The SQL snapshot was captured from the code before time zones; ``SLAYER_REGEN_DEV2000_GOLDENS=1`` rewrites it.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery

from tests._dev2000_fixtures import (
    BACKENDS,
    DS,
    PinnedClock,
    d,
    field,
    instant,
    m,
    marks_dataset,
    marks_table,
    orders_dataset,
    orders_table,
    tz_engine,
)
from tests._engine_helpers import _engine_generate

SNAPSHOT = Path(__file__).parent / "fixtures" / "dev2000_no_zone_sql.json"
REGEN = os.environ.get("SLAYER_REGEN_DEV2000_GOLDENS") == "1"
CLOCK = "2025-06-01 12:00:00"

DERIVED = field("ordered_plus", DataType.DATETIME, expression="date_add(ordered_at, 1, 'hour')")
DAY = "day"

QUERIES: dict[str, dict[str, Any]] = {
    "bucket": {"time_dimensions": [{"field": "orders.ordered_at", "granularity": DAY}],
               "metrics": [m("sum(orders.amount)", "a")]},
    "bucket_instant": {"time_dimensions": [{"field": "orders.paid_at", "granularity": "month"}],
                       "metrics": [m("sum(orders.amount)", "a")]},
    "bucket_civil": {"time_dimensions": [{"field": "orders.local_slot", "granularity": DAY}],
                     "metrics": [m("count(orders.*)", "n")]},
    "filter": {"dimensions": ["orders.id"], "filter": [
        "orders.ordered_at >= '2025-01-01'", "orders.paid_at < '2025-02-01'"]},
    "filter_civil": {"dimensions": ["orders.id"], "filter": ["orders.local_slot in '2025-01-01'"]},
    "date_range": {"time_dimensions": [{"field": "orders.ordered_at", "granularity": DAY,
                                        "date_range": ["2025-01-01", "2025-01-31"]}],
                   "metrics": [m("count(orders.*)", "n")]},
    "extract": {"dimensions": ["orders.id", d("hour(orders.ordered_at)", "h"), d("day_of_week(orders.paid_at)", "w"),
                               d("date_part('day', orders.local_slot)", "dd")]},
    "window": {"time_dimensions": [{"field": "orders.ordered_at", "granularity": DAY}],
               "metrics": [m("sum(orders.amount, window='1d')", "w")]},
    "shift": {"time_dimensions": [{"field": "orders.ordered_at", "granularity": "month"}],
              "metrics": [m("sum(orders.amount)", "a"), m("time_shift(sum(orders.amount), -1)", "p")]},
    "project": {"dimensions": ["orders.id", "orders.ordered_at", "orders.paid_at", "orders.local_slot"]},
    "cast": {"dimensions": ["orders.id", d("CAST(orders.ordered_at AS DATE)", "od")]},
    "derived": {"dimensions": ["orders.id", "orders.ordered_plus"]},
    "join": {"dimensions": ["orders.id", "marks.label"]},
}

# Source-zone variants; ``bare`` declares none and is what the snapshot holds.
VARIANTS: dict[str, tuple[Optional[str], dict[str, str]]] = {
    "bare": (None, {}),
    "datasource_zone": ("UTC", {}),
    "field_zones": ("UTC", {"ordered_at": "Asia/Tokyo", "local_slot": "civil"}),
}


def _snapshot() -> dict[str, str]:
    return json.loads(SNAPSHOT.read_text()) if SNAPSHOT.exists() else {}


def _check(key: str, sql: Optional[str], *, variant: str) -> None:
    assert sql is not None
    if REGEN and variant in ("bare", "datetime"):
        data = _snapshot()
        data[key] = sql
        SNAPSHOT.write_text(json.dumps(data, indent=1, sort_keys=True) + "\n")
        return
    assert sql == _snapshot()[key], f"{key} ({variant}) changed:\n{sql}"


def _rows(resp) -> list[tuple]:
    return sorted((tuple(repr(r[c]) for c in resp.columns) for r in resp.data), key=repr)


def _engine(backend: str, variant: str):
    ds_zone, field_zones = VARIANTS[variant]
    return tz_engine(
        backend, datasets=[orders_dataset(field_zones=field_zones, extra_fields=[DERIVED]), marks_dataset()],
        tables=[orders_table(), marks_table()], ds_zone=ds_zone, clock=PinnedClock(instant(CLOCK)),
    )


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("variant", sorted(VARIANTS))
@pytest.mark.parametrize("name", sorted(QUERIES))
async def test_source_zones_alone_change_nothing(backend, variant, name) -> None:
    query = SlayerQuery.model_validate({"population": "orders", **QUERIES[name]})
    async with _engine(backend, variant) as engine:
        resp = await engine.execute(query)
    async with _engine(backend, "bare") as bare_engine:
        bare = await bare_engine.execute(query)
    _check(f"{backend}:{name}", resp.sql, variant=variant)
    assert _rows(resp) == _rows(bare)


# ---------------------------------------------------------------------------
# Instant fields typed DateTime vs DateTimeTz, per dialect that now types them DateTimeTz
# ---------------------------------------------------------------------------

INSTANT_DB_TYPE = {"bigquery": "TIMESTAMP", "mysql": None, "clickhouse": None}

INSTANT_QUERIES: dict[str, dict[str, Any]] = {
    "filter_now": {"population": "ev", "dimensions": ["ev.id"], "filter": ["ev.ts < now()"]},
    "filter_literal": {"population": "ev", "dimensions": ["ev.id"], "filter": ["ev.ts >= '2025-01-01 00:00:00'"]},
    "cast": {"population": "ev", "dimensions": ["ev.id", d("CAST(ev.ts AS DATE)", "v")]},
    "truncate": {"population": "ev", "time_dimensions": [{"field": "ev.ts", "granularity": "month"}],
                 "metrics": [m("count(ev.*)", "n")]},
    "join": {"population": "ev", "dimensions": ["ev.id", "slots.label"]},
    "derived": {"population": "ev", "dimensions": ["ev.id", "ev.ts_plus"]},
}


def _instant_datasets(dialect: str, datatype: DataType) -> list[SlayerDataset]:
    db_type = INSTANT_DB_TYPE[dialect]
    ev = SlayerDataset(
        name="ev", sql_table="ev", data_source=DS,
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="ts", datatype=datatype, db_type=db_type),
                DatasetField(name="ts_plus", datatype=datatype, expression="date_add(ts, 1, 'hour')")],
        relationships=[DatasetRelationship(to="slots", from_columns=["ts"], to_columns=["slot_ts"])],
    )
    slots = SlayerDataset(
        name="slots", sql_table="slots", data_source=DS,
        fields=[DatasetField(name="slot_ts", datatype=datatype, db_type=db_type, primary_key=True),
                DatasetField(name="label", datatype=DataType.STRING)],
    )
    return [ev, slots]


@pytest.mark.parametrize("dialect", sorted(INSTANT_DB_TYPE))
@pytest.mark.parametrize("datatype", [DataType.DATETIME, DataType.DATETIME_TZ], ids=["datetime", "datetimetz"])
@pytest.mark.parametrize("name", sorted(INSTANT_QUERIES))
async def test_retyped_instant_fields_change_nothing(dialect, datatype, name) -> None:
    datasets = _instant_datasets(dialect, datatype)
    sql = await _engine_generate(
        query=SlayerQuery.model_validate(INSTANT_QUERIES[name]), dataset=datasets[0], extra_datasets=datasets[1:],
        dialect=dialect, validate=False, clock=PinnedClock(instant(CLOCK)),
    )
    _check(f"{dialect}:{name}", sql, variant="datetime" if datatype is DataType.DATETIME else "datetimetz")
