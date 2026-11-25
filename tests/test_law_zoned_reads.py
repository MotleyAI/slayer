"""System principle 12 (zoned reads): under a query zone every dataset timestamp read, in every position, sees the
zone's wall clock, converted once at the read.

The oracle for each (field kind, position) is the same query run without a zone over a database storing the zone's
wall-clock values; ``civil`` and undeclared reads must equal the zone-less query on the same database.
"""

from __future__ import annotations

import os
from collections import Counter
from collections.abc import Callable
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetMetric, DatasetRelationship, SlayerDataset
from slayer.core.policy import ColumnFilterRuleset, SessionPolicy
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.sql import zoned_read_check
from slayer.sql.zoned_read_check import ZonedReadLeakError

from tests._dev2000_fixtures import (
    BACKENDS,
    DS,
    HOURLY_COUNT,
    HOURLY_START,
    MARKS,
    NY,
    ROWS,
    ZONES,
    PinnedClock,
    Table,
    d,
    day,
    field,
    hourly_dataset,
    hourly_table,
    instant,
    local,
    m,
    marks_dataset,
    orders_fields,
    orders_table,
    to_zone,
    tz_engine,
)

CLOCK = "2025-01-01 03:30:00"  # UTC instant; 2024-12-31 22:30 in New York, 2025-01-01 12:30 in Tokyo
STAMP = "%Y-%m-%d %H:%M:%S"


# ---------------------------------------------------------------------------
# Data: the scenario tables, or the same tables storing a zone's wall clock
# ---------------------------------------------------------------------------

def _items_table() -> Table:
    return Table(name="items", columns=[("id", "INT"), ("order_id", "INT")], rows=[(i, i) for i in ROWS])


def _tables(zone: Optional[str]) -> list[Table]:
    """Scenario tables; with ``zone``, ``orders.ordered_at`` and ``marks.marked_at`` hold its wall clock."""
    if zone is None:
        return [orders_table(), Table(name="marks", columns=[("marked_at", "TIMESTAMP"), ("label", "TEXT")],
                                      rows=list(MARKS.items())), _items_table(), hourly_table()]
    shifted = {i: local(i, zone).strftime(STAMP) for i in ROWS}
    marks = [(to_zone(datetime.fromisoformat(k), zone).strftime(STAMP), v) for k, v in MARKS.items()]
    return [orders_table(ordered_at=shifted),
            Table(name="marks", columns=[("marked_at", "TIMESTAMP"), ("label", "TEXT")], rows=marks),
            _items_table(), hourly_table()]


def _orders(name: str = "orders", *, join_on: str = "ordered_at", time_dimension: str = "ordered_at",
            field_zones: Optional[dict[str, str]] = None) -> SlayerDataset:
    return SlayerDataset(
        name=name, sql_table="orders", data_source=DS, default_time_dimension=time_dimension,
        fields=[*orders_fields(field_zones=field_zones),
                field("ordered_derived", DataType.DATETIME, expression="coalesce(ordered_at, ordered_at)")],
        metrics=[DatasetMetric(name="latest", expression="max(ordered_at)")],
        relationships=[DatasetRelationship(to="marks", from_columns=[join_on], to_columns=["marked_at"])],
    )


def _items() -> SlayerDataset:
    return SlayerDataset(
        name="items", sql_table="items", data_source=DS,
        fields=[field("id", DataType.INTEGER, primary_key=True), field("order_id", DataType.INTEGER)],
        relationships=[DatasetRelationship(to="orders", from_columns=["order_id"], to_columns=["id"])],
    )


def _marks_to_orders() -> SlayerDataset:
    base = marks_dataset()
    return base.model_copy(update={"name": "marks_many", "relationships": [
        DatasetRelationship(to="orders", from_columns=["marked_at"], to_columns=["ordered_at"])]})


def _orders_qb() -> SlayerDataset:
    return SlayerDataset(name="orders_qb", data_source=DS, source_queries=[SlayerQuery.model_validate({
        "population": "orders", "dimensions": ["orders.id", "orders.ordered_at", "orders.amount"]})])


def _datasets(*, field_zones: Optional[dict[str, str]] = None) -> list[SlayerDataset]:
    return [_orders(field_zones=field_zones), _orders("orders_by_paid", join_on="paid_at", time_dimension="paid_at",
                                                       field_zones=field_zones),
            marks_dataset(), _marks_to_orders(), _items(), hourly_dataset(), _orders_qb()]


def _clock(zone: Optional[str]) -> PinnedClock:
    """The engine clock: the instant, or (for a zone's wall-clock database) that instant's wall clock."""
    return PinnedClock(instant(CLOCK) if zone is None else to_zone(datetime.fromisoformat(CLOCK), zone))


def _engine(backend: str, *, zone: Optional[str], ds_zone: Optional[str] = "UTC"):
    """``zone=None``: the scenario database; else a zone-less database storing ``zone`` wall clock."""
    return tz_engine(backend, datasets=_datasets(), tables=_tables(zone), ds_zone=ds_zone if zone is None else None,
                     clock=_clock(zone))


# ---------------------------------------------------------------------------
# Field kinds and positions
# ---------------------------------------------------------------------------

class Kind:
    """A timestamp read: population ``pop``, subject ``subject``, amount ``amount``, extra leading stages."""

    def __init__(self, name: str, *, pop: str, subject: str, amount: str, reference: Optional[str] = None,
                 stages: tuple[dict[str, Any], ...] = ()) -> None:
        self.name, self.pop, self.subject, self.amount = name, pop, subject, amount
        self.reference = reference or subject  # the subject on the wall-clock database
        self.stages = stages


STAGE = {"name": "s1", "population": "orders", "dimensions": ["orders.id", "orders.ordered_at", "orders.amount"]}

CONVERTED_KINDS = [
    Kind("local", pop="orders", subject="orders.ordered_at", amount="orders.amount"),
    Kind("instant", pop="orders", subject="orders.paid_at", amount="orders.amount", reference="orders.ordered_at"),
    Kind("derived", pop="orders", subject="orders.ordered_derived", amount="orders.amount"),
    Kind("joined", pop="items", subject="orders.ordered_at", amount="orders.amount"),
    Kind("stage", pop="s1", subject="s1.ordered_at", amount="s1.amount", stages=(STAGE,)),
    Kind("query_backed", pop="orders_qb", subject="orders_qb.ordered_at", amount="orders_qb.amount"),
]
STORED_KINDS = [
    Kind("civil", pop="orders", subject="orders.local_slot", amount="orders.amount"),
    Kind("undeclared", pop="orders", subject="orders.ordered_at", amount="orders.amount"),
]

Position = Callable[[Kind, str], dict[str, Any]]


def _id(kind: Kind) -> str:
    return "items.id" if kind.pop == "items" else f"{kind.pop}.id"


POSITIONS: dict[str, Position] = {
    "time_dimension": lambda k, s: {"time_dimensions": [{"field": s, "granularity": "day"}],
                                    "metrics": [m(f"sum({k.amount})", "a")]},
    "date_trunc_dimension": lambda k, s: {"dimensions": [d(f"date_trunc('month', {s})", "v")],
                                          "metrics": [m(f"sum({k.amount})", "a")]},
    "date_trunc_filter": lambda k, s: {"dimensions": [_id(k)], "filter": [f"date_trunc('day', {s}) = '2025-01-01'"]},
    "date_trunc_order": lambda k, s: {"dimensions": [_id(k), d(f"date_trunc('hour', {s})", "v")],
                                      "order": [{"field": "v", "direction": "desc"},
                                                {"field": _id(k), "direction": "asc"}]},
    "custom_granularity": lambda k, s: {"time_dimensions": [{"field": s, "granularity": "fiscal_week"}],
                                        "metrics": [m(f"sum({k.amount})", "a")]},
    "date_range": lambda k, s: {"time_dimensions": [{"field": s, "granularity": "day",
                                                     "date_range": ["2025-01-01", "2025-01-31"]}],
                                "metrics": [m(f"sum({k.amount})", "a")]},
    "relative_token": lambda k, s: {"dimensions": [_id(k)], "filter": [f"{s} = 'today'"]},
    "whole_periods_only": lambda k, s: {"time_dimensions": [{"field": s, "granularity": "day"}],
                                        "metrics": [m(f"sum({k.amount})", "a")], "whole_periods_only": True},
    "date_part": lambda k, s: {"dimensions": [_id(k), d(f"hour({s})", "h"), d(f"day_of_week({s})", "w"),
                                              d(f"date_part('day', {s})", "dd")]},
    "date_diff": lambda k, s: {"dimensions": [_id(k), d(f"date_diff('day', {s}, '2025-01-01 00:00')", "v")]},
    "date_add": lambda k, s: {"dimensions": [_id(k), d(f"date_add({s}, 1, 'day')", "v")]},
    "trailing_window": lambda k, s: {"time_dimensions": [{"field": s, "granularity": "day"}],
                                     "metrics": [m(f"sum({k.amount}, window='1d')", "w")]},
    "window_transform": lambda k, s: {"time_dimensions": [{"field": s, "granularity": "day"}],
                                      "metrics": [m(f"cumsum(sum({k.amount}))", "c")]},
    "time_shift": lambda k, s: {"time_dimensions": [{"field": s, "granularity": "month"}],
                                "metrics": [m(f"sum({k.amount})", "a"), m(f"time_shift(sum({k.amount}), -1)", "p")]},
    "consecutive_periods": lambda k, s: {"time_dimensions": [{"field": s, "granularity": "day"}],
                                         "metrics": [m(f"consecutive_periods(sum({k.amount}) > 5)", "cp")]},
    "first_last": lambda k, s: {"time_dimensions": [{"field": s, "granularity": "month"}],
                                "metrics": [m(f"first({k.amount}, {s})", "f"), m(f"last({k.amount}, {s})", "l")]},
    "min_max": lambda k, s: {"metrics": [m(f"min({s})", "lo"), m(f"max({s})", "hi")]},
    "raw_rows": lambda k, s: {"dimensions": [_id(k), d(s, "v")], "distinct_dimension_values": False},
}
ORDERED = {"date_trunc_order"}


def _query(kind: Kind, position: str, *, subject: str, zone: Optional[str]) -> list[dict[str, Any]]:
    final: dict[str, Any] = {"population": kind.pop, **POSITIONS[position](kind, subject)}
    if zone is not None:
        final["time_zone"] = zone
    return [*kind.stages, final]


def _cell(value: Any) -> Any:
    if isinstance(value, datetime):
        assert value.tzinfo is None, f"zoned result {value!r}"
        return value.strftime(STAMP)
    if isinstance(value, date):
        return f"{value.isoformat()} 00:00:00"
    if isinstance(value, str) and len(value) >= 10 and value[4] == "-" and value[7] == "-":
        text = value.replace("T", " ")
        assert "+" not in text[10:], f"zoned result {value!r}"
        moment = datetime.fromisoformat(text if len(text) > 10 else f"{text} 00:00:00")
        return moment.isoformat(sep=" ")  # ISO text in one spelling: a zero fraction dropped
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        return round(float(value), 6)
    return value


def _rows(resp: SlayerResponse, *, ordered: bool) -> list[tuple]:
    out = [tuple(_cell(r[c]) for c in resp.columns) for r in resp.data]
    return out if ordered else sorted(out, key=repr)


async def _run(engine: SlayerQueryEngine, query: list[dict[str, Any]]) -> SlayerResponse:
    return await engine.execute([SlayerQuery.model_validate(s) for s in query])


async def _mismatches(zoned: SlayerQueryEngine, reference: SlayerQueryEngine, *, kind: Kind,
                      zone: str, reference_zone: Optional[str]) -> list[str]:
    out: list[str] = []
    for position in POSITIONS:
        got = await _run(zoned, _query(kind, position, subject=kind.subject, zone=zone))
        want = await _run(reference, _query(kind, position, subject=kind.reference, zone=reference_zone))
        ordered = position in ORDERED
        if _rows(got, ordered=ordered) != _rows(want, ordered=ordered):
            out.append(f"{position}: got {_rows(got, ordered=ordered)}, want {_rows(want, ordered=ordered)}\n"
                       f"  sql: {got.sql}")
    return out


# ---------------------------------------------------------------------------
# The position matrix
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("zone", ZONES)
@pytest.mark.parametrize("kind", CONVERTED_KINDS, ids=lambda k: k.name)
async def test_every_position_reads_the_zone_wall_clock(backend, zone, kind) -> None:
    async with _engine(backend, zone=None) as zoned, _engine(backend, zone=zone) as reference:
        bad = await _mismatches(zoned, reference, kind=kind, zone=zone, reference_zone=None)
    assert not bad, "\n".join(bad)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("zone", ZONES)
@pytest.mark.parametrize("kind", STORED_KINDS, ids=lambda k: k.name)
async def test_civil_and_undeclared_reads_are_stored_values(backend, zone, kind) -> None:
    ds_zone = None if kind.name == "undeclared" else "UTC"
    clock = PinnedClock(datetime.fromisoformat(CLOCK))
    async with tz_engine(backend, datasets=_datasets(), tables=_tables(None), ds_zone=ds_zone, clock=clock) as engine:
        clock.now = instant(CLOCK)
        zoned_bad: list[str] = []
        for position in POSITIONS:
            if position == "relative_token":
                continue  # the zone moves "today" itself
            got = await _run(engine, _query(kind, position, subject=kind.subject, zone=zone))
            want = await _run(engine, _query(kind, position, subject=kind.subject, zone=None))
            ordered = position in ORDERED
            if _rows(got, ordered=ordered) != _rows(want, ordered=ordered):
                zoned_bad.append(f"{position}: got {_rows(got, ordered=ordered)}, want {_rows(want, ordered=ordered)}")
    assert not zoned_bad, "\n".join(zoned_bad)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("zone", ZONES)
async def test_join_keys_convert_on_both_sides(backend, zone) -> None:
    async with _engine(backend, zone=None) as zoned, _engine(backend, zone=zone) as reference:
        for pop in ("orders", "orders_by_paid"):
            query = {"population": pop, "dimensions": [f"{pop}.id", "marks.label"]}
            got = await _run(zoned, [{**query, "time_zone": zone}])
            want = await _run(reference, [{"population": "orders", "dimensions": ["orders.id", "marks.label"]}])
            assert _rows(got, ordered=False) == _rows(want, ordered=False), got.sql
            assert {r["marks.label"] for r in got.data} == {"first", "month_end", None}, got.sql


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("zone", ZONES)
async def test_spine_reads_the_zone_wall_clock(backend, zone) -> None:
    spine = {"field": "time_spine.timestamp", "granularity": "month", "date_range": ["2024-12-01", "2025-03-31"]}
    async with _engine(backend, zone=None) as zoned, _engine(backend, zone=zone) as reference:
        for pop in ("orders", "orders_by_paid"):
            got = await _run(zoned, [{"population": pop, "time_zone": zone, "time_dimensions": [spine],
                                      "metrics": [m(f"sum({pop}.amount)", "a")]}])
            want = await _run(reference, [{"population": "orders", "time_dimensions": [spine],
                                           "metrics": [m("sum(orders.amount)", "a")]}])
            assert _rows(got, ordered=False) == _rows(want, ordered=False), got.sql


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("zone", ZONES)
async def test_day_buckets_across_the_dst_transitions(backend, zone) -> None:
    async with _engine(backend, zone=None) as engine:
        resp = await engine.execute(SlayerQuery.model_validate({
            "population": "hourly", "time_zone": zone, "metrics": [m("count(hourly.*)", "n")],
            "time_dimensions": [{"field": "hourly.ts", "granularity": "day"}],
        }))
    expected = Counter(to_zone(HOURLY_START + timedelta(hours=h), zone).date() for h in range(HOURLY_COUNT))
    assert {day(r["hourly.ts"]): int(r["n"]) for r in resp.data} == dict(expected), resp.sql


# ---------------------------------------------------------------------------
# Known bypasses of the read path, one focused test each
# ---------------------------------------------------------------------------

BYPASSES: dict[str, dict[str, Any]] = {
    "aggregate_input": {"population": "orders", "metrics": [
        m("max(orders.ordered_at)", "hi"), m("count_distinct(date_trunc('day', orders.ordered_at))", "days")]},
    "derived_inner_read": {"population": "orders", "dimensions": [
        "orders.id", d("date_add(orders.ordered_derived, 1, 'hour')", "v")]},
    "semi_join_key": {"population": "marks_many", "dimensions": ["marks_many.label", "marks_many.marked_at"],
                      "filter": ["orders.amount >= 30"]},
    "dataset_metric_anchor": {"population": "orders", "metrics": ["orders.latest"]},
    "extension_metric_anchor": {"population": "orders", "metrics": ["orders.first_seen"], "extensions": [
        {"dataset": "orders", "metrics": [{"name": "first_seen", "expression": "min(ordered_at)"}]}]},
}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("bypass", sorted(BYPASSES))
async def test_known_bypass_reads_the_zone_wall_clock(backend, bypass) -> None:
    query = BYPASSES[bypass]
    async with _engine(backend, zone=None) as zoned, _engine(backend, zone=NY) as reference:
        got = await _run(zoned, [{**query, "time_zone": NY}])
        want = await _run(reference, [query])
    assert _rows(got, ordered=False) == _rows(want, ordered=False), got.sql
    assert got.data, got.sql


@pytest.mark.parametrize("backend", BACKENDS)
async def test_session_policy_predicate_reads_stored_values(backend) -> None:
    policy_value = ROWS[1][1]  # row 1's stored UTC wall clock
    policy = SessionPolicy(ruleset=ColumnFilterRuleset(column="ordered_at", value=policy_value))
    async with tz_engine(backend, datasets=_datasets(), tables=_tables(None), policy=policy) as engine:
        resp = await engine.execute(SlayerQuery.model_validate({
            "population": "orders", "time_zone": "Asia/Tokyo", "metrics": [m("count(orders.*)", "n")]}))
    assert int(resp.data[0]["n"]) == 1, resp.sql


# ---------------------------------------------------------------------------
# The AST-level read check
# ---------------------------------------------------------------------------

def test_the_check_is_enabled_in_the_test_harness() -> None:
    assert os.environ.get("SLAYER_VALIDATE_ZONED_READS") == "1"


async def test_an_untagged_read_fails_the_check(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(zoned_read_check, "read_tag", lambda node: None)
    query = SlayerQuery.model_validate({"population": "orders", "time_zone": NY, "dimensions": ["orders.ordered_at"]})
    async with _engine("duckdb", zone=None) as engine:
        with pytest.raises(ZonedReadLeakError, match="ordered_at"):
            await engine.execute(query, dry_run=True)


async def test_the_check_passes_a_zoned_query() -> None:
    async with _engine("duckdb", zone=None) as engine:
        resp = await engine.execute(SlayerQuery.model_validate({
            "population": "orders", "time_zone": NY, "dimensions": ["orders.ordered_at", "orders.paid_at"],
            "filter": ["orders.ordered_at >= '2025-01-01'"]}), dry_run=True)
    assert resp.sql
