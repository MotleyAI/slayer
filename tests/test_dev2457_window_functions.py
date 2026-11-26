"""Window functions in Mode B: the functions read, and a row-level window as a windowed column of one dataset.

Spec: queries/window-functions › A window function is read in every Mode B position; › A window with no
aggregate-valued operand is a windowed column of one dataset; › A window in a field's filter masks on its computed
value; queries/expression-grammar › Forms rejected with a hint (OVER is not a rejected form).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core import syntax
from slayer.core.enums import DataType
from slayer.core.errors import IllegalWindowInFilterError, PositionTypingError, QueryTypeError
from slayer.core.models import SlayerDataset
from slayer.core.policy import ColumnFilterRuleset, SessionPolicy
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2000_fixtures import DS as ZONED_DS
from tests._dev2000_fixtures import Table, tz_engine
from tests._dev2101_fixtures import emit
from tests._dev2457_fixtures import (
    BACKENDS,
    RN,
    RN_INLINE,
    RN_OF_ID,
    by,
    col,
    column,
    day,
    floats,
    grouped,
    ints,
    orders_dataset,
    raw,
    run,
    window_datasets,
    window_engine,
)
from tests._ossie_gate import TIER1

MASK = "row_number() over (partition by region order by ordered_at) = 1"
EVENT_DAY_RN = "ROW_NUMBER() OVER (PARTITION BY CAST(events.at AS DATE) ORDER BY events.id)"
LAST_TWO = "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW"


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
async def engine(backend: str) -> AsyncIterator[SlayerQueryEngine]:
    async with window_engine(backend) as eng:
        yield eng


def _per_order(*values: Any) -> dict[int, Any]:
    """``values`` for orders 1 to 7, rounded as ``floats`` rounds."""
    return floats(dict(zip(range(1, 8), values, strict=True)))


async def _values(engine: SlayerQueryEngine, expression: str, **extra: Any) -> dict[int, Any]:
    return floats(await column(engine, "orders", expression, **extra))


def _metrics(*metrics: str, **extra: Any) -> dict[str, Any]:
    return {"population": "orders", "metrics": [{"metric": m, "alias": f"m{i}"} for i, m in enumerate(metrics)], **extra}


# --------------------------------------------------------------------------- #
# A window function is read in every Mode B position
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("expression", "expected"), [
    ("ROW_NUMBER() OVER (ORDER BY orders.amount)", _per_order(1, 2, 3, 4, 5, 6, 7)),
    ("RANK() OVER (ORDER BY orders.ordered_at)", _per_order(1, 4, 6, 1, 4, 6, 1)),
    ("DENSE_RANK() OVER (ORDER BY orders.ordered_at)", _per_order(1, 2, 3, 1, 2, 3, 1)),
    ("NTILE(2) OVER (ORDER BY orders.amount)", _per_order(1, 1, 1, 1, 2, 2, 2)),
    ("PERCENT_RANK() OVER (ORDER BY orders.amount)", _per_order(*((i - 1) / 6 for i in range(1, 8)))),
    ("CUME_DIST() OVER (ORDER BY orders.amount)", _per_order(*(i / 7 for i in range(1, 8)))),
], ids=["row_number", "rank", "dense_rank", "ntile", "percent_rank", "cume_dist"])
async def test_each_ranking_function(engine, expression, expected):
    assert await _values(engine, expression) == expected


@pytest.mark.parametrize(("expression", "expected"), [
    ("LAG(orders.amount, 1, 0) OVER (ORDER BY orders.amount)", _per_order(0, 10, 20, 30, 40, 50, 60)),
    ("LEAD(orders.amount, 1, 0) OVER (ORDER BY orders.amount)", _per_order(20, 30, 40, 50, 60, 70, 0)),
    ("LAG(orders.amount) OVER (ORDER BY orders.amount)", _per_order(None, 10, 20, 30, 40, 50, 60)),
    ("FIRST_VALUE(orders.amount) OVER (ORDER BY orders.amount)", _per_order(*[10] * 7)),
    ("LAST_VALUE(orders.amount) OVER (ORDER BY orders.amount)", _per_order(10, 20, 30, 40, 50, 60, 70)),
    ("NTH_VALUE(orders.amount, 2) OVER (ORDER BY orders.amount)", _per_order(None, *[20] * 6)),
], ids=["lag_default", "lead_default", "lag", "first_value", "last_value", "nth_value"])
async def test_each_offset_function(engine, expression, expected):
    assert await _values(engine, expression) == expected


@pytest.mark.parametrize(("expression", "expected"), [
    ("SUM(orders.amount) OVER (PARTITION BY orders.region)", _per_order(60, 60, 60, 150, 150, 150, 70)),
    ("COUNT(*) OVER (PARTITION BY orders.region)", _per_order(3, 3, 3, 3, 3, 3, 1)),
    ("orders.amount / SUM(orders.amount) OVER () * 100", _per_order(*(i * 10 / 2.8 for i in range(1, 8)))),
    ("AVG(orders.amount) OVER (ORDER BY orders.id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)",
     _per_order(10, 15, 20, 25, 30, 35, 40)),
    ("SUM(orders.amount) FILTER (WHERE orders.amount > 10) OVER (PARTITION BY orders.region)",
     _per_order(50, 50, 50, 150, 150, 150, 70)),
], ids=["sum", "count_star", "share", "trailing_avg", "filtered_sum"])
async def test_window_aggregations(engine, expression, expected):
    assert await _values(engine, expression) == expected


@pytest.mark.parametrize(("expression", "expected"), [
    ("SUM(orders.amount) OVER (ORDER BY orders.id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
     _per_order(10, 30, 60, 100, 150, 210, 280)),
    ("SUM(orders.amount) OVER (ORDER BY orders.id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)",
     _per_order(30, 60, 90, 120, 150, 180, 130)),
    ("SUM(orders.amount) OVER (ORDER BY orders.ordered_at RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
     _per_order(120, 190, 280, 120, 190, 280, 120)),
], ids=["rows_running", "rows_centered", "range_running"])
async def test_the_three_listed_frames(engine, expression, expected):
    assert await _values(engine, expression) == expected


async def test_a_frame_on_a_ranking_function_changes_nothing(engine):
    got = await _values(engine, "RANK() OVER (ORDER BY orders.amount ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)")
    assert got == _per_order(1, 2, 3, 4, 5, 6, 7)


def _is_lag(node: Expression) -> bool:
    return isinstance(node, exp.Lag) or (isinstance(node, exp.Anonymous) and node.name.lower() == "laginframe")


def _is_whole_partition(spec: exp.WindowSpec) -> bool:
    return (spec.args.get("kind") == "ROWS" and spec.args.get("start") == "UNBOUNDED"
            and spec.args.get("end") == "UNBOUNDED" and spec.args.get("end_side") == "FOLLOWING")


async def _emitted_windows(dialect: str, expression: str, picks: Any) -> list[exp.Window]:
    sql = await emit(SlayerQuery.model_validate(raw("orders", expression)), dialect=dialect,
                     datasets=window_datasets())
    windows = [w for w in sqlglot.parse_one(sql, dialect=dialect).find_all(exp.Window) if picks(w.this)]
    assert windows, sql
    return windows


@pytest.mark.parametrize("dialect", TIER1)
@pytest.mark.parametrize(("expression", "picks"), [
    (f"RANK() OVER (ORDER BY orders.amount {LAST_TWO})", lambda n: isinstance(n, exp.Rank)),
    (f"LAG(orders.amount) OVER (ORDER BY orders.amount {LAST_TWO})", _is_lag),
], ids=["rank", "lag"])
async def test_a_frame_on_a_ranking_or_offset_function_is_dropped(dialect, expression, picks):
    for window in await _emitted_windows(dialect, expression, picks):
        spec = window.args.get("spec")
        # ClickHouse's in-frame LAG needs the whole partition as its frame: that one is the rewrite's, not the query's.
        if spec is not None and dialect == "clickhouse" and _is_lag(window.this):
            assert _is_whole_partition(spec), window.sql(dialect=dialect)
            continue
        assert spec is None or not spec.args.get("kind"), window.sql(dialect=dialect)


@pytest.mark.parametrize("dialect", TIER1)
async def test_a_frame_on_an_aggregate_window_is_kept(dialect):
    windows = await _emitted_windows(dialect, f"SUM(orders.amount) OVER (ORDER BY orders.id {LAST_TWO})",
                                     lambda n: isinstance(n, exp.Sum))
    for window in windows:
        spec = window.args.get("spec")
        assert spec is not None, window.sql(dialect=dialect)
        assert (spec.args.get("kind"), str(spec.args.get("start")), spec.args.get("start_side"),
                spec.args.get("end")) == ("ROWS", "1", "PRECEDING", "CURRENT ROW"), window.sql(dialect=dialect)


@pytest.mark.parametrize(("expression", "named"), [
    ("last(orders.amount, orders.ordered_at) OVER ()", r"(?i)\blast\b"),
    ("collect(orders.region) OVER ()", r"(?i)\bcollect\b"),
    ("UPPER(orders.region) OVER ()", r"(?i)\bupper\b"),
], ids=["last", "collect", "upper"])
async def test_a_function_ossie_does_not_window_is_refused(engine, expression, named):
    body = grouped("orders", expression, "sum(orders.amount)")
    with pytest.raises(QueryTypeError, match=named) as exc:
        await run(engine, body)
    assert not isinstance(exc.value, IllegalWindowInFilterError)


@pytest.mark.parametrize(("expression", "named"), [
    ("ROW_NUMBER(orders.amount) OVER ()", r"(?i)row_number"),
    ("orders.amount OVER ()", r"(?i)\bover\b"),
    ("NTILE(0) OVER (ORDER BY orders.amount)", r"(?i)ntile"),
    ("NTILE(orders.id) OVER (ORDER BY orders.amount)", r"(?i)ntile"),
    ("LAG(orders.amount, -1) OVER (ORDER BY orders.amount)", r"(?i)\blag\b"),
    ("NTH_VALUE(orders.amount, 0) OVER (ORDER BY orders.amount)", r"(?i)nth_value"),
    ("SUM(orders.amount) OVER (ORDER BY orders.id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)", r"(?i)frame"),
    ("SUM(orders.amount) OVER (ORDER BY orders.id, orders.amount RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)",
     r"(?i)\brange\b"),
], ids=["argument_count", "over_a_column", "ntile_zero", "ntile_not_literal", "negative_lag_offset",
        "nth_value_zero", "frame_out_of_order", "range_offset_two_keys"])
async def test_malformed_windows_are_refused(engine, expression, named):
    body = grouped("orders", expression, "sum(orders.amount)")
    with pytest.raises(ValueError, match=named) as exc:
        await run(engine, body, dry_run=True)
    assert not isinstance(exc.value, IllegalWindowInFilterError)


async def test_a_window_inside_a_window(engine):
    got = await _values(engine, f"MAX({RN_INLINE}) OVER ()")
    assert got == _per_order(*[3] * 7)


# --------------------------------------------------------------------------- #
# A window with no aggregate-valued operand is a windowed column of one dataset
# --------------------------------------------------------------------------- #
async def test_inline_as_a_dimension(engine):
    resp = await run(engine, grouped("orders", RN_INLINE, "sum(orders.amount)"))
    assert ints(by(resp, "d", "m")) == {1: 120, 2: 70, 3: 90}


async def test_a_query_filter_does_not_change_the_window(engine):
    got = await column(engine, "orders", RN_INLINE, filter=["orders.id > 1"])
    assert ints(got) == {i: RN_OF_ID[i] for i in range(2, 8)}


async def test_inline_as_a_query_filter(engine):
    resp = await run(engine, grouped("orders", "orders.region", "sum(orders.amount)", filter=[f"{RN_INLINE} = 1"]))
    assert ints(by(resp, "d", "m")) == {"EU": 10, "US": 40, "AS": 70}


async def test_inline_as_an_aggregation_source_and_inside_filter(engine):
    resp = await run(engine, _metrics(f"max({RN_INLINE})", f"sum(orders.amount) FILTER (WHERE {RN_INLINE} = 1)"))
    assert [(int(r["m0"]), int(r["m1"])) for r in resp.data] == [(3, 120)]


async def test_inline_inside_a_case(engine):
    resp = await run(engine, _metrics(f"sum(CASE WHEN {RN_INLINE} = 1 THEN orders.amount ELSE 0 END)"))
    assert [int(r["m0"]) for r in resp.data] == [120]


async def test_inline_as_a_partition_key(engine):
    resp = await run(engine, {
        "population": "orders",
        "dimensions": [{"field": "orders.region", "alias": "region"}, {"field": RN_INLINE, "alias": "rn"}],
        "metrics": [{"metric": f"sum(orders.amount, partition_by={RN_INLINE})", "alias": "m"}]})
    assert len(resp.data) == 7
    assert {(int(r["rn"]), int(r["m"])) for r in resp.data} == {(1, 120), (2, 70), (3, 90)}


async def test_inline_as_a_ranking_key(engine):
    resp = await run(engine, grouped("orders", "orders.region", f"last(orders.amount, {RN_INLINE})"))
    assert ints(by(resp, "d", "m")) == {"EU": 30, "US": 60, "AS": 70}


async def test_inline_as_an_order_key_of_a_grouped_query(engine):
    resp = await run(engine, grouped("orders", "orders.region", "sum(orders.amount)", order=[
        {"field": RN_INLINE, "direction": "desc"}, {"field": "orders.region", "direction": "asc"}]))
    assert [r["d"] for r in resp.data] == ["EU", "US", "AS"]


async def test_inline_as_a_time_dimension(engine):
    previous = "LAG(orders.ordered_at) OVER (PARTITION BY orders.region ORDER BY orders.ordered_at)"
    resp = await run(engine, _metrics("count(orders.*)", time_dimensions=[
        {"field": previous, "granularity": "day", "alias": "t"}]))
    assert {day(r["t"]): int(r["m0"]) for r in resp.data} == {None: 3, "2024-01-01": 2, "2024-01-02": 2}


async def test_the_anchor_decides_whose_rows_are_numbered(engine):
    resp = await run(engine, raw("orders", "RANK() OVER (ORDER BY orders.customers.id DESC)",
                                 "RANK() OVER (ORDER BY customers.id DESC)"))
    assert floats(by(resp)) == _per_order(5, 5, 3, 3, 1, 1, 5)
    assert floats(by(resp, value="v1")) == _per_order(4, 4, 3, 3, 2, 2, 4)


async def test_a_window_reading_a_to_one_joined_field(engine):
    got = await _values(engine, "ROW_NUMBER() OVER (PARTITION BY orders.customers.tier ORDER BY orders.id)")
    assert got == _per_order(1, 2, 1, 2, 3, 4, 5)


async def test_inline_equals_the_field(engine, backend):
    await engine.storage.save_dataset(orders_dataset(fields=[col("rn", expression=RN)]), _validate=False)
    body = raw("orders", "orders.rn", RN_INLINE)
    resp = await run(engine, body)
    assert ints(by(resp)) == ints(by(resp, value="v1")) == RN_OF_ID
    dry = await run(engine, body, dry_run=True)
    assert dry.sql is not None
    assert len(list(sqlglot.parse_one(dry.sql, dialect=backend).find_all(exp.Window))) == 1, dry.sql


@pytest.mark.parametrize("expression", ["ROW_NUMBER() OVER ()", "ROW_NUMBER() OVER (ORDER BY time_spine.timestamp)"],
                         ids=["no_reference", "time_spine_only"])
async def test_a_window_with_no_reference_is_refused(engine, expression):
    body = grouped("orders", expression, "sum(orders.amount)")
    with pytest.raises(QueryTypeError, match=r"(?i)\bno\b.*\bdataset") as exc:
        await run(engine, body)
    assert not isinstance(exc.value, IllegalWindowInFilterError)
    assert "ROW_NUMBER" in str(exc.value).upper()


async def test_a_window_over_a_query_stage(engine):
    stage = {"name": "by_region", "population": "orders", "dimensions": ["orders.region"],
             "metrics": [{"metric": "sum(orders.amount)", "alias": "total"}]}
    final = {"population": "by_region", "distinct_dimension_values": False, "dimensions": [
        {"field": "by_region.region", "alias": "region"},
        {"field": "RANK() OVER (ORDER BY by_region.total DESC)", "alias": "v0"}]}
    resp = await engine.execute([stage, final])
    assert ints(by(resp, key="region")) == {"US": 1, "AS": 2, "EU": 3}


async def test_a_row_level_window_as_a_metric(engine):
    rounded = grouped("orders", "orders.region", "round(orders.amount, 2)")
    windowed = grouped("orders", "orders.region", "SUM(orders.amount) OVER (PARTITION BY orders.region)")
    with pytest.raises(PositionTypingError, match="amount") as reference:
        await run(engine, rounded)
    with pytest.raises(PositionTypingError, match="amount") as exc:
        await run(engine, windowed)
    assert type(exc.value) is type(reference.value)
    assert not isinstance(exc.value, IllegalWindowInFilterError)


@pytest.mark.parametrize(("zone", "expected"), [("Europe/Berlin", {1: 1, 2: 1}), (None, {1: 1, 2: 2})],
                         ids=["berlin", "no_zone"])
async def test_the_query_zone_applies_inside_the_window(backend, zone, expected):
    table = Table(name="events", columns=[("id", "INT"), ("happened_at", "TIMESTAMP")],
                  rows=[(1, "2024-01-01 22:30:00"), (2, "2024-01-01 23:30:00")])
    events = SlayerDataset(name="events", sql_table="events", data_source=ZONED_DS, fields=[
        col("id", primary_key=True), col("at", DataType.DATETIME, expression="happened_at")])
    extra = {} if zone is None else {"time_zone": zone}
    async with tz_engine(backend, datasets=[events], tables=[table], ds_zone="UTC") as zoned:
        resp = await run(zoned, raw("events", EVENT_DAY_RN, **extra))
    assert ints(by(resp)) == expected


async def test_filters_beneath_the_window_see_stored_values(backend):
    table = Table(name="events", columns=[("id", "INT"), ("happened_at", "TIMESTAMP")],
                  rows=[(1, "2024-01-01 22:30:00"), (2, "2024-01-01 23:30:00"), (3, "2024-01-02 01:30:00")])
    events = SlayerDataset(name="events", sql_table="events", data_source=ZONED_DS,
                           filters=["events.at < '2024-01-02 00:00:00'"], fields=[
                               col("id", primary_key=True), col("at", DataType.DATETIME, expression="happened_at")])
    async with tz_engine(backend, datasets=[events], tables=[table], ds_zone="UTC") as zoned:
        resp = await run(zoned, raw("events", "COUNT(*) OVER ()", EVENT_DAY_RN, time_zone="Europe/Berlin"))
    # Stored UTC the filter keeps 1 and 2 (Berlin's 00:30 on 2 Jan would drop 2): two rows, on two Berlin days.
    assert ints(by(resp)) == {1: 2, 2: 2}
    assert ints(by(resp, value="v1")) == {1: 1, 2: 1}


async def test_the_session_policy_restricts_the_rows_before_the_window(engine):
    policy = SessionPolicy(ruleset=ColumnFilterRuleset(column="region", value="US", on_unapplicable="pass"))
    scoped = SlayerQueryEngine(storage=engine.storage, policy=policy)
    try:
        resp = await run(scoped, raw("orders", "ROW_NUMBER() OVER (ORDER BY orders.id)"))
    finally:
        scoped.close()
    assert ints(by(resp)) == {4: 1, 5: 2, 6: 3}


# --------------------------------------------------------------------------- #
# A window in a field's filter masks on its computed value
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("door", ["engine", "storage"])
async def test_a_mask_on_a_window(engine, door):
    save = engine.save_dataset if door == "engine" else engine.storage.save_dataset
    await save(orders_dataset(name="orders_masked", fields=[col("first_amount", expression="amount", filter=MASK)]))
    assert await engine.storage.get_dataset("orders_masked", data_source="test") is not None
    resp = await run(engine, {"population": "orders_masked",
                              "metrics": [{"metric": "sum(orders_masked.first_amount)", "alias": "m"}]})
    assert [int(r["m"]) for r in resp.data] == [120]


# --------------------------------------------------------------------------- #
# queries/expression-grammar
# --------------------------------------------------------------------------- #
def test_over_is_not_a_rejected_form():
    parsed = syntax.parse_expr("ROW_NUMBER() OVER (ORDER BY orders.amount)")
    assert isinstance(parsed, syntax.WindowCall)
    assert isinstance(parsed.function, syntax.ScalarCall)
    assert parsed.function.name.lower() == "row_number"
