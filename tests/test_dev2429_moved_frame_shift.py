"""time_shift evaluates its operand in the moved frame (spec: queries/transforms), on the moved-frame seed."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import date, datetime
from typing import Any, Optional

import pytest

from slayer.core.enums import TimeGranularity
from slayer.core.keys import ArithmeticKey, FieldKey, LiteralKey, TimeTruncKey
from slayer.engine.compile.shift import moved_frame
from slayer.engine.plan import plan_query
from slayer.ir.bound import bound_filter_from_key
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.planned import PlannedQuery, RegroupAttachPlan, regroup_producer_identity
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev2429_fixtures import (
    AMOUNT,
    BACKENDS,
    FEB_MAY,
    JAN_APR,
    REWRITE,
    cells,
    datasets,
    months,
    moved_frame_engine,
    query,
)

SHIFT = f"time_shift({AMOUNT}, -1)"
SHIFT_RW = f"time_shift({REWRITE}, -1)"
SHIFTED_AMOUNT = {"2024-02": 30.0, "2024-04": None, "2024-05": 30.0}
CHANGE = {"2024-02": 10.0, "2024-04": None, "2024-05": -25.0}


@pytest.fixture(params=BACKENDS)
async def engine(request: pytest.FixtureRequest) -> AsyncIterator[SlayerQueryEngine]:
    async with moved_frame_engine(request.param) as eng:
        yield eng


async def _run(engine: SlayerQueryEngine, *metrics: str, region: str = "North",
               **kw: Any) -> list[dict[str, Optional[float]]]:
    """One ``{YYYY-MM: value}`` per metric, for ``region``."""
    resp = await engine.execute(query(*metrics, **kw))
    return [cells(resp, f"m{i}", region=region) for i in range(len(metrics))]


def _approx(got: dict[str, Optional[float]], expected: dict[str, Optional[float]]) -> None:
    assert set(got) == set(expected), got
    for month, value in expected.items():
        assert got[month] == (None if value is None else pytest.approx(value)), (month, got)


def _back(month: str, n: int = 1) -> str:
    year, mon = int(month[:4]), int(month[5:7]) - n
    while mon < 1:
        year, mon = year - 1, mon + 12
    return f"{year:04d}-{mon:02d}"


class TestEqualRewrites:
    async def test_an_equal_rewrite_shifts_to_the_same_value(self, engine) -> None:
        plain, rewrite = await _run(engine, SHIFT, SHIFT_RW)
        assert plain == rewrite == SHIFTED_AMOUNT

    async def test_change_and_change_pct_of_an_equal_rewrite_agree(self, engine) -> None:
        got = await _run(engine, f"change({AMOUNT})", f"change({REWRITE})",
                         f"change_pct({AMOUNT})", f"change_pct({REWRITE})")
        for change in got[:2]:
            _approx(change, CHANGE)
        for pct in got[2:]:
            _approx(pct, {"2024-02": 1 / 3, "2024-04": None, "2024-05": -5 / 6})

    async def test_a_null_absorbing_equal_rewrite_agrees(self, engine) -> None:
        plain, rewrite = await _run(engine, f"time_shift(coalesce({AMOUNT}, 0), -1)",
                                    f"time_shift(coalesce({AMOUNT}, 0) + 0 * cumsum({AMOUNT}), -1)")
        assert plain == rewrite == SHIFTED_AMOUNT


class TestFrameRelativeOperands:
    async def test_a_shifted_running_total_reads_the_moved_frame(self, engine) -> None:
        shifted, running = await _run(engine, f"time_shift(cumsum({AMOUNT}), -1)", f"cumsum({AMOUNT})")
        assert shifted == {"2024-02": 30.0, "2024-04": None, "2024-05": 100.0}
        assert running == {"2024-02": 40.0, "2024-04": 70.0, "2024-05": 75.0}

    async def test_two_offsets_read_their_own_moved_frames(self, engine) -> None:
        one, two = await _run(engine, f"time_shift(cumsum({AMOUNT}), -1)", f"time_shift(cumsum({AMOUNT}), -2)")
        assert one == {"2024-02": 30.0, "2024-04": None, "2024-05": 100.0}
        assert two == {"2024-02": 20.0, "2024-04": 90.0, "2024-05": None}

    async def test_a_shifted_lag_steps_over_the_moved_frame(self, engine) -> None:
        lag, shifted = await _run(engine, f"lag({AMOUNT})", f"time_shift(lag({AMOUNT}), -1)")
        assert lag == {"2024-02": None, "2024-04": 40.0, "2024-05": 30.0}
        assert shifted == {"2024-02": None, "2024-04": None, "2024-05": 40.0}

    async def test_a_shifted_lead_steps_over_the_moved_frame(self, engine) -> None:
        [got] = await _run(engine, f"time_shift(lead({AMOUNT}), -1)")
        assert got == {"2024-02": 40.0, "2024-04": None, "2024-05": None}  # May's source April is the frame's end

    async def test_a_shifted_streak_counts_over_the_moved_frame(self, engine) -> None:
        [got] = await _run(engine, f"time_shift(consecutive_periods({AMOUNT} > 25), -1)")
        assert got == {"2024-02": 1.0, "2024-04": None, "2024-05": 1.0}

    async def test_a_shifted_rank_equals_the_moved_query(self, engine) -> None:
        rank = f"rank({AMOUNT}, direction='desc')"
        [shifted] = await _run(engine, f"time_shift({rank}, -1)")
        [moved] = await _run(engine, rank, date_range=JAN_APR)
        assert shifted == {m: moved.get(_back(m)) for m in shifted}
        assert shifted["2024-02"] is not None

    async def test_an_axis_free_constituent_keeps_its_in_frame_value(self, engine) -> None:
        [got] = await _run(engine, f"time_shift(({REWRITE}) / sum(orders.amount, partition_by=[orders.region]), -1)")
        _approx(got, {"2024-02": 0.4, "2024-04": None, "2024-05": 0.4})  # 30 / North's Feb–May total 75

    async def test_a_nested_shift_reads_outside_the_frame(self, engine) -> None:
        [got] = await _run(engine, f"time_shift(time_shift({AMOUNT}, -1), -1)")
        assert got == {"2024-02": 20.0, "2024-04": None, "2024-05": None}


class TestOutsideTheFrame:
    async def test_a_shifted_predicate(self, engine) -> None:
        [got] = await _run(engine, f"time_shift({AMOUNT} > 35, -1)")
        assert {k: None if v is None else bool(v) for k, v in got.items()} == {
            "2024-02": False, "2024-04": None, "2024-05": False}

    async def test_a_cross_model_operand_equals_the_moved_query(self, engine) -> None:
        operands = ["sum(customers.spend)", f"{AMOUNT} + sum(customers.spend)"]
        shifted = await _run(engine, *(f"time_shift({o}, -1)" for o in operands))
        moved = await _run(engine, *operands, date_range=JAN_APR)
        for got, source in zip(shifted, moved):
            assert got == {m: source.get(_back(m)) for m in got}
            assert got["2024-02"] is not None

    async def test_a_forward_shift_reads_after_the_frame(self, engine) -> None:
        [got] = await _run(engine, f"time_shift({AMOUNT}, 1)")
        assert got == {"2024-02": None, "2024-04": 5.0, "2024-05": 7.0}

    @pytest.mark.parametrize(("spine", "expected"), [
        (False, SHIFTED_AMOUNT),
        (True, {**SHIFTED_AMOUNT, "2024-03": 40.0}),
    ], ids=["plain", "spine"])
    async def test_an_unaligned_shift_reads_the_whole_source_bucket(
        self, engine, spine: bool, expected: dict[str, Optional[float]],
    ) -> None:
        [got] = await _run(engine, f"time_shift({AMOUNT}, -1, 'day')", spine=spine)
        assert got == expected

    async def test_a_shifted_windowed_operand_reads_before_the_moved_frame(self, engine) -> None:
        window = "sum(orders.amount, window='60d')"
        [shifted] = await _run(engine, f"time_shift({window}, -1)")
        [unbounded] = await _run(engine, window, date_range=None)
        assert shifted["2024-02"] == unbounded["2024-01"] == 50.0  # December's 20 + January's 30
        assert shifted["2024-05"] == unbounded["2024-04"] == 30.0

    async def test_a_year_over_year_running_total(self, engine) -> None:
        [shifted] = await _run(engine, f"time_shift(cumsum({AMOUNT}), -1, 'year')", date_range="2025", region="East")
        [prior] = await _run(engine, f"cumsum({AMOUNT})", date_range="2024", region="East")
        assert shifted == {m: prior.get(f"2024{m[4:]}") for m in shifted}
        assert shifted == {"2025-02": 3.0, "2025-03": 13.0}


class TestMovedBounds:
    async def test_an_open_upper_frame_moves_its_lower_bound(self, engine) -> None:
        [got] = await _run(engine, f"time_shift(cumsum({AMOUNT}), -1)", date_range=["2024-02-01", None])
        assert got == {"2024-02": 30.0, "2024-04": None, "2024-05": 100.0, "2024-06": 105.0}

    async def test_an_open_lower_frame_moves_its_upper_bound(self, engine) -> None:
        [got] = await _run(engine, f"time_shift({REWRITE}, 1)", date_range=[None, "2024-04-30"])
        assert got == {"2023-12": 30.0, "2024-01": 40.0, "2024-02": None, "2024-04": 5.0}

    async def test_a_relational_frame_bound_moves_beside_date_range(self, engine) -> None:
        [got] = await _run(engine, f"time_shift(cumsum({AMOUNT}), -1)", date_range=["2024-01-01", "2024-05-31"],
                           filter=["orders.ordered_at >= '2024-02-01'"])
        assert got == {"2024-02": 30.0, "2024-04": None, "2024-05": 100.0}

    @pytest.mark.parametrize("shift", [SHIFT, SHIFT_RW], ids=["plain", "rewrite"])
    async def test_a_frame_bound_on_another_time_dimension_stays(self, engine, shift: str) -> None:
        [got] = await _run(engine, shift, filter=["orders.shipped_at >= '2024-01-15'"],
                           extra_time_dimensions=({"field": "orders.shipped_at", "granularity": "year"},))
        assert got == {"2024-02": None, "2024-04": None, "2024-05": 30.0}  # January's row shipped 01-12


class TestRouting:
    @pytest.mark.parametrize("shift", [SHIFT, SHIFT_RW], ids=["plain", "rewrite"])
    async def test_a_filter_only_shift(self, engine, shift: str) -> None:
        [got] = await _run(engine, AMOUNT, filter=[f"{shift} > 25"])
        assert got == {"2024-02": 40.0, "2024-05": 5.0}

    @pytest.mark.parametrize("shift", [SHIFT, SHIFT_RW], ids=["plain", "rewrite"])
    async def test_an_order_only_shift(self, engine, shift: str) -> None:
        resp = await engine.execute(query(AMOUNT, order=[{"field": shift, "direction": "asc", "nulls": "first"},
                                                         {"field": "orders.ordered_at", "direction": "desc"}]))
        assert months(resp) == ["2024-04", "2024-05", "2024-02"]

    async def test_a_shift_hidden_in_a_composite_measure(self, engine) -> None:
        [got] = await _run(engine, f"{AMOUNT} - {SHIFT_RW}")
        assert got == CHANGE

    async def test_a_shift_nested_under_another_transform(self, engine) -> None:
        plain, rewrite = await _run(engine, f"cumsum({SHIFT})", f"cumsum({SHIFT_RW})")
        assert plain == rewrite == {"2024-02": 30.0, "2024-04": 30.0, "2024-05": 60.0}


#: shape → (metric, months whose shifted evaluation reads March, a month only the spine query has).
SPINE_SHAPES: dict[str, tuple[str, frozenset[str]]] = {
    "shift": (SHIFT, frozenset()),
    "rewrite": (SHIFT_RW, frozenset()),
    "change": (f"change({REWRITE})", frozenset()),
    "change_pct": (f"change_pct({REWRITE})", frozenset()),
    "coalesce": (f"time_shift(coalesce({AMOUNT}, 0), -1)", frozenset({"2024-04"})),
    "running_total": (f"time_shift(cumsum({AMOUNT}), -1)", frozenset({"2024-04"})),
    "running_total_2": (f"time_shift(cumsum({AMOUNT}), -2)", frozenset({"2024-05"})),
    "lag": (f"time_shift(lag({AMOUNT}), -1)", frozenset({"2024-04", "2024-05"})),
    "nested_shift": (f"time_shift(time_shift({AMOUNT}, -1), -1)", frozenset({"2024-04"})),
    "predicate": (f"time_shift({AMOUNT} > 35, -1)", frozenset()),
    "cross_model": ("time_shift(sum(customers.spend), -1)", frozenset()),
    "cross_model_composite": (f"time_shift({AMOUNT} + sum(customers.spend), -1)", frozenset()),
    "forward": (f"time_shift({AMOUNT}, 1)", frozenset()),
    "unaligned": (f"time_shift({AMOUNT}, -1, 'day')", frozenset()),
    "windowed": ("time_shift(sum(orders.amount, window='60d'), -1)", frozenset({"2024-04"})),
    "hidden": (f"{AMOUNT} - {SHIFT_RW}", frozenset()),
    "under_transform": (f"cumsum({SHIFT_RW})", frozenset({"2024-04", "2024-05"})),
}


class TestSpineAgrees:
    @pytest.mark.parametrize("shape", SPINE_SHAPES)
    async def test_spine_carries_the_non_spine_value(self, engine, shape: str) -> None:
        metric, reads_spine_only = SPINE_SHAPES[shape]
        [plain] = await _run(engine, metric)
        [spine] = await _run(engine, metric, spine=True)
        assert {m: spine[m] for m in plain if m not in reads_spine_only} == {
            m: v for m, v in plain.items() if m not in reads_spine_only}

    async def test_year_over_year(self, engine) -> None:
        metric = f"time_shift(cumsum({AMOUNT}), -1, 'year')"
        [plain] = await _run(engine, metric, date_range="2025", region="East")
        [spine] = await _run(engine, metric, date_range="2025", region="East", spine=True)
        assert {m: spine[m] for m in plain} == plain

    async def test_filter(self, engine) -> None:
        [plain] = await _run(engine, AMOUNT, filter=[f"{SHIFT_RW} > 25"])
        [spine] = await _run(engine, AMOUNT, filter=[f"{SHIFT_RW} > 25"], spine=True)
        assert set(plain) == set(spine) - {"2024-03"}  # March reads February's 40 on the spine only

    # Order position: spine queries refuse transform order keys until DEV-2506.


def _plan(*metrics: str, date_range: Any = FEB_MAY) -> PlannedQuery:
    models = datasets()
    return plan_query(query=query(*metrics, date_range=date_range),
                      bundle=ResolvedSourceBundle(dialect="postgres", population=models[0],
                                                  referenced_datasets=models[1:]))


def _shifted(pq: PlannedQuery) -> list[RegroupAttachPlan]:
    return [a for a in pq.regroup_attach_plans if a.attach_phase == "shifted"]


class TestProducerIdentity:
    def test_distinct_offsets_under_a_frame_read_distinct_producers(self) -> None:
        attaches = _shifted(_plan(SHIFT_RW, f"time_shift({REWRITE}, -2)"))
        assert len(attaches) == 2
        assert len({regroup_producer_identity(a) for a in attaches}) == 2

    def test_equal_moved_frames_intern_to_one_producer(self) -> None:
        attaches = _shifted(_plan(SHIFT_RW, f"time_shift({REWRITE}, -1, 'month')"))
        assert len(attaches) == 2
        assert len({regroup_producer_identity(a) for a in attaches}) == 1

    def test_distinct_offsets_without_a_frame_intern_to_one_producer(self) -> None:
        attaches = _shifted(_plan(SHIFT_RW, f"time_shift({REWRITE}, -2)", date_range=None))
        assert len(attaches) == 2
        assert len({regroup_producer_identity(a) for a in attaches}) == 1


_AT = FieldKey(path=(), leaf="ordered_at")


def _moved(op: str, value: date, *, bucket: TimeGranularity, date_axis: bool) -> list[tuple[str, object]]:
    """``(op, literal)`` of each frame bound ``ordered_at <op> value`` moves to under a one-``bucket`` back shift."""
    frame = moved_frame(
        filters=[bound_filter_from_key(ArithmeticKey(op=op, operands=(_AT, LiteralKey(value=value))))],
        n_date_range=0, axis=TimeTruncKey(source=_AT, granularity=bucket), date_axis=date_axis, periods=-1,
        unit=bucket,
    )
    assert frame.n_frame == len(frame.filters)
    out = []
    for bf in frame.filters:
        assert isinstance(bf.value_key, ArithmeticKey)
        literal = bf.value_key.operands[1]
        assert isinstance(literal, LiteralKey)
        out.append((bf.value_key.op, literal.value))
    return out


class TestStrictBoundsStepByTheAxisType:
    def test_a_strict_lower_date_on_a_datetime_axis_keeps_its_first_bucket(self) -> None:
        assert _moved(">", date(2024, 1, 1), bucket=TimeGranularity.DAY, date_axis=False) == [
            (">=", date(2023, 12, 31))]

    def test_an_inclusive_upper_date_on_a_datetime_axis_ends_at_midnight(self) -> None:
        assert _moved("<=", date(2024, 1, 1), bucket=TimeGranularity.HOUR, date_axis=False) == [
            ("<", date(2024, 1, 1))]

    def test_a_strict_lower_date_on_a_date_axis_starts_the_next_day(self) -> None:
        assert _moved(">", date(2024, 1, 31), bucket=TimeGranularity.DAY, date_axis=True) == [
            (">=", date(2024, 1, 31))]

    def test_an_inclusive_lower_instant_on_a_date_axis_starts_the_next_day(self) -> None:
        assert _moved(">=", datetime(2024, 1, 1, 12), bucket=TimeGranularity.DAY, date_axis=True) == [
            (">=", date(2024, 1, 1))]

    def test_an_inclusive_upper_instant_on_a_date_axis_ends_with_its_day(self) -> None:
        assert _moved("<=", datetime(2024, 1, 1, 12), bucket=TimeGranularity.DAY, date_axis=True) == [
            ("<", date(2024, 1, 1))]

    def test_a_datetime_bound_keeps_its_type(self) -> None:
        assert _moved(">=", datetime(2024, 3, 5, 7), bucket=TimeGranularity.HOUR, date_axis=False) == [
            (">=", datetime(2024, 3, 5, 6))]
