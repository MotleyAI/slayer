"""``whole_periods_only`` snaps frame bounds down to bucket boundaries and clamps the upper bound to now."""

from __future__ import annotations

from datetime import datetime

import pytest

from slayer.core.query import SlayerQuery

from tests._time_points_fixtures import (
    BACKENDS, EV_TS, PinnedClock, bucket_text, ev_dt, ids_in, month_key, monthly, tp_engine,
)

D = datetime
PLAIN = {"formula": "sum(amount)", "name": "s"}
WINDOW = {"formula": "sum(amount, window='90d')", "name": "w"}


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with tp_engine(request.param) as eng:
        yield eng


def _months_with_data(start: datetime, end: datetime) -> set[str]:
    return {t.strftime("%Y-%m") for t in map(ev_dt, EV_TS) if start <= t < end}


def _month_sum(year: int, month: int) -> float:
    start = D(year, month, 1)
    end = D(year + month // 12, month % 12 + 1, 1)
    return float(sum(ids_in(start, end)))


class TestSnappingAtMonth:
    @pytest.mark.parametrize(("date_range", "start", "end"), [
        (["2025-01-15", "2025-03-10"], D(2025, 1, 1), D(2025, 3, 1)),
        (["2025-01-01", "2025-03-31"], D(2025, 1, 1), D(2025, 4, 1)),
        ("this year", D(2026, 1, 1), D(2026, 9, 1)),
        (None, D(1900, 1, 1), D(2026, 9, 1)),
    ], ids=["mid-month", "month-aligned", "this-year", "no-range"])
    async def test_returned_months_are_whole(self, engine, date_range, start, end) -> None:
        got = await monthly(engine, measures=[PLAIN], date_range=date_range, whole_periods_only=True)
        assert set(got) == _months_with_data(start, end)
        for key, row in got.items():
            year, month = map(int, key.split("-"))
            assert row["s"] == pytest.approx(_month_sum(year, month)), key

    async def test_snapped_filter_bound_keeps_residual_condition(self, engine) -> None:
        got = await monthly(
            engine, measures=[PLAIN], filters=["ts >= '2025-01-15' and amount >= 7"], whole_periods_only=True,
        )
        assert min(got) == "2025-01"
        assert got["2025-01"]["s"] == pytest.approx(24.0)  # ids 7, 8, 9
        residual = await monthly(
            engine, measures=[PLAIN], filters=["ts >= '2025-01-15' and amount >= 8"], whole_periods_only=True,
        )
        assert residual["2025-01"]["s"] == pytest.approx(17.0)  # ids 8, 9

    @pytest.mark.parametrize("filt", [
        "ts >= '2025-01-15' or amount < 0",
        "not (ts < '2025-01-15')",
        "shipped_at >= '2025-01-15'",
    ], ids=["under-or", "under-not", "other-column"])
    async def test_non_frame_bounds_are_untouched(self, engine, filt) -> None:
        got = await monthly(engine, measures=[PLAIN], filters=[filt], whole_periods_only=True)
        assert got["2025-01"]["s"] == pytest.approx(17.0)  # ids 8, 9; snapping would add id 7

    async def test_lower_bound_in_the_future_is_empty(self, engine) -> None:
        assert await monthly(engine, measures=[PLAIN], date_range=["next month", None], whole_periods_only=True) == {}
        assert await monthly(engine, measures=[PLAIN], filters=["ts >= '2027-01-01'"], whole_periods_only=True) == {}

    async def test_with_trailing_window(self, engine) -> None:
        got = await monthly(engine, measures=[WINDOW, PLAIN], date_range="last 3 months", whole_periods_only=True)
        assert set(got) == {"2026-06", "2026-07", "2026-08"}
        assert got["2026-06"]["w"] > got["2026-06"]["s"]


class TestSubDay:
    async def test_current_hour_excluded(self) -> None:
        async with tp_engine("duckdb", clock=PinnedClock(D(2026, 9, 29, 12, 30))) as engine:
            resp = await engine.execute(SlayerQuery.model_validate({
                "source_model": "ev", "measures": [PLAIN],
                "time_dimensions": [{"dimension": "ts", "granularity": "hour", "date_range": ["2026-09-28", None]}],
                "whole_periods_only": True,
            }))
        buckets = {bucket_text(r["ev.ts"]) for r in resp.data}
        assert buckets == {"2026-09-28 15:00:00", "2026-09-29 06:00:00", "2026-09-29 11:00:00"}

    async def test_current_hour_excluded_sqlite(self) -> None:
        async with tp_engine("sqlite", clock=PinnedClock(D(2026, 9, 29, 12, 30))) as engine:
            resp = await engine.execute(SlayerQuery.model_validate({
                "source_model": "ev", "measures": [PLAIN],
                "time_dimensions": [{"dimension": "ts", "granularity": "hour", "date_range": ["2026-09-28", None]}],
                "whole_periods_only": True,
            }))
        assert "2026-09-29 12:00:00" not in {bucket_text(r["ev.ts"]) for r in resp.data}
        assert "2026-09-29 11:00:00" in {bucket_text(r["ev.ts"]) for r in resp.data}


def _two_grain_query(order: list[str]) -> SlayerQuery:
    return SlayerQuery.model_validate({
        "source_model": "ev", "measures": [PLAIN],
        "time_dimensions": [{"dimension": "ts", "granularity": g} for g in order],
        "filters": ["ts >= '2025-01-15'"], "whole_periods_only": True,
    })


class TestTwoGranularities:
    @pytest.mark.parametrize("order", [["day", "month"], ["month", "day"]])
    async def test_day_and_month_snap_to_the_earliest_floor(self, engine, order) -> None:
        resp = await engine.execute(_two_grain_query(order))
        days = {bucket_text(r["ev.ts.day"])[:10] for r in resp.data}
        months = {month_key(r["ev.ts.month"]) for r in resp.data}
        assert "2025-01-01" in days  # id 7, before the stated 2025-01-15
        assert min(months) == "2025-01"
        assert max(months) == "2026-08"  # upper clamp: floor(now) at month
        assert not any(d >= "2026-09-01" for d in days)

    async def test_week_and_month_warn(self, engine) -> None:
        resp = await engine.execute(_two_grain_query(["week", "month"]))
        assert resp.data
        dumps = [w.model_dump_json() for w in resp.warnings]
        assert any("week" in d and "month" in d and "whole_periods_only" in d for d in dumps), dumps

    async def test_day_and_month_do_not_warn(self, engine) -> None:
        resp = await engine.execute(_two_grain_query(["day", "month"]))
        assert not any("whole_periods_only" in w.model_dump_json() for w in resp.warnings)


class TestStageTimeDimension:
    async def test_stage_column_snaps(self, engine) -> None:
        got = await monthly(engine, measures=[{"formula": "sum(rev)", "name": "r"}], model="daily",
                            date_range=["2025-01-15", None], whole_periods_only=True)
        assert min(got) == "2025-01"
        assert max(got) == "2026-08"
        assert got["2025-01"]["r"] == pytest.approx(_month_sum(2025, 1))


class TestDateOperand:
    async def test_date_column_snaps(self, engine) -> None:
        got = await monthly(engine, measures=[PLAIN], column="d", date_range=["2025-01-15", None], whole_periods_only=True)
        assert min(got) == "2025-01"
        assert max(got) == "2026-08"
        assert got["2025-01"]["s"] == pytest.approx(_month_sum(2025, 1))

    async def test_future_date_lower_bound_is_empty(self, engine) -> None:
        got = await monthly(engine, measures=[PLAIN], column="d", date_range=["2027-01-01", None], whole_periods_only=True)
        assert got == {}
