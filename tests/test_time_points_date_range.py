"""``date_range`` elements are time points: single periods, one-sided ranges, typed shape errors."""

from __future__ import annotations

from datetime import datetime

import pydantic
import pytest

from slayer.core.errors import TimeLiteralError
from slayer.core.query import SlayerQuery, TimeDimension

from tests._time_points_fixtures import (
    BACKENDS, EV_TS, ev_dt, ids_in, ids_where, ids_with_range, month_key, monthly, tp_engine,
)

D = datetime


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with tp_engine(request.param) as eng:
        yield eng


class TestBounds:
    async def test_date_only_upper_bound_covers_its_whole_day(self, engine) -> None:
        got = await ids_with_range(engine, ["2024-01-01", "2024-12-31"])
        assert {3, 5, 6} <= got
        assert got == ids_in(D(2024, 1, 1), D(2025, 1, 1))

    @pytest.mark.parametrize(("date_range", "start", "end"), [
        ("2025-Q1", D(2025, 1, 1), D(2025, 4, 1)),
        (["2025-Q1"], D(2025, 1, 1), D(2025, 4, 1)),
        (["last month"], D(2026, 8, 1), D(2026, 9, 1)),
        ("last month", D(2026, 8, 1), D(2026, 9, 1)),
        ("2024-01-01", D(2024, 1, 1), D(2024, 1, 2)),
    ])
    async def test_single_period(self, engine, date_range, start, end) -> None:
        assert await ids_with_range(engine, date_range) == ids_in(start, end)

    async def test_period_bounds(self, engine) -> None:
        assert await ids_with_range(engine, ["2025-01-15", "2025-03"]) == ids_in(D(2025, 1, 15), D(2025, 4, 1))

    async def test_relative_bounds(self, engine) -> None:
        got = await ids_with_range(engine, ["12 months ago", "last month"])
        assert got == ids_in(D(2025, 9, 1), D(2026, 9, 1))

    async def test_instant_upper_bound_is_inclusive(self, engine) -> None:
        got = await ids_with_range(engine, ["2024-01-01", "2024-01-10 00:00:00"])
        assert 36 in got
        assert 37 not in got
        assert got == ids_where(lambda t: D(2024, 1, 1) <= t <= D(2024, 1, 10))

    async def test_instant_lower_bound_is_inclusive(self, engine) -> None:
        got = await ids_with_range(engine, ["2024-01-10 00:00:01", "2024-01-10"])
        assert got == {37}

    async def test_open_upper_bound(self, engine) -> None:
        assert await ids_with_range(engine, ["2024-01-01", None]) == ids_where(lambda t: t >= D(2024, 1, 1))

    async def test_open_lower_bound(self, engine) -> None:
        assert await ids_with_range(engine, [None, "2024-12-31"]) == ids_where(lambda t: t < D(2025, 1, 1))

    async def test_unknown_relative_unit_fails_at_binding(self, engine) -> None:
        # The unit may be a datasource granularity, so it is checked at binding.
        with pytest.raises(TimeLiteralError) as ei:
            await ids_with_range(engine, ["last fortnight", None])
        for form in ("YYYY-Qn", "YYYY-MM", "last N"):
            assert form in str(ei.value)

    async def test_reversed_bounds_return_no_rows(self, engine) -> None:
        assert await ids_with_range(engine, ["2025-06", "2025-01"]) == set()

    async def test_date_column(self, engine) -> None:
        got = await ids_with_range(engine, ["2025-01-15", "2025-03"], column="d", granularity="day")
        assert got == ids_in(D(2025, 1, 15), D(2025, 4, 1))

    async def test_sub_day_bound_on_date_column_fails_at_planning(self, engine) -> None:
        with pytest.raises(TimeLiteralError):
            await ids_with_range(engine, ["2025-01-01 10:00:00", None], column="d", granularity="day")

    async def test_monthly_buckets_with_open_upper(self, engine) -> None:
        got = await monthly(engine, measures=[{"formula": "sum(amount)", "name": "s"}], date_range=["2026-06", None])
        assert set(got) == {t.strftime("%Y-%m") for t in map(ev_dt, EV_TS) if t >= D(2026, 6, 1)}


class TestStages:
    async def test_multi_stage_model_accepts_one_sided_range(self, engine) -> None:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "daily", "measures": [{"formula": "sum(rev)", "name": "r"}],
            "time_dimensions": [{"dimension": "ts", "granularity": "year", "date_range": ["2026-01-01", None]}],
        }))
        assert sum(r["daily.r"] for r in resp.data) == pytest.approx(
            float(sum(ids_where(lambda t: t >= D(2026, 1, 1)))),
        )

    async def test_stage_range_with_period_bounds(self, engine) -> None:
        inner = {
            "name": "m", "source_model": "ev",
            "time_dimensions": [{"dimension": "ts", "granularity": "month"}],
            "measures": [{"formula": "sum(amount)", "name": "rev"}],
        }
        outer = {
            "source_model": "m", "measures": [{"formula": "sum(rev)", "name": "r"}],
            "time_dimensions": [{"dimension": "ts", "granularity": "month", "date_range": ["2025-02", "2025-03"]}],
        }
        resp = await engine.execute([SlayerQuery.model_validate(inner), SlayerQuery.model_validate(outer)])
        got = {month_key(r["m.ts"]): r["m.r"] for r in resp.data}
        assert got == {
            "2025-02": pytest.approx(float(sum(ids_in(D(2025, 2, 1), D(2025, 3, 1))))),
            "2025-03": pytest.approx(float(sum(ids_in(D(2025, 3, 1), D(2025, 4, 1))))),
        }

    async def test_stage_range_on_coarser_rebucketing(self, engine) -> None:
        inner = {
            "name": "m", "source_model": "ev",
            "time_dimensions": [{"dimension": "ts", "granularity": "month"}],
            "measures": [{"formula": "sum(amount)", "name": "rev"}],
        }
        outer = {
            "source_model": "m", "measures": [{"formula": "sum(rev)", "name": "r"}],
            "time_dimensions": [{"dimension": "ts", "granularity": "year", "date_range": "2025-Q1"}],
        }
        resp = await engine.execute([SlayerQuery.model_validate(inner), SlayerQuery.model_validate(outer)])
        [row] = resp.data
        assert row["m.r"] == pytest.approx(float(sum(ids_in(D(2025, 1, 1), D(2025, 4, 1)))))


def _td(date_range) -> dict:
    return {"dimension": "ts", "granularity": "month", "date_range": date_range}


class TestConstruction:
    @pytest.mark.parametrize("date_range", [
        [], ["2024-01-01", "2024-02-01", "2024-03-01"], [None, None],
    ])
    def test_malformed_shapes_rejected(self, date_range) -> None:
        payload = {"source_model": "ev", "time_dimensions": [_td(date_range)]}
        with pytest.raises(pydantic.ValidationError) as ei:
            SlayerQuery.model_validate(payload)
        msg = str(ei.value)
        assert "ts" in msg, msg
        assert "date_range" in msg, msg

    @pytest.mark.parametrize("date_range", ["2025/01/01", ["2025-13", "2025-Q1"]])
    def test_unparseable_bound_lists_forms(self, date_range) -> None:
        payload = {"source_model": "ev", "time_dimensions": [_td(date_range)]}
        with pytest.raises(pydantic.ValidationError) as ei:
            SlayerQuery.model_validate(payload)
        msg = str(ei.value)
        for form in ("YYYY-Qn", "YYYY-MM", "last N"):
            assert form in msg, msg

    @pytest.mark.parametrize(("given", "stored"), [
        ("2025-Q1", ["2025-Q1"]),
        (["last month"], ["last month"]),
        (["2024-01-01", None], ["2024-01-01", None]),
        ([None, "2024-12-31"], [None, "2024-12-31"]),
    ])
    def test_accepted_shapes(self, given, stored) -> None:
        assert TimeDimension.model_validate(_td(given)).date_range == stored

    def test_schema_advertises_string_and_null_bounds(self) -> None:
        schema = TimeDimension.model_json_schema()
        text = str(schema["properties"]["date_range"])
        assert "'string'" in text, text
        assert "'null'" in text, text
