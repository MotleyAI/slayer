"""Time bounds from every spelling are frame bounds: they never clip trailing windows or time_shift."""

from __future__ import annotations

import pytest

from tests._time_points_fixtures import BACKENDS, monthly, tp_engine

WINDOW = {"formula": "sum(amount, window='90d')", "name": "w"}
PLAIN = {"formula": "sum(amount)", "name": "s"}
SHIFT = {"formula": "time_shift(sum(amount), -1)", "name": "p"}

# "last 3 months" at the pinned clock is [2026-06-01, 2026-09-01).
LAST_3_MONTHS = [
    {"filters": ["ts in 'last 3 months'"]},
    {"filters": ["ts = 'last 3 months'"]},
    {"filters": ["ts >= 'last 3 months' and ts <= 'last 3 months'"]},
    {"filters": ["ts >= '2026-06' and ts < '2026-09'"]},
    {"filters": ["ts >= '2026-06-01' and ts < '2026-09-01'"]},
    {"filters": ["ts >= '2026-06-01 00:00:00' and ts < '2026-09-01 00:00:00'"]},
    {"filters": ["month(ts) >= '2026-06' and month(ts) < '2026-09'"]},
    {"filters": ["month(ts) in 'last 3 months'"]},
    {"date_range": "last 3 months"},
    {"date_range": ["last 3 months"]},
    {"date_range": ["2026-06", "2026-08"]},
    {"date_range": ["2026-06-01", "2026-08-31"]},
]


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with tp_engine(request.param) as eng:
        yield eng


class TestTrailingWindow:
    @pytest.mark.parametrize("spelling", LAST_3_MONTHS, ids=lambda s: str(s))
    async def test_every_spelling_gives_identical_window_values(self, engine, spelling) -> None:
        got = await monthly(engine, measures=[WINDOW, PLAIN], **spelling)
        reference = await monthly(engine, measures=[WINDOW, PLAIN], date_range="last 3 months")
        assert got == reference
        assert set(got) == {"2026-06", "2026-07", "2026-08"}

    async def test_earliest_visible_month_reaches_before_its_start(self, engine) -> None:
        got = await monthly(engine, measures=[WINDOW, PLAIN], filters=["ts in 'last 3 months'"])
        unbounded = await monthly(engine, measures=[WINDOW, PLAIN])
        assert got["2026-06"]["w"] > got["2026-06"]["s"]
        assert got["2026-06"] == unbounded["2026-06"]


class TestTimeShift:
    @pytest.mark.parametrize("spelling", [
        {"filters": ["month(ts) >= '2025-01'"]},
        {"filters": ["ts >= '2025'"]},
        {"filters": ["ts > '2024-12'"]},
        {"filters": ["ts >= '2025-01-01'"]},
        {"date_range": ["2025-01", None]},
    ], ids=lambda s: str(s))
    async def test_period_bound_does_not_clip_shift(self, engine, spelling) -> None:
        got = await monthly(engine, measures=[SHIFT, PLAIN], **spelling)
        # December 2024 holds ids 5 and 6.
        assert got["2025-01"]["p"] == pytest.approx(11.0)
        assert "2024-12" not in got


class TestEqualityFrameStatus:
    async def test_period_equality_is_a_frame_bound(self, engine) -> None:
        got = await monthly(engine, measures=[WINDOW, PLAIN], filters=["ts = '2024-06-01'"])
        unbounded = await monthly(engine, measures=[WINDOW, PLAIN])
        assert set(got) == {"2024-06"}
        # The bucket's own rows are that day's (ids 3, 4); the window still reads May (id 38).
        assert got["2024-06"]["s"] == pytest.approx(7.0)
        assert got["2024-06"]["w"] == unbounded["2024-06"]["w"]

    async def test_single_string_in_is_a_frame_bound(self, engine) -> None:
        got = await monthly(engine, measures=[WINDOW], filters=["ts in '2024-06'"])
        unbounded = await monthly(engine, measures=[WINDOW])
        assert got == {"2024-06": unbounded["2024-06"]}

    async def test_instant_equality_is_a_row_filter(self, engine) -> None:
        got = await monthly(engine, measures=[WINDOW, PLAIN], filters=["ts = '2024-06-01 00:00:00'"])
        assert got == {"2024-06": {"w": pytest.approx(3.0), "s": pytest.approx(3.0)}}

    @pytest.mark.parametrize("filt", ["ts != '2025-Q1'", "ts not in '2025-Q1'"])
    async def test_period_inequality_is_a_row_filter(self, engine, filt) -> None:
        got = await monthly(engine, measures=[WINDOW, PLAIN], filters=[filt, "ts >= '2025-04' and ts < '2025-05'"])
        # April 2025's window would read Q1 rows if the != were a frame bound.
        assert got == {"2025-04": {"w": pytest.approx(17.0), "s": pytest.approx(17.0)}}
