"""Custom-granularity calendar steps for time-ordered transforms, and windowed aggregates at every population cell."""

from __future__ import annotations

import pytest

from slayer.core.query import SlayerQuery

from tests._dev2015_fixtures import (
    BACKENDS,
    JAN_JUN,
    MONTHS,
    bucket_key,
    by_bucket,
    calendar_models,
    cg_engine,
    m,
    num,
    spine_engine,
    spine_query,
    value,
)

FY = ["2023-04-01", "2024-04-01", "2025-04-01"]


@pytest.fixture(params=BACKENDS)
async def cg(request):
    async with cg_engine(request.param) as eng:
        yield eng


def _orders(*, granularity: str, measures: list) -> SlayerQuery:
    return SlayerQuery.model_validate({
        "source_model": "orders", "measures": measures,
        "time_dimensions": [{"dimension": "order_date", "granularity": granularity}],
    })


def _rows(resp, names: list[str], *, column: str = "order_date", width: int = 10) -> dict[str, tuple]:
    return {bucket_key(value(r, column), width=width): tuple(num(value(r, n)) for n in names) for r in resp.data}


# ---------------------------------------------------------------------------
# Custom calendar steps
# ---------------------------------------------------------------------------

class TestCustomSteps:
    async def test_shift_and_change_by_the_fiscal_year(self, cg) -> None:
        resp = await cg.execute(_orders(granularity="fiscal_year", measures=[
            m("time_shift(sum(amount), -1)", "prev"), m("change(sum(amount))", "chg"),
            m("change_pct(sum(amount))", "pct"),
        ]))
        got = _rows(resp, ["prev", "chg", "pct"])
        assert [got[k][:2] for k in FY] == [(None, None), (10.0, 40.0), (50.0, -10.0)]
        assert got[FY[1]][2] == pytest.approx(4.0)
        assert got[FY[2]][2] == pytest.approx(-0.2)

    async def test_custom_unit_on_a_finer_axis(self, cg) -> None:
        resp = await cg.execute(_orders(granularity="month", measures=[
            m("time_shift(sum(amount), -1, 'fiscal_year')", "fy"), m("time_shift(sum(amount), -1, 'year')", "y"),
        ]))
        got = _rows(resp, ["fy", "y"], width=7)
        assert got == {
            "2024-03": (None, None), "2024-04": (None, None), "2025-03": (10.0, 10.0), "2025-04": (20.0, 20.0),
        }

    async def test_streaks_count_custom_buckets(self, cg) -> None:
        resp = await cg.execute(SlayerQuery.model_validate({
            "source_model": "events", "filters": ["series = 'streak'"],
            "time_dimensions": [{"dimension": "ts", "granularity": "sprint"}],
            "measures": [m("consecutive_periods(count(*) > 0)", "cp")],
        }))
        got = _rows(resp, ["cp"], column="ts")
        assert got == {"2025-01-06": (1.0,), "2025-01-20": (2.0,), "2025-02-17": (1.0,)}


# ---------------------------------------------------------------------------
# Windowed aggregates at every population cell
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("backend", BACKENDS)
async def test_month_without_home_rows_reads_its_trailing_interval(backend) -> None:
    async with spine_engine(backend, models=calendar_models()) as eng:
        resp = await eng.execute(SlayerQuery.model_validate({
            "source_model": "calendar", "measures": [m("sum(orders.amount, window='2m')", "w")],
            "time_dimensions": [{"dimension": "date", "granularity": "month", "date_range": JAN_JUN}],
        }))
    got = by_bucket(resp, ["w"], bucket="calendar.date")
    assert [got[k] for k in MONTHS] == [(150.0,), (220.0,), (70.0,), (None,), (None,), (None,)]


WINDOWED = [
    m("sum(orders.amount, window='2m')", "sw"),
    m("sum(orders.amount, window='2m', partition_by=customers.region)", "pw"),
    m("last(orders.amount, window='2m')", "lw"),
    m("count(returns.id, window='3m')", "cw"),
]
_N = {"sw": [100, 170, 70, None, None, None], "lw": [100, 70, 70, None, None, None], "cw": [0, 1, 1, 1, 1, 1]}
_S = {"sw": [50, 50, None, None, None, None], "lw": [50, 50, None, None, None, None], "cw": [0, 0, 1, 1, 1, 0]}
_E = {"sw": [None] * 6, "lw": [None] * 6, "cw": [0] * 6}


def _expected(names: list[str]) -> dict[tuple, tuple]:
    out = {}
    for region, cols in (("N", _N), ("S", _S), ("E", _E)):
        cols = {**cols, "pw": cols["sw"]}
        for i, month in enumerate(MONTHS):
            out[(region, month)] = tuple(num(cols[n][i]) for n in names)
    return out


class TestTwoHomesPartitionsAndRankedPicks:
    @pytest.fixture(params=BACKENDS)
    async def engine(self, request):
        async with spine_engine(request.param) as eng:
            yield eng

    async def test_every_cell_reads_its_interval(self, engine) -> None:
        resp = await engine.execute(spine_query(measures=WINDOWED, dimensions=["customers.region"]))
        names = [x["name"] for x in WINDOWED]
        assert by_bucket(resp, names, by=["region"]) == _expected(names)

    @pytest.mark.parametrize("drop", [x["name"] for x in WINDOWED])
    async def test_removing_one_measure_changes_nothing_else(self, engine, drop) -> None:
        kept = [x for x in WINDOWED if x["name"] != drop]
        resp = await engine.execute(spine_query(measures=kept, dimensions=["customers.region"]))
        names = [x["name"] for x in kept]
        assert by_bucket(resp, names, by=["region"]) == _expected(names)
