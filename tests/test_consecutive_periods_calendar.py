"""``consecutive_periods`` counts calendar periods; SQLite sub-day time offsets.

Executed on SQLite + DuckDB over ``tests/_consecutive_periods_calendar_fixtures.py``.
"""

from __future__ import annotations

import pytest

from slayer.core.query import SlayerQuery

from tests._consecutive_periods_calendar_fixtures import (
    C1_MONTHS,
    C1_STREAK,
    NULL_DATE_ROW,
    ORDERS_ROWS,
    SHIFT_SERIES,
    STREAK_SERIES,
    calendar_engine,
    calendar_models,
    int_or_none,
    month_of,
    orders_query,
    ticks_query,
)
from tests._engine_helpers import _engine_generate


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async with calendar_engine(request.param, ORDERS_ROWS) as engine:
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def null_date_engine(request):
    async with calendar_engine(request.param, [*ORDERS_ROWS, NULL_DATE_ROW]) as engine:
        yield engine


def _unique(pairs: list) -> dict:
    """``dict(pairs)``, failing on a duplicated key instead of overwriting it."""
    keys = [k for k, _ in pairs]
    assert len(keys) == len(set(keys)), keys
    return dict(pairs)


async def _by_month(engine, query: SlayerQuery) -> dict:
    resp = await engine.execute(query)
    return _unique([
        (month_of(r["orders.order_date"]), int_or_none(r["orders.v"])) for r in resp.data
    ])


_STREAK = "consecutive_periods(sum(amount) > 100)"


class TestCalendarStreak:
    async def test_gaps_in_the_middle_break_the_run(self, exec_engine) -> None:
        got = await _by_month(exec_engine, orders_query(formula=_STREAK))
        assert got == dict(zip(C1_MONTHS, C1_STREAK))

    async def test_per_group_series(self, exec_engine) -> None:
        resp = await exec_engine.execute(orders_query(
            formula=_STREAK, dimensions=["customer_id"], filters=[]))
        got = _unique([
            ((int(r["orders.customer_id"]), month_of(r["orders.order_date"])), int(r["orders.v"]))
            for r in resp.data
        ])
        assert got == {
            **{(1, m): v for m, v in zip(C1_MONTHS, C1_STREAK)},
            (2, "2024-02"): 1, (2, "2024-03"): 0, (2, "2024-04"): 1,
            (2, "2024-06"): 0, (2, "2024-07"): 1, (2, "2024-08"): 2,
        }

    async def test_date_range_bounds_the_series(self, exec_engine) -> None:
        got = await _by_month(exec_engine, orders_query(
            formula=_STREAK, date_range=["2025-02-01", "2025-06-30"]))
        assert got == {"2025-02": 1, "2025-03": 2, "2025-05": 1, "2025-06": 2}

    async def test_row_filter_that_empties_a_bucket_makes_a_gap(self, exec_engine) -> None:
        got = await _by_month(exec_engine, orders_query(
            formula=_STREAK, filters=["customer_id = 1", "amount != 500"]))
        assert got == {
            "2024-01": 1, "2024-03": 1, "2024-12": 1, "2025-01": 2,
            "2025-02": 3, "2025-03": 4, "2025-05": 1, "2025-06": 2,
        }

    async def test_measure_filter_keeps_surviving_streaks(self, exec_engine) -> None:
        got = await _by_month(exec_engine, orders_query(
            formula=_STREAK, filters=["customer_id = 1", "sum(amount) > 200"]))
        assert got == {"2024-02": 2, "2024-12": 1, "2025-05": 1, "2025-06": 2}

    @pytest.mark.parametrize(("formula", "expected"), [
        ("cumsum(sum(amount))",
         {"2024-02": 650, "2024-12": 990, "2025-05": 1695, "2025-06": 1905}),
        ("lag(sum(amount))",
         {"2024-02": 150, "2024-12": 120, "2025-05": 140, "2025-06": 225}),
    ])
    async def test_measure_filter_keeps_other_transforms_unfiltered(
        self, exec_engine, formula: str, expected: dict,
    ) -> None:
        got = await _by_month(exec_engine, orders_query(
            formula=formula, filters=["customer_id = 1", "sum(amount) > 200"]))
        assert got == expected

    async def test_null_time_bucket_is_adjacent_to_nothing(self, null_date_engine) -> None:
        got = await _by_month(null_date_engine, orders_query(formula=_STREAK))
        assert got == {None: 1, **dict(zip(C1_MONTHS, C1_STREAK))}

    async def test_downstream_stage_time_dimension(self, exec_engine) -> None:
        inner = SlayerQuery.model_validate({
            "name": "s1", "source_model": "orders",
            "time_dimensions": [{"dimension": "order_date", "granularity": "month"}],
            "filters": ["customer_id = 1"],
            "measures": [{"formula": "amount:sum", "name": "total"}],
        })
        outer = SlayerQuery.model_validate({
            "source_model": "s1",
            "time_dimensions": [{"dimension": "order_date", "granularity": "month"}],
            "measures": [{"formula": "consecutive_periods(total:sum > 100)", "name": "v"}],
        })
        resp = await exec_engine.execute(query=[inner, outer])
        got = _unique([(month_of(r["s1.order_date"]), int(r["s1.v"])) for r in resp.data])
        assert got == dict(zip(C1_MONTHS, C1_STREAK))


class TestNesting:
    async def test_predicate_over_change(self, exec_engine) -> None:
        got = await _by_month(exec_engine, orders_query(
            formula="consecutive_periods(change(sum(amount)) > 0)"))
        assert got == dict(zip(C1_MONTHS, [0, 1, 0, 0, 0, 1, 0, 0, 0]))

    async def test_change_over_streak(self, exec_engine) -> None:
        got = await _by_month(exec_engine, orders_query(formula=f"change({_STREAK})"))
        assert got == dict(zip(C1_MONTHS, [None, 1, 1, None, 1, 1, 1, None, 1]))

    async def test_lag_over_streak_steps_present_rows(self, exec_engine) -> None:
        got = await _by_month(exec_engine, orders_query(formula=f"lag({_STREAK})"))
        assert got == dict(zip(C1_MONTHS, [None, 1, 2, 3, 1, 2, 3, 4, 1]))

    async def test_streak_over_lag_is_calendar_based(self, exec_engine) -> None:
        got = await _by_month(exec_engine, orders_query(
            formula="consecutive_periods(lag(sum(amount)) > 100)"))
        assert got == dict(zip(C1_MONTHS, [0, 1, 2, 1, 2, 3, 4, 1, 2]))


async def _ticks_columns(engine, query: SlayerQuery, *names: str) -> dict:
    """Each named measure's values in bucket order."""
    resp = await engine.execute(query)
    rows = sorted(resp.data, key=lambda r: r["ticks.ts"])
    assert len(rows) == 4, rows
    return {n: [int_or_none(r[f"ticks.{n}"]) for r in rows] for n in names}


class TestEveryGranularity:
    @pytest.mark.parametrize("granularity", list(STREAK_SERIES))
    async def test_own_calendar_step(self, exec_engine, granularity: str) -> None:
        got = await _ticks_columns(exec_engine, ticks_query(
            series=f"streak_{granularity}", granularity=granularity,
            measures=[{"formula": "consecutive_periods(sum(amount) > 0)", "name": "v"}],
        ), "v")
        assert got["v"] == [1, 2, 1, 2]


class TestSubDayTimeOffsets:
    @pytest.mark.parametrize("granularity", list(SHIFT_SERIES))
    async def test_time_shift_and_change_read_prior_bucket(
        self, exec_engine, granularity: str,
    ) -> None:
        got = await _ticks_columns(exec_engine, ticks_query(
            series=f"shift_{granularity}", granularity=granularity,
            measures=[
                {"formula": "time_shift(sum(amount), -1)", "name": "prev"},
                {"formula": "change(sum(amount))", "name": "chg"},
            ],
        ), "prev", "chg")
        assert got == {"prev": [None, 1, 2, None], "chg": [None, 1, 1, None]}


class TestPeriodKeywordRejected:
    async def test_period_keyword_raises(self) -> None:
        models = calendar_models()
        query = orders_query(formula="consecutive_periods(sum(amount) > 100, period='year')")
        with pytest.raises(ValueError, match=r"consecutive_periods.*'period'"):
            await _engine_generate(
                query=query, model=models[0], extra_models=models[1:], dialect="duckdb",
            )
