"""A selected composite alongside any consumer containing it executes and equals its single-measure runs."""

from __future__ import annotations

import pytest

from tests._dev2062_fixtures import (
    AOV,
    AOV_BY_MONTH,
    AOV_CUMSUM_BY_MONTH,
    CROSS_RATIO,
    CROSS_RATIO_BY_MONTH,
    DATE_FILTER,
    DATE_RANGE,
    FEB,
    JAN,
    MAR,
    approx_map,
    by_month,
    m,
    make_exec_engine,
    month_order,
    month_td,
    monthly_q,
)

_HAVING_PLACEHOLDER = "__having_ref__"


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


async def _run(engine, query):
    resp = await engine.execute(query)
    assert _HAVING_PLACEHOLDER not in (resp.sql or ""), resp.sql
    return resp


def _assert_cells_equal(actual: dict, expected: dict) -> None:
    assert set(actual) == set(expected), (actual, expected)
    for month, value in expected.items():
        if value is None:
            assert actual[month] is None, (month, actual)
        else:
            assert actual[month] == pytest.approx(value), (month, actual)


# --------------------------------------------------------------------------- #
# The reported query: saved aov + cumsum(aov)
# --------------------------------------------------------------------------- #
_SPELLINGS = {"saved": ("aov", "cumsum(aov)"), "inline": (AOV, f"cumsum({AOV})")}
_TIME_FILTERS = {
    "date_range": {"time_dimensions": month_td(date_range=DATE_RANGE)},
    "date_filter": {"filters": [DATE_FILTER]},
}


@pytest.mark.parametrize("time_filter", sorted(_TIME_FILTERS))
@pytest.mark.parametrize("spelling", sorted(_SPELLINGS))
async def test_ratio_and_its_cumsum(exec_engine, spelling, time_filter) -> None:
    ratio, running = _SPELLINGS[spelling]
    kw = _TIME_FILTERS[time_filter]
    both = await _run(exec_engine, monthly_q(m(ratio, "a"), m(running, "r"), **kw))
    assert by_month(both, "a") == approx_map(AOV_BY_MONTH)
    assert by_month(both, "r") == approx_map(AOV_CUMSUM_BY_MONTH)
    alone_a = await _run(exec_engine, monthly_q(m(ratio, "a"), **kw))
    alone_r = await _run(exec_engine, monthly_q(m(running, "r"), **kw))
    _assert_cells_equal(by_month(both, "a"), by_month(alone_a, "a"))
    _assert_cells_equal(by_month(both, "r"), by_month(alone_r, "r"))


# --------------------------------------------------------------------------- #
# Consumer matrix: a local ratio and a ratio with a cross-model operand
# --------------------------------------------------------------------------- #
_RATIOS = {"local": (AOV, AOV_BY_MONTH), "cross": (CROSS_RATIO, CROSS_RATIO_BY_MONTH)}

_CONSUMERS = {
    "cumsum": "cumsum(({r}))",
    "lag": "lag(({r}))",
    "lead": "lead(({r}))",
    "change": "change(({r}))",
    "change_pct": "change_pct(({r}))",
    "consecutive_periods": "consecutive_periods(({r}) > 15)",
    "rank": "rank(({r}), direction='desc')",
    "transform_over_ratio": "cumsum(sum(amount)) / ({r})",
    "transform_plus_ratio": "cumsum(({r})) + ({r})",
    "transform_of_larger": "cumsum(({r}) * 2)",
}

#: Hand values for the local ratio (module docstring of the fixtures).
_LOCAL_CONSUMER_VALUES = {
    "cumsum": {JAN: 20, FEB: 30, MAR: 60},
    "lag": {JAN: None, FEB: 20, MAR: 10},
    "lead": {JAN: 10, FEB: 30, MAR: None},
    "change": {JAN: None, FEB: -10, MAR: 20},
    "change_pct": {JAN: None, FEB: -0.5, MAR: 2},
    "consecutive_periods": {JAN: 1, FEB: 0, MAR: 1},
    "rank": {JAN: 2, FEB: 3, MAR: 1},
    "transform_over_ratio": {JAN: 2, FEB: 5, MAR: 140 / 30},
    "transform_plus_ratio": {JAN: 40, FEB: 40, MAR: 90},
    "transform_of_larger": {JAN: 40, FEB: 60, MAR: 120},
}


@pytest.mark.parametrize("consumer", list(_CONSUMERS))
@pytest.mark.parametrize("ratio", sorted(_RATIOS))
async def test_selected_ratio_alongside_consumer(exec_engine, ratio, consumer) -> None:
    r, ratio_values = _RATIOS[ratio]
    formula = _CONSUMERS[consumer].format(r=r)
    both = await _run(exec_engine, monthly_q(m(r, "a"), m(formula, "c")))
    alone_a = await _run(exec_engine, monthly_q(m(r, "a")))
    alone_c = await _run(exec_engine, monthly_q(m(formula, "c")))
    assert by_month(both, "a") == approx_map(ratio_values)
    _assert_cells_equal(by_month(both, "a"), by_month(alone_a, "a"))
    _assert_cells_equal(by_month(both, "c"), by_month(alone_c, "c"))
    if ratio == "local":
        _assert_cells_equal(by_month(both, "c"), _LOCAL_CONSUMER_VALUES[consumer])


@pytest.mark.parametrize("ratio", sorted(_RATIOS))
async def test_selected_larger_expression_alongside_its_transform(exec_engine, ratio) -> None:
    r, ratio_values = _RATIOS[ratio]
    larger = f"({r}) * 2"
    both = await _run(exec_engine, monthly_q(m(larger, "a"), m(f"cumsum({larger})", "c")))
    alone_a = await _run(exec_engine, monthly_q(m(larger, "a")))
    alone_c = await _run(exec_engine, monthly_q(m(f"cumsum({larger})", "c")))
    assert by_month(both, "a") == approx_map({k: 2 * v for k, v in ratio_values.items()})
    _assert_cells_equal(by_month(both, "a"), by_month(alone_a, "a"))
    _assert_cells_equal(by_month(both, "c"), by_month(alone_c, "c"))


@pytest.mark.parametrize("measure", [AOV, "sum(amount)"])
async def test_change_and_change_pct_in_one_step(exec_engine, measure) -> None:
    # change_pct contains change's composite; siblings of one step never read each other's alias.
    ch, cp = f"change(({measure}))", f"change_pct(({measure}))"
    both = await _run(exec_engine, monthly_q(m(ch, "ch"), m(cp, "cp")))
    alone_cp = await _run(exec_engine, monthly_q(m(cp, "cp")))
    _assert_cells_equal(by_month(both, "cp"), by_month(alone_cp, "cp"))
    if measure == AOV:
        _assert_cells_equal(by_month(both, "ch"), _LOCAL_CONSUMER_VALUES["change"])
        _assert_cells_equal(by_month(both, "cp"), _LOCAL_CONSUMER_VALUES["change_pct"])


#: (filter threshold on cumsum(ratio), months it keeps) — cumsum local 20/30/60, cross 8/10/28.
_FILTER_CASES = {"local": (25, {FEB, MAR}), "cross": (9, {FEB, MAR})}


@pytest.mark.parametrize("ratio", sorted(_RATIOS))
async def test_measure_filter_on_transform_of_selected_ratio(exec_engine, ratio) -> None:
    r, ratio_values = _RATIOS[ratio]
    threshold, kept = _FILTER_CASES[ratio]
    predicate = f"cumsum(({r})) > {threshold}"
    with_ratio = await _run(exec_engine, monthly_q(m(r, "a"), filters=[predicate]))
    without = await _run(exec_engine, monthly_q(m("sum(amount)", "s"), filters=[predicate]))
    assert set(by_month(without, "s")) == kept
    assert by_month(with_ratio, "a") == approx_map({k: ratio_values[k] for k in kept})


@pytest.mark.parametrize("ratio", sorted(_RATIOS))
async def test_order_by_transform_of_selected_ratio(exec_engine, ratio) -> None:
    r, ratio_values = _RATIOS[ratio]
    order = [{"column": f"cumsum(({r}))", "direction": "desc"}]
    with_ratio = await _run(exec_engine, monthly_q(m(r, "a"), order=order))
    without = await _run(exec_engine, monthly_q(m("sum(amount)", "s"), order=order))
    assert month_order(without) == [MAR, FEB, JAN]
    assert month_order(with_ratio) == [MAR, FEB, JAN]
    assert by_month(with_ratio, "a") == approx_map(ratio_values)
