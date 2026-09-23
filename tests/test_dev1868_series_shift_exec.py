"""DEV-1868 — series-regime ``time_shift`` (spec: queries/transforms,
composite-input requirement): nested-transform inputs, cross-model composite
leaves, and aggregate-typed predicate inputs shift the materialised series;
NULL outside it. Row-level leaves stay fail-closed."""

from __future__ import annotations

import re

import pytest

from tests._dev1846_fixtures import (
    ColumnRef,
    ModelMeasure,
    SlayerQuery,
    TimeDimension,
    TimeGranularity,
    gen,
    make_exec_engine,
    month_td,
)
from tests._dev1868_fixtures import (
    SHIFTED_CUMSUM,
    SHIFTED_CUMSUM_BY_STORE,
    SHIFTED_CUMSUM_MONTH_END,
    SHIFTED_CUMSUM_STATUS_A,
    SHIFTED_GT_PRED,
    SHIFTED_IN_PRED,
    SHIFTED_REV_PLUS_FACTOR,
    as_bool,
    keyed_by_month,
    make_daily_exec_engine,
)


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "sales")
    return SlayerQuery(**kw)


def _dr_td() -> list:
    return [TimeDimension(dimension=ColumnRef(name="ordered_at"),
                          granularity=TimeGranularity.MONTH,
                          date_range=["2024-02-01", "2024-03-31"])]


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _assert_months(resp, *, name: str, expected: dict, boolean: bool = False) -> None:
    by = keyed_by_month(resp)
    assert set(by) == {(m,) for m in expected}
    for month, value in expected.items():
        cell = by[(month,)][name]
        if value is None:
            assert cell is None
        elif boolean:
            assert as_bool(cell) is value
        else:
            assert float(cell) == pytest.approx(value)


class TestNestedTransformInput:
    async def test_shifts_the_inner_series(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="time_shift(cumsum(revenue:sum), -1)",
                                   name="t"),
                      ModelMeasure(formula="revenue:sum", name="r")],
        ))
        _assert_months(resp, name="sales.t", expected=SHIFTED_CUMSUM)
        # The shifted measure changes neither the row count nor its sibling.
        for month, rev in {"2024-01": 60.0, "2024-02": 100.0,
                           "2024-03": 60.0}.items():
            assert float(keyed_by_month(resp)[(month,)]["sales.r"]) == rev

    async def test_partitions_by_the_query_dimension(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            dimensions=["store"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula="time_shift(cumsum(revenue:sum), -1)",
                                   name="t")],
        ))
        by = keyed_by_month(resp, "sales.store")
        assert set(by) == set(SHIFTED_CUMSUM_BY_STORE)
        for key, value in SHIFTED_CUMSUM_BY_STORE.items():
            cell = by[key]["sales.t"]
            if value is None:
                assert cell is None
            else:
                assert float(cell) == pytest.approx(value)

    async def test_inner_series_is_computed_once(self) -> None:
        sql = await gen(_q(
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="time_shift(cumsum(revenue:sum), -1)",
                                   name="t")],
        ))
        assert len(re.findall(r"OVER\s*\(", sql)) == 1, sql


class TestCrossModelCompositeLeaf:
    async def test_shifts_the_composed_series(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="time_shift(revenue:sum + regions.factor:sum, -1)",
                name="t")],
        ))
        _assert_months(resp, name="sales.t", expected=SHIFTED_REV_PLUS_FACTOR)

    async def test_cross_model_operand_has_one_producer(self) -> None:
        sql = await gen(_q(
            time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="time_shift(revenue:sum + regions.factor:sum, -1)",
                name="t")],
        ))
        assert len(set(re.findall(r"_cm_\w+", sql))) == 1, sql


class TestAggregateTypedPredicateInput:
    async def test_in_predicate_shifts_as_a_boolean_series(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="time_shift(revenue:sum in (100, 999), -1)",
                                   name="t")],
        ))
        _assert_months(resp, name="sales.t", expected=SHIFTED_IN_PRED, boolean=True)

    async def test_comparison_predicate_shifts_as_a_boolean_series(
        self, exec_engine,
    ) -> None:
        resp = await exec_engine.execute(_q(
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="time_shift(revenue:sum > 80, -1)",
                                   name="t")],
        ))
        _assert_months(resp, name="sales.t", expected=SHIFTED_GT_PRED, boolean=True)

    async def test_change_over_a_predicate_stays_rejected(self, exec_engine) -> None:
        # The desugared subtraction consumes a boolean — typed error, no
        # silent 0/1 arithmetic and no engine-level error.
        query = _q(
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="change(revenue:sum > 100)",
                                   name="t")],
        )
        with pytest.raises(ValueError, match=r"(?i)boolean"):
            await exec_engine.execute(query)


class TestSeriesEdgeVsReaggregation:
    """The regime boundary at the date_range edge: series inputs yield NULL
    outside the materialised series; re-aggregation inputs may resolve the
    shifted bucket from source rows outside the range."""

    async def test_series_input_is_null_outside_the_range(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            time_dimensions=_dr_td(),
            measures=[ModelMeasure(formula="time_shift(cumsum(revenue:sum), -1)",
                                   name="t")],
        ))
        _assert_months(resp, name="sales.t",
                       expected={"2024-02": None, "2024-03": 100.0})

    async def test_predicate_input_is_null_outside_the_range(
        self, exec_engine,
    ) -> None:
        resp = await exec_engine.execute(_q(
            time_dimensions=_dr_td(),
            measures=[ModelMeasure(formula="time_shift(revenue:sum > 80, -1)",
                                   name="t")],
        ))
        _assert_months(resp, name="sales.t",
                       expected={"2024-02": None, "2024-03": True}, boolean=True)

    async def test_bare_leaf_keeps_the_lookback(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            time_dimensions=_dr_td(),
            measures=[ModelMeasure(formula="time_shift(revenue:sum, -1)",
                                   name="t")],
        ))
        _assert_months(resp, name="sales.t",
                       expected={"2024-02": 60.0, "2024-03": 100.0})

    async def test_local_composite_keeps_the_lookback(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            time_dimensions=_dr_td(),
            measures=[ModelMeasure(formula="time_shift(revenue:sum / qty:sum, -1)",
                                   name="t")],
        ))
        _assert_months(resp, name="sales.t",
                       expected={"2024-02": 6.0, "2024-03": 100.0 / 11.0})


class TestFiltersApplyOnce:
    async def test_row_filter_shapes_the_series_once(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            time_dimensions=month_td(), filters=["status = 'a'"],
            measures=[ModelMeasure(formula="time_shift(cumsum(revenue:sum), -1)",
                                   name="t")],
        ))
        _assert_months(resp, name="sales.t", expected=SHIFTED_CUMSUM_STATUS_A)

    async def test_the_predicate_renders_once(self) -> None:
        sql = await gen(_q(
            time_dimensions=month_td(), filters=["status = 'a'"],
            measures=[ModelMeasure(formula="time_shift(cumsum(revenue:sum), -1)",
                                   name="t")],
        ))
        assert sql.count("= 'a'") == 1, sql


class TestManyToOneCalendarShift:
    """A coarser explicit shift over finer buckets (day series, month offset)
    is a per-row lookup: leap-year month-end clamping (Jan 29/30/31 → Feb 29)
    must never fan the series join out or duplicate result rows."""

    @pytest.fixture(params=["sqlite", "duckdb"])
    async def daily_engine(self, request):
        async for engine in make_daily_exec_engine(request):
            yield engine

    async def test_month_end_days_neither_duplicate_nor_collide(
        self, daily_engine,
    ) -> None:
        resp = await daily_engine.execute(SlayerQuery(
            source_model="daily",
            time_dimensions=[TimeDimension(dimension=ColumnRef(name="ordered_at"),
                                           granularity=TimeGranularity.DAY)],
            measures=[ModelMeasure(formula="time_shift(cumsum(revenue:sum), -1, 'month')",
                                   name="t")],
        ))
        td_key = next(k for k in resp.data[0] if k.endswith(".ordered_at"))
        by = {str(r[td_key])[:10]: r for r in resp.data}
        assert len(resp.data) == len(SHIFTED_CUMSUM_MONTH_END)  # no fan-out
        assert set(by) == set(SHIFTED_CUMSUM_MONTH_END)
        for day, value in SHIFTED_CUMSUM_MONTH_END.items():
            cell = by[day]["daily.t"]
            if value is None:
                assert cell is None, (day, cell)
            else:
                assert float(cell) == pytest.approx(value), (day, cell)


class TestRowLeafStaysFailClosed:
    async def _assert_row_leaf_rejected(
        self, exec_engine, query, *, op: str = "time_shift",
    ) -> None:
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(query)
        message = str(ei.value)
        assert op in message
        assert re.search(r"(?i)row", message)
        assert "source_queries" in message

    async def test_mixed_aggregate_and_row_composite(self, exec_engine) -> None:
        await self._assert_row_leaf_rejected(exec_engine, _q(
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="time_shift(revenue:sum * weight, -1)",
                                   name="t")],
        ))

    async def test_row_level_predicate_input(self, exec_engine) -> None:
        await self._assert_row_leaf_rejected(exec_engine, _q(
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="time_shift(store in ('A', 'B'), -1)",
                                   name="t")],
        ))

    async def test_nested_transform_over_row_leaf(self, exec_engine) -> None:
        await self._assert_row_leaf_rejected(exec_engine, _q(
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="time_shift(cumsum(weight), -1)",
                                   name="t")],
        ), op="cumsum")

    async def test_composite_hiding_transform_over_row_leaf(
        self, exec_engine,
    ) -> None:
        await self._assert_row_leaf_rejected(exec_engine, _q(
            time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="time_shift(revenue:sum + cumsum(weight), -1)",
                name="t")],
        ), op="cumsum")
