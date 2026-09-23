"""Composite-input time_shift: every leaf keeps its own grain when shifted, the
frame rule, and the shared lookup join-back (queries/transforms, "Composite-input
time_shift"). Executed on SQLite + DuckDB over the ``monthly`` fixture
(North Jan 10 / Feb 20 / Mar 30, South Jan 5 / Feb 15, West Feb NULL)."""

from __future__ import annotations

import re

import pytest

from tests._dev1832_fixtures import (
    ColumnRef,
    ModelMeasure,
    dev1832_models,
    TimeDimension,
    TimeGranularity,
    gen,
    make_exec_engine,
    month_key,
    month_td,
    monthly_q,
)

SHARE = "amount:sum / amount:sum(partition_by=[ordered_at])"
N, S, W = "North", "South", "West"
JAN, FEB, MAR = "2024-01", "2024-02", "2024-03"
#: Every (region, month) cell of the unfiltered query / the Feb–Mar date range.
ALL_CELLS = {(N, JAN), (N, FEB), (N, MAR), (S, JAN), (S, FEB), (W, FEB)}
DR_CELLS = {(N, FEB), (N, MAR), (S, FEB), (W, FEB)}

SHIFTED_SHARE = {(N, FEB): 10 / 15, (S, FEB): 5 / 15, (N, MAR): 20 / 35}
CHANGE_SHARE = {(N, FEB): -2 / 21, (S, FEB): 2 / 21, (N, MAR): 3 / 7}
CHANGE_PCT_SHARE = {(N, FEB): -1 / 7, (S, FEB): 2 / 7, (N, MAR): 0.75}
HALF_MONTH_TOTAL = {(N, FEB): 7.5, (S, FEB): 7.5, (N, MAR): 17.5}
REGION_TOTAL_SHARE = {(N, FEB): 10 / 60, (N, MAR): 20 / 60, (S, FEB): 5 / 20}
REAGG_BY_MONTH = {JAN: None, FEB: 0.5, MAR: 20 / 35}
DR_MONTH_TOTAL = {(N, FEB): 15.0, (S, FEB): 15.0, (N, MAR): 35.0}
DR_LAST = {(N, FEB): 10.0, (S, FEB): 5.0, (N, MAR): 20.0}
DR_IN_FRAME_REGION_SHARE = {(N, FEB): 10 / 50, (N, MAR): 20 / 50, (S, FEB): 5 / 15}
HALF_LAST = {(N, FEB): 5.0, (N, MAR): 10.0, (S, FEB): 2.5}
HALF_WINDOW = {(N, FEB): 5.0, (N, MAR): 15.0, (S, FEB): 2.5}
DR_WINDOW = {(N, FEB): 10.0, (N, MAR): 30.0, (S, FEB): 5.0}
TWO_BACK_SHARE = {(N, MAR): 10 / 15}
DAY_BACK = {(N, FEB): 10.0, (N, MAR): 20.0, (S, FEB): 5.0}


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _filtered_models() -> list:
    """``monthly`` with the model-level filter ``amount > 5`` (drops South Jan and West Feb)."""
    models = dev1832_models()
    next(m for m in models if m.name == "monthly").filters.append("amount > 5")
    return models


@pytest.fixture(params=["sqlite", "duckdb"])
async def filtered_engine(request):
    async for engine in make_exec_engine(request, models=_filtered_models()):
        yield engine


def _dr_td() -> list:
    return [TimeDimension(dimension=ColumnRef(name="ordered_at"),
                          granularity=TimeGranularity.MONTH,
                          date_range=["2024-02-01", "2024-03-31"])]


def _q(formula: str, *, date_range: bool = False, **kw):
    kw.setdefault("dimensions", ["region"])
    return monthly_q(time_dimensions=_dr_td() if date_range else month_td(),
                     measures=[ModelMeasure(formula=formula, name="t")], **kw)


def _cells(resp, name: str = "monthly.t") -> dict:
    out = {(r["monthly.region"], month_key(r["monthly.ordered_at"])): r[name]
           for r in resp.data}
    assert len(out) == len(resp.data), "duplicate result rows for one cell"
    return out


def _assert_cells(got: dict, *, cells: set, expected: dict,
                  unpinned: frozenset = frozenset()) -> None:
    """Row set is exactly ``cells``; listed cells match, every other pinned cell is NULL."""
    assert set(got) == cells
    for cell in cells - unpinned:
        want = expected.get(cell)
        if want is None:
            assert got[cell] is None, (cell, got[cell])
        else:
            assert got[cell] is not None, cell
            assert float(got[cell]) == pytest.approx(want), (cell, got[cell])


class TestPartitionedLeafKeepsItsGrain:
    async def test_prior_month_share(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(f"time_shift({SHARE}, -1)"))
        _assert_cells(_cells(resp), cells=ALL_CELLS, expected=SHIFTED_SHARE)

    async def test_change(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(f"change({SHARE})"))
        _assert_cells(_cells(resp), cells=ALL_CELLS, expected=CHANGE_SHARE)

    async def test_change_pct(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(f"change_pct({SHARE})"))
        _assert_cells(_cells(resp), cells=ALL_CELLS, expected=CHANGE_PCT_SHARE)

    async def test_single_partitioned_leaf_in_trivial_composite(self, exec_engine) -> None:
        resp = await exec_engine.execute(
            _q("time_shift(amount:sum(partition_by=[ordered_at]) / 2, -1)"))
        _assert_cells(_cells(resp), cells=ALL_CELLS, expected=HALF_MONTH_TOTAL)

    async def test_non_time_partition_is_constant_along_the_axis(self, exec_engine) -> None:
        resp = await exec_engine.execute(
            _q("time_shift(amount:sum / amount:sum(partition_by=[region]), -1)"))
        _assert_cells(_cells(resp), cells=ALL_CELLS, expected=REGION_TOTAL_SHARE)

    async def test_reaggregation_over_shifted_composite(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            "avg(time_shift(amount:sum(partition_by=[region, ordered_at]) "
            "/ amount:sum(partition_by=[ordered_at]), -1))", dimensions=[]))
        got = {month_key(r["monthly.ordered_at"]): r["monthly.t"] for r in resp.data}
        assert set(got) == set(REAGG_BY_MONTH)
        for month, want in REAGG_BY_MONTH.items():
            if want is None:
                assert got[month] is None
            else:
                assert float(got[month]) == pytest.approx(want)


class TestFrameRule:
    async def test_bare_partitioned_leaf_reaches_outside_date_range(self, exec_engine) -> None:
        resp = await exec_engine.execute(
            _q("time_shift(amount:sum(partition_by=[ordered_at]), -1)", date_range=True))
        # West Feb is not pinned by the scenario.
        _assert_cells(_cells(resp), cells=DR_CELLS, expected=DR_MONTH_TOTAL,
                      unpinned=frozenset({(W, FEB)}))

    async def test_bare_ranked_leaf_reaches_outside_date_range(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q("time_shift(amount:last, -1)", date_range=True))
        _assert_cells(_cells(resp), cells=DR_CELLS, expected=DR_LAST)

    async def test_bare_windowed_leaf_reaches_outside_date_range(self, exec_engine) -> None:
        resp = await exec_engine.execute(
            _q("time_shift(amount:sum(window='90d'), -1)", date_range=True))
        _assert_cells(_cells(resp), cells=DR_CELLS, expected=DR_WINDOW)

    async def test_non_time_partition_keeps_in_frame_value(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(
            "time_shift(amount:sum / amount:sum(partition_by=[region]), -1)",
            date_range=True))
        _assert_cells(_cells(resp), cells=DR_CELLS, expected=DR_IN_FRAME_REGION_SHARE)

    async def test_model_filter_applies_to_shifted_evaluation(self, filtered_engine) -> None:
        resp = await filtered_engine.execute(
            _q("time_shift(amount:sum(partition_by=[ordered_at]), -1)", date_range=True))
        _assert_cells(_cells(resp), cells={(N, FEB), (N, MAR), (S, FEB)},
                      expected={(N, FEB): 10.0, (S, FEB): 10.0, (N, MAR): 35.0})

    async def test_population_predicate_conjoined_with_frame_bound(self, exec_engine) -> None:
        """Regression guard: already correct before the producer rewrite."""
        resp = await exec_engine.execute(_q(
            "time_shift(amount:sum + amount:sum(partition_by=[ordered_at]), -1)",
            filters=["ordered_at >= '2024-02-01' and region = 'North'"]))
        _assert_cells(_cells(resp), cells={(N, FEB), (N, MAR)},
                      expected={(N, FEB): 20.0, (N, MAR): 40.0})


class TestRankedAndWindowedLeavesInComposite:
    async def test_ranked_leaf_executes(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q("time_shift(amount:last / 2, -1)"))
        _assert_cells(_cells(resp), cells=ALL_CELLS, expected=HALF_LAST)

    async def test_windowed_leaf_keeps_its_window(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q("time_shift(amount:sum(window='90d') / 2, -1)"))
        _assert_cells(_cells(resp), cells=ALL_CELLS, expected=HALF_WINDOW)


class TestSharedShiftedRelation:
    def _two_offsets(self):
        return monthly_q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula=f"time_shift({SHARE}, -1)", name="t1"),
                      ModelMeasure(formula=f"time_shift({SHARE}, -2)", name="t2")])

    async def test_two_offsets_values(self, exec_engine) -> None:
        resp = await exec_engine.execute(self._two_offsets())
        _assert_cells(_cells(resp, "monthly.t1"), cells=ALL_CELLS, expected=SHIFTED_SHARE)
        _assert_cells(_cells(resp, "monthly.t2"), cells=ALL_CELLS, expected=TWO_BACK_SHARE)

    async def test_two_offsets_read_one_shifted_relation(self) -> None:
        sql = await gen(self._two_offsets())
        shifted = set(re.findall(r"\b(shifted_\w+) AS \(", sql))
        assert len(shifted) == 1, sql

    async def test_bare_partitioned_leaf_computes_one_producer(self) -> None:
        """Without a frame mask the shift interns with the base's own producer."""
        sql = await gen(_q("time_shift(amount:sum(partition_by=[ordered_at]), -1)"))
        assert len(set(re.findall(r"\b(_cm_\w+) AS \(", sql))) == 1, sql


class TestUnalignedShift:
    async def test_reads_bucket_containing_offset_instant(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q("time_shift(amount:sum, -1, 'day')"))
        _assert_cells(_cells(resp), cells=ALL_CELLS, expected=DAY_BACK)
