"""DEV-1750 — execution ground truth on SQLite AND DuckDB (both required by the
issue). Every expectation is hand-computed from the fixture dataset in
``tests/_dev1750_fixtures.py``; a missing join or a wrong grain is not a cosmetic
difference — the SQL either fails to bind or returns the wrong number.

Dataset recap (weights: region 1 → 2.0, region 2 → 3.0):
  month   amount:sum   wscaled_sum                         customers.spend:sum
  Jan     15   (10+5)   35   (10*2 + 5*3)                  300  (c1 100 + c2 200)
  Feb     24   (20+4)   48   (20*2 + 4*2, both c1/reg1)    100  (c1 only, ×orders)
  Mar     18   (10+8)   46   (10*3 + 8*2)                  ...  (c2 200 + c1 100)

The Feb/Mar ``customers.spend:sum`` values depend on the join fan-out and are
asserted only where hand-verifiable; the transform columns are the focus.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import RenderContextMissingFacilityError

from tests._dev1750_fixtures import (
    ColumnRef,
    ModelMeasure,
    SlayerQuery,
    TimeDimension,
    TimeGranularity,
    make_exec_engine,
    month_key,
    month_td,
    rows_by,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    """SQLite + DuckDB engines over the hand-computed dataset (issue-required
    backends). Defined here so the fixture name is not a cross-module import that
    ruff reads as shadowing the test parameter (F811)."""
    async for engine in make_exec_engine(request):
        yield engine


def _q(*, measures, dimensions=None) -> SlayerQuery:
    kw = dict(source_model="orders", time_dimensions=month_td(), measures=measures)
    if dimensions is not None:
        kw["dimensions"] = dimensions
    return SlayerQuery(**kw)


def _by_month(resp) -> dict:
    return {
        month_key(k[0]): r
        for k, r in rows_by(resp, "orders.ordered_at").items()
    }


class TestShapeALocalTimeShift:
    """(a) local ``time_shift(amount:sum)`` beside a cross-model sibling."""

    async def test_prev_amount_sum_and_sibling(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(formula="customers.spend:sum", name="cm"),
            ModelMeasure(formula="amount:sum", name="s"),
            ModelMeasure(formula="time_shift(amount:sum, -1)", name="prev"),
        ]))
        by = _by_month(resp)
        # Local sum per month.
        assert float(by["2024-01"]["orders.s"]) == pytest.approx(15.0)
        assert float(by["2024-02"]["orders.s"]) == pytest.approx(24.0)
        assert float(by["2024-03"]["orders.s"]) == pytest.approx(18.0)
        # Prior-month sum; Jan (earliest) has none.
        assert by["2024-01"]["orders.prev"] is None
        assert float(by["2024-02"]["orders.prev"]) == pytest.approx(15.0)
        assert float(by["2024-03"]["orders.prev"]) == pytest.approx(24.0)
        # The cross-model sibling is unaffected by the shift: customers.spend:sum
        # has no shared grain with the month axis, so it broadcasts the
        # all-customers total (100 + 200 + 50 = 350) across every month — the
        # same value the shift machinery must not perturb.
        assert float(by["2024-01"]["orders.cm"]) == pytest.approx(350.0)
        assert float(by["2024-03"]["orders.cm"]) == pytest.approx(350.0)


class TestShapeBHostRootedCrossingFragment:
    """(b) ``time_shift(amount:wscaled_sum)`` — the issue's named repro. The
    prior-period WEIGHTED-SCALED sum only computes if the shifted CTE joined
    ``regions`` (Part 1). A missing join → the SQL does not bind at all."""

    async def test_prev_weighted_scaled_sum(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(formula="amount:wscaled_sum", name="w"),
            ModelMeasure(formula="time_shift(amount:wscaled_sum, -1)", name="prev"),
        ]))
        by = _by_month(resp)
        assert float(by["2024-01"]["orders.w"]) == pytest.approx(35.0)
        assert float(by["2024-02"]["orders.w"]) == pytest.approx(48.0)
        assert float(by["2024-03"]["orders.w"]) == pytest.approx(46.0)
        assert by["2024-01"]["orders.prev"] is None
        assert float(by["2024-02"]["orders.prev"]) == pytest.approx(35.0)
        assert float(by["2024-03"]["orders.prev"]) == pytest.approx(48.0)

    async def test_sibling_local_sum_not_multiplied_by_fragment_join(
        self, exec_engine
    ) -> None:
        """A local ``amount:sum`` beside the crossing wscaled shift must keep its
        true per-month value — the fragment's 1:N-safe join lives only in the
        crossing measure's own CTEs, never multiplying the sibling."""
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(formula="amount:sum", name="s"),
            ModelMeasure(formula="time_shift(amount:wscaled_sum, -1)", name="prev"),
        ]))
        by = _by_month(resp)
        assert float(by["2024-01"]["orders.s"]) == pytest.approx(15.0)
        assert float(by["2024-02"]["orders.s"]) == pytest.approx(24.0)
        assert float(by["2024-03"]["orders.s"]) == pytest.approx(18.0)


class TestChangeDesugars:
    """``change`` = current − prior. With a LOCAL inner beside a cross-model
    sibling (the shape the guard lift naturally enables — the chain's outer
    arithmetic-materialization step renders the subtraction), it must produce the
    period-over-period delta."""

    async def test_change_of_local_with_cross_model_sibling(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(formula="customers.spend:sum", name="cm"),
            ModelMeasure(formula="change(amount:sum)", name="delta"),
        ]))
        by = _by_month(resp)
        assert by["2024-01"]["orders.delta"] is None
        assert float(by["2024-02"]["orders.delta"]) == pytest.approx(24.0 - 15.0)
        assert float(by["2024-03"]["orders.delta"]) == pytest.approx(18.0 - 24.0)

    async def test_change_pct_of_local_with_cross_model_sibling(self, exec_engine) -> None:
        """``change_pct`` = (current − prior) / prior — pins the outer
        arithmetic-materialization step, not just CTE creation."""
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(formula="customers.spend:sum", name="cm"),
            ModelMeasure(formula="change_pct(amount:sum)", name="pct"),
        ]))
        by = _by_month(resp)
        assert by["2024-01"]["orders.pct"] is None
        assert float(by["2024-02"]["orders.pct"]) == pytest.approx((24.0 - 15.0) / 15.0)
        assert float(by["2024-03"]["orders.pct"]) == pytest.approx((18.0 - 24.0) / 24.0)


class TestChangeOverCrossModelInnerIsOutOfScope:
    """``change`` / ``change_pct`` over a CROSS-MODEL/crossing-fragment inner
    adds an arithmetic layer the combined SELECT renders BEFORE the transform
    chain, hitting a pre-existing ``RenderContextMissingFacilityError`` (the
    TransformKey is not yet materialised as a slot). DEV-1750 does not fix that
    separate gap; the contract is that it stays a loud, SPECIFIC error — never
    wrong values or unbound SQL. Pinning the exact type stops a real regression
    (bad-SQL binding error, silent wrong number) from passing as 'expected'."""

    async def test_change_of_wscaled_raises_render_context(self, exec_engine) -> None:
        q = _q(measures=[
            ModelMeasure(formula="change(amount:wscaled_sum)", name="delta"),
        ])
        with pytest.raises(RenderContextMissingFacilityError):
            await exec_engine.execute(q)

    async def test_change_pct_of_wscaled_raises_render_context(self, exec_engine) -> None:
        q = _q(measures=[
            ModelMeasure(formula="change_pct(amount:wscaled_sum)", name="delta"),
        ])
        with pytest.raises(RenderContextMissingFacilityError):
            await exec_engine.execute(q)


class TestSiblingProtectionUnderFanOut:
    """The crossing measure's join is 1:N (``orders → line_items``). If it leaked
    into the host base, a sibling ``amount:sum`` would be MULTIPLIED by the
    line-item count. Isolation keeps the fan-out inside the crossing measure's
    own CTEs, so the sibling stays true — a claim only a fan-out dataset can
    actually test (Codex F1)."""

    async def test_local_sibling_not_multiplied_by_one_to_many_fragment(
        self, exec_engine
    ) -> None:
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(formula="amount:sum", name="s"),
            ModelMeasure(formula="amount:liscaled_sum", name="li"),
            ModelMeasure(formula="time_shift(amount:liscaled_sum, -1)", name="prev"),
        ]))
        by = _by_month(resp)
        # Sibling amount:sum stays unmultiplied (order 1 counted ONCE, not twice).
        assert float(by["2024-01"]["orders.s"]) == pytest.approx(15.0)
        assert float(by["2024-02"]["orders.s"]) == pytest.approx(24.0)
        assert float(by["2024-03"]["orders.s"]) == pytest.approx(18.0)
        # The crossing measure itself keeps the fanned-out weighted sum...
        assert float(by["2024-01"]["orders.li"]) == pytest.approx(50.0)
        # ...and its prior-period value (Part 1: the 1:N join is pulled into the
        # shifted CTE, so the shifted re-aggregation fans out identically).
        assert by["2024-01"]["orders.prev"] is None
        assert float(by["2024-02"]["orders.prev"]) == pytest.approx(50.0)
        assert float(by["2024-03"]["orders.prev"]) == pytest.approx(24.0)


class TestConsecutivePeriodsExecution:
    """cp is lifted ENTIRELY (no re-aggregation). Executed streak values pin that
    it reads the materialised alias correctly on both backends."""

    async def test_cp_varying_predicate_with_cross_model_sibling(self, exec_engine) -> None:
        # amount:sum > 20 → Jan 15=F, Feb 24=T, Mar 18=F → streak 0,1,0.
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(formula="customers.spend:sum", name="cm"),
            ModelMeasure(formula="consecutive_periods(amount:sum > 20)", name="streak"),
        ]))
        by = _by_month(resp)
        assert int(by["2024-01"]["orders.streak"]) == 0
        assert int(by["2024-02"]["orders.streak"]) == 1
        assert int(by["2024-03"]["orders.streak"]) == 0

    async def test_cp_over_target_grain_inner(self, exec_engine) -> None:
        # customers.spend:sum > 0 holds every month → cumulative streak 1,2,3.
        # This is the shape that stays GUARDED for time_shift but renders for cp.
        resp = await exec_engine.execute(_q(measures=[
            ModelMeasure(formula="consecutive_periods(customers.spend:sum > 0)",
                         name="streak"),
        ]))
        by = _by_month(resp)
        assert int(by["2024-01"]["orders.streak"]) == 1
        assert int(by["2024-02"]["orders.streak"]) == 2
        assert int(by["2024-03"]["orders.streak"]) == 3


class TestNullDimPartitionSurvival:
    """A ``status`` dimension with a NULL group: the shift is partitioned by
    status and joined back null-safely, so the NULL-status group keeps its
    prior-month value instead of dropping to NULL."""

    async def test_null_status_group_keeps_prev(self, exec_engine) -> None:
        # status groups by month:
        #   'ok'  : Jan (o1 10, o2 5)=15,  Feb (o3 20)=20
        #   'hold': Mar (o4 10)=10
        #   NULL  : Feb (o5 4)=4,  Mar (o6 8)=8
        resp = await exec_engine.execute(_q(
            dimensions=["status"],
            measures=[
                ModelMeasure(formula="amount:sum", name="s"),
                ModelMeasure(formula="time_shift(amount:sum, -1)", name="prev"),
            ],
        ))
        by = {
            (r["orders.status"], month_key(r["orders.ordered_at"])): r
            for r in resp.data
        }
        # NULL-status: Feb=4 has no prior (Jan NULL group empty) → None;
        # Mar=8's prior is Feb's NULL-group 4 — found only via a null-safe
        # join-back on the NULL status grain.
        assert float(by[(None, "2024-02")]["orders.s"]) == pytest.approx(4.0)
        assert float(by[(None, "2024-03")]["orders.prev"]) == pytest.approx(4.0)


class TestDateRangeFrameBoundOmitted:
    """7b.3c: a ``date_range`` frame bound is omitted from the shifted CTE, so
    the earliest VISIBLE bucket still finds its prior period from raw data
    outside the range."""

    async def test_feb_prev_reads_january_outside_range(self, exec_engine) -> None:
        # Restrict to Feb–Mar; Feb's prior (January) is outside the range but the
        # shifted CTE reads raw, so Feb.prev = Jan's 15 (local sum). A sibling
        # cross-model measure keeps the query on the cross-model chain.
        resp = await exec_engine.execute(SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="ordered_at"),
                granularity=TimeGranularity.MONTH,
                date_range=["2024-02-01", "2024-03-31"],
            )],
            measures=[
                ModelMeasure(formula="amount:sum", name="s"),
                ModelMeasure(formula="time_shift(amount:sum, -1)", name="prev"),
                ModelMeasure(formula="customers.spend:sum", name="cm"),
            ],
        ))
        by = _by_month(resp)
        assert "2024-01" not in by, "January must be filtered out of the result"
        assert float(by["2024-02"]["orders.prev"]) == pytest.approx(15.0)
