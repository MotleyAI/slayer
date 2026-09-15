"""DEV-1859 task 1.5 — leg B: non-shift transforms reject grain-refining
row-level leaves with a typed plan-time error (the checker); projected grain
keys stay legal; the shift family keeps its bare-leaf regime byte-for-byte.

The matrix parametrizes over the DERIVED non-shift set (every transform op
minus the shift family; first/last are aggregation-dispatched), so a newly
added transform op lands in the rejected set by default.

Spec: openspec …/specs/queries/transforms — "Non-shift transforms reject
grain-refining row-level leaves".
"""

from __future__ import annotations

import re
from collections import Counter

import pytest

from slayer.core.formula import ALL_TRANSFORMS
from slayer.engine.elaborate_env import _SHIFT_FAMILY_OPS
from slayer.engine.plan import plan_query
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1846_fixtures import (
    ModelMeasure,
    SlayerQuery,
    dev1846_models,
    make_exec_engine,
    month_key,
    month_td,
    rows_by,
)
from tests.test_law_guard_ratchet import DEFERRAL_CLASSIFIER

NON_SHIFT_OPS = sorted(ALL_TRANSFORMS - _SHIFT_FAMILY_OPS - {"first", "last"})

#: input-shape id -> the formula body around the unprojected row leaf.
SHAPES = {
    "bare": "weight",
    "composite": "weight * qty",
    "predicate": "weight > 0",
    "mixed": "weight * revenue:sum(partition_by=store)",
}


def _call(op: str, inner: str) -> str:
    return f"ntile({inner}, n=4)" if op == "ntile" else f"{op}({inner})"


def _bundle() -> ResolvedSourceBundle:
    models = dev1846_models()
    return ResolvedSourceBundle(source_model=models[0],
                                referenced_models=models[1:])


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "sales")
    return SlayerQuery(**kw)


def _assert_leg_b_message(msg: str, op: str) -> None:
    """The typed message names the transform, the row-level leaf kind, and
    the aggregate-the-leaf remedy (e.g. ``cumsum(weight:sum)``); it cites no
    tracking issue and is ratchet-clean."""
    assert op in msg, msg
    assert re.search(r"(?i)row-level", msg), msg
    assert ":sum" in msg, msg
    assert "DEV-" not in msg, msg
    assert not DEFERRAL_CLASSIFIER.search(msg), msg


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


class TestDerivedSet:
    def test_shift_family_exemption_is_exactly_three_ops(self):
        """A new transform op must default into the rejected set."""
        assert _SHIFT_FAMILY_OPS == {"time_shift", "change", "change_pct"}
        assert {"first", "last"} <= ALL_TRANSFORMS
        assert set(NON_SHIFT_OPS) <= ALL_TRANSFORMS
        assert not set(NON_SHIFT_OPS) & _SHIFT_FAMILY_OPS


class TestGrainRefiningLeafRejected:
    """The full op × input-shape matrix raises at plan time — before any SQL
    generation (plan_query never reaches the generator)."""

    @pytest.mark.parametrize("op", NON_SHIFT_OPS)
    @pytest.mark.parametrize("shape", list(SHAPES))
    def test_matrix_cell_rejected_at_plan_time(self, op, shape):
        kw = {"time_dimensions": month_td(),
              "measures": [ModelMeasure(formula=_call(op, SHAPES[shape]),
                                        name="t")]}
        if shape == "mixed":  # the constituent's partition key is projected
            kw["dimensions"] = ["store"]
        with pytest.raises(ValueError) as ei:
            plan_query(query=_q(**kw), bundle=_bundle())
        _assert_leg_b_message(str(ei.value), op)

    async def test_bare_leaf_measure_via_engine(self, exec_engine):
        """Scenario: Bare grain-refining leaf rejected — never SQL whose base
        grain is one row per (bucket, weight-value)."""
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(_q(
                time_dimensions=month_td(),
                measures=[ModelMeasure(formula="cumsum(weight)", name="t")]))
        _assert_leg_b_message(str(ei.value), "cumsum")

    async def test_rank_family_covered(self, exec_engine):
        """Scenario: Rank family is covered."""
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(_q(
                dimensions=["store"], time_dimensions=month_td(),
                measures=[ModelMeasure(formula="rank(qty)", name="t")]))
        _assert_leg_b_message(str(ei.value), "rank")

    async def test_predicate_over_unprojected_column(self, exec_engine):
        """Scenario: Predicate over an unprojected row column rejected."""
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(_q(
                time_dimensions=month_td(),
                measures=[ModelMeasure(formula="consecutive_periods(weight > 0)",
                                       name="t")]))
        _assert_leg_b_message(str(ei.value), "consecutive_periods")

    async def test_raw_time_source_column_refines_the_bucket(self, exec_engine):
        """The bucketed TD's raw source column is not a projected grain key."""
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(_q(
                time_dimensions=month_td(),
                measures=[ModelMeasure(formula="rank(ordered_at)", name="t")]))
        _assert_leg_b_message(str(ei.value), "rank")

    async def test_filter_position(self, exec_engine):
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(_q(
                time_dimensions=month_td(),
                filters=["cumsum(weight) > 0"],
                measures=[ModelMeasure(formula="revenue:sum", name="r")]))
        _assert_leg_b_message(str(ei.value), "cumsum")

    async def test_order_position(self, exec_engine):
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(_q(
                time_dimensions=month_td(),
                order=[{"column": "rank(qty)", "direction": "desc"}],
                measures=[ModelMeasure(formula="revenue:sum", name="r")]))
        _assert_leg_b_message(str(ei.value), "rank")


class TestProjectedGrainKeyStaysLegal:
    async def test_rank_over_projected_dimension(self, exec_engine):
        """Scenario: A projected grain key stays legal — rank(weight) with
        weight a query dimension, verified sound today."""
        resp = await exec_engine.execute(_q(
            dimensions=["weight"],
            measures=[ModelMeasure(formula="rank(weight)", name="t")]))
        by = rows_by(resp, "sales.weight")
        assert len(resp.data) == 2
        assert int(by[(2.0,)]["sales.t"]) == 1
        assert int(by[(1.0,)]["sales.t"]) == 2

    async def test_cumsum_over_projected_dimension_with_time(self, exec_engine):
        """The projected leaf evaluates at the query grain — one row per
        (weight, month) pair present, cumulative within the weight group."""
        resp = await exec_engine.execute(_q(
            dimensions=["weight"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula="cumsum(weight)", name="t")]))
        got = {(r["sales.weight"], month_key(r["sales.ordered_at"])):
               float(r["sales.t"]) for r in resp.data}
        assert got == {
            (1.0, "2024-01"): 1.0, (1.0, "2024-02"): 2.0, (1.0, "2024-03"): 3.0,
            (2.0, "2024-01"): 2.0, (2.0, "2024-02"): 4.0,
        }

    async def test_rank_over_projected_computed_dimension(self, exec_engine):
        """An input whose expression EQUALS a projected computed dimension is
        a projected grain key (design decision 8): it executes at the query
        grain against the dimension's slot — neither the leg-B rejection nor
        today's transform-layer RuntimeError leak."""
        wband = "CASE WHEN weight > 1 THEN 2 ELSE 1 END"
        resp = await exec_engine.execute(_q(
            dimensions=[{"expression": wband, "name": "wband"}],
            measures=[ModelMeasure(formula=f"rank({wband})", name="t")]))
        by = rows_by(resp, "sales.wband")
        assert len(resp.data) == 2
        assert int(by[(2,)]["sales.t"]) == 1
        assert int(by[(1,)]["sales.t"]) == 2


class TestShiftFamilyRegimeUnchanged:
    """Scenario: Shift family keeps its bare-leaf regime — the established
    read-and-rebucket output, pinned as an exact row multiset."""

    async def _pairs(self, exec_engine, formula) -> Counter:
        resp = await exec_engine.execute(_q(
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula=formula, name="t")]))
        return Counter((month_key(r["sales.ordered_at"]), r["sales.t"])
                       for r in resp.data)

    async def test_time_shift_bare_leaf(self, exec_engine):
        got = await self._pairs(exec_engine, "time_shift(weight, -1)")
        assert got == Counter({
            ("2024-01", None): 2,
            ("2024-02", 1.0): 2, ("2024-02", 2.0): 2,
            ("2024-03", 1.0): 1, ("2024-03", 2.0): 1,
        })

    async def test_change_bare_leaf(self, exec_engine):
        got = await self._pairs(exec_engine, "change(weight)")
        assert got == Counter({
            ("2024-01", None): 2,
            ("2024-02", -1.0): 1, ("2024-02", 0.0): 2, ("2024-02", 1.0): 1,
            ("2024-03", -1.0): 1, ("2024-03", 0.0): 1,
        })

    async def test_change_pct_bare_leaf(self, exec_engine):
        got = await self._pairs(exec_engine, "change_pct(weight)")
        assert got == Counter({
            ("2024-01", None): 2,
            ("2024-02", -0.5): 1, ("2024-02", 0.0): 2, ("2024-02", 1.0): 1,
            ("2024-03", -0.5): 1, ("2024-03", 0.0): 1,
        })
