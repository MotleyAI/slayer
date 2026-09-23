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

import pytest

from slayer.core.formula import ALL_TRANSFORMS
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

SHIFT_OPS = frozenset({"time_shift", "change", "change_pct"})
NON_SHIFT_OPS = sorted(ALL_TRANSFORMS - SHIFT_OPS - {"first", "last"})

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
        assert SHIFT_OPS <= ALL_TRANSFORMS
        assert {"first", "last"} <= ALL_TRANSFORMS
        assert set(NON_SHIFT_OPS) <= ALL_TRANSFORMS
        assert not set(NON_SHIFT_OPS) & SHIFT_OPS


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
        query, bundle = _q(**kw), _bundle()
        with pytest.raises(ValueError) as ei:
            plan_query(query=query, bundle=bundle)
        _assert_leg_b_message(str(ei.value), op)

    async def test_bare_leaf_measure_via_engine(self, exec_engine):
        """Scenario: Bare grain-refining leaf rejected — never SQL whose base
        grain is one row per (bucket, weight-value)."""
        q = _q(time_dimensions=month_td(),
               measures=[ModelMeasure(formula="cumsum(weight)", name="t")])
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(q)
        _assert_leg_b_message(str(ei.value), "cumsum")

    async def test_rank_family_covered(self, exec_engine):
        """Scenario: Rank family is covered."""
        q = _q(dimensions=["store"], time_dimensions=month_td(),
               measures=[ModelMeasure(formula="rank(qty)", name="t")])
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(q)
        _assert_leg_b_message(str(ei.value), "rank")

    async def test_predicate_over_unprojected_column(self, exec_engine):
        """Scenario: Predicate over an unprojected row column rejected."""
        q = _q(time_dimensions=month_td(),
                measures=[ModelMeasure(formula="consecutive_periods(weight > 0)",
                                       name="t")])
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(q)
        _assert_leg_b_message(str(ei.value), "consecutive_periods")

    async def test_raw_time_source_column_refines_the_bucket(self, exec_engine):
        """The bucketed TD's raw source column is not a projected grain key."""
        q = _q(time_dimensions=month_td(),
               measures=[ModelMeasure(formula="rank(ordered_at)", name="t")])
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(q)
        _assert_leg_b_message(str(ei.value), "rank")

    async def test_filter_position(self, exec_engine):
        q = _q(time_dimensions=month_td(),
               filters=["cumsum(weight) > 0"],
               measures=[ModelMeasure(formula="revenue:sum", name="r")])
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(q)
        _assert_leg_b_message(str(ei.value), "cumsum")

    async def test_order_position(self, exec_engine):
        q = _q(time_dimensions=month_td(),
               order=[{"column": "rank(qty)", "direction": "desc"}],
               measures=[ModelMeasure(formula="revenue:sum", name="r")])
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(q)
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


class TestWalkerBehaviourUnchanged:
    """Behaviour guards for the §5.5 unification (green before and after): an
    aggregate input and an all-projected composite stay legal."""

    async def test_pure_aggregate_input_is_legal(self, exec_engine):
        resp = await exec_engine.execute(_q(
            dimensions=["store"], time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="cumsum(weight:sum(partition_by=store))", name="t")]))
        assert resp.data

    async def test_all_projected_composite_is_legal(self, exec_engine):
        resp = await exec_engine.execute(_q(
            dimensions=["weight", "qty"],
            measures=[ModelMeasure(formula="rank(weight * qty)", name="t")]))
        assert resp.data


class TestShiftFamilyRejectsRowLeaf:
    """The shift family obeys the one grain-refining row-leaf rule: a bare
    unprojected row column is rejected, never a multiplied result."""

    @pytest.mark.parametrize("formula, op", [
        ("time_shift(weight, -1)", "time_shift"),
        ("change(weight)", "change"),
        ("change_pct(weight)", "change_pct"),
    ])
    async def test_bare_row_leaf_rejected(self, exec_engine, formula, op):
        query = _q(time_dimensions=month_td(),
                   measures=[ModelMeasure(formula=formula, name="t")])
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(query)
        msg = str(ei.value)
        for part in (f"'{op}'", "'weight'", "weight:sum", "source_queries"):
            assert part in msg, msg


class TestLastOverShiftComposition:
    """A window transform whose input is a shift-derived composite (``change`` →
    ``x - time_shift(x)``) materialises only in the POST phase, yet must resolve
    within the transform chain by descending to its children — regression for
    the ``last(change(x))`` layer-readiness stall."""

    async def test_last_over_change_broadcasts_latest_period(self, exec_engine):
        resp = await exec_engine.execute(_q(
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="last(change(revenue:sum))", name="t")]))
        # revenue:sum 60/100/60 → change NULL/+40/-40; last() = -40 broadcast.
        got = {month_key(r["sales.ordered_at"]): r["sales.t"] for r in resp.data}
        assert got == {"2024-01": -40.0, "2024-02": -40.0, "2024-03": -40.0}

    async def test_last_over_change_in_filter_selects_declining_partition(self, exec_engine):
        # Notebook CELL 12: change(x) projected AND last(change(x)) < 0 filtered,
        # partitioned by a projected dimension. The projected change gives the
        # composite the POST-phase slot the `last` layer depends on — the exact
        # shape that stalled. Store B revenue 30/60/10 → latest change -50 (kept);
        # store A 30/40/50 → +10 (dropped). Pin the survivors and their change.
        resp = await exec_engine.execute(_q(
            time_dimensions=month_td(), dimensions=["store"],
            measures=["revenue:sum",
                      ModelMeasure(formula="change(revenue:sum)", name="ch")],
            filters=["last(change(revenue:sum)) < 0"]))
        rows = [(r["sales.store"], month_key(r["sales.ordered_at"]), r["sales.ch"])
                for r in resp.data]
        assert len(rows) == 3  # no duplicate broadcast rows
        assert set(rows) == {
            ("B", "2024-01", None), ("B", "2024-02", 30.0), ("B", "2024-03", -50.0)}
