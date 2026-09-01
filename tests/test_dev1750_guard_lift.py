"""DEV-1750 — lifting the ``time_shift`` / ``consecutive_periods`` ×
cross-model guard (``stage 7b.15e``).

The old guard rejected EITHER temporal op the moment the query also routed
through the cross-model chain, regardless of the transform's own inner
aggregate. DEV-1750 narrowed it to the one shape that could not render: a
``time_shift`` whose inner aggregate is a TARGET-GRAIN cross-model aggregate —
host-rooted re-aggregation there multiplied target rows through the 1:N join.
DEV-1836 removes that failure mode: the target-grain cross-model aggregate is
computed by a target-rooted producer and broadcast, so this shape now renders
too (its value broadcasts across the query grain, with a grain warning). Every
shape here renders:

* (a) local inner + a sibling cross-model measure,
* (b) host-rooted crossing-fragment inner (``amount:wscaled_sum``),
* (c) target-grain cross-model inner (``customers.spend:sum``) — DEV-1836,
* ``consecutive_periods`` over any inner (it reads a materialised alias, never
  re-aggregates),
* ``change`` / ``change_pct`` (they desugar to ``time_shift``).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from slayer.sql.generator import SQLGenerator

from tests._dev1750_fixtures import (
    ModelMeasure,
    SlayerQuery,
    gen,
    month_td,
)

pytestmark = pytest.mark.asyncio


def _q(*, measures) -> SlayerQuery:
    return SlayerQuery(
        source_model="orders", time_dimensions=month_td(), measures=measures,
    )


# --------------------------------------------------------------------------- #
# Shapes that must now RENDER (no raise).
# --------------------------------------------------------------------------- #
class TestLiftedShapesRender:
    async def test_a_local_time_shift_with_cross_model_sibling(self) -> None:
        """(a) A local ``time_shift`` beside a cross-model measure: the sibling
        routes the query to the cross-model chain, but the shift's own inner is
        local and re-aggregates host-rooted."""
        sql = await gen(_q(measures=[
            ModelMeasure(formula="customers.spend:sum", name="cm"),
            ModelMeasure(formula="time_shift(amount:sum, -1)", name="prev"),
        ]))
        assert "shifted_" in sql, sql
        assert "_cm_" in sql, sql  # the sibling still isolates into its own CTE

    async def test_b_host_rooted_crossing_fragment_inner(self) -> None:
        """(b) ``time_shift`` over the crossing-fragment aggregate — the issue's
        named repro. Renders; the shifted CTE pulls the fragment's join (pinned
        in the fragment-join module)."""
        sql = await gen(_q(measures=[
            ModelMeasure(formula="time_shift(amount:wscaled_sum, -1)", name="prev"),
        ]))
        assert "shifted_" in sql, sql

    async def test_consecutive_periods_with_cross_model_sibling(self) -> None:
        sql = await gen(_q(measures=[
            ModelMeasure(formula="customers.spend:sum", name="cm"),
            ModelMeasure(formula="consecutive_periods(amount:sum > 0)", name="streak"),
        ]))
        assert "cp_" in sql, sql

    async def test_consecutive_periods_over_target_grain_inner(self) -> None:
        """cp has NO target-grain failure mode: it reads the combined SELECT's
        already-materialised cross-model alias and never re-aggregates. So cp
        over a target-grain aggregate — the shape that stays guarded for
        time_shift — renders."""
        sql = await gen(_q(measures=[
            ModelMeasure(
                formula="consecutive_periods(customers.spend:sum > 0)",
                name="streak",
            ),
        ]))
        assert "cp_" in sql, sql

    async def test_change_desugars_and_renders(self) -> None:
        sql = await gen(_q(measures=[
            ModelMeasure(formula="customers.spend:sum", name="cm"),
            ModelMeasure(formula="change(amount:sum)", name="delta"),
        ]))
        assert "shifted_" in sql, sql

    async def test_change_pct_desugars_and_renders(self) -> None:
        sql = await gen(_q(measures=[
            ModelMeasure(formula="customers.spend:sum", name="cm"),
            ModelMeasure(formula="change_pct(amount:sum)", name="delta"),
        ]))
        assert "shifted_" in sql, sql


# --------------------------------------------------------------------------- #
# The target-grain shape DEV-1836 lifts — renders via a target-rooted producer.
# --------------------------------------------------------------------------- #
class TestTargetGrainCrossModelRenders:
    async def test_c_target_grain_inner_renders(self) -> None:
        """(c) ``time_shift(customers.spend:sum, -1)`` — DEV-1836 computes the
        target-grain cross-model aggregate with a target-rooted producer and
        broadcasts it, so the row-multiplying re-aggregation the 7b.15e guard
        protected against no longer occurs; it renders."""
        sql = await gen(_q(measures=[
            ModelMeasure(formula="time_shift(customers.spend:sum, -1)", name="prev"),
        ]))
        assert "shifted_" in sql, sql

    async def test_c_reaches_the_emitter(self) -> None:
        """The lifted shape now reaches ``_emit_time_shift_ctes_for_planned`` —
        the shifted CTE is genuinely emitted, counter to the old guard that
        raised before the emitter ran."""
        real = SQLGenerator._emit_time_shift_ctes_for_planned
        calls: list = []

        def _spy(self, **kwargs):
            calls.append(kwargs.get("slot"))
            return real(self, **kwargs)

        q = _q(measures=[
            ModelMeasure(formula="time_shift(customers.spend:sum, -1)", name="prev"),
        ])
        with patch.object(
            SQLGenerator, "_emit_time_shift_ctes_for_planned", _spy,
        ):
            await gen(q)
        assert calls, "the lifted target-grain shape never reached the emitter"

    async def test_b_does_reach_the_emitter(self) -> None:
        """The host-rooted shape (b) also reaches
        ``_emit_time_shift_ctes_for_planned``, with exactly one shifted slot."""
        real = SQLGenerator._emit_time_shift_ctes_for_planned
        calls: list = []

        def _spy(self, **kwargs):
            calls.append(kwargs.get("slot"))
            return real(self, **kwargs)

        with patch.object(
            SQLGenerator, "_emit_time_shift_ctes_for_planned", _spy,
        ):
            await gen(_q(measures=[
                ModelMeasure(formula="time_shift(amount:wscaled_sum, -1)", name="prev"),
            ]))
        assert len(calls) == 1, calls


# --------------------------------------------------------------------------- #
# Regression: window ops over cross-model were always allowed; still are.
# --------------------------------------------------------------------------- #
class TestWindowOpsUnaffected:
    async def test_cumsum_over_cross_model_still_renders(self) -> None:
        """``cumsum`` (a window op, never guarded) over a cross-model aggregate
        must keep rendering — the narrowing must not disturb the window arm."""
        sql = await gen(_q(measures=[
            ModelMeasure(formula="cumsum(customers.spend:sum)", name="run"),
        ]))
        # A cumsum renders as a windowed running sum in the transform-chain step
        # CTE — pin the actual window shape, not a vacuous SUM( that any aggregate
        # SQL contains.
        assert "step1" in sql, sql
        assert "OVER (" in sql, sql


# --------------------------------------------------------------------------- #
# Pre-existing loud errors that must survive the lift.
# --------------------------------------------------------------------------- #
class TestPreExistingGuardsSurvive:
    async def test_composite_input_time_shift_still_raises_7b11(self) -> None:
        """A composite-input transform (``time_shift`` over an arithmetic of two
        aggregates) is deferred by 7b.11 and must keep raising that — the lift
        touches only the 7b.15e guard."""
        q = _q(measures=[
            ModelMeasure(formula="customers.spend:sum", name="cm"),
            ModelMeasure(
                formula="time_shift(amount:sum + amount:sum, -1)", name="prev",
            ),
        ])
        with pytest.raises(NotImplementedError) as ei:
            await gen(q)
        assert "7b.11" in str(ei.value), str(ei.value)
