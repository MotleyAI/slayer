"""DEV-1750 — lifting the ``time_shift`` / ``consecutive_periods`` ×
cross-model guard (``stage 7b.15e``).

The old guard rejected EITHER temporal op the moment the query also routed
through the cross-model chain, regardless of what the transform's own inner
aggregate was. DEV-1750 narrows it to exactly the one shape that cannot render:
a ``time_shift`` whose inner aggregate is a TARGET-GRAIN cross-model aggregate
(``cte_root_model`` is None) — host-rooted re-aggregation there would multiply
target rows through the 1:N join. Everything else renders:

* (a) local inner + a sibling cross-model measure,
* (b) host-rooted crossing-fragment inner (``amount:wscaled_sum``),
* ``consecutive_periods`` over any inner (it reads a materialised alias, never
  re-aggregates, so it has no target-grain failure mode — lifted entirely),
* ``change`` / ``change_pct`` (they desugar to ``time_shift``).

Every "renders" case here RAISES ``stage 7b.15e`` on ``main`` — the feature is
missing — so each fails for the right reason.
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


# --------------------------------------------------------------------------- #
# The narrowed-guard message contract (asserted by tests; defined once).
# --------------------------------------------------------------------------- #
_GUARD_TAG = "stage 7b.15e"
_NARROWED_MARKER = "TARGET-GRAIN"  # the distinguishing phrase the new message adds


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
# The one shape that must STAY guarded — narrowed message.
# --------------------------------------------------------------------------- #
class TestTargetGrainStaysGuarded:
    async def test_c_target_grain_inner_raises_narrowed_guard(self) -> None:
        """(c) ``time_shift(customers.spend:sum, -1)`` — the inner aggregate is
        target-rooted. Host-rooted re-aggregation would multiply target rows, so
        it stays behind a guard whose message now NAMES the target-grain shape
        (not the blanket op-based text)."""
        with pytest.raises(NotImplementedError) as ei:
            await gen(_q(measures=[
                ModelMeasure(formula="time_shift(customers.spend:sum, -1)", name="prev"),
            ]))
        msg = str(ei.value)
        assert _GUARD_TAG in msg, msg
        assert _NARROWED_MARKER in msg, msg
        # It must NOT be the old blanket wording (which raised for ANY cross-model
        # coexistence, including the now-supported shapes).
        assert "also has a cross-model aggregate" not in msg, msg

    async def test_c_guard_fires_before_the_emitter_runs(self) -> None:
        """The narrowed guard is a plan-ownership decision made BEFORE any shifted
        CTE is emitted — a target-grain shape must never reach
        ``_emit_time_shift_ctes_for_planned`` (which would emit the row-
        multiplying host-rooted re-aggregation)."""
        real = SQLGenerator._emit_time_shift_ctes_for_planned
        calls: list = []

        def _spy(self, **kwargs):
            calls.append(kwargs.get("slot"))
            return real(self, **kwargs)

        with patch.object(
            SQLGenerator, "_emit_time_shift_ctes_for_planned", _spy,
        ):
            with pytest.raises(NotImplementedError):
                await gen(_q(measures=[
                    ModelMeasure(
                        formula="time_shift(customers.spend:sum, -1)", name="prev",
                    ),
                ]))
        assert calls == [], (
            "the target-grain guard let the shifted emitter run before raising"
        )

    async def test_b_does_reach_the_emitter(self) -> None:
        """Counter-case, so the spy above is not vacuous: the host-rooted shape
        (b) DOES reach ``_emit_time_shift_ctes_for_planned``."""
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
        assert "step1" in sql or "cumsum" in sql.lower() or "SUM(" in sql, sql


# --------------------------------------------------------------------------- #
# Pre-existing loud errors that must survive the lift.
# --------------------------------------------------------------------------- #
class TestPreExistingGuardsSurvive:
    async def test_composite_input_time_shift_still_raises_7b11(self) -> None:
        """A composite-input transform (``time_shift`` over an arithmetic of two
        aggregates) is deferred by 7b.11 and must keep raising that — the lift
        touches only the 7b.15e guard."""
        with pytest.raises(NotImplementedError) as ei:
            await gen(_q(measures=[
                ModelMeasure(formula="customers.spend:sum", name="cm"),
                ModelMeasure(
                    formula="time_shift(amount:sum + amount:sum, -1)", name="prev",
                ),
            ]))
        assert "7b.11" in str(ei.value), str(ei.value)
