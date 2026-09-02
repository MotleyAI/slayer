"""DEV-1750/1836 — the ``time_shift`` / ``consecutive_periods`` × cross-model
shapes all render now: (a) local inner + a cross-model sibling; (b) host-rooted
crossing-fragment inner; (c) target-grain cross-model inner (lifted by DEV-1836);
``consecutive_periods`` over any inner; ``change`` / ``change_pct``. DEV-1846
adds a composite-input ``time_shift`` case at the bottom.
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


class TestLiftedShapesRender:
    async def test_a_local_time_shift_with_cross_model_sibling(self) -> None:
        """(a) A local ``time_shift`` beside a cross-model sibling: the shift's
        own inner is local and re-aggregates host-rooted."""
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
        """cp has no target-grain failure mode: it reads the materialised
        cross-model alias and never re-aggregates, so it renders."""
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


class TestTargetGrainCrossModelRenders:
    async def test_c_target_grain_inner_renders(self) -> None:
        """(c) DEV-1836 broadcasts the target-grain cross-model aggregate from a
        target-rooted producer, so the row-multiplying re-aggregation is gone."""
        sql = await gen(_q(measures=[
            ModelMeasure(formula="time_shift(customers.spend:sum, -1)", name="prev"),
        ]))
        assert "shifted_" in sql, sql

    async def test_c_reaches_the_emitter(self) -> None:
        """The lifted shape now reaches the emitter — the shifted CTE is emitted,
        counter to the old guard that raised before the emitter ran."""
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


class TestCompositeInputTimeShiftRenders:
    async def test_composite_input_time_shift_now_renders(self) -> None:
        """A composite-input ``time_shift`` (arithmetic of two aggregates) that
        the old 7b.11 guard deferred now renders a shifted-CTE re-aggregation —
        no NotImplementedError remains."""
        q = _q(measures=[
            ModelMeasure(formula="customers.spend:sum", name="cm"),
            ModelMeasure(
                formula="time_shift(amount:sum + amount:sum, -1)", name="prev",
            ),
        ])
        sql = await gen(q)
        assert "shifted_" in sql, sql
        assert "sjoin_" in sql, sql
        assert "7b.11" not in sql
