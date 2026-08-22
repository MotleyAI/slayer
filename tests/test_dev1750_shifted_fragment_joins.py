"""DEV-1750 Part 1 — the shifted (``time_shift``) CTE must register the joins
its inner aggregate's template FRAGMENTS (and positional column args) cross,
through the same one door the host and ``_cm_`` paths use
(``_register_fragment_kwarg_joins`` / ``scope.resolve``).

Before this, ``_emit_time_shift_ctes_for_planned`` registered source / typed
column kwargs / ``column_filter_key`` but never the string fragments, so a
crossing default like ``w='customers__regions.weight'`` would re-aggregate
``SUM(orders.amount * customers__regions.weight)`` in the shifted CTE with no
join to ``regions`` — SQL no database binds.

The SQL-shape assertions target the ``shifted_*`` CTE body specifically: a
whole-SQL substring check can be satisfied by a valid alias in the base or the
combined SELECT.
"""

from __future__ import annotations

import pytest

from slayer.core.keys import AggregateKey, ColumnKey
from slayer.sql.generator import SQLGenerator

from tests._dev1750_fixtures import (
    ModelMeasure,
    SlayerQuery,
    base_cte_body,
    gen,
    month_td,
    orders_model,
    shifted_cte_body,
)


def _q(*, measures) -> SlayerQuery:
    return SlayerQuery(
        source_model="orders", time_dimensions=month_td(), measures=measures,
    )


class TestShiftedCteFragmentDefaultParam:
    """Shape (b): the crossing DEFAULT ``AggregationParam.sql`` fragment pulls
    its two join hops into the shifted CTE's own FROM."""

    async def test_shifted_cte_joins_the_fragment_hops(self) -> None:
        sql = await gen(_q(measures=[
            ModelMeasure(formula="time_shift(amount:wscaled_sum, -1)", name="prev"),
        ]))
        shifted = shifted_cte_body(sql)
        # Both hops of the fragment's path are real join clauses in the shifted
        # CTE (orders → customers → regions), the second under the __-path alias.
        assert "JOIN customers" in shifted, shifted
        assert "regions AS customers__regions" in shifted, shifted
        # The fragment rendered qualified at the host path (not a bare, unbound
        # ``regions.weight``), so it binds to the join the CTE now carries.
        assert "customers__regions.weight" in shifted, shifted
        # And the re-aggregation actually multiplies by the fragment: the host
        # ``amount`` operand appears alongside the weight fragment pinned above.
        assert "orders.amount" in shifted, shifted

    async def test_host_base_does_not_carry_the_shifted_reaggregation(self) -> None:
        """The shifted re-aggregation belongs to the shifted CTE, not the host
        ``base`` — a leak there would double-join ``regions`` at host grain."""
        sql = await gen(_q(measures=[
            ModelMeasure(formula="amount:sum", name="s"),
            ModelMeasure(formula="time_shift(amount:wscaled_sum, -1)", name="prev"),
        ]))
        base = base_cte_body(sql)
        # The local sibling ``amount:sum`` lives in the base; the crossing
        # wscaled re-aggregation does not (it is isolated + shifted elsewhere).
        assert "SUM(orders.amount)" in base, base
        assert "customers__regions.weight" not in base, base


class TestShiftedCteFragmentUserKwarg:
    """A user-supplied string kwarg that substitutes into the template is a
    fragment too (parity with the ``_cm_`` path) — its crossing join must land
    in the shifted CTE."""

    async def test_user_string_kwarg_crossing_join_registers(self) -> None:
        sql = await gen(_q(measures=[
            ModelMeasure(
                formula="time_shift(amount:wscaled_sum(w='customers__regions.weight'), -1)",
                name="prev",
            ),
        ]))
        shifted = shifted_cte_body(sql)
        assert "JOIN customers" in shifted, shifted
        assert "regions AS customers__regions" in shifted, shifted
        assert "customers__regions.weight" in shifted, shifted


class TestFragmentRegistrationUnit:
    """Unit-level parity with ``tests/test_dev1745_fragment_joins.py``: the
    shared helper treats an OVERRIDDEN default as replaced, so the shifted path
    (which calls the same helper) cannot double-register the default fragment's
    join."""

    @staticmethod
    def _entered(*, kwargs) -> list:
        gen_ = SQLGenerator(dialect="duckdb")
        seen: list = []
        gen_._enter_mode_a_expression = (  # type: ignore[method-assign]
            lambda **kw: seen.append(kw["sql"])
        )
        gen_._register_fragment_kwarg_joins(
            key=AggregateKey(
                source=ColumnKey(path=(), leaf="amount"), agg="wscaled_sum",
                kwargs=kwargs,
            ),
            scope=object(),
            model=orders_model(),
        )
        return seen

    def test_non_overridden_default_is_scanned(self) -> None:
        entered = self._entered(kwargs=())
        assert entered == ["customers__regions.weight"], entered

    def test_overridden_default_uses_the_override_not_the_default(self) -> None:
        # Overriding ``w`` replaces the default: only the override is scanned,
        # never the default ``customers__regions.weight``.
        entered = self._entered(kwargs=(("w", "customers.region_id"),))
        assert entered == ["customers.region_id"], entered
        assert "customers__regions.weight" not in entered, entered


class TestShiftedCtePositionalArgRegistration:
    """DEV-1750 (Codex F2/F3): the shifted block must resolve every crossing
    positional column arg through the scope, mirroring the ``_cm_`` path's
    ``for _arg in local_agg_key.args`` loop — a first/last explicit time arg is
    the standing case.

    First/last over ``time_shift`` is not renderable through the shifted
    re-aggregation seam (it needs a ranked subquery), so end-to-end this shape
    raises a loud error rather than emitting wrong SQL. The contract pinned here
    is: NO silent scope leak / unbound alias — either the join is present, or a
    specific NotImplementedError is raised."""

    @pytest.mark.parametrize("agg", ["last", "first"])
    async def test_first_last_crossing_time_arg_no_silent_leak(self, agg: str) -> None:
        q = _q(measures=[
            ModelMeasure(formula="customers.spend:sum", name="cm"),
            ModelMeasure(
                formula=f"time_shift(amount:{agg}(customers.signup_at), -1)",
                name="prev",
            ),
        ])
        try:
            sql = await gen(q)
        except NotImplementedError as exc:
            # A loud deferral is acceptable (the ranked-in-shifted seam does not
            # exist) — but it must be a SPECIFIC deferral, never the old blanket
            # op guard, whose wording the lift removes.
            assert "also has a cross-model aggregate" not in str(exc), str(exc)
            return
        # If it DID render, the ranking arg's join must be in the shifted CTE —
        # never an unbound reference / silent scope leak.
        shifted = shifted_cte_body(sql)
        assert "customers.signup_at" in shifted, shifted
        assert "JOIN customers" in shifted, shifted


class TestCrossModelHiddenAliasReservation:
    """DEV-1750: a hidden ``time_shift`` placeholder (``_time_shift_inner``)
    minted inside the cross-model transform chain must never shadow a real user
    measure of that name. The chain reuses the generation-wide ``_gen_allocator``
    (already carrying every projected alias from the ``_cm_`` / combined-SELECT
    build), so ``allocate_cte`` bumps the hidden one to ``_time_shift_inner_2`` —
    the host chain gets the same guarantee via its own explicit reservation on a
    fresh allocator (``test_dev1713_naming``)."""

    async def test_hidden_alias_bumped_off_user_column(self) -> None:
        # ``customers.spend:sum`` forces the cross-model transform chain; the
        # arithmetic-wrapped ``time_shift`` mints a hidden ``_time_shift_inner``
        # slot that would collide with the like-named user measure.
        sql = await gen(_q(measures=[
            ModelMeasure(formula="customers.spend:sum", name="cm"),
            ModelMeasure(formula="amount:sum", name="_time_shift_inner"),
            ModelMeasure(
                formula="amount:sum - time_shift(amount:sum, -1)", name="growth",
            ),
        ]))
        # The user measure keeps its own key (closing quote pins the exact
        # name, not the ``_2`` suffix).
        assert '"orders._time_shift_inner"' in sql, sql
        # The hidden shift alias was bumped off the user's name, not collided.
        assert '"orders._time_shift_inner_2"' in sql, sql
