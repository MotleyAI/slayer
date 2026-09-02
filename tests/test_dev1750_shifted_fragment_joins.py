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
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1750_fixtures import (
    ModelMeasure,
    SlayerQuery,
    base_cte_body,
    gen,
    month_td,
    orders_model,
    shifted_cte_body,
)
from tests._engine_helpers import _extract_cte_body


def _q(*, measures) -> SlayerQuery:
    return SlayerQuery(
        source_model="orders", time_dimensions=month_td(), measures=measures,
    )


class TestShiftedCteFragmentDefaultParam:
    """Shape (b): DEV-1838 D5 — the crossing-fragment inner is a HOST-rooted
    producer; the shifted CTE reads the producer's joined-back column, and the
    fragment's join hops live inside the producer's own ``_cm_`` CTE."""

    async def test_shifted_cte_joins_the_fragment_hops(self) -> None:
        sql = await gen(_q(measures=[
            ModelMeasure(formula="time_shift(amount:wscaled_sum, -1)", name="prev"),
        ]))
        shifted = shifted_cte_body(sql)
        # The shifted CTE LEFT JOINs the producer (DEV-1835 D7 join-back).
        assert "JOIN _cm_" in shifted, shifted
        # The fragment's two hops + the qualified weight fragment + the host
        # ``amount`` operand all live in the producer's own CTE body.
        producer = _extract_cte_body(sql, r"_cm_\w+")
        assert "JOIN customers" in producer, producer
        assert "regions AS customers__regions" in producer, producer
        assert "customers__regions.weight" in producer, producer
        assert "orders.amount" in producer, producer

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
                formula="time_shift(amount:wscaled_sum(w='customers.regions.weight'), -1)",
                name="prev",
            ),
        ]))
        shifted = shifted_cte_body(sql)
        assert "JOIN _cm_" in shifted, shifted
        producer = _extract_cte_body(sql, r"_cm_\w+")
        assert "JOIN customers" in producer, producer
        assert "regions AS customers__regions" in producer, producer
        assert "customers__regions.weight" in producer, producer


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
        assert entered == ["customers.regions.weight"], entered

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

    DEV-1835 lift: the first/last now DESUGARS into a ``_cm_`` producer that
    owns the crossing join and the ranked subquery, so end-to-end this shape now
    RENDERS (the ranked-in-shifted seam is no longer needed). The contract
    pinned here is unchanged: NO silent scope leak / unbound alias — the crossing
    is bound inside the producer the shifted CTE references, never leaked bare
    into the shifted re-aggregation."""

    @pytest.mark.parametrize("agg", ["last", "first"])
    async def test_first_last_crossing_time_arg_no_silent_leak(self, agg: str) -> None:
        q = _q(measures=[
            ModelMeasure(formula="customers.spend:sum", name="cm"),
            ModelMeasure(
                formula=f"time_shift(amount:{agg}(customers.signup_at), -1)",
                name="prev",
            ),
        ])
        sql = await gen(q)  # DEV-1835 lift: renders via the desugared producer
        assert_scope_closed(sql, dialect="duckdb")
        assert "__regroup__" not in sql, sql
        # The crossing does not leak bare into the shifted CTE; the shifted CTE
        # references the producer's already-computed column instead.
        shifted = shifted_cte_body(sql)
        assert "_cm_" in shifted, shifted
        assert "customers.signup_at" not in shifted, shifted
        # The ranking arg's crossing join + ORDER BY key live INSIDE the producer.
        producer = _extract_cte_body(sql, rf"_cm_orders__amount_{agg}\w+")
        assert "JOIN customers" in producer, producer
        assert "customers.signup_at" in producer, producer


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
