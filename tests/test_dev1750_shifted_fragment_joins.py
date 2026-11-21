"""Part 1 — the shifted (``time_shift``) CTE must register the relationships
its inner aggregate's template FRAGMENTS (and positional field args) cross,
through the same one door the host and ``_cm_`` paths use.

Before this, ``_emit_time_shift_ctes_for_planned`` registered source / typed
field kwargs but never the string fragments, so a
crossing default like ``w='customers__regions.weight'`` would re-aggregate
``SUM(orders.amount * customers__regions.weight)`` in the shifted CTE with no
join to ``regions`` — SQL no database binds.

The SQL-shape assertions target the ``shifted_*`` CTE body specifically: a
whole-SQL substring check can be satisfied by a valid alias in the base or the
combined SELECT.
"""

from __future__ import annotations

import pytest

from slayer.core.keys import AggregateKey, FieldKey
from slayer.core.scope import DatasetScope
from slayer.engine.binding import bind_expr
from slayer.core.syntax import parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.scope_check import assert_scope_closed

from slayer.core.query import QueryMetric

from tests._dev1750_fixtures import (
    SlayerQuery,
    base_cte_body,
    gen,
    month_td,
    dev1750_datasets,
    shifted_cte_body,
    shifted_relation,
)
from tests._engine_helpers import _extract_cte_body


def _q(*, metrics) -> SlayerQuery:
    return SlayerQuery(
        population="orders", time_dimensions=month_td(), metrics=metrics,
    )


class TestShiftedCteFragmentDefaultParam:
    """Shape (b): the crossing-fragment inner is a HOST-rooted
    producer; the shifted CTE reads the producer's joined-back column, and the
    fragment's relationship hops live inside the producer's own ``_cm_`` CTE."""

    async def test_shifted_cte_joins_the_fragment_hops(self) -> None:
        sql = await gen(_q(metrics=[
            QueryMetric(metric="time_shift(wscaled_sum(orders.amount), -1)", alias="prev"),
        ]))
        # The shift reads the crossing producer; the fragment's two hops + the
        # qualified weight fragment + the host ``amount`` operand all live there.
        assert shifted_relation(sql).startswith("_cm_"), sql
        producer = shifted_cte_body(sql)
        assert "JOIN customers" in producer, producer
        assert "regions AS customers__regions" in producer, producer
        assert "customers__regions.weight" in producer, producer
        assert "orders.amount" in producer, producer

    async def test_host_base_does_not_carry_the_shifted_reaggregation(self) -> None:
        """The shifted re-aggregation belongs to the shifted CTE, not the host
        ``base`` — a leak there would double-join ``regions`` at host grain."""
        sql = await gen(_q(metrics=[
            QueryMetric(metric="sum(orders.amount)", alias="s"),
            QueryMetric(metric="time_shift(wscaled_sum(orders.amount), -1)", alias="prev"),
        ]))
        base = base_cte_body(sql)
        # The local sibling ``sum(amount)`` lives in the base; the crossing
        # wscaled re-aggregation does not (it is isolated + shifted elsewhere).
        assert "SUM(orders.amount)" in base, base
        assert "customers__regions.weight" not in base, base


class TestShiftedCteFragmentUserKwarg:
    """A user-supplied string kwarg that substitutes into the template is a
    fragment too (parity with the ``_cm_`` path) — its crossing relationship must land
    in the shifted CTE."""

    async def test_user_string_kwarg_crossing_join_registers(self) -> None:
        sql = await gen(_q(metrics=[
            QueryMetric(
                metric="time_shift(wscaled_sum(orders.amount, w='customers.regions.weight'), -1)",
                alias="prev",
            ),
        ]))
        assert shifted_relation(sql).startswith("_cm_"), sql
        producer = shifted_cte_body(sql)
        assert "JOIN customers" in producer, producer
        assert "regions AS customers__regions" in producer, producer
        assert "customers__regions.weight" in producer, producer


class TestFragmentBindingUnit:
    """An OVERRIDDEN default is replaced at bind: only the override lands on the
    key, so no path can register the default fragment's relationship."""

    @staticmethod
    def _kwargs(expression: str) -> dict:
        models = dev1750_datasets()
        key = bind_expr(
            parse_expr(expression), scope=DatasetScope(population=models[0]),
            bundle=ResolvedSourceBundle(dialect="duckdb", population=models[0],
                                        referenced_datasets=models[1:]),
        ).value_key
        assert isinstance(key, AggregateKey), key
        return dict(key.kwargs)

    def test_non_overridden_default_is_bound(self) -> None:
        assert self._kwargs("wscaled_sum(amount)") == {
            "w": FieldKey(path=("customers", "regions"), leaf="weight")}

    def test_overridden_default_uses_the_override_not_the_default(self) -> None:
        assert self._kwargs("wscaled_sum(amount, w='customers.region_id')") == {
            "w": FieldKey(path=("customers",), leaf="region_id")}


class TestShiftedCtePositionalArgRegistration:
    """Codex F2/F3: the shifted block must resolve every crossing
    positional field arg through the scope, mirroring the ``_cm_`` path's
    ``for _arg in local_agg_key.args`` loop — a first/last explicit time arg is
    the standing case.

    The first/last now DESUGARS into a ``_cm_`` producer that
    owns the crossing relationship and the ranked subquery, so end-to-end this shape now
    RENDERS (the ranked-in-shifted seam is no longer needed). The contract
    pinned here is unchanged: NO silent scope leak / unbound alias — the crossing
    is bound inside the producer the shifted CTE references, never leaked bare
    into the shifted re-aggregation."""

    @pytest.mark.parametrize("agg", ["last", "first"])
    async def test_first_last_crossing_time_arg_no_silent_leak(self, agg: str) -> None:
        q = _q(metrics=[
            QueryMetric(metric="sum(customers.spend)", alias="cm"),
            QueryMetric(
                metric=f"time_shift({agg}(orders.amount, customers.signup_at), -1)",
                alias="prev",
            ),
        ])
        sql = await gen(q)  # renders via the desugared producer
        assert_scope_closed(sql, dialect="duckdb")
        assert "__regroup__" not in sql, sql
        # The crossing does not leak bare into the join-back; the shift reads the
        # ranked producer, whose crossing relationship + ORDER BY key live inside it.
        sjoin = _extract_cte_body(sql, r"sjoin_\w+")
        assert "customers.signup_at" not in sjoin, sjoin
        assert shifted_relation(sql).startswith(f"_cm_orders__amount_{agg}"), sql
        producer = shifted_cte_body(sql)
        assert "JOIN customers" in producer, producer
        assert "customers.signup_at" in producer, producer


class TestCrossModelHiddenAliasReservation:
    """A hidden ``time_shift`` placeholder (``_time_shift_inner``)
    minted inside the cross-dataset transform chain must never shadow a real user
    metric of that name. The chain reuses the generation-wide ``_gen_allocator``
    (already carrying every projected alias from the ``_cm_`` / combined-SELECT
    build), so ``allocate_cte`` bumps the hidden one to ``_time_shift_inner_2`` —
    the host chain gets the same guarantee via its own explicit reservation on a
    fresh allocator (``test_dev1713_naming``)."""

    async def test_hidden_alias_bumped_off_user_column(self) -> None:
        # ``sum(customers.spend)`` forces the cross-dataset transform chain; the
        # arithmetic-wrapped ``time_shift`` mints a hidden ``_time_shift_inner``
        # slot that would collide with the like-named user metric.
        sql = await gen(_q(metrics=[
            QueryMetric(metric="sum(customers.spend)", alias="cm"),
            QueryMetric(metric="sum(orders.amount)", alias="_time_shift_inner"),
            QueryMetric(
                metric="sum(orders.amount) - time_shift(sum(orders.amount), -1)", alias="growth",
            ),
        ]))
        # The user metric keeps its own key (closing quote pins the exact
        # name, not the ``_2`` suffix).
        assert '"_time_shift_inner"' in sql, sql
        # The hidden shift alias was bumped off the user's name, not collided.
        assert '"_time_shift_inner_2"' in sql, sql
