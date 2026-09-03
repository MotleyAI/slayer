"""DEV-1842 task 1.9 — the generic path-map visitor in both directions.

``reroot_value_key`` becomes one total, fail-closed visitor parameterised by a
per-path transform; ``strip`` (existing, byte-identical) and the new ``prepend``
are thin wrappers over it. ``prepend_value_key(key, *, host_path)`` is the
inverse of ``reroot_value_key(key, target_path=host_path)``: it prefixes every
embedded join path with ``host_path``, re-anchoring a target-local bound tree
into the host's coordinate system.

Strip cases pin today (``reroot_value_key`` exists); prepend cases fail until
``prepend_value_key`` lands (referenced through the module so this file still
collects). ``AggregateKey.column_filter_key`` stays owner-anchored — unchanged in
BOTH directions — and the strip∘prepend round trip is the identity that makes a
dotted reference bound-tree-identical to the hand-written form.
"""

from __future__ import annotations

from decimal import Decimal
from typing import get_args

import pytest

from slayer.core import keys as K
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    InKey,
    LiteralKey,
    ScalarCallKey,
    SqlExprKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    reroot_value_key,
)

HOST = ("customers",)
DEEP = ("customers", "regions")


def _samples() -> dict:
    """One instance of every ValueKey member, anchored in TARGET-LOCAL space
    (paths start below the host) so prepend has something to re-anchor."""
    return {
        ColumnKey: ColumnKey(path=(), leaf="spend"),
        ColumnSqlKey: ColumnSqlKey(path=(), model="customers", column_name="cr"),
        TimeTruncKey: TimeTruncKey(
            column=ColumnKey(path=(), leaf="signup_at"), granularity="day",
        ),
        StarKey: StarKey(path=()),
        LiteralKey: LiteralKey(value=Decimal("1")),
        AggregateKey: AggregateKey(
            source=ColumnKey(path=(), leaf="spend"), agg="sum",
        ),
        TransformKey: TransformKey(
            op="cumsum",
            input=AggregateKey(source=ColumnKey(path=(), leaf="spend"), agg="sum"),
        ),
        ArithmeticKey: ArithmeticKey(
            op="+",
            operands=(ColumnKey(path=(), leaf="spend"), LiteralKey(value=Decimal("1"))),
        ),
        ScalarCallKey: ScalarCallKey(
            name="coalesce", args=(ColumnKey(path=(), leaf="tier"),),
        ),
        BetweenKey: BetweenKey(
            column=ColumnKey(path=(), leaf="signup_at"),
            low=LiteralKey(value="a"), high=LiteralKey(value="b"),
        ),
        InKey: InKey(
            column=ColumnKey(path=(), leaf="tier"),
            values=(LiteralKey(value="gold"),),
        ),
    }


# --------------------------------------------------------------------------- #
# Strip direction — pins today (the generic visitor keeps reroot byte-identical).
# --------------------------------------------------------------------------- #
class TestStripStillWorks:
    def test_column_key_strips_prefix(self) -> None:
        out = reroot_value_key(
            ColumnKey(path=("customers", "regions"), leaf="name"), target_path=HOST,
        )
        assert out == ColumnKey(path=("regions",), leaf="name")

    def test_aggregate_source_strips(self) -> None:
        out = reroot_value_key(
            AggregateKey(source=ColumnKey(path=("customers",), leaf="spend"), agg="sum"),
            target_path=HOST,
        )
        assert out.source == ColumnKey(path=(), leaf="spend")


# --------------------------------------------------------------------------- #
# Prepend direction — leaves.
# --------------------------------------------------------------------------- #
class TestPrependLeafKinds:
    def test_local_column_gains_host_prefix(self) -> None:
        out = K.prepend_value_key(ColumnKey(path=(), leaf="spend"), host_path=HOST)
        assert out == ColumnKey(path=("customers",), leaf="spend")

    def test_target_local_join_keeps_residual_under_prefix(self) -> None:
        out = K.prepend_value_key(
            ColumnKey(path=("regions",), leaf="pop"), host_path=HOST,
        )
        assert out == ColumnKey(path=("customers", "regions"), leaf="pop")

    def test_column_sql_key_gains_prefix(self) -> None:
        out = K.prepend_value_key(
            ColumnSqlKey(path=(), model="customers", column_name="cr"), host_path=HOST,
        )
        assert out == ColumnSqlKey(path=("customers",), model="customers", column_name="cr")

    def test_star_key_gains_prefix(self) -> None:
        out = K.prepend_value_key(StarKey(path=()), host_path=HOST)
        assert out == StarKey(path=("customers",))

    def test_literal_is_identity(self) -> None:
        key = LiteralKey(value=Decimal("1"))
        assert K.prepend_value_key(key, host_path=HOST) == key

    def test_time_trunc_prepends_wrapped_column(self) -> None:
        out = K.prepend_value_key(
            TimeTruncKey(column=ColumnKey(path=(), leaf="signup_at"), granularity="month"),
            host_path=HOST,
        )
        assert out.column == ColumnKey(path=("customers",), leaf="signup_at")

    def test_deep_host_path_prefixes_fully(self) -> None:
        out = K.prepend_value_key(ColumnKey(path=(), leaf="pop"), host_path=DEEP)
        assert out == ColumnKey(path=("customers", "regions"), leaf="pop")

    def test_standalone_sql_expr_key_prepends_referenced_paths(self) -> None:
        out = K.prepend_value_key(
            SqlExprKey(canonical_sql="regions.name = 'US'",
                       referenced_join_paths=(("regions",),)),
            host_path=HOST,
        )
        assert out.referenced_join_paths == (("customers", "regions"),)

    def test_sql_expr_key_recanonicalises_for_identity(self) -> None:
        """Prepend must RECONSTRUCT the SqlExprKey (not ``model_copy``), so the
        ``before`` validator re-sorts/dedups ``referenced_join_paths`` — two
        orderings of the same paths must prepend to keys that are equal AND hash
        equal, or the registry mints two slots for one value."""
        a = K.prepend_value_key(
            SqlExprKey(canonical_sql="x", referenced_join_paths=(("regions",), ("segments",))),
            host_path=HOST)
        b = K.prepend_value_key(
            SqlExprKey(canonical_sql="x", referenced_join_paths=(("segments",), ("regions",))),
            host_path=HOST)
        assert a == b
        assert hash(a) == hash(b)


# --------------------------------------------------------------------------- #
# Prepend direction — composites.
# --------------------------------------------------------------------------- #
class TestPrependCompositeKinds:
    def test_aggregate_source_args_kwargs_partitions(self) -> None:
        out = K.prepend_value_key(
            AggregateKey(
                source=ColumnKey(path=(), leaf="spend"),
                agg="last",
                args=(ColumnKey(path=(), leaf="signup_at"),),
                kwargs=(("weight", ColumnKey(path=("regions",), leaf="pop")),),
                partition_keys=frozenset({ColumnKey(path=(), leaf="tier")}),
            ),
            host_path=HOST,
        )
        assert out.source == ColumnKey(path=("customers",), leaf="spend")
        assert out.args == (ColumnKey(path=("customers",), leaf="signup_at"),)
        assert out.kwargs == (("weight", ColumnKey(path=("customers", "regions"), leaf="pop")),)
        assert out.partition_keys == frozenset({ColumnKey(path=("customers",), leaf="tier")})

    def test_scalar_kwarg_passes_through(self) -> None:
        out = K.prepend_value_key(
            AggregateKey(source=ColumnKey(path=(), leaf="spend"), agg="percentile",
                         kwargs=(("p", Decimal("0.5")),)),
            host_path=HOST,
        )
        assert out.kwargs == (("p", Decimal("0.5")),)

    def test_transform_input_partition_time(self) -> None:
        out = K.prepend_value_key(
            TransformKey(
                op="cumsum",
                input=AggregateKey(source=ColumnKey(path=(), leaf="spend"), agg="sum"),
                partition_keys=frozenset({ColumnKey(path=(), leaf="tier")}),
                time_key=TimeTruncKey(
                    column=ColumnKey(path=(), leaf="signup_at"), granularity="month",
                ),
            ),
            host_path=HOST,
        )
        assert out.input.source == ColumnKey(path=("customers",), leaf="spend")
        assert out.partition_keys == frozenset({ColumnKey(path=("customers",), leaf="tier")})
        assert out.time_key.column == ColumnKey(path=("customers",), leaf="signup_at")

    def test_arithmetic_scalarcall_between_in(self) -> None:
        out = K.prepend_value_key(
            ArithmeticKey(op="/", operands=(
                ScalarCallKey(name="coalesce", args=(
                    InKey(column=ColumnKey(path=(), leaf="tier"),
                          values=(LiteralKey(value="gold"),)),
                    BetweenKey(column=ColumnKey(path=(), leaf="signup_at"),
                               low=LiteralKey(value="a"), high=LiteralKey(value="b")),
                )),
                ColumnKey(path=("regions",), leaf="pop"),
            )),
            host_path=HOST,
        )
        call, col = out.operands
        assert col == ColumnKey(path=("customers", "regions"), leaf="pop")
        in_key, between = call.args
        assert in_key.column == ColumnKey(path=("customers",), leaf="tier")
        assert between.column == ColumnKey(path=("customers",), leaf="signup_at")


class TestPrependColumnFilterKeyInvariance:
    """``AggregateKey.column_filter_key`` is owner-anchored — copied UNCHANGED in
    both directions (parity with the strip visitor)."""

    def test_column_filter_key_unchanged(self) -> None:
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="spend"), agg="sum",
            column_filter_key=SqlExprKey(canonical_sql="regions.name = 'US'",
                                         referenced_join_paths=(("regions",),)),
        )
        out = K.prepend_value_key(key, host_path=HOST)
        assert out.source == ColumnKey(path=("customers",), leaf="spend")
        assert out.column_filter_key == key.column_filter_key


class TestPrependTotalityAndFailClosed:
    def test_every_member_is_handled(self) -> None:
        samples = _samples()
        members = set(get_args(ValueKey))
        assert not (members - set(samples)), "add a sample for the new ValueKey member"
        for member in members:
            K.prepend_value_key(samples[member], host_path=HOST)

    def test_unknown_kind_raises(self) -> None:
        class NotAKey:
            path = ("customers",)

        not_a_key = NotAKey()
        with pytest.raises(TypeError):
            K.prepend_value_key(not_a_key, host_path=HOST)

    def test_empty_host_path_is_identity(self) -> None:
        key = AggregateKey(source=ColumnKey(path=(), leaf="spend"), agg="sum")
        assert K.prepend_value_key(key, host_path=()) == key

    def test_none_vs_empty_partition_keys_preserved(self) -> None:
        absent = AggregateKey(source=ColumnKey(path=(), leaf="spend"), agg="sum",
                              partition_keys=None)
        grand = AggregateKey(source=ColumnKey(path=(), leaf="spend"), agg="sum",
                             partition_keys=frozenset())
        assert K.prepend_value_key(absent, host_path=HOST).partition_keys is None
        assert K.prepend_value_key(grand, host_path=HOST).partition_keys == frozenset()


class TestRoundTrip:
    """strip ∘ prepend is the identity — the property that makes a dotted
    reference bound-tree-identical to its hand-written twin."""

    @pytest.mark.parametrize("member", list(_samples()))
    def test_strip_undoes_prepend(self, member) -> None:
        key = _samples()[member]
        prepended = K.prepend_value_key(key, host_path=HOST)
        restored = reroot_value_key(prepended, target_path=HOST)
        # Equal AND hash-equal — the round trip must intern back onto one slot.
        assert restored == key
        assert hash(restored) == hash(key)

    def test_strip_undoes_prepend_deep(self) -> None:
        key = AggregateKey(
            source=ColumnKey(path=("regions",), leaf="pop"), agg="sum",
            partition_keys=frozenset({ColumnKey(path=(), leaf="tier")}),
        )
        prepended = K.prepend_value_key(key, host_path=HOST)
        assert reroot_value_key(prepended, target_path=HOST) == key

    def test_prepend_undoes_strip_for_target_prefixed_key(self) -> None:
        key = AggregateKey(source=ColumnKey(path=("customers", "regions"), leaf="pop"),
                           agg="sum")
        stripped = reroot_value_key(key, target_path=HOST)
        assert K.prepend_value_key(stripped, host_path=HOST) == key
