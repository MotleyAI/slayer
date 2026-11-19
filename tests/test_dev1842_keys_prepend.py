"""The generic path-map visitor in both directions.

``reroot_value_key`` becomes one total, fail-closed visitor parameterised by a
per-path transform; ``strip`` (existing, byte-identical) and the new ``prepend``
are thin wrappers over it. ``prepend_value_key(key, *, host_path)`` is the
inverse of ``reroot_value_key(key, target_path=host_path)``: it prefixes every
embedded join path with ``host_path``, re-anchoring a target-local bound tree
into the host's coordinate system.

Strip cases pin today (``reroot_value_key`` exists); prepend cases fail until
``prepend_value_key`` lands (referenced through the module so this file still
collects). The strip∘prepend round trip is the identity that makes a dotted
reference bound-tree-identical to the hand-written form.
"""

from __future__ import annotations

from decimal import Decimal
from typing import get_args

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core import keys as K
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ArrayLiteralKey,
    CastKey,
    ListSetKey,
    FieldKey,
    FieldSqlKey,
    InKey,
    LiteralKey,
    ScalarCallKey,
    StarKey,
    TimePointCmpKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    reroot_value_key,
    SqlFragmentKey,
)
from slayer.core.keys import Grain

HOST = ("customers",)
DEEP = ("customers", "regions")


def _samples() -> dict:
    """One instance of every ValueKey member, anchored in TARGET-LOCAL space
    (paths start below the host) so prepend has something to re-anchor."""
    return {
        FieldKey: FieldKey(path=(), leaf="spend"),
        FieldSqlKey: FieldSqlKey(path=(), dataset="customers", field_name="cr"),
        TimeTruncKey: TimeTruncKey(
            source=FieldKey(path=(), leaf="signup_at"), granularity=TimeGranularity.DAY,
        ),
        StarKey: StarKey(path=()),
        LiteralKey: LiteralKey(value=Decimal("1")),
        AggregateKey: AggregateKey(
            source=FieldKey(path=(), leaf="spend"), agg="sum",
        ),
        TransformKey: TransformKey(
            op="cumsum",
            input=AggregateKey(source=FieldKey(path=(), leaf="spend"), agg="sum"),
        ),
        ArithmeticKey: ArithmeticKey(
            op="+",
            operands=(FieldKey(path=(), leaf="spend"), LiteralKey(value=Decimal("1"))),
        ),
        ScalarCallKey: ScalarCallKey(
            name="coalesce", args=(FieldKey(path=(), leaf="tier"),),
        ),
        TimePointCmpKey: TimePointCmpKey(
            op=">=", operand=FieldKey(path=(), leaf="signup_at"), point="2025-Q1",
        ),
        InKey: InKey(
            field=FieldKey(path=(), leaf="tier"),
            values=(LiteralKey(value="gold"),),
        ),
        SqlFragmentKey: SqlFragmentKey(
            template="{r0} * 2", refs=(FieldKey(path=(), leaf="spend"),),
        ),
        CastKey: CastKey(source=FieldKey(path=(), leaf="spend"), type_text="INT", data_type=DataType.INTEGER),
        ArrayLiteralKey: ArrayLiteralKey(values=(LiteralKey(value="gold"),)),
        ListSetKey: ListSetKey(source=FieldKey(path=(), leaf="tags")),
    }


# --------------------------------------------------------------------------- #
# Strip direction — pins today (the generic visitor keeps reroot byte-identical).
# --------------------------------------------------------------------------- #
class TestStripStillWorks:
    def test_column_key_strips_prefix(self) -> None:
        out = reroot_value_key(
            FieldKey(path=("customers", "regions"), leaf="name"), target_path=HOST,
        )
        assert out == FieldKey(path=("regions",), leaf="name")

    def test_aggregate_source_strips(self) -> None:
        out = reroot_value_key(
            AggregateKey(source=FieldKey(path=("customers",), leaf="spend"), agg="sum"),
            target_path=HOST,
        )
        assert out.source == FieldKey(path=(), leaf="spend")


# --------------------------------------------------------------------------- #
# Prepend direction — leaves.
# --------------------------------------------------------------------------- #
class TestPrependLeafKinds:
    def test_local_column_gains_host_prefix(self) -> None:
        out = K.prepend_value_key(FieldKey(path=(), leaf="spend"), host_path=HOST)
        assert out == FieldKey(path=("customers",), leaf="spend")

    def test_target_local_join_keeps_residual_under_prefix(self) -> None:
        out = K.prepend_value_key(
            FieldKey(path=("regions",), leaf="pop"), host_path=HOST,
        )
        assert out == FieldKey(path=("customers", "regions"), leaf="pop")

    def test_column_sql_key_gains_prefix(self) -> None:
        out = K.prepend_value_key(
            FieldSqlKey(path=(), dataset="customers", field_name="cr"), host_path=HOST,
        )
        assert out == FieldSqlKey(path=("customers",), dataset="customers", field_name="cr")

    def test_star_key_gains_prefix(self) -> None:
        out = K.prepend_value_key(StarKey(path=()), host_path=HOST)
        assert out == StarKey(path=("customers",))

    def test_literal_is_identity(self) -> None:
        key = LiteralKey(value=Decimal("1"))
        assert K.prepend_value_key(key, host_path=HOST) == key

    def test_time_trunc_prepends_wrapped_column(self) -> None:
        out = K.prepend_value_key(
            TimeTruncKey(source=FieldKey(path=(), leaf="signup_at"), granularity=TimeGranularity.MONTH),
            host_path=HOST,
        )
        assert out.source == FieldKey(path=("customers",), leaf="signup_at")

    def test_deep_host_path_prefixes_fully(self) -> None:
        out = K.prepend_value_key(FieldKey(path=(), leaf="pop"), host_path=DEEP)
        assert out == FieldKey(path=("customers", "regions"), leaf="pop")

# --------------------------------------------------------------------------- #
# Prepend direction — composites.
# --------------------------------------------------------------------------- #
class TestPrependCompositeKinds:
    def test_aggregate_source_args_kwargs_partitions(self) -> None:
        out = K.prepend_value_key(
            AggregateKey(
                source=FieldKey(path=(), leaf="spend"),
                agg="last",
                args=(FieldKey(path=(), leaf="signup_at"),),
                kwargs=(("weight", FieldKey(path=("regions",), leaf="pop")),),
                partition_keys=Grain.of({FieldKey(path=(), leaf="tier")}),
            ),
            host_path=HOST,
        )
        assert out.source == FieldKey(path=("customers",), leaf="spend")
        assert out.args == (FieldKey(path=("customers",), leaf="signup_at"),)
        assert out.kwargs == (("weight", FieldKey(path=("customers", "regions"), leaf="pop")),)
        assert out.partition_keys == Grain.of({FieldKey(path=("customers",), leaf="tier")})

    def test_scalar_kwarg_passes_through(self) -> None:
        out = K.prepend_value_key(
            AggregateKey(source=FieldKey(path=(), leaf="spend"), agg="percentile",
                         kwargs=(("p", Decimal("0.5")),)),
            host_path=HOST,
        )
        assert out.kwargs == (("p", Decimal("0.5")),)

    def test_transform_input_partition_time(self) -> None:
        out = K.prepend_value_key(
            TransformKey(
                op="cumsum",
                input=AggregateKey(source=FieldKey(path=(), leaf="spend"), agg="sum"),
                partition_keys=Grain.of({FieldKey(path=(), leaf="tier")}),
                time_key=TimeTruncKey(
                    source=FieldKey(path=(), leaf="signup_at"), granularity=TimeGranularity.MONTH,
                ),
            ),
            host_path=HOST,
        )
        assert isinstance(out.input, AggregateKey)
        assert isinstance(out.time_key, TimeTruncKey)
        assert out.input.source == FieldKey(path=("customers",), leaf="spend")
        assert out.partition_keys == Grain.of({FieldKey(path=("customers",), leaf="tier")})
        assert out.time_key.source == FieldKey(path=("customers",), leaf="signup_at")

    def test_arithmetic_scalarcall_time_point_in(self) -> None:
        out = K.prepend_value_key(
            ArithmeticKey(op="/", operands=(
                ScalarCallKey(name="coalesce", args=(
                    InKey(field=FieldKey(path=(), leaf="tier"),
                          values=(LiteralKey(value="gold"),)),
                    TimePointCmpKey(op=">=", operand=FieldKey(path=(), leaf="signup_at"), point="2025-Q1"),
                )),
                FieldKey(path=("regions",), leaf="pop"),
            )),
            host_path=HOST,
        )
        call, col = out.operands
        assert col == FieldKey(path=("customers", "regions"), leaf="pop")
        assert isinstance(call, ScalarCallKey)
        in_key, time_point = call.args
        assert isinstance(in_key, InKey)
        assert isinstance(time_point, TimePointCmpKey)
        assert in_key.field == FieldKey(path=("customers",), leaf="tier")
        assert time_point.operand == FieldKey(path=("customers",), leaf="signup_at")


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
        key = AggregateKey(source=FieldKey(path=(), leaf="spend"), agg="sum")
        assert K.prepend_value_key(key, host_path=()) == key

    def test_none_vs_empty_partition_keys_preserved(self) -> None:
        absent = AggregateKey(source=FieldKey(path=(), leaf="spend"), agg="sum",
                              partition_keys=None)
        grand = AggregateKey(source=FieldKey(path=(), leaf="spend"), agg="sum",
                             partition_keys=Grain.EMPTY)
        assert K.prepend_value_key(absent, host_path=HOST).partition_keys is None
        assert K.prepend_value_key(grand, host_path=HOST).partition_keys == Grain.EMPTY


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
            source=FieldKey(path=("regions",), leaf="pop"), agg="sum",
            partition_keys=Grain.of({FieldKey(path=(), leaf="tier")}),
        )
        prepended = K.prepend_value_key(key, host_path=HOST)
        assert reroot_value_key(prepended, target_path=HOST) == key

    def test_prepend_undoes_strip_for_target_prefixed_key(self) -> None:
        key = AggregateKey(source=FieldKey(path=("customers", "regions"), leaf="pop"),
                           agg="sum")
        stripped = reroot_value_key(key, target_path=HOST)
        assert K.prepend_value_key(stripped, host_path=HOST) == key
