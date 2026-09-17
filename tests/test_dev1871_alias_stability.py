"""DEV-1871 group 2 — the pinned legacy key spelling behind SQL aliases.

Pins the exact historical tokens (``grain_target``, ``partition_keys_frozenset_…``)
so the locus rename and Grain retype cannot move emitted SQL: bypassing the
serializer (falling back to Pydantic str/repr) breaks these pins the moment a
field is renamed. Producer interning stays repr-based and is pinned separately.
"""

from __future__ import annotations

from decimal import Decimal

from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    Grain,
    LiteralKey,
    ScalarCallKey,
    TransformKey,
)
from slayer.core.refs import (
    _value_key_display,
    expression_source_leaf,
    legacy_key_repr,
    legacy_key_str,
    partition_by_suffix,
)
from slayer.ir.planned import PlannedQuery, RegroupAttachPlan, regroup_producer_identity
from slayer.sql.naming import canonical_aggregate_alias

_AMOUNT = ColumnKey(leaf="amount")
_CITY = ColumnKey(leaf="city")


def _iif_partition_key() -> ScalarCallKey:
    inner = AggregateKey(
        source=_AMOUNT, agg="sum", partition_keys=Grain.of([_CITY]),
    )
    return ScalarCallKey(
        name="iif",
        args=(
            ArithmeticKey(
                op=">=", operands=(inner, LiteralKey(value=Decimal("35"))),
            ),
            LiteralKey(value=Decimal("1")),
            LiteralKey(value=Decimal("0")),
        ),
    )


_IIF_SUFFIX = (
    "_partition_by_name_iif_args_ArithmeticKey_op_operands_AggregateKey"
    "_source_ColumnKey_path_leaf_amount_agg_sum_args_kwargs_grain_target"
    "_partition_keys_frozenset_ColumnKey_path_leaf"
    "_city_LiteralKey_value_Decimal_35_LiteralKey_value_Decimal_1"
    "_LiteralKey_value_Decimal_0"
)


class TestPinnedSpellings:
    def test_partition_by_suffix_over_a_scalar_call_key(self) -> None:
        assert partition_by_suffix(frozenset({_iif_partition_key()})) == _IIF_SUFFIX

    def test_stage_formula_alias_carries_the_pinned_suffix(self) -> None:
        key = AggregateKey(
            source=_AMOUNT, agg="sum",
            partition_keys=Grain.of({_iif_partition_key()}),
        )
        assert (
            canonical_aggregate_alias(key, profile="stage_formula")
            == "amount_sum" + _IIF_SUFFIX
        )

    def test_aggregate_key_legacy_str(self) -> None:
        key = AggregateKey(
            source=_AMOUNT, agg="sum", partition_keys=Grain.of([_CITY]),
        )
        assert legacy_key_str(key) == (
            "source=ColumnKey(path=(), leaf='amount') agg='sum' args=() "
            "kwargs=() grain='target' "
            "partition_keys=frozenset({ColumnKey(path=(), leaf='city')})"
        )

    def test_transform_key_legacy_repr(self) -> None:
        key = TransformKey(op="rank", input=_AMOUNT)
        assert legacy_key_repr(key) == (
            "TransformKey(op='rank', input=ColumnKey(path=(), leaf='amount'), "
            "args=(), kwargs=(), partition_keys=frozenset(), time_key=None)"
        )

    def test_value_key_display_fallback_uses_the_serializer(self) -> None:
        key = TransformKey(op="rank", input=_AMOUNT)
        assert _value_key_display(key) == legacy_key_str(key)

    def test_expression_source_leaf_over_an_aggregate_operand(self) -> None:
        source = ArithmeticKey(
            op="-",
            operands=(
                AggregateKey(source=_AMOUNT, agg="sum"),
                LiteralKey(value=Decimal("1")),
            ),
        )
        assert expression_source_leaf(source) == (
            "source_columnkey_path_leaf_a_edd6d14d_s_none_1"
        )

    def test_expression_source_leaf_over_a_raw_scalar_arg(self) -> None:
        """Desugared change_pct keeps a raw Decimal in nullif's args."""
        source = ScalarCallKey(name="nullif", args=(_AMOUNT, Decimal("0")))
        assert expression_source_leaf(source) == "nullif_amount_0"


class TestProducerInterningStability:
    def _attach(self, *, alias_hint: str, relation: str = "r") -> RegroupAttachPlan:
        return RegroupAttachPlan(
            producer_plan=PlannedQuery(source_relation=relation),
            alias_hint=alias_hint,
            producer_root_model="orders",
        )

    def test_equal_producers_intern_to_one_identity(self) -> None:
        a = regroup_producer_identity(self._attach(alias_hint="x"))
        b = regroup_producer_identity(self._attach(alias_hint="y"))
        assert a == b
        assert hash(a) == hash(b)

    def test_differing_producer_bodies_stay_distinct(self) -> None:
        a = regroup_producer_identity(self._attach(alias_hint="x"))
        b = regroup_producer_identity(self._attach(alias_hint="x", relation="s"))
        assert a != b
