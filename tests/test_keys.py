"""Stage 1 (DEV-1450) — typed identity primitives for the new resolution
pipeline.

These keys identify slots structurally (P2). Two expression occurrences with
the same key intern to the same slot. The keys carry only identity — no
rendering state, no display alias, no projection position.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.keys import (
    SCALAR_FUNCTIONS,
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    ColumnSqlKey,
    Phase,
    ScalarCallKey,
    SqlExprKey,
    StarKey,
    TransformKey,
    ValueKey,
    normalize_scalar,
)


# ---------------------------------------------------------------------------
# ColumnKey
# ---------------------------------------------------------------------------


class TestColumnKey:
    def test_local_ref_has_empty_path(self):
        k = ColumnKey(path=(), leaf="revenue")
        assert k.path == ()
        assert k.leaf == "revenue"
        assert k.phase is Phase.ROW

    def test_joined_single_hop(self):
        k = ColumnKey(path=("customers",), leaf="name")
        assert k.path == ("customers",)
        assert k.phase is Phase.ROW

    def test_joined_multi_hop(self):
        k = ColumnKey(path=("customers", "regions"), leaf="name")
        assert k.path == ("customers", "regions")
        assert k.leaf == "name"

    def test_value_equality_and_hash(self):
        a = ColumnKey(path=("customers",), leaf="name")
        b = ColumnKey(path=("customers",), leaf="name")
        assert a == b
        assert hash(a) == hash(b)
        assert {a: 1}[b] == 1

    def test_path_distinguishes(self):
        assert ColumnKey(path=(), leaf="a") != ColumnKey(path=("x",), leaf="a")

    def test_leaf_distinguishes(self):
        assert ColumnKey(path=(), leaf="a") != ColumnKey(path=(), leaf="b")

    def test_frozen_no_mutation(self):
        k = ColumnKey(path=(), leaf="a")
        with pytest.raises((TypeError, ValueError)):
            k.leaf = "b"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# ColumnSqlKey
# ---------------------------------------------------------------------------


class TestColumnSqlKey:
    def test_basic(self):
        k = ColumnSqlKey(model="orders", column_name="rev_after_tax")
        assert k.model == "orders"
        assert k.column_name == "rev_after_tax"
        assert k.phase is Phase.ROW

    def test_equality(self):
        a = ColumnSqlKey(model="orders", column_name="x")
        b = ColumnSqlKey(model="orders", column_name="x")
        assert a == b
        assert hash(a) == hash(b)

    def test_different_models_distinguish(self):
        a = ColumnSqlKey(model="orders", column_name="x")
        b = ColumnSqlKey(model="customers", column_name="x")
        assert a != b


# ---------------------------------------------------------------------------
# StarKey
# ---------------------------------------------------------------------------


class TestStarKey:
    def test_all_instances_equal(self):
        assert StarKey() == StarKey()
        assert hash(StarKey()) == hash(StarKey())

    def test_phase_is_row(self):
        assert StarKey().phase is Phase.ROW

    def test_usable_as_dict_key(self):
        d = {StarKey(): "count"}
        assert d[StarKey()] == "count"


# ---------------------------------------------------------------------------
# SqlExprKey
# ---------------------------------------------------------------------------


class TestSqlExprKey:
    def test_basic(self):
        k = SqlExprKey(canonical_sql="status = 'paid'")
        assert k.canonical_sql == "status = 'paid'"
        assert k.phase is Phase.ROW

    def test_equality(self):
        a = SqlExprKey(canonical_sql="x = 1")
        b = SqlExprKey(canonical_sql="x = 1")
        assert a == b
        assert hash(a) == hash(b)

    def test_different_sql_differs(self):
        a = SqlExprKey(canonical_sql="x = 1")
        b = SqlExprKey(canonical_sql="x = 2")
        assert a != b


# ---------------------------------------------------------------------------
# AggregateKey
# ---------------------------------------------------------------------------


class TestAggregateKey:
    def test_simple_local_agg(self):
        k = AggregateKey(source=ColumnKey(path=(), leaf="revenue"), agg="sum")
        assert k.agg == "sum"
        assert k.args == ()
        assert k.kwargs == ()
        assert k.column_filter_key is None
        assert k.phase is Phase.AGGREGATE

    def test_star_count(self):
        k = AggregateKey(source=StarKey(), agg="count")
        assert isinstance(k.source, StarKey)

    def test_cross_model_has_non_empty_path(self):
        k = AggregateKey(
            source=ColumnKey(path=("customers",), leaf="revenue"),
            agg="sum",
        )
        assert k.source.path == ("customers",)
        # Same class as local — only `source.path` differs (P3).
        local = AggregateKey(
            source=ColumnKey(path=(), leaf="revenue"), agg="sum"
        )
        assert type(k) is type(local)

    def test_kwargs_canonicalized_to_sorted(self):
        k = AggregateKey(
            source=ColumnKey(path=(), leaf="x"),
            agg="weighted_avg",
            kwargs=(("z", Decimal("1")), ("a", Decimal("2"))),
        )
        assert k.kwargs == (("a", Decimal("2")), ("z", Decimal("1")))

    def test_kwargs_reorder_inputs_intern(self):
        a = AggregateKey(
            source=ColumnKey(path=(), leaf="x"),
            agg="weighted_avg",
            kwargs=(("z", Decimal("1")), ("a", Decimal("2"))),
        )
        b = AggregateKey(
            source=ColumnKey(path=(), leaf="x"),
            agg="weighted_avg",
            kwargs=(("a", Decimal("2")), ("z", Decimal("1"))),
        )
        assert a == b
        assert hash(a) == hash(b)

    def test_decimal_precision_irrelevant_to_identity(self):
        # P2 / kwarg-scalar normalization: 0.5 and 0.50 intern.
        a = AggregateKey(
            source=ColumnKey(path=(), leaf="x"),
            agg="percentile",
            kwargs=(("p", Decimal("0.5")),),
        )
        b = AggregateKey(
            source=ColumnKey(path=(), leaf="x"),
            agg="percentile",
            kwargs=(("p", Decimal("0.50")),),
        )
        assert a == b
        assert hash(a) == hash(b)

    def test_identifier_kwargs_differ_by_referenced_column(self):
        # weighted_avg(weight=quantity) vs weighted_avg(weight=quantity_v2)
        a = AggregateKey(
            source=ColumnKey(path=(), leaf="price"),
            agg="weighted_avg",
            kwargs=(("weight", ColumnKey(path=(), leaf="quantity")),),
        )
        b = AggregateKey(
            source=ColumnKey(path=(), leaf="price"),
            agg="weighted_avg",
            kwargs=(("weight", ColumnKey(path=(), leaf="quantity_v2")),),
        )
        assert a != b
        assert hash(a) != hash(b)

    def test_column_filter_key_distinguishes(self):
        base = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"), agg="sum"
        )
        with_filter = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="sum",
            column_filter_key=SqlExprKey(canonical_sql="paid = TRUE"),
        )
        assert base != with_filter

    def test_same_column_filter_interns(self):
        a = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="sum",
            column_filter_key=SqlExprKey(canonical_sql="paid = TRUE"),
        )
        b = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="sum",
            column_filter_key=SqlExprKey(canonical_sql="paid = TRUE"),
        )
        assert a == b
        assert hash(a) == hash(b)


# ---------------------------------------------------------------------------
# TransformKey
# ---------------------------------------------------------------------------


class TestTransformKey:
    def test_basic_transform(self):
        agg = AggregateKey(source=ColumnKey(path=(), leaf="rev"), agg="sum")
        k = TransformKey(op="cumsum", input=agg)
        assert k.op == "cumsum"
        assert k.input == agg
        assert k.phase is Phase.POST

    def test_partition_keys_order_irrelevant(self):
        agg = AggregateKey(source=ColumnKey(path=(), leaf="rev"), agg="sum")
        a = TransformKey(
            op="cumsum",
            input=agg,
            partition_keys=frozenset({
                ColumnKey(path=(), leaf="region"),
                ColumnKey(path=(), leaf="store"),
            }),
        )
        b = TransformKey(
            op="cumsum",
            input=agg,
            partition_keys=frozenset({
                ColumnKey(path=(), leaf="store"),
                ColumnKey(path=(), leaf="region"),
            }),
        )
        assert a == b
        assert hash(a) == hash(b)

    def test_time_key_distinguishes(self):
        agg = AggregateKey(source=ColumnKey(path=(), leaf="rev"), agg="sum")
        a = TransformKey(op="time_shift", input=agg)
        b = TransformKey(
            op="time_shift",
            input=agg,
            time_key=ColumnKey(path=(), leaf="ts"),
        )
        assert a != b

    def test_nested_transform_input(self):
        agg = AggregateKey(source=ColumnKey(path=(), leaf="rev"), agg="sum")
        inner = TransformKey(op="cumsum", input=agg)
        outer = TransformKey(op="rank", input=inner)
        assert outer.input == inner
        assert outer.phase is Phase.POST


# ---------------------------------------------------------------------------
# ArithmeticKey
# ---------------------------------------------------------------------------


class TestArithmeticKey:
    def test_basic(self):
        a = ColumnKey(path=(), leaf="x")
        b = ColumnKey(path=(), leaf="y")
        k = ArithmeticKey(op="+", operands=(a, b))
        assert k.op == "+"
        assert k.operands == (a, b)
        assert k.phase is Phase.ROW

    def test_phase_is_max_operand_phase(self):
        col = ColumnKey(path=(), leaf="x")
        agg = AggregateKey(source=ColumnKey(path=(), leaf="y"), agg="sum")
        k = ArithmeticKey(op="+", operands=(col, agg))
        assert k.phase is Phase.AGGREGATE

    def test_phase_with_transform_is_post(self):
        agg = AggregateKey(source=ColumnKey(path=(), leaf="x"), agg="sum")
        tx = TransformKey(op="cumsum", input=agg)
        col = ColumnKey(path=(), leaf="y")
        k = ArithmeticKey(op="-", operands=(col, tx))
        assert k.phase is Phase.POST

    def test_operand_order_matters(self):
        a = ColumnKey(path=(), leaf="a")
        b = ColumnKey(path=(), leaf="b")
        ab = ArithmeticKey(op="-", operands=(a, b))
        ba = ArithmeticKey(op="-", operands=(b, a))
        assert ab != ba


# ---------------------------------------------------------------------------
# ScalarCallKey
# ---------------------------------------------------------------------------


class TestScalarCallKey:
    def test_basic(self):
        col = ColumnKey(path=(), leaf="x")
        k = ScalarCallKey(name="coalesce", args=(col, Decimal("0")))
        assert k.name == "coalesce"

    def test_phase_pure_row(self):
        col = ColumnKey(path=(), leaf="x")
        k = ScalarCallKey(name="coalesce", args=(col, Decimal("0")))
        assert k.phase is Phase.ROW

    def test_phase_with_agg_is_aggregate(self):
        agg = AggregateKey(source=ColumnKey(path=(), leaf="x"), agg="sum")
        k = ScalarCallKey(name="nullif", args=(agg, Decimal("0")))
        assert k.phase is Phase.AGGREGATE

    def test_phase_all_scalars_is_row(self):
        k = ScalarCallKey(name="concat", args=(Decimal("1"), Decimal("2")))
        assert k.phase is Phase.ROW

    def test_args_order_matters(self):
        col = ColumnKey(path=(), leaf="x")
        a = ScalarCallKey(name="nullif", args=(col, Decimal("0")))
        b = ScalarCallKey(name="nullif", args=(Decimal("0"), col))
        assert a != b


# ---------------------------------------------------------------------------
# SCALAR_FUNCTIONS allowlist (C12)
# ---------------------------------------------------------------------------


class TestScalarFunctionsAllowlist:
    def test_contains_null_handling(self):
        assert {"nullif", "coalesce", "ifnull"} <= SCALAR_FUNCTIONS

    def test_contains_math(self):
        assert {
            "ln", "log10", "log2", "log",
            "exp", "sqrt", "pow", "power",
            "abs", "floor", "ceil", "round",
        } <= SCALAR_FUNCTIONS

    def test_contains_string_hygiene(self):
        assert {
            "lower", "upper", "trim", "replace",
            "substr", "instr", "length", "concat",
        } <= SCALAR_FUNCTIONS

    def test_excludes_unknown(self):
        assert "regexp_match" not in SCALAR_FUNCTIONS
        assert "date_part" not in SCALAR_FUNCTIONS
        assert "json_extract" not in SCALAR_FUNCTIONS

    def test_is_frozen(self):
        assert isinstance(SCALAR_FUNCTIONS, frozenset)


# ---------------------------------------------------------------------------
# normalize_scalar
# ---------------------------------------------------------------------------


class TestNormalizeScalar:
    def test_int_to_decimal(self):
        result = normalize_scalar(3)
        assert isinstance(result, Decimal)
        assert result == Decimal("3")

    def test_float_via_str_avoids_binary_imprecision(self):
        # Critical: float→str→Decimal so 0.5 → Decimal("0.5") not the
        # binary-approximation form Decimal(0.5).
        assert normalize_scalar(0.5) == Decimal("0.5")
        assert normalize_scalar(0.50) == Decimal("0.5")

    def test_decimal_passthrough(self):
        d = Decimal("0.5")
        assert normalize_scalar(d) is d

    def test_str_passthrough(self):
        assert normalize_scalar("paid") == "paid"

    def test_true_and_false_passthrough(self):
        assert normalize_scalar(True) is True
        assert normalize_scalar(False) is False

    def test_none_passthrough(self):
        assert normalize_scalar(None) is None

    def test_bool_not_converted_to_decimal(self):
        # bool is-a int in Python; the normalizer must check bool BEFORE
        # the int branch, otherwise True → Decimal(1).
        result = normalize_scalar(True)
        assert result is True
        assert not isinstance(result, Decimal)

    def test_list_rejected(self):
        with pytest.raises(TypeError):
            normalize_scalar([1, 2, 3])  # type: ignore[arg-type]

    def test_dict_rejected(self):
        with pytest.raises(TypeError):
            normalize_scalar({"a": 1})  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Identity interning (P2 / P3)
# ---------------------------------------------------------------------------


class TestIdentityInterning:
    def test_three_occurrences_share_one_slot(self):
        # P2: revenue:sum as a declared measure, as the inner ref of
        # change(revenue:sum), and as a filter occurrence all bind to one
        # slot via structural identity.
        rev_sum_a = AggregateKey(
            source=ColumnKey(path=(), leaf="revenue"), agg="sum"
        )
        rev_sum_b = AggregateKey(
            source=ColumnKey(path=(), leaf="revenue"), agg="sum"
        )
        rev_sum_c = AggregateKey(
            source=ColumnKey(path=(), leaf="revenue"), agg="sum"
        )

        registry: dict = {rev_sum_a: "slot_1"}
        assert rev_sum_b in registry
        assert rev_sum_c in registry
        assert registry[rev_sum_b] == "slot_1"
        assert registry[rev_sum_c] == "slot_1"

    def test_local_and_cross_model_share_class(self):
        # P3: local and cross-model aggregates have the same shape; only
        # source.path differs.
        local = AggregateKey(
            source=ColumnKey(path=(), leaf="revenue"), agg="sum"
        )
        cross = AggregateKey(
            source=ColumnKey(path=("customers",), leaf="revenue"), agg="sum"
        )
        assert type(local) is type(cross)
        assert local != cross
        assert local.source.path == ()
        assert cross.source.path == ("customers",)

    def test_transform_wrapping_preserves_inner_identity(self):
        # C2 / DEV-1446: change(revenue:sum) wraps the same inner slot.
        inner_a = AggregateKey(
            source=ColumnKey(path=(), leaf="revenue"), agg="sum"
        )
        inner_b = AggregateKey(
            source=ColumnKey(path=(), leaf="revenue"), agg="sum"
        )
        wrap_a = TransformKey(op="time_shift", input=inner_a)
        wrap_b = TransformKey(op="time_shift", input=inner_b)
        # The wrappers differ from the inner slot.
        assert wrap_a != inner_a
        # Two wrappers built from structurally identical inputs intern.
        assert wrap_a == wrap_b
        assert hash(wrap_a) == hash(wrap_b)


# ---------------------------------------------------------------------------
# Phase
# ---------------------------------------------------------------------------


class TestPhase:
    def test_ordering(self):
        assert Phase.ROW < Phase.AGGREGATE < Phase.POST

    def test_max(self):
        assert max(Phase.ROW, Phase.AGGREGATE) is Phase.AGGREGATE
        assert max(Phase.AGGREGATE, Phase.POST) is Phase.POST
        assert max(Phase.ROW, Phase.ROW) is Phase.ROW

    def test_int_values(self):
        assert int(Phase.ROW) == 0
        assert int(Phase.AGGREGATE) == 1
        assert int(Phase.POST) == 2


# ---------------------------------------------------------------------------
# ValueKey union (type-level sanity)
# ---------------------------------------------------------------------------


class TestValueKeyUnion:
    def test_all_variants_are_value_keys(self):
        # Smoke test: each concrete key is usable wherever ValueKey is
        # expected. Pydantic union validation will reject anything else.
        agg = AggregateKey(source=ColumnKey(path=(), leaf="x"), agg="sum")
        tx = TransformKey(op="cumsum", input=agg)
        # ArithmeticKey accepts every ValueKey variant as an operand.
        ArithmeticKey(
            op="+",
            operands=(
                ColumnKey(path=(), leaf="a"),
                ColumnSqlKey(model="m", column_name="c"),
                agg,
                tx,
                ScalarCallKey(name="abs", args=(ColumnKey(path=(), leaf="z"),)),
            ),
        )

    def test_value_key_alias_resolves(self):
        # The Union alias should be importable and usable at runtime.
        assert ValueKey is not None
