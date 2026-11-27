"""The total reroot visitor over the ValueKey union.

Rerooting was once done by SERIALIZING typed keys back to expression text and
re-parsing them into a nested ``SlayerQuery``. That was replaced with typed
re-rooting via ``reroot_value_key(key, *, target_path)`` — a visitor that is:

* **total** over the union — every member has an explicit case, so a key kind
  added later cannot silently ride through unrerooted; and
* **fail-closed** — an unhandled kind RAISES rather than being returned as-is,
  because "returned unchanged" is indistinguishable from "correctly identity"
  and is exactly how a mis-anchored ref reaches the SQL generator.

The reroot rule is prefix-strip-with-residual, identical to the one
``reroot_aggregate_key`` already applies to ``AggregateKey``: a ``path``
starting with ``target_path`` drops that prefix and keeps the residual hops;
any other ``path``, and any scalar, is returned unchanged.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.enums import DataType, TimeGranularity
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
    SqlFragmentKey,
    WindowKey,
    prepend_value_key,
)
from slayer.core.keys import Grain

# The visitor under construction. Imported at module scope (not inside each
# test) so the whole module reports ONE clear collection error while it does
# not exist yet, rather than N identical failures.
from slayer.core.keys import reroot_value_key  # noqa: E402
from slayer.core.keys import ValueKey
from slayer.core.keys import reroot_aggregate_key
from typing import get_args

TARGET = ("customers",)
DEEP_TARGET = ("customers", "regions")


# ---------------------------------------------------------------------------
# Group 1 — leaf kinds
# ---------------------------------------------------------------------------
class TestLeafKinds:
    """The three path-bearing leaves plus the two path-free ones."""

    def test_column_key_exact_match_becomes_local(self) -> None:
        out = reroot_value_key(
            FieldKey(path=("customers",), leaf="tier"), target_path=TARGET,
        )
        assert out == FieldKey(path=(), leaf="tier")

    def test_column_key_deeper_hop_keeps_residual(self) -> None:
        out = reroot_value_key(
            FieldKey(path=("customers", "regions"), leaf="name"), target_path=TARGET,
        )
        assert out == FieldKey(path=("regions",), leaf="name")

    def test_column_key_off_path_is_unchanged(self) -> None:
        """A path that does not START with target_path is left alone — the
        function never invents an anchoring it cannot justify."""
        key = FieldKey(path=("suppliers",), leaf="name")
        assert reroot_value_key(key, target_path=TARGET) == key

    def test_column_key_local_is_unchanged(self) -> None:
        key = FieldKey(path=(), leaf="amount")
        assert reroot_value_key(key, target_path=TARGET) == key

    def test_column_sql_key_strips_prefix(self) -> None:
        out = reroot_value_key(
            FieldSqlKey(path=("customers",), dataset="customers", field_name="cr"),
            target_path=TARGET,
        )
        assert out == FieldSqlKey(path=(), dataset="customers", field_name="cr")

    def test_star_key_strips_prefix(self) -> None:
        out = reroot_value_key(StarKey(path=("customers",)), target_path=TARGET)
        assert out == StarKey(path=())

    def test_literal_key_is_identity(self) -> None:
        key = LiteralKey(value=Decimal("1"))
        assert reroot_value_key(key, target_path=TARGET) == key

    def test_time_trunc_key_reroots_its_column(self) -> None:
        """``TimeTruncKey`` carries its path on the WRAPPED field, which is
        why ``walk_value_keys`` needs a special case for it — the visitor must
        not inherit that blind spot."""
        out = reroot_value_key(
            TimeTruncKey(
                source=FieldKey(path=("customers",), leaf="signup_at"),
                granularity=TimeGranularity.MONTH,
            ),
            target_path=TARGET,
        )
        assert out == TimeTruncKey(
            source=FieldKey(path=(), leaf="signup_at"), granularity=TimeGranularity.MONTH,
        )

    def test_time_trunc_key_over_derived_column(self) -> None:
        out = reroot_value_key(
            TimeTruncKey(
                source=FieldSqlKey(
                    path=("customers",), dataset="customers", field_name="signup_d",
                ),
                granularity=TimeGranularity.DAY,
            ),
            target_path=TARGET,
        )
        assert out.source == FieldSqlKey(
            path=(), dataset="customers", field_name="signup_d",
        )


# ---------------------------------------------------------------------------
# Group 2 — composite kinds
# ---------------------------------------------------------------------------
class TestCompositeKinds:
    def test_aggregate_source_args_and_kwargs(self) -> None:
        out = reroot_value_key(
            AggregateKey(
                source=FieldKey(path=("customers",), leaf="spend"),
                agg="last",
                args=(FieldKey(path=("customers",), leaf="signup_at"),),
                kwargs=(("weight", FieldKey(path=("customers", "regions"), leaf="pop")),),
            ),
            target_path=TARGET,
        )
        assert out.source == FieldKey(path=(), leaf="spend")
        assert out.args == (FieldKey(path=(), leaf="signup_at"),)
        assert out.kwargs == (("weight", FieldKey(path=("regions",), leaf="pop")),)

    def test_aggregate_scalar_kwarg_passes_through(self) -> None:
        out = reroot_value_key(
            AggregateKey(
                source=FieldKey(path=("customers",), leaf="spend"),
                agg="percentile",
                kwargs=(("p", Decimal("0.5")),),
            ),
            target_path=TARGET,
        )
        assert out.kwargs == (("p", Decimal("0.5")),)

    def test_transform_input_partition_and_time_keys(self) -> None:
        out = reroot_value_key(
            TransformKey(
                op="cumsum",
                input=AggregateKey(
                    source=FieldKey(path=("customers",), leaf="spend"), agg="sum",
                ),
                partition_keys=Grain.of({FieldKey(path=("customers",), leaf="tier")}),
                time_key=TimeTruncKey(
                    source=FieldKey(path=("customers",), leaf="signup_at"),
                    granularity=TimeGranularity.MONTH,
                ),
            ),
            target_path=TARGET,
        )
        assert isinstance(out.input, AggregateKey)
        assert isinstance(out.time_key, TimeTruncKey)
        assert out.input.source == FieldKey(path=(), leaf="spend")
        assert out.partition_keys == Grain.of({FieldKey(path=(), leaf="tier")})
        assert out.time_key.source == FieldKey(path=(), leaf="signup_at")

    def test_transform_scalar_args_are_type_prohibited_from_holding_keys(self) -> None:
        """``TransformKey.args``/``kwargs`` are ``Tuple[Scalar, ...]`` where
        ``Scalar = Union[Decimal, str, bool, None]`` — no ValueKey union member
        can hide there. Pinned so that widening the annotation later trips this
        test and forces the visitor to grow the matching traversal."""

        def _atomic_types(anno) -> set:
            args = get_args(anno)
            if not args:
                return {anno}
            return {t for a in args if a is not Ellipsis for t in _atomic_types(a)}

        members = set(get_args(ValueKey))
        for field in ("args", "kwargs"):
            anno = TransformKey.model_fields[field].annotation
            leaked = members & _atomic_types(anno)
            assert not leaked, (
                f"TransformKey.{field} now admits "
                f"{sorted(m.__name__ for m in leaked)}; reroot_value_key must "
                f"traverse it (§5.4 totality)."
            )

    def test_arithmetic_operands(self) -> None:
        out = reroot_value_key(
            ArithmeticKey(
                op="+",
                operands=(
                    FieldKey(path=("customers",), leaf="spend"),
                    LiteralKey(value=Decimal("1")),
                ),
            ),
            target_path=TARGET,
        )
        assert out.operands[0] == FieldKey(path=(), leaf="spend")
        assert out.operands[1] == LiteralKey(value=Decimal("1"))

    def test_scalar_call_args(self) -> None:
        out = reroot_value_key(
            ScalarCallKey(
                name="coalesce",
                args=(FieldKey(path=("customers",), leaf="tier"), "unknown"),
            ),
            target_path=TARGET,
        )
        assert out.args[0] == FieldKey(path=(), leaf="tier")
        assert out.args[1] == "unknown"

    def test_time_point_members(self) -> None:
        out = reroot_value_key(
            TimePointCmpKey(
                op="<=", operand=FieldKey(path=("customers",), leaf="signup_at"), point="2024-12-31",
            ),
            target_path=TARGET,
        )
        assert out.operand == FieldKey(path=(), leaf="signup_at")
        assert out.point == "2024-12-31"

    def test_in_members(self) -> None:
        out = reroot_value_key(
            InKey(
                field=FieldKey(path=("customers",), leaf="tier"),
                values=(LiteralKey(value="gold"),),
            ),
            target_path=TARGET,
        )
        assert out.field == FieldKey(path=(), leaf="tier")
        assert out.values == (LiteralKey(value="gold"),)
        assert out.negated is False

    def test_deeply_nested_mixed_composition(self) -> None:
        """One tree that exercises every traversal edge at once — per-member
        tests can each pass while a combination still drops a branch."""
        key = ArithmeticKey(
            op="/",
            operands=(
                TransformKey(
                    op="cumsum",
                    input=AggregateKey(
                        source=FieldKey(path=("customers", "regions"), leaf="pop"),
                        agg="sum",
                    ),
                    partition_keys=Grain.of({
                        FieldKey(path=("customers",), leaf="tier"),
                    }),
                ),
                ScalarCallKey(
                    name="coalesce",
                    args=(
                        InKey(
                            field=FieldKey(path=("customers",), leaf="tier"),
                            values=(LiteralKey(value="gold"),),
                        ),
                        TimePointCmpKey(
                            op=">=",
                            operand=TimeTruncKey(
                                source=FieldKey(
                                    path=("customers",), leaf="signup_at",
                                ),
                                granularity=TimeGranularity.DAY,
                            ),
                            point="2025-Q1",
                        ),
                    ),
                ),
            ),
        )
        out = reroot_value_key(key, target_path=TARGET)
        transform, call = out.operands
        assert isinstance(transform, TransformKey)
        assert isinstance(transform.input, AggregateKey)
        assert isinstance(call, ScalarCallKey)
        assert transform.input.source == FieldKey(path=("regions",), leaf="pop")
        assert transform.partition_keys == Grain.of({FieldKey(path=(), leaf="tier")})
        in_key, time_point = call.args
        assert isinstance(in_key, InKey)
        assert isinstance(time_point, TimePointCmpKey)
        assert isinstance(time_point.operand, TimeTruncKey)
        assert in_key.field == FieldKey(path=(), leaf="tier")
        assert time_point.operand.source == FieldKey(path=(), leaf="signup_at")


# ---------------------------------------------------------------------------
# Group 3 — totality and fail-closed
# ---------------------------------------------------------------------------
class TestTotalityAndFailClosed:
    def test_every_union_member_is_handled(self) -> None:
        """Enumerate the union and assert the visitor accepts each member.

        Reading the union rather than hard-coding the list means a NEW key kind
        fails this test the day it is added, which is the whole point of a
        total visitor.
        """


        samples = {
            FieldKey: FieldKey(path=("customers",), leaf="tier"),
            FieldSqlKey: FieldSqlKey(
                path=("customers",), dataset="customers", field_name="cr",
            ),
            TimeTruncKey: TimeTruncKey(
                source=FieldKey(path=("customers",), leaf="signup_at"),
                granularity=TimeGranularity.DAY,
            ),
            StarKey: StarKey(path=("customers",)),
            LiteralKey: LiteralKey(value="x"),
            AggregateKey: AggregateKey(
                source=FieldKey(path=("customers",), leaf="spend"), agg="sum",
            ),
            TransformKey: TransformKey(
                op="cumsum",
                input=AggregateKey(
                    source=FieldKey(path=("customers",), leaf="spend"), agg="sum",
                ),
            ),
            ArithmeticKey: ArithmeticKey(
                op="+",
                operands=(
                    FieldKey(path=("customers",), leaf="spend"),
                    LiteralKey(value=Decimal("1")),
                ),
            ),
            ScalarCallKey: ScalarCallKey(
                name="coalesce", args=(FieldKey(path=("customers",), leaf="tier"),),
            ),
            TimePointCmpKey: TimePointCmpKey(
                op=">=", operand=FieldKey(path=("customers",), leaf="signup_at"), point="2025-Q1",
            ),
            InKey: InKey(
                field=FieldKey(path=("customers",), leaf="tier"),
                values=(LiteralKey(value="gold"),),
            ),
            SqlFragmentKey: SqlFragmentKey(
                template="{r0} * 2", refs=(FieldKey(path=("customers",), leaf="spend"),),
            ),
            CastKey: CastKey(
                source=FieldKey(path=("customers",), leaf="spend"), type_text="INT", data_type=DataType.INTEGER,
            ),
            ArrayLiteralKey: ArrayLiteralKey(values=(LiteralKey(value="gold"),)),
            ListSetKey: ListSetKey(source=FieldKey(path=("customers",), leaf="tags")),
            WindowKey: WindowKey(
                function="row_number", order=(FieldKey(path=("customers",), leaf="spend"),),
                descending=(False,), nulls_first=(False,), anchor=("customers",),
            ),
        }
        members = set(get_args(ValueKey))
        missing = members - set(samples)
        assert not missing, (
            f"ValueKey grew {sorted(m.__name__ for m in missing)}; add a sample "
            f"here and a case to reroot_value_key (§5.4 totality)."
        )
        for member in members:
            reroot_value_key(samples[member], target_path=TARGET)

    def test_a_row_level_window_anchor_moves_with_its_references(self) -> None:
        spend = FieldKey(path=("customers",), leaf="spend")
        window = WindowKey(function="row_number", order=(spend,), descending=(False,), nulls_first=(False,),
                           anchor=("customers",))
        local = reroot_value_key(window, target_path=TARGET)
        assert local.anchor == ()
        assert prepend_value_key(local, host_path=TARGET) == window

    def test_unknown_kind_raises(self) -> None:
        """Fail closed. Returning an unhandled kind unchanged is what lets a
        mis-anchored ref reach the generator looking correct."""

        class NotAKey:
            path = ("customers",)

        not_a_key = NotAKey()
        with pytest.raises(TypeError):
            reroot_value_key(not_a_key, target_path=TARGET)

    def test_empty_target_path_is_identity(self) -> None:
        """``target_path == ()`` is the filtered-local case — the empty prefix
        strips zero hops, so every key comes back equal."""
        key = AggregateKey(
            source=FieldKey(path=("customers",), leaf="spend"), agg="sum",
        )
        assert reroot_value_key(key, target_path=()) == key

    def test_reroot_strips_one_prefix_per_application(self) -> None:
        """Each call strips exactly ONE matching prefix.

        Rerooting is therefore NOT idempotent for a path that repeats the
        target hop, so the planner must dispatch it exactly once. Pinned so a
        double-dispatch shows up as a corrupted ref rather than a silent no-op.

        (The name and docstring used to claim the opposite — "must not strip a
        second time" — while the assertions required the strip. A reader would
        have taken the stated invariant as a real contract; CodeRabbit.)"""
        once = reroot_value_key(
            FieldKey(path=("customers", "customers"), leaf="x"), target_path=TARGET,
        )
        assert once == FieldKey(path=("customers",), leaf="x")
        twice = reroot_value_key(once, target_path=TARGET)
        assert twice == FieldKey(path=(), leaf="x")


# ---------------------------------------------------------------------------
# Group 5 — the public-identity invariant §5.4 names explicitly
# ---------------------------------------------------------------------------
class TestPublicResultKeysUnchanged:
    def test_reroot_aggregate_key_delegates_to_the_visitor(self) -> None:
        """``reroot_aggregate_key`` stays (P-J state 1) but must become a thin
        wrapper, so the two cannot drift into two reroot semantics — which is
        precisely the drift §5.4 exists to end."""

        key = AggregateKey(
            source=FieldKey(path=("customers",), leaf="spend"),
            agg="last",
            args=(FieldKey(path=("customers", "regions"), leaf="opened_at"),),
        )
        assert reroot_aggregate_key(key, target_path=TARGET) == reroot_value_key(
            key, target_path=TARGET,
        )

    def test_agg_and_locus_fields_ride_through(self) -> None:
        """Fields the visitor does not own must survive — a rebuild that
        enumerated only the rerootable fields would silently drop them.

        ``locus`` is set NON-default ("host"): left at its "target" default a
        dropped-field rebuild would re-default to the same value, so the
        assertion could not fail."""
        key = AggregateKey(
            source=FieldKey(path=("customers",), leaf="spend"),
            agg="approx_count_distinct",
            locus="host",
        )
        out = reroot_value_key(key, target_path=TARGET)
        assert out.agg == "approx_count_distinct"
        assert out.locus == "host"

    def test_kwargs_stay_canonically_sorted_after_reroot(self) -> None:
        key = AggregateKey(
            source=FieldKey(path=("customers",), leaf="spend"),
            agg="corr",
            kwargs=(
                ("other", FieldKey(path=("customers",), leaf="x")),
                ("alpha", Decimal("1")),
            ),
        )
        out = reroot_value_key(key, target_path=TARGET)
        assert [k for k, _ in out.kwargs] == sorted(k for k, _ in out.kwargs)
