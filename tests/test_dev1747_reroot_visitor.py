"""DEV-1747 §5.4 — the total reroot visitor over the ValueKey union.

Rerooting today is done by SERIALIZING typed keys back to formula text
(``_local_agg_formula`` / ``_reroot_ref`` in ``slayer/engine/cross_model_planner.py``)
and re-parsing them into a nested ``SlayerQuery``. §5.4 replaces that with
``reroot_value_key(key, *, target_path)`` — a visitor that is:

* **total** over the union — every member has an explicit case, so a key kind
  added later cannot silently ride through unrerooted; and
* **fail-closed** — an unhandled kind RAISES rather than being returned as-is,
  because "returned unchanged" is indistinguishable from "correctly identity"
  and is exactly how a mis-anchored ref reaches the SQL generator.

The reroot rule is prefix-strip-with-residual, identical to the one
``reroot_aggregate_key`` already applies to ``AggregateKey``: a ``path``
starting with ``target_path`` drops that prefix and keeps the residual hops;
any other ``path``, and any scalar, is returned unchanged.

``AggregateKey.column_filter_key`` is deliberately copied UNCHANGED — see
``TestColumnFilterKeyInvariance`` for why that is not an oversight.

Refs: DEV-1747 (§5.4), DEV-1707 (the symmetric ``reroot_aggregate_key`` this
generalises), DEV-1742 P-E.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

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
)

# The visitor under construction. Imported at module scope (not inside each
# test) so the whole module reports ONE clear collection error while it does
# not exist yet, rather than N identical failures.
from slayer.core.keys import reroot_value_key  # noqa: E402

TARGET = ("customers",)
DEEP_TARGET = ("customers", "regions")


# ---------------------------------------------------------------------------
# Group 1 — leaf kinds
# ---------------------------------------------------------------------------
class TestLeafKinds:
    """The three path-bearing leaves plus the two path-free ones."""

    def test_column_key_exact_match_becomes_local(self) -> None:
        out = reroot_value_key(
            ColumnKey(path=("customers",), leaf="tier"), target_path=TARGET,
        )
        assert out == ColumnKey(path=(), leaf="tier")

    def test_column_key_deeper_hop_keeps_residual(self) -> None:
        out = reroot_value_key(
            ColumnKey(path=("customers", "regions"), leaf="name"), target_path=TARGET,
        )
        assert out == ColumnKey(path=("regions",), leaf="name")

    def test_column_key_off_path_is_unchanged(self) -> None:
        """A path that does not START with target_path is left alone — the
        function never invents an anchoring it cannot justify."""
        key = ColumnKey(path=("suppliers",), leaf="name")
        assert reroot_value_key(key, target_path=TARGET) == key

    def test_column_key_local_is_unchanged(self) -> None:
        key = ColumnKey(path=(), leaf="amount")
        assert reroot_value_key(key, target_path=TARGET) == key

    def test_column_sql_key_strips_prefix(self) -> None:
        out = reroot_value_key(
            ColumnSqlKey(path=("customers",), model="customers", column_name="cr"),
            target_path=TARGET,
        )
        assert out == ColumnSqlKey(path=(), model="customers", column_name="cr")

    def test_star_key_strips_prefix(self) -> None:
        out = reroot_value_key(StarKey(path=("customers",)), target_path=TARGET)
        assert out == StarKey(path=())

    def test_literal_key_is_identity(self) -> None:
        key = LiteralKey(value=Decimal("1"))
        assert reroot_value_key(key, target_path=TARGET) == key

    def test_time_trunc_key_reroots_its_column(self) -> None:
        """``TimeTruncKey`` carries its path on the WRAPPED column, which is
        why ``walk_value_keys`` needs a special case for it — the visitor must
        not inherit that blind spot."""
        out = reroot_value_key(
            TimeTruncKey(
                column=ColumnKey(path=("customers",), leaf="signup_at"),
                granularity="month",
            ),
            target_path=TARGET,
        )
        assert out == TimeTruncKey(
            column=ColumnKey(path=(), leaf="signup_at"), granularity="month",
        )

    def test_time_trunc_key_over_derived_column(self) -> None:
        out = reroot_value_key(
            TimeTruncKey(
                column=ColumnSqlKey(
                    path=("customers",), model="customers", column_name="signup_d",
                ),
                granularity="day",
            ),
            target_path=TARGET,
        )
        assert out.column == ColumnSqlKey(
            path=(), model="customers", column_name="signup_d",
        )

    def test_sql_expr_key_strips_referenced_join_paths(self) -> None:
        """§5.4 lists ``SqlExprKey`` paths explicitly. A standalone fragment
        anchored at the query root must be re-anchored at the target.

        The EXACT-match path (``("customers",)`` under target ``("customers",)``)
        does not survive as ``()``: the field is documented as "non-anchor
        join-path prefixes", and ``()`` is its "same-model filter, no crossing"
        marker. Carrying an empty tuple in the list said the opposite of what
        the strip means (CodeRabbit)."""
        out = reroot_value_key(
            SqlExprKey(
                canonical_sql="customers__regions.name = 'US'",
                referenced_join_paths=(("customers",), ("customers", "regions")),
            ),
            target_path=TARGET,
        )
        assert out.canonical_sql == "customers__regions.name = 'US'"
        assert out.referenced_join_paths == (("regions",),)

    def test_stripping_re_canonicalises_for_identity(self) -> None:
        """``model_copy`` skips validators in Pydantic v2, and the ``before``
        validator is what sorts and de-duplicates the paths — while
        ``__hash__`` / ``__eq__`` read the tuple directly.

        So a strip that leaves two paths sharing a residual, or leaves them out
        of sorted order, produces a key that will not intern against its own
        equal. Both inputs below reroot to the same residual set; the two
        results must be equal AND hash equal, or the registry mints two slots
        for one value."""
        a = reroot_value_key(
            SqlExprKey(
                canonical_sql="x",
                # ``("customers", "regions")`` and a bare ``("regions",)``
                # collapse onto the SAME residual after the strip.
                referenced_join_paths=(("customers", "regions"), ("regions",)),
            ),
            target_path=TARGET,
        )
        assert a.referenced_join_paths == (("regions",),), (
            f"duplicate residuals survived the strip: {a.referenced_join_paths}"
        )

        b = reroot_value_key(
            SqlExprKey(
                canonical_sql="x",
                referenced_join_paths=(("regions",), ("customers", "regions")),
            ),
            target_path=TARGET,
        )
        assert a == b, (
            "two orderings of the same paths rerooted to keys that compare "
            "unequal — identity depends on the canonical form the validator "
            "produces, which model_copy would have skipped"
        )
        assert hash(a) == hash(b), (
            "the keys compare equal but hash differently, so they still land "
            "in different registry buckets and mint two slots for one value"
        )


# ---------------------------------------------------------------------------
# Group 2 — composite kinds
# ---------------------------------------------------------------------------
class TestCompositeKinds:
    def test_aggregate_source_args_and_kwargs(self) -> None:
        out = reroot_value_key(
            AggregateKey(
                source=ColumnKey(path=("customers",), leaf="spend"),
                agg="last",
                args=(ColumnKey(path=("customers",), leaf="signup_at"),),
                kwargs=(("weight", ColumnKey(path=("customers", "regions"), leaf="pop")),),
            ),
            target_path=TARGET,
        )
        assert out.source == ColumnKey(path=(), leaf="spend")
        assert out.args == (ColumnKey(path=(), leaf="signup_at"),)
        assert out.kwargs == (("weight", ColumnKey(path=("regions",), leaf="pop")),)

    def test_aggregate_scalar_kwarg_passes_through(self) -> None:
        out = reroot_value_key(
            AggregateKey(
                source=ColumnKey(path=("customers",), leaf="spend"),
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
                    source=ColumnKey(path=("customers",), leaf="spend"), agg="sum",
                ),
                partition_keys=frozenset({ColumnKey(path=("customers",), leaf="tier")}),
                time_key=TimeTruncKey(
                    column=ColumnKey(path=("customers",), leaf="signup_at"),
                    granularity="month",
                ),
            ),
            target_path=TARGET,
        )
        assert out.input.source == ColumnKey(path=(), leaf="spend")
        assert out.partition_keys == frozenset({ColumnKey(path=(), leaf="tier")})
        assert out.time_key.column == ColumnKey(path=(), leaf="signup_at")

    def test_transform_scalar_args_are_type_prohibited_from_holding_keys(self) -> None:
        """``TransformKey.args``/``kwargs`` are ``Tuple[Scalar, ...]`` where
        ``Scalar = Union[Decimal, str, bool, None]`` — no ValueKey can hide
        there. Pinned so that widening the annotation later trips this test
        and forces the visitor to grow the matching traversal."""
        anno = TransformKey.model_fields["args"].annotation
        assert "ValueKey" not in str(anno), (
            f"TransformKey.args now admits {anno!r}; reroot_value_key must "
            f"traverse it (§5.4 totality)."
        )

    def test_arithmetic_operands(self) -> None:
        out = reroot_value_key(
            ArithmeticKey(
                op="+",
                operands=(
                    ColumnKey(path=("customers",), leaf="spend"),
                    LiteralKey(value=Decimal("1")),
                ),
            ),
            target_path=TARGET,
        )
        assert out.operands[0] == ColumnKey(path=(), leaf="spend")
        assert out.operands[1] == LiteralKey(value=Decimal("1"))

    def test_scalar_call_args(self) -> None:
        out = reroot_value_key(
            ScalarCallKey(
                name="coalesce",
                args=(ColumnKey(path=("customers",), leaf="tier"), "unknown"),
            ),
            target_path=TARGET,
        )
        assert out.args[0] == ColumnKey(path=(), leaf="tier")
        assert out.args[1] == "unknown"

    def test_between_members(self) -> None:
        out = reroot_value_key(
            BetweenKey(
                column=ColumnKey(path=("customers",), leaf="signup_at"),
                low=LiteralKey(value="2024-01-01"),
                high=LiteralKey(value="2024-12-31"),
            ),
            target_path=TARGET,
        )
        assert out.column == ColumnKey(path=(), leaf="signup_at")
        assert out.low == LiteralKey(value="2024-01-01")

    def test_in_members(self) -> None:
        out = reroot_value_key(
            InKey(
                column=ColumnKey(path=("customers",), leaf="tier"),
                values=(LiteralKey(value="gold"),),
            ),
            target_path=TARGET,
        )
        assert out.column == ColumnKey(path=(), leaf="tier")
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
                        source=ColumnKey(path=("customers", "regions"), leaf="pop"),
                        agg="sum",
                    ),
                    partition_keys=frozenset({
                        ColumnKey(path=("customers",), leaf="tier"),
                    }),
                ),
                ScalarCallKey(
                    name="coalesce",
                    args=(
                        InKey(
                            column=ColumnKey(path=("customers",), leaf="tier"),
                            values=(LiteralKey(value="gold"),),
                        ),
                        BetweenKey(
                            column=TimeTruncKey(
                                column=ColumnKey(
                                    path=("customers",), leaf="signup_at",
                                ),
                                granularity="day",
                            ),
                            low=LiteralKey(value="a"),
                            high=LiteralKey(value="b"),
                        ),
                    ),
                ),
            ),
        )
        out = reroot_value_key(key, target_path=TARGET)
        transform, call = out.operands
        assert transform.input.source == ColumnKey(path=("regions",), leaf="pop")
        assert transform.partition_keys == frozenset({ColumnKey(path=(), leaf="tier")})
        in_key, between_key = call.args
        assert in_key.column == ColumnKey(path=(), leaf="tier")
        assert between_key.column.column == ColumnKey(path=(), leaf="signup_at")


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
        from typing import get_args

        from slayer.core.keys import ValueKey

        samples = {
            ColumnKey: ColumnKey(path=("customers",), leaf="tier"),
            ColumnSqlKey: ColumnSqlKey(
                path=("customers",), model="customers", column_name="cr",
            ),
            TimeTruncKey: TimeTruncKey(
                column=ColumnKey(path=("customers",), leaf="signup_at"),
                granularity="day",
            ),
            StarKey: StarKey(path=("customers",)),
            LiteralKey: LiteralKey(value="x"),
            AggregateKey: AggregateKey(
                source=ColumnKey(path=("customers",), leaf="spend"), agg="sum",
            ),
            TransformKey: TransformKey(
                op="cumsum",
                input=AggregateKey(
                    source=ColumnKey(path=("customers",), leaf="spend"), agg="sum",
                ),
            ),
            ArithmeticKey: ArithmeticKey(
                op="+",
                operands=(
                    ColumnKey(path=("customers",), leaf="spend"),
                    LiteralKey(value=Decimal("1")),
                ),
            ),
            ScalarCallKey: ScalarCallKey(
                name="coalesce", args=(ColumnKey(path=("customers",), leaf="tier"),),
            ),
            BetweenKey: BetweenKey(
                column=ColumnKey(path=("customers",), leaf="signup_at"),
                low=LiteralKey(value="a"),
                high=LiteralKey(value="b"),
            ),
            InKey: InKey(
                column=ColumnKey(path=("customers",), leaf="tier"),
                values=(LiteralKey(value="gold"),),
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
            source=ColumnKey(path=("customers",), leaf="spend"), agg="sum",
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
            ColumnKey(path=("customers", "customers"), leaf="x"), target_path=TARGET,
        )
        assert once == ColumnKey(path=("customers",), leaf="x")
        twice = reroot_value_key(once, target_path=TARGET)
        assert twice == ColumnKey(path=(), leaf="x")


# ---------------------------------------------------------------------------
# Group 4 — column_filter_key invariance
# ---------------------------------------------------------------------------
class TestColumnFilterKeyInvariance:
    """``AggregateKey.column_filter_key`` is copied unchanged, and that is
    correct rather than an oversight.

    ``binding._resolve_column_filter_key`` walks ``source.path`` FIRST and then
    stamps ``anchor_model = <terminal model>``, so the fragment's
    ``referenced_join_paths`` are expressed relative to the model that OWNS the
    filtered column. Rerooting only changes how that owner is reached from the
    query root; it never moves the owner. Hence the paths are invariant.

    A standalone ``SqlExprKey`` (not attached to an aggregate) is a different
    animal — it can be anchored at the query root, so it DOES strip. Both
    directions are pinned so a future "simplification" that reroutes
    ``column_filter_key`` through the stripping case fails here.
    """

    def _filtered_agg(self) -> AggregateKey:
        return AggregateKey(
            source=ColumnKey(path=("customers",), leaf="spend"),
            agg="sum",
            column_filter_key=SqlExprKey(
                canonical_sql="regions.name = 'US'",
                referenced_join_paths=(("regions",),),
            ),
        )

    def test_column_filter_key_survives_reroot_unchanged(self) -> None:
        key = self._filtered_agg()
        out = reroot_value_key(key, target_path=TARGET)
        assert out.source == ColumnKey(path=(), leaf="spend")
        assert out.column_filter_key == key.column_filter_key

    def test_column_filter_key_unchanged_even_when_paths_share_the_prefix(self) -> None:
        """The adversarial case: the fragment's own paths LOOK strippable.
        They must still not be stripped — they are owner-relative, and a strip
        here would silently re-anchor the filter one hop too shallow."""
        key = AggregateKey(
            source=ColumnKey(path=("customers",), leaf="spend"),
            agg="sum",
            column_filter_key=SqlExprKey(
                canonical_sql="customers.tier = 'gold'",
                referenced_join_paths=(("customers",),),
            ),
        )
        out = reroot_value_key(key, target_path=TARGET)
        assert out.column_filter_key.referenced_join_paths == (("customers",),)

    def test_standalone_sql_expr_key_does_strip(self) -> None:
        """The contrasting direction — proves the invariance above is a
        deliberate per-position rule, not a missing traversal."""
        out = reroot_value_key(
            SqlExprKey(
                canonical_sql="x", referenced_join_paths=(("customers", "regions"),),
            ),
            target_path=TARGET,
        )
        assert out.referenced_join_paths == (("regions",),)

    def test_multi_hop_target_keeps_owner_relative_filter(self) -> None:
        key = AggregateKey(
            source=ColumnKey(path=("customers", "regions"), leaf="pop"),
            agg="sum",
            column_filter_key=SqlExprKey(
                canonical_sql="active = 1", referenced_join_paths=(),
            ),
        )
        out = reroot_value_key(key, target_path=DEEP_TARGET)
        assert out.source == ColumnKey(path=(), leaf="pop")
        assert out.column_filter_key == key.column_filter_key


# ---------------------------------------------------------------------------
# Group 5 — the public-identity invariant §5.4 names explicitly
# ---------------------------------------------------------------------------
class TestPublicResultKeysUnchanged:
    def test_reroot_aggregate_key_delegates_to_the_visitor(self) -> None:
        """``reroot_aggregate_key`` stays (P-J state 1) but must become a thin
        wrapper, so the two cannot drift into two reroot semantics — which is
        precisely the drift §5.4 exists to end."""
        from slayer.core.keys import reroot_aggregate_key

        key = AggregateKey(
            source=ColumnKey(path=("customers",), leaf="spend"),
            agg="last",
            args=(ColumnKey(path=("customers", "regions"), leaf="opened_at"),),
        )
        assert reroot_aggregate_key(key, target_path=TARGET) == reroot_value_key(
            key, target_path=TARGET,
        )

    def test_agg_and_column_filter_fields_ride_through(self) -> None:
        """Fields the visitor does not own must survive — a rebuild that
        enumerated only the rerootable fields would silently drop them."""
        key = AggregateKey(
            source=ColumnKey(path=("customers",), leaf="spend"),
            agg="approx_count_distinct",
            column_filter_key=SqlExprKey(canonical_sql="a = 1"),
        )
        out = reroot_value_key(key, target_path=TARGET)
        assert out.agg == "approx_count_distinct"
        assert out.column_filter_key is not None

    def test_kwargs_stay_canonically_sorted_after_reroot(self) -> None:
        key = AggregateKey(
            source=ColumnKey(path=("customers",), leaf="spend"),
            agg="corr",
            kwargs=(
                ("other", ColumnKey(path=("customers",), leaf="x")),
                ("alpha", Decimal("1")),
            ),
        )
        out = reroot_value_key(key, target_path=TARGET)
        assert [k for k, _ in out.kwargs] == sorted(k for k, _ in out.kwargs)
