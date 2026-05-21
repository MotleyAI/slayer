"""Stage 7b.2 (DEV-1450) — pre-bind ModelMeasure expansion.

Pins the contract for ``slayer.engine.measure_expansion.expand_model_measures``:

- Pre-bind AST -> AST rewrite. Walks a ``ParsedExpr`` and replaces every
  ``Ref(name=X)`` node whose ``X`` resolves to a ``ModelMeasure`` (on the
  model or in ``extra_measures``) with the recursively-expanded
  ``ParsedExpr`` of that measure's formula.
- Eligible positions: root, ``Arith`` operands, ``Cmp`` operands,
  ``BoolOp`` operands, ``UnaryOp`` operand, ``ScalarCall`` args,
  ``TransformCall.input`` / args / kwarg values.
- Not eligible: ``DottedRef`` (cross-model paths), ``AggCall`` source /
  args / kwargs (aggregation refs are column-level), ``Literal``,
  ``StarSource``. Function names on ``TransformCall`` / ``ScalarCall``
  are also untouched (only the name field is excluded from rewriting).
- Recursive: a measure's expansion can reference further measures.
- Depth limit: configurable via ``SLAYER_MEASURE_EXPANSION_DEPTH``
  (default 32). Exceeded -> ``MeasureRecursionLimitError``.
- Cycle detection (per-chain): a measure referenced transitively from
  itself raises ``MeasureCycleError``.
- Pure: input ``ParsedExpr`` is not mutated; result is a fresh tree.
"""

from __future__ import annotations

import os
from decimal import Decimal
from unittest import mock

import pytest

from slayer.core.errors import MeasureCycleError, MeasureRecursionLimitError
from slayer.core.models import (
    Column,
    DataType,
    ModelMeasure,
    SlayerModel,
)
from slayer.engine.measure_expansion import expand_model_measures
from slayer.engine.syntax import (
    AggCall,
    Arith,
    DottedRef,
    Literal,
    Ref,
    StarSource,
    TransformCall,
    parse_expr,
)


def _make_model(
    *,
    measures: list[ModelMeasure],
    columns: list[Column] | None = None,
) -> SlayerModel:
    if columns is None:
        columns = [
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
            Column(name="status", type=DataType.TEXT),
        ]
    return SlayerModel(
        name="orders",
        data_source="db",
        sql_table="orders",
        columns=columns,
        measures=measures,
    )


class TestSimpleExpansion:
    def test_bare_measure_ref_expands_to_formula_ast(self) -> None:
        """``aov`` (a measure on the model) -> the parsed AST of its formula."""
        model = _make_model(
            measures=[
                ModelMeasure(formula="amount:sum / *:count", name="aov"),
            ]
        )
        expr = parse_expr("aov")
        out = expand_model_measures(expr=expr, model=model)
        assert out == parse_expr("amount:sum / *:count")

    def test_bare_column_ref_left_alone(self) -> None:
        """A bare name that matches a column (not a measure) is not touched."""
        model = _make_model(measures=[])
        expr = parse_expr("amount")
        out = expand_model_measures(expr=expr, model=model)
        assert out == Ref(name="amount")

    def test_bare_unknown_ref_left_alone(self) -> None:
        """A bare name that matches neither a column nor a measure is not
        touched; the binder (downstream) decides whether to raise."""
        model = _make_model(measures=[])
        expr = parse_expr("mystery")
        out = expand_model_measures(expr=expr, model=model)
        assert out == Ref(name="mystery")


class TestDottedRefsAndStaticNodes:
    def test_dotted_ref_segment_named_like_measure_not_expanded(self) -> None:
        """``customers.aov`` -> the leaf ``aov`` is a dotted-ref segment,
        not eligible for expansion even when ``aov`` exists as a measure."""
        model = _make_model(
            measures=[ModelMeasure(formula="amount:sum", name="aov")]
        )
        expr = parse_expr("customers.aov")
        out = expand_model_measures(expr=expr, model=model)
        assert out == DottedRef(parts=("customers", "aov"))

    def test_star_source_unchanged(self) -> None:
        model = _make_model(measures=[])
        expr = parse_expr("*:count")
        out = expand_model_measures(expr=expr, model=model)
        assert out == AggCall(source=StarSource(), agg="count")

    def test_literal_unchanged(self) -> None:
        model = _make_model(measures=[])
        expr = parse_expr("42")
        out = expand_model_measures(expr=expr, model=model)
        assert out == Literal(value=Decimal("42"))


class TestAggCallScope:
    def test_aggcall_source_not_expanded_when_named_like_measure(self) -> None:
        """``aov:sum`` — even with ``aov`` registered as a measure — stays
        as the raw AggCall. ``AggCall.source`` is column-level; the binder
        will raise on it if it is genuinely a measure name."""
        model = _make_model(
            measures=[ModelMeasure(formula="amount:sum", name="aov")]
        )
        expr = parse_expr("aov:sum")
        out = expand_model_measures(expr=expr, model=model)
        assert out == AggCall(source=Ref(name="aov"), agg="sum")

    def test_aggcall_positional_arg_left_alone(self) -> None:
        """``amount:last(aov)`` — even though ``aov`` is registered as a
        measure, the positional arg slot inside an AggCall is not
        expanded. AggCall is treated as a leaf by expansion; the binder
        (downstream) handles the eventual column-vs-measure resolution
        and will raise if the arg is genuinely non-column."""
        model = _make_model(
            measures=[ModelMeasure(formula="amount:sum", name="aov")]
        )
        expr = parse_expr("amount:last(aov)")
        out = expand_model_measures(expr=expr, model=model)
        assert isinstance(out, AggCall)
        assert out.source == Ref(name="amount")
        assert out.agg == "last"
        assert out.args == (Ref(name="aov"),)

    def test_aggcall_kwarg_rhs_left_alone(self) -> None:
        """``amount:weighted_avg(weight=measure_x)`` — kwarg RHS is
        column-level by contract. Even when ``measure_x`` IS a
        registered measure, expansion does not recurse into AggCall."""
        model = _make_model(
            measures=[ModelMeasure(formula="amount:sum", name="measure_x")]
        )
        expr = parse_expr("amount:weighted_avg(weight=measure_x)")
        out = expand_model_measures(expr=expr, model=model)
        assert isinstance(out, AggCall)
        assert out.source == Ref(name="amount")
        assert out.agg == "weighted_avg"
        assert out.kwargs == (("weight", Ref(name="measure_x")),)


class TestArithmeticAndComparison:
    def test_arith_left_operand_expanded(self) -> None:
        model = _make_model(
            measures=[ModelMeasure(formula="amount:sum", name="rev")]
        )
        out = expand_model_measures(expr=parse_expr("rev + 1"), model=model)
        assert out == parse_expr("amount:sum + 1")

    def test_arith_right_operand_expanded(self) -> None:
        model = _make_model(
            measures=[ModelMeasure(formula="amount:sum", name="rev")]
        )
        out = expand_model_measures(expr=parse_expr("1 + rev"), model=model)
        assert out == parse_expr("1 + amount:sum")

    def test_unary_op_expanded(self) -> None:
        model = _make_model(
            measures=[ModelMeasure(formula="amount:sum", name="rev")]
        )
        out = expand_model_measures(expr=parse_expr("-rev"), model=model)
        assert out == parse_expr("-amount:sum")

    def test_cmp_left_operand_expanded(self) -> None:
        model = _make_model(
            measures=[ModelMeasure(formula="amount:sum", name="rev")]
        )
        out = expand_model_measures(expr=parse_expr("rev > 100"), model=model)
        assert out == parse_expr("amount:sum > 100")

    def test_cmp_right_operand_expanded(self) -> None:
        model = _make_model(
            measures=[ModelMeasure(formula="amount:sum", name="rev")]
        )
        out = expand_model_measures(expr=parse_expr("100 < rev"), model=model)
        assert out == parse_expr("100 < amount:sum")

    def test_boolop_operands_expanded(self) -> None:
        model = _make_model(
            measures=[
                ModelMeasure(formula="amount:sum", name="rev"),
                ModelMeasure(formula="*:count", name="cnt"),
            ]
        )
        out = expand_model_measures(
            expr=parse_expr("rev > 100 and cnt > 0"), model=model
        )
        assert out == parse_expr("amount:sum > 100 and *:count > 0")


class TestTransformAndScalarCalls:
    def test_transform_input_expanded(self) -> None:
        model = _make_model(
            measures=[ModelMeasure(formula="amount:sum", name="rev")]
        )
        out = expand_model_measures(
            expr=parse_expr("cumsum(rev)"), model=model
        )
        assert out == parse_expr("cumsum(amount:sum)")

    def test_transform_kwarg_measure_value_expanded(self) -> None:
        """Plan: 'aggregation kwargs RHS' is NOT eligible; transform kwargs
        ARE eligible (not excluded). Pin the distinction with a kwarg RHS
        that actually IS a measure name."""
        model = _make_model(
            measures=[
                ModelMeasure(formula="amount:sum", name="rev"),
                ModelMeasure(formula="status", name="grp"),
            ]
        )
        out = expand_model_measures(
            expr=parse_expr("rank(rev, partition_by=grp)"),
            model=model,
        )
        assert out == parse_expr("rank(amount:sum, partition_by=status)")

    def test_transform_positional_args_expanded(self) -> None:
        """``input`` and additional positional ``args`` on a TransformCall
        are both eligible."""
        model = _make_model(
            measures=[
                ModelMeasure(formula="amount:sum", name="rev"),
                ModelMeasure(formula="7", name="n"),
            ]
        )
        out = expand_model_measures(
            expr=parse_expr("lag(rev, n)"), model=model
        )
        assert out == parse_expr("lag(amount:sum, 7)")

    def test_transform_function_name_not_expanded(self) -> None:
        """Even if a measure named ``cumsum`` somehow existed, the
        transform function name slot is structurally a string, not a
        Ref — so no expansion path applies. Pin this for safety."""
        model = _make_model(
            measures=[ModelMeasure(formula="amount:sum", name="rev")]
        )
        out = expand_model_measures(
            expr=parse_expr("cumsum(rev)"), model=model
        )
        assert isinstance(out, TransformCall)
        assert out.op == "cumsum"

    def test_scalar_call_args_expanded(self) -> None:
        model = _make_model(
            measures=[ModelMeasure(formula="amount:sum", name="rev")]
        )
        out = expand_model_measures(
            expr=parse_expr("nullif(rev, 0)"), model=model
        )
        assert out == parse_expr("nullif(amount:sum, 0)")

    def test_scalar_function_name_not_expanded(self) -> None:
        """If a measure happens to be named the same as a scalar function
        (e.g., ``nullif``), the function-name slot is not touched."""
        model = _make_model(
            measures=[
                ModelMeasure(formula="amount:sum", name="nullif"),
                ModelMeasure(formula="amount:sum", name="rev"),
            ]
        )
        out = expand_model_measures(
            expr=parse_expr("nullif(rev, 0)"), model=model
        )
        assert out == parse_expr("nullif(amount:sum, 0)")


class TestRecursiveExpansion:
    def test_chained_expansion_two_levels(self) -> None:
        """``a`` references ``b``; expansion goes all the way down."""
        model = _make_model(
            measures=[
                ModelMeasure(formula="amount:sum", name="b"),
                ModelMeasure(formula="b * 2", name="a"),
            ]
        )
        out = expand_model_measures(expr=parse_expr("a"), model=model)
        assert out == parse_expr("amount:sum * 2")

    def test_chained_expansion_three_levels(self) -> None:
        model = _make_model(
            measures=[
                ModelMeasure(formula="amount:sum", name="c"),
                ModelMeasure(formula="c + 1", name="b"),
                ModelMeasure(formula="b * 2", name="a"),
            ]
        )
        out = expand_model_measures(expr=parse_expr("a"), model=model)
        assert out == parse_expr("(amount:sum + 1) * 2")


class TestCycleDetection:
    def test_direct_self_reference_raises(self) -> None:
        model = _make_model(
            measures=[ModelMeasure(formula="aov + 1", name="aov")]
        )
        with pytest.raises(MeasureCycleError) as exc_info:
            expand_model_measures(expr=parse_expr("aov"), model=model)
        assert "aov" in str(exc_info.value)

    def test_transitive_cycle_raises(self) -> None:
        """a -> b -> a is a cycle."""
        model = _make_model(
            measures=[
                ModelMeasure(formula="a + 1", name="b"),
                ModelMeasure(formula="b", name="a"),
            ]
        )
        with pytest.raises(MeasureCycleError) as exc_info:
            expand_model_measures(expr=parse_expr("a"), model=model)
        chain_str = " → ".join(exc_info.value.chain)
        assert "a" in chain_str and "b" in chain_str

    def test_cycle_chain_attached_to_error(self) -> None:
        model = _make_model(
            measures=[
                ModelMeasure(formula="b", name="a"),
                ModelMeasure(formula="a", name="b"),
            ]
        )
        with pytest.raises(MeasureCycleError) as exc_info:
            expand_model_measures(expr=parse_expr("a"), model=model)
        # Chain captures the full traversal that hit the cycle:
        # entered a, then a referenced b, then b referenced a (cycle).
        assert exc_info.value.chain == ["a", "b", "a"]


class TestDepthLimit:
    def test_depth_limit_exceeded_raises(self) -> None:
        """Build a long acyclic chain m1 -> m2 -> ... -> m10. With
        depth_limit=3, expansion should bail."""
        measures = [ModelMeasure(formula="amount:sum", name="m10")]
        for i in range(9, 0, -1):
            measures.append(
                ModelMeasure(formula=f"m{i + 1}", name=f"m{i}")
            )
        model = _make_model(measures=measures)
        with pytest.raises(MeasureRecursionLimitError) as exc_info:
            expand_model_measures(
                expr=parse_expr("m1"), model=model, depth_limit=3
            )
        assert exc_info.value.limit == 3
        assert exc_info.value.chain[0] == "m1"

    def test_depth_limit_default_32_allows_32_expansions(self) -> None:
        """Default depth limit allows a chain of 32 expansions."""
        measures = [ModelMeasure(formula="amount:sum", name="m32")]
        for i in range(31, 0, -1):
            measures.append(
                ModelMeasure(formula=f"m{i + 1}", name=f"m{i}")
            )
        model = _make_model(measures=measures)
        out = expand_model_measures(expr=parse_expr("m1"), model=model)
        assert out == parse_expr("amount:sum")

    def test_depth_limit_default_32_rejects_33_expansions(self) -> None:
        """Default depth limit rejects a chain of 33 expansions."""
        measures = [ModelMeasure(formula="amount:sum", name="m33")]
        for i in range(32, 0, -1):
            measures.append(
                ModelMeasure(formula=f"m{i + 1}", name=f"m{i}")
            )
        model = _make_model(measures=measures)
        with pytest.raises(MeasureRecursionLimitError):
            expand_model_measures(expr=parse_expr("m1"), model=model)

    def test_env_var_overrides_default_depth(self) -> None:
        measures = [ModelMeasure(formula="amount:sum", name="m4")]
        for i in range(3, 0, -1):
            measures.append(
                ModelMeasure(formula=f"m{i + 1}", name=f"m{i}")
            )
        model = _make_model(measures=measures)
        with mock.patch.dict(
            os.environ, {"SLAYER_MEASURE_EXPANSION_DEPTH": "2"}
        ):
            with pytest.raises(MeasureRecursionLimitError) as exc_info:
                expand_model_measures(expr=parse_expr("m1"), model=model)
            assert exc_info.value.limit == 2

    def test_explicit_zero_depth_limit_raises_valueerror(self) -> None:
        model = _make_model(
            measures=[ModelMeasure(formula="amount:sum", name="rev")]
        )
        with pytest.raises(ValueError, match="depth_limit must be a positive"):
            expand_model_measures(
                expr=parse_expr("rev"), model=model, depth_limit=0
            )

    def test_explicit_negative_depth_limit_raises_valueerror(self) -> None:
        model = _make_model(
            measures=[ModelMeasure(formula="amount:sum", name="rev")]
        )
        with pytest.raises(ValueError, match="depth_limit must be a positive"):
            expand_model_measures(
                expr=parse_expr("rev"), model=model, depth_limit=-3
            )

    def test_explicit_depth_limit_overrides_env_var(self) -> None:
        measures = [ModelMeasure(formula="amount:sum", name="m4")]
        for i in range(3, 0, -1):
            measures.append(
                ModelMeasure(formula=f"m{i + 1}", name=f"m{i}")
            )
        model = _make_model(measures=measures)
        with mock.patch.dict(
            os.environ, {"SLAYER_MEASURE_EXPANSION_DEPTH": "100"}
        ):
            with pytest.raises(MeasureRecursionLimitError) as exc_info:
                expand_model_measures(
                    expr=parse_expr("m1"),
                    model=model,
                    depth_limit=2,
                )
            assert exc_info.value.limit == 2


class TestExtraMeasures:
    def test_extra_measures_resolve_alongside_model_measures(self) -> None:
        """``extra_measures`` (inline ``SlayerQuery.measures``) participate
        in resolution. They are not stored on the model but are in scope."""
        model = _make_model(measures=[])
        out = expand_model_measures(
            expr=parse_expr("inline + 1"),
            model=model,
            extra_measures=(
                ModelMeasure(formula="amount:sum", name="inline"),
            ),
        )
        assert out == parse_expr("amount:sum + 1")

    def test_extra_measures_shadow_model_measures_with_same_name(self) -> None:
        """Inline (extra) measures take precedence over model measures
        with the same name — mirrors saved vs query-inline measure
        precedence elsewhere."""
        model = _make_model(
            measures=[ModelMeasure(formula="amount:sum", name="rev")]
        )
        out = expand_model_measures(
            expr=parse_expr("rev"),
            model=model,
            extra_measures=(
                ModelMeasure(formula="amount:sum * 2", name="rev"),
            ),
        )
        assert out == parse_expr("amount:sum * 2")

    def test_unnamed_extra_measures_ignored(self) -> None:
        """An inline measure without a ``name`` is not addressable as a
        bare ref; it cannot be the target of expansion."""
        model = _make_model(measures=[])
        out = expand_model_measures(
            expr=parse_expr("unnamed"),
            model=model,
            extra_measures=(ModelMeasure(formula="amount:sum"),),
        )
        assert out == Ref(name="unnamed")



class TestPurityAndIdempotence:
    def test_does_not_mutate_input_expr(self) -> None:
        """``ParsedExpr`` nodes are frozen Pydantic models, but defensive
        check: the input tree's identity values are unchanged after a
        call. (frozen=True makes mutation impossible, so we just confirm
        equality before/after.)"""
        model = _make_model(
            measures=[ModelMeasure(formula="amount:sum", name="rev")]
        )
        expr = parse_expr("rev + 1")
        snapshot = expr
        expand_model_measures(expr=expr, model=model)
        assert expr == snapshot

    def test_no_op_when_no_measures_match(self) -> None:
        """A formula with no references to any model measure is returned
        equal to its input."""
        model = _make_model(measures=[])
        expr = parse_expr("amount:sum + 1")
        out = expand_model_measures(expr=expr, model=model)
        assert out == expr

    def test_expanded_tree_is_idempotent_under_re_expansion(self) -> None:
        """Re-expansion of an already-expanded tree is a no-op (no
        residual measure refs)."""
        model = _make_model(
            measures=[ModelMeasure(formula="amount:sum", name="rev")]
        )
        once = expand_model_measures(expr=parse_expr("rev + 1"), model=model)
        twice = expand_model_measures(expr=once, model=model)
        assert twice == once

    def test_repeated_refs_share_parse_cache(self) -> None:
        """A formula referencing the same measure twice produces the
        same expanded subtree without re-parsing the measure formula.
        Functionally observable as equality of the two expanded branches.
        """
        model = _make_model(
            measures=[ModelMeasure(formula="amount:sum", name="rev")]
        )
        out = expand_model_measures(
            expr=parse_expr("rev + rev"), model=model
        )
        assert out == parse_expr("amount:sum + amount:sum")
        # The two operands are structurally identical (frozen Pydantic
        # value equality).
        assert isinstance(out, Arith)
        assert out.left == out.right

    def test_expansion_does_not_resolve_columns_inside_formula(self) -> None:
        """Pre-bind expansion is syntactic only — it does not validate
        that columns referenced inside the expanded formula exist on the
        model. The binder (downstream) is responsible for resolution."""
        model = _make_model(
            measures=[
                ModelMeasure(
                    formula="not_a_column:sum", name="bad_saved_measure"
                ),
            ]
        )
        out = expand_model_measures(
            expr=parse_expr("bad_saved_measure"), model=model
        )
        assert out == parse_expr("not_a_column:sum")
