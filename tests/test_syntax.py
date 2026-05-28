"""Stage 7a.3 (DEV-1450) — Mode-B Python-AST parser tests.

The parser at ``slayer.engine.syntax.parse_expr`` consumes a Mode-B
expression string (the SLayer DSL) and emits a typed ``ParsedExpr`` AST
the binder (stage 7a.5) compiles to ``BoundExpr``. Coverage:

- Bare refs, dotted refs, star, literals.
- Colon-syntax aggregations (``revenue:sum``, ``*:count``, parametric).
- Transforms (``cumsum`` / ``rank`` / etc.).
- Scalar functions (closed allowlist from ``SCALAR_FUNCTIONS``).
- Arithmetic, comparison, boolean, unary ops.
- Rejection: unknown function calls, raw ``OVER(...)``, plain syntax errors.

The parser is dormant in stage 7a — no engine code calls it yet. The
binder (7a.5) is the first consumer.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.errors import IllegalWindowInFilterError, UnknownFunctionError
from slayer.engine.syntax import (
    AggCall,
    Arith,
    BoolOp,
    Cmp,
    DottedRef,
    Literal,
    Ref,
    ScalarCall,
    StarSource,
    TransformCall,
    UnaryOp,
    parse_expr,
    parse_filter_expr,
)


# ---------------------------------------------------------------------------
# Refs and literals
# ---------------------------------------------------------------------------


class TestRefsAndLiterals:
    def test_bare_ref(self):
        assert parse_expr("revenue") == Ref(name="revenue")

    def test_dotted_ref_two_parts(self):
        assert parse_expr("customers.revenue") == DottedRef(
            parts=("customers", "revenue"),
        )

    def test_dotted_ref_three_parts(self):
        assert parse_expr("customers.regions.name") == DottedRef(
            parts=("customers", "regions", "name"),
        )

    def test_integer_literal(self):
        assert parse_expr("42") == Literal(value=Decimal(42))

    def test_decimal_literal(self):
        assert parse_expr("0.5") == Literal(value=Decimal("0.5"))

    def test_string_literal_single_quote(self):
        assert parse_expr("'hello'") == Literal(value="hello")

    def test_string_literal_double_quote(self):
        assert parse_expr('"hello"') == Literal(value="hello")

    def test_bool_true(self):
        assert parse_expr("True") == Literal(value=True)

    def test_bool_false(self):
        assert parse_expr("False") == Literal(value=False)

    def test_none(self):
        assert parse_expr("None") == Literal(value=None)


# ---------------------------------------------------------------------------
# Aggregations (colon syntax)
# ---------------------------------------------------------------------------


class TestAggregations:
    def test_local_aggregation(self):
        assert parse_expr("revenue:sum") == AggCall(
            source=Ref(name="revenue"), agg="sum",
        )

    def test_cross_model_aggregation(self):
        assert parse_expr("customers.revenue:sum") == AggCall(
            source=DottedRef(parts=("customers", "revenue")), agg="sum",
        )

    def test_star_count(self):
        assert parse_expr("*:count") == AggCall(
            source=StarSource(), agg="count",
        )

    def test_count_distinct(self):
        assert parse_expr("customer_id:count_distinct") == AggCall(
            source=Ref(name="customer_id"), agg="count_distinct",
        )

    def test_aggregation_with_positional_arg(self):
        # revenue:last(ordered_at) — `ordered_at` is a positional arg.
        result = parse_expr("revenue:last(ordered_at)")
        assert isinstance(result, AggCall)
        assert result.source == Ref(name="revenue")
        assert result.agg == "last"
        assert result.args == (Ref(name="ordered_at"),)
        assert result.kwargs == ()

    def test_aggregation_with_kwarg(self):
        # price:weighted_avg(weight=quantity)
        result = parse_expr("price:weighted_avg(weight=quantity)")
        assert isinstance(result, AggCall)
        assert result.source == Ref(name="price")
        assert result.agg == "weighted_avg"
        assert result.kwargs == (("weight", Ref(name="quantity")),)

    def test_aggregation_with_numeric_kwarg(self):
        # revenue:percentile(p=0.5)
        result = parse_expr("revenue:percentile(p=0.5)")
        assert isinstance(result, AggCall)
        assert result.kwargs == (("p", Literal(value=Decimal("0.5"))),)


# ---------------------------------------------------------------------------
# Transforms
# ---------------------------------------------------------------------------


class TestTransforms:
    def test_cumsum(self):
        result = parse_expr("cumsum(revenue:sum)")
        assert isinstance(result, TransformCall)
        assert result.op == "cumsum"
        assert result.input == AggCall(source=Ref(name="revenue"), agg="sum")
        assert result.args == ()
        assert result.kwargs == ()

    def test_rank(self):
        result = parse_expr("rank(revenue:sum)")
        assert isinstance(result, TransformCall)
        assert result.op == "rank"

    def test_change_with_partition_by(self):
        # change(revenue:sum, partition_by=region) — C6 routing kwarg.
        result = parse_expr("change(revenue:sum, partition_by=region)")
        assert isinstance(result, TransformCall)
        assert result.op == "change"
        assert result.input == AggCall(source=Ref(name="revenue"), agg="sum")
        assert result.kwargs == (("partition_by", Ref(name="region")),)

    def test_nested_transform(self):
        # change(cumsum(revenue:sum))
        result = parse_expr("change(cumsum(revenue:sum))")
        assert isinstance(result, TransformCall)
        assert result.op == "change"
        assert isinstance(result.input, TransformCall)
        assert result.input.op == "cumsum"

    def test_ntile_with_n(self):
        # ntile(revenue:sum, n=4)
        result = parse_expr("ntile(revenue:sum, n=4)")
        assert isinstance(result, TransformCall)
        assert result.op == "ntile"
        assert result.kwargs == (("n", Literal(value=Decimal(4))),)

    def test_first_transform(self):
        # DEV-1484 backfill from test_formula.py::TestFirstTransform —
        # ``first(...)`` is the FIRST_VALUE window transform (distinct from
        # the ``:first`` aggregation), parsed as a TransformCall.
        result = parse_expr("first(revenue:sum)")
        assert isinstance(result, TransformCall)
        assert result.op == "first"
        assert result.input == AggCall(source=Ref(name="revenue"), agg="sum")

    def test_last_transform(self):
        # DEV-1484 backfill from test_formula.py::TestFirstTransform.
        result = parse_expr("last(revenue:sum)")
        assert isinstance(result, TransformCall)
        assert result.op == "last"
        assert result.input == AggCall(source=Ref(name="revenue"), agg="sum")


# ---------------------------------------------------------------------------
# Scalar functions (closed allowlist)
# ---------------------------------------------------------------------------


class TestScalarFunctions:
    def test_lower(self):
        result = parse_expr("lower(name)")
        assert isinstance(result, ScalarCall)
        assert result.name == "lower"
        assert result.args == (Ref(name="name"),)

    def test_coalesce(self):
        result = parse_expr("coalesce(revenue, 0)")
        assert isinstance(result, ScalarCall)
        assert result.name == "coalesce"
        assert result.args == (Ref(name="revenue"), Literal(value=Decimal(0)))

    def test_nested_scalar_call(self):
        result = parse_expr("lower(coalesce(name, 'unknown'))")
        assert isinstance(result, ScalarCall)
        assert result.name == "lower"
        inner = result.args[0]
        assert isinstance(inner, ScalarCall)
        assert inner.name == "coalesce"
        assert inner.args == (Ref(name="name"), Literal(value="unknown"))

    def test_scalar_call_wrapping_aggregate(self):
        # nullif(revenue:max, 0) — scalar function over an aggregate.
        result = parse_expr("nullif(revenue:max, 0)")
        assert isinstance(result, ScalarCall)
        assert result.name == "nullif"
        assert result.args[0] == AggCall(source=Ref(name="revenue"), agg="max")


# ---------------------------------------------------------------------------
# Arithmetic / comparison / boolean / unary
# ---------------------------------------------------------------------------


class TestOperators:
    def test_add(self):
        result = parse_expr("revenue:sum + 1")
        assert isinstance(result, Arith)
        assert result.op == "+"
        assert result.left == AggCall(source=Ref(name="revenue"), agg="sum")
        assert result.right == Literal(value=Decimal(1))

    def test_divide_two_aggregates(self):
        # revenue:sum / *:count → the AOV formula.
        result = parse_expr("revenue:sum / *:count")
        assert isinstance(result, Arith)
        assert result.op == "/"
        assert isinstance(result.left, AggCall)
        assert isinstance(result.right, AggCall)
        assert result.right.source == StarSource()

    def test_subtract(self):
        result = parse_expr("revenue:sum - cost:sum")
        assert isinstance(result, Arith)
        assert result.op == "-"

    def test_multiply(self):
        result = parse_expr("amount * 2")
        assert isinstance(result, Arith)
        assert result.op == "*"

    def test_parens_dont_change_tree(self):
        # (revenue:sum) is still an AggCall — parens just group.
        result = parse_expr("(revenue:sum)")
        assert result == AggCall(source=Ref(name="revenue"), agg="sum")

    def test_comparison_lt(self):
        result = parse_expr("revenue:sum < 100")
        assert isinstance(result, Cmp)
        assert result.op == "<"

    def test_comparison_le(self):
        result = parse_expr("revenue:sum <= 100")
        assert isinstance(result, Cmp)
        assert result.op == "<="

    def test_comparison_gt(self):
        result = parse_expr("revenue:sum > 100")
        assert isinstance(result, Cmp)
        assert result.op == ">"

    def test_comparison_ge(self):
        result = parse_expr("revenue:sum >= 100")
        assert isinstance(result, Cmp)
        assert result.op == ">="

    def test_comparison_ne(self):
        result = parse_expr("status != 'cancelled'")
        assert isinstance(result, Cmp)
        assert result.op == "!="

    def test_comparison_eq(self):
        # Python AST uses `==`; SLayer DSL allows that and we normalise.
        result = parse_expr("status == 'paid'")
        assert isinstance(result, Cmp)
        assert result.op == "=="

    def test_bool_and(self):
        result = parse_expr("x > 1 and y < 10")
        assert isinstance(result, BoolOp)
        assert result.op == "and"
        assert len(result.operands) == 2
        # Verify inner Cmps have the right operators.
        ops = {op.op for op in result.operands if isinstance(op, Cmp)}
        assert ops == {">", "<"}

    def test_bool_or(self):
        result = parse_expr("x > 1 or y < 10")
        assert isinstance(result, BoolOp)
        assert result.op == "or"

    def test_bool_and_multi_operand(self):
        # Python AST flattens `a and b and c` into one BoolOp(and, [a,b,c]).
        result = parse_expr("a > 1 and b > 2 and c > 3")
        assert isinstance(result, BoolOp)
        assert result.op == "and"
        assert len(result.operands) == 3

    def test_bool_or_multi_operand(self):
        result = parse_expr("a > 1 or b > 2 or c > 3")
        assert isinstance(result, BoolOp)
        assert result.op == "or"
        assert len(result.operands) == 3

    def test_unary_minus(self):
        result = parse_expr("-revenue")
        assert isinstance(result, UnaryOp)
        assert result.op == "-"
        assert result.operand == Ref(name="revenue")

    def test_unary_plus(self):
        # Python AST has UAdd for `+x`. Either accept as UnaryOp("+") or
        # collapse to the operand. We require: same as operand (collapsed).
        result = parse_expr("+revenue")
        # `+x` is semantically a no-op; either UnaryOp("+") or Ref is fine.
        assert isinstance(result, (UnaryOp, Ref))
        if isinstance(result, UnaryOp):
            assert result.op == "+"

    def test_unary_not(self):
        result = parse_expr("not x")
        assert isinstance(result, UnaryOp)
        assert result.op == "not"


# ---------------------------------------------------------------------------
# Rejection cases
# ---------------------------------------------------------------------------


class TestRejection:
    def test_unknown_function_call_raises(self):
        # `random_func` is not in SCALAR_FUNCTIONS / transforms / aggregations.
        with pytest.raises(UnknownFunctionError):
            parse_expr("random_func(revenue)")

    def test_raw_over_clause_raises(self):
        # `SUM(x) OVER (...)` is a window expression — DSL rejects.
        with pytest.raises(IllegalWindowInFilterError):
            parse_expr("sum(x) OVER (PARTITION BY region)")

    def test_raw_over_case_insensitive(self):
        with pytest.raises(IllegalWindowInFilterError):
            parse_expr("count(*) over ()")

    def test_empty_input_raises(self):
        with pytest.raises(ValueError):
            parse_expr("")

    def test_syntax_error_raises(self):
        with pytest.raises(ValueError):
            parse_expr("revenue:sum +")

    def test_double_underscore_in_ref_raises(self):
        # Mode B rejects `__` in identifiers — `__` is reserved for
        # internal join-path aliases on the SQL side.
        with pytest.raises(ValueError, match="__|double-underscore"):
            parse_expr("customers__regions.name")

    def test_double_underscore_in_bare_ref_raises(self):
        with pytest.raises(ValueError, match="__|double-underscore"):
            parse_expr("foo__bar")

    def test_double_underscore_inside_dotted_part_raises(self):
        with pytest.raises(ValueError, match="__|double-underscore"):
            parse_expr("customers.foo__bar")

    def test_function_style_aggregation_rejected(self):
        # `sum(revenue)` is canonicalised by the slack normalization layer
        # before the parser sees it. If it still reaches the parser, that's
        # a contract violation — reject with a clear error pointing to
        # colon syntax.
        with pytest.raises((UnknownFunctionError, ValueError)):
            parse_expr("sum(revenue)")

    def test_function_style_count_star_rejected(self):
        with pytest.raises((UnknownFunctionError, ValueError)):
            parse_expr("count(*)")

    def test_chained_comparison_rejected(self):
        # `1 < x < 10` in Python is a chained comparison — DSL rejects
        # to keep `Cmp.op` single-valued. Users split as
        # `1 < x and x < 10`.
        with pytest.raises(ValueError, match="chain|chained|single"):
            parse_expr("1 < x < 10")

    def test_double_colon_aggregation_rejected(self):
        # `revenue:sum:avg` — extra trailing colon after the agg name.
        with pytest.raises(ValueError):
            parse_expr("revenue:sum:avg")

    def test_scalar_function_with_kwarg_rejected(self):
        # Scalar functions in SCALAR_FUNCTIONS take only positional args.
        # `lower(name=...)` is invalid.
        with pytest.raises((ValueError, UnknownFunctionError)):
            parse_expr("lower(name='x')")

    def test_colon_inside_string_literal_preserved(self):
        # `status == 'revenue:sum'` — the string literal is data, not
        # syntax. Must NOT be rewritten by the colon preprocessor.
        result = parse_expr("status == 'revenue:sum'")
        assert isinstance(result, Cmp)
        assert result.op == "=="
        assert result.right == Literal(value="revenue:sum")

    def test_dunder_inside_string_literal_allowed(self):
        # `__` inside a string literal is data, not an identifier.
        result = parse_expr("status == 'foo__bar'")
        assert isinstance(result, Cmp)
        assert result.right == Literal(value="foo__bar")


# ---------------------------------------------------------------------------
# Combination smoke tests
# ---------------------------------------------------------------------------


class TestCombinations:
    def test_transform_inside_arithmetic(self):
        # cumsum(revenue:sum) / *:count
        result = parse_expr("cumsum(revenue:sum) / *:count")
        assert isinstance(result, Arith)
        assert result.op == "/"
        assert isinstance(result.left, TransformCall)
        assert isinstance(result.right, AggCall)

    def test_complex_filter_predicate(self):
        # (revenue:sum > 100) and (status == 'paid')
        result = parse_expr("(revenue:sum > 100) and (status == 'paid')")
        assert isinstance(result, BoolOp)
        assert result.op == "and"
        assert len(result.operands) == 2

    def test_change_filter(self):
        # change(revenue:sum) > 0 — desugar handled later by lowering, not parser.
        result = parse_expr("change(revenue:sum) > 0")
        assert isinstance(result, Cmp)
        assert result.op == ">"
        assert isinstance(result.left, TransformCall)
        assert result.left.op == "change"


# ---------------------------------------------------------------------------
# Filter-operator normalization (parse_filter_expr)
# ---------------------------------------------------------------------------


class TestFilterOperatorNormalization:
    """``parse_filter_expr`` accepts SQL operator spellings (``=`` for
    equality, ``AND`` / ``IS`` / ``NULL`` ...) on top of the Python DSL."""

    def test_sql_equality_normalised(self):
        result = parse_filter_expr("status = 'paid'")
        assert isinstance(result, Cmp)
        assert result.op == "=="
        assert result.right == Literal(value="paid")

    def test_transform_no_kwargs_in_filter(self):
        # The no-kwargs top-N form is unaffected by the operator rewrite.
        result = parse_filter_expr("dense_rank(revenue:sum) <= 5")
        assert isinstance(result, Cmp)
        assert isinstance(result.left, TransformCall)
        assert result.left.op == "dense_rank"

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "DEV-1492: parse_filter_expr's SQL '='->'==' rewrite mangles a "
            "transform kwarg inside call parens (n=4 -> n==4), so the kwarg "
            "lands as a positional Cmp arg instead of kwargs. Auto-promotes "
            "to PASS when DEV-1492 is fixed."
        ),
    )
    def test_transform_kwarg_preserved_in_filter(self):
        # ntile(revenue:sum, n=4) <= 1 — the n=4 kwarg must survive the
        # filter operator-normalization, not be rewritten to ``n == 4``.
        result = parse_filter_expr("ntile(revenue:sum, n=4) <= 1")
        assert isinstance(result, Cmp)
        assert isinstance(result.left, TransformCall)
        assert result.left.op == "ntile"
        assert result.left.args == ()
        assert result.left.kwargs == (("n", Literal(value=Decimal(4))),)
