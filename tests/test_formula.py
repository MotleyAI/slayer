"""Formula-level parse checks: functional aggregation spellings, order-column normalization,
filter injection hardening and string hygiene, all over the one Mode-B parser."""

import warnings

import pytest

from slayer.core.models import Aggregation
from slayer.core.query import _FUNCSTYLE_PENDING, OrderItem
from slayer.core.syntax import AggCall, BoolOp, Cmp, Literal, Ref, ScalarCall, parse_expr


class TestFilterInjection:
    """SQL-injection hardening for ``parse_expr`` — the choke-point for
    all user-supplied Mode-B filter expressions. Payloads must be rejected at
    parse time; literal SQL emission is dialect-owned (sqlglot) and covered by
    the SQL-generator round-trip suites."""

    @pytest.mark.parametrize(
        "payload",
        [
            # Classic "break out of string, run DROP, comment rest".
            "status = 'a'; DROP TABLE orders; --'",
            # SQL block comment.
            "status = 'a' /* foo */ OR 1=1",
            # Stacked UNION SELECT.
            "status = 'a' UNION SELECT * FROM users --'",
            # Stacked statement via semicolon.
            "status = 'a'; SELECT 1",
            # DROP smuggled where an identifier is expected.
            "status; DROP TABLE users; --",
        ],
    )
    def test_payload_rejected_at_parse(self, payload: str) -> None:
        with pytest.raises(ValueError, match="Invalid Mode-B expression"):
            parse_expr(payload)

    def test_allows_tautology_with_literal(self) -> None:
        # ``1 = 1`` is a legal, user-authored tautology — not injection per se.
        result = parse_expr("status = 'a' or 1 = 1")
        assert isinstance(result, BoolOp)
        assert result.op == "or"

    def test_deeply_nested_boolean_does_not_crash(self) -> None:
        # 200 chained ORs must parse bounded or raise cleanly — never crash.
        payload = " or ".join(["x = 1"] * 200)
        try:
            result = parse_expr(payload)
        except ValueError:
            return
        assert isinstance(result, BoolOp)
        assert len(result.operands) >= 100


# ---------------------------------------------------------------------------
# Function-style aggregation rewrite
# ---------------------------------------------------------------------------


class TestFuncStyleRewrite:
    """The functional aggregation spelling parses to the same node as the colon spelling."""

    @pytest.mark.parametrize(("functional", "colon"), [
        ("sum(revenue)", "revenue:sum"),
        ("avg(amount)", "amount:avg"),
        ("min(price)", "price:min"),
        ("max(price)", "price:max"),
        ("count(*)", "*:count"),
        ("count(customer_id)", "customer_id:count"),
        ("count_distinct(id)", "id:count_distinct"),
        ("median(price)", "price:median"),
        ("sum(customers.revenue)", "customers.revenue:sum"),
        ("sum(a.b.c.d)", "a.b.c.d:sum"),
        ("weighted_avg(price, weight=qty)", "price:weighted_avg(weight=qty)"),
        ("last(revenue, ordered_at)", "revenue:last(ordered_at)"),
        ("first(revenue, ordered_at)", "revenue:first(ordered_at)"),
        ("percentile(revenue, p=0.95)", "revenue:percentile(p=0.95)"),
        ("sum(revenue) / count(*)", "revenue:sum / *:count"),
        ("sum(revenue) + avg(amount)", "revenue:sum + amount:avg"),
        ("cumsum(sum(revenue))", "cumsum(revenue:sum)"),
        ("change(sum(revenue))", "change(revenue:sum)"),
        ("last(revenue:sum) + sum(amount)", "last(revenue:sum) + amount:sum"),
        ("rolling_avg(revenue)", "revenue:rolling_avg"),
        ("count(customers.*)", "customers.*:count"),
        ("sum(revenue) > 0 and name = 'count(x)'", "revenue:sum > 0 and name == 'count(x)'"),
        ("sum(revenue) > 100", "revenue:sum > 100"),
        ("stddev_samp(latency)", "latency:stddev_samp"),
        ("stddev_pop(latency)", "latency:stddev_pop"),
        ("var_samp(latency)", "latency:var_samp"),
        ("var_pop(latency)", "latency:var_pop"),
        ("corr(price, other=quantity)", "price:corr(other=quantity)"),
        ("covar_samp(price, other=quantity)", "price:covar_samp(other=quantity)"),
        ("covar_pop(price, other=quantity)", "price:covar_pop(other=quantity)"),
    ])
    def test_functional_equals_colon(self, functional: str, colon: str) -> None:
        assert parse_expr(functional) == parse_expr(colon)

    def test_custom_agg_name(self) -> None:
        assert parse_expr("rolling_avg(revenue)") == AggCall(source=Ref(name="revenue"), agg="rolling_avg")

    def test_emits_no_warning(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            parse_expr("sum(revenue)")
            assert w == []

    @pytest.mark.parametrize(("text", "value"), [
        ("name = 'sum(revenue)'", "sum(revenue)"),
        ("name = 'it''s sum(x)'", "it's sum(x)"),
    ])
    def test_inside_string_literal(self, text: str, value: str) -> None:
        assert parse_expr(text) == Cmp(op="==", left=Ref(name="name"), right=Literal(value=value))

    @pytest.mark.parametrize("scalar_call", [
        "ln(amount)", "log10(amount)", "log(10, amount)", "exp(rate)", "sqrt(price)", "pow(2, 10)", "power(2, 10)",
    ])
    def test_scalar_math_is_not_an_aggregation(self, scalar_call: str) -> None:
        assert isinstance(parse_expr(scalar_call), ScalarCall)


class TestAggregationNameValidation:
    """Custom aggregation names must not conflict with transform names."""

    def test_rejects_transform_name(self) -> None:
        with pytest.raises(ValueError, match="conflicts with a built-in transform"):
            Aggregation(name="cumsum", formula="SUM({value})")

    def test_rejects_time_shift(self) -> None:
        with pytest.raises(ValueError, match="conflicts with a built-in transform"):
            Aggregation(name="time_shift", formula="SUM({value})")

    def test_allows_non_conflicting_name(self) -> None:
        agg = Aggregation(name="rolling_avg", formula="AVG({value})")
        assert agg.name == "rolling_avg"

    def test_allows_builtin_override(self) -> None:
        """Built-in names like 'sum' that are also in ALL_TRANSFORMS (first/last) are fine."""
        agg = Aggregation(name="sum")  # built-in override, no formula needed
        assert agg.name == "sum"


class TestOrderColumnNormalization:
    """Order column normalization with function-style syntax."""

    def test_funcstyle_sum(self) -> None:
        # The author's functional spelling is preserved — the item
        # carries a placeholder + raw_formula, resolved at binding.
        item = OrderItem(column="sum(revenue)", direction="desc")
        assert item.column.name == _FUNCSTYLE_PENDING
        assert item.raw_formula == "sum(revenue)"

    def test_funcstyle_count_star(self) -> None:
        item = OrderItem(column="count(*)", direction="desc")
        assert item.column.name == _FUNCSTYLE_PENDING
        assert item.raw_formula == "count(*)"

    def test_colon_syntax_still_works(self) -> None:
        item = OrderItem(column="revenue:sum", direction="desc")
        assert item.column.name == "revenue_sum"
        assert item.raw_formula == "revenue:sum"

    def test_star_count_colon_still_works(self) -> None:
        item = OrderItem(column="*:count", direction="asc")
        assert item.column.name == "_count"
        assert item.raw_formula == "*:count"

    def test_plain_name_unchanged(self) -> None:
        item = OrderItem(column="revenue_sum", direction="desc")
        assert item.column.name == "revenue_sum"
        assert item.raw_formula is None

    def test_parameterized_agg_stripped(self) -> None:
        item = OrderItem(column="revenue:last(ordered_at)", direction="desc")
        assert item.column.name == "revenue_last"
        assert item.raw_formula == "revenue:last(ordered_at)"

    def test_weighted_avg_args_stripped(self) -> None:
        item = OrderItem(column="price:weighted_avg(weight=qty)", direction="asc")
        assert item.column.name == "price_weighted_avg"
        assert item.raw_formula == "price:weighted_avg(weight=qty)"


class TestStringHygieneFilters:
    """String-hygiene shapes on the typed filter parser; single
    scalar calls and the two-operand ``||`` are covered by
    ``test_syntax.py::TestScalarFunctions`` /
    ``TestFilterOperatorNormalization``."""

    def test_pipe_pipe_chain_three_operands(self) -> None:
        # Chained `||` desugars left-associatively to nested concat calls.
        result = parse_expr("a || b || c = 'foo'")
        assert isinstance(result, Cmp)
        outer = result.left
        assert outer.name == "concat"
        assert outer.args[0].name == "concat"

    def test_pipe_pipe_no_spaces(self) -> None:
        result = parse_expr("a||b = 'foo'")
        assert isinstance(result, Cmp)
        assert result.left.name == "concat"

    def test_pipe_pipe_with_function_call_operands(self) -> None:
        result = parse_expr("lower(name) || ' ' || trim(addr) = 'eu london'")
        assert isinstance(result, Cmp)
        assert result.left.name == "concat"

    def test_pipe_pipe_preserved_in_string_literal(self) -> None:
        # `||` inside a string literal must NOT be rewritten.
        result = parse_expr("note = 'a||b'")
        assert isinstance(result, Cmp)
        assert result.right.value == "a||b"

    def test_function_name_preserved_in_string_literal(self) -> None:
        result = parse_expr("note = 'lower(x)'")
        assert isinstance(result, Cmp)
        assert result.right.value == "lower(x)"
