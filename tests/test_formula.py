"""Tests for the retained formula helpers — ``_rewrite_funcstyle_aggregations``
(function-style -> colon rewrite + ORDER BY normalization) — plus the typed
filter parser's injection hardening.

The legacy free-function formula parser, its AST node types, and the legacy
``parse_filter`` were removed when the typed pipeline took over; their
coverage now lives in ``test_syntax.py`` (parse shape, LIKE / ``||`` / scalar
calls in filters), ``test_sql_boolean_literal_filters.py`` (SQL-cased boolean
literals), ``test_binding.py`` / ``test_transforms_planner.py`` (bind-time
validation), ``test_transform_lowerer.py`` (change/change_pct desugar),
``test_named_measures.py`` (named-measure expansion, end-to-end),
``test_model_measure_expansion.py`` (bind-time expansion, cycles, scoping),
``test_measure_expansion.py`` (expansion eligibility), and
``test_schema_drift_typed.py`` / ``test_memories_resolver_typed.py`` (typed
ref-walk path extraction).
"""

import warnings

import pytest

from slayer.core.formula import _rewrite_funcstyle_aggregations
from slayer.core.models import Aggregation
from slayer.core.query import _FUNCSTYLE_PENDING, OrderItem
from slayer.engine.syntax import BoolOp, parse_filter_expr


class TestFilterInjection:
    """SQL-injection hardening for ``parse_filter_expr`` — the choke-point for
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
            parse_filter_expr(payload)

    def test_allows_tautology_with_literal(self) -> None:
        # ``1 = 1`` is a legal, user-authored tautology — not injection per se.
        result = parse_filter_expr("status = 'a' or 1 = 1")
        assert isinstance(result, BoolOp)
        assert result.op == "or"

    def test_deeply_nested_boolean_does_not_crash(self) -> None:
        # 200 chained ORs must parse bounded or raise cleanly — never crash.
        payload = " or ".join(["x = 1"] * 200)
        try:
            result = parse_filter_expr(payload)
        except ValueError:
            return
        assert isinstance(result, BoolOp)
        assert len(result.operands) >= 100


# ---------------------------------------------------------------------------
# Function-style aggregation rewrite
# ---------------------------------------------------------------------------


class TestFuncStyleRewrite:
    """Unit tests for _rewrite_funcstyle_aggregations."""

    def test_sum(self) -> None:
        assert _rewrite_funcstyle_aggregations("sum(revenue)") == "revenue:sum"

    def test_avg(self) -> None:
        assert _rewrite_funcstyle_aggregations("avg(amount)") == "amount:avg"

    def test_min(self) -> None:
        assert _rewrite_funcstyle_aggregations("min(price)") == "price:min"

    def test_max(self) -> None:
        assert _rewrite_funcstyle_aggregations("max(price)") == "price:max"

    def test_count_star(self) -> None:
        assert _rewrite_funcstyle_aggregations("count(*)") == "*:count"

    def test_count_column(self) -> None:
        assert _rewrite_funcstyle_aggregations("count(customer_id)") == "customer_id:count"

    def test_count_distinct(self) -> None:
        assert _rewrite_funcstyle_aggregations("count_distinct(id)") == "id:count_distinct"

    def test_median(self) -> None:
        assert _rewrite_funcstyle_aggregations("median(price)") == "price:median"

    def test_first_bare(self) -> None:
        assert _rewrite_funcstyle_aggregations("first(revenue)") == "revenue:first"

    def test_last_bare(self) -> None:
        assert _rewrite_funcstyle_aggregations("last(revenue)") == "revenue:last"

    def test_cross_model(self) -> None:
        assert _rewrite_funcstyle_aggregations("sum(customers.revenue)") == "customers.revenue:sum"

    def test_multi_hop(self) -> None:
        assert _rewrite_funcstyle_aggregations("sum(a.b.c.d)") == "a.b.c.d:sum"

    def test_weighted_avg_kwargs(self) -> None:
        assert _rewrite_funcstyle_aggregations("weighted_avg(price, weight=qty)") == "price:weighted_avg(weight=qty)"

    def test_last_with_positional_arg(self) -> None:
        assert _rewrite_funcstyle_aggregations("last(revenue, ordered_at)") == "revenue:last(ordered_at)"

    def test_first_with_positional_arg(self) -> None:
        assert _rewrite_funcstyle_aggregations("first(revenue, ordered_at)") == "revenue:first(ordered_at)"

    def test_percentile_kwargs(self) -> None:
        assert _rewrite_funcstyle_aggregations("percentile(revenue, p=0.95)") == "revenue:percentile(p=0.95)"

    # Compound expressions
    def test_arithmetic(self) -> None:
        assert _rewrite_funcstyle_aggregations("sum(revenue) / count(*)") == "revenue:sum / *:count"

    def test_addition(self) -> None:
        assert _rewrite_funcstyle_aggregations("sum(revenue) + avg(amount)") == "revenue:sum + amount:avg"

    # Nested in transforms
    def test_nested_in_transform(self) -> None:
        assert _rewrite_funcstyle_aggregations("cumsum(sum(revenue))") == "cumsum(revenue:sum)"

    def test_nested_in_change(self) -> None:
        assert _rewrite_funcstyle_aggregations("change(sum(revenue))") == "change(revenue:sum)"

    # Ambiguity: last/first as transform (colon syntax in inner) — leave alone
    def test_last_transform_untouched(self) -> None:
        assert _rewrite_funcstyle_aggregations("last(revenue:sum)") == "last(revenue:sum)"

    def test_first_transform_untouched(self) -> None:
        assert _rewrite_funcstyle_aggregations("first(revenue:sum)") == "first(revenue:sum)"

    # Mixed: transform + function-style aggregation in same expression
    def test_transform_and_funcstyle(self) -> None:
        result = _rewrite_funcstyle_aggregations("last(revenue:sum) + sum(amount)")
        assert "last(revenue:sum)" in result
        assert "amount:sum" in result

    # No-op cases
    def test_already_colon_syntax(self) -> None:
        assert _rewrite_funcstyle_aggregations("revenue:sum") == "revenue:sum"

    def test_plain_transform(self) -> None:
        assert _rewrite_funcstyle_aggregations("cumsum(revenue:sum)") == "cumsum(revenue:sum)"

    def test_unknown_function(self) -> None:
        assert _rewrite_funcstyle_aggregations("some_func(x)") == "some_func(x)"

    def test_no_args(self) -> None:
        assert _rewrite_funcstyle_aggregations("revenue") == "revenue"

    def test_colon_syntax_with_args_in_last(self) -> None:
        """revenue:last(ordered_at) should not be touched."""
        assert _rewrite_funcstyle_aggregations("revenue:last(ordered_at)") == "revenue:last(ordered_at)"

    # Custom aggregation names
    def test_custom_agg_name(self) -> None:
        result = _rewrite_funcstyle_aggregations(
            "rolling_avg(revenue)", extra_agg_names=frozenset({"rolling_avg"})
        )
        assert result == "revenue:rolling_avg"

    def test_custom_agg_unknown_without_extra(self) -> None:
        """Without extra_agg_names, custom agg names are not rewritten."""
        assert _rewrite_funcstyle_aggregations("rolling_avg(revenue)") == "rolling_avg(revenue)"

    # Emits warning
    def test_emits_warning(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _rewrite_funcstyle_aggregations("sum(revenue)")
            assert len(w) == 1
            assert "Auto-rewrote" in str(w[0].message)

    # Quoted string literals — must not be rewritten
    def test_inside_single_quoted_string(self) -> None:
        assert _rewrite_funcstyle_aggregations("name = 'sum(revenue)'") == "name = 'sum(revenue)'"

    def test_mixed_quoted_and_unquoted(self) -> None:
        result = _rewrite_funcstyle_aggregations("sum(revenue) > 0 and name = 'count(x)'")
        assert result == "revenue:sum > 0 and name = 'count(x)'"

    # Escaped quotes inside strings
    def test_escaped_quote_in_string(self) -> None:
        """Backslash-escaped quote inside a string must not break string tracking."""
        assert _rewrite_funcstyle_aggregations(r"name = 'it\'s sum(x)'") == r"name = 'it\'s sum(x)'"

    # Filter context
    def test_in_filter_expression(self) -> None:
        result = _rewrite_funcstyle_aggregations("sum(revenue) > 100")
        assert result == "revenue:sum > 100"

    # New stat aggregations (DEV-1317)
    def test_stddev_samp_funcstyle(self) -> None:
        """`stddev_samp(latency)` must rewrite to colon syntax once the
        aggregation name is registered as built-in."""
        assert _rewrite_funcstyle_aggregations("stddev_samp(latency)") == "latency:stddev_samp"

    def test_stddev_pop_funcstyle(self) -> None:
        assert _rewrite_funcstyle_aggregations("stddev_pop(latency)") == "latency:stddev_pop"

    def test_var_samp_funcstyle(self) -> None:
        assert _rewrite_funcstyle_aggregations("var_samp(latency)") == "latency:var_samp"

    def test_var_pop_funcstyle(self) -> None:
        assert _rewrite_funcstyle_aggregations("var_pop(latency)") == "latency:var_pop"

    @pytest.mark.parametrize("agg", ["corr", "covar_samp", "covar_pop"])
    def test_two_arg_stat_funcstyle_with_other_kwarg(self, agg: str) -> None:
        """`corr(price, other=quantity)` and `covar_*(price, other=quantity)`
        all mirror `weighted_avg(price, weight=qty)` — first positional arg
        becomes the LHS column, named kwarg(s) become agg_kwargs."""
        assert (
            _rewrite_funcstyle_aggregations(f"{agg}(price, other=quantity)")
            == f"price:{agg}(other=quantity)"
        )

    # Scalar math functions must NOT be rewritten — they are plain SQL
    # passthrough used inside Column.sql / formula expressions.
    @pytest.mark.parametrize(
        "scalar_call",
        [
            "ln(amount)",
            "log10(amount)",
            "log(10, amount)",
            "exp(rate)",
            "sqrt(price)",
            "pow(2, 10)",
            "power(2, 10)",
        ],
    )
    def test_scalar_math_unchanged(self, scalar_call: str) -> None:
        # Scalar math UDF names are not aggregations; the rewrite must
        # leave them untouched.
        assert _rewrite_funcstyle_aggregations(scalar_call) == scalar_call


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
        # DEV-1826: the author's functional spelling is preserved — the item
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
    """DEV-1378 string-hygiene shapes on the typed filter parser; single
    scalar calls and the two-operand ``||`` are covered by
    ``test_syntax.py::TestScalarFunctions`` /
    ``TestFilterOperatorNormalization``."""

    def test_pipe_pipe_chain_three_operands(self) -> None:
        # Chained `||` desugars left-associatively to nested concat calls.
        result = parse_filter_expr("a || b || c = 'foo'")
        outer = result.left
        assert outer.name == "concat"
        assert outer.args[0].name == "concat"

    def test_pipe_pipe_no_spaces(self) -> None:
        result = parse_filter_expr("a||b = 'foo'")
        assert result.left.name == "concat"

    def test_pipe_pipe_with_function_call_operands(self) -> None:
        result = parse_filter_expr("lower(name) || ' ' || trim(addr) = 'eu london'")
        assert result.left.name == "concat"

    def test_pipe_pipe_preserved_in_string_literal(self) -> None:
        # `||` inside a string literal must NOT be rewritten.
        result = parse_filter_expr("note = 'a||b'")
        assert result.right.value == "a||b"

    def test_function_name_preserved_in_string_literal(self) -> None:
        result = parse_filter_expr("note = 'lower(x)'")
        assert result.right.value == "lower(x)"
