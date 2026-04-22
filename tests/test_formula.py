"""Tests for the formula parser and unified fields."""

import warnings

import pytest

from slayer.core.formula import (
    AggregatedMeasureRef,
    ArithmeticField,
    MixedArithmeticField,
    TransformField,
    _rewrite_funcstyle_aggregations,
    parse_filter,
    parse_formula,
)
from slayer.engine.enrichment import extract_filter_transforms


class TestFormulaParser:
    def test_bare_measure_raises(self) -> None:
        with pytest.raises(ValueError, match="Bare measure name"):
            parse_formula("count")

    def test_bare_measure_in_arithmetic_raises(self) -> None:
        with pytest.raises(ValueError, match="Bare measure name"):
            parse_formula("revenue / count")

    def test_aggregated_measure(self) -> None:
        result = parse_formula("*:count")
        assert isinstance(result, AggregatedMeasureRef)
        assert result.measure_name == "*"
        assert result.aggregation_name == "count"

    def test_aggregated_measure_sum(self) -> None:
        result = parse_formula("revenue:sum")
        assert isinstance(result, AggregatedMeasureRef)
        assert result.measure_name == "revenue"
        assert result.aggregation_name == "sum"

    def test_arithmetic(self) -> None:
        result = parse_formula("revenue:sum / *:count")
        assert isinstance(result, ArithmeticField)

    def test_arithmetic_complex(self) -> None:
        result = parse_formula("(revenue:sum - cost:sum) / *:count")
        assert isinstance(result, ArithmeticField)

    def test_transform_cumsum(self) -> None:
        result = parse_formula("cumsum(revenue:sum)")
        assert isinstance(result, TransformField)
        assert result.transform == "cumsum"
        assert isinstance(result.inner, AggregatedMeasureRef)
        assert result.inner.measure_name == "revenue"

    def test_time_shift_row_based(self) -> None:
        result = parse_formula("time_shift(revenue:sum, -1)")
        assert isinstance(result, TransformField)
        assert result.transform == "time_shift"
        assert result.args == [-1]

    def test_time_shift_calendar_based(self) -> None:
        result = parse_formula("time_shift(revenue:sum, -1, 'year')")
        assert isinstance(result, TransformField)
        assert result.transform == "time_shift"
        assert result.args == [-1, "year"]

    def test_transform_last(self) -> None:
        result = parse_formula("last(revenue:sum)")
        assert isinstance(result, TransformField)
        assert result.transform == "last"
        assert isinstance(result.inner, AggregatedMeasureRef)
        assert result.inner.measure_name == "revenue"

    def test_transform_change(self) -> None:
        result = parse_formula("change(revenue:sum)")
        assert isinstance(result, TransformField)
        assert result.transform == "change"

    def test_nested_transform_with_arithmetic(self) -> None:
        result = parse_formula("cumsum(revenue:sum / *:count)")
        assert isinstance(result, TransformField)
        assert result.transform == "cumsum"
        assert isinstance(result.inner, ArithmeticField)

    def test_rank(self) -> None:
        result = parse_formula("rank(revenue:sum)")
        assert isinstance(result, TransformField)
        assert result.transform == "rank"

    def test_change_pct(self) -> None:
        result = parse_formula("change_pct(revenue:sum)")
        assert isinstance(result, TransformField)
        assert result.transform == "change_pct"

    def test_unknown_function_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown transform"):
            parse_formula("unknown_func(revenue)")

    def test_invalid_syntax_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid formula"):
            parse_formula("revenue +")

    def test_no_args_raises(self) -> None:
        with pytest.raises(ValueError, match="requires at least one argument"):
            parse_formula("cumsum()")

    def test_nested_transforms(self) -> None:
        """change(cumsum(revenue:sum)) → TransformField wrapping TransformField."""
        result = parse_formula("change(cumsum(revenue:sum))")
        assert isinstance(result, TransformField)
        assert result.transform == "change"
        assert isinstance(result.inner, TransformField)
        assert result.inner.transform == "cumsum"
        assert isinstance(result.inner.inner, AggregatedMeasureRef)
        assert result.inner.inner.measure_name == "revenue"

    def test_mixed_arithmetic_with_transform(self) -> None:
        """cumsum(revenue:sum) / *:count → MixedArithmeticField."""
        from slayer.core.formula import MixedArithmeticField
        result = parse_formula("cumsum(revenue:sum) / *:count")
        assert isinstance(result, MixedArithmeticField)
        assert len(result.sub_transforms) == 1
        placeholder, transform = result.sub_transforms[0]
        assert isinstance(transform, TransformField)
        assert transform.transform == "cumsum"

    def test_triple_nesting(self) -> None:
        """last(change(cumsum(revenue:sum))) → three levels deep."""
        result = parse_formula("last(change(cumsum(revenue:sum)))")
        assert isinstance(result, TransformField)
        assert result.transform == "last"
        assert isinstance(result.inner, TransformField)
        assert result.inner.transform == "change"
        assert isinstance(result.inner.inner, TransformField)
        assert result.inner.inner.transform == "cumsum"


class TestExtractFilterTransforms:
    """Tests for extract_filter_transforms reverse mapping."""

    def test_no_args_aggregation(self) -> None:
        """revenue:sum → preserved as-is in reconstructed filter."""
        rewritten, transforms = extract_filter_transforms("change(revenue:sum) > 0")
        assert len(transforms) == 1
        assert "revenue:sum" in transforms[0][1]

    def test_positional_args_aggregation(self) -> None:
        """revenue:last(ordered_at) → positional arg preserved."""
        rewritten, transforms = extract_filter_transforms("change(revenue:last(ordered_at)) > 0")
        assert len(transforms) == 1
        assert "revenue:last(ordered_at)" in transforms[0][1]

    def test_kwargs_only_aggregation(self) -> None:
        """price:weighted_avg(weight=quantity) → kwarg preserved."""
        rewritten, transforms = extract_filter_transforms(
            "change(price:weighted_avg(weight=quantity)) > 0"
        )
        assert len(transforms) == 1
        assert "price:weighted_avg(weight=quantity)" in transforms[0][1]

    def test_mixed_args_and_kwargs(self) -> None:
        """Aggregation with both positional and keyword args preserved."""
        rewritten, transforms = extract_filter_transforms(
            "change(price:weighted_avg(col1, weight=quantity)) > 0"
        )
        assert len(transforms) == 1
        assert "price:weighted_avg(col1, weight=quantity)" in transforms[0][1]


class TestParseFilterInjection:
    """SQL-injection hardening for ``parse_filter``.

    ``parse_filter`` is the single choke-point for all user-supplied filter
    expressions (measure-level ``filter``, model-level ``filters``, and
    query-level filters). These tests assert each injection payload is either
    rejected at parse time (``ValueError``) or neutralised — i.e. the payload
    appears in the output SQL only as a properly-quoted string literal, never
    as executable SQL tokens.
    """

    # --- Payloads rejected outright by ast.parse ---------------------------

    def test_rejects_statement_terminator_dropout(self) -> None:
        """Classic "break out of string, run DROP, comment rest" payload.

        Trailing ``--`` terminates with a single-quoted ``D`` followed by an
        unclosed apostrophe, which cannot parse as a Python expression.
        """
        with pytest.raises(ValueError, match="Invalid filter syntax"):
            parse_filter("status = 'a'; DROP TABLE orders; --'")

    def test_rejects_block_comment(self) -> None:
        """SQL block-comment tokens must not survive — ``/`` without a RHS
        operand yields a Python SyntaxError."""
        with pytest.raises(ValueError, match="Invalid filter syntax"):
            parse_filter("status = 'a' /* foo */ OR 1=1")

    def test_rejects_union_select(self) -> None:
        """Stacked UNION SELECT payload — ``SELECT`` is not a Python operand."""
        with pytest.raises(ValueError, match="Invalid filter syntax"):
            parse_filter("status = 'a' UNION SELECT * FROM users --'")

    def test_rejects_stacked_semicolon(self) -> None:
        """A bare semicolon separates Python statements; ``eval`` mode rejects."""
        with pytest.raises(ValueError, match="Invalid filter syntax"):
            parse_filter("status = 'a'; SELECT 1")

    def test_rejects_unknown_function_call(self) -> None:
        """Only the internal ``__like__`` / ``__notlike__`` helpers are allowed."""
        with pytest.raises(ValueError, match="Unknown filter function"):
            parse_filter("pg_sleep(10)")

    # --- Payloads that are legitimate expressions ---------------------------

    def test_allows_tautology_with_literal(self) -> None:
        """``1 = 1`` is a legal, user-authored tautology — not injection per se.

        A measure filter written by the model author is by design trusted to
        express arbitrary boolean logic; this test pins the intended semantics
        so we don't accidentally over-restrict the grammar.
        """
        result = parse_filter("status = 'a' or 1 = 1")
        assert "OR" in result.sql
        assert "1 = 1" in result.sql

    # --- Payloads that must be neutralised in the emitted SQL --------------

    def test_embedded_quote_is_doubled(self) -> None:
        """Single quote inside a string literal must emit as ``''`` (SQL standard)."""
        # The runtime filter value here contains an embedded apostrophe.
        result = parse_filter("name = 'O\\'Brien'")
        # Emitted literal must have a doubled quote, never a bare ``'``.
        assert "'O''Brien'" in result.sql

    def test_backslash_in_string_literal_is_escaped(self) -> None:
        """A backslash inside a string literal must not be able to escape the
        closing quote in MySQL-family dialects.

        Before the fix: ``parse_filter`` emits ``'a\\'`` (single backslash
        inside single quotes). In MySQL default mode, ``\\'`` is a literal
        apostrophe and the string remains open, letting trailing tokens be
        read as string content. After the fix: the backslash is doubled so
        the emitted literal is ``'a\\\\'`` (two backslashes = one literal
        backslash in MySQL's escape-aware string parsing).
        """
        # Runtime filter string is:  name = 'a\'       (six chars)
        # Python source:              "name = 'a\\\\'"  (escape both backslashes)
        result = parse_filter("name = 'a\\\\'")
        # The emitted SQL must not contain an unescaped trailing ``\'`` that
        # MySQL would read as a literal quote.
        assert "'a\\\\'" in result.sql, (
            f"Expected backslash-escaped literal, got {result.sql!r}"
        )

    def test_backslash_mid_string_is_escaped(self) -> None:
        """Backslash anywhere inside a string literal must be doubled so that
        subsequent characters can't be (mis)interpreted as escape sequences.
        """
        # Runtime string:  name = 'a\b' and x = 1
        result = parse_filter("name = 'a\\\\b' and x = 1")
        assert "'a\\\\b'" in result.sql
        # Sanity: the surrounding AND clause is preserved intact.
        assert "x = 1" in result.sql

    def test_backslash_in_like_pattern_is_escaped(self) -> None:
        """The ``LIKE`` pattern path runs through ``_get_string_arg`` — make
        sure it applies the same backslash protection as ``_filter_node_to_sql``.
        """
        # Runtime string:  name like 'a\'
        result = parse_filter("name like 'a\\\\'")
        assert "LIKE" in result.sql
        assert "'a\\\\'" in result.sql

    def test_identifier_cannot_inject_sql(self) -> None:
        """Bare column names are constrained to valid Python identifiers.

        A name containing a space / punctuation can't even reach the AST as
        an ``ast.Name``, so there's no way to sneak ``DROP`` in via a name.
        """
        with pytest.raises(ValueError, match="Invalid filter syntax"):
            parse_filter("status; DROP TABLE users; --")

    def test_deeply_nested_boolean_does_not_crash(self) -> None:
        """A very deep boolean expression must either parse bounded or raise
        cleanly — never crash the interpreter / exhaust the stack."""
        payload = " or ".join(["x = 1"] * 200)
        # Either accepted (returns SQL containing many ORs) or rejected with
        # a normal ValueError; both are acceptable outcomes.
        try:
            result = parse_filter(payload)
        except ValueError:
            return
        assert result.sql.count("OR") >= 100


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


class TestFuncStyleEndToEnd:
    """End-to-end tests through parse_formula and parse_filter."""

    def test_sum_parses(self) -> None:
        result = parse_formula("sum(revenue)")
        assert isinstance(result, AggregatedMeasureRef)
        assert result.measure_name == "revenue"
        assert result.aggregation_name == "sum"

    def test_count_star_parses(self) -> None:
        result = parse_formula("count(*)")
        assert isinstance(result, AggregatedMeasureRef)
        assert result.measure_name == "*"
        assert result.aggregation_name == "count"

    def test_cross_model_parses(self) -> None:
        result = parse_formula("sum(customers.revenue)")
        assert isinstance(result, AggregatedMeasureRef)
        assert result.measure_name == "customers.revenue"
        assert result.aggregation_name == "sum"

    def test_nested_in_cumsum_parses(self) -> None:
        result = parse_formula("cumsum(sum(revenue))")
        assert isinstance(result, TransformField)
        assert result.transform == "cumsum"
        assert isinstance(result.inner, AggregatedMeasureRef)
        assert result.inner.measure_name == "revenue"
        assert result.inner.aggregation_name == "sum"

    def test_arithmetic_parses(self) -> None:
        result = parse_formula("sum(revenue) / count(*)")
        assert isinstance(result, (ArithmeticField, MixedArithmeticField))

    def test_weighted_avg_parses(self) -> None:
        result = parse_formula("weighted_avg(price, weight=qty)")
        assert isinstance(result, AggregatedMeasureRef)
        assert result.measure_name == "price"
        assert result.aggregation_name == "weighted_avg"
        assert result.agg_kwargs == {"weight": "qty"}

    def test_filter_funcstyle(self) -> None:
        result = parse_filter("sum(revenue) > 100")
        assert "revenue_sum" in result.sql
        assert ">" in result.sql

    def test_filter_count_star(self) -> None:
        result = parse_filter("count(*) >= 5")
        assert "_count" in result.sql

    def test_last_bare_parses_as_aggregation(self) -> None:
        result = parse_formula("last(revenue)")
        assert isinstance(result, AggregatedMeasureRef)
        assert result.measure_name == "revenue"
        assert result.aggregation_name == "last"

    def test_first_bare_parses_as_aggregation(self) -> None:
        result = parse_formula("first(revenue)")
        assert isinstance(result, AggregatedMeasureRef)
        assert result.measure_name == "revenue"
        assert result.aggregation_name == "first"

    def test_cross_model_custom_agg_parses(self) -> None:
        result = parse_formula(
            "rolling_avg(customers.score)",
            extra_agg_names=frozenset({"rolling_avg"}),
        )
        assert isinstance(result, AggregatedMeasureRef)
        assert result.measure_name == "customers.score"
        assert result.aggregation_name == "rolling_avg"


class TestAggregationNameValidation:
    """Custom aggregation names must not conflict with transform names."""

    def test_rejects_transform_name(self) -> None:
        from slayer.core.models import Aggregation
        with pytest.raises(ValueError, match="conflicts with a built-in transform"):
            Aggregation(name="cumsum", formula="SUM({value})")

    def test_rejects_time_shift(self) -> None:
        from slayer.core.models import Aggregation
        with pytest.raises(ValueError, match="conflicts with a built-in transform"):
            Aggregation(name="time_shift", formula="SUM({value})")

    def test_allows_non_conflicting_name(self) -> None:
        from slayer.core.models import Aggregation
        agg = Aggregation(name="rolling_avg", formula="AVG({value})")
        assert agg.name == "rolling_avg"

    def test_allows_builtin_override(self) -> None:
        """Built-in names like 'sum' that are also in ALL_TRANSFORMS (first/last) are fine."""
        from slayer.core.models import Aggregation
        agg = Aggregation(name="sum")  # built-in override, no formula needed
        assert agg.name == "sum"


class TestFirstTransform:
    """Tests for the first() transform (mirroring last())."""

    def test_first_transform_parses(self) -> None:
        result = parse_formula("first(revenue:sum)")
        assert isinstance(result, TransformField)
        assert result.transform == "first"
        assert isinstance(result.inner, AggregatedMeasureRef)
        assert result.inner.measure_name == "revenue"
        assert result.inner.aggregation_name == "sum"

    def test_first_transform_in_filter(self) -> None:
        """first() can be used in filter expressions like last()."""
        rewritten, transforms = extract_filter_transforms("first(revenue:sum) > 0")
        assert len(transforms) == 1
        assert "revenue:sum" in transforms[0][1]

    def test_last_transform_still_works(self) -> None:
        """Existing last() transform should be unaffected."""
        result = parse_formula("last(revenue:sum)")
        assert isinstance(result, TransformField)
        assert result.transform == "last"


class TestOrderColumnNormalization:
    """Order column normalization with function-style syntax."""

    def test_funcstyle_sum(self) -> None:
        from slayer.core.query import OrderItem
        item = OrderItem(column="sum(revenue)", direction="desc")
        assert item.column.name == "revenue_sum"
        assert item.raw_formula == "revenue:sum"

    def test_funcstyle_count_star(self) -> None:
        from slayer.core.query import OrderItem
        item = OrderItem(column="count(*)", direction="desc")
        assert item.column.name == "_count"
        assert item.raw_formula == "*:count"

    def test_colon_syntax_still_works(self) -> None:
        from slayer.core.query import OrderItem
        item = OrderItem(column="revenue:sum", direction="desc")
        assert item.column.name == "revenue_sum"
        assert item.raw_formula == "revenue:sum"

    def test_star_count_colon_still_works(self) -> None:
        from slayer.core.query import OrderItem
        item = OrderItem(column="*:count", direction="asc")
        assert item.column.name == "_count"
        assert item.raw_formula == "*:count"

    def test_plain_name_unchanged(self) -> None:
        from slayer.core.query import OrderItem
        item = OrderItem(column="revenue_sum", direction="desc")
        assert item.column.name == "revenue_sum"
        assert item.raw_formula is None

    def test_parameterized_agg_stripped(self) -> None:
        from slayer.core.query import OrderItem
        item = OrderItem(column="revenue:last(ordered_at)", direction="desc")
        assert item.column.name == "revenue_last"
        assert item.raw_formula == "revenue:last(ordered_at)"

    def test_weighted_avg_args_stripped(self) -> None:
        from slayer.core.query import OrderItem
        item = OrderItem(column="price:weighted_avg(weight=qty)", direction="asc")
        assert item.column.name == "price_weighted_avg"
        assert item.raw_formula == "price:weighted_avg(weight=qty)"
