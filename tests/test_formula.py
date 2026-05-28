"""Tests for the retained formula helpers — ``parse_filter`` (filter
injection + string-hygiene scalars) and ``_rewrite_funcstyle_aggregations``
(function-style -> colon rewrite + ORDER BY normalization).

The legacy free-function formula parser and its AST node types were removed
when the typed pipeline took over; their coverage now lives in
``test_syntax.py`` (parse shape), ``test_binding.py`` /
``test_transforms_planner.py`` (bind-time validation),
``test_transform_lowerer.py`` (change/change_pct desugar),
``test_named_measures.py`` (named-measure expansion, end-to-end),
``test_measure_expansion.py`` (expansion eligibility), and
``test_schema_drift_typed.py`` / ``test_memories_resolver_typed.py`` (typed
ref-walk path extraction).
"""

import warnings

import pytest

from slayer.core.formula import (
    _rewrite_funcstyle_aggregations,
    parse_filter,
)


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

    # --- DEV-1376: path-qualified LIKE / NOT LIKE ---------------------------

    def test_like_path_qualified_simple_literal(self) -> None:
        """``<joined_model>.<col> like '...'`` must parse — agents reach for
        this shape because dotted refs work in dimensions/measures."""
        result = parse_filter("infrastructure.wateraccess like '%yes%'")
        assert "infrastructure.wateraccess LIKE '%yes%'" in result.sql

    def test_like_path_qualified_messy_literal(self) -> None:
        """Literal content (commas, spaces, mixed case) must not affect
        whether path-qualified LIKE parses. Reproduces the original
        benchmark failure (households_14)."""
        result = parse_filter(
            "infrastructure.wateraccess like '%Yes, available at least in one room%'"
        )
        assert (
            "infrastructure.wateraccess LIKE "
            "'%Yes, available at least in one room%'"
        ) in result.sql

    def test_not_like_path_qualified(self) -> None:
        """NOT LIKE on a dotted path mirrors the LIKE fix."""
        result = parse_filter("customers.email not like '%spam.com'")
        assert "customers.email NOT LIKE '%spam.com'" in result.sql

    # --- DEV-1378: hygiene-call LHS for LIKE / NOT LIKE ---------------------

    def test_like_hygiene_call_lhs(self) -> None:
        """``lower(name) like 'a%'`` and friends must parse — DEV-1378
        added hygiene scalars but the LIKE preprocessor only matched
        bare/dotted identifiers, so call LHS surfaced as a syntax error."""
        result = parse_filter("lower(name) like 'a%'")
        assert "lower(name) LIKE 'a%'" in result.sql

    def test_not_like_hygiene_call_lhs(self) -> None:
        result = parse_filter("trim(email) not like '%@test.com'")
        assert "trim(email) NOT LIKE '%@test.com'" in result.sql

    def test_like_hygiene_call_dotted_arg(self) -> None:
        """The hygiene call's argument itself can be a dotted ref."""
        result = parse_filter("lower(customers.email) like '%@motley.ai'")
        assert "lower(customers.email) LIKE '%@motley.ai'" in result.sql

    # --- DEV-1376: subquery-in-filter helpful error -------------------------

    def test_filter_subquery_in_clause_raises(self) -> None:
        """``IN (SELECT ...)`` should surface the targeted error instead of
        Python's misleading "Perhaps you forgot a comma" advice."""
        with pytest.raises(ValueError, match="Subqueries are not allowed"):
            parse_filter("housenum in (select houselink from properties)")

    def test_filter_subquery_not_in_clause_raises(self) -> None:
        """``NOT IN (SELECT ...)`` is also a subquery shape."""
        with pytest.raises(ValueError, match="Subqueries are not allowed"):
            parse_filter("id not in (select id from t)")

    def test_filter_exists_subquery_raises(self) -> None:
        """``EXISTS (SELECT ...)`` is also a subquery shape."""
        with pytest.raises(ValueError, match="Subqueries are not allowed"):
            parse_filter("exists (select 1 from t)")

    def test_filter_subquery_shape_inside_string_literal_does_not_raise(self) -> None:
        """The subquery sniff must ignore SQL-shaped text that lives inside a
        string-literal RHS of a comparison — it's data, not syntax."""
        result = parse_filter("note = 'in (select 1 from t)'")
        assert "note = 'in (select 1 from t)'" in result.sql


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


class TestStringHygieneFilters:
    """DEV-1378: lowercase string-hygiene scalar functions accepted inline
    in Mode B (DSL) filters: ``lower``, ``upper``, ``trim``, ``replace``,
    ``substr``, ``instr``, ``length``, ``concat``. The SQL ``||``
    operator is rewritten to ``concat(...)`` by ``_preprocess_concat``.
    """

    @pytest.mark.parametrize("op", ["lower", "upper", "trim", "length"])
    def test_unary_op_round_trips(self, op: str) -> None:
        pf = parse_filter(f"{op}(name) = 'eu'")
        assert pf.sql == f"{op}(name) = 'eu'"
        assert "name" in pf.columns

    def test_replace_three_arg(self) -> None:
        pf = parse_filter("replace(x, ',', '') = 'foo'")
        assert pf.sql == "replace(x, ',', '') = 'foo'"
        assert "x" in pf.columns

    def test_substr_three_arg(self) -> None:
        pf = parse_filter("substr(s, 1, 5) = 'abcde'")
        assert pf.sql == "substr(s, 1, 5) = 'abcde'"
        assert "s" in pf.columns

    def test_substr_two_arg(self) -> None:
        pf = parse_filter("substr(s, 3) = 'abc'")
        assert pf.sql == "substr(s, 3) = 'abc'"

    def test_instr_with_string_literal(self) -> None:
        pf = parse_filter("instr(s, ',') > 0")
        assert pf.sql == "instr(s, ',') > 0"
        assert "s" in pf.columns

    def test_concat_explicit_call(self) -> None:
        pf = parse_filter("concat(a, b, c) = 'abc'")
        assert pf.sql == "concat(a, b, c) = 'abc'"
        assert {"a", "b", "c"}.issubset(set(pf.columns))

    def test_nested_length_replace(self) -> None:
        pf = parse_filter("length(replace(x, ',', '')) > 0")
        assert pf.sql == "length(replace(x, ',', '')) > 0"
        assert "x" in pf.columns

    def test_substr_instr_pairing(self) -> None:
        # Canonical "first delimited token" pattern from the issue.
        pf = parse_filter("substr(s, 1, instr(s, ',') - 1) = 'first'")
        assert pf.sql == "substr(s, 1, instr(s, ',') - 1) = 'first'"

    def test_pipe_pipe_two_operands(self) -> None:
        pf = parse_filter("a || b = 'foo'")
        assert pf.sql == "concat(a, b) = 'foo'"
        assert {"a", "b"}.issubset(set(pf.columns))

    def test_pipe_pipe_chain_three_operands(self) -> None:
        # Chained `||` folds into a flat n-ary concat.
        pf = parse_filter("a || b || c = 'foo'")
        assert pf.sql == "concat(a, b, c) = 'foo'"

    def test_pipe_pipe_no_spaces(self) -> None:
        pf = parse_filter("a||b = 'foo'")
        assert pf.sql == "concat(a, b) = 'foo'"

    def test_pipe_pipe_with_function_call_operands(self) -> None:
        pf = parse_filter("lower(name) || ' ' || trim(addr) = 'eu london'")
        assert pf.sql == "concat(lower(name), ' ', trim(addr)) = 'eu london'"

    def test_pipe_pipe_preserves_string_literal(self) -> None:
        # `||` inside a string literal must NOT be rewritten.
        pf = parse_filter("note = 'a||b'")
        assert pf.sql == "note = 'a||b'"

    def test_function_name_preserved_in_string_literal(self) -> None:
        pf = parse_filter("note = 'lower(x)'")
        assert pf.sql == "note = 'lower(x)'"

    def test_uppercase_function_name_rejected(self) -> None:
        # Casing is lowercase-only — consistent with existing transform names.
        with pytest.raises(ValueError, match="Unknown filter function 'LOWER'"):
            parse_filter("LOWER(name) = 'eu'")

    def test_substring_synonym_rejected(self) -> None:
        # The canonical name is ``substr`` (SQLite spelling); ``substring``
        # is an unknown DSL function.
        with pytest.raises(ValueError, match="Unknown filter function 'substring'"):
            parse_filter("substring(s, 1, 5) = 'abcde'")
