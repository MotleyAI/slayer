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
from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.engine.enrichment import _collect_needed_paths, extract_filter_transforms


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

    def test_rank_partition_by_single_column(self) -> None:
        result = parse_formula("rank(revenue:sum, partition_by=region)")
        assert isinstance(result, TransformField)
        assert result.transform == "rank"
        assert result.kwargs == {"partition_by": ["region"]}

    def test_rank_partition_by_list(self) -> None:
        result = parse_formula("rank(revenue:sum, partition_by=[region, channel])")
        assert isinstance(result, TransformField)
        assert result.kwargs == {"partition_by": ["region", "channel"]}

    def test_rank_partition_by_dotted_path(self) -> None:
        result = parse_formula("rank(revenue:sum, partition_by=customers.region)")
        assert isinstance(result, TransformField)
        assert result.kwargs == {"partition_by": ["customers.region"]}

    def test_percent_rank_default(self) -> None:
        result = parse_formula("percent_rank(revenue:sum)")
        assert isinstance(result, TransformField)
        assert result.transform == "percent_rank"
        assert result.kwargs == {}

    def test_percent_rank_with_partition(self) -> None:
        result = parse_formula("percent_rank(revenue:sum, partition_by=region)")
        assert isinstance(result, TransformField)
        assert result.transform == "percent_rank"
        assert result.kwargs == {"partition_by": ["region"]}

    def test_dense_rank_default(self) -> None:
        result = parse_formula("dense_rank(revenue:sum)")
        assert isinstance(result, TransformField)
        assert result.transform == "dense_rank"

    def test_dense_rank_with_partition(self) -> None:
        result = parse_formula("dense_rank(revenue:sum, partition_by=region)")
        assert isinstance(result, TransformField)
        assert result.transform == "dense_rank"
        assert result.kwargs == {"partition_by": ["region"]}

    def test_ntile_with_required_n(self) -> None:
        result = parse_formula("ntile(revenue:sum, n=4)")
        assert isinstance(result, TransformField)
        assert result.transform == "ntile"
        assert result.kwargs == {"n": 4}

    def test_ntile_with_partition(self) -> None:
        result = parse_formula("ntile(revenue:sum, n=4, partition_by=cohort)")
        assert isinstance(result, TransformField)
        assert result.transform == "ntile"
        assert result.kwargs == {"n": 4, "partition_by": ["cohort"]}

    def test_ntile_missing_n_raises(self) -> None:
        with pytest.raises(ValueError, match=r"ntile.*\bn\b"):
            parse_formula("ntile(revenue:sum)")

    def test_ntile_zero_n_raises(self) -> None:
        with pytest.raises(ValueError, match="positive integer"):
            parse_formula("ntile(revenue:sum, n=0)")

    def test_ntile_negative_n_raises(self) -> None:
        with pytest.raises(ValueError, match="positive integer"):
            parse_formula("ntile(revenue:sum, n=-1)")

    def test_unknown_transform_kwarg_raises(self) -> None:
        with pytest.raises(ValueError, match="partition_by"):
            parse_formula("dense_rank(revenue:sum, foo=bar)")

    def test_rank_rejects_n_kwarg(self) -> None:
        """``n`` is only valid on ``ntile``."""
        with pytest.raises(ValueError, match="partition_by"):
            parse_formula("rank(revenue:sum, n=4)")

    def test_cumsum_rejects_partition_by(self) -> None:
        """``partition_by`` is rank-family only — ``cumsum`` partitions by query dims."""
        with pytest.raises(ValueError, match="partition_by"):
            parse_formula("cumsum(revenue:sum, partition_by=region)")

    def test_rank_rejects_extra_positional_arg(self) -> None:
        """rank-family is keyword-only after the measure; extra positionals must fail fast."""
        with pytest.raises(ValueError, match="positional arguments"):
            parse_formula("rank(revenue:sum, 2)")

    def test_ntile_rejects_extra_positional_n(self) -> None:
        """ntile(x, 4) is rejected — n must be passed by keyword (n=4)."""
        with pytest.raises(ValueError, match="positional arguments"):
            parse_formula("ntile(revenue:sum, 4)")

    def test_consecutive_periods_predicate(self) -> None:
        result = parse_formula("consecutive_periods(revenue:sum > 0)")
        assert isinstance(result, TransformField)
        assert result.transform == "consecutive_periods"
        assert isinstance(result.inner, ArithmeticField)
        assert ">" in result.inner.sql

    def test_consecutive_periods_comparison(self) -> None:
        result = parse_formula("consecutive_periods(revenue:sum > 0) >= 3")
        assert isinstance(result, MixedArithmeticField)
        assert len(result.sub_transforms) == 1
        placeholder, transform = result.sub_transforms[0]
        assert placeholder in result.sql
        assert transform.transform == "consecutive_periods"

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


class TestNamedMeasureExpansion:
    """Bare-name resolution against ``SlayerModel.measures`` saved formulas.

    The S3 milestone introduced ``ModelMeasure`` (a named saved formula). Queries
    can reference these by bare name in their own measure formulas — e.g., a model
    with ``measures=[ModelMeasure(name="aov", formula="revenue:sum / *:count")]``
    can be queried with ``measures=[{"formula": "aov"}]`` or
    ``measures=[{"formula": "cumsum(aov)"}]``. Expansion happens at parse time
    via the ``named_measures`` argument.
    """

    def test_root_aggregated(self) -> None:
        result = parse_formula("rev", named_measures={"rev": "revenue:sum"})
        assert isinstance(result, AggregatedMeasureRef)
        assert result.measure_name == "revenue"
        assert result.aggregation_name == "sum"

    def test_root_arithmetic(self) -> None:
        result = parse_formula(
            "aov", named_measures={"aov": "revenue:sum / *:count"}
        )
        assert isinstance(result, ArithmeticField)
        agg_measure_names = sorted(r.measure_name for r in result.agg_refs.values())
        assert agg_measure_names == ["*", "revenue"]

    def test_inside_transform(self) -> None:
        result = parse_formula(
            "cumsum(aov)", named_measures={"aov": "revenue:sum"}
        )
        assert isinstance(result, TransformField)
        assert result.transform == "cumsum"
        assert isinstance(result.inner, AggregatedMeasureRef)
        assert result.inner.measure_name == "revenue"

    def test_inside_transform_arithmetic_inner(self) -> None:
        """cumsum(aov) where aov expands to arithmetic → TransformField wrapping ArithmeticField."""
        result = parse_formula(
            "cumsum(aov)", named_measures={"aov": "revenue:sum / *:count"}
        )
        assert isinstance(result, TransformField)
        assert result.transform == "cumsum"
        assert isinstance(result.inner, ArithmeticField)

    def test_in_arithmetic(self) -> None:
        """``aov + tax`` where both are saved measures."""
        result = parse_formula(
            "aov + tax",
            named_measures={"aov": "revenue:sum", "tax": "tax_amount:sum"},
        )
        assert isinstance(result, ArithmeticField)
        agg_measure_names = sorted(r.measure_name for r in result.agg_refs.values())
        assert agg_measure_names == ["revenue", "tax_amount"]

    def test_arithmetic_with_constant(self) -> None:
        result = parse_formula(
            "aov * 1.1", named_measures={"aov": "revenue:sum"}
        )
        assert isinstance(result, ArithmeticField)

    def test_chained_expansion(self) -> None:
        """``a → b → revenue:sum`` is fully expanded."""
        result = parse_formula(
            "a", named_measures={"a": "b", "b": "revenue:sum"}
        )
        assert isinstance(result, AggregatedMeasureRef)
        assert result.measure_name == "revenue"

    def test_cycle_raises(self) -> None:
        """``a → b → a`` raises with the chain in the error message."""
        with pytest.raises(ValueError, match="cyclic"):
            parse_formula("a", named_measures={"a": "b", "b": "a"})

    def test_self_reference_raises(self) -> None:
        with pytest.raises(ValueError, match="cyclic"):
            parse_formula("a", named_measures={"a": "a"})

    def test_not_substituted_in_colon_syntax(self) -> None:
        """``revenue:sum`` parses as the column-aggregation form even if a saved
        measure called ``revenue`` exists. Real models prevent this name
        collision via the column/measure disjointness validator, but the
        expander itself must not over-substitute.
        """
        result = parse_formula(
            "revenue:sum", named_measures={"revenue": "*:count"}
        )
        assert isinstance(result, AggregatedMeasureRef)
        assert result.measure_name == "revenue"
        assert result.aggregation_name == "sum"

    def test_not_substituted_after_dot(self) -> None:
        """``customers.aov`` is a cross-model reference and is NOT expanded."""
        with pytest.raises(ValueError, match="Cross-model measure"):
            parse_formula(
                "customers.aov", named_measures={"aov": "revenue:sum"}
            )

    def test_not_substituted_when_called(self) -> None:
        """A saved measure shadowing a transform name is not expanded as an identifier
        when followed by ``(``. The ``cumsum(...)`` token sequence is still parsed
        as the transform.
        """
        result = parse_formula(
            "cumsum(rev)",
            named_measures={"cumsum": "*:count", "rev": "revenue:sum"},
        )
        assert isinstance(result, TransformField)
        assert result.transform == "cumsum"
        assert isinstance(result.inner, AggregatedMeasureRef)
        assert result.inner.measure_name == "revenue"

    def test_unknown_bare_name_still_raises(self) -> None:
        with pytest.raises(ValueError, match="Bare measure name"):
            parse_formula(
                "unknown_thing", named_measures={"aov": "revenue:sum"}
            )

    def test_no_named_measures_preserves_old_behavior(self) -> None:
        """Calling ``parse_formula`` without ``named_measures`` keeps the
        existing bare-name rejection — no regression for callers that don't
        opt in.
        """
        with pytest.raises(ValueError, match="Bare measure name"):
            parse_formula("aov")


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

    def test_rank_family_kwargs_preserved(self) -> None:
        """``dense_rank(revenue:sum, partition_by=region) <= 5`` round-trips
        through filter extraction without losing partition_by — re-parsing the
        extracted formula yields a TransformField with the original kwargs.

        Regression coverage for DEV-1353: a silent drop here would rank globally
        instead of within the partition.
        """
        _, transforms = extract_filter_transforms(
            "dense_rank(revenue:sum, partition_by=region) <= 5"
        )
        assert len(transforms) == 1
        _name, formula = transforms[0]
        assert "partition_by=region" in formula or "partition_by = region" in formula

        reparsed = parse_formula(formula)
        assert isinstance(reparsed, TransformField)
        assert reparsed.transform == "dense_rank"
        assert reparsed.kwargs == {"partition_by": ["region"]}

    def test_ntile_kwargs_preserved(self) -> None:
        """ntile(x, n=4, partition_by=cohort) preserves both kwargs through the
        filter-extraction round-trip. n is required, so a silent drop would
        cause the extracted formula to fail re-parsing.
        """
        _, transforms = extract_filter_transforms(
            "ntile(revenue:sum, n=4, partition_by=cohort) <= 1"
        )
        assert len(transforms) == 1
        _name, formula = transforms[0]
        reparsed = parse_formula(formula)
        assert isinstance(reparsed, TransformField)
        assert reparsed.transform == "ntile"
        assert reparsed.kwargs == {"n": 4, "partition_by": ["cohort"]}

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

    def test_windowed_sum_parses(self) -> None:
        result = parse_formula("revenue:sum(window='1y2m3w5d6h7min8s')")
        assert isinstance(result, AggregatedMeasureRef)
        assert result.measure_name == "revenue"
        assert result.aggregation_name == "sum"
        assert result.agg_kwargs == {"window": "'1y2m3w5d6h7min8s'"}

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


class TestCollectNeededPathsExtraAggNames:
    """Verify _collect_needed_paths forwards extra_agg_names to parse_filter."""

    def test_funcstyle_custom_agg_in_filter_does_not_error(self) -> None:
        """A filter with a function-style custom agg should parse without error
        when extra_agg_names is provided."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            columns=[
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
        )
        # Filter uses a custom aggregation in function style: custom_total(revenue) > 100
        # Without extra_agg_names, parse_filter won't rewrite it → potential misparse
        paths = _collect_needed_paths(
            model=model,
            dimensions=[],
            time_dimensions=[],
            measures=[],
            cross_model_measures=[],
            processed_filters=["custom_total(revenue) > 100"],
            extra_agg_names=frozenset({"custom_total"}),
        )
        # No cross-model references → empty paths
        assert paths == set()

    def test_funcstyle_custom_agg_with_dotted_column(self) -> None:
        """A filter with a custom agg on a cross-model column should extract join paths."""
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            columns=[
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            ],
        )
        paths = _collect_needed_paths(
            model=model,
            dimensions=[],
            time_dimensions=[],
            measures=[],
            cross_model_measures=[],
            processed_filters=["customers.total:custom_total > 100"],
            extra_agg_names=frozenset({"custom_total"}),
        )
        # Should detect the "customers" join path from the dotted column reference
        assert ("customers",) in paths


class TestPlaceholderLeakRegression:
    """DEV-1341: aggregated refs nested inside non-transform SQL function calls
    (``nullif``, ``coalesce``, etc.) must be registered in the resulting
    ``MixedArithmeticField`` so the ``__aggN__`` placeholders cannot leak through
    to emitted SQL.
    """

    def test_colon_count_over_nullif_max(self) -> None:
        """``*:count / nullif(temperature_c:max, 0)`` registers both refs."""
        result = parse_formula("*:count / nullif(temperature_c:max, 0)")
        assert isinstance(result, MixedArithmeticField)
        # Both placeholders must be tracked by the field
        assert "__agg0__" in result.measure_names
        assert "__agg1__" in result.measure_names
        assert "__agg0__" in result.agg_refs
        assert "__agg1__" in result.agg_refs
        # Verify the refs themselves are correct (order is preprocessing order)
        ref0 = result.agg_refs["__agg0__"]
        ref1 = result.agg_refs["__agg1__"]
        assert (ref0.measure_name, ref0.aggregation_name) == ("*", "count")
        assert (ref1.measure_name, ref1.aggregation_name) == ("temperature_c", "max")
        # The serialized SQL still carries both placeholders for resolution
        assert "__agg0__" in result.sql
        assert "__agg1__" in result.sql

    def test_funcstyle_count_over_nullif_max(self) -> None:
        """Issue's verbatim function-style formula round-trips through the
        function-style rewrite to colon syntax and registers both refs.
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = parse_formula("count(*) / nullif(max(temperature_c), 0)")
        assert isinstance(result, MixedArithmeticField)
        assert "__agg0__" in result.measure_names
        assert "__agg1__" in result.measure_names
        assert "__agg0__" in result.agg_refs
        assert "__agg1__" in result.agg_refs
        names = {(r.measure_name, r.aggregation_name) for r in result.agg_refs.values()}
        assert ("*", "count") in names
        assert ("temperature_c", "max") in names

    def test_coalesce_wraps_aggregation(self) -> None:
        """``coalesce(revenue:sum, 0) + amount:avg`` — coalesce-wrapped ref + outside ref."""
        result = parse_formula("coalesce(revenue:sum, 0) + amount:avg")
        assert isinstance(result, MixedArithmeticField)
        assert "__agg0__" in result.measure_names
        assert "__agg1__" in result.measure_names
        assert "__agg0__" in result.agg_refs
        assert "__agg1__" in result.agg_refs
        names = {(r.measure_name, r.aggregation_name) for r in result.agg_refs.values()}
        assert ("revenue", "sum") in names
        assert ("amount", "avg") in names

    def test_predicate_with_nullif_wrapper(self) -> None:
        """``nullif(*:count, 0) > 5`` is a predicate; the wrapped ref must be tracked."""
        result = parse_formula("nullif(*:count, 0) > 5")
        assert isinstance(result, MixedArithmeticField)
        assert result.is_predicate is True
        assert "__agg0__" in result.measure_names
        assert "__agg0__" in result.agg_refs
        ref0 = result.agg_refs["__agg0__"]
        assert (ref0.measure_name, ref0.aggregation_name) == ("*", "count")

    def test_nested_non_transform_calls(self) -> None:
        """``coalesce(nullif(*:count, 0), 1) / temperature_c:max`` — two nesting layers."""
        result = parse_formula("coalesce(nullif(*:count, 0), 1) / temperature_c:max")
        assert isinstance(result, MixedArithmeticField)
        assert "__agg0__" in result.measure_names
        assert "__agg1__" in result.measure_names
        assert "__agg0__" in result.agg_refs
        assert "__agg1__" in result.agg_refs
        names = {(r.measure_name, r.aggregation_name) for r in result.agg_refs.values()}
        assert ("*", "count") in names
        assert ("temperature_c", "max") in names
