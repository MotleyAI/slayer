"""DEV-1576 — parse-level coverage for the three SlayerQuery heals.

1. Aggregation-name alias / casing normalization (``normalize_aggregation_name``
   + colon-syntax healing in ``parse_formula`` / ``parse_filter``).
2. ``round()`` / ``abs()`` as top-level formula functions (parse into a
   ``MixedArithmeticField`` passthrough; arity validation).

The §3 error-message split and end-to-end execution live in
``tests/test_aggregation_gating.py`` and the integration suites respectively.
"""

import pytest

from slayer.core.enums import (
    AGGREGATION_ALIASES,
    BUILTIN_AGGREGATIONS,
    normalize_aggregation_name,
)
from slayer.core.formula import (
    AggregatedMeasureRef,
    MixedArithmeticField,
    parse_filter,
    parse_formula,
)


# ---------------------------------------------------------------------------
# §1 — normalize_aggregation_name
# ---------------------------------------------------------------------------


class TestNormalizeAggregationName:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("countd", "count_distinct"),
            ("countdistinct", "count_distinct"),
            ("countDistinct", "count_distinct"),
            ("COUNTD", "count_distinct"),
            ("stddev", "stddev_samp"),
            ("STDDEV", "stddev_samp"),
            ("var", "var_samp"),
            ("variance", "var_samp"),
            ("VARIANCE", "var_samp"),
        ],
    )
    def test_known_aliases_heal(self, raw: str, expected: str) -> None:
        assert normalize_aggregation_name(raw) == expected

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("SUM", "sum"),
            ("Count", "count"),
            ("Count_Distinct", "count_distinct"),
            ("WEIGHTED_AVG", "weighted_avg"),
        ],
    )
    def test_casing_of_canonical_names_heals(self, raw: str, expected: str) -> None:
        assert normalize_aggregation_name(raw) == expected

    def test_canonical_names_pass_through_unchanged(self) -> None:
        for agg in BUILTIN_AGGREGATIONS:
            assert normalize_aggregation_name(agg) == agg

    @pytest.mark.parametrize("raw", ["bogus", "stdev", "sum_over", "count_if", "group_concat"])
    def test_unknown_names_returned_unchanged(self, raw: str) -> None:
        # An unknown token must be returned verbatim so the §3 enrichment
        # error still fires (and so a genuinely unknown name is never
        # silently swallowed).
        assert normalize_aggregation_name(raw) == raw

    def test_custom_agg_name_preserved_with_original_casing(self) -> None:
        # A custom aggregation name (not a builtin / not an alias) keeps its
        # exact spelling — normalization never lowercases names it cannot
        # resolve to the builtin vocabulary.
        assert normalize_aggregation_name("myCustomAgg") == "myCustomAgg"

    def test_unknown_uppercase_name_preserved(self) -> None:
        # An unresolvable name keeps its original casing (no silent lowercasing
        # of names that don't map to the vocabulary).
        assert normalize_aggregation_name("BOGUS") == "BOGUS"

    def test_alias_table_only_targets_builtins(self) -> None:
        # Every alias must resolve to a real builtin aggregation.
        for target in AGGREGATION_ALIASES.values():
            assert target in BUILTIN_AGGREGATIONS


# ---------------------------------------------------------------------------
# §1 — colon-syntax healing through parse_formula / parse_filter
# ---------------------------------------------------------------------------


class TestColonSyntaxAliasHealing:
    @pytest.mark.parametrize(
        "raw_agg,expected",
        [
            ("countd", "count_distinct"),
            ("countDistinct", "count_distinct"),
            ("countdistinct", "count_distinct"),
            ("stddev", "stddev_samp"),
            ("var", "var_samp"),
            ("variance", "var_samp"),
            ("SUM", "sum"),
        ],
    )
    def test_formula_colon_alias_heals(self, raw_agg: str, expected: str) -> None:
        result = parse_formula(f"revenue:{raw_agg}")
        assert isinstance(result, AggregatedMeasureRef)
        assert result.aggregation_name == expected
        assert result.measure_name == "revenue"

    def test_star_count_alias_unaffected(self) -> None:
        result = parse_formula("*:count")
        assert isinstance(result, AggregatedMeasureRef)
        assert result.aggregation_name == "count"

    def test_unknown_agg_in_formula_left_for_enrichment(self) -> None:
        # parse_formula does not validate the vocabulary — an unknown agg
        # parses through (heal leaves it unchanged); enrichment raises later.
        result = parse_formula("revenue:bogus")
        assert isinstance(result, AggregatedMeasureRef)
        assert result.aggregation_name == "bogus"

    def test_filter_colon_alias_heals(self) -> None:
        pf = parse_filter("revenue:countd > 5")
        assert any(ref.aggregation_name == "count_distinct" for ref in pf.agg_refs)
        # The canonical alias used downstream reflects the healed name.
        assert any("count_distinct" in a for a in pf.synthesized_aliases)

    def test_filter_stddev_alias_heals(self) -> None:
        pf = parse_filter("amount:stddev > 1")
        assert any(ref.aggregation_name == "stddev_samp" for ref in pf.agg_refs)

    def test_custom_agg_named_like_alias_not_healed(self) -> None:
        # A model custom aggregation named like an alias key takes precedence —
        # an exact custom-name match is NOT rewritten to the builtin.
        result = parse_formula("revenue:countd", extra_agg_names=frozenset({"countd"}))
        assert isinstance(result, AggregatedMeasureRef)
        assert result.aggregation_name == "countd"

    def test_alias_still_heals_when_custom_name_differs(self) -> None:
        result = parse_formula(
            "revenue:countd", extra_agg_names=frozenset({"some_other_agg"})
        )
        assert result.aggregation_name == "count_distinct"

    def test_custom_agg_named_like_alias_not_healed_in_filter(self) -> None:
        pf = parse_filter("revenue:countd > 5", extra_agg_names=frozenset({"countd"}))
        assert any(ref.aggregation_name == "countd" for ref in pf.agg_refs)


# ---------------------------------------------------------------------------
# §2 — round() / abs() as top-level formula functions
# ---------------------------------------------------------------------------


def _agg_names(field: MixedArithmeticField) -> set[str]:
    return {ref.aggregation_name for ref in field.agg_refs.values()}


class TestScalarFunctionsParse:
    def test_round_two_args(self) -> None:
        result = parse_formula("round(revenue:sum, 2)")
        assert isinstance(result, MixedArithmeticField)
        assert "round" in result.sql.lower()
        assert "2" in result.sql
        assert _agg_names(result) == {"sum"}
        assert result.sub_transforms == []

    def test_round_one_arg(self) -> None:
        result = parse_formula("round(revenue:sum)")
        assert isinstance(result, MixedArithmeticField)
        assert "round" in result.sql.lower()
        assert _agg_names(result) == {"sum"}

    def test_abs_one_arg(self) -> None:
        result = parse_formula("abs(revenue:sum)")
        assert isinstance(result, MixedArithmeticField)
        assert "abs" in result.sql.lower()
        assert _agg_names(result) == {"sum"}

    def test_abs_over_arithmetic(self) -> None:
        result = parse_formula("abs(revenue:sum - cost:sum)")
        assert isinstance(result, MixedArithmeticField)
        assert "abs" in result.sql.lower()
        assert _agg_names(result) == {"sum"}

    def test_round_over_arithmetic(self) -> None:
        result = parse_formula("round(revenue:sum / *:count, 2)")
        assert isinstance(result, MixedArithmeticField)
        assert "round" in result.sql.lower()
        assert _agg_names(result) == {"sum", "count"}

    def test_round_negative_ndigits(self) -> None:
        # round(x, -2) is valid SQL (round to hundreds); negative integer
        # literals must be accepted.
        result = parse_formula("round(revenue:sum, -2)")
        assert isinstance(result, MixedArithmeticField)
        assert "round" in result.sql.lower()

    def test_round_wraps_nested_transform(self) -> None:
        # A transform nested inside round is still extracted as a sub-transform.
        result = parse_formula("round(cumsum(revenue:sum), 2)")
        assert isinstance(result, MixedArithmeticField)
        assert result.sub_transforms, "nested cumsum should become a sub-transform"
        assert result.sub_transforms[0][1].transform == "cumsum"

    @pytest.mark.parametrize(
        "raw",
        [
            "ROUND(revenue:sum, 2)",
            "Round(revenue:sum, 2)",
            "ABS(revenue:sum)",
            "Abs(revenue:sum)",
        ],
    )
    def test_scalar_functions_case_insensitive(self, raw: str) -> None:
        result = parse_formula(raw)
        assert isinstance(result, MixedArithmeticField)
        assert _agg_names(result) == {"sum"}


class TestScalarFunctionArity:
    def test_round_no_args_raises(self) -> None:
        with pytest.raises(ValueError, match="round"):
            parse_formula("round()")

    def test_round_three_args_raises(self) -> None:
        with pytest.raises(ValueError, match="round"):
            parse_formula("round(revenue:sum, 2, 3)")

    def test_round_non_integer_ndigits_raises(self) -> None:
        with pytest.raises(ValueError, match="round"):
            parse_formula("round(revenue:sum, 2.5)")

    def test_round_non_literal_ndigits_raises(self) -> None:
        # ndigits must be a literal, not another measure / expression.
        with pytest.raises(ValueError, match="round"):
            parse_formula("round(revenue:sum, x)")

    def test_round_kwarg_raises(self) -> None:
        with pytest.raises(ValueError, match="round"):
            parse_formula("round(revenue:sum, ndigits=2)")

    def test_abs_two_args_raises(self) -> None:
        with pytest.raises(ValueError, match="abs"):
            parse_formula("abs(revenue:sum, cost:sum)")

    def test_abs_no_args_raises(self) -> None:
        with pytest.raises(ValueError, match="abs"):
            parse_formula("abs()")

    def test_abs_kwarg_raises(self) -> None:
        with pytest.raises(ValueError, match="abs"):
            parse_formula("abs(revenue:sum, foo=1)")


class TestScalarAllowlistIsExclusive:
    @pytest.mark.parametrize("fn", ["ceil", "floor", "sqrt", "ln", "nullif", "coalesce"])
    def test_other_functions_still_reject_at_top_level(self, fn: str) -> None:
        # Only round/abs are promoted to top-level scalar functions. Every
        # other function call at the top level keeps raising "Unknown
        # transform" (the inside-arithmetic passthrough is unchanged and
        # tested separately).
        with pytest.raises(ValueError, match="Unknown transform"):
            parse_formula(f"{fn}(revenue:sum)")

    def test_inside_arithmetic_passthrough_unchanged(self) -> None:
        # Pre-existing behaviour: a non-transform call inside arithmetic
        # passes through and registers the inner agg ref.
        result = parse_formula("*:count / nullif(revenue:max, 0)")
        assert isinstance(result, MixedArithmeticField)
        assert "nullif" in result.sql.lower()

    @pytest.mark.parametrize("raw", ["countd(revenue)", "countDistinct(revenue)"])
    def test_funcstyle_aggregation_alias_out_of_scope(self, raw: str) -> None:
        # §1 heals colon syntax only. Function-style alias calls are NOT
        # rewritten — they fall through to the parser as unknown calls.
        with pytest.raises(ValueError, match="Unknown transform"):
            parse_formula(raw)
