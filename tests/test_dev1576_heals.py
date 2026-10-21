"""DEV-1576 — parse-level coverage for the three SlayerQuery heals.

1. Aggregation-name alias / casing normalization (``normalize_aggregation_name``;
   colon-syntax healing happens at binding).
2. ``round()`` / ``abs()`` as top-level formula functions (scalar calls over
   aggregations; arity validated at binding).

The §3 error-message split and end-to-end execution live in
``tests/test_aggregation_gating.py`` and the integration suites respectively.
"""

import pytest

from slayer.core.enums import (
    AGGREGATION_ALIASES,
    BUILTIN_AGGREGATIONS,
    normalize_aggregation_name,
)
from slayer.core.enums import DataType
from slayer.core.errors import UnknownFunctionError
from slayer.core.keys import AggregateKey, ValueKey
from slayer.core.models import Aggregation, Column, SlayerModel
from slayer.core.scope import ModelScope
from slayer.engine.binding import bind_expr
from slayer.core.syntax import AggCall, Literal, Ref, ScalarCall, TransformCall, parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle


def _bind(text: str, *, custom: tuple[str, ...] = ()) -> ValueKey:
    model = SlayerModel(
        name="orders", sql_table="orders", data_source="t",
        columns=[Column(name=n, type=DataType.DOUBLE) for n in ("revenue", "cost", "other", "x")],
        aggregations=[Aggregation(name=n, formula="COUNT(DISTINCT {value})") for n in custom],
    )
    bundle = ResolvedSourceBundle(dialect="postgres", source_model=model, referenced_models=[])
    return bind_expr(parse_expr(text), scope=ModelScope(source_model=model), bundle=bundle,
                     allow_measures=True).value_key


def _nodes(node):
    yield node
    for value in getattr(node, "__dict__", {}).values():
        for item in value if isinstance(value, tuple) else (value,):
            if hasattr(item, "__dict__"):
                yield from _nodes(item)
            elif isinstance(item, tuple):
                for sub in item:
                    if hasattr(sub, "__dict__"):
                        yield from _nodes(sub)


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
# §1 — colon-syntax healing at binding
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
        key = _bind(f"revenue:{raw_agg}")
        assert isinstance(key, AggregateKey)
        assert key.agg == expected
        assert getattr(key.source, "leaf", None) == "revenue"

    def test_star_count_alias_unaffected(self) -> None:
        key = _bind("*:count")
        assert isinstance(key, AggregateKey)
        assert key.agg == "count"

    def test_unknown_agg_in_formula_left_for_enrichment(self) -> None:
        # The parser does not validate the vocabulary — an unknown agg parses through.
        assert parse_expr("revenue:bogus") == AggCall(source=Ref(name="revenue"), agg="bogus")

    def test_custom_agg_named_like_alias_not_healed(self) -> None:
        # An exact custom-name match takes precedence over the alias.
        key = _bind("revenue:countd", custom=("countd",))
        assert isinstance(key, AggregateKey) and key.agg == "countd"

    def test_alias_still_heals_when_custom_name_differs(self) -> None:
        key = _bind("revenue:countd", custom=("some_other_agg",))
        assert isinstance(key, AggregateKey) and key.agg == "count_distinct"


# ---------------------------------------------------------------------------
# §2 — round() / abs() as top-level formula functions
# ---------------------------------------------------------------------------


def _agg_names(parsed) -> set[str]:
    return {n.agg for n in _nodes(parsed) if isinstance(n, AggCall)}


def _scalar(parsed, name: str) -> ScalarCall:
    assert isinstance(parsed, ScalarCall) and parsed.name.lower() == name, parsed
    return parsed


class TestScalarFunctionsParse:
    def test_round_two_args(self) -> None:
        result = _scalar(parse_expr("round(revenue:sum, 2)"), "round")
        assert result.args[1] == Literal(value=2)
        assert _agg_names(result) == {"sum"}
        assert not any(isinstance(n, TransformCall) for n in _nodes(result))

    def test_round_one_arg(self) -> None:
        assert _agg_names(_scalar(parse_expr("round(revenue:sum)"), "round")) == {"sum"}

    def test_abs_one_arg(self) -> None:
        assert _agg_names(_scalar(parse_expr("abs(revenue:sum)"), "abs")) == {"sum"}

    def test_abs_over_arithmetic(self) -> None:
        assert _agg_names(_scalar(parse_expr("abs(revenue:sum - cost:sum)"), "abs")) == {"sum"}

    def test_round_over_arithmetic(self) -> None:
        assert _agg_names(_scalar(parse_expr("round(revenue:sum / *:count, 2)"), "round")) == {"sum", "count"}

    def test_round_negative_ndigits(self) -> None:
        # round(x, -2) is valid SQL (round to hundreds); negative integer literals are accepted.
        _scalar(parse_expr("round(revenue:sum, -2)"), "round")
        _bind("round(revenue:sum, -2)")

    def test_round_wraps_nested_transform(self) -> None:
        result = _scalar(parse_expr("round(cumsum(revenue:sum), 2)"), "round")
        transforms = [n for n in _nodes(result) if isinstance(n, TransformCall)]
        assert [t.op for t in transforms] == ["cumsum"]

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
        assert parse_expr(raw) == parse_expr(raw.split("(", 1)[0].lower() + "(" + raw.split("(", 1)[1])
        assert _agg_names(parse_expr(raw)) == {"sum"}


class TestScalarFunctionArity:
    @pytest.mark.parametrize(("formula", "fn"), [
        ("round()", "round"),
        ("round(revenue:sum, 2, 3)", "round"),
        ("round(revenue:sum, ndigits=2)", "round"),
        ("abs(revenue:sum, cost:sum)", "abs"),
        ("abs()", "abs"),
        ("abs(revenue:sum, foo=1)", "abs"),
        ("round(revenue:sum, 2.5)", "round"),
        ("round(revenue:sum, x)", "round"),
    ])
    def test_arity_rejected(self, formula: str, fn: str) -> None:
        with pytest.raises(ValueError, match=fn):
            _bind(formula)


class TestScalarPassthroughPolicy:
    """Mode B has one canonical scalar-function allowlist (SCALAR_PASSTHROUGH)
    applied uniformly at top-level and inside arithmetic."""

    @pytest.mark.parametrize("fn", ["ceil", "floor", "sqrt", "ln", "nullif", "coalesce"])
    def test_common_scalars_accepted_at_top_level(self, fn: str) -> None:
        _scalar(parse_expr(f"{fn}(revenue:sum)"), fn)

    def test_inside_arithmetic_passthrough_still_works(self) -> None:
        parsed = parse_expr("*:count / nullif(revenue:max, 0)")
        assert any(isinstance(n, ScalarCall) and n.name == "nullif" for n in _nodes(parsed))
        _bind("*:count / nullif(revenue:max, 0)")

    def test_unknown_call_inside_arithmetic_now_rejected(self) -> None:
        with pytest.raises(UnknownFunctionError, match="Unknown function"):
            _bind("revenue:sum + bogus_fn(other:sum)")

    @pytest.mark.parametrize("raw", ["countd(revenue)", "countDistinct(revenue)"])
    def test_funcstyle_aggregation_alias_heals(self, raw: str) -> None:
        key = _bind(raw)
        assert isinstance(key, AggregateKey) and key.agg == "count_distinct"
