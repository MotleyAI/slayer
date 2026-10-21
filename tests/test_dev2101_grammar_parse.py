"""Parse-level tests for the Mode-B grammar (spec: queries/expression-grammar, queries/casts, aggregations/functional-form)."""

from __future__ import annotations

import importlib.util
import re
from decimal import Decimal

import pytest

from slayer.core import syntax
from slayer.core.errors import IllegalWindowInFilterError
from slayer.core.syntax import AggCall, Cast, DottedRef, Literal, Ref, ScalarCall, TransformCall, parse_expr


def same(a: str, b: str) -> None:
    assert parse_expr(a) == parse_expr(b), (a, b)


def rejected(text: str, *needles: str, exc: type[Exception] = ValueError) -> str:
    with pytest.raises(exc) as info:
        parse_expr(text)
    msg = str(info.value)
    for needle in needles:
        assert needle.lower() in msg.lower(), msg
    return msg


class TestOneParser:
    def test_parser_lives_in_core(self) -> None:
        assert importlib.util.find_spec("slayer.engine.syntax") is None

    def test_no_filter_only_entry_point(self) -> None:
        assert not hasattr(syntax, "parse_filter_expr")

    def test_parse_is_cached(self) -> None:
        assert hasattr(parse_expr, "cache_info")
        assert parse_expr("sum(amount) + 1") is parse_expr("sum(amount) + 1")

    @pytest.mark.parametrize("helper", [
        "_rewrite_case_when", "_rewrite_sql_like", "_preprocess_colons", "_normalize_sql_filter_operators",
        "_rewrite_extract", "_rewrite_count_distinct", "_preprocess_star_args", "_rewrite_comparison_equals",
    ])
    def test_text_pre_passes_are_gone(self, helper: str) -> None:
        assert not hasattr(syntax, helper)


class TestSqlSpellings:
    @pytest.mark.parametrize(("sql", "legacy"), [
        ("status = 'paid'", "status == 'paid'"),
        ("status <> 'paid'", "status != 'paid'"),
        ("amount IS NULL", "amount is None"),
        ("amount IS NOT NULL", "amount is not None"),
        ("amount is not null", "amount is not None"),
        ("a = 1 AND b = 2 OR NOT c = 3", "a == 1 and b == 2 or not c == 3"),
        ("region IN ('EU', 'US')", "region in ['EU', 'US']"),
        ("region NOT IN ('EU')", "region not in ['EU']"),
        ("amount BETWEEN 10 AND 20", "amount >= 10 and amount <= 20"),
        ("amount NOT BETWEEN 10 AND 20", "not (amount >= 10 and amount <= 20)"),
        ("amount between 1 + 1 and 3 * 2", "amount >= 1 + 1 and amount <= 3 * 2"),
        ("count(distinct region)", "count_distinct(region)"),
        ("COUNT(DISTINCT region)", "count_distinct(region)"),
        ("EXTRACT(month FROM created_at)", "date_part('month', created_at)"),
        ("POSITION('a' IN status)", "instr(status, 'a')"),
        ("SUBSTRING(status FROM 1 FOR 3)", "substr(status, 1, 3)"),
        ("SUBSTRING(status FROM 2)", "substr(status, 2)"),
        ("CURRENT_DATE", "current_date()"),
        ("CURRENT_TIMESTAMP", "now()"),
        ("status LIKE 'pa%'", "like(status, 'pa%')"),
        ("status NOT LIKE 'pa%'", "not like(status, 'pa%')"),
        ("lower(trim(status)) LIKE 'pa%'", "like(lower(trim(status)), 'pa%')"),
        ("amount ** 2", "pow(amount, 2)"),
    ])
    def test_spelling_collapses_to_one_node(self, sql: str, legacy: str) -> None:
        same(sql, legacy)

    @pytest.mark.parametrize("text", [
        "status ILIKE 'PA%'", "status NOT ILIKE 'pa%'", "status || '-' || region",
        "region IS DISTINCT FROM 'EU'", "region IS NOT DISTINCT FROM NULL",
        "(amount > 15) IS TRUE", "(amount > 15) IS NOT TRUE", "(amount > 15) IS FALSE", "(amount > 15) IS NOT FALSE",
        "-7 % 3", "DATE '2024-01-01'", "TIMESTAMP '2024-01-01 00:00'",
    ])
    def test_sql_form_parses(self, text: str) -> None:
        parse_expr(text)

    def test_like_inside_string_is_text(self) -> None:
        assert parse_expr("note = 'we like cats'") == parse_expr("note == 'we like cats'")

    def test_ilike_is_not_like(self) -> None:
        assert parse_expr("status ILIKE 'pa%'") != parse_expr("status LIKE 'pa%'")


class TestCase:
    @pytest.mark.parametrize(("case", "iif"), [
        ("CASE WHEN amount > 100 THEN 'big' ELSE 'small' END", "iif(amount > 100, 'big', 'small')"),
        ("CASE WHEN amount > 100 THEN 'big' END", "iif(amount > 100, 'big', None)"),
        ("CASE status WHEN 'a' THEN 1 WHEN 'b' THEN 2 END", "iif(status == 'a', 1, iif(status == 'b', 2, None))"),
        ("CASE WHEN region = 'EU' AND amount IS NOT NULL THEN 1 ELSE 0 END",
         "iif(region == 'EU' and amount is not None, 1, 0)"),
        ("case when a > 1 then case when b > 1 then 1 else 2 end else 3 end", "iif(a > 1, iif(b > 1, 1, 2), 3)"),
        ("sum(CASE WHEN status = 'paid' THEN amount ELSE 0 END)", "sum(iif(status == 'paid', amount, 0))"),
    ])
    def test_case_lowers_to_iif(self, case: str, iif: str) -> None:
        same(case, iif)

    @pytest.mark.parametrize(("text", "missing"), [
        ("CASE WHEN a THEN 1", "END"),
        ("CASE WHEN a 1 END", "THEN"),
    ])
    def test_malformed_case_names_the_keyword(self, text: str, missing: str) -> None:
        msg = rejected(text, "CASE")
        assert missing in msg, msg


class TestKeywordsOnlyInTheirSlot:
    @pytest.mark.parametrize("name", ["case", "end", "with", "model", "date", "filter", "timestamp", "between"])
    def test_keyword_word_is_an_identifier(self, name: str) -> None:
        assert parse_expr(name) == Ref(name=name)
        assert parse_expr(f"sum({name})") == AggCall(source=Ref(name=name), agg="sum")

    def test_case_is_conditional_only_before_when(self) -> None:
        assert parse_expr("case + 1").left == Ref(name="case")
        assert isinstance(parse_expr("CASE WHEN a THEN 1 END"), ScalarCall)

    def test_date_literal_vs_date_column(self) -> None:
        assert parse_expr("date + 1") != parse_expr("DATE '2024-01-01'")
        assert parse_expr("DATE '2024-01-01'") != parse_expr("'2024-01-01'")
        same("date '2024-01-01'", "DATE '2024-01-01'")

    @pytest.mark.parametrize("text", ["amount \u0131s NULL", "a \u0430nd b"])
    def test_non_ascii_keyword_spoof_is_not_a_keyword(self, text: str) -> None:
        with pytest.raises(ValueError):
            parse_expr(text)


class TestPrecedence:
    @pytest.mark.parametrize(("text", "explicit"), [
        ("NOT amount > 100 AND status = 'paid'", "(not (amount > 100)) and (status == 'paid')"),
        ("a OR b AND c", "a or (b and c)"),
        ("a = 1 OR b = 2 AND c = 3", "(a == 1) or ((b == 2) and (c == 3))"),
        ("status || 'x' = 'paidx'", "(status || 'x') = 'paidx'"),
        ("a || b + c", "a || (b + c)"),
        ("a + b * c", "a + (b * c)"),
        ("a - b - c", "(a - b) - c"),
        ("a / b % c", "(a / b) % c"),
        ("2 ** 3 ** 2", "2 ** (3 ** 2)"),
        ("-2 ** 2", "-(2 ** 2)"),
        ("2 ** -2", "2 ** (-2)"),
        ("amount::int + 1", "(amount::int) + 1"),
        ("-amount::int", "-(amount::int)"),
        ("amount::int ** 2", "(amount::int) ** 2"),
    ])
    def test_binding(self, text: str, explicit: str) -> None:
        same(text, explicit)

    def test_parenthesised_negative_base(self) -> None:
        assert parse_expr("(-2) ** 2") != parse_expr("-2 ** 2")


class TestKeywordArguments:
    def test_kwarg_in_aggregation(self) -> None:
        parsed = parse_expr("percentile(amount, p=0.9)")
        assert isinstance(parsed, AggCall)
        assert dict(parsed.kwargs) == {"p": Literal(value=Decimal("0.9"))}

    def test_kwarg_in_custom_aggregation(self) -> None:
        parsed = parse_expr("wsum(price, weight = quantity)")
        assert isinstance(parsed, AggCall)
        assert dict(parsed.kwargs) == {"weight": Ref(name="quantity")}

    def test_kwarg_in_transform(self) -> None:
        parsed = parse_expr("rank(sum(amount), direction = 'desc')")
        assert isinstance(parsed, TransformCall)
        assert dict(parsed.kwargs)["direction"] == Literal(value="desc")

    def test_comparison_inside_scalar_call(self) -> None:
        parsed = parse_expr("sum(iif(status = 'paid', amount, 0))")
        assert isinstance(parsed, AggCall) and isinstance(parsed.source, ScalarCall)
        assert parsed.source.args[0] == parse_expr("status == 'paid'")

    def test_comparison_first_argument_of_coalesce(self) -> None:
        parsed = parse_expr("coalesce(a = 1, b)")
        assert isinstance(parsed, ScalarCall)
        assert parsed.args[0] == parse_expr("a == 1")

    def test_predicate_inside_transform_argument(self) -> None:
        parsed = parse_expr("cumsum(sum(amount) = 10)")
        assert isinstance(parsed, TransformCall)
        assert parsed.input == parse_expr("sum(amount) == 10")

    def test_predicate_inside_aggregation_source(self) -> None:
        same("sum(amount = 10)", "sum(amount == 10)")


class TestColonAndCast:
    def test_colon_aggregation_kept(self) -> None:
        same("amount:sum", "sum(amount)")

    def test_double_colon_is_a_cast(self) -> None:
        parsed = parse_expr("amount::int")
        assert isinstance(parsed, Cast)
        assert parsed.source == Ref(name="amount")
        assert parsed.type_text.upper() == "INT"
        assert parsed.try_ is False

    def test_chained_casts(self) -> None:
        parsed = parse_expr("amount::int::text")
        assert isinstance(parsed, Cast) and parsed.type_text.upper() == "TEXT"
        assert isinstance(parsed.source, Cast) and parsed.source.type_text.upper() == "INT"
        assert parsed.source.source == Ref(name="amount")

    def test_cast_call_equals_double_colon(self) -> None:
        same("CAST(amount AS int)", "amount::int")
        same("cast(amount as int)", "amount::int")

    def test_parameterised_type_kept(self) -> None:
        parsed = parse_expr("CAST(amount AS DECIMAL(10,2))")
        assert isinstance(parsed, Cast)
        assert re.fullmatch(r"DECIMAL\(\s*10\s*,\s*2\s*\)", parsed.type_text.upper()), parsed.type_text

    def test_try_cast(self) -> None:
        parsed = parse_expr("TRY_CAST(code AS INT)")
        assert isinstance(parsed, Cast) and parsed.try_ is True
        assert parsed != parse_expr("CAST(code AS INT)")

    def test_cast_inside_aggregation(self) -> None:
        parsed = parse_expr("sum(CAST(amount AS DECIMAL(10,2)))")
        assert isinstance(parsed, AggCall) and isinstance(parsed.source, Cast)

    def test_colon_aggregation_over_cast_free_text(self) -> None:
        assert isinstance(parse_expr("revenue:sum"), AggCall)
        assert isinstance(parse_expr("revenue::text"), Cast)


class TestIdentifiers:
    def test_quoted_identifier(self) -> None:
        assert parse_expr('"order total"') == Ref(name="order total")

    def test_quoted_plain_name_equals_bare(self) -> None:
        same('"status"', "status")

    def test_quoted_segment_in_dotted_path(self) -> None:
        assert parse_expr('customers."Region"') == DottedRef(parts=("customers", "Region"))

    def test_doubled_quote_inside_identifier(self) -> None:
        assert parse_expr('"a""b"') == Ref(name='a"b')

    def test_quoted_identifier_as_aggregation_source(self) -> None:
        assert parse_expr('sum("order total")') == AggCall(source=Ref(name="order total"), agg="sum")

    @pytest.mark.parametrize("text", ['"__slayer_x"', "__slayer_x", 'customers."__slayer_x"'])
    def test_reserved_prefix_rejected_quoted_or_not(self, text: str) -> None:
        with pytest.raises(ValueError):
            parse_expr(text)


class TestStrings:
    @pytest.mark.parametrize(("text", "value"), [
        ("'O''Brien'", "O'Brien"),
        (r"'C:\temp'", r"C:\temp"),
        (r"'a\nb'", r"a\nb"),
        ("''", ""),
        ("'say \"hi\"'", 'say "hi"'),
        ("'OVER('", "OVER("),
    ])
    def test_single_quoted_literal(self, text: str, value: str) -> None:
        assert parse_expr(text) == Literal(value=value)

    def test_double_quotes_never_make_a_string(self) -> None:
        assert parse_expr('status = "paid"') == parse_expr("status == paid")


class TestFilterClause:
    def test_filter_attaches_to_the_aggregation(self) -> None:
        parsed = parse_expr("sum(amount) FILTER (WHERE status = 'paid')")
        assert isinstance(parsed, AggCall)
        assert parsed.filter == parse_expr("status == 'paid'")
        assert parsed.model_copy(update={"filter": None}) == parse_expr("sum(amount)")

    def test_unfiltered_aggregation_has_no_filter(self) -> None:
        parsed = parse_expr("sum(amount)")
        assert isinstance(parsed, AggCall) and parsed.filter is None

    def test_keywords_case_insensitive(self) -> None:
        same("count(*) filter (where status = 'paid')", "count(*) FILTER (WHERE status = 'paid')")

    def test_filter_with_kwargs(self) -> None:
        parsed = parse_expr("sum(amount, partition_by=region) FILTER (WHERE status = 'paid')")
        assert isinstance(parsed, AggCall) and parsed.filter is not None
        assert dict(parsed.kwargs)["partition_by"] == Ref(name="region")

    def test_filtered_aggregation_in_arithmetic(self) -> None:
        parsed = parse_expr("sum(amount) FILTER (WHERE status = 'paid') / sum(amount)")
        assert isinstance(parsed.left, AggCall) and parsed.left.filter is not None

    @pytest.mark.parametrize("text", [
        "sum(amount) FILTER (status = 'paid')",
        "amount FILTER (WHERE status = 'paid')",
        "sum(amount) FILTER (WHERE)",
    ])
    def test_malformed_filter_rejected(self, text: str) -> None:
        rejected(text, "FILTER")


class TestPlaceholders:
    @pytest.mark.parametrize("text", [
        "amount > {min}", "status = '{s}'", "note LIKE '%{q}%'", "region IN ({regions})",
        "sum(amount) * {k} / 100", "sum({k})", "round(sum(amount), {p})",
    ])
    def test_literal_slots_parse(self, text: str) -> None:
        parse_expr(text)

    @pytest.mark.parametrize("text", [
        "{f}(amount)", "customers.{c}", "{col}:sum", "t.{c} > 1", "sum(amount, {k}=1)",
    ])
    def test_non_literal_slots_rejected(self, text: str) -> None:
        rejected(text, "literal values")

    def test_error_positions_survive_masking(self) -> None:
        msg = rejected("{abcdef} > > 5")
        assert re.search(r"\b(11|12)\b", msg), msg


class TestRejectionsWithHints:
    def test_bitwise_or(self) -> None:
        rejected("min(status | region)", "|", "||")

    def test_floor_division(self) -> None:
        rejected("sum(amount) // 2", "div(", "floor(")

    def test_two_argument_scalar_max(self) -> None:
        rejected("max(amount, 10)", "greatest(amount, 10)")

    def test_two_argument_scalar_min(self) -> None:
        rejected("min(amount, 10)", "least(amount, 10)")

    @pytest.mark.parametrize("text", ["TRIM(BOTH ' ' FROM status)", "trim(leading 'x' from status)",
                                      "TRIM(TRAILING FROM status)"])
    def test_trim_keyword_forms(self, text: str) -> None:
        rejected(text, "TRIM")

    def test_time_literal(self) -> None:
        rejected("created_at > TIME '10:00'", "TIME")

    def test_similar_to(self) -> None:
        rejected("status SIMILAR TO 'p%'", "SIMILAR TO")

    @pytest.mark.parametrize("op", ["REGEXP", "RLIKE"])
    def test_regex_operators(self, op: str) -> None:
        rejected(f"status {op} '^p'", op)

    def test_raw_over(self) -> None:
        with pytest.raises(IllegalWindowInFilterError):
            parse_expr("sum(amount) OVER (PARTITION BY region)")

    def test_over_inside_string_is_text(self) -> None:
        same("status = 'OVER('", "status == 'OVER('")

    def test_syntax_error_position(self) -> None:
        msg = rejected("amount > > 5")
        assert re.search(r"\b(9|10)\b", msg), msg

    def test_chained_comparison(self) -> None:
        with pytest.raises(ValueError):
            parse_expr("a < b < c")

    def test_empty(self) -> None:
        with pytest.raises(ValueError):
            parse_expr("   ")


class TestLegacySpellingsKept:
    @pytest.mark.parametrize(("legacy", "canonical"), [
        ("customers.sum(revenue)", "sum(customers.revenue)"),
        ("revenue:sum", "sum(revenue)"),
        ("*:count", "count(*)"),
        ("status == 'paid' and region is not None", "status = 'paid' AND region IS NOT NULL"),
        ("region in ['EU', 'US']", "region IN ('EU', 'US')"),
    ])
    def test_legacy_equals_canonical(self, legacy: str, canonical: str) -> None:
        same(legacy, canonical)

    @pytest.mark.parametrize(("text", "value"), [("True", True), ("False", False), ("None", None), ("NULL", None),
                                                 ("TRUE", True), ("false", False)])
    def test_constant_words(self, text: str, value) -> None:
        assert parse_expr(text) == Literal(value=value)
