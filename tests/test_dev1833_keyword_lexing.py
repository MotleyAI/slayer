"""DEV-1833 — Mode-B keyword lexing hardened for Unicode identifiers.

Identifiers named after, containing, or qualified by SQL keywords must parse
as ordinary references; keyword recognition is ASCII-exact, and CASE lowers
only when a depth-0 WHEN follows. Spec:
openspec/changes/dev-1833-harden-mode-b-keyword-lexing-case-like-for-unicode.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.engine.syntax import (
    Arith,
    BoolOp,
    Cmp,
    DottedRef,
    Literal,
    Ref,
    ScalarCall,
    UnaryOp,
    _rewrite_sql_like,
    parse_expr,
    parse_filter_expr,
)


def _iif(node) -> ScalarCall:
    """Assert ``node`` is the iif ScalarCall and return it."""
    assert isinstance(node, ScalarCall)
    assert node.name == "iif"
    return node


# --------------------------------------------------------------------------- #
# Bare keyword-named identifiers
# --------------------------------------------------------------------------- #
class TestBareKeywordIdentifiers:
    def test_bare_case_is_a_ref(self) -> None:
        assert parse_expr("case") == Ref(name="case")

    def test_case_in_arithmetic(self) -> None:
        assert parse_expr("case + 1") == Arith(
            op="+", left=Ref(name="case"), right=Literal(value=Decimal(1)),
        )

    def test_case_as_call_argument(self) -> None:
        node = _iif(parse_expr("iif(case, 1, 2)"))
        assert node.args[0] == Ref(name="case")

    def test_case_in_scalar_call(self) -> None:
        assert parse_expr("upper(case)") == ScalarCall(
            name="upper", args=(Ref(name="case"),),
        )

    def test_case_in_filter_predicate(self) -> None:
        assert parse_filter_expr("case = 1") == Cmp(
            op="==", left=Ref(name="case"), right=Literal(value=Decimal(1)),
        )

    def test_case_prefixed_name_untouched(self) -> None:
        assert parse_expr("case_total * 2") == Arith(
            op="*", left=Ref(name="case_total"), right=Literal(value=Decimal(2)),
        )


# --------------------------------------------------------------------------- #
# Keyword-named leaves of dotted references
# --------------------------------------------------------------------------- #
class TestQualifiedKeywordIdentifiers:
    def test_dotted_case(self) -> None:
        assert parse_expr("customers.case") == DottedRef(parts=("customers", "case"))

    def test_dotted_case_spaced_dot(self) -> None:
        assert parse_expr("customers . case") == DottedRef(
            parts=("customers", "case"),
        )

    def test_dotted_end_in_then_value(self) -> None:
        node = _iif(parse_expr("CASE WHEN a THEN customers.end ELSE 0 END"))
        assert node.args == (
            Ref(name="a"),
            DottedRef(parts=("customers", "end")),
            Literal(value=Decimal(0)),
        )


# --------------------------------------------------------------------------- #
# Unicode identifiers containing / adjacent to keyword spellings
# --------------------------------------------------------------------------- #
class TestUnicodeIdentifiers:
    def test_unicode_prefixed_case(self) -> None:
        assert parse_expr("écase") == Ref(name="écase")

    def test_decomposed_prefix(self) -> None:
        # e + COMBINING ACUTE; Python NFKC-normalizes identifiers to composed é.
        assert parse_expr("écase") == Ref(name="écase")

    def test_other_id_start_prefix(self) -> None:
        assert parse_expr("℘case") == Ref(name="℘case")

    def test_unicode_name_with_underscore(self) -> None:
        assert parse_expr("é_case") == Ref(name="é_case")

    def test_cjk_name(self) -> None:
        assert parse_expr("变量") == Ref(name="变量")

    def test_trailing_combining_mark(self) -> None:
        # A combining mark directly AFTER the keyword spelling; NFKC composes.
        assert parse_expr("casé") == Ref(name="casé")


# --------------------------------------------------------------------------- #
# Unicode case-fold spoofs are never keywords
# --------------------------------------------------------------------------- #
class TestCaseFoldSpoofs:
    def test_spoof_alone_is_identifier(self) -> None:
        # LATIN SMALL LETTER LONG S uppercases to S; NFKC folds it to "case".
        assert parse_expr("caſe") == Ref(name="case")

    def test_spoof_beside_real_case(self) -> None:
        node = parse_expr("caſe + CASE WHEN x THEN 1 END")
        assert isinstance(node, Arith)
        assert node.left == Ref(name="case")
        _iif(node.right)


# --------------------------------------------------------------------------- #
# Keyword-named identifiers coexisting with a real CASE
# --------------------------------------------------------------------------- #
class TestKeywordAlongsideRealCase:
    def test_identifier_plus_case_when(self) -> None:
        node = parse_expr("case + CASE WHEN x THEN 1 END")
        assert isinstance(node, Arith)
        assert node.left == Ref(name="case")
        right = _iif(node.right)
        assert right.args[0] == Ref(name="x")

    def test_identifier_in_when_condition(self) -> None:
        node = _iif(parse_expr("CASE WHEN case THEN 1 WHEN other THEN 2 END"))
        assert node.args[0] == Ref(name="case")
        inner = _iif(node.args[2])
        assert inner.args[0] == Ref(name="other")


# --------------------------------------------------------------------------- #
# The WHEN-lookahead gate
# --------------------------------------------------------------------------- #
class TestWhenLookaheadGate:
    def test_parenthesized_keyword_operand(self) -> None:
        node = _iif(parse_expr("CASE (case) WHEN 1 THEN 2 END"))
        assert node.args == (
            Cmp(op="==", left=Ref(name="case"), right=Literal(value=Decimal(1))),
            Literal(value=Decimal(2)),
            Literal(value=None),
        )

    def test_bare_keyword_operand_errors_loudly(self) -> None:
        # Genuinely ambiguous — must error (any message), never silently corrupt.
        with pytest.raises(ValueError):
            parse_expr("CASE case WHEN 1 THEN 2 END")

    def test_case_with_no_when_degrades_to_generic_error(self) -> None:
        with pytest.raises(ValueError, match=r"Invalid Mode-B expression"):
            parse_expr("CASE case_total")

    def test_else_stops_the_gate(self) -> None:
        node = _iif(parse_expr("CASE WHEN a THEN case ELSE 1 END"))
        assert node.args[1] == Ref(name="case")

    def test_end_stops_the_gate(self) -> None:
        node = _iif(parse_expr("CASE WHEN a THEN case END"))
        assert node.args == (
            Ref(name="a"), Ref(name="case"), Literal(value=None),
        )


# --------------------------------------------------------------------------- #
# Real CASE still lowers (unchanged before/after the hardening)
# --------------------------------------------------------------------------- #
class TestCaseStillLowers:
    def test_searched(self) -> None:
        _iif(parse_expr("CASE WHEN amount > 5 THEN 1 ELSE 0 END"))

    def test_simple(self) -> None:
        node = _iif(parse_expr("CASE region WHEN 'EU' THEN 1 ELSE 0 END"))
        assert node.args[0] == Cmp(
            op="==", left=Ref(name="region"), right=Literal(value="EU"),
        )

    def test_nested_in_then(self) -> None:
        node = _iif(parse_expr(
            "CASE WHEN a > 2 THEN CASE WHEN b > 1 THEN 1 ELSE 2 END ELSE 0 END"
        ))
        _iif(node.args[1])

    def test_sql_operators_in_when(self) -> None:
        node = _iif(parse_expr(
            "CASE WHEN region = 'EU' AND amount IS NOT NULL THEN 1 ELSE 0 END"
        ))
        cond = node.args[0]
        assert isinstance(cond, BoolOp)
        assert cond.op == "and"

    def test_missing_end_still_specific(self) -> None:
        with pytest.raises(ValueError, match=r"missing END"):
            parse_expr("CASE WHEN a THEN 1")

    def test_missing_then_still_specific(self) -> None:
        with pytest.raises(ValueError, match=r"missing its THEN"):
            parse_expr("CASE WHEN a 1 END")


# --------------------------------------------------------------------------- #
# LIKE hardening
# --------------------------------------------------------------------------- #
class TestLikeHardening:
    def test_escaped_quote_pattern(self) -> None:
        assert parse_filter_expr(r"col LIKE 'It\'s%'") == ScalarCall(
            name="like", args=(Ref(name="col"), Literal(value="It's%")),
        )

    def test_like_inside_string_literal_untouched(self) -> None:
        node = parse_filter_expr("note == \"we like 'cats'\"")
        assert node == Cmp(
            op="==", left=Ref(name="note"), right=Literal(value="we like 'cats'"),
        )

    def test_dotless_i_is_not_like(self) -> None:
        text = "x lıke 'p%'"
        assert _rewrite_sql_like(text) == text

    def test_kelvin_sign_is_not_like(self) -> None:
        text = "x liKe 'p%'"
        assert _rewrite_sql_like(text) == text

    def test_spoofed_like_fails_loudly(self) -> None:
        with pytest.raises(ValueError, match=r"Invalid Mode-B expression"):
            parse_filter_expr("x lıke 'p%'")

    def test_spoofed_is_keyword_not_normalized(self) -> None:
        with pytest.raises(ValueError, match=r"Invalid Mode-B expression"):
            parse_filter_expr("x ıs None")


# --------------------------------------------------------------------------- #
# LIKE still rewrites (unchanged before/after the hardening)
# --------------------------------------------------------------------------- #
class TestLikeStillWorks:
    def test_basic_like(self) -> None:
        assert parse_filter_expr("name LIKE 'a%'") == ScalarCall(
            name="like", args=(Ref(name="name"), Literal(value="a%")),
        )

    def test_not_like(self) -> None:
        node = parse_filter_expr("name NOT LIKE 'a%'")
        assert isinstance(node, UnaryOp)
        assert node.op == "not"
        assert isinstance(node.operand, ScalarCall)
        assert node.operand.name == "like"

    def test_scalar_call_lhs(self) -> None:
        node = parse_filter_expr("lower(customers.email) like '%@x.io'")
        assert isinstance(node, ScalarCall)
        assert node.name == "like"
        assert node.args[0] == ScalarCall(
            name="lower", args=(DottedRef(parts=("customers", "email")),),
        )

    def test_unicode_lhs_still_rewrites(self) -> None:
        assert parse_filter_expr("é_col LIKE 'p%'") == ScalarCall(
            name="like", args=(Ref(name="é_col"), Literal(value="p%")),
        )

    def test_ascii_mixed_case_still_rewrites(self) -> None:
        node = parse_filter_expr("x LiKe 'p%'")
        assert isinstance(node, ScalarCall)
        assert node.name == "like"

    def test_double_quoted_pattern_rejected(self) -> None:
        with pytest.raises(ValueError, match=r"Invalid Mode-B expression"):
            parse_filter_expr('col like "p%"')
