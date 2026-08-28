"""DEV-1740 Part A — the CASE/iif surface at parse time.

Pins the ``_rewrite_case_when`` preprocessor and the reserved-name
``ScalarCall(name="iif")`` it lowers to: searched + simple CASE, nesting,
multi-WHEN, missing ELSE, case-insensitive keywords, keywords inside string
literals, malformed rejection, ``iif`` arity, and the deliberate rejection of
the Python ternary (SQL spelling only).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.keys import (
    SCALAR_FUNCTION_ARITY,
    SCALAR_FUNCTIONS,
    check_scalar_arity,
)
from slayer.engine.syntax import (
    BoolOp,
    Cmp,
    Literal,
    Ref,
    ScalarCall,
    _rewrite_case_when,
    parse_expr,
    parse_filter_expr,
)


def _iif(node) -> ScalarCall:
    """Assert ``node`` is the reserved-name iif ScalarCall and return it."""
    assert isinstance(node, ScalarCall)
    assert node.name == "iif"
    return node


# --------------------------------------------------------------------------- #
# The text rewrite: CASE ... END -> nested iif(...)
# --------------------------------------------------------------------------- #
class TestRewriteToIif:
    def test_searched_single_branch(self) -> None:
        out = _rewrite_case_when("CASE WHEN amount > 5 THEN 1 ELSE 0 END")
        assert "CASE" not in out.upper()
        assert out.strip() == "iif(amount > 5, 1, 0)"

    def test_missing_else_becomes_null(self) -> None:
        out = _rewrite_case_when("CASE WHEN amount > 5 THEN 1 END")
        assert out.strip() == "iif(amount > 5, 1, None)"

    def test_multi_when_nests_right(self) -> None:
        out = _rewrite_case_when(
            "CASE WHEN a > 2 THEN 10 WHEN a > 1 THEN 5 ELSE 0 END"
        )
        assert out.strip() == "iif(a > 2, 10, iif(a > 1, 5, 0))"

    def test_simple_form_synthesizes_equality(self) -> None:
        out = _rewrite_case_when("CASE region WHEN 'EU' THEN 1 ELSE 0 END")
        assert out.strip() == "iif(region == 'EU', 1, 0)"

    def test_non_case_text_is_untouched(self) -> None:
        assert _rewrite_case_when("amount + 1") == "amount + 1"

    def test_keyword_inside_string_literal_is_not_syntax(self) -> None:
        # A value that merely contains CASE/END words must survive verbatim.
        text = "status == 'END of the CASE when done'"
        assert _rewrite_case_when(text) == text


# --------------------------------------------------------------------------- #
# parse_expr produces a reserved-name iif ScalarCall
# --------------------------------------------------------------------------- #
class TestParseSearchedCase:
    def test_searched_case_parses_to_iif_call(self) -> None:
        node = _iif(parse_expr("CASE WHEN amount > 5 THEN 1 ELSE 0 END"))
        cond, then, otherwise = node.args
        assert isinstance(cond, Cmp)
        assert cond.op == ">"
        assert isinstance(then, Literal)
        assert then.value == Decimal(1)
        assert isinstance(otherwise, Literal)
        assert otherwise.value == Decimal(0)

    def test_missing_else_is_null_branch(self) -> None:
        node = _iif(parse_expr("CASE WHEN amount > 5 THEN 1 END"))
        otherwise = node.args[2]
        assert isinstance(otherwise, Literal)
        assert otherwise.value is None

    def test_multi_when_is_nested_iif(self) -> None:
        node = _iif(parse_expr("CASE WHEN a > 2 THEN 10 WHEN a > 1 THEN 5 ELSE 0 END"))
        inner = _iif(node.args[2])
        assert inner.args[1].value == Decimal(5)
        assert inner.args[2].value == Decimal(0)

    def test_case_insensitive_keywords(self) -> None:
        _iif(parse_expr("case when amount > 5 then 1 else 0 end"))

    def test_mixed_case_keywords(self) -> None:
        _iif(parse_expr("Case When amount > 5 Then 1 Else 0 End"))


class TestParseSimpleCase:
    def test_simple_case_equality(self) -> None:
        node = _iif(parse_expr("CASE region WHEN 'EU' THEN 1 ELSE 0 END"))
        cond = node.args[0]
        assert isinstance(cond, Cmp)
        assert cond.op == "=="
        assert isinstance(cond.left, Ref)
        assert cond.left.name == "region"
        assert cond.right.value == "EU"

    def test_simple_case_multi_branch(self) -> None:
        node = _iif(parse_expr(
            "CASE region WHEN 'EU' THEN 1 WHEN 'US' THEN 2 ELSE 0 END"
        ))
        _iif(node.args[2])


class TestNestedCase:
    def test_case_inside_then_branch(self) -> None:
        node = _iif(parse_expr(
            "CASE WHEN a > 2 THEN CASE WHEN b > 1 THEN 1 ELSE 2 END ELSE 0 END"
        ))
        _iif(node.args[1])


# --------------------------------------------------------------------------- #
# Operator normalization inside WHEN conditions (sign-off item 4): the
# condition segment accepts SQL operators in ALL contexts, not just filters.
# --------------------------------------------------------------------------- #
class TestConditionOperatorNormalization:
    def test_sql_equals_in_measure_context(self) -> None:
        # parse_expr is the MEASURE path (no blanket `=`->`==` rewrite), yet the
        # WHEN condition must still accept SQL `=` and `AND`.
        node = _iif(parse_expr("CASE WHEN a = 5 AND b = 2 THEN 1 ELSE 0 END"))
        cond = node.args[0]
        assert isinstance(cond, BoolOp)
        assert cond.op == "and"
        assert all(isinstance(o, Cmp) and o.op == "==" for o in cond.operands)

    def test_sql_like_in_condition(self) -> None:
        node = _iif(parse_expr("CASE WHEN city LIKE 'Par%' THEN 1 ELSE 0 END"))
        cond = node.args[0]
        assert isinstance(cond, ScalarCall)
        assert cond.name == "like"

    def test_not_equal_operator(self) -> None:
        node = _iif(parse_expr("CASE WHEN a <> 5 THEN 1 ELSE 0 END"))
        assert isinstance(node.args[0], Cmp)
        assert node.args[0].op == "!="

    def test_or_operator(self) -> None:
        node = _iif(parse_expr("CASE WHEN a = 1 OR b = 2 THEN 1 ELSE 0 END"))
        assert isinstance(node.args[0], BoolOp)
        assert node.args[0].op == "or"

    def test_not_operator(self) -> None:
        node = _iif(parse_expr("CASE WHEN NOT a = 1 THEN 1 ELSE 0 END"))
        # `not a == 1` -> UnaryOp(not, Cmp(==)).
        assert node.args[0].op == "not"


class TestBranchesNotBlanketNormalized:
    def test_then_else_string_literals_keep_sql_keywords(self) -> None:
        # Only the WHEN condition is operator-normalized; THEN/ELSE values must
        # survive verbatim — a blanket rewrite would lowercase AND/OR here.
        node = _iif(parse_expr(
            "CASE WHEN region = 'EU' THEN 'a AND b' ELSE 'x OR y' END"
        ))
        then, otherwise = node.args[1], node.args[2]
        assert isinstance(then, Literal)
        assert then.value == "a AND b"
        assert isinstance(otherwise, Literal)
        assert otherwise.value == "x OR y"


class TestNestingEverywhere:
    def test_nested_in_when_condition(self) -> None:
        node = _iif(parse_expr(
            "CASE WHEN (CASE WHEN a > 1 THEN 1 ELSE 0 END) = 1 THEN 10 ELSE 0 END"
        ))
        cond = node.args[0]
        assert isinstance(cond, Cmp)
        _iif(cond.left)

    def test_nested_in_else_branch(self) -> None:
        node = _iif(parse_expr(
            "CASE WHEN a > 1 THEN 1 ELSE CASE WHEN b > 1 THEN 2 ELSE 3 END END"
        ))
        _iif(node.args[2])

    def test_full_case_sequence_inside_a_string_literal(self) -> None:
        node = _iif(parse_expr(
            "CASE WHEN region = 'CASE WHEN x THEN y END' THEN 1 ELSE 0 END"
        ))
        assert node.args[0].right.value == "CASE WHEN x THEN y END"


# --------------------------------------------------------------------------- #
# iif() call form — an allowlisted scalar with a pinned (3, 3) arity. The
# wrong-arity RAISE now happens at bind time (the generic scalar path), pinned
# end-to-end in test_dev1740_conditional_execution.py.
# --------------------------------------------------------------------------- #
class TestIifCall:
    def test_iif_three_args(self) -> None:
        node = _iif(parse_expr("iif(amount > 5, 1, 0)"))
        assert node.args[1].value == Decimal(1)

    def test_iif_case_insensitive(self) -> None:
        _iif(parse_expr("IIF(amount > 5, 1, 0)"))

    def test_iif_is_allowlisted_with_exact_arity(self) -> None:
        assert "iif" in SCALAR_FUNCTIONS
        assert SCALAR_FUNCTION_ARITY["iif"] == (3, 3)

    @pytest.mark.parametrize("argc", [1, 2, 4])
    def test_wrong_arity_message_names_iif(self, argc: int) -> None:
        msg = check_scalar_arity(name="iif", argc=argc)
        assert msg is not None
        assert "iif" in msg


# --------------------------------------------------------------------------- #
# Filters accept CASE too
# --------------------------------------------------------------------------- #
class TestFilterContext:
    def test_case_in_filter_predicate(self) -> None:
        node = parse_filter_expr(
            "CASE WHEN region = 'EU' THEN 1 ELSE 0 END = 1"
        )
        assert isinstance(node, Cmp)
        assert node.op == "=="
        _iif(node.left)


# --------------------------------------------------------------------------- #
# Malformed CASE + the deliberately-rejected Python ternary
# --------------------------------------------------------------------------- #
class TestRejections:
    @pytest.mark.parametrize("expr", [
        "CASE WHEN amount > 5 THEN 1",           # no END
        "CASE WHEN amount > 5 ELSE 0 END",       # no THEN
        "CASE amount > 5 THEN 1 ELSE 0 END",     # WHEN missing after searched CASE
        "CASE END",                              # empty
    ])
    def test_malformed_case_raises_clearly(self, expr: str) -> None:
        with pytest.raises(ValueError, match=r"(?i)case"):
            parse_expr(expr)

    def test_python_ternary_is_rejected_with_pointer(self) -> None:
        # SQL spelling only — the Python conditional stays unsupported, and the
        # error points at the CASE/iif spelling.
        with pytest.raises(ValueError, match=r"(?i)case|iif"):
            parse_expr("1 if amount > 5 else 0")
