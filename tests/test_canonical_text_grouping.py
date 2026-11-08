"""Canonical expression text keeps its grouping (spec: queries/expression-grammar › Canonical expression text)."""

from __future__ import annotations

import pytest

from slayer.core.syntax import canonical_measure_text, parse_expr

#: Every operator level, associativity, unary chains, casts, FILTER and CASE-lowered iif.
CORPUS = [
    "a + b * c", "(a + b) * c", "a - (b - c)", "a - b - c", "(a - b) - c", "a / (b / c)", "a / b / c",
    "a * (b % c)", "a % (b * c)", "a * (b / c)", "a * b / c", "a + (b + c)",
    "-(-b)", "-(-(-b))", "-(a + b)", "-a * b", "-(a * b)", "a - -b", "a - (-b)", "a + -b",
    "a ** b ** c", "(a ** b) ** c", "-a ** 2", "(-a) ** 2", "(a + b) ** 2",
    "a || (b || c)", "(a || b) || c", "(a - b) || c", "a ->> 'k' || 'x'",
    "a::int + b", "(a + b)::int", "-a::int", "(-a)::int", "CAST(a + b AS INT) * c", "TRY_CAST(a - b AS INT)",
    "a = b and c = d or e = f", "a = b and (c = d or e = f)", "(a = b or c = d) and e = f",
    "not (a = b and c = d)", "not a = b", "(not a) = b", "(a = b) = (c = d)", "not (not a)",
    "(a + b) > c", "a > b + c", "a in (1, 2) and b is null", "(a > 1) is not null",
    "(a between 1 and 2) = (b > 1)", "not (a between 1 and 2)",
    "sum(a + b) * 2", "sum(a) / (sum(b) - sum(c))", "sum(amount) - sum(refund)", "-(sum(a) - sum(b))",
    "sum(a) FILTER (WHERE b > 1 or c < 2)", "sum(a) FILTER (WHERE (b > 1 or c < 2) and d = 1)",
    "sum(a - b) FILTER (WHERE not (b > 1 and c < 2))",
    "CASE WHEN a > 1 THEN b + c ELSE (b - c) * 2 END", "iif(a > 1 or b > 2, c, (d - e) - f)",
    "coalesce(a - (b - c), 0) + 1",
]


@pytest.mark.parametrize("text", CORPUS)
def test_round_trip(text: str) -> None:
    parsed = parse_expr(text)
    assert parse_expr(canonical_measure_text(parsed)) == parsed, canonical_measure_text(parsed)


@pytest.mark.parametrize("text", CORPUS)
def test_never_reads_as_a_comment(text: str) -> None:
    rendered = canonical_measure_text(parse_expr(text))
    assert "--" not in rendered, rendered
    assert "/*" not in rendered, rendered


def test_grouping_is_kept() -> None:
    texts = [canonical_measure_text(parse_expr(t)) for t in ("(a + b) * c", "a + b * c", "a - (b - c)", "a - b - c")]
    assert len(set(texts)) == 4, texts


def test_double_negation() -> None:
    parsed = parse_expr("-(-b)")
    rendered = canonical_measure_text(parsed)
    assert "--" not in rendered, rendered
    assert parse_expr(rendered) == parsed
