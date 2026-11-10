"""The colon-span rewriter behind the stored-text upgrade (spec: models/stored-upgrade › Stored colon spellings are
upgraded to the functional spelling)."""

from __future__ import annotations

import pytest

from slayer.core.syntax import parse_expr, rewrite_colon_spans

#: (stored text, rewritten text).
REWRITES = [
    pytest.param("amount:sum", "sum(amount)", id="column"),
    pytest.param("*:count", "count(*)", id="star"),
    pytest.param("customers.*:count", "count(customers.*)", id="cross-model-star"),
    pytest.param("customers.regions.population:sum", "sum(customers.regions.population)", id="join-path"),
    pytest.param('"order total":sum', 'sum("order total")', id="quoted-source"),
    pytest.param("price:percentile(p=0.9)", "percentile(price, p=0.9)", id="keyword-param"),
    pytest.param("balance:last(updated_at)", "last(balance, updated_at)", id="positional-arg"),
    pytest.param("amount:sum(window = '30d')", "sum(amount, window = '30d')", id="args-as-written"),
    pytest.param("amount:countd", "countd(amount)", id="alias-as-written"),
    pytest.param("amount:my_agg(k=2)", "my_agg(amount, k=2)", id="custom-aggregation"),
    pytest.param("created_at:month", "date_trunc('month', created_at)", id="granularity"),
    pytest.param("customers.created_at:year", "date_trunc('year', customers.created_at)", id="joined-granularity"),
    pytest.param("created_at:fiscal_quarter", "fiscal_quarter(created_at)", id="custom-granularity"),
    pytest.param("rank(rev:sum)", "rank(sum(rev))", id="transform-input"),
    pytest.param("x:last(y:max)", "last(x, max(y))", id="nested-innermost-first"),
    pytest.param("cumsum(amount:sum(partition_by=region)) / *:count",
                 "cumsum(sum(amount, partition_by=region)) / count(*)", id="several-spans"),
    pytest.param("amount:sum > 100 AND note = 'a:sum'", "sum(amount) > 100 AND note = 'a:sum'", id="string-literal"),
    pytest.param("sum(amount::INT) / *:count", "sum(amount::INT) / count(*)", id="cast"),
    pytest.param("amount:sum   >   {threshold}", "sum(amount)   >   {threshold}", id="placeholder-and-spacing"),
    pytest.param("amount:sum FILTER (WHERE note = 'x:count')", "sum(amount) FILTER (WHERE note = 'x:count')",
                 id="filter-literal"),
    pytest.param("amount:sum -- x:sum\n+ 1", "sum(amount) -- x:sum\n+ 1", id="comment"),
    pytest.param("AMOUNT:SUM", "SUM(AMOUNT)", id="case-as-written"),
]

#: Text with no colon span is returned byte-identical.
UNCHANGED = [
    "sum(amount)",
    "note = 'x:sum'",
    '"x:sum" > 1',
    "x::INT + 1",
    "sum(x) FILTER (WHERE note = 'a:b')",
    "sum(amount)  /  count(*)   -- x:sum",
    "date_trunc('month', created_at)",
    "status",
]


@pytest.mark.parametrize(("stored", "rewritten"), REWRITES)
def test_rewrite(stored: str, rewritten: str) -> None:
    assert rewrite_colon_spans(stored) == rewritten


@pytest.mark.parametrize(("stored", "rewritten"), REWRITES)
def test_rewrite_is_a_fixed_point(stored: str, rewritten: str) -> None:
    assert rewrite_colon_spans(rewritten) == rewritten


@pytest.mark.parametrize("text", UNCHANGED)
def test_text_without_spans_is_unchanged(text: str) -> None:
    assert rewrite_colon_spans(text) == text


@pytest.mark.parametrize(("stored", "rewritten"), [
    ("amount:sum / *:count", "sum(amount) / count(*)"),
    ("rank(rev:sum)", "rank(sum(rev))"),
    ("price:percentile(p=0.9)", "percentile(price, p=0.9)"),
])
def test_rewrite_parses_like_the_functional_text(stored: str, rewritten: str) -> None:
    assert parse_expr(rewrite_colon_spans(stored)) == parse_expr(rewritten)

