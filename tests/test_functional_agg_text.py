"""``functional_agg_text``: the one renderer of a ``(source, suffix)`` aggregation as functional text."""

import pytest

from slayer.core import refs


@pytest.mark.parametrize(
    ("source", "suffix", "expected"),
    [
        ("amount", "sum", "sum(amount)"),
        ("*", "count", "count(*)"),
        ("orders.*", "count", "count(orders.*)"),
        ("customers.regions.population", "sum", "sum(customers.regions.population)"),
        ("price", "percentile(p=0.9)", "percentile(price, p=0.9)"),
        ("amount", "sum(window='30d')", "sum(amount, window='30d')"),
        ("*", "count(window='7d')", "count(*, window='7d')"),
        ("balance", "last(updated_at)", "last(balance, updated_at)"),
        ("amount", "weighted_avg(weight=qty)", "weighted_avg(amount, weight=qty)"),
        ("amount", "sum()", "sum(amount)"),
    ],
)
def test_renders_functional(source: str, suffix: str, expected: str) -> None:
    assert refs.functional_agg_text(source=source, suffix=suffix) == expected
