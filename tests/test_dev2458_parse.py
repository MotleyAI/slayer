"""Mode B spellings of the DISTINCT aggregates and ordered-set percentiles.

Spec: aggregations/distinct-and-percentiles › Ordered-set and DISTINCT spellings; aggregations/functional-form ›
Syntax boundaries are preserved.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError
from slayer.core.syntax import AggCall, WindowCall, canonical_metric_text, parse_expr

DISTINCT_FORMS = ("count", "sum", "avg", "min", "max", "array_agg")
ORDERED_SET_FORMS = ("PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)", "PERCENTILE_DISC(p) WITHIN GROUP (ORDER BY x)")


@pytest.mark.parametrize(("written", "native"), [
    ("count(distinct user_id)", "count_distinct(user_id)"),
    ("sum(distinct amount)", "sum_distinct(amount)"),
    ("SUM(DISTINCT amount)", "sum_distinct(amount)"),
    ("Sum(Distinct amount)", "sum_distinct(amount)"),
    ("AVG(DISTINCT amount)", "avg_distinct(amount)"),
    ("min(distinct amount)", "min(amount)"),
    ("MAX(DISTINCT amount, partition_by=region)", "max(amount, partition_by=region)"),
    ("sum(DISTINCT coalesce(amount, 0), partition_by=region)", "sum_distinct(coalesce(amount, 0), partition_by=region)"),
    ("sum(  distinct  /* c */ amount)", "sum_distinct(amount)"),
    ("SUM(DISTINCT amount) FILTER (WHERE region = 'EU')", "sum_distinct(amount) FILTER (WHERE region = 'EU')"),
    ("array_agg(distinct status)", "collect(status)"),
])
def test_distinct_spellings_equal_their_builtins(written: str, native: str) -> None:
    """Scenario: sum, avg, min and max with DISTINCT."""
    assert parse_expr(written) == parse_expr(native)


@pytest.mark.parametrize("text", [
    "stddev(distinct amount)", "median(DISTINCT amount)", "count_distinct(distinct amount)",
    "count(distinct *)", "sum(DISTINCT *)", "count(distinct a, b)", "sum(DISTINCT a, b)", "avg(distinct a, b)",
])
def test_unsupported_distinct_lists_the_accepted_forms(text: str) -> None:
    """Scenarios: Unsupported DISTINCT; DISTINCT keyword rejected."""
    with pytest.raises(ValueError) as refused:
        parse_expr(text)
    message = str(refused.value).lower()
    assert all(f"{name}(distinct" in message for name in DISTINCT_FORMS), message


def test_distinct_inside_a_string_is_text() -> None:
    parsed = parse_expr("sum(iif(note = 'distinct x', 1, 0))")
    assert isinstance(parsed, AggCall)
    assert parsed.agg == "sum"


@pytest.mark.parametrize(("written", "native"), [
    ("PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY amount)", "percentile_disc(amount, p=0.25)"),
    ("percentile_disc(0.25) within group (order by amount asc)", "percentile_disc(amount, p=0.25)"),
    ("PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY status) FILTER (WHERE region = 'EU')",
     "percentile_disc(status, p=0.5) FILTER (WHERE region = 'EU')"),
    ("PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY amount)", "percentile(amount, p=0.25)"),
])
def test_ordered_set_spellings_equal_their_builtins(written: str, native: str) -> None:
    """Requirement: Discrete percentile (``PERCENTILE_DISC(p) WITHIN GROUP (ORDER BY x)``)."""
    assert parse_expr(written) == parse_expr(native)


@pytest.mark.parametrize("text", [
    "MODE() WITHIN GROUP (ORDER BY status)",
    "SUM(amount) WITHIN GROUP (ORDER BY amount)",
    "PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY amount DESC)",
    "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount DESC)",
    "PERCENTILE_DISC(0.5, 0.1) WITHIN GROUP (ORDER BY amount)",
    "PERCENTILE_DISC() WITHIN GROUP (ORDER BY amount)",
    "PERCENTILE_DISC(DISTINCT 0.5) WITHIN GROUP (ORDER BY amount)",
])
def test_unsupported_ordered_set_names_both_forms(text: str) -> None:
    """Scenario: Unsupported ordered-set function."""
    with pytest.raises(ValueError) as refused:
        parse_expr(text)
    assert all(form in str(refused.value) for form in ORDERED_SET_FORMS), str(refused.value)


@pytest.mark.parametrize("text", [
    "SUM(DISTINCT amount) OVER (PARTITION BY region)",
    "avg(distinct amount) OVER ()",
    "PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY amount) OVER ()",
])
def test_distinct_or_ordered_set_inside_a_window_is_a_typed_error(text: str) -> None:
    """Scenario: DISTINCT inside a window."""
    with pytest.raises(SlayerError):
        parse_expr(text)


@pytest.mark.parametrize(("windowed", "plain"), [
    ("sum(DISTINCT x) OVER ()", "sum(DISTINCT x)"),
    ("PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY x) OVER ()", "PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY x)"),
])
def test_the_window_reading_wraps_the_plain_reading(windowed: str, plain: str) -> None:
    window = parse_expr(windowed, windows=True)
    assert isinstance(window, WindowCall)
    assert isinstance(window.function, AggCall)
    assert window.function == parse_expr(plain)


@pytest.mark.parametrize(("written", "canonical"), [
    ("SUM(DISTINCT amount)", "sum_distinct(amount)"),
    ("avg(distinct amount)", "avg_distinct(amount)"),
    ("MIN(DISTINCT amount)", "min(amount)"),
    ("PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY amount)", "percentile_disc(amount, p=0.25)"),
    ("percentile_disc(amount, p=0.25)", "percentile_disc(amount, p=0.25)"),
])
def test_canonical_text_round_trips(written: str, canonical: str) -> None:
    text = canonical_metric_text(parse_expr(written))
    assert text == canonical
    assert parse_expr(text) == parse_expr(written)
