"""The ``core.syntax`` mapping-expression production: ``<value>`` or ``<value> WHERE ( <predicate> )``.

Spec: ontologies/ontology-documents › Mapping expressions are row-level Layer 1 over the referenced semantic model.
"""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core import syntax
from slayer.core.syntax import BoolOp, Cmp, DottedRef, Literal, ScalarCall


def parse(text: str) -> Any:
    return syntax.parse_mapping_expression(text)


def test_a_plain_value_has_no_predicate() -> None:
    parsed = parse("FLIGHT.id")
    assert parsed.value == DottedRef(parts=("FLIGHT", "id"))
    assert parsed.predicate is None


def test_a_filtered_value_splits_into_value_and_predicate() -> None:
    parsed = parse("FLIGHT.id WHERE ( FLIGHT.diverted == TRUE )")
    assert parsed.value == DottedRef(parts=("FLIGHT", "id"))
    assert isinstance(parsed.predicate, Cmp)
    assert parsed.predicate.left == DottedRef(parts=("FLIGHT", "diverted"))
    assert parsed.predicate.right == Literal(value=True)


def test_the_sap_filter_reads_its_empty_string() -> None:
    parsed = parse("CUSTOMER_SALES_AREA.SALES_ORG WHERE ( CUSTOMER_SALES_AREA.DELIVERY_BLOCK <> '' )")
    assert parsed.value == DottedRef(parts=("CUSTOMER_SALES_AREA", "SALES_ORG"))
    assert isinstance(parsed.predicate, Cmp)
    assert parsed.predicate.right == Literal(value="")


def test_nested_parentheses_stay_in_the_predicate() -> None:
    parsed = parse("FLIGHT.id WHERE ((FLIGHT.distance > 100) AND (FLIGHT.air_time < 60))")
    assert parsed.value == DottedRef(parts=("FLIGHT", "id"))
    assert isinstance(parsed.predicate, BoolOp)


def test_where_inside_a_string_literal_is_not_the_split() -> None:
    parsed = parse("FLIGHT.nr WHERE ( FLIGHT.note = 'late WHERE ( raining )' )")
    assert parsed.value == DottedRef(parts=("FLIGHT", "nr"))
    assert isinstance(parsed.predicate, Cmp)
    assert parsed.predicate.right == Literal(value="late WHERE ( raining )")


def test_a_value_whose_string_holds_where_has_no_predicate() -> None:
    parsed = parse("CONCAT(FLIGHT.nr, ' WHERE ( x )')")
    assert isinstance(parsed.value, ScalarCall)
    assert parsed.predicate is None


def test_a_value_expression_is_any_row_level_expression() -> None:
    parsed = parse("ROUTE.orig_airport_code || ' -> ' || ROUTE.dest_airport_code WHERE ( ROUTE.id IS NOT NULL )")
    assert parsed.predicate is not None


@pytest.mark.parametrize("text", [
    pytest.param("AVG(FLIGHT.dep_delay)", id="aggregate-value"),
    pytest.param("FLIGHT.id WHERE ( SUM(FLIGHT.distance) > 100 )", id="aggregate-predicate"),
    pytest.param("COUNT(*)", id="count-star"),
    pytest.param("rank() OVER (ORDER BY FLIGHT.distance)", id="window-value"),
    pytest.param("FLIGHT.id WHERE ( rank() OVER (ORDER BY FLIGHT.distance) = 1 )", id="window-predicate"),
])
def test_aggregates_and_windows_are_refused(text: str) -> None:
    with pytest.raises(ValueError):
        parse(text)


@pytest.mark.parametrize("text", [
    pytest.param("FLIGHT.id WHERE FLIGHT.diverted", id="unparenthesised-predicate"),
    pytest.param("FLIGHT.id WHERE ( FLIGHT.diverted = TRUE ) OR TRUE", id="text-after-the-predicate"),
    pytest.param("FLIGHT.id WHERE ( )", id="empty-predicate"),
    pytest.param("WHERE ( FLIGHT.diverted = TRUE )", id="no-value"),
    pytest.param("FLIGHT.id WHERE ( FLIGHT.a = 1", id="unclosed"),
])
def test_malformed_filtered_expressions_are_refused(text: str) -> None:
    with pytest.raises(ValueError):
        parse(text)
