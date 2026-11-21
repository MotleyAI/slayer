"""Unit tests for the naive Cypher label-filter parser (DEV-1532).

``slayer.search.cypher_naive.parse_naive_label_filter`` parses
``MATCH (var:Label1:Label2:...) RETURN var.id AS id`` patterns (case-insensitive,
whitespace-tolerant) and returns the set of kind strings to filter on.

When the pattern does not match (complex Cypher), it raises ``SlayerError``
with an install hint for the ``advanced_search`` extra.
When a label is unrecognised, it raises ``SlayerError`` with "unknown".
"""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError
from slayer.search.cypher_naive import parse_naive_label_filter


# ---------------------------------------------------------------------------
# Happy paths: recognised labels → kind set
# ---------------------------------------------------------------------------


def test_single_label_model() -> None:
    kinds = parse_naive_label_filter("MATCH (n:Dataset) RETURN n.id AS id")
    assert kinds == {"dataset"}


def test_single_label_memory() -> None:
    kinds = parse_naive_label_filter("MATCH (n:Memory) RETURN n.id AS id")
    assert kinds == {"memory"}


def test_single_label_column() -> None:
    kinds = parse_naive_label_filter("MATCH (n:Field) RETURN n.id AS id")
    assert kinds == {"field"}


def test_single_label_datasource() -> None:
    kinds = parse_naive_label_filter("MATCH (n:Datasource) RETURN n.id AS id")
    assert kinds == {"datasource"}


def test_single_label_measure() -> None:
    kinds = parse_naive_label_filter("MATCH (n:Metric) RETURN n.id AS id")
    assert kinds == {"metric"}


def test_single_label_aggregation() -> None:
    kinds = parse_naive_label_filter("MATCH (n:Aggregation) RETURN n.id AS id")
    assert kinds == {"aggregation"}


def test_multi_label_colon_separated_two() -> None:
    kinds = parse_naive_label_filter(
        "MATCH (n:Dataset:Field) RETURN n.id AS id"
    )
    assert kinds == {"dataset", "field"}


def test_multi_label_colon_separated_three() -> None:
    kinds = parse_naive_label_filter(
        "MATCH (n:Memory:Dataset:Field) RETURN n.id AS id"
    )
    assert kinds == {"memory", "dataset", "field"}


def test_all_six_labels_together() -> None:
    kinds = parse_naive_label_filter(
        "MATCH (n:Memory:Datasource:Dataset:Field:Metric:Aggregation) "
        "RETURN n.id AS id"
    )
    assert kinds == {"memory", "datasource", "dataset", "field", "metric", "aggregation"}


# ---------------------------------------------------------------------------
# Case-insensitivity and whitespace tolerance
# ---------------------------------------------------------------------------


def test_case_insensitive_keyword() -> None:
    kinds = parse_naive_label_filter("match (n:Dataset) return n.id as id")
    assert kinds == {"dataset"}


def test_case_insensitive_label() -> None:
    kinds = parse_naive_label_filter("MATCH (n:dataset) RETURN n.id AS id")
    assert kinds == {"dataset"}


def test_whitespace_around_labels() -> None:
    kinds = parse_naive_label_filter(
        "MATCH ( n : Dataset : Field ) RETURN n.id AS id"
    )
    assert kinds == {"dataset", "field"}


def test_extra_whitespace_in_return() -> None:
    kinds = parse_naive_label_filter("MATCH (n:Dataset)  RETURN  n.id  AS  id")
    assert kinds == {"dataset"}


# ---------------------------------------------------------------------------
# Error cases: unknown label
# ---------------------------------------------------------------------------


def test_unknown_label_raises_slayer_error() -> None:
    with pytest.raises(SlayerError, match="(?i)unknown"):
        parse_naive_label_filter("MATCH (n:Foo) RETURN n.id AS id")


def test_unknown_label_in_multi_raises_slayer_error() -> None:
    with pytest.raises(SlayerError, match="(?i)unknown"):
        parse_naive_label_filter("MATCH (n:Dataset:Foo) RETURN n.id AS id")


# ---------------------------------------------------------------------------
# Error cases: complex Cypher → requires advanced_search
# ---------------------------------------------------------------------------


def test_complex_cypher_with_where_raises_advanced_search_error() -> None:
    with pytest.raises(SlayerError, match="(?i)advanced_search"):
        parse_naive_label_filter(
            "MATCH (n:Dataset) WHERE n.name = 'orders' RETURN n.id AS id"
        )


def test_complex_cypher_with_relationship_raises() -> None:
    with pytest.raises(SlayerError, match="(?i)advanced_search"):
        parse_naive_label_filter(
            "MATCH (m:Memory)-[:MENTIONS]->(e:Dataset) RETURN m.id AS id"
        )


def test_complex_cypher_multi_clause_raises() -> None:
    with pytest.raises(SlayerError, match="(?i)advanced_search"):
        parse_naive_label_filter(
            "MATCH (m:Memory) MATCH (e:Dataset) RETURN m.id AS id"
        )


def test_bare_match_no_label_raises_advanced_search_error() -> None:
    with pytest.raises(SlayerError, match="(?i)advanced_search"):
        parse_naive_label_filter("MATCH (n) RETURN n.id AS id")


# ---------------------------------------------------------------------------
# Error cases: missing AS id alias
# ---------------------------------------------------------------------------


def test_missing_as_id_raises_slayer_error() -> None:
    """Queries without 'AS id' are invalid even for the naive path."""
    with pytest.raises(SlayerError):
        parse_naive_label_filter("MATCH (n:Dataset) RETURN n.id")


def test_wrong_alias_raises_slayer_error() -> None:
    with pytest.raises(SlayerError):
        parse_naive_label_filter("MATCH (n:Dataset) RETURN n.id AS entity_id")
