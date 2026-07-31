"""Pure cardinality-inference helpers (DEV-1688).

These functions are dialect/DB-free and encode the evidence model:

* ``is_key_set_unique`` — a join key-set is unique iff some PK/unique key-set is
  a NON-EMPTY SUBSET of it (if ``(a)`` is unique then ``(a, b)`` is unique;
  a superset constraint does NOT imply it).
* ``classify_cardinality`` — total function mapping the two sides' uniqueness to
  a definite cardinality. Used by the data-profiling path (both booleans are
  observed facts).
* ``infer_structural_cardinality`` — the ingest-time guess. It returns ``None``
  unless the target key-set is VERIFIED unique (Codex #1): a declared
  relationship whose target isn't a known PK/unique must stay undetermined.
"""

from slayer.core.enums import JoinCardinality
from slayer.engine.cardinality import (
    classify_cardinality,
    infer_structural_cardinality,
    is_key_set_unique,
)


# ---------------------------------------------------------------------------
# is_key_set_unique — subset, not exact/superset
# ---------------------------------------------------------------------------


def test_single_column_exact_match_is_unique() -> None:
    assert is_key_set_unique(key_columns=["id"], unique_key_sets=[["id"]]) is True


def test_composite_key_with_subset_constraint_is_unique() -> None:
    # (org_id) unique => (org_id, code) unique.
    assert (
        is_key_set_unique(
            key_columns=["org_id", "code"], unique_key_sets=[["org_id"]]
        )
        is True
    )


def test_composite_constraint_exactly_covering_is_unique() -> None:
    assert (
        is_key_set_unique(
            key_columns=["org_id", "code"], unique_key_sets=[["org_id", "code"]]
        )
        is True
    )


def test_superset_constraint_does_not_imply_unique() -> None:
    # A unique constraint on (org_id, code) does NOT make (org_id) alone unique.
    assert (
        is_key_set_unique(key_columns=["org_id"], unique_key_sets=[["org_id", "code"]])
        is False
    )


def test_unrelated_constraint_is_not_unique() -> None:
    assert is_key_set_unique(key_columns=["a", "b"], unique_key_sets=[["c"]]) is False


def test_no_constraints_is_not_unique() -> None:
    assert is_key_set_unique(key_columns=["a"], unique_key_sets=[]) is False


def test_subset_match_is_order_independent() -> None:
    assert (
        is_key_set_unique(
            key_columns=["code", "org_id"], unique_key_sets=[["org_id"]]
        )
        is True
    )


# ---------------------------------------------------------------------------
# classify_cardinality — total, data-profiling classification
# ---------------------------------------------------------------------------


def test_classify_both_unique_is_one_to_one() -> None:
    assert (
        classify_cardinality(source_unique=True, target_unique=True)
        is JoinCardinality.ONE_TO_ONE
    )


def test_classify_target_unique_source_not_is_many_to_one() -> None:
    assert (
        classify_cardinality(source_unique=False, target_unique=True)
        is JoinCardinality.MANY_TO_ONE
    )


def test_classify_source_unique_target_not_is_one_to_many() -> None:
    assert (
        classify_cardinality(source_unique=True, target_unique=False)
        is JoinCardinality.ONE_TO_MANY
    )


def test_classify_neither_unique_is_many_to_many() -> None:
    assert (
        classify_cardinality(source_unique=False, target_unique=False)
        is JoinCardinality.MANY_TO_MANY
    )


# ---------------------------------------------------------------------------
# infer_structural_cardinality — honest ingest-time guess
# ---------------------------------------------------------------------------


def test_structural_target_verified_unique_source_not_is_many_to_one() -> None:
    assert (
        infer_structural_cardinality(source_unique=False, target_verified_unique=True)
        is JoinCardinality.MANY_TO_ONE
    )


def test_structural_both_unique_is_one_to_one() -> None:
    assert (
        infer_structural_cardinality(source_unique=True, target_verified_unique=True)
        is JoinCardinality.ONE_TO_ONE
    )


def test_structural_target_not_verified_returns_none() -> None:
    # Declared relationship with no known PK/unique on the target: undetermined.
    assert (
        infer_structural_cardinality(source_unique=True, target_verified_unique=False)
        is None
    )
    assert (
        infer_structural_cardinality(source_unique=False, target_verified_unique=False)
        is None
    )
