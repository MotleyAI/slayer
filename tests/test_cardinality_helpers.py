"""Pure, DB-free cardinality-inference helpers."""

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import Column
from slayer.engine.cardinality import (
    classify_cardinality,
    declares_solo_unique,
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


# ---------------------------------------------------------------------------
# declares_solo_unique — a composite-PK member claims nothing on its own
# ---------------------------------------------------------------------------


def _c(name: str, *, pk: bool = False, unique: bool = False) -> Column:
    return Column(name=name, type=DataType.INT, primary_key=pk, unique=unique)


def test_solo_pk_column_declares_uniqueness() -> None:
    cols = [_c("id", pk=True), _c("amount")]
    assert declares_solo_unique(columns=cols, column=cols[0]) is True


def test_composite_pk_member_does_not_declare_uniqueness() -> None:
    # PK (id, sku): every member is stamped primary_key, but neither is unique
    # alone — the same subset rule is_key_set_unique applies.
    cols = [_c("id", pk=True), _c("sku", pk=True), _c("cost")]
    assert declares_solo_unique(columns=cols, column=cols[0]) is False
    assert declares_solo_unique(columns=cols, column=cols[1]) is False


def test_explicit_unique_flag_declares_uniqueness_even_with_composite_pk() -> None:
    # `unique` is single-column by definition, so it stands on its own
    # regardless of how many PK columns the model has.
    cols = [_c("id", pk=True), _c("sku", pk=True), _c("email", unique=True)]
    assert declares_solo_unique(columns=cols, column=cols[2]) is True


def test_plain_column_declares_nothing() -> None:
    cols = [_c("id", pk=True), _c("amount")]
    assert declares_solo_unique(columns=cols, column=cols[1]) is False
