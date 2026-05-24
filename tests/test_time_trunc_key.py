"""Stage 7b.3a (DEV-1450) — TimeTruncKey identity and interning.

Pins the structural-identity contract for the new ``TimeTruncKey`` in
``slayer.core.keys``:

- Identity is by ``(column, granularity)``: two ``TimeTruncKey``s with
  the same underlying ``ColumnKey`` and the same granularity string
  intern to the same slot.
- Different granularities on the same column are STRUCTURALLY distinct
  (``TimeTruncKey(c, 'day') != TimeTruncKey(c, 'month')``).
- Different columns are distinct even with the same granularity.
- Phase is ``ROW``: time truncation is a per-row transformation that
  belongs in the base SELECT / GROUP BY, not in a window / outer SELECT.
- ``ColumnKey.path`` is preserved through ``TimeTruncKey.column`` so
  cross-model time dimensions (``customers.signup_at``) still carry
  the join walk.

This commit is the first sub-stage of stage 7b.3 (time dimensions).
The binder + planner wiring follows in 7b.3b; ``date_range`` -> filter
conversion + main-TD resolution in 7b.3c.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import TimeGranularity
from slayer.core.keys import (
    ColumnKey,
    ColumnSqlKey,
    Phase,
    TimeTruncKey,
    ValueKey,
)


class TestTimeTruncKeyConstruction:
    def test_constructs_from_columnkey_and_granularity_string(self) -> None:
        col = ColumnKey(leaf="ordered_at")
        k = TimeTruncKey(column=col, granularity="month")
        assert k.column == col
        assert k.granularity == "month"

    def test_accepts_timegranularity_enum_as_string(self) -> None:
        """TimeGranularity is a StrEnum, so its members are valid strings.
        Accept the enum value directly."""
        col = ColumnKey(leaf="ordered_at")
        k = TimeTruncKey(
            column=col, granularity=TimeGranularity.MONTH.value
        )
        assert k.granularity == "month"

    def test_preserves_columnkey_path_for_cross_model(self) -> None:
        col = ColumnKey(path=("customers",), leaf="signup_at")
        k = TimeTruncKey(column=col, granularity="day")
        assert k.column.path == ("customers",)
        assert k.column.leaf == "signup_at"


class TestTimeTruncKeyIdentity:
    def test_same_column_and_granularity_intern_equal(self) -> None:
        a = TimeTruncKey(column=ColumnKey(leaf="ordered_at"), granularity="month")
        b = TimeTruncKey(column=ColumnKey(leaf="ordered_at"), granularity="month")
        assert a == b
        assert hash(a) == hash(b)

    def test_different_granularities_on_same_column_distinct(self) -> None:
        col = ColumnKey(leaf="ordered_at")
        a = TimeTruncKey(column=col, granularity="day")
        b = TimeTruncKey(column=col, granularity="month")
        assert a != b
        assert hash(a) != hash(b)

    def test_different_columns_with_same_granularity_distinct(self) -> None:
        a = TimeTruncKey(column=ColumnKey(leaf="ordered_at"), granularity="month")
        b = TimeTruncKey(column=ColumnKey(leaf="shipped_at"), granularity="month")
        assert a != b

    def test_cross_model_paths_with_same_leaf_distinct(self) -> None:
        a = TimeTruncKey(
            column=ColumnKey(path=("customers",), leaf="signup_at"),
            granularity="month",
        )
        b = TimeTruncKey(
            column=ColumnKey(leaf="signup_at"),
            granularity="month",
        )
        assert a != b

    def test_distinct_from_raw_columnkey(self) -> None:
        """A bare ColumnKey for `ordered_at` and TimeTruncKey for the
        same column must not collide as ValueKey identities — they end
        up as separate slots in the ValueRegistry."""
        col = ColumnKey(leaf="ordered_at")
        k_trunc = TimeTruncKey(column=col, granularity="month")
        # Cannot be == across types; that would imply slot collision.
        assert col != k_trunc
        # Hashes may collide by chance but the registry uses dict-key
        # equality, so the strict check is __eq__.

    def test_usable_as_dict_key(self) -> None:
        k = TimeTruncKey(column=ColumnKey(leaf="ordered_at"), granularity="month")
        d = {k: "slot_1"}
        same = TimeTruncKey(column=ColumnKey(leaf="ordered_at"), granularity="month")
        assert d[same] == "slot_1"


class TestTimeTruncKeyPhase:
    def test_phase_is_row(self) -> None:
        k = TimeTruncKey(column=ColumnKey(leaf="ordered_at"), granularity="month")
        assert k.phase == Phase.ROW


class TestTimeTruncKeyImmutability:
    def test_is_frozen(self) -> None:
        k = TimeTruncKey(column=ColumnKey(leaf="ordered_at"), granularity="month")
        with pytest.raises(Exception):
            k.granularity = "day"  # type: ignore[misc]


class TestTimeTruncKeyWithColumnSqlKey:
    """DEV-1450 follow-up #4a: ``TimeTruncKey.column`` is widened to accept
    ``ColumnSqlKey`` so a derived (``Column.sql`` set) temporal column can be
    a time dimension. Identity is structural over the (derived-column, grain)
    pair, exactly like the base-column case."""

    def test_constructs_from_columnsqlkey(self) -> None:
        col = ColumnSqlKey(model="orders", column_name="effective_at")
        k = TimeTruncKey(column=col, granularity="month")
        assert k.column == col
        assert k.granularity == "month"
        assert k.phase == Phase.ROW

    def test_preserves_columnsqlkey_path_for_cross_model(self) -> None:
        col = ColumnSqlKey(
            path=("customers",), model="customers", column_name="effective_at",
        )
        k = TimeTruncKey(column=col, granularity="day")
        assert k.column.path == ("customers",)
        assert k.column.column_name == "effective_at"

    def test_same_derived_column_and_grain_intern_equal(self) -> None:
        a = TimeTruncKey(
            column=ColumnSqlKey(model="orders", column_name="effective_at"),
            granularity="month",
        )
        b = TimeTruncKey(
            column=ColumnSqlKey(model="orders", column_name="effective_at"),
            granularity="month",
        )
        assert a == b
        assert hash(a) == hash(b)

    def test_different_grain_on_derived_column_distinct(self) -> None:
        col = ColumnSqlKey(model="orders", column_name="effective_at")
        a = TimeTruncKey(column=col, granularity="day")
        b = TimeTruncKey(column=col, granularity="month")
        assert a != b

    def test_base_and_derived_columns_distinct(self) -> None:
        a = TimeTruncKey(
            column=ColumnKey(leaf="effective_at"), granularity="month",
        )
        b = TimeTruncKey(
            column=ColumnSqlKey(model="orders", column_name="effective_at"),
            granularity="month",
        )
        assert a != b

    def test_usable_as_dict_key(self) -> None:
        k = TimeTruncKey(
            column=ColumnSqlKey(model="orders", column_name="effective_at"),
            granularity="month",
        )
        d = {k: "slot_1"}
        same = TimeTruncKey(
            column=ColumnSqlKey(model="orders", column_name="effective_at"),
            granularity="month",
        )
        assert d[same] == "slot_1"


class TestValueKeyUnionMembership:
    def test_timetrunckey_in_valuekey_union(self) -> None:
        """ValueKey is a Union; the new key is part of it so binders and
        planners that dispatch on ValueKey see it."""
        from typing import get_args

        members = get_args(ValueKey)
        assert TimeTruncKey in members
