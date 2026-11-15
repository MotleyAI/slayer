"""The DATE-in-TIMESTAMP-position rule in core, and DATE-typed bounds for a date-time literal."""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.keys import (
    ArithmeticKey,
    ColumnKey,
    LiteralKey,
    ScalarCallKey,
    TimeTruncKey,
    ValueKey,
    date_promotion_positions,
    mixed_nullif,
    temporal_pair,
)
from slayer.core.time_points import date_bound

_TYPES = {"d": DataType.DATE, "ts": DataType.TIMESTAMP, "s": DataType.TEXT}
D = ColumnKey(leaf="d")
TS = ColumnKey(leaf="ts")
S = ColumnKey(leaf="s")


def _column_type(key: ValueKey) -> Optional[DataType]:
    assert isinstance(key, ColumnKey)
    return _TYPES[key.leaf]


def _cmp(op: str, left: ValueKey, right: ValueKey) -> ArithmeticKey:
    return ArithmeticKey(op=op, operands=(left, right))


def _call(name: str, *args: ValueKey) -> ScalarCallKey:
    return ScalarCallKey(name=name, args=args)


class TestPositions:
    @pytest.mark.parametrize(("key", "expected"), [
        (_cmp("==", D, TS), {0}),
        (_cmp("<", TS, D), {1}),
        (_cmp("is not distinct from", D, TS), {0}),
        (_cmp("==", TimeTruncKey(source=TS, granularity=TimeGranularity.DAY), D), {1}),
        (_cmp("==", TimeTruncKey(source=D, granularity=TimeGranularity.HOUR), D), {1}),
        (_cmp("==", D, D), set()),
        (_cmp("==", TS, TS), set()),
        (_cmp("==", D, LiteralKey(value=datetime(2025, 1, 1))), set()),
        (_cmp("==", S, TS), set()),
        (_call("coalesce", TS, D), {1}),
        (_call("greatest", D, D), set()),
        (_call("iif", _cmp("==", S, S), D, TS), {1}),
        (_call("nullif", D, TS), set()),
    ], ids=repr)
    def test_date_promotion_positions(self, key: ValueKey, expected: set[int]) -> None:
        assert date_promotion_positions(key, column_type=_column_type) == frozenset(expected)

    @pytest.mark.parametrize(("key", "expected"), [
        (_cmp("==", D, TS), (DataType.DATE, DataType.TIMESTAMP)),
        (_cmp("!=", TS, TS), (DataType.TIMESTAMP, DataType.TIMESTAMP)),
        (_cmp("+", D, TS), None),
        (_cmp("==", D, LiteralKey(value=date(2025, 1, 1))), None),
        (_cmp("==", D, S), None),
    ], ids=repr)
    def test_temporal_pair(self, key: ValueKey, expected) -> None:
        assert temporal_pair(key, column_type=_column_type) == expected

    @pytest.mark.parametrize(("key", "expected"), [
        (_call("nullif", D, TS), 0),
        (_call("nullif", TS, D), 1),
        (_call("nullif", D, D), None),
        (_call("coalesce", D, TS), None),
    ], ids=repr)
    def test_mixed_nullif(self, key: ValueKey, expected: Optional[int]) -> None:
        assert mixed_nullif(key, column_type=_column_type) == expected


class TestDateBound:
    @pytest.mark.parametrize(("op", "value", "expected"), [
        (">=", datetime(2024, 5, 15, 6), (">=", date(2024, 5, 16))),
        (">=", datetime(2024, 5, 15), (">=", date(2024, 5, 15))),
        ("<", datetime(2024, 5, 15, 6), ("<", date(2024, 5, 16))),
        ("<", datetime(2024, 5, 15), ("<", date(2024, 5, 15))),
        (">", datetime(2024, 5, 15), (">=", date(2024, 5, 16))),
        (">", datetime(2024, 5, 15, 6), (">=", date(2024, 5, 16))),
        ("<=", datetime(2024, 5, 15), ("<", date(2024, 5, 16))),
        ("<=", datetime(2024, 5, 15, 6), ("<", date(2024, 5, 16))),
        ("=", datetime(2024, 5, 15), ("=", date(2024, 5, 15))),
        ("=", datetime(2024, 5, 15, 6), None),
    ])
    def test_exact_for_every_date(self, op: str, value: datetime, expected) -> None:
        assert date_bound(op, value) == expected
