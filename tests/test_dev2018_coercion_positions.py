"""The DATE-in-TIMESTAMP-position rule in core, and DATE-typed bounds for a date-time literal."""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.keys import (
    ArithmeticKey,
    FieldKey,
    LiteralKey,
    ScalarCallKey,
    TimeTruncKey,
    ValueKey,
    date_promotion_positions,
    mixed_nullif,
    temporal_pair,
)
from slayer.core.time_points import date_bound

_TYPES = {"d": DataType.DATE, "ts": DataType.DATETIME, "s": DataType.STRING}
D = FieldKey(leaf="d")
TS = FieldKey(leaf="ts")
S = FieldKey(leaf="s")


def _field_type(key: ValueKey) -> Optional[DataType]:
    assert isinstance(key, FieldKey)
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
        assert date_promotion_positions(key, field_type=_field_type) == frozenset(expected)

    @pytest.mark.parametrize(("key", "expected"), [
        (_cmp("==", D, TS), (DataType.DATE, DataType.DATETIME)),
        (_cmp("!=", TS, TS), (DataType.DATETIME, DataType.DATETIME)),
        (_cmp("+", D, TS), None),
        (_cmp("==", D, LiteralKey(value=date(2025, 1, 1))), None),
        (_cmp("==", D, S), None),
    ], ids=repr)
    def test_temporal_pair(self, key: ValueKey, expected) -> None:
        assert temporal_pair(key, field_type=_field_type) == expected

    @pytest.mark.parametrize(("key", "expected"), [
        (_call("nullif", D, TS), 0),
        (_call("nullif", TS, D), 1),
        (_call("nullif", D, D), None),
        (_call("coalesce", D, TS), None),
    ], ids=repr)
    def test_mixed_nullif(self, key: ValueKey, expected: Optional[int]) -> None:
        assert mixed_nullif(key, field_type=_field_type) == expected


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
