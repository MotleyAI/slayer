"""Design D10: ``shift_offset_of`` parses a time_shift key's offset once for planner
and emitter; ``_series_mode`` has one definition in ``compile/shift.py``."""

from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.keys import ColumnKey, TransformKey, shift_offset_of
from slayer.engine.compile import shift, staging


def _key(**kwargs) -> TransformKey:
    return TransformKey(op="time_shift", input=ColumnKey(path=(), leaf="amount"),
                        kwargs=tuple(kwargs.items()))


class TestShiftOffsetOf:
    def test_periods_without_granularity(self) -> None:
        assert shift_offset_of(_key(periods=-1)) == (-1, None)

    def test_periods_with_granularity(self) -> None:
        assert shift_offset_of(_key(periods=-1, granularity="day")) == (-1, "day")

    def test_integral_decimal_accepted(self) -> None:
        assert shift_offset_of(_key(periods=Decimal("-2"))) == (-2, None)

    @pytest.mark.parametrize("bad", [True, Decimal("1.5"), "x"])
    def test_non_integer_periods_rejected(self, bad) -> None:
        key = _key(periods=bad)
        with pytest.raises(ValueError, match="periods"):
            shift_offset_of(key)

    def test_missing_periods_rejected(self) -> None:
        key = _key()
        with pytest.raises(ValueError, match="periods"):
            shift_offset_of(key)


def test_series_mode_has_one_definition() -> None:
    assert staging._series_mode is shift._series_mode
