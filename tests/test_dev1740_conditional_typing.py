"""DEV-1740 Part A — conditional branch-type inference (Postgres semantics).

Identical branches keep their type; a numeric mix widens; a NULL branch is
absorbed by the other; any other mix is a plan-time error naming both types.
Number-format propagates only when both branches agree.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.keys import (
    conditional_number_format,
    join_conditional_branch_types,
)


class TestBranchTypeJoin:
    @pytest.mark.parametrize("t", [DataType.INT, DataType.TEXT, DataType.BOOLEAN,
                                   DataType.DATE, DataType.TIMESTAMP, DataType.DOUBLE])
    def test_identical_types_pass_through(self, t: DataType) -> None:
        assert join_conditional_branch_types(t, t) == t

    @pytest.mark.parametrize("a,b", [
        (DataType.INT, DataType.DOUBLE),
        (DataType.DOUBLE, DataType.INT),
    ])
    def test_numeric_mix_widens_to_double(self, a: DataType, b: DataType) -> None:
        assert join_conditional_branch_types(a, b) == DataType.DOUBLE

    @pytest.mark.parametrize("other", [DataType.INT, DataType.TEXT, DataType.DATE])
    def test_null_branch_is_absorbed(self, other: DataType) -> None:
        # ``None`` marks a NULL-literal branch; it takes the other branch's type.
        assert join_conditional_branch_types(other, None) == other
        assert join_conditional_branch_types(None, other) == other

    @pytest.mark.parametrize("a,b", [
        (DataType.INT, DataType.TEXT),
        (DataType.TEXT, DataType.INT),   # both orders
        (DataType.DATE, DataType.INT),
        (DataType.BOOLEAN, DataType.DOUBLE),
    ])
    def test_incomparable_mix_raises_naming_both(self, a: DataType, b: DataType) -> None:
        with pytest.raises(ValueError) as ei:
            join_conditional_branch_types(a, b)
        msg = str(ei.value)
        assert a.value in msg and b.value in msg


class TestFormatPropagation:
    def test_identical_format_propagates(self) -> None:
        fmt = NumberFormat(type=NumberFormatType.CURRENCY, symbol="USD")
        assert conditional_number_format(fmt, fmt) == fmt

    def test_differing_formats_drop(self) -> None:
        assert conditional_number_format(
            NumberFormat(type=NumberFormatType.CURRENCY, symbol="USD"),
            NumberFormat(type=NumberFormatType.PERCENT),
        ) is None

    def test_missing_format_drops(self) -> None:
        assert conditional_number_format(
            NumberFormat(type=NumberFormatType.CURRENCY, symbol="USD"), None,
        ) is None
