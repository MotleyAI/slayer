"""Queries carrying ``{var}`` placeholders on formula surfaces construct; granularity / column placeholders don't."""

from __future__ import annotations

import re
from typing import Any

import pytest

from slayer.core.query import ComputedDimension, SlayerQuery

_CANNOT_NAME = re.compile(r"can(?:'|no)t name")


def build(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "orders", **kw})


def test_string_dimension_with_placeholder() -> None:
    q = build(dimensions=["amount * {k}"])
    assert q.dimensions == [ComputedDimension(expression="amount * {k}", name="amount_k")]


@pytest.mark.parametrize("column", ["sum(amount) * {k}", "amount:sum * {k}"])
def test_order_expression_with_placeholder(column: str) -> None:
    q = build(order=[{"column": column, "direction": "desc"}])
    assert q.order is not None
    assert q.order[0].raw_formula == column
    assert q.order[0].direction == "desc"


@pytest.mark.parametrize(("date_range", "stored"), [
    (["{start}", None], ["{start}", None]),
    (["{start}", "{end}"], ["{start}", "{end}"]),
    ("{period}", ["{period}"]),
])
def test_placeholder_date_range_bound_accepted(date_range, stored) -> None:
    q = build(time_dimensions=[{"dimension": "ordered_at", "granularity": "month", "date_range": date_range}])
    assert q.time_dimensions is not None
    assert q.time_dimensions[0].date_range == stored


def test_literal_bound_beside_placeholder_still_checked() -> None:
    with pytest.raises(ValueError, match="YYYY-Qn"):
        build(time_dimensions=[{"dimension": "ordered_at", "granularity": "month", "date_range": ["{start}", "2025/01/01"]}])


@pytest.mark.parametrize(("time_dimension", "what"), [
    ({"dimension": "ordered_at", "granularity": "{g}"}, "granularity"),
    ("{g}(ordered_at)", "granularity"),
    ({"dimension": "{col}", "granularity": "month"}, "column"),
    ("month({col})", "column"),
])
def test_granularity_or_column_placeholder_rejected(time_dimension, what: str) -> None:
    with pytest.raises(ValueError) as info:
        build(time_dimensions=[time_dimension])
    msg = str(info.value)
    assert "variable" in msg.lower(), msg
    assert what in msg, msg
    assert _CANNOT_NAME.search(msg), msg


def test_multi_element_set_dimension_still_rejected() -> None:
    with pytest.raises(ValueError, match="unsupported AST node Set"):
        build(dimensions=["amount * {a, b}"])
