"""Errors for `name = value` in the wrong call kind say how the call reads it."""

import pytest

from slayer.core.errors import AggregationArgumentError, TransformArgumentError, UnknownReferenceError
from slayer.core.scope import ModelScope
from slayer.core.syntax import parse_expr
from slayer.engine.binding import bind_expr
from slayer.ir.source_bundle import ResolvedSourceBundle
from tests._dev1737_fixtures import customers_model, orders_model


def _bind(text: str) -> None:
    model = orders_model()
    bundle = ResolvedSourceBundle(dialect="postgres", source_model=model, referenced_models=[customers_model()])
    bind_expr(parse_expr(text), scope=ModelScope(source_model=model), bundle=bundle, allow_measures=True)


@pytest.mark.parametrize(("expr", "error"), [
    ("sum(amount, is_paid = true)", AggregationArgumentError),
    ("percentile(amount, q = 0.9)", AggregationArgumentError),
    ("rank(sum(amount), is_paid = true)", TransformArgumentError),
])
def test_keyword_slot_suggests_double_equals(expr: str, error: type[Exception]) -> None:
    with pytest.raises(error, match="== …"):
        _bind(expr)


@pytest.mark.parametrize(("expr", "error"), [
    ("date_diff('day', start=created_at, end=shipped_at)", UnknownReferenceError),
    ("now(tz='UTC')", ValueError),
    ("date_add(created_at, n=1, unit='day')", ValueError),
])
def test_scalar_call_says_arguments_are_positional(expr: str, error: type[Exception]) -> None:
    with pytest.raises(error, match="positional arguments"):
        _bind(expr)


def test_double_equals_gets_no_positional_hint() -> None:
    with pytest.raises(UnknownReferenceError) as info:
        _bind("coalesce(bogus == 1, false)")
    assert "positional" not in str(info.value)


def test_spelling_is_not_identity() -> None:
    assert parse_expr("coalesce(status = 'x', false)") == parse_expr("coalesce(status == 'x', false)")
