"""``boolean_valued`` — the one boolean-type authority over value keys.

Spec: aggregations/boolean-inputs › "Boolean-valued inputs are recognised by their
value, never their spelling".
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.join_walker import model_column_type
from slayer.core.keys import ColumnKey, ValueKey, boolean_valued, numeric_valued
from slayer.core.scope import ModelScope, StageColumn, StageSchema
from slayer.engine.binding import bind_expr
from slayer.engine.key_metadata import stage_column_type
from slayer.engine.syntax import parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev2046_fixtures import customers_model, orders_model

ORDERS = orders_model()
CUSTOMERS = customers_model()
BUNDLE = ResolvedSourceBundle(dialect="postgres", source_model=ORDERS, referenced_models=[CUSTOMERS])
COLUMN_TYPE = model_column_type(model=ORDERS, models_by_name={"orders": ORDERS, "customers": CUSTOMERS})


def _bind(text: str) -> ValueKey:
    return bind_expr(
        parse_expr(text), scope=ModelScope(source_model=ORDERS), bundle=BUNDLE, allow_measures=True,
    ).value_key


@pytest.mark.parametrize("text,expected", [
    # columns: base, derived, joined
    ("flag", True),
    ("big_order", True),
    ("customers.vip", True),
    ("amount", False),
    ("status", False),
    # literals
    ("true", True),
    ("false", True),
    ("15", False),
    ("'ok'", False),
    # predicates and connectives
    ("amount > 15", True),
    ("amount == 10", True),
    ("amount is None", True),
    ("not flag", True),
    ("flag and amount > 15", True),
    ("flag or big_order", True),
    ("status in ('ok', 'hold')", True),
    ("status not in ('ok', 'hold')", True),
    ("like(status, 'o%')", True),
    ("ordered_at >= '2025-02'", True),
    # conditionals: every returnable branch must be boolean
    ("iif(amount > 15, flag, false)", True),
    ("iif(amount > 15, flag, 1)", False),
    ("iif(flag, 1, 0)", False),
    ("coalesce(flag, false)", True),
    ("coalesce(flag, amount)", False),
    ("ifnull(flag, amount > 15)", True),
    ("ifnull(flag, amount)", False),
    ("greatest(flag, true)", True),
    ("greatest(flag, amount)", False),
    ("least(flag, big_order)", True),
    ("least(amount, 1)", False),
    ("nullif(flag, false)", True),
    ("nullif(amount, 0)", False),
    # aggregates: min / max / first / last follow their source
    ("max(flag)", True),
    ("min(big_order)", True),
    ("max(amount > 15)", True),
    ("first(flag)", True),
    ("last(flag)", True),
    ("max(amount)", False),
    ("sum(flag)", False),
    ("avg(flag)", False),
    ("count(flag)", False),
    ("count_distinct(flag)", False),
    # arithmetic is numeric
    ("amount + 1", False),
    ("iif(flag, amount, 0) * 2", False),
])
def test_recognition(text: str, expected: bool) -> None:
    assert boolean_valued(_bind(text), column_type=COLUMN_TYPE) is expected


@pytest.mark.parametrize("text,expected", [
    ("amount", True),
    ("15", True),
    ("amount + 1", True),
    ("abs(amount)", True),
    ("length(status)", True),
    ("date_part('year', ordered_at)", True),
    ("count(*)", True),
    ("sum(flag)", True),
    ("avg(amount > 15)", True),
    ("max(amount)", True),
    ("coalesce(amount, 0)", True),
    ("iif(flag, amount, false)", True),
    ("flag", False),
    ("max(flag)", False),
    ("status", False),
    ("'1'", False),
    ("ordered_at", False),
    ("coalesce(flag, false)", False),
    ("lower(status)", False),
])
def test_numeric_recognition(text: str, expected: bool) -> None:
    assert numeric_valued(_bind(text), column_type=COLUMN_TYPE) is expected


def test_stage_column() -> None:
    """A stage's BOOLEAN column is boolean-valued through the stage's column types."""
    schema = StageSchema(relation_name="s1", columns=[
        StageColumn(name="fm", sql_alias="fm", type=DataType.BOOLEAN),
        StageColumn(name="fs", sql_alias="fs", type=DataType.INT),
    ])
    column_type = stage_column_type(schema)
    assert boolean_valued(ColumnKey(leaf="fm"), column_type=column_type) is True
    assert boolean_valued(ColumnKey(leaf="fs"), column_type=column_type) is False


def test_unknown_column_type_is_not_boolean() -> None:
    assert boolean_valued(ColumnKey(leaf="flag"), column_type=lambda _key: None) is False
