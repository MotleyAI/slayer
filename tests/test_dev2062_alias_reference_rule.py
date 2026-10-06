"""Alias mode reads any key whose slot is available by that slot; else a composite renders structurally."""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

import pytest
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import TimeGranularity
from slayer.core.errors import RenderContextMissingFacilityError
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    LiteralKey,
    ScalarCallKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
)
from slayer.sql.dialects import get_dialect
from slayer.sql.generator import SQLGenerator
from slayer.sql.render.value_expr import AliasFacilities, RenderContext, render_value_key

_SUM = AggregateKey(source=ColumnKey(leaf="amount"), agg="sum")
_COUNT = AggregateKey(source=StarKey(), agg="count")
_RATIO = ArithmeticKey(op="/", operands=(_SUM, _COUNT))
_ABS = ScalarCallKey(name="abs", args=(_SUM,))
_COMPOSITES = {"ratio": _RATIO, "scalar_call": _ABS}

_OPERAND_ALIASES = {"s_sum": "orders.amount_sum", "s_count": "orders._count"}


def _ctx(
    *,
    slots: dict,
    available: Optional[dict] = None,
    values: Optional[dict] = None,
) -> RenderContext:
    return RenderContext(
        dialect=get_dialect("postgres"),
        aliases=AliasFacilities(
            slot_id_by_key=slots,
            available_alias_by_slot_id=available or {},
            value_by_slot_id=values or {},
        ),
    )


def _emit(node: Expression) -> str:
    return node.sql(dialect="postgres")


@pytest.mark.parametrize("label", sorted(_COMPOSITES))
def test_available_composite_reads_its_alias(label) -> None:
    key = _COMPOSITES[label]
    out = render_value_key(key=key, ctx=_ctx(slots={key: "s_c"}, available={"s_c": "orders.c"}))
    assert isinstance(out, exp.Column), _emit(out)
    assert out.name == "orders.c", _emit(out)


def test_available_composite_wins_over_available_operands() -> None:
    ctx = _ctx(
        slots={_RATIO: "s_c", _SUM: "s_sum", _COUNT: "s_count"},
        available={"s_c": "orders.c", **_OPERAND_ALIASES},
    )
    assert _emit(render_value_key(key=_RATIO, ctx=ctx)) == '"orders.c"'


def test_composite_slot_value_reads_that_value() -> None:
    value = exp.column("aov", table="_cm_x", quoted=True)
    ctx = _ctx(
        slots={_RATIO: "s_c", _SUM: "s_sum", _COUNT: "s_count"},
        available=_OPERAND_ALIASES,
        values={"s_c": value},
    )
    assert _emit(render_value_key(key=_RATIO, ctx=ctx)) == _emit(value)


def test_unavailable_interned_composite_renders_structurally() -> None:
    ctx = _ctx(
        slots={_RATIO: "s_c", _SUM: "s_sum", _COUNT: "s_count"},
        available=_OPERAND_ALIASES,
    )
    out = _emit(render_value_key(key=_RATIO, ctx=ctx))
    assert '"orders.amount_sum"' in out, out
    assert '"orders._count"' in out, out


def test_unslotted_composite_renders_structurally() -> None:
    ctx = _ctx(slots={_SUM: "s_sum", _COUNT: "s_count"}, available=_OPERAND_ALIASES)
    out = _emit(render_value_key(key=_RATIO, ctx=ctx))
    assert '"orders.amount_sum"' in out, out
    assert '"orders._count"' in out, out


_SLOTTED = {
    "column": ColumnKey(leaf="amount"),
    "time_trunc": TimeTruncKey(column=ColumnKey(leaf="order_date"), granularity=TimeGranularity.MONTH),
    "aggregate": _SUM,
    "transform": TransformKey(op="cumsum", input=_RATIO),
}


@pytest.mark.parametrize("label", sorted(_SLOTTED))
def test_unavailable_slotted_kind_still_raises(label) -> None:
    key = _SLOTTED[label]
    ctx = _ctx(slots={key: "s1"})
    with pytest.raises(RenderContextMissingFacilityError):
        render_value_key(key=key, ctx=ctx)


def test_unslotted_literal_renders_as_literal() -> None:
    out = render_value_key(key=LiteralKey(value=Decimal(1)), ctx=_ctx(slots={}))
    assert isinstance(out, exp.Literal), _emit(out)
    assert _emit(out) == "1"


def test_available_literal_slot_reads_its_alias() -> None:
    key = LiteralKey(value=Decimal(1))
    out = render_value_key(key=key, ctx=_ctx(slots={key: "s_l"}, available={"s_l": "orders.one"}))
    assert _emit(out) == '"orders.one"'


def test_computed_dimension_special_case_is_retired() -> None:
    assert "composite_alias_slot_ids" not in AliasFacilities.model_fields
    assert not hasattr(SQLGenerator, "_dimension_composite_slot_ids")
