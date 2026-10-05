"""``{name}`` parses to a ``Placeholder`` wherever a ``Literal`` is admitted."""

from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.errors import QueryTypeError, SlayerError, UnresolvedPlaceholderError
from slayer.engine.syntax import (
    AggCall,
    Arith,
    Cmp,
    Literal,
    Placeholder,
    Ref,
    ScalarCall,
    TransformCall,
    UnaryOp,
    canonical_measure_text,
    parse_expr,
    parse_filter_expr,
)

K = Placeholder(name="k")
AMOUNT = Ref(name="amount")
AMOUNT_SUM = AggCall(source=AMOUNT, agg="sum")


@pytest.mark.parametrize(("text", "expected"), [
    ("{k}", K),
    ("amount * {k}", Arith(op="*", left=AMOUNT, right=K)),
    ("{k} + amount", Arith(op="+", left=K, right=AMOUNT)),
    ("-{k}", UnaryOp(op="-", operand=K)),
    ("amount > {k}", Cmp(op=">", left=AMOUNT, right=K)),
    ("{k} < amount", Cmp(op="<", left=K, right=AMOUNT)),
    ("region in ({k})", Cmp(op="in", left=Ref(name="region"), right=K)),
    ("region not in ({k})", Cmp(op="not in", left=Ref(name="region"), right=K)),
    ("sum({k})", AggCall(source=K, agg="sum")),
    ("amount:percentile({k})", AggCall(source=AMOUNT, agg="percentile", args=(K,))),
    ("amount:sum(window={k})", AggCall(source=AMOUNT, agg="sum", kwargs=(("window", K),))),
    ("lag(amount:sum, {k})", TransformCall(op="lag", input=AMOUNT_SUM, args=(K,))),
    ("lag(amount:sum, offset={k})", TransformCall(op="lag", input=AMOUNT_SUM, kwargs=(("offset", K),))),
    ("round(amount, {k})", ScalarCall(name="round", args=(AMOUNT, K))),
    (
        "CASE WHEN amount > {k} THEN {k} ELSE 0 END",
        ScalarCall(name="iif", args=(Cmp(op=">", left=AMOUNT, right=K), K, Literal(value=Decimal(0)))),
    ),
    ("iif(amount > 1, {k}, 0)", ScalarCall(
        name="iif", args=(Cmp(op=">", left=AMOUNT, right=Literal(value=Decimal(1))), K, Literal(value=Decimal(0))),
    )),
    ("CASE WHEN amount > 1 THEN 0 ELSE {k} END", ScalarCall(
        name="iif", args=(Cmp(op=">", left=AMOUNT, right=Literal(value=Decimal(1))), Literal(value=Decimal(0)), K),
    )),
    ("amount:sum * {k}", Arith(op="*", left=AMOUNT_SUM, right=K)),
])
def test_placeholder_position(text: str, expected) -> None:
    assert parse_expr(text) == expected


def test_filter_parser_admits_placeholder() -> None:
    assert parse_filter_expr("amount = {k}") == Cmp(op="==", left=AMOUNT, right=K)


@pytest.mark.parametrize("text", ["amount:sum * {k} / 100", "sum(amount) * {k} / 100"])
def test_canonical_text_renders_placeholder(text: str) -> None:
    assert canonical_measure_text(parse_expr(text)) == "amount:sum * {k} / 100"


@pytest.mark.parametrize("text", ["amount * {a, b}", "amount * {1}", "amount * {a.b}", "amount * {}"])
def test_other_brace_shapes_stay_unsupported(text: str) -> None:
    with pytest.raises(ValueError, match="unsupported AST node"):
        parse_expr(text)


def test_unresolved_placeholder_error_is_a_binding_input_error() -> None:
    assert issubclass(UnresolvedPlaceholderError, SlayerError)
    assert not issubclass(UnresolvedPlaceholderError, QueryTypeError)
