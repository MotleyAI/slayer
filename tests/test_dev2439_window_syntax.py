"""``core.syntax``'s window-reading mode: the window tree, its traversal, and the native refusal it leaves unchanged.

Spec: queries/ossie-l3-binding › The filter is an Ossie expression.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Any

import pytest

from slayer.core import syntax
from slayer.core.errors import IllegalWindowInFilterError, UnknownFunctionError
from slayer.core.syntax import (
    AggCall, BoundVariable, Cmp, DottedRef, Literal, Ref, ScalarCall, TransformCall, bind_variables, call_spans,
    legacy_spans, nonossie_spellings, parse_bound_expr, parse_expr, walk_parsed,
)

_NATIVE_MESSAGE = (
    "IllegalWindowInFilterError: Window expressions are not allowed in filters.\n"
    "  expr: {expr!r}\n"
    "  source: raw OVER(...) is not allowed in Mode-B DSL\n"
    "  suggestion: use a transform instead (rank, percent_rank, dense_rank, ntile, cumsum, lag, lead, time_shift, …)."
)
NATIVE_REFUSED = ["x OVER (", "sum(x) FILTER (WHERE c) OVER (PARTITION BY r)", "ROW_NUMBER() OVER (ORDER BY x)"]


def _window(text: str) -> Any:
    return parse_expr(text, windows=True)


# --------------------------------------------------------------------------- #
# The tree
# --------------------------------------------------------------------------- #
def test_the_window_tree_shape() -> None:
    node = _window("ROW_NUMBER() OVER (PARTITION BY a.p, a.q ORDER BY a.o DESC NULLS LAST, a.r) = 1")
    assert isinstance(node, Cmp)
    assert node.right == Literal(value=Decimal(1))
    window = node.left
    assert isinstance(window, syntax.WindowCall)
    assert isinstance(window.function, ScalarCall)
    assert (window.function.name.lower(), window.function.args) == ("row_number", ())
    assert window.partition == (DottedRef(parts=("a", "p")), DottedRef(parts=("a", "q")))
    assert window.order == (
        syntax.OrderItem(value=DottedRef(parts=("a", "o")), descending=True, nulls="last"),
        syntax.OrderItem(value=DottedRef(parts=("a", "r")), descending=False, nulls=None),
    )
    assert window.frame is None


def test_an_empty_window() -> None:
    window = _window("RANK() OVER ()")
    assert isinstance(window, syntax.WindowCall)
    assert (window.partition, window.order, window.frame) == ((), (), None)


def test_ascending_and_nulls_first() -> None:
    window = _window("RANK() OVER (ORDER BY x ASC NULLS FIRST)")
    assert isinstance(window, syntax.WindowCall)
    assert window.order == (syntax.OrderItem(value=Ref(name="x"), descending=False, nulls="first"),)


@pytest.mark.parametrize(("frame_text", "unit", "start", "end"), [
    ("ROWS BETWEEN 6 PRECEDING AND CURRENT ROW", "rows", ("preceding", Decimal(6)), ("current row", None)),
    ("RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING", "range", ("unbounded preceding", None),
     ("unbounded following", None)),
    ("ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING", "rows", ("current row", None), ("following", Decimal(2))),
    ("RANGE UNBOUNDED PRECEDING", "range", ("unbounded preceding", None), None),
    ("ROWS 3 PRECEDING", "rows", ("preceding", Decimal(3)), None),
])
def test_frame_bounds(frame_text: str, unit: str, start: tuple[str, Decimal | None],
                      end: tuple[str, Decimal | None] | None) -> None:
    window = _window(f"SUM(x) OVER (ORDER BY y {frame_text})")
    assert isinstance(window, syntax.WindowCall)
    assert window.frame == syntax.WindowFrame(unit=unit, start=start, end=end)


def test_a_frame_offset_is_never_expanded_to_its_digits() -> None:
    # An ``int`` offset would make ``1e1000000000`` allocate its billion digits.
    window = _window("ROW_NUMBER() OVER (ORDER BY y ROWS 1e30 PRECEDING)")
    assert isinstance(window, syntax.WindowCall)
    assert window.frame is not None
    assert type(window.frame.start[1]) is Decimal


def test_walk_reaches_every_reference_in_a_window() -> None:
    found = {n.parts for n in walk_parsed(_window("LAG(a.x, 1) OVER (PARTITION BY a.p ORDER BY a.o)"))
             if isinstance(n, DottedRef)}
    assert found == {("a", "x"), ("a", "p"), ("a", "o")}


@pytest.mark.parametrize(("windowed", "plain"), [
    ("COUNT(*) OVER ()", "COUNT(*)"),
    ("sum(x) FILTER (WHERE c > 1) OVER (PARTITION BY r)", "sum(x) FILTER (WHERE c > 1)"),
    ("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x) OVER ()", "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)"),
    ("count(DISTINCT x) OVER ()", "count(DISTINCT x)"),
])
def test_a_windowed_aggregate_wraps_the_plain_reading(windowed: str, plain: str) -> None:
    window = _window(windowed)
    assert isinstance(window, syntax.WindowCall)
    assert isinstance(window.function, AggCall)
    assert window.function == parse_expr(plain)


@pytest.mark.parametrize(("name", "args"), [
    ("ROW_NUMBER", ""), ("RANK", ""), ("DENSE_RANK", ""), ("PERCENT_RANK", ""), ("CUME_DIST", ""), ("NTILE", "4"),
    ("LAG", "x"), ("LEAD", "x, 1, 0"), ("FIRST_VALUE", "x"), ("LAST_VALUE", "x"), ("NTH_VALUE", "x, 2"),
])
def test_ossie_window_names_are_not_dispatched(name: str, args: str) -> None:
    window = _window(f"{name}({args}) OVER (ORDER BY y)")
    assert isinstance(window, syntax.WindowCall)
    assert isinstance(window.function, ScalarCall)
    assert window.function.name.lower() == name.lower()


def test_without_over_a_call_reads_as_natively() -> None:
    for text in ("lag(x)", "rank(x)", "sum(x) > 1"):
        assert _window(text) == parse_expr(text)
    assert isinstance(_window("lag(x)"), TransformCall)


@pytest.mark.parametrize("text", [
    "ROW_NUMBER() OVER (",
    "x OVER ()",
    "ROW_NUMBER() OVER () OVER ()",
    "sum(x) OVER () FILTER (WHERE c)",
    "ROW_NUMBER() OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING)",
    "ROW_NUMBER() OVER (ORDER BY x GROUPS UNBOUNDED PRECEDING)",
    "ROW_NUMBER() OVER (PARTITION x)",
    "FIRST_VALUE(*) OVER ()",
    "LAG(*) OVER (ORDER BY y)",
])
def test_a_malformed_window_does_not_parse(text: str) -> None:
    with pytest.raises(ValueError) as raised:
        _window(text)
    assert not isinstance(raised.value, IllegalWindowInFilterError)


def test_an_unknown_window_function_is_unknown() -> None:
    with pytest.raises(UnknownFunctionError):
        _window("FOO() OVER ()")


# --------------------------------------------------------------------------- #
# Variables and the text readers
# --------------------------------------------------------------------------- #
def test_variables_bind_inside_and_beside_a_window() -> None:
    text = "x IN {vals} AND LAG(y, {k}) OVER (ORDER BY z) = {n}"
    variables = {"vals": ["a", "b"], "k": 1, "n": 2}
    bound = bind_variables(text, variables, windows=True)
    assert bound == "x IN {vals} AND LAG(y, 1) OVER (ORDER BY z) = 2"
    tree = parse_bound_expr(bound, variables, windows=True)
    nodes = list(walk_parsed(tree))
    assert any(isinstance(n, BoundVariable) and n.values == ("a", "b") for n in nodes)
    assert any(isinstance(n, syntax.WindowCall) for n in nodes)


@pytest.mark.parametrize(("reader", "text", "found"), [
    (nonossie_spellings, "a ~ '^c' AND ROW_NUMBER() OVER (ORDER BY y) = 1", lambda r: r == ("~",)),
    (legacy_spans, "a == 1 AND ROW_NUMBER() OVER (ORDER BY y) = 1", lambda r: [s.written for s in r] == ["=="]),
    (call_spans, "abs(a) > 1 AND ROW_NUMBER() OVER (ORDER BY y) = 1", lambda r: "abs" in [c.callee for c in r]),
])
def test_the_text_readers_see_beside_a_window(reader: Callable[..., Any], text: str,
                                              found: Callable[[Any], bool]) -> None:
    assert reader(text) == ()
    assert found(reader(text, windows=True))


# --------------------------------------------------------------------------- #
# The native reading is unchanged
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("text", NATIVE_REFUSED)
def test_native_parse_refuses_a_window_as_before(text: str) -> None:
    with pytest.raises(IllegalWindowInFilterError) as raised:
        parse_expr(text)
    assert str(raised.value) == _NATIVE_MESSAGE.format(expr=text)


@pytest.mark.parametrize("text", NATIVE_REFUSED)
def test_native_binding_refuses_a_window_as_before(text: str) -> None:
    template = f"{text} = {{n}}"
    with pytest.raises(IllegalWindowInFilterError) as raised:
        bind_variables(template, {"n": 1})
    assert str(raised.value) == _NATIVE_MESSAGE.format(expr=template)


def _native_refuses(text: str) -> None:
    with pytest.raises(IllegalWindowInFilterError):
        parse_expr(text)


def _strict_reads(text: str) -> None:
    assert any(isinstance(n, syntax.WindowCall) for n in walk_parsed(_window(text)))


@pytest.mark.parametrize("order", [(_native_refuses, _strict_reads), (_strict_reads, _native_refuses)],
                         ids=["native-first", "strict-first"])
def test_the_readings_alternate_independently(order: tuple[Callable[[str], None], ...]) -> None:
    text = f"DENSE_RANK() OVER (ORDER BY alternating_{order[0].__name__}) = 1"
    for _ in range(2):
        for read in order:
            read(text)
