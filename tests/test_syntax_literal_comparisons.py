"""``core.syntax.literal_comparisons``: string literals compared directly with a column reference, with their spans."""

from __future__ import annotations

import pytest

from slayer.core.syntax import LiteralComparison, literal_comparisons

OP_TEXTS = [
    pytest.param("ordered_at = '2024/01/01'", id="eq"),
    pytest.param("ordered_at == '2024/01/01'", id="double-eq"),
    pytest.param("ordered_at != '2024/01/01'", id="neq"),
    pytest.param("ordered_at <> '2024/01/01'", id="ltgt"),
    pytest.param("ordered_at < '2024/01/01'", id="lt"),
    pytest.param("ordered_at <= '2024/01/01'", id="lte"),
    pytest.param("ordered_at > '2024/01/01'", id="gt"),
    pytest.param("ordered_at >= '2024/01/01'", id="gte"),
    pytest.param("'2024/01/01' = ordered_at", id="eq-reversed"),
    pytest.param("'2024/01/01' == ordered_at", id="double-eq-reversed"),
    pytest.param("'2024/01/01' != ordered_at", id="neq-reversed"),
    pytest.param("'2024/01/01' <> ordered_at", id="ltgt-reversed"),
    pytest.param("'2024/01/01' < ordered_at", id="lt-reversed"),
    pytest.param("'2024/01/01' <= ordered_at", id="lte-reversed"),
    pytest.param("'2024/01/01' > ordered_at", id="gt-reversed"),
    pytest.param("'2024/01/01' >= ordered_at", id="gte-reversed"),
    pytest.param("(ordered_at >= '2024/01/01')", id="parenthesised"),
    pytest.param("('2024/01/01' <= ordered_at) and status = 'ok'", id="parenthesised-in-conjunction"),
]


def _spans(text: str) -> list[tuple[str, str, tuple[str, ...], tuple[bool, ...]]]:
    return [(text[c.start:c.end], c.value, c.parts, c.quoted) for c in literal_comparisons(text)]


@pytest.mark.parametrize("text", OP_TEXTS)
def test_every_comparison_operator_in_either_order(text: str) -> None:
    assert _spans(text)[0] == ("'2024/01/01'", "2024/01/01", ("ordered_at",), (False,))


def test_items_are_pydantic_and_in_source_order() -> None:
    text = "ordered_at >= '2024/01/01' and ordered_at <= '2024/06/30'"
    found = literal_comparisons(text)
    assert all(isinstance(c, LiteralComparison) for c in found)
    assert [c.value for c in found] == ["2024/01/01", "2024/06/30"]
    assert [c.start for c in found] == sorted(c.start for c in found)


def test_each_element_of_an_all_literal_in_list() -> None:
    text = "ordered_at IN ('2024/01/01', '2024/02/01')"
    assert _spans(text) == [
        ("'2024/01/01'", "2024/01/01", ("ordered_at",), (False,)),
        ("'2024/02/01'", "2024/02/01", ("ordered_at",), (False,)),
    ]


def test_each_element_of_an_all_literal_not_in_list() -> None:
    text = "customers.signed_up_at NOT IN ('2024/01/01')"
    assert _spans(text) == [("'2024/01/01'", "2024/01/01", ("customers", "signed_up_at"), (False, False))]


@pytest.mark.parametrize("text", [
    pytest.param("ordered_at IN ('2024/01/01', status)", id="in-with-reference"),
    pytest.param("ordered_at NOT IN ('2024/01/01', status)", id="not-in-with-reference"),
    pytest.param("like(status, 'n%')", id="function-argument"),
    pytest.param("lower(status) = 'ok'", id="call-operand"),
    pytest.param("'a' = 'b'", id="literal-against-literal"),
    pytest.param("ordered_at >= 1", id="numeric-literal"),
    pytest.param("coalesce(ordered_at, '2024/01/01') >= ordered_at", id="literal-inside-call-operand"),
    pytest.param("ordered_at >=", id="unparseable"),
    pytest.param("unknown() = '2024/01/01'", id="unknown-function"),
    pytest.param("ordered_at = '2024/01/01' over (partition by status)", id="raw-window"),
    pytest.param("", id="empty"),
])
def test_not_reported(text: str) -> None:
    assert list(literal_comparisons(text)) == []


def test_function_argument_beside_a_comparison_is_not_reported() -> None:
    text = "ordered_at >= '2024/01/01' and like(status, 'n%')"
    assert _spans(text) == [("'2024/01/01'", "2024/01/01", ("ordered_at",), (False,))]


@pytest.mark.parametrize(("text", "parts", "quoted"), [
    pytest.param('"Regions"."opened_on" >= \'2024/01/01\'', ("Regions", "opened_on"), (True, True), id="all-quoted"),
    pytest.param('"Regions".opened_on >= \'2024/01/01\'', ("Regions", "opened_on"), (True, False), id="hop-quoted"),
    pytest.param('regions."Opened On" >= \'2024/01/01\'', ("regions", "Opened On"), (False, True), id="leaf-quoted"),
    pytest.param('"Opened On" >= \'2024/01/01\'', ("Opened On",), (True,), id="bare-quoted"),
    pytest.param("customers.regions.opened_on >= '2024/01/01'", ("customers", "regions", "opened_on"),
                 (False, False, False), id="three-parts"),
])
def test_reference_parts_carry_per_segment_quoting(text: str, parts: tuple[str, ...], quoted: tuple[bool, ...]) -> None:
    [found] = literal_comparisons(text)
    assert (found.parts, found.quoted) == (parts, quoted)


def test_escaped_quote_span_and_value() -> None:
    text = "status = 'it''s' or ordered_at >= '2024/01/01'"
    assert _spans(text) == [
        ("'it''s'", "it's", ("status",), (False,)),
        ("'2024/01/01'", "2024/01/01", ("ordered_at",), (False,)),
    ]


@pytest.mark.parametrize("text", [
    "ordered_at >= '2024/01/01' and like(status, 'n%')",
    "  ( '2024/01/01'   <=  ordered_at )  ",
    "x IN ('a', 'b') or y <> 'c'",
    "status = 'it''s'",
])
def test_splicing_every_span_back_yields_the_exact_text(text: str) -> None:
    out = text
    for c in sorted(literal_comparisons(text), key=lambda c: c.start, reverse=True):
        written = "'" + c.value.replace("'", "''") + "'"
        assert text[c.start:c.end] == written
        out = out[:c.start] + written + out[c.end:]
    assert out == text
    assert literal_comparisons(text)
