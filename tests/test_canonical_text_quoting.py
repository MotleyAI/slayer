"""Canonical expression text keeps identifier quoting (spec: queries/expression-grammar)."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import AsyncIterator
from typing import Any

import pytest
from pydantic import BaseModel

from slayer.core.query import QueryRefinement, SlayerQuery, refine_query
from slayer.core.syntax import (
    DottedRef,
    Ref,
    canonical_metric_text,
    legacy_result_name_seed,
    metric_text_at_path,
    parse_expr,
    quote_blind_metric_text,
    walk_parsed,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.naming import expression_text_alias
from tests._dev2215_fixtures import BACKENDS, dev2215_engine, q


def _refs(node: Any) -> list[tuple[tuple[str, ...], tuple[bool, ...]]]:
    """Every reference's segments and per-segment quoting."""
    out = []
    for n in walk_parsed(node):
        if isinstance(n, Ref):
            out.append(((n.name,), (n.quoted,)))
        elif isinstance(n, DottedRef):
            out.append((n.parts, tuple(n.quoted_at(i) for i in range(len(n.parts)))))
    return out


def _dump(value: Any) -> Any:
    """The full parse tree, quoting included; a deferred library error only by presence (it quotes the text)."""
    if isinstance(value, BaseModel):
        fields = {k: _dump(getattr(value, k)) for k in type(value).model_fields}
        if "library_error" in fields:
            fields["library_error"] = fields["library_error"] is not None
        return (type(value).__name__, fields)
    if isinstance(value, tuple):
        return tuple(_dump(v) for v in value)
    return value


def _assert_reads_back(text: str) -> str:
    parsed = parse_expr(text)
    rendered = canonical_metric_text(parsed)
    again = parse_expr(rendered)
    assert _dump(again) == _dump(parsed), rendered
    assert canonical_metric_text(again) == rendered
    return rendered


# --------------------------------------------------------------------------- #
# Written quoting survives
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("text", ['lower("REGIONS".lbl)', '"Regions"."Lbl"', "lower(REGIONS.lbl)"])
def test_a_quoted_segment_keeps_its_quotes(text: str) -> None:
    assert _assert_reads_back(text) == text


@pytest.mark.parametrize("text", [
    'date_part("year", d)', 'datediff("day", a, b)', 'lower("x" = 1)', '"null"', '"true"', '"false"', 'not "not"',
])
def test_a_quoted_word_stays_a_column(text: str) -> None:
    _assert_reads_back(text)


@pytest.mark.parametrize("text", [
    '"a b".c', 'sum(x) FILTER (WHERE "y" = 1)', 'AGG("Rev")', 'count("Regions".*)', '"Regions".amount + x',
    '"*"', 'x."*"', 'AGG("*")',
])
def test_quoting_reads_back_in_every_position(text: str) -> None:
    assert _assert_reads_back(text) == text


@pytest.mark.parametrize("text", ["lower(REGIONS.lbl)", "x.null", "a.b.not", "a.true.y"])
def test_an_unquoted_segment_stays_bare(text: str) -> None:
    assert _assert_reads_back(text) == text


# --------------------------------------------------------------------------- #
# Built segments are quoted only where the bare spelling would not read back
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("path", "expected"), [
    (("null",), '"null".amount'),
    (("a", "null"), "a.null.amount"),
    (("true",), '"true".amount'),
    (("false",), '"false".amount'),
    (("a", "not"), "a.not.amount"),
    (("customers",), "customers.amount"),
])
def test_a_built_reserved_word_segment_is_quoted_only_where_it_must_be(path: tuple[str, ...], expected: str) -> None:
    text = metric_text_at_path("amount", path=path)
    assert text == expected
    assert parse_expr(text) == DottedRef(parts=(*path, "amount"))


def test_a_method_call_twin_rendered_mid_parse_keeps_quoting() -> None:
    # The twin renders inside a parse action, so the read-back probe runs mid-parse (it once deadlocked); a fresh
    # interpreter has cold caches and is killed on a hang.
    code = ("from slayer.core.syntax import legacy_spans; "
            "print([s.twin for s in legacy_spans('h.sum(\"Amount\")')])")
    done = subprocess.run([sys.executable, "-W", "ignore", "-c", code], capture_output=True, text=True, timeout=120,
                          check=True)
    assert done.stdout.strip() == str(['sum(h."Amount")'])


def test_a_built_reserved_word_head_under_a_star_source() -> None:
    text = metric_text_at_path("count(*)", path=("null",))
    assert text == 'count("null".*)'
    assert _refs(parse_expr(text)) == [(("null", "*"), (True, False))]


# --------------------------------------------------------------------------- #
# Identity and naming stay quote-blind
# --------------------------------------------------------------------------- #
def test_quote_only_time_dimension_variants_are_one() -> None:
    query = q(time_dimensions=[
        {"field": "customers.created_at", "granularity": "month", "alias": "m"},
        {"field": '"customers".created_at', "granularity": "month", "alias": "m"},
    ])
    assert len(query.time_dimensions or []) == 1


def test_quote_only_expression_time_dimension_variants_are_one() -> None:
    query = q(time_dimensions=[
        {"field": "coalesce(customers.created_at, shipped_at)", "granularity": "month", "alias": "m"},
        {"field": 'coalesce("customers".created_at, shipped_at)', "granularity": "month", "alias": "m"},
    ])
    assert len(query.time_dimensions or []) == 1


@pytest.mark.parametrize(("saved", "refined"), [
    ("count(customers.id)", 'count("customers".id)'),
    ("sum(amount) FILTER (WHERE status == 'ok')", "sum(amount) FILTER (WHERE \"status\" = 'ok')"),
])
def test_a_refinement_restating_an_unnamed_measure_by_quoting_adds_nothing(saved: str, refined: str) -> None:
    base = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": saved}]})
    merged = refine_query(saved=base, refinement=QueryRefinement.model_validate({"metrics": [{"metric": refined}]}))
    assert [m.metric for m in merged.metrics or []] == [saved]


@pytest.mark.parametrize(("plain", "quoted"), [
    ("count(customers.id)", 'count("customers".id)'),
    ("count(customers.*)", 'count("customers".*)'),
    ("sum(amount)", 'sum("amount")'),
    ("sum(amount) FILTER (WHERE status = 'ok')", "sum(amount) FILTER (WHERE \"status\" = 'ok')"),
])
def test_quoting_leaves_the_generated_result_name_unchanged(plain: str, quoted: str) -> None:
    assert expression_text_alias(parse_expr(quoted)) == expression_text_alias(parse_expr(plain))
    assert legacy_result_name_seed(parse_expr(quoted)) == legacy_result_name_seed(parse_expr(plain))


def test_quoting_a_compared_argument_name_leaves_its_identity_unchanged() -> None:
    assert quote_blind_metric_text(parse_expr('sum(("x" = 1))')) == quote_blind_metric_text(parse_expr("sum((x = 1))"))


@pytest.fixture(params=BACKENDS)
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with dev2215_engine(request.param) as eng:
        yield eng


async def test_a_quoted_measure_keeps_its_result_key(engine: SlayerQueryEngine) -> None:
    quoted = await engine.execute(q(metrics=['count("customers".id)']))
    plain = await engine.execute(q(metrics=["count(customers.id)"]))
    assert quoted.columns == plain.columns
    assert quoted.data == plain.data
