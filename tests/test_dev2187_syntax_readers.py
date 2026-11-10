"""The ``core.syntax`` readers every Mode-B consumer goes through."""

from __future__ import annotations

import pytest

from slayer.core.syntax import (
    drop_root_qualifier,
    parse_expr,
    referenced_paths,
    root_relative,
    text_paths,
    variable_names,
)


@pytest.mark.parametrize(("parts", "expected"), [
    (("orders", "status"), ("status",)),
    (("orders", "customers", "name"), ("customers", "name")),
    (("customers", "name"), ("customers", "name")),
    (("customers", "orders", "id"), ("customers", "orders", "id")),
    (("status",), ("status",)),
    (("orders",), ("orders",)),
    (("reorders", "status"), ("reorders", "status")),
])
def test_drop_root_qualifier(parts: tuple[str, ...], expected: tuple[str, ...]) -> None:
    assert tuple(drop_root_qualifier(parts, "orders")) == expected


@pytest.mark.parametrize(("text", "expected"), [
    ("sum(orders.amount)", "sum(amount)"),
    ("orders.customers.score + 1", "customers.score + 1"),
    ("count(orders.*)", "count(*)"),
    ("date_trunc('month', orders.created_at)", "date_trunc('month', created_at)"),
    ("note = 'orders.x'", "note = 'orders.x'"),
    ('"orders.x" = 1', '"orders.x" = 1'),
    ("count(customers.orders.id)", "count(customers.orders.id)"),
    ("orders.amount > {x}", "amount > {x}"),
])
def test_root_relative(text: str, expected: str) -> None:
    assert root_relative(parse_expr(text), "orders") == parse_expr(expected)


@pytest.mark.parametrize(("text", "heads"), [
    ("sum(orders.amount) + count(customers.id)", {"orders", "customers"}),
    ("note = 'qb.x' AND amount > 1", {"note", "amount"}),
    ("note = 'time_spine.x'", {"note"}),
    ('"time_spine.x" = 1', {"time_spine.x"}),
    ("time_spine.timestamp >= '2025-01-01'", {"time_spine"}),
])
def test_referenced_paths_never_read_quoted_text(text: str, heads: set[str]) -> None:
    assert {tuple(path)[0] for path in referenced_paths(parse_expr(text))} == heads


@pytest.mark.parametrize(("text", "paths"), [
    ("orders.status = 'a'", [("status",)]),
    ("orders.time_spine.timestamp > 1", [("time_spine", "timestamp")]),
    ("sum(orders.events.amount)", [("events", "amount")]),
    ("orders.orders.name = 'a'", [("orders", "name")]),
    ("time_spine.timestamp >= date '2025-01-01' + interval '{n} days'", [("time_spine", "timestamp")]),
])
def test_text_paths_read_the_template_root_relative(text: str, paths: list[tuple[str, ...]]) -> None:
    assert text_paths(text, root="orders") == paths


@pytest.mark.parametrize(("text", "names"), [
    ("note = '{x}' AND amount > {y}", {"x", "y"}),
    ("note = '{{x}}'", set()),
    ("regexp_like(note, '[0-9]{3}')", set()),
    ("note = '{1, 2}'", set()),
    ("note = 'plain'", set()),
])
def test_variable_names_are_binding_lookups(text: str, names: set[str]) -> None:
    assert set(variable_names(text)) == names
