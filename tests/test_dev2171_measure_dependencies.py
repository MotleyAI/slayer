"""``measure_dependency_problems`` checks aggregation inputs when given the model's columns and join paths."""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from slayer.core.syntax import measure_dependency_problems

COLUMNS = frozenset({"amount", "status"})


def _joins(parts: Sequence[str]) -> bool:
    return tuple(parts) == ("customers", "discount")


def _problem(formula: str) -> str | None:
    (problem,) = measure_dependency_problems([("m", formula)], columns=COLUMNS, join_path=_joins)
    return problem


@pytest.mark.parametrize("formula", [
    "sum(amount)", "count(*)", "sum(amount * 2)", "count(status) FILTER (WHERE status = 'paid')",
    "sum(customers.discount)", "sum(amount) / count(*)",
])
def test_resolvable_inputs(formula: str) -> None:
    assert _problem(formula) is None


@pytest.mark.parametrize(("formula", "name"), [
    ("sum(missing)", "missing"), ("sum(amount * qty)", "qty"), ("sum(customers.nope)", "customers.nope"),
    ("sum(amount) / max(missing)", "missing"), ("count(status) FILTER (WHERE flag = 1)", "flag"),
])
def test_unresolvable_input_names_the_reference(formula: str, name: str) -> None:
    problem = _problem(formula)
    assert problem is not None
    assert name in problem


def test_without_columns_aggregation_inputs_are_not_checked() -> None:
    assert measure_dependency_problems([("m", "sum(missing)")]) == [None]


def test_a_peer_with_an_unresolvable_input_poisons_its_dependants() -> None:
    problems = measure_dependency_problems([("a", "sum(missing)"), ("b", "a * 2")], columns=COLUMNS,
                                           join_path=_joins)
    assert problems[0] is not None
    assert problems[1] is not None
