"""DEV-1798 — a transform measure projected under two names surfaces BOTH.

The step-CTE emission used to record only ``public_aliases[0]``, so the outer
SELECT re-emitted the first name for the second occurrence and silently dropped
it. Assertions here are structural (outermost-SELECT projection via sqlglot):
each alias also legitimately appears in step CTEs and the inner SELECT, so
whole-SQL substring counts would mislead.
"""

from __future__ import annotations

import asyncio

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.errors import DuplicateMetricNameError
from slayer.core.query import SlayerQuery

from tests._dev1747_fixtures import dev1747_datasets
from tests._engine_helpers import _engine_generate

_MONTH = [{"field": "created_at", "granularity": "month"}]


def _generate(metrics: list[dict], dialect: str = "postgres") -> str:
    models = dev1747_datasets()
    query = SlayerQuery(
        population="orders",
        time_dimensions=_MONTH,
        metrics=metrics,
    )
    return asyncio.run(_engine_generate(
        query=query, dataset=models[0], extra_datasets=models[1:],
        dialect=dialect, validate=False,
    ))


def _outer_projection(sql: str, dialect: str = "postgres") -> list[str]:
    """Output names of the OUTERMOST SELECT."""
    outer = sqlglot.parse_one(sql, dialect=dialect)
    assert isinstance(outer, exp.Select), f"outermost node is not SELECT:\n{sql}"
    return [e.alias_or_name for e in outer.expressions]


def _cte(sql: str, name: str, dialect: str = "postgres") -> exp.Select:
    for cte in sqlglot.parse_one(sql, dialect=dialect).find_all(exp.CTE):
        if cte.alias == name:
            return cte.this
    raise AssertionError(f"no CTE named {name!r} in:\n{sql}")


def _cte_projection(sql: str, name: str, dialect: str = "postgres") -> list[str]:
    return [e.alias_or_name for e in _cte(sql, name, dialect).expressions]


@pytest.mark.parametrize("dialect", ["postgres", "sqlite"])
def test_transform_two_names_both_surface(dialect: str) -> None:
    sql = _generate([
        {"metric": "cumsum(sum(orders.amount))", "alias": "run_a"},
        {"metric": "cumsum(sum(orders.amount))", "alias": "run_b"},
    ], dialect=dialect)
    assert _outer_projection(sql, dialect) == [
        "orders.created_at", "run_a", "run_b",
    ]


@pytest.mark.parametrize("dialect", ["postgres", "sqlite"])
def test_post_arithmetic_two_names_both_surface(dialect: str) -> None:
    # change(...) lowers to an ArithmeticKey materialised by the
    # unmaterialised-POST branch of the same step-CTE emission.
    sql = _generate([
        {"metric": "change(sum(orders.amount))", "alias": "ch_a"},
        {"metric": "change(sum(orders.amount))", "alias": "ch_b"},
    ], dialect=dialect)
    assert _outer_projection(sql, dialect) == [
        "orders.created_at", "ch_a", "ch_b",
    ]


def test_two_names_carried_through_later_step() -> None:
    # change(...) forces a step2, so both names must be carried through the
    # intermediate CTEs, not just materialised in their own step.
    sql = _generate([
        {"metric": "cumsum(sum(orders.amount))", "alias": "run_a"},
        {"metric": "cumsum(sum(orders.amount))", "alias": "run_b"},
        {"metric": "change(sum(orders.amount))", "alias": "ch"},
    ])
    step2 = _cte(sql, "step2")
    step2_names = [e.alias_or_name for e in step2.expressions]
    assert "run_a" in step2_names
    assert "run_b" in step2_names
    # The CTE step2 selects from must itself carry both (sqlglot does not
    # resolve columns, so step2's projection alone would not prove it).
    feeder = step2.args["from_"].this.name
    feeder_names = _cte_projection(sql, feeder)
    assert "run_a" in feeder_names
    assert "run_b" in feeder_names
    assert _outer_projection(sql) == [
        "orders.created_at", "run_a", "run_b", "ch",
    ]


@pytest.mark.parametrize("dialect", ["postgres", "sqlite"])
def test_consecutive_periods_two_names_both_surface(dialect: str) -> None:
    # consecutive_periods has its own cp_reset/cp_value CTE path, distinct from
    # _emit_step_cte; it must surface both names too (Codex review of #311).
    sql = _generate([
        {"metric": "consecutive_periods(sum(orders.amount))", "alias": "streak_a"},
        {"metric": "consecutive_periods(sum(orders.amount))", "alias": "streak_b"},
    ], dialect=dialect)
    assert _outer_projection(sql, dialect) == [
        "orders.created_at", "streak_a", "streak_b",
    ]


def test_cross_model_transform_two_names_both_surface() -> None:
    sql = _generate([
        {"metric": "cumsum(sum(customers.spend))", "alias": "run_a"},
        {"metric": "cumsum(sum(customers.spend))", "alias": "run_b"},
    ])
    assert _outer_projection(sql) == [
        "orders.created_at", "run_a", "run_b",
    ]


def test_single_name_emission_unchanged() -> None:
    sql = _generate([{"metric": "cumsum(sum(orders.amount))", "alias": "cs"}])
    assert _outer_projection(sql) == ["orders.created_at", "cs"]
    assert _cte_projection(sql, "step1").count("cs") == 1


def test_same_name_same_formula_raises() -> None:
    with pytest.raises(
        ValueError, match="Stage field name collision on 'run_a'",
    ):
        _generate([
            {"metric": "cumsum(sum(orders.amount))", "alias": "run_a"},
            {"metric": "cumsum(sum(orders.amount))", "alias": "run_a"},
        ])


def test_same_name_different_formula_raises() -> None:
    with pytest.raises(DuplicateMetricNameError):
        _generate([
            {"metric": "cumsum(sum(orders.amount))", "alias": "run_a"},
            {"metric": "sum(orders.amount)", "alias": "run_a"},
        ])
