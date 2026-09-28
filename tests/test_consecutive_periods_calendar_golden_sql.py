"""Golden SQL for calendar-aware ``consecutive_periods`` (and sub-day
``time_shift``); mechanics in ``tests/_golden_harness.py``. Also asserts every
comparison renders only in a condition position.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.query import SlayerQuery

from tests._consecutive_periods_calendar_fixtures import calendar_models, orders_query
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise


GOLDEN_PATH = Path(__file__).parent / "golden" / "consecutive_periods_calendar_sql_baseline.json"

DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]

# ``<case_id>::<dialect>`` -> why this entry is allowed to change right now.
# A PENDING list, not a log: a committed state always has this empty.
ALLOWED_DELTAS: dict[str, str] = {}

_STREAK = "consecutive_periods(sum(amount) > 100)"


def _cases() -> dict:
    """The matrix. Keys are stable ids — renaming one is a golden change."""
    return {
        "cp/month": orders_query(formula=_STREAK),
        "cp/grouped": orders_query(formula=_STREAK, dimensions=["customer_id"], filters=[]),
        "cp/quarter": orders_query(formula=_STREAK, granularity="quarter"),
        "cp/week_sunday": orders_query(formula=_STREAK, granularity="week_sunday"),
        "cp/hour": orders_query(formula=_STREAK, granularity="hour"),
        "cp/date_range": orders_query(
            formula=_STREAK, date_range=["2025-02-01", "2025-06-30"]),
        "cp/over_change": orders_query(
            formula="consecutive_periods(change(sum(amount)) > 0)"),
        "cp/over_lag": orders_query(formula="consecutive_periods(lag(sum(amount)) > 100)"),
        "cp/under_lag": orders_query(formula=f"lag({_STREAK})"),
        "ts/hour": orders_query(formula="time_shift(sum(amount), -1)", granularity="hour"),
    }


async def _generate_one(query: SlayerQuery, dialect: str):
    """Emitted SQL, or a structured record of the raised error."""
    models = calendar_models()
    try:
        return await _engine_generate(
            query=query, model=models[0], extra_models=models[1:],
            dialect=dialect, validate=False,
        )
    except Exception as exc:  # noqa: BLE001 — the exception itself is contract
        return record_raise(exc)


bind_golden_tests(
    namespace=globals(),
    golden_path=GOLDEN_PATH,
    cases=_cases,
    dialects=DIALECTS,
    allowed=ALLOWED_DELTAS,
    generate_one=_generate_one,
)


_COMPARISONS = (exp.GT, exp.GTE, exp.LT, exp.LTE, exp.EQ, exp.NEQ)
_CONNECTIVES = (exp.And, exp.Or, exp.Not, exp.Paren)


def _in_condition_position(node: exp.Expression) -> bool:
    child, parent = node, node.parent
    while isinstance(parent, _CONNECTIVES):
        child, parent = parent, parent.parent
    if isinstance(parent, exp.If):
        return child is parent.this
    if isinstance(parent, (exp.Where, exp.Having, exp.Qualify)):
        return True
    return isinstance(parent, exp.Join) and child is parent.args.get("on")


@pytest.mark.parametrize("case_id", sorted(_cases()))
@pytest.mark.parametrize("dialect", DIALECTS)
def test_comparisons_only_in_condition_positions(case_id: str, dialect: str) -> None:
    sql = asyncio.run(_generate_one(_cases()[case_id], dialect))
    assert isinstance(sql, str), sql
    comparisons = list(sqlglot.parse_one(sql, dialect=dialect).find_all(*_COMPARISONS))
    if case_id.startswith("cp/"):
        assert any(isinstance(n, exp.GT) for n in comparisons), sql
    misplaced = [n.sql(dialect=dialect) for n in comparisons if not _in_condition_position(n)]
    assert not misplaced, misplaced
