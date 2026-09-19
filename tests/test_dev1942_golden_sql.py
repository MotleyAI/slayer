"""DEV-1942 task 1.3 — golden SQL for the dual-phase re-aggregation across the seven
Tier-1 dialects.

The ``dual_phase/*`` shapes and ``population/dual_phase_with_filter`` fail closed today
(the standalone+mixed checker guard), so their baseline records a raise; the fix flips
each to SQL (re-blessed through ``ALLOWED_DELTAS``). ``population/mixed_reagg_constituent``
renders today but multiplies its rows through the fanning join; the fix rewrites it to a
correlated EXISTS, another blessed delta.
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.enums import TimeGranularity
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension

from tests._dev1832_fixtures import dev1832_models
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1942_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]
ALLOWED_DELTAS: dict[str, str] = {}

_MONTH_TD = [TimeDimension(dimension=ColumnRef(name="ordered_at"),
                           granularity=TimeGranularity.MONTH)]
_X = "amount:sum(partition_by=[region, ordered_at])"
_STANDALONE = f"min({_X}, partition_by=region)"
_MIXED = f"sum(amount * min({_X}, partition_by=region))"
_POP_MIXED = "sum(spend * avg(spend:sum(partition_by=[tier, plan_code]), partition_by=tier))"
_POP_AVG = "avg(spend:sum(partition_by=[tier, plan_code]), partition_by=tier)"
_OK = "orders.status = 'ok'"


def _cases() -> dict:
    return {
        # dual_phase/* — the standalone+mixed collision; records a raise today.
        "dual_phase/measure": {
            "source": "monthly", "mode": None,
            "kw": {"dimensions": ["region"], "time_dimensions": _MONTH_TD,
                   "measures": [{"formula": _STANDALONE, "name": "a"},
                                {"formula": _MIXED, "name": "b"}]}},
        "dual_phase/filter_position": {
            "source": "monthly", "mode": None,
            "kw": {"dimensions": ["region"], "time_dimensions": _MONTH_TD,
                   "measures": [{"formula": _MIXED, "name": "b"}],
                   "filters": [f"{_STANDALONE} > 5"]}},
        "dual_phase/order_position": {
            "source": "monthly", "mode": None,
            "kw": {"dimensions": ["region"], "time_dimensions": _MONTH_TD,
                   "measures": [{"formula": _STANDALONE, "name": "a"},
                                {"formula": _MIXED, "name": "b"}],
                   "order": [{"column": _STANDALONE, "direction": "desc"}]}},
        "dual_phase/with_transform": {
            "source": "monthly", "mode": None,
            "kw": {"dimensions": ["region"], "time_dimensions": _MONTH_TD,
                   "measures": [{"formula": _STANDALONE, "name": "a"},
                                {"formula": _MIXED, "name": "b"},
                                {"formula": "cumsum(amount:sum)", "name": "c"}]}},
        # population/* — the fanning-hop population filter over a mixed constituent.
        "population/mixed_reagg_constituent": {
            "source": "customers", "mode": None,
            "kw": {"dimensions": ["tier"],
                   "measures": [{"formula": _POP_MIXED, "name": "m"}],
                   "filters": [_OK]}},
        "population/dual_phase_with_filter": {
            "source": "customers", "mode": None,
            "kw": {"dimensions": ["tier"],
                   "measures": [{"formula": _POP_AVG, "name": "a"},
                                {"formula": _POP_MIXED, "name": "m"}],
                   "filters": [_OK]}},
    }


#: The shapes that fail closed today (the standalone+mixed guard); their baseline is a
#: recorded raise until the emission-ordering fix flips them to SQL.
FAIL_CLOSED = {
    "dual_phase/measure", "dual_phase/filter_position", "dual_phase/order_position",
    "dual_phase/with_transform", "population/dual_phase_with_filter",
}


async def _generate_one(case, dialect: str):
    models = dev1832_models()
    try:
        kw = dict(case["kw"])
        if case["mode"] is not None:
            kw["to_many_handling"] = case["mode"]
        query = SlayerQuery(source_model=case["source"], **kw)
        root = next(m for m in models if m.name == case["source"])
        return await _engine_generate(
            query=query, model=root,
            extra_models=[m for m in models if m.name != case["source"]],
            dialect=dialect, validate=False)
    except Exception as exc:  # noqa: BLE001 — the raise itself is the contract
        return record_raise(exc)


bind_golden_tests(
    namespace=globals(),
    golden_path=GOLDEN_PATH,
    cases=_cases,
    dialects=DIALECTS,
    allowed=ALLOWED_DELTAS,
    generate_one=_generate_one,
)


def test_fail_closed_cases_generate_sql(baseline) -> None:
    """The standalone+mixed shapes now execute (DEV-1942): once the emission-ordering
    fix lands they emit SQL, never a recorded raise — a stale raise means the fix
    regressed."""
    for key, value in baseline.items():
        if key.split("::", 1)[0] in FAIL_CLOSED:
            assert not isinstance(value, dict), f"{key} still records a raise: {value}"
