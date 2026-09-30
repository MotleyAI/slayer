"""Golden SQL for spine queries and custom-granularity bucketing; mechanics in ``tests/_golden_harness.py``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from tests._dev2015_fixtures import DS_GRANULARITIES, JAN_MAR, TWO_FACTS, cg_orders_model, m, spine_models, spine_td
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise
from tests._time_points_fixtures import PinnedClock

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev2015_sql_baseline.json"

DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]

# ``<case_id>::<dialect>`` -> why this entry is allowed to change right now.
ALLOWED_DELTAS: dict[str, str] = {}


def _cases() -> dict[str, dict[str, Any]]:
    """Raw query dicts (a custom granularity must not be parsed at collection)."""
    return {
        "spine/two_facts": {"measures": TWO_FACTS, "time_dimensions": [spine_td()]},
        "spine/per_group": {
            "measures": [m("sum(orders.amount)", "o"), m("sum(returns.amount)", "r")],
            "dimensions": ["customers.region"], "time_dimensions": [spine_td(date_range=JAN_MAR)],
        },
        "spine/quarter_hour": {
            "measures": [m("count(orders.id)", "n")],
            "time_dimensions": [spine_td(granularity="quarter_hour", date_range=["2025-01-01", "2025-01-01"])],
        },
        "custom/fiscal_year": {
            "source_model": "orders", "measures": [m("sum(amount)", "s")],
            "time_dimensions": [{"dimension": "order_date", "granularity": "fiscal_year"}],
        },
    }


async def _generate_one(query: dict[str, Any], dialect: str):
    """Emitted SQL, or a structured record of the raised error."""
    models = [cg_orders_model()] if query.get("source_model") == "orders" else spine_models()
    try:
        return await _engine_generate(
            query=query, model=models[0], extra_models=models[1:], dialect=dialect, validate=False,
            clock=PinnedClock(), datasource_fields=DS_GRANULARITIES,
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


def test_no_case_records_an_error(baseline) -> None:
    assert not [k for k, v in baseline.items() if isinstance(v, dict) and "error" in v]
