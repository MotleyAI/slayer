"""DEV-1800 — golden SQL baseline for change/change_pct (any transform) over a
cross-model inner combined into a composite, across the standard five-dialect
matrix (harness: tests/_golden_harness.py), on the DEV-1750 models.

The ``fc/*`` cases are the failing composite class: they are recorded as raises
today and become SQL after the change (each such flip is an ALLOWED_DELTAS entry
approved with executed-value parity during implementation, per tasks.md §5.2).
The ``named/*`` and ``pos/*`` cases already render and are pinned byte-for-byte
against regression (the "already-legal shapes keep their SQL" scenario); any
placement divergence the stage rule produces is an approved delta. The ``alt/*``
cases are the D10 alternation class (a transform over a composite over a
transform), pinned the same way — raises where they stall today.
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import SlayerQuery

from tests._dev1750_fixtures import dev1750_models
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1800_sql_baseline.json"

DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]

# ``<case_id>::<dialect>`` -> why this entry is allowed to change right now.
# A PENDING list, not a log: a committed state always has this empty.
ALLOWED_DELTAS: dict[str, str] = {}

_MONTH = [{"dimension": "ordered_at", "granularity": "month"}]
CM = "customers.spend:sum"


def _orders(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    kw.setdefault("time_dimensions", _MONTH)
    return SlayerQuery(**kw)


def _cases() -> dict:
    """The matrix. Keys are stable ids — renaming one is a golden change."""
    return {
        # Failing composite class — measure position (recorded as raises today).
        "fc/change_plus_sum": _orders(
            measures=[{"formula": f"change({CM}) + amount:sum", "name": "c"}]),
        "fc/change_plus_count": _orders(
            measures=[{"formula": f"change({CM}) + *:count", "name": "c"}]),
        "fc/change_plus_max": _orders(
            measures=[{"formula": f"change({CM}) + amount:max", "name": "c"}]),
        "fc/time_shift_plus_sum": _orders(
            measures=[{"formula": f"time_shift({CM}, -1) + amount:sum", "name": "c"}]),
        "fc/cumsum_plus_sum": _orders(
            measures=[{"formula": f"cumsum({CM}) + amount:sum", "name": "c"}]),
        "fc/change_wscaled_plus_sum": _orders(
            measures=[{"formula": "change(amount:wscaled_sum) + amount:sum", "name": "c"}]),
        "fc/iif_over_change": _orders(
            measures=[{"formula": f"iif(change({CM}) > 0, {CM}, amount:sum)", "name": "c"}]),
        "fc/change_plus_sum_by_status": _orders(
            dimensions=["status"],
            measures=[{"formula": f"change({CM}) + amount:sum", "name": "c"}]),
        # The issue's named single-transform shapes (already render).
        "named/change_wscaled": _orders(
            measures=[{"formula": "change(amount:wscaled_sum)", "name": "d"}]),
        "named/change_pct_wscaled": _orders(
            measures=[{"formula": "change_pct(amount:wscaled_sum)", "name": "d"}]),
        "named/change_cm": _orders(
            measures=[{"formula": f"change({CM})", "name": "d"}]),
        "named/change_pct_cm": _orders(
            measures=[{"formula": f"change_pct({CM})", "name": "d"}]),
        # Composite in filter / order positions (post-wrapper path; placement).
        "pos/filter_composite": _orders(
            dimensions=["status"],
            filters=[f"change({CM}) + amount:sum > 0"],
            measures=[{"formula": "amount:sum", "name": "a"}]),
        "pos/order_composite": _orders(
            dimensions=["status"],
            measures=[{"formula": "amount:sum", "name": "a"}],
            order=[{"column": f"change({CM}) + amount:sum", "direction": "desc"}]),
        # Filter placement by mask typing: row mask (base WHERE) vs measure mask
        # (HAVING) — the already-legal placements the stage rule must preserve.
        "pos/row_mask": _orders(
            dimensions=["status"], filters=["status = 'ok'"],
            measures=[{"formula": "amount:sum", "name": "a"}]),
        "pos/measure_mask": _orders(
            dimensions=["status"], filters=["amount:sum > 10"],
            measures=[{"formula": "amount:sum", "name": "a"}]),
        # Windowed-value filter (a ranked cross-model value) — D5 places it at the
        # combined outer WHERE; the "with transform" variant is pos/filter_composite.
        "pos/windowed_value_filter": _orders(
            dimensions=["status"],
            filters=["customers.spend:last(customers.signup_at) > 50"],
            measures=[{"formula": "amount:sum", "name": "a"}]),
        # Transform over a composite over a transform (D10 alternation class).
        "alt/last_over_change": _orders(
            measures=[{"formula": "last(change(amount:sum))", "name": "t"}]),
        "alt/last_over_change_filtered": _orders(
            dimensions=["status"],
            measures=[{"formula": "change(amount:sum)", "name": "ch"}],
            filters=["last(change(amount:sum)) < 0"]),
        "alt/last_over_change_cumsum": _orders(
            measures=[{"formula": "last(change(cumsum(amount:sum)))", "name": "t"}]),
        # Derived composites at two different levels — pins the one fused
        # trailing composite step (D8/D10) byte-for-byte.
        "alt/two_level_composites": _orders(
            measures=[{"formula": "change(amount:sum)", "name": "ch1"},
                      {"formula": "change(cumsum(amount:sum))", "name": "ch2"}]),
    }


async def _generate_one(query: SlayerQuery, dialect: str):
    """Emitted SQL, or a structured record of the raised error."""
    models = dev1750_models()
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
