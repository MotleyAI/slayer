"""Golden SQL baseline for change/change_pct (any transform) over a
cross-model inner combined into a composite, across the standard five-dialect
matrix (harness: tests/_golden_harness.py), on the ``_dev1750_fixtures`` models.

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

from tests._dev1750_fixtures import dev1750_datasets
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1800_sql_baseline.json"

DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]

# ``<case_id>::<dialect>`` -> why this entry is allowed to change right now.
# A PENDING list, not a log: a committed state always has this empty.
ALLOWED_DELTAS: dict[str, str] = {}

_MONTH = [{"field": "orders.ordered_at", "granularity": "month"}]
CM = "sum(customers.spend)"


def _orders(**kw) -> SlayerQuery:
    kw.setdefault("population", "orders")
    kw.setdefault("time_dimensions", _MONTH)
    return SlayerQuery(**kw)


def _cases() -> dict:
    """The matrix. Keys are stable ids — renaming one is a golden change."""
    return {
        # Failing composite class — measure position (recorded as raises today).
        "fc/change_plus_sum": _orders(
            to_many_handling="broadcast",
            metrics=[{"metric": f"change({CM}) + sum(orders.amount)", "alias": "c"}]),
        "fc/change_plus_count": _orders(
            to_many_handling="broadcast",
            metrics=[{"metric": f"change({CM}) + count(orders.*)", "alias": "c"}]),
        "fc/change_plus_max": _orders(
            to_many_handling="broadcast",
            metrics=[{"metric": f"change({CM}) + max(orders.amount)", "alias": "c"}]),
        "fc/time_shift_plus_sum": _orders(
            to_many_handling="broadcast",
            metrics=[{"metric": f"time_shift({CM}, -1) + sum(orders.amount)", "alias": "c"}]),
        "fc/cumsum_plus_sum": _orders(
            to_many_handling="broadcast",
            metrics=[{"metric": f"cumsum({CM}) + sum(orders.amount)", "alias": "c"}]),
        "fc/change_wscaled_plus_sum": _orders(
            metrics=[{"metric": "change(wscaled_sum(orders.amount)) + sum(orders.amount)", "alias": "c"}]),
        "fc/iif_over_change": _orders(
            to_many_handling="broadcast",
            metrics=[{"metric": f"iif(change({CM}) > 0, {CM}, sum(orders.amount))", "alias": "c"}]),
        "fc/change_plus_sum_by_status": _orders(
            to_many_handling="broadcast",
            dimensions=["orders.status"],
            metrics=[{"metric": f"change({CM}) + sum(orders.amount)", "alias": "c"}]),
        # The issue's named single-transform shapes (already render).
        "named/change_wscaled": _orders(
            metrics=[{"metric": "change(wscaled_sum(orders.amount))", "alias": "d"}]),
        "named/change_pct_wscaled": _orders(
            metrics=[{"metric": "change_pct(wscaled_sum(orders.amount))", "alias": "d"}]),
        "named/change_cm": _orders(
            to_many_handling="broadcast",
            metrics=[{"metric": f"change({CM})", "alias": "d"}]),
        "named/change_pct_cm": _orders(
            to_many_handling="broadcast",
            metrics=[{"metric": f"change_pct({CM})", "alias": "d"}]),
        # Composite in filter / order positions (post-wrapper path; placement).
        "pos/filter_composite": _orders(
            to_many_handling="broadcast",
            dimensions=["orders.status"],
            filter=[f"change({CM}) + sum(orders.amount) > 0"],
            metrics=[{"metric": "sum(orders.amount)", "alias": "a"}]),
        "pos/order_composite": _orders(
            to_many_handling="broadcast",
            dimensions=["orders.status"],
            metrics=[{"metric": "sum(orders.amount)", "alias": "a"}],
            order=[{"field": f"change({CM}) + sum(orders.amount)", "direction": "desc"}]),
        # Filter placement by mask typing: row mask (base WHERE) vs measure mask
        # (HAVING) — the already-legal placements the stage rule must preserve.
        "pos/row_mask": _orders(
            dimensions=["orders.status"], filter=["orders.status = 'ok'"],
            metrics=[{"metric": "sum(orders.amount)", "alias": "a"}]),
        "pos/measure_mask": _orders(
            dimensions=["orders.status"], filter=["sum(orders.amount) > 10"],
            metrics=[{"metric": "sum(orders.amount)", "alias": "a"}]),
        # Windowed-value filter (a ranked cross-model value) — D5 places it at the
        # combined outer WHERE; the "with transform" variant is pos/filter_composite.
        "pos/windowed_value_filter": _orders(
            to_many_handling="broadcast",
            dimensions=["orders.status"],
            filter=["last(customers.spend, customers.signup_at) > 50"],
            metrics=[{"metric": "sum(orders.amount)", "alias": "a"}]),
        # Transform over a composite over a transform (D10 alternation class).
        "alt/last_over_change": _orders(
            metrics=[{"metric": "last(change(sum(orders.amount)))", "alias": "t"}]),
        "alt/last_over_change_filtered": _orders(
            dimensions=["orders.status"],
            metrics=[{"metric": "change(sum(orders.amount))", "alias": "ch"}],
            filter=["last(change(sum(orders.amount))) < 0"]),
        "alt/last_over_change_cumsum": _orders(
            metrics=[{"metric": "last(change(cumsum(sum(orders.amount))))", "alias": "t"}]),
        # Derived composites at two different levels — pins the one fused
        # trailing composite step (D8/D10) byte-for-byte.
        "alt/two_level_composites": _orders(
            metrics=[{"metric": "change(sum(orders.amount))", "alias": "ch1"},
                      {"metric": "change(cumsum(sum(orders.amount)))", "alias": "ch2"}]),
    }


async def _generate_one(query: SlayerQuery, dialect: str):
    """Emitted SQL, or a structured record of the raised error."""
    models = dev1750_datasets()
    try:
        return await _engine_generate(
            query=query, dataset=models[0], extra_datasets=models[1:],
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
