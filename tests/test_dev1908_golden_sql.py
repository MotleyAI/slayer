"""DEV-1908 golden SQL — the reverse-hop-cancellation shapes across the seven
Tier-1 dialects, plus the fail-closed ones recorded as raises. On the current
tree most cancellation cases fail closed and are recorded as raises; the
spec-implement stage re-blesses them to SQL per the divergence protocol
(``SLAYER_UPDATE_GOLDEN=1``). DEV-1892 / 1900 / 1931 goldens stay untouched.
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.models import ModelMeasure
from slayer.core.query import SlayerQuery

from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise
from tests._dev1908_fixtures import (
    dev1908_models,
    ship_region_extension,
    signup_month_td,
)

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1908_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]


def _m(formula: str) -> list[ModelMeasure]:
    return [ModelMeasure(formula=formula, name="w")]


def _cases() -> dict:
    G = "customers.regions.countries.gdp"
    return {
        "cancel/single": {"kw": {"measures": _m(f"{G}:wsum_region_pop")}},
        "cancel/double": {"kw": {"measures": _m(f"{G}:wsum_cust_spend2")}},
        "cancel/forward": {"kw": {"measures": _m(f"{G}:wsum_plan_fee")}},
        "cancel/two_frame": {"kw": {"measures": _m(f"{G}:wsum_two")}},
        "cancel/events_reverse_hop": {
            "kw": {"measures": _m("customers.regions.region_events.value:wsum_rp")}},
        "cancel/owner_at_root": {"kw": {"measures": _m("amount:wself")}},
        "cancel/association_expr": {
            "kw": {"dimensions": ["status"], "to_many_handling": "associate",
                   "measures": _m(f"{G}:wsum_region_pop_expr")}},
        "cancel/windowed_expr": {
            "kw": {"time_dimensions": signup_month_td(),
                   "measures": _m(f"{G}:wsum_cust_spend2_expr(window='1y')")}},
        "cancel/ship_region_extension": {
            "ext": True,
            "kw": {"measures": _m("ship_region.countries.gdp:wsum_region_pop")}},
        "cancel/second_order_root_frame": {
            "kw": {"measures": _m(
                "wavg_pop_expr(sum(amount, partition_by=customers.regions.id))")}},
        "closed/cancel_then_fanning": {"kw": {"measures": _m(f"{G}:wsum_fan")}},
        "closed/second_order_nowhere": {
            "kw": {"measures": _m("wnowhere(sum(amount, partition_by=status))")}},
        "closed/query_typed_revisit": {
            "kw": {"measures": _m("customers.regions.customers.spend:sum")}},
    }


async def _generate_one(case, dialect: str):
    models = dev1908_models()
    try:
        source = ship_region_extension() if case.get("ext") else "orders"
        query = SlayerQuery(source_model=source, **case["kw"])
        return await _engine_generate(
            query=query, model=models[0], extra_models=models[1:],
            dialect=dialect, validate=False)
    except Exception as exc:  # noqa: BLE001 — the raise itself is the contract
        return record_raise(exc)


ALLOWED_DELTAS: dict[str, str] = {}

bind_golden_tests(
    namespace=globals(),
    golden_path=GOLDEN_PATH,
    cases=_cases,
    dialects=DIALECTS,
    allowed=ALLOWED_DELTAS,
    generate_one=_generate_one,
)
