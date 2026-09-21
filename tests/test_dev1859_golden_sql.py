"""DEV-1859 golden SQL (task 4.9) — the row-grain + attached-parameter shapes
across the Tier-1 dialects. ``test_param_cases_generate`` guards that no
attached-parameter case regresses to recording a raise.
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import SlayerQuery

from tests._dev1840_fixtures import dev1840_models, signup_month_td
from tests._dev1847_fixtures import INNER_UP_PRODUCT, dev1847_models
from tests._dev1859_fixtures import customers_wsum_models, sales_wsum_models
from tests._dev1919_fixtures import WINDOWED_CUSTOM_PARAM, WINDOWED_PARAM
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1859_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]

_MODEL_SETS = {
    "sales": dev1847_models,
    "sales_wsum": sales_wsum_models,
    "orders": dev1840_models,
    "orders_wsum": customers_wsum_models,
}
_REGION_SUM = "sum(amount, partition_by=region)"
_HEADLINE = ("customers.spend:weighted_avg("
             "weight=sum(amount, partition_by=customers.regions.name))")
_MIXED_TWIN = "sum(customers.spend * sum(amount, partition_by=customers.regions.name))"


def _cases() -> dict:
    return {
        "mixed/headline": {
            "models": "sales", "source": "sales", "mode": None,
            "kw": {"dimensions": ["region"], "measures": [
                {"formula": f"sum(quantity * {INNER_UP_PRODUCT})", "name": "m"}]}},
        "mixed/outer_partition": {
            "models": "sales", "source": "sales", "mode": None,
            "kw": {"dimensions": ["region", "product"], "measures": [
                {"formula": f"sum(quantity * {INNER_UP_PRODUCT}, partition_by=region)",
                 "name": "m"}]}},
        "param/associate": {
            "models": "orders", "source": "orders", "mode": "associate",
            "kw": {"dimensions": ["status"],
                   "measures": [{"formula": _HEADLINE, "name": "w"}]}},
        "param/broadcast": {
            "models": "orders", "source": "orders", "mode": None,
            "kw": {"dimensions": ["status"],
                   "measures": [{"formula": _HEADLINE, "name": "w"}]}},
        "mixed/cross_model_constituent": {
            "models": "orders", "source": "orders", "mode": None,
            "kw": {"dimensions": ["status"],
                   "measures": [{"formula": _MIXED_TWIN, "name": "w"}]}},
        "param/ordinary": {
            "models": "sales_wsum", "source": "sales", "mode": None,
            "kw": {"dimensions": ["region"], "measures": [
                {"formula": f"weighted_avg(amount, weight={_REGION_SUM})",
                 "name": "w"}]}},
        "param/mixed_plus": {
            "models": "sales", "source": "sales", "mode": None,
            "kw": {"dimensions": ["region"], "measures": [
                {"formula": (f"weighted_avg(quantity * {INNER_UP_PRODUCT}, "
                             f"weight={_REGION_SUM})"), "name": "w"}]}},
        "param/literal": {
            "models": "sales_wsum", "source": "sales", "mode": None,
            "kw": {"dimensions": ["region"], "measures": [
                {"formula": f"wsum(1, weight={_REGION_SUM})", "name": "w"}]}},
        "param/ungrained_reagg": {
            "models": "sales_wsum", "source": "sales", "mode": None,
            "kw": {"dimensions": ["region"], "measures": [
                {"formula": ("wsum(sum(amount, partition_by=[city, region]), "
                             "weight=count(id))"), "name": "w"}]}},
        "param/windowed": {
            "models": "orders", "source": "orders", "mode": None,
            "kw": {"time_dimensions": signup_month_td(),
                   "measures": [{"formula": WINDOWED_PARAM, "name": "w"}]}},
        "param/windowed_custom": {
            "models": "orders_wsum", "source": "orders", "mode": None,
            "kw": {"time_dimensions": signup_month_td(),
                   "measures": [{"formula": WINDOWED_CUSTOM_PARAM, "name": "w"}]}},
    }


async def _generate_one(case, dialect: str):
    models = _MODEL_SETS[case["models"]]()
    try:
        kw = dict(case["kw"])
        if case["mode"] is not None:
            kw["to_many_handling"] = case["mode"]
        query = SlayerQuery(source_model=case["source"], **kw)
        return await _engine_generate(
            query=query, model=models[0], extra_models=models[1:],
            dialect=dialect, validate=False,
        )
    except Exception as exc:  # noqa: BLE001 — the raise itself is the contract
        return record_raise(exc)


ALLOWED_DELTAS: dict[str, str] = {
    f"param/associate::{d}": (
        "DEV-1919: the nested parameter's partition key is spelled in producer "
        "coordinates (alias-only)")
    for d in DIALECTS
}

bind_golden_tests(
    namespace=globals(),
    golden_path=GOLDEN_PATH,
    cases=_cases,
    dialects=DIALECTS,
    allowed=ALLOWED_DELTAS,
    generate_one=_generate_one,
)


def test_param_cases_generate(baseline) -> None:
    """Every attached-parameter case generates SQL rather than recording a raise."""
    for key, value in baseline.items():
        if key.startswith("param/"):
            assert not isinstance(value, dict), f"{key} still raises: {value}"
