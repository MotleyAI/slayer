"""Golden SQL — the row-grain + attached-parameter shapes
across the Tier-1 dialects. ``test_param_cases_generate`` guards that no
attached-parameter case regresses to recording a raise.
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import SlayerQuery

from tests._dev1840_fixtures import dev1840_datasets, signup_month_td
from tests._dev1847_fixtures import INNER_UP_PRODUCT, dev1847_datasets
from tests._dev1859_fixtures import customers_wsum_datasets, sales_wsum_datasets
from tests._dev1919_fixtures import WINDOWED_CUSTOM_PARAM, WINDOWED_PARAM
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1859_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]

_DATASET_SETS = {
    "sales": dev1847_datasets,
    "sales_wsum": sales_wsum_datasets,
    "orders": dev1840_datasets,
    "orders_wsum": customers_wsum_datasets,
}
_REGION_SUM = "sum(sales.amount, partition_by=sales.region)"
_HEADLINE = ("weighted_avg(customers.spend, "
             "weight=sum(orders.amount, partition_by=customers.regions.name))")
_MIXED_TWIN = "sum(customers.spend * sum(orders.amount, partition_by=customers.regions.name))"
_RANKED_TRANSFORM = ("weighted_avg(customers.spend, "
                     "weight=rank(sum(orders.amount, partition_by=customers.regions.name), direction='desc'))")
_LOCAL_RANKED_TRANSFORM = f"weighted_avg(sales.amount, weight=rank({_REGION_SUM}, direction='desc'))"


def _cases() -> dict:
    return {
        "mixed/headline": {
            "models": "sales", "source": "sales", "mode": None,
            "kw": {"dimensions": ["sales.region"], "metrics": [
                {"metric": f"sum(sales.quantity * {INNER_UP_PRODUCT})", "alias": "m"}]}},
        "mixed/outer_partition": {
            "models": "sales", "source": "sales", "mode": None,
            "kw": {"dimensions": ["sales.region", "sales.product"], "metrics": [
                {"metric": f"sum(sales.quantity * {INNER_UP_PRODUCT}, partition_by=sales.region)",
                 "alias": "m"}]}},
        "param/associate": {
            "models": "orders", "source": "orders", "mode": "associate",
            "kw": {"dimensions": ["orders.status"],
                   "metrics": [{"metric": _HEADLINE, "alias": "w"}]}},
        "param/broadcast": {
            "models": "orders", "source": "orders", "mode": "broadcast",
            "kw": {"dimensions": ["orders.status"],
                   "metrics": [{"metric": _HEADLINE, "alias": "w"}]}},
        "mixed/cross_model_constituent": {
            "models": "orders", "source": "orders", "mode": "broadcast",
            "kw": {"dimensions": ["orders.status"],
                   "metrics": [{"metric": _MIXED_TWIN, "alias": "w"}]}},
        "param/ordinary": {
            "models": "sales_wsum", "source": "sales", "mode": None,
            "kw": {"dimensions": ["sales.region"], "metrics": [
                {"metric": f"weighted_avg(sales.amount, weight={_REGION_SUM})",
                 "alias": "w"}]}},
        "param/mixed_plus": {
            "models": "sales", "source": "sales", "mode": None,
            "kw": {"dimensions": ["sales.region"], "metrics": [
                {"metric": (f"weighted_avg(sales.quantity * {INNER_UP_PRODUCT}, "
                             f"weight={_REGION_SUM})"), "alias": "w"}]}},
        "param/literal": {
            "models": "sales_wsum", "source": "sales", "mode": None,
            "kw": {"dimensions": ["sales.region"], "metrics": [
                {"metric": f"wsum(1, weight={_REGION_SUM})", "alias": "w"}]}},
        "param/ungrained_reagg": {
            "models": "sales_wsum", "source": "sales", "mode": None,
            "kw": {"dimensions": ["sales.region"], "metrics": [
                {"metric": ("wsum(sum(sales.amount, partition_by=[sales.city, sales.region]), "
                             "weight=count(sales.id))"), "alias": "w"}]}},
        "param/ranked_transform": {
            "models": "orders", "source": "orders", "mode": "broadcast",
            "kw": {"dimensions": ["orders.status"],
                   "metrics": [{"metric": _RANKED_TRANSFORM, "alias": "w"}]}},
        "param/local_ranked_transform": {
            "models": "sales", "source": "sales", "mode": None,
            "kw": {"dimensions": ["sales.region"],
                   "metrics": [{"metric": _LOCAL_RANKED_TRANSFORM, "alias": "w"}]}},
        "param/windowed": {
            "models": "orders", "source": "orders", "mode": None,
            "kw": {"time_dimensions": signup_month_td(),
                   "metrics": [{"metric": WINDOWED_PARAM, "alias": "w"}]}},
        "param/windowed_custom": {
            "models": "orders_wsum", "source": "orders", "mode": None,
            "kw": {"time_dimensions": signup_month_td(),
                   "metrics": [{"metric": WINDOWED_CUSTOM_PARAM, "alias": "w"}]}},
    }


async def _generate_one(case, dialect: str):
    models = _DATASET_SETS[case["models"]]()
    try:
        kw = dict(case["kw"])
        if case["mode"] is not None:
            kw["to_many_handling"] = case["mode"]
        query = SlayerQuery(population=case["source"], **kw)
        return await _engine_generate(
            query=query, dataset=models[0], extra_datasets=models[1:],
            dialect=dialect, validate=False,
        )
    except Exception as exc:  # noqa: BLE001 — the raise itself is the contract
        return record_raise(exc)


ALLOWED_DELTAS: dict[str, str] = {}  # PENDING re-bless list; empty in a committed state.

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
