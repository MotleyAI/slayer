"""DEV-1892 golden SQL (task 1.6) — the lifted parameter shapes across seven
Tier-1 dialects. Blessed pre-implementation as today's feature-missing raises;
at implementation each case's flip to real SQL enters ALLOWED_DELTAS, is
re-blessed, and the manifest is emptied. ``test_lifted_cases_generate_not_raise``
is this module's feature-missing tripwire until then.
"""

from __future__ import annotations

from pathlib import Path

from tests._dev1841_fixtures import field_default_agg_datasets, dev1840_datasets
from tests._dev1847_fixtures import INNER_CR, dev1847_datasets
from tests._dev1892_fixtures import (
    qualified_expr_default_datasets,
    qualified_reagg_default_datasets,
)
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise
from slayer.core.query import SlayerQuery

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1892_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]

_MODEL_SETS = {
    "orders": dev1840_datasets,
    "orders_coldef": field_default_agg_datasets,
    "orders_wsum6": qualified_expr_default_datasets,
    "sales": dev1847_datasets,
    "sales_cwavg": qualified_reagg_default_datasets,
}
_WEIGHT_COUNT = "weight=count(id, partition_by=[city, region])"


def _cases() -> dict:
    return {
        "assoc/wavg_by_status": {
            "models": "orders", "source": "orders", "mode": "associate",
            "kw": {"dimensions": ["status"], "metrics": [
                {"metric": "weighted_avg(customers.spend, weight=customers.spend)",
                 "alias": "w"}]}},
        "assoc/wsum_default": {
            "models": "orders_coldef", "source": "orders", "mode": "associate",
            "kw": {"dimensions": ["status"],
                   "metrics": [{"metric": "wsum(customers.spend)", "alias": "w"}]}},
        "reagg/wavg_by_region": {
            "models": "sales", "source": "sales", "mode": None,
            "kw": {"dimensions": ["region"], "metrics": [
                {"metric": f"weighted_avg({INNER_CR}, {_WEIGHT_COUNT})",
                 "alias": "w"}]}},
        "reagg/degenerate_wavg": {
            "models": "sales", "source": "sales", "mode": None,
            "kw": {"dimensions": ["region"], "metrics": [
                {"metric": ("weighted_avg(sum(amount, partition_by=region), "
                             "weight=count(id, partition_by=region))"),
                 "alias": "w"}]}},
        "reagg/corders_toone": {
            "models": "sales", "source": "corders", "mode": None,
            "kw": {"metrics": [
                {"metric": ("weighted_avg(sum(amount, partition_by=customer_id), "
                             "weight=customers.region_id)"), "alias": "w"}]}},
        "assoc/wsum6_expr_default": {
            "models": "orders_wsum6", "source": "orders", "mode": "associate",
            "kw": {"dimensions": ["status"],
                   "metrics": [{"metric": "wsum6(customers.spend)", "alias": "w"}]}},
        "reagg/corders_expr_default": {
            "models": "sales_cwavg", "source": "corders", "mode": None,
            "kw": {"metrics": [
                {"metric": "cwavg(sum(amount, partition_by=customer_id))",
                 "alias": "w"}]}},
    }


async def _generate_one(case, dialect: str):
    models = _MODEL_SETS[case["models"]]()
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


def test_lifted_cases_generate_not_raise(baseline) -> None:
    """Feature-missing tripwire: every lifted shape must generate SQL rather than
    record a raise. Fails until the parameter lift lands."""
    for key, value in baseline.items():
        assert not isinstance(value, dict), f"{key} still raises: {value}"
