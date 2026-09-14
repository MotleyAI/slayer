"""DEV-1892 golden SQL (task 1.6) — the lifted parameter shapes across seven
Tier-1 dialects. Blessed pre-implementation as today's feature-missing raises;
at implementation each case's flip to real SQL enters ALLOWED_DELTAS, is
re-blessed, and the manifest is emptied. ``test_lifted_cases_generate_not_raise``
is this module's feature-missing tripwire until then.
"""

from __future__ import annotations

from pathlib import Path

from tests._dev1841_fixtures import column_default_agg_models, dev1840_models
from tests._dev1847_fixtures import INNER_CR, dev1847_models
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise
from slayer.core.query import SlayerQuery

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1892_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]
ALLOWED_DELTAS: dict[str, str] = {}

_MODEL_SETS = {
    "orders": dev1840_models,
    "orders_coldef": column_default_agg_models,
    "sales": dev1847_models,
}
_WEIGHT_COUNT = "weight=count(id, partition_by=[city, region])"


def _cases() -> dict:
    return {
        "assoc/wavg_by_status": {
            "models": "orders", "source": "orders", "mode": "associate",
            "kw": {"dimensions": ["status"], "measures": [
                {"formula": "customers.spend:weighted_avg(weight=customers.spend)",
                 "name": "w"}]}},
        "assoc/wsum_default": {
            "models": "orders_coldef", "source": "orders", "mode": "associate",
            "kw": {"dimensions": ["status"],
                   "measures": [{"formula": "customers.spend:wsum", "name": "w"}]}},
        "reagg/wavg_by_region": {
            "models": "sales", "source": "sales", "mode": None,
            "kw": {"dimensions": ["region"], "measures": [
                {"formula": f"weighted_avg({INNER_CR}, {_WEIGHT_COUNT})",
                 "name": "w"}]}},
        "reagg/degenerate_wavg": {
            "models": "sales", "source": "sales", "mode": None,
            "kw": {"dimensions": ["region"], "measures": [
                {"formula": ("weighted_avg(sum(amount, partition_by=region), "
                             "weight=count(id, partition_by=region))"),
                 "name": "w"}]}},
        "reagg/corders_toone": {
            "models": "sales", "source": "corders", "mode": None,
            "kw": {"measures": [
                {"formula": ("weighted_avg(sum(amount, partition_by=customer_id), "
                             "weight=customers.region_id)"), "name": "w"}]}},
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
