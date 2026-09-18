"""DEV-1900 golden SQL (task 1.7) — the fanning-derived shapes across seven
Tier-1 dialects. Blessed pre-implementation as today's feature-missing SQL (the
multiplying join / silent miscompute); at implementation each ``fanning/`` case
flips to a recorded raise, enters ALLOWED_DELTAS, is re-blessed, and the
manifest is emptied. ``test_fanning_cases_fail_closed`` is this module's
feature-missing tripwire until then. The ``positive/`` shapes cross no fanning
hop and MUST stay byte-identical SQL through the change.
"""

from __future__ import annotations

from pathlib import Path

from tests._dev1900_fixtures import dev1900_models
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise
from slayer.core.query import SlayerQuery

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1900_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]

_MODEL_SETS = {"dev1900": dev1900_models}
_REAGG_GOOD = ("weighted_avg(sum(amount, partition_by=customers.regions.id), "
               "weight=customers.regions.derived_pop)")
_REAGG_BAD = ("weighted_avg(sum(amount, partition_by=customers.regions.id), "
              "weight=customers.regions.bad_pop)")


def _cases() -> dict:
    return {
        # fanning/* — must fail closed after the change (today: multiplying SQL).
        "fanning/cross_model_kwarg": {
            "models": "dev1900", "source": "orders", "mode": None,
            "kw": {"measures": [{"formula":
                "customers.spend:weighted_avg(weight=customers.regions.bad_pop)",
                "name": "w"}]}},
        "fanning/local_kwarg": {
            "models": "dev1900", "source": "orders", "mode": None,
            "kw": {"measures": [{"formula":
                "amount:weighted_avg(weight=customers.regions.bad_pop)", "name": "w"}]}},
        "fanning/definition_default": {
            "models": "dev1900", "source": "orders", "mode": None,
            "kw": {"measures": [{"formula": "customers.spend:wsumx", "name": "w"}]}},
        "fanning/reagg_param": {
            "models": "dev1900", "source": "orders", "mode": None,
            "kw": {"measures": [{"formula": _REAGG_BAD, "name": "w"}]}},
        "fanning/dim_error_mode": {
            "models": "dev1900", "source": "orders", "mode": "error",
            "kw": {"dimensions": ["customers.regions.bad_pop"],
                   "measures": [{"formula": "amount:sum", "name": "amt"}]}},
        "fanning/pop_filter_derived": {
            "models": "dev1900", "source": "orders", "mode": None,
            "kw": {"measures": [{"formula": "amount:sum", "name": "amt"}],
                   "filters": ["customers.regions.bad_pop > 0"]}},
        # positive/* — cross no fanning hop; byte-identical SQL through the change.
        "positive/reagg_derived_pop": {
            "models": "dev1900", "source": "orders", "mode": None,
            "kw": {"measures": [{"formula": _REAGG_GOOD, "name": "ra"}]}},
        "positive/to_one_filter": {
            "models": "dev1900", "source": "orders", "mode": None,
            "kw": {"measures": [{"formula": "amount:sum", "name": "amt"}],
                   "filters": ["customers.tier = 'gold'"]}},
        "positive/assoc_region_name": {
            "models": "dev1900", "source": "orders", "mode": "associate",
            "kw": {"dimensions": ["customers.regions.name"], "measures": [
                {"formula": "amount:sum", "name": "amt"},
                {"formula": "customers.spend:sum", "name": "csp"}]}},
    }


# DEV-1909 flips the derived population filter to an EXISTS restriction, so it is
# no longer fail-closed here — it generates SQL like the positive shapes.
FAIL_CLOSED = {
    k for k in _cases()
    if k.startswith("fanning/") and k != "fanning/pop_filter_derived"
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


_DEV1909 = ("DEV-1909: the derived fanning population filter now restricts by "
            "association (EXISTS on the host base) instead of failing closed.")
ALLOWED_DELTAS: dict[str, str] = {
    f"fanning/pop_filter_derived::{d}": _DEV1909 for d in DIALECTS
}

bind_golden_tests(
    namespace=globals(),
    golden_path=GOLDEN_PATH,
    cases=_cases,
    dialects=DIALECTS,
    allowed=ALLOWED_DELTAS,
    generate_one=_generate_one,
)


def test_fanning_cases_fail_closed(baseline) -> None:
    """Feature-missing tripwire: every fanning-derived shape must record a raise
    (fail closed), not SQL. Red until the closure lands."""
    for key, value in baseline.items():
        if key.split("::", 1)[0] in FAIL_CLOSED:
            assert isinstance(value, dict), f"{key} still emits SQL instead of failing closed"


def test_positive_cases_generate_sql(baseline) -> None:
    """The non-fanning shapes must stay real SQL through the change."""
    for key, value in baseline.items():
        if key.split("::", 1)[0] not in FAIL_CLOSED:
            assert not isinstance(value, dict), f"{key} unexpectedly raises: {value}"
