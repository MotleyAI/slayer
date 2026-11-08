"""Golden SQL for a trailing ``window=`` on every aggregation (DEV-1915), over
the seven harness dialects.

On the current tree every case records the "only supported for sum and avg"
raise, so the two invariant tests below are red; after the change the baseline
is regenerated (``SLAYER_UPDATE_GOLDEN=1``) and only the real dialect gaps —
``median``/``percentile`` on MySQL and T-SQL — record raises (D5: windowed gaps
equal the plain gaps; corr/covar and BigQuery median render fine).
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import SlayerQuery
from tests._dev1915_fixtures import ModelMeasure, dev1915_models, month_td
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise, render_value

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1915_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]

#: The only real render-time dialect gaps (probed on the plain forms; D5).
AGG_UNSUPPORTED = {f"{c}::{d}" for c in ("median", "percentile_p09")
                   for d in ("mysql", "tsql")}
#: Cases whose emitted SQL must rank interval rows, never a plain aggregate.
FIRST_LAST = {"first", "last", "last_explicit_time", "cross_model_last"}


def _cm(formula, name):
    return [ModelMeasure(formula=formula, name=name)]


def _cases() -> dict:
    m = month_td()
    sm = month_td("customers.signup_at")
    return {
        "count": {"kw": {"time_dimensions": m, "measures": _cm("amount:count(window='90d')", "w")}},
        "min": {"kw": {"time_dimensions": m, "measures": _cm("amount:min(window='90d')", "w")}},
        "max": {"kw": {"time_dimensions": m, "measures": _cm("amount:max(window='90d')", "w")}},
        "count_distinct": {"kw": {"time_dimensions": m, "measures": _cm("amount:count_distinct(window='90d')", "w")}},
        "count_distinct_approx": {"kw": {"time_dimensions": m, "measures": _cm("amount:count_distinct_approx(window='90d')", "w")}},
        "star_count": {"kw": {"time_dimensions": m, "measures": _cm("*:count(window='90d')", "w")}},
        "median": {"kw": {"time_dimensions": m, "measures": _cm("amount:median(window='90d')", "w")}},
        "percentile_p09": {"kw": {"time_dimensions": m, "measures": _cm("amount:percentile(p=0.9, window='90d')", "w")}},
        "stddev_samp": {"kw": {"time_dimensions": m, "measures": _cm("amount:stddev_samp(window='90d')", "w")}},
        "var_pop": {"kw": {"time_dimensions": m, "measures": _cm("amount:var_pop(window='90d')", "w")}},
        "corr": {"kw": {"time_dimensions": m, "measures": _cm("amount:corr(other=qty, window='90d')", "w")}},
        "covar_samp": {"kw": {"time_dimensions": m, "measures": _cm("amount:covar_samp(other=qty, window='90d')", "w")}},
        "stddev_pop": {"kw": {"time_dimensions": m, "measures": _cm("amount:stddev_pop(window='90d')", "w")}},
        "var_samp": {"kw": {"time_dimensions": m, "measures": _cm("amount:var_samp(window='90d')", "w")}},
        "covar_pop": {"kw": {"time_dimensions": m, "measures": _cm("amount:covar_pop(other=qty, window='90d')", "w")}},
        "weighted_avg": {"kw": {"time_dimensions": m, "measures": _cm("amount:weighted_avg(weight=qty, window='90d')", "w")}},
        "weighted_avg_attached_weight": {"kw": {"time_dimensions": m, "measures": _cm("amount:weighted_avg(weight=qty:sum(partition_by=region), window='90d')", "w")}},
        "custom_trimmed_mean": {"kw": {"time_dimensions": m, "measures": _cm("amount:trimmed_mean(lo=150, hi=350, window='90d')", "w")}},
        "custom_default_literal_p": {"kw": {"time_dimensions": m, "measures": _cm("amount:trimmed_mean(window='90d')", "w")}},
        "custom_joined_default_column": {"kw": {"time_dimensions": sm, "measures": _cm("customers.spend:disc_spend(window='1y')", "w")}},
        "first": {"kw": {"time_dimensions": m, "measures": _cm("amount:first(window='90d')", "w")}},
        "last": {"kw": {"time_dimensions": m, "measures": _cm("amount:last(window='90d')", "w")}},
        "last_explicit_time": {"kw": {"time_dimensions": m, "measures": _cm("amount:last(updated_at, window='90d')", "w")}},
        "partitioned_count": {"kw": {"dimensions": ["region"], "time_dimensions": m, "measures": _cm("amount:count(window='90d', partition_by=region)", "w")}},
        "cross_model_last": {"kw": {"time_dimensions": sm, "measures": _cm("customers.spend:last(customers.signup_at, window='1y')", "w")}},
        "expression_source_count_distinct": {"kw": {"time_dimensions": m, "measures": _cm("count_distinct(amount - qty, window='90d')", "w")}},
        "order_only_max": {"kw": {"time_dimensions": m, "measures": _cm("amount:sum", "s"),
                                  "order": [{"column": "amount:max(window='90d')", "direction": "desc"}]}},
        "filter_only_count": {"kw": {"time_dimensions": m, "measures": _cm("amount:sum", "s"),
                                     "filters": ["amount:count(window='90d') >= 3"]}},
    }


async def _generate_one(case, dialect: str):
    models = dev1915_models()
    try:
        query = SlayerQuery(source_model="orders", **case["kw"])
        return await _engine_generate(
            query=query, model=models[0], extra_models=models[1:],
            dialect=dialect, validate=False,
        )
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


def test_first_last_cases_rank_and_never_raise(baseline) -> None:
    for key, value in baseline.items():
        if key.split("::", 1)[0] in FIRST_LAST:
            assert not isinstance(value, dict), (
                f"{key} must emit ranked SQL, not a raise: {render_value(value)}")


def test_only_median_percentile_gaps_record_raises(baseline) -> None:
    for key, value in baseline.items():
        if key in AGG_UNSUPPORTED:
            assert isinstance(value, dict), (
                f"{key} should record the plain dialect gap: {render_value(value)}")
            assert value.get("error") == "NotImplementedError", (
                f"{key} should record the plain dialect gap: {render_value(value)}")
        else:
            assert not isinstance(value, dict), (
                f"{key} unexpectedly records a raise: {render_value(value)}")
