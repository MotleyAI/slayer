"""Golden SQL for a trailing ``window=`` on every aggregation (DEV-1915), over
the seven harness dialects.

No case records a raise: windowed gaps equal the plain gaps (D5), and every dialect renders
``median``/``percentile``.
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import QueryMetric, SlayerQuery
from tests._dev1915_fixtures import dev1915_datasets, month_td
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise, render_value

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1915_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]

#: Cases whose emitted SQL must rank interval rows, never a plain aggregate.
FIRST_LAST = {"first", "last", "last_explicit_time", "cross_model_last"}


def _cm(expression, name):
    return [QueryMetric(metric=expression, alias=name)]


def _cases() -> dict:
    m = month_td()
    sm = month_td("customers.signup_at")
    return {
        "count": {"kw": {"time_dimensions": m, "metrics": _cm("count(orders.amount, window='90d')", "w")}},
        "min": {"kw": {"time_dimensions": m, "metrics": _cm("min(orders.amount, window='90d')", "w")}},
        "max": {"kw": {"time_dimensions": m, "metrics": _cm("max(orders.amount, window='90d')", "w")}},
        "count_distinct": {"kw": {"time_dimensions": m, "metrics": _cm("count_distinct(orders.amount, window='90d')", "w")}},
        "count_distinct_approx": {"kw": {"time_dimensions": m, "metrics": _cm("count_distinct_approx(orders.amount, window='90d')", "w")}},
        "star_count": {"kw": {"time_dimensions": m, "metrics": _cm("count(orders.*, window='90d')", "w")}},
        "median": {"kw": {"time_dimensions": m, "metrics": _cm("median(orders.amount, window='90d')", "w")}},
        "percentile_p09": {"kw": {"time_dimensions": m, "metrics": _cm("percentile(orders.amount, p=0.9, window='90d')", "w")}},
        "stddev_samp": {"kw": {"time_dimensions": m, "metrics": _cm("stddev_samp(orders.amount, window='90d')", "w")}},
        "var_pop": {"kw": {"time_dimensions": m, "metrics": _cm("var_pop(orders.amount, window='90d')", "w")}},
        "corr": {"kw": {"time_dimensions": m, "metrics": _cm("corr(orders.amount, other=orders.qty, window='90d')", "w")}},
        "covar_samp": {"kw": {"time_dimensions": m, "metrics": _cm("covar_samp(orders.amount, other=orders.qty, window='90d')", "w")}},
        "stddev_pop": {"kw": {"time_dimensions": m, "metrics": _cm("stddev_pop(orders.amount, window='90d')", "w")}},
        "var_samp": {"kw": {"time_dimensions": m, "metrics": _cm("var_samp(orders.amount, window='90d')", "w")}},
        "covar_pop": {"kw": {"time_dimensions": m, "metrics": _cm("covar_pop(orders.amount, other=orders.qty, window='90d')", "w")}},
        "weighted_avg": {"kw": {"time_dimensions": m, "metrics": _cm("weighted_avg(orders.amount, weight=orders.qty, window='90d')", "w")}},
        "weighted_avg_attached_weight": {"kw": {"time_dimensions": m, "metrics": _cm("weighted_avg(orders.amount, weight=sum(orders.qty, partition_by=orders.region), window='90d')", "w")}},
        "custom_trimmed_mean": {"kw": {"time_dimensions": m, "metrics": _cm("trimmed_mean(orders.amount, lo=150, hi=350, window='90d')", "w")}},
        "custom_default_literal_p": {"kw": {"time_dimensions": m, "metrics": _cm("trimmed_mean(orders.amount, window='90d')", "w")}},
        "custom_joined_default_column": {"kw": {"time_dimensions": sm, "metrics": _cm("disc_spend(customers.spend, window='1y')", "w")}},
        "first": {"kw": {"time_dimensions": m, "metrics": _cm("first(orders.amount, window='90d')", "w")}},
        "last": {"kw": {"time_dimensions": m, "metrics": _cm("last(orders.amount, window='90d')", "w")}},
        "last_explicit_time": {"kw": {"time_dimensions": m, "metrics": _cm("last(orders.amount, orders.updated_at, window='90d')", "w")}},
        "partitioned_count": {"kw": {"dimensions": ["orders.region"], "time_dimensions": m, "metrics": _cm("count(orders.amount, window='90d', partition_by=orders.region)", "w")}},
        "cross_model_last": {"kw": {"time_dimensions": sm, "metrics": _cm("last(customers.spend, customers.signup_at, window='1y')", "w")}},
        "expression_source_count_distinct": {"kw": {"time_dimensions": m, "metrics": _cm("count_distinct(orders.amount - orders.qty, window='90d')", "w")}},
        "order_only_max": {"kw": {"time_dimensions": m, "metrics": _cm("sum(orders.amount)", "s"),
                                  "order": [{"field": "max(orders.amount, window='90d')", "direction": "desc"}]}},
        "filter_only_count": {"kw": {"time_dimensions": m, "metrics": _cm("sum(orders.amount)", "s"),
                                     "filter": ["count(orders.amount, window='90d') >= 3"]}},
    }


async def _generate_one(case, dialect: str):
    models = dev1915_datasets()
    try:
        query = SlayerQuery(population="orders", **case["kw"])
        return await _engine_generate(
            query=query, dataset=models[0], extra_datasets=models[1:],
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


def test_no_case_records_a_raise(baseline) -> None:
    for key, value in baseline.items():
        assert not isinstance(value, dict), f"{key} unexpectedly records a raise: {render_value(value)}"
