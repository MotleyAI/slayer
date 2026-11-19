"""Golden SQL — the lifted ``partition_by=`` shapes and the remaining
guard raises across five dialects.

``build_baseline`` forces ``SLAYER_VALIDATE_SCOPES=1`` so each working entry is
scope-closed; guard cases record their raise. The baseline is blessed alongside
the implementation (``SLAYER_UPDATE_GOLDEN=1``) — until then every case fails
with the missing-baseline message, which is this suite's feature-missing state.
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import QueryMetric

from tests._dev1824_fixtures import BAND35, dev1824_models, month_td, q
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1824_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]
ALLOWED_DELTAS: dict[str, str] = {}

WORKING_PREFIX = "lift/"

RANK_DIM = "rank(sum(orders.amount, partition_by=orders.region), direction='desc')"
WBAND = (
    "CASE WHEN sum(orders.amount, window='90d', partition_by=orders.region) > 50 THEN 1 ELSE 0 END"
)
CBAND = (
    "CASE WHEN cumsum(sum(amount, partition_by=[region, ordered_at])) > 50 "
    "THEN 1 ELSE 0 END"
)


def _cases() -> dict:
    return {
        "lift/window_partition": q(
            dimensions=["orders.region", "orders.city"], time_dimensions=month_td(),
            metrics=[QueryMetric(
                metric="sum(orders.amount, window='90d', partition_by=orders.region)", alias="w",
            )],
        ),
        "lift/two_ranked_producers": q(
            dimensions=["orders.region", "orders.city"],
            metrics=[
                QueryMetric(metric="last(orders.amount, partition_by=orders.region)", alias="a"),
                QueryMetric(metric="last(orders.amount, partition_by=orders.city)", alias="b"),
            ],
        ),
        "lift/three_level_nesting": q(
            dimensions=["orders.region", {"field": CBAND, "alias": "cband"}],
            time_dimensions=month_td(),
            metrics=[
                QueryMetric(metric="sum(orders.amount)", alias="s"),
                QueryMetric(
                    metric="sum(orders.amount, window='90d', partition_by=orders.region)",
                    alias="w",
                ),
                QueryMetric(metric="last(orders.amount, partition_by=orders.region)", alias="l"),
            ],
        ),
        "lift/first_last_partition": q(
            dimensions=["orders.region", "orders.city"],
            metrics=[QueryMetric(
                metric="last(orders.amount, partition_by=orders.region)", alias="l",
            )],
        ),
        "lift/temporal_partition_last": q(
            dimensions=["orders.region", "orders.city"], time_dimensions=month_td(),
            metrics=[QueryMetric(
                metric="last(orders.amount, partition_by=[orders.region, orders.ordered_at])", alias="l",
            )],
        ),
        "lift/cumsum_partitioned": q(
            dimensions=["orders.region", "orders.city"], time_dimensions=month_td(),
            metrics=[QueryMetric(
                metric="cumsum(sum(orders.amount, partition_by=[orders.region, orders.ordered_at]))",
                alias="c",
            )],
        ),
        "lift/rank_partitioned_measure": q(
            dimensions=["orders.region", "orders.city"],
            metrics=[QueryMetric(metric=RANK_DIM, alias="r")],
        ),
        "lift/filter_partitioned": q(
            dimensions=["orders.region", "orders.city"],
            filter=["sum(orders.amount, partition_by=orders.region) > 50"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ),
        "lift/filter_conjunct_split": q(
            dimensions=["orders.region", "orders.city"],
            filter=["sum(orders.amount, partition_by=orders.region) > 50 and orders.city != 'CityB'"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ),
        "lift/coexist_row_combined": q(
            dimensions=["orders.region", {"field": BAND35, "alias": "band"}],
            metrics=[
                QueryMetric(metric="sum(orders.amount)", alias="s"),
                QueryMetric(metric="sum(orders.amount, partition_by=orders.region)", alias="rt"),
            ],
        ),
        "lift/coexist_same_agg_both": q(
            dimensions=[
                "orders.region",
                {"field": "CASE WHEN sum(amount, partition_by=region) > 55 "
                               "THEN 1 ELSE 0 END", "alias": "band"},
            ],
            metrics=[QueryMetric(
                metric="sum(orders.amount, partition_by=orders.region)", alias="rt",
            )],
        ),
        "lift/order_raw_aggregate": q(
            dimensions=["orders.region", "orders.city", {"field": BAND35, "alias": "band"}],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
            order=[{"field": "sum(orders.amount, partition_by=orders.city)", "direction": "asc"}],
        ),
        "lift/coexist_windowed": q(
            dimensions=["orders.region", "orders.city"], time_dimensions=month_td(),
            metrics=[
                QueryMetric(metric="sum(orders.amount, partition_by=orders.region)", alias="rt"),
                QueryMetric(metric="sum(orders.amount, window='1y')", alias="w"),
            ],
        ),
        "lift/coexist_first_last": q(
            dimensions=["orders.region", "orders.city"],
            metrics=[
                QueryMetric(metric="sum(orders.amount, partition_by=orders.region)", alias="rt"),
                QueryMetric(metric="last(orders.amount)", alias="l"),
            ],
        ),
        "lift/coexist_cross_model": q(
            to_many_handling="broadcast",
            dimensions=["orders.region", "orders.city"],
            metrics=[
                QueryMetric(metric="sum(orders.amount, partition_by=orders.region)", alias="rt"),
                QueryMetric(metric="sum(customers.spend)", alias="cm"),
            ],
        ),
        "lift/coexist_transform": q(
            dimensions=["orders.region", "orders.city"], time_dimensions=month_td(),
            metrics=[
                QueryMetric(metric="sum(orders.amount, partition_by=orders.region)", alias="rt"),
                QueryMetric(metric="cumsum(sum(orders.amount))", alias="c"),
            ],
        ),
        "lift/dim_two_partition_sets": q(
            dimensions=[
                "orders.region",
                {"field": "sum(amount, partition_by=city) - "
                               "sum(amount, partition_by=region)", "alias": "gap"},
            ],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ),
        "lift/dim_rank": q(
            dimensions=["orders.region", {"field": RANK_DIM, "alias": "rr"}],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ),
        "lift/dim_first_last": q(
            dimensions=[
                "orders.region",
                {"field": "last(orders.amount, partition_by=orders.region)", "alias": "la"},
            ],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ),
        "lift/dim_window": q(
            dimensions=["orders.region", {"field": WBAND, "alias": "wband"}],
            time_dimensions=month_td(),
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ),
        "guard/dim_bare_aggregate": q(
            dimensions=[
                "orders.region",
                {"field": "CASE WHEN sum(orders.amount) > 50 THEN 1 ELSE 0 END",
                 "alias": "b"},
            ],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ),
        "guard/dim_cross_model_source": q(
            to_many_handling="broadcast",
            dimensions=[
                "orders.region",
                {"field": "CASE WHEN sum(customers.spend, partition_by=region) "
                               "> 100 THEN 1 ELSE 0 END", "alias": "cband"},
            ],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ),
        "guard/filter_no_common_scope": q(
            dimensions=["orders.region", "orders.city"],
            filter=["sum(orders.amount, partition_by=orders.region) > 50 or orders.status == 'ok'"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ),
        "lift/nested_attach": q(
            dimensions=[
                {"field": BAND35, "alias": "band"},
                # partition_by carries extra keys so the internal CTE alias stays
                # over bigquery's 300-byte limit — the tier-1 identifier-fitting
                # premise pinned in test_dev1891_internal_identifier_fit.py (D5's
                # Column.filter desugar shortened the encoding).
                {"field": "sum(orders.amount, partition_by=[band, orders.region, orders.customer_id])",
                 "alias": "b2"},
            ],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ),
    }


async def _generate_one(query, dialect: str):
    models = dev1824_models()
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


def test_lifted_cases_render_and_guards_raise(baseline) -> None:
    for key, value in baseline.items():
        case_id = key.rsplit("::", 1)[0]
        if case_id.startswith(WORKING_PREFIX):
            assert not isinstance(value, dict), f"{key} unexpectedly raised: {value}"
            assert "__regroup__" not in value, f"{key} leaked a placeholder"
        else:
            assert isinstance(value, dict), f"{key} should raise, got SQL"
