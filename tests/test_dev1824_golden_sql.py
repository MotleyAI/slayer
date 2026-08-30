"""DEV-1824 golden SQL — the lifted ``partition_by=`` shapes and the remaining
guard raises across five dialects.

``build_baseline`` forces ``SLAYER_VALIDATE_SCOPES=1`` so each working entry is
scope-closed; guard cases record their raise. The baseline is blessed alongside
the implementation (``SLAYER_UPDATE_GOLDEN=1``) — until then every case fails
with the missing-baseline message, which is this suite's feature-missing state.
"""

from __future__ import annotations

from pathlib import Path

from tests._dev1824_fixtures import BAND35, ModelMeasure, dev1824_models, month_td, q
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1824_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]
ALLOWED_DELTAS: dict[str, str] = {}

WORKING_PREFIX = "lift/"

RANK_DIM = "rank(amount:sum(partition_by=region))"
WBAND = (
    "CASE WHEN amount:sum(window='90d', partition_by=region) > 50 THEN 1 ELSE 0 END"
)
CBAND = (
    "CASE WHEN cumsum(amount:sum(partition_by=[region, ordered_at])) > 50 "
    "THEN 1 ELSE 0 END"
)


def _cases() -> dict:
    return {
        "lift/window_partition": q(
            dimensions=["region", "city"], time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="amount:sum(window='90d', partition_by=region)", name="w",
            )],
        ),
        "lift/two_ranked_producers": q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(formula="amount:last(partition_by=region)", name="a"),
                ModelMeasure(formula="amount:last(partition_by=city)", name="b"),
            ],
        ),
        "lift/three_level_nesting": q(
            dimensions=["region", {"expression": CBAND, "name": "cband"}],
            time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum", name="s"),
                ModelMeasure(
                    formula="amount:sum(window='90d', partition_by=region)",
                    name="w",
                ),
                ModelMeasure(formula="amount:last(partition_by=region)", name="l"),
            ],
        ),
        "lift/first_last_partition": q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(
                formula="amount:last(partition_by=region)", name="l",
            )],
        ),
        "lift/temporal_partition_last": q(
            dimensions=["region", "city"], time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="amount:last(partition_by=[region, ordered_at])", name="l",
            )],
        ),
        "lift/cumsum_partitioned": q(
            dimensions=["region", "city"], time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="cumsum(amount:sum(partition_by=[region, ordered_at]))",
                name="c",
            )],
        ),
        "lift/rank_partitioned_measure": q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula=RANK_DIM, name="r")],
        ),
        "lift/filter_partitioned": q(
            dimensions=["region", "city"],
            filters=["amount:sum(partition_by=region) > 50"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ),
        "lift/filter_conjunct_split": q(
            dimensions=["region", "city"],
            filters=["amount:sum(partition_by=region) > 50 and city != 'CityB'"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ),
        "lift/coexist_row_combined": q(
            dimensions=["region", {"expression": BAND35, "name": "band"}],
            measures=[
                ModelMeasure(formula="amount:sum", name="s"),
                ModelMeasure(formula="amount:sum(partition_by=region)", name="rt"),
            ],
        ),
        "lift/coexist_same_agg_both": q(
            dimensions=[
                "region",
                {"expression": "CASE WHEN amount:sum(partition_by=region) > 55 "
                               "THEN 1 ELSE 0 END", "name": "band"},
            ],
            measures=[ModelMeasure(
                formula="amount:sum(partition_by=region)", name="rt",
            )],
        ),
        "lift/order_raw_aggregate": q(
            dimensions=["region", "city", {"expression": BAND35, "name": "band"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
            order=[{"column": "amount:sum(partition_by=city)", "direction": "asc"}],
        ),
        "lift/coexist_windowed": q(
            dimensions=["region", "city"], time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum(partition_by=region)", name="rt"),
                ModelMeasure(formula="amount:sum(window='1y')", name="w"),
            ],
        ),
        "lift/coexist_first_last": q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(formula="amount:sum(partition_by=region)", name="rt"),
                ModelMeasure(formula="amount:last", name="l"),
            ],
        ),
        "lift/coexist_cross_model": q(
            dimensions=["region", "city"],
            measures=[
                ModelMeasure(formula="amount:sum(partition_by=region)", name="rt"),
                ModelMeasure(formula="customers.spend:sum", name="cm"),
            ],
        ),
        "lift/coexist_transform": q(
            dimensions=["region", "city"], time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum(partition_by=region)", name="rt"),
                ModelMeasure(formula="cumsum(amount:sum)", name="c"),
            ],
        ),
        "lift/dim_two_partition_sets": q(
            dimensions=[
                "region",
                {"expression": "amount:sum(partition_by=city) - "
                               "amount:sum(partition_by=region)", "name": "gap"},
            ],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ),
        "lift/dim_rank": q(
            dimensions=["region", {"expression": RANK_DIM, "name": "rr"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ),
        "lift/dim_first_last": q(
            dimensions=[
                "region",
                {"expression": "amount:last(partition_by=region)", "name": "la"},
            ],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ),
        "lift/dim_window": q(
            dimensions=["region", {"expression": WBAND, "name": "wband"}],
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ),
        "guard/dim_bare_aggregate": q(
            dimensions=[
                "region",
                {"expression": "CASE WHEN amount:sum > 50 THEN 1 ELSE 0 END",
                 "name": "b"},
            ],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ),
        "guard/dim_cross_model_source": q(
            dimensions=[
                "region",
                {"expression": "CASE WHEN customers.spend:sum(partition_by=region) "
                               "> 100 THEN 1 ELSE 0 END", "name": "cband"},
            ],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ),
        "guard/filter_no_common_scope": q(
            dimensions=["region", "city"],
            filters=["amount:sum(partition_by=region) > 50 or status == 'ok'"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ),
        "guard/nested_attach": q(
            dimensions=[
                {"expression": BAND35, "name": "band"},
                {"expression": "amount:sum(partition_by=band)", "name": "b2"},
            ],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ),
    }


async def _generate_one(query, dialect: str):
    models = dev1824_models()
    try:
        return await _engine_generate(
            query=query, model=models[0], extra_models=models[1:],
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
