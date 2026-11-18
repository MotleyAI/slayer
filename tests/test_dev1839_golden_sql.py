"""DEV-1839 golden SQL — the union-grain lifts and the remaining guard raises
across five dialects (incl. SQL Server, whose nested-WITH restriction drives
design D5, and BigQuery).

``build_baseline`` forces ``SLAYER_VALIDATE_SCOPES=1`` so each working entry is
scope-closed; guard cases record their raise. The baseline is blessed alongside
the implementation (``SLAYER_UPDATE_GOLDEN=1``) — until then every case fails
with the missing-baseline message, which is this suite's feature-missing state.
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import QueryMetric

from tests._dev1839_fixtures import (
    EXPLICIT_PART_RANK,
    KEYLESS_RANK,
    MEASURE_DIFF,
    MIXED_RANK,
    DatasetMetric,
    NESTED_RANK,
    SAMEGRAIN_DIFF,
    SAMEGRAIN_RANK,
    SUBSET_RANK,
    dev1839_models,
    month_td,
    q,
)
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1839_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]
ALLOWED_DELTAS: dict[str, str] = {}

WORKING_PREFIX = "lift/"


def _cases() -> dict:
    s = DatasetMetric(expression="sum(amount)", name="s")
    return {
        "lift/dim_mixed_rank": q(
            dimensions=["region", "city", {"field": MIXED_RANK, "alias": "rr"}],
            metrics=[s],
        ),
        "lift/dim_keyless_share": q(
            dimensions=["region", {"field": KEYLESS_RANK, "alias": "kr"}],
            metrics=[s],
        ),
        "lift/dim_subset_rank": q(
            dimensions=["region", "city", {"field": SUBSET_RANK, "alias": "sr"}],
            metrics=[s],
        ),
        "lift/dim_nested_cumsum": q(
            dimensions=["region", "city", {"field": NESTED_RANK, "alias": "nr"}],
            time_dimensions=month_td(),
            metrics=[s],
        ),
        "lift/dim_explicit_partition": q(
            dimensions=[
                "region", "city", {"field": EXPLICIT_PART_RANK, "alias": "er"},
            ],
            metrics=[s],
        ),
        "lift/dual_role": q(
            dimensions=[
                "region", "city", "channel",
                {"field": MIXED_RANK, "alias": "rr"},
            ],
            metrics=[QueryMetric(metric=MIXED_RANK, alias="rm"), s],
        ),
        "lift/measure_mixed_arithmetic": q(
            dimensions=["region", "city"],
            metrics=[QueryMetric(metric=MEASURE_DIFF, alias="d"), s],
        ),
        "lift/measure_same_grain_arithmetic": q(
            dimensions=["region"],
            metrics=[QueryMetric(metric=SAMEGRAIN_DIFF, alias="d")],
        ),
        "lift/dim_same_grain_rank": q(
            dimensions=["region", {"field": SAMEGRAIN_RANK, "alias": "gr"}],
            metrics=[s],
        ),
        # DEV-1835 stage-2 lifts the windowed / first-last mixed-grain union
        # (design D9): these render now instead of raising the residual guard.
        "lift/dim_mixed_windowed": q(
            dimensions=[
                "region", "city",
                {"field": "rank(sum(amount, window='90d', partition_by=region)"
                               " - sum(amount, partition_by=city), direction='desc')", "alias": "x"},
            ],
            time_dimensions=month_td(),
            metrics=[s],
        ),
        "lift/dim_mixed_first_last": q(
            dimensions=[
                "region", "city",
                {"field": "rank(last(amount, partition_by=region) - "
                               "sum(amount, partition_by=city), direction='desc')", "alias": "x"},
            ],
            metrics=[s],
        ),
        "guard/dim_temporal_axis_missing": q(
            dimensions=[
                "region", "city",
                {"field": "cumsum(sum(amount, partition_by=[region, city]))",
                 "alias": "cs"},
            ],
            time_dimensions=month_td(),
            metrics=[s],
        ),
        "guard/dim_kwarg_outside_union": q(
            dimensions=[
                "region", "city",
                {"field": "rank(sum(amount, partition_by=region) - "
                               "sum(amount, partition_by=city), "
                               "partition_by=channel, direction='desc')", "alias": "x"},
            ],
            metrics=[s],
        ),
    }


async def _generate_one(query, dialect: str):
    models = dev1839_models()
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
