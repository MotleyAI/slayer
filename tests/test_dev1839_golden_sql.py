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

from tests._dev1839_fixtures import (
    EXPLICIT_PART_RANK,
    KEYLESS_RANK,
    MEASURE_DIFF,
    MIXED_RANK,
    ModelMeasure,
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
    s = ModelMeasure(formula="amount:sum", name="s")
    return {
        "lift/dim_mixed_rank": q(
            dimensions=["region", "city", {"expression": MIXED_RANK, "name": "rr"}],
            measures=[s],
        ),
        "lift/dim_keyless_share": q(
            dimensions=["region", {"expression": KEYLESS_RANK, "name": "kr"}],
            measures=[s],
        ),
        "lift/dim_subset_rank": q(
            dimensions=["region", "city", {"expression": SUBSET_RANK, "name": "sr"}],
            measures=[s],
        ),
        "lift/dim_nested_cumsum": q(
            dimensions=["region", "city", {"expression": NESTED_RANK, "name": "nr"}],
            time_dimensions=month_td(),
            measures=[s],
        ),
        "lift/dim_explicit_partition": q(
            dimensions=[
                "region", "city", {"expression": EXPLICIT_PART_RANK, "name": "er"},
            ],
            measures=[s],
        ),
        "lift/dual_role": q(
            dimensions=[
                "region", "city", "channel",
                {"expression": MIXED_RANK, "name": "rr"},
            ],
            measures=[ModelMeasure(formula=MIXED_RANK, name="rm"), s],
        ),
        "lift/measure_mixed_arithmetic": q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula=MEASURE_DIFF, name="d"), s],
        ),
        "lift/measure_same_grain_arithmetic": q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=SAMEGRAIN_DIFF, name="d")],
        ),
        "lift/dim_same_grain_rank": q(
            dimensions=["region", {"expression": SAMEGRAIN_RANK, "name": "gr"}],
            measures=[s],
        ),
        "guard/dim_mixed_windowed": q(
            dimensions=[
                "region", "city",
                {"expression": "rank(amount:sum(window='90d', partition_by=region)"
                               " - amount:sum(partition_by=city))", "name": "x"},
            ],
            time_dimensions=month_td(),
            measures=[s],
        ),
        "guard/dim_mixed_first_last": q(
            dimensions=[
                "region", "city",
                {"expression": "rank(amount:last(partition_by=region) - "
                               "amount:sum(partition_by=city))", "name": "x"},
            ],
            measures=[s],
        ),
        "guard/dim_temporal_axis_missing": q(
            dimensions=[
                "region", "city",
                {"expression": "cumsum(amount:sum(partition_by=[region, city]))",
                 "name": "cs"},
            ],
            time_dimensions=month_td(),
            measures=[s],
        ),
        "guard/dim_kwarg_outside_union": q(
            dimensions=[
                "region", "city",
                {"expression": "rank(amount:sum(partition_by=region) - "
                               "amount:sum(partition_by=city), "
                               "partition_by=channel)", "name": "x"},
            ],
            measures=[s],
        ),
    }


async def _generate_one(query, dialect: str):
    models = dev1839_models()
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
