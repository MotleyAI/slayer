"""DEV-1740 Part B2 golden SQL — the desugared regroup across five dialects.

``build_baseline`` forces ``SLAYER_VALIDATE_SCOPES=1`` so each working entry is
scope-closed; guards record their raise. The baseline is blessed alongside the
implementation (``SLAYER_UPDATE_GOLDEN=1``).
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import SlayerQuery

from tests._dev1740_fixtures import dev1740_datasets, month_td
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1740_regroup_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]
ALLOWED_DELTAS: dict[str, str] = {}

WORKING_PREFIX = "regroup/"
BAND = "CASE WHEN sum(orders.amount, partition_by=orders.city) > 5000 THEN 1 ELSE 0 END"


def _q(**kw) -> SlayerQuery:
    kw.setdefault("population", "orders")
    return SlayerQuery(**kw)


def _cases() -> dict:
    return {
        "regroup/flagship": _q(
            dimensions=["orders.region", {"field": BAND, "alias": "band"}],
            metrics=[{"metric": "sum(orders.amount)", "alias": "band_total"}],
        ),
        "regroup/cross_model_partition": _q(
            dimensions=[
                "orders.region",
                {"field": "CASE WHEN sum(orders.amount, partition_by=customers.region_id) "
                               "> 10000 THEN 1 ELSE 0 END", "alias": "band"},
            ],
            metrics=[{"metric": "sum(orders.amount)", "alias": "s"}],
        ),
        "regroup/filter_on_band": _q(
            dimensions=["orders.region", {"field": BAND, "alias": "band"}],
            filter=["band == 1"],
            metrics=[{"metric": "sum(orders.amount)", "alias": "band_total"}],
        ),
        "guard/missing_partition_by": _q(
            dimensions=[
                "orders.region",
                {"field": "CASE WHEN sum(orders.amount) > 5000 THEN 1 ELSE 0 END",
                 "alias": "band"},
            ],
            metrics=[{"metric": "sum(orders.amount)", "alias": "t"}],
        ),
        "guard/transform_in_dim": _q(
            dimensions=[
                "orders.region",
                {"field": "CASE WHEN cumsum(sum(orders.amount)) > 5000 THEN 1 ELSE 0 END",
                 "alias": "x"},
            ],
            time_dimensions=month_td(),
            metrics=[{"metric": "sum(orders.amount)", "alias": "t"}],
        ),
        "guard/ddv_false": _q(
            dimensions=[{"field": BAND, "alias": "band"}],
            distinct_dimension_values=False,
        ),
    }


async def _generate_one(query: SlayerQuery, dialect: str):
    models = dev1740_datasets()
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


def test_working_cases_regroup_and_guards_raise(baseline) -> None:
    for key, value in baseline.items():
        case_id = key.rsplit("::", 1)[0]
        if case_id.startswith(WORKING_PREFIX):
            assert not isinstance(value, dict), f"{key} unexpectedly raised: {value}"
            # The desugar isolates the finer-grain aggregate in its own CTE.
            assert "_cm_" in value, f"{key} did not isolate the partitioned aggregate"
        else:
            assert isinstance(value, dict), f"{key} should raise, got SQL"
