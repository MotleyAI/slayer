"""DEV-1740 Part B2 golden SQL — the desugared regroup across five dialects.

``build_baseline`` forces ``SLAYER_VALIDATE_SCOPES=1`` so each working entry is
scope-closed; guards record their raise. The baseline is blessed alongside the
implementation (``SLAYER_UPDATE_GOLDEN=1``).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from slayer.core.query import SlayerQuery

from tests._dev1740_fixtures import dev1740_models, month_td
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

# Five-dialect golden for the B2 desugar — deferred to DEV-1825 (regroup
# primitive). The baseline is blessed when the desugar lands.
pytestmark = pytest.mark.skip(
    reason="B2 aggregate-then-regroup deferred to DEV-1825 (regroup primitive)"
)

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1740_regroup_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]
ALLOWED_DELTAS: dict[str, str] = {}

WORKING_PREFIX = "regroup/"
BAND = "CASE WHEN amount:sum(partition_by=city) > 5000 THEN 1 ELSE 0 END"


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def _cases() -> dict:
    return {
        "regroup/flagship": _q(
            dimensions=["region", {"expression": BAND, "name": "band"}],
            measures=[{"formula": "amount:sum", "name": "band_total"}],
        ),
        "regroup/cross_model_partition": _q(
            dimensions=[
                "region",
                {"expression": "CASE WHEN amount:sum(partition_by=customers.region_id) "
                               "> 10000 THEN 1 ELSE 0 END", "name": "band"},
            ],
            measures=[{"formula": "amount:sum", "name": "s"}],
        ),
        "regroup/filter_on_band": _q(
            dimensions=["region", {"expression": BAND, "name": "band"}],
            filters=["band == 1"],
            measures=[{"formula": "amount:sum", "name": "band_total"}],
        ),
        "guard/missing_partition_by": _q(
            dimensions=[
                "region",
                {"expression": "CASE WHEN amount:sum > 5000 THEN 1 ELSE 0 END",
                 "name": "band"},
            ],
            measures=[{"formula": "amount:sum", "name": "t"}],
        ),
        "guard/transform_in_dim": _q(
            dimensions=[
                "region",
                {"expression": "CASE WHEN cumsum(amount:sum) > 5000 THEN 1 ELSE 0 END",
                 "name": "x"},
            ],
            time_dimensions=month_td(),
            measures=[{"formula": "amount:sum", "name": "t"}],
        ),
        "guard/ddv_false": _q(
            dimensions=[{"expression": BAND, "name": "band"}],
            distinct_dimension_values=False,
        ),
    }


async def _generate_one(query: SlayerQuery, dialect: str):
    models = dev1740_models()
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


def test_working_cases_regroup_and_guards_raise(baseline) -> None:
    for key, value in baseline.items():
        case_id = key.rsplit("::", 1)[0]
        if case_id.startswith(WORKING_PREFIX):
            assert not isinstance(value, dict), f"{key} unexpectedly raised: {value}"
            # The desugar isolates the finer-grain aggregate in its own CTE.
            assert "_cm_" in value, f"{key} did not isolate the partitioned aggregate"
        else:
            assert isinstance(value, dict), f"{key} should raise, got SQL"
