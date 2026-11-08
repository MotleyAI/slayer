"""DEV-1838 design D2 — fusion snapshots: today's fast-path SQL, pinned
byte-for-byte BEFORE the node fold, across five dialects.

These cases are the fusion fixed points (plain single SELECT / single-CTE
bodies). The stage-3 node pipeline must keep every one byte-identical — a
shape failing a fusion precondition may only ADD a CTE via the class-(b)
protocol, never change these. Blessed at authoring time (unlike the feature
suites, this baseline pins the PRESENT).
"""

from __future__ import annotations

from pathlib import Path

from tests._dev1838_fixtures import (
    ModelMeasure,
    OrderItem,
    dev1838_models,
    month_td,
    q,
)
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1838_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]
ALLOWED_DELTAS: dict[str, str] = {}


def _cases() -> dict:
    return {
        "fuse/plain_hidden_order": q(
            dimensions=["status"],
            measures=[ModelMeasure(formula="amount:sum", name="m")],
            order=[OrderItem(column="amount:avg", direction="desc")],
        ),
        "fuse/post_filter_on_transform": q(
            dimensions=["status"], time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum", name="m"),
                ModelMeasure(formula="cumsum(amount:sum)", name="x"),
            ],
            filters=["cumsum(amount:sum) > 50"],
        ),
        "fuse/multi_alias": q(
            dimensions=["status"],
            measures=[
                ModelMeasure(formula="amount:sum", name="m1"),
                ModelMeasure(formula="amount:sum", name="m2"),
            ],
        ),
        "fuse/dim_only_dedup": q(dimensions=["region", "city"]),
        "fuse/scalar_local": q(
            measures=[ModelMeasure(formula="amount:sum", name="m")],
        ),
        "fuse/scalar_cm_spine": q(
            measures=[ModelMeasure(formula="customers.spend:sum", name="cm")],
        ),
    }


async def _generate_one(query, dialect: str):
    models = dev1838_models()
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


def test_every_case_renders_without_placeholders(baseline) -> None:  # noqa: F821
    for key, value in baseline.items():
        assert not isinstance(value, dict), f"{key} unexpectedly raised: {value}"
        assert "__regroup__" not in value, f"{key} leaked a placeholder"
