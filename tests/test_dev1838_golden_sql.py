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

from slayer.core.query import QueryMetric

from tests._dev1838_fixtures import (
    OrderItem,
    dev1838_datasets,
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
            dimensions=["orders.status"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="m")],
            order=[OrderItem.model_validate({"field": "avg(orders.amount)", "direction": "desc"})],
        ),
        "fuse/post_filter_on_transform": q(
            dimensions=["orders.status"], time_dimensions=month_td(),
            metrics=[
                QueryMetric(metric="sum(orders.amount)", alias="m"),
                QueryMetric(metric="cumsum(sum(orders.amount))", alias="x"),
            ],
            filter=["cumsum(sum(orders.amount)) > 50"],
        ),
        "fuse/multi_alias": q(
            dimensions=["orders.status"],
            metrics=[
                QueryMetric(metric="sum(orders.amount)", alias="m1"),
                QueryMetric(metric="sum(orders.amount)", alias="m2"),
            ],
        ),
        "fuse/dim_only_dedup": q(dimensions=["orders.region", "orders.city"]),
        "fuse/scalar_local": q(
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="m")],
        ),
        "fuse/scalar_cm_spine": q(
            metrics=[QueryMetric(metric="sum(customers.spend)", alias="cm")],
        ),
    }


async def _generate_one(query, dialect: str):
    models = dev1838_datasets()
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


def test_every_case_renders_without_placeholders(baseline) -> None:  # noqa: F821
    for key, value in baseline.items():
        assert not isinstance(value, dict), f"{key} unexpectedly raised: {value}"
        assert "__regroup__" not in value, f"{key} leaked a placeholder"
