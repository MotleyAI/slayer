"""DEV-1865 golden SQL — the newly-legal filter/order shapes, pinned per dialect.

The baseline is seeded during implementation (``SLAYER_UPDATE_GOLDEN=1 poetry
run pytest tests/test_dev1865_golden_sql.py``) once the shapes compile; until
then these fail with "golden baseline missing", which is the correct pre-impl
state. Currently-legal shapes are covered by the existing golden suites, which
must keep passing byte-identically.
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import QueryMetric

from tests._dev1865_fixtures import BAND35, SlayerQuery, dev1865_models
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1865_sql_baseline.json"

DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]

ALLOWED_DELTAS: dict[str, str] = {}


def _q(**kw) -> SlayerQuery:
    kw.setdefault("population", "orders")
    return SlayerQuery(**kw)


def _cases() -> dict:
    band = {"field": BAND35, "alias": "band"}
    cm_part = "sum(customers.spend, partition_by=customers.regions.name)"
    return {
        "filter/cross_model_partitioned": _q(
            dimensions=["customers.regions.name", "customers.tier"],
            filter=[f"{cm_part} > 100"],
            metrics=[QueryMetric(metric="sum(customers.spend)", alias="sp")],
        ),
        "filter/computed_dim_mix_field": _q(
            dimensions=["region", "city", band],
            filter=["band == 1 or channel == 'app'"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ),
        "filter/computed_dim_mix_measure": _q(
            dimensions=["region", "city", band],
            filter=["sum(amount, partition_by=city) > 45 or sum(amount) > 55"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ),
        "order/cross_model_partitioned": _q(
            dimensions=["customers.regions.name", "customers.tier"],
            metrics=[QueryMetric(metric="sum(customers.spend)", alias="sp")],
            order=[{"field": cm_part, "direction": "desc"}],
        ),
    }


async def _generate_one(query: SlayerQuery, dialect: str):
    models = dev1865_models()
    try:
        return await _engine_generate(
            query=query, dataset=models[0], extra_datasets=models[1:],
            dialect=dialect, validate=False,
        )
    except Exception as exc:  # noqa: BLE001 — the exception itself is the contract
        return record_raise(exc)


bind_golden_tests(
    namespace=globals(),
    golden_path=GOLDEN_PATH,
    cases=_cases,
    dialects=DIALECTS,
    allowed=ALLOWED_DELTAS,
    generate_one=_generate_one,
)
