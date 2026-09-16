"""DEV-1865 golden SQL — the newly-legal filter/order shapes, pinned per dialect.

The baseline is seeded during implementation (``SLAYER_UPDATE_GOLDEN=1 poetry
run pytest tests/test_dev1865_golden_sql.py``) once the shapes compile; until
then these fail with "golden baseline missing", which is the correct pre-impl
state. Currently-legal shapes are covered by the existing golden suites, which
must keep passing byte-identically.
"""

from __future__ import annotations

from pathlib import Path

from tests._dev1865_fixtures import BAND35, ModelMeasure, SlayerQuery, dev1865_models
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1865_sql_baseline.json"

DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]

ALLOWED_DELTAS: dict[str, str] = {}


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def _cases() -> dict:
    band = {"expression": BAND35, "name": "band"}
    cm_part = "customers.spend:sum(partition_by=customers.regions.name)"
    return {
        "filter/cross_model_partitioned": _q(
            dimensions=["customers.regions.name", "customers.tier"],
            filters=[f"{cm_part} > 100"],
            measures=[ModelMeasure(formula="customers.spend:sum", name="sp")],
        ),
        "filter/computed_dim_mix_field": _q(
            dimensions=["region", "city", band],
            filters=["band == 1 or channel == 'app'"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ),
        "filter/computed_dim_mix_measure": _q(
            dimensions=["region", "city", band],
            filters=["amount:sum(partition_by=city) > 45 or amount:sum > 55"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ),
        "order/cross_model_partitioned": _q(
            dimensions=["customers.regions.name", "customers.tier"],
            measures=[ModelMeasure(formula="customers.spend:sum", name="sp")],
            order=[{"column": cm_part, "direction": "desc"}],
        ),
    }


async def _generate_one(query: SlayerQuery, dialect: str):
    models = dev1865_models()
    try:
        return await _engine_generate(
            query=query, model=models[0], extra_models=models[1:],
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
