"""DEV-1840 golden SQL — EXISTS pushdown shapes plus the inline byte-identity
tripwires across seven Tier-1 dialects (ClickHouse is covered by the gating
suite, whose dry-runs need a mocked server version).

The baseline is blessed BEFORE the implementation, so every ``inline/`` case
pins today's bytes (the N:1 tripwire, including the composite-key shape the
corpus lacked). Each ``exists/`` case's flip to a semi-join is an approved
divergence: at implementation time it enters ALLOWED_DELTAS, is re-blessed,
and the manifest is emptied again. ``test_exists_cases_carry_an_exists`` is
this module's feature-missing failure until then.
"""

from __future__ import annotations

from pathlib import Path

from tests._dev1840_fixtures import (
    ModelMeasure,
    SPEND_BAND_170,
    ambiguity_models,
    dev1840_models,
    q,
    tq,
)
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1840_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery",
            "snowflake"]
ALLOWED_DELTAS: dict[str, str] = {}

_MODEL_SETS = {
    "default": lambda: dev1840_models(),
    "weak": lambda: dev1840_models(strong_plans=False),
    "rev": lambda: dev1840_models(declare_reverse=True),
    "amb": ambiguity_models,
}

M = ModelMeasure(formula="amount:sum", name="m")
CM = ModelMeasure(formula="customers.spend:sum", name="cm")
RM = ModelMeasure(formula="stores.rent:sum", name="rm")
SM = ModelMeasure(formula="agents.score:sum", name="sm")


def _cases() -> dict:
    return {
        "exists/app_by_tier": {"models": "default", "query": q(
            dimensions=["customers.tier"], measures=[M, CM],
            filters=["channel = 'app'"])},
        "exists/same_branch_two_conjuncts": {"models": "default", "query": q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["status = 'ok'", "channel = 'app'"])},
        "exists/two_branches": {"models": "weak", "query": q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["channel = 'web'", "customers.plans.level = 'basic'"])},
        "exists/composite_inverted_hop": {"models": "default", "query": q(
            dimensions=["status"], measures=[M, RM],
            filters=["channel = 'app'"])},
        "exists/multi_hop_union_tree": {"models": "default", "query": q(
            dimensions=["status"], measures=[M, RM],
            filters=["channel = 'app'", "customers.tier = 'gold'"])},
        "exists/correlated_outer_ref": {"models": "default", "query": q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["customers.spend > amount"])},
        "exists/forward_unproven_hop": {"models": "weak", "query": q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["customers.plans.level = 'basic'"])},
        "exists/derived_deps_cust_tier": {"models": "default", "query": q(
            dimensions=["status"], measures=[CM],
            filters=["cust_tier = 'gold'"])},
        "exists/derived_1n_last_status": {"models": "rev", "query": q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["customers.last_status = 'ok'"])},
        "exists/nested_band_producer": {"models": "default", "query": q(
            dimensions=[{"expression": SPEND_BAND_170, "name": "sband"}],
            measures=[M], filters=["channel = 'app'"])},
        "exists/declared_reverse": {"models": "rev", "query": q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["channel = 'app'"])},
        "exists/negated_cross_path": {"models": "default", "query": q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["NOT (channel = 'app')"])},
        "inline/safe_hop_regions": {"models": "default", "query": q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["customers.regions.name = 'North'"])},
        "inline/strong_plans_basic": {"models": "default", "query": q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["customers.plans.level = 'basic'"])},
        "inline/composite_n1_root_local": {"models": "default", "query": q(
            dimensions=["status"], measures=[M, RM],
            filters=["stores.city = 'NYC'"])},
        "excluded/mixed_or": {"models": "default", "query": q(
            dimensions=["customers.tier"], measures=[CM],
            filters=["customers.tier = 'gold' OR channel = 'app'"])},
        "excluded/ambiguous_inversion": {"models": "amb", "query": tq(
            measures=[SM], filters=["effort > 2"])},
    }


async def _generate_one(case, dialect: str):
    models = _MODEL_SETS[case["models"]]()
    try:
        return await _engine_generate(
            query=case["query"], model=models[0], extra_models=models[1:],
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


def test_exists_cases_carry_an_exists(baseline) -> None:
    """Feature-missing tripwire: every pushdown case must emit a semi-join;
    inline and excluded cases must not."""
    for key, value in baseline.items():
        case_id = key.rsplit("::", 1)[0]
        assert not isinstance(value, dict), f"{key} unexpectedly raised: {value}"
        assert "__regroup__" not in value, f"{key} leaked a placeholder"
        if case_id.startswith("exists/"):
            assert "EXISTS" in value.upper(), f"{key} lacks the semi-join"
        else:
            assert "EXISTS" not in value.upper(), f"{key} grew an EXISTS"
