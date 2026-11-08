"""DEV-1835 golden SQL — flagship migrated shapes across five dialects, plus
the generation smoke (tsql / bigquery / one case-folding dialect).

``build_baseline`` forces ``SLAYER_VALIDATE_SCOPES=1`` so each entry is
scope-closed; the baseline is blessed alongside the implementation after the
divergence batch is approved (``SLAYER_UPDATE_GOLDEN=1``, design D10) — until
then every case fails with the missing-baseline message, which is this suite's
feature-missing state.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

from slayer.sql.scope_check import assert_scope_closed

from tests._dev1835_fixtures import (
    BAND35,
    ModelMeasure,
    dev1835_models,
    month_td,
    q,
)
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1835_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]
SMOKE_DIALECTS = ["tsql", "bigquery", "snowflake"]
ALLOWED_DELTAS: dict[str, str] = {}

BAND = {"expression": BAND35, "name": "band"}
RBAND = {
    "expression": "CASE WHEN amount:sum(partition_by=region) > 55 THEN 1 ELSE 0 END",
    "name": "rband",
}


def _cases() -> dict:
    return {
        "mig/bare_wm": q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum", name="m"),
                ModelMeasure(formula="amount:sum(window='90d')", name="w"),
            ],
        ),
        "mig/bare_rk": q(
            dimensions=["region"],
            measures=[
                ModelMeasure(formula="amount:sum", name="m"),
                ModelMeasure(formula="amount:last", name="l"),
            ],
        ),
        "lift/band_x_wm": q(
            dimensions=["region", BAND], time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum", name="m"),
                ModelMeasure(formula="amount:sum(window='1y')", name="w"),
            ],
        ),
        "lift/cumsum_over_bare_wm": q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="cumsum(amount:sum(window='90d'))", name="x"),
            ],
        ),
        "dedup/dual_role": q(
            dimensions=["region", RBAND],
            measures=[
                ModelMeasure(formula="amount:sum", name="m"),
                ModelMeasure(formula="amount:sum(partition_by=region)", name="rt"),
            ],
        ),
    }


async def _generate_one(query, dialect: str):
    models = dev1835_models()
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


def test_every_case_renders_without_placeholders(baseline) -> None:
    for key, value in baseline.items():
        assert not isinstance(value, dict), f"{key} unexpectedly raised: {value}"
        assert "__regroup__" not in value, f"{key} leaked a placeholder"


def test_no_case_uses_the_deleted_family_prefixes(baseline) -> None:
    """Design D3 — producers render under the uniform ``_cm_`` naming."""
    for key, value in baseline.items():
        assert "_wm_" not in value, f"{key} still names a _wm_ relation"
        assert "_rk_" not in value, f"{key} still names a _rk_ relation"


FLAGSHIP = ["mig/bare_wm", "lift/band_x_wm", "lift/cumsum_over_bare_wm"]


@pytest.mark.parametrize("case_id", FLAGSHIP)
@pytest.mark.parametrize("dialect", SMOKE_DIALECTS)
async def test_generation_smoke(case_id: str, dialect: str) -> None:
    """Parses, one flat WITH (no nesting), scope-closed."""
    sql = await _generate_one(_cases()[case_id], dialect)
    assert not isinstance(sql, dict), f"raised: {sql}"
    tree = sqlglot.parse_one(sql, read=dialect)
    assert tree is not None
    with_nodes = list(tree.find_all(exp.With))
    assert len(with_nodes) == 1, f"{len(with_nodes)} WITH clauses:\n{sql}"
    assert_scope_closed(sql, dialect=dialect)
