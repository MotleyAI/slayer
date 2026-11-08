"""DEV-1740 Part A golden SQL — CASE WHEN rendering across five dialects.

``build_baseline`` forces ``SLAYER_VALIDATE_SCOPES=1`` so each entry is
scope-closed. The baseline is blessed alongside the implementation
(``SLAYER_UPDATE_GOLDEN=1``); until Part A lands every case records a raise.
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import SlayerQuery

from tests._dev1740_fixtures import dev1740_models
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise


GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1740_conditional_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]
ALLOWED_DELTAS: dict[str, str] = {}


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def _cases() -> dict:
    return {
        "measure/simple": _q(
            dimensions=["region"],
            measures=[
                {"formula": "CASE WHEN amount:sum >= 10000 THEN 1 ELSE 0 END",
                 "name": "big"},
            ],
        ),
        "measure/nested": _q(
            dimensions=["region"],
            measures=[
                {"formula": "CASE WHEN amount:sum >= 14000 THEN 2 "
                            "WHEN amount:sum >= 8000 THEN 1 ELSE 0 END",
                 "name": "tier"},
            ],
        ),
        "measure/iif": _q(
            dimensions=["region"],
            measures=[{"formula": "iif(amount:sum >= 10000, 1, 0)", "name": "big"}],
        ),
        "measure/missing_else_null": _q(
            dimensions=["region"],
            measures=[
                {"formula": "CASE WHEN amount:sum >= 10000 THEN 1 END", "name": "big"},
            ],
        ),
        "filter/case_predicate": _q(
            dimensions=["region"],
            filters=["CASE WHEN amount:sum >= 10000 THEN 1 ELSE 0 END == 1"],
            measures=[{"formula": "amount:sum", "name": "rev"}],
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


def test_every_case_emits_sql_with_a_case_expression(baseline) -> None:
    """Every Part A case must produce SQL (none are guards) AND contain a SQL
    CASE. Requiring non-raising is what stops an all-raises baseline from
    satisfying this vacuously."""
    for key, value in baseline.items():
        assert not isinstance(value, dict), f"{key} unexpectedly raised: {value}"
        assert "CASE" in value.upper(), f"{key} lost its CASE expression"
