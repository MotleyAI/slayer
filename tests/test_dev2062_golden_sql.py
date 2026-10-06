"""Golden SQL for a selected composite reused by a filter and by an order-only composite; mechanics in ``tests/_golden_harness.py``."""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import SlayerQuery

from tests._dev2062_fixtures import AOV, CROSS_RATIO, customers_model, m, monthly_q, orders_model
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev2062_sql_baseline.json"

DIALECTS = ["postgres", "tsql"]

# ``<case_id>::<dialect>`` -> why this entry is allowed to change right now.
ALLOWED_DELTAS: dict[str, str] = {}

_RATIOS = {"local": AOV, "cross": CROSS_RATIO}


def _cases() -> dict[str, SlayerQuery]:
    out: dict[str, SlayerQuery] = {}
    for label, r in _RATIOS.items():
        out[f"{label}/filter"] = monthly_q(m(r, "a"), filters=[f"cumsum(({r})) > 9"])
        out[f"{label}/order_composite"] = monthly_q(
            m(r, "a"), order=[{"column": f"cumsum(({r})) + 1", "direction": "desc"}],
        )
    return out


async def _generate_one(query: SlayerQuery, dialect: str):
    """Emitted SQL, or a structured record of the raised error."""
    try:
        return await _engine_generate(
            query=query, model=orders_model(), extra_models=[customers_model()], dialect=dialect, validate=False,
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


def test_no_case_records_an_error(baseline) -> None:
    assert not [k for k, v in baseline.items() if isinstance(v, dict) and "error" in v]
