"""Golden SQL for AST-combined WHERE / HAVING / post-phase conjuncts (DEV-1965 D3, D5)."""

from __future__ import annotations

from pathlib import Path

from tests._dev1965_fixtures import MODE_A_OR, cumsum_chain, gen, orders_model, where_having_query
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1965_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]

#: Pending re-bless manifest (empty when committed).
ALLOWED_DELTAS: dict[str, str] = {}


def _cases() -> dict:
    return {
        "where_having_or_mode_a": {"query": where_having_query(), "filters": [MODE_A_OR]},
        "post_or_conjuncts": {"query": cumsum_chain(filters=[
            "cumsum(amount:sum) > 250 or cumsum(amount:sum) < 50", "cumsum(amount:sum) < 260",
        ])},
    }


async def _generate_one(case, dialect: str):
    try:
        return await gen(case["query"], dialect=dialect,
                         model=orders_model(filters=case.get("filters")))
    except Exception as exc:  # noqa: BLE001 — the raise itself is the contract
        return record_raise(exc)


bind_golden_tests(
    namespace=globals(),
    golden_path=GOLDEN_PATH,
    cases=_cases,
    dialects=DIALECTS,
    allowed=ALLOWED_DELTAS,
    generate_one=_generate_one,
)
