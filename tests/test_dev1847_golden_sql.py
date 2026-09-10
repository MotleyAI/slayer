"""DEV-1847 golden SQL (task 6.1) — re-aggregation shapes across seven Tier-1
dialects, blessed as the stacked producer-over-producer SQL (originally
recorded as feature-missing raises; re-blessed per the ALLOWED_DELTAS
protocol when the feature landed)."""

from __future__ import annotations

from pathlib import Path

from tests._dev1847_fixtures import INNER_CR, INNER_CRP, dev1847_models
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise
from slayer.core.query import SlayerQuery

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1847_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]
ALLOWED_DELTAS: dict[str, str] = {}


def _cases() -> dict:
    return {
        "reagg/avg_by_region": {
            "kw": {"dimensions": ["region"],
                   "measures": [{"formula": f"avg({INNER_CR})", "name": "acr"}]}},
        "reagg/count_by_region": {
            "kw": {"dimensions": ["region"],
                   "measures": [{"formula": f"count({INNER_CR})", "name": "nc"}]}},
        "reagg/degenerate": {
            "kw": {"dimensions": ["region"],
                   "measures": [{"formula": "avg(sum(amount))", "name": "d"}]}},
        "reagg/broadcast_city": {
            "kw": {"dimensions": ["region"], "to_many_handling": "broadcast",
                   "measures": [{"formula": "avg(sum(amount, partition_by=city))",
                                 "name": "acc"}]}},
        "reagg/associate_city": {
            "kw": {"dimensions": ["region"], "to_many_handling": "associate",
                   "measures": [{"formula": "avg(sum(amount, partition_by=city))",
                                 "name": "acc"}]}},
        "reagg/explicit_grain": {
            "kw": {"dimensions": ["region", "product"],
                   "measures": [{"formula": f"avg({INNER_CR}, partition_by=region)",
                                 "name": "acr"}]}},
        "reagg/depth3": {
            "kw": {"dimensions": ["product"],
                   "measures": [{"formula":
                                 f"max(avg({INNER_CRP}, partition_by=[region, product]))",
                                 "name": "d3"}]}},
    }


async def _generate_one(case, dialect: str):
    models = dev1847_models()
    try:
        query = SlayerQuery(source_model="sales", **case["kw"])
        return await _engine_generate(
            query=query, model=models[0], extra_models=models[1:],
            dialect=dialect, validate=False,
        )
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


def test_reaggregation_cases_generate_not_raise(baseline) -> None:
    """Feature-missing tripwire: every case must generate SQL rather than record
    a raise. Fails until the outer wrap compiles. (The SQL *shape* — stacked
    producers, flat WITH, closed scopes — is pinned by the nesting/equivalence
    suites; the degenerate and broadcast cases deliberately need not emit two
    GROUP BY levels.)"""
    for key, value in baseline.items():
        assert not isinstance(value, dict), f"{key} still raises: {value}"

    # The non-degenerate re-aggregations that group at two grains DO stack two
    # producers; the degenerate/broadcast identities are exempt.
    stacked = {k for k in baseline if not k.startswith(("reagg/degenerate",
                                                        "reagg/broadcast_city"))}
    for key in stacked:
        assert baseline[key].upper().count("GROUP BY") >= 2, (
            f"{key} lacks the stacked producer GROUP BY levels")
