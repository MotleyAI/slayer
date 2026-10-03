"""Golden SQL for rank-family window ordering and NULL isolation; mechanics in ``tests/_golden_harness.py``."""

from __future__ import annotations

from pathlib import Path

from slayer.core.models import ModelMeasure
from slayer.core.query import SlayerQuery

from tests._dev1847_fixtures import gen, sales_q
from tests._golden_harness import bind_golden_tests, record_raise
from tests._rank_direction_fixtures import DIALECTS

GOLDEN_PATH = Path(__file__).parent / "golden" / "rank_direction_sql_baseline.json"

# ``<case_id>::<dialect>`` -> why this entry is allowed to change right now.
ALLOWED_DELTAS: dict[str, str] = {}


def _q(formula: str, dimensions: list[str]) -> SlayerQuery:
    return sales_q(dimensions=dimensions, measures=[ModelMeasure(formula=formula, name="r")])


def _cases() -> dict[str, SlayerQuery]:
    region, region_city = ["region"], ["region", "city"]
    return {
        "rank/asc": _q("rank(sum(amount), direction='asc')", region),
        "rank/desc": _q("rank(sum(amount), direction='desc')", region),
        "rank/asc_partitioned": _q("rank(sum(amount), partition_by=region, direction='asc')", region_city),
        "dense_rank/asc": _q("dense_rank(sum(amount), direction='asc')", region),
        "dense_rank/desc": _q("dense_rank(sum(amount), direction='desc')", region),
        "ntile/n4": _q("ntile(sum(amount), n=4)", region),
        "percent_rank/plain": _q("percent_rank(sum(amount))", region),
        "rank/non_numeric": _q("rank(min(city), direction='asc')", region),
    }


async def _generate_one(query: SlayerQuery, dialect: str):
    """Emitted SQL, or a structured record of the raised error."""
    try:
        return await gen(query, dialect=dialect)
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
