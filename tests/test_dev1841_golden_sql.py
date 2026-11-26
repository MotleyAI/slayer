"""Golden SQL — the two-level association producer
across seven Tier-1 dialects, plus cross-dataset broadcast byte-identity
tripwires.

Blessed BEFORE implementation, so every ``bcast/`` case pins today's bytes
(broadcast must stay byte-identical) and every ``assoc/`` case records today's
feature-missing raise. At implementation each ``assoc/`` case's flip to the
nested GROUP BY enters ALLOWED_DELTAS, is re-blessed, and the manifest is
emptied. ``test_association_cases_emit_two_level_group_by`` is this module's
feature-missing failure until then.

Query construction is deferred into ``_generate_one`` (never at import) so the
associate cases record a raise pre-implementation instead of breaking
collection.
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import QueryMetric

from tests._dev1840_fixtures import dev1840_datasets
from tests._dev1841_fixtures import SlayerQuery, dev1841_datasets
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1841_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery",
            "snowflake"]

_DATASET_SETS = {
    "default": dev1840_datasets,
    "ext": dev1841_datasets,
}

CM = QueryMetric(metric="sum(customers.spend)", alias="cm")
NC = QueryMetric(metric="count(customers.*)", alias="nc")
POP = QueryMetric(metric="sum(customers.regions.pop)", alias="pop")
MED = QueryMetric(metric="median(customers.spend)", alias="med")
RENT = QueryMetric(metric="sum(stores.rent)", alias="rent")
GS = QueryMetric(metric="sum(customers.gold_spend)", alias="gs")
PART = QueryMetric(metric="sum(customers.spend, partition_by=orders.status)", alias="cm")


def _cases() -> dict:
    return {
        "assoc/spend_by_status": {"models": "default", "mode": "associate",
                                  "kw": {"dimensions": ["orders.status"], "metrics": [CM]}},
        "assoc/count_by_status": {"models": "default", "mode": "associate",
                                  "kw": {"dimensions": ["orders.status"], "metrics": [NC]}},
        "assoc/pop_by_status": {"models": "default", "mode": "associate",
                                "kw": {"dimensions": ["orders.status"], "metrics": [POP]}},
        "assoc/median_by_status": {"models": "default", "mode": "associate",
                                   "kw": {"dimensions": ["orders.status"], "metrics": [MED]}},
        "assoc/rent_composite_key": {"models": "default", "mode": "associate",
                                     "kw": {"dimensions": ["orders.status"], "metrics": [RENT]}},
        "assoc/measure_local_filter": {"models": "ext", "mode": "associate",
                                       "kw": {"dimensions": ["orders.status"], "metrics": [GS]}},
        "assoc/explicit_partition": {"models": "default", "mode": "associate",
                                     "kw": {"dimensions": ["orders.status"], "metrics": [PART]}},
        "bcast/spend_by_status": {"models": "default", "mode": "broadcast",
                                  "kw": {"dimensions": ["orders.status"], "metrics": [CM]}},
        "bcast/attributable_tier": {"models": "default", "mode": None,
                                    "kw": {"dimensions": ["customers.tier"], "metrics": [CM]}},
    }


async def _generate_one(case, dialect: str):
    models = _DATASET_SETS[case["models"]]()
    try:
        kw = dict(case["kw"])
        if case["mode"] is not None:
            kw["to_many_handling"] = case["mode"]
        query = SlayerQuery(population="orders", **kw)
        return await _engine_generate(
            query=query, dataset=models[0], extra_datasets=models[1:],
            dialect=dialect, validate=False,
        )
    except Exception as exc:  # noqa: BLE001 — the raise itself is the contract
        return record_raise(exc)


ALLOWED_DELTAS: dict[str, str] = {}  # PENDING re-bless list; empty in a committed state.

bind_golden_tests(
    namespace=globals(),
    golden_path=GOLDEN_PATH,
    cases=_cases,
    dialects=DIALECTS,
    allowed=ALLOWED_DELTAS,
    generate_one=_generate_one,
)


# median/percentile have no grouped-aggregate form on these dialects — a general
# SLayer limitation (tsql.py/mysql.py build_median raise), not the association
# producer. The producer inherits the raise; it is pinned, not hidden.
def test_association_cases_emit_two_level_group_by(baseline) -> None:
    """Feature-missing tripwire: each associate case must generate (not raise)
    and carry the two-level GROUP BY of the dedup-then-aggregate producer;
    broadcast cases must generate today unchanged."""
    for key, value in baseline.items():
        case_id = key.rsplit("::", 1)[0]
        if case_id.startswith("assoc/"):
            assert not isinstance(value, dict), f"{key} still raises: {value}"
            assert value.upper().count("GROUP BY") >= 2, (
                f"{key} lacks the two-level association GROUP BY")
        else:
            assert not isinstance(value, dict), f"{key} unexpectedly raised: {value}"
