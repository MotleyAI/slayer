"""DEV-1841 golden SQL (task 3.2 / 4.5) — the two-level association producer
across seven Tier-1 dialects, plus cross-model broadcast byte-identity
tripwires.

Blessed BEFORE implementation, so every ``bcast/`` case pins today's bytes (the
default must stay byte-identical) and every ``assoc/`` case records today's
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

from tests._dev1840_fixtures import dev1840_models
from tests._dev1841_fixtures import ModelMeasure, SlayerQuery, dev1841_models
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1841_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery",
            "snowflake"]
ALLOWED_DELTAS: dict[str, str] = {}

_MODEL_SETS = {
    "default": dev1840_models,
    "ext": dev1841_models,
}

CM = ModelMeasure(formula="customers.spend:sum", name="cm")
NC = ModelMeasure(formula="customers.*:count", name="nc")
POP = ModelMeasure(formula="customers.regions.pop:sum", name="pop")
MED = ModelMeasure(formula="customers.spend:median", name="med")
RENT = ModelMeasure(formula="stores.rent:sum", name="rent")
GS = ModelMeasure(formula="customers.gold_spend:sum", name="gs")
PART = ModelMeasure(formula="customers.spend:sum(partition_by=status)", name="cm")


def _cases() -> dict:
    return {
        "assoc/spend_by_status": {"models": "default", "mode": "associate",
                                  "kw": {"dimensions": ["status"], "measures": [CM]}},
        "assoc/count_by_status": {"models": "default", "mode": "associate",
                                  "kw": {"dimensions": ["status"], "measures": [NC]}},
        "assoc/pop_by_status": {"models": "default", "mode": "associate",
                                "kw": {"dimensions": ["status"], "measures": [POP]}},
        "assoc/median_by_status": {"models": "default", "mode": "associate",
                                   "kw": {"dimensions": ["status"], "measures": [MED]}},
        "assoc/rent_composite_key": {"models": "default", "mode": "associate",
                                     "kw": {"dimensions": ["status"], "measures": [RENT]}},
        "assoc/measure_local_filter": {"models": "ext", "mode": "associate",
                                       "kw": {"dimensions": ["status"], "measures": [GS]}},
        "assoc/explicit_partition": {"models": "default", "mode": "associate",
                                     "kw": {"dimensions": ["status"], "measures": [PART]}},
        "bcast/spend_by_status": {"models": "default", "mode": None,
                                  "kw": {"dimensions": ["status"], "measures": [CM]}},
        "bcast/attributable_tier": {"models": "default", "mode": None,
                                    "kw": {"dimensions": ["customers.tier"], "measures": [CM]}},
    }


async def _generate_one(case, dialect: str):
    models = _MODEL_SETS[case["models"]]()
    try:
        kw = dict(case["kw"])
        if case["mode"] is not None:
            kw["to_many_handling"] = case["mode"]
        query = SlayerQuery(source_model="orders", **kw)
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


# median/percentile have no grouped-aggregate form on these dialects — a general
# SLayer limitation (tsql.py/mysql.py build_median raise), not the association
# producer. The producer inherits the raise; it is pinned, not hidden.
_AGG_UNSUPPORTED = {f"assoc/median_by_status::{d}" for d in ("tsql", "mysql")}


def test_association_cases_emit_two_level_group_by(baseline) -> None:
    """Feature-missing tripwire: each associate case must generate (not raise)
    and carry the two-level GROUP BY of the dedup-then-aggregate producer, except
    where the level-2 aggregate is unsupported by the dialect itself (pinned as a
    NotImplementedError); broadcast cases must generate today unchanged."""
    for key, value in baseline.items():
        case_id = key.rsplit("::", 1)[0]
        if key in _AGG_UNSUPPORTED:
            assert isinstance(value, dict), (
                f"{key} should pin the dialect's median limitation, got {value!r}")
            assert value.get("error") == "NotImplementedError", (
                f"{key} should pin the dialect's median limitation, got {value!r}")
        elif case_id.startswith("assoc/"):
            assert not isinstance(value, dict), f"{key} still raises: {value}"
            assert value.upper().count("GROUP BY") >= 2, (
                f"{key} lacks the two-level association GROUP BY")
        else:
            assert not isinstance(value, dict), f"{key} unexpectedly raised: {value}"
