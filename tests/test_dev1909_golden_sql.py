"""Golden SQL for the host-population pushdown shapes across seven Tier-1
dialects. Blessed pre-implementation as today's feature-missing output (a
recorded raise for the inline-aggregate shapes, a fanning join for the producer
shapes). At implementation each ``pop_push/`` case flips to a correlated
semi-join (``EXISTS``, no fanning join), enters ALLOWED_DELTAS, is re-blessed,
and the manifest is emptied. ``test_population_pushes_carry_exists`` is this
module's feature-missing tripwire until then. The ``inline/`` shapes cross a
provably to-one hop and MUST stay byte-identical SQL through the change.
"""

from __future__ import annotations

from pathlib import Path

from tests._dev1900_fixtures import dev1900_models
from tests._engine_helpers import _engine_generate, _join_aliases
from tests._golden_harness import bind_golden_tests, record_raise
from slayer.core.query import SlayerQuery

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1909_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]
ALLOWED_DELTAS: dict[str, str] = {}

_MODEL_SETS = {"dev1900": dev1900_models}


def _cases() -> dict:
    return {
        # pop_push/* — the population restricts by association; must carry EXISTS
        # and drop the fanning join after the change (today: raise or fanning SQL).
        "pop_push/structural": {
            "models": "dev1900", "source": "customers", "mode": None,
            "kw": {"measures": [{"formula": "spend:sum", "name": "w"}],
                   "filters": ["orders.status = 'ok'"]}},
        "pop_push/derived_orders": {
            "models": "dev1900", "source": "orders", "mode": None,
            "kw": {"measures": [{"formula": "amount:sum", "name": "amt"}],
                   "filters": ["customers.regions.bad_pop > 0"]}},
        "pop_push/two_branch": {
            "models": "dev1900", "source": "customers", "mode": None,
            "kw": {"measures": [{"formula": "spend:sum", "name": "w"}],
                   "filters": ["orders.status = 'ok'",
                               "regions.region_events.value >= 50"]}},
        "pop_push/partitioned": {
            "models": "dev1900", "source": "customers", "mode": None,
            "kw": {"dimensions": ["tier"],
                   "measures": [{"formula": "sum(spend, partition_by=tier)",
                                 "name": "w"}],
                   "filters": ["orders.status = 'ok'"]}},
        "pop_push/nested": {
            "models": "dev1900", "source": "customers", "mode": None,
            "kw": {"dimensions": ["tier"],
                   "measures": [{"formula":
                                 "avg(sum(spend, partition_by=tier))", "name": "w"}],
                   "filters": ["orders.status = 'ok'"]}},
        # inline/* — a provably to-one filter; byte-identical SQL through the change.
        "inline/to_one": {
            "models": "dev1900", "source": "orders", "mode": None,
            "kw": {"measures": [{"formula": "amount:sum", "name": "amt"}],
                   "filters": ["customers.tier = 'gold'"]}},
    }


PUSH_CASES = {k for k in _cases() if k.startswith("pop_push/")}

#: The fanning relation each push case must restrict by EXISTS, never join.
_PUSH_ABSENT_TABLE = {
    "pop_push/structural": "orders",
    "pop_push/derived_orders": "region_events",
    "pop_push/two_branch": "orders",
    "pop_push/partitioned": "orders",
    "pop_push/nested": "orders",
}


async def _generate_one(case, dialect: str):
    models = _MODEL_SETS[case["models"]]()
    try:
        kw = dict(case["kw"])
        if case["mode"] is not None:
            kw["to_many_handling"] = case["mode"]
        query = SlayerQuery(source_model=case["source"], **kw)
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


def test_population_pushes_carry_exists(baseline) -> None:
    """Feature-missing tripwire: every population-push shape must emit a
    correlated semi-join (EXISTS) and no longer join the fanning relation. Red
    until the pushdown lands (today: a recorded raise or a fanning join)."""
    for key, value in baseline.items():
        case_id, dialect = key.rsplit("::", 1)
        if case_id not in PUSH_CASES:
            continue
        assert not isinstance(value, dict), f"{key} still fails closed: {value}"
        assert "EXISTS" in value.upper(), f"{key} lacks the semi-join:\n{value}"
        absent = _PUSH_ABSENT_TABLE[case_id]
        assert absent not in _join_aliases(value, dialect=dialect), (
            f"{key} still joins the fanning relation {absent!r}:\n{value}")


def test_inline_cases_stay_plain_sql(baseline) -> None:
    """A provably to-one filter stays a plain row restriction — real SQL, no
    semi-join — byte-identical through the change."""
    for key, value in baseline.items():
        if key.split("::", 1)[0] in PUSH_CASES:
            continue
        assert not isinstance(value, dict), f"{key} unexpectedly raises: {value}"
        assert "EXISTS" not in value.upper(), f"{key} grew an EXISTS:\n{value}"
