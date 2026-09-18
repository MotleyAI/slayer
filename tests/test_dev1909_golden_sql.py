"""DEV-1909 golden SQL (task 1.4) — the population-restriction shapes across the
seven Tier-1 dialects. Blessed pre-implementation as today's state: the
fanning-inline shapes record the DEV-1900 guard's raise, the producer / dims-only
shapes record today's fanning SQL, and the to-one invariant records its plain SQL.
At implementation each ``fanning_inline/`` case flips from the raise to an EXISTS
base query, each ``producer/`` and ``dims_only/`` case gains the semi-join and
loses the fanning join, all entering ALLOWED_DELTAS for a single re-bless; the
``positive/`` case MUST stay byte-identical. ``test_fanning_inline_cases_fail_closed``
is this module's feature-missing tripwire until then.
"""

from __future__ import annotations

from pathlib import Path

from tests._dev1900_fixtures import dev1900_models
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise
from slayer.core.query import SlayerQuery

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1909_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]

_OK = "orders.status = 'ok'"


def _cases() -> dict:
    return {
        # fanning_inline/* — a fanning population filter with an aggregate inline
        # over the population. Today: the DEV-1900 guard raises. After: EXISTS base.
        "fanning_inline/structural": {
            "source": "customers", "mode": None,
            "kw": {"measures": [{"formula": "spend:sum", "name": "sp"}],
                   "filters": [_OK]}},
        "fanning_inline/derived": {
            "source": "orders", "mode": None,
            "kw": {"measures": [{"formula": "amount:sum", "name": "amt"}],
                   "filters": ["customers.regions.bad_pop > 0"]}},
        "fanning_inline/count": {
            "source": "customers", "mode": None,
            "kw": {"dimensions": ["tier"],
                   "measures": [{"formula": "*:count", "name": "n"}],
                   "filters": [_OK]}},
        "fanning_inline/two_branch": {
            "source": "customers", "mode": None,
            "kw": {"measures": [{"formula": "spend:sum", "name": "sp"}],
                   "filters": [_OK, "regions.region_events.value >= 50"]}},
        # producer/* — host-rooted regroup producers that fan silently today; after
        # the change each carries the semi-join and drops the fanning join.
        "producer/partitioned": {
            "source": "customers", "mode": None,
            "kw": {"dimensions": ["tier"],
                   "measures": [{"formula": "spend:sum(partition_by=tier)", "name": "pt"}],
                   "filters": [_OK]}},
        "producer/windowed": {
            "source": "customers", "mode": None,
            "kw": {"time_dimensions": [
                {"dimension": "orders.ordered_at", "granularity": "month"}],
                   "measures": [{"formula": "spend:sum(window='1y')", "name": "w"}],
                   "filters": [_OK]}},
        "producer/producer_only": {
            "source": "customers", "mode": None,
            "kw": {"measures": [{"formula": "orders.amount:sum", "name": "oa"}],
                   "filters": [_OK]}},
        # dims_only/* — no aggregate; the host base fans today, carries the EXISTS after.
        "dims_only/fanning": {
            "source": "customers", "mode": None,
            "kw": {"dimensions": ["tier"], "filters": [_OK]}},
        # positive/* — a provably to-one filter; plain row restriction, byte-identical
        # SQL through the change.
        "positive/to_one_filter": {
            "source": "orders", "mode": None,
            "kw": {"measures": [{"formula": "amount:sum", "name": "amt"}],
                   "filters": ["customers.tier = 'gold'"]}},
    }


FAIL_CLOSED = {k for k in _cases() if k.startswith("fanning_inline/")}


async def _generate_one(case, dialect: str):
    models = dev1900_models()
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


ALLOWED_DELTAS: dict[str, str] = {}  # PENDING re-bless list; empty in a committed state.

bind_golden_tests(
    namespace=globals(),
    golden_path=GOLDEN_PATH,
    cases=_cases,
    dialects=DIALECTS,
    allowed=ALLOWED_DELTAS,
    generate_one=_generate_one,
)


def test_fanning_inline_cases_fail_closed(baseline) -> None:
    """Feature-missing tripwire: every fanning-inline shape currently records the
    guard's raise, not SQL. Flips at implementation (each becomes an EXISTS base)."""
    for key, value in baseline.items():
        if key.split("::", 1)[0] in FAIL_CLOSED:
            assert isinstance(value, dict), f"{key} unexpectedly emits SQL: {value}"


def test_to_one_case_generates_sql(baseline) -> None:
    """The to-one invariant must stay real SQL through the change."""
    for key, value in baseline.items():
        if key.split("::", 1)[0] == "positive/to_one_filter":
            assert not isinstance(value, dict), f"{key} unexpectedly raises: {value}"
