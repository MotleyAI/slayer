"""DEV-1909 golden SQL — the population-restriction shapes across the seven Tier-1
dialects. Each ``fanning_inline/`` shape restricts the population by a correlated
EXISTS base query; each ``producer/`` and ``dims_only/`` shape carries the semi-join
and no fanning join; the ``positive/`` to-one case stays a plain row restriction;
and ``producer/windowed_fanning_axis`` associates (its window axis fans from the
population root, so each customer counts once per frame).
"""

from __future__ import annotations

from pathlib import Path

from tests._dev1900_fixtures import dev1900_datasets
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
            "kw": {"metrics": [{"metric": "sum(customers.spend)", "alias": "sp"}],
                   "filter": [_OK]}},
        "fanning_inline/derived": {
            "source": "orders", "mode": None,
            "kw": {"metrics": [{"metric": "sum(orders.amount)", "alias": "amt"}],
                   "filter": ["customers.regions.bad_pop > 0"]}},
        "fanning_inline/count": {
            "source": "customers", "mode": None,
            "kw": {"dimensions": ["customers.tier"],
                   "metrics": [{"metric": "count(customers.*)", "alias": "n"}],
                   "filter": [_OK]}},
        "fanning_inline/two_branch": {
            "source": "customers", "mode": None,
            "kw": {"metrics": [{"metric": "sum(customers.spend)", "alias": "sp"}],
                   "filter": [_OK, "regions.region_events.value >= 50"]}},
        # producer/* — host-rooted regroup producers that fan silently today; after
        # the change each carries the semi-join and drops the fanning join.
        "producer/partitioned": {
            "source": "customers", "mode": None,
            "kw": {"dimensions": ["customers.tier"],
                   "metrics": [{"metric": "sum(customers.spend, partition_by=customers.tier)", "alias": "pt"}],
                   "filter": [_OK]}},
        "producer/windowed": {
            "source": "customers", "mode": None,
            "kw": {"time_dimensions": [
                {"field": "customers.signup_at", "granularity": "month"}],
                   "metrics": [{"metric": "sum(customers.spend, window='1y')", "alias": "w"}],
                   "filter": [_OK]}},
        "producer/producer_only": {
            "source": "customers", "mode": None,
            "kw": {"metrics": [{"metric": "sum(orders.amount)", "alias": "oa"}],
                   "filter": [_OK]}},
        # dims_only/* — no aggregate; the host base fans today, carries the EXISTS after.
        "dims_only/fanning": {
            "source": "customers", "mode": None,
            "kw": {"dimensions": ["customers.tier"], "filter": [_OK]}},
        # windowed over a fanning time axis (orders.ordered_at from customers):
        # it associates, each customer once per frame.
        "producer/windowed_fanning_axis": {
            "source": "customers", "mode": None,
            "kw": {"time_dimensions": [
                {"field": "orders.ordered_at", "granularity": "month"}],
                   "metrics": [{"metric": "sum(customers.spend, window='1y')", "alias": "w"}],
                   "filter": [_OK]}},
        # positive/* — a provably to-one filter; plain row restriction, byte-identical
        # SQL through the change.
        "positive/to_one_filter": {
            "source": "orders", "mode": None,
            "kw": {"metrics": [{"metric": "sum(orders.amount)", "alias": "amt"}],
                   "filter": ["customers.tier = 'gold'"]}},
    }


#: Shapes that fail closed; none since a window over a fanning time axis associates.
FAIL_CLOSED: set[str] = set()
#: A window over a fanning time axis: real SQL, never a recorded raise.
FANNING_WINDOW = "producer/windowed_fanning_axis"


async def _generate_one(case, dialect: str):
    models = dev1900_datasets()
    try:
        kw = dict(case["kw"])
        if case["mode"] is not None:
            kw["to_many_handling"] = case["mode"]
        query = SlayerQuery(population=case["source"], **kw)
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


def test_fail_closed_cases_record_a_raise(baseline) -> None:
    """Each fail-closed shape's baseline is a recorded raise, never SQL."""
    for key, value in baseline.items():
        if key.split("::", 1)[0] in FAIL_CLOSED:
            assert isinstance(value, dict), f"{key} unexpectedly emits SQL: {value}"


def test_fanning_window_emits_sql(baseline) -> None:
    """A window over a fanning time axis associates: its baseline is SQL, never a recorded raise."""
    keys = [k for k in baseline if k.split("::", 1)[0] == FANNING_WINDOW]
    assert keys
    for key in keys:
        assert not isinstance(baseline[key], dict), f"{key} unexpectedly raises: {baseline[key]}"


def test_to_one_case_generates_sql(baseline) -> None:
    """The to-one invariant must stay real SQL through the change."""
    for key, value in baseline.items():
        if key.split("::", 1)[0] == "positive/to_one_filter":
            assert not isinstance(value, dict), f"{key} unexpectedly raises: {value}"
