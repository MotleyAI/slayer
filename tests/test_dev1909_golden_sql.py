"""DEV-1909 golden SQL — the population-restriction shapes across the seven Tier-1
dialects. Each ``fanning_inline/`` shape restricts the population by a correlated
EXISTS base query; each ``producer/`` and ``dims_only/`` shape carries the semi-join
and no fanning join; the ``positive/`` to-one case stays a plain row restriction;
and ``producer/windowed_fanning_axis`` fails closed (decision 12 — its window axis
is not attributable from the population root).
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
                {"dimension": "signup_at", "granularity": "month"}],
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
        # windowed over a fanning time axis (orders.ordered_at from customers):
        # its axis is not attributable from the root, so it fails closed (decision 12).
        "producer/windowed_fanning_axis": {
            "source": "customers", "mode": None,
            "kw": {"time_dimensions": [
                {"dimension": "orders.ordered_at", "granularity": "month"}],
                   "measures": [{"formula": "spend:sum(window='1y')", "name": "w"}],
                   "filters": [_OK]}},
        # positive/* — a provably to-one filter; plain row restriction, byte-identical
        # SQL through the change.
        "positive/to_one_filter": {
            "source": "orders", "mode": None,
            "kw": {"measures": [{"formula": "amount:sum", "name": "amt"}],
                   "filters": ["customers.tier = 'gold'"]}},
    }


#: The only shape that still fails closed post-implementation: a window whose time
#: axis is not attributable from the population root (decision 12). Every
#: fanning_inline/producer/dims_only shape now restricts by EXISTS instead.
FAIL_CLOSED = {"producer/windowed_fanning_axis"}


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


def test_fail_closed_cases_record_a_raise(baseline) -> None:
    """A window over a fanning time axis fails closed (decision 12): its baseline
    is a recorded raise, never SQL — the fan-multiply must not slip through as SQL."""
    for key, value in baseline.items():
        if key.split("::", 1)[0] in FAIL_CLOSED:
            assert isinstance(value, dict), f"{key} unexpectedly emits SQL: {value}"


def test_to_one_case_generates_sql(baseline) -> None:
    """The to-one invariant must stay real SQL through the change."""
    for key, value in baseline.items():
        if key.split("::", 1)[0] == "positive/to_one_filter":
            assert not isinstance(value, dict), f"{key} unexpectedly raises: {value}"
