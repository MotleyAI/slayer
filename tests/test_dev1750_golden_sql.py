"""Golden SQL baseline for the time_shift/cp × cross-model shapes.

Same harness and four-step blessing loop as ``tests/test_dev1748_golden_sql.py``
(read ``tests/_golden_harness.py`` for the mechanics); only the matrix differs.

This pins exactly WHAT the guard-lift emits, as a diff a reviewer reads before
approving — and reaches dialects execution cannot (Postgres, T-SQL, BigQuery).
The baseline was first recorded against PRE-lift code, where every lifted case
is a recorded ``stage 7b.15e`` raise; the lift moves those entries to SQL (and
re-narrows case (c)'s message), each re-blessed through ``ALLOWED_DELTAS`` with
the manifest emptied again.
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import SlayerQuery

from tests._dev1750_fixtures import dev1750_datasets
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise


GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1750_sql_baseline.json"

#: Postgres/SQLite/DuckDB for the executable regimes; T-SQL and BigQuery because
#: both mangle dotted aliases at emission and T-SQL rejects a nested WITH.
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]

# ``<case_id>::<dialect>`` -> why this entry is allowed to change right now.
# A PENDING list, not a log: a committed state always has this empty.
ALLOWED_DELTAS: dict[str, str] = {}

_MONTH = [{"field": "ordered_at", "granularity": "month"}]


def _q(**kw) -> SlayerQuery:
    kw.setdefault("population", "orders")
    kw.setdefault("time_dimensions", _MONTH)
    return SlayerQuery(**kw)


def _cases() -> dict:
    """The matrix. Keys are stable ids — renaming one is a golden change."""
    return {
        # (a) local time_shift beside a cross-model sibling.
        "a/local_ts_cm_sibling": _q(to_many_handling="broadcast", metrics=[
            {"metric": "sum(customers.spend)", "alias": "cm"},
            {"metric": "time_shift(sum(amount), -1)", "alias": "prev"},
        ]),
        # (b) host-rooted crossing-fragment inner — the named repro.
        "b/host_rooted_wscaled": _q(metrics=[
            {"metric": "time_shift(wscaled_sum(amount), -1)", "alias": "prev"},
        ]),
        "b/host_rooted_wscaled_user_kwarg": _q(metrics=[
            {"metric": "time_shift(wscaled_sum(amount, w='customers.regions.weight'), -1)",
             "alias": "prev"},
        ]),
        "b/wscaled_with_local_sibling": _q(metrics=[
            {"metric": "sum(amount)", "alias": "s"},
            {"metric": "time_shift(wscaled_sum(amount), -1)", "alias": "prev"},
        ]),
        # (b) crossing a 1:N join — the shifted CTE must pull the fan-out join.
        "b/liscaled_one_to_many": _q(metrics=[
            {"metric": "sum(amount)", "alias": "s"},
            {"metric": "time_shift(liscaled_sum(amount), -1)", "alias": "prev"},
        ]),
        # consecutive_periods — lifted entirely (no target-grain failure mode).
        "cp/local_with_cm_sibling": _q(to_many_handling="broadcast", metrics=[
            {"metric": "sum(customers.spend)", "alias": "cm"},
            {"metric": "consecutive_periods(sum(amount) > 0)", "alias": "streak"},
        ]),
        "cp/over_target_grain": _q(to_many_handling="broadcast", metrics=[
            {"metric": "consecutive_periods(sum(customers.spend) > 0)", "alias": "streak"},
        ]),
        # change / change_pct desugar to time_shift.
        "change/of_wscaled": _q(metrics=[
            {"metric": "change(wscaled_sum(amount))", "alias": "delta"},
        ]),
        "change_pct/of_local_with_cm_sibling": _q(to_many_handling="broadcast", metrics=[
            {"metric": "sum(customers.spend)", "alias": "cm"},
            {"metric": "change_pct(sum(amount))", "alias": "delta"},
        ]),
        # (c) target-grain inner — stays guarded (records the narrowed raise).
        "c/target_grain_guarded": _q(to_many_handling="broadcast", metrics=[
            {"metric": "time_shift(sum(customers.spend), -1)", "alias": "prev"},
        ]),
        # window op over cross-model — never guarded; a regression anchor.
        "window/cumsum_cross_model": _q(to_many_handling="broadcast", metrics=[
            {"metric": "cumsum(sum(customers.spend))", "alias": "run"},
        ]),
        # composite-input transform — lifted; it now renders a
        # shifted-CTE re-aggregation beside the cross-model sibling.
        "composite/lifted": _q(to_many_handling="broadcast", metrics=[
            {"metric": "sum(customers.spend)", "alias": "cm"},
            {"metric": "time_shift(sum(amount) + sum(amount), -1)", "alias": "prev"},
        ]),
        # first/last crossing time arg — records whatever the seam does.
        "first_last/crossing_time_arg": _q(to_many_handling="broadcast", metrics=[
            {"metric": "sum(customers.spend)", "alias": "cm"},
            {"metric": "time_shift(last(amount, customers.signup_at), -1)", "alias": "prev"},
        ]),
    }


async def _generate_one(query: SlayerQuery, dialect: str):
    """Emitted SQL, or a structured record of the raised error."""
    models = dev1750_datasets()
    try:
        return await _engine_generate(
            query=query, dataset=models[0], extra_datasets=models[1:],
            dialect=dialect, validate=False,
        )
    except Exception as exc:  # noqa: BLE001 — the exception itself is contract
        return record_raise(exc)


# The blessing-loop wiring (baseline fixture + the five shared guards) is bound
# once in the harness; this module keeps only its matrix, path, and dialects.
bind_golden_tests(
    namespace=globals(),
    golden_path=GOLDEN_PATH,
    cases=_cases,
    dialects=DIALECTS,
    allowed=ALLOWED_DELTAS,
    generate_one=_generate_one,
)
