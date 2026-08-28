"""DEV-1750 — golden SQL baseline for the time_shift/cp × cross-model shapes.

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

from tests._dev1750_fixtures import dev1750_models
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise


GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1750_sql_baseline.json"

#: Postgres/SQLite/DuckDB for the executable regimes; T-SQL and BigQuery because
#: both mangle dotted aliases at emission and T-SQL rejects a nested WITH.
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]

# ``<case_id>::<dialect>`` -> why this entry is allowed to change right now.
# A PENDING list, not a log: a committed state always has this empty.
ALLOWED_DELTAS: dict[str, str] = {}

_MONTH = [{"dimension": "ordered_at", "granularity": "month"}]


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    kw.setdefault("time_dimensions", _MONTH)
    return SlayerQuery(**kw)


def _cases() -> dict:
    """The matrix. Keys are stable ids — renaming one is a golden change."""
    return {
        # (a) local time_shift beside a cross-model sibling.
        "a/local_ts_cm_sibling": _q(measures=[
            {"formula": "customers.spend:sum", "name": "cm"},
            {"formula": "time_shift(amount:sum, -1)", "name": "prev"},
        ]),
        # (b) host-rooted crossing-fragment inner — the named repro.
        "b/host_rooted_wscaled": _q(measures=[
            {"formula": "time_shift(amount:wscaled_sum, -1)", "name": "prev"},
        ]),
        "b/host_rooted_wscaled_user_kwarg": _q(measures=[
            {"formula": "time_shift(amount:wscaled_sum(w='customers.regions.weight'), -1)",
             "name": "prev"},
        ]),
        "b/wscaled_with_local_sibling": _q(measures=[
            {"formula": "amount:sum", "name": "s"},
            {"formula": "time_shift(amount:wscaled_sum, -1)", "name": "prev"},
        ]),
        # (b) crossing a 1:N join — the shifted CTE must pull the fan-out join.
        "b/liscaled_one_to_many": _q(measures=[
            {"formula": "amount:sum", "name": "s"},
            {"formula": "time_shift(amount:liscaled_sum, -1)", "name": "prev"},
        ]),
        # consecutive_periods — lifted entirely (no target-grain failure mode).
        "cp/local_with_cm_sibling": _q(measures=[
            {"formula": "customers.spend:sum", "name": "cm"},
            {"formula": "consecutive_periods(amount:sum > 0)", "name": "streak"},
        ]),
        "cp/over_target_grain": _q(measures=[
            {"formula": "consecutive_periods(customers.spend:sum > 0)", "name": "streak"},
        ]),
        # change / change_pct desugar to time_shift.
        "change/of_wscaled": _q(measures=[
            {"formula": "change(amount:wscaled_sum)", "name": "delta"},
        ]),
        "change_pct/of_local_with_cm_sibling": _q(measures=[
            {"formula": "customers.spend:sum", "name": "cm"},
            {"formula": "change_pct(amount:sum)", "name": "delta"},
        ]),
        # (c) target-grain inner — stays guarded (records the narrowed raise).
        "c/target_grain_guarded": _q(measures=[
            {"formula": "time_shift(customers.spend:sum, -1)", "name": "prev"},
        ]),
        # window op over cross-model — never guarded; a regression anchor.
        "window/cumsum_cross_model": _q(measures=[
            {"formula": "cumsum(customers.spend:sum)", "name": "run"},
        ]),
        # composite-input transform — 7b.11 must survive the lift (records raise).
        "composite/still_7b11": _q(measures=[
            {"formula": "customers.spend:sum", "name": "cm"},
            {"formula": "time_shift(amount:sum + amount:sum, -1)", "name": "prev"},
        ]),
        # first/last crossing time arg — records whatever the seam does.
        "first_last/crossing_time_arg": _q(measures=[
            {"formula": "customers.spend:sum", "name": "cm"},
            {"formula": "time_shift(amount:last(customers.signup_at), -1)", "name": "prev"},
        ]),
    }


async def _generate_one(query: SlayerQuery, dialect: str):
    """Emitted SQL, or a structured record of the raised error."""
    models = dev1750_models()
    try:
        return await _engine_generate(
            query=query, model=models[0], extra_models=models[1:],
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
