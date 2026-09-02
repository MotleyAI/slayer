"""DEV-1846 — golden SQL baseline for the composite-input transform shapes.
Same harness / four-step blessing loop as ``tests/test_dev1750_golden_sql.py``
(mechanics in ``tests/_golden_harness.py``); only the matrix differs. Pins WHAT
the lift emits across dialects execution cannot reach (Postgres, T-SQL,
BigQuery).
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import SlayerQuery

from tests._dev1846_fixtures import dev1846_models
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise


GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1846_sql_baseline.json"

#: Postgres/SQLite/DuckDB for the executable regimes; T-SQL and BigQuery because
#: both mangle dotted aliases at emission and T-SQL rejects a nested WITH.
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]

# ``<case_id>::<dialect>`` -> why this entry is allowed to change right now.
# A PENDING list, not a log: a committed state always has this empty.
ALLOWED_DELTAS: dict[str, str] = {}

_MONTH = [{"dimension": "ordered_at", "granularity": "month"}]


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "sales")
    kw.setdefault("time_dimensions", _MONTH)
    return SlayerQuery(**kw)


def _cases() -> dict:
    """The matrix. Keys are stable ids — renaming one is a golden change."""
    return {
        # time_shift over composites — the lift's core.
        "ts/ratio_by_store": _q(
            dimensions=["store"],
            measures=[{"formula": "time_shift(revenue:sum / qty:sum, -1)",
                       "name": "prev"}],
        ),
        "ts/change_pct_ratio": _q(
            dimensions=["store"],
            measures=[{"formula": "change_pct(revenue:sum / *:count)", "name": "pct"}],
        ),
        "ts/coalesce_missing": _q(measures=[
            {"formula": "time_shift(coalesce(revenue:sum, 0), -1)", "name": "prev"},
        ]),
        "ts/two_leaves": _q(measures=[
            {"formula": "time_shift(revenue:wrevenue_sum + hi_rev:sum, -1)",
             "name": "prev"},
        ]),
        "ts/crossing_join": _q(measures=[
            {"formula": "time_shift(revenue:wrevenue_sum * 2, -1)", "name": "prev"},
        ]),
        # consecutive_periods over the newly lifted value/predicate shapes.
        "cp/numeric_delta": _q(measures=[
            {"formula": "consecutive_periods(revenue:sum - cost:sum)", "name": "streak"},
        ]),
        "cp/bare_cumsum": _q(measures=[
            {"formula": "consecutive_periods(cumsum(revenue:sum))", "name": "streak"},
        ]),
        "cp/or": _q(measures=[
            {"formula": "consecutive_periods(revenue:sum > 90 or cost:sum > 40)",
             "name": "streak"},
        ]),
        "cp/not": _q(measures=[
            {"formula": "consecutive_periods(not (revenue:sum > 90))", "name": "streak"},
        ]),
        "cp/in": _q(
            dimensions=["store"],
            measures=[{"formula": "consecutive_periods(store in ('A', 'B'))",
                       "name": "streak"}],
        ),
        "cp/negated_in": _q(
            dimensions=["store"],
            measures=[{"formula": "consecutive_periods(store not in ('B', 'C'))",
                       "name": "streak"}],
        ),
        "cp/and_simple": _q(measures=[
            {"formula": "consecutive_periods(revenue:sum > 40 and cost:sum > 10)",
             "name": "streak"},
        ]),
        "cp/nested_in_and": _q(
            dimensions=["store"],
            measures=[{
                "formula": "consecutive_periods(store in ('A', 'C') and revenue:sum > 0)",
                "name": "streak"}],
        ),
        "cp/iif": _q(measures=[
            {"formula": "consecutive_periods(iif(revenue:sum > 0, 1, 0))",
             "name": "streak"},
        ]),
        # comparison-predicate cp already renders — a regression anchor.
        "cp/change_gt_anchor": _q(measures=[
            {"formula": "consecutive_periods(change(revenue:sum) > 0)", "name": "streak"},
        ]),
        # Still fail-closed — records the raise (must stay a ValueError post-lift).
        "reject/ts_nested_transform": _q(measures=[
            {"formula": "time_shift(cumsum(revenue:sum), -1)", "name": "prev"},
        ]),
        "reject/ts_cross_model_leaf": _q(measures=[
            {"formula": "time_shift(revenue:sum + regions.factor:sum, -1)",
             "name": "prev"},
        ]),
        "reject/cp_boolean_numeric": _q(measures=[
            {"formula": "consecutive_periods((revenue:sum > 0) + (cost:sum > 0))",
             "name": "x"},
        ]),
        "reject/cp_string_family": _q(measures=[
            {"formula": "consecutive_periods(lower(sku:max))", "name": "x"},
        ]),
    }


async def _generate_one(query: SlayerQuery, dialect: str):
    """Emitted SQL, or a structured record of the raised error."""
    models = dev1846_models()
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
