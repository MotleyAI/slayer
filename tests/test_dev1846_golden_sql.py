"""DEV-1846 — golden SQL baseline for the composite-input transform shapes.
Same harness / four-step blessing loop as ``tests/test_dev1750_golden_sql.py``
(mechanics in ``tests/_golden_harness.py``); only the matrix differs. Pins WHAT
the lift emits across dialects execution cannot reach (Postgres, T-SQL,
BigQuery).
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import SlayerQuery

from tests._dev1846_fixtures import dev1846_datasets
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise


GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1846_sql_baseline.json"

#: Postgres/SQLite/DuckDB for the executable regimes; T-SQL and BigQuery because
#: both mangle dotted aliases at emission and T-SQL rejects a nested WITH.
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]

# ``<case_id>::<dialect>`` -> why this entry is allowed to change right now.
# A PENDING list, not a log: a committed state always has this empty.
ALLOWED_DELTAS: dict[str, str] = {}

_MONTH = [{"field": "sales.ordered_at", "granularity": "month"}]


def _q(**kw) -> SlayerQuery:
    kw.setdefault("population", "sales")
    kw.setdefault("time_dimensions", _MONTH)
    return SlayerQuery(**kw)


def _cases() -> dict:
    """The matrix. Keys are stable ids — renaming one is a golden change."""
    return {
        # time_shift over composites — the lift's core.
        "ts/ratio_by_store": _q(
            dimensions=["sales.store"],
            metrics=[{"metric": "time_shift(sum(sales.revenue) / sum(sales.qty), -1)",
                       "alias": "prev"}],
        ),
        "ts/change_pct_ratio": _q(
            dimensions=["sales.store"],
            metrics=[{"metric": "change_pct(sum(sales.revenue) / count(sales.*))", "alias": "pct"}],
        ),
        "ts/coalesce_missing": _q(metrics=[
            {"metric": "time_shift(coalesce(sum(sales.revenue), 0), -1)", "alias": "prev"},
        ]),
        "ts/two_leaves": _q(metrics=[
            {"metric": "time_shift(wrevenue_sum(sales.revenue) + sum(sales.hi_rev), -1)",
             "alias": "prev"},
        ]),
        "ts/crossing_join": _q(metrics=[
            {"metric": "time_shift(wrevenue_sum(sales.revenue) * 2, -1)", "alias": "prev"},
        ]),
        # consecutive_periods over the newly lifted value/predicate shapes.
        "cp/numeric_delta": _q(metrics=[
            {"metric": "consecutive_periods(sum(sales.revenue) - sum(sales.cost))", "alias": "streak"},
        ]),
        "cp/bare_cumsum": _q(metrics=[
            {"metric": "consecutive_periods(cumsum(sum(sales.revenue)))", "alias": "streak"},
        ]),
        "cp/or": _q(metrics=[
            {"metric": "consecutive_periods(sum(sales.revenue) > 90 or sum(sales.cost) > 40)",
             "alias": "streak"},
        ]),
        "cp/not": _q(metrics=[
            {"metric": "consecutive_periods(not (sum(sales.revenue) > 90))", "alias": "streak"},
        ]),
        "cp/in": _q(
            dimensions=["sales.store"],
            metrics=[{"metric": "consecutive_periods(sales.store in ('A', 'B'))",
                       "alias": "streak"}],
        ),
        "cp/negated_in": _q(
            dimensions=["sales.store"],
            metrics=[{"metric": "consecutive_periods(sales.store not in ('B', 'C'))",
                       "alias": "streak"}],
        ),
        "cp/and_simple": _q(metrics=[
            {"metric": "consecutive_periods(sum(sales.revenue) > 40 and sum(sales.cost) > 10)",
             "alias": "streak"},
        ]),
        "cp/nested_in_and": _q(
            dimensions=["sales.store"],
            metrics=[{
                "metric": "consecutive_periods(sales.store in ('A', 'C') and sum(sales.revenue) > 0)",
                "alias": "streak"}],
        ),
        "cp/iif": _q(metrics=[
            {"metric": "consecutive_periods(iif(sum(sales.revenue) > 0, 1, 0))",
             "alias": "streak"},
        ]),
        # null-test predicates: bare top level and under a connective.
        "cp/is_null_top": _q(metrics=[
            {"metric": "consecutive_periods(sum(sales.hi_rev) is None)", "alias": "streak"},
        ]),
        "cp/is_not_null_and": _q(metrics=[
            {"metric": "consecutive_periods(sum(sales.hi_rev) is not None and sum(sales.cost) > 0)",
             "alias": "streak"},
        ]),
        # comparison-predicate cp already renders — a regression anchor.
        "cp/change_gt_anchor": _q(metrics=[
            {"metric": "consecutive_periods(change(sum(sales.revenue)) > 0)", "alias": "streak"},
        ]),
        "cp/boolean_numeric": _q(metrics=[
            {"metric": "consecutive_periods((sum(sales.revenue) > 0) + (sum(sales.cost) > 0))",
             "alias": "x"},
        ]),
        # Still fail-closed — records the raise. The DEV-1868 series-regime
        # lifts moved the ts_* shapes to tests/test_dev1868_golden_sql.py.
        "reject/cp_string_family": _q(metrics=[
            {"metric": "consecutive_periods(lower(max(sales.sku)))", "alias": "x"},
        ]),
    }


async def _generate_one(query: SlayerQuery, dialect: str):
    """Emitted SQL, or a structured record of the raised error."""
    models = dev1846_datasets()
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
