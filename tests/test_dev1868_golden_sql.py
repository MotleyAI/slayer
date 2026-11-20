"""Golden SQL baseline for the closure-sweep shapes across the
standard five-dialect matrix (harness: tests/_golden_harness.py). The class-a
tripwire for pre-existing legal ``time_shift`` forms is the untouched
tests/test_dev1846_golden_sql.py baseline."""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import SlayerQuery

from tests._dev1836_fixtures import dev1836_datasets
from tests._dev1846_fixtures import dev1846_datasets
from tests._dev1868_fixtures import CM_PART_TIER, FIRST_REGION, LAST_REGION, LAST_TIER
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1868_sql_baseline.json"

DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]

# ``<case_id>::<dialect>`` -> why this entry is allowed to change right now.
# A PENDING list, not a log: a committed state always has this empty.
ALLOWED_DELTAS: dict[str, str] = {}

def _month(pop: str) -> list:
    return [{"field": f"{pop}.ordered_at", "granularity": "month"}]


def _orders(**kw) -> SlayerQuery:
    kw.setdefault("population", "orders")
    return SlayerQuery(**kw)


def _sales(**kw) -> SlayerQuery:
    kw.setdefault("population", "sales")
    kw.setdefault("time_dimensions", _month("sales"))
    return SlayerQuery(**kw)


def _cases() -> dict:
    """The matrix. Keys are stable ids — renaming one is a golden change."""
    return {
        # W1 — cross-dataset ranked × partition_by, every position.
        "w1/last_tier_measure": _orders(
            dimensions=["customers.tier"],
            metrics=[{"metric": LAST_TIER, "alias": "l"}],
        ),
        "w1/first_region_measure": _orders(
            dimensions=["customers.regions.name"],
            metrics=[{"metric": FIRST_REGION, "alias": "f"}],
        ),
        "w1/filter": _orders(
            dimensions=["customers.tier"],
            filter=[f"{LAST_TIER} > 100"],
            metrics=[{"metric": "sum(orders.amount)", "alias": "s"}],
        ),
        "w1/order": _orders(
            dimensions=["customers.tier"],
            filter=["customers.tier is not null"],
            metrics=[{"metric": "sum(orders.amount)", "alias": "s"}],
            order=[{"field": LAST_TIER, "direction": "desc"}],
        ),
        # W2 — cross-dataset partitioned inners under transforms.
        "w2/cumsum_part_tier": _orders(
            dimensions=["customers.tier"], time_dimensions=_month("orders"),
            metrics=[{"metric": f"cumsum({CM_PART_TIER})", "alias": "c"}],
        ),
        "w2/change_last_region": _orders(
            dimensions=["customers.regions.name"], time_dimensions=_month("orders"),
            metrics=[{"metric": f"change({LAST_REGION})", "alias": "ch"}],
        ),
        "w2/shared_inner": _orders(
            dimensions=["customers.tier"], time_dimensions=_month("orders"),
            filter=[f"{CM_PART_TIER} > 100"],
            metrics=[{"metric": f"cumsum({CM_PART_TIER})", "alias": "c"}],
        ),
        # W3 — composite routes (already combined; pinned against regression).
        "w3/scalar_call": _orders(
            dimensions=["customers.tier"],
            metrics=[{"metric": "round(sum(customers.spend) / sum(orders.amount), 2)",
                       "alias": "r"}],
        ),
        "w3/multi_remote": _orders(
            dimensions=["customers.tier"],
            metrics=[{"metric": "sum(customers.spend) - max(customers.spend)",
                       "alias": "r"}],
        ),
        "w3/two_roots": _orders(
            dimensions=["customers.regions.name"],
            metrics=[{"metric": "sum(customers.spend) / sum(customers.regions.pop)",
                       "alias": "r"}],
        ),
        # Series-regime time_shift emission.
        "ts/series_nested_cumsum": _sales(metrics=[
            {"metric": "time_shift(cumsum(sum(sales.revenue)), -1)", "alias": "t"},
        ]),
        "ts/series_nested_by_store": _sales(
            dimensions=["sales.store"],
            metrics=[{"metric": "time_shift(cumsum(sum(sales.revenue)), -1)",
                       "alias": "t"}],
        ),
        "ts/series_cm_leaf": _sales(to_many_handling="broadcast", metrics=[
            {"metric": "time_shift(sum(sales.revenue) + sum(regions.factor), -1)",
             "alias": "t"},
        ]),
        "ts/series_in_pred": _sales(metrics=[
            {"metric": "time_shift(sum(sales.revenue) in (100, 999), -1)", "alias": "t"},
        ]),
        "ts/series_filters_once": _sales(
            filter=["sales.status = 'a'"],
            metrics=[{"metric": "time_shift(cumsum(sum(sales.revenue)), -1)",
                       "alias": "t"}],
        ),
        # Residue split — the two permanent typed errors, recorded as raises.
        "res/transform_no_aggregate": _orders(
            dimensions=["customers.tier",
                        {"field": "CASE WHEN cumsum(orders.amount) > 50 "
                                       "THEN 1 ELSE 0 END", "alias": "b"}],
            time_dimensions=_month("orders"),
            metrics=[{"metric": "sum(orders.amount)", "alias": "s"}],
        ),
        "res/transform_ungrained": _orders(
            dimensions=["customers.tier",
                        {"field": "CASE WHEN cumsum(sum(orders.amount)) > 50 "
                                       "THEN 1 ELSE 0 END", "alias": "b"}],
            time_dimensions=_month("orders"),
            metrics=[{"metric": "sum(orders.amount)", "alias": "s"}],
        ),
    }


async def _generate_one(query: SlayerQuery, dialect: str):
    """Emitted SQL, or a structured record of the raised error."""
    models = dev1846_datasets() if query.population == "sales" else dev1836_datasets()
    try:
        return await _engine_generate(
            query=query, dataset=models[0], extra_datasets=models[1:],
            dialect=dialect, validate=False,
        )
    except Exception as exc:  # noqa: BLE001 — the exception itself is contract
        return record_raise(exc)


bind_golden_tests(
    namespace=globals(),
    golden_path=GOLDEN_PATH,
    cases=_cases,
    dialects=DIALECTS,
    allowed=ALLOWED_DELTAS,
    generate_one=_generate_one,
)
