"""DEV-1832 golden SQL (task 1.7) — the lifted expression-aggregation shapes
across seven Tier-1 dialects.

Every ``lifted/`` shape now emits SQL (the v1 boundaries have fallen);
``test_lifted_cases_generate_sql`` guards against a regression back to a raise
record. The ``positive/`` shapes were already supported and stay byte-identical
SQL through the change.
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.enums import TimeGranularity
from slayer.core.query import SlayerQuery, TimeDimension

from tests._dev1832_fixtures import dev1832_datasets
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1832_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]
ALLOWED_DELTAS: dict[str, str] = {}

_MONTH_TD = [TimeDimension(field="monthly.ordered_at",
                           granularity=TimeGranularity.MONTH)]


def _cases() -> dict:
    return {
        # lifted/* — a v1 boundary today (records a raise); SQL after the change.
        "lifted/host_homed_arith": {
            "source": "orders", "mode": None,
            "kw": {"dimensions": ["orders.status"],
                   "metrics": [{"metric": "sum(orders.amount - customers.discount)",
                                 "alias": "m"}]}},
        "lifted/target_homed": {
            "source": "orders", "mode": None,
            "kw": {"dimensions": ["customers.tier"], "metrics": [
                {"metric": "sum(customers.spend - customers.regions.pop)",
                 "alias": "m"}]}},
        "lifted/two_branch": {
            "source": "orders", "mode": None,
            "kw": {"dimensions": ["orders.status"], "metrics": [
                {"metric": "sum(customers.spend - stores.rent)", "alias": "m"}]}},
        "lifted/cross_model_partition": {
            "source": "orders", "mode": None,
            "kw": {"dimensions": ["orders.status"], "metrics": [
                {"metric": "sum(orders.amount - customers.discount, partition_by=orders.status)",
                 "alias": "m"}]}},
        "lifted/filtered_operand": {
            "source": "sales", "mode": None,
            "kw": {"dimensions": ["sales.region"],
                   "metrics": [{"metric": "sum(sales.q_amount - 1)", "alias": "m"}]}},
        "lifted/filtered_leaf_joined": {
            "source": "orders", "mode": None,
            "kw": {"dimensions": ["orders.status"], "metrics": [
                {"metric": "sum(orders.amount - customers.north_spend)", "alias": "m"}]}},
        "lifted/transform_constituent": {
            "source": "monthly", "mode": None,
            "kw": {"time_dimensions": _MONTH_TD, "metrics": [{"metric":
                "sum(cumsum(sum(monthly.amount, partition_by=[monthly.region, monthly.ordered_at])) - 1)",
                "alias": "m"}]}},
        "lifted/mixed_transform": {
            "source": "sales", "mode": None,
            "kw": {"dimensions": ["sales.region"], "metrics": [{"metric":
                "sum(sales.quantity * rank(avg(sales.unit_price, partition_by=sales.product), direction='desc'))",
                "alias": "m"}]}},
        # DEV-1928 — re-aggregation constituents in a mixed source; windowed inner.
        "lifted/mixed_reagg": {
            "source": "monthly", "mode": None,
            "kw": {"time_dimensions": _MONTH_TD, "metrics": [{"metric":
                "sum(monthly.amount * min(sum(monthly.amount, partition_by=[monthly.region, monthly.ordered_at]), "
                "partition_by=monthly.region))", "alias": "m"}]}},
        "lifted/mixed_collapse": {
            "source": "monthly", "mode": None,
            "kw": {"time_dimensions": _MONTH_TD, "metrics": [{"metric":
                "sum(monthly.amount * last(sum(monthly.amount, partition_by=[monthly.region, monthly.ordered_at])))",
                "alias": "m"}]}},
        "lifted/windowed_inner": {
            "source": "monthly", "mode": None,
            "kw": {"time_dimensions": _MONTH_TD, "metrics": [{"metric":
                "sum(rank(sum(monthly.amount, window='90d', partition_by=monthly.region), direction='desc'))",
                "alias": "m"}]}},
        # positive/* — already supported; byte-identical SQL through the change.
        "positive/same_model_arith": {
            "source": "orders", "mode": None,
            "kw": {"dimensions": ["orders.status"],
                   "metrics": [{"metric": "sum(orders.amount - orders.cost)", "alias": "m"}]}},
        "positive/reaggregation": {
            "source": "sales", "mode": None,
            "kw": {"dimensions": ["sales.region"], "metrics": [{"metric":
                "avg(sum(sales.amount, partition_by=[sales.city, sales.region]))", "alias": "m"}]}},
        "positive/mixed_row_attached": {
            "source": "sales", "mode": None,
            "kw": {"dimensions": ["sales.region"], "metrics": [{"metric":
                "sum(sales.quantity * avg(sales.unit_price, partition_by=sales.product))", "alias": "m"}]}},
    }


LIFTED = {k for k in _cases() if k.startswith("lifted/")}


async def _generate_one(case, dialect: str):
    models = dev1832_datasets()
    try:
        kw = dict(case["kw"])
        if case["mode"] is not None:
            kw["to_many_handling"] = case["mode"]
        query = SlayerQuery(population=case["source"], **kw)
        root = next(m for m in models if m.name == case["source"])
        return await _engine_generate(
            query=query, dataset=root,
            extra_datasets=[m for m in models if m.name != case["source"]],
            dialect=dialect, validate=False)
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


def test_lifted_cases_generate_sql(baseline) -> None:
    """Regression guard: every lifted shape must emit SQL, never a raise record."""
    for key, value in baseline.items():
        if key.split("::", 1)[0] in LIFTED:
            assert not isinstance(value, dict), (
                f"{key} still records the v1 raise instead of SQL: {value}")


def test_positive_cases_generate_sql(baseline) -> None:
    """The already-supported shapes must stay real SQL through the change."""
    for key, value in baseline.items():
        if key.split("::", 1)[0] not in LIFTED:
            assert not isinstance(value, dict), f"{key} unexpectedly raises: {value}"
