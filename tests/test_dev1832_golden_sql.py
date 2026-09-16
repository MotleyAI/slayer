"""DEV-1832 golden SQL (task 1.7) — the lifted expression-aggregation shapes
across seven Tier-1 dialects.

Blessed pre-implementation as today's feature-missing output: every ``lifted/``
shape records a RAISE (the v1 boundary). ``test_lifted_cases_generate_sql`` is
this module's feature-missing tripwire — RED until the boundaries fall. At
implementation each ``lifted/`` case flips to SQL, enters ALLOWED_DELTAS, and is
re-blessed with executed values pinned. The ``positive/`` shapes are already
supported and MUST stay byte-identical SQL through the change.
"""

from __future__ import annotations

from pathlib import Path

from slayer.core.enums import TimeGranularity
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension

from tests._dev1832_fixtures import dev1832_models
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1832_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]
ALLOWED_DELTAS: dict[str, str] = {}

_MONTH_TD = [TimeDimension(dimension=ColumnRef(name="ordered_at"),
                           granularity=TimeGranularity.MONTH)]


def _cases() -> dict:
    return {
        # lifted/* — a v1 boundary today (records a raise); SQL after the change.
        "lifted/host_homed_arith": {
            "source": "orders", "mode": None,
            "kw": {"dimensions": ["status"],
                   "measures": [{"formula": "sum(amount - customers.discount)",
                                 "name": "m"}]}},
        "lifted/target_homed": {
            "source": "orders", "mode": None,
            "kw": {"dimensions": ["customers.tier"], "measures": [
                {"formula": "sum(customers.spend - customers.regions.pop)",
                 "name": "m"}]}},
        "lifted/two_branch": {
            "source": "orders", "mode": None,
            "kw": {"dimensions": ["status"], "measures": [
                {"formula": "sum(customers.spend - stores.rent)", "name": "m"}]}},
        "lifted/cross_model_partition": {
            "source": "orders", "mode": None,
            "kw": {"dimensions": ["status"], "measures": [
                {"formula": "sum(amount - customers.discount, partition_by=status)",
                 "name": "m"}]}},
        "lifted/filtered_operand": {
            "source": "sales", "mode": None,
            "kw": {"dimensions": ["region"],
                   "measures": [{"formula": "sum(q_amount - 1)", "name": "m"}]}},
        "lifted/filtered_leaf_joined": {
            "source": "orders", "mode": None,
            "kw": {"dimensions": ["status"], "measures": [
                {"formula": "sum(amount - customers.north_spend)", "name": "m"}]}},
        "lifted/transform_constituent": {
            "source": "monthly", "mode": None,
            "kw": {"time_dimensions": _MONTH_TD, "measures": [{"formula":
                "sum(cumsum(amount:sum, partition_by=[region, month(ordered_at)]) - 1)",
                "name": "m"}]}},
        "lifted/mixed_transform": {
            "source": "sales", "mode": None,
            "kw": {"dimensions": ["region"], "measures": [{"formula":
                "sum(quantity * rank(avg(unit_price, partition_by=product)))",
                "name": "m"}]}},
        # positive/* — already supported; byte-identical SQL through the change.
        "positive/same_model_arith": {
            "source": "orders", "mode": None,
            "kw": {"dimensions": ["status"],
                   "measures": [{"formula": "sum(amount - cost)", "name": "m"}]}},
        "positive/reaggregation": {
            "source": "sales", "mode": None,
            "kw": {"dimensions": ["region"], "measures": [{"formula":
                "avg(sum(amount, partition_by=[city, region]))", "name": "m"}]}},
        "positive/mixed_row_attached": {
            "source": "sales", "mode": None,
            "kw": {"dimensions": ["region"], "measures": [{"formula":
                "sum(quantity * avg(unit_price, partition_by=product))", "name": "m"}]}},
    }


LIFTED = {k for k in _cases() if k.startswith("lifted/")}


async def _generate_one(case, dialect: str):
    models = dev1832_models()
    try:
        kw = dict(case["kw"])
        if case["mode"] is not None:
            kw["to_many_handling"] = case["mode"]
        query = SlayerQuery(source_model=case["source"], **kw)
        root = next(m for m in models if m.name == case["source"])
        return await _engine_generate(
            query=query, model=root,
            extra_models=[m for m in models if m.name != case["source"]],
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
    """Feature-missing tripwire: every lifted shape must emit SQL once the v1
    boundaries fall. RED now (each records the v1 raise), green after the change."""
    for key, value in baseline.items():
        if key.split("::", 1)[0] in LIFTED:
            assert not isinstance(value, dict), (
                f"{key} still records the v1 raise instead of SQL: {value}")


def test_positive_cases_generate_sql(baseline) -> None:
    """The already-supported shapes must stay real SQL through the change."""
    for key, value in baseline.items():
        if key.split("::", 1)[0] not in LIFTED:
            assert not isinstance(value, dict), f"{key} unexpectedly raises: {value}"
