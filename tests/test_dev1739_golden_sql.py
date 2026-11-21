"""Golden SQL baseline. Pins the emitted SQL (and recorded raises) for
every partition_by shape across five dialects; ``build_baseline`` forces
``SLAYER_VALIDATE_SCOPES=1`` so each entry is scope-closed. Regenerate with
``SLAYER_UPDATE_GOLDEN=1``."""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import SlayerQuery

from tests._dev1739_fixtures import dev1739_datasets, month_td
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise


GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1739_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]
ALLOWED_DELTAS: dict[str, str] = {}


def _q(**kw) -> SlayerQuery:
    kw.setdefault("population", "orders")
    return SlayerQuery(**kw)


WORKING_PREFIXES = ("local/", "cross_model/")


def _cases() -> dict:
    return {
        "local/single_key": _q(
            dimensions=["orders.region", "orders.city"],
            metrics=[
                {"metric": "sum(orders.amount)", "alias": "city_rev"},
                {"metric": "sum(orders.amount, partition_by=orders.region)", "alias": "region_rev"},
            ],
        ),
        "local/grand_total": _q(
            dimensions=["orders.region", "orders.city"],
            metrics=[{"metric": "sum(orders.amount, partition_by=[])", "alias": "g"}],
        ),
        "local/multi_key": _q(
            dimensions=["orders.region", "orders.channel", "orders.city"],
            metrics=[
                {"metric": "sum(orders.amount, partition_by=[orders.region, orders.channel])", "alias": "rc"},
            ],
        ),
        "local/share_composite": _q(
            dimensions=["orders.region", "orders.city"],
            metrics=[
                {"metric": "sum(orders.amount) / sum(orders.amount, partition_by=orders.region)", "alias": "sh"},
            ],
        ),
        "local/filtered": _q(
            dimensions=["orders.region", "orders.city"],
            metrics=[{"metric": "sum(orders.ok_amount, partition_by=orders.region)", "alias": "o"}],
        ),
        "local/star_count": _q(
            dimensions=["orders.region", "orders.city"],
            metrics=[{"metric": "count(orders.*, partition_by=orders.region)", "alias": "n"}],
        ),
        "local/time_dim": _q(
            dimensions=["orders.region"], time_dimensions=month_td(),
            metrics=[{"metric": "sum(orders.amount, partition_by=orders.ordered_at)", "alias": "m"}],
        ),
        "local/full_grain_degenerate": _q(
            dimensions=["orders.region", "orders.city"],
            metrics=[
                {"metric": "sum(orders.amount, partition_by=[orders.region, orders.city])", "alias": "d"},
            ],
        ),
        "cross_model/grand_total": _q(
            dimensions=["customers.tier"],
            metrics=[
                {"metric": "sum(customers.spend, partition_by=[])", "alias": "t"},
            ],
        ),
        "cross_model/rerooted": _q(
            dimensions=["customers.regions.name"],
            metrics=[
                {"metric": "sum(customers.spend, partition_by=[])", "alias": "t"},
            ],
        ),
        "cross_model/rerooted_partition_key": _q(
            dimensions=["customers.regions.name", "customers.tier"],
            metrics=[
                {"metric": "sum(customers.spend, partition_by=customers.regions.name)",
                 "alias": "rt"},
            ],
        ),
        "guard/non_dimension": _q(
            dimensions=["orders.region"],
            metrics=[{"metric": "sum(orders.amount, partition_by=orders.city)", "alias": "x"}],
        ),
        # guard/first, guard/last, guard/nested_transform, guard/in_filter,
        # guard/window_plus_partition removed — these LOCAL shapes are lifted;
        # their goldens now live in
        # tests/test_dev1824_golden_sql.py (lift/first_last_partition,
        # lift/cumsum_partitioned, lift/filter_partitioned, lift/window_partition).
        "guard/cross_model_outside_grain": _q(
            to_many_handling="broadcast",
            dimensions=["orders.region", "customers.tier"],
            metrics=[
                {"metric": "sum(customers.spend, partition_by=orders.region)", "alias": "x"},
            ],
        ),
    }


async def _generate_one(query: SlayerQuery, dialect: str):
    models = dev1739_datasets()
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


def test_working_cases_isolate_and_guards_raise(baseline) -> None:
    for key, value in baseline.items():
        case_id = key.rsplit("::", 1)[0]
        if case_id.startswith(WORKING_PREFIXES):
            assert not isinstance(value, dict), f"{key} unexpectedly raised: {value}"
            assert "_cm_" in value, f"{key} did not isolate the partitioned aggregate"
        else:
            assert isinstance(value, dict), f"{key} should raise, got SQL"
