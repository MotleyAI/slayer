"""DEV-1739 golden SQL baseline. Pins the emitted SQL (and recorded raises) for
every partition_by shape across five dialects; ``build_baseline`` forces
``SLAYER_VALIDATE_SCOPES=1`` so each entry is scope-closed. Regenerate with
``SLAYER_UPDATE_GOLDEN=1``."""

from __future__ import annotations

from pathlib import Path

from slayer.core.query import SlayerQuery

from tests._dev1739_fixtures import dev1739_models, month_td
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise


GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1739_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]
ALLOWED_DELTAS: dict[str, str] = {}


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


WORKING_PREFIXES = ("local/", "cross_model/")


def _cases() -> dict:
    return {
        "local/single_key": _q(
            dimensions=["region", "city"],
            measures=[
                {"formula": "amount:sum", "name": "city_rev"},
                {"formula": "amount:sum(partition_by=region)", "name": "region_rev"},
            ],
        ),
        "local/grand_total": _q(
            dimensions=["region", "city"],
            measures=[{"formula": "amount:sum(partition_by=[])", "name": "g"}],
        ),
        "local/multi_key": _q(
            dimensions=["region", "channel", "city"],
            measures=[
                {"formula": "amount:sum(partition_by=[region, channel])", "name": "rc"},
            ],
        ),
        "local/share_composite": _q(
            dimensions=["region", "city"],
            measures=[
                {"formula": "amount:sum / amount:sum(partition_by=region)", "name": "sh"},
            ],
        ),
        "local/filtered": _q(
            dimensions=["region", "city"],
            measures=[{"formula": "ok_amount:sum(partition_by=region)", "name": "o"}],
        ),
        "local/star_count": _q(
            dimensions=["region", "city"],
            measures=[{"formula": "*:count(partition_by=region)", "name": "n"}],
        ),
        "local/time_dim": _q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[{"formula": "amount:sum(partition_by=ordered_at)", "name": "m"}],
        ),
        "local/full_grain_degenerate": _q(
            dimensions=["region", "city"],
            measures=[
                {"formula": "amount:sum(partition_by=[region, city])", "name": "d"},
            ],
        ),
        "cross_model/grand_total": _q(
            dimensions=["customers.tier"],
            measures=[
                {"formula": "customers.spend:sum(partition_by=[])", "name": "t"},
            ],
        ),
        "cross_model/rerooted": _q(
            dimensions=["customers.regions.name"],
            measures=[
                {"formula": "customers.spend:sum(partition_by=[])", "name": "t"},
            ],
        ),
        "cross_model/rerooted_partition_key": _q(
            dimensions=["customers.regions.name", "customers.tier"],
            measures=[
                {"formula": "customers.spend:sum(partition_by=customers.regions.name)",
                 "name": "rt"},
            ],
        ),
        "guard/non_dimension": _q(
            dimensions=["region"],
            measures=[{"formula": "amount:sum(partition_by=city)", "name": "x"}],
        ),
        # guard/first, guard/last, guard/nested_transform, guard/in_filter,
        # guard/window_plus_partition removed — DEV-1824 (tasks 3.3/3.4/3.5/3.6)
        # lifts these LOCAL shapes; their goldens now live in
        # tests/test_dev1824_golden_sql.py (lift/first_last_partition,
        # lift/cumsum_partitioned, lift/filter_partitioned, lift/window_partition).
        "guard/cross_model_outside_grain": _q(
            dimensions=["region", "customers.tier"],
            measures=[
                {"formula": "customers.spend:sum(partition_by=region)", "name": "x"},
            ],
        ),
    }


async def _generate_one(query: SlayerQuery, dialect: str):
    models = dev1739_models()
    try:
        return await _engine_generate(
            query=query, model=models[0], extra_models=models[1:],
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
