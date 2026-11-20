"""DEV-1837 golden SQL — the lifted computed-dimension × transform shapes and
the fixed M-part × time_shift across five dialects, plus the design-D7
generation smoke (tsql / bigquery / one case-folding dialect).

``build_baseline`` forces ``SLAYER_VALIDATE_SCOPES=1`` so each entry is
scope-closed; the baseline is blessed alongside the implementation
(``SLAYER_UPDATE_GOLDEN=1``) — until then every case fails with the
missing-baseline message, which is this suite's feature-missing state.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp

from slayer.sql.scope_check import assert_scope_closed

from slayer.core.query import QueryMetric

from tests._dev1837_fixtures import (
    BAND35,
    BARE_DIM,
    dev1837_datasets,
    month_td,
    q,
)
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1837_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]
#: D7 — generation smoke: two picky emitters + one case-folding dialect.
SMOKE_DIALECTS = ["tsql", "bigquery", "snowflake"]
ALLOWED_DELTAS: dict[str, str] = {}

BAND = {"field": BAND35, "alias": "band"}


def _cases() -> dict:
    return {
        "lift/band_time_shift_plain": q(
            dimensions=["orders.region", BAND], time_dimensions=month_td(),
            metrics=[
                QueryMetric(metric="sum(orders.amount)", alias="m"),
                QueryMetric(metric="time_shift(sum(orders.amount), -1)", alias="prev"),
            ],
        ),
        "lift/band_part_time_shift_cross": q(
            dimensions=["orders.region", BAND], time_dimensions=month_td(),
            metrics=[
                QueryMetric(metric="sum(orders.amount)", alias="m"),
                QueryMetric(metric="sum(orders.amount, partition_by=orders.region)", alias="rt"),
                QueryMetric(metric="time_shift(sum(orders.amount), -1)", alias="prev"),
            ],
        ),
        "lift/band_cumsum": q(
            dimensions=["orders.region", BAND], time_dimensions=month_td(),
            metrics=[
                QueryMetric(metric="sum(orders.amount)", alias="m"),
                QueryMetric(metric="cumsum(sum(orders.amount))", alias="c"),
            ],
        ),
        "lift/bare_cumsum": q(
            dimensions=["orders.region", {"field": BARE_DIM, "alias": "ct"}],
            time_dimensions=month_td(),
            metrics=[
                QueryMetric(metric="sum(orders.amount)", alias="m"),
                QueryMetric(metric="cumsum(sum(orders.amount))", alias="c"),
            ],
        ),
        "lift/part_time_shift_fixed": q(
            dimensions=["orders.region"], time_dimensions=month_td(),
            metrics=[
                QueryMetric(metric="sum(orders.amount, partition_by=orders.region)", alias="rt"),
                QueryMetric(metric="time_shift(sum(orders.amount), -1)", alias="prev"),
            ],
        ),
    }


async def _generate_one(query, dialect: str):
    models = dev1837_datasets()
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


def test_every_case_renders_without_placeholders(baseline) -> None:
    for key, value in baseline.items():
        assert not isinstance(value, dict), f"{key} unexpectedly raised: {value}"
        assert "__regroup__" not in value, f"{key} leaked a placeholder"


FLAGSHIP = [
    "lift/band_time_shift_plain",
    "lift/band_part_time_shift_cross",
    "lift/band_cumsum",
]


@pytest.mark.parametrize("case_id", FLAGSHIP)
@pytest.mark.parametrize("dialect", SMOKE_DIALECTS)
async def test_generation_smoke(case_id: str, dialect: str) -> None:
    """D7 — parses, one flat WITH (no nesting), scope-closed."""
    sql = await _generate_one(_cases()[case_id], dialect)
    assert not isinstance(sql, dict), f"raised: {sql}"
    tree = sqlglot.parse_one(sql, read=dialect)
    assert tree is not None
    with_nodes = list(tree.find_all(exp.With))
    assert len(with_nodes) == 1, f"{len(with_nodes)} WITH clauses:\n{sql}"
    assert_scope_closed(sql, dialect=dialect)
