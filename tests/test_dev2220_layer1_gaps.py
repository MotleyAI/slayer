"""Layer 1 gaps: every gap row of the REQUIRED-construct manifest is a strict xfail asserting Ossie's value."""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest

from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2215_fixtures import BACKENDS, dev2215_engine, q
from tests._dev2446_required_manifest import ROWS, Row
from tests.test_dev2446_required_constructs import assert_ossie_value

GAPS = [r for r in ROWS if r.gap is not None]


@pytest.fixture(scope="module", params=BACKENDS)
async def engine(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    async with dev2215_engine(request.param) as e:
        yield e


def test_every_gap_row_carries_ossies_value_and_its_error() -> None:
    missing = [f"{r.form}|{r.expression}" for r in GAPS if "value" not in r.model_fields_set or r.fails_with is None]
    assert not missing


@pytest.mark.parametrize("row", [
    pytest.param(r, id=f"{r.form}|{r.expression}",
                 marks=pytest.mark.xfail(strict=True, raises=r.fails_with, reason=r.gap)) for r in GAPS
])
async def test_gap_has_ossies_value(engine: SlayerQueryEngine, row: Row) -> None:
    if row.kind == "aggregate":
        query = q(metrics=[{"metric": row.expression, "alias": "v"}])
    else:
        query = q(dimensions=[{"field": row.expression, "alias": "v"}], filter="orders.id = 3")
    response = await engine.execute(query)
    assert len(response.data) == 1, response.data
    assert_ossie_value(response.data[0]["v"], row.value)
