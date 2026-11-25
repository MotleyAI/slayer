"""Layer 1 gaps: every gap row of the REQUIRED-construct manifest is a strict xfail asserting Ossie's value."""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest

from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2215_fixtures import BACKENDS, dev2215_engine, q
from tests._dev2446_required_manifest import ROWS, Row
from tests.test_dev2446_required_constructs import assert_ossie_value

GAPS = [r for r in ROWS if r.gap is not None]
NULL_PLACEMENT = "DEFAULT NULL PLACEMENT"
#: ``RANK() OVER (ORDER BY orders.amount DESC)`` per order id: NULL sorts first for DESC (row 5's amount is NULL).
NULL_PLACEMENT_RANKS = {5: 1, 6: 2, 4: 3, 3: 4, 2: 5, 1: 6}


@pytest.fixture(scope="module")
async def engine(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    async with dev2215_engine(request.param) as e:
        yield e


def test_every_gap_row_carries_ossies_value_and_its_error() -> None:
    missing = [f"{r.form}|{r.expression}" for r in GAPS if "value" not in r.model_fields_set or r.fails_with is None]
    assert not missing


def test_holds_on_names_tested_databases() -> None:
    assert all(r.holds_on <= set(BACKENDS) for r in GAPS)


def _cases() -> list[object]:
    """One parameter per (case, database); a strict xfail except where the row already holds."""
    return [
        pytest.param(backend, r, id=f"{r.form}|{r.expression}|{backend}",
                     marks=() if backend in r.holds_on else pytest.mark.xfail(strict=True, raises=r.fails_with,
                                                                              reason=str(r.gap)))
        for backend in BACKENDS for r in GAPS
    ]


@pytest.mark.parametrize(("engine", "row"), _cases(), indirect=["engine"])
async def test_gap_has_ossies_value(engine: SlayerQueryEngine, row: Row) -> None:
    if row.form == NULL_PLACEMENT:  # one row ranks 1 under any NULL order: rank every order
        response = await engine.execute(q(dimensions=[{"field": "orders.id", "alias": "id"},
                                                      {"field": row.expression, "alias": "v"}]))
        assert {r["id"]: int(r["v"]) for r in response.data} == NULL_PLACEMENT_RANKS
        return
    if row.kind == "aggregate":
        query = q(metrics=[{"metric": row.expression, "alias": "v"}])
    else:
        query = q(dimensions=[{"field": row.expression, "alias": "v"}], filter="orders.id = 3")
    response = await engine.execute(query)
    assert len(response.data) == 1, response.data
    assert_ossie_value(response.data[0]["v"], row.value)
