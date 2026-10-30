"""A granularity call over any row-level argument, executed on SQLite and DuckDB (spec: queries/time-points)."""

from __future__ import annotations

import pytest

from slayer.core.errors import DateOperandTypeError, TimeDimensionColumnError
from slayer.engine.query_engine import SlayerResponse
from tests._dev2119_fixtures import BACKENDS, EFF, engine_for, m, num, q


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with engine_for(request.param) as e:
        yield e


def _ids(resp: SlayerResponse) -> set[int]:
    return {int(r["orders.id"]) for r in resp.data}


def _by_id(resp: SlayerResponse, name: str) -> dict[int, float | None]:
    return {int(r["orders.id"]): num(r[f"orders.{name}"]) for r in resp.data}


class TestExpressionArgument:
    async def test_in_a_filter(self, engine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=[f"month({EFF}) >= '2025-02-15'"]))
        assert _ids(resp) == {4, 5, 6}

    async def test_compared_with_another_granularity_call(self, engine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=[f"month({EFF}) = month(created_at)"]))
        assert _ids(resp) == {1, 3}

    async def test_in_a_computed_dimension_over_a_cast(self, engine) -> None:
        resp = await engine.execute(q(
            dimensions=[{"expression": "year(CAST(created_txt AS DATE))", "name": "y"}], measures=[m("count(*)")],
        ))
        assert {str(r["orders.y"])[:4]: num(r["orders.v"]) for r in resp.data} == {"2024": 1, "2025": 5}

    async def test_in_an_aggregation_source(self, engine) -> None:
        resp = await engine.execute(q(measures=[m(f"count_distinct(month({EFF}))")]))
        assert [num(r["orders.v"]) for r in resp.data] == [4]

    async def test_in_arithmetic(self, engine) -> None:
        resp = await engine.execute(q(dimensions=[
            "id", {"expression": f"date_diff('day', month({EFF}), {EFF})", "name": "d"},
        ]))
        assert _by_id(resp, "d") == {1: 6, 2: 1, 3: 9, 4: 2, 5: 0, 6: 19}


class TestNonTemporalArgument:
    async def test_in_a_filter_names_the_operand(self, engine) -> None:
        query = q(dimensions=["id"], filters=["month(upper(status)) >= '2025-01-01'"])
        with pytest.raises(DateOperandTypeError) as ei:
            await engine.execute(query, dry_run=True)
        assert "upper(status)" in str(ei.value)

    async def test_as_a_dimension_is_a_time_dimension_type_error(self, engine) -> None:
        query = q(dimensions=["month(upper(status))"], measures=[m("count(*)")])
        with pytest.raises(TimeDimensionColumnError) as ei:
            await engine.execute(query, dry_run=True)
        assert "TEXT" in str(ei.value).upper()
