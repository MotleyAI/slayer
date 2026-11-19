"""A granularity call over any row-level argument, executed on SQLite and DuckDB (spec: queries/time-points)."""

from __future__ import annotations

import pytest

from slayer.core.errors import DateOperandTypeError, TimeDimensionFieldError
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
        resp = await engine.execute(q(dimensions=["orders.id"], filter=[f"date_trunc('month', {EFF}) >= '2025-02-15'"]))
        assert _ids(resp) == {4, 5, 6}

    async def test_compared_with_another_granularity_call(self, engine) -> None:
        resp = await engine.execute(q(dimensions=["orders.id"], filter=[f"date_trunc('month', {EFF}) = date_trunc('month', orders.created_at)"]))
        assert _ids(resp) == {1, 3}

    async def test_in_a_computed_dimension_over_a_cast(self, engine) -> None:
        resp = await engine.execute(q(
            dimensions=[{"field": "date_trunc('year', CAST(orders.created_txt AS DATE))", "alias": "y"}], metrics=[m("count(orders.*)")],
        ))
        assert {str(r["y"])[:4]: num(r["v"]) for r in resp.data} == {"2024": 1, "2025": 5}

    async def test_in_an_aggregation_source(self, engine) -> None:
        resp = await engine.execute(q(metrics=[m(f"count_distinct(date_trunc('month', {EFF}))")]))
        assert [num(r["v"]) for r in resp.data] == [4]

    async def test_in_arithmetic(self, engine) -> None:
        resp = await engine.execute(q(dimensions=[
            "orders.id", {"field": f"date_diff('day', date_trunc('month', {EFF}), {EFF})", "alias": "d"},
        ]))
        assert _by_id(resp, "d") == {1: 6, 2: 1, 3: 9, 4: 2, 5: 0, 6: 19}


class TestNonTemporalArgument:
    async def test_in_a_filter_names_the_operand(self, engine) -> None:
        query = q(dimensions=["orders.id"], filter=["date_trunc('month', upper(orders.status)) >= '2025-01-01'"])
        with pytest.raises(DateOperandTypeError) as ei:
            await engine.execute(query, dry_run=True)
        assert "upper(status)" in str(ei.value)

    async def test_as_a_dimension_is_a_time_dimension_type_error(self, engine) -> None:
        query = q(dimensions=["date_trunc('month', upper(orders.status))"], metrics=[m("count(orders.*)")])
        with pytest.raises(TimeDimensionFieldError) as ei:
            await engine.execute(query, dry_run=True)
        assert "STRING" in str(ei.value).upper()
