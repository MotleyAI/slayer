"""Ranked first/last kernel over expression values and ranking keys: executed values and Tier-1 emission."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2119_fixtures import BACKENDS, EFF, TIER1, by_bucket, by_dims, emit, engine_for, m, q, td

MIXED = "quantity * avg(price, partition_by=product)"

#: (formula, per-region value) over ``dimensions=["region"]``.
REGION_SHAPES: dict[str, tuple[str, dict[str, Any]]] = {
    "expression_value": ("last(amount * quantity)", {"EU": 200.0, "US": 80.0}),
    "expression_key_last": (f"last(amount, {EFF})", {"EU": 50.0, "US": 60.0}),
    "expression_key_first": (f"first(amount, {EFF})", {"EU": 10.0, "US": 30.0}),
    "value_and_key": (f"last(amount * quantity, {EFF})", {"EU": 200.0, "US": 60.0}),
    "joined_leaves": ("last(amount - coalesce(customers.discount, 0), coalesce(customers.signup_ts, created_at))",
                      {"EU": 49.5, "US": 40.0}),
    "attached_constituent": (f"last({MIXED}, {EFF})", {"EU": pytest.approx(28 / 3), "US": pytest.approx(14 / 3)}),
}
#: (formula, per created_at month value) over a month time dimension on ``created_at``.
WINDOWED_SHAPES: dict[str, tuple[str, dict[str, float]]] = {
    "windowed_last": (f"last(amount, {EFF}, window='2m')",
                      {"2024-12": 60.0, "2025-01": 60.0, "2025-02": 40.0, "2025-03": 50.0}),
    "windowed_first": (f"first(amount, {EFF}, window='2m')",
                       {"2024-12": 60.0, "2025-01": 10.0, "2025-02": 10.0, "2025-03": 30.0}),
}


def _region_query(formula: str) -> SlayerQuery:
    return q(dimensions=["region"], measures=[m(formula)])


def _windowed_query(formula: str) -> SlayerQuery:
    return q(time_dimensions=[td("created_at")], measures=[m(formula)])


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with engine_for(request.param) as e:
        yield e


class TestExecutedValues:
    @pytest.mark.parametrize("shape", list(REGION_SHAPES))
    async def test_region_shapes(self, engine: SlayerQueryEngine, shape: str) -> None:
        formula, expected = REGION_SHAPES[shape]
        resp = await engine.execute(_region_query(formula))
        assert {k[0]: v for k, v in by_dims(resp, ["orders.region"]).items()} == expected

    @pytest.mark.parametrize("shape", list(WINDOWED_SHAPES))
    async def test_windowed_shapes(self, engine: SlayerQueryEngine, shape: str) -> None:
        formula, expected = WINDOWED_SHAPES[shape]
        resp = await engine.execute(_windowed_query(formula))
        assert by_bucket(resp, "orders.created_at", "orders.v") == expected


class TestTier1Emission:
    @pytest.mark.parametrize("dialect", TIER1)
    @pytest.mark.parametrize("shape", [*REGION_SHAPES, *WINDOWED_SHAPES])
    async def test_emits_on_every_dialect(self, shape: str, dialect: str) -> None:
        query = (_region_query(REGION_SHAPES[shape][0]) if shape in REGION_SHAPES
                 else _windowed_query(WINDOWED_SHAPES[shape][0]))
        sql = await emit(query, dialect=dialect)
        assert "ROW_NUMBER" in sql.upper(), sql
