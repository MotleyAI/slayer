"""A granularity call in ``partition_by=`` binds to the matching query time dimension (spec: queries/partitioned-aggregates)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest

from slayer.core.errors import PartitionKeyError
from slayer.engine.query_engine import SlayerQueryEngine
from tests._query_heal_fixtures import BACKENDS, assert_equivalent, heal_engine

MONTH = {"dimension": "created_at", "granularity": "month"}
YEAR = {"dimension": "created_at", "granularity": "year"}


@pytest.fixture(params=BACKENDS)
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with heal_engine(request.param) as eng:
        yield eng


def partitioned(key: str, *, time_dimensions: list[Any], source: str = "orders", value: str = "amount",
                dimensions: tuple[str, ...] = ("region",)) -> dict[str, Any]:
    return {
        "source_model": source, "dimensions": list(dimensions), "time_dimensions": time_dimensions,
        "measures": [{"formula": f"sum({value}, partition_by=[{key}])", "name": "part_total"}],
    }


class TestProjectedTimeDimension:
    async def test_aggregation_and_rank(self, engine: SlayerQueryEngine) -> None:
        def query(key: str) -> dict[str, Any]:
            return {
                "source_model": "orders", "dimensions": ["region"], "time_dimensions": [MONTH],
                "measures": [
                    {"formula": f"sum(amount, partition_by=[{key}])", "name": "month_total"},
                    {"formula": f"rank(sum(amount), partition_by=[{key}], direction='desc')", "name": "r"},
                ],
            }

        await assert_equivalent(engine, query("month(created_at)"), query("created_at"))

    async def test_singular_element(self, engine: SlayerQueryEngine) -> None:
        def query(key: str) -> dict[str, Any]:
            return {
                "source_model": "orders", "dimensions": ["region"], "time_dimensions": [MONTH],
                "measures": [{"formula": f"sum(amount, partition_by={key})", "name": "month_total"}],
            }

        await assert_equivalent(engine, query("month(created_at)"), query("created_at"))

    async def test_filter_position(self, engine: SlayerQueryEngine) -> None:
        def query(key: str) -> dict[str, Any]:
            return {
                "source_model": "orders", "dimensions": ["region"], "time_dimensions": [MONTH],
                "measures": ["sum(amount)"], "filters": [f"sum(amount, partition_by=[{key}]) > 50"],
            }

        await assert_equivalent(engine, query("month(created_at)"), query("created_at"))

    async def test_picks_month_among_several_time_dimensions(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(partitioned("month(created_at)", time_dimensions=[MONTH, YEAR], dimensions=()))
        totals = {str(r["orders.created_at.month"])[:7]: r["orders.part_total"] for r in resp.data}
        assert totals == {"2024-01": 30.0, "2024-02": 70.0, "2024-03": 110.0, "2024-12": 70.0}

    async def test_bare_column_among_several_time_dimensions_stays_ambiguous(self, engine: SlayerQueryEngine) -> None:
        query = partitioned("created_at", time_dimensions=[MONTH, YEAR], dimensions=())
        with pytest.raises(PartitionKeyError, match="ambiguous"):
            await engine.execute(query)


class TestOtherTimeColumns:
    async def test_joined_time_column(self, engine: SlayerQueryEngine) -> None:
        td = [{"dimension": "customers.signup_at", "granularity": "month"}]
        await assert_equivalent(
            engine, partitioned("month(customers.signup_at)", time_dimensions=td),
            partitioned("customers.signup_at", time_dimensions=td),
        )

    async def test_custom_granularity(self, engine: SlayerQueryEngine) -> None:
        td = [{"dimension": "created_at", "granularity": "fiscal_year"}]
        await assert_equivalent(
            engine, partitioned("fiscal_year(created_at)", time_dimensions=td), partitioned("created_at", time_dimensions=td),
        )

    async def test_stage_time_column(self, engine: SlayerQueryEngine) -> None:
        inner = {
            "name": "s", "source_model": "orders", "dimensions": ["region"], "time_dimensions": [MONTH],
            "measures": [{"formula": "sum(amount)", "name": "rev"}],
        }

        def query(key: str) -> list[dict[str, Any]]:
            return [inner, partitioned(key, time_dimensions=[MONTH], source="s", value="rev")]

        await assert_equivalent(engine, query("month(created_at)"), query("created_at"))


class TestNoMatchingTimeDimension:
    @pytest.mark.parametrize("key", ["week(created_at)", "month(shipped_at)"])
    async def test_names_bucket_and_remedy(self, engine: SlayerQueryEngine, key: str) -> None:
        query = partitioned(key, time_dimensions=[MONTH])
        with pytest.raises(PartitionKeyError) as info:
            await engine.execute(query)
        message = str(info.value)
        assert key in message
        assert "time_dimensions" in message

    async def test_without_time_dimensions(self, engine: SlayerQueryEngine) -> None:
        query = partitioned("month(created_at)", time_dimensions=[])
        with pytest.raises(PartitionKeyError) as info:
            await engine.execute(query)
        assert "month(created_at)" in str(info.value)
