"""Measures may be named after transforms (spec: queries/measure-naming)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest

from slayer.core.models import Aggregation, DatasetMetric
from slayer.core.query import QueryRefinement, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from tests._query_heal_fixtures import BACKENDS, heal_engine, heal_datasets, orders_dataset, table

MONTH = {"field": "created_at", "granularity": "month"}


@pytest.fixture(params=BACKENDS)
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with heal_engine(request.param) as eng:
        yield eng


def monthly(name: str, **kw: Any) -> dict[str, Any]:
    return {
        "population": "orders", "time_dimensions": [MONTH],
        "metrics": [{"metric": "change(sum(orders.amount))", "alias": name}], **kw,
    }


async def assert_renamed_twin(engine: SlayerQueryEngine, healed: Any, control: Any, *, ordered: bool = False) -> None:
    """``healed`` (naming ``change``) equals ``control`` (naming ``chg``) up to the name."""
    expected = await engine.execute(control)
    actual = await engine.execute(healed)
    assert actual.columns == [c.replace("chg", "change") for c in expected.columns]
    assert table(actual, ordered=ordered) == table(expected, ordered=ordered)
    assert actual.sql == (expected.sql or "").replace("chg", "change")


class TestQueryMeasureNamedAfterTransform:
    async def test_result_key_rows_and_sql(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(monthly("change"))
        assert "change" in resp.columns
        await assert_renamed_twin(engine, monthly("change"), monthly("chg"))

    async def test_filter_reference(self, engine: SlayerQueryEngine) -> None:
        await assert_renamed_twin(
            engine, monthly("change", filter=["orders.change > 0"]), monthly("chg", filter=["orders.chg > 0"]),
        )

    async def test_order_reference(self, engine: SlayerQueryEngine) -> None:
        await assert_renamed_twin(
            engine,
            monthly("change", order=[{"field": "orders.change", "direction": "desc"}, {"field": "orders.created_at"}]),
            monthly("chg", order=[{"field": "orders.chg", "direction": "desc"}, {"field": "orders.created_at"}]),
            ordered=True,
        )

    async def test_outer_stage_reads_flat_column(self, engine: SlayerQueryEngine) -> None:
        def stages(name: str) -> list[dict[str, Any]]:
            return [
                {**monthly(name), "name": "s"},
                {"population": "s", "metrics": [{"metric": f"max({name})", "alias": f"top_{name}"}]},
            ]

        await assert_renamed_twin(engine, stages("change"), stages("chg"))

    @pytest.mark.parametrize("name", ["cumsum", "change", "change_pct", "time_shift", "first", "last", "lag", "lead",
                                      "consecutive_periods", "rank", "percent_rank", "dense_rank", "ntile"])
    def test_every_transform_name_accepted(self, name: str) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": [{"metric": "count(orders.*)", "alias": name}]})
        assert [m.alias for m in query.metrics or []] == [name]

    def test_refinement_measure_accepted(self) -> None:
        refinement = QueryRefinement.model_validate({"metrics": [{"metric": "count(*)", "alias": "rank"}]})
        assert [m.alias for m in refinement.metrics or []] == ["rank"]


class TestModelExtensionMeasure:
    async def test_extension_measure_named_lag(self, engine: SlayerQueryEngine) -> None:
        def query(name: str) -> dict[str, Any]:
            return {
                "population": "orders",
                "extensions": [{"dataset": "orders", "metrics": [{"name": name, "expression": "sum(amount)"}]}],
                "dimensions": ["orders.region"], "metrics": [name],
            }

        expected = await engine.execute(query("total"))
        actual = await engine.execute(query("lag"))
        assert actual.columns == ["orders.region", "orders.lag"]
        assert table(actual, ordered=False) == table(expected, ordered=False)


class TestSavedMeasureNamedRank:
    def test_model_measure_accepted(self) -> None:
        assert DatasetMetric(name="rank", expression="count(*)").name == "rank"

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_bare_name_reads_measure_and_call_is_transform(self, backend: str) -> None:
        orders = orders_dataset(metrics=[
            DatasetMetric(name="rank", expression="count(*)"), DatasetMetric(name="n", expression="count(*)"),
        ])

        def query(saved: str) -> dict[str, Any]:
            return {
                "population": "orders", "dimensions": ["orders.region"],
                "metrics": [
                    {"metric": f"{saved} * 2", "alias": "doubled"},
                    {"metric": "rank(sum(orders.amount), direction='desc')", "alias": "r"},
                ],
            }

        async with heal_engine(backend, datasets=heal_datasets(orders=orders)) as eng:
            actual = await eng.execute(query("rank"))
            expected = await eng.execute(query("n"))
        assert actual.columns == expected.columns
        assert table(actual, ordered=False) == table(expected, ordered=False)
        by_region = {r["orders.region"]: (r["doubled"], r["r"]) for r in actual.data}
        assert by_region == {"EU": (6, 1), "US": (6, 2), None: (2, 3)}


def test_custom_aggregation_keeps_transform_reservation() -> None:
    with pytest.raises(ValueError, match="transform"):
        Aggregation(name="cumsum", expression="SUM({value})")
