"""Measures may be named after transforms (spec: queries/measure-naming)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest

from slayer.core.models import Aggregation, ModelMeasure
from slayer.core.query import QueryRefinement, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from tests._query_heal_fixtures import BACKENDS, heal_engine, heal_models, orders_model, table

MONTH = {"dimension": "created_at", "granularity": "month"}


@pytest.fixture(params=BACKENDS)
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with heal_engine(request.param) as eng:
        yield eng


def monthly(name: str, **kw: Any) -> dict[str, Any]:
    return {
        "source_model": "orders", "time_dimensions": [MONTH],
        "measures": [{"formula": "change(sum(amount))", "name": name}], **kw,
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
        assert "orders.change" in resp.columns
        await assert_renamed_twin(engine, monthly("change"), monthly("chg"))

    async def test_filter_reference(self, engine: SlayerQueryEngine) -> None:
        await assert_renamed_twin(
            engine, monthly("change", filters=["change > 0"]), monthly("chg", filters=["chg > 0"]),
        )

    async def test_order_reference(self, engine: SlayerQueryEngine) -> None:
        await assert_renamed_twin(
            engine,
            monthly("change", order=[{"column": "change", "direction": "desc"}]),
            monthly("chg", order=[{"column": "chg", "direction": "desc"}]),
            ordered=True,
        )

    async def test_outer_stage_reads_flat_column(self, engine: SlayerQueryEngine) -> None:
        def stages(name: str) -> list[dict[str, Any]]:
            return [
                {**monthly(name), "name": "s"},
                {"source_model": "s", "measures": [{"formula": f"max({name})", "name": f"top_{name}"}]},
            ]

        await assert_renamed_twin(engine, stages("change"), stages("chg"))

    @pytest.mark.parametrize("name", ["cumsum", "change", "change_pct", "time_shift", "first", "last", "lag", "lead",
                                      "consecutive_periods", "rank", "percent_rank", "dense_rank", "ntile"])
    def test_every_transform_name_accepted(self, name: str) -> None:
        query = SlayerQuery.model_validate({"source_model": "orders", "measures": [{"formula": "count(*)", "name": name}]})
        assert [m.name for m in query.measures or []] == [name]

    def test_refinement_measure_accepted(self) -> None:
        refinement = QueryRefinement.model_validate({"measures": [{"formula": "count(*)", "name": "rank"}]})
        assert [m.name for m in refinement.measures or []] == ["rank"]


class TestModelExtensionMeasure:
    async def test_extension_measure_named_lag(self, engine: SlayerQueryEngine) -> None:
        def query(name: str) -> dict[str, Any]:
            return {
                "source_model": {"source_name": "orders", "measures": [{"name": name, "formula": "sum(amount)"}]},
                "dimensions": ["region"], "measures": [name],
            }

        expected = await engine.execute(query("total"))
        actual = await engine.execute(query("lag"))
        assert actual.columns == ["orders.region", "orders.lag"]
        assert table(actual, ordered=False) == table(expected, ordered=False)


class TestSavedMeasureNamedRank:
    def test_model_measure_accepted(self) -> None:
        assert ModelMeasure(name="rank", formula="count(*)").name == "rank"

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_bare_name_reads_measure_and_call_is_transform(self, backend: str) -> None:
        orders = orders_model(measures=[
            ModelMeasure(name="rank", formula="count(*)"), ModelMeasure(name="n", formula="count(*)"),
        ])

        def query(saved: str) -> dict[str, Any]:
            return {
                "source_model": "orders", "dimensions": ["region"],
                "measures": [
                    {"formula": f"{saved} * 2", "name": "doubled"},
                    {"formula": "rank(sum(amount), direction='desc')", "name": "r"},
                ],
            }

        async with heal_engine(backend, models=heal_models(orders=orders)) as eng:
            actual = await eng.execute(query("rank"))
            expected = await eng.execute(query("n"))
        assert actual.columns == expected.columns
        assert table(actual, ordered=False) == table(expected, ordered=False)
        by_region = {r["orders.region"]: (r["orders.doubled"], r["orders.r"]) for r in actual.data}
        assert by_region == {"EU": (6, 1), "US": (6, 2), None: (2, 3)}


def test_custom_aggregation_keeps_transform_reservation() -> None:
    with pytest.raises(ValueError, match="transform"):
        Aggregation(name="cumsum", formula="SUM({value})")
