"""Population recommendation reads qualified items (spec: queries/population › Root recommendation follows the
population rule; queries/references › A row count names its dataset, A bare name is a metric)."""

from __future__ import annotations

import re

import pytest

from slayer.core.errors import SlayerError
from slayer.core.recommend import ItemPath, PopulationRecommendation

from tests._dev2277_fixtures import BACKENDS, by, execute_recording, response_conversions, seed_engine


def path_of(rec: PopulationRecommendation, item: str) -> ItemPath:
    (hit,) = [p for p in rec.item_paths if p.input_item == item]
    return hit


async def test_a_row_count_is_an_attachment_on_its_dataset() -> None:
    async with seed_engine("sqlite") as eng:
        rec = await eng.recommend_population(["customers.region", "count(orders.*)"])
    assert rec.reachable is True
    assert rec.population == "customers"
    count = path_of(rec, "count(orders.*)")
    assert (count.path, count.attachment, count.unrelated) == ("count(orders.*)", True, False)
    assert path_of(rec, "customers.region").attachment is False


async def test_a_row_count_does_not_steer_the_choice() -> None:
    async with seed_engine("sqlite") as eng:
        alone = await eng.recommend_population(["customers.region"])
        with_count = await eng.recommend_population(["customers.region", "count(orders.*)", "count(returns.*)"])
    assert alone.population == with_count.population == "customers"


async def test_an_item_resolving_along_the_chosen_path_is_its_own_path() -> None:
    items = ["orders.status", "customers.region", "sum(customers.manager_id)", "orders.total_revenue"]
    async with seed_engine("sqlite") as eng:
        rec = await eng.recommend_population(items)
    assert rec.population == "orders"
    assert {p.input_item: p.path for p in rec.item_paths} == {item: item for item in items}


@pytest.mark.parametrize("backend", BACKENDS)
async def test_the_recommended_row_count_executes(backend: str) -> None:
    async with seed_engine(backend) as eng:
        rec = await eng.recommend_population(["customers.region", "count(orders.*)"])
        assert rec.population is not None
        resp, _ = await execute_recording(eng, {
            "population": rec.population,
            "dimensions": [path_of(rec, "customers.region").path],
            "metrics": [path_of(rec, "count(orders.*)").path],
        })
    assert response_conversions(resp) == []
    region_key, count_key = resp.columns
    counts = by(resp, region_key, count_key)
    assert (counts["EAST"], counts["WEST"]) == (3.0, 1.0)


@pytest.mark.parametrize("items", [
    ["customers.region", "count(*)"], ["count(*)"],
], ids=["with-a-dimension", "alone"])
async def test_an_unqualified_row_count_is_refused(items: list[str]) -> None:
    async with seed_engine("sqlite") as eng:
        with pytest.raises(SlayerError) as info:
            await eng.recommend_population(items)
    assert "count(*)" in str(info.value)
    assert re.search(r"count\((<dataset>|\w+)\.\*\)", str(info.value)), str(info.value)


@pytest.mark.parametrize(("item", "suggestions"), [
    ("amount", ["orders.amount", "returns.amount"]),
    ("sum(amount)", ["orders.amount", "returns.amount"]),
    ("region", ["customers.region"]),
    ("total_revenue", ["orders.total_revenue"]),
])
async def test_a_bare_name_is_refused_with_qualified_suggestions(item: str, suggestions: list[str]) -> None:
    async with seed_engine("sqlite") as eng:
        with pytest.raises(SlayerError) as info:
            await eng.recommend_population(["customers.region", item])
    bare = item.removeprefix("sum(").removesuffix(")")
    assert repr(bare) in str(info.value) or f"`{bare}`" in str(info.value)
    for suggestion in suggestions:
        assert suggestion in str(info.value)
