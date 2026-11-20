"""``population_inferred`` and saved-query pinning (spec: queries/population › Population may be omitted, A saved
query's inferred populations are pinned at save; queries/saved-query-refinement)."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from slayer.core.models import SlayerDataset
from slayer.core.query import QueryRefinement, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.migrations import CURRENT_VERSIONS

from tests._dev2212_fixtures import (
    BACKENDS, orders_dataset, returns_dataset, rows, run, seed_datasets, seed_engine, table,
)
from tests._dev2277_fixtures import cycle_engine

R, REV, STATUS = "customers.region", "orders.total_revenue", "orders.status"
BY_REGION: dict[str, Any] = {"dimensions": [R], "metrics": [REV]}
#: The refined rows of ``BY_REGION`` + ``orders.status``, inferred with anchor ``customers``.
REFINED = [("EAST", "completed", 150), ("EAST", "open", 200), ("WEST", "open", 300)]


@pytest.fixture(params=BACKENDS)
async def engine(request: pytest.FixtureRequest):
    async with seed_engine(request.param) as eng:
        yield eng


async def stored_stages(engine: SlayerQueryEngine, name: str) -> list[SlayerQuery]:
    model = await engine.storage.get_dataset(name)
    assert model is not None
    return list(model.source_queries or [])


async def store_raw(engine: SlayerQueryEngine, name: str, stage: dict[str, Any]) -> None:
    """Store ``stage`` verbatim, bypassing save-time pinning."""
    await engine.storage.save_dataset(SlayerDataset(
        name=name, data_source="test", source_queries=[SlayerQuery.model_validate(stage)]))


class TestTheMarker:
    async def test_a_pinned_anchor_keeps_the_rows_inferred(self, engine: SlayerQueryEngine) -> None:
        pinned = await run(engine, population="customers", population_inferred=True, **BY_REGION)
        unpinned = await run(engine, **BY_REGION)
        assert table(pinned, R, REV) == table(unpinned, R, REV) == rows(("EAST", 350), ("WEST", 300))
        assert (pinned.population, pinned.population_inferred, pinned.row_sources) == (
            "customers", True, ["orders"])

    def test_the_marker_needs_a_population(self) -> None:
        with pytest.raises(ValidationError, match="population_inferred"):
            SlayerQuery.model_validate({"population_inferred": True, **BY_REGION})

    def test_the_marker_defaults_off_and_round_trips(self) -> None:
        assert SlayerQuery.model_validate(BY_REGION).population_inferred is False
        pinned = SlayerQuery.model_validate({"population": "customers", "population_inferred": True, **BY_REGION})
        again = SlayerQuery.model_validate(pinned.model_dump(mode="json", exclude_none=True))
        assert (again.population, again.population_inferred) == ("customers", True)

    def test_a_refinement_may_not_carry_the_marker(self) -> None:
        with pytest.raises(ValidationError):
            QueryRefinement.model_validate({"population_inferred": True})


class TestStoredVersion:
    def test_slayer_query_version_bumped(self) -> None:
        assert CURRENT_VERSIONS["SlayerQuery"] == 18

    def test_a_v17_stored_query_loads_unmarked(self) -> None:
        loaded = SlayerQuery.model_validate({"version": 17, "population": "customers", **BY_REGION})
        assert (loaded.population, loaded.population_inferred, loaded.version) == ("customers", False, 18)


class TestPinnedOnSave:
    async def test_pinned_on_save(self, engine: SlayerQueryEngine) -> None:
        before = await run(engine, **BY_REGION)
        await engine.create_dataset_from_query(query=BY_REGION, name="by_region")
        (stage,) = await stored_stages(engine, "by_region")
        assert (stage.population, stage.population_inferred) == ("customers", True)
        after = await engine.execute("by_region")
        assert table(after, R, REV) == table(before, R, REV) == rows(("EAST", 350), ("WEST", 300))
        assert (after.population, after.population_inferred) == ("customers", True)

    async def test_save_dataset_pins_the_marker(self, engine: SlayerQueryEngine) -> None:
        await engine.save_dataset(SlayerDataset(
            name="by_region", data_source="test", source_queries=[SlayerQuery.model_validate(BY_REGION)]))
        (stage,) = await stored_stages(engine, "by_region")
        assert (stage.population, stage.population_inferred) == ("customers", True)

    async def test_a_unit_stage_stays_unpinned(self, engine: SlayerQueryEngine) -> None:
        await engine.create_dataset_from_query(query={"metrics": [REV]}, name="total")
        (stage,) = await stored_stages(engine, "total")
        assert (stage.population, stage.population_inferred) == (None, False)
        assert table(await engine.execute("total"), REV) == rows((650,))

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_an_undefined_anchor_stage_stays_unpinned(self, backend: str) -> None:
        async with cycle_engine(backend) as eng:
            await eng.create_dataset_from_query(
                query={"dimensions": ["x.v", "y.v", "z.v"], "metrics": [{"metric": "count(x.*)", "alias": "n"}]},
                name="cyc")
            (stage,) = await stored_stages(eng, "cyc")
        assert (stage.population, stage.population_inferred) == (None, False)


class TestRefinement:
    async def test_a_refinement_never_moves_the_population(self, engine: SlayerQueryEngine) -> None:
        await engine.create_dataset_from_query(query=BY_REGION, name="by_region")
        refined = await engine.execute("by_region", refine={"dimensions": [STATUS]})
        assert (refined.population, refined.population_inferred) == ("customers", True)
        assert table(refined, R, STATUS, REV) == rows(*REFINED)

    async def test_legacy_unpinned_model_refined(self, engine: SlayerQueryEngine) -> None:
        await store_raw(engine, "legacy", BY_REGION)
        refined = await engine.execute("legacy", refine={"dimensions": [STATUS]})
        assert (refined.population, refined.population_inferred) == ("customers", True)
        assert table(refined, R, STATUS, REV) == rows(*REFINED)
        (stage,) = await stored_stages(engine, "legacy")
        assert stage.population is None
        unrefined = await engine.execute("legacy")
        assert table(unrefined, R, REV) == table(await run(engine, **BY_REGION), R, REV)

    async def test_a_stage_pinned_before_this_rule_stays_anchored(self, engine: SlayerQueryEngine) -> None:
        await store_raw(engine, "old_pin", {"population": "customers", **BY_REGION})
        resp = await engine.execute("old_pin")
        assert table(resp, R, REV) == rows(("EAST", 350), ("WEST", 300), ("NORTH", None))
        assert (resp.population, resp.population_inferred) == ("customers", False)


async def test_a_saved_inferred_stage_read_by_another_query_keeps_its_rows(engine) -> None:
    await engine.create_dataset_from_query(query=BY_REGION, name="by_region")
    direct = await run(engine, **BY_REGION)
    spliced = await run(engine, population="by_region", dimensions=["by_region.region", "by_region.orders__total_revenue"])
    assert table(direct, R, REV) == table(spliced, "by_region.region", "by_region.orders__total_revenue") == rows(
        ("EAST", 350), ("WEST", 300))


async def test_an_inferred_stage_keeps_its_column_names(engine) -> None:
    await engine.create_dataset_from_query(query=BY_REGION, name="by_region")
    saved = await engine.storage.get_dataset("by_region")
    assert saved is not None
    assert [f.name for f in saved.fields] == ["region", "orders__total_revenue"]
    inline = await engine.execute([
        SlayerQuery.model_validate({"name": "s1", **BY_REGION}),
        SlayerQuery.model_validate({"population": "s1", "dimensions": ["s1.region", "s1.orders__total_revenue"]}),
    ])
    assert table(inline, "s1.region", "s1.orders__total_revenue") == rows(("EAST", 350), ("WEST", 300))


@pytest.mark.parametrize(("query", "total"), [
    ({"metrics": [REV]}, 650), ({"metrics": [REV], "filter": "orders.status = 'completed'"}, 150),
])
async def test_a_saved_query_with_no_dimensions_is_read_by_another_query(engine, query: dict, total: int) -> None:
    await engine.create_dataset_from_query(query=query, name="tot")
    resp = await run(engine, population="tot", metrics=[{"metric": "sum(tot.orders__total_revenue)", "alias": "t"}])
    assert [r["t"] for r in resp.data] == [total]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_spliced_stage_reads_its_supply_with_its_dataset_variables(backend: str) -> None:
    # orders' own filter carries a Mode-A variable: the cells of the spliced stage read it bound, as the stage does.
    orders = orders_dataset(filters=["status = '{s}'"], query_variables={"s": "open"})
    async with seed_engine(backend, datasets=seed_datasets(orders=orders)) as eng:
        query = {"dimensions": [R], "metrics": [REV, "returns.total_returns"]}
        await eng.create_dataset_from_query(query=query, name="saved")
        direct = await run(eng, **query)
        spliced = await run(eng, population="saved", dimensions=["saved.region"],
                            metrics=[{"metric": "sum(saved.orders__total_revenue)", "alias": "rev"}])
    assert {r[R] for r in direct.data} == {r["saved.region"] for r in spliced.data} == {"EAST", "NORTH", "WEST"}
    assert {r["saved.region"]: r["rev"] for r in spliced.data}["WEST"] == 300


@pytest.mark.xfail(strict=True, reason="DEV-2392: a joined dataset's own Mode-A variables are never substituted")
@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_spliced_stage_reads_a_non_host_supply_with_its_dataset_variables(backend: str) -> None:
    returns = returns_dataset().model_copy(update={"filters": ["amount > {m}"], "query_variables": {"m": 15}})
    async with seed_engine(backend, datasets=[*seed_datasets()[:2], returns, *seed_datasets()[3:]]) as eng:
        await eng.create_dataset_from_query(query={"dimensions": [R], "metrics": [REV, "returns.total_returns"]},
                                            name="saved")
        spliced = await run(eng, population="saved", dimensions=["saved.region"],
                            metrics=[{"metric": "sum(saved.returns__total_returns)", "alias": "ret"}])
    assert {r["saved.region"]: r["ret"] for r in spliced.data} == {"EAST": 25, "WEST": None}
