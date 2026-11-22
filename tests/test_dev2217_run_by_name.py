"""Saved queries run by name, run variables, explain, dry runs and the cache under the strict binding.

Spec: queries/ossie-l3-binding › Saved queries run by name under the strict binding; Variables bind before the filter
is judged; Explain and dry runs judge first.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest

from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2217_fixtures import DS, fixture_engine, rows, strict, strict_error

BY_REGION = {"dimensions": ["customers.region"], "metrics": ["orders.total_revenue"]}
LIMITED = {"metrics": ["orders.total_revenue"], "limit": 10}


@pytest.fixture
async def engine(tmp_path: Path) -> AsyncGenerator[SlayerQueryEngine]:
    async with fixture_engine(tmp_path, "retail") as engine:
        yield engine


async def _save(engine: SlayerQueryEngine, name: str, *stages: dict[str, Any]) -> None:
    await engine.storage.save_dataset(SlayerDataset(
        name=name, data_source=DS, source_queries=[SlayerQuery.model_validate(stage) for stage in stages]))


async def test_a_saved_ossie_shaped_query_runs_strictly(engine: SlayerQueryEngine) -> None:
    """A saved Ossie-shaped query runs strictly."""
    await _save(engine, "by_region", BY_REGION)
    alone = await strict(engine, "by_region")
    assert alone.columns == ["region", "total_revenue"]
    assert rows(alone) == [("EAST", 350.0), ("WEST", 300.0)]
    refined = await strict(engine, "by_region", refine={"metrics": ["returns.total_returns"]})
    assert refined.columns == ["region", "total_revenue", "total_returns"]
    assert rows(refined) == [("EAST", 350.0, 25.0), ("NORTH", None, 10.0), ("WEST", 300.0, None)]


async def test_a_saved_query_with_a_pinned_inferred_population(engine: SlayerQueryEngine) -> None:
    await _save(engine, "pinned", {"population": "customers", "population_inferred": True, **BY_REGION})
    assert (await strict(engine, "pinned")).columns == ["region", "total_revenue"]


async def test_a_stored_one_condition_filter_is_the_filter(engine: SlayerQueryEngine) -> None:
    await _save(engine, "east", {"metrics": ["orders.total_revenue"], "filter": "customers.region = 'EAST'"})
    assert (await strict(engine, "east")).data == [{"total_revenue": 350}]


async def test_a_saved_query_with_an_explicit_population_is_refused(engine: SlayerQueryEngine) -> None:
    """A saved query with an explicit population is refused."""
    await _save(engine, "explicit", {"population": "customers", **BY_REGION})
    await strict_error(engine, "explicit", "E_INVALID_QUERY")
    assert (await engine.execute("explicit")).data


@pytest.mark.parametrize(("stages", "refine"), [
    pytest.param(({"name": "s", **BY_REGION}, {"population": "s", "metrics": ["sum(s.total_revenue)"]}), None,
                 id="multi-stage"),
    pytest.param((LIMITED,), None, id="stored-limit"),
    pytest.param((BY_REGION,), {"limit": 1}, id="refined-limit"),
    pytest.param((BY_REGION,), {"order": [{"field": "customers.region"}]}, id="refined-order"),
])
async def test_saved_parts_outside_ossies_shape_are_refused(engine: SlayerQueryEngine,
                                                            stages: tuple[dict[str, Any], ...],
                                                            refine: dict[str, Any] | None) -> None:
    await _save(engine, "saved", *stages)
    await strict_error(engine, "saved", "E_INVALID_QUERY", refine=refine)


async def test_a_placeholder_bound_by_the_runs_variables(engine: SlayerQueryEngine) -> None:
    """A placeholder bound by the run's variables."""
    response = await strict(engine, {"metrics": ["orders.total_revenue"], "filter": "customers.region = {r}"},
                            variables={"r": "EAST"})
    assert response.data == [{"total_revenue": 350}]


async def test_a_query_level_variables_part_is_refused(engine: SlayerQueryEngine) -> None:
    await strict_error(engine, {"metrics": ["orders.total_revenue"], "filter": "customers.region = {r}",
                                "variables": {"r": "EAST"}}, "E_INVALID_QUERY")


@pytest.mark.parametrize("mode", ["explain", "dry_run", "cache"])
async def test_a_refused_query_is_refused_by_explain(engine: SlayerQueryEngine, mode: str) -> None:
    """A refused query is refused by explain (and by a dry run, and with the cache on)."""
    await strict_error(engine, LIMITED, "E_INVALID_QUERY", **{mode: True})
    await strict_error(engine, {"dimensions": ["customers.region"], "metrics": ["orders.total_revenue",
                                                                                "tickets.ticket_count"]},
                       "E_PARTIAL_DIMENSION_REACHABILITY", **{mode: True})


async def test_explain_exposes_the_admitted_lowering(engine: SlayerQueryEngine) -> None:
    response = await strict(engine, BY_REGION, explain=True)
    assert response.sql


async def test_a_saved_query_is_judged_by_a_dry_run(engine: SlayerQueryEngine) -> None:
    await _save(engine, "explicit", {"population": "customers", **BY_REGION})
    await strict_error(engine, "explicit", "E_INVALID_QUERY", dry_run=True)


async def test_the_cache_answers_with_output_names(engine: SlayerQueryEngine) -> None:
    first = await strict(engine, BY_REGION, cache=True)
    second = await strict(engine, BY_REGION, cache=True)
    native = await engine.execute(BY_REGION, cache=True)
    assert first.columns == second.columns == ["region", "total_revenue"]
    assert native.columns == ["customers.region", "orders.total_revenue"]
