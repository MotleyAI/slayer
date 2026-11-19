"""A list's element type survives stages, query-backed models and re-rooted producers (design D1)."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.models import SlayerDataset
from tests._dev2137_fixtures import as_list, list_engine, metric, num
from slayer.engine.query_engine import SlayerQueryEngine


@pytest.fixture
async def engine():
    async with list_engine() as e:
        yield e


def _typed(dataset: SlayerDataset, name: str) -> tuple[DataType, Any]:
    col = dataset.get_field(name)
    assert col is not None, [c.name for c in dataset.fields]
    return col.datatype, col.element_type


def _cell(row: dict[str, Any], suffix: str) -> Any:
    (value,) = [v for k, v in row.items() if k.endswith(f".{suffix}")]
    return value


TEAM_SETS = {"population": "contractors", "dimensions": ["team"],
             "metrics": [metric("collect(skills.value)", "team_skills"), metric("collect(rate)", "rates")]}


class TestQueryBackedModels:
    async def test_collect_columns_are_typed_lists(self, engine: SlayerQueryEngine) -> None:
        model = await engine.create_dataset_from_query(query=TEAM_SETS, name="team_sets")
        assert _typed(model, "team_skills") == (DataType.ARRAY, DataType.STRING)
        assert _typed(model, "rates") == (DataType.ARRAY, DataType.INTEGER)

    async def test_count_of_a_list_is_an_integer(self, engine: SlayerQueryEngine) -> None:
        model = await engine.create_dataset_from_query(query={
            "population": "contractors", "dimensions": ["team"], "metrics": [metric("count(skills)", "n")]},
            name="list_counts")
        assert _typed(model, "n") == (DataType.INTEGER, None)

    async def test_element_access_on_a_stored_query_backed_model(self, engine: SlayerQueryEngine) -> None:
        await engine.create_dataset_from_query(query=TEAM_SETS, name="team_sets")
        resp = await engine.execute({"population": "team_sets", "dimensions": ["team_skills.value"],
                                     "metrics": [metric("count(team_skills.*)", "n")]})
        assert {_cell(r, "team_skills.value"): num(_cell(r, "n")) for r in resp.data} == {"go": 2, "sql": 1}

    async def test_through_two_stages(self, engine: SlayerQueryEngine) -> None:
        model = await engine.create_dataset_from_query(query=[
            {**TEAM_SETS, "name": "s"},
            {"population": "s", "metrics": [metric("collect(team_skills.value)", "all_skills"),
                                               metric("collect(rates.value)", "all_rates")]},
        ], name="all_sets")
        assert _typed(model, "all_skills") == (DataType.ARRAY, DataType.STRING)
        assert _typed(model, "all_rates") == (DataType.ARRAY, DataType.INTEGER)


class TestStages:
    async def test_numeric_elements_survive_a_stage(self, engine) -> None:
        stage = {**TEAM_SETS, "name": "s"}
        kept = await engine.execute([stage, {"population": "s", "dimensions": ["team"],
                                             "filter": ["contains(rates, '10')"]}])
        assert {_cell(r, "team") for r in kept.data} == {"A"}
        above = await engine.execute([stage, {"population": "s", "dimensions": ["team"],
                                              "filter": ["rates.value > '45'"]}])
        assert {_cell(r, "team") for r in above.data} == {"B"}

    async def test_list_values_survive_a_stage(self, engine) -> None:
        resp = await engine.execute([{**TEAM_SETS, "name": "s"},
                                     {"population": "s", "dimensions": ["team"],
                                      "metrics": [metric("collect(team_skills.value)", "again")]}])
        assert {_cell(r, "team"): as_list(_cell(r, "again")) for r in resp.data} == {
            "A": ["go", "sql"], "B": ["go"]}


class TestReRootedProducers:
    async def test_collect_across_a_to_one_join(self, engine: SlayerQueryEngine) -> None:
        query = {"population": "assignments", "dimensions": ["contractors.id"],
                 "metrics": [metric("collect(contractors.skills.value)", "s")]}
        resp = await engine.execute(query)
        got = {None if (c := r["assignments.contractors.id"]) is None else int(c): as_list(r["assignments.s"])
               for r in resp.data}
        assert got == {1: ["go", "sql"], 2: ["sql"], 3: [], 5: ["go"], None: []}
        model = await engine.create_dataset_from_query(query=query, name="per_contractor")
        assert _typed(model, "s") == (DataType.ARRAY, DataType.STRING)
