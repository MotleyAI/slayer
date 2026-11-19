"""A query's ``filter`` is one condition string or a list of them; it is dumped as written, and a refinement appends.

Spec: compatibility/ossie-vocabulary › A query filter is a string or a list.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from slayer.api.server import QueryRequest
from slayer.core.query import QueryRefinement, SlayerQuery, refine_query
from tests._deprecated_input_helpers import deprecations, recorded
from tests._engine_helpers import owned_query_engine
from tests._ossie_shapes_fixtures import execute, shop_engine_store, sorted_rows

BASE: dict[str, Any] = {"population": "orders", "dimensions": ["orders.status"], "metrics": ["count(orders.*)"]}


def query(**extra: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({**BASE, **extra})


def merged_filter(*, saved: Any, refined: Any) -> Any:
    merged = refine_query(saved=query(filter=saved), refinement=QueryRefinement.model_validate({"filter": refined}))
    return merged.model_dump(mode="json").get("filter")


class TestDump:
    def test_a_string_dumps_as_that_string(self) -> None:
        assert query(filter="amount > 15").model_dump(mode="json")["filter"] == "amount > 15"

    def test_a_list_dumps_as_that_list(self) -> None:
        assert query(filter=["amount > 15"]).model_dump(mode="json")["filter"] == ["amount > 15"]
        assert query(filter=["a > 1", "b < 2"]).model_dump(mode="json")["filter"] == ["a > 1", "b < 2"]

    @pytest.mark.parametrize("empty", [None, []], ids=["null", "empty_list"])
    def test_an_empty_filter_is_omitted(self, empty: Any) -> None:
        assert "filter" not in query(filter=empty).model_dump(mode="json")

    def test_an_omitted_filter_is_omitted(self) -> None:
        assert "filter" not in query().model_dump(mode="json")

    def test_a_string_round_trips(self) -> None:
        dumped = query(filter="amount > 15").model_dump(mode="json")
        with recorded() as caught:
            again = SlayerQuery.model_validate(dumped)
        assert again.model_dump(mode="json") == dumped
        assert deprecations(caught) == []

    def test_a_refinement_string_dumps_as_written(self) -> None:
        refinement = QueryRefinement.model_validate({"filter": "amount > 15"})
        assert refinement.model_dump(mode="json", exclude_none=True)["filter"] == "amount > 15"

    def test_a_rest_request_takes_a_string(self) -> None:
        request = QueryRequest.model_validate({**BASE, "filter": "orders.amount > 15"})
        assert request.filter == "amount > 15"


class TestRefinement:
    def test_a_refinement_appends_to_a_string(self) -> None:
        assert merged_filter(saved="a > 1", refined="b < 2") == ["a > 1", "b < 2"]

    def test_a_refinement_appends_a_list(self) -> None:
        assert merged_filter(saved=["a > 1"], refined=["b < 2", "c = 3"]) == ["a > 1", "b < 2", "c = 3"]

    def test_a_refinement_adding_nothing_keeps_the_saved_string(self) -> None:
        assert merged_filter(saved="a > 1", refined="a > 1") == "a > 1"

    def test_a_refinement_adding_nothing_keeps_the_saved_list(self) -> None:
        assert merged_filter(saved=["a > 1"], refined=["a > 1"]) == ["a > 1"]

    def test_a_refinement_onto_no_filter_dumps_as_written(self) -> None:
        assert merged_filter(saved=None, refined="b < 2") == "b < 2"


class TestExecution:
    async def test_a_string_filter_returns_the_list_forms_rows(self, tmp_path: Path) -> None:
        storage = await shop_engine_store(tmp_path)
        as_string = await execute(storage, {**BASE, "filter": "orders.amount > 15"})
        as_list = await execute(storage, {**BASE, "filter": ["orders.amount > 15"]})
        assert sorted_rows(as_string) == sorted_rows(as_list)
        assert sorted_rows(as_string) != sorted_rows(await execute(storage, BASE))

    async def test_an_empty_filter_means_none(self, tmp_path: Path) -> None:
        storage = await shop_engine_store(tmp_path)
        omitted = sorted_rows(await execute(storage, BASE))
        assert sorted_rows(await execute(storage, {**BASE, "filter": None})) == omitted
        assert sorted_rows(await execute(storage, {**BASE, "filter": []})) == omitted

    async def test_a_saved_string_filter_and_a_refinement_both_apply(self, tmp_path: Path) -> None:
        storage = await shop_engine_store(tmp_path)
        both = await execute(storage, {**BASE, "filter": ["orders.amount > 15", "orders.status = 'ok'"]})
        async with owned_query_engine(storage=storage) as engine:
            await engine.create_dataset_from_query(query={**BASE, "filter": "orders.amount > 15"}, name="by_status")
            refined = await engine.execute("by_status", refine={"filter": "orders.status = 'ok'"})
        assert sorted_rows(refined) == sorted_rows(both)
        assert sorted_rows(refined) != sorted_rows(await execute(storage, {**BASE, "filter": "orders.amount > 15"}))
