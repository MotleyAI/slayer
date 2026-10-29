"""A list is refused as a group key but projected once per record by a raw-rows query
(spec: models/list-columns)."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.errors import ListTypeError, SlayerError
from tests._dev2120_fixtures import CONTRACTORS
from tests._dev2137_fixtures import ASSIGNMENTS, list_engine, query


def _lists(rows: list[dict[str, Any]], *, key: str, value: str) -> dict[Any, Any]:
    return {r[key]: None if r[value] is None else list(r[value]) for r in rows}


SKILLS = {c["id"]: c["skills"] for c in CONTRACTORS}


class TestRawRows:
    async def test_list_projected_per_record(self) -> None:
        async with list_engine() as engine:
            resp = await engine.execute(query(dimensions=["id", "skills"], distinct_dimension_values=False))
        assert len(resp.data) == len(CONTRACTORS)
        assert _lists(resp.data, key="contractors.id", value="contractors.skills") == SKILLS

    async def test_list_valued_expression_projected_per_record(self) -> None:
        async with list_engine() as engine:
            resp = await engine.execute(query(dimensions=["id", {"name": "s", "expression": "skills"}],
                                              distinct_dimension_values=False))
        assert _lists(resp.data, key="contractors.id", value="contractors.s") == SKILLS

    async def test_identical_keyless_rows_stay_two_rows(self) -> None:
        async with list_engine() as engine:
            resp = await engine.execute({"source_model": "twins", "dimensions": ["name", "skills"],
                                         "distinct_dimension_values": False})
        assert [list(r["twins.skills"]) for r in resp.data] == [["go"], ["go"]]

    async def test_list_through_a_to_one_join(self) -> None:
        async with list_engine() as engine:
            resp = await engine.execute({"source_model": "assignments", "dimensions": ["id", "contractors.skills"],
                                         "distinct_dimension_values": False})
        assert _lists(resp.data, key="assignments.id", value="assignments.contractors.skills") == {
            a: SKILLS[c] for a, c in ASSIGNMENTS}

    @pytest.mark.parametrize("distinct", [False, True])
    async def test_ordering_by_a_list_is_refused(self, distinct: bool) -> None:
        async with list_engine() as engine:
            with pytest.raises(ListTypeError, match="list"):
                await engine.execute(query(dimensions=["id"], distinct_dimension_values=distinct,
                                           order=[{"column": "skills", "direction": "asc"}]))


class TestGrouping:
    @pytest.mark.parametrize("body", [
        {"dimensions": ["skills"]},
        {"dimensions": ["skills"], "measures": ["count(*)"]},
        {"dimensions": [{"name": "s", "expression": "skills"}]},
        {"dimensions": [{"name": "s", "expression": "skills"}], "measures": ["count(*)"]},
    ])
    async def test_list_as_a_group_key_names_its_elements(self, body: dict[str, Any]) -> None:
        async with list_engine() as engine:
            with pytest.raises(ListTypeError, match=r"skills\.value"):
                await engine.execute(query(**body))

    async def test_computed_dimension_checks_keep_their_precedence(self) -> None:
        """A raw-rows computed dimension holding an aggregation is refused for that, before any list rule."""
        async with list_engine() as engine:
            with pytest.raises(SlayerError, match="distinct_dimension_values") as caught:
                await engine.execute(query(
                    dimensions=["id", {"name": "s", "expression": "iif(count(*) > 1, skills, required)"}],
                    distinct_dimension_values=False))
        assert not isinstance(caught.value, ListTypeError)
