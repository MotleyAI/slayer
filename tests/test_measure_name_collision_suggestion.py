"""A measure name colliding with a source column suggests a free name."""

from __future__ import annotations

import re
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import MeasureNameCollidesWithColumnError
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.compile.projection import free_measure_name

from tests._engine_helpers import _engine_generate

_COLLIDING = {"formula": "sum(credit)", "name": "credit"}


def _customers(*extra_columns: str) -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="credit", type=DataType.INT),
            *(Column(name=c, type=DataType.INT) for c in extra_columns),
        ],
    )


async def _suggestion(measures: list[Any], *extra_columns: str) -> str:
    query = SlayerQuery.model_validate({"source_model": "customers", "measures": measures})
    with pytest.raises(MeasureNameCollidesWithColumnError) as ei:
        await _engine_generate(query=query, model=_customers(*extra_columns), dialect="duckdb", validate=False)
    exc = ei.value
    assert exc.suggestion
    assert str(exc).splitlines()[-1] == f"  suggestion: {exc.suggestion}"
    return exc.suggestion


def _names(suggestion: str, name: str) -> bool:
    return re.search(rf"\b{re.escape(name)}\b", suggestion) is not None


async def test_suggests_the_derived_name() -> None:
    assert _names(await _suggestion([_COLLIDING]), "credit_sum")


async def test_derived_name_taken_by_an_explicit_name() -> None:
    assert _names(await _suggestion([_COLLIDING, {"formula": "count(*)", "name": "credit_sum"}]), "credit_2")


@pytest.mark.parametrize("unnamed_first", [False, True])
async def test_derived_name_taken_by_another_measures_derived_name(unnamed_first: bool) -> None:
    measures: list[Any] = ["sum(credit)", _COLLIDING] if unnamed_first else [_COLLIDING, "sum(credit)"]
    assert _names(await _suggestion(measures), "credit_2")


async def test_candidates_skip_source_columns() -> None:
    assert _names(await _suggestion([_COLLIDING], "credit_sum", "credit_2"), "credit_3")


@pytest.mark.parametrize(("canonical", "taken", "expected"), [
    pytest.param("credit_sum", set(), "credit_sum", id="derived-free"),
    pytest.param(None, set(), "credit_2", id="no-derived-name"),
    pytest.param("credit_sum", {"credit_sum"}, "credit_2", id="derived-taken"),
    pytest.param("credit_sum", {"credit_sum", "credit_2", "credit_3"}, "credit_4", id="numbered-taken"),
    pytest.param(None, {"credit_2"}, "credit_3", id="hidden-name-taken"),
])
def test_free_measure_name(canonical: str | None, taken: set[str], expected: str) -> None:
    assert free_measure_name(declared="credit", canonical=canonical, taken=frozenset(taken)) == expected


def test_suggestion_kwarg_is_optional_and_rendered() -> None:
    assert MeasureNameCollidesWithColumnError(name="credit", model="customers").suggestion is None
    exc = MeasureNameCollidesWithColumnError(name="credit", model="customers", suggestion="rename it to 'credit_sum'")
    assert exc.suggestion == "rename it to 'credit_sum'"
    assert str(exc).splitlines()[-1] == "  suggestion: rename it to 'credit_sum'"
