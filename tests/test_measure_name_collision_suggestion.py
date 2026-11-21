"""A metric name colliding with a source field suggests a free name."""

from __future__ import annotations

import re
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import MetricNameCollidesWithFieldError
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.compile.projection import free_metric_name

from tests._engine_helpers import _engine_generate

_COLLIDING = {"metric": "sum(customers.credit)", "alias": "credit"}


def _customers(*extra_fields: str) -> SlayerDataset:
    return SlayerDataset(
        name="customers", sql_table="customers", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="name", datatype=DataType.STRING),
            DatasetField(name="credit", datatype=DataType.INTEGER),
            *(DatasetField(name=c, datatype=DataType.INTEGER) for c in extra_fields),
        ],
    )


async def _suggestion(metrics: list[Any], *extra_fields: str) -> str:
    query = SlayerQuery.model_validate({"population": "customers", "metrics": metrics})
    model = _customers(*extra_fields)
    with pytest.raises(MetricNameCollidesWithFieldError) as ei:
        await _engine_generate(query=query, dataset=model, dialect="duckdb", validate=False)
    exc = ei.value
    assert exc.suggestion
    assert str(exc).splitlines()[-1] == f"  suggestion: {exc.suggestion}"
    return exc.suggestion


def _names(suggestion: str, name: str) -> bool:
    return re.search(rf"\b{re.escape(name)}\b", suggestion) is not None


async def test_suggests_the_derived_name() -> None:
    assert _names(await _suggestion([_COLLIDING]), "credit_sum")


async def test_derived_name_taken_by_an_explicit_name() -> None:
    assert _names(await _suggestion([_COLLIDING, {"metric": "count(customers.*)", "alias": "credit_sum"}]), "credit_2")


@pytest.mark.parametrize("unnamed_first", [False, True])
async def test_derived_name_taken_by_another_measures_derived_name(unnamed_first: bool) -> None:
    measures: list[Any] = ["sum(customers.credit)", _COLLIDING] if unnamed_first else [_COLLIDING, "sum(customers.credit)"]
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
    assert free_metric_name(declared="credit", canonical=canonical, taken=frozenset(taken)) == expected


def test_suggestion_kwarg_is_optional_and_rendered() -> None:
    assert MetricNameCollidesWithFieldError(name="credit", dataset="customers").suggestion is None
    exc = MetricNameCollidesWithFieldError(name="credit", dataset="customers", suggestion="rename it to 'credit_sum'")
    assert exc.suggestion == "rename it to 'credit_sum'"
    assert str(exc).splitlines()[-1] == "  suggestion: rename it to 'credit_sum'"
