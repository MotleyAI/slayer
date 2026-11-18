"""The 1.x query-entry classes ``ColumnRef`` and ``ComputedDimension`` resolve to ``QueryDimension`` and still build
entries from their 1.x keywords.

Spec: compatibility/ossie-vocabulary › Python names follow the vocabulary (An old entry class still builds an entry).

The 1.x names below are deliberate: keep this file out of any respelling pass.
"""

from __future__ import annotations

import importlib
from typing import Any

import pytest

from slayer.core.query import QueryDimension, SlayerQuery
from tests._deprecated_input_helpers import recorded


def future(caught: list[Any]) -> list[Any]:
    return [w for w in caught if issubclass(w.category, FutureWarning)]


def old_class(name: str) -> Any:
    return getattr(importlib.import_module("slayer.core.query"), name)


@pytest.mark.parametrize("name", ["ColumnRef", "ComputedDimension"])
def test_an_old_entry_class_is_query_dimension(name: str) -> None:
    with recorded() as caught:
        resolved = old_class(name)
    assert issubclass(resolved, QueryDimension)
    [w] = future(caught)
    assert "QueryDimension" in str(w.message)
    assert "SLayer 3.0" in str(w.message)


def test_old_entry_classes_still_build_entries() -> None:
    with recorded() as caught:
        column_ref, computed = old_class("ColumnRef"), old_class("ComputedDimension")
        dims = [column_ref(name="region", model="customers"), computed(expression="upper(status)", name="s")]
        query = SlayerQuery(population="orders", metrics=["count(*)"], dimensions=dims)
    assert query.model_dump(mode="json")["dimensions"] == ["customers.region", {"field": "upper(status)", "alias": "s"}]
    named = [str(w.message) for w in future(caught)]
    assert len([m for m in named if "ColumnRef" in m]) == 1
    assert len([m for m in named if "ComputedDimension" in m]) == 1

