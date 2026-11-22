"""Law: every model reachable from a dataset or datasource is frozen or validates assignment atomically."""

from __future__ import annotations

import typing
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from slayer.core.assignment import AtomicAssignment
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.format import NumberFormat
from slayer.core.granularity import CustomGranularity
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    DatasetField,
    DatasetMetric,
    DatasetRelationship,
    DatasourceConfig,
    ExtensionNote,
    SlayerDataset,
    SourceDatasetOrigin,
)

from tests._validation_refusals import state

ROOTS: tuple[type[BaseModel], ...] = (SlayerDataset, DatasourceConfig)
#: Query-side models answer to the query-object law instead.
QUERY_MODULE = "slayer.core.query"


def _models_in(annotation: Any) -> list[type[BaseModel]]:
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return [annotation]
    out: list[type[BaseModel]] = []
    for arg in typing.get_args(annotation):
        out += _models_in(arg)
    return out


def reachable_models() -> set[type[BaseModel]]:
    """Every model a root's field annotations reach, transitively, short of query-side models."""
    seen: set[type[BaseModel]] = set()
    todo = list(ROOTS)
    while todo:
        cls = todo.pop()
        if cls in seen or cls.__module__ == QUERY_MODULE:
            continue
        seen.add(cls)
        for info in cls.model_fields.values():
            todo += _models_in(info.annotation)
    return seen


def _frozen(cls: type[BaseModel]) -> bool:
    return bool(cls.model_config.get("frozen"))


#: A minimal valid payload per non-frozen reachable model, and an attribute to assign a wrong-typed value to.
MINIMAL: dict[type[BaseModel], tuple[dict[str, Any], str]] = {
    SlayerDataset: ({"name": "d", "sql_table": "t", "data_source": "ds"}, "hidden"),
    DatasetField: ({"name": "f"}, "label"),
    DatasetMetric: ({"name": "m", "expression": "count(*)"}, "label"),
    DatasetRelationship: ({"to": "c", "from_columns": ["a"], "to_columns": ["b"]}, "description"),
    Aggregation: ({"name": "sum"}, "description"),
    AggregationParam: ({"name": "p", "sql": "x"}, "sql"),
    SourceDatasetOrigin: ({"name": "s"}, "data_source"),
    ExtensionNote: ({"kind": "k", "entity": "e", "message": "m"}, "message"),
    DatasourceConfig: ({"name": "ds"}, "port"),
    NumberFormat: ({}, "precision"),
    ExpressionObject: ({"sql": "x"}, "sql"),
    DialectEntry: ({"dialect": "duckdb", "expression": "x"}, "expression"),
}
NON_FROZEN = sorted((c for c in reachable_models() if not _frozen(c)), key=lambda c: c.__name__)


def test_the_walk_reaches_every_dataset_side_model() -> None:
    reached = reachable_models()
    assert {*MINIMAL, CustomGranularity} <= reached
    assert not any(c.__module__ == QUERY_MODULE for c in reached)


def test_every_non_frozen_reachable_model_has_a_minimal_payload() -> None:
    assert set(NON_FROZEN) <= set(MINIMAL), sorted(c.__name__ for c in set(NON_FROZEN) - set(MINIMAL))


@pytest.mark.parametrize("cls", NON_FROZEN, ids=lambda c: c.__name__)
def test_a_reachable_model_validates_assignment_atomically(cls: type[BaseModel]) -> None:
    assert cls.model_config.get("validate_assignment") is True
    assert issubclass(cls, AtomicAssignment)


@pytest.mark.parametrize("cls", NON_FROZEN, ids=lambda c: c.__name__)
def test_a_wrong_typed_assignment_raises_and_changes_nothing(cls: type[BaseModel]) -> None:
    payload, attribute = MINIMAL[cls]
    obj = cls.model_validate(payload)
    before = state(obj)
    with pytest.raises(ValidationError):
        setattr(obj, attribute, object())
    assert state(obj) == before


class _Sorted(AtomicAssignment, BaseModel):
    """An early validator sorts ``items`` in place; a later one may refuse the sorted state."""

    model_config = ConfigDict(validate_assignment=True)
    items: list[int] = Field(default_factory=list)
    descending: bool = False

    @model_validator(mode="after")
    def _order(self) -> "_Sorted":
        self.items.sort(reverse=self.descending)
        return self

    @model_validator(mode="after")
    def _no_descending_duplicates(self) -> "_Sorted":
        if self.descending and len(set(self.items)) < len(self.items):
            raise ValueError("descending items must be distinct")
        return self


class TestCandidateCommit:
    def test_a_refused_assignment_keeps_a_list_an_earlier_validator_edited(self) -> None:
        obj = _Sorted(items=[1, 1, 2])
        kept = obj.items
        with pytest.raises(ValidationError):
            obj.descending = True
        assert obj.descending is False
        assert obj.items is kept
        assert kept == [1, 1, 2]

    def test_a_refused_assign_keeps_a_list_an_earlier_validator_edited(self) -> None:
        obj = _Sorted(items=[1, 1, 2])
        kept = obj.items
        with pytest.raises(ValidationError):
            obj.assign(descending=True)
        assert obj.descending is False
        assert obj.items is kept
        assert kept == [1, 1, 2]

    def test_an_accepted_assignment_commits_the_validated_state(self) -> None:
        obj = _Sorted(items=[1, 2])
        obj.descending = True
        assert obj.items == [2, 1]
        assert "descending" in obj.model_fields_set

    def test_an_accepted_assign_commits_every_change(self) -> None:
        obj = _Sorted(items=[1, 2])
        obj.assign(items=[3, 1, 2], descending=True)
        assert obj.items == [3, 2, 1]
        assert {"items", "descending"} <= obj.model_fields_set
