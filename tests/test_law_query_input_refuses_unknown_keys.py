"""Law: every query input object reachable from a query or refinement refuses a key it does not read with a typed
error, reads a camelCase key as its snake_case key, and validates assignments.

Spec: compatibility/ossie-vocabulary › Queries refuse unknown keys; Query objects validate assignments.
"""

from __future__ import annotations

import copy
from collections.abc import Iterator, Mapping
from typing import Any, get_args

import pytest
from pydantic import BaseModel, ValidationError

from slayer.core import vocabulary
from slayer.core.models import DatasetPart
from slayer.core.query import QueryRefinement, SlayerQuery
from tests._deprecated_input_helpers import deprecations, payload_of, recorded
from tests._query_input_fixtures import STRAY, assert_lists_keys, refusal

#: A minimal valid payload per reachable query input model; a reachable model missing here fails the law.
MINIMAL: dict[str, dict[str, Any]] = {
    "SlayerQuery": {"population": "orders", "metrics": ["count(orders.*)"]},
    "QueryRefinement": {},
    "QueryDimension": {"field": "orders.status"},
    "QueryMetric": {"metric": "count(orders.*)"},
    "TimeDimension": {"field": "orders.ordered_at", "granularity": "month"},
    "OrderItem": {"field": "orders.status"},
    "DatasetExtension": {"dataset": "orders"},
}

#: Every multi-word field of each model; a new one must be added here (and to ``VALUES``).
MULTI_WORD_FIELDS: dict[str, set[str]] = {
    "SlayerQuery": {"population_inferred", "time_dimensions", "main_time_dimension", "whole_periods_only",
                    "distinct_dimension_values", "to_many_handling"},
    "QueryRefinement": {"time_dimensions", "main_time_dimension", "whole_periods_only", "distinct_dimension_values",
                        "to_many_handling"},
    "QueryDimension": set(),
    "QueryMetric": set(),
    "TimeDimension": {"date_range"},
    "OrderItem": {"raw_expression"},
    "DatasetExtension": set(),
}

#: Each model's pre-2.0 key table.
PRE_2_0_TABLES: dict[str, Mapping[str, str]] = {
    "SlayerQuery": vocabulary.QUERY_KEYS,
    "QueryRefinement": vocabulary.REFINEMENT_KEYS,
    "QueryDimension": vocabulary.DIMENSION_EXPRESSION_KEYS,
    "QueryMetric": {"formula": "metric", "name": "alias", "type": "datatype"},
    "TimeDimension": vocabulary.TIME_DIMENSION_KEYS,
    "OrderItem": vocabulary.ORDER_ITEM_KEYS,
    "DatasetExtension": vocabulary.EXTENSION_KEYS,
}

#: Every multi-word pre-2.0 key, as model -> old -> new.
MULTI_WORD_PRE_2_0: dict[str, dict[str, str]] = {
    "SlayerQuery": {"source_model": "population"},
    "OrderItem": {"raw_formula": "raw_expression"},
    "DatasetExtension": {"source_name": "dataset"},
}

_TD = [{"field": "orders.ordered_at", "granularity": "month"}]
#: A valid value per multi-word key (canonical or pre-2.0).
VALUES: dict[str, Any] = {
    "population_inferred": True, "time_dimensions": _TD, "main_time_dimension": "orders.ordered_at",
    "whole_periods_only": True, "distinct_dimension_values": True, "to_many_handling": "error",
    "date_range": ["2024-01-01", "2024-06-30"], "raw_expression": "sum(orders.amount)",
    "source_model": "orders", "raw_formula": "sum(orders.amount)", "source_name": "orders",
}


def _models_in(annotation: Any) -> Iterator[type[BaseModel]]:
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        yield annotation
        return
    for arg in get_args(annotation):
        yield from _models_in(arg)


def reachable_models() -> dict[str, type[BaseModel]]:
    """Every Pydantic model reachable from the query and refinement field annotations, stopping at dataset-side ones."""
    seen: dict[str, type[BaseModel]] = {}
    stack: list[type[BaseModel]] = [SlayerQuery, QueryRefinement]
    while stack:
        model = stack.pop()
        if model.__name__ in seen or issubclass(model, DatasetPart):
            continue
        seen[model.__name__] = model
        for info in model.model_fields.values():
            stack.extend(_models_in(info.annotation))
    return seen


REACHABLE = reachable_models()


def model_of(name: str) -> type[BaseModel]:
    return REACHABLE[name]


def camel(key: str) -> str:
    head, *rest = key.split("_")
    return head + "".join(part.capitalize() for part in rest)


def minimal(name: str, *, without: str | None = None) -> dict[str, Any]:
    return {k: v for k, v in copy.deepcopy(MINIMAL[name]).items() if k != without}


def deprecation_payloads(caught: list) -> list[dict[str, Any]]:
    return [payload_of(w).model_dump(mode="json") for w in deprecations(caught)]


CAMEL_CASES = [(name, key) for name, keys in sorted(MULTI_WORD_FIELDS.items()) for key in sorted(keys)]
PRE_2_0_CASES = [(name, old, new) for name, table in sorted(MULTI_WORD_PRE_2_0.items()) for old, new in table.items()]


class TestCoverage:
    def test_the_minimal_payload_table_covers_every_reachable_model(self) -> None:
        assert set(REACHABLE) == set(MINIMAL)

    def test_the_multi_word_field_inventory_is_complete(self) -> None:
        actual = {name: {k for k in model.model_fields if "_" in k} for name, model in REACHABLE.items()}
        assert actual == MULTI_WORD_FIELDS

    def test_the_multi_word_pre_2_0_inventory_is_complete(self) -> None:
        actual = {name: {old: new for old, new in PRE_2_0_TABLES[name].items() if "_" in old}
                  for name in REACHABLE}
        assert {n: t for n, t in actual.items() if t} == MULTI_WORD_PRE_2_0

    def test_every_multi_word_key_has_a_value(self) -> None:
        keys = {k for ks in MULTI_WORD_FIELDS.values() for k in ks} | {
            old for table in MULTI_WORD_PRE_2_0.values() for old in table}
        assert keys <= set(VALUES)


@pytest.mark.parametrize("name", sorted(MINIMAL))
class TestEachModel:
    def test_the_minimal_payload_validates(self, name: str) -> None:
        model_of(name).model_validate(minimal(name))

    def test_extra_keys_are_forbidden(self, name: str) -> None:
        assert model_of(name).model_config.get("extra") == "forbid"

    def test_assignments_are_validated(self, name: str) -> None:
        assert model_of(name).model_config.get("validate_assignment") is True

    def test_a_stray_key_is_refused_naming_it_and_listing_the_advertised_keys(self, name: str) -> None:
        model = model_of(name)
        payload = {**minimal(name), STRAY: 1}
        with pytest.raises((ValidationError, ValueError)) as info:
            model.model_validate(payload)
        err, _ = refusal(info.value)
        assert err.key == STRAY
        assert STRAY in str(err)
        assert_lists_keys(str(err), model)


@pytest.mark.parametrize(("name", "key"), CAMEL_CASES, ids=[f"{n}.{k}" for n, k in CAMEL_CASES])
def test_a_camel_case_field_reads_as_its_snake_case_key(name: str, key: str) -> None:
    model = model_of(name)
    with recorded() as caught:
        healed = model.model_validate({**minimal(name, without=key), camel(key): copy.deepcopy(VALUES[key])})
    assert key in healed.model_fields_set
    assert deprecation_payloads(caught) == []
    plain = model.model_validate({**minimal(name, without=key), key: copy.deepcopy(VALUES[key])})
    assert healed.model_dump(mode="json") == plain.model_dump(mode="json")


@pytest.mark.parametrize(("name", "old", "new"), PRE_2_0_CASES, ids=[f"{n}.{o}" for n, o, _ in PRE_2_0_CASES])
def test_a_camel_case_pre_2_0_key_converts_on_with_its_deprecation(name: str, old: str, new: str) -> None:
    model = model_of(name)
    with recorded() as healed_caught:
        healed = model.model_validate({**minimal(name, without=new), camel(old): copy.deepcopy(VALUES[old])})
    with recorded() as plain_caught:
        plain = model.model_validate({**minimal(name, without=new), old: copy.deepcopy(VALUES[old])})
    assert new in healed.model_fields_set
    assert healed.model_dump(mode="json") == plain.model_dump(mode="json")
    assert deprecation_payloads(healed_caught) == deprecation_payloads(plain_caught) != []
