"""Input that spells one concept both the pre-2.0 way and the Ossie way, in one object, is refused with a typed error
naming both.

The pre-2.0 spellings below are deliberate: keep this file out of the vocabulary respelling pass.

Spec: compatibility/ossie-vocabulary › An old and a new spelling together are refused.
"""

from __future__ import annotations

import copy
from typing import Any

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.models import SlayerDataset
from slayer.core.query import QueryRefinement, SlayerQuery
from slayer.core.vocabulary import SpellingConflictError
from tests._deprecated_input_helpers import errors_in, recorded
from tests._ossie_vocabulary_fixtures import shop_orders, vocab_engine

ORDERS = shop_orders()
REL = {"to": "customers", "join_pairs": [["customer_id", "id"]], "name": "buyer"}
QUERY: dict[str, Any] = {"population": "orders", "metrics": ["count(*)"]}


def plus(doc: dict[str, Any], **extra: Any) -> dict[str, Any]:
    out = copy.deepcopy(doc)
    out.update(extra)
    return out


def member(doc: dict[str, Any], *, section: str, **extra: Any) -> dict[str, Any]:
    out = copy.deepcopy(doc)
    out[section][0].update(extra)
    return out


#: id -> (input, the new spelling, the old spelling)
DATASET_CASES: dict[str, tuple[dict[str, Any], str, str]] = {
    "fields_columns": (plus(ORDERS, columns=ORDERS["fields"]), "fields", "columns"),
    "metrics_measures": (plus(ORDERS, measures=ORDERS["metrics"]), "metrics", "measures"),
    "relationships_joins": (plus(ORDERS, joins=ORDERS["relationships"]), "relationships", "joins"),
    "field_datatype_type": (member(ORDERS, section="fields", type="INT"), "datatype", "type"),
    "metric_datatype_type": (member(ORDERS, section="metrics", type="DOUBLE"), "datatype", "type"),
    "metric_expression_formula": (member(ORDERS, section="metrics", formula="sum(amount)"), "expression", "formula"),
    "aggregation_expression_formula": (
        plus(ORDERS, aggregations=[{"name": "d", "expression": "SUM({value})", "formula": "SUM({value})"}]),
        "expression", "formula"),
    "to_target_model": (member(ORDERS, section="relationships", target_model="customers"), "to", "target_model"),
    "nested_query_population_source_model": (
        {"name": "by_status", "data_source": "shop",
         "source_queries": [plus(QUERY, dimensions=["status"], source_model="orders")]},
        "population", "source_model"),
}
QUERY_CASES: dict[str, tuple[dict[str, Any], str, str]] = {
    "population_source_model": (plus(QUERY, source_model="orders"), "population", "source_model"),
    "metrics_measures": (plus(QUERY, measures=["count(*)"]), "metrics", "measures"),
    "filter_filters": (plus(QUERY, filter=["amount > 1"], filters=["amount > 1"]), "filter", "filters"),
    "metric_entry_expression_formula": (
        plus(QUERY, metrics=[{"expression": "count(*)", "formula": "count(*)", "name": "n"}]),
        "expression", "formula"),
    "field_ref_dataset_model": (plus(QUERY, dimensions=[{"name": "name", "dataset": "customers",
                                                         "model": "customers"}]), "dataset", "model"),
    "time_dimension_field_dimension": (plus(QUERY, time_dimensions=[
        {"field": "created_at", "dimension": "created_at", "granularity": "month"}]), "field", "dimension"),
    "time_dimension_field_column": (plus(QUERY, time_dimensions=[
        {"field": "created_at", "column": "created_at", "granularity": "month"}]), "field", "column"),
    "order_field_column": (plus(QUERY, order=[{"field": "status", "column": "status"}]), "field", "column"),
    "extension_metrics_measures": (plus(QUERY, extensions=[{
        "source_name": "orders", "metrics": [{"name": "a", "expression": "count(*)"}],
        "measures": [{"name": "b", "expression": "count(*)"}]}]), "metrics", "measures"),
    "extension_relationships_joins": (plus(QUERY, extensions=[{
        "source_name": "orders", "relationships": [REL], "joins": [REL]}]), "relationships", "joins"),
    "extension_to_target_model": (plus(QUERY, extensions=[{
        "source_name": "orders", "relationships": [dict(REL, target_model="customers")]}]), "to", "target_model"),
}
REFINEMENT_CASES: dict[str, tuple[dict[str, Any], str, str]] = {
    "metrics_measures": ({"metrics": ["count(*)"], "measures": ["count(*)"]}, "metrics", "measures"),
    "filter_filters": ({"filter": ["amount > 1"], "filters": ["amount > 1"]}, "filter", "filters"),
}


def assert_refused(cls: Any, raw: dict[str, Any], *, new: str, old: str) -> None:
    with recorded(), pytest.raises(Exception) as ei:
        cls.model_validate(raw)
    typed = [e for e in errors_in(ei.value) if isinstance(e, SpellingConflictError)]
    assert typed, ei.value
    message = str(typed[0])
    assert new in message
    assert old in message


@pytest.mark.parametrize("case", sorted(DATASET_CASES))
def test_a_dataset_with_both_spellings_is_refused(case: str) -> None:
    raw, new, old = DATASET_CASES[case]
    assert_refused(SlayerDataset, raw, new=new, old=old)


@pytest.mark.parametrize("case", sorted(QUERY_CASES))
def test_a_query_with_both_spellings_is_refused(case: str) -> None:
    raw, new, old = QUERY_CASES[case]
    assert_refused(SlayerQuery, raw, new=new, old=old)


@pytest.mark.parametrize("case", sorted(REFINEMENT_CASES))
def test_a_refinement_with_both_spellings_is_refused(case: str) -> None:
    raw, new, old = REFINEMENT_CASES[case]
    assert_refused(QueryRefinement, raw, new=new, old=old)


async def test_rest_refuses_both_query_keys_as_a_client_error() -> None:
    async with vocab_engine("sqlite") as engine:
        resp = TestClient(create_app(storage=engine.storage)).post("/query", json=plus(QUERY, source_model="orders"))
    assert resp.status_code in (400, 422)
    assert "population" in resp.text
    assert "source_model" in resp.text
