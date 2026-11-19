"""Queries are written in Ossie's vocabulary: ``population``, ``metrics``, ``filter``, ``metric`` / ``alias``
entries, ``field`` on dimensions, time dimensions and order items, and extension ``dataset`` / ``metrics`` /
``relationships``.

Spec: compatibility/ossie-vocabulary › Queries are written in Ossie's vocabulary.

The pre-2.0 spellings below are deliberate: keep this file out of the vocabulary respelling pass.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import QueryRequest, create_app
from slayer.core.models import SlayerDataset
from slayer.core.query import QueryDimension, QueryRefinement, SlayerQuery
from tests._deprecated_input_helpers import deprecations, recorded
from tests._ossie_vocabulary_fixtures import (
    BACKENDS,
    SAVED_QUERY,
    fresh,
    normal,
    refine_vocab_engine,
    shop_customers,
    shop_orders,
    shop_store,
    stored_document,
)

PRE_2_0_QUERY_KEYS = {"source_model", "measures", "filters", "formula", "column", "dimension", "joins",
                      "target_model", "model"}

EXTENSION: dict[str, Any] = {
    "dataset": "orders",
    "metrics": [{"name": "aov", "expression": "sum(amount) / count(*)"}],
    "relationships": [{"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"], "name": "buyer"}],
}
TWO_STAGES: list[dict[str, Any]] = [
    {"name": "per_customer", "population": "orders", "dimensions": ["orders.customer_id", "buyer.name"],
     "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}],
     "metrics": [{"metric": "sum(orders.amount)", "alias": "total"}, "orders.aov"],
     "filter": ["orders.amount > 5"], "order": [{"field": "total", "direction": "desc"}],
     "extensions": [EXTENSION]},
    {"population": "per_customer", "metrics": [{"metric": "avg(per_customer.total)", "alias": "avg_total"}],
     "filter": ["per_customer.total > 0"], "order": [{"field": "avg_total"}]},
]


def keys_of(value: Any) -> Iterator[str]:
    """Every dict key anywhere in ``value``."""
    if isinstance(value, dict):
        for k, v in value.items():
            yield k
            yield from keys_of(v)
    elif isinstance(value, list):
        for v in value:
            yield from keys_of(v)


def assert_ossie_stage(dumped: dict[str, Any]) -> None:
    assert {"population", "metrics", "filter"} <= dumped.keys()
    assert not PRE_2_0_QUERY_KEYS & set(keys_of(dumped)), dumped
    assert isinstance(dumped["filter"], list)
    assert all("field" in o for o in dumped["order"])


class TestDump:
    def test_every_stage_dumps_in_ossie_keys(self) -> None:
        with recorded() as caught:
            stages = [SlayerQuery.model_validate(fresh(s)) for s in TWO_STAGES]
        assert deprecations(caught) == []
        dumped = [s.model_dump(mode="json") for s in stages]
        for stage in dumped:
            assert_ossie_stage(stage)
        assert dumped[0]["time_dimensions"][0]["field"] == "created_at"
        [ext] = dumped[0]["extensions"]
        assert {"dataset", "metrics", "relationships"} <= ext.keys()
        assert ext["relationships"][0]["to"] == "customers"
        assert ext["metrics"][0]["expression"] == "sum(amount) / count(*)"
        assert dumped[0]["metrics"][0]["metric"] == "sum(amount)"

    def test_a_field_reference_names_its_dataset(self) -> None:
        query = SlayerQuery.model_validate({"population": "orders", "metrics": ["count(orders.*)"],
                                            "dimensions": [{"field": "customers.name"}]})
        [dim] = query.dimensions or []
        assert isinstance(dim, QueryDimension)
        assert dim.ref is not None
        assert (dim.ref.name, dim.ref.dataset) == ("name", "customers")
        assert "model" not in set(keys_of(query.model_dump(mode="json")))

    def test_the_dump_validates_back_without_deprecation(self) -> None:
        dumped = SlayerQuery.model_validate(fresh(TWO_STAGES[0])).model_dump(mode="json")
        with recorded() as caught:
            again = SlayerQuery.model_validate(dumped)
        assert deprecations(caught) == []
        assert again.model_dump(mode="json") == dumped

    def test_a_refinement_dumps_in_ossie_keys(self) -> None:
        with recorded() as caught:
            refinement = QueryRefinement.model_validate(
                {"metrics": [{"metric": "count(orders.*)", "alias": "n"}], "filter": ["amount > 5"]})
        assert deprecations(caught) == []
        dumped = refinement.model_dump(mode="json", exclude_none=True)
        assert {"metrics", "filter"} <= dumped.keys()
        assert not PRE_2_0_QUERY_KEYS & set(keys_of(dumped))

    def test_a_rest_query_request_takes_ossie_keys(self) -> None:
        with recorded() as caught:
            request = QueryRequest.model_validate(fresh(TWO_STAGES[0]))
        assert deprecations(caught) == []
        assert request.population == "orders"
        assert request.filter == ["amount > 5"]
        assert request.metrics is not None
        assert len(request.metrics) == 2


@pytest.mark.parametrize("backend", ["yaml", "sqlite"])
async def test_a_nested_query_is_stored_in_ossie_keys(backend: str, tmp_path: Path) -> None:
    storage = await shop_store(backend=backend, base=str(tmp_path))
    await storage.save_dataset(SlayerDataset.model_validate(shop_customers()))
    await storage.save_dataset(SlayerDataset.model_validate(shop_orders()))
    saved = SlayerDataset.model_validate({"name": "by_region", "data_source": "shop", "source_queries": [
        {"population": "orders", "dimensions": ["customers.region"],
         "metrics": [{"metric": "sum(orders.amount)", "alias": "total"}], "filter": ["orders.amount > 0"],
         "order": [{"field": "total"}]}]})
    await storage.save_dataset(saved)
    doc = stored_document(backend=backend, base=str(tmp_path), name="by_region")
    [stage] = doc["source_queries"]
    assert_ossie_stage(stage)


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_refinement_in_ossie_keys_applies_as_before(backend: str) -> None:
    refine = {"metrics": [{"metric": "count(orders.*)", "alias": "n"}], "filter": ["orders.region = 'US'"]}
    async with refine_vocab_engine(backend) as engine:
        with recorded() as caught:
            refined = await engine.execute(SAVED_QUERY, refine=fresh(refine))
        expected = await engine.execute({
            "population": "orders", "time_dimensions": [{"field": "orders.ordered_at", "granularity": "month"}],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "revenue"}, {"metric": "count(orders.*)", "alias": "n"}],
            "filter": ["orders.status = 'paid'", "orders.region = 'US'"],
        })
    assert deprecations(caught) == []
    assert normal(refined)["rows"] == normal(expected)["rows"]
    assert [k.split(".")[-1] for k in refined.columns] == ["ordered_at", "revenue", "n"]


async def test_rest_runs_a_query_in_ossie_keys(tmp_path: Path) -> None:
    storage = await shop_store(backend="yaml", base=str(tmp_path))
    await storage.save_dataset(SlayerDataset.model_validate(shop_customers()))
    await storage.save_dataset(SlayerDataset.model_validate(shop_orders()))
    resp = TestClient(create_app(storage=storage)).post("/query", json={
        "population": "orders", "dimensions": ["customers.region"],
        "metrics": [{"metric": "sum(orders.amount)", "alias": "total"}], "filter": ["orders.amount > 0"],
        "order": [{"field": "total", "direction": "desc"}],
    })
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert [k.split(".")[-1] for k in body["columns"]] == ["region", "total"]
    assert not [w for w in body.get("warnings") or [] if w.get("kind") == "deprecated_input"]
