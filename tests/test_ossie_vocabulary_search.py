"""Search reports the kinds ``dataset`` / ``field`` / ``metric`` and labels graph nodes ``Dataset`` / ``Field`` /
``Metric`` / ``DatasetField``; a kind or label from the pre-2.0 vocabulary is read as its replacement with a
deprecation.

Spec: compatibility/ossie-vocabulary › Search speaks the vocabulary.

The pre-2.0 spellings below are deliberate: keep this file out of the vocabulary respelling pass.
"""

from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from slayer.core.models import DatasetMetric
from slayer.search import graph as search_graph
from slayer.search.service import SearchResponse, SearchService
from slayer.storage.base import StorageBackend, resolve_storage
from tests._deprecated_input_helpers import deprecations, payload_of, recorded
from tests.search_helpers import seed_warehouse_models

QUESTION = "orders amount paid revenue customers email"
OLD_KINDS = {"model", "column", "measure"}
NEW_KINDS = {"dataset", "field", "metric"}
CONVERTED_LABELS = [("Model", "Dataset"), ("Column", "Field"), ("Measure", "Metric"), ("column", "field"),
                    ("ModelColumn", "DatasetField")]


@pytest_asyncio.fixture
async def storage() -> AsyncIterator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmp:
        st = resolve_storage(tmp)
        await seed_warehouse_models(st)
        orders = await st.get_dataset("orders", data_source="warehouse")
        assert orders is not None
        metric = DatasetMetric.model_validate({"name": "paid_revenue", "expression": "sum(amount_paid)"})
        await st.save_dataset(orders.model_copy(update={"metrics": [*orders.metrics, metric]}))
        yield st


def ids(response: SearchResponse) -> list[str]:
    return sorted(h.id for h in response.results)


def mentions(response: SearchResponse, *tokens: str) -> bool:
    return any(all(t in w for t in tokens) for w in response.warnings)


def cypher(label: str) -> str:
    return f"MATCH (n:{label}) RETURN n.id AS id"


async def test_results_carry_the_new_kinds(storage: StorageBackend) -> None:
    response = await SearchService(storage=storage).search(question=QUESTION, max_results=50)
    kinds = {h.kind for h in response.results}
    assert NEW_KINDS <= kinds
    assert not OLD_KINDS & kinds


@pytest.mark.parametrize(("old", "new"), CONVERTED_LABELS)
async def test_an_old_kind_converts_in_the_naive_filter(
    storage: StorageBackend, monkeypatch: pytest.MonkeyPatch, old: str, new: str,
) -> None:
    monkeypatch.setattr(search_graph, "is_available", lambda: False)
    service = SearchService(storage=storage)
    fresh = await service.search(question=QUESTION, max_results=50, cypher_filter=cypher(new))
    with recorded() as caught:
        legacy = await service.search(question=QUESTION, max_results=50, cypher_filter=cypher(old))
    assert fresh.results
    assert ids(legacy) == ids(fresh)
    assert {h.kind for h in fresh.results} <= NEW_KINDS
    assert mentions(legacy, old, new) and not mentions(fresh, old)
    assert [(payload_of(w).old, payload_of(w).new) for w in deprecations(caught)] == [(old, new)]


@pytest.mark.skipif(not search_graph.is_available(), reason="the advanced_search graph is not installed")
@pytest.mark.parametrize(("old", "new"), [("Model", "Dataset"), ("ModelColumn", "DatasetField"), ("Measure", "Metric")])
async def test_an_old_label_converts_in_graph_cypher(storage: StorageBackend, old: str, new: str) -> None:
    service = SearchService(storage=storage)
    fresh = await service.search(question=QUESTION, max_results=50, cypher_filter=cypher(new))
    with recorded() as caught:
        legacy = await service.search(question=QUESTION, max_results=50, cypher_filter=cypher(old))
    assert fresh.results
    assert ids(legacy) == ids(fresh)
    assert mentions(legacy, old, new)
    assert len(deprecations(caught)) == 1


@pytest.mark.skipif(not search_graph.is_available(), reason="the advanced_search graph is not installed")
@pytest.mark.parametrize(("label", "kind"), [("Dataset", "dataset"), ("DatasetField", "field"), ("Metric", "metric")])
async def test_the_graph_labels_its_nodes_in_the_new_vocabulary(storage: StorageBackend, label: str, kind: str) -> None:
    response = await SearchService(storage=storage).search(question=QUESTION, max_results=50,
                                                           cypher_filter=cypher(label))
    assert response.results, label
    assert {h.kind for h in response.results} == {kind}
