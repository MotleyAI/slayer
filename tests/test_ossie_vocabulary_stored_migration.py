"""Stored datasets, queries and memories in the pre-2.0 vocabulary load as their Ossie twins, silently, nested
queries included with or without a version of their own; a migrated dataset is written back.

The pre-2.0 spellings below are deliberate: keep this file out of the vocabulary respelling pass.

Spec: compatibility/ossie-vocabulary › Stored documents migrate to the new vocabulary.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.memories.models import Memory
from slayer.storage.migrations import CURRENT_VERSIONS
from tests._deprecated_input_helpers import recorded
from tests._ossie_vocabulary_fixtures import (
    SHOP_REVENUE_BY_REGION,
    shop_customers,
    shop_orders,
    shop_store,
    stored_document,
)
from tests._stored_upgrade_fixtures import AMOUNT_BY_REGION, BACKENDS, DS, by_dimension, memory_v2, run
from tests._upgrade_corpus import MEMORY_PREFIX, Corpus, load_all, offline, online
from tests._upgrade_corpus import run as run_recorded

PRE = {"SlayerModel": 25, "SlayerQuery": 15, "Memory": 11, "Embedding": 1}
CORPORA = {
    "v0_10_2": Corpus.load(release="v0_10_2", datasources=("shop", "cube", "lite"),
                           data_files=("data.duckdb", "data.sqlite")),
    "v1_8_2": Corpus.load(release="v1_8_2", datasources=("shop",), data_files=("data.duckdb",)),
}


def future(caught: list[Any]) -> list[Any]:
    return [w for w in caught if issubclass(w.category, FutureWarning)]


def stored_query(**extra: Any) -> dict[str, Any]:
    """A source query as the pre-rename release stored it."""
    return {
        "source_model": "orders",
        "measures": [{"formula": "sum(amount)", "name": "rev"}],
        "dimensions": [{"name": "region", "model": "customers"}, {"expression": "upper(status)", "name": "s"}],
        "time_dimensions": [{"dimension": "ordered_at", "granularity": "month"}],
        "filters": ["amount > 1"],
        "order": [{"column": {"name": "rev"}, "direction": "desc"}],
        **extra,
    }


def assert_new_query_keys(dumped: dict[str, Any]) -> None:
    assert not {"source_model", "measures", "filters"} & dumped.keys(), dumped
    for entry in dumped.get("metrics") or []:
        assert "formula" not in entry, entry
    for entry in dumped.get("dimensions") or []:
        assert "model" not in entry, entry
    for entry in dumped.get("time_dimensions") or []:
        assert not {"dimension", "column"} & entry.keys(), entry
    for entry in dumped.get("order") or []:
        assert "column" not in entry, entry
    for extension in dumped.get("extensions") or []:
        assert not {"measures", "joins"} & extension.keys(), extension


def assert_new_dataset_keys(dumped: dict[str, Any]) -> None:
    assert not {"columns", "measures", "joins"} & dumped.keys(), sorted(dumped)
    assert all("type" not in f for f in dumped.get("fields") or [])
    assert all(not {"type", "formula"} & m.keys() for m in dumped.get("metrics") or [])
    assert all("target_model" not in r for r in dumped.get("relationships") or [])
    assert all("formula" not in a for a in dumped.get("aggregations") or [])
    for query in dumped.get("source_queries") or []:
        assert_new_query_keys(query)


def assert_migrated_query(query: SlayerQuery | None) -> None:
    assert query is not None
    assert query.population == "orders"
    assert [(m.expression, m.name) for m in query.metrics or []] == [("sum(amount)", "rev")]
    region, computed = query.dimensions or []
    assert (region.name, region.dataset) == ("region", "customers")
    assert (computed.expression, computed.name) == ("upper(status)", "s")
    assert [td.field for td in query.time_dimensions or []] == ["ordered_at"]
    assert query.filter == ["amount > 1"]
    assert [o.field.name for o in query.order or []] == ["rev"]
    assert_new_query_keys(query.model_dump(mode="json"))


def stored(doc: dict[str, Any]) -> dict[str, Any]:
    return {"version": PRE["SlayerModel"], **doc}


def stored_orders() -> dict[str, Any]:
    return stored(shop_orders("old", aggregations=[{"name": "doubled", "formula": "SUM({value}) * 2"}]))


def assert_migrated_orders(dataset: SlayerDataset | None) -> None:
    assert dataset is not None
    assert [f.name for f in dataset.fields] == ["order_id", "customer_id", "status", "ordered_at", "amount"]
    assert next(f for f in dataset.fields if f.name == "ordered_at").datatype == DataType.TIMESTAMP
    [metric] = dataset.metrics
    assert (metric.name, metric.expression, metric.datatype) == ("revenue", "sum(amount)", DataType.DOUBLE)
    [relationship] = dataset.relationships
    assert (relationship.to, relationship.join_pairs) == ("customers", [["customer_id", "id"]])
    [aggregation] = dataset.aggregations
    assert aggregation.expression == "SUM({value}) * 2"
    dumped = dataset.model_dump(mode="json")
    assert "fields" in dumped
    assert_new_dataset_keys(dumped)


class TestVersionSteps:
    @pytest.mark.parametrize("entity", sorted(PRE))
    def test_current_versions(self, entity: str) -> None:
        assert CURRENT_VERSIONS[entity] == PRE[entity] + 1

    def test_a_dataset_steps_silently(self) -> None:
        with recorded() as caught:
            dataset = SlayerDataset.model_validate(stored_orders())
        assert future(caught) == []
        assert_migrated_orders(dataset)
        assert dataset.version == PRE["SlayerModel"] + 1

    def test_a_query_steps_silently(self) -> None:
        with recorded() as caught:
            query = SlayerQuery.model_validate(stored_query(version=PRE["SlayerQuery"]))
        assert future(caught) == []
        assert_migrated_query(query)
        assert query.version == PRE["SlayerQuery"] + 1

    def test_a_stored_extension_steps_silently(self) -> None:
        with recorded() as caught:
            query = SlayerQuery.model_validate({
                "version": PRE["SlayerQuery"], "source_model": "orders", "measures": ["aov"],
                "extensions": [{"source_name": "orders",
                                "measures": [{"name": "aov", "formula": "sum(amount) / count(*)"}],
                                "joins": [{"target_model": "customers", "join_pairs": [["customer_id", "id"]],
                                           "name": "buyer"}]}],
            })
        assert future(caught) == []
        [extension] = query.extensions or []
        assert [(m.name, m.expression) for m in extension.metrics or []] == [("aov", "sum(amount) / count(*)")]
        assert [(r.to, r.name) for r in extension.relationships or []] == [("customers", "buyer")]
        assert_new_query_keys(query.model_dump(mode="json"))

    def test_an_unversioned_nested_query_steps_silently(self) -> None:
        with recorded() as caught:
            dataset = SlayerDataset.model_validate(stored({"name": "by_region", "data_source": DS,
                                                           "source_queries": [stored_query()]}))
        assert future(caught) == []
        [query] = dataset.source_queries or []
        assert_migrated_query(query)

    @pytest.mark.parametrize("query_version", [None, PRE["SlayerQuery"]])
    def test_a_memory_steps_silently(self, query_version: int | None) -> None:
        query = stored_query() if query_version is None else stored_query(version=query_version)
        with recorded() as caught:
            memory = Memory.model_validate(memory_v2(memory_id="1", query=query, version=PRE["Memory"]))
        assert future(caught) == []
        assert_migrated_query(memory.query)
        assert memory.version == PRE["Memory"] + 1


@pytest.mark.parametrize("backend", BACKENDS)
class TestStores:
    async def test_a_pre_2_0_dataset_loads_silently(self, backend: str, tmp_path: Path) -> None:
        storage = await shop_store(backend=backend, base=str(tmp_path),
                                   datasets=[stored(shop_customers("old")), stored_orders()])
        with recorded() as caught:
            dataset = await storage.get_dataset("orders", data_source=DS)
            response = await run(storage, SHOP_REVENUE_BY_REGION["new"])
        assert future(caught) == []
        assert_migrated_orders(dataset)
        assert by_dimension(response, dim_suffix="region") == AMOUNT_BY_REGION

    async def test_a_migrated_dataset_is_written_back(self, backend: str, tmp_path: Path) -> None:
        nested = {"name": "by_region", "data_source": DS, "source_queries": [stored_query()]}
        storage = await shop_store(backend=backend, base=str(tmp_path),
                                   datasets=[stored(shop_customers("old")), stored_orders(), stored(nested)])
        await storage.get_dataset("orders", data_source=DS)
        await storage.get_dataset("by_region", data_source=DS)
        for name in ("orders", "by_region"):
            doc = stored_document(backend=backend, base=str(tmp_path), name=name)
            assert doc["version"] == PRE["SlayerModel"] + 1
            assert_new_dataset_keys(doc)
        assert "fields" in stored_document(backend=backend, base=str(tmp_path), name="orders")

    @pytest.mark.parametrize("query_version", [None, PRE["SlayerQuery"]])
    async def test_a_nested_stored_query_loads_silently(
        self, backend: str, tmp_path: Path, query_version: int | None,
    ) -> None:
        nested = stored_query() if query_version is None else stored_query(version=query_version)
        saved = stored({"name": "by_region", "data_source": DS, "source_queries": [nested]})
        storage = await shop_store(backend=backend, base=str(tmp_path),
                                   datasets=[stored(shop_customers("old")), stored_orders(), saved])
        with recorded() as caught:
            dataset = await storage.get_dataset("by_region", data_source=DS)
        assert future(caught) == []
        assert dataset is not None
        [query] = dataset.source_queries or []
        assert_migrated_query(query)

    @pytest.mark.parametrize("query_version", [None, PRE["SlayerQuery"]])
    async def test_a_memory_query_loads_silently(self, backend: str, tmp_path: Path, query_version: int | None) -> None:
        query: dict[str, Any] = {"source_model": "orders", "filters": ["amount > 1"],
                                 "measures": [{"formula": "sum(amount)", "name": "rev"}]}
        if query_version is not None:
            query["version"] = query_version
        storage = await shop_store(
            backend=backend, base=str(tmp_path), datasets=[stored(shop_customers("old")), stored_orders()],
            memories=[memory_v2(memory_id="1", query=query, version=PRE["Memory"])],
        )
        with recorded() as caught:
            memory = await storage.get_memory("1")
        assert future(caught) == []
        assert memory is not None and memory.query is not None
        assert memory.query.filter == ["amount > 1"]
        assert [(m.expression, m.name) for m in memory.query.metrics or []] == [("sum(amount)", "rev")]
        assert_new_query_keys(memory.query.model_dump(mode="json"))


@pytest.mark.parametrize("release", sorted(CORPORA))
@pytest.mark.parametrize("backend", BACKENDS)
async def test_the_1x_upgrade_stores_load_in_the_new_vocabulary(
    release: str, backend: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    corpus = CORPORA[release]
    storage = offline(corpus, backend=backend, tmp=tmp_path, monkeypatch=monkeypatch)
    with recorded() as caught:
        datasets, memories = await load_all(corpus, storage)
    assert future(caught) == []
    assert datasets
    for dataset in datasets:
        assert_new_dataset_keys(dataset.model_dump(mode="json"))
    for memory in memories:
        assert memory is not None
        if memory.query is not None:
            assert_new_query_keys(memory.query.model_dump(mode="json"))


def fixture_digest(corpus: Corpus) -> dict[str, str]:
    return {str(p.relative_to(corpus.root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(corpus.root.rglob("*")) if p.is_file()}


@pytest.mark.parametrize("release", sorted(CORPORA))
@pytest.mark.parametrize("backend", BACKENDS)
async def test_the_1x_upgrade_stores_return_their_release_results(release: str, backend: str, tmp_path: Path) -> None:
    corpus = CORPORA[release]
    before = fixture_digest(corpus)
    async with online(corpus, backend=backend, tmp=tmp_path) as storage:
        datasets, _ = await load_all(corpus, storage)
        assert all("fields" in d.model_dump(mode="json") for d in datasets)
        for key in sorted(corpus.expected):
            with recorded() as caught:
                rows = await run_recorded(corpus, storage, key)
            assert rows == corpus.expected[key]["rows"], key
            if key.startswith(MEMORY_PREFIX):
                assert future(caught) == [], key
    assert fixture_digest(corpus) == before
