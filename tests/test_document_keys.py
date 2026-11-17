"""Raw stored-document walkers read the keys of the document's own version."""

import pytest

from slayer.storage.document_keys import CURRENT_KEYS, LEGACY_KEYS, VOCABULARY_VERSIONS, keys_for
from slayer.storage.granularity_rename import memory_source_dataset, rename_in_dataset, rename_in_memory
from slayer.storage.mode_b_texts import rewrite_dataset_doc, rewrite_memory_doc


@pytest.mark.parametrize("entity", sorted(VOCABULARY_VERSIONS))
def test_keys_switch_at_the_vocabulary_version(entity: str) -> None:
    first = VOCABULARY_VERSIONS[entity]
    assert keys_for(entity=entity, version=first - 1) == LEGACY_KEYS
    assert keys_for(entity=entity, version=first) == CURRENT_KEYS


def _upper(text: str) -> str:
    return text.upper()


def test_mode_b_walk_reads_legacy_keys() -> None:
    doc = {"measures": [{"name": "m", "formula": "sum(x)"}],
           "source_queries": [{"source_model": "o", "measures": [{"formula": "count(*)"}], "filters": ["a > 1"]}]}
    out = rewrite_dataset_doc(doc, _upper, keys=LEGACY_KEYS)
    assert out["measures"][0]["formula"] == "SUM(X)"
    stage = out["source_queries"][0]
    assert stage["measures"][0]["formula"] == "COUNT(*)"
    assert stage["filters"] == ["A > 1"]


def test_mode_b_walk_reads_current_keys() -> None:
    doc = {"metrics": [{"name": "m", "expression": "sum(x)"}],
           "source_queries": [{"population": "o", "metrics": [{"expression": "count(*)"}], "filter": ["a > 1"]}]}
    out = rewrite_dataset_doc(doc, _upper, keys=CURRENT_KEYS)
    assert out["metrics"][0]["expression"] == "SUM(X)"
    stage = out["source_queries"][0]
    assert stage["metrics"][0]["expression"] == "COUNT(*)"
    assert stage["filter"] == ["A > 1"]


def test_mode_b_walk_leaves_the_other_spelling_alone() -> None:
    legacy = {"measures": [{"formula": "sum(x)"}]}
    assert rewrite_dataset_doc(legacy, _upper, keys=CURRENT_KEYS) == legacy


def test_memory_walk_follows_the_given_keys() -> None:
    memory = {"query": {"population": "o", "metrics": [{"expression": "sum(x)"}]}}
    out = rewrite_memory_doc(memory, _upper, keys=CURRENT_KEYS)
    assert out["query"]["metrics"][0]["expression"] == "SUM(X)"


@pytest.mark.parametrize(("version", "fields", "metrics", "expression"), [
    (VOCABULARY_VERSIONS["SlayerModel"] - 1, "columns", "measures", "formula"),
    (VOCABULARY_VERSIONS["SlayerModel"], "fields", "metrics", "expression"),
])
def test_granularity_rename_follows_the_dataset_version(version: int, fields: str, metrics: str, expression: str) -> None:
    doc = {"version": version, fields: [{"name": "d", "granularity": "fy"}],
           metrics: [{"name": "m", expression: "count(*)"}]}
    out = rename_in_dataset(doc, renames={"fy": "fiscal_year"})
    assert out[fields][0]["granularity"] == "fiscal_year"
    assert out[metrics] == doc[metrics]


@pytest.mark.parametrize(("version", "population"), [
    (VOCABULARY_VERSIONS["Memory"] - 1, "source_model"),
    (VOCABULARY_VERSIONS["Memory"], "population"),
])
def test_memory_root_follows_the_memory_version(version: int, population: str) -> None:
    memory = {"version": version, "query": {population: "orders",
                                            "time_dimensions": [{"granularity": "fy"}]}}
    assert memory_source_dataset(memory) == (None, "orders")
    out = rename_in_memory(memory, renames={"fy": "fiscal_year"})
    assert out["query"]["time_dimensions"][0]["granularity"] == "fiscal_year"


def test_unversioned_memory_reads_legacy_keys() -> None:
    assert memory_source_dataset({"query": {"source_model": "orders"}}) == (None, "orders")
    assert memory_source_dataset({"query": {"population": "orders"}}) == (None, None)
