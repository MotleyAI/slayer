"""The datasource an unpinned stored query reads when its population names no stored dataset (models/stored-upgrade)."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.memories.models import Memory
from slayer.storage.stored_datasource import (
    fallback_datasource,
    query_dataset_names,
    raw_memory_dataset_names,
    uses_fallback_datasource,
)
from tests._stored_upgrade_fixtures import memory_v2

AB = {"a": {"orders"}, "b": {"orders", "customers"}}


class TestTrigger:
    @pytest.mark.parametrize(("population", "stored", "expected"), [
        pytest.param(None, {"orders"}, True, id="no-population"),
        pytest.param("time_spine", {"orders"}, True, id="spine"),
        pytest.param("TIME_SPINE", {"orders"}, True, id="spine-other-case"),
        pytest.param("Time_Spine", {"orders", "Time_Spine"}, False, id="spine-spelled-exactly-by-a-dataset"),
        pytest.param("orders", {"orders"}, False, id="stored-population"),
        pytest.param("ordrs", {"orders"}, False, id="missing-explicit-population"),
    ])
    def test_trigger(self, population: str | None, stored: set[str], expected: bool) -> None:
        assert uses_fallback_datasource(population, stored_names=stored) is expected


class TestFallback:
    @pytest.mark.parametrize(("names", "datasets_by_ds", "priority", "expected"), [
        pytest.param([], {"b": {"orders"}, "a": {"orders"}}, [], "a", id="no-names-first-by-name"),
        pytest.param([], {"a": {"orders"}, "b": {"orders"}}, ["b"], "b", id="no-names-priority"),
        pytest.param(["customers"], AB, [], "b", id="named-holder"),
        pytest.param(["Customers"], AB, [], "b", id="named-holder-by-case"),
        pytest.param(["customers"], {**AB, "c": {"customers"}}, ["a", "c", "b"], "c", id="priority-among-holders"),
        pytest.param(["customers"], {**AB, "c": {"customers"}}, ["a", "c"], "c", id="priority-before-name"),
        pytest.param(["ghost"], AB, [], "a", id="named-nowhere-every-datasource"),
        pytest.param(["time_spine"], {"a": {"orders"}, "b": {"orders", "Time_Spine"}}, [], "a",
                     id="spine-names-ignored"),
    ])
    def test_fallback(self, names: list[str], datasets_by_ds: dict[str, set[str]], priority: list[str],
                      expected: str) -> None:
        assert fallback_datasource(names, datasets_by_ds=datasets_by_ds, priority=priority) == expected

    def test_no_datasource(self) -> None:
        assert fallback_datasource([], datasets_by_ds={}, priority=[]) is None


JOIN = {"name": "cust", "target_model": "customers", "join_pairs": [["customer_id", "id"]], "cardinality": "many_to_one"}

#: Raw stored memory queries at an old version, and the dataset names they reference.
NAMED_CASES = [
    pytest.param({"dimensions": [{"name": "customers.region"}]}, {"customers"}, id="dimension"),
    pytest.param({"time_dimensions": [{"dimension": {"name": "customers.signed_up_at"}, "granularity": "year"}]},
                 {"customers"}, id="time-dimension-structured"),
    pytest.param({"time_dimensions": [{"dimension": "customers.signed_up_at", "granularity": "year"}]},
                 {"customers"}, id="time-dimension-text"),
    pytest.param({"measures": [{"formula": "sum(customers.id)", "name": "n"}]}, {"customers"}, id="metric"),
    pytest.param({"measures": [{"formula": "customers.id:count", "name": "n"}]}, {"customers"}, id="colon-metric"),
    pytest.param({"filters": ["customers.region == 'North' and not orders.status"]}, {"customers", "orders"},
                 id="python-spelled-filter"),
    pytest.param({"dimensions": [{"name": "status"}], "order": [{"column": {"name": "customers.region"}}]},
                 {"customers"}, id="order-key"),
    pytest.param({"order": [{"column": {"name": "_funcstyle_pending"}, "direction": "asc",
                             "raw_formula": "max(customers.id)"}]}, {"customers"}, id="order-expression"),
    pytest.param({"main_time_dimension": "customers.signed_up_at", "measures": [{"formula": "sum(amount)", "name": "r"}]},
                 {"customers"}, id="main-time-dimension"),
    pytest.param({"dimensions": [{"name": "status"}], "measures": [{"formula": "sum(amount)", "name": "r"}]}, set(),
                 id="bare-names-only"),
]


def _memory(query: dict[str, Any], *, version: int = 4, memory_version: int = 2) -> dict[str, Any]:
    return memory_v2(memory_id="m", query={"version": version, **query}, version=memory_version)


class TestNameReader:
    @pytest.mark.parametrize(("query", "expected"), NAMED_CASES)
    def test_raw_and_validated_agree(self, query: dict[str, Any], expected: set[str]) -> None:
        raw = _memory(query)
        validated = Memory.model_validate(raw).query
        assert validated is not None
        assert set(raw_memory_dataset_names(raw)) == expected
        assert set(query_dataset_names(validated)) == expected

    @pytest.mark.parametrize(("query", "expected"), [
        pytest.param({"dimensions": ["customers.region"]}, {"customers"}, id="dimension"),
        pytest.param({"time_dimensions": [{"dimension": "customers.signed_up_at", "granularity": "year"}]},
                     {"customers"}, id="time-dimension"),
        pytest.param({"measures": [{"formula": "sum(customers.id)", "name": "n"}]}, {"customers"}, id="metric"),
        pytest.param({"filters": ["customers.region == 'North' and not orders.status"]}, {"customers", "orders"},
                     id="python-spelled-filter"),
        pytest.param({"main_time_dimension": "customers.signed_up_at",
                      "measures": [{"formula": "sum(amount)", "name": "r"}]}, {"customers"}, id="main-time-dimension"),
    ])
    def test_raw_and_validated_agree_at_a_later_version(self, query: dict[str, Any], expected: set[str]) -> None:
        raw = _memory(query, version=16, memory_version=12)
        validated = Memory.model_validate(raw).query
        assert validated is not None
        assert set(raw_memory_dataset_names(raw)) == set(query_dataset_names(validated)) == expected

    def test_extended_dataset_and_join_target(self) -> None:
        raw = _memory({"dimensions": ["status"], "extensions": [{"source_name": "orders", "joins": [JOIN]}]},
                      version=16, memory_version=12)
        validated = Memory.model_validate(raw).query
        assert validated is not None
        assert set(raw_memory_dataset_names(raw)) == set(query_dataset_names(validated)) == {"orders", "customers"}

    def test_unparsable_text_names_nothing(self) -> None:
        raw = _memory({"dimensions": [{"name": "orders.status"}], "filters": ["customers.region = = ("]})
        assert set(raw_memory_dataset_names(raw)) == {"orders"}

    @pytest.mark.parametrize("query", [None, "not a query"])
    def test_no_query_names_nothing(self, query: Any) -> None:
        assert raw_memory_dataset_names({**memory_v2(memory_id="m", query={}), "query": query}) == []
