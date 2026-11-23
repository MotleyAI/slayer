"""The datasource of an unpinned stored memory whose population names no stored dataset (models/stored-upgrade)."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.storage.stored_datasource import (
    memory_fallback_population,
    raw_memory_datasource,
    uses_fallback_datasource,
)
from tests._stored_upgrade_fixtures import memory_v2

TWO = [("shop", "orders"), ("shop", "customers"), ("shop2", "orders"), ("shop2", "customers")]


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

    @pytest.mark.parametrize(("query", "expected"), [
        pytest.param({"version": 4, "dimensions": [{"name": "status"}]}, (True, None), id="no-population"),
        pytest.param({"version": 4, "source_model": "time_spine"}, (True, "time_spine"), id="spine"),
        pytest.param({"version": 16, "population": "time_spine"}, (True, "time_spine"), id="spine-current-keys"),
        pytest.param({"version": 4, "source_model": "orders"}, (False, "orders"), id="stored-population"),
        pytest.param({"version": 4, "source_model": "ordrs"}, (False, "ordrs"), id="missing-explicit-population"),
        pytest.param({"version": 4, "source_model": {"name": "inline", "sql_table": "t", "data_source": "shop"}},
                     (False, None), id="inline-dataset"),
        pytest.param({"version": 16, "population": {"dataset": "time_spine", "metrics": []}}, (True, "time_spine"),
                     id="spine-extension"),
    ])
    def test_memory_trigger(self, query: dict[str, Any], expected: tuple[bool, str | None]) -> None:
        assert memory_fallback_population(memory_v2(memory_id="m", query=query), identities=TWO) == expected

    @pytest.mark.parametrize("query", [None, "not a query"])
    def test_no_query_does_not_trigger(self, query: Any) -> None:
        data = {**memory_v2(memory_id="m", query={}), "query": query}
        assert memory_fallback_population(data, identities=TWO) == (False, None)


class TestRawDatasource:
    @pytest.mark.parametrize(("entities", "identities", "expected"), [
        pytest.param([], [("shop", "orders")], "shop", id="only-datasource"),
        pytest.param([], TWO, None, id="no-entities-two-datasources"),
        pytest.param(["shop2.customers"], TWO, "shop2", id="dataset-entity"),
        pytest.param(["shop2.customers.status", "shop2.customers.amount"], TWO, "shop2", id="leaf-entities"),
        pytest.param(["shop2"], TWO, None, id="bare-datasource-ignored"),
        pytest.param(["shop2.customers.region.name"], TWO, None, id="deeper-path-ignored"),
        pytest.param(["shop2.gone"], TWO, None, id="missing-dataset-ignored"),
        pytest.param(["Shop2.customers"], TWO, None, id="other-case-ignored"),
        pytest.param(["memory:5"], TWO, None, id="memory-entity-ignored"),
        pytest.param(["shop.orders", "shop2.customers"], TWO, None, id="two-datasources"),
        pytest.param(["shop2.gone", "shop.orders.status"], TWO, "shop", id="pin-beside-ignored"),
        pytest.param(["shop2.customers"], [("shop", "orders")], "shop", id="stale-pin-only-datasource"),
        pytest.param([], [], None, id="no-datasource"),
        pytest.param(7, [("shop", "orders")], "shop", id="malformed-entities-only-datasource"),
        pytest.param("shop2.customers", TWO, None, id="malformed-entities-two-datasources"),
    ])
    def test_raw_datasource(self, entities: Any, identities: list[tuple[str, str]], expected: str | None) -> None:
        data = {**memory_v2(memory_id="m", query={"version": 4}), "entities": entities}
        assert raw_memory_datasource(data, identities=identities) == expected
