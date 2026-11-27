"""Stored 1.x documents load in Ossie's shapes and datatype values, silently.

Spec: compatibility/ossie-vocabulary › Stored documents migrate to the new vocabulary.

The 1.x documents below are deliberate: keep this file out of any respelling pass.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from slayer.core.models import SlayerDataset
from slayer.core.query import QueryDimension, QueryMetric, SlayerQuery
from tests._deprecated_input_helpers import deprecations, recorded
from tests._ossie_vocabulary_fixtures import shop_store
from tests._stored_upgrade_fixtures import DS, ossie_dataset, stored_relationships

PRE_DATASET = 25
PRE_QUERY = 15


def pre_dataset(*columns: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return {"version": PRE_DATASET, "name": "orders", "sql_table": "orders", "data_source": DS,
            "columns": [{"name": "order_id", "type": "INT", "primary_key": True}, *columns], **extra}


def loaded(doc: dict[str, Any]) -> SlayerDataset:
    with recorded() as caught:
        dataset = SlayerDataset.model_validate(doc)
    assert deprecations(caught) == []
    return dataset


def field(dataset: SlayerDataset, name: str) -> Any:
    return next(f for f in dataset.fields if f.name == name)


class TestDatatypes:
    @pytest.mark.parametrize(("old", "db_type", "new"), [
        ("DOUBLE", "NUMERIC(18,2)", "Decimal"), ("DOUBLE", None, "Float"), ("DOUBLE", "DOUBLE", "Float"),
        ("TIMESTAMP", "TIMESTAMPTZ", "DateTimeTz"), ("TIMESTAMP", None, "DateTime"), ("UNKNOWN", None, "Opaque"),
        ("TEXT", None, "String"), ("INT", None, "Integer"),
    ])
    def test_a_stored_type_converts_by_database_type(self, old: str, db_type: str | None, new: str) -> None:
        dataset = loaded(pre_dataset({"name": "x", "type": old, **({"db_type": db_type} if db_type else {})}))
        assert field(dataset, "x").datatype.value == new

    def test_a_stored_metric_type_converts(self) -> None:
        dataset = loaded(pre_dataset(measures=[{"name": "m", "formula": "max(order_id)", "type": "TIMESTAMP"}]))
        [metric] = dataset.metrics
        assert metric.datatype is not None
        assert metric.datatype.value == "DateTime"

    def test_a_stash_that_maps_to_the_stored_type_becomes_the_datatype(self) -> None:
        dataset = loaded(pre_dataset({"name": "at", "type": "TIMESTAMP", "meta": {"osi_datatype": "DateTimeTz",
                                                                                 "owner": "x"}}))
        at = field(dataset, "at")
        assert at.datatype.value == "DateTimeTz"
        assert at.meta == {"owner": "x"}

    def test_a_stash_that_does_not_map_is_dropped(self) -> None:
        dataset = loaded(pre_dataset({"name": "qty", "type": "INT", "meta": {"osi_datatype": "String"}}))
        qty = field(dataset, "qty")
        assert qty.datatype.value == "Integer"
        assert "osi_datatype" not in (qty.meta or {})

    def test_a_stored_metric_stash_becomes_the_datatype(self) -> None:
        dataset = loaded(pre_dataset(measures=[{"name": "m", "formula": "sum(order_id)",
                                                "meta": {"osi_datatype": "Decimal"}}]))
        [metric] = dataset.metrics
        assert metric.datatype is not None
        assert metric.datatype.value == "Decimal"
        assert "osi_datatype" not in (metric.meta or {})


class TestShapes:
    def test_stored_join_pairs_become_key_lists_in_order(self) -> None:
        dataset = loaded(pre_dataset({"name": "a", "type": "INT"}, {"name": "b", "type": "INT"}, joins=[
            {"target_model": "parts", "join_pairs": [["b", "y"], ["a", "x"]]}]))
        [rel] = dataset.relationships
        assert (rel.to, rel.from_columns, rel.to_columns) == ("parts", ["b", "a"], ["y", "x"])

    def test_a_stored_query_takes_ossie_entries(self) -> None:
        with recorded() as caught:
            query = SlayerQuery.model_validate({
                "version": PRE_QUERY, "source_model": "orders",
                "measures": [{"formula": "sum(amount)", "name": "rev"}, "count(*)"],
                "dimensions": [{"name": "region", "model": "customers"}, {"expression": "upper(status)", "name": "s"},
                               "status"],
                "time_dimensions": [{"dimension": "ordered_at", "granularity": "month"}],
                "filters": ["amount > 1"],
                "order": [{"column": {"name": "rev"}, "direction": "desc"}],
                "extensions": [{"source_name": "orders", "joins": [
                    {"target_model": "customers", "join_pairs": [["customer_id", "id"]], "name": "buyer"}]}],
            })
        assert deprecations(caught) == []
        assert all(isinstance(m, QueryMetric) for m in query.metrics or [])
        assert all(isinstance(d, QueryDimension) for d in query.dimensions or [])
        dumped = query.model_dump(mode="json")
        assert dumped["metrics"] == [{"metric": "sum(amount)", "alias": "rev"}, "count(*)"]
        assert dumped["dimensions"] == ["customers.region", {"field": "upper(status)", "alias": "s"}, "status"]
        assert dumped["order"] == [{"field": "rev", "direction": "desc"}]
        [ext] = dumped["extensions"]
        assert ext["dataset"] == "orders"
        [rel] = ext["relationships"]
        assert (rel["from_columns"], rel["to_columns"]) == (["customer_id"], ["id"])

    @pytest.mark.parametrize("backend", ["yaml", "sqlite"])
    async def test_a_migrated_dataset_is_saved_in_ossie_shapes(self, backend: str, tmp_path: Path) -> None:
        doc = pre_dataset({"name": "customer_id", "type": "INT"}, {"name": "amount", "type": "DOUBLE"},
                          joins=[{"target_model": "customers", "join_pairs": [["customer_id", "id"]]}])
        customers = {"version": PRE_DATASET, "name": "customers", "sql_table": "customers", "data_source": DS,
                     "columns": [{"name": "id", "type": "INT", "primary_key": True}]}
        storage = await shop_store(backend=backend, base=str(tmp_path), datasets=[customers, doc])
        with recorded() as caught:
            loaded = await storage.get_dataset("orders", data_source=DS)
            assert loaded is not None
        assert deprecations(caught) == []
        stored_customers = await storage.get_dataset("customers", data_source=DS)
        assert stored_customers is not None
        await storage.save_dataset(stored_customers)  # the relationship's target is stored first
        await storage.save_dataset(loaded)
        written = ossie_dataset(backend=backend, base=str(tmp_path), name="orders")
        [rel] = stored_relationships(backend=backend, base=str(tmp_path), name="orders")
        assert (rel["from_columns"], rel["to_columns"]) == (["customer_id"], ["id"])
        assert "join_pairs" not in rel
        assert {f["name"]: f["datatype"] for f in written["fields"]}["amount"] == "Float"
