"""Datasets are dumped, saved and served in Ossie's vocabulary; the kept keys keep their names.

Spec: compatibility/ossie-vocabulary › Datasets are written in Ossie's vocabulary.

The pre-2.0 spellings below are deliberate: keep this file out of the vocabulary respelling pass.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.models import SlayerDataset
from tests._deprecated_input_helpers import recorded
from tests._ossie_vocabulary_fixtures import shop_customers, shop_orders, shop_store, stored_document
from tests._stored_upgrade_fixtures import AMOUNT_BY_REGION, BACKENDS, DS, by_dimension, run

PRE_2_0_DATASET_KEYS = {"columns", "measures", "joins"}
PRE_2_0_MEMBER_KEYS = {"type", "formula", "target_model"}
KEPT_FILTER = "status <> 'void'"
AGGREGATION = {"name": "doubled", "expression": "SUM({value}) * 2"}


def full_orders() -> dict[str, Any]:
    """``shop_orders`` plus a row filter and a custom aggregation."""
    return shop_orders(filters=[KEPT_FILTER], aggregations=[AGGREGATION])


def assert_ossie_document(doc: dict[str, Any]) -> None:
    """``doc`` spells every member the Ossie way, at the dataset and the member level."""
    assert {"fields", "metrics", "relationships"} <= doc.keys()
    assert not PRE_2_0_DATASET_KEYS & doc.keys()
    for member in [*doc["fields"], *doc["metrics"], *doc["relationships"], *doc.get("aggregations", [])]:
        assert not PRE_2_0_MEMBER_KEYS & member.keys(), member
    assert all("datatype" in f for f in doc["fields"])
    [metric] = doc["metrics"]
    assert (metric["expression"], metric["datatype"]) == ("sum(amount)", "DOUBLE")
    [rel] = doc["relationships"]
    assert (rel["to"], rel["join_pairs"]) == ("customers", [["customer_id", "id"]])


def assert_kept_keys(doc: dict[str, Any]) -> None:
    assert (doc["sql_table"], doc["data_source"], doc["filters"]) == ("orders", DS, [KEPT_FILTER])
    [aggregation] = doc["aggregations"]
    assert aggregation["expression"] == "SUM({value}) * 2"
    assert "join_pairs" in doc["relationships"][0]


class TestDump:
    @pytest.mark.parametrize("mode", ["python", "json"])
    @pytest.mark.parametrize("exclude_none", [False, True])
    def test_a_dataset_dumps_in_ossie_keys(self, mode: str, exclude_none: bool) -> None:
        doc = SlayerDataset.model_validate(full_orders()).model_dump(mode=mode, exclude_none=exclude_none)
        assert_ossie_document(doc)

    def test_kept_keys_keep_their_names(self) -> None:
        assert_kept_keys(SlayerDataset.model_validate(full_orders()).model_dump(mode="json"))

    def test_the_dump_validates_back_to_the_same_dataset(self) -> None:
        dataset = SlayerDataset.model_validate(full_orders())
        assert SlayerDataset.model_validate(dataset.model_dump(mode="json")) == dataset


@pytest.mark.parametrize("backend", BACKENDS)
class TestSavedDocument:
    async def test_the_saved_document_uses_ossie_keys(self, backend: str, tmp_path: Path) -> None:
        storage = await shop_store(backend=backend, base=str(tmp_path))
        await storage.save_dataset(SlayerDataset.model_validate(shop_customers()))
        orders = SlayerDataset.model_validate(full_orders())
        await storage.save_dataset(orders)
        doc = stored_document(backend=backend, base=str(tmp_path), name="orders")
        assert_ossie_document(doc)
        assert_kept_keys(doc)
        assert await storage.get_dataset("orders", data_source=DS) == orders

    async def test_a_saved_dataset_runs(self, backend: str, tmp_path: Path) -> None:
        storage = await shop_store(backend=backend, base=str(tmp_path))
        await storage.save_dataset(SlayerDataset.model_validate(shop_customers()))
        await storage.save_dataset(SlayerDataset.model_validate(shop_orders()))
        response = await run(storage, {"population": "orders", "dimensions": ["customers.region"],
                                       "metrics": ["sum(amount)"]})
        assert by_dimension(response, dim_suffix="region") == AMOUNT_BY_REGION


async def test_rest_serves_ossie_keys(tmp_path: Path) -> None:
    storage = await shop_store(backend="yaml", base=str(tmp_path))
    await storage.save_dataset(SlayerDataset.model_validate(shop_customers()))
    await storage.save_dataset(SlayerDataset.model_validate(full_orders()))
    resp = TestClient(create_app(storage=storage)).get(f"/datasets/orders?data_source={DS}")
    assert resp.status_code == 200, resp.text
    assert_ossie_document(resp.json())
    assert_kept_keys(resp.json())


#: A v1-shaped dataset (``{name, sql}`` metric entries become fields), in each vocabulary.
V1_SHAPED = {
    "new": {"name": "orders", "sql_table": "orders", "data_source": DS, "metrics": [{"name": "revenue", "sql": "amount"}]},
    "old": {"name": "orders", "sql_table": "orders", "data_source": DS, "measures": [{"name": "revenue", "sql": "amount"}]},
}


@pytest.mark.parametrize(("spelling", "reported"), [("new", []), ("old", [("measures", "metrics")])])
def test_a_v1_shaped_dataset_reports_only_what_it_spells(spelling: str, reported: list[tuple[str, str]]) -> None:
    with recorded():
        dataset = SlayerDataset.model_validate(V1_SHAPED[spelling])
    assert [f.name for f in dataset.fields] == ["revenue"]
    assert [(p.old, p.new) for p in dataset.deprecated_inputs] == reported
