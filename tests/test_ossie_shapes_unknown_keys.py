"""A fresh dataset with a key SLayer does not read is refused, nested parts included; a stored one drops the key with
one ``unknown_stored_key`` warning per document.

Spec: compatibility/ossie-vocabulary › Datasets refuse unknown keys.
"""

from __future__ import annotations

import copy
import warnings
from pathlib import Path
from collections.abc import Callable
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from slayer.api.server import create_app
from slayer.client.slayer_client import SlayerClient
from slayer.core.errors import UnknownKeyError
from slayer.core.models import Aggregation, AggregationParam, DatasetField, DatasetMetric, DatasetRelationship, SlayerDataset
from slayer.core.warnings import SlayerWarning
from slayer.storage.migrations import CURRENT_VERSIONS
from tests._deprecated_input_helpers import errors_in, recorded
from tests._engine_helpers import owned_query_engine
from tests._model_access_fixtures import mcp_text
from tests._ossie_vocabulary_fixtures import shop_customers, shop_orders, shop_store, stored_document
from tests._stored_upgrade_fixtures import DS

#: Where a stray key goes in ``orders()``, by the dataset-side model holding it.
LOCATIONS: dict[str, tuple[tuple[str | int, ...], str]] = {
    "dataset": ((), "stray"),
    "field": (("fields", 0), "fields[0].stray"),
    "metric": (("metrics", 0), "metrics[0].stray"),
    "relationship": (("relationships", 0), "relationships[0].stray"),
    "aggregation": (("aggregations", 0), "aggregations[0].stray"),
    "aggregation_parameter": (("aggregations", 0, "params", 0), "aggregations[0].params[0].stray"),
}


def orders(**extra: Any) -> dict[str, Any]:
    """``shop_orders`` with a parametrised custom aggregation."""
    return shop_orders(aggregations=[{"name": "scaled", "expression": "SUM({value}) * {k}",
                                      "params": [{"name": "k", "sql": "2"}]}], **extra)


def with_stray(doc: dict[str, Any], location: str, *, key: str = "stray") -> dict[str, Any]:
    out = copy.deepcopy(doc)
    node: Any = out
    for step in LOCATIONS[location][0]:
        node = node[step]
    node[key] = 1
    return out


def unknown_key(exc: BaseException) -> UnknownKeyError:
    [err] = [e for e in errors_in(exc) if isinstance(e, UnknownKeyError)]
    return err


def stored_payloads(caught: list[warnings.WarningMessage]) -> list[SlayerWarning]:
    carried = [getattr(w.message, "payload", None) for w in caught]
    return [p for p in carried if isinstance(p, SlayerWarning) and p.kind == "unknown_stored_key"]


def current(doc: dict[str, Any]) -> dict[str, Any]:
    return {"version": CURRENT_VERSIONS["SlayerModel"], **doc}


def _placed(slot: str, build: Callable[[], Any]) -> SlayerDataset:
    """A fresh dataset holding ``build()``'s object in ``slot`` (building either may refuse)."""
    return SlayerDataset.model_validate({"name": "orders", "sql_table": "orders", "data_source": DS, slot: [build()]})


class TestFresh:
    @pytest.mark.parametrize("location", sorted(LOCATIONS))
    def test_a_stray_key_is_refused_naming_its_path(self, location: str) -> None:
        raw = with_stray(orders(), location)
        with pytest.raises(ValidationError) as info:
            SlayerDataset.model_validate(raw)
        err = unknown_key(info.value)
        assert (err.key, err.path, err.dataset) == ("stray", LOCATIONS[location][1], "orders")
        for part in ("stray", LOCATIONS[location][1], "orders"):
            assert part in str(err)

    @pytest.mark.parametrize("key", ["ai_context", "unique_keys", "nonportable_extensions"])
    def test_an_ossie_key_slayer_does_not_read_yet_is_refused(self, key: str) -> None:
        raw = {**orders(), key: []}
        with pytest.raises(ValidationError) as info:
            SlayerDataset.model_validate(raw)
        assert unknown_key(info.value).key == key

    def test_a_1x_key_converts_before_the_check(self) -> None:
        raw = orders()
        raw["columns"] = raw.pop("fields")
        raw["columns"][0]["type"] = raw["columns"][0].pop("datatype")
        with recorded():
            dataset = SlayerDataset.model_validate(raw)
        assert dataset.fields[0].name == "order_id"

    @pytest.mark.parametrize(("slot", "build"), [
        pytest.param("fields", lambda: DatasetField(name="x", datatype="Integer", stray=1), id="field"),  # pyright: ignore[reportCallIssue]  # deliberate stray key
        pytest.param("metrics", lambda: DatasetMetric(name="m", expression="count(*)", stray=1), id="metric"),  # pyright: ignore[reportCallIssue]  # deliberate stray key
        pytest.param("relationships", lambda: DatasetRelationship(
            to="customers", from_columns=["customer_id"], to_columns=["id"], stray=1), id="relationship"),  # pyright: ignore[reportCallIssue]  # deliberate stray key
        pytest.param("aggregations", lambda: Aggregation(name="a", expression="SUM({value})", stray=1),  # pyright: ignore[reportCallIssue]  # deliberate stray key
                     id="aggregation"),
        pytest.param("aggregations", lambda: Aggregation(name="a", expression="SUM({value}) * {k}",
                                                         params=[AggregationParam(name="k", sql="2", stray=1)]),  # pyright: ignore[reportCallIssue]  # deliberate stray key
                     id="aggregation_parameter"),
    ])
    def test_a_prebuilt_nested_object_keeps_its_stray_key_for_the_refusal(self, slot: str, build: Any) -> None:
        with pytest.raises((ValidationError, UnknownKeyError)) as info:
            _placed(slot, build)
        assert unknown_key(info.value).key == "stray"


class TestFreshSurfaces:
    BAD = {"primaryKey": True}

    def bad_orders(self) -> dict[str, Any]:
        raw = shop_orders()
        raw["fields"][0] = {**raw["fields"][0], **self.BAD}
        return raw

    async def test_mcp_refuses(self, tmp_path: Path) -> None:
        storage = await shop_store(backend="yaml", base=str(tmp_path))
        raw = self.bad_orders()
        text = await mcp_text(storage, "create_dataset", name="orders", sql_table="orders", data_source=DS,
                              fields=raw["fields"], metrics=raw["metrics"])
        for part in ("primaryKey", "fields[0]", "orders"):
            assert part in text, text
        assert await storage.get_dataset("orders", data_source=DS) is None

    async def test_rest_refuses(self, tmp_path: Path) -> None:
        storage = await shop_store(backend="yaml", base=str(tmp_path))
        resp = TestClient(create_app(storage=storage)).post("/datasets", json=self.bad_orders())
        assert 400 <= resp.status_code < 500
        for part in ("primaryKey", "fields[0]", "orders"):
            assert part in resp.text, resp.text
        assert await storage.get_dataset("orders", data_source=DS) is None

    async def test_the_python_client_refuses(self, tmp_path: Path) -> None:
        storage = await shop_store(backend="yaml", base=str(tmp_path))
        client, bad = SlayerClient(storage=storage), self.bad_orders()
        with pytest.raises((ValidationError, UnknownKeyError)) as info:
            await client.create_dataset(bad)
        err = unknown_key(info.value)
        assert (err.key, err.path, err.dataset) == ("primaryKey", "fields[0].primaryKey", "orders")


class TestStored:
    @pytest.mark.parametrize("location", sorted(LOCATIONS))
    def test_a_stored_stray_key_is_dropped_with_one_warning(self, location: str) -> None:
        with recorded() as caught:
            dataset = SlayerDataset.model_validate(current(with_stray(orders(), location)))
        assert "stray" not in str(dataset.model_dump(mode="json"))
        [payload] = stored_payloads(caught)
        dumped = payload.model_dump(mode="json")
        assert (dumped["dataset"], dumped["paths"]) == ("orders", [LOCATIONS[location][1]])

    @pytest.mark.parametrize("backend", ["yaml", "sqlite"])
    async def test_two_stored_keys_load_with_one_warning(self, backend: str, tmp_path: Path) -> None:
        doc = current(with_stray(with_stray(orders(), "relationship", key="weight"), "dataset", key="x_owner"))
        storage = await shop_store(backend=backend, base=str(tmp_path),
                                   datasets=[current(shop_customers()), doc])
        with recorded() as caught:
            dataset = await storage.get_dataset("orders", data_source=DS)
        assert dataset is not None
        dumped = dataset.model_dump(mode="json")
        assert "x_owner" not in dumped
        assert "weight" not in dumped["relationships"][0]
        [payload] = stored_payloads(caught)
        assert payload.model_dump(mode="json")["dataset"] == "orders"
        assert sorted(payload.model_dump(mode="json")["paths"]) == ["relationships[0].weight", "x_owner"]

    async def test_a_query_reports_the_dropped_keys(self, tmp_path: Path) -> None:
        doc = current(with_stray(orders(), "dataset", key="x_owner"))
        storage = await shop_store(backend="yaml", base=str(tmp_path), datasets=[current(shop_customers()), doc])
        async with owned_query_engine(storage=storage) as engine:
            with recorded():
                response = await engine.execute({"population": "orders", "metrics": ["count(orders.*)"]})
        [entry] = [w for w in response.warnings or [] if getattr(w, "kind", None) == "unknown_stored_key"]
        assert entry.model_dump(mode="json")["paths"] == ["x_owner"]

    async def test_a_migrated_document_is_written_back_without_the_key(self, tmp_path: Path) -> None:
        old = {**with_stray(orders(), "field", key="colour"), "version": CURRENT_VERSIONS["SlayerModel"] - 1}
        storage = await shop_store(backend="yaml", base=str(tmp_path), datasets=[current(shop_customers()), old])
        with recorded():
            assert await storage.get_dataset("orders", data_source=DS) is not None
        doc = stored_document(backend="yaml", base=str(tmp_path), name="orders")
        assert doc["version"] == CURRENT_VERSIONS["SlayerModel"]
        assert "colour" not in doc["fields"][0]

    def test_a_stored_query_backed_dataset_warns_once(self) -> None:
        doc = current({"name": "by_status", "data_source": DS, "x_owner": "team",
                       "source_queries": [{"population": "orders", "dimensions": ["orders.status"], "metrics": ["count(orders.*)"]}]})
        with recorded() as caught:
            dataset = SlayerDataset.model_validate(doc)
        assert len(dataset.source_queries or []) == 1
        assert len(stored_payloads(caught)) == 1
