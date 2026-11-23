"""Saves re-validate the whole dataset or datasource.

Spec: models/save-validation › Saves re-validate the whole dataset or datasource.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

import slayer.storage.base as storage_base
from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetMetric, DatasourceConfig, SlayerDataset
from slayer.sql import connection_policy
from slayer.storage.base import StorageBackend

from tests._dev2243_fixtures import DS, SALES, STORAGES, dataset, document_engine, document_text, field, metric
from tests._ossie_vocabulary_fixtures import shop_customers, shop_orders, shop_store
from tests._validation_refusals import construction_refusal, refusal, state


def _orders_data() -> dict[str, Any]:
    data = shop_orders()
    for f in data["fields"]:
        if f["name"] == "amount":
            f["allowed_aggregations"] = ["sum"]
    return data


async def _store(kind: str, tmp_path: Path) -> StorageBackend:
    storage = await shop_store(backend=kind, base=str(tmp_path))
    await storage.save_dataset(SlayerDataset.model_validate(shop_customers()))
    return storage


def _calls(monkeypatch: pytest.MonkeyPatch, owner: Any, name: str) -> list[Any]:
    """Record every call of ``owner.name``, still running it."""
    calls: list[Any] = []
    original = getattr(owner, name)

    def spy(*args: Any, **kwargs: Any) -> Any:
        calls.append((args, kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(owner, name, spy)
    return calls


def _duplicate_field(ds: SlayerDataset, data: dict[str, Any]) -> None:
    ds.fields.append(DatasetField(name="amount", datatype=DataType.FLOAT))
    data["fields"].append({"name": "amount", "datatype": "Float"})


def _one_sided_key(ds: SlayerDataset, data: dict[str, Any]) -> None:
    ds.relationships[0].from_columns.append("status")
    data["relationships"][0]["from_columns"].append("status")


def _non_field(ds: SlayerDataset, data: dict[str, Any]) -> None:
    ds.fields.append(42)  # pyright: ignore[reportArgumentType]
    data["fields"].append(42)


def _bogus_aggregation(ds: SlayerDataset, data: dict[str, Any]) -> None:
    amount = ds.get_field("amount")
    assert amount is not None
    assert amount.allowed_aggregations is not None
    amount.allowed_aggregations.append("bogus")
    next(f for f in data["fields"] if f["name"] == "amount")["allowed_aggregations"].append("bogus")


IN_PLACE: dict[str, Callable[[SlayerDataset, dict[str, Any]], None]] = {
    "duplicate_field": _duplicate_field,
    "one_sided_key": _one_sided_key,
    "non_field": _non_field,
    "bogus_aggregation": _bogus_aggregation,
}


class TestDatasetChangedInPlace:
    @pytest.mark.parametrize("kind", STORAGES)
    @pytest.mark.parametrize("case", sorted(IN_PLACE))
    async def test_the_save_is_refused_before_any_work(
        self, kind: str, case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        storage = await _store(kind, tmp_path)
        orders, data = SlayerDataset.model_validate(_orders_data()), _orders_data()
        IN_PLACE[case](orders, data)
        writes = _calls(monkeypatch, storage, "_save_dataset_impl")
        peers = _calls(monkeypatch, storage, "load_datasets")
        canonical = _calls(monkeypatch, storage_base, "with_canonical_spellings")
        with pytest.raises(Exception) as caught:  # noqa: PT011 — compared with construction's refusal below
            await storage.save_dataset(orders)
        assert refusal(caught.value) == construction_refusal(SlayerDataset, data)
        assert (writes, peers, canonical) == ([], [], [])
        assert await storage.get_dataset("orders", data_source=DS) is None


class TestModelOfAnotherClass:
    @pytest.mark.parametrize("kind", STORAGES)
    async def test_a_metric_placed_among_the_fields_refuses_the_save(
        self, kind: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        storage = await _store(kind, tmp_path)
        orders = SlayerDataset.model_validate(_orders_data())
        orders.fields.append(DatasetMetric(name="doubled", expression="amount"))  # pyright: ignore[reportArgumentType]
        held = {name: orders.__dict__[name] for name in SlayerDataset.model_fields}
        writes = _calls(monkeypatch, storage, "_save_dataset_impl")
        with pytest.raises(Exception) as caught:  # noqa: PT011 — compared with construction's refusal below
            await storage.save_dataset(orders)
        assert refusal(caught.value) == construction_refusal(SlayerDataset, held)
        assert writes == []
        assert await storage.get_dataset("orders", data_source=DS) is None


class TestDocumentExtensions:
    @pytest.mark.parametrize("kind", STORAGES)
    async def test_a_duplicate_field_refuses_the_extensions_save(
        self, kind: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async with document_engine(kind=kind) as engine:
            storage = engine.storage
            orders = await storage.get_dataset("orders", data_source=DS)
            assert orders is not None
            data = orders.model_dump()
            _duplicate_field(orders, data)
            before = await storage.get_semantic_model_text("sales", data_source=DS)
            owners = _calls(monkeypatch, storage, "_owner_of")
            patches = _calls(monkeypatch, storage, "_patch_semantic_model_text")
            with pytest.raises(Exception) as caught:  # noqa: PT011 — compared with construction's refusal below
                await storage.save_dataset_extensions(orders)
            work = (list(owners), list(patches))
            after = await storage.get_semantic_model_text("sales", data_source=DS)
        assert refusal(caught.value) == construction_refusal(SlayerDataset, data)
        assert work == ([], [])
        assert after == before


class TestDatasourceChangedInPlace:
    @pytest.mark.parametrize("kind", STORAGES)
    async def test_a_non_granularity_refuses_the_save(
        self, kind: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        storage = await _store(kind, tmp_path)
        datasource = await storage.get_datasource(DS)
        assert datasource is not None
        data = {**datasource.model_dump(), "granularities": ["bogus"]}
        datasource.granularities.append("bogus")  # pyright: ignore[reportArgumentType]
        writes = _calls(monkeypatch, storage, "_save_datasource_impl")
        rules = _calls(monkeypatch, DatasourceConfig, "check_granularities")
        policy = _calls(monkeypatch, connection_policy, "check_transaction_pooler")
        with pytest.raises(Exception) as caught:  # noqa: PT011 — compared with construction's refusal below
            await storage.save_datasource(datasource)
        assert refusal(caught.value) == construction_refusal(DatasourceConfig, data)
        assert (writes, rules, policy) == ([], [], [])
        stored = await storage.get_datasource(DS)
        assert stored is not None
        assert stored.granularities == []


#: ``sales`` whose ``orders`` carries a dataset metric SLayer cannot evaluate yet (a window function).
WINDOWED_ORDERS = document_text(
    datasets=[
        dataset("orders", primary_key=["order_id"],
                fields=[field("order_id"), field("customer_id"), field("amount", datatype="Float")],
                metrics=[metric("running", "SUM(amount) OVER ()")]),
    ],
)


class TestRuntimeOnlyState:
    @pytest.mark.parametrize("kind", STORAGES)
    async def test_a_metric_with_a_load_warning_saves_unchanged(self, kind: str) -> None:
        async with document_engine(kind=kind, documents=(WINDOWED_ORDERS,)) as engine:
            storage = engine.storage
            orders = await storage.get_dataset("orders", data_source=DS)
            assert orders is not None
            running = orders.get_metric("running")
            assert running is not None
            assert running.load_warning is not None
            before = state(orders)
            await storage.save_dataset(orders)
            reloaded = await storage.get_dataset("orders", data_source=DS)
        assert state(orders) == before
        assert reloaded is not None
        assert reloaded.model_dump() == orders.model_dump()

    @pytest.mark.parametrize("kind", STORAGES)
    async def test_a_document_dataset_saves_unchanged(self, kind: str) -> None:
        async with document_engine(kind=kind, documents=(SALES,)) as engine:
            storage = engine.storage
            customers = await storage.get_dataset("customers", data_source=DS)
            assert customers is not None
            assert customers.semantic_model == "sales"
            text = await storage.get_semantic_model_text("sales", data_source=DS)
            before = state(customers)
            await storage.save_dataset(customers)
            after = await storage.get_semantic_model_text("sales", data_source=DS)
        assert state(customers) == before
        assert after == text
