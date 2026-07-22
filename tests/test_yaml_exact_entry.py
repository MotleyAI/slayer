"""YAML read/delete exact-name verification.

On a case-insensitive filesystem ``open`` / ``os.path.exists`` match any
case variant of a filename, so a lookup for ``Orders`` would silently hit
``orders.yaml`` — and a delete would remove it. The YAML backend compares
against ``os.listdir`` instead, so a case mismatch behaves as "not
found". These assertions hold on both case-sensitive and
case-insensitive dev machines.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator

import pytest

from slayer.core.errors import MemoryNotFoundError
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.storage.yaml_storage import YAMLStorage, _exact_entry_exists


@pytest.fixture
def base_dir() -> Iterator[str]:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def storage(base_dir: str) -> YAMLStorage:
    return YAMLStorage(base_dir=base_dir)


class TestExactEntryExists:
    def test_exact_name(self, base_dir: str) -> None:
        open(os.path.join(base_dir, "orders.yaml"), "w").close()
        assert _exact_entry_exists(base_dir, "orders.yaml") is True

    def test_case_variant_not_found(self, base_dir: str) -> None:
        open(os.path.join(base_dir, "orders.yaml"), "w").close()
        assert _exact_entry_exists(base_dir, "Orders.yaml") is False

    def test_missing_dir(self, base_dir: str) -> None:
        assert _exact_entry_exists(os.path.join(base_dir, "nope"), "x") is False


class TestModelReads:
    @pytest.fixture(autouse=True)
    async def _seed(self, storage: YAMLStorage) -> None:
        await storage.save_model(
            SlayerModel(name="orders", data_source="db", sql_table="t"),
        )

    async def test_get_case_variant_name_returns_none(
        self, storage: YAMLStorage,
    ) -> None:
        assert await storage.get_model("Orders", data_source="db") is None
        assert await storage.get_model("orders", data_source="db") is not None

    async def test_get_case_variant_data_source_returns_none(
        self, storage: YAMLStorage,
    ) -> None:
        assert await storage.get_model("orders", data_source="DB") is None

    async def test_delete_case_variant_is_noop(
        self, storage: YAMLStorage,
    ) -> None:
        assert await storage.delete_model("Orders", data_source="db") is False
        # The real file is still there.
        assert await storage.get_model("orders", data_source="db") is not None

    async def test_update_column_sampled_case_variant_raises(
        self, storage: YAMLStorage,
    ) -> None:
        with pytest.raises(ValueError, match="not found"):
            await storage.update_column_sampled(
                data_source="db",
                model_name="Orders",
                column_name="c",
                sampled=None,
                sampled_values=None,
                distinct_count=None,
            )


class TestDatasourceReads:
    @pytest.fixture(autouse=True)
    async def _seed(self, storage: YAMLStorage) -> None:
        await storage.save_datasource(
            DatasourceConfig(name="db", type="postgres", host="h"),
        )

    async def test_get_case_variant_returns_none(
        self, storage: YAMLStorage,
    ) -> None:
        assert await storage.get_datasource("DB") is None
        assert await storage.get_datasource("db") is not None

    async def test_delete_case_variant_is_noop(
        self, storage: YAMLStorage,
    ) -> None:
        assert await storage.delete_datasource("DB") is False
        assert await storage.get_datasource("db") is not None


class TestMemoryReads:
    @pytest.fixture(autouse=True)
    async def _seed(self, storage: YAMLStorage) -> None:
        await storage.save_memory(
            id="x", learning="lower", entities=["mydb.orders"],
        )

    async def test_get_case_variant_not_found(
        self, storage: YAMLStorage,
    ) -> None:
        with pytest.raises(MemoryNotFoundError):
            await storage.get_memory("X")
        assert (await storage.get_memory("x")).learning == "lower"

    async def test_delete_case_variant_not_found(
        self, storage: YAMLStorage,
    ) -> None:
        with pytest.raises(MemoryNotFoundError):
            await storage.delete_memory("X")
        # The real file is still there.
        assert (await storage.get_memory("x")).learning == "lower"
