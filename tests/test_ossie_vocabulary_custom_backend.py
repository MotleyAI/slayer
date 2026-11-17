"""A storage backend written for 1.x, overriding the pre-2.0 method names, keeps working until 3.0: the new method
names reach its overrides, with one ``FutureWarning`` per backend class naming the methods to rename.

The pre-2.0 method names below are deliberate: keep this file out of the vocabulary respelling pass.

Spec: compatibility/ossie-vocabulary › Custom storage backends written for 1.x keep working.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.embeddings.models import Embedding
from slayer.memories.models import Memory
from slayer.storage.base import StorageBackend
from tests._deprecated_input_helpers import recorded
from tests._engine_helpers import owned_query_engine
from tests._ossie_vocabulary_fixtures import shop_customers
from tests._stored_upgrade_fixtures import DS, seed_shop

#: The 1.x abstract hooks a backend implements.
HOOKS = ("_save_model_impl", "get_model", "_list_all_model_identities", "_delete_model_row", "update_column_sampled")
#: The 1.x public methods the spec's backend overrides.
PUBLIC = ("save_model", "get_model", "list_models", "delete_model")


def future(caught: list[Any]) -> list[Any]:
    return [w for w in caught if issubclass(w.category, FutureWarning)]


def _hooks() -> dict[str, Any]:
    """The 1.x abstract hooks, keyed by their pre-2.0 names, each logging its call."""

    async def save_impl(self: Any, dataset: Any) -> None:
        self.log.append("_save_model_impl")
        self.rows[(dataset.data_source, dataset.name)] = dataset

    async def get(self: Any, name: str, data_source: str | None = None) -> Any:
        self.log.append("get_model")
        return next((d for (ds, n), d in self.rows.items() if n == name and data_source in (None, ds)), None)

    async def identities(self: Any) -> list[tuple[str, str]]:
        self.log.append("_list_all_model_identities")
        return list(self.rows)

    async def delete_row(self: Any, *, data_source: str, name: str) -> bool:
        self.log.append("_delete_model_row")
        return self.rows.pop((data_source, name), None) is not None

    async def sampled(self: Any, **kwargs: Any) -> None:
        self.log.append("update_column_sampled")

    return dict(zip(HOOKS, (save_impl, get, identities, delete_row, sampled), strict=True))


def _public() -> dict[str, Any]:
    """1.x overrides of the public methods, keyed by their pre-2.0 names, each logging its call."""

    async def save(self: Any, dataset: Any, **kwargs: Any) -> None:
        self.log.append("save_model")
        self.rows[(dataset.data_source, dataset.name)] = dataset

    async def get(self: Any, name: str, data_source: str | None = None) -> Any:
        self.log.append("get_model")
        return next((d for (ds, n), d in self.rows.items() if n == name and data_source in (None, ds)), None)

    async def listed(self: Any, data_source: str | None = None) -> list[str]:
        self.log.append("list_models")
        return sorted(n for ds, n in self.rows if data_source in (None, ds))

    async def delete(self: Any, name: str, data_source: str | None = None) -> bool:
        self.log.append("delete_model")
        return self.rows.pop((data_source or DS, name), None) is not None

    return dict(zip(PUBLIC, (save, get, listed, delete), strict=True))


class _Base(StorageBackend):
    """Everything but the dataset methods: datasources, memories and embeddings in memory."""

    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], Any] = {}
        self.datasources: dict[str, DatasourceConfig] = {}
        self.memories: dict[str, Memory] = {}
        self.priority: list[str] = []
        self.log: list[str] = []

    async def _save_datasource_impl(self, datasource: DatasourceConfig) -> None:
        self.datasources[datasource.name] = datasource

    async def get_datasource(self, name: str) -> DatasourceConfig | None:
        return self.datasources.get(name)

    async def list_datasources(self) -> list[str]:
        return sorted(self.datasources)

    async def _delete_datasource_row(self, name: str) -> bool:
        return self.datasources.pop(name, None) is not None

    async def get_datasource_priority(self) -> list[str]:
        return list(self.priority)

    async def _set_datasource_priority_raw(self, priority: list[str]) -> None:
        self.priority = list(priority)

    async def _save_memory_row(self, memory: Memory) -> None:
        self.memories[memory.id] = memory

    async def _get_memory_row(self, memory_id: str) -> Memory | None:
        return self.memories.get(memory_id)

    async def _list_memories_rows(self, *, entities: list[str] | None) -> tuple[list[Memory], list[Any]]:
        return list(self.memories.values()), []

    async def _delete_memory_row(self, memory_id: str) -> bool:
        return self.memories.pop(memory_id, None) is not None

    async def _next_memory_seq(self) -> str:
        return str(len(self.memories) + 1)

    async def save_embedding(self, row: Embedding) -> None:
        return None

    async def get_embedding(self, *, canonical_id: str, embedding_model_name: str) -> Embedding | None:
        return None

    async def list_embeddings(self, *, embedding_model_name: str) -> list[Embedding]:
        return []

    async def delete_embeddings_for_canonical(self, *, canonical_id_prefix: str) -> int:
        return 0


def define_backend(*, public: bool) -> type[StorageBackend]:
    """A 1.x backend class: the pre-2.0 hooks, plus the pre-2.0 public methods when ``public``."""
    namespace = {**_hooks(), **(_public() if public else {})}
    return type("Backend1x", (_Base,), namespace)


def _datasource(base: Path) -> DatasourceConfig:
    db = os.path.join(base, "shop.db")
    seed_shop(db, dialect="sqlite")
    return DatasourceConfig(name=DS, type="sqlite", database=db)


@pytest.mark.parametrize("public", [False, True], ids=["hooks", "public_methods"])
async def test_a_1x_backend_is_used_unchanged(public: bool, tmp_path: Path) -> None:
    datasource = _datasource(tmp_path)
    with recorded() as caught:
        backend: Any = define_backend(public=public)()
        async with owned_query_engine(storage=backend, datasources=[datasource]) as engine:
            await engine.save_dataset(SlayerDataset.model_validate(shop_customers()))
        loaded = await backend.get_dataset("customers", data_source=DS)
        listed = await backend.list_datasets(data_source=DS)
        deleted = await backend.delete_dataset("customers", data_source=DS)
    assert loaded is not None
    assert loaded.name == "customers"
    assert listed == ["customers"]
    assert deleted
    reached = set(PUBLIC) if public else {"_save_model_impl", "get_model", "_list_all_model_identities",
                                          "_delete_model_row"}
    assert reached <= set(backend.log)
    [w] = future(caught)
    assert "SLayer 3.0" in str(w.message)


async def test_one_warning_per_class_however_many_instances() -> None:
    with recorded() as caught:
        cls = define_backend(public=False)
        for _ in range(3):
            await cls().list_datasets()
    assert len(future(caught)) == 1


@pytest.mark.parametrize("hook", ["get_model", "_save_model_impl", "_list_all_model_identities", "_delete_model_row"])
def test_the_warning_names_the_methods_to_rename(hook: str) -> None:
    with recorded() as caught:
        define_backend(public=False)()
    [w] = future(caught)
    assert hook in str(w.message)


def test_a_backend_on_the_new_names_warns_nothing() -> None:
    with recorded() as caught:
        cls = type("Modern", (StorageBackend,), {})
    assert cls.__name__ == "Modern"
    assert future(caught) == []
