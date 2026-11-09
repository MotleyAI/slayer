"""The StorageBackend hook contract: ``_save_datasource_impl`` writes, ``_list_memories_rows`` returns
``(memories, failed_rows)``; 0.9.x-style subclasses are not adapted. Spec: storage/custom-backends."""

import contextlib
import warnings
from collections.abc import Generator

import pytest

from slayer.core.errors import GranularityDefinitionError, StoredDocumentLoadError
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.warnings import SlayerUnloadableDocumentWarning
from slayer.embeddings.models import Embedding
from slayer.memories.models import Memory
from slayer.storage.base import StorageBackend
from slayer.storage.document_loading import DocumentLoadFailures, Loaded
from slayer.storage.yaml_storage import YAMLStorage


def _datasource(*, granularities: list[dict] | None = None) -> DatasourceConfig:
    return DatasourceConfig.model_validate({
        "name": "shop", "type": "sqlite", "database": ":memory:", "granularities": granularities or [],
    })


def _valid() -> DatasourceConfig:
    return _datasource(granularities=[{"name": "fortnight", "base": "week", "multiple": 2}])


def _invalid() -> DatasourceConfig:
    return _datasource(granularities=[{"name": "year", "base": "day"}])


@contextlib.contextmanager
def _recorded() -> Generator[list[warnings.WarningMessage]]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        yield caught


def _future(caught: list[warnings.WarningMessage]) -> list[str]:
    return [str(w.message) for w in caught if issubclass(w.category, FutureWarning)]


def _matching(entities: list[str] | None, rows: dict[str, dict]) -> list[dict]:
    if entities is None:
        return list(rows.values())
    return [raw for raw in rows.values() if set(raw.get("entities") or []) & set(entities)]


class _DictStore(StorageBackend):
    """Dict-backed hooks unchanged since 0.9.12; subclasses add the datasource write and memory listing."""

    def __init__(self) -> None:
        self.models: dict[tuple[str, str], SlayerModel] = {}
        self.datasources: dict[str, DatasourceConfig] = {}
        self.memories: dict[str, dict] = {}
        self.embeddings: dict[tuple[str, str], Embedding] = {}
        self.priority: list[str] = []
        self.log: list[str] = []

    async def _save_model_impl(self, model: SlayerModel) -> None:
        self.models[(model.data_source, model.name)] = model

    async def _list_all_model_identities(self) -> list[tuple[str, str]]:
        return list(self.models)

    async def get_model(self, name: str, data_source: str | None = None) -> SlayerModel | None:
        return next((m for (ds, n), m in self.models.items() if n == name and data_source in (None, ds)), None)

    async def _delete_model_row(self, *, data_source: str, name: str) -> bool:
        return self.models.pop((data_source, name), None) is not None

    async def update_column_sampled(
        self, *, data_source: str, model_name: str, column_name: str,
        sampled: str | None, sampled_values: list[str] | None, distinct_count: int | None,
    ) -> None:
        raise ValueError(model_name)

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
        self.memories[memory.id] = memory.model_dump(mode="json")

    async def _get_memory_row(self, memory_id: str) -> Memory | None:
        raw = self.memories.get(memory_id)
        return None if raw is None else Memory.model_validate(raw)

    async def _delete_memory_row(self, memory_id: str) -> bool:
        return self.memories.pop(memory_id, None) is not None

    async def _next_memory_seq(self) -> str:
        return str(max((int(k) for k in self.memories if k.isdigit()), default=0) + 1)

    async def save_embedding(self, row: Embedding) -> None:
        self.embeddings[(row.canonical_id, row.embedding_model_name)] = row

    async def get_embedding(self, *, canonical_id: str, embedding_model_name: str) -> Embedding | None:
        return self.embeddings.get((canonical_id, embedding_model_name))

    async def list_embeddings(self, *, embedding_model_name: str) -> list[Embedding]:
        return [e for (_, m), e in self.embeddings.items() if m == embedding_model_name]

    async def delete_embeddings_for_canonical(self, *, canonical_id_prefix: str) -> int:
        doomed = [k for k in self.embeddings if k[0] == canonical_id_prefix or k[0].startswith(f"{canonical_id_prefix}.")]
        for key in doomed:
            del self.embeddings[key]
        return len(doomed)


def _legacy_cls() -> type[_DictStore]:
    class Legacy09Backend(_DictStore):
        async def save_datasource(self, datasource: DatasourceConfig) -> None:
            self.log.append("body")
            self.datasources[datasource.name] = datasource

        async def _list_memories_rows(  # pyright: ignore[reportIncompatibleMethodOverride]
            self, *, entities: list[str] | None,
        ) -> list[Memory]:
            return [Memory.model_validate(raw) for raw in _matching(entities, self.memories)]

    return Legacy09Backend


def _new_style_cls() -> type[_DictStore]:
    class NewStyleBackend(_DictStore):
        async def _save_datasource_impl(self, datasource: DatasourceConfig) -> None:
            self.log.append("impl")
            self.datasources[datasource.name] = datasource

        async def _list_memories_rows(self, *, entities: list[str] | None) -> Loaded[Memory]:
            rows: list[Memory] = []
            failed: list[StoredDocumentLoadError] = []
            for raw in _matching(entities, self.memories):
                try:
                    rows.append(await self._memory_from_stored(memory_id=str(raw["id"]), decode=lambda raw=raw: raw))
                except StoredDocumentLoadError as exc:
                    failed.append(exc)
            return rows, failed

    return NewStyleBackend


@pytest.fixture
def spy_check(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Shared call log; ``DatasourceConfig.check_granularities`` appends ``"check"`` to it."""
    log: list[str] = []
    original = DatasourceConfig.check_granularities

    def check(self: DatasourceConfig) -> None:
        log.append("check")
        original(self)

    monkeypatch.setattr(DatasourceConfig, "check_granularities", check)
    return log


async def test_new_style_backend_untouched(spy_check: list[str]) -> None:
    with _recorded() as caught:
        cls = _new_style_cls()
    assert _future(caught) == []
    backend = cls()
    backend.log = spy_check
    await backend.save_datasource(_valid())
    assert spy_check == ["check", "impl"]
    invalid = _invalid()
    with pytest.raises(GranularityDefinitionError):
        await backend.save_datasource(invalid)
    assert spy_check == ["check", "impl", "check"]

async def test_bundled_backend_subclass_override_untouched(tmp_path) -> None:
    with _recorded() as caught:
        class AuditedYAMLStorage(YAMLStorage):
            log: list[str] = []

            async def save_datasource(self, datasource: DatasourceConfig) -> None:
                self.log.append("override")
                await super().save_datasource(datasource)
    assert _future(caught) == []
    backend = AuditedYAMLStorage(base_dir=str(tmp_path))
    backend.log = []
    await backend.save_datasource(_valid())
    assert backend.log == ["override"]
    assert await backend.get_datasource("shop") == _valid()
    invalid = _invalid()
    with pytest.raises(GranularityDefinitionError):
        await backend.save_datasource(invalid)


def test_a_09x_style_backend_does_not_instantiate() -> None:
    with _recorded():
        cls = _legacy_cls()
    with pytest.raises(TypeError, match="_save_datasource_impl"):
        cls()


async def _store_memories(*, backend: StorageBackend, count: int) -> list[Memory]:
    return [await backend.save_memory(learning=f"note {i}", entities=["shop.orders"]) for i in range(count)]


def _ids(memories: list[Memory]) -> list[str]:
    return sorted(m.id for m in memories)


async def test_new_style_backend_keeps_failure_reporting() -> None:
    backend = _new_style_cls()()
    good = await backend.save_memory(learning="fine", entities=["shop.orders"])
    backend.memories["9"] = {"id": "9", "learning": None}
    with _recorded() as caught:
        loaded, failed = await backend.load_memories()
        failures = DocumentLoadFailures()
        listed = await backend.list_memories(failures=failures)
    assert _future(caught) == []
    assert _ids(loaded) == [good.id]
    assert [e.name for e in failed] == ["9"]
    assert _ids(listed) == [good.id]
    assert [e.name for e in failures.errors] == ["9"]
    assert any(issubclass(w.category, SlayerUnloadableDocumentWarning) for w in caught)


class SelfBuildingBackend(_DictStore):
    """Builds datasources from its own records; models and memories decode through the base load pipeline."""

    def __init__(self) -> None:
        super().__init__()
        self.raw_models: dict[tuple[str, str], dict] = {}
        self.calls: list[str] = []

    async def _save_datasource_impl(self, datasource: DatasourceConfig) -> None:
        self.datasources[datasource.name] = datasource

    async def _save_model_impl(self, model: SlayerModel) -> None:
        self.raw_models[(model.data_source, model.name)] = model.model_dump(mode="json", exclude_none=True)

    async def _list_all_model_identities(self) -> list[tuple[str, str]]:
        return list(self.raw_models)

    async def get_model(self, name: str, data_source: str | None = None) -> SlayerModel | None:
        for (ds, n), raw in self.raw_models.items():
            if n == name and data_source in (None, ds):
                return await self._migrate_and_refine_on_load(name=n, data=raw, data_source=ds)
        return None

    async def get_datasource(self, name: str) -> DatasourceConfig | None:
        self.calls.append("get_datasource")
        return await super().get_datasource(name)

    async def list_datasources(self) -> list[str]:
        self.calls.append("list_datasources")
        return await super().list_datasources()

    async def _raw_datasource_dicts(self) -> dict[str, dict]:
        self.calls.append("_raw_datasource_dicts")
        return await super()._raw_datasource_dicts()

    async def _get_memory_row(self, memory_id: str) -> Memory | None:
        raw = self.memories.get(memory_id)
        return None if raw is None else await self._memory_from_stored(memory_id=memory_id, decode=lambda: raw)

    async def _list_memories_rows(self, *, entities: list[str] | None) -> Loaded[Memory]:
        rows = [await self._memory_from_stored(memory_id=str(raw["id"]), decode=lambda raw=raw: raw)
                for raw in _matching(entities, self.memories)]
        return rows, []


def _self_building_backend() -> SelfBuildingBackend:
    """A fresh instance holding datasource ``shop``, model ``shop.orders`` and memory ``1``."""
    backend = SelfBuildingBackend()
    backend.datasources["shop"] = _valid()
    model = SlayerModel.model_validate({"name": "orders", "data_source": "shop", "sql_table": "orders",
                                        "columns": [{"name": "id", "type": "INT", "primary_key": True}]})
    backend.raw_models[("shop", "orders")] = model.model_dump(mode="json", exclude_none=True)
    memory = Memory.model_validate({"id": "1", "learning": "note", "entities": ["shop.orders"]})
    backend.memories["1"] = memory.model_dump(mode="json")
    return backend


class TestLibraryGranularitySettle:
    @pytest.mark.parametrize("first", ["model", "memory"])
    async def test_self_building_backend_never_lists_or_loads_datasources(self, first: str) -> None:
        backend = _self_building_backend()

        async def load_model() -> None:
            assert await backend.get_model("orders", data_source="shop") is not None
            models, failed = await backend.load_models()
            assert ([m.name for m in models], failed) == (["orders"], [])

        async def load_memory() -> None:
            assert (await backend.get_memory("1")).learning == "note"
            memories, failed = await backend.load_memories()
            assert ([m.id for m in memories], failed) == (["1"], [])

        loads = [load_model, load_memory] if first == "model" else [load_memory, load_model]
        for load in loads:
            await load()
        assert backend.calls == ["_raw_datasource_dicts"]
