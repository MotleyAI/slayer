"""StorageBackend subclasses written against the 0.9.x hook contract keep working until 2.0."""

import contextlib
import inspect
from abc import abstractmethod
import re
import warnings
from collections.abc import Callable, Generator
from typing import Any

import pytest

from slayer.core.errors import GranularityDefinitionError, StoredDocumentLoadError
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.warnings import SlayerUnloadableDocumentWarning
from slayer.embeddings.models import Embedding
from slayer.memories.models import Memory
from slayer.storage.base import StorageBackend
from slayer.storage.access_filtered import AccessFilteredStorage
from slayer.storage.document_loading import DocumentLoadFailures, Loaded
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage

_SHAPE = "(memories, failed_rows)"


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


@contextlib.contextmanager
def _quiet() -> Generator[None]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        yield


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


def _list_rows_cls() -> type[_DictStore]:
    class ListRowsBackend(_DictStore):
        async def _save_datasource_impl(self, datasource: DatasourceConfig) -> None:
            self.datasources[datasource.name] = datasource

        async def _list_memories_rows(  # pyright: ignore[reportIncompatibleMethodOverride]
            self, *, entities: list[str] | None,
        ) -> list[Memory]:
            return [Memory.model_validate(raw) for raw in _matching(entities, self.memories)]

    return ListRowsBackend


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


def _returning_cls(result: Callable[[], Any]) -> type[_DictStore]:
    class BadShapeBackend(_DictStore):
        async def _save_datasource_impl(self, datasource: DatasourceConfig) -> None:
            self.datasources[datasource.name] = datasource

        async def _list_memories_rows(self, *, entities: list[str] | None) -> Any:
            return result()

    return BadShapeBackend


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


class TestLegacySaveDatasource:
    def test_definition_warns_once_and_class_instantiates(self) -> None:
        with _recorded() as caught:
            cls = _legacy_cls()
        messages = [w for w in caught if issubclass(w.category, FutureWarning)]
        assert len(messages) == 1
        text = str(messages[0].message)
        for needle in ("Legacy09Backend", "save_datasource", "_save_datasource_impl", "2.0"):
            assert needle in text
        assert messages[0].filename == __file__
        assert messages[0].lineno == _legacy_cls.__code__.co_firstlineno + 1
        assert cls.__abstractmethods__ == frozenset()
        with _recorded() as caught:
            cls()
        assert _future(caught) == []

    async def test_save_validates_then_runs_old_body(self, spy_check: list[str]) -> None:
        with _quiet():
            backend = _legacy_cls()()
        backend.log = spy_check
        with _recorded() as caught:
            await backend.save_datasource(_valid())
        assert spy_check == ["check", "body"]
        assert await backend.get_datasource("shop") == _valid()
        assert _future(caught) == []

    async def test_invalid_granularity_rejected_before_old_body(self) -> None:
        with _quiet():
            backend = _legacy_cls()()
        invalid = _invalid()
        with pytest.raises(GranularityDefinitionError):
            await backend.save_datasource(invalid)
        assert backend.log == []
        assert backend.datasources == {}

    @pytest.mark.parametrize("calls_super", [True, False])
    async def test_legacy_child_of_legacy_backend_validates(self, calls_super: bool) -> None:
        with _quiet():
            root = _legacy_cls()
        with _recorded() as caught:
            class Legacy09Child(root):
                async def save_datasource(self, datasource: DatasourceConfig) -> None:
                    self.log.append("child")
                    if calls_super:
                        await super().save_datasource(datasource)
                    else:
                        self.datasources[datasource.name] = datasource
        child_warnings = _future(caught)
        assert len(child_warnings) == 1
        for needle in ("Legacy09Child", "save_datasource", "_save_datasource_impl", "2.0"):
            assert needle in child_warnings[0]
        backend = Legacy09Child()  # pyright: ignore[reportAbstractUsage]
        invalid = _invalid()
        with pytest.raises(GranularityDefinitionError):
            await backend.save_datasource(invalid)
        assert backend.log == []
        assert backend.datasources == {}
        await backend.save_datasource(_valid())
        assert backend.log == (["child", "body"] if calls_super else ["child"])
        assert await backend.get_datasource("shop") == _valid()

    async def test_legacy_backend_below_abstract_redeclaration_is_adapted(self, spy_check: list[str]) -> None:
        class ReDeclaringBase(_DictStore):
            @abstractmethod
            async def _save_datasource_impl(self, datasource: DatasourceConfig) -> None: ...

        with _recorded() as caught:
            class Legacy09OnAbstractBase(ReDeclaringBase):
                async def save_datasource(self, datasource: DatasourceConfig) -> None:
                    self.log.append("body")
                    self.datasources[datasource.name] = datasource

                async def _list_memories_rows(self, *, entities: list[str] | None) -> Loaded[Memory]:
                    return [], []
        assert len(_future(caught)) == 1
        assert Legacy09OnAbstractBase.__abstractmethods__ == frozenset()
        backend = Legacy09OnAbstractBase()  # pyright: ignore[reportAbstractUsage]
        backend.log = spy_check
        await backend.save_datasource(_valid())
        assert spy_check == ["check", "body"]

    async def test_new_style_backend_untouched(self, spy_check: list[str]) -> None:
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

    async def test_bundled_backend_subclass_override_untouched(self, tmp_path) -> None:
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


async def _store_memories(*, backend: StorageBackend, count: int) -> list[Memory]:
    return [await backend.save_memory(learning=f"note {i}", entities=["shop.orders"]) for i in range(count)]


def _ids(memories: list[Memory]) -> list[str]:
    return sorted(m.id for m in memories)


class TestListMemoriesRowsShape:
    @pytest.mark.parametrize("count", [0, 1, 2, 3])
    async def test_list_returning_backend_reads_every_count(self, count: int) -> None:
        backend = _list_rows_cls()()
        stored = await _store_memories(backend=backend, count=count)
        with _quiet():
            listed = await backend.list_memories()
            loaded, failed = await backend.load_memories()
        assert isinstance(listed, list)
        assert _ids(listed) == _ids(stored)
        assert isinstance(loaded, list)
        assert _ids(loaded) == _ids(stored)
        assert failed == []

    async def test_list_returning_backend_filters_by_entities(self) -> None:
        backend = _list_rows_cls()()
        kept = await backend.save_memory(learning="kept", entities=["shop.orders"])
        await backend.save_memory(learning="other", entities=["shop.customers"])
        with _quiet():
            listed = await backend.list_memories(entities=["shop.orders"])
        assert _ids(listed) == [kept.id]

    async def test_old_shape_warns_once_per_class(self) -> None:
        cls = _list_rows_cls()
        backend = cls()
        await _store_memories(backend=backend, count=2)
        with _recorded() as caught:
            for _ in range(3):
                await backend.list_memories()
            await backend.load_memories()
            await cls().list_memories()
        messages = _future(caught)
        assert len(messages) == 1
        for needle in ("ListRowsBackend", "_list_memories_rows", _SHAPE, "2.0"):
            assert needle in messages[0]

    async def test_old_shape_warning_survives_an_error_filter(self) -> None:
        backend = _list_rows_cls()()
        with warnings.catch_warnings():
            warnings.simplefilter("error", FutureWarning)
            with pytest.raises(FutureWarning):
                await backend.list_memories()
        with _recorded() as caught:
            await backend.list_memories()
            await backend.list_memories()
        assert len(_future(caught)) == 1

    async def test_inheriting_subclass_shares_defining_class_warning(self) -> None:
        parent = _list_rows_cls()

        class InheritingBackend(parent):
            pass

        with _recorded() as caught:
            await InheritingBackend().list_memories()  # pyright: ignore[reportAbstractUsage]
            await parent().list_memories()
        messages = _future(caught)
        assert len(messages) == 1
        assert "ListRowsBackend" in messages[0]

    @pytest.mark.parametrize("method", ["list_memories", "load_memories"])
    async def test_old_shape_load_failure_raises(self, method: str) -> None:
        backend = _list_rows_cls()()
        await _store_memories(backend=backend, count=1)
        backend.memories["9"] = {"id": "9", "learning": None}
        call = getattr(backend, method)
        with _quiet(), pytest.raises(ValueError):
            await call()

    @pytest.mark.parametrize("cls", [YAMLStorage, SQLiteStorage, AccessFilteredStorage])
    def test_bundled_overrides_are_shape_checked(self, cls: type[StorageBackend]) -> None:
        assert inspect.unwrap(cls._list_memories_rows) is not cls._list_memories_rows

    @pytest.mark.parametrize("result", [
        pytest.param(lambda: {}, id="dict"),
        pytest.param(lambda: (m for m in ()), id="generator"),
        pytest.param(lambda: ([], [], []), id="3-tuple"),
        pytest.param(lambda: ([], ()), id="2-tuple-second-not-list"),
        pytest.param(lambda: ((), []), id="2-tuple-first-not-list"),
        pytest.param(lambda: ("a", "b"), id="2-tuple-of-strings"),
        pytest.param(lambda: None, id="none"),
    ])
    async def test_wrong_shape_raises_naming_class(self, result: Callable[[], Any]) -> None:
        backend = _returning_cls(result)()
        pattern = rf"(?s)(?=.*{re.escape('BadShapeBackend')})(?=.*{re.escape(_SHAPE)})"
        with pytest.raises(TypeError, match=pattern):
            await backend.list_memories()
        with pytest.raises(TypeError, match=pattern):
            await backend.load_memories()

    async def test_new_style_backend_keeps_failure_reporting(self) -> None:
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


async def test_full_09x_backend_round_trip() -> None:
    with _quiet():
        backend = _legacy_cls()()
        await backend.save_datasource(_valid())
        stored = await _store_memories(backend=backend, count=2)
        listed = await backend.list_memories()
    assert await backend.list_datasources() == ["shop"]
    assert _ids(listed) == _ids(stored)
