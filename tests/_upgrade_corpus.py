"""Shared harness for the released-store upgrade corpora under ``tests/fixtures/upgrade/``."""

from __future__ import annotations

import contextlib
import json
import shutil
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any, ClassVar

import duckdb
import pytest
from pydantic import BaseModel, ConfigDict

from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.database import engine_factory
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema.ingestion import ingest_datasource_idempotent
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_conn import transaction
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage

MEMORY_PREFIX = "memory:"


class Corpus(BaseModel):
    """One committed store: its directory, datasources and data files, and the release's recorded results."""

    model_config = ConfigDict(frozen=True)
    root: Path
    datasources: tuple[str, ...]
    data_files: tuple[str, ...]
    expected: dict[str, dict[str, Any]]

    @classmethod
    def load(cls, *, release: str, datasources: tuple[str, ...], data_files: tuple[str, ...]) -> Corpus:
        root = Path(__file__).parent / "fixtures" / "upgrade" / release
        return cls(root=root, datasources=datasources, data_files=data_files,
                   expected=json.loads((root / "expected.json").read_text()))

    @property
    def memory_ids(self) -> list[str]:
        return sorted(k.removeprefix(MEMORY_PREFIX) for k in self.expected if k.startswith(MEMORY_PREFIX))


def open_store(corpus: Corpus, *, backend: str, tmp: Path, online: bool) -> StorageBackend:
    """A temp copy of the corpus store; ``online`` points its datasources at copies of the data, else drops them."""
    for name in corpus.data_files:
        shutil.copy(corpus.root / name, tmp / name)
    if backend == "yaml":
        root = tmp / "yaml_store"
        shutil.copytree(corpus.root / "yaml_store", root)
        for path in (root / "datasources").glob("*.yaml"):
            if not online:
                path.unlink()
                continue
            text = path.read_text()
            for name in corpus.data_files:
                text = text.replace(f"database: {name}", f"database: {tmp / name}")
            path.write_text(text)
        return YAMLStorage(base_dir=str(root))
    path = tmp / "sqlite_store.db"
    shutil.copy(corpus.root / "sqlite_store.db", path)
    with transaction(str(path)) as cur:
        if not online:
            cur.execute("DELETE FROM datasources")
        for name in corpus.data_files:
            cur.execute("UPDATE datasources SET data = replace(data, ?, ?)",
                        (f'"database": "{name}"', f'"database": "{tmp / name}"'))
    return SQLiteStorage(db_path=str(path))


@contextlib.asynccontextmanager
async def online(corpus: Corpus, *, backend: str, tmp: Path) -> AsyncGenerator[StorageBackend, None]:
    """The store with its datasources pointed at data copies; every datasource's engine is disposed on exit."""
    storage = open_store(corpus, backend=backend, tmp=tmp, online=True)
    try:
        yield storage
    finally:
        for name in corpus.datasources:
            ds = await storage.get_datasource(name)
            if ds is not None:
                engine_factory.invalidate_engine(ds)


def offline(corpus: Corpus, *, backend: str, tmp: Path, monkeypatch: pytest.MonkeyPatch) -> StorageBackend:
    """The store without datasources, any connection attempt failing the test."""
    def _no_connection(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError(f"connection attempted: {args!r} {kwargs!r}")

    storage = open_store(corpus, backend=backend, tmp=tmp, online=False)
    monkeypatch.setattr(engine_factory, "get_engine", _no_connection)
    monkeypatch.setattr(duckdb, "connect", _no_connection)
    return storage


async def load_all(corpus: Corpus, storage: StorageBackend) -> tuple[list[SlayerModel], list]:
    identities = await storage._list_all_model_identities()
    assert {ds for ds, _ in identities} == set(corpus.datasources)
    models = []
    for ds, name in sorted(identities):
        model = await storage.get_model(name, data_source=ds)
        assert model is not None, (ds, name)
        models.append(model)
    memories = [await storage.get_memory(mid) for mid in corpus.memory_ids]
    assert sorted(m.id for m in await storage.list_memories()) == corpus.memory_ids
    return models, memories


def cell(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        return [cell(v) for v in value]
    return round(value, 6) if isinstance(value, float) else (None if value is None else str(value))


def rows(resp, *, ordered: bool) -> list[dict]:
    out = [{k.rsplit(".", 1)[-1]: cell(v) for k, v in row.items()} for row in resp.data]
    return out if ordered else sorted(out, key=lambda r: json.dumps(r, sort_keys=True))


async def execute(corpus: Corpus, storage: StorageBackend, key: str) -> Any:
    """The response to the recorded query ``key`` (a memory's stored query for ``memory:<id>``)."""
    spec = corpus.expected[key]
    if key.startswith(MEMORY_PREFIX):
        query = (await storage.get_memory(key.removeprefix(MEMORY_PREFIX))).query
        assert query is not None
    else:
        query = SlayerQuery.model_validate(spec["query"])
    engine = SlayerQueryEngine(storage=storage)
    try:
        resp = await engine.execute(query, data_source=spec["data_source"])
    finally:
        await engine.aclose()
    return resp


async def run(corpus: Corpus, storage: StorageBackend, key: str) -> list[dict]:
    """The recorded query ``key`` as normalised rows."""
    return rows(await execute(corpus, storage, key), ordered=corpus.expected[key]["ordered"])


async def reingest(corpus: Corpus, storage: StorageBackend, data_source: str) -> None:
    """Re-ingest ``data_source`` without errors, after which every document still loads."""
    datasource = await storage.get_datasource(data_source)
    assert datasource is not None
    engine = SlayerQueryEngine(storage=storage)
    try:
        result = await ingest_datasource_idempotent(
            datasource=datasource, storage=storage, client=engine.client_for(datasource),
        )
    finally:
        await engine.aclose()
        engine.close()
    assert result.errors == []
    await load_all(corpus, storage)


async def loads_through_an_engine(corpus: Corpus, storage: StorageBackend) -> None:
    engine = SlayerQueryEngine(storage=storage)
    try:
        models, memories = await load_all(corpus, engine.storage)
    finally:
        engine.close()
    assert models
    assert len(memories) == len(corpus.memory_ids)


class OnlineChecks:
    """Checks on the store with its datasources present, on both backends; a subclass sets ``corpus`` and its module
    defines the ``key`` (each recorded query) and ``data_source`` fixtures."""

    corpus: ClassVar[Corpus]

    @pytest.fixture(params=["yaml", "sqlite"])
    async def online_store(self, request, tmp_path) -> AsyncGenerator[StorageBackend, None]:
        async with online(self.corpus, backend=request.param, tmp=tmp_path) as storage:
            yield storage

    async def test_every_document_loads(self, online_store):
        models, memories = await load_all(self.corpus, online_store)
        assert models
        assert len(memories) == len(self.corpus.memory_ids)

    async def test_recorded_query_returns_the_release_rows(self, online_store, key):
        assert await run(self.corpus, online_store, key) == self.corpus.expected[key]["rows"]

    async def test_recorded_query_returns_the_release_keys(self, online_store, key):
        resp = await execute(self.corpus, online_store, key)
        assert sorted(c.rsplit(".", 1)[-1] for c in resp.columns) == sorted(self.corpus.expected[key]["rows"][0])

    async def test_every_model_resaves(self, online_store):
        models, _ = await load_all(self.corpus, online_store)
        for model in models:
            await online_store.save_model(model)

    async def test_datasource_reingests(self, online_store, data_source):
        await reingest(self.corpus, online_store, data_source)


class OfflineChecks:
    """Checks on the store with its datasource entries removed, on both backends; a subclass sets ``corpus``."""

    corpus: ClassVar[Corpus]

    @pytest.fixture(params=["yaml", "sqlite"])
    def offline_store(self, request, tmp_path, monkeypatch) -> StorageBackend:
        return offline(self.corpus, backend=request.param, tmp=tmp_path, monkeypatch=monkeypatch)

    async def test_every_document_loads_without_a_connection(self, offline_store):
        models, memories = await load_all(self.corpus, offline_store)
        assert models
        assert len(memories) == len(self.corpus.memory_ids)

    async def test_every_document_loads_through_an_engine_without_a_connection(self, offline_store):
        await loads_through_an_engine(self.corpus, offline_store)
