"""A store written by ``motley-slayer==0.10.2`` loads, re-saves, re-ingests and queries with 0.10.2's results."""

from __future__ import annotations

import json
import re
import shutil
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import duckdb
import pytest

from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.live_schema.ingestion import ingest_datasource_idempotent
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database import engine_factory
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_conn import transaction
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage

CORPUS = Path(__file__).parent / "fixtures" / "upgrade" / "v0_10_2"
EXPECTED: dict[str, dict] = json.loads((CORPUS / "expected.json").read_text())
MEMORY_PREFIX = "memory:"
MEMORY_IDS = sorted(k.removeprefix(MEMORY_PREFIX) for k in EXPECTED if k.startswith(MEMORY_PREFIX))
DATASOURCES = ("shop", "cube", "lite")
DATA_FILES = ("data.duckdb", "data.sqlite")


def _open_store(*, backend: str, tmp: Path, online: bool) -> StorageBackend:
    """A temp copy of the corpus store; ``online`` points its datasources at copies of the data, else drops them."""
    for name in DATA_FILES:
        shutil.copy(CORPUS / name, tmp / name)
    if backend == "yaml":
        root = tmp / "yaml_store"
        shutil.copytree(CORPUS / "yaml_store", root)
        for path in (root / "datasources").glob("*.yaml"):
            if not online:
                path.unlink()
                continue
            text = path.read_text()
            for name in DATA_FILES:
                text = text.replace(f"database: {name}", f"database: {tmp / name}")
            path.write_text(text)
        return YAMLStorage(base_dir=str(root))
    path = tmp / "sqlite_store.db"
    shutil.copy(CORPUS / "sqlite_store.db", path)
    with transaction(str(path)) as cur:
        if not online:
            cur.execute("DELETE FROM datasources")
        for name in DATA_FILES:
            cur.execute("UPDATE datasources SET data = replace(data, ?, ?)",
                        (f'"database": "{name}"', f'"database": "{tmp / name}"'))
    return SQLiteStorage(db_path=str(path))


@pytest.fixture(params=["yaml", "sqlite"])
async def online_store(request, tmp_path) -> AsyncIterator[StorageBackend]:
    storage = _open_store(backend=request.param, tmp=tmp_path, online=True)
    try:
        yield storage
    finally:
        for name in DATASOURCES:
            ds = await storage.get_datasource(name)
            if ds is not None:
                engine_factory.invalidate_engine(ds)


@pytest.fixture(params=["yaml", "sqlite"])
def offline_store(request, tmp_path, monkeypatch) -> StorageBackend:
    def _no_connection(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError(f"connection attempted: {args!r} {kwargs!r}")

    storage = _open_store(backend=request.param, tmp=tmp_path, online=False)
    monkeypatch.setattr(engine_factory, "get_engine", _no_connection)
    monkeypatch.setattr(duckdb, "connect", _no_connection)
    return storage


async def _load_all(storage: StorageBackend) -> tuple[list[SlayerModel], list]:
    identities = await storage._list_all_model_identities()
    assert {ds for ds, _ in identities} == set(DATASOURCES)
    models = []
    for ds, name in sorted(identities):
        model = await storage.get_model(name, data_source=ds)
        assert model is not None, (ds, name)
        models.append(model)
    memories = [await storage.get_memory(mid) for mid in MEMORY_IDS]
    assert sorted(m.id for m in await storage.list_memories()) == MEMORY_IDS
    return models, memories


_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'")
_COLON_SPAN = re.compile(r"[\w*\"]:[A-Za-z_]")
#: Keys whose string values are Mode-B text.
_MODE_B_KEYS = frozenset({"formula", "expression", "raw_formula", "filters", "dimensions", "time_dimensions",
                          "dimension", "column", "order", "main_time_dimension"})


def _mode_b_texts(value: Any, *, mode_b: bool = False) -> list[str]:
    """Every Mode-B string inside a dumped model or query."""
    if isinstance(value, str):
        return [value] if mode_b else []
    if isinstance(value, list):
        return [t for v in value for t in _mode_b_texts(v, mode_b=mode_b)]
    if isinstance(value, dict):
        return [t for k, v in value.items() if k != "columns"
                for t in _mode_b_texts(v, mode_b=mode_b or k in _MODE_B_KEYS)]
    return []


def _colon_spans(text: str) -> list[str]:
    return _COLON_SPAN.findall(_STRING_LITERAL.sub("''", text))


def _cell(value: Any) -> Any:
    return round(value, 6) if isinstance(value, float) else (None if value is None else str(value))


def _rows(resp, *, ordered: bool) -> list[dict]:
    rows = [{k.rsplit(".", 1)[-1]: _cell(v) for k, v in row.items()} for row in resp.data]
    return rows if ordered else sorted(rows, key=lambda r: json.dumps(r, sort_keys=True))


async def _execute(storage: StorageBackend, key: str) -> Any:
    spec = EXPECTED[key]
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


async def _run(storage: StorageBackend, key: str) -> list[dict]:
    return _rows(await _execute(storage, key), ordered=EXPECTED[key]["ordered"])


class TestOnline:
    async def test_every_document_loads(self, online_store):
        models, memories = await _load_all(online_store)
        assert models
        assert len(memories) == len(MEMORY_IDS)

    @pytest.mark.parametrize("key", sorted(EXPECTED))
    async def test_recorded_query_returns_0_10_2_rows(self, online_store, key):
        assert await _run(online_store, key) == EXPECTED[key]["rows"]

    @pytest.mark.parametrize("key", sorted(k for k in EXPECTED if EXPECTED[k]["rows"]))
    async def test_recorded_query_returns_0_10_2_keys(self, online_store, key):
        resp = await _execute(online_store, key)
        assert sorted(c.rsplit(".", 1)[-1] for c in resp.columns) == sorted(EXPECTED[key]["rows"][0])

    async def test_every_model_resaves(self, online_store):
        models, _ = await _load_all(online_store)
        for model in models:
            await online_store.save_model(model)

    @pytest.mark.parametrize("data_source", [
        pytest.param("shop", marks=pytest.mark.xfail(
            strict=True, reason="DEV-2057: false stale-reference error for m_rank over uncached status_rank")),
        "cube", "lite",
    ])
    async def test_datasource_reingests(self, online_store, data_source):
        datasource = await online_store.get_datasource(data_source)
        assert datasource is not None
        engine = SlayerQueryEngine(storage=online_store)
        try:
            result = await ingest_datasource_idempotent(
                datasource=datasource, storage=online_store, client=engine.client_for(datasource),
            )
        finally:
            await engine.aclose()
            engine.close()
        assert result.errors == []
        await _load_all(online_store)


class TestOffline:
    async def test_every_colon_formula_loads_functional(self, offline_store):
        models, memories = await _load_all(offline_store)
        dumped = [m.model_dump(mode="json") for m in models] + [
            m.query.model_dump(mode="json") for m in memories if m.query is not None]
        texts = [t for d in dumped for t in _mode_b_texts(d)]
        assert any("sum(amount)" in t for t in texts)
        assert [t for t in texts if _colon_spans(t)] == []

    async def test_every_document_loads_without_a_connection(self, offline_store):
        models, memories = await _load_all(offline_store)
        assert models
        assert len(memories) == len(MEMORY_IDS)

    async def test_every_document_loads_through_an_engine_without_a_connection(self, offline_store):
        engine = SlayerQueryEngine(storage=offline_store)
        try:
            models, memories = await _load_all(engine.storage)
        finally:
            engine.close()
        assert models
        assert len(memories) == len(MEMORY_IDS)
