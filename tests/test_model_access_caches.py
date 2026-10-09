"""Caches are keyed by the store's view: tag sets never share entries, bypass shares the inner one."""

from __future__ import annotations

import asyncio
import gc
import os
import tempfile
from collections.abc import AsyncGenerator
from typing import cast

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.pg_facade.connection import PgConnection
from slayer.search import graph as search_graph
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.tag_filtered import TagFilteredStorage
from slayer.storage.yaml_storage import YAMLStorage
from tests._model_access_fixtures import (
    ACCESS_PARAMS,
    ALL_LOADABLE,
    DS,
    HIDDEN_FROM_FIN,
    VISIBLE_TO_FIN,
    AccessStore,
    access_store,
    must_get,
)
from tests._stored_upgrade_fixtures import customers_v10, orders_v10, raw_doc, raw_store

MODELS = "MATCH (m:Model) RETURN m.id AS id"
VISIBLE_TO_HR = ALL_LOADABLE - {"fin", "fin_hop", "fin_ordered", "fin_report"}


@pytest.fixture(params=ACCESS_PARAMS)
async def store(request) -> AsyncGenerator[AccessStore]:
    backend, dialect = request.param
    async with access_store(backend=backend, dialect=dialect) as s:
        yield s


@pytest.fixture(autouse=True)
def _fresh_graph_cache():
    search_graph.clear_cache()
    yield
    search_graph.clear_cache()


def canonical(names) -> set[str]:
    return {f"{DS}.{n}" for n in names}


async def graph_models(storage: StorageBackend) -> set[str]:
    return set(await search_graph.get_filtered_ids(MODELS, storage))


class _NoFingerprintYAML(YAMLStorage):
    """A backend reporting no content fingerprint (the base default)."""

    async def graph_fingerprint(self):
        return await StorageBackend.graph_fingerprint(self)

    async def cache_identity(self):
        return await StorageBackend.cache_identity(self)


def _table_model(name: str) -> SlayerModel:
    return SlayerModel(name=name, sql_table="t", data_source=DS, columns=[Column(name="id", type=DataType.INT, primary_key=True)])


class TestGraphCache:
    async def test_alternating_tag_sets_never_share_a_graph(self, store: AccessStore) -> None:
        fin = TagFilteredStorage(store.storage, tags={"fin"}, bypass=False)
        hr = TagFilteredStorage(store.storage, tags={"hr"}, bypass=False)
        for _ in range(3):
            assert await graph_models(fin) == canonical(VISIBLE_TO_FIN)
            assert await graph_models(hr) == canonical(VISIBLE_TO_HR)
        assert len(search_graph._cache) <= 2

    async def test_short_lived_wrappers_never_share_a_graph(self, store: AccessStore) -> None:
        for _ in range(3):
            fin = TagFilteredStorage(store.storage, tags={"fin"}, bypass=False)
            assert await graph_models(fin) == canonical(VISIBLE_TO_FIN)
            del fin
            gc.collect()
            hr = TagFilteredStorage(store.storage, tags={"hr"}, bypass=False)
            assert await graph_models(hr) == canonical(VISIBLE_TO_HR)
            del hr
            gc.collect()
        assert len(search_graph._cache) <= 2

    async def test_bypass_shares_the_inner_entry(self, store: AccessStore) -> None:
        await graph_models(store.storage)
        assert await graph_models(TagFilteredStorage(store.storage, tags=set(), bypass=True)) == canonical(ALL_LOADABLE)
        assert len(search_graph._cache) == 1

    async def test_no_fingerprint_backend_rebuilds_after_a_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = _NoFingerprintYAML(base_dir=tmp)
            await storage.save_datasource(DatasourceConfig(name=DS, type="sqlite", database=os.path.join(tmp, "x.db")))
            await storage.save_model(_table_model("first"), _validate=False)
            assert await graph_models(storage) == canonical({"first"})
            await storage.save_model(_table_model("second"), _validate=False)
            assert await graph_models(storage) == canonical({"first", "second"})


class TestCacheIdentity:
    async def test_file_backends_use_their_absolute_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            assert await YAMLStorage(base_dir=tmp).cache_identity() == os.path.abspath(tmp)
            db = os.path.join(tmp, "s.db")
            assert await SQLiteStorage(db_path=db).cache_identity() == os.path.abspath(db)

    async def test_other_backends_get_a_stable_per_instance_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            a, b = _NoFingerprintYAML(base_dir=tmp), _NoFingerprintYAML(base_dir=tmp)
            assert await a.cache_identity() == await a.cache_identity()
            assert await a.cache_identity() != await b.cache_identity()

    async def test_wrapper_identity_is_the_view(self, store: AccessStore) -> None:
        inner = await store.storage.cache_identity()

        def wrap(*tags: str, bypass: bool = False) -> TagFilteredStorage:
            return TagFilteredStorage(store.storage, tags=set(tags), bypass=bypass)

        assert await wrap("fin").cache_identity() == (inner, frozenset({"fin"}), False)
        assert await wrap("fin").cache_identity() == await wrap("fin").cache_identity()
        assert await wrap("fin").cache_identity() != await wrap("hr").cache_identity()
        assert await wrap("x", bypass=True).cache_identity() == inner

    async def test_default_fingerprint_is_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            assert await _NoFingerprintYAML(base_dir=tmp).graph_fingerprint() is None

    async def test_wrapper_fingerprint_follows_the_inner_one(self, store: AccessStore) -> None:
        fin = TagFilteredStorage(store.storage, tags={"fin"}, bypass=False)
        hr = TagFilteredStorage(store.storage, tags={"hr"}, bypass=False)
        before = await fin.graph_fingerprint()
        assert before is not None
        assert before != await hr.graph_fingerprint()
        await store.storage.save_model(_table_model("extra"), _validate=False)
        assert await fin.graph_fingerprint() != before

    async def test_wrapper_over_unknown_fingerprint_is_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fin = TagFilteredStorage(_NoFingerprintYAML(base_dir=tmp), tags={"fin"}, bypass=False)
            assert await fin.graph_fingerprint() is None


class TestViewFreshness:
    async def test_view_follows_inner_writes(self, store: AccessStore) -> None:
        fin = TagFilteredStorage(store.storage, tags={"fin"}, bypass=False)
        assert set(await fin.list_models(DS)) == VISIBLE_TO_FIN
        await store.storage.save_model(_table_model("extra"), _validate=False)
        assert set(await fin.list_models(DS)) == VISIBLE_TO_FIN | {"extra"}
        pub = await must_get(store.storage, "pub")
        await store.storage.save_model(pub.model_copy(update={"access_tags": ["hr"]}), _validate=False)
        assert set(await fin.list_models(DS)) == (VISIBLE_TO_FIN | {"extra"}) - {"pub"}
        assert HIDDEN_FROM_FIN.isdisjoint(await fin.list_models(DS))

    async def test_facade_catalog_rebuilds_without_a_fingerprint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = _NoFingerprintYAML(base_dir=tmp)
            await storage.save_datasource(DatasourceConfig(name=DS, type="sqlite", database=os.path.join(tmp, "x.db")))
            await storage.save_model(_table_model("first"), _validate=False)
            conn = PgConnection(asyncio.StreamReader(), cast(asyncio.StreamWriter, None), engine=None, storage=storage, catalog_ttl_seconds=0.0)
            conn._catalog = await conn._build_catalog()
            conn._catalog_fingerprint = await conn._read_fingerprint()
            await storage.save_model(_table_model("second"), _validate=False)
            await conn._maybe_refresh_catalog()
            assert conn._catalog is not None
            assert "second" in {t.name for s in conn._catalog.schemas for t in s.tables}


@pytest.mark.parametrize("backend", ["yaml", "sqlite"])
async def test_concurrent_first_reads_of_a_pre_v15_store_agree(backend: str) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        inner = await raw_store(
            backend=backend, base=tmp, datasource=DatasourceConfig(name="shop", type="sqlite", database=os.path.join(tmp, "x.db")),
            models=[orders_v10(version=14), customers_v10(version=14)],
        )
        fin = TagFilteredStorage(inner, tags={"fin"}, bypass=False)
        listings = await asyncio.gather(*(fin.list_models("shop") for _ in range(8)))
        models = await asyncio.gather(*(fin.get_model("orders", data_source="shop") for _ in range(8)))
        assert all(sorted(names) == ["customers", "orders"] for names in listings)
        assert all(m is not None and [j.target_model for j in m.joins] == ["customers"] for m in models)
        assert (await raw_doc(inner, "orders"))["version"] == 15
        assert sorted(await fin.list_models("shop")) == ["customers", "orders"]
