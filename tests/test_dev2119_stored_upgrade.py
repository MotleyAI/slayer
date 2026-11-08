"""Stored object-form time dimensions are upgraded to expression text (spec: models/stored-upgrade)."""

from __future__ import annotations

import json
from typing import Any

import pytest
import yaml

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.memories.models import Memory
from slayer.sql import engine_factory
from slayer.storage import migrations as mig
from slayer.storage.sqlite_conn import transaction
from tests._stored_upgrade_fixtures import (
    BACKENDS,
    DS,
    customers_v10,
    memory_v2,
    orders_v10,
    query_backed_v10,
    raw_doc,
    raw_store,
    rev_query,
    run,
    seed_shop,
    stored_bytes,
)
from tests.test_upgrade_corpus_v0_10_2 import DATASOURCES, EXPECTED, MEMORY_PREFIX, _open_store, _run

SIGNUP_OBJECT = {"name": "signed_up_at", "model": "customers"}
SIGNUP_TEXT = "customers.signed_up_at"
#: sum(amount) by the month of the customer's signup.
REV_BY_SIGNUP_MONTH = {"2023-06": 30.0, "2024-03": 90.0, "2024-08": 90.0}
#: sum(amount) by the year of ordered_at.
REV_BY_ORDER_YEAR = {"2023": 10.0, "2024": 150.0, "2025": 50.0}


def _td(dimension: Any, *, key: str = "dimension", granularity: str = "month") -> dict:
    return {key: dimension, "granularity": granularity}


def _query(*, version: int | None, tds: list[dict]) -> dict:
    data: dict = rev_query(time_dimensions=tds)
    if version is not None:
        data["version"] = version
    return data


def _dumped_tds(query: SlayerQuery) -> list:
    return query.model_dump(mode="json", exclude_none=True)["time_dimensions"]


def _cells(resp, *, dim_suffix: str, width: int) -> dict[str, float]:
    row = resp.data[0]
    dim = next(k for k in row if k.endswith(dim_suffix))
    val = next(k for k in row if k != dim)
    return {str(r[dim]).replace("T", " ")[:width]: float(r[val]) for r in resp.data}


class TestObjectFormBecomesText:
    @pytest.mark.parametrize(("stored", "text"), [
        pytest.param(_td(SIGNUP_OBJECT), SIGNUP_TEXT, id="dimension-object"),
        pytest.param(_td(SIGNUP_OBJECT, key="column"), SIGNUP_TEXT, id="column-alias-object"),
        pytest.param(_td({"name": "ordered_at"}), "ordered_at", id="name-only"),
        pytest.param(_td({"name": "ordered_at", "model": None, "label": None}), "ordered_at", id="null-model"),
    ])
    def test_stored_query(self, stored: dict, text: str) -> None:
        query = SlayerQuery.model_validate(_query(version=7, tds=[stored]))
        [td] = query.time_dimensions or []
        assert td.dimension == text
        assert _dumped_tds(query) == [{"dimension": text, "granularity": "month"}]

    def test_migration_rewrites_only_stored_objects(self) -> None:
        text = [_td("coalesce(shipped_at, created_at)"), _td(SIGNUP_TEXT, granularity="year")]
        versioned = mig.migrate("SlayerQuery", _query(version=7, tds=[_td(SIGNUP_OBJECT), *text]))
        assert versioned["time_dimensions"] == [{"dimension": SIGNUP_TEXT, "granularity": "month"}, *text]
        unversioned = mig.migrate("SlayerQuery", _query(version=None, tds=[_td(SIGNUP_OBJECT)]))
        assert unversioned["time_dimensions"][0]["dimension"] == SIGNUP_OBJECT

    def test_model_source_queries(self) -> None:
        stages = [_query(version=None, tds=[_td(SIGNUP_OBJECT)])]
        model = SlayerModel.model_validate(query_backed_v10(name="rev_q", version=16, stages=stages))
        [query] = model.source_queries or []
        assert [td.dimension for td in query.time_dimensions or []] == [SIGNUP_TEXT]
        dumped = model.model_dump(mode="json", exclude_none=True)
        assert dumped["source_queries"][0]["time_dimensions"] == [{"dimension": SIGNUP_TEXT, "granularity": "month"}]

    def test_every_stage_of_a_multi_stage_model(self) -> None:
        inner = {**_query(version=None, tds=[_td(SIGNUP_OBJECT)]), "name": "s1"}
        outer = {"source_model": "s1", "measures": [{"formula": "sum(rev)", "name": "rev"}],
                 "time_dimensions": [_td({"name": "customers__signed_up_at"}, key="column", granularity="year")]}
        model = SlayerModel.model_validate(query_backed_v10(name="two", version=16, stages=[inner, outer]))
        assert [[td.dimension for td in s.time_dimensions or []] for s in model.source_queries or []] == [
            [SIGNUP_TEXT], ["customers__signed_up_at"],
        ]

    def test_memory_query(self) -> None:
        mem = Memory.model_validate(memory_v2(memory_id="m", version=5, query=_query(version=None, tds=[
            _td({"name": "ordered_at"}, granularity="year"),
        ])))
        assert mem.query is not None
        assert _dumped_tds(mem.query) == [{"dimension": "ordered_at", "granularity": "year"}]


@pytest.fixture
def shop_ds(tmp_path):
    db = str(tmp_path / "shop.duckdb")
    seed_shop(db, dialect="duckdb")
    ds = DatasourceConfig(name=DS, type="duckdb", database=db)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


class TestStoredDocumentsUpgrade:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_query_backed_model(self, tmp_path, shop_ds, backend: str) -> None:
        stage = rev_query(time_dimensions=[_td(SIGNUP_OBJECT)])
        storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=shop_ds,
                                  models=[orders_v10(), customers_v10(), query_backed_v10(name="rev_q", stages=[stage])])
        model = await storage.get_model("rev_q", data_source=DS)
        assert model is not None
        [loaded] = model.source_queries or []
        assert _dumped_tds(loaded) == [{"dimension": SIGNUP_TEXT, "granularity": "month"}]
        raw = await raw_doc(storage, "rev_q")
        assert raw["version"] == mig.CURRENT_VERSIONS["SlayerModel"]
        assert raw["source_queries"][0]["time_dimensions"][0]["dimension"] == SIGNUP_TEXT
        resp = await run(storage, {"source_model": "rev_q", "dimensions": ["customers__signed_up_at"],
                                   "measures": ["sum(rev)"]})
        assert _cells(resp, dim_suffix="signed_up_at", width=7) == REV_BY_SIGNUP_MONTH

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_resave_writes_text(self, tmp_path, shop_ds, backend: str) -> None:
        stage = rev_query(time_dimensions=[_td(SIGNUP_OBJECT)])
        base = str(tmp_path / "store")
        storage = await raw_store(backend=backend, base=base, datasource=shop_ds,
                                  models=[orders_v10(), customers_v10(), query_backed_v10(name="rev_q", stages=[stage])])
        model = await storage.get_model("rev_q", data_source=DS)
        assert model is not None
        await storage.save_model(model)
        text = stored_bytes(backend=backend, base=base, name="rev_q")
        doc = yaml.safe_load(text) if backend == "yaml" else json.loads(text)
        assert doc["source_queries"][0]["time_dimensions"][0]["dimension"] == SIGNUP_TEXT

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_memory_bundled_query(self, tmp_path, shop_ds, backend: str) -> None:
        query = _query(version=4, tds=[_td({"name": "ordered_at", "model": None}, granularity="year")])
        base = str(tmp_path / "store")
        storage = await raw_store(backend=backend, base=base, datasource=shop_ds,
                                  models=[orders_v10(), customers_v10()],
                                  memories=[memory_v2(memory_id="m_td", query=query)])
        mem = await storage.get_memory("m_td")
        assert mem.query is not None
        assert _dumped_tds(mem.query) == [{"dimension": "ordered_at", "granularity": "year"}]
        assert _cells(await run(storage, mem.query), dim_suffix="ordered_at", width=4) == REV_BY_ORDER_YEAR
        await storage.save_memory(learning=mem.learning, entities=mem.entities, query=mem.query, id=mem.id)
        assert _stored_memory_td(backend=backend, storage=storage, base=base, memory_id="m_td") == "ordered_at"


def _stored_memory_td(*, backend: str, storage: Any, base: str, memory_id: str) -> Any:
    if backend == "yaml":
        with open(storage._memory_md_path(memory_id), encoding="utf-8") as f:  # NOSONAR(S7493) — test read
            front = f.read().split("---\n")[1]
        doc = yaml.safe_load(front)
    else:
        with transaction(f"{base}/store.db") as conn:
            doc = json.loads(conn.execute("SELECT data FROM memories WHERE id = ?", (memory_id,)).fetchone()[0])
    return doc["query"]["time_dimensions"][0]["dimension"]


class TestReleasedCorpus:
    @pytest.mark.parametrize("backend", ["yaml", "sqlite"])
    async def test_every_time_dimension_loads_as_text_with_its_results(self, tmp_path, backend: str) -> None:
        storage = _open_store(backend=backend, tmp=tmp_path, online=True)
        try:
            texts: list = []
            keys: list[str] = []
            for ds, name in sorted(await storage._list_all_model_identities()):
                model = await storage.get_model(name, data_source=ds)
                assert model is not None
                for query in model.source_queries or []:
                    if query.time_dimensions:
                        texts += [td["dimension"] for td in _dumped_tds(query)]
                        keys.append(name)
            for mid in sorted(k.removeprefix(MEMORY_PREFIX) for k in EXPECTED if k.startswith(MEMORY_PREFIX)):
                mem = await storage.get_memory(mid)
                if mem.query is not None and mem.query.time_dimensions:
                    texts += [td["dimension"] for td in _dumped_tds(mem.query)]
                    keys.append(f"{MEMORY_PREFIX}{mid}")
            assert texts == ["ordered_at"] * 7
            for key in keys:
                assert await _run(storage, key) == EXPECTED[key]["rows"], key
        finally:
            for name in DATASOURCES:
                ds_config = await storage.get_datasource(name)
                if ds_config is not None:
                    engine_factory.invalidate_engine(ds_config)
