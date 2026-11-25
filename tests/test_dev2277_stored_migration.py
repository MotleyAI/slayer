"""Stored queries load in the qualified syntax, keeping their meaning and stage column names.

Spec: models/stored-upgrade › Stored queries are upgraded to qualified references.
"""

from __future__ import annotations

import os
import warnings
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from slayer.core.errors import StoredDocumentLoadError
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.database import engine_factory
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.storage.base import StorageBackend
from slayer.storage.migrations import CURRENT_VERSIONS
from slayer.storage.sqlite_conn import transaction

from tests._dev1737_fixtures import seed_backend
from tests._dev2277_fixtures import (
    CYCLE_TABLES,
    SEED_TABLES,
    conversions,
    cycle_datasets,
    orders_dataset,
    rel,
    seed_datasets,
)
from tests._stored_upgrade_fixtures import BACKENDS, DS, memory_v2, raw_doc, raw_store, run, stored_bytes
from tests._upgrade_corpus import Corpus, online

#: The versions this change introduces; a concurrent bump renumbers only this line.
UPGRADED_AT = {"SlayerModel": 28, "SlayerQuery": 17, "Memory": 13}
PRE = {entity: version - 1 for entity, version in UPGRADED_AT.items()}

#: The column ``sum(amount) / count(customers.*)`` had in an ``orders`` stage before this change.
OLD_RATIO_COLUMN = "amount_sum_customers_count"

CORPORA = [
    Corpus.load(release="v0_10_2", datasources=("shop", "cube", "lite"), data_files=("data.duckdb", "data.sqlite")),
    Corpus.load(release="v1_8_2", datasources=("shop",), data_files=("data.duckdb",)),
]


# --------------------------------------------------------------------------- #
# Documents and stores
# --------------------------------------------------------------------------- #
def dataset_doc(dataset: SlayerDataset, *, version: int = PRE["SlayerModel"]) -> dict:
    doc = dataset.model_dump(mode="json", exclude_none=True)
    return {**doc, "version": version, "data_source": DS}


def stage(*, version: int | None = PRE["SlayerQuery"], **body: Any) -> dict:
    return body if version is None else {"version": version, **body}


def saved_doc(name: str, *stages: dict, version: int = PRE["SlayerModel"]) -> dict:
    return {"version": version, "name": name, "data_source": DS, "source_queries": list(stages)}


def memory(memory_id: str, body: dict, *, version: int = PRE["Memory"]) -> dict:
    return memory_v2(memory_id=memory_id, query=body, version=version)


@pytest.fixture
def seed_ds(tmp_path: Path) -> Generator[DatasourceConfig]:
    db = str(tmp_path / "seed.db")
    seed_backend("sqlite", db, [*SEED_TABLES, *CYCLE_TABLES])
    ds = DatasourceConfig(name=DS, type="sqlite", database=db)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


def base_datasets(*, stores: bool = False) -> list[dict]:
    return [dataset_doc(d) for d in (*seed_datasets(stores=stores), *cycle_datasets())]


async def store(tmp_path: Path, ds: DatasourceConfig, *, backend: str, models: tuple[dict, ...] = (),
                memories: tuple[dict, ...] = (), datasets: list[dict] | None = None) -> StorageBackend:
    return await raw_store(backend=backend, base=str(tmp_path / f"store-{backend}"), datasource=ds,
                           models=[*(base_datasets() if datasets is None else datasets), *models],
                           memories=memories)


def base_of(tmp_path: Path, backend: str) -> str:
    return str(tmp_path / f"store-{backend}")


async def load(storage: StorageBackend, name: str) -> tuple[SlayerDataset, list[warnings.WarningMessage]]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        dataset = await storage.get_dataset(name, data_source=DS)
    assert dataset is not None
    return dataset, list(caught)


def future_warnings(caught: list[warnings.WarningMessage]) -> list[str]:
    return [str(w.message) for w in caught if issubclass(w.category, FutureWarning)]


def stages_of(dataset: SlayerDataset) -> list[SlayerQuery]:
    out = [q for q in dataset.source_queries or [] if isinstance(q, SlayerQuery)]
    assert len(out) == len(dataset.source_queries or [])
    return out


def texts(query: SlayerQuery) -> dict[str, list[Any]]:
    return {
        "dimensions": [(d.field, d.alias) for d in query.dimensions or []],
        "metrics": [(m.metric, m.alias) for m in query.metrics or []],
        "filter": list(query.filter or []),
    }


def table(resp: SlayerResponse, *keys: str) -> set[tuple]:
    def norm(v: Any) -> Any:
        return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v
    return {tuple(norm(r[k]) for k in keys) for r in resp.data}


def memory_bytes(*, backend: str, base: str, storage: StorageBackend, memory_id: str) -> str:
    if backend == "yaml":
        with open(storage._memory_md_path(memory_id), encoding="utf-8") as f:  # type: ignore[attr-defined]  # NOSONAR(S7493) — test read
            return f.read()
    with transaction(os.path.join(base, "store.db")) as conn:
        return conn.execute("SELECT data FROM memories WHERE id = ?", (memory_id,)).fetchone()[0]


def test_versions_bumped() -> None:
    # Later bumps: SlayerQuery 18 stamps ``population_inferred``; 29/19/14 trust stored relationships; SlayerModel 30
    # unwraps AGG in field expressions; 31 types BigQuery TIMESTAMP fields DateTimeTz.
    assert {entity: CURRENT_VERSIONS[entity] for entity in UPGRADED_AT} == {
        "SlayerModel": 31, "SlayerQuery": 19, "Memory": 14}


# --------------------------------------------------------------------------- #
# Rewrites
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("backend", BACKENDS)
class TestRewrites:
    async def test_a_saved_query_loads_qualified_with_its_results(self, tmp_path: Path, seed_ds, backend: str) -> None:
        sq = saved_doc("sq", stage(population="orders", dimensions=["status"], metrics=["sum(amount)"],
                                   filter=["customers.region = 'EAST'"]))
        storage = await store(tmp_path, seed_ds, backend=backend, models=(sq,))
        dataset, caught = await load(storage, "sq")
        assert future_warnings(caught) == []
        assert texts(stages_of(dataset)[0]) == {
            "dimensions": [("orders.status", None)], "metrics": [("sum(orders.amount)", None)],
            "filter": ["customers.region = 'EAST'"],
        }
        resp = await run(storage, {"population": "sq", "dimensions": ["sq.status", "sq.amount_sum"]})
        assert table(resp, "sq.status", "sq.amount_sum") == {("completed", 150.0), ("open", 200.0)}

    async def test_a_saved_query_is_persisted_by_a_save(self, tmp_path: Path, seed_ds, backend: str) -> None:
        sq = saved_doc("sq", stage(population="orders", dimensions=["status"], metrics=["count(*)"]))
        storage = await store(tmp_path, seed_ds, backend=backend, models=(sq,))
        dataset, _ = await load(storage, "sq")
        await storage.save_dataset(dataset)
        raw = await raw_doc(storage, "sq")
        assert raw["version"] == CURRENT_VERSIONS["SlayerModel"]
        (written,) = raw["source_queries"]
        assert written["dimensions"] == ["orders.status"]
        assert written["metrics"] == ["count(orders.*)"]

    async def test_a_stored_cycle_reference_keeps_its_path(self, tmp_path: Path, seed_ds, backend: str) -> None:
        cyc = saved_doc("cyc", stage(population="x", dimensions=["v"], metrics=["sum(z.amount)"]))
        storage = await store(tmp_path, seed_ds, backend=backend, models=(cyc,))
        dataset, caught = await load(storage, "cyc")
        assert future_warnings(caught) == []
        assert texts(stages_of(dataset)[0])["metrics"] == [("sum(x.z.amount)", None)]
        resp = await run(storage, {"population": "cyc", "dimensions": ["cyc.v", "cyc.z__amount_sum"]})
        assert table(resp, "cyc.v", "cyc.z__amount_sum") == {("x1", None), ("x2", 30.0)}

    async def test_a_direct_edge_with_a_second_to_one_path_keeps_its_path(
        self, tmp_path: Path, seed_ds, backend: str,
    ) -> None:
        q2 = saved_doc("q2", stage(population="orders", dimensions=["customers.region"],
                                   metrics=[{"metric": "count(*)", "alias": "n"}]))
        storage = await store(tmp_path, seed_ds, backend=backend, models=(q2,), datasets=base_datasets(stores=True))
        dataset, _ = await load(storage, "q2")
        assert texts(stages_of(dataset)[0])["dimensions"] == [("orders.customers.region", None)]
        resp = await run(storage, {"population": "q2", "dimensions": ["q2.customers__region", "q2.n"]})
        assert table(resp, "q2.customers__region", "q2.n") == {("EAST", 3.0), ("WEST", 1.0)}

    async def test_an_edge_first_path_gets_its_population(self, tmp_path: Path, seed_ds, backend: str) -> None:
        datasets = [dataset_doc(d) for d in seed_datasets(buyer=True)]
        q = saved_doc("q", stage(population="orders", dimensions=["buyer.region"],
                                 metrics=[{"metric": "count(*)", "alias": "n"}]))
        storage = await store(tmp_path, seed_ds, backend=backend, models=(q,), datasets=datasets)
        dataset, _ = await load(storage, "q")
        assert texts(stages_of(dataset)[0])["dimensions"] == [("orders.buyer.region", None)]
        resp = await run(storage, {"population": "q", "dimensions": ["q.buyer__region", "q.n"]})
        assert table(resp, "q.buyer__region", "q.n") == {("EAST", 3.0), ("WEST", 1.0)}

    async def test_a_routed_short_form_gets_its_explicit_route(self, tmp_path: Path, seed_ds, backend: str) -> None:
        rs = saved_doc("rs", stage(population="returns", dimensions=["return_id"], metrics=["sum(orders.amount)"]))
        storage = await store(tmp_path, seed_ds, backend=backend, models=(rs,))
        dataset, caught = await load(storage, "rs")
        assert future_warnings(caught) == []
        assert texts(stages_of(dataset)[0])["metrics"] == [("sum(returns.customers.orders.amount)", None)]
        resp = await run(storage, {"population": "rs", "dimensions": ["rs.return_id", "rs.customers__orders__amount_sum"]})
        assert table(resp, "rs.return_id", "rs.customers__orders__amount_sum") == {("R1", 300.0), ("R2", None)}

    async def test_a_stored_stage_chain_keeps_its_column_names(self, tmp_path: Path, seed_ds, backend: str) -> None:
        qb = saved_doc(
            "qb",
            stage(name="s1", population="orders", dimensions=["status"], metrics=["sum(amount) / count(customers.*)"]),
            stage(population="s1", dimensions=["status", OLD_RATIO_COLUMN]),
        )
        reader = saved_doc("reader", stage(population="qb", dimensions=[OLD_RATIO_COLUMN]))
        storage = await store(tmp_path, seed_ds, backend=backend, models=(qb, reader))
        dataset, caught = await load(storage, "qb")
        assert future_warnings(caught) == []
        first, second = (texts(q) for q in stages_of(dataset))
        assert first["metrics"] == [("sum(orders.amount) / count(customers.*)", OLD_RATIO_COLUMN)]
        assert second["dimensions"] == [("s1.status", None), (f"s1.{OLD_RATIO_COLUMN}", None)]
        resp = await run(storage, {"population": "qb", "dimensions": ["qb.status", f"qb.{OLD_RATIO_COLUMN}"]})
        assert table(resp, "qb.status", f"qb.{OLD_RATIO_COLUMN}") == {("completed", 75.0), ("open", 250.0)}
        downstream = await run(storage, {"population": "reader", "dimensions": [f"reader.{OLD_RATIO_COLUMN}"]})
        assert table(downstream, f"reader.{OLD_RATIO_COLUMN}") == {(75.0,), (250.0,)}

    async def test_nested_queries_with_and_without_a_version(self, tmp_path: Path, seed_ds, backend: str) -> None:
        both = saved_doc(
            "both",
            stage(name="a", population="orders", dimensions=["status"], metrics=[{"metric": "count(*)", "alias": "n"}]),
            stage(version=None, population="a", dimensions=["status", "n"], filter=["n > 1"]),
        )
        storage = await store(tmp_path, seed_ds, backend=backend, models=(both,))
        dataset, _ = await load(storage, "both")
        first, second = (texts(q) for q in stages_of(dataset))
        assert first["metrics"] == [("count(orders.*)", "n")]
        assert second["dimensions"] == [("a.status", None), ("a.n", None)]
        assert second["filter"] == ["a.n > 1"]

    async def test_a_current_nested_query_is_left_alone(self, tmp_path: Path, seed_ds, backend: str) -> None:
        current = stage(version=UPGRADED_AT["SlayerQuery"], population="orders", dimensions=["orders.status"],
                        metrics=[{"metric": "count(orders.*)", "alias": "n"}], filter=["n > 1"])
        storage = await store(tmp_path, seed_ds, backend=backend, models=(saved_doc("cur", current),))
        dataset, _ = await load(storage, "cur")
        assert texts(stages_of(dataset)[0]) == {
            "dimensions": [("orders.status", None)], "metrics": [("count(orders.*)", "n")], "filter": ["n > 1"],
        }

    async def test_a_legacy_vocabulary_query_upgrades_through(self, tmp_path: Path, seed_ds, backend: str) -> None:
        old = {"version": 13, "source_model": "orders", "dimensions": ["status"],
               "measures": [{"formula": "count(*)", "name": "n"}], "filters": ["amount > 60"]}
        storage = await store(tmp_path, seed_ds, backend=backend, memories=(memory("m_old", old, version=9),))
        mem = await storage.get_memory("m_old")
        assert mem.query is not None
        assert texts(mem.query) == {
            "dimensions": [("orders.status", None)], "metrics": [("count(orders.*)", "n")],
            "filter": ["orders.amount > 60"],
        }


# --------------------------------------------------------------------------- #
# Memories
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("query_version", [PRE["SlayerQuery"], None], ids=["versioned", "version-less"])
async def test_a_memory_converts_on_every_load(tmp_path: Path, seed_ds, backend: str,
                                               query_version: int | None) -> None:
    body = stage(version=query_version, population="orders", metrics=["count(*)"])
    storage = await store(tmp_path, seed_ds, backend=backend, memories=(memory("m_count", body),))
    base = base_of(tmp_path, backend)
    before = memory_bytes(backend=backend, base=base, storage=storage, memory_id="m_count")
    for _ in range(2):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            mem = await storage.get_memory("m_count")
        assert future_warnings(list(caught)) == []
        assert mem.query is not None
        assert texts(mem.query)["metrics"] == [("count(orders.*)", None)]
    assert mem.query is not None
    assert memory_bytes(backend=backend, base=base, storage=storage, memory_id="m_count") == before
    resp = await run(storage, mem.query)
    assert table(resp, "orders._count") == {(4.0,)}


# --------------------------------------------------------------------------- #
# Unloadable documents
# --------------------------------------------------------------------------- #
def _parallel_orders() -> dict:
    orders = orders_dataset()
    return dataset_doc(orders.model_copy(update={
        "relationships": [*orders.relationships, rel("customers", "seller_id", "customer_id")]}))


UNCONVERTIBLE = {
    # The old reading crossed one of two unnamed orders→customers edges.
    "unspellable-path": ("customers.region", stage(population="orders", dimensions=["customers.region"])),
    "missing-dataset": ("ghosts.name", stage(population="orders", dimensions=["ghosts.name"])),
}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("case", sorted(UNCONVERTIBLE))
async def test_an_unconvertible_document_fails_to_load_untouched(tmp_path: Path, seed_ds, backend: str,
                                                                 case: str) -> None:
    reference, body = UNCONVERTIBLE[case]
    datasets = [d if d["name"] != "orders" else _parallel_orders() for d in base_datasets()]
    good = saved_doc("good", stage(population="returns", dimensions=["amount"]))
    storage = await store(tmp_path, seed_ds, backend=backend, models=(good, saved_doc("bad", body)),
                          datasets=datasets)
    base = base_of(tmp_path, backend)
    before = stored_bytes(backend=backend, base=base, name="bad")
    with pytest.raises(StoredDocumentLoadError) as exc:
        await storage.get_dataset("bad", data_source=DS)
    assert f"{DS}.bad" in str(exc.value)
    assert reference in str(exc.value)
    assert stored_bytes(backend=backend, base=base, name="bad") == before
    loaded, failed = await storage.load_datasets(data_source=DS)
    assert "good" in {d.name for d in loaded}
    assert [e.name for e in failed] == ["bad"]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_an_unconvertible_memory_fails_to_load(tmp_path: Path, seed_ds, backend: str) -> None:
    datasets = [d if d["name"] != "orders" else _parallel_orders() for d in base_datasets()]
    storage = await store(tmp_path, seed_ds, backend=backend, datasets=datasets,
                          memories=(memory("m_bad", UNCONVERTIBLE["unspellable-path"][1]),))
    with pytest.raises(StoredDocumentLoadError) as exc:
        await storage.get_memory("m_bad")
    assert "m_bad" in str(exc.value)
    assert "customers.region" in str(exc.value)


# --------------------------------------------------------------------------- #
# Released stores
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("backend", ["yaml", "sqlite"])
@pytest.mark.parametrize("corpus", CORPORA, ids=[c.root.name for c in CORPORA])
async def test_released_store_queries_load_qualified(tmp_path: Path, backend: str, corpus: Corpus) -> None:
    """Every stored memory query loads without a deprecation and runs with no conversion."""
    async with online(corpus, backend=backend, tmp=tmp_path) as storage:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            memories = [await storage.get_memory(mid) for mid in corpus.memory_ids]
        assert future_warnings(list(caught)) == []
        engine = SlayerQueryEngine(storage=storage)
        try:
            for mem in memories:
                if mem.query is None:
                    continue
                spec = corpus.expected[f"memory:{mem.id}"]
                with warnings.catch_warnings(record=True) as ran:
                    warnings.simplefilter("always")
                    await engine.execute(mem.query, data_source=spec["data_source"])
                assert conversions(list(ran)) == [], mem.id
        finally:
            await engine.aclose()
            engine.close()
