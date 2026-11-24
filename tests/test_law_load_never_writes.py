"""A load never writes: loading every stored document leaves the store's logical contents unchanged and calls no
storage write hook. Opening a pre-v4 flat-layout YAML store is the one licensed write.

Spec: models/stored-upgrade › A load never writes. Principle: storage arc42 3.1.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml

from slayer.core.models import DatasourceConfig
from slayer.storage.base import StorageBackend
from slayer.storage.migrations import CURRENT_VERSIONS
from slayer.storage.sqlite_conn import transaction
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage
from tests import _granularity_rename_fixtures as gran
from tests import _stored_upgrade_fixtures as su
from tests._upgrade_corpus import Corpus, online

CORPORA = {
    "v0_10_2": Corpus.load(release="v0_10_2", datasources=("shop", "cube", "lite"),
                           data_files=("data.duckdb", "data.sqlite")),
    "v1_8_2": Corpus.load(release="v1_8_2", datasources=("shop",), data_files=("data.duckdb",)),
}

#: Every hook through which a backend persists or deletes a document.
WRITE_HOOKS = (
    "_save_dataset_impl", "_write_raw_dataset_dict", "_delete_dataset_row", "_save_datasource_impl",
    "_write_raw_datasource_dict", "_delete_datasource_row", "_set_datasource_priority_raw", "_save_memory_row",
    "_write_raw_memory_dict", "_delete_memory_row", "_save_semantic_model_text_impl", "_patch_semantic_model_text",
    "_delete_semantic_model_row", "save_embedding", "save_embeddings", "update_field_sampled",
)


def _contents(storage: StorageBackend) -> dict[str, Any]:
    """The store's logical contents: YAML document bytes, or every SQLite row."""
    if isinstance(storage, YAMLStorage):
        root = Path(storage.base_dir)
        return {
            str(path.relative_to(root)): path.read_bytes()
            for path in sorted(root.rglob("*"))
            if path.is_file() and path.suffix in (".yaml", ".yml", ".md")
        }
    assert isinstance(storage, SQLiteStorage)
    out: dict[str, Any] = {}
    with transaction(storage.db_path) as cur:
        tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")]
        for table in tables:
            out[table] = sorted(map(repr, cur.execute(f'SELECT * FROM "{table}"').fetchall()))  # noqa: S608 — table names from sqlite_master
    return out


def _spy_writes(storage: StorageBackend, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []
    for name in WRITE_HOOKS:
        original = getattr(type(storage), name, None)
        if original is None:
            continue

        def _recording(*args: Any, _name: str = name, _original: Callable[..., Any] = original, **kwargs: Any) -> Any:
            calls.append(_name)
            return _original(*args, **kwargs)

        monkeypatch.setattr(type(storage), name, _recording)
    return calls


async def _load_everything(storage: StorageBackend) -> dict[str, Any]:
    """Every datasource, dataset, semantic-model document and memory, as loaded."""
    loaded: dict[str, Any] = {}
    for name in await storage.list_datasources():
        ds = await storage.get_datasource(name)
        loaded[f"datasource:{name}"] = ds.model_dump(mode="json") if ds is not None else None
    for data_source, name in sorted(await storage._list_all_dataset_identities()):
        dataset = await storage.get_dataset(name, data_source=data_source)
        loaded[f"dataset:{data_source}.{name}"] = dataset.model_dump(mode="json") if dataset is not None else None
    datasets, failures = await storage.load_datasets()
    loaded["load_datasets"] = (sorted(d.name for d in datasets), len(failures))
    for data_source, name in await storage.list_semantic_models():
        loaded[f"semantic_model:{data_source}.{name}"] = await storage.get_semantic_model_text(name, data_source)
    for memory in await storage.list_memories():
        # A memory stored without `created_at` is stamped at read time.
        loaded[f"memory:{memory.id}"] = (await storage.get_memory(memory.id)).model_dump(mode="json",
                                                                                        exclude={"created_at"})
    return loaded


async def _assert_loads_never_write(storage: StorageBackend, monkeypatch: pytest.MonkeyPatch) -> None:
    before = _contents(storage)
    calls = _spy_writes(storage, monkeypatch)
    first = await _load_everything(storage)
    second = await _load_everything(storage)
    assert first == second
    assert calls == []
    assert _contents(storage) == before


@pytest.mark.parametrize("backend", ["yaml", "sqlite"])
@pytest.mark.parametrize("release", sorted(CORPORA))
async def test_loading_an_old_store_leaves_it_unchanged(
    release: str, backend: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with online(CORPORA[release], backend=backend, tmp=tmp_path) as storage:
        await _assert_loads_never_write(storage, monkeypatch)


@pytest.mark.parametrize("backend", gran.BACKENDS)
async def test_the_granularity_settle_writes_nothing(
    backend: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = str(tmp_path / "store")
    db_path = gran.live_db(base)
    storage = await gran.write_raw_store(
        backend=backend, base=base,
        datasources=[gran.datasource_doc(db_path=db_path, granularities=["left"])],
        datasets=[gran.events_doc(), gran.query_backed_doc(name="bucketed", stage=gran.bucket_stage("left"))],
        memories=[gran.memory_doc(memory_id="m1", query=gran.bucket_stage("left"))],
    )
    await _assert_loads_never_write(storage, monkeypatch)
    loaded = await storage.get_dataset("bucketed", data_source=gran.DS)
    assert loaded is not None and "left_granularity" in loaded.model_dump_json()


@pytest.mark.parametrize("backend", su.BACKENDS)
async def test_join_key_and_target_repairs_write_nothing(
    backend: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = str(tmp_path / "store")
    db_path = os.path.join(base, "shop.db")
    os.makedirs(base, exist_ok=True)
    su.seed_shop(db_path, dialect="sqlite")
    storage = await su.raw_store(
        backend=backend, base=base, datasource=DatasourceConfig(name=su.DS, type="sqlite", database=db_path),
        models=[
            su.orders_v10(declare_fk=False, joins=[
                {"target_model": "Customers", "join_pairs": [["customer_id", "id"]], "cardinality": "many_to_one"},
            ]),
            su.customers_v10(declare_pk=False, name="customers"),
        ],
        memories=[su.memory_v2(memory_id="m1", query=su.rev_query())],
    )
    await _assert_loads_never_write(storage, monkeypatch)


@pytest.mark.parametrize("backend", su.BACKENDS)
async def test_a_save_persists_the_upgraded_form(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    db_path = os.path.join(base, "shop.db")
    os.makedirs(base, exist_ok=True)
    su.seed_shop(db_path, dialect="sqlite")
    storage = await su.raw_store(
        backend=backend, base=base, datasource=DatasourceConfig(name=su.DS, type="sqlite", database=db_path),
        models=[su.orders_v10(), su.customers_v10()],
    )
    orders = await storage.get_dataset("orders", data_source=su.DS)
    assert orders is not None
    assert (await su.raw_doc(storage, "orders"))["version"] == 10
    await storage.save_dataset(orders)
    stored = await su.raw_doc(storage, "orders")
    assert stored["version"] == CURRENT_VERSIONS["SlayerModel"]
    assert [f["name"] for f in stored["fields"]] == [f.name for f in orders.fields]


async def test_opening_a_pre_v4_flat_layout_store_moves_its_files_only(tmp_path: Path) -> None:
    base = tmp_path / "store"
    (base / "datasources").mkdir(parents=True)
    (base / "datasources" / "demo.yaml").write_text(yaml.safe_dump({"name": "demo", "type": "sqlite", "version": 1}))
    flat = {"version": 15, "name": "orders", "data_source": "demo", "sql_table": "orders",
            "columns": [{"name": "status", "type": "TEXT"}]}
    (base / "models").mkdir()
    (base / "models" / "orders.yaml").write_text(yaml.safe_dump(flat, sort_keys=False))
    YAMLStorage(base_dir=str(base))
    assert not (base / "models" / "orders.yaml").exists()
    assert yaml.safe_load((base / "models" / "demo" / "orders.yaml").read_text()) == flat
