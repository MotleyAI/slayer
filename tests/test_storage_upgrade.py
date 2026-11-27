"""``slayer storage upgrade`` persists the current format; a load warns when a document needs it.

Spec: models/stored-upgrade › A load warns when a document needs upgrading; › slayer storage upgrade persists the
current format; › Stored granularities colliding with a library name are renamed; › Live type refinement applies only
to models written before refining ingest. cli/commands › storage upgrade is a storage command.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import warnings
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import TypeAdapter

import slayer.core.warnings as slayer_warnings
from slayer.core.models import DatasourceConfig
from slayer.embeddings.models import Embedding
from slayer.storage.base import StorageBackend
from slayer.storage.migrations import CURRENT_VERSIONS
from slayer.storage.sqlite_conn import transaction
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.upgrade import upgrade_store
from slayer.storage.yaml_storage import YAMLStorage
from tests import _granularity_rename_fixtures as gran
from tests import _stored_upgrade_fixtures as su
from tests._dev2243_fixtures import SALES
from tests._integrator_1x import DATA_SOURCE, raw_documents
from tests._storage_upgrade_cli import (
    REVENUE_BY_STATUS, raw, store_path, upgrade, warehouse_datasource, warehouse_db, warehouse_store,
)

BACKENDS = ["yaml", "sqlite"]


def _upgrade_warnings(record: list[warnings.WarningMessage]) -> list[Any]:
    carrier = slayer_warnings.SlayerStoredUpgradeNeededWarning
    return [getattr(w.message, "payload") for w in record if isinstance(w.message, carrier)]


def _versions(docs: dict[str, str]) -> dict[str, int]:
    """Each stored document's top-level version."""
    out: dict[str, int] = {}
    for key, text in docs.items():
        if key.startswith("memories/") and text.startswith("---"):
            out[key] = yaml.safe_load(text.split("---")[1])["version"]
        else:
            out[key] = (json.loads(text) if text.lstrip().startswith("{") else yaml.safe_load(text))["version"]
    return out


def _rows(storage: StorageBackend, query: dict[str, Any]) -> list[Any]:
    resp = asyncio.run(su.run(storage, query))
    return sorted(map(repr, resp.data))


async def _load_all(storage: StorageBackend) -> None:
    for data_source, name in await storage._list_all_dataset_identities():
        await storage.get_dataset(name, data_source=data_source)
    for memory in await storage.list_memories():
        await storage.get_memory(memory.id)
    for name in await storage.list_datasources():
        await storage.get_datasource(name)


# --- A load warns when a document needs upgrading ------------------------------------------------------------------


@pytest.mark.parametrize("backend", BACKENDS)
def test_an_old_dataset_warns_once(backend: str, tmp_path: Path) -> None:
    storage = warehouse_store(backend=backend, base=str(tmp_path / "store"))

    async def load_thrice() -> None:
        for _ in range(3):
            await storage.get_dataset("orders", data_source=DATA_SOURCE)

    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        asyncio.run(load_thrice())
    found = [p for p in _upgrade_warnings(record) if p.name == "orders"]
    assert len(found) == 1
    assert (found[0].document_kind, found[0].data_source) == ("dataset", DATA_SOURCE)
    message = found[0].human_message()
    assert "orders" in message
    assert DATA_SOURCE in message
    assert "slayer storage upgrade" in message


@pytest.mark.parametrize("backend", BACKENDS)
def test_a_current_document_with_an_old_nested_stage_warns(backend: str, tmp_path: Path) -> None:
    stage = {"version": 6, "source_model": "orders", "measures": [{"formula": "count(*)", "name": "n"}]}
    backed = {"version": CURRENT_VERSIONS["SlayerModel"], "name": "order_counts", "data_source": DATA_SOURCE,
              "source_queries": [stage]}
    storage = warehouse_store(backend=backend, base=str(tmp_path / "store"),
                              datasets=[*raw_documents().values(), backed])
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        loaded = asyncio.run(storage.get_dataset("order_counts", data_source=DATA_SOURCE))
    assert loaded is not None
    assert [p.name for p in _upgrade_warnings(record) if p.name == "order_counts"] == ["order_counts"]


@pytest.mark.parametrize("backend", BACKENDS)
def test_an_upgraded_store_is_quiet(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    storage = warehouse_store(backend=backend, base=base)
    assert upgrade(store_path(storage)).code == 0
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        asyncio.run(_load_all(gran.reopen(backend=backend, base=base)))
    assert _upgrade_warnings(record) == []


def test_the_load_warning_round_trips_as_a_response_warning() -> None:
    payload = slayer_warnings.StoredUpgradeNeededWarning(document_kind="dataset", data_source=DATA_SOURCE, name="orders")
    assert TypeAdapter(slayer_warnings.AnySlayerWarning).validate_python(payload.model_dump(mode="json")) == payload


# --- slayer storage upgrade persists the current format ------------------------------------------------------------


#: A saved query as the 1.x line stored it: a query-backed dataset with an old stage.
SAVED_QUERY = {"version": 15, "name": "revenue_by_status", "data_source": DATA_SOURCE, "source_queries": [
    {"version": 6, "source_model": "orders", "dimensions": [{"name": "status"}],
     "measures": [{"formula": "sum(amount)", "name": "rev"}]},
]}
SAVED_QUERY_ROWS = {"population": "revenue_by_status", "dimensions": ["revenue_by_status.status"],
                    "metrics": ["sum(revenue_by_status.rev)"]}


@pytest.mark.parametrize("backend", BACKENDS)
def test_an_old_store_upgrades(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    storage = warehouse_store(backend=backend, base=base, datasets=[*raw_documents().values(), SAVED_QUERY])
    before = _rows(storage, REVENUE_BY_STATUS)
    saved_before = _rows(storage, SAVED_QUERY_ROWS)
    result = upgrade(store_path(storage))
    assert result.code == 0, result
    assert set(_versions(raw(backend=backend, base=base)).items()) == {
        (f"datasources/{DATA_SOURCE}", CURRENT_VERSIONS["DatasourceConfig"]),
        (f"models/{DATA_SOURCE}/orders", CURRENT_VERSIONS["SlayerModel"]),
        (f"models/{DATA_SOURCE}/customers", CURRENT_VERSIONS["SlayerModel"]),
        (f"models/{DATA_SOURCE}/revenue_by_status", CURRENT_VERSIONS["SlayerModel"]),
        ("memories/m1", CURRENT_VERSIONS["Memory"]),
    }
    reopened = gran.reopen(backend=backend, base=base)
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        asyncio.run(_load_all(reopened))
    assert _upgrade_warnings(record) == []
    assert _rows(reopened, REVENUE_BY_STATUS) == before
    assert _rows(reopened, SAVED_QUERY_ROWS) == saved_before
    memory = asyncio.run(reopened.get_memory("m1"))
    assert memory.query is not None


@pytest.mark.parametrize("backend", BACKENDS)
def test_each_document_gets_a_line(backend: str, tmp_path: Path) -> None:
    storage = warehouse_store(backend=backend, base=str(tmp_path / "store"))
    result = upgrade(store_path(storage))
    assert result.code == 0, result
    lines = [line for line in result.out.splitlines() if line.strip()]
    for document in (f"{DATA_SOURCE}.orders", f"{DATA_SOURCE}.customers", "m1"):
        assert any(document in line and "upgraded" in line for line in lines), (document, lines)
    assert any(DATA_SOURCE in line and f"{DATA_SOURCE}." not in line and "upgraded" in line for line in lines), lines


@pytest.mark.parametrize("backend", BACKENDS)
def test_a_dry_run_shows_the_change_and_writes_nothing(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    storage = warehouse_store(backend=backend, base=base)
    before = raw(backend=backend, base=base)
    result = upgrade(store_path(storage), "--dry-run")
    assert result.code == 0, result
    assert raw(backend=backend, base=base) == before
    for document in (f"{DATA_SOURCE}.orders", f"{DATA_SOURCE}.customers", "m1"):
        assert document in result.out, document
    assert "version" in result.out


@pytest.mark.parametrize("backend", BACKENDS)
def test_a_legacy_cycle_is_upgraded_not_refused(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    orders = raw_documents()["orders"]
    orders["columns"] += [{"name": "a", "sql": "b + 1", "type": "DOUBLE"},
                          {"name": "b", "sql": "a + 1", "type": "DOUBLE"}]
    storage = warehouse_store(backend=backend, base=base, datasets=[orders, raw_documents()["customers"]])
    result = upgrade(store_path(storage))
    assert result.code == 0, result
    assert _versions(raw(backend=backend, base=base))[f"models/{DATA_SOURCE}/orders"] == CURRENT_VERSIONS["SlayerModel"]


@pytest.mark.parametrize("backend", BACKENDS)
def test_a_legacy_fanning_reference_is_upgraded_not_refused(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    customers = raw_documents()["customers"]
    customers["joins"] = [{"target_model": "orders", "join_pairs": [["id", "customer_id"]],
                           "cardinality": "one_to_many"}]
    customers["columns"].append({"name": "order_amount", "sql": "orders.amount", "type": "DOUBLE"})
    storage = warehouse_store(backend=backend, base=base, datasets=[raw_documents()["orders"], customers])
    result = upgrade(store_path(storage))
    assert result.code == 0, result
    stored = raw(backend=backend, base=base)[f"models/{DATA_SOURCE}/customers"]
    assert _versions({"c": stored})["c"] == CURRENT_VERSIONS["SlayerModel"]
    assert "order_amount" in stored


@pytest.mark.parametrize("backend", BACKENDS)
def test_the_datasource_priority_is_untouched(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    other_db = warehouse_db(str(tmp_path / "other_data"))
    storage = warehouse_store(backend=backend, base=base,
                              extra_datasources=[warehouse_datasource(other_db, name="other")])
    asyncio.run(storage.set_datasource_priority(["other", DATA_SOURCE]))
    assert upgrade(store_path(storage)).code == 0
    assert asyncio.run(gran.reopen(backend=backend, base=base).get_datasource_priority()) == ["other", DATA_SOURCE]


def test_a_current_semantic_model_document_is_checked_and_left(tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    shop_db = warehouse_db(str(tmp_path / "shop_data"))
    storage = warehouse_store(backend="yaml", base=base, extra_datasources=[warehouse_datasource(shop_db, name="shop")])
    path = Path(base, "semantic_models", "shop", "sales.yaml")
    path.parent.mkdir(parents=True)
    path.write_text(SALES)
    result = upgrade(store_path(storage))
    assert result.code == 0, result
    assert path.read_text() == SALES
    assert any("sales" in line for line in result.out.splitlines()), result.out


@pytest.mark.parametrize("invalid", [
    SALES.replace("version:", "version: '0.0.1'\nold_version:", 1).encode(), b"\xff\xfe not utf-8",
], ids=["unsupported-version", "undecodable"])
def test_an_invalid_semantic_model_document_is_reported_not_current(tmp_path: Path, invalid: bytes) -> None:
    base = str(tmp_path / "store")
    shop_db = warehouse_db(str(tmp_path / "shop_data"))
    storage = warehouse_store(backend="yaml", base=base, extra_datasources=[warehouse_datasource(shop_db, name="shop")])
    path = Path(base, "semantic_models", "shop", "sales.yaml")
    path.parent.mkdir(parents=True)
    path.write_bytes(invalid)
    result = upgrade(store_path(storage))
    assert result.code == 1, result
    assert path.read_bytes() == invalid
    assert _versions(raw(backend="yaml", base=base))[f"models/{DATA_SOURCE}/orders"] == CURRENT_VERSIONS["SlayerModel"]
    sales = [line for line in (result.out + result.err).splitlines() if "shop.sales" in line]
    assert sales, result
    assert all("current" not in line for line in sales), sales


@pytest.mark.parametrize("backend", BACKENDS)
def test_a_dataset_waiting_for_its_dialect(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    storage = warehouse_store(backend=backend, base=base, datasource=False)
    orders_key = f"models/{DATA_SOURCE}/orders"
    before = raw(backend=backend, base=base)
    first = upgrade(store_path(storage))
    assert first.code == 1, first
    after = raw(backend=backend, base=base)
    assert after[orders_key] == before[orders_key]
    assert _versions(after)[f"models/{DATA_SOURCE}/customers"] == CURRENT_VERSIONS["SlayerModel"]
    assert any(f"{DATA_SOURCE}.orders" in line for line in (first.out + first.err).splitlines())
    second = upgrade(store_path(storage), "--datasource-type", f"{DATA_SOURCE}=duckdb")
    assert second.code == 0, second
    upgraded = raw(backend=backend, base=base)[orders_key]
    assert _versions({orders_key: upgraded})[orders_key] == CURRENT_VERSIONS["SlayerModel"]
    assert "DUCKDB" in upgraded


@pytest.mark.parametrize("backend", BACKENDS)
def test_a_corrupt_document_does_not_stop_the_others(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    storage = warehouse_store(backend=backend, base=base)
    corrupt = "version: 15\nname: broken\ncolumns: [unclosed\n"
    if backend == "yaml":
        Path(base, "models", DATA_SOURCE, "broken.yaml").write_text(corrupt)
    else:
        with transaction(os.path.join(base, "store.db")) as conn:
            conn.execute("INSERT INTO models (data_source, name, data) VALUES (?, ?, ?)",
                         (DATA_SOURCE, "broken", "{not json"))
    embedding = Embedding(canonical_id=f"{DATA_SOURCE}.orders", embedding_model_name="m", entity_kind="dataset",
                          content_hash="h", embedding=[1.0, 0.0])
    asyncio.run(storage.save_embeddings([embedding]))
    before = raw(backend=backend, base=base)
    result = upgrade(store_path(storage))
    assert result.code == 1, result
    after = raw(backend=backend, base=base)
    assert after[f"models/{DATA_SOURCE}/broken"] == before[f"models/{DATA_SOURCE}/broken"]
    assert _versions({k: v for k, v in after.items() if "broken" not in k}) == {
        k: CURRENT_VERSIONS[kind] for k, kind in (
            (f"datasources/{DATA_SOURCE}", "DatasourceConfig"), (f"models/{DATA_SOURCE}/orders", "SlayerModel"),
            (f"models/{DATA_SOURCE}/customers", "SlayerModel"), ("memories/m1", "Memory"),
        )
    }
    assert any("broken" in line for line in (result.out + result.err).splitlines())
    kept = asyncio.run(gran.reopen(backend=backend, base=base).get_embedding(
        canonical_id=f"{DATA_SOURCE}.orders", embedding_model_name="m"))
    assert kept is not None
    assert kept.embedding == [1.0, 0.0]


@pytest.mark.parametrize("backend", BACKENDS)
def test_an_unreadable_memory_and_datasource_are_reported(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    storage = warehouse_store(backend=backend, base=base)
    if backend == "yaml":
        Path(base, "memories", "broken_memory.md").write_text("---\nid: [unclosed\n---\nbody\n")
        Path(base, "datasources", "broken_ds.yaml").write_text("name: [unclosed\n")
    else:
        with transaction(os.path.join(base, "store.db")) as conn:
            conn.execute("INSERT INTO memories (id, data) VALUES (?, ?)", ("broken_memory", "{not json"))
            conn.execute("INSERT INTO datasources (name, data) VALUES (?, ?)", ("broken_ds", "{not json"))
    result = upgrade(store_path(storage))
    assert result.code == 1, result
    lines = (result.out + result.err).splitlines()
    for document in ("memory broken_memory", "datasource broken_ds"):
        assert any(line.startswith(f"{document}: not upgraded") for line in lines), (document, lines)
    orders = f"models/{DATA_SOURCE}/orders"
    assert _versions({orders: raw(backend=backend, base=base)[orders]})[orders] == CURRENT_VERSIONS["SlayerModel"]


def test_a_concurrent_edit_is_not_overwritten(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base = str(tmp_path / "store")
    storage = warehouse_store(backend="yaml", base=base)
    path = Path(base, "models", DATA_SOURCE, "orders.yaml")
    edited = {**raw_documents()["orders"], "description": "edited meanwhile"}
    read = YAMLStorage._load_raw_dataset_dict
    edits: list[str] = []

    async def read_then_edit(self: YAMLStorage, *args: Any, **kwargs: Any) -> Any:
        doc = await read(self, *args, **kwargs)
        if doc is not None and doc.get("name") == "orders" and not edits:
            edits.append("orders")
            path.write_text(yaml.safe_dump(edited, sort_keys=False))
        return doc

    monkeypatch.setattr(YAMLStorage, "_load_raw_dataset_dict", read_then_edit)
    result = upgrade(store_path(storage))
    assert edits == ["orders"]
    assert result.code == 1, result
    assert yaml.safe_load(path.read_text()) == edited
    assert any("orders" in line and "conflict" in line.lower() for line in (result.out + result.err).splitlines())


def test_a_concurrent_edit_is_not_overwritten_on_sqlite(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base = str(tmp_path / "store")
    storage = warehouse_store(backend="sqlite", base=base)
    db_path = os.path.join(base, "store.db")
    edited = json.dumps({**raw_documents()["orders"], "description": "edited meanwhile"})
    read = SQLiteStorage._load_raw_dataset_dict
    edits: list[str] = []

    async def read_then_edit(self: SQLiteStorage, *args: Any, **kwargs: Any) -> Any:
        doc = await read(self, *args, **kwargs)
        if doc is not None and doc.get("name") == "orders" and not edits:
            edits.append("orders")
            with transaction(db_path) as conn:
                conn.execute("UPDATE models SET data = ? WHERE data_source = ? AND name = ?",
                             (edited, DATA_SOURCE, "orders"))
        return doc

    monkeypatch.setattr(SQLiteStorage, "_load_raw_dataset_dict", read_then_edit)
    result = upgrade(store_path(storage))
    assert edits == ["orders"]
    assert result.code == 1, result
    assert _select(db_path, data_source=DATA_SOURCE, name="orders") == [(edited,)]
    assert any("orders" in line and "conflict" in line.lower() for line in (result.out + result.err).splitlines())


# --- The library-granularity settle through the upgrade ------------------------------------------------------------


async def _granularity_store(*, backend: str, base: str, name: str) -> StorageBackend:
    db_path = gran.live_db(base)
    return await gran.write_raw_store(
        backend=backend, base=base,
        datasources=[gran.datasource_doc(db_path=db_path, granularities=[name])],
        datasets=[gran.events_doc(), gran.query_backed_doc(name="bucketed", stage=gran.bucket_stage(name)),
                  gran.query_backed_doc(name="bucketed_too", stage=gran.bucket_stage(name))],
        memories=[gran.memory_doc(memory_id="m1", query=gran.bucket_stage(name))],
    )


def _comparable(docs: dict[str, str], *, base: str) -> dict[str, Any]:
    """Each stored document with its own store's path a placeholder, a memory without its read-time ``created_at``."""
    out: dict[str, Any] = {}
    for key, text in docs.items():
        text = text.replace(base, "<store>")
        if key.startswith("memories/"):
            markdown = text.startswith("---")
            data = yaml.safe_load(text.split("---")[1]) if markdown else json.loads(text)
            out[key] = ({k: v for k, v in data.items() if k != "created_at"}, text.split("---")[-1] if markdown else "")
        else:
            out[key] = text
    return out


def _mentions_granularity(text: str, name: str) -> bool:
    return f"'{name}'" in text or f'"{name}"' in text or f": {name}\n" in text


def _copy_document(*, backend: str, source: str, target: str, key: str) -> None:
    """Copy one stored dataset document from the store at ``source`` into the store at ``target``."""
    _, data_source, name = key.split("/")
    if backend == "yaml":
        shutil.copy(Path(source, "models", data_source, f"{name}.yaml"), Path(target, "models", data_source, f"{name}.yaml"))
        return
    [(data,)] = _select(os.path.join(source, "store.db"), data_source=data_source, name=name)
    with transaction(os.path.join(target, "store.db")) as conn:
        conn.execute("UPDATE models SET data = ? WHERE data_source = ? AND name = ?", (data, data_source, name))


def _select(db_path: str, *, data_source: str, name: str) -> list[tuple[str]]:
    with transaction(db_path) as conn:
        return conn.execute("SELECT data FROM models WHERE data_source = ? AND name = ?", (data_source, name)).fetchall()


@pytest.mark.parametrize("backend", BACKENDS)
def test_an_interrupted_settle_converges(backend: str, tmp_path: Path) -> None:
    done, interrupted = str(tmp_path / "done"), str(tmp_path / "interrupted")
    asyncio.run(_granularity_store(backend=backend, base=done, name="left"))
    asyncio.run(_granularity_store(backend=backend, base=interrupted, name="left"))
    assert upgrade(store_path(gran.reopen(backend=backend, base=done))).code == 0
    # Interrupted after persisting one dependent and before the datasource.
    _copy_document(backend=backend, source=done, target=interrupted, key=f"models/{gran.DS}/bucketed")

    between = gran.reopen(backend=backend, base=interrupted)
    for name in ("bucketed", "bucketed_too"):
        loaded = asyncio.run(between.get_dataset(name, data_source=gran.DS))
        assert loaded is not None, name
        assert "left_granularity" in loaded.model_dump_json(), name
    assert asyncio.run(gran.bucket_totals(between)) == gran.EXPECTED_BUCKETS

    assert upgrade(store_path(gran.reopen(backend=backend, base=interrupted))).code == 0
    finished = raw(backend=backend, base=interrupted)
    assert _comparable(finished, base=interrupted) == _comparable(raw(backend=backend, base=done), base=done)
    assert "left_granularity" in finished[f"datasources/{gran.DS}"]
    assert not any(_mentions_granularity(text, "left") for key, text in finished.items()
                   if not key.startswith("datasources/"))


def _write_datasource(*, backend: str, base: str, doc: dict[str, Any]) -> None:
    """Replace stored datasource ``gran.DS`` behind the backend's back."""
    if backend == "yaml":
        Path(base, "datasources", f"{gran.DS}.yaml").write_text(yaml.safe_dump(doc))
    else:
        with transaction(os.path.join(base, "store.db")) as conn:
            conn.execute("UPDATE datasources SET data = ? WHERE name = ?", (json.dumps(doc), gran.DS))


@pytest.mark.parametrize("backend", BACKENDS)
def test_a_datasource_changed_elsewhere_renames_on_the_next_load(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    db_path = gran.live_db(base)
    storage = asyncio.run(gran.write_raw_store(backend=backend, base=base,
                                               datasources=[gran.datasource_doc(db_path=db_path)]))
    assert asyncio.run(storage._granularity_renames()) == {}
    _write_datasource(backend=backend, base=base, doc=gran.datasource_doc(db_path=db_path, granularities=["left"]))
    assert asyncio.run(storage._granularity_renames()) == {gran.DS: {"left": gran.renamed("left")}}


@pytest.mark.parametrize("backend", BACKENDS)
def test_one_upgrade_run_reads_one_rename_table(backend: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base = str(tmp_path / "store")
    asyncio.run(_granularity_store(backend=backend, base=base, name="left"))
    unrenamed = gran.datasource_doc(db_path=os.path.join(base, "live.db"))
    upgraded = StorageBackend._upgraded_dataset

    async def edited_after_each_plan(self: StorageBackend, **kwargs: Any) -> Any:
        out = await upgraded(self, **kwargs)
        _write_datasource(backend=backend, base=base, doc=unrenamed)
        return out

    monkeypatch.setattr(StorageBackend, "_upgraded_dataset", edited_after_each_plan)
    report = asyncio.run(upgrade_store(gran.reopen(backend=backend, base=base)))
    stored = raw(backend=backend, base=base)
    for name in ("bucketed", "bucketed_too"):
        assert "left_granularity" in stored[f"models/{gran.DS}/{name}"], name
    assert [o.status for o in report.outcomes if o.kind == "datasource"] == ["conflict"]


@pytest.mark.parametrize("backend", BACKENDS)
def test_datasource_writes_keep_the_runs_rename_table(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    db_path = gran.live_db(base)
    names = (gran.DS, gran.OTHER_DS)
    storage = asyncio.run(gran.write_raw_store(
        backend=backend, base=base,
        datasources=[gran.datasource_doc(db_path=db_path, granularities=["left"], name=n) for n in names],
        datasets=[d for n in names for d in (gran.events_doc(data_source=n), gran.query_backed_doc(
            name="bucketed", stage=gran.bucket_stage("left"), data_source=n))],
    ))
    reads = storage._raw_datasource_dicts
    calls: list[None] = []

    async def counted() -> dict[str, dict]:
        calls.append(None)
        return await reads()

    setattr(storage, "_raw_datasource_dicts", counted)
    report = asyncio.run(upgrade_store(storage))
    assert report.complete, report
    # The plans' read, one rename table, and (YAML; SQLite checks in its write transaction) each datasource write's
    # unchanged-check.
    assert len(calls) == 1 + 1 + (len(names) if backend == "yaml" else 0)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("load", ["get_memory", "load_memories"])
def test_a_memory_load_reads_one_rename_table(backend: str, tmp_path: Path, load: str) -> None:
    base = str(tmp_path / "store")
    storage = asyncio.run(_granularity_store(backend=backend, base=base, name="left"))
    reads = storage._raw_datasource_dicts
    calls: list[None] = []

    async def counted() -> dict[str, dict]:
        calls.append(None)
        return await reads()

    setattr(storage, "_raw_datasource_dicts", counted)
    loaded = asyncio.run(storage.get_memory("m1") if load == "get_memory" else storage.load_memories())
    assert "left_granularity" in str(loaded)
    assert len(calls) == 1


@pytest.mark.parametrize("backend", BACKENDS)
def test_a_rename_converges(backend: str, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    base = str(tmp_path / "store")
    asyncio.run(_granularity_store(backend=backend, base=base, name="contains"))
    assert upgrade(store_path(gran.reopen(backend=backend, base=base))).code == 0
    upgraded = raw(backend=backend, base=base)
    caplog.clear()
    asyncio.run(_load_all(gran.reopen(backend=backend, base=base)))
    second = upgrade(store_path(gran.reopen(backend=backend, base=base)))
    assert second.code == 0, second
    assert raw(backend=backend, base=base) == upgraded
    assert not any("contains_granularity" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("backend", BACKENDS)
def test_a_settle_load_renames_in_memory_only(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    storage = asyncio.run(_granularity_store(backend=backend, base=base, name="left"))
    before = raw(backend=backend, base=base)
    ds = asyncio.run(storage.get_datasource(gran.DS))
    assert ds is not None
    assert [g.name for g in ds.granularities] == ["left_granularity"]
    assert asyncio.run(gran.bucket_totals(storage)) == gran.EXPECTED_BUCKETS
    assert raw(backend=backend, base=base) == before


# --- Refinement-gated documents and the upgrade --------------------------------------------------------------------


@pytest.mark.parametrize("backend", su.BACKENDS)
def test_a_v10_model_without_its_datasource_entry_upgrades_later(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    items = {"version": 10, "name": "items", "sql_table": "items", "data_source": su.DS,
             "columns": [{"name": "qty", "type": "DOUBLE"}]}
    storage = asyncio.run(su.raw_store(backend=backend, base=base, datasource=None, models=[items]))
    loaded = asyncio.run(storage.get_dataset("items", data_source=su.DS))
    assert loaded is not None
    assert loaded.fields[0].datatype is not None
    assert loaded.fields[0].datatype.value == "Float"
    assert asyncio.run(su.raw_doc(storage, "items"))["version"] == 10
    assert upgrade(store_path(storage)).code == 0
    assert asyncio.run(su.raw_doc(gran.reopen(backend=backend, base=base), "items"))["version"] == (
        CURRENT_VERSIONS["SlayerModel"])


def test_a_pre_v8_model_without_a_reachable_datasource_is_refused(tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    items = {"version": 5, "name": "items", "sql_table": "items", "data_source": su.DS,
             "columns": [{"name": "qty", "sql": "qty", "type": "number"}]}
    storage = asyncio.run(su.raw_store(backend="yaml", base=base, datasource=None, models=[items]))
    before = su.stored_bytes(backend="yaml", base=base, name="items")
    result = upgrade(store_path(storage))
    assert result.code == 1, result
    assert su.stored_bytes(backend="yaml", base=base, name="items") == before
    assert any("items" in line for line in (result.out + result.err).splitlines())


# --- cli/commands: storage upgrade is a storage command ------------------------------------------------------------


@pytest.mark.parametrize("backend", BACKENDS)
def test_upgrading_one_datasource(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    other_db = warehouse_db(str(tmp_path / "other_data"))
    other_orders = {**raw_documents()["orders"], "data_source": "other", "joins": []}
    storage = warehouse_store(
        backend=backend, base=base, extra_datasources=[warehouse_datasource(other_db, name="other")],
        datasets=[*raw_documents().values(), other_orders],
    )
    before = raw(backend=backend, base=base)
    result = upgrade(store_path(storage), "--data-source", DATA_SOURCE)
    assert result.code == 0, result
    after = raw(backend=backend, base=base)
    assert after["models/other/orders"] == before["models/other/orders"]
    assert after["datasources/other"] == before["datasources/other"]
    assert _versions(after)[f"models/{DATA_SOURCE}/orders"] == CURRENT_VERSIONS["SlayerModel"]
    assert _versions(after)[f"models/{DATA_SOURCE}/customers"] == CURRENT_VERSIONS["SlayerModel"]


@pytest.mark.parametrize("backend", BACKENDS)
def test_a_malformed_type_option(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    storage = warehouse_store(backend=backend, base=base, datasource=False)
    before = raw(backend=backend, base=base)
    result = upgrade(store_path(storage), "--datasource-type", DATA_SOURCE)
    assert result.code != 0
    error_lines = [line for line in result.err.splitlines() if line.strip()]
    assert len(error_lines) == 1, error_lines
    assert "NAME=TYPE" in error_lines[0]
    assert raw(backend=backend, base=base) == before


def test_the_datasource_config_is_used_for_the_dialect(tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    storage = warehouse_store(backend="yaml", base=base)
    assert upgrade(store_path(storage)).code == 0
    reopened = gran.reopen(backend="yaml", base=base)
    ds = asyncio.run(reopened.get_datasource(DATA_SOURCE))
    assert isinstance(ds, DatasourceConfig)
    assert ds.type == "duckdb"
    orders = asyncio.run(reopened.get_dataset("orders", data_source=DATA_SOURCE))
    assert orders is not None
    status_upper = orders.get_field("status_upper")
    assert status_upper is not None
    assert "DUCKDB" in status_upper.model_dump_json()


def test_a_dry_run_on_a_pre_v4_store_moves_its_files_only(tmp_path: Path) -> None:
    base = tmp_path / "store"
    (base / "datasources").mkdir(parents=True)
    (base / "datasources" / "demo.yaml").write_text(yaml.safe_dump({"name": "demo", "type": "sqlite", "version": 1}))
    flat = {"version": 15, "name": "orders", "data_source": "demo", "sql_table": "orders",
            "columns": [{"name": "status", "type": "TEXT"}]}
    (base / "models").mkdir()
    (base / "models" / "orders.yaml").write_text(yaml.safe_dump(flat, sort_keys=False))
    result = upgrade(str(base), "--dry-run")
    assert result.code == 0, result
    assert not (base / "models" / "orders.yaml").exists()
    assert yaml.safe_load((base / "models" / "demo" / "orders.yaml").read_text()) == flat
    assert yaml.safe_load((base / "datasources" / "demo.yaml").read_text())["version"] == 1
    assert any(line.startswith("dataset demo.orders: would upgrade") for line in result.out.splitlines()), result.out
