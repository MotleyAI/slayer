"""2.0 standalone datasets read as datasource-model members and ``slayer storage upgrade`` moves them into the layout.

Spec: models/stored-upgrade › Standalone datasets stored by 2.0 upgrade into the stored format.
"""

from __future__ import annotations

import asyncio
import builtins
import contextlib
import gc
import io
import json
import shutil
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import pytest
import yaml

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, DatasourceConfig, SlayerDataset
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_conn import open_connection, transaction
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.upgrade import upgrade_datasets
from slayer.storage.yaml_storage import YAMLStorage
from tests import _stored_upgrade_fixtures as su
from tests._storage_upgrade_cli import store_path, upgrade
from tests._stored_format_fixtures import (
    DS, STORAGES, Interrupted, dataset_documents, dataset_file, file_ops, folder, legacy_folder, legacy_rows,
    model_documents, model_file, parse, recorded, refs, reopen, slayer_entry, sqlite_path, stored_dataset, texts,
    upgrade_warning_names,
)
from tests._upgrade_corpus import Corpus, open_store
from tests._upgrade_corpus import run as run_recorded
from tests.test_dev2243_custom_backend import DatasetsOnlyBackend

BY_REGION = {"population": "orders", "dimensions": ["customers.region"], "metrics": ["sum(orders.amount)"]}
ORDER_COUNT = {"population": "orders", "metrics": ["count(*)"]}
DBT_ENTRY = {"vendor_name": "DBT", "data": {"x": 1}}


# --------------------------------------------------------------------------- #
# 2.0 stores
# --------------------------------------------------------------------------- #
def _orders() -> SlayerDataset:
    return SlayerDataset(
        name="orders", data_source=DS, sql_table="orders",
        fields=[
            DatasetField(name="order_id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="status", meta={"osi_ai_context": "order status"}),
            DatasetField(name="ordered_at", datatype=DataType.DATETIME, meta={"osi_is_time": False}),
            DatasetField(name="amount", datatype=DataType.FLOAT),
        ],
        metrics=[DatasetMetric(name="revenue", expression="sum(amount)")],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        meta={"osi_ai_context": {"synonyms": ["sales"]}, "osi_unique_keys": [["order_id", "status"]],
              "osi_nonportable_extensions": [DBT_ENTRY]},
    )


def _customers() -> SlayerDataset:
    return SlayerDataset(name="customers", data_source=DS, sql_table="customers",
                         fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                                 DatasetField(name="name"), DatasetField(name="region")])


def _returns() -> SlayerDataset:
    return SlayerDataset(name="returns", data_source=DS, sql_table="orders",
                         fields=[DatasetField(name="order_id", datatype=DataType.INTEGER, primary_key=True)])


def legacy(dataset: SlayerDataset) -> dict[str, Any]:
    """``dataset`` as 2.0 stored it standalone."""
    return dataset.model_dump(mode="json", exclude_none=True)


def store_root(kind: str, tmp: Path) -> Path:
    """The ``base`` of :func:`su.raw_store` matching the stored-format helpers' layout."""
    return tmp / "store" if kind == "yaml" else tmp


def seeded_db(tmp: Path) -> str:
    db = tmp / "shop.duckdb"
    if not db.exists():
        su.seed_shop(str(db), dialect="duckdb")
    return str(db)


def shop_datasource(tmp: Path) -> DatasourceConfig:
    return DatasourceConfig(name=DS, type="duckdb", database=seeded_db(tmp))


async def legacy_store(kind: str, tmp: Path, *datasets: SlayerDataset) -> StorageBackend:
    """A 2.0 store over the seeded shop database holding ``datasets`` standalone."""
    return await su.raw_store(backend=kind, base=str(store_root(kind, tmp)), datasource=shop_datasource(tmp),
                              models=[legacy(d) for d in datasets or (_orders(), _customers())])


def place_2_0_document(kind: str, tmp: Path, text: str, *, name: str) -> None:
    """A document stored by 2.0, under its 2.0 file name or SQLite row."""
    if kind == "yaml":
        path = folder(tmp) / f"{name}.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return
    with transaction(str(sqlite_path(tmp))) as conn:
        conn.execute("INSERT INTO semantic_models (data_source, name, text) VALUES (?, ?, ?)", (DS, name, text))


def rows(storage: StorageBackend, query: dict[str, Any]) -> list[str]:
    return sorted(map(repr, asyncio.run(su.run(storage, query)).data))


def sqlite_dump(path: Path) -> dict[str, list[tuple[Any, ...]]]:
    """Every row of every table of a SQLite file."""
    with open_connection(path) as con:
        tables = [t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")]
        return {t: sorted(map(tuple, con.execute(f'SELECT * FROM "{t}"'))) for t in tables}  # NOSONAR(S2077) — test


def stored_state(kind: str, tmp: Path) -> Any:
    return sqlite_dump(sqlite_path(tmp)) if kind == "sqlite" else {
        str(p.relative_to(tmp)): p.read_bytes() for p in sorted((tmp / "store").rglob("*"))
        if p.is_file() and p.suffix != ".db"}


def legacy_gone(kind: str, tmp: Path) -> bool:
    if kind == "yaml":
        root = legacy_folder(tmp)
        return not root.exists() or not any(root.iterdir())
    return legacy_rows(tmp) == []


def conflict_reported(out: str, *, name: str) -> bool:
    return any(name in line and "conflict" in line.lower() for line in out.splitlines())


class _EditOnClose:
    """A read handle that runs ``edit`` once the reader closes it."""

    def __init__(self, handle: Any, edit: Callable[[], None]) -> None:
        self._handle = handle
        self._edit: Callable[[], None] | None = edit

    def __getattr__(self, name: str) -> Any:
        return getattr(self._handle, name)

    def __iter__(self) -> Any:
        return iter(self._handle)

    def __enter__(self) -> _EditOnClose:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._handle.close()
        if self._edit is not None:
            edit, self._edit = self._edit, None
            edit()


@contextlib.contextmanager
def edited_after_first_read(path: Path, *, text: str) -> Generator[list[str]]:
    """Replace ``path``'s content with ``text`` right after the first read of it finishes, as a concurrent writer."""
    original = builtins.open
    target = str(path.resolve())
    done: list[str] = []

    def write() -> None:
        with original(target, "w", encoding="utf-8") as f:
            f.write(text)
        done.append(target)

    def patched(file: Any, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        handle = original(file, mode, *args, **kwargs)
        reading = not any(c in mode for c in "wax+")
        if reading and not done and isinstance(file, (str, Path)) and str(Path(file).resolve()) == target:
            return _EditOnClose(handle, write)
        return handle

    builtins.open = patched  # type: ignore[assignment]
    io.open = patched  # type: ignore[assignment]
    try:
        yield done
    finally:
        builtins.open = original
        io.open = original


# --------------------------------------------------------------------------- #
# A 2.0 store upgrades
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_a_2_0_store_upgrades(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A 2.0 store upgrades."""
    storage = await legacy_store(kind, tmp_path)
    before = await asyncio.to_thread(rows, storage, BY_REGION)
    result = await asyncio.to_thread(upgrade, store_path(storage))
    assert result.code == 0, result
    assert {dataset_file(kind, tmp_path, n) for n in ("orders", "customers")} == {
        "orders.dataset.yaml", "customers.dataset.yaml"}
    shop = model_documents(kind, tmp_path)[DS]
    assert sorted(refs(shop)) == ["customers.dataset.yaml", "orders.dataset.yaml"]
    assert [(r["from"], r["to"], r["from_columns"], r["to_columns"]) for r in shop.get("relationships") or []] == [
        ("orders", "customers", ["customer_id"], ["id"])]
    orders = stored_dataset(kind, tmp_path, "orders")
    assert orders["source"] == "orders"
    assert orders["primary_key"] == ["order_id", "customer_id"]
    assert ["order_id", "status"] in orders["unique_keys"]
    assert orders["ai_context"] == {"synonyms": ["sales"]}
    assert DBT_ENTRY in orders["nonportable_extensions"]
    by_name = {f["name"]: f for f in orders["fields"]}
    assert by_name["ordered_at"]["dimension"] == {"is_time": False}
    assert by_name["status"]["ai_context"] == "order status"
    assert "osi_" not in json.dumps(slayer_entry(orders) or {})
    assert legacy_gone(kind, tmp_path)
    upgraded = reopen(kind, tmp_path)
    assert await upgraded.list_semantic_models(data_source=DS) == [(DS, DS)]
    assert await asyncio.to_thread(rows, upgraded, BY_REGION) == before


@pytest.mark.parametrize("kind", STORAGES)
async def test_loading_before_the_upgrade_writes_nothing(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Loading before the upgrade writes nothing."""
    storage = await legacy_store(kind, tmp_path)
    before = stored_state(kind, tmp_path)
    with recorded() as record:
        loaded, errs = await storage.load_datasets(data_source=DS)
        listed = await storage.list_semantic_models(data_source=DS)
        owners = {n: await storage.semantic_model_owner(n, data_source=DS) for n in ("orders", "customers")}
    assert errs == []
    assert {d.name for d in loaded} == {"orders", "customers"}
    assert listed == [(DS, DS)]
    assert owners == {"orders": DS, "customers": DS}
    assert {"orders", "customers"} <= set(upgrade_warning_names(record))
    assert stored_state(kind, tmp_path) == before


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_store_holding_both_forms_reads_correctly(tmp_path: Path, kind: str) -> None:
    storage = await legacy_store(kind, tmp_path, _returns())
    await storage.save_dataset(_orders(), _validate=False)
    await storage.save_dataset(_customers(), _validate=False)
    assert {dataset_documents(kind, tmp_path)[f]["dataset"]["name"] for f in dataset_documents(kind, tmp_path)} == {
        "orders", "customers"}
    with recorded() as record:
        fresh = reopen(kind, tmp_path)
        loaded, errs = await fresh.load_datasets(data_source=DS)
    assert errs == []
    assert {d.name for d in loaded} == {"orders", "customers", "returns"}
    assert upgrade_warning_names(record) == ["returns"]
    assert await fresh.semantic_model_owner("returns", data_source=DS) == DS
    assert await asyncio.to_thread(rows, fresh, {"population": "returns", "metrics": ["count(*)"]}) == \
        await asyncio.to_thread(rows, fresh, ORDER_COUNT)


DOCUMENT_SHOP = """\
version: 0.3.0.dev0
name: shop
# kept by hand
datasets:
  - name: customers
    source: customers
    primary_key: [id]
    fields:
      - name: id
        expression: {sql: id}
        datatype: Integer
      - name: region
        expression: {sql: region}
        datatype: String
"""


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_same_named_document_is_adopted(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A same-named document is adopted."""
    storage = await legacy_store(kind, tmp_path, _returns())
    place_2_0_document(kind, tmp_path, DOCUMENT_SHOP, name=DS)
    result = await asyncio.to_thread(upgrade, store_path(storage))
    assert result.code == 0, result
    assert model_file(kind, tmp_path, DS) == "shop.yaml"
    text = texts(kind, tmp_path)["shop.yaml"]
    doc = parse(text)
    assert refs(doc) == ["returns.dataset.yaml"]
    assert {k: v for k, v in doc.items() if k != "dataset_refs"} == yaml.safe_load(DOCUMENT_SHOP)
    lines = iter(text.splitlines())
    assert all(any(line == kept for kept in lines) for line in DOCUMENT_SHOP.splitlines())
    assert legacy_gone(kind, tmp_path)


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_refused_dataset_stays_legacy_beside_its_upgraded_siblings(tmp_path: Path, kind: str) -> None:
    storage = await legacy_store(kind, tmp_path)
    if kind == "yaml":
        broken = legacy_folder(tmp_path) / "broken.yaml"
        broken.write_text("version: 15\nname: broken\ncolumns: [unclosed\n", encoding="utf-8")
        kept = broken.read_bytes()
    else:
        with transaction(str(sqlite_path(tmp_path))) as conn:
            conn.execute("INSERT INTO models (data_source, name, data) VALUES (?, ?, ?)", (DS, "broken", "{not json"))
    result = await asyncio.to_thread(upgrade, store_path(storage))
    assert result.code == 1, result
    assert any("broken" in line for line in (result.out + result.err).splitlines())
    assert {d["dataset"]["name"] for d in dataset_documents(kind, tmp_path).values()} == {"orders", "customers"}
    assert sorted(refs(model_documents(kind, tmp_path)[DS])) == ["customers.dataset.yaml", "orders.dataset.yaml"]
    if kind == "yaml":
        assert broken.read_bytes() == kept
        assert [p.name for p in legacy_folder(tmp_path).iterdir() if p.suffix == ".yaml"] == ["broken.yaml"]
    else:
        assert legacy_rows(tmp_path) == [(DS, "broken")]
    loaded, _ = await reopen(kind, tmp_path).load_datasets(data_source=DS)
    assert {"orders", "customers"} <= {d.name for d in loaded}


# --------------------------------------------------------------------------- #
# Conflicts and interruption
# --------------------------------------------------------------------------- #
async def test_a_concurrent_edit_of_a_legacy_dataset_is_not_overwritten(tmp_path: Path) -> None:
    """Spec scenario: A concurrent edit is not overwritten."""
    storage = await legacy_store("yaml", tmp_path)
    path = legacy_folder(tmp_path) / "orders.yaml"
    edited = yaml.safe_dump({**legacy(_orders()), "description": "edited meanwhile"}, sort_keys=False)
    customers = (legacy_folder(tmp_path) / "customers.yaml").read_bytes()
    with edited_after_first_read(path, text=edited) as done:
        result = await asyncio.to_thread(upgrade, store_path(storage))
    assert done
    assert result.code == 1, result
    assert conflict_reported(result.out + result.err, name="orders")
    assert path.read_text(encoding="utf-8") == edited
    assert (legacy_folder(tmp_path) / "customers.yaml").read_bytes() == customers
    assert dataset_documents("yaml", tmp_path) == {}
    again = await asyncio.to_thread(upgrade, store_path(storage))
    assert again.code == 0, again
    assert stored_dataset("yaml", tmp_path, "orders")["description"] == "edited meanwhile"
    assert legacy_gone("yaml", tmp_path)


async def test_a_concurrent_edit_of_a_legacy_row_is_not_overwritten(tmp_path: Path,
                                                                    monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec scenario: A concurrent edit is not overwritten."""
    storage = await legacy_store("sqlite", tmp_path)
    edited = json.dumps({**legacy(_orders()), "description": "edited meanwhile"})
    read = SQLiteStorage._load_raw_dataset_dict
    edits: list[str] = []

    async def read_then_edit(self: SQLiteStorage, *args: Any, **kwargs: Any) -> Any:
        doc = await read(self, *args, **kwargs)
        if doc is not None and doc.get("name") == "orders" and not edits:
            edits.append("orders")
            with transaction(str(sqlite_path(tmp_path))) as conn:
                conn.execute("UPDATE models SET data = ? WHERE data_source = ? AND name = ?", (edited, DS, "orders"))
        return doc

    monkeypatch.setattr(SQLiteStorage, "_load_raw_dataset_dict", read_then_edit)
    result = await asyncio.to_thread(upgrade, store_path(storage))
    monkeypatch.undo()
    assert edits == ["orders"]
    assert result.code == 1, result
    assert conflict_reported(result.out + result.err, name="orders")
    assert sorted(legacy_rows(tmp_path)) == [(DS, "customers"), (DS, "orders")]
    again = await asyncio.to_thread(upgrade, store_path(storage))
    assert again.code == 0, again
    assert stored_dataset("sqlite", tmp_path, "orders")["description"] == "edited meanwhile"
    assert legacy_gone("sqlite", tmp_path)


async def test_a_concurrent_edit_of_the_model_document_is_not_overwritten(tmp_path: Path) -> None:
    """Spec scenario: A concurrent edit is not overwritten."""
    storage = await legacy_store("yaml", tmp_path, _returns())
    place_2_0_document("yaml", tmp_path, DOCUMENT_SHOP, name=DS)
    path = folder(tmp_path) / "shop.yaml"
    edited = DOCUMENT_SHOP.replace("# kept by hand", "description: edited meanwhile")
    with edited_after_first_read(path, text=edited) as done:
        result = await asyncio.to_thread(upgrade, store_path(storage))
    assert done
    assert result.code == 1, result
    assert conflict_reported(result.out + result.err, name=DS)
    assert path.read_text(encoding="utf-8") == edited
    assert (legacy_folder(tmp_path) / "returns.yaml").exists()
    again = await asyncio.to_thread(upgrade, store_path(storage))
    assert again.code == 0, again
    assert refs(parse(path.read_text(encoding="utf-8"))) == ["returns.dataset.yaml"]


async def test_a_concurrent_edit_of_the_model_document_row_is_not_overwritten(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec scenario: A concurrent edit is not overwritten."""
    storage = await legacy_store("sqlite", tmp_path, _returns())
    place_2_0_document("sqlite", tmp_path, DOCUMENT_SHOP, name=DS)
    edited = DOCUMENT_SHOP.replace("# kept by hand", "description: edited meanwhile")
    read = SQLiteStorage._get_semantic_model_text_impl
    edits: list[str] = []

    async def read_then_edit(self: SQLiteStorage, *, data_source: str, name: str) -> str | None:
        text = await read(self, data_source=data_source, name=name)
        if name == DS and not edits:
            edits.append(name)
            with transaction(str(sqlite_path(tmp_path))) as conn:
                conn.execute("UPDATE semantic_models SET text = ? WHERE data_source = ? AND name = ?",
                             (edited, DS, DS))
        return text

    monkeypatch.setattr(SQLiteStorage, "_get_semantic_model_text_impl", read_then_edit)
    result = await asyncio.to_thread(upgrade, store_path(storage))
    monkeypatch.undo()
    assert edits == [DS]
    assert result.code == 1, result
    assert conflict_reported(result.out + result.err, name=DS)
    assert legacy_rows(tmp_path) == [(DS, "returns")]
    with open_connection(sqlite_path(tmp_path)) as con:
        [(kept,)] = con.execute("SELECT text FROM semantic_models WHERE data_source = ? AND name = ?", (DS, DS))
    assert kept == edited
    again = await asyncio.to_thread(upgrade, store_path(storage))
    assert again.code == 0, again
    assert refs(model_documents("sqlite", tmp_path)[DS]) == ["returns.dataset.yaml"]
    assert model_documents("sqlite", tmp_path)[DS]["description"] == "edited meanwhile"


def _layout(tmp: Path) -> dict[str, bytes]:
    """The stored documents and legacy files of a YAML store, without temporaries."""
    root = tmp / "store"
    return {str(p.relative_to(root)): p.read_bytes() for sub in ("semantic_models", "models")
            for p in sorted((root / sub).rglob("*")) if p.is_file() and ".tmp." not in p.name}


async def test_an_interrupted_upgrade_converges(tmp_path: Path) -> None:
    reference = tmp_path / "reference"
    reference.mkdir()
    storage = await legacy_store("yaml", reference, _orders(), _customers(), _returns())
    with file_ops(reference / "store") as ops:
        assert (await asyncio.to_thread(upgrade, store_path(storage))).code == 0
    expected = _layout(reference)
    assert model_documents("yaml", reference)[DS]
    assert ops
    for n in range(len(ops)):
        run_dir = tmp_path / f"run{n}"
        run_dir.mkdir()
        shutil.copy(reference / "shop.duckdb", run_dir / "shop.duckdb")
        interrupted = await legacy_store("yaml", run_dir, _orders(), _customers(), _returns())
        with file_ops(run_dir / "store", stop_after=n), contextlib.suppress(Interrupted):
            await asyncio.to_thread(upgrade, store_path(interrupted))
        gc.collect()
        result = await asyncio.to_thread(upgrade, store_path(interrupted))
        assert result.code == 0, (n, result)
        assert _layout(run_dir) == expected, n


# --------------------------------------------------------------------------- #
# Saves, released stores, quiet loads
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_a_save_of_a_legacy_dataset_writes_the_final_form(tmp_path: Path, kind: str) -> None:
    storage = await legacy_store(kind, tmp_path)
    orders = await storage.get_dataset("orders", data_source=DS)
    assert orders is not None
    await storage.save_dataset(orders.model_copy(update={"description": "saved"}))
    assert stored_dataset(kind, tmp_path, "orders")["description"] == "saved"
    assert "orders.dataset.yaml" in refs(model_documents(kind, tmp_path)[DS])
    if kind == "yaml":
        assert not (legacy_folder(tmp_path) / "orders.yaml").exists()
        assert (legacy_folder(tmp_path) / "customers.yaml").exists()
    else:
        assert legacy_rows(tmp_path) == [(DS, "customers")]


@pytest.mark.parametrize("kind", STORAGES)
async def test_an_upgraded_store_loads_quietly_as_a_datasource_model(tmp_path: Path, kind: str) -> None:
    storage = await legacy_store(kind, tmp_path)
    assert (await asyncio.to_thread(upgrade, store_path(storage))).code == 0
    with recorded() as record:
        fresh = reopen(kind, tmp_path)
        loaded, errs = await fresh.load_datasets(data_source=DS)
    assert errs == []
    assert {d.name for d in loaded} == {"orders", "customers"}
    assert upgrade_warning_names(record) == []
    assert await fresh.list_semantic_models(data_source=DS) == [(DS, DS)]


RELEASES = [
    pytest.param(("v0_10_2", ("shop", "cube", "lite"), ("data.duckdb", "data.sqlite")), id="0.10.2"),
    pytest.param(("v1_8_2", ("shop",), ("data.duckdb",)), id="1.8.2"),
]


def _released_documents(storage: StorageBackend, data_source: str) -> tuple[list[str], dict[str, dict[str, Any]]]:
    """``(legacy dataset names, {file name: parsed document})`` of a released store's datasource."""
    if isinstance(storage, YAMLStorage):
        root = Path(storage.base_dir)
        legacy_names = sorted(p.stem for p in (root / "models" / data_source).glob("*.yaml"))
        docs = {p.name: parse(p.read_text(encoding="utf-8"))
                for p in sorted((root / "semantic_models" / data_source).glob("*"))
                if p.is_file() and not p.name.startswith(".")}
        return legacy_names, docs
    assert isinstance(storage, SQLiteStorage)
    with open_connection(Path(storage.db_path)) as con:
        legacy_names = sorted(n for (n,) in con.execute("SELECT name FROM models WHERE data_source = ?", (data_source,)))
        docs = {f: parse(t) for f, t in con.execute(
            "SELECT file_name, text FROM documents WHERE data_source = ?", (data_source,))}
    return legacy_names, docs


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize("release", RELEASES)
async def test_a_released_store_upgrades_into_the_layout(tmp_path: Path, kind: str,
                                                        release: tuple[str, tuple[str, ...], tuple[str, ...]]) -> None:
    name, datasources, data_files = release
    corpus = Corpus.load(release=name, datasources=datasources, data_files=data_files)
    storage = open_store(corpus, backend=kind, tmp=tmp_path, online=True)
    identities = await storage._list_all_dataset_identities()
    result = await asyncio.to_thread(upgrade, store_path(storage))
    assert result.code == 0, result
    fresh = YAMLStorage(base_dir=store_path(storage)) if kind == "yaml" else SQLiteStorage(db_path=store_path(storage))
    for data_source in datasources:
        legacy_names, docs = _released_documents(fresh, data_source)
        assert legacy_names == []
        stored = {d["dataset"]["name"] for d in docs.values() if "dataset" in d}
        stored |= {e["name"] for d in docs.values() if "dataset" not in d for e in d.get("datasets") or []}
        assert {n for ds, n in identities if ds == data_source} <= stored
        assert data_source in {d.get("name") for d in docs.values() if "dataset" not in d}
    for key in sorted(corpus.expected):
        assert await run_recorded(corpus, fresh, key) == corpus.expected[key]["rows"], key


# --------------------------------------------------------------------------- #
# Unchanged: custom backends and the public entry point
# --------------------------------------------------------------------------- #
async def test_a_custom_backend_keeps_standalone_datasets() -> None:
    backend = DatasetsOnlyBackend()
    await backend.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=":memory:"))
    await backend.save_dataset(_customers(), _validate=False)
    assert isinstance(backend.datasets[(DS, "customers")], SlayerDataset)
    assert await backend.list_semantic_models() == []
    assert await backend.semantic_model_owner("customers", data_source=DS) is None


def test_the_public_entry_point_returns_dataset_dicts() -> None:
    result = upgrade_datasets(documents=[legacy(_orders()), legacy(_customers())], datasource_type="duckdb")
    assert result.refused == []
    assert sorted(d["name"] for d in result.documents) == ["customers", "orders"]
    for document in result.documents:
        assert document["data_source"] == DS
        assert "dataset" not in document
        assert SlayerDataset.model_validate(document).sql_table == document["sql_table"]
