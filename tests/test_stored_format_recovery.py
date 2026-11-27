"""Spec: models/stored-format — serialised and recoverable writes, isolated resolution errors, shared and stray
dataset documents."""

from __future__ import annotations

import asyncio
import re
import threading
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any

import pytest
import yaml

from slayer.core.models import SlayerDataset
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_conn import transaction
from tests._stored_format_fixtures import (
    DS, STORAGES, Interrupted, dataset_file, dataset_documents, file_ops, folder, messages, model_documents,
    model_file, parse, recorded, reopen, snapshot, sqlite_path, store_with_datasource, stored_dataset, table_dataset,
    texts,
)

VERSION = "0.3.0.dev0"
RACE_ROUNDS = 12
#: An atomic write's temporary file, which a crash may leave behind.
_TEMP_FILE = re.compile(r"^\..+\.tmp\.[^.]{8}$")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def failed(errs: Sequence[Any], kind: str) -> dict[str, str]:
    """``{name: cause_text}`` of the load failures of ``kind``."""
    return {e.name: e.cause_text for e in errs if e.kind == kind}


def dataset_doc(name: str, *, fields: Sequence[str] = ("id",), version: str = VERSION) -> str:
    entry = {"name": name, "source": name, "fields": [{"name": f, "expression": {"sql": f}, "datatype": "String"}
                                                       for f in fields]}
    return yaml.safe_dump({"version": version, "dataset": entry}, sort_keys=False)


def split_model(name: str, *, refs: Sequence[tuple[str, str]], **extra: Any) -> str:
    """A split model document whose ``dataset_refs`` are ``(name, iri)`` pairs."""
    doc = {"version": VERSION, "name": name, **extra, "dataset_refs": [{"name": n, "iri": i} for n, i in refs]}
    return yaml.safe_dump(doc, sort_keys=False)


def flat_model(name: str, *, datasets: Sequence[str]) -> str:
    entries = [yaml.safe_load(dataset_doc(d))["dataset"] for d in datasets]
    return yaml.safe_dump({"version": VERSION, "name": name, "datasets": entries}, sort_keys=False)


def place(kind: str, base: Path, files: dict[str, str]) -> None:
    """Store ``files`` (file name → text) as hand-placed documents of ``DS``."""
    if kind == "yaml":
        root = folder(base)
        root.mkdir(parents=True, exist_ok=True)
        for name, text in files.items():
            (root / name).write_text(text, encoding="utf-8")
        return
    with transaction(sqlite_path(base)) as con:
        for name, text in files.items():
            con.execute("INSERT INTO documents (data_source, file_name, text) VALUES (?, ?, ?)", (DS, name, text))


async def loaded_names(storage: StorageBackend) -> set[str]:
    loaded, _ = await storage.load_datasets(data_source=DS)
    return {d.name for d in loaded}


def holder(kind: str, base: Path, name: str) -> str:
    """The file storing dataset ``name`` (exact spelling): its dataset document, or the model document declaring it."""
    hits = [f for f, t in texts(kind, base).items() if isinstance(d := parse(t), dict) and (
        d.get("dataset", {}).get("name") == name or name in [e["name"] for e in d.get("datasets") or []])]
    assert len(hits) == 1, f"dataset {name!r}: {hits}"
    return hits[0]


def owners(kind: str, base: Path) -> dict[str, str]:
    """``{dataset name: model name}`` of every dataset the stored model documents reference or declare inline."""
    out: dict[str, str] = {}
    for model, doc in model_documents(kind, base).items():
        for ref in doc.get("dataset_refs") or []:
            out[ref["name"]] = model
        for entry in doc.get("datasets") or []:
            out[entry["name"]] = model
    return out


# --------------------------------------------------------------------------- #
# Concurrent writes
# --------------------------------------------------------------------------- #
def race(*saves: Callable[[], Awaitable[None]]) -> list[BaseException]:
    """Run ``saves`` at once, each in its own thread and event loop; the errors they raised."""
    barrier = threading.Barrier(len(saves))
    errors: list[BaseException] = []

    def run(save: Callable[[], Awaitable[None]]) -> None:
        barrier.wait()
        try:
            asyncio.run(save())  # type: ignore[arg-type]
        except BaseException as exc:  # noqa: BLE001 — reported by the test
            errors.append(exc)

    threads = [threading.Thread(target=run, args=(s,)) for s in saves]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return errors


def saver(kind: str, base: Path, dataset: SlayerDataset) -> Callable[[], Awaitable[None]]:
    """A storage-level save of ``dataset`` through its own storage instance."""
    async def save() -> None:
        await reopen(kind, base).save_dataset(dataset, _validate=False)
    return save


@pytest.mark.parametrize("kind", STORAGES)
async def test_concurrent_creation_in_two_models(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Concurrent creation in two models."""
    for i in range(RACE_ROUNDS):
        base = tmp_path / str(i)
        base.mkdir()
        storage = await store_with_datasource(kind, base)
        await storage.save_semantic_model_text(data_source=DS, name="sales", text=flat_model("sales", datasets=["customers"]))
        upper = table_dataset("Orders", fields=["id", "a_only"]).model_copy(update={"semantic_model": "sales"})
        lower = table_dataset("orders", fields=["id", "b_only"])
        assert race(saver(kind, base, upper), saver(kind, base, lower)) == []
        assert {f["name"] for f in stored_dataset(kind, base, "Orders")["fields"]} == {"id", "a_only"}
        assert {f["name"] for f in stored_dataset(kind, base, "orders")["fields"]} == {"id", "b_only"}
        assert holder(kind, base, "Orders") != holder(kind, base, "orders")
        assert (owners(kind, base)["Orders"], owners(kind, base)["orders"]) == ("sales", DS)
        assert "customers" in await loaded_names(reopen(kind, base))


@pytest.mark.parametrize("kind", STORAGES)
async def test_concurrent_creations_in_the_datasource_model_lose_no_reference(tmp_path: Path, kind: str) -> None:
    for i in range(RACE_ROUNDS):
        base = tmp_path / str(i)
        base.mkdir()
        storage = await store_with_datasource(kind, base)
        await storage.save_dataset(table_dataset("seed"), _validate=False)
        assert race(saver(kind, base, table_dataset("alpha")), saver(kind, base, table_dataset("beta"))) == []
        assert sorted(r["name"] for r in model_documents(kind, base)[DS]["dataset_refs"]) == ["alpha", "beta", "seed"]
        assert {"alpha", "beta", "seed"} <= await loaded_names(reopen(kind, base))


# --------------------------------------------------------------------------- #
# Interrupted multi-file writes (YAML)
# --------------------------------------------------------------------------- #
Step = Callable[[StorageBackend], Awaitable[Any]]


async def _shop_with(storage: StorageBackend, *names: str) -> None:
    for name in names:
        await storage.save_dataset(table_dataset(name), _validate=False)


async def _setup_two(storage: StorageBackend) -> None:
    await _shop_with(storage, "orders", "customers")


async def _setup_sales_and_two(storage: StorageBackend) -> None:
    await _shop_with(storage, "products", "orders", "customers")
    await storage.create_semantic_model(data_source=DS, name="sales", datasets=["products"])


async def _setup_one(storage: StorageBackend) -> None:
    await _shop_with(storage, "orders")


async def _create(storage: StorageBackend) -> None:
    await storage.save_dataset(table_dataset("orders"), _validate=False)


async def _setup_customers(storage: StorageBackend) -> None:
    await _shop_with(storage, "customers")


async def _create_model(storage: StorageBackend) -> None:
    await storage.create_semantic_model(data_source=DS, name="sales", datasets=["orders"])


async def _add_datasets(storage: StorageBackend) -> None:
    await storage.edit_semantic_model("sales", data_source=DS, add_datasets=["orders"])


async def _delete(storage: StorageBackend) -> None:
    await storage.delete_dataset("orders", data_source=DS)


class Operation:
    """A multi-file write: its setup, the write, and the owners it leaves (``None`` for no stored model)."""

    def __init__(self, *, setup: Step, act: Step, before: dict[str, str], after: dict[str, str]) -> None:
        self.setup, self.act, self.before, self.after = setup, act, before, after


OPERATIONS = {
    "create": Operation(setup=_setup_customers, act=_create, before={"customers": DS},
                        after={"customers": DS, "orders": DS}),
    "move-by-create": Operation(setup=_setup_two, act=_create_model, before={"orders": DS, "customers": DS},
                                after={"orders": "sales", "customers": DS}),
    "move-by-add": Operation(setup=_setup_sales_and_two, act=_add_datasets,
                             before={"products": "sales", "orders": DS, "customers": DS},
                             after={"products": "sales", "orders": "sales", "customers": DS}),
    "delete": Operation(setup=_setup_two, act=_delete, before={"orders": DS, "customers": DS},
                        after={"customers": DS}),
    "delete-last": Operation(setup=_setup_one, act=_delete, before={"orders": DS}, after={}),
}


async def _fresh(base: Path, op: Operation) -> StorageBackend:
    storage = await store_with_datasource("yaml", base)
    await op.setup(storage)
    return storage


async def _model_of(storage: StorageBackend) -> dict[str, str]:
    """``{dataset: owning model}`` as a load reads it."""
    return {n: str(await storage.semantic_model_owner(n, DS)) for n in await loaded_names(storage)}


def _only_documents(base: Path) -> None:
    root = folder(base)
    if not root.exists():
        return
    for path in root.iterdir():
        if path.name == ".write.lock":  # the folder's write lock, never removed
            continue
        if path.name.startswith("."):
            assert _TEMP_FILE.match(path.name), f"leftover {path.name}"
            continue
        doc = parse(path.read_text(encoding="utf-8"))
        assert isinstance(doc, dict), path.name
        assert "dataset" in doc or "dataset_refs" in doc or "datasets" in doc, path.name


async def _count_ops(base: Path, op: Operation) -> list[str]:
    storage = await _fresh(base, op)
    with file_ops(folder(base)) as seen:
        await op.act(storage)
    return list(seen)


@pytest.mark.parametrize("name", list(OPERATIONS))
async def test_a_multi_file_write_is_recorded_in_several_file_writes(tmp_path: Path, name: str) -> None:
    op = OPERATIONS[name]
    ops = await _count_ops(tmp_path, op)
    assert len(ops) >= 2, ops
    assert await _model_of(reopen("yaml", tmp_path)) == op.after
    assert owners("yaml", tmp_path) == op.after


@pytest.mark.parametrize("name", list(OPERATIONS))
async def test_an_interrupted_write_reads_as_completed_and_the_next_write_completes_it(tmp_path: Path,
                                                                                      name: str) -> None:
    """Spec scenario: An interrupted move."""
    op = OPERATIONS[name]
    total = len(await _count_ops(tmp_path / "count", op))
    assert total >= 2
    for n in range(1, total):
        base = tmp_path / str(n)
        storage = await _fresh(base, op)
        with file_ops(folder(base), stop_after=n), pytest.raises(Interrupted):
            await op.act(storage)
        reader = reopen("yaml", base)
        before = snapshot(base)
        assert await _model_of(reader) == op.after, f"after {n} of {total} file writes"
        assert snapshot(base) == before, "a load wrote"
        writer = reopen("yaml", base)
        await writer.save_dataset(table_dataset("extra"), _validate=False)
        _only_documents(base)
        with recorded() as record:
            assert await _model_of(reopen("yaml", base)) == {**op.after, "extra": DS}
        assert not [m for m in messages(record) if ".dataset.yaml" in m], messages(record)
        assert {k: v for k, v in owners("yaml", base).items() if k != "extra"} == op.after


@pytest.mark.parametrize("name", list(OPERATIONS))
async def test_a_write_interrupted_before_its_first_file_write_changes_nothing(tmp_path: Path, name: str) -> None:
    op = OPERATIONS[name]
    storage = await _fresh(tmp_path, op)
    before = snapshot(tmp_path)
    assert owners("yaml", tmp_path) == op.before
    with file_ops(folder(tmp_path), stop_after=0), pytest.raises(Interrupted):
        await op.act(storage)
    assert snapshot(tmp_path) == before
    assert await _model_of(reopen("yaml", tmp_path)) == op.before


def _index(ops: list[str], *, verb: str, file_name: str) -> int:
    hits = [i for i, o in enumerate(ops) if o.startswith(f"{verb} ") and Path(o.split(" ", 1)[1]).name == file_name]
    assert hits, f"no {verb} of {file_name}: {ops}"
    return hits[0]


async def test_a_dataset_document_is_written_before_its_reference(tmp_path: Path) -> None:
    ops = await _count_ops(tmp_path, OPERATIONS["create"])
    dataset_at = _index(ops, verb="replace", file_name=dataset_file("yaml", tmp_path, "orders"))
    model_at = _index(ops, verb="replace", file_name=model_file("yaml", tmp_path, DS))
    assert dataset_at < model_at


async def test_a_reference_is_removed_before_its_dataset_document(tmp_path: Path) -> None:
    op = OPERATIONS["delete"]
    storage = await _fresh(tmp_path, op)
    orders_file, model_name = dataset_file("yaml", tmp_path, "orders"), model_file("yaml", tmp_path, DS)
    with file_ops(folder(tmp_path)) as seen:
        await op.act(storage)
    assert _index(list(seen), verb="replace", file_name=model_name) < _index(list(seen), verb="unlink",
                                                                              file_name=orders_file)
    assert not (folder(tmp_path) / orders_file).exists()


# --------------------------------------------------------------------------- #
# Resolution errors
# --------------------------------------------------------------------------- #
OTHER = flat_model("other", datasets=["customers"])
ORDERS_REF = ("orders", "orders.dataset.yaml")


class Broken:
    """A ``sales`` model that does not resolve: its files, and the files its error must name."""

    def __init__(self, *, refs: Sequence[tuple[str, str]], files: dict[str, str], involved: Sequence[str]) -> None:
        self.refs, self.files, self.involved = refs, files, involved


BROKEN = {
    "iri-with-slash": Broken(refs=[ORDERS_REF, ("returns", "a/b.dataset.yaml")], files={}, involved=["a/b.dataset.yaml"]),
    "iri-leaving-the-folder": Broken(refs=[ORDERS_REF, ("returns", "../x.dataset.yaml")], files={},
                                     involved=["../x.dataset.yaml"]),
    "missing-file": Broken(refs=[ORDERS_REF, ("returns", "returns.dataset.yaml")], files={},
                           involved=["returns.dataset.yaml"]),
    "case-differs": Broken(refs=[ORDERS_REF, ("returns", "Returns.dataset.yaml")],
                           files={"returns.dataset.yaml": dataset_doc("returns")}, involved=["Returns.dataset.yaml"]),
    "not-yaml": Broken(refs=[ORDERS_REF, ("returns", "returns.txt")], files={"returns.txt": "{unclosed: [\n"},
                       involved=["returns.txt"]),
    "not-a-dataset-document": Broken(refs=[ORDERS_REF, ("returns", "returns.dataset.yaml")],
                                     files={"returns.dataset.yaml": flat_model("returns", datasets=["returns"])},
                                     involved=["returns.dataset.yaml"]),
    "version-differs": Broken(refs=[ORDERS_REF, ("returns", "returns.dataset.yaml")],
                              files={"returns.dataset.yaml": dataset_doc("returns", version="0.2.0.dev0")},
                              involved=["returns.dataset.yaml"]),
    "name-differs": Broken(refs=[ORDERS_REF, ("returns", "returns.dataset.yaml")],
                           files={"returns.dataset.yaml": dataset_doc("refunds")}, involved=["returns.dataset.yaml"]),
    "two-references-to-one-file": Broken(refs=[ORDERS_REF, ("returns", "returns.dataset.yaml"),
                                               ("returns", "returns.dataset.yaml")],
                                         files={"returns.dataset.yaml": dataset_doc("returns")},
                                         involved=["returns.dataset.yaml"]),
}


async def _broken_store(kind: str, base: Path, broken: Broken) -> StorageBackend:
    storage = await store_with_datasource(kind, base)
    place(kind, base, {"sales.semantic_model.yaml": split_model("sales", refs=broken.refs),
                       "orders.dataset.yaml": dataset_doc("orders"), "other.semantic_model.yaml": OTHER,
                       **broken.files})
    return storage


def _assert_isolated(loaded: set[str], errs: Sequence[Any], *, involved: Sequence[str]) -> None:
    assert loaded == {"customers"}
    cause = failed(errs, "semantic_model")["sales"]
    for name in involved:
        assert name in cause, cause
    assert "other" not in failed(errs, "semantic_model")


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_resolving_split_model_loads(tmp_path: Path, kind: str) -> None:
    storage = await _broken_store(kind, tmp_path, Broken(refs=[ORDERS_REF, ("returns", "returns.dataset.yaml")],
                                                         files={"returns.dataset.yaml": dataset_doc("returns")},
                                                         involved=[]))
    loaded, errs = await storage.load_datasets(data_source=DS)
    assert errs == []
    assert {d.name for d in loaded} == {"orders", "returns", "customers"}
    assert await storage.semantic_model_owner("returns", DS) == "sales"


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize("case", list(BROKEN))
async def test_a_model_that_does_not_resolve_is_isolated(tmp_path: Path, kind: str, case: str) -> None:
    broken = BROKEN[case]
    storage = await _broken_store(kind, tmp_path, broken)
    loaded, errs = await storage.load_datasets(data_source=DS)
    _assert_isolated({d.name for d in loaded}, errs, involved=broken.involved)


async def test_a_reference_to_a_missing_file(tmp_path: Path) -> None:
    """Spec scenario: A reference to a missing file."""
    storage = await _broken_store("yaml", tmp_path, BROKEN["missing-file"])
    loaded, errs = await storage.load_datasets(data_source=DS)
    cause = failed(errs, "semantic_model")["sales"]
    assert "sales" in cause or "sales.semantic_model.yaml" in cause
    assert "returns.dataset.yaml" in cause
    assert {d.name for d in loaded} == {"customers"}


async def test_an_iri_naming_a_file_in_a_subfolder_is_refused(tmp_path: Path) -> None:
    storage = await _broken_store("yaml", tmp_path, BROKEN["iri-with-slash"])
    (folder(tmp_path) / "a").mkdir()
    (folder(tmp_path) / "a" / "b.dataset.yaml").write_text(dataset_doc("returns"), encoding="utf-8")
    loaded, errs = await storage.load_datasets(data_source=DS)
    _assert_isolated({d.name for d in loaded}, errs, involved=["a/b.dataset.yaml"])


async def test_an_iri_leading_out_of_the_folder_is_refused_even_when_the_file_exists(tmp_path: Path) -> None:
    storage = await _broken_store("yaml", tmp_path, BROKEN["iri-leaving-the-folder"])
    (folder(tmp_path).parent / "x.dataset.yaml").write_text(dataset_doc("returns"), encoding="utf-8")
    loaded, errs = await storage.load_datasets(data_source=DS)
    _assert_isolated({d.name for d in loaded}, errs, involved=["../x.dataset.yaml"])


async def test_a_reference_to_a_directory_is_refused(tmp_path: Path) -> None:
    storage = await _broken_store("yaml", tmp_path, BROKEN["missing-file"])
    (folder(tmp_path) / "returns.dataset.yaml").mkdir()
    loaded, errs = await storage.load_datasets(data_source=DS)
    _assert_isolated({d.name for d in loaded}, errs, involved=["returns.dataset.yaml"])


async def test_a_symlink_leading_out_of_the_folder_is_refused(tmp_path: Path) -> None:
    storage = await _broken_store("yaml", tmp_path, BROKEN["missing-file"])
    outside = tmp_path / "outside.dataset.yaml"
    outside.write_text(dataset_doc("returns"), encoding="utf-8")
    (folder(tmp_path) / "returns.dataset.yaml").symlink_to(outside)
    loaded, errs = await storage.load_datasets(data_source=DS)
    _assert_isolated({d.name for d in loaded}, errs, involved=["returns.dataset.yaml"])


async def test_a_symlink_within_the_folder_resolves(tmp_path: Path) -> None:
    storage = await _broken_store("yaml", tmp_path, BROKEN["missing-file"])
    (folder(tmp_path) / "returns.data.yaml").write_text(dataset_doc("returns"), encoding="utf-8")
    (folder(tmp_path) / "returns.dataset.yaml").symlink_to(folder(tmp_path) / "returns.data.yaml")
    loaded, errs = await storage.load_datasets(data_source=DS)
    assert not failed(errs, "semantic_model")
    assert {d.name for d in loaded} == {"orders", "returns", "customers"}


async def test_an_undecodable_dataset_document_is_refused(tmp_path: Path) -> None:
    storage = await _broken_store("yaml", tmp_path, BROKEN["missing-file"])
    (folder(tmp_path) / "returns.dataset.yaml").write_bytes(b"\xff\xfe not utf-8")
    loaded, errs = await storage.load_datasets(data_source=DS)
    _assert_isolated({d.name for d in loaded}, errs, involved=["returns.dataset.yaml"])


# --------------------------------------------------------------------------- #
# Shared and stray dataset documents
# --------------------------------------------------------------------------- #
SHARED_SALES = yaml.safe_dump({
    "version": VERSION, "name": "sales",
    "dataset_refs": [{"name": "orders", "iri": "orders.dataset.yaml"}, {"name": "customers", "iri": "customers.dataset.yaml"}],
    "relationships": [{"name": "orders_to_customers", "from": "orders", "to": "customers",
                       "from_columns": ["customer_id"], "to_columns": ["customer_id"]}],
    "metrics": [{"name": "revenue", "expression": {"sql": "SUM(orders.amount)"}},
                {"name": "regions", "expression": {"sql": "COUNT(DISTINCT customers.region)"}}],
}, sort_keys=False)
SHARED_LEDGER = split_model("ledger", refs=[("orders", "orders.dataset.yaml")])


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_dataset_document_listed_by_two_models_is_quarantined_in_both(tmp_path: Path, kind: str) -> None:
    storage = await store_with_datasource(kind, tmp_path)
    place(kind, tmp_path, {"sales.semantic_model.yaml": SHARED_SALES, "ledger.semantic_model.yaml": SHARED_LEDGER,
                           "orders.dataset.yaml": dataset_doc("orders", fields=("order_id", "customer_id", "amount")),
                           "customers.dataset.yaml": dataset_doc("customers", fields=("customer_id", "region"))})
    loaded, errs = await storage.load_datasets(data_source=DS)
    assert {d.name for d in loaded} == {"customers"}
    assert len([e for e in errs if e.kind == "dataset" and e.name == "orders"]) == 2
    for entity_kind, name in (("relationship", "orders_to_customers"), ("metric", "revenue")):
        [hit] = [e for e in errs if e.kind == entity_kind and e.name == name]
        assert "orders" in hit.cause_text
    assert not [e for e in errs if e.name in {"customers", "regions"}]
    assert {m.name for m in await storage.list_model_scoped_metrics(DS)} == {"regions"}


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_stray_dataset_document(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A stray dataset document."""
    storage = await store_with_datasource(kind, tmp_path)
    place(kind, tmp_path, {f"{DS}.semantic_model.yaml": split_model(DS, refs=[ORDERS_REF]),
                           "orders.dataset.yaml": dataset_doc("orders"), "draft.dataset.yaml": dataset_doc("draft")})
    with recorded() as record:
        loaded, errs = await storage.load_datasets(data_source=DS)
    assert errs == []
    assert {d.name for d in loaded} == {"orders"}
    assert await storage.get_dataset("draft", data_source=DS) is None
    assert any("draft.dataset.yaml" in m for m in messages(record)), messages(record)
    assert set(dataset_documents(kind, tmp_path)) == {"orders.dataset.yaml", "draft.dataset.yaml"}
    assert set(texts(kind, tmp_path)) == {f"{DS}.semantic_model.yaml", "orders.dataset.yaml", "draft.dataset.yaml"}
