"""Spec: importers/osi-import and models/semantic-model-documents — repeated mapping keys and unbuildable scalars."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.inspect.service import InspectService
from slayer.osi.exporter import export_osi
from slayer.search.service import SearchService
from slayer.storage.access_filtered import AccessFilteredStorage
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_conn import transaction
from tests._cli_inprocess import run_cli_in_process
from tests._engine_helpers import owned_query_engine
from tests._dev2243_fixtures import (
    DS, SALES, STORAGES, dataset, document_engine, document_text, error, field, importer_module,
    make_storage, place_document, store_document, upstream_validate,
)

SUPPORT = document_text(name="support", datasets=[
    dataset("returns", primary_key=["return_id"], fields=[field("return_id"), field("customer_id")]),
])

#: ``orders`` declares ``source`` twice.
DUP_SOURCE = SALES.replace("    source: orders\n", "    source: orders\n    source: orders_archive\n", 1)
#: ``orders`` declares ``name`` twice and its field ``amount`` declares ``expression`` twice.
DUP_TWO = SALES.replace("  - name: orders\n", "  - name: orders\n    name: orders\n", 1).replace(
    "        expression: {sql: amount}\n", "        expression: {sql: amount}\n        expression: {sql: amount * 2}\n", 1)
#: The field ``status`` declares ``name`` twice.
DUP_FIELD_NAME = SALES.replace("      - name: status\n", "      - name: status\n        name: state\n", 1)
#: A repeated key inside a flow mapping, five levels down.
DUP_DEEP = SALES.replace("expression: {sql: amount}", "expression: {sql: amount, sql: amount * 2}", 1)
#: A repeated key beside a key Ossie does not define.
DUP_AND_UNKNOWN = DUP_SOURCE.replace("    primary_key: [order_id]\n", "    primary_key: [order_id]\n    colour: red\n", 1)
#: ``SALES`` as JSON, ``orders`` declaring ``source`` twice.
DUP_JSON = json.dumps(yaml.safe_load(SALES), indent=2).replace(
    '"source": "orders",', '"source": "orders",\n      "source": "orders_archive",', 1)
BAD_DATE = SALES.replace("description: Orders and customers.", "description: 2024-02-30", 1)
BAD_INT = SALES.replace("description: Orders and customers.", "description: !!int abc", 1)
EQUAL_BY_VALUE = SALES.replace("  - name: customers\n", "  - name: customers\n    nonportable_extensions:\n"
                               "      - vendor_name: ACME\n        data: {1: one, true: yes}\n", 1)

MERGED = """\
version: 0.3.0.dev0
name: merged
datasets:
  - name: orders
    source: orders
    primary_key: [order_id]
    fields:
      - &id_field
        name: order_id
        expression: {sql: order_id}
        datatype: String
        description: Identifier.
      - &amount_field
        name: amount
        expression: {sql: amount}
        datatype: Float
        label: Amount
      - <<: *id_field
        <<: *amount_field
        name: amount_doubled
        expression: {sql: amount * 2}
        datatype: Float
"""
MERGE_OVERRIDE = """\
version: 0.3.0.dev0
name: merged
datasets:
  - name: orders
    source: orders
    primary_key: [order_id]
    fields:
      - name: order_id
        expression: {sql: order_id}
        datatype: String
      - &amount_field
        name: amount
        expression: {sql: amount}
        datatype: Float
      - <<: *amount_field
        name: amount_doubled
        expression: {sql: amount * 2}
"""


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def line_of(text: str, fragment: str, *, nth: int = 1) -> int:
    """The 1-based line of the ``nth`` line of ``text`` containing ``fragment``."""
    hits = [i for i, line in enumerate(text.splitlines(), start=1) if fragment in line]
    return hits[nth - 1]


def names_repeated(reasons: str, *, key: str, line: int) -> bool:
    """``reasons`` name ``key`` as repeated at ``line``."""
    return key in reasons and f"line {line}" in reasons and re.search(r"repeat|duplicate", reasons, re.I) is not None


def names_scalar(reasons: str, *, value: str, line: int) -> bool:
    return value in reasons and f"line {line}" in reasons


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _no_database(_self: SlayerQueryEngine, _datasource: DatasourceConfig) -> Any:
    raise AssertionError("database accessed")


async def offline_storage(kind: str, base: Path, monkeypatch: pytest.MonkeyPatch) -> StorageBackend:
    storage = make_storage(kind, base)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(base / "absent.duckdb")))
    monkeypatch.setattr(SlayerQueryEngine, "client_for", _no_database)
    return storage


async def run_import(storage: StorageBackend, path: Path) -> Any:
    return await importer_module().import_osi(storage=storage, path=path, data_source=DS)


def refusal_text(result: Any, file_name: str) -> str:
    hits = [r for r in result.refused if Path(r.file).name == file_name]
    assert len(hits) == 1, result.refused
    return "\n".join(hits[0].reasons)


async def import_beside_support(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bad: str, *,
                                file_name: str = "bad.yaml") -> tuple[StorageBackend, str]:
    """Import ``bad`` and a valid ``support`` document from one folder; the bad file's reasons."""
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    folder = tmp_path / "in"
    write(folder / file_name, bad)
    write(folder / "good.yaml", SUPPORT)
    result = await run_import(storage, folder)
    assert result.stored == ["support"]
    assert await storage.get_semantic_model_text("support", DS) == SUPPORT
    assert await storage.get_semantic_model_text("sales", DS) is None
    return storage, refusal_text(result, file_name)


def failed(errs: list[Any], kind: str) -> dict[str, str]:
    return {e.name: e.cause_text for e in errs if e.kind == kind}


# --------------------------------------------------------------------------- #
# Import
# --------------------------------------------------------------------------- #
async def test_every_repeated_key_is_listed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec scenario: Every repeated key is listed."""
    _, reasons = await import_beside_support(tmp_path, monkeypatch, DUP_TWO)
    assert names_repeated(reasons, key="name", line=line_of(DUP_TWO, "    name: orders"))
    assert names_repeated(reasons, key="expression", line=line_of(DUP_TWO, "expression: {sql: amount * 2}"))


async def test_a_repeated_key_in_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec scenario: A repeated key in JSON."""
    _, reasons = await import_beside_support(tmp_path, monkeypatch, DUP_JSON, file_name="bad.json")
    assert names_repeated(reasons, key="source", line=line_of(DUP_JSON, '"source": "orders_archive"'))


async def test_a_repeated_key_at_depth_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, reasons = await import_beside_support(tmp_path, monkeypatch, DUP_DEEP)
    assert names_repeated(reasons, key="sql", line=line_of(DUP_DEEP, "sql: amount * 2"))


async def test_a_reader_failure_is_the_only_reason(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, reasons = await import_beside_support(tmp_path, monkeypatch, DUP_AND_UNKNOWN)
    assert names_repeated(reasons, key="source", line=line_of(DUP_AND_UNKNOWN, "source: orders_archive"))
    assert "colour" not in reasons


async def test_repeated_merge_directives_are_allowed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec scenario: Repeated merge directives are allowed (import)."""
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    result = await run_import(storage, write(tmp_path / "merged.yaml", MERGED))
    assert result.refused == []
    assert result.stored == ["merged"]
    assert await storage.get_semantic_model_text("merged", DS) == MERGED


@pytest.mark.parametrize("text", [MERGED, MERGE_OVERRIDE], ids=["two-merges", "one-merge"])
async def test_a_merged_field_takes_the_overriding_value(text: str) -> None:
    """Spec scenario: Repeated merge directives are allowed (the override wins)."""
    async with document_engine(documents=(text,)) as engine:
        resp = await engine.execute({"population": "orders",
                                     "metrics": [{"metric": "sum(orders.amount_doubled)", "alias": "v"}]})
        assert [float(r["v"]) for r in resp.data] == [1300.0]


async def test_two_merges_supply_their_disjoint_keys() -> None:
    async with document_engine(documents=(MERGED,)) as engine:
        orders = await engine.storage.get_dataset("orders", data_source=DS)
        assert orders is not None
        [doubled] = [f for f in orders.fields if f.name == "amount_doubled"]
        assert (doubled.description, doubled.label) == ("Identifier.", "Amount")


@pytest.mark.parametrize(("text", "value"), [(BAD_DATE, "2024-02-30"), (BAD_INT, "abc")], ids=["date", "int"])
async def test_an_unbuildable_scalar_is_a_reason(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, text: str,
                                                 value: str) -> None:
    """Spec scenario: An unbuildable scalar is a reason (import)."""
    _, reasons = await import_beside_support(tmp_path, monkeypatch, text)
    assert names_scalar(reasons, value=value, line=line_of(text, "description:"))


# --------------------------------------------------------------------------- #
# import-osi exit status
# --------------------------------------------------------------------------- #
async def _cli_import(tmp_path: Path, bad: str) -> tuple[StorageBackend, str, int]:
    storage = make_storage("yaml", tmp_path)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(tmp_path / "absent.duckdb")))
    folder = tmp_path / "in"
    write(folder / "bad.yaml", bad)
    write(folder / "good.yaml", SUPPORT)
    result = run_cli_in_process(["import-osi", str(folder), "--datasource", DS, "--storage", str(tmp_path / "store")])
    return storage, result.stdout + result.stderr, result.returncode


async def test_import_osi_exits_non_zero_on_a_repeated_key(tmp_path: Path) -> None:
    """Spec scenario: Every repeated key is listed (command exit status)."""
    storage, output, code = await _cli_import(tmp_path, DUP_TWO)
    assert code != 0
    assert "bad.yaml" in output
    assert names_repeated(output, key="expression", line=line_of(DUP_TWO, "expression: {sql: amount * 2}"))
    assert await storage.list_semantic_models(DS) == [(DS, "support")]


async def test_import_osi_reports_an_unbuildable_scalar_without_a_traceback(tmp_path: Path) -> None:
    """Spec scenario: An unbuildable scalar is a reason (command exit status)."""
    storage, output, code = await _cli_import(tmp_path, BAD_DATE)
    assert code != 0
    assert "Traceback" not in output
    assert names_scalar(output, value="2024-02-30", line=line_of(BAD_DATE, "description:"))
    assert await storage.list_semantic_models(DS) == [(DS, "support")]


# --------------------------------------------------------------------------- #
# Upstream loader parity
# --------------------------------------------------------------------------- #
LOADER_CASES = [
    pytest.param(DUP_DEEP, "repeated", ["sql"], "sql: amount * 2", id="repeated-key-at-depth"),
    pytest.param(MERGED, None, [], "", id="repeated-merge-directives"),
    pytest.param(MERGE_OVERRIDE, None, [], "", id="merge-override"),
    pytest.param(DUP_JSON, "repeated", ["source"], '"source": "orders_archive"', id="json-duplicate"),
    pytest.param(EQUAL_BY_VALUE, "repeated", ["true"], "data: {1: one, true: yes}", id="keys-equal-by-value"),
    pytest.param(BAD_DATE, "scalar", ["2024-02-30"], "description:", id="unbuildable-date"),
    pytest.param(BAD_INT, "scalar", ["abc"], "description:", id="unbuildable-int"),
]


async def _slayer_refusal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, text: str) -> str | None:
    """The refusal reasons of importing ``text`` alone, ``None`` when stored."""
    storage = await offline_storage("yaml", tmp_path, monkeypatch)
    result = await run_import(storage, write(tmp_path / "case.yaml", text))
    if not result.refused:
        assert result.stored
        return None
    return refusal_text(result, "case.yaml")


@pytest.mark.parametrize(("text", "verdict", "needles", "at"), LOADER_CASES)
async def test_loader_case_verdict(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, text: str, verdict: str | None,
                                   needles: list[str], at: str) -> None:
    reasons = await _slayer_refusal(tmp_path, monkeypatch, text)
    if verdict is None:
        assert reasons is None
        return
    assert reasons is not None
    line = line_of(text, at)
    for needle in needles:
        if verdict == "repeated":
            assert names_repeated(reasons.lower(), key=needle, line=line), reasons
        else:
            assert names_scalar(reasons, value=needle, line=line), reasons


@pytest.mark.parametrize(("text", "verdict", "needles", "at"), LOADER_CASES)
async def test_loader_case_matches_upstream(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, text: str,
                                            verdict: str | None, needles: list[str], at: str) -> None:
    done = upstream_validate([write(tmp_path / "upstream" / "case.yaml", text)], tmp=tmp_path)
    reasons = await _slayer_refusal(tmp_path, monkeypatch, text)
    assert (reasons is not None) == (done.returncode != 0), done.stdout + done.stderr
    assert (verdict is not None) == (done.returncode != 0)


# --------------------------------------------------------------------------- #
# Storage load
# --------------------------------------------------------------------------- #
UNREADABLE = [
    pytest.param(DUP_SOURCE, "repeated", "source", "source: orders_archive", id="yaml-repeated-key"),
    pytest.param(DUP_JSON, "repeated", "source", '"source": "orders_archive"', id="json-repeated-key"),
    pytest.param(BAD_DATE, "scalar", "2024-02-30", "description:", id="unbuildable-scalar"),
]


def names_problem(reasons: str, *, text: str, verdict: str, needle: str, at: str) -> bool:
    """``reasons`` name the repeated key or unbuildable value ``needle`` at the line of ``at`` in ``text``."""
    if verdict == "repeated":
        return names_repeated(reasons, key=needle, line=line_of(text, at))
    return names_scalar(reasons, value=needle, line=line_of(text, at))


async def hand_placed(kind: str, base: Path, text: str, *, name: str = "sales") -> StorageBackend:
    """A store holding ``text`` as document ``name``, written around SLayer's save checks."""
    if kind == "yaml":
        place_document(base, text, name=name)
    storage = make_storage(kind, base)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(base / "absent.duckdb")))
    if kind == "sqlite":
        with transaction(base / "store.db") as con:
            con.execute("INSERT INTO semantic_models (data_source, name, text) VALUES (?, ?, ?)", (DS, name, text))
    return storage


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize(("text", "verdict", "needle", "at"), UNREADABLE)
async def test_a_hand_placed_document_with_a_repeated_key(tmp_path: Path, kind: str, text: str, verdict: str,
                                                          needle: str, at: str) -> None:
    """Spec scenario: A hand-placed document with a repeated key."""
    storage = await hand_placed(kind, tmp_path, text)
    await store_document(storage, SUPPORT)
    loaded, errs = await storage.load_datasets(data_source=DS)
    assert {d.name for d in loaded} == {"returns"}
    assert names_problem(failed(errs, "semantic_model")["sales"], text=text, verdict=verdict, needle=needle, at=at)


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_hand_placed_document_with_merge_directives_loads(tmp_path: Path, kind: str) -> None:
    storage = await hand_placed(kind, tmp_path, MERGED, name="merged")
    loaded, errs = await storage.load_datasets(data_source=DS)
    assert errs == []
    assert {d.name for d in loaded} == {"orders"}


# --------------------------------------------------------------------------- #
# Save, create and edit
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_a_save_with_a_repeated_key(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A save with a repeated key."""
    storage = make_storage(kind, tmp_path)
    await store_document(storage, SALES)
    with pytest.raises(error("OssieDocumentInvalidError")) as info:
        await storage.save_semantic_model_text(data_source=DS, name="sales", text=DUP_FIELD_NAME)
    assert names_repeated(str(info.value), key="name", line=line_of(DUP_FIELD_NAME, "name: state"))
    assert await storage.get_semantic_model_text("sales", DS) == SALES


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_json_save_with_a_repeated_key(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    with pytest.raises(error("OssieDocumentInvalidError")) as info:
        await storage.save_semantic_model_text(data_source=DS, name="sales", text=DUP_JSON)
    assert names_repeated(str(info.value), key="source", line=line_of(DUP_JSON, '"source": "orders_archive"'))
    assert await storage.list_semantic_models(DS) == []


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_save_with_an_unbuildable_scalar(tmp_path: Path, kind: str) -> None:
    storage = make_storage(kind, tmp_path)
    with pytest.raises(error("OssieDocumentInvalidError")) as info:
        await storage.save_semantic_model_text(data_source=DS, name="sales", text=BAD_INT)
    assert names_scalar(str(info.value), value="abc", line=line_of(BAD_INT, "description:"))
    assert await storage.list_semantic_models(DS) == []


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize(("text", "verdict", "needle", "at"), UNREADABLE)
async def test_creating_from_unreadable_content_is_refused(tmp_path: Path, kind: str, text: str, verdict: str,
                                                          needle: str, at: str) -> None:
    storage = make_storage(kind, tmp_path)
    with pytest.raises(error("OssieDocumentInvalidError")) as info:
        await storage.create_semantic_model(data_source=DS, content=text)
    assert names_problem(str(info.value), text=text, verdict=verdict, needle=needle, at=at)
    assert await storage.list_semantic_models(DS) == []


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize(("text", "verdict", "needle", "at"), UNREADABLE)
async def test_an_edit_of_an_unreadable_document_is_refused(tmp_path: Path, kind: str, text: str, verdict: str,
                                                            needle: str, at: str) -> None:
    storage = await hand_placed(kind, tmp_path, text)
    with pytest.raises(error("OssieDocumentInvalidError")) as info:
        await storage.edit_semantic_model("sales", data_source=DS, description="Changed.")
    assert names_problem(str(info.value), text=text, verdict=verdict, needle=needle, at=at)
    assert await storage.get_semantic_model_text("sales", DS) == text


# --------------------------------------------------------------------------- #
# Other readers of stored documents
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize(("text", "verdict", "needle", "at"), UNREADABLE)
async def test_export_leaves_out_an_unreadable_document(tmp_path: Path, kind: str, text: str, verdict: str,
                                                        needle: str, at: str) -> None:
    storage = await hand_placed(kind, tmp_path, text)
    await store_document(storage, SUPPORT)
    async with owned_query_engine(storage=storage) as engine:
        exported = await export_osi(engine=engine, datasource=DS)
    assert exported.text == SUPPORT


@pytest.mark.parametrize(("text", "verdict", "needle", "at"), UNREADABLE)
async def test_inspect_names_why_a_document_does_not_load(tmp_path: Path, text: str, verdict: str, needle: str,
                                                          at: str) -> None:
    storage = await hand_placed("yaml", tmp_path, text)
    async with owned_query_engine(storage=storage) as engine:
        payload = json.loads(await InspectService(storage=storage, engine=engine).inspect(
            reference=f"{DS}.sales", entity_type="semantic_model", format="json"))
    assert payload["datasets"] == []
    assert payload["description"] is None
    assert names_problem("\n".join(payload["warnings"]), text=text, verdict=verdict, needle=needle, at=at)


async def test_search_indexes_an_unreadable_document_by_name(tmp_path: Path) -> None:
    storage = await hand_placed("yaml", tmp_path, BAD_DATE)
    await store_document(storage, SUPPORT)
    async with owned_query_engine(storage=storage) as engine:
        response = await SearchService(storage=storage, engine=engine).search(question="support returns",
                                                                              datasource=DS, max_results=50)
    assert response is not None


@pytest.mark.parametrize(("text", "verdict", "needle", "at"), UNREADABLE)
def test_listing_an_unreadable_document(tmp_path: Path, text: str, verdict: str, needle: str, at: str) -> None:
    place_document(tmp_path, text, name="sales")
    result = run_cli_in_process(["semantic-models", "list", "--datasource", DS, "--storage", str(tmp_path / "store")])
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"{DS}.sales: (no datasets)" in result.stdout


# --------------------------------------------------------------------------- #
# Access-filtered storage
# --------------------------------------------------------------------------- #
STORED_OPERATIONS = {
    "read": lambda s: s.get_semantic_model_text("sales", DS),
    "edit": lambda s: s.edit_semantic_model("sales", data_source=DS, description="Changed."),
    "delete": lambda s: s.delete_semantic_model("sales", DS),
}
WRITES = {
    "save": lambda s, text: s.save_semantic_model_text(data_source=DS, name="sales", text=text),
    "create": lambda s, text: s.create_semantic_model(data_source=DS, content=text),
    "adopt": lambda s, text: s.adopt_semantic_model(data_source=DS, name="sales", text=text, replacing=[]),
}


@pytest.mark.parametrize("operation", STORED_OPERATIONS)
@pytest.mark.parametrize(("text", "verdict", "needle", "at"), UNREADABLE)
async def test_a_filtered_caller_is_refused_an_unreadable_document(tmp_path: Path, operation: str, text: str,
                                                                    verdict: str, needle: str, at: str) -> None:
    storage = await hand_placed("yaml", tmp_path, text)
    filtered = AccessFilteredStorage(storage, tags=[], bypass=False)
    with pytest.raises(error("OssieDocumentInvalidError")) as info:
        await STORED_OPERATIONS[operation](filtered)
    assert names_problem(str(info.value), text=text, verdict=verdict, needle=needle, at=at)
    assert await storage.get_semantic_model_text("sales", DS) == text


async def test_a_bypass_caller_reads_and_deletes_an_unreadable_document(tmp_path: Path) -> None:
    storage = await hand_placed("yaml", tmp_path, DUP_SOURCE)
    admin = AccessFilteredStorage(storage, tags=[], bypass=True)
    assert await admin.get_semantic_model_text("sales", DS) == DUP_SOURCE
    assert await admin.delete_semantic_model("sales", DS)
    assert await storage.list_semantic_models(DS) == []


@pytest.mark.parametrize("write", WRITES)
@pytest.mark.parametrize(("text", "verdict", "needle", "at"), UNREADABLE)
async def test_a_filtered_write_of_unreadable_content_is_refused(tmp_path: Path, write: str, text: str, verdict: str,
                                                                 needle: str, at: str) -> None:
    storage = make_storage("yaml", tmp_path)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(tmp_path / "absent.duckdb")))
    filtered = AccessFilteredStorage(storage, tags=[], bypass=False)
    with pytest.raises(error("OssieDocumentInvalidError")) as info:
        await WRITES[write](filtered, text)
    assert names_problem(str(info.value), text=text, verdict=verdict, needle=needle, at=at)
    assert await storage.list_semantic_models(DS) == []


async def test_a_batch_inspect_keeps_each_documents_warnings(tmp_path: Path) -> None:
    storage = await hand_placed("yaml", tmp_path, BAD_DATE)
    await store_document(storage, SUPPORT)
    async with owned_query_engine(storage=storage) as engine:
        sales, support = json.loads(await InspectService(storage=storage, engine=engine).inspect(
            reference=[f"{DS}.sales", f"{DS}.support"], entity_type="semantic_model", format="json"))
    assert names_scalar("\n".join(sales["warnings"]), value="2024-02-30", line=line_of(BAD_DATE, "description:"))
    assert support["warnings"] == []


async def test_a_filtered_inspect_names_why_a_document_does_not_load(tmp_path: Path) -> None:
    storage = await hand_placed("yaml", tmp_path, DUP_SOURCE)
    filtered = AccessFilteredStorage(storage, tags=[], bypass=False)
    async with owned_query_engine(storage=storage) as engine:
        payload = json.loads(await InspectService(storage=filtered, engine=engine).inspect(
            reference=f"{DS}.sales", entity_type="semantic_model", format="json"))
    assert names_repeated("\n".join(payload["warnings"]), key="source", line=line_of(DUP_SOURCE, "orders_archive"))


async def test_a_filtered_search_survives_an_unreadable_document(tmp_path: Path) -> None:
    storage = await hand_placed("yaml", tmp_path, DUP_SOURCE)
    await store_document(storage, SUPPORT)
    filtered = AccessFilteredStorage(storage, tags=[], bypass=False)
    async with owned_query_engine(storage=storage) as engine:
        response = await SearchService(storage=filtered, engine=engine).search(question="returns", datasource=DS,
                                                                               max_results=50)
    assert f"{DS}.returns" in {hit.id for hit in response.results}


async def test_export_leaves_out_a_document_edited_unreadable_after_it_loaded(tmp_path: Path,
                                                                             monkeypatch: pytest.MonkeyPatch) -> None:
    storage = make_storage("yaml", tmp_path)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(tmp_path / "absent.duckdb")))
    await store_document(storage, SALES)
    await store_document(storage, SUPPORT)
    read = storage.get_semantic_model_text

    async def edited(name: str, data_source: str | None = None) -> str | None:
        return DUP_SOURCE if name == "sales" else await read(name, data_source=data_source)

    async with owned_query_engine(storage=storage) as engine:
        monkeypatch.setattr(storage, "get_semantic_model_text", edited)
        exported = (await export_osi(engine=engine, datasource=DS, extensions=True)).document
    assert "returns" in {d["name"] for d in exported["datasets"]}


async def test_a_filtered_export_leaves_out_a_document_edited_unreadable_after_it_loaded(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = make_storage("yaml", tmp_path)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(tmp_path / "absent.duckdb")))
    await store_document(storage, SALES)
    await store_document(storage, SUPPORT)
    filtered = AccessFilteredStorage(storage, tags=[], bypass=False)
    read = storage.get_semantic_model_text

    async def edited(name: str, data_source: str | None = None) -> str | None:
        return DUP_SOURCE if name == "sales" else await read(name, data_source=data_source)

    async with owned_query_engine(storage=filtered) as engine:
        monkeypatch.setattr(storage, "get_semantic_model_text", edited)
        exported = (await export_osi(engine=engine, datasource=DS)).document
    assert "returns" in {d["name"] for d in exported["datasets"]}
