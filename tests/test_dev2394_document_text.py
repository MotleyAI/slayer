"""Document text with a repeated key or a scalar its tag cannot construct is invalid on every surface reading it;
YAML merge keys keep YAML 1.1 precedence.

Spec: models/semantic-model-documents › Only Ossie's current document shape is accepted natively;
importers/osi-import › Invalid documents are refused with every reason.
"""

from __future__ import annotations

import contextlib
import re
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
import yaml

from slayer.core.errors import SlayerError
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.osi.exporter import export_osi
from slayer.storage.base import StorageBackend
from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev2243_fixtures import DS, STORAGES, error, importer_module, make_storage, place_document
from tests._engine_helpers import owned_query_engine

FLAT_HEAD = "version: 0.3.0.dev0\nname: sales\n"
LEGACY_HEAD = "version: 0.2.0.dev0\nsemantic_model:\n  - name: sales\n"


def flat(datasets: str) -> str:
    return FLAT_HEAD + "datasets:\n" + datasets


def legacy(datasets: str) -> str:
    """The same datasets block inside a legacy wrapper (indented two more spaces)."""
    return LEGACY_HEAD + "    datasets:\n" + "".join(f"  {line}\n" if line else "\n" for line in datasets.splitlines())


REPEATED_KEY = """\
  - name: orders
    source: orders
    source: other
    fields:
      - name: order_id
        expression: {sql: order_id}
"""
MALFORMED = {
    "timestamp": ("!!timestamp not-a-date", "not-a-date"),
    "int": ("!!int abc", "abc"),
}


def malformed(tagged: str) -> str:
    return f"""\
  - name: orders
    source: orders
    description: {tagged}
    fields:
      - name: order_id
        expression: {{sql: order_id}}
"""


MERGED = """\
  - name: orders
    source: orders
    fields:
      - &first {name: a, expression: {sql: a}, description: First}
      - &second {name: b, expression: {sql: b}, description: Second}
      - <<: [*first, *second]
        name: c
        expression: {sql: c}
"""

#: The source table legacy documents introspect.
ORDERS_TABLE = TableSpec(name="orders", columns=[("order_id", "TEXT"), ("a", "TEXT"), ("b", "TEXT"), ("c", "TEXT")],
                         rows=[("O1", "x", "y", "z")])
REPEATED = r"\bsource\b"
BAD_TEXTS = [
    pytest.param(flat(REPEATED_KEY), REPEATED, id="repeated-key"),
    *[pytest.param(flat(malformed(tagged)), needle, id=f"malformed-{tag}") for tag, (tagged, needle) in MALFORMED.items()],
]
BAD_LEGACY = [
    pytest.param(legacy(REPEATED_KEY), REPEATED, id="repeated-key"),
    *[pytest.param(legacy(malformed(tagged)), needle, id=f"malformed-{tag}") for tag, (tagged, needle) in MALFORMED.items()],
]


async def storage_with_datasource(base: Path, kind: str = "yaml") -> StorageBackend:
    storage = make_storage(kind, base)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(base / "absent.duckdb")))
    return storage


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


@contextlib.asynccontextmanager
async def legacy_engine(base: Path) -> AsyncGenerator[SlayerQueryEngine]:
    """An engine over a DuckDB holding ``orders`` and ``other``, so a legacy document can upgrade."""
    base.mkdir(parents=True, exist_ok=True)
    db = base / "seed.duckdb"
    seed_backend("duckdb", str(db), [ORDERS_TABLE, ORDERS_TABLE.model_copy(update={"name": "other"})])
    config = DatasourceConfig(name=DS, type="duckdb", database=str(db))
    async with owned_query_engine(storage=make_storage("yaml", base), datasources=[config]) as engine:
        yield engine


async def run_import(storage: StorageBackend, path: Path, *, engine: SlayerQueryEngine | None = None) -> Any:
    return await importer_module().import_osi(storage=storage, path=path, data_source=DS, engine=engine)


def names(needle: str, text: str) -> bool:
    """``text`` contains ``needle``: a regex for the repeated key, else a literal value."""
    return re.search(needle if needle == REPEATED else re.escape(needle), text) is not None


def refusal(result: Any, file_name: str) -> str:
    hits = [r for r in result.refused if Path(r.file).name == file_name]
    assert len(hits) == 1, result.refused
    return "\n".join(hits[0].reasons)


def field_description(doc: dict[str, Any], name: str) -> str:
    [orders] = [d for d in doc["datasets"] if d["name"] == "orders"]
    return next(f for f in orders["fields"] if f["name"] == name)["description"]


# --------------------------------------------------------------------------- #
# Storage
# --------------------------------------------------------------------------- #
def test_the_fixture_texts_are_what_they_claim() -> None:
    assert yaml.safe_load(flat(REPEATED_KEY))["datasets"][0]["source"] == "other"
    assert field_description(yaml.safe_load(flat(MERGED)), "c") == "First"
    assert yaml.safe_load(legacy(MERGED))["semantic_model"][0]["datasets"][0]["fields"][2]["description"] == "First"


@pytest.mark.parametrize(("text", "needle"), BAD_TEXTS)
async def test_a_hand_placed_document_is_unloadable_naming_the_cause(tmp_path: Path, text: str, needle: str) -> None:
    """Spec scenarios: A repeated key refuses the document; A malformed timestamp refuses the document."""
    place_document(tmp_path, text, name="sales")
    storage = await storage_with_datasource(tmp_path)
    loaded, errs = await storage.load_datasets(data_source=DS)
    assert "orders" not in {d.name for d in loaded}
    causes = {e.name: e.cause_text for e in errs if e.kind == "semantic_model"}
    assert names(needle, causes["sales"]), causes


@pytest.mark.parametrize("kind", STORAGES)
@pytest.mark.parametrize(("text", "needle"), BAD_TEXTS)
async def test_saving_the_text_is_refused_naming_the_cause(tmp_path: Path, kind: str, text: str, needle: str) -> None:
    storage = make_storage(kind, tmp_path)
    with pytest.raises(error("OssieDocumentInvalidError")) as info:
        await storage.save_semantic_model_text(data_source=DS, name="sales", text=text)
    assert names(needle, str(info.value))
    assert await storage.list_semantic_models(DS) == []


@pytest.mark.parametrize("kind", STORAGES)
async def test_merge_keys_keep_their_precedence_in_storage(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Merge keys keep their precedence."""
    storage = make_storage(kind, tmp_path)
    await storage.save_semantic_model_text(data_source=DS, name="sales", text=flat(MERGED))
    orders = await storage.get_dataset("orders", data_source=DS)
    assert orders is not None
    assert next(f for f in orders.fields if f.name == "c").description == "First"


# --------------------------------------------------------------------------- #
# Import
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("text", "needle"), BAD_TEXTS)
async def test_a_flat_import_is_refused_naming_the_cause(tmp_path: Path, text: str, needle: str) -> None:
    """Spec scenario: A repeated key is refused in flat and legacy documents (flat)."""
    storage = await storage_with_datasource(tmp_path)
    result = await run_import(storage, write(tmp_path / "in" / "sales.yaml", text))
    assert result.stored == []
    assert names(needle, refusal(result, "sales.yaml"))
    assert await storage.list_semantic_models(DS) == []


@pytest.mark.parametrize(("text", "needle"), BAD_LEGACY)
async def test_a_legacy_import_is_refused_naming_the_cause(tmp_path: Path, text: str, needle: str) -> None:
    """Spec scenario: A repeated key is refused in flat and legacy documents (legacy)."""
    async with legacy_engine(tmp_path) as engine:
        result = await run_import(engine.storage, write(tmp_path / "in" / "sales.yaml", text), engine=engine)
        assert result.stored == []
        assert names(needle, refusal(result, "sales.yaml"))
        assert await engine.storage.list_semantic_models(DS) == []


async def test_merge_keys_keep_their_precedence_on_a_flat_import(tmp_path: Path) -> None:
    storage = await storage_with_datasource(tmp_path)
    result = await run_import(storage, write(tmp_path / "in" / "sales.yaml", flat(MERGED)))
    assert result.stored == ["sales"]
    orders = await storage.get_dataset("orders", data_source=DS)
    assert orders is not None
    assert next(f for f in orders.fields if f.name == "c").description == "First"


async def test_merge_keys_keep_their_precedence_on_a_legacy_import(tmp_path: Path) -> None:
    async with legacy_engine(tmp_path) as engine:
        result = await run_import(engine.storage, write(tmp_path / "in" / "sales.yaml", legacy(MERGED)), engine=engine)
        text = await engine.storage.get_semantic_model_text("sales", DS)
    assert result.stored == ["sales"], result.refused
    assert text is not None
    assert field_description(yaml.safe_load(text), "c") == "First"


# --------------------------------------------------------------------------- #
# Export of already-stored text
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("text", "needle"), BAD_TEXTS)
async def test_export_refuses_stored_text_naming_the_cause(tmp_path: Path, text: str, needle: str) -> None:
    place_document(tmp_path, text, name="sales")
    storage = await storage_with_datasource(tmp_path)
    config = DatasourceConfig(name=DS, type="duckdb", database=str(tmp_path / "absent.duckdb"))
    async with owned_query_engine(storage=storage, datasources=[config]) as engine:
        with pytest.raises(SlayerError, match=needle if needle == REPEATED else re.escape(needle)):
            await export_osi(engine=engine, datasource=DS)


async def test_merge_keys_keep_their_precedence_on_export(tmp_path: Path) -> None:
    place_document(tmp_path, flat(MERGED), name="sales")
    storage = await storage_with_datasource(tmp_path)
    config = DatasourceConfig(name=DS, type="duckdb", database=str(tmp_path / "absent.duckdb"))
    async with owned_query_engine(storage=storage, datasources=[config]) as engine:
        result = await export_osi(engine=engine, datasource=DS)
    assert result.text == flat(MERGED)
    assert field_description(result.document, "c") == "First"
