"""Untyped semantic-model document fields are a type finding that ``--force-clean`` writes in.

Spec: models/schema-drift › An untyped document field is a type finding that cleanup writes in.
"""

from __future__ import annotations

import contextlib
import difflib
import json
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend

from tests._cli_inprocess import run_cli_in_process
from tests._dev2243_fixtures import DS, STORAGES, make_storage, store_document
from tests._engine_helpers import owned_query_engine
from tests._dataset_access_fixtures import mcp_text

LIVE = (
    "CREATE TABLE orders (order_id VARCHAR, amount DECIMAL(10,2), tags VARCHAR[], status VARCHAR)",
    "INSERT INTO orders VALUES ('O1', 10.50, ['a', 'b'], 'open'), ('O2', 4.25, [], 'done')",
)

#: ``amount`` declares no datatype; its live column is DECIMAL(10,2).
DECIMAL_DOC = """\
version: 0.3.0.dev0
name: sales
# hand-written; keep this comment
datasets:
  - name: orders
    source: orders
    primary_key: [order_id]
    fields:
      - name: order_id
        expression: {sql: order_id}
        datatype: String
      - name: amount  # net of tax
        expression: {sql: amount}
      - name: status
        expression: {sql: status}
        datatype: String
"""

#: ``tags`` declares no datatype; its live column is VARCHAR[].
LIST_DOC = """\
version: 0.3.0.dev0
name: sales
datasets:
  - name: orders
    source: orders
    primary_key: [order_id]
    fields:
      - name: order_id
        expression: {sql: order_id}
        datatype: String
      - name: tags
        expression: {sql: tags}
"""

#: ``ghost`` reads no live column, so its type cannot be determined.
GHOST_DOC = """\
version: 0.3.0.dev0
name: sales
datasets:
  - name: orders
    source: orders
    primary_key: [order_id]
    fields:
      - name: order_id
        expression: {sql: order_id}
        datatype: String
      - name: amount
        expression: {sql: amount}
      - name: ghost
        expression: {sql: ghost_column}
"""


def _storage_arg(kind: str, base: Path) -> str:
    return str(base / "store") if kind == "yaml" else str(base / "store.db")


@contextlib.asynccontextmanager
async def _store(tmp_path: Path, *, kind: str = "yaml", text: str = DECIMAL_DOC) -> AsyncGenerator[StorageBackend]:
    """Storage over a seeded DuckDB datasource ``shop`` holding the document ``text``."""
    duckdb = pytest.importorskip("duckdb")
    db_path = tmp_path / "live.duckdb"
    with contextlib.closing(duckdb.connect(str(db_path))) as con:
        for stmt in LIVE:
            con.execute(stmt)
    storage = make_storage(kind, tmp_path)
    await storage.save_datasource(DatasourceConfig(name=DS, type="duckdb", database=str(db_path)))
    await store_document(storage, text)
    yield storage


@contextlib.asynccontextmanager
async def _engine(storage: StorageBackend) -> AsyncGenerator[SlayerQueryEngine]:
    async with owned_query_engine(storage=storage) as engine:
        yield engine


def _untyped(entries: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """``{field: finding}`` of every untyped finding."""
    return {e["field"]: e for e in entries if e.get("finding") == "untyped"}


def _assert_amount_finding(finding: dict[str, Any]) -> None:
    assert {k: finding[k] for k in ("finding", "dataset_name", "data_source", "semantic_model", "field", "live_type")} \
        == {"finding": "untyped", "dataset_name": "orders", "data_source": DS, "semantic_model": "sales",
            "field": "amount", "live_type": "Decimal"}, finding
    assert isinstance(finding["summary"], str)
    assert "amount" in finding["summary"]


async def _text(storage: StorageBackend) -> str | None:
    return await storage.get_semantic_model_text("sales", data_source=DS)


def _doc_field(text: str, name: str) -> dict[str, Any]:
    (orders,) = yaml.safe_load(text)["datasets"]
    return next(f for f in orders["fields"] if f["name"] == name)


class TestReportedOnEverySurface:
    @pytest.mark.parametrize("kind", STORAGES)
    async def test_engine(self, tmp_path: Path, kind: str) -> None:
        """Spec: models/schema-drift › An untyped decimal is reported and written (engine report)."""
        async with _store(tmp_path, kind=kind) as storage, _engine(storage) as engine:
            entries = [e.model_dump(mode="json") for e in await engine.validate_datasets(data_source=DS)]
        found = _untyped(entries)
        assert set(found) == {"amount"}, entries
        _assert_amount_finding(found["amount"])

    async def test_mcp(self, tmp_path: Path) -> None:
        """Spec: models/schema-drift › An untyped decimal is reported and written (MCP report)."""
        async with _store(tmp_path) as storage:
            before = await _text(storage)
            reply = await mcp_text(storage, "validate_datasets", data_source=DS)
            after = await _text(storage)
        found = _untyped(json.loads(reply))
        _assert_amount_finding(found["amount"])
        assert after == before

    async def test_rest(self, tmp_path: Path) -> None:
        """Spec: models/schema-drift › An untyped decimal is reported and written (REST report)."""
        async with _store(tmp_path) as storage:
            before = await _text(storage)
            resp = TestClient(create_app(storage=storage)).post("/validate-datasets", json={"data_source": DS})
            after = await _text(storage)
        assert resp.status_code == 200, resp.text
        _assert_amount_finding(_untyped(resp.json())["amount"])
        assert after == before

    @pytest.mark.parametrize("kind", STORAGES)
    async def test_cli(self, tmp_path: Path, kind: str) -> None:
        """Spec: models/schema-drift › An untyped decimal is reported and written (CLI report)."""
        async with _store(tmp_path, kind=kind) as storage:
            before = await _text(storage)
        result = run_cli_in_process(["validate-datasets", "--datasource", DS, "--format", "json",
                                     "--storage", _storage_arg(kind, tmp_path)])
        assert result.returncode == 0, result.stderr
        _assert_amount_finding(_untyped(json.loads(result.stdout)["drift"])["amount"])
        assert await _text(make_storage(kind, tmp_path)) == before

    async def test_a_field_whose_live_type_cannot_be_determined_is_not_reported(self, tmp_path: Path) -> None:
        """Spec: models/schema-drift › An untyped document field is a type finding (undeterminable: not reported)."""
        async with _store(tmp_path, text=GHOST_DOC) as storage, _engine(storage) as engine:
            entries = [e.model_dump(mode="json") for e in await engine.validate_datasets(data_source=DS)]
        assert set(_untyped(entries)) == {"amount"}, entries


class TestForceClean:
    @pytest.mark.parametrize("kind", STORAGES)
    async def test_an_untyped_decimal_is_reported_and_written(self, tmp_path: Path, kind: str) -> None:
        """Spec: models/schema-drift › An untyped decimal is reported and written."""
        async with _store(tmp_path, kind=kind) as storage:
            before = await _text(storage)
        assert before is not None
        result = run_cli_in_process(["validate-datasets", "--datasource", DS, "--force-clean", "--yes",
                                     "--storage", _storage_arg(kind, tmp_path)])
        assert result.returncode == 0, result.stdout + result.stderr
        assert "orders.amount" in result.stdout
        assert "Decimal" in result.stdout
        storage = make_storage(kind, tmp_path)
        after = await _text(storage)
        assert after is not None
        old, new = before.splitlines(), after.splitlines()
        ops = difflib.SequenceMatcher(a=old, b=new, autojunk=False).get_opcodes()
        assert all(tag in ("equal", "insert") for tag, *_ in ops), list(difflib.unified_diff(old, new))
        inserted = [line.strip() for tag, _i1, _i2, j1, j2 in ops if tag == "insert" for line in new[j1:j2]]
        assert inserted == ["datatype: Decimal"], inserted
        assert _doc_field(after, "amount")["datatype"] == "Decimal"
        orders = await storage.get_dataset("orders", data_source=DS)
        assert orders is not None
        amount = orders.get_field("amount")
        assert amount is not None
        assert amount.datatype is DataType.DECIMAL

    @pytest.mark.parametrize("kind", STORAGES)
    async def test_a_list_is_written_as_opaque_with_a_slayer_entry(self, tmp_path: Path, kind: str) -> None:
        """Spec: models/schema-drift › A list is written as Opaque with a SLAYER entry."""
        async with _store(tmp_path, kind=kind, text=LIST_DOC):
            pass
        result = run_cli_in_process(["validate-datasets", "--datasource", DS, "--force-clean", "--yes",
                                     "--storage", _storage_arg(kind, tmp_path)])
        assert result.returncode == 0, result.stdout + result.stderr
        storage = make_storage(kind, tmp_path)
        after = await _text(storage)
        assert after is not None
        tags = _doc_field(after, "tags")
        assert tags["datatype"] == "Opaque", tags
        entries = [e for e in tags.get("nonportable_extensions") or [] if e.get("vendor_name") == "SLAYER"]
        assert len(entries) == 1, tags
        assert "String" in json.dumps(entries[0]["data"]), entries
        orders = await storage.get_dataset("orders", data_source=DS)
        assert orders is not None
        field = orders.get_field("tags")
        assert field is not None
        assert (field.datatype, field.element_type) == (DataType.ARRAY, DataType.STRING)

    async def test_without_yes_nothing_is_written(self, tmp_path: Path) -> None:
        """Spec: models/schema-drift › An untyped document field is a type finding (same prompt as deletes)."""
        async with _store(tmp_path) as storage:
            before = await _text(storage)
        result = run_cli_in_process(["validate-datasets", "--datasource", DS, "--force-clean",
                                     "--storage", _storage_arg("yaml", tmp_path)], stdin="n\n")
        assert "amount" in result.stdout
        assert await _text(make_storage("yaml", tmp_path)) == before
