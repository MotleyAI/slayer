"""``datasources create --ingest`` and MCP ``create_datasource`` run the idempotent ingest and render like it."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import duckdb
import pytest
from mcp.types import TextContent

from slayer.cli import _confirm as real_confirm
from slayer.core.models import DatasourceConfig
from slayer.core.warnings import BrokenDependentWarning
from slayer.database.errors import DatabaseUnavailable
from slayer.live_schema import ingestion
from slayer.live_schema.ingestion import IngestableObject, InternalTable, SkippedTable
from slayer.live_schema.schema_drift import (
    DatasetAddition,
    IdempotentIngestResult,
    IngestionError,
    WholeDatasetDelete,
)
from slayer.live_schema.schema_scope import SkippedSchema
from slayer.mcp.server import create_mcp_server
from slayer.sql.dialects.reflection import TableKeyReadFailure, TableLocation
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._cli_inprocess import CliResult, run_cli_in_process
from tests._gateway_harness import seed_shop
from tests._key_read_failure import LOST, fail_key_read

_EMPTY = "No tables or views found"
_USER_WORDS = "Hand-written by the analyst"
_LOST_READ = TableKeyReadFailure(location=TableLocation(name="customers"), read="primary key", error="lost")


def _sqlite(tmp_path: Path, *tables: str) -> str:
    db = str(tmp_path / "app.db")
    with transaction(db) as conn:
        conn.execute("SELECT 1")
        for table in tables:
            conn.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, amount REAL)")
    return db


def _store(tmp_path: Path) -> str:
    return str(tmp_path / "store")


def _cli(args: list[str]) -> CliResult:
    return run_cli_in_process(args, uncaught_exit=True)


def _create(tmp_path: Path, db: str, *extra: str) -> CliResult:
    return _cli(["datasources", "--storage", _store(tmp_path), "create", f"sqlite:///{db}", "--name", "app",
                 "--ingest", *extra])


def _ingest(tmp_path: Path, *extra: str) -> CliResult:
    return _cli(["ingest", "--storage", _store(tmp_path), "--datasource", "app", *extra])


def _dataset_description(tmp_path: Path, name: str) -> str | None:
    dataset = asyncio.run(YAMLStorage(base_dir=_store(tmp_path)).get_dataset(name, data_source="app"))
    assert dataset is not None, name
    return dataset.description


def _describe_dataset(tmp_path: Path, name: str) -> None:
    storage = YAMLStorage(base_dir=_store(tmp_path))
    dataset = asyncio.run(storage.get_dataset(name, data_source="app"))
    assert dataset is not None, name
    asyncio.run(storage.save_dataset(dataset.model_copy(update={"description": _USER_WORDS})))


def _stored_datasource(tmp_path: Path, datasource: DatasourceConfig) -> None:
    asyncio.run(YAMLStorage(base_dir=_store(tmp_path)).save_datasource(datasource))


def _patch_idempotent(monkeypatch: pytest.MonkeyPatch, outcome: IdempotentIngestResult | Exception) -> None:
    async def _fake(**_kwargs: Any) -> IdempotentIngestResult:
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr("slayer.live_schema.ingestion.ingest_datasource_idempotent", _fake)


def _record_idempotent(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    async def _fake(**kwargs: Any) -> IdempotentIngestResult:
        calls.append(kwargs)
        return IdempotentIngestResult()

    monkeypatch.setattr("slayer.live_schema.ingestion.ingest_datasource_idempotent", _fake)
    return calls


def _subset(call: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    return {key: call.get(key) for key in expected}


# --- CLI datasources create --ingest --------------------------------------------------------------


class TestCliCreate:
    def test_fresh_create_prints_created_lines(self, tmp_path: Path) -> None:
        result = _create(tmp_path, _sqlite(tmp_path, "a", "b"))
        assert (result.returncode, "Created: a" in result.stdout, "Created: b" in result.stdout,
                "Ingested:" in result.stdout) == (0, True, True, False), result.stdout + result.stderr

    def test_fresh_create_stores_the_datasets(self, tmp_path: Path) -> None:
        _create(tmp_path, _sqlite(tmp_path, "a", "b"))
        assert (_dataset_description(tmp_path, "a"), _dataset_description(tmp_path, "b")) == (None, None)

    def test_re_create_keeps_a_user_written_dataset_description(self, tmp_path: Path) -> None:
        db = _sqlite(tmp_path, "a", "b")
        _create(tmp_path, db)
        _describe_dataset(tmp_path, "a")
        result = _create(tmp_path, db, "--yes")
        assert (result.returncode, _dataset_description(tmp_path, "a")) == (0, _USER_WORDS), result.stderr

    def test_re_create_asks_nothing_about_datasets(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        prompts: list[str] = []

        def _recording(prompt: str, *, assume_yes: bool) -> bool:
            prompts.append(prompt)
            return real_confirm(prompt, assume_yes=assume_yes)

        db = _sqlite(tmp_path, "a", "b")
        _create(tmp_path, db)
        monkeypatch.setattr("slayer.cli._confirm", _recording)
        result = _create(tmp_path, db, "--yes")
        assert (result.returncode, [p for p in prompts if "atasource" not in p]) == (0, []), prompts

    def test_re_create_replaces_the_datasource_description(self, tmp_path: Path) -> None:
        db = _sqlite(tmp_path, "a")
        _create(tmp_path, db, "--description", "old words")
        _describe_dataset(tmp_path, "a")
        result = _create(tmp_path, db, "--description", "new words", "--yes")
        stored = asyncio.run(YAMLStorage(base_dir=_store(tmp_path)).get_datasource("app"))
        assert (result.returncode, stored.description if stored else None, _dataset_description(tmp_path, "a")) == (
            0, "new words", _USER_WORDS,
        ), result.stderr

    def test_skipped_object_is_listed_and_exits_zero(self, tmp_path: Path) -> None:
        result = _create(tmp_path, _sqlite(tmp_path, "orders", "__slayer_reserved"))
        assert (result.returncode, "Skipped (1)" in result.stdout, "__slayer_reserved" in result.stdout) == (
            0, True, True,
        ), result.stdout

    def test_empty_database_prints_the_empty_message_and_exits_zero(self, tmp_path: Path) -> None:
        result = _create(tmp_path, _sqlite(tmp_path))
        assert (result.returncode, _EMPTY in result.stdout) == (0, True), result.stdout

    def test_an_ingest_that_raises_fails_once_and_keeps_the_datasource(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _patch_idempotent(monkeypatch, RuntimeError("boom"))
        result = _create(tmp_path, _sqlite(tmp_path, "a"))
        failures = [line for line in result.stderr.splitlines() if "Ingestion failed:" in line]
        stored = asyncio.run(YAMLStorage(base_dir=_store(tmp_path)).get_datasource("app"))
        assert (result.returncode, len(failures), "boom" in failures[0] if failures else False, stored is not None) == (
            1, 1, True, True,
        ), result.stderr

    def test_a_failed_key_read_is_named_once_and_exits_zero(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        db = str(tmp_path / "shop.db")
        seed_shop(db)
        fail_key_read(monkeypatch, table="customers")
        result = _create(tmp_path, db)
        named = [line for line in result.stdout.splitlines()
                 if "customers" in line and "primary key" in line.lower() and LOST in line]
        assert (result.returncode, len(named)) == (0, 1), result.stdout + result.stderr

    @pytest.mark.parametrize(("flags", "expected"), [
        pytest.param(["--schema", "main", "--include", "a,b", "--exclude", "c", "--no-views", "--surface-internals"],
                     {"schemas": ["main"], "all_schemas": False, "include_tables": ["a", "b"],
                      "exclude_tables": ["c"], "include_views": False, "surface_internals": True}, id="scoped"),
        pytest.param(["--all-schemas"], {"schemas": None, "all_schemas": True, "include_tables": None,
                                         "exclude_tables": None, "include_views": True, "surface_internals": False},
                     id="all-schemas"),
    ])
    def test_the_scope_flags_reach_the_idempotent_ingest(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, flags: list[str], expected: dict[str, Any],
    ) -> None:
        calls = _record_idempotent(monkeypatch)
        result = _create(tmp_path, _sqlite(tmp_path, "a"), *flags)
        assert [_subset(call, expected) for call in calls] == [expected], result.stdout + result.stderr


# --- The empty-ingest message needs an empty result ---------------------------------------------


class TestEmptyIngestMessage:
    def test_every_requested_schema_skipped_lists_the_reason(self, tmp_path: Path) -> None:
        db = str(tmp_path / "x.duckdb")
        with duckdb.connect(db) as con:
            con.execute("CREATE TABLE t (id INTEGER)")
        _stored_datasource(tmp_path, DatasourceConfig(name="app", type="duckdb", database=db))
        result = _ingest(tmp_path, "--schema", "information_schema")
        assert (result.returncode, "Skipped schemas (1)" in result.stdout, "information_schema" in result.stdout,
                "never ingested" in result.stdout, _EMPTY in result.stdout) == (1, True, True, True, False), (
            result.stdout
        )

    def test_truly_empty_schema_prints_the_message_and_exits_one(self, tmp_path: Path) -> None:
        _stored_datasource(tmp_path, DatasourceConfig(name="app", type="sqlite", database=_sqlite(tmp_path)))
        result = _ingest(tmp_path)
        assert (result.returncode, _EMPTY in result.stdout) == (1, True), result.stdout

    def test_a_result_with_only_failed_key_reads_is_not_empty(self) -> None:
        result = IdempotentIngestResult(key_read_failures=[_LOST_READ])
        assert (ingestion._ingest_result_is_empty(IdempotentIngestResult()),
                ingestion._ingest_result_is_empty(result)) == (True, False)

    _NON_EMPTY = [
        pytest.param({"objects": [IngestableObject(name="a", kind="table")]}, "", id="objects-in-sync"),
        pytest.param({"additions": [DatasetAddition(dataset_name="a", data_source="app", new_fields=["x"])]},
                     "Updated: a", id="updated"),
        pytest.param({"to_delete": [WholeDatasetDelete(dataset_name="gone", data_source="app")]},
                     "Pending drift", id="drift"),
        pytest.param({"skipped": [SkippedTable(table_name="odd", reason="not modellable")]}, "Skipped (1)",
                     id="skipped"),
        pytest.param({"errors": [IngestionError(dataset_name="a", data_source="app", error="save failed")]},
                     "Errors (1)", id="error"),
        pytest.param({"datasource_described": True}, "Datasource description imported.", id="description-only"),
        pytest.param({"skipped_schemas": [SkippedSchema(token="other", reason="out of reach")]},
                     "Skipped schemas (1)", id="skipped-schema"),
        pytest.param({"key_read_failures": [_LOST_READ]}, "Lost key reads (1)", id="key-read-failure"),
        pytest.param({"hidden_internals": [InternalTable(table_name="_dlt_loads", dataset_name="_dlt_loads",
                                                         tool="dlt")]}, "Hidden (1)", id="hidden-internal"),
        pytest.param({"broken_dependents": [BrokenDependentWarning(
            dataset="a", data_source="app", dependent_kind="metric", name="x", field="a.p",
            error_type="ListTypeError", error="e")]}, "Broken by a list re-type (1)", id="broken-dependent"),
    ]

    @pytest.mark.parametrize(("fields", "section"), _NON_EMPTY)
    def test_slayer_ingest_prints_the_section_not_the_empty_message(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fields: dict[str, Any], section: str,
    ) -> None:
        _stored_datasource(tmp_path, DatasourceConfig(name="app", type="sqlite", database=_sqlite(tmp_path)))
        _patch_idempotent(monkeypatch, IdempotentIngestResult.model_validate(fields))
        result = _ingest(tmp_path)
        assert (section in result.stdout, _EMPTY in result.stdout) == (True, False), result.stdout

    #: ``slayer ingest`` exit codes from before the shared emptiness check: 1 without a live object or dataset change.
    _INGEST_EXIT = {"objects-in-sync": 0, "updated": 0, "drift": 0, "skipped": 1, "error": 1, "description-only": 1,
                    "skipped-schema": 1, "key-read-failure": 1, "hidden-internal": 1, "broken-dependent": 1}

    @pytest.mark.parametrize(("fields", "section"), _NON_EMPTY)
    def test_slayer_ingest_keeps_its_exit_code(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest,
        fields: dict[str, Any], section: str,
    ) -> None:
        _stored_datasource(tmp_path, DatasourceConfig(name="app", type="sqlite", database=_sqlite(tmp_path)))
        _patch_idempotent(monkeypatch, IdempotentIngestResult.model_validate(fields))
        result = _ingest(tmp_path)
        assert result.returncode == self._INGEST_EXIT[request.node.callspec.id], section + result.stdout

    @pytest.mark.parametrize(("fields", "section"), _NON_EMPTY)
    def test_create_prints_the_section_not_the_empty_message(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fields: dict[str, Any], section: str,
    ) -> None:
        _patch_idempotent(monkeypatch, IdempotentIngestResult.model_validate(fields))
        result = _create(tmp_path, _sqlite(tmp_path))
        assert (result.returncode, section in result.stdout, _EMPTY in result.stdout) == (0, True, False), (
            result.stdout
        )


# --- MCP create_datasource -------------------------------------------------------------------------


async def _mcp_create(storage: YAMLStorage, db: str, **scope: Any) -> str:
    server = create_mcp_server(storage=storage, _seed_help=False)
    blocks = (await server.call_tool(name="create_datasource",
                                       arguments={"name": "app", "type": "sqlite", "database": db, **scope})).content
    block = next(iter(blocks))
    assert isinstance(block, TextContent), block
    return block.text


class TestMcpCreate:
    async def test_fresh_create_names_both_datasets_as_new(self, tmp_path: Path) -> None:
        storage = YAMLStorage(base_dir=_store(tmp_path))
        text = await _mcp_create(storage, _sqlite(tmp_path, "a", "b"))
        stored = [await storage.get_dataset(name, data_source="app") for name in ("a", "b")]
        assert ("Created 2 new dataset(s):" in text, "- a (" in text, "- b (" in text,
                "Use datasets_summary and inspect" in text, None in stored) == (True, True, True, True, False), text

    async def test_re_create_says_replaced_and_keeps_a_user_written_dataset_description(self, tmp_path: Path) -> None:
        storage = YAMLStorage(base_dir=_store(tmp_path))
        db = _sqlite(tmp_path, "a", "b")
        await _mcp_create(storage, db)
        dataset = await storage.get_dataset("a", data_source="app")
        assert dataset is not None
        await storage.save_dataset(dataset.model_copy(update={"description": _USER_WORDS}))
        text = await _mcp_create(storage, db)
        kept = await storage.get_dataset("a", data_source="app")
        assert ("Datasource 'app' replaced" in text, kept.description if kept else None) == (True, _USER_WORDS), text

    async def test_database_failure_during_ingest_keeps_the_datasource(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _patch_idempotent(monkeypatch, DatabaseUnavailable("database went away"))
        storage = YAMLStorage(base_dir=_store(tmp_path))
        text = await _mcp_create(storage, _sqlite(tmp_path, "a"))
        lines = text.splitlines()
        assert (lines[0].startswith("Datasource 'app' created"),
                "Auto-ingestion failed: Database error: database went away" in text,
                await storage.get_datasource("app") is not None) == (True, True, True), text

    async def test_a_non_database_failure_during_ingest_propagates(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _patch_idempotent(monkeypatch, RuntimeError("ingest bug"))
        storage = YAMLStorage(base_dir=_store(tmp_path))
        db = _sqlite(tmp_path, "a")
        with pytest.raises(Exception, match="ingest bug"):
            await _mcp_create(storage, db)

    async def test_failed_key_reads_alone_are_listed_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _patch_idempotent(monkeypatch, IdempotentIngestResult(key_read_failures=[_LOST_READ]))
        text = await _mcp_create(YAMLStorage(base_dir=_store(tmp_path)), _sqlite(tmp_path, "a"))
        assert (text.count("Lost key reads (1)"), _EMPTY in text) == (1, False), text

    @pytest.mark.parametrize(("scope", "expected"), [
        pytest.param({"schema_name": "main"}, {"schema": "main", "schemas": None, "all_schemas": False}, id="schema"),
        pytest.param({"schemas": "main,aux"}, {"schema": None, "schemas": ["main", "aux"], "all_schemas": False},
                     id="schemas"),
        pytest.param({"all_schemas": True}, {"schema": None, "schemas": None, "all_schemas": True}, id="all-schemas"),
    ])
    async def test_the_scope_reaches_the_idempotent_ingest(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scope: dict[str, Any], expected: dict[str, Any],
    ) -> None:
        calls = _record_idempotent(monkeypatch)
        text = await _mcp_create(YAMLStorage(base_dir=_store(tmp_path)), _sqlite(tmp_path, "a"), **scope)
        assert [_subset(call, expected) for call in calls] == [expected], text
