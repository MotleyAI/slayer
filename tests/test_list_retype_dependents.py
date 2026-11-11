"""Re-ingest reports the stored definitions a list re-type breaks (spec: models/list-columns › Re-ingest
re-types TEXT list columns)."""

from __future__ import annotations

import contextlib
import os
import tempfile
import warnings
from collections.abc import AsyncGenerator, Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import duckdb
import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.async_utils import run_sync
from slayer.cli import _run_ingest
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.warnings import BrokenDependentWarning, SlayerBrokenDependentWarning
from slayer.live_schema.broken_dependents import FoundBreak, report_broken
from slayer.live_schema.ingestion import ingest_datasource_idempotent
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema.schema_drift import IdempotentIngestResult
from slayer.mcp.server import create_mcp_server
from slayer.database import engine_factory
from slayer.database.client import SlayerSQLClient
from slayer.storage.yaml_storage import YAMLStorage

LIVE = (
    "CREATE TABLE contractors (id INTEGER, name VARCHAR, tags VARCHAR[])",
    "INSERT INTO contractors VALUES (1, 'a', ['go', 'sql']), (2, 'b', [])",
    "CREATE TABLE orders (id INTEGER, contractor_id INTEGER, amount DOUBLE)",
    "INSERT INTO orders VALUES (1, 1, 10.0), (2, 2, 5.0)",
    "CREATE TABLE notes (id INTEGER, tags VARCHAR)",
    "INSERT INTO notes VALUES (1, 'x')",
    "CREATE TABLE skills (id INTEGER, langs VARCHAR[], tools VARCHAR[])",
    "INSERT INTO skills VALUES (1, ['go'], ['git'])",
)
TAGS = "contractors.tags"
NT = ModelMeasure(name="nt", formula="max(tags)")
FIRST_TAG = Column(name="first_tag", sql="upper(tags)", type=DataType.TEXT)
BY_TAG = {"source_model": "contractors", "measures": ["*:count"], "filters": ["tags = 'go'"]}

Key = tuple[str, str, str]


def contractors(*, columns: tuple[Column, ...] = (), measures: tuple[ModelMeasure, ...] = (NT,),
                filters: tuple[str, ...] = ()) -> SlayerModel:
    return SlayerModel(name="contractors", sql_table="contractors", data_source="ds", filters=list(filters), columns=[
        Column(name="id", type=DataType.INT, primary_key=True), Column(name="name", type=DataType.TEXT),
        Column(name="tags", type=DataType.TEXT), *columns], measures=list(measures))


def orders(*, columns: tuple[Column, ...] = (), measures: tuple[ModelMeasure, ...] = ()) -> SlayerModel:
    return SlayerModel(name="orders", sql_table="orders", data_source="ds", columns=[
        Column(name="id", type=DataType.INT, primary_key=True), Column(name="contractor_id", type=DataType.INT),
        Column(name="amount", type=DataType.DOUBLE), *columns], measures=list(measures),
        joins=[ModelJoin(target_model="contractors", join_pairs=[["contractor_id", "id"]])])


def notes() -> SlayerModel:
    return SlayerModel(name="notes", sql_table="notes", data_source="ds", columns=[
        Column(name="id", type=DataType.INT, primary_key=True), Column(name="tags", type=DataType.TEXT)],
        measures=[ModelMeasure(name="nn", formula="max(tags)")])


def skills(*, measures: tuple[ModelMeasure, ...]) -> SlayerModel:
    return SlayerModel(name="skills", sql_table="skills", data_source="ds", columns=[
        Column(name="id", type=DataType.INT, primary_key=True), Column(name="langs", type=DataType.TEXT),
        Column(name="tools", type=DataType.TEXT)], measures=list(measures))


async def _seed(base: str, *, models: tuple[SlayerModel, ...],
                saved: dict[str, dict[str, Any]]) -> tuple[DatasourceConfig, YAMLStorage]:
    db_path = os.path.join(base, "live.duckdb")
    with contextlib.closing(duckdb.connect(db_path)) as con:
        for stmt in LIVE:
            con.execute(stmt)
    ds = DatasourceConfig(name="ds", type="duckdb", database=db_path)
    storage = YAMLStorage(base_dir=os.path.join(base, "store"))
    await storage.save_datasource(ds)
    for model in models:
        await storage.save_model(model)
    engine = SlayerQueryEngine(storage=storage)
    try:
        for name, query in saved.items():
            await engine.create_model_from_query(query=query, name=name)
    finally:
        await engine.aclose()
    return ds, storage


class Env:
    def __init__(self, *, ds: DatasourceConfig, storage: YAMLStorage) -> None:
        self.ds = ds
        self.storage = storage

    async def reingest(self, client: SlayerSQLClient | None = None) -> IdempotentIngestResult:
        engine = SlayerQueryEngine(storage=self.storage)
        try:
            return await ingest_datasource_idempotent(datasource=self.ds, storage=self.storage,
                                                      client=client or engine.client_for(self.ds))
        finally:
            await engine.aclose()

    async def column(self, model: str, name: str) -> Column:
        loaded = await self.storage.get_model(model, data_source="ds")
        assert loaded is not None
        column = loaded.get_column(name)
        assert column is not None
        return column


@contextlib.asynccontextmanager
async def _env(*models: SlayerModel, saved: dict[str, dict[str, Any]] | None = None) -> AsyncGenerator[Env]:
    with tempfile.TemporaryDirectory() as tmp:
        ds, storage = await _seed(tmp, models=models, saved=saved or {})
        try:
            yield Env(ds=ds, storage=storage)
        finally:
            engine_factory.invalidate_engine(ds)


def _by_key(result: IdempotentIngestResult) -> dict[Key, BrokenDependentWarning]:
    return {(b.model, b.dependent_kind, b.name): b for b in result.broken_dependents}


def _assert_list_type_break(entry: BrokenDependentWarning, *, column: str = TAGS) -> None:
    assert (entry.checked, entry.data_source, entry.column, entry.error_type) == (True, "ds", column,
                                                                                 "ListTypeError"), entry
    assert entry.error, entry
    assert "tags" in entry.error, entry


def _refuse_planning(monkeypatch: pytest.MonkeyPatch, marker: str) -> None:
    """Planning any query whose repr mentions ``marker`` fails with an infrastructure error."""
    original = SlayerQueryEngine._plan_and_render

    async def refusing(self: SlayerQueryEngine, **kwargs: Any) -> Any:
        if marker in repr(kwargs.get("query")):
            raise ConnectionError("planner offline")
        return await original(self, **kwargs)

    monkeypatch.setattr(SlayerQueryEngine, "_plan_and_render", refusing)


class TestReported:
    async def test_dependants_broken_by_the_retype_are_reported(self) -> None:
        async with _env(contractors(), saved={"by_tag": BY_TAG}) as env:
            result = await env.reingest()
            tags = await env.column("contractors", "tags")
        assert (tags.type, tags.element_type) == (DataType.ARRAY, DataType.TEXT)
        broken = _by_key(result)
        for key in (("contractors", "measure", "nt"), ("by_tag", "saved_query", "by_tag")):
            assert key in broken, broken
            _assert_list_type_break(broken[key])

    async def test_each_is_a_structured_warning(self) -> None:
        async with _env(contractors(), saved={"by_tag": BY_TAG}) as env:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                result = await env.reingest()
        payloads = [m.payload for w in caught if isinstance(m := w.message, SlayerBrokenDependentWarning)]
        assert result.broken_dependents, result
        assert all(p.kind == "broken_dependent" for p in payloads), payloads
        assert sorted(p.model_dump_json() for p in payloads) == sorted(
            b.model_dump_json() for b in result.broken_dependents)

    async def test_a_dependant_across_a_join(self) -> None:
        nc = ModelMeasure(name="nc", formula="max(contractors.tags)")
        async with _env(contractors(), orders(measures=(nc,))) as env:
            broken = _by_key(await env.reingest())
        _assert_list_type_break(broken[("orders", "measure", "nc")])

    async def test_a_namesake_in_another_datasource(self) -> None:
        async with _env(contractors()) as env:
            await env.storage.save_datasource(env.ds.model_copy(update={"name": "other"}))
            await env.storage.save_model(contractors().model_copy(update={"data_source": "other"}))
            broken = _by_key(await env.reingest())
        _assert_list_type_break(broken[("contractors", "measure", "nt")])

    async def test_attributed_to_the_column_that_breaks_it(self) -> None:
        mixed = ModelMeasure(name="mixed", formula="count(langs) + max(tools)")
        async with _env(skills(measures=(mixed,))) as env:
            broken = _by_key(await env.reingest())
        assert broken[("skills", "measure", "mixed")].column == "skills.tools", broken

    async def test_the_retype_is_saved_although_dependants_break(self) -> None:
        async with _env(contractors()) as env:
            result = await env.reingest()
            tags = await env.column("contractors", "tags")
        assert ("contractors", "measure", "nt") in _by_key(result)
        assert tags.type is DataType.ARRAY


class TestNotReported:
    async def test_raw_sql_definitions_are_not_reported(self) -> None:
        async with _env(contractors(columns=(FIRST_TAG,), filters=("tags = 'go'",))) as env:
            broken = _by_key(await env.reingest())
        assert ("contractors", "measure", "nt") in broken, broken
        assert not [k for k in broken if k[1] in ("column", "filter")], broken

    async def test_a_raw_sql_derived_column_is_checked(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _refuse_planning(monkeypatch, "first_tag")
        async with _env(contractors(columns=(FIRST_TAG,))) as env:
            broken = _by_key(await env.reingest())
        assert broken[("contractors", "column", "first_tag")].checked is False, broken

    async def test_a_dependant_that_still_works(self) -> None:
        n_tagged = ModelMeasure(name="n_tagged", formula="count(tags)")
        n_sets = ModelMeasure(name="n_sets", formula="count_distinct(tags)")
        by_set = {"source_model": "contractors", "dimensions": ["tags"], "measures": ["*:count"]}
        async with _env(contractors(measures=(NT, n_tagged, n_sets)), saved={"by_set": by_set}) as env:
            broken = _by_key(await env.reingest())
        assert ("contractors", "measure", "nt") in broken, broken
        for key in (("contractors", "measure", "n_tagged"), ("contractors", "measure", "n_sets"),
                    ("by_set", "saved_query", "by_set")):
            assert key not in broken, broken

    async def test_a_definition_already_broken(self) -> None:
        ghostly = ModelMeasure(name="ghostly", formula="max(tags) + sum(ghost)")
        async with _env(contractors(measures=(NT, ghostly))) as env:
            broken = _by_key(await env.reingest())
        assert ("contractors", "measure", "nt") in broken, broken
        assert ("contractors", "measure", "ghostly") not in broken, broken

    async def test_a_retype_whose_save_failed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async with _env(contractors()) as env:
            save = env.storage.save_model

            async def refusing(model: SlayerModel, **kwargs: Any) -> None:
                if model.name == "contractors":
                    raise OSError("disk full")
                await save(model, **kwargs)

            monkeypatch.setattr(env.storage, "save_model", refusing)
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                result = await env.reingest()
        assert [e.model_name for e in result.errors] == ["contractors"], result.errors
        assert result.broken_dependents == [], result.broken_dependents
        assert not [w for w in caught if isinstance(w.message, SlayerBrokenDependentWarning)], caught

    @pytest.mark.parametrize("marker", ["notes", "label"])
    async def test_a_name_mention_that_is_no_reference_is_not_checked(self, monkeypatch: pytest.MonkeyPatch,
                                                                     marker: str) -> None:
        label = Column(name="label", sql="'tags'", type=DataType.TEXT)
        _refuse_planning(monkeypatch, marker)
        async with _env(contractors(), orders(columns=(label,)), notes()) as env:
            result = await env.reingest()
        assert ("contractors", "measure", "nt") in _by_key(result), result.broken_dependents
        assert not [b for b in result.broken_dependents if b.model == "notes" or b.name == "label"], (
            result.broken_dependents)


class TestReportBroken:
    @staticmethod
    def _found(*, joint: bool) -> FoundBreak:
        payload = BrokenDependentWarning(model="m", data_source="ds", dependent_kind="measure", name="x",
                                         column="a.p", error_type="ListTypeError", error="e")
        return FoundBreak(causes=[("a", "p"), ("b", "q")], payload=payload, joint=joint)

    def test_a_joint_break_needs_every_cause_saved(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            assert report_broken([self._found(joint=True)], unsaved={"b"}) == []

    def test_a_single_cause_break_names_its_first_saved_column(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            (entry,) = report_broken([self._found(joint=False)], unsaved={"a"})
        assert entry.column == "b.q", entry


class TestUnchecked:
    async def test_an_unchecked_dependant_is_reported_with_its_reason(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _refuse_planning(monkeypatch, "audited")
        audited = ModelMeasure(name="audited", formula="max(tags)")
        async with _env(contractors(measures=(audited,))) as env:
            broken = _by_key(await env.reingest())
        entry = broken[("contractors", "measure", "audited")]
        assert (entry.checked, entry.column, entry.error_type) == (False, TAGS, None), entry
        assert entry.reason, entry
        assert "planner offline" in entry.reason, entry


class TestCallerClient:
    async def test_the_ingest_client_stays_usable(self) -> None:
        async with _env(contractors()) as env:
            client = SlayerSQLClient(env.ds)
            try:
                result = await env.reingest(client=client)
                rows = (await client.execute("SELECT 1 AS one")).rows
            finally:
                await client.aclose()
        assert ("contractors", "measure", "nt") in _by_key(result)
        assert rows == [{"one": 1}]


class TestSurfaces:
    @pytest.fixture
    def workspace(self) -> Iterator[Path]:
        with tempfile.TemporaryDirectory() as tmp:
            yield Path(tmp)

    def test_cli_lists_each_broken_dependant(self, workspace: Path, capsys: pytest.CaptureFixture[str]) -> None:
        ds, _ = run_sync(_seed(str(workspace), models=(contractors(),), saved={"by_tag": BY_TAG}))
        args = SimpleNamespace(datasource="ds", schema=None, include=None, exclude=None, include_views=True,
                               surface_internals=False, storage=str(workspace / "store"))
        try:
            with contextlib.suppress(SystemExit):
                _run_ingest(args)
        finally:
            engine_factory.invalidate_engine(ds)
        lines = capsys.readouterr().out.splitlines()
        for model, name in (("contractors", "nt"), ("by_tag", "by_tag")):
            assert any(name in line and model in line and TAGS in line for line in lines), lines

    async def test_mcp_lists_each_broken_dependant(self) -> None:
        async with _env(contractors(), saved={"by_tag": BY_TAG}) as env:
            server = create_mcp_server(storage=env.storage)
            blocks, _ = cast(Any, await server.call_tool(name="ingest_datasource_models",
                                                         arguments={"datasource_name": "ds"}))
        lines = blocks[0].text.splitlines()
        for model, name in (("contractors", "nt"), ("by_tag", "by_tag")):
            assert any(name in line and model in line and TAGS in line for line in lines), lines

    async def test_rest_returns_broken_dependents(self) -> None:
        async with _env(contractors()) as env:
            with TestClient(create_app(storage=env.storage)) as http:
                response = http.post("/ingest", json={"datasource": "ds"})
        body = response.json()
        entries = [e for e in body["broken_dependents"] if (e["model"], e["name"]) == ("contractors", "nt")]
        assert len(entries) == 1, body
        assert {k: entries[0][k] for k in ("kind", "dependent_kind", "column", "error_type", "checked")} == {
            "kind": "broken_dependent", "dependent_kind": "measure", "column": TAGS, "error_type": "ListTypeError",
            "checked": True}
