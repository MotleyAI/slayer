"""A type mismatch on a live column that still exists is a report-only re-type finding, never a delete
(spec: models/schema-drift)."""

from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from pytest_postgresql import factories
from sqlalchemy.dialects import mysql

from slayer.api.server import create_app
from slayer.core.enums import DataType
from slayer.core.errors import SchemaDriftError
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.engine import schema_drift
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.sql import engine_factory
from slayer.sql.client import DelegatingSqlClient, SlayerSQLClient, SqlClient
from slayer.sql.dialects.reflection import ReflectedColumn, TableLocation, column_from_sa_type
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._cli_inprocess import run_cli_in_process

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

RETYPE_FIELDS = {"finding", "model_name", "data_source", "column", "stored_type", "live_type", "live_db_type",
                 "remedy_kind", "remedy"}

DUCK_LIVE = (
    "CREATE TABLE kinds (id INTEGER, flags BIT, span INTERVAL, doc JSON, tz TIMETZ, code INTEGER, "
    "skills VARCHAR[], kept VARCHAR)",
    "INSERT INTO kinds VALUES (1, '101'::BIT, INTERVAL 1 DAY, '{\"a\": 1}', '10:00:00+01', 7, ['go'], 'x'), "
    "(2, '011'::BIT, INTERVAL 2 DAY, '{\"a\": 2}', '11:00:00+01', 8, [], 'y')",
)

PG_LIVE = (
    "CREATE TABLE pgtypes (id INTEGER PRIMARY KEY, dur INTERVAL, payload JSON, ref OID, amount MONEY, doc XML)",
    "INSERT INTO pgtypes VALUES (1, '1 day', '{\"a\": 1}', 42, 12.50, '<a/>'), "
    "(2, '2 days', '{\"a\": 1}', 7, 1.25, '<b/>')",
)


def _pk() -> Column:
    return Column(name="id", type=DataType.INT, primary_key=True)


# DuckDB models typed as 1.6.0 typed them: info-schema reflection (table-backed), the category probe (sql-mode).

def _legacy() -> SlayerModel:
    return SlayerModel(name="legacy", sql_table="kinds", data_source="ds", columns=[
        _pk(), Column(name="flags", type=DataType.TEXT), Column(name="span", type=DataType.TEXT),
        Column(name="doc", type=DataType.TEXT), Column(name="flags_upper", sql="upper(flags)", type=DataType.TEXT),
    ], measures=[ModelMeasure(name="n_flags", formula="count_distinct(flags)")])


def _legacy_sql() -> SlayerModel:
    return SlayerModel(name="legacy_sql", sql="SELECT id, span, doc, flags, tz FROM kinds", data_source="ds", columns=[
        Column(name="id", type=DataType.DOUBLE, primary_key=True), Column(name="span", type=DataType.TIMESTAMP),
        Column(name="doc", type=DataType.TEXT), Column(name="flags", type=DataType.TEXT),
        Column(name="tz", type=DataType.TIMESTAMP),
    ])


def _retyped() -> SlayerModel:
    return SlayerModel(name="retyped", sql_table="kinds", data_source="ds", columns=[
        _pk(), Column(name="code", type=DataType.TEXT),
    ], measures=[ModelMeasure(name="n_codes", formula="count_distinct(code)")])


def _listy() -> SlayerModel:
    return SlayerModel(name="listy", sql_table="kinds", data_source="ds", columns=[
        _pk(), Column(name="skills", type=DataType.TEXT),
    ])


def _listy_sql() -> SlayerModel:
    return SlayerModel(name="listy_sql", sql="SELECT id, skills FROM kinds", data_source="ds", columns=[
        _pk(), Column(name="skills", type=DataType.TEXT),
    ])


def _dropped() -> SlayerModel:
    return SlayerModel(name="dropped", sql_table="kinds", data_source="ds", columns=[
        _pk(), Column(name="kept", type=DataType.TEXT), Column(name="gone", type=DataType.TEXT),
    ], measures=[ModelMeasure(name="n_gone", formula="count_distinct(gone)")])


DUCK_MODELS = (_legacy, _legacy_sql, _retyped, _listy, _listy_sql, _dropped)


class Duck:
    def __init__(self, *, root: Path, ds: DatasourceConfig, storage: YAMLStorage) -> None:
        self.root = root
        self.ds = ds
        self.storage = storage

    def engine(self, **kwargs: Any) -> SlayerQueryEngine:
        return SlayerQueryEngine(storage=self.storage, **kwargs)

    async def model(self, name: str) -> SlayerModel:
        loaded = await self.storage.get_model(name, data_source="ds")
        assert loaded is not None
        return loaded


@pytest.fixture
async def duck(tmp_path: Path) -> AsyncGenerator[Duck]:
    duckdb = pytest.importorskip("duckdb")
    db_path = str(tmp_path / "live.duckdb")
    with contextlib.closing(duckdb.connect(db_path)) as con:
        for stmt in DUCK_LIVE:
            con.execute(stmt)
    ds = DatasourceConfig(name="ds", type="duckdb", database=db_path)
    storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
    await storage.save_datasource(ds)
    for build in DUCK_MODELS:
        await storage.save_model(build())
    try:
        yield Duck(root=tmp_path, ds=ds, storage=storage)
    finally:
        engine_factory.invalidate_engine(ds)


@pytest.fixture
async def pg(postgresql, tmp_path: Path) -> AsyncGenerator[SlayerQueryEngine]:
    cur = postgresql.cursor()
    for stmt in PG_LIVE:
        cur.execute(stmt)
    postgresql.commit()
    info = postgresql.info
    ds = DatasourceConfig(name="pg", type="postgres", host=info.host, port=info.port, database=info.dbname,
                          username=info.user, password="")
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(ds)
    # As 1.6.0 reflection typed the table (interval TEXT, json UNKNOWN, oid TEXT, money DOUBLE, xml TEXT),
    # plus ``duration`` stored as TIMESTAMP over the interval.
    await storage.save_model(SlayerModel(name="pg_table", sql_table="pgtypes", data_source="pg", columns=[
        _pk(), Column(name="duration", sql="dur", type=DataType.TIMESTAMP), Column(name="dur", type=DataType.TEXT),
        Column(name="payload", type=DataType.UNKNOWN), Column(name="ref", type=DataType.TEXT),
        Column(name="amount", type=DataType.DOUBLE), Column(name="doc", type=DataType.TEXT),
    ]))
    # As the 1.6.0 category probe typed the result (json, xml "string"; interval "time"; oid, money "number").
    await storage.save_model(SlayerModel(
        name="pg_sql", sql="SELECT id, payload, dur, ref, amount, doc FROM pgtypes", data_source="pg", columns=[
            Column(name="id", type=DataType.DOUBLE, primary_key=True), Column(name="payload", type=DataType.TEXT),
            Column(name="dur", type=DataType.TIMESTAMP), Column(name="ref", type=DataType.DOUBLE),
            Column(name="amount", type=DataType.DOUBLE), Column(name="doc", type=DataType.TEXT),
        ]))
    engine = SlayerQueryEngine(storage=storage)
    try:
        yield engine
    finally:
        await engine.aclose()
        engine.close()
        engine_factory.invalidate_engine(ds)


def _dump(entries: list[Any]) -> list[dict[str, Any]]:
    return [e.model_dump(mode="json") for e in entries]


def _assert_remedy_agrees(finding: dict[str, Any]) -> None:
    """``remedy`` and ``summary`` agree with ``remedy_kind``."""
    expected = {
        "reingest": f"re-ingest datasource '{finding['data_source']}' (`slayer ingest`) to re-type it",
        "set_type": f"set the column's type to {finding['live_type']}",
    }[finding["remedy_kind"]]
    assert finding["remedy"] == expected, finding
    assert finding["summary"].endswith(f"; {expected}."), finding


def _retypes(entries: list[dict[str, Any]], model: str) -> dict[str, dict[str, Any]]:
    found = [e for e in entries if e.get("finding") == "retype"]
    for e in found:
        _assert_remedy_agrees(e)
    return {e["column"]: e for e in found if e.get("model_name") == model}


def _removed(entries: list[dict[str, Any]], model: str) -> set[str]:
    """Every column and measure ``model`` loses to the deletes in ``entries``."""
    out: set[str] = set()
    for e in entries:
        if e.get("model_name") != model:
            continue
        if e.get("tool") == "delete_model":
            out.add("<model>")
        elif e.get("tool") == "edit_model":
            out |= set(e["remove"]["columns"]) | set(e["remove"]["measures"])
    return out


async def _validate(engine: SlayerQueryEngine, data_source: str) -> list[dict[str, Any]]:
    try:
        return _dump(await engine.validate_models(data_source=data_source))
    finally:
        await engine.aclose()
        engine.close()


def _assert_live(finding: dict[str, Any], *, stored: DataType, live: DataType, db_type: str | None) -> None:
    assert RETYPE_FIELDS <= set(finding), finding
    assert (finding["stored_type"], finding["live_type"]) == (stored.value, live.value), finding
    if db_type is None:
        assert finding["live_db_type"] is None, finding
    else:
        assert (finding["live_db_type"] or "").lower() == db_type.lower(), finding


def _assert_set_type_remedy(finding: dict[str, Any]) -> None:
    remedy = finding["remedy"]
    assert finding["live_type"] in remedy, finding
    assert "type" in remedy.lower(), finding


class TestTypedBy160Reflection:
    async def test_duckdb_bit_stored_as_text(self, duck: Duck) -> None:
        entries = await _validate(duck.engine(), "ds")
        found = _retypes(entries, "legacy")
        assert set(found) == {"flags"}, entries
        _assert_live(found["flags"], stored=DataType.TEXT, live=DataType.BOOLEAN, db_type="BIT")
        _assert_set_type_remedy(found["flags"])
        assert found["flags"]["remedy_kind"] == "set_type", found
        assert _removed(entries, "legacy") == set(), entries

    async def test_postgres_interval_and_oid(self, pg: SlayerQueryEngine) -> None:
        entries = _dump(await pg.validate_models(data_source="pg"))
        found = _retypes(entries, "pg_table")
        assert set(found) == {"duration", "ref"}, entries
        _assert_live(found["duration"], stored=DataType.TIMESTAMP, live=DataType.TEXT, db_type="interval")
        _assert_live(found["ref"], stored=DataType.TEXT, live=DataType.INT, db_type="oid")
        assert {c: f["remedy_kind"] for c, f in found.items()} == {"duration": "set_type", "ref": "set_type"}
        assert _removed(entries, "pg_table") == set(), entries

    async def test_mysql_year_stored_as_text(self, duck: Duck) -> None:
        """MySQL's ``YEAR`` as HEAD reflects it (1.6.0 stored TEXT), served by a gateway over the DuckDB table."""
        model = SlayerModel(name="years", sql_table="kinds", data_source="ds", columns=[
            _pk(), Column(name="kept", type=DataType.TEXT)])
        await duck.storage.save_model(model)
        entries = await _validate(duck.engine(client_factory=_year_gateway), "ds")
        found = _retypes(entries, "years")
        assert set(found) == {"kept"}, entries
        assert (found["kept"]["stored_type"], found["kept"]["live_type"]) == ("TEXT", "INT"), found
        assert _removed(entries, "years") == set(), entries


class _YearGateway(DelegatingSqlClient):
    async def reflect_columns(self, location: TableLocation) -> list[ReflectedColumn]:
        columns = await self.inner.reflect_columns(location)
        return [column_from_sa_type(name=c.name, sa_type=mysql.YEAR()) if c.name == "kept" else c for c in columns]


def _year_gateway(datasource: DatasourceConfig) -> SqlClient:
    return _YearGateway(SlayerSQLClient(datasource=datasource))


class TestTypedBy160Probe:
    async def test_postgres_json_xml_and_interval(self, pg: SlayerQueryEngine) -> None:
        entries = _dump(await pg.validate_models(data_source="pg"))
        found = _retypes(entries, "pg_sql")
        assert set(found) == {"payload", "dur", "doc"}, entries
        _assert_live(found["payload"], stored=DataType.TEXT, live=DataType.UNKNOWN, db_type="json")
        _assert_live(found["doc"], stored=DataType.TEXT, live=DataType.UNKNOWN, db_type="xml")
        _assert_live(found["dur"], stored=DataType.TIMESTAMP, live=DataType.TEXT, db_type="interval")
        assert {f["remedy_kind"] for f in found.values()} == {"set_type"}, found
        assert _removed(entries, "pg_sql") == set(), entries

    async def test_duckdb_interval_bit_and_timetz(self, duck: Duck) -> None:
        entries = await _validate(duck.engine(), "ds")
        found = _retypes(entries, "legacy_sql")
        assert set(found) == {"span", "flags", "tz"}, entries
        _assert_live(found["span"], stored=DataType.TIMESTAMP, live=DataType.TEXT, db_type="INTERVAL")
        _assert_live(found["flags"], stored=DataType.TEXT, live=DataType.BOOLEAN, db_type="BIT")
        _assert_live(found["tz"], stored=DataType.TIMESTAMP, live=DataType.TEXT, db_type="TIME WITH TIME ZONE")
        assert {f["remedy_kind"] for f in found.values()} == {"set_type"}, found
        assert _removed(entries, "legacy_sql") == set(), entries


class TestScalarAndList:
    async def test_a_genuinely_retyped_column(self, duck: Duck) -> None:
        entries = await _validate(duck.engine(), "ds")
        (finding,) = _retypes(entries, "retyped").values()
        _assert_live(finding, stored=DataType.TEXT, live=DataType.INT, db_type="INTEGER")
        _assert_set_type_remedy(finding)
        assert finding["remedy_kind"] == "set_type", finding
        assert _removed(entries, "retyped") == set(), entries

    async def test_a_list_keeps_the_reingest_remedy(self, duck: Duck) -> None:
        entries = await _validate(duck.engine(), "ds")
        (finding,) = _retypes(entries, "listy").values()
        _assert_live(finding, stored=DataType.TEXT, live=DataType.ARRAY, db_type="VARCHAR[]")
        assert "ingest" in finding["remedy"].lower(), finding
        assert finding["remedy_kind"] == "reingest", finding
        assert not any(e.get("finding") == "list_retype" for e in entries), entries

    async def test_a_list_in_an_sql_mode_model_keeps_the_reingest_remedy(self, duck: Duck) -> None:
        entries = await _validate(duck.engine(), "ds")
        (finding,) = _retypes(entries, "listy_sql").values()
        _assert_live(finding, stored=DataType.TEXT, live=DataType.ARRAY, db_type="VARCHAR[]")
        assert "ingest" in finding["remedy"].lower(), finding
        assert finding["remedy_kind"] == "reingest", finding
        assert _removed(entries, "listy_sql") == set(), entries

    async def test_a_missing_column_is_still_deleted_with_its_cascade(self, duck: Duck) -> None:
        entries = await _validate(duck.engine(), "ds")
        assert _removed(entries, "dropped") == {"gone", "n_gone"}, entries
        assert _retypes(entries, "dropped") == {}, entries

    async def test_no_type_name_from_a_160_client(self, duck: Duck) -> None:
        """A 1.6.0 client's category probe reports no database type name."""
        engine = duck.engine(client_factory=_category_gateway)
        with pytest.warns(FutureWarning, match="get_result_column_types"):
            entries = await _validate(engine, "ds")
        found = _retypes(entries, "legacy_sql")
        _assert_live(found["span"], stored=DataType.TIMESTAMP, live=DataType.TEXT, db_type=None)


_CATEGORY = {DataType.INT: "number", DataType.DOUBLE: "number", DataType.TIMESTAMP: "time", DataType.DATE: "time",
             DataType.BOOLEAN: "boolean"}


class _CategoryGateway(DelegatingSqlClient):
    """A 1.6.0 gateway: its only type probe is ``get_column_types``."""

    async def get_column_types(self, sql: str) -> dict[str, str]:
        types = await self.inner.get_result_column_types(sql)
        return {name: _CATEGORY.get(t.type, "string") for name, t in types.items()}


def _category_gateway(datasource: DatasourceConfig) -> SqlClient:
    return _CategoryGateway(SlayerSQLClient(datasource=datasource))


class TestForceClean:
    async def test_force_clean_keeps_retyped_columns_and_their_measures(self, duck: Duck) -> None:
        result = run_cli_in_process(["validate-models", "--datasource", "ds", "--force-clean", "--yes",
                                     "--storage", str(duck.root / "storage")])
        assert result.returncode == 0, result.stderr
        legacy, retyped, dropped = [await duck.model(n) for n in ("legacy", "retyped", "dropped")]
        assert {"flags", "flags_upper"} <= {c.name for c in legacy.columns}
        assert [m.name for m in legacy.measures] == ["n_flags"]
        assert "code" in {c.name for c in retyped.columns}
        assert [m.name for m in retyped.measures] == ["n_codes"]
        assert "gone" not in {c.name for c in dropped.columns}
        lines = [line for line in result.stdout.splitlines() if "RE-TYPE:" in line]
        for model, column in (("legacy", "flags"), ("legacy_sql", "span"), ("legacy_sql", "flags"),
                              ("legacy_sql", "tz"), ("retyped", "code"), ("listy", "skills")):
            assert sum(f"{model}.{column}" in line for line in lines) == 1, (model, column, result.stdout)


def _carried(err: SchemaDriftError, *, model: str, column: str) -> bool:
    wanted = {"finding": "retype", "model_name": model, "column": column}
    carried = [f.model_dump(mode="json") for f in getattr(err, "findings", [])]
    for f in carried:
        _assert_remedy_agrees(f)
    return any(wanted.items() <= f.items() for f in carried)


class TestQueryTime:
    async def test_a_query_failure_explained_by_a_retype(self, duck: Duck) -> None:
        engine = duck.engine()
        try:
            with pytest.raises(SchemaDriftError) as info:
                await engine.execute({"source_model": "legacy", "dimensions": ["flags_upper"]})
        finally:
            await engine.aclose()
            engine.close()
        err = info.value
        assert "flags" not in _removed(_dump(err.to_delete), "legacy"), err.to_delete
        assert "legacy.flags" in str(err), str(err)
        assert "BIT" in str(err), str(err)
        assert _carried(err, model="legacy", column="flags"), err.findings

    async def test_postgres_json_grouped(self, pg: SlayerQueryEngine) -> None:
        with pytest.raises(SchemaDriftError) as info:
            await pg.execute({"source_model": "pg_sql", "dimensions": ["payload"], "measures": ["*:count"]})
        err = info.value
        assert "payload" not in _removed(_dump(err.to_delete), "pg_sql"), err.to_delete
        assert "pg_sql.payload" in str(err), str(err)
        assert "json" in str(err), str(err)
        assert _carried(err, model="pg_sql", column="payload"), err.findings


class TestOneShape:
    async def test_rest_mcp_and_python_agree(self, duck: Duck) -> None:
        python = [e for e in await _validate(duck.engine(), "ds") if e.get("finding") == "retype"]
        rest = TestClient(create_app(storage=duck.storage)).post("/validate-models", json={"data_source": "ds"})
        assert rest.status_code == 200, rest.text
        server: Any = create_mcp_server(storage=duck.storage)
        try:
            blocks, _ = await server.call_tool(name="validate_models", arguments={"data_source": "ds"})
        finally:
            await server._slayer_engine.aclose()
        mcp = json.loads(blocks[0].text)

        def key(e: dict[str, Any]) -> tuple[str, str]:
            return e["model_name"], e["column"]

        assert python, python
        assert all(RETYPE_FIELDS <= set(e) for e in python), python
        for e in python:
            _assert_remedy_agrees(e)
        expected = sorted(python, key=key)
        assert sorted((e for e in rest.json() if e.get("finding") == "retype"), key=key) == expected
        assert sorted((e for e in mcp if e.get("finding") == "retype"), key=key) == expected


class TestSqliteProbe:
    async def test_runs_only_when_findings_are_collected(self, tmp_path: Path,
                                                         monkeypatch: pytest.MonkeyPatch) -> None:
        db = tmp_path / "s.db"
        with transaction(db) as con:
            con.execute("CREATE TABLE t (id INTEGER)")
        ds = DatasourceConfig(name="s", type="sqlite", database=str(db))
        model = SlayerModel(name="t", sql_table="t", data_source="s", columns=[_pk()])
        calls: list[str] = []
        probe = schema_drift._sqlite_probe_drifts_for_models

        async def counting(**kwargs: Any) -> Any:
            calls.append(kwargs["datasource"].name)
            return await probe(**kwargs)

        monkeypatch.setattr(schema_drift, "_sqlite_probe_drifts_for_models", counting)
        client = SlayerSQLClient(datasource=ds)
        try:
            await schema_drift.validate_datasource(datasource=ds, models=[model], client=client)
            uncollected = list(calls)
            await schema_drift.validate_datasource(datasource=ds, models=[model], client=client, findings_out=[])
        finally:
            await client.aclose()
            engine_factory.invalidate_engine(ds)
        assert (uncollected, calls) == ([], ["s"])

    async def test_probes_only_live_integer_columns(self, tmp_path: Path) -> None:
        db = tmp_path / "s.db"
        with transaction(db) as con:
            con.execute("CREATE TABLE t (id INTEGER, code TEXT, amt INTEGER)")
            con.execute("INSERT INTO t VALUES (1, '12', 1.5), (2, '7', 2)")
        ds = DatasourceConfig(name="s", type="sqlite", database=str(db))
        model = SlayerModel(name="t", sql_table="t", data_source="s", columns=[
            _pk(), Column(name="code", type=DataType.INT), Column(name="amt", type=DataType.INT),
            Column(name="amount", sql='"amt"', type=DataType.INT)])
        findings: list[Any] = []
        client = SlayerSQLClient(datasource=ds)
        try:
            await schema_drift.validate_datasource(datasource=ds, models=[model], client=client,
                                                   findings_out=findings)
        finally:
            await client.aclose()
            engine_factory.invalidate_engine(ds)
        assert sorted((f.column, f.live_type) for f in findings) == [
            ("amount", DataType.DOUBLE), ("amt", DataType.DOUBLE), ("code", DataType.TEXT)]
        for f in findings:
            _assert_remedy_agrees(f.model_dump(mode="json"))
        # ``code`` is a bucket conflict over a live TEXT column, not a probe verdict.
        assert {f.column: f.remedy_kind for f in findings} == {
            "amount": "reingest", "amt": "reingest", "code": "set_type"}


_REINGEST_REMEDY = "re-ingest datasource 'ds' (`slayer ingest`) to re-type it"
_SET_TYPE_REMEDY = "set the column's type to INT"


def _legacy_payload(*, live_type: str, live_db_type: str, remedy: str) -> dict[str, Any]:
    """A re-type finding as 1.8.0 serialized it, before ``remedy_kind``."""
    return {
        "finding": "retype", "model_name": "m", "data_source": "ds", "column": "c", "stored_type": "TEXT",
        "live_type": live_type, "live_db_type": live_db_type, "remedy": remedy,
        "summary": f"Column 'm.c' is stored as TEXT but its live column is {live_db_type} ({live_type}); {remedy}.",
    }


class TestSerializedFinding:
    @pytest.mark.parametrize(("live_type", "live_db_type", "remedy", "kind"), [
        ("ARRAY", "VARCHAR[]", _REINGEST_REMEDY, "reingest"),
        ("INT", "INTEGER", _SET_TYPE_REMEDY, "set_type"),
    ])
    def test_a_finding_without_a_remedy_kind_loads(self, live_type: str, live_db_type: str, remedy: str,
                                                   kind: str) -> None:
        payload = _legacy_payload(live_type=live_type, live_db_type=live_db_type, remedy=remedy)
        loaded = schema_drift.RetypeFinding.model_validate(payload)
        assert loaded.remedy_kind == kind
        assert (loaded.remedy, loaded.summary) == (payload["remedy"], payload["summary"])

    def test_an_unrecognized_remedy_text_is_refused(self) -> None:
        payload = _legacy_payload(live_type="INT", live_db_type="INTEGER", remedy="drop the column")
        with pytest.raises(ValidationError):
            schema_drift.RetypeFinding.model_validate(payload)

    def test_a_contradicting_remedy_text_is_recomputed(self) -> None:
        payload = {**_legacy_payload(live_type="INT", live_db_type="INTEGER", remedy=_REINGEST_REMEDY),
                   "remedy_kind": "set_type"}
        loaded = schema_drift.RetypeFinding.model_validate(payload)
        assert loaded.remedy == _SET_TYPE_REMEDY
        assert loaded.summary.endswith(f"; {_SET_TYPE_REMEDY}.")

    @pytest.mark.parametrize("kind", ["reingest", "set_type"])
    def test_a_full_dump_round_trips(self, kind: schema_drift.RemedyKind) -> None:
        model = SlayerModel(name="m", sql_table="t", data_source="ds", columns=[_pk()])
        finding = schema_drift.RetypeFinding.of(
            model=model, column=Column(name="c", type=DataType.TEXT), live_type=DataType.INT,
            live_db_type="INTEGER", remedy_kind=kind)
        dumped = finding.model_dump(mode="json")
        assert dumped["remedy_kind"] == kind
        _assert_remedy_agrees(dumped)
        assert schema_drift.RetypeFinding.model_validate(dumped) == finding
