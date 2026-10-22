"""``json_value``, its spellings, the path subset, RETURNING and JSON inputs (spec: queries/json)."""

from __future__ import annotations

import contextlib
import os
import tempfile
from typing import Any, Optional

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mysql, postgresql

from slayer.core.enums import DataType
from slayer.core.errors import SlayerError
from slayer.core.keys import CastKey
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.scope import ModelScope
from slayer.core.syntax import parse_expr
from slayer.engine.binding import bind_expr
from slayer.engine.ingestion import _introspected_column, ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql import engine_factory
from tests._dev2120_fixtures import (
    DOCS_ROWS,
    EXEC_BACKENDS,
    TIER1,
    check_native_json,
    emit_lib,
    lib_engine,
    lib_model,
    lib_query,
    lib_values,
    patch_profile,
)
from tests._engine_helpers import build_exec_engine


def same(a: str, b: str) -> None:
    assert parse_expr(a) == parse_expr(b), (a, b)


@pytest.fixture(params=EXEC_BACKENDS)
async def engine(request):
    async with lib_engine(request.param) as e:
        yield e


async def _rejected(engine: SlayerQueryEngine, expr: str, *, any_of: tuple[str, ...] = (), **kw: Any) -> str:
    with pytest.raises(SlayerError) as info:
        await engine.execute(lib_query(expr, **kw))
    msg = str(info.value)
    if any_of:
        assert any(n.lower() in msg.lower() for n in any_of), msg
    return msg


class TestSpellings:
    @pytest.mark.parametrize(("spelling", "canonical"), [
        ("payload ->> 'a'", "json_value(payload, '$.a')"),
        ("payload -> 'a' -> 0 ->> 'b'", "json_value(payload, '$.a[0].b')"),
        ("payload ->> '$.a.b'", "json_value(payload, '$.a.b')"),
        ("payload ->> 'unit price'", "json_value(payload, '$.\"unit price\"')"),
        ("json_extract(payload, '$.a.b')", "json_value(payload, '$.a.b')"),
        ("json_extract_scalar(payload, '$.a.b')", "json_value(payload, '$.a.b')"),
        ("json_extract_path_text(payload, 'a', 'b')", "json_value(payload, '$.a.b')"),
        ("jsonb_extract_path_text(payload, 'a', 'b')", "json_value(payload, '$.a.b')"),
        ("JSON_VALUE(payload, '$.a')", "json_value(payload, '$.a')"),
        ("json_value(payload, '$.a' RETURNING INT)", "TRY_CAST(json_value(payload, '$.a') AS INT)"),
        ("json_value(payload, '$.a' returning double)", "TRY_CAST(json_value(payload, '$.a') AS DOUBLE)"),
    ])
    def test_collapses_to_json_value(self, spelling: str, canonical: str) -> None:
        same(spelling, canonical)

    @pytest.mark.parametrize(("text", "explicit"), [
        ("payload ->> 'n' || 'x'", "(payload ->> 'n') || 'x'"),
        ("payload ->> 'a' = 'x'", "(payload ->> 'a') = 'x'"),
        ("payload ->> 'a' LIKE 'x%'", "(payload ->> 'a') LIKE 'x%'"),
        ("'x' || payload ->> 'a'", "('x' || payload) ->> 'a'"),
    ])
    def test_arrows_bind_at_the_concat_level(self, text: str, explicit: str) -> None:
        same(text, explicit)

    @pytest.mark.parametrize("text", ["payload -> 'a'", "payload -> 'a' -> 0", "payload ->> 'a' -> 'b'"])
    def test_bare_arrow_hints_double_arrow(self, text: str) -> None:
        with pytest.raises(ValueError, match="->>"):
            parse_expr(text)

    @pytest.mark.parametrize("text", ["payload ->> word", "payload ->> ('a' || 'b')", "payload -> word ->> 'a'"])
    def test_non_literal_arrow_operand_rejected(self, text: str) -> None:
        with pytest.raises(ValueError):
            parse_expr(text)

    @pytest.mark.parametrize("text", [
        "json_extract(payload, '$.a' RETURNING INT)", "json_extract(payload, '$.a', '$.b')",
        "json_extract_scalar(payload)", "json_value(payload)", "json_value(payload, '$.a' RETURNING JSON)",
    ])
    async def test_malformed_call_rejected(self, engine: SlayerQueryEngine, text: str) -> None:
        with pytest.raises(ValueError):
            await engine.execute(lib_query(text))


class TestReturningIsTryCast:
    def test_returning_binds_to_a_try_cast_key(self) -> None:
        model = lib_model()
        bundle = ResolvedSourceBundle(dialect="postgres", source_model=model, referenced_models=[])
        key = bind_expr(parse_expr("json_value(payload, '$.qty' RETURNING INT)"), scope=ModelScope(source_model=model),
                        bundle=bundle, allow_measures=True).value_key
        assert isinstance(key, CastKey), key
        assert key.try_ is True
        assert key.data_type is DataType.INT


class TestPathSubset:
    @pytest.mark.parametrize(("path", "names"), [
        ("$.items[*].sku", ("wildcard", "*")),
        ("$.*", ("wildcard", "*")),
        ("$..a", ("recursive", "..")),
        ("$.a[-1]", ("negative", "-1")),
        ("$.a[?(@.b)]", ("filter", "?")),
        ("a.b", ("$",)),
        ("$['a']", ("[",)),
        ("$.a[1.5]", ("index", "1.5")),
        ("$.a[last]", ("last", "index")),
        ("lax $.a", ("lax",)),
        ("$.\"a", ("quote", '"')),
    ])
    async def test_unsupported_path_named(self, engine: SlayerQueryEngine, path: str, names: tuple[str, ...]) -> None:
        quoted = path.replace("'", "''")
        await _rejected(engine, f"json_value(payload, '{quoted}')", any_of=names)

    async def test_non_literal_path_rejected(self, engine: SlayerQueryEngine) -> None:
        await _rejected(engine, "json_value(payload, prefix)", any_of=("path",))

    async def test_variable_path(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(lib_query("json_value(payload, {p})", filters=["id = 1"], variables={"p": "$.a.b"}))
        assert lib_values(resp) == {1: "x"}

    async def test_bad_variable_path_rejected(self, engine: SlayerQueryEngine) -> None:
        await _rejected(engine, "json_value(payload, {p})", any_of=("wildcard", "*"), variables={"p": "$.a[*]"})


def _measure_query(formula: str, **kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "lib", "measures": [{"formula": formula, "name": "v"}], **kw})


class TestTyping:
    async def test_typed_sum(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(_measure_query("sum(json_value(payload, '$.qty' RETURNING INT))",
                                                   filters=["id IN (1, 2, 3, 8)"]))
        assert float(resp.data[0]["lib.v"]) == 5

    async def test_text_result_keeps_the_numeric_aggregation_error(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(SlayerError, match="TRY_CAST"):
            await engine.execute(_measure_query("sum(json_value(payload, '$.qty'))"))

    @pytest.mark.parametrize("expr", ["json_value(qty, '$.a')", "json_value(ts, '$.a')", "qty ->> 'a'"])
    async def test_non_text_input_rejected(self, engine: SlayerQueryEngine, expr: str) -> None:
        await _rejected(engine, expr, any_of=("json",))


def _json_model(*, db_type: Optional[str], type_: DataType = DataType.TEXT) -> SlayerModel:
    base = lib_model()
    columns = [c for c in base.columns if c.name != "payload"]
    columns.append(Column(name="payload", type=type_, db_type=db_type))
    return base.model_copy(update={"columns": columns})


JSON_VALUE_Q = lib_query("json_value(payload, '$.a')")


class TestInputForms:
    async def test_postgres_reads_every_input_as_jsonb(self) -> None:
        for db_type in ("jsonb", "json", None):
            sql = await emit_lib(JSON_VALUE_Q, dialect="postgres", models=[_json_model(db_type=db_type)])
            assert "JSONB" in sql.upper(), (db_type, sql)

    async def test_snowflake_variant_read_directly(self) -> None:
        native = await emit_lib(JSON_VALUE_Q, dialect="snowflake", models=[_json_model(db_type="VARIANT")])
        text = await emit_lib(JSON_VALUE_Q, dialect="snowflake", models=[_json_model(db_type=None)])
        assert "TRY_PARSE_JSON" not in native.upper(), native
        assert "TRY_PARSE_JSON" in text.upper(), text

    async def test_clickhouse_json_column_differs_from_string(self) -> None:
        native = await emit_lib(JSON_VALUE_Q, dialect="clickhouse", models=[_json_model(db_type="JSON")])
        text = await emit_lib(JSON_VALUE_Q, dialect="clickhouse", models=[_json_model(db_type=None)])
        assert native != text

    @pytest.mark.parametrize(("dialect", "db_type"), [
        ("postgres", "jsonb"), ("postgres", "json"), ("mysql", "JSON"), ("duckdb", "JSON"), ("bigquery", "JSON"),
        ("snowflake", "VARIANT"), ("snowflake", "OBJECT"), ("snowflake", "ARRAY"), ("clickhouse", "JSON"),
    ])
    async def test_native_json_column_emits(self, dialect: str, db_type: str) -> None:
        for type_ in (DataType.TEXT, DataType.UNKNOWN):
            await emit_lib(lib_query("json_value(payload, '$.a' RETURNING INT)"), dialect=dialect,
                           models=[_json_model(db_type=db_type, type_=type_)])

    async def test_opaque_non_json_column_rejected(self) -> None:
        with pytest.raises(SlayerError):
            await emit_lib(JSON_VALUE_Q, dialect="postgres", models=[_json_model(db_type="bytea", type_=DataType.UNKNOWN)])

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_returning_types_emit(self, dialect: str) -> None:
        for target in ("INT", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP", "TEXT"):
            await emit_lib(lib_query(f"json_value(payload, '$.a' RETURNING {target})"), dialect=dialect)


class TestPostgresVersion:
    async def test_postgres_16_guards_malformed_text(self, monkeypatch) -> None:
        patch_profile(monkeypatch, version=(16, 2))
        sql = await emit_lib(JSON_VALUE_Q, dialect="postgres")
        assert "PG_INPUT_IS_VALID(" in sql.upper(), sql

    @pytest.mark.parametrize("version", [(15, 4), None])
    async def test_older_or_unknown_server_follows_the_cast_contract(self, monkeypatch, version) -> None:
        patch_profile(monkeypatch, version=version)
        sql = await emit_lib(JSON_VALUE_Q, dialect="postgres")
        assert "PG_INPUT_IS_VALID(" not in sql.upper(), sql

    async def test_profile_probed_for_json_value(self, monkeypatch) -> None:
        calls = patch_profile(monkeypatch, version=(16, 2))
        await emit_lib(lib_query("lower(word)"), dialect="postgres")
        assert calls == []
        await emit_lib(JSON_VALUE_Q, dialect="postgres")
        assert calls == [1]


def _db_type(sa_type: sa.types.TypeEngine) -> Optional[str]:
    return _introspected_column({"type": sa_type}, name="payload", primary_key=False).db_type


class TestIngestionKeepsJsonDbType:
    @pytest.mark.parametrize(("sa_type", "expected"), [
        (postgresql.JSONB(), "JSONB"), (postgresql.JSON(), "JSON"), (mysql.JSON(), "JSON"), (sa.JSON(), "JSON"),
    ])
    def test_sqlalchemy_json_types(self, sa_type: sa.types.TypeEngine, expected: str) -> None:
        assert (_db_type(sa_type) or "").upper() == expected

    @pytest.mark.parametrize("name", ["VARIANT", "OBJECT", "ARRAY"])
    def test_snowflake_semi_structured_types(self, name: str) -> None:
        sf = pytest.importorskip("snowflake.sqlalchemy")
        assert (_db_type(getattr(sf, name)()) or "").upper().startswith(name)

    async def test_duckdb_json_column_ingested_and_read(self) -> None:
        duckdb = pytest.importorskip("duckdb")
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "docs.duckdb")
            with contextlib.closing(duckdb.connect(db_path)) as con:
                con.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, payload JSON)")
                con.executemany("INSERT INTO docs VALUES (?, ?)", DOCS_ROWS)
            ds = DatasourceConfig(name="test", type="duckdb", database=db_path)
            try:
                models = ingest_datasource(datasource=ds, include_tables=["docs"])
                payload = next(c for c in models[0].columns if c.name == "payload")
                assert (payload.db_type or "").upper() == "JSON"
                engine = await build_exec_engine(db_path, dialect="duckdb", models=models)
                try:
                    await check_native_json(engine)
                finally:
                    engine.close()
            finally:
                engine_factory.invalidate_engine(ds)
