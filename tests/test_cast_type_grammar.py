"""The CAST type grammar, lossless rendering and array casts (spec: queries/casts)."""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.errors import CastTypeError
from slayer.core.keys import ScalarCallKey, ValueKey, is_list_call, list_element_type
from slayer.core.scope import ModelScope
from slayer.core.syntax import Cast, parse_expr
from slayer.engine.binding import bind_expr
from slayer.engine.key_metadata import dimension_key_metadata
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle
from tests._cast_fixtures import check_array_cast, duckdb_tags_engine
from tests._dev2101_fixtures import TIER1, customers_model, emit, orders_model, q

ORDERS = orders_model()


def _dim(expr: str):
    return q(dimensions=["id", {"expression": expr, "name": "x"}])


def _casts(sql: str, *, dialect: str) -> list[exp.Cast]:
    return list(sqlglot.parse_one(sql, dialect=dialect).find_all(exp.Cast))


def _type_sql(sql: str, *, dialect: str) -> list[str]:
    return [c.to.sql(dialect=dialect).upper().replace(" ", "") for c in _casts(sql, dialect=dialect)]


async def _refused(expr: str, *, dialect: str, needle: str) -> None:
    query = _dim(expr)
    with pytest.raises(CastTypeError) as info:
        await emit(query, dialect=dialect)
    msg = str(info.value)
    assert needle.replace(" ", "").lower() in msg.replace(" ", "").lower(), msg
    assert dialect in msg.lower(), msg


def _bind(text: str, *, dialect: str) -> ValueKey:
    bundle = ResolvedSourceBundle(dialect=dialect, source_model=ORDERS, referenced_models=[customers_model()])
    return bind_expr(parse_expr(text), scope=ModelScope(source_model=ORDERS), bundle=bundle,
                     allow_measures=True).value_key


def _planned_type(text: str, *, dialect: str) -> DataType | None:
    bundle = ResolvedSourceBundle(dialect=dialect, source_model=ORDERS, referenced_models=[customers_model()])
    return dimension_key_metadata(model=ORDERS, key=_bind(text, dialect=dialect), bundle=bundle)[0]


def _element_type(text: str, *, dialect: str) -> DataType | None:
    return list_element_type(_bind(text, dialect=dialect), column_type=lambda _k: None, column_element=lambda _k: None)


class TestTypeGrammar:
    @pytest.mark.parametrize("type_text", [
        "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP(3) WITHOUT TIME ZONE", "CHARACTER VARYING(10)",
        "DECIMAL(10,2) UNSIGNED", "INT UNSIGNED", "DOUBLE PRECISION", "INT[]", "TEXT[]", "DECIMAL(10,2)[]",
        "TIMESTAMP WITH TIME ZONE",
    ])
    def test_cast_and_double_colon_parse_alike(self, type_text: str) -> None:
        cast = parse_expr(f"CAST(created_at AS {type_text})")
        assert isinstance(cast, Cast)
        assert cast == parse_expr(f"created_at::{type_text}")

    def test_precision_between_words_is_kept(self) -> None:
        cast = parse_expr("CAST(created_at AS TIMESTAMP(6) WITH TIME ZONE)")
        assert isinstance(cast, Cast)
        assert "6" in cast.type_text, cast.type_text
        assert cast != parse_expr("CAST(created_at AS TIMESTAMP WITH TIME ZONE)")

    def test_array_suffix_is_part_of_the_type(self) -> None:
        assert parse_expr("CAST(code AS INT[])") != parse_expr("CAST(code AS INT)")

    @pytest.mark.parametrize("text", [
        "CAST(amount AS DECIMAL(10,))", "amount::DECIMAL(10,)", "CAST(amount AS DECIMAL(,2))",
        "CAST(amount AS DECIMAL(x))", "CAST(amount AS INT[)",
    ])
    def test_malformed_type_names_the_type(self, text: str) -> None:
        with pytest.raises(ValueError) as info:
            parse_expr(text)
        msg = str(info.value)
        assert "missing its ')'" not in msg, msg
        assert "type" in msg.lower(), msg

    def test_chained_casts_with_array_and_words(self) -> None:
        outer = parse_expr("code::INT[]::TEXT")
        assert isinstance(outer, Cast)
        assert isinstance(outer.source, Cast)


class TestLosslessRendering:
    async def test_postgres_keeps_precision_before_time_zone_words(self) -> None:
        sql = await emit(_dim("CAST(created_at AS TIMESTAMP(6) WITH TIME ZONE)"), dialect="postgres")
        types = _type_sql(sql, dialect="postgres")
        assert any(t in ("TIMESTAMPTZ(6)", "TIMESTAMP(6)WITHTIMEZONE") for t in types), sql

    async def test_trino_keeps_precision_before_time_zone_words(self) -> None:
        sql = await emit(_dim("CAST(created_at AS TIMESTAMP(6) WITH TIME ZONE)"), dialect="trino")
        assert "TIMESTAMP(6)WITHTIMEZONE" in _type_sql(sql, dialect="trino"), sql

    @pytest.mark.parametrize("dialect", ["postgres", "duckdb", "trino", "snowflake"])
    async def test_precision_after_time_zone_words_is_refused(self, dialect: str) -> None:
        await _refused("CAST(created_at AS TIMESTAMP WITH TIME ZONE(6))", dialect=dialect, needle="TIME ZONE")

    async def test_duckdb_refuses_a_zoned_precision_it_drops(self) -> None:
        await _refused("CAST(created_at AS TIMESTAMP(6) WITH TIME ZONE)", dialect="duckdb", needle="TIME ZONE")

    async def test_mysql_keeps_an_unsigned_decimal(self) -> None:
        sql = await emit(_dim("CAST(amount AS DECIMAL(10,2) UNSIGNED)"), dialect="mysql")
        assert "DECIMAL(10,2)UNSIGNED" in _type_sql(sql, dialect="mysql"), sql

    @pytest.mark.parametrize("dialect", [
        "postgres", "duckdb", "sqlite", "trino", "snowflake", "bigquery", "tsql", "clickhouse",
    ])
    async def test_unsigned_decimal_refused_without_unsigned_types(self, dialect: str) -> None:
        await _refused("CAST(amount AS DECIMAL(10,2) UNSIGNED)", dialect=dialect, needle="UNSIGNED")

    @pytest.mark.parametrize(("dialect", "type_text"), [
        (d, t) for d in TIER1 if d not in ("sqlite", "duckdb", "clickhouse") for t in ("HUGEINT", "INT128", "INT256")
    ] + [("duckdb", "INT256")])
    async def test_wide_integer_refused_where_the_database_lacks_it(self, dialect: str, type_text: str) -> None:
        await _refused(f"CAST(amount AS {type_text})", dialect=dialect, needle=type_text)

    @pytest.mark.parametrize(("dialect", "type_text"), [
        ("duckdb", "HUGEINT"), ("duckdb", "INT128"), ("clickhouse", "Int128"), ("clickhouse", "Int256"),
        ("sqlite", "INT128"), ("sqlite", "INT256"),
    ])
    async def test_wide_integer_accepted_where_the_database_has_it(self, dialect: str, type_text: str) -> None:
        await emit(_dim(f"CAST(amount AS {type_text})"), dialect=dialect)

    @pytest.mark.parametrize("dialect", ["postgres", "sqlite", "trino", "snowflake", "bigquery", "tsql"])
    async def test_unsigned_int_refused_without_unsigned_types(self, dialect: str) -> None:
        await _refused("CAST(amount AS INT UNSIGNED)", dialect=dialect, needle="UNSIGNED")

    @pytest.mark.parametrize("dialect", ["mysql", "clickhouse"])
    async def test_unsigned_int_accepted_where_the_dialect_has_one(self, dialect: str) -> None:
        await emit(_dim("CAST(amount AS INT UNSIGNED)"), dialect=dialect)

    @pytest.mark.parametrize(("type_text", "dialects"), [
        ("INT4", ["postgres", "duckdb"]),
        ("CHARACTER VARYING(10)", ["postgres", "mysql", "tsql", "trino", "snowflake"]),
        ("TIMESTAMP(3)", ["postgres", "trino", "snowflake"]),
    ])
    async def test_aliases_stay_accepted(self, type_text: str, dialects: list[str]) -> None:
        for dialect in dialects:
            await emit(_dim(f"CAST(code AS {type_text})"), dialect=dialect)

    @pytest.mark.parametrize("type_text", [
        "INT", "INTEGER", "BIGINT", "TEXT", "DOUBLE", "DOUBLE PRECISION", "DECIMAL(10,2)", "DATE", "TIMESTAMP",
    ])
    @pytest.mark.parametrize("dialect", TIER1)
    async def test_common_types_stay_accepted_everywhere(self, dialect: str, type_text: str) -> None:
        await emit(_dim(f"CAST(code AS {type_text})"), dialect=dialect)


class TestArrayCasts:
    @pytest.mark.parametrize(("dialect", "text"), [
        ("postgres", "CAST(code AS INT[])"), ("duckdb", "code::INT[]"), ("clickhouse", "CAST(code AS INT[])"),
        ("bigquery", "CAST(code AS INT[])"), ("trino", "CAST(code AS INT[])"),
    ])
    async def test_rendered_as_the_database_array_cast(self, dialect: str, text: str) -> None:
        sql = await emit(_dim(f"length({text})"), dialect=dialect)
        assert any(c.to.is_type(exp.DataType.Type.ARRAY) for c in _casts(sql, dialect=dialect)), sql

    @pytest.mark.parametrize("dialect", ["sqlite", "mysql", "tsql"])
    async def test_refused_without_array_types(self, dialect: str) -> None:
        await _refused("CAST(code AS INT[])", dialect=dialect, needle="INT[]")

    @pytest.mark.parametrize("dialect", ["postgres", "duckdb"])
    async def test_array_of_arrays_refused(self, dialect: str) -> None:
        query = _dim("CAST(code AS INT[][])")
        with pytest.raises(CastTypeError):
            await emit(query, dialect=dialect)

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_try_cast_to_an_array_refused_everywhere(self, dialect: str) -> None:
        query = _dim("TRY_CAST(code AS INT[])")
        with pytest.raises(CastTypeError):
            await emit(query, dialect=dialect)

    async def test_plain_cast_to_an_array_accepted_where_try_cast_is_not(self) -> None:
        await emit(_dim("length(CAST(code AS INT[]))"), dialect="postgres")
        query = _dim("TRY_CAST(code AS INT[])")
        with pytest.raises(CastTypeError):
            await emit(query, dialect="postgres")


class TestArrayTyping:
    @pytest.mark.parametrize(("text", "element"), [
        ("CAST(code AS INT[])", DataType.INT), ("code::BIGINT[]", DataType.INT), ("CAST(code AS TEXT[])", DataType.TEXT),
        ("CAST(code AS DOUBLE PRECISION[])", DataType.DOUBLE), ("CAST(code AS DATE[])", DataType.DATE),
        ("CAST(code AS UUID[])", DataType.UNKNOWN),
    ])
    def test_array_cast_is_a_list_of_its_element_type(self, text: str, element: DataType) -> None:
        assert _planned_type(text, dialect="postgres") is DataType.ARRAY
        assert _element_type(text, dialect="postgres") is element

    def test_contains_binds_as_a_list_call(self) -> None:
        key = _bind("contains(CAST(code AS INT[]), 3)", dialect="postgres")
        assert isinstance(key, ScalarCallKey)
        assert is_list_call(key, column_type=lambda _k: None)

    def test_scalar_cast_is_not_a_list(self) -> None:
        assert _planned_type("CAST(code AS INT)", dialect="postgres") is DataType.INT

    def test_unsigned_integer_is_an_integer(self) -> None:
        assert _planned_type("CAST(amount AS INT UNSIGNED)", dialect="mysql") is DataType.INT

    @pytest.mark.parametrize(("dialect", "type_text", "expected"), [
        ("duckdb", "UBIGINT", DataType.INT), ("duckdb", "UINTEGER", DataType.INT), ("clickhouse", "UInt32", DataType.INT),
        ("clickhouse", "Int16", DataType.INT), ("clickhouse", "Float32", DataType.DOUBLE),
    ])
    def test_vendor_type_names(self, dialect: str, type_text: str, expected: DataType) -> None:
        assert _planned_type(f"CAST(amount AS {type_text})", dialect=dialect) is expected

    def test_unsigned_decimal_is_a_double(self) -> None:
        assert _planned_type("CAST(amount AS DECIMAL(10,2) UNSIGNED)", dialect="mysql") is DataType.DOUBLE

    def test_zoned_timestamp_is_a_timestamp(self) -> None:
        assert _planned_type("CAST(created_at AS TIMESTAMP(6) WITH TIME ZONE)", dialect="postgres") \
            is DataType.TIMESTAMP


@pytest.fixture
async def tags_engine():
    async for engine in duckdb_tags_engine():
        yield engine


async def test_array_cast_executes_on_duckdb(tags_engine: SlayerQueryEngine) -> None:
    await check_array_cast(tags_engine)
