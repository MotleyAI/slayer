"""The scalar library's rules: Postgres edge cases, out-of-domain NULL, version gating, exclusions (spec: queries/scalar-functions)."""

from __future__ import annotations

import pytest
from sqlglot import exp, parse_one

from slayer.core.enums import DataType
from slayer.core.errors import SlayerError, UnknownFunctionError, UnsupportedServerFunctionError
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.models import DatasetField
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.dialects import get_dialect
from slayer.sql.dialects.base import ServerProfile
from tests._dev2120_fixtures import (
    EXEC_BACKENDS,
    TIER1,
    emit_lib,
    lib_engine,
    lib_dataset,
    lib_ids,
    lib_query,
    lib_values,
    patch_profile,
)


@pytest.fixture(params=EXEC_BACKENDS)
async def engine(request):
    async with lib_engine(request.param) as e:
        yield e


async def _value(engine: SlayerQueryEngine, expr: str, *, row: int = 1):
    return lib_values(await engine.execute(lib_query(expr, filter=[f"lib.id = {row}"])))[row]


async def _rejected(engine: SlayerQueryEngine, expr: str, *needles: str) -> str:
    query = lib_query(expr)
    with pytest.raises(SlayerError) as info:
        await engine.execute(query)
    msg = str(info.value)
    for needle in needles:
        assert needle.lower() in msg.lower(), msg
    return msg


class TestPostgresMeaning:
    async def test_negative_left(self, engine: SlayerQueryEngine) -> None:
        assert await _value(engine, "left(lib.s, -2)") == "ABC"

    async def test_non_ascii_characters(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(lib_query({"field": "reverse(lib.s)", "alias": "v1"},
                                              {"field": "ascii(lib.s)", "alias": "v2"},
                                              {"field": "left(lib.s, 1)", "alias": "v3"}, filter=["lib.id = 2"]))
        row = resp.data[0]
        assert (row["v1"], int(row["v2"]), row["v3"]) == ("é€", 8364, "€")

    async def test_astral_code_point(self, engine: SlayerQueryEngine) -> None:
        assert await _value(engine, "chr(128512)") == "\U0001F600"

    async def test_split_part_index(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(lib_query({"field": "split_part(lib.s, '/', 2)", "alias": "v1"},
                                              {"field": "split_part(lib.s, '/', -1)", "alias": "v2"},
                                              {"field": "split_part(lib.s, '/', 9)", "alias": "v3"},
                                              filter=["lib.id = 3"]))
        row = resp.data[0]
        assert (row["v1"], row["v2"], row["v3"]) == ("b", "c", "")

    async def test_lpad_truncates(self, engine: SlayerQueryEngine) -> None:
        values = lib_values(await engine.execute(lib_query("lpad(lib.s, 3, '0')", filter=["lib.id IN (1, 8)"])))
        assert values == {1: "ABC", 8: "0ab"}

    async def test_concat_ws_skips_nulls(self, engine: SlayerQueryEngine) -> None:
        assert await _value(engine, "concat_ws('-', 'x', lib.p, 'z')", row=10) == "x-z"

    async def test_plain_text_containment(self, engine: SlayerQueryEngine) -> None:
        assert lib_ids(await engine.execute(lib_query(filter=["contains(lib.tag, '5%')"]))) == {6}

    @pytest.mark.parametrize(("expr", "expected"), [
        ("div(7, 2)", 3), ("div(-7, 2)", -3), ("div(7, -2)", -3), ("div(-7, -2)", 3),
    ])
    async def test_div_truncates_toward_zero(self, engine: SlayerQueryEngine, expr: str, expected: int) -> None:
        assert int(await _value(engine, expr)) == expected

    async def test_cbrt_of_a_perfect_cube_is_exact(self, engine: SlayerQueryEngine) -> None:
        assert float(await _value(engine, "cbrt(64)")) == 4.0

    async def test_cbrt_of_a_negative_number(self, engine: SlayerQueryEngine) -> None:
        assert float(await _value(engine, "cbrt(lib.x)", row=6)) == pytest.approx(-2.0)

    async def test_division_by_zero_column(self, engine: SlayerQueryEngine) -> None:
        assert await _value(engine, "div(lib.i, lib.m)", row=3) is None

    async def test_asin_column_out_of_domain(self, engine: SlayerQueryEngine) -> None:
        values = lib_values(await engine.execute(lib_query("asin(lib.x)", filter=["lib.id IN (1, 2)"])))
        assert values[2] is None
        assert float(values[1]) == pytest.approx(0.5235987755982989)

    async def test_greatest_ignores_nulls_in_any_position(self, engine: SlayerQueryEngine) -> None:
        values = lib_values(await engine.execute(lib_query("greatest(lib.a, lib.b, lib.c)", filter=["lib.id IN (1, 2, 3, 4)"])))
        assert {k: (None if v is None else int(v)) for k, v in values.items()} == {1: 2, 2: 3, 3: 2, 4: None}


class TestLiteralOutOfDomain:
    @pytest.mark.parametrize(("expr", "function", "argument"), [
        ("asin(2)", "asin", "2"),
        ("asin(-1.5)", "asin", "-1.5"),
        ("acos(2)", "acos", "2"),
        ("acos(-1.5)", "acos", "-1.5"),
        ("div(lib.i, 0)", "div", "0"),
        ("div(7, 0)", "div", "0"),
        ("chr(0)", "chr", "0"),
        ("chr(55296)", "chr", "55296"),
        ("chr(57343)", "chr", "57343"),
        ("chr(1114112)", "chr", "1114112"),
    ])
    async def test_rejected_at_plan_time(self, engine: SlayerQueryEngine, expr: str, function: str,
                                         argument: str) -> None:
        await _rejected(engine, expr, function, argument)

    @pytest.mark.parametrize("expr", ["asin(1)", "asin(-1)", "acos(1)", "acos(-1)", "chr(1114111)", "div(0, 3)"])
    async def test_boundary_literal_accepted(self, engine: SlayerQueryEngine, expr: str) -> None:
        await engine.execute(lib_query(expr))

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_rejected_before_emission(self, dialect: str) -> None:
        query = lib_query("asin(2)")
        with pytest.raises(SlayerError, match="asin"):
            await emit_lib(query, dialect=dialect)


class TestSplitPartEmptyDelimiter:
    @pytest.mark.parametrize(("index", "expected"), [(1, "a/b/c"), (2, ""), (-1, "a/b/c"), (-2, "")])
    async def test_whole_value_is_the_one_field(self, engine: SlayerQueryEngine, index: int, expected: str) -> None:
        assert await _value(engine, f"split_part(lib.s, '', {index})", row=3) == expected

    @pytest.mark.parametrize(("index", "expected"), [(2, "b"), (-1, "c")])
    async def test_space_delimiter_is_not_empty(self, engine: SlayerQueryEngine, index: int, expected: str) -> None:
        assert await _value(engine, f"split_part('a b c', ' ', {index})") == expected

    async def test_tsql_space_has_length_one(self) -> None:
        sql = await emit_lib(lib_query("split_part(lib.word, ' ', -1)"), dialect="tsql")
        assert "LEN(( CAST(' ' AS NVARCHAR(MAX)) + 'x' ) COLLATE Latin1_General_100_CS_AS_SC) - 1" in " ".join(sql.split()), sql

    @pytest.mark.parametrize("dialect", ["tsql", "mysql"])
    async def test_field_count_never_divides_by_zero(self, dialect: str) -> None:
        sql = await emit_lib(lib_query("split_part(lib.word, lib.tag, -1)"), dialect=dialect)
        assert "NULLIF(" in sql.upper(), sql


class TestSplitPartIndex:
    @pytest.mark.parametrize("expr", [
        "split_part(lib.s, '/', lib.n)", "split_part(lib.s, '/', 0)", "split_part(lib.s, '/', 1.5)", "split_part(lib.s, '/', 1 + 1)",
    ])
    async def test_non_literal_or_zero_index_rejected(self, engine: SlayerQueryEngine, expr: str) -> None:
        await _rejected(engine, expr, "split_part")


class TestVersionGating:
    def test_sql_server_profile_probe(self) -> None:
        probe = get_dialect("tsql").server_profile_sql()
        assert probe is not None
        assert "SERVERPROPERTY('ProductMajorVersion')" in probe

    @pytest.mark.parametrize(("row", "version"), [
        (("16",), (16, 0, 0)), (("17",), (17, 0, 0)), ((15,), (15, 0, 0)), (("garbage",), None), ((None,), None),
    ])
    def test_sql_server_profile_parse(self, row: tuple, version) -> None:
        assert get_dialect("tsql").parse_server_profile(base_row=row).version == version

    def test_sql_server_profile_parse_tolerates_no_row(self) -> None:
        assert get_dialect("tsql").parse_server_profile(base_row=None) == ServerProfile()

    @pytest.mark.parametrize("function", ["greatest", "least"])
    async def test_old_sql_server_fails_closed(self, monkeypatch: pytest.MonkeyPatch, function: str) -> None:
        patch_profile(monkeypatch, version=(15, 0))
        query = lib_query(f"{function}(lib.a, lib.b)")
        with pytest.raises(SlayerError) as info:
            await emit_lib(query, dialect="tsql")
        msg = str(info.value)
        assert function in msg.lower(), msg
        assert "2022" in msg, msg

    @pytest.mark.parametrize("version", [(16, 0), (17, 0), None])
    async def test_supported_or_unknown_version_emits(self, monkeypatch: pytest.MonkeyPatch, version) -> None:
        patch_profile(monkeypatch, version=version)
        sql = await emit_lib(lib_query("greatest(lib.a, lib.b)"), dialect="tsql")
        assert "GREATEST(" in sql.upper(), sql

    async def test_failed_probe_emits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        patch_profile(monkeypatch, fail=True)
        await emit_lib(lib_query("least(lib.a, lib.b)"), dialect="tsql")

    async def test_profile_probed_only_when_rendering_reads_it(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls = patch_profile(monkeypatch, version=(16, 0))
        await emit_lib(lib_query("abs(lib.a)"), dialect="tsql")
        assert calls == []
        await emit_lib(lib_query("greatest(lib.a, lib.b)"), dialect="tsql")
        assert calls == [1]


class TestGreatestLeastLowering:
    @pytest.mark.parametrize("dialect", ["sqlite", "mysql", "bigquery"])
    async def test_null_propagating_engines_coalesce(self, dialect: str) -> None:
        sql = await emit_lib(lib_query("greatest(lib.a, lib.b, lib.c)"), dialect=dialect)
        assert "COALESCE(" in sql.upper(), sql

    async def test_snowflake_ignores_nulls_natively(self) -> None:
        sql = await emit_lib(lib_query("greatest(lib.a, lib.b, lib.c)"), dialect="snowflake")
        assert "GREATEST_IGNORE_NULLS(" in sql.upper(), sql


class TestLogLowering:
    @pytest.mark.parametrize("dialect", TIER1)
    async def test_one_argument_log_is_base_ten(self, dialect: str) -> None:
        sql = " ".join((await emit_lib(lib_query("log(lib.x)"), dialect=dialect)).upper().split())
        assert "LOG10(" in sql or "LOG(10, " in sql, sql
        assert "LN(" not in sql, sql

    async def test_bigquery_log2_takes_the_base_argument(self) -> None:
        sql = (await emit_lib(lib_query("log2(lib.x)"), dialect="bigquery")).upper()
        assert "LOG2(" not in sql, sql
        assert "LOG(CASE WHEN LIB.X > 0 THEN LIB.X END, 2)" in sql, sql


class TestTrimLowering:
    @pytest.mark.parametrize("dialect", ["bigquery", "trino"])
    @pytest.mark.parametrize("fn", ["trim", "ltrim", "rtrim"])
    async def test_whitespace_trimming_engines_trim_spaces_only(self, dialect: str, fn: str) -> None:
        sql = await emit_lib(lib_query(f"{fn}(lib.code)"), dialect=dialect)
        assert f"{fn.upper()}(LIB.CODE, ' ')" in sql.upper(), sql


_TEXT_MATCHES = ["starts_with(lib.word, 'ad')", "ends_with(lib.word, 'ad')", "contains(lib.word, 'ad')", "strpos(lib.word, 'ad')",
                 "instr(lib.word, 'ad')", "POSITION('ad' IN lib.word)"]


class TestTextMatchingIgnoresCollation:
    @pytest.mark.parametrize(("dialect", "marker"), [("mysql", "COLLATE UTF8MB4_BIN"),
                                                     ("tsql", "COLLATE LATIN1_GENERAL_100_BIN2")])
    @pytest.mark.parametrize("expr", _TEXT_MATCHES)
    async def test_insensitive_default_engines_compare_binary(self, dialect: str, marker: str, expr: str) -> None:
        sql = (await emit_lib(lib_query(expr), dialect=dialect)).upper()
        assert sql.count(marker) >= 2, sql

    @pytest.mark.parametrize("expr", ["replace(lib.word, 'a', 'x')", "split_part(lib.word, 'a', 2)", "split_part(lib.word, 'a', -1)"])
    async def test_tsql_replace_and_split_part_compare_binary(self, expr: str) -> None:
        sql = (await emit_lib(lib_query(expr), dialect="tsql")).upper()
        assert sql.count("COLLATE LATIN1_GENERAL_100_BIN2") >= 2, sql

    async def test_tsql_collates_a_compound_operand_whole(self) -> None:
        sql = await emit_lib(lib_query("contains(lib.word || 'x', 'ad')"), dialect="tsql")
        assert "( lib.word + 'x' ) COLLATE" in " ".join(sql.split()), sql

    @pytest.mark.parametrize("dialect", [d for d in TIER1 if d not in ("mysql", "tsql")])
    async def test_other_engines_compare_as_written(self, dialect: str) -> None:
        sql = (await emit_lib(lib_query("starts_with(lib.word, 'ad')"), dialect=dialect)).upper()
        assert "COLLATE" not in sql, sql


class TestExcludedFunctions:
    @pytest.mark.parametrize("expr", [
        "initcap(lib.word)", "random()", "to_date(lib.word, 'YYYY')", "strftime(lib.word, '%Y')",
        "json_object('a', lib.word)", "json_array(lib.word)", "json_each(lib.payload)",
    ])
    async def test_unknown_function(self, engine: SlayerQueryEngine, expr: str) -> None:
        query = lib_query(expr)
        with pytest.raises(UnknownFunctionError):
            await engine.execute(query)


class TestFloorDivisionHint:
    async def test_hint_names_a_working_function(self, engine: SlayerQueryEngine) -> None:
        query = lib_query("i // m")
        with pytest.raises(ValueError, match=r"div\(a, b\)"):
            await engine.execute(query)
        values = lib_values(await engine.execute(lib_query("div(lib.i, lib.m)", filter=["lib.id = 1"])))
        assert int(values[1]) == 3


class TestMySQLDoublePrecision:
    async def test_pi_is_not_display_rounded(self) -> None:
        sql = (await emit_lib(lib_query("pi()"), dialect="mysql")).upper()
        assert "ACOS(-1)" in sql, sql
        assert "PI()" not in sql, sql

    async def test_cube_root_exponent_is_double(self) -> None:
        sql = (await emit_lib(lib_query("cbrt(lib.x)"), dialect="mysql")).upper()
        assert "1 / 3E0" in sql, sql


async def _tsql(expr: str) -> str:
    return " ".join((await emit_lib(lib_query(expr), dialect="tsql")).split())


class TestSQLServerCodePoints:
    @pytest.mark.parametrize("expr", ["length(lib.s)", "substr(lib.s, 2)", "reverse(lib.s)", "lpad(lib.s, 4, '*')"])
    async def test_counting_is_supplementary_aware(self, expr: str) -> None:
        assert "COLLATE Latin1_General_100_CS_AS_SC" in await _tsql(expr)

    async def test_position_recounts_units_as_characters(self) -> None:
        sql = await _tsql("strpos(lib.s, lib.p)")
        assert "LEN( ( CAST(LEFT( lib.s COLLATE Latin1_General_100_BIN2" in sql, sql
        assert "WHEN DATALENGTH(lib.p) = 0 THEN CASE WHEN NOT lib.s IS NULL THEN 1 END" in sql, sql

    async def test_runtime_negated_length_is_kept(self) -> None:
        sql = await _tsql("left(lib.s, -lib.n)")
        lengths = [s.args["length"] for s in parse_one(sql, read="tsql").find_all(exp.Substring)]
        assert any(isinstance(n, exp.Neg) and isinstance(n.this, exp.Column) for n in lengths), sql

    async def test_author_sql_is_not_rewritten(self) -> None:
        base = lib_dataset()
        model = base.model_copy(update={"fields": [*base.fields, DatasetField(
            name="head", expression=ExpressionObject(dialects=[DialectEntry(dialect="TSQL", expression="SUBSTRING(s, 1, 2) + CAST(ROUND(POWER(a, 2), 0) AS NVARCHAR(9))")]), datatype=DataType.STRING)]})
        sql = " ".join((await emit_lib(lib_query("lib.head"), dialect="tsql", datasets=[model])).split())
        assert "SUBSTRING(lib.s, 1, 2)" in sql, sql
        assert "POWER(lib.a, 2)" in sql, sql
        assert "COLLATE" not in sql, sql
        assert "FLOAT" not in sql, sql

    @pytest.mark.parametrize("expr", ["left(lib.s, -2)", "right(lib.s, -1)"])
    async def test_no_negative_literal_substring_length(self, expr: str) -> None:
        sql = await _tsql(expr)
        lengths = [s.args["length"] for s in parse_one(sql, read="tsql").find_all(exp.Substring)]
        assert lengths, sql
        assert not any(isinstance(n, exp.Neg) for n in lengths), sql


class TestSQLServerNumbers:
    async def test_round_has_digits(self) -> None:
        assert "ROUND(lib.x + 0.1, 0)" in await _tsql("round(lib.x + 0.1)")

    async def test_power_base_is_float(self) -> None:
        assert "POWER(CAST(2 AS FLOAT), lib.a)" in await _tsql("power(2, lib.a)")

    async def test_cube_root_exponent_is_float(self) -> None:
        assert "/ 3E0)" in await _tsql("cbrt(lib.x)")


class TestSQLServerQuirks:
    async def test_split_part_before_the_first_field_is_empty(self) -> None:
        sql = await _tsql("split_part(lib.s, '/', -4)")
        assert "WHEN 4 >" in sql, sql
        assert "THEN ''" in sql, sql

    async def test_replacement_ampersand_is_literal(self) -> None:
        sql = await _tsql("regexp_replace(lib.tag, 'USD', '$ & co')")
        assert "'$ & co'" in sql, sql
        assert "\\&" not in sql, sql

    async def test_returning_timestamp_is_datetime2(self) -> None:
        sql = await _tsql("json_value(lib.payload, '$.t' RETURNING TIMESTAMP)")
        assert "AS DATETIME2)" in sql, sql
        assert "ROWVERSION" not in sql, sql


async def _clickhouse(expr: str) -> str:
    return " ".join((await emit_lib(lib_query(expr), dialect="clickhouse")).split())


class TestClickHouseLowering:
    async def test_cbrt_is_lowercase(self) -> None:
        assert "cbrt(lib.x)" in await _clickhouse("cbrt(lib.x)")

    async def test_ln_reads_the_exact_log2(self) -> None:
        sql = await _clickhouse("ln(lib.x)")
        assert "log2(CASE WHEN lib.x > 0 THEN lib.x END) * 0.6931471805599453" in sql, sql
        assert "LN(" not in sql.upper(), sql

    @pytest.mark.parametrize(("expr", "expected"), [
        ("log(lib.x, 8)", "log2(8) / log2(CASE WHEN lib.x > 0 AND lib.x <> 1 THEN lib.x END)"),
        ("log(3, lib.x)", "log2(CASE WHEN lib.x > 0 THEN lib.x END) / log2(3)"),
    ])
    async def test_based_log_is_a_ratio_of_log2(self, expr: str, expected: str) -> None:
        sql = await _clickhouse(expr)
        assert expected in sql, sql
        assert "LOG(" not in sql.upper(), sql

    async def test_base_two_log_stays_native(self) -> None:
        assert "log2(CASE WHEN lib.x > 0 THEN lib.x END)" in await _clickhouse("log(2, lib.x)")

    async def test_negative_right_keeps_a_length(self) -> None:
        sql = await _clickhouse("right(lib.s, -1)")
        assert "rightUTF8(lib.s, CASE WHEN -1 < 0 THEN greatest(lengthUTF8(lib.s) + -1, 0) ELSE -1 END)" in sql, sql

    async def test_reverse_splits_by_code_point(self) -> None:
        sql = await _clickhouse("reverse(lib.s)")
        assert "arrayReverse(extractAll(assumeNotNull(lib.s), '(?s).'))" in sql, sql
        assert "reverseUTF8" not in sql, sql

    async def test_split_part_never_splits_a_nullable(self) -> None:
        sql = await _clickhouse("split_part(lib.s, '/', 2)")
        assert "splitByString(assumeNotNull('/'), assumeNotNull(lib.s))" in sql, sql
        assert "lib.s IS NULL OR '/' IS NULL" in sql, sql


class TestMariaDBRefusesWhatItCannotMean:
    @pytest.mark.parametrize("expr", [
        "regexp_like(lib.word, '^a')", "lib.word ~ '^a'", "regexp_extract(lib.word, 'a(d)', 1)", "regexp_replace(lib.word, 'a', 'b')",
        "TRY_CAST(lib.num_txt AS INT)", "lib.num_txt > 100", "json_value(lib.payload, '$.ok')", "lib.payload ->> 'ok'",
    ])
    async def test_refused_at_plan_time(self, expr: str) -> None:
        query = lib_query(expr)
        with pytest.raises(UnsupportedServerFunctionError, match="MariaDB"):
            await emit_lib(query, dialect="mariadb")

    @pytest.mark.parametrize("expr", ["starts_with(lib.word, 'a')", "TRY_CAST(lib.word AS BOOLEAN)", "upper(lib.word)"])
    async def test_the_rest_renders_as_mysql(self, expr: str) -> None:
        assert await emit_lib(lib_query(expr), dialect="mariadb") == await emit_lib(lib_query(expr), dialect="mysql")
