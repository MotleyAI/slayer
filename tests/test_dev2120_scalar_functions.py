"""The scalar library's rules: Postgres edge cases, out-of-domain NULL, version gating, exclusions (spec: queries/scalar-functions)."""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError, UnknownFunctionError
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.dialects import get_dialect
from slayer.sql.dialects.base import ServerProfile
from tests._dev2120_fixtures import (
    EXEC_BACKENDS,
    TIER1,
    emit_lib,
    lib_engine,
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
    return lib_values(await engine.execute(lib_query(expr, filters=[f"id = {row}"])))[row]


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
        assert await _value(engine, "left(s, -2)") == "ABC"

    async def test_non_ascii_characters(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(lib_query({"expression": "reverse(s)", "name": "v1"},
                                              {"expression": "ascii(s)", "name": "v2"},
                                              {"expression": "left(s, 1)", "name": "v3"}, filters=["id = 2"]))
        row = resp.data[0]
        assert (row["lib.v1"], int(row["lib.v2"]), row["lib.v3"]) == ("é€", 8364, "€")

    async def test_astral_code_point(self, engine: SlayerQueryEngine) -> None:
        assert await _value(engine, "chr(128512)") == "\U0001F600"

    async def test_split_part_index(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(lib_query({"expression": "split_part(s, '/', 2)", "name": "v1"},
                                              {"expression": "split_part(s, '/', -1)", "name": "v2"},
                                              {"expression": "split_part(s, '/', 9)", "name": "v3"},
                                              filters=["id = 3"]))
        row = resp.data[0]
        assert (row["lib.v1"], row["lib.v2"], row["lib.v3"]) == ("b", "c", "")

    async def test_lpad_truncates(self, engine: SlayerQueryEngine) -> None:
        values = lib_values(await engine.execute(lib_query("lpad(s, 3, '0')", filters=["id IN (1, 8)"])))
        assert values == {1: "ABC", 8: "0ab"}

    async def test_concat_ws_skips_nulls(self, engine: SlayerQueryEngine) -> None:
        assert await _value(engine, "concat_ws('-', 'x', p, 'z')", row=10) == "x-z"

    async def test_plain_text_containment(self, engine: SlayerQueryEngine) -> None:
        assert lib_ids(await engine.execute(lib_query(filters=["contains(tag, '5%')"]))) == {6}

    @pytest.mark.parametrize(("expr", "expected"), [
        ("div(7, 2)", 3), ("div(-7, 2)", -3), ("div(7, -2)", -3), ("div(-7, -2)", 3),
    ])
    async def test_div_truncates_toward_zero(self, engine: SlayerQueryEngine, expr: str, expected: int) -> None:
        assert int(await _value(engine, expr)) == expected

    async def test_cbrt_of_a_negative_number(self, engine: SlayerQueryEngine) -> None:
        assert float(await _value(engine, "cbrt(x)", row=6)) == pytest.approx(-2.0)

    async def test_division_by_zero_column(self, engine: SlayerQueryEngine) -> None:
        assert await _value(engine, "div(i, m)", row=3) is None

    async def test_asin_column_out_of_domain(self, engine: SlayerQueryEngine) -> None:
        values = lib_values(await engine.execute(lib_query("asin(x)", filters=["id IN (1, 2)"])))
        assert values[2] is None
        assert float(values[1]) == pytest.approx(0.5235987755982989)

    async def test_greatest_ignores_nulls_in_any_position(self, engine: SlayerQueryEngine) -> None:
        values = lib_values(await engine.execute(lib_query("greatest(a, b, c)", filters=["id IN (1, 2, 3, 4)"])))
        assert {k: (None if v is None else int(v)) for k, v in values.items()} == {1: 2, 2: 3, 3: 2, 4: None}


class TestLiteralOutOfDomain:
    @pytest.mark.parametrize(("expr", "function", "argument"), [
        ("asin(2)", "asin", "2"),
        ("asin(-1.5)", "asin", "-1.5"),
        ("acos(2)", "acos", "2"),
        ("acos(-1.5)", "acos", "-1.5"),
        ("div(i, 0)", "div", "0"),
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
        assert await _value(engine, f"split_part(s, '', {index})", row=3) == expected

    @pytest.mark.parametrize(("index", "expected"), [(2, "b"), (-1, "c")])
    async def test_space_delimiter_is_not_empty(self, engine: SlayerQueryEngine, index: int, expected: str) -> None:
        assert await _value(engine, f"split_part('a b c', ' ', {index})") == expected

    async def test_tsql_space_has_length_one(self) -> None:
        sql = await emit_lib(lib_query("split_part(word, ' ', -1)"), dialect="tsql")
        assert "LEN(CAST(' ' AS NVARCHAR(MAX)) + 'x') - 1" in " ".join(sql.split()), sql

    @pytest.mark.parametrize("dialect", ["tsql", "mysql"])
    async def test_field_count_never_divides_by_zero(self, dialect: str) -> None:
        sql = await emit_lib(lib_query("split_part(word, tag, -1)"), dialect=dialect)
        assert "NULLIF(" in sql.upper(), sql


class TestSplitPartIndex:
    @pytest.mark.parametrize("expr", [
        "split_part(s, '/', n)", "split_part(s, '/', 0)", "split_part(s, '/', 1.5)", "split_part(s, '/', 1 + 1)",
    ])
    async def test_non_literal_or_zero_index_rejected(self, engine: SlayerQueryEngine, expr: str) -> None:
        await _rejected(engine, expr, "split_part")


class TestVersionGating:
    def test_sql_server_profile_probe(self) -> None:
        probe = get_dialect("tsql").server_profile_sql()
        assert probe is not None
        assert "SERVERPROPERTY('ProductMajorVersion')" in probe

    @pytest.mark.parametrize(("row", "version"), [
        (("16",), (16, 0)), (("17",), (17, 0)), ((15,), (15, 0)), (("garbage",), None), ((None,), None),
    ])
    def test_sql_server_profile_parse(self, row: tuple, version) -> None:
        assert get_dialect("tsql").parse_server_profile(base_row=row).version == version

    def test_sql_server_profile_parse_tolerates_no_row(self) -> None:
        assert get_dialect("tsql").parse_server_profile(base_row=None) == ServerProfile()

    @pytest.mark.parametrize("function", ["greatest", "least"])
    async def test_old_sql_server_fails_closed(self, monkeypatch, function: str) -> None:
        patch_profile(monkeypatch, version=(15, 0))
        query = lib_query(f"{function}(a, b)")
        with pytest.raises(SlayerError) as info:
            await emit_lib(query, dialect="tsql")
        msg = str(info.value)
        assert function in msg.lower(), msg
        assert "2022" in msg, msg

    @pytest.mark.parametrize("version", [(16, 0), (17, 0), None])
    async def test_supported_or_unknown_version_emits(self, monkeypatch, version) -> None:
        patch_profile(monkeypatch, version=version)
        sql = await emit_lib(lib_query("greatest(a, b)"), dialect="tsql")
        assert "GREATEST(" in sql.upper(), sql

    async def test_failed_probe_emits(self, monkeypatch) -> None:
        patch_profile(monkeypatch, fail=True)
        await emit_lib(lib_query("least(a, b)"), dialect="tsql")

    async def test_profile_probed_only_when_rendering_reads_it(self, monkeypatch) -> None:
        calls = patch_profile(monkeypatch, version=(16, 0))
        await emit_lib(lib_query("abs(a)"), dialect="tsql")
        assert calls == []
        await emit_lib(lib_query("greatest(a, b)"), dialect="tsql")
        assert calls == [1]


class TestGreatestLeastLowering:
    @pytest.mark.parametrize("dialect", ["sqlite", "mysql", "bigquery"])
    async def test_null_propagating_engines_coalesce(self, dialect: str) -> None:
        sql = await emit_lib(lib_query("greatest(a, b, c)"), dialect=dialect)
        assert "COALESCE(" in sql.upper(), sql

    async def test_snowflake_ignores_nulls_natively(self) -> None:
        sql = await emit_lib(lib_query("greatest(a, b, c)"), dialect="snowflake")
        assert "GREATEST_IGNORE_NULLS(" in sql.upper(), sql


class TestLogLowering:
    @pytest.mark.parametrize("dialect", TIER1)
    async def test_one_argument_log_is_base_ten(self, dialect: str) -> None:
        sql = " ".join((await emit_lib(lib_query("log(x)"), dialect=dialect)).upper().split())
        assert "LOG10(" in sql or "LOG(10, " in sql, sql
        assert "LN(" not in sql, sql

    async def test_bigquery_log2_takes_the_base_argument(self) -> None:
        sql = (await emit_lib(lib_query("log2(x)"), dialect="bigquery")).upper()
        assert "LOG2(" not in sql, sql
        assert "LOG(LIB.X, 2)" in sql, sql


class TestTrimLowering:
    @pytest.mark.parametrize("dialect", ["bigquery", "trino"])
    @pytest.mark.parametrize("fn", ["trim", "ltrim", "rtrim"])
    async def test_whitespace_trimming_engines_trim_spaces_only(self, dialect: str, fn: str) -> None:
        sql = await emit_lib(lib_query(f"{fn}(code)"), dialect=dialect)
        assert f"{fn.upper()}(LIB.CODE, ' ')" in sql.upper(), sql


_TEXT_MATCHES = ["starts_with(word, 'ad')", "ends_with(word, 'ad')", "contains(word, 'ad')", "strpos(word, 'ad')",
                 "instr(word, 'ad')", "POSITION('ad' IN word)"]


class TestTextMatchingIgnoresCollation:
    @pytest.mark.parametrize(("dialect", "marker"), [("mysql", "COLLATE UTF8MB4_BIN"),
                                                     ("tsql", "COLLATE LATIN1_GENERAL_100_BIN2")])
    @pytest.mark.parametrize("expr", _TEXT_MATCHES)
    async def test_insensitive_default_engines_compare_binary(self, dialect: str, marker: str, expr: str) -> None:
        sql = (await emit_lib(lib_query(expr), dialect=dialect)).upper()
        assert sql.count(marker) >= 2, sql

    @pytest.mark.parametrize("expr", ["replace(word, 'a', 'x')", "split_part(word, 'a', 2)", "split_part(word, 'a', -1)"])
    async def test_tsql_replace_and_split_part_compare_binary(self, expr: str) -> None:
        sql = (await emit_lib(lib_query(expr), dialect="tsql")).upper()
        assert sql.count("COLLATE LATIN1_GENERAL_100_BIN2") >= 2, sql

    async def test_tsql_collates_a_compound_operand_whole(self) -> None:
        sql = await emit_lib(lib_query("contains(word || 'x', 'ad')"), dialect="tsql")
        assert "( lib.word + 'x' ) COLLATE" in " ".join(sql.split()), sql

    @pytest.mark.parametrize("dialect", [d for d in TIER1 if d not in ("mysql", "tsql")])
    async def test_other_engines_compare_as_written(self, dialect: str) -> None:
        sql = (await emit_lib(lib_query("starts_with(word, 'ad')"), dialect=dialect)).upper()
        assert "COLLATE" not in sql, sql


class TestExcludedFunctions:
    @pytest.mark.parametrize("expr", [
        "initcap(word)", "random()", "to_date(word, 'YYYY')", "strftime(word, '%Y')",
        "json_object('a', word)", "json_array(word)", "json_each(payload)",
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
        values = lib_values(await engine.execute(lib_query("div(i, m)", filters=["id = 1"])))
        assert int(values[1]) == 3
