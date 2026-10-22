"""Regex functions, operators, the portable pattern subset and replacements (spec: queries/regex)."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.errors import SlayerError
from slayer.core.syntax import parse_expr
from slayer.engine.query_engine import SlayerQueryEngine
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


def same(a: str, b: str) -> None:
    assert parse_expr(a) == parse_expr(b), (a, b)


@pytest.fixture(params=EXEC_BACKENDS)
async def engine(request):
    async with lib_engine(request.param) as e:
        yield e


async def _rejected(engine: SlayerQueryEngine, *needles: str, any_of: tuple[str, ...] = (), **kw: Any) -> str:
    with pytest.raises(SlayerError) as info:
        await engine.execute(lib_query(**kw))
    msg = str(info.value)
    for needle in needles:
        assert needle.lower() in msg.lower(), msg
    if any_of:
        assert any(n.lower() in msg.lower() for n in any_of), msg
    return msg


class TestOperators:
    @pytest.mark.parametrize(("op", "call"), [
        ("name ~ '^ad'", "regexp_like(name, '^ad')"),
        ("name ~* '^ad'", "regexp_like(name, '^ad', 'i')"),
        ("name !~ '^ad'", "NOT regexp_like(name, '^ad')"),
        ("name !~* '^ad'", "NOT regexp_like(name, '^ad', 'i')"),
    ])
    def test_operator_is_the_function(self, op: str, call: str) -> None:
        same(op, call)

    @pytest.mark.parametrize(("text", "explicit"), [
        ("a ~ 'x' AND b ~ 'y'", "(a ~ 'x') AND (b ~ 'y')"),
        ("NOT a ~ 'x'", "NOT (a ~ 'x')"),
        ("a || 'b' ~ 'c'", "(a || 'b') ~ 'c'"),
    ])
    def test_binds_like_a_comparison(self, text: str, explicit: str) -> None:
        same(text, explicit)

    async def test_operator_keeps_the_function_rows(self, engine: SlayerQueryEngine) -> None:
        op = lib_ids(await engine.execute(lib_query(filters=["word ~* '^ad'"])))
        fn = lib_ids(await engine.execute(lib_query(filters=["regexp_like(word, '^ad', 'i')"])))
        assert op == fn == {3, 4, 8, 9}


class TestPatternSource:
    async def test_variable_pattern(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(lib_query(filters=["regexp_like(s, {p})"], variables={"p": "^a.c$"}))
        assert lib_ids(resp) == {9}

    @pytest.mark.parametrize("pattern", ["prefix", "'^' || prefix", "lower(prefix)"])
    async def test_non_literal_pattern_rejected(self, engine: SlayerQueryEngine, pattern: str) -> None:
        await _rejected(engine, "pattern", filters=[f"regexp_like(word, {pattern})"])

    async def test_non_literal_replacement_rejected(self, engine: SlayerQueryEngine) -> None:
        await _rejected(engine, "replacement", dimensions=["id", {"expression": "regexp_replace(word, 'a', p)",
                                                                  "name": "v"}])


UNSUPPORTED = [
    ("(?=a)b", ("lookahead", "(?=")),
    ("(?!a)b", ("lookahead", "(?!")),
    ("(?<=a)b", ("lookbehind", "(?<=")),
    ("(a)\\1", ("backreference", "\\1")),
    ("a+?", ("lazy", "+?")),
    ("a*?", ("lazy", "*?")),
    ("a{1,2}?", ("lazy", "?")),
    ("(?i)a", ("(?",)),
    ("(?:a)", ("(?",)),
    ("\\bword", ("\\b",)),
    ("\\Bword", ("\\B",)),
    ("\\p{L}", ("\\p",)),
    ("[[:alpha:]]", ("[:alpha:]", "POSIX")),
    ("\\Aab", ("\\A",)),
    ("ab\\z", ("\\z",)),
    ("\\x41", ("\\x",)),
    ("\\u0041", ("\\u",)),
    ("a\\", ("\\",)),
    ("(ab", ("(", "parenthes", "group")),
    ("[ab", ("[", "bracket")),
]


class TestPortableSubset:
    @pytest.mark.parametrize(("pattern", "names"), UNSUPPORTED, ids=[p for p, _ in UNSUPPORTED])
    async def test_unsupported_construct_named(self, engine: SlayerQueryEngine, pattern: str,
                                               names: tuple[str, ...]) -> None:
        await _rejected(engine, any_of=names, filters=[f"regexp_like(word, '{pattern}')"])

    async def test_unsupported_construct_in_a_variable_names_it(self, engine: SlayerQueryEngine) -> None:
        msg = await _rejected(engine, "\\b", filters=["regexp_like(word, {p})"], variables={"p": "\\bword"})
        assert "'p'" in msg or "{p}" in msg or "variable p" in msg.lower(), msg

    @pytest.mark.parametrize("pattern", [
        "a\\tb", "a\\rb", "a\\nb", "x{2}", "x{2,}", "x{2,3}", "[^a-z]", "[a-c0-9_]", "\\.", "\\$", "\\^", "\\|",
        "\\\\", "\\{", "\\}", "\\(", "\\)", "\\[", "\\]", "\\*", "\\+", "\\?", "\\D", "\\W", "\\S", "a|b|c",
        "(a|b)+", "^$", "[\\s\\d]",
    ])
    async def test_subset_construct_accepted(self, engine: SlayerQueryEngine, pattern: str) -> None:
        await engine.execute(lib_query(filters=[f"regexp_like(word, '{pattern}')"]))

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_classes_emit_as_ascii(self, dialect: str) -> None:
        sql = await emit_lib(lib_query(filters=["regexp_like(word, '^\\d+$')"]), dialect=dialect)
        assert "0-9" in sql, sql
        assert "\\d" not in sql, sql


class TestFunctionArguments:
    @pytest.mark.parametrize("expr", [
        "regexp_extract(tag, '@(.+)$', 2)", "regexp_extract(tag, '@(.+)$', -1)",
        "regexp_extract(tag, '@(.+)$', n)", "regexp_extract(tag, '@.+$', 1)",
    ])
    async def test_bad_group_rejected(self, engine: SlayerQueryEngine, expr: str) -> None:
        await _rejected(engine, "group", dimensions=["id", {"expression": expr, "name": "v"}])

    @pytest.mark.parametrize("flags", ["'g'", "'c'", "'im'", "word"])
    async def test_only_the_i_flag(self, engine: SlayerQueryEngine, flags: str) -> None:
        await _rejected(engine, "flag", filters=[f"regexp_like(word, 'a', {flags})"])

    async def test_null_input_yields_null(self, engine: SlayerQueryEngine) -> None:
        values = lib_values(await engine.execute(lib_query("regexp_replace(word, 'a', 'b')", filters=["id = 5"])))
        assert values == {5: None}


class TestReplacement:
    @pytest.mark.parametrize("replacement", ["\\q", "\\0", "\\3", "\\", "\\n"])
    async def test_bad_escape_or_group_rejected(self, engine: SlayerQueryEngine, replacement: str) -> None:
        expr = f"regexp_replace(s, '(\\w+) (\\w+)', '{replacement}')"
        await _rejected(engine, "replacement", dimensions=["id", {"expression": expr, "name": "v"}])

    async def test_variable_replacement(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(lib_query("regexp_replace(s, '(\\w+) (\\w+)', {r})", filters=["id = 6"],
                                              variables={"r": "\\2 $1"}))
        assert lib_values(resp) == {6: "Lovelace $1"}


def _assert_marker(sql: str, *markers: str, absent: tuple[str, ...] = ()) -> None:
    upper = sql.upper()
    for marker in markers:
        assert marker.upper() in upper, sql
    for marker in absent:
        assert marker.upper() not in upper, sql


class TestLowerings:
    async def test_snowflake_like_via_instr(self) -> None:
        _assert_marker(await emit_lib(lib_query(filters=["regexp_like(word, 'a')"]), dialect="snowflake"),
                       "REGEXP_INSTR(")

    async def test_postgres_replace_is_global(self) -> None:
        sql = await emit_lib(lib_query("regexp_replace(word, 'a', 'b')"), dialect="postgres")
        _assert_marker(sql, "REGEXP_REPLACE(", "'g'")

    async def test_sqlite_uses_registered_functions(self) -> None:
        sql = await emit_lib(lib_query("regexp_extract(word, 'a')", filters=["regexp_like(word, 'a')"]),
                             dialect="sqlite")
        _assert_marker(sql, "SLAYER_REGEXP_LIKE(", "SLAYER_REGEXP_EXTRACT(")

    async def test_sqlite_replace_uses_registered_function(self) -> None:
        _assert_marker(await emit_lib(lib_query("regexp_replace(word, 'a', 'b')"), dialect="sqlite"),
                       "SLAYER_REGEXP_REPLACE(")

    @pytest.mark.parametrize("dialect", ["mysql", "trino"])
    async def test_group_references_rewritten(self, dialect: str) -> None:
        sql = await emit_lib(lib_query("regexp_replace(s, '(\\w+) (\\w+)', '\\2, \\1')"), dialect=dialect)
        _assert_marker(sql, "$2", "$1")

    async def test_mysql_is_case_sensitive_without_the_flag(self) -> None:
        sql = await emit_lib(lib_query(filters=["regexp_like(word, '^paid$')"]), dialect="mysql")
        _assert_marker(sql, "'c'")

    async def test_trino_anchors_the_whole_string(self) -> None:
        sql = await emit_lib(lib_query(filters=["regexp_like(code, '^ab$')"]), dialect="trino")
        _assert_marker(sql, "\\z")


class TestSqlServerVersion:
    @pytest.mark.parametrize("expr", ["regexp_like(code, '^a')", "code ~ '^a'"])
    async def test_sql_server_2022_fails_closed(self, monkeypatch, expr: str) -> None:
        patch_profile(monkeypatch, version=(16, 0))
        with pytest.raises(SlayerError) as info:
            await emit_lib(lib_query(filters=[expr]), dialect="tsql")
        assert "2025" in str(info.value), str(info.value)

    @pytest.mark.parametrize("expr", ["regexp_extract(code, 'a')", "regexp_replace(code, 'a', 'b')"])
    async def test_every_regex_function_is_gated(self, monkeypatch, expr: str) -> None:
        patch_profile(monkeypatch, version=(16, 0))
        with pytest.raises(SlayerError, match="2025"):
            await emit_lib(lib_query(expr), dialect="tsql")

    async def test_sql_server_2025_lowers_via_instr(self, monkeypatch) -> None:
        patch_profile(monkeypatch, version=(17, 0))
        sql = await emit_lib(lib_query(filters=["regexp_like(code, '^a')"]), dialect="tsql")
        _assert_marker(sql, "REGEXP_INSTR(", absent=("REGEXP_LIKE(",))

    async def test_unknown_version_emits(self, monkeypatch) -> None:
        patch_profile(monkeypatch, version=None)
        await emit_lib(lib_query(filters=["regexp_like(code, '^a')"]), dialect="tsql")

    async def test_profile_probed_for_regex(self, monkeypatch) -> None:
        calls = patch_profile(monkeypatch, version=(17, 0))
        await emit_lib(lib_query(filters=["code LIKE 'a%'"]), dialect="tsql")
        assert calls == []
        await emit_lib(lib_query(filters=["code ~ 'a'"]), dialect="tsql")
        assert calls == [1]


class TestBracesInStringLiterals:
    @pytest.mark.parametrize("variables", [None, {"unused": 1}])
    async def test_quantifier_braces_are_text(self, engine: SlayerQueryEngine, variables) -> None:
        resp = await engine.execute(lib_query(filters=["regexp_like(code, '^[a-z]{1,2}[0-9]{2,}$')"],
                                              variables=variables))
        assert lib_ids(resp) == {1, 2}

    @pytest.mark.parametrize("variables", [None, {"unused": 1}])
    @pytest.mark.parametrize(("literal", "text"), [("{1, 2, 3}", "{1, 2, 3}"), ("{}", "{}"), ("{3}", "{3}")])
    async def test_digit_and_comma_braces_are_text(self, engine: SlayerQueryEngine, variables, literal: str,
                                                   text: str) -> None:
        resp = await engine.execute(lib_query(f"'{literal}' || code", filters=["id = 7"], variables=variables))
        assert lib_values(resp) == {7: f"{text}a1"}

    async def test_name_shaped_braces_in_a_pattern_hint_escaping(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(ValueError, match="Undefined variable 'L'") as info:
            await engine.execute(lib_query(filters=["regexp_like(word, '\\p{L}')"]))
        assert "{{" in str(info.value) and "}}" in str(info.value), str(info.value)
