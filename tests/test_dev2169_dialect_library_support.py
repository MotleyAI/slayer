"""Every dialect translates a portable-library function or refuses it (spec: queries/scalar-functions › Every dialect
translates a library function or refuses it; models/list-columns; aggregations/collect)."""

from __future__ import annotations

import re
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import sqlglot
from pydantic import BaseModel, ConfigDict
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType, DatePart, TimeGranularity
from slayer.core.errors import ListTypeError, UnsupportedServerFunctionError
from slayer.core.keys import SCALAR_SPECS, CastKey, LiteralKey
from slayer.core.models import DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.dialects import (
    _ALL_DIALECTS,
    _DS_TYPE_ONLY_DIALECTS,
    OracleDialect,
    PostgresDialect,
    SqlDialect,
    dialect_for_ds_type,
    dialect_for_sa_backend,
)
from slayer.sql.dialects.base import is_list_fn
from slayer.sql.render.library import LIBRARY_FUNCTIONS
from slayer.sql.render.row_expr import DATE_FUNCTIONS, render_cast, render_scalar_call
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2101_fixtures import emit
from tests._dev2120_fixtures import TIER1, emit_lib, lib_model, lib_query, patch_profile
from tests._dev2137_fixtures import all_models

LIBRARY = LIBRARY_FUNCTIONS | {"try_cast"}
REGEX = frozenset({"regexp_like", "regexp_extract", "regexp_replace"})
TIER2_SET = frozenset({"like", "ilike", "trim", "ltrim", "rtrim", "replace", "instr", "strpos", "greatest", "least"})
TIER2 = ("redshift", "databricks", "spark", "oracle")
PRESTO = ("presto", "athena")
#: The golden support sets; Presto's is checked structurally (``TestDeclaredSets``).
EXPECTED: dict[str, frozenset[str]] = {
    **{d: LIBRARY for d in TIER1},
    "mariadb": LIBRARY - REGEX - {"json_value"},
    **{d: TIER2_SET for d in TIER2},
}
DS_TYPES = (*TIER1, "mariadb", *TIER2, *PRESTO)
NO_LIST = ("sqlite", "mysql", "mariadb", "tsql", *TIER2)
NATIVE_LIST = ("postgres", "duckdb", "clickhouse", "trino", "snowflake", "bigquery", *PRESTO)

#: Postgres-only spellings no other dialect's SQL may carry; per dialect, the ones that are its own SQL.
FORBIDDEN = {
    "JSONB_": r"\bJSONB_",
    "PG_INPUT_IS_VALID": r"\bPG_INPUT_IS_VALID\b",
    "REGEXP_MATCH": r"\bREGEXP_MATCH\s*\(",
    "REGEXP_I_LIKE": r"\bREGEXP_I_LIKE\b",
    "~ operator": r"\s!?~\*?\s",
    "'g' flag": r",\s*'gi?'\s*\)",
}
OWN_SPELLINGS: dict[str, frozenset[str]] = {
    "postgres": frozenset(FORBIDDEN), "duckdb": frozenset({"'g' flag"}),
}


def declared(ds_type: str) -> frozenset[str]:
    return dialect_for_ds_type(ds_type).library_functions


def supported(ds_type: str, name: str) -> bool:
    """Every case's ``TRY_CAST`` is numeric, which MariaDB refuses although it translates the others."""
    if (ds_type, name) == ("mariadb", "try_cast"):
        return False
    return name in (EXPECTED[ds_type] if ds_type in EXPECTED else declared(ds_type))


def database(ds_type: str) -> str:
    return dialect_for_ds_type(ds_type).database_name


def assert_own_sql(sql: str, *, ds_type: str) -> None:
    allowed = OWN_SPELLINGS.get(ds_type, frozenset())
    found = [name for name, pattern in FORBIDDEN.items() if name not in allowed and re.search(pattern, sql, re.I)]
    assert not found, f"{ds_type} SQL carries Postgres-only {found}:\n{sql}"


@pytest.fixture(autouse=True)
def _unknown_server_version(monkeypatch) -> None:
    patch_profile(monkeypatch, version=None)


class TestDeclaredSets:
    def test_every_registered_dialect_is_covered(self) -> None:
        registered = {type(d) for d in (*_ALL_DIALECTS, *_DS_TYPE_ONLY_DIALECTS)}
        assert {type(dialect_for_ds_type(t)) for t in DS_TYPES} == registered

    @pytest.mark.parametrize("ds_type", sorted(EXPECTED))
    def test_golden_set(self, ds_type: str) -> None:
        assert declared(ds_type) == EXPECTED[ds_type]

    @pytest.mark.parametrize("ds_type", PRESTO)
    def test_presto_translates_a_subset_of_trino(self, ds_type: str) -> None:
        got = declared(ds_type)
        assert got <= declared("trino")
        assert TIER2_SET | REGEX | {"json_value", "try_cast"} <= got

    def test_base_declares_nothing(self) -> None:
        assert SqlDialect().library_functions == frozenset()

    def test_undeclared_backend_declares_nothing(self) -> None:
        assert dialect_for_sa_backend("no_such_backend").library_functions == frozenset()

    def test_database_names_are_unique(self) -> None:
        names = [database(t) for t in DS_TYPES if t != "athena"]
        assert len(set(names)) == len(names)


# --------------------------------------------------------------------------- #
# Every dialect × every library function and spelling
# --------------------------------------------------------------------------- #
class LibCase(BaseModel):
    """One library construct: ``name`` is the canonical function its gate decides on; ``mentions`` what a refusal
    names."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    case_id: str
    name: str
    build: Callable[[], Any]
    mentions: str
    models: Callable[[], list[SlayerModel]] = lambda: [lib_model()]


def _dim(case_id: str, name: str, expr: str, *, mentions: str | None = None) -> LibCase:
    return LibCase(case_id=case_id, name=name, build=lambda: lib_query(expr), mentions=mentions or name)


def _flt(case_id: str, name: str, expr: str, *, mentions: str | None = None) -> LibCase:
    return LibCase(case_id=case_id, name=name, build=lambda: lib_query(filters=[expr]), mentions=mentions or name)


def _lib(**body: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "lib", **body})


def _with_saved_measure() -> list[SlayerModel]:
    base = lib_model()
    return [base.model_copy(update={"measures": [ModelMeasure(name="txt_total", formula="sum(num_txt * 2)")]})]


def _stages() -> list[SlayerQuery]:
    return [SlayerQuery.model_validate({"name": "s1", "source_model": "lib", "dimensions": ["id", "num_txt"]}),
            SlayerQuery.model_validate({"source_model": "s1", "dimensions": ["id"], "filters": ["num_txt > 100"]})]


FUNCTION_CASES = [
    _dim("left", "left", "left(s, 2)"),
    _dim("right", "right", "right(s, 2)"),
    _dim("starts_with", "starts_with", "starts_with(s, 'a')"),
    _dim("ends_with", "ends_with", "ends_with(s, 'a')"),
    _dim("contains", "contains", "contains(s, 'a')"),
    _dim("lpad", "lpad", "lpad(s, 4, '*')"),
    _dim("rpad", "rpad", "rpad(s, 4, '*')"),
    _dim("reverse", "reverse", "reverse(s)"),
    _dim("repeat", "repeat", "repeat(s, 2)"),
    _dim("concat_ws", "concat_ws", "concat_ws('-', s, p)"),
    _dim("chr", "chr", "chr(cp)"),
    _dim("ascii", "ascii", "ascii(s)"),
    _dim("trim", "trim", "trim(code)"),
    _dim("ltrim", "ltrim", "ltrim(code)"),
    _dim("rtrim", "rtrim", "rtrim(code)"),
    _dim("strpos", "strpos", "strpos(s, p)"),
    _dim("instr", "instr", "instr(s, p)"),
    _dim("replace", "replace", "replace(code, '_', '-')"),
    _dim("div", "div", "div(i, m)"),
    _dim("pi", "pi", "pi()"),
    *(_dim(fn, fn, f"{fn}(x)") for fn in ("sin", "cos", "tan", "asin", "acos", "atan", "cbrt")),
    _dim("atan2", "atan2", "atan2(y, x)"),
    _dim("greatest", "greatest", "greatest(a, b)"),
    _dim("least", "least", "least(a, b)"),
    _dim("split_part", "split_part", "split_part(s, '/', 2)"),
    _flt("like", "like", r"code LIKE 'a\_%'"),
    _flt("ilike", "ilike", "word ILIKE 'ad%'"),
    _dim("json_value", "json_value", "json_value(payload, '$.a.b')"),
    _flt("regexp_like", "regexp_like", "regexp_like(code, '^a')"),
    _dim("regexp_extract", "regexp_extract", "regexp_extract(tag, '@(.+)$', 1)"),
    _dim("regexp_replace", "regexp_replace", "regexp_replace(tag, '[^0-9]', '')"),
    _dim("try_cast", "try_cast", "TRY_CAST(num_txt AS DOUBLE)", mentions="TRY_CAST"),
]
SPELLING_CASES = [
    _flt("spelling|tilde", "regexp_like", "word ~ '^ad'"),
    _flt("spelling|tilde-star", "regexp_like", "word ~* '^ad'"),
    _flt("spelling|not-tilde", "regexp_like", "word !~ '^ad'"),
    _dim("spelling|arrow-text", "json_value", "payload ->> 'ok'"),
    _dim("spelling|arrow-chain", "json_value", "payload -> 'a' ->> 'b'"),
    _dim("spelling|json_extract", "json_value", "json_extract(payload, '$.ok')"),
    _dim("spelling|json_extract_scalar", "json_value", "json_extract_scalar(payload, '$.a.b')"),
    _dim("spelling|json_extract_path_text", "json_value", "json_extract_path_text(payload, 'a', 'b')"),
    _dim("spelling|jsonb_extract_path_text", "json_value", "jsonb_extract_path_text(payload, 'a', 'b')"),
    _dim("spelling|position", "strpos", "POSITION(p IN s)"),
    _dim("spelling|returning", "json_value", "json_value(payload, '$.qty' RETURNING INT)"),
]
COERCION_CASES = [
    _flt("coerce|comparison", "try_cast", "num_txt > 100", mentions="number"),
    _flt("coerce|in-list", "try_cast", "num_txt IN (1, 2)", mentions="number"),
    _dim("coerce|arithmetic", "try_cast", "num_txt * 2", mentions="number"),
    _flt("coerce|column-comparison", "try_cast", "num_txt = qty", mentions="number"),
    _flt("coerce|literal-first", "try_cast", "100 < num_txt", mentions="number"),
    _flt("coerce|between", "try_cast", "num_txt BETWEEN 1 AND 10", mentions="number"),
    _flt("coerce|is-distinct-from", "try_cast", "num_txt IS DISTINCT FROM 99", mentions="number"),
    _dim("coerce|greatest", "try_cast", "greatest(num_txt, 5)", mentions="number"),
    _dim("coerce|least", "try_cast", "least(5, num_txt)", mentions="number"),
    LibCase(case_id="coerce|measure", name="try_cast", mentions="number",
            build=lambda: _lib(measures=[{"formula": "sum(num_txt * 2)", "name": "t"}])),
    LibCase(case_id="coerce|saved-measure", name="try_cast", mentions="number",
            build=lambda: _lib(measures=["txt_total"]), models=_with_saved_measure),
    LibCase(case_id="coerce|order-by", name="try_cast", mentions="number",
            build=lambda: _lib(dimensions=["id"], order=[{"column": "num_txt * 2"}])),
    LibCase(case_id="coerce|stage", name="try_cast", mentions="number", build=_stages),
]
CASES = [*FUNCTION_CASES, *SPELLING_CASES, *COERCION_CASES]


def test_every_library_function_has_a_case() -> None:
    assert sorted(LIBRARY - {c.name for c in FUNCTION_CASES}) == []


async def _emit(case: LibCase, ds_type: str) -> str:
    return await emit(case.build(), dialect=ds_type, models=case.models())


@pytest.mark.parametrize("ds_type", DS_TYPES)
@pytest.mark.parametrize("case", CASES, ids=lambda c: c.case_id)
async def test_translated_or_refused(case: LibCase, ds_type: str) -> None:
    if supported(ds_type, case.name):
        assert_own_sql(await _emit(case, ds_type), ds_type=ds_type)
        return
    with pytest.raises(UnsupportedServerFunctionError) as info:
        await _emit(case, ds_type)
    msg = str(info.value)
    assert database(ds_type) in msg, msg
    assert case.mentions.lower() in msg.lower(), msg


@pytest.mark.parametrize("ds_type", TIER2)
async def test_refused_on_execution_too(ds_type: str) -> None:
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(DatasourceConfig(name="test", type=ds_type, host="localhost", database="db"))
        await storage.save_model(lib_model())
        engine, query, name = SlayerQueryEngine(storage=storage), lib_query("json_value(payload, '$.a')"), database(ds_type)
        with pytest.raises(UnsupportedServerFunctionError, match=name):
            await engine.execute(query)


class TestTier2Like:
    async def test_databricks_like_keeps_the_escape(self) -> None:
        sql = await emit_lib(lib_query(filters=[r"code LIKE 'a\_%'"]), dialect="databricks")
        escape = next(sqlglot.parse_one(sql, read="databricks").find_all(exp.Escape))
        assert isinstance(escape.this, exp.Like), sql
        assert (escape.this.expression.name, escape.expression.name) == ("a\\_%", "\\"), sql


# --------------------------------------------------------------------------- #
# Presto spells the portable library its own way (spec: queries/regex › Presto spells the same pattern)
# --------------------------------------------------------------------------- #
def _strings(sql: str, *, read: str) -> set[str]:
    return {lit.name for lit in sqlglot.parse_one(sql, read=read).find_all(exp.Literal) if lit.is_string}


class TestPresto:
    @pytest.mark.parametrize("expr", ["regexp_like(notes, 'paid.*late')", "regexp_like(code, '^ab$')",
                                      "regexp_like(word, '^paid$', 'i')"])
    async def test_patterns_match_trino(self, expr: str) -> None:
        presto = await emit_lib(lib_query(filters=[expr]), dialect="presto")
        trino = await emit_lib(lib_query(filters=[expr]), dialect="trino")
        assert _strings(presto, read="presto") == _strings(trino, read="trino"), presto

    async def test_dot_matches_newline(self) -> None:
        sql = await emit_lib(lib_query(filters=["regexp_like(notes, 'paid.*late')"]), dialect="presto")
        assert any("(?s)" in s for s in _strings(sql, read="presto")), sql

    @pytest.mark.parametrize(("expr", "function"), [
        ("regexp_extract(notes, 'a(b)', 1)", "REGEXP_EXTRACT"),
        ("json_value(payload, '$.a.b')", "JSON_EXTRACT_SCALAR"),
        ("regexp_replace(tag, '[^0-9]', '')", "REGEXP_REPLACE"),
    ])
    @pytest.mark.parametrize("ds_type", PRESTO)
    async def test_own_functions(self, ds_type: str, expr: str, function: str) -> None:
        sql = await emit_lib(lib_query(expr), dialect=ds_type)
        assert f"{function}(" in sql.upper(), sql
        assert_own_sql(sql, ds_type=ds_type)


# --------------------------------------------------------------------------- #
# The render doors and the base class
# --------------------------------------------------------------------------- #
_COL = exp.column("s", table="t")


def _args(name: str) -> list[Expression]:
    special: dict[str, list[Expression]] = {
        "split_part": [_COL, exp.Literal.string("/"), exp.Literal.number(2)],
        "regexp_like": [_COL, exp.Literal.string("a")],
        "regexp_extract": [_COL, exp.Literal.string("a(b)"), exp.Literal.number(1)],
        "regexp_replace": [_COL, exp.Literal.string("a"), exp.Literal.string("b")],
        "json_value": [_COL, exp.Literal.string("$.a")],
        "like": [_COL, exp.Literal.string("a%")],
        "ilike": [_COL, exp.Literal.string("a%")],
        "strpos": [_COL, exp.Literal.string("a")],
    }
    if name in special:
        return special[name]
    return [_COL, exp.Literal.number(2), exp.Literal.number(3)][:SCALAR_SPECS[name].min_args]


_UNSUPPORTED = [SqlDialect(), OracleDialect()]


@pytest.mark.parametrize("dialect", _UNSUPPORTED, ids=lambda d: type(d).__name__)
@pytest.mark.parametrize("name", sorted(LIBRARY_FUNCTIONS))
def test_render_scalar_call_refuses(name: str, dialect: SqlDialect) -> None:
    if name in dialect.library_functions:
        pytest.skip(f"{type(dialect).__name__} translates {name}")
    args = _args(name)
    with pytest.raises(UnsupportedServerFunctionError, match=dialect.database_name):
        render_scalar_call(name=name, args=args, dialect=dialect)


@pytest.mark.parametrize("name", sorted(LIBRARY_FUNCTIONS))
def test_render_scalar_call_translates_on_postgres(name: str) -> None:
    render_scalar_call(name=name, args=_args(name), dialect=PostgresDialect())


def _col_part(_key: object) -> Expression:
    return _COL.copy()


def _cast(*, try_: bool) -> CastKey:
    return CastKey(source=LiteralKey(value="1"), type_text="DOUBLE", data_type=DataType.DOUBLE, try_=try_)


@pytest.mark.parametrize("dialect", _UNSUPPORTED, ids=lambda d: type(d).__name__)
def test_render_try_cast_refuses(dialect: SqlDialect) -> None:
    key = _cast(try_=True)
    with pytest.raises(UnsupportedServerFunctionError):
        render_cast(key=key, part=_col_part, dialect=dialect)


@pytest.mark.parametrize("dialect", _UNSUPPORTED, ids=lambda d: type(d).__name__)
def test_render_plain_cast_renders(dialect: SqlDialect) -> None:
    assert isinstance(render_cast(key=_cast(try_=False), part=_col_part, dialect=dialect), exp.Cast)


_X, _Y = exp.column("x"), exp.column("y")
_LIST_HOOKS: dict[str, Callable[[SqlDialect], Any]] = {
    "length": lambda d: d.build_list_call("length", (_X,)),
    "contains": lambda d: d.build_list_call("contains", (_X, exp.Literal.string("a"))),
    "contains_any": lambda d: d.build_list_call("contains_any", (_X, _Y)),
    "contains_all": lambda d: d.build_list_call("contains_all", (_X, _Y)),
    "collect": lambda d: d.build_collect(_X, DataType.TEXT),
}


@pytest.mark.parametrize("hook", sorted(_LIST_HOOKS))
def test_base_list_hooks_refuse(hook: str) -> None:
    build, dialect = _LIST_HOOKS[hook], SqlDialect()
    with pytest.raises((UnsupportedServerFunctionError, ListTypeError)):
        build(dialect)


def _passthrough_names() -> list[str]:
    return sorted(n for n in SCALAR_SPECS
                  if n not in LIBRARY_FUNCTIONS and n not in DATE_FUNCTIONS and not is_list_fn(n) and n != "interval")


def _neutral_defaults() -> dict[str, Expression]:
    d = SqlDialect()
    out: dict[str, Expression] = {}
    for g in TimeGranularity:
        out[f"date_trunc:{g.value}"] = d.build_date_trunc(_X, g)
        out[f"date_diff:{g.value}"] = d.build_date_diff(unit=g, start=_X, end=_Y, operand=DataType.TIMESTAMP)
        out[f"date_add:{g.value}"] = d.build_date_add(expr=_X, count=exp.Literal.number(2), unit=g,
                                                       operand=DataType.DATE)
    for p in DatePart:
        out[f"date_part:{p.value}"] = d.build_date_part(part=p, expr=_X, operand=DataType.DATE)
    out |= {"current_date": d.build_current_date(), "current_timestamp": d.build_current_timestamp(),
            "median": d.build_median(_X), "percentile": d.build_percentile(exp.Literal.number("0.9"), _X),
            "count_distinct_approx": d.build_approx_count_distinct(_X), "modulo": d.build_modulo(_X, _Y, False)}
    for agg in ("stddev_samp", "stddev_pop", "var_samp", "var_pop"):
        out[agg] = d.build_stat_agg_1arg(agg, _X)
    for agg in ("corr", "covar_samp", "covar_pop"):
        out[agg] = d.build_covar_2arg(agg, _X, _Y)
    for name in _passthrough_names():
        out[f"scalar:{name}"] = render_scalar_call(name=name, args=_args(name), dialect=d)
    return out


@pytest.mark.parametrize(("hook", "tree"), sorted(_neutral_defaults().items()), ids=lambda v: v if isinstance(v, str) else "")
def test_base_defaults_outside_the_library_are_neutral(hook: str, tree: Expression) -> None:
    vendor = sorted({a.name for a in tree.find_all(exp.Anonymous)})
    assert not vendor, f"{hook}: base default spells {vendor}: {tree.sql()}"


# --------------------------------------------------------------------------- #
# List constructs (spec: models/list-columns › Backends without native lists reject list constructs)
# --------------------------------------------------------------------------- #
def _contractors(**body: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "contractors", **body})


def _count(name: str = "n") -> dict[str, str]:
    return {"formula": "count(*)", "name": name}


class ListCase(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    case_id: str
    build: Callable[[], Any]
    mentions: str


LIST_CASES = [
    ListCase(case_id="contains", mentions="contains",
             build=lambda: _contractors(dimensions=["id"], filters=["contains(skills, 'go')"])),
    ListCase(case_id="contains_any-literal", mentions="contains_any",
             build=lambda: _contractors(dimensions=["id"], filters=["contains_any(skills, ['go', 'sql'])"])),
    ListCase(case_id="contains_all-list-column", mentions="contains_all",
             build=lambda: _contractors(dimensions=["id"], filters=["contains_all(skills, required)"])),
    ListCase(case_id="length", mentions="length",
             build=lambda: _contractors(dimensions=["id", {"expression": "length(skills)", "name": "v"}])),
    ListCase(case_id="array-literal", mentions="length",
             build=lambda: _contractors(dimensions=["id", {"expression": "length(['go', 'sql'])", "name": "v"}])),
    ListCase(case_id="element-access", mentions="skills", build=lambda: _contractors(dimensions=["skills.value"])),
    ListCase(case_id="list-child", mentions="skills",
             build=lambda: _contractors(measures=[{"formula": "count(skills.*)", "name": "n"}])),
    ListCase(case_id="child-filter", mentions="skills",
             build=lambda: _contractors(measures=[_count()], filters=["skills.value = 'go'"])),
    ListCase(case_id="nested-hop", mentions="skills",
             build=lambda: SlayerQuery.model_validate({"source_model": "assignments", "dimensions": ["id"],
                                                       "filters": ["contractors.skills.value = 'sql'"]})),
    ListCase(case_id="stage", mentions="contains",
             build=lambda: [
                 SlayerQuery.model_validate({"name": "s1", "source_model": "contractors", "dimensions": ["id"],
                                             "filters": ["contains(skills, 'go')"]}),
                 SlayerQuery.model_validate({"source_model": "s1", "measures": [_count()]}),
             ]),
    ListCase(case_id="collect", mentions="collect",
             build=lambda: _contractors(dimensions=["team"], measures=[{"formula": "collect(city)", "name": "c"}])),
    ListCase(case_id="collect-elements", mentions="collect",
             build=lambda: _contractors(dimensions=["team"],
                                        measures=[{"formula": "collect(skills.value)", "name": "c"}])),
]


@pytest.mark.parametrize("ds_type", NO_LIST)
@pytest.mark.parametrize("case", LIST_CASES, ids=lambda c: c.case_id)
async def test_list_construct_refused(case: ListCase, ds_type: str) -> None:
    query, models = case.build(), all_models()
    with pytest.raises(ListTypeError) as info:
        await emit(query, dialect=ds_type, models=models)
    msg = str(info.value)
    assert database(ds_type) in msg, msg
    assert case.mentions in msg, msg


#: A correlated EXISTS on ClickHouse needs the live server's settings (covered by its live list suite).
_NEEDS_SERVER = {("nested-hop", "clickhouse")}


@pytest.mark.parametrize(("case", "ds_type"), [
    (c, d) for c in LIST_CASES for d in NATIVE_LIST if (c.case_id, d) not in _NEEDS_SERVER
], ids=lambda v: v if isinstance(v, str) else v.case_id)
async def test_list_construct_emits(case: ListCase, ds_type: str) -> None:
    sql = await emit(case.build(), dialect=ds_type, models=all_models())
    if ds_type != "postgres":
        assert_own_sql(sql, ds_type=ds_type)


@pytest.mark.parametrize("ds_type", TIER2)
async def test_list_refusal_suggests_presto(ds_type: str) -> None:
    query, models = LIST_CASES[0].build(), all_models()
    with pytest.raises(ListTypeError, match="Presto"):
        await emit(query, dialect=ds_type, models=models)


# --------------------------------------------------------------------------- #
# docs/database-support.md lists, per dialect, what it translates
# --------------------------------------------------------------------------- #
DOCS = Path(__file__).resolve().parent.parent / "docs" / "database-support.md"
SUPPORT_HEADING = "## Portable library support"


def _support_table() -> tuple[list[str], dict[str, list[str]]]:
    """``(databases, {function: cells})`` of the first table under ``SUPPORT_HEADING``."""
    lines = DOCS.read_text(encoding="utf-8").splitlines()
    assert SUPPORT_HEADING in lines, f"{DOCS} has no {SUPPORT_HEADING!r} section"
    rows = []
    for line in lines[lines.index(SUPPORT_HEADING) + 1:]:
        if line.startswith("|"):
            rows.append([cell.strip() for cell in line.strip().strip("|").split("|")])
        elif rows:
            break
    header, _rule, *body = rows
    return header[1:], {row[0].strip("`"): row[1:] for row in body}


class TestDocsSupportTable:
    def test_one_column_per_database(self) -> None:
        databases, _ = _support_table()
        assert sorted(databases) == sorted({database(t) for t in DS_TYPES})

    def test_one_row_per_function(self) -> None:
        _, rows = _support_table()
        assert sorted(rows) == sorted(LIBRARY)

    @pytest.mark.parametrize("ds_type", [t for t in DS_TYPES if t != "athena"])
    def test_agrees_with_the_code(self, ds_type: str) -> None:
        databases, rows = _support_table()
        column = databases.index(database(ds_type))
        documented = {fn for fn, cells in rows.items() if cells[column] == "✓"}
        assert documented == declared(ds_type)
