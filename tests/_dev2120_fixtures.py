"""Shared fixtures for the portable Mode-B library: the ``lib`` table, the case table every dialect runs, seeding.

Expected values are the Postgres meaning (spec: queries/scalar-functions and siblings).
"""

from __future__ import annotations

import contextlib
import json
import math
from collections.abc import AsyncGenerator, Callable, Iterable, Mapping
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Optional

import pytest
from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType
from slayer.core.errors import SlayerError
from slayer.core.keys import SCALAR_SPECS
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.sql.client import SlayerSQLClient
from slayer.sql.dialects.base import ServerProfile
from slayer.storage.sqlite_conn import transaction
from tests._dev1737_fixtures import SERVER_TYPES, TableSpec, as_temporal, seed_backend
from tests._dev2101_fixtures import emit
from tests._engine_helpers import seeded_exec_engine

TIER1 = ("sqlite", "postgres", "duckdb", "mysql", "clickhouse", "tsql", "trino", "snowflake", "bigquery")
EXEC_BACKENDS = ("sqlite", "duckdb")
IDS = tuple(range(1, 11))

#: Functions this change adds to ``SCALAR_SPECS``.
NEW_FUNCTIONS = frozenset({
    "left", "right", "starts_with", "ends_with", "contains", "lpad", "rpad", "reverse", "repeat",
    "split_part", "concat_ws", "chr", "ascii",
    "div", "pi", "sin", "cos", "tan", "asin", "acos", "atan", "atan2", "cbrt",
    "regexp_like", "regexp_extract", "regexp_replace", "json_value",
})
#: Spellings the parser collapses into a library function or the granularity call.
SPELLINGS = frozenset({
    "strpos", "POSITION", "date_trunc", "json_extract", "json_extract_scalar",
    "json_extract_path_text", "jsonb_extract_path_text",
})
#: Mode-B operators whose meaning the library fixes.
OPERATORS = frozenset({
    "=", "<>", "<", "<=", ">", ">=", "IN", "BETWEEN", "IS DISTINCT FROM",
    "+", "-", "*", "/", "%", "||", "LIKE", "ILIKE", "~", "~*", "!~", "!~*", "->", "->>", "RETURNING",
})

# --------------------------------------------------------------------------- #
# The ``lib`` table
# --------------------------------------------------------------------------- #
P1 = '{"a": {"b": "x"}, "qty": 2, "n": 0, "f": false, "ok": true}'
P7 = '{"items": [{}, {"unit price": "9.5"}], "a": [{"b": "x"}]}'
P9 = '{"a": {"b": 1}, "n": 12, "f": true, "ok": false}'
P10 = '{"a": {"b": 2.5}, "d": "2024-02-29", "t": "2024-02-29 10:00:00", "n": "7"}'

LIB = TableSpec(
    name="lib",
    columns=[
        ("id", "INT"), ("s", "TEXT"), ("p", "TEXT"), ("n", "INT"), ("i", "INT"), ("m", "INT"),
        ("x", "DOUBLE"), ("y", "DOUBLE"), ("a", "INT"), ("b", "INT"), ("c", "INT"), ("cp", "INT"),
        ("code", "TEXT"), ("word", "TEXT"), ("tag", "TEXT"), ("notes", "TEXT"), ("prefix", "TEXT"),
        ("payload", "TEXT"), ("num_txt", "TEXT"), ("qty", "INT"), ("ts", "TIMESTAMP"), ("doc", "TEXT"),
        ("big", "TEXT"),
    ],
    rows=[
        (1, "ABCDE", "CD", 2, 7, 2, 0.5, 1.0, None, 2, 1, 65, "ab123", "paid", "+1 (555) 01",
         "paid\nlate", "ab1", P1, "120.5", 3, "2025-03-05 10:00:00", '{"v": null}', "12345678901234567"),
        (2, "€é", "é", -2, -7, -2, 2.0, -1.0, 3, None, 1, 8364, "ab12", "PAID", "5 USD",
         "line1\nline2", "zz", '{"a": {}, "qty": "3"}', "99", 0, "2025-03-31 23:59:59", '{}', None),
        (3, "a/b/c", "/", 0, 7, 0, -1.0, 0.5, 1, 2, None, 128512, "a_1", "ada", "a@x.org",
         "nothing", "a_", '{"a": {"b": null}, "qty": "x"}', "abc", 1, "2025-04-01 00:00:00", '{"v": {"k": 1}}', None),
        (4, "", "x", 9, -7, 3, 1.0, 1.0, None, None, None, 55296, "abc1", "Ada", "nobody",
         "paid late", "a_", '{"a": {"b": [1]}}', None, 2, "2024-12-31 12:00:00", '{"v": [1]}', None),
        (5, None, "a", None, 7, None, None, 1.0, 5, 5, 5, None, None, None, None,
         None, None, "not json", "3.0", 3, None, '{"v": "x"}', None),
        (6, "Ada Lovelace", "Love", 3, 7, -3, -8.0, None, -1, 0, 1, 1114112, "[ab]1", "123", "15%",
         "Paid Later", "[a", None, "10", 1, "2025-02-10 08:00:00", '{"v": 0}', None),
        (7, "x\U0001F600y", "\U0001F600", -9, -7, 7, -1.5, 2.0, 2, None, None, 57343, "a1", "١٢٣", "150",
         "x", "%", P7, "1.5", 2, "2025-03-15 00:00:00", '{"v": false}', None),
        (8, "ab", "", 1, 0, 1, 0.0, 1.0, None, None, 4, 1114111, "a*?1", "admin", "x",
         "late paid", "a*", "{}", "1", 1, "2025-01-01 00:00:00", '{"v": "2024-02-29"}', None),
        (9, "abc", "z", -1, None, -1, 27.0, -3.0, 0, -3, None, 233, "ab\n", "Adele", "USD USD",
         "a\nb", None, P9, "02", 2, "2025-06-30 18:30:00", '{"v": "2024-02-29 10:00:00"}', None),
        (10, "Ünïcode", None, 5, 8, 4, 0.25, 0.5, 7, 1, 3, 97, "1_2", "c:\\x1", "",
         "", "1\\_", P10, "x", None, "2025-03-01 00:00:00", '{"v": 12}', None),
    ],
)
_NAMES = [c for c, _ in LIB.columns]
ROWS: dict[int, dict[str, Any]] = {r[0]: dict(zip(_NAMES, r)) for r in LIB.rows}


def lib_model(*, data_source: str = "test") -> SlayerModel:
    types = {"INT": DataType.INT, "DOUBLE": DataType.DOUBLE, "TEXT": DataType.TEXT, "TIMESTAMP": DataType.TIMESTAMP}
    return SlayerModel(
        name="lib", sql_table="lib", data_source=data_source, default_time_dimension="ts",
        columns=[Column(name=c, type=types[k], primary_key=c == "id") for c, k in LIB.columns],
    )


# --------------------------------------------------------------------------- #
# Postgres-meaning oracle
# --------------------------------------------------------------------------- #
def _nulls(fn: Callable[..., Any]) -> Callable[..., Any]:
    def wrapped(*args: Any) -> Any:
        return None if any(a is None for a in args) else fn(*args)
    return wrapped


@_nulls
def pg_left(s: str, n: int) -> str:
    return s[:n]


@_nulls
def pg_right(s: str, n: int) -> str:
    return s[max(len(s) - n, 0):] if n >= 0 else s[-n:]


@_nulls
def pg_lpad(s: str, n: int, fill: str = " ") -> str:
    if n <= 0:
        return ""
    return s[:n] if len(s) >= n else (fill * n)[:n - len(s)] + s


@_nulls
def pg_rpad(s: str, n: int, fill: str = " ") -> str:
    if n <= 0:
        return ""
    return s[:n] if len(s) >= n else s + (fill * n)[:n - len(s)]


@_nulls
def pg_split_part(s: str, delimiter: str, k: int) -> str:
    parts = s.split(delimiter)
    if k > 0:
        return parts[k - 1] if k <= len(parts) else ""
    return parts[k] if -k <= len(parts) else ""


@_nulls
def pg_strpos(s: str, p: str) -> int:
    return s.find(p) + 1


@_nulls
def pg_div(a: float, b: float) -> Optional[float]:
    return None if b == 0 else math.trunc(a / b)


def pg_greatest(*args: Any) -> Any:
    present = [a for a in args if a is not None]
    return max(present) if present else None


def pg_least(*args: Any) -> Any:
    present = [a for a in args if a is not None]
    return min(present) if present else None


@_nulls
def pg_chr(cp: int) -> Optional[str]:
    return None if 0xD800 <= cp <= 0xDFFF or cp > 0x10FFFF else chr(cp)


@_nulls
def pg_ascii(s: str) -> int:
    return ord(s[0]) if s else 0


@_nulls
def in_unit(fn: Callable[[float], float], x: float) -> Optional[float]:
    return fn(x) if -1 <= x <= 1 else None


@_nulls
def cbrt(x: float) -> float:
    return math.copysign(abs(x) ** (1 / 3), x)


def num_text(value: Any) -> Optional[float]:
    """``TRY_CAST(text AS DOUBLE)``."""
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# The case table
# --------------------------------------------------------------------------- #
class NumText(BaseModel):
    """A JSON number read as TEXT: the database's own rendering of ``value``."""

    model_config = ConfigDict(frozen=True)
    value: float


class Unspecified(BaseModel):
    """A cell the plan leaves to the database (e.g. a JSON number read as BOOLEAN)."""

    model_config = ConfigDict(frozen=True)


ANY = Unspecified()


class Case(BaseModel):
    """One executed case: a row value per id, or the ids a filter keeps."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    case_id: str
    covers: tuple[str, ...]
    expr: str
    values: Optional[dict[int, Any]] = None
    keeps: Optional[frozenset[int]] = None
    variables: Optional[dict[str, Any]] = None
    model: str = "lib"

    def query(self) -> SlayerQuery:
        body: dict[str, Any] = {"source_model": self.model, "distinct_dimension_values": False}
        if self.keeps is not None:
            body |= {"dimensions": ["id"], "filters": [self.expr]}
        else:
            body |= {"dimensions": ["id", {"expression": self.expr, "name": "v"}]}
        if self.variables:
            body["variables"] = self.variables
        return SlayerQuery.model_validate(body)


def every(fn: Callable[[dict[str, Any]], Any]) -> dict[int, Any]:
    return {rid: fn(row) for rid, row in ROWS.items()}


def only(mapping: Mapping[int, Any], *, default: Any = None) -> dict[int, Any]:
    return {rid: mapping.get(rid, default) for rid in IDS}


def keeps(pred: Callable[[dict[str, Any]], Optional[bool]]) -> frozenset[int]:
    return frozenset(rid for rid, row in ROWS.items() if pred(row))


def _v(case_id: str, covers: Iterable[str], expr: str, values: dict[int, Any], **kw: Any) -> Case:
    return Case(case_id=case_id, covers=tuple(covers), expr=expr, values=values, **kw)


def _f(case_id: str, covers: Iterable[str], expr: str, kept: Iterable[int], **kw: Any) -> Case:
    return Case(case_id=case_id, covers=tuple(covers), expr=expr, keeps=frozenset(kept), **kw)


def _ts(row: dict[str, Any]) -> Optional[datetime]:
    return None if row["ts"] is None else datetime.fromisoformat(row["ts"])


def _month(row: dict[str, Any]) -> Optional[date]:
    t = _ts(row)
    return None if t is None else date(t.year, t.month, 1)


def _string_cases() -> list[Case]:
    s = lambda r: r["s"]  # noqa: E731
    return [
        _v("left|column-count", ["left"], "left(s, n)", every(lambda r: pg_left(r["s"], r["n"]))),
        _v("left|negative-literal", ["left"], "left(s, -2)", every(lambda r: pg_left(r["s"], -2))),
        _v("right|column-count", ["right"], "right(s, n)", every(lambda r: pg_right(r["s"], r["n"]))),
        _v("right|negative-literal", ["right"], "right(s, -1)", every(lambda r: pg_right(r["s"], -1))),
        _v("starts_with", ["starts_with"], "starts_with(s, 'a/')",
           every(lambda r: None if r["s"] is None else r["s"].startswith("a/"))),
        _v("ends_with", ["ends_with"], "ends_with(s, 'c')",
           every(lambda r: None if r["s"] is None else r["s"].endswith("c"))),
        _f("contains|plain-text", ["contains"], "contains(tag, '5%')", [6]),
        # Case and accent near-misses: a case- or accent-insensitive collation would match these.
        _v("starts_with|case", ["starts_with"], "starts_with(s, 'abc')",
           every(lambda r: None if r["s"] is None else r["s"].startswith("abc"))),
        _v("ends_with|case", ["ends_with"], "ends_with(word, 'A')",
           every(lambda r: None if r["word"] is None else r["word"].endswith("A"))),
        _v("contains|case-and-accent", ["contains"], "contains(s, 'e')",
           every(lambda r: None if r["s"] is None else "e" in r["s"])),
        _v("strpos|case", ["strpos"], "strpos(s, 'cd')", every(lambda r: pg_strpos(r["s"], "cd"))),
        _v("strpos", ["strpos"], "strpos(s, p)", every(lambda r: pg_strpos(r["s"], r["p"]))),
        _v("instr", ["instr"], "instr(s, p)", every(lambda r: pg_strpos(r["s"], r["p"]))),
        _v("position", ["POSITION"], "POSITION(p IN s)", every(lambda r: pg_strpos(r["s"], r["p"]))),
        _v("lpad|fill", ["lpad"], "lpad(s, 4, '*')", every(lambda r: pg_lpad(r["s"], 4, "*"))),
        _v("lpad|default-fill", ["lpad"], "lpad(s, 6)", every(lambda r: pg_lpad(r["s"], 6))),
        _v("lpad|multi-char-fill", ["lpad"], "lpad(s, 7, 'xy')", every(lambda r: pg_lpad(r["s"], 7, "xy"))),
        _v("lpad|column-count", ["lpad"], "lpad(s, n, '0')", every(lambda r: pg_lpad(r["s"], r["n"], "0"))),
        _v("rpad|fill", ["rpad"], "rpad(s, 4, '*')", every(lambda r: pg_rpad(r["s"], 4, "*"))),
        _v("rpad|column-count", ["rpad"], "rpad(s, n, '-')", every(lambda r: pg_rpad(r["s"], r["n"], "-"))),
        _v("reverse", ["reverse"], "reverse(s)", every(lambda r: None if r["s"] is None else r["s"][::-1])),
        _v("repeat", ["repeat"], "repeat(s, n)",
           every(lambda r: None if r["s"] is None or r["n"] is None else r["s"] * max(r["n"], 0))),
        _v("split_part|second", ["split_part"], "split_part(s, '/', 2)",
           every(lambda r: pg_split_part(r["s"], "/", 2))),
        _v("split_part|last", ["split_part"], "split_part(s, '/', -1)",
           every(lambda r: pg_split_part(r["s"], "/", -1))),
        _v("split_part|beyond", ["split_part"], "split_part(s, '/', 9)",
           every(lambda r: pg_split_part(r["s"], "/", 9))),
        _v("split_part|beyond-negative", ["split_part"], "split_part(s, '/', -4)",
           every(lambda r: pg_split_part(r["s"], "/", -4))),
        _v("split_part|case", ["split_part"], "split_part(word, 'D', 2)",
           every(lambda r: pg_split_part(r["word"], "D", 2))),
        _v("split_part|case-negative", ["split_part"], "split_part(word, 'D', -1)",
           every(lambda r: pg_split_part(r["word"], "D", -1))),
        _v("concat_ws|skips-nulls", ["concat_ws"], "concat_ws('-', word, p, code)",
           every(lambda r: "-".join(v for v in (r["word"], r["p"], r["code"]) if v is not None))),
        _v("chr", ["chr"], "chr(cp)", every(lambda r: pg_chr(r["cp"]))),
        _v("ascii", ["ascii"], "ascii(s)", every(lambda r: pg_ascii(r["s"]))),
        _v("length|characters", ["length"], "length(s)", every(lambda r: None if s(r) is None else len(s(r)))),
        _v("substr", ["substr"], "substr(s, 2, 2)", every(lambda r: None if s(r) is None else s(r)[1:3])),
        _v("substring", ["substring"], "substring(s, 2)", every(lambda r: None if s(r) is None else s(r)[1:])),
        _v("replace", ["replace"], "replace(code, '_', '-')",
           every(lambda r: None if r["code"] is None else r["code"].replace("_", "-"))),
        _v("replace|case", ["replace"], "replace(word, 'A', '_')",
           every(lambda r: None if r["word"] is None else r["word"].replace("A", "_"))),
        _v("lower", ["lower"], "lower(word)", every(lambda r: None if r["word"] is None else r["word"].lower())),
        _v("upper", ["upper"], "upper(word)", every(lambda r: None if r["word"] is None else r["word"].upper())),
        _v("trim", ["trim", "||"], "trim(' ' || code || ' ')",
           every(lambda r: None if r["code"] is None else r["code"].strip(" "))),
        _v("ltrim", ["ltrim"], "ltrim(' ' || code)", every(lambda r: r["code"])),
        _v("rtrim", ["rtrim"], "rtrim(code || ' ')", every(lambda r: r["code"])),
        _v("concat", ["concat"], "concat(coalesce(word, '?'), '|')", every(lambda r: f"{r['word'] or '?'}|")),
        _v("concat|number-to-text", ["||"], "qty || '#'", every(lambda r: None if r["qty"] is None else f"{r['qty']}#")),
    ]


def _math_cases() -> list[Case]:
    x = lambda r: r["x"]  # noqa: E731

    def on_x(fn: Callable[[float], float]) -> dict[int, Any]:
        return every(lambda r: None if x(r) is None else fn(x(r)))

    return [
        _v("div|int", ["div"], "div(i, m)", every(lambda r: pg_div(r["i"], r["m"]))),
        _v("div|double", ["div"], "div(i + 0.5, m)",
           every(lambda r: None if r["i"] is None else pg_div(r["i"] + 0.5, r["m"]))),
        _v("pi", ["pi"], "pi() + 0 * id", every(lambda r: math.pi)),
        _v("sin", ["sin"], "sin(x)", on_x(math.sin)),
        _v("cos", ["cos"], "cos(x)", on_x(math.cos)),
        _v("tan", ["tan"], "tan(x)", on_x(math.tan)),
        _v("asin|domain", ["asin"], "asin(x)", every(lambda r: in_unit(math.asin, r["x"]))),
        _v("acos|domain", ["acos"], "acos(x)", every(lambda r: in_unit(math.acos, r["x"]))),
        _v("atan", ["atan"], "atan(x)", on_x(math.atan)),
        _v("atan2", ["atan2"], "atan2(y, x)",
           every(lambda r: None if r["x"] is None or r["y"] is None else math.atan2(r["y"], r["x"]))),
        _v("cbrt", ["cbrt"], "cbrt(x)", every(lambda r: cbrt(r["x"]))),
        _v("greatest|nulls", ["greatest"], "greatest(a, b, c)", every(lambda r: pg_greatest(r["a"], r["b"], r["c"]))),
        _v("least|nulls", ["least"], "least(a, b, c)", every(lambda r: pg_least(r["a"], r["b"], r["c"]))),
        _v("greatest|two", ["greatest"], "greatest(a, b)", every(lambda r: pg_greatest(r["a"], r["b"]))),
        _v("nullif", ["nullif"], "nullif(qty, 0)", every(lambda r: None if r["qty"] == 0 else r["qty"])),
        _v("coalesce", ["coalesce"], "coalesce(p, 'zz')", every(lambda r: "zz" if r["p"] is None else r["p"])),
        _v("ifnull", ["ifnull"], "ifnull(a, -100)", every(lambda r: -100 if r["a"] is None else r["a"])),
        _v("ln", ["ln", "abs", "+"], "ln(abs(x) + 1)", on_x(lambda v: math.log(abs(v) + 1))),
        _v("log10", ["log10"], "log10(abs(x) + 1)", on_x(lambda v: math.log10(abs(v) + 1))),
        _v("log2", ["log2"], "log2(abs(x) + 1)", on_x(lambda v: math.log2(abs(v) + 1))),
        _v("log|one-arg", ["log"], "log(abs(x) + 1)", on_x(lambda v: math.log10(abs(v) + 1))),
        _v("log|base", ["log"], "log(2, abs(x) + 1)", on_x(lambda v: math.log2(abs(v) + 1))),
        _v("exp", ["exp"], "exp(x)", on_x(math.exp)),
        _v("sqrt", ["sqrt"], "sqrt(abs(x))", on_x(lambda v: math.sqrt(abs(v)))),
        _v("pow", ["pow"], "pow(x, 2)", on_x(lambda v: v ** 2)),
        _v("power", ["power"], "power(2, a)", every(lambda r: None if r["a"] is None else 2.0 ** r["a"])),
        _v("floor", ["floor"], "floor(x)", on_x(math.floor)),
        _v("ceil", ["ceil"], "ceil(x)", on_x(math.ceil)),
        _v("ceiling", ["ceiling"], "ceiling(x)", on_x(math.ceil)),
        _v("round", ["round"], "round(x + 0.1)", on_x(lambda v: float(round(Decimal(str(v + 0.1)))))),
        _v("round|digits", ["round", "/"], "round(x / 3, 2)",
           on_x(lambda v: float(Decimal(str(v / 3)).quantize(Decimal("0.01"))))),
        _v("sign", ["sign"], "sign(x)", on_x(lambda v: (v > 0) - (v < 0))),
        _v("trunc", ["trunc"], "trunc(x)", on_x(lambda v: float(math.trunc(v)))),
        _v("mod|int", ["mod"], "mod(i, 3)", every(lambda r: None if r["i"] is None else int(math.fmod(r["i"], 3)))),
        _v("mod|double", ["mod"], "mod(x, 0.75)", on_x(lambda v: math.fmod(v, 0.75))),
        _v("percent|int", ["%"], "i % 3", every(lambda r: None if r["i"] is None else int(math.fmod(r["i"], 3)))),
        _v("percent|double", ["%"], "x % 2", on_x(lambda v: math.fmod(v, 2))),
        _v("arith|minus-times", ["-", "*"], "qty * 2 - 1", every(lambda r: None if r["qty"] is None else r["qty"] * 2 - 1)),
    ]


def _on_ts(fn: Callable[[datetime], Any]) -> dict[int, Any]:
    def at(row: dict[str, Any]) -> Any:
        t = _ts(row)
        return None if t is None else fn(t)
    return every(at)


def _date_cases() -> list[Case]:
    return [
        _v("date_trunc|month", ["date_trunc"], "date_trunc('month', ts)", every(_month)),
        _v("date_part", ["date_part"], "date_part('month', ts)", _on_ts(lambda t: t.month)),
        _v("date_diff", ["date_diff"], "date_diff('day', TIMESTAMP '2025-01-01 00:00:00', ts)",
           _on_ts(lambda t: (t.date() - date(2025, 1, 1)).days)),
        _v("date_add", ["date_add"], "date_add(ts, 2, 'day')", _on_ts(lambda t: t + timedelta(days=2))),
        _v("interval", ["interval"], "ts + interval(1, 'hour')", _on_ts(lambda t: t + timedelta(hours=1))),
        _f("current_date", ["current_date"], "date_diff('day', ts, current_date()) > 0",
           [rid for rid, r in ROWS.items() if r["ts"] is not None]),
        _f("now", ["now"], "ts < now()", [rid for rid, r in ROWS.items() if r["ts"] is not None]),
        _v("iif", ["iif"], "iif(qty > 1, 'many', 'few')",
           every(lambda r: "many" if r["qty"] is not None and r["qty"] > 1 else "few")),
    ]


def _like_cases() -> list[Case]:
    return [
        _f("like|escaped-underscore", ["LIKE", "like"], r"code LIKE 'a\_%'", [3]),
        _f("like|underscore-wildcard", ["LIKE"], "code LIKE 'a__1'", [4, 8]),
        _f("like|bracket-literal", ["LIKE"], "code LIKE '[ab]%'", [6]),
        _f("like|lone-backslash", ["LIKE"], r"word LIKE 'c:\x%'", [10]),
        _f("like|escaped-percent", ["LIKE"], r"tag LIKE '%5\%'", [6]),
        _f("like|escaped-backslash", ["LIKE"], r"word LIKE 'c:\\x%'", [10]),
        _f("like|column-pattern", ["LIKE", "||"], "code LIKE prefix || '%'", [1, 3, 4, 6, 7, 8, 10]),
        _f("like|glob-metacharacters", ["LIKE"], "code LIKE 'a*?%'", [8]),
        _f("like|percent-spans-newline", ["LIKE"], "notes LIKE 'paid%late'", [1, 4]),
        _f("not-like", ["LIKE"], "code NOT LIKE 'a%'", [6, 10]),
        _f("ilike|case-insensitive", ["ILIKE", "ilike"], "word ILIKE 'ADA%'", [3, 4]),
        _f("ilike|escaped-underscore", ["ILIKE"], r"code ILIKE 'A\_%'", [3]),
    ]


def _regex_cases() -> list[Case]:
    return [
        _f("regexp_like|partial", ["regexp_like"], "regexp_like(code, '[0-9]{3}')", [1]),
        _f("regexp_like|case-sensitive", ["regexp_like"], "regexp_like(word, '^paid$')", [1]),
        _f("regexp_like|flag-i", ["regexp_like"], "regexp_like(word, '^paid$', 'i')", [1, 2]),
        _f("op|tilde", ["~"], "word ~ '^ad'", [3, 8]),
        _f("op|tilde-star", ["~*"], "word ~* '^ad'", [3, 4, 8, 9]),
        _f("op|not-tilde", ["!~"], "word !~ '^ad'", [1, 2, 4, 6, 7, 9, 10]),
        _f("op|not-tilde-star", ["!~*"], "word !~* '^ad'", [1, 2, 6, 7, 10]),
        _f("regex|dot-newline", ["regexp_like"], "regexp_like(notes, 'paid.*late')", [1, 4]),
        _f("regex|dollar-true-end", ["regexp_like"], "regexp_like(code, '^ab$|^a1$')", [7]),
        _f("regex|dollar-isolated", ["regexp_like"], "regexp_like(code, '^ab$')", []),
        _f("regex|dollar-before-newline-escape", ["regexp_like"], r"regexp_like(code, '^ab\n?$')", [9]),
        _f("regex|caret-string-start", ["regexp_like"], "regexp_like(notes, '^late|^line')", [2, 8]),
        _f("regex|flag-i-trailing-newline", ["regexp_like"], "regexp_like(code, '^AB$|^A1$', 'i')", [7]),
        _f("regex|newline-escape", ["regexp_like"], r"regexp_like(code, '^ab\n$')", [9]),
        _f("regex|ascii-digit", ["regexp_like"], r"regexp_like(word, '^\d+$')", [6]),
        _f("regex|digit-in-brackets", ["regexp_like"], r"regexp_like(code, '^[\d_]+$')", [10]),
        _f("regex|ascii-word", ["regexp_like"], r"regexp_like(s, '^\w+$')", [1, 8, 9]),
        _f("regex|space-class", ["regexp_like"], r"regexp_like(notes, 'paid\slate')", [1, 4]),
        _f("regex|escaped-metachar", ["regexp_like"], r"regexp_like(tag, '\(555\)')", [1]),
        _f("regex|escaped-star", ["regexp_like"], r"regexp_like(code, 'a\*\?')", [8]),
        _f("regex|escaped-bracket", ["regexp_like"], r"regexp_like(code, '\[ab\]')", [6]),
        _f("regex|bounded-repeat", ["regexp_like"], "regexp_like(code, '^[a-z]{1,2}[0-9]{2,}$')", [1, 2]),
        _f("regex|variable-pattern", ["regexp_like"], "regexp_like(s, {p})", [9], variables={"p": "^a.c$"}),
        _v("regexp_extract|group", ["regexp_extract"], "regexp_extract(tag, '@(.+)$', 1)", only({3: "x.org"})),
        _v("regexp_extract|whole", ["regexp_extract"], "regexp_extract(code, '[a-z]+')",
           only({1: "ab", 2: "ab", 3: "a", 4: "abc", 6: "ab", 7: "a", 8: "a", 9: "ab"})),
        _v("regexp_extract|non-participating-group", ["regexp_extract"], "regexp_extract(code, '(x)?b', 1)",
           only({1: "", 2: "", 4: "", 6: "", 9: ""})),
        _v("regexp_extract|negated-digit", ["regexp_extract"], r"regexp_extract(word, '\D+')",
           only({1: "paid", 2: "PAID", 3: "ada", 4: "Ada", 7: "١٢٣", 8: "admin", 9: "Adele", 10: "c:\\x"})),
        _v("regexp_extract|first-line", ["regexp_extract"], r"regexp_extract(notes, '^[^\n]*')",
           only({1: "paid", 2: "line1", 3: "nothing", 4: "paid late", 6: "Paid Later", 7: "x", 8: "late paid",
                 9: "a", 10: ""})),
        _v("regexp_replace|every-match", ["regexp_replace"], "regexp_replace(tag, '[^0-9]', '')",
           every(lambda r: None if r["tag"] is None else "".join(ch for ch in r["tag"] if ch in "0123456789"))),
        _v("regexp_replace|groups", ["regexp_replace"], r"regexp_replace(s, '(\w+) (\w+)', '\2, \1')",
           every(lambda r: "Lovelace, Ada" if r["s"] == "Ada Lovelace" else r["s"])),
        _v("regexp_replace|literal-dollar-amp", ["regexp_replace"], "regexp_replace(tag, 'USD', '$ & co')",
           every(lambda r: None if r["tag"] is None else r["tag"].replace("USD", "$ & co"))),
        _v("regexp_replace|flag-i", ["regexp_replace"], "regexp_replace(word, 'a', 'X', 'i')",
           every(lambda r: None if r["word"] is None else r["word"].replace("a", "X").replace("A", "X"))),
        _v("regexp_replace|backslash", ["regexp_replace"], r"regexp_replace(word, ':', '\\')",
           every(lambda r: None if r["word"] is None else r["word"].replace(":", "\\"))),
        _v("regexp_replace|nine-groups", ["regexp_replace"],
           r"regexp_replace(s, '^(A)(B)(C)(D)(E)(x)?(x)?(x)?(x)?$', '\9\8\7\6\5\4\3\2\1')",
           every(lambda r: "EDCBA" if r["s"] == "ABCDE" else r["s"])),
        _v("regexp_replace|empty-group", ["regexp_replace"], r"regexp_replace(code, '(x)?b', '[\1]')",
           every(lambda r: None if r["code"] is None else r["code"].replace("b", "[]"))),
    ]


def _json_cases() -> list[Case]:
    ab = {1: "x", 9: NumText(value=1), 10: NumText(value=2.5)}
    flags = {1: "true", 9: "false"}
    return [
        _v("json_value|scalars-missing-nonscalars", ["json_value"], "json_value(payload, '$.a.b')", only(ab)),
        _v("json_value|boolean-text", ["json_value"], "json_value(payload, '$.ok')", only(flags)),
        _v("json_value|index-and-quoted-key", ["json_value"], "json_value(payload, '$.items[1].\"unit price\"')",
           only({7: "9.5"})),
        _v("json_value|returning-int", ["json_value", "RETURNING"], "json_value(payload, '$.qty' RETURNING INT)",
           only({1: 2, 2: 3})),
        _v("json_value|returning-int-zero", ["RETURNING"], "json_value(payload, '$.n' RETURNING INT)",
           only({1: 0, 9: 12, 10: 7})),
        _v("json_value|returning-boolean", ["RETURNING"], "json_value(payload, '$.f' RETURNING BOOLEAN)",
           only({1: False, 9: True})),
        _v("json_value|returning-double", ["RETURNING"], "json_value(payload, '$.a.b' RETURNING DOUBLE)",
           only({9: 1.0, 10: 2.5})),
        _v("json_value|returning-date", ["RETURNING"], "json_value(payload, '$.d' RETURNING DATE)",
           only({10: date(2024, 2, 29)})),
        _v("json_value|returning-timestamp", ["RETURNING"], "json_value(payload, '$.t' RETURNING TIMESTAMP)",
           only({10: datetime(2024, 2, 29, 10, 0)})),
        _v("json_value|returning-text", ["RETURNING"], "json_value(payload, '$.ok' RETURNING TEXT)", only(flags)),
        _v("json_value|boolean-to-int-is-null", ["RETURNING"], "json_value(payload, '$.ok' RETURNING INT)", only({})),
        _v("json_value|object-to-int-is-null", ["RETURNING"], "json_value(payload, '$.a' RETURNING INT)", only({})),
        _v("arrow|postgres-chain", ["->", "->>"], "payload -> 'a' -> 0 ->> 'b'", only({7: "x"})),
        _v("arrow|mysql-path", ["->>"], "payload ->> '$.a.b'", only(ab)),
        _v("arrow|key", ["->>"], "payload ->> 'ok'", only(flags)),
        _v("arrow|precedence", ["->>", "||"], "payload ->> 'ok' || '!'", only({1: "true!", 9: "false!"})),
        _v("json_extract|object-is-null", ["json_extract"], "json_extract(payload, '$.a')", only({})),
        _v("json_extract|scalar", ["json_extract"], "json_extract(payload, '$.ok')", only(flags)),
        _v("json_extract_scalar", ["json_extract_scalar"], "json_extract_scalar(payload, '$.a.b')", only(ab)),
        _v("json_extract_path_text", ["json_extract_path_text"], "json_extract_path_text(payload, 'a', 'b')", only(ab)),
        _v("jsonb_extract_path_text", ["jsonb_extract_path_text"], "jsonb_extract_path_text(payload, 'a', 'b')",
           only(ab)),
    ]


def _txt_keeps(pred: Callable[[float, dict[str, Any]], bool]) -> frozenset[int]:
    """Ids whose ``num_txt`` reads as a number satisfying ``pred``."""
    def at(row: dict[str, Any]) -> bool:
        v = num_text(row["num_txt"])
        return v is not None and pred(v, row)
    return keeps(at)


def _on_txt(fn: Callable[[float, dict[str, Any]], Any]) -> dict[int, Any]:
    def at(row: dict[str, Any]) -> Any:
        v = num_text(row["num_txt"])
        return None if v is None else fn(v, row)
    return every(at)


#: ``json_value(doc, '$.v' RETURNING <type>)`` per row: null, missing, object, array, "x", 0, false,
#: a date string, a timestamp string, 12.
RETURNING_GRID: dict[str, dict[int, Any]] = {
    "INT": {6: 0, 10: 12},
    "DOUBLE": {6: 0.0, 10: 12.0},
    "BOOLEAN": {6: ANY, 7: False, 10: ANY},
    "TEXT": {5: "x", 6: NumText(value=0), 7: "false", 8: "2024-02-29", 9: "2024-02-29 10:00:00",
             10: NumText(value=12)},
    "DATE": {8: date(2024, 2, 29), 9: ANY},
    "TIMESTAMP": {8: ANY, 9: datetime(2024, 2, 29, 10, 0)},
}


def _returning_cases() -> list[Case]:
    return [_v(f"json_value|returning-grid-{target.lower()}", ["RETURNING"],
               f"json_value(doc, '$.v' RETURNING {target})", only(expected))
            for target, expected in RETURNING_GRID.items()]


def _coercion_cases() -> list[Case]:
    return [
        _f("coerce|gt-literal", [">"], "num_txt > 100", _txt_keeps(lambda v, r: v > 100)),
        _f("coerce|eq-column", ["="], "num_txt = qty", _txt_keeps(lambda v, r: r["qty"] is not None and v == r["qty"])),
        _f("coerce|lt-numeric-not-lexicographic", ["<"], "num_txt < 9", _txt_keeps(lambda v, r: v < 9)),
        _f("coerce|le", ["<="], "num_txt <= 10", _txt_keeps(lambda v, r: v <= 10)),
        _f("coerce|ge", [">="], "num_txt >= 99", _txt_keeps(lambda v, r: v >= 99)),
        _f("coerce|ne", ["<>"], "num_txt <> 1", _txt_keeps(lambda v, r: v != 1)),
        _f("coerce|in", ["IN"], "num_txt IN (1, 2)", _txt_keeps(lambda v, r: v in (1.0, 2.0))),
        _f("coerce|between", ["BETWEEN"], "num_txt BETWEEN 1 AND 10", _txt_keeps(lambda v, r: 1 <= v <= 10)),
        _f("coerce|is-distinct-from", ["IS DISTINCT FROM"], "num_txt IS DISTINCT FROM 99",
           keeps(lambda r: num_text(r["num_txt"]) != 99)),
        _f("coerce|literal-text", [">"], "qty > '1'", keeps(lambda r: r["qty"] is not None and r["qty"] > 1)),
        _v("coerce|times", ["*"], "num_txt * 2", _on_txt(lambda v, r: v * 2)),
        _v("coerce|plus-column", ["+"], "num_txt + qty", _on_txt(lambda v, r: None if r["qty"] is None else v + r["qty"])),
        _v("coerce|minus", ["-"], "num_txt - 1", _on_txt(lambda v, r: v - 1)),
        _v("coerce|divide", ["/"], "num_txt / 2", _on_txt(lambda v, r: v / 2)),
        _v("coerce|modulo", ["%"], "num_txt % 7", _on_txt(lambda v, r: math.fmod(v, 7))),
        _f("coerce|exact-bigint", ["="], "TRY_CAST(big AS BIGINT) = 12345678901234567", [1]),
        _f("coerce|exact-bigint-off-by-one", ["="], "TRY_CAST(big AS BIGINT) = 12345678901234568", []),
        _v("coerce|boolean-stays-numeric", ["*"], "(qty > 1) * 2",
           every(lambda r: None if r["qty"] is None else 2 * int(r["qty"] > 1))),
        _v("coerce|greatest", ["greatest"], "greatest(num_txt, 5)",
           every(lambda r: pg_greatest(num_text(r["num_txt"]), 5))),
        _v("coerce|least", ["least"], "least(num_txt, 5)", every(lambda r: pg_least(num_text(r["num_txt"]), 5))),
    ]


def library_cases() -> list[Case]:
    return [*_string_cases(), *_math_cases(), *_date_cases(), *_like_cases(), *_regex_cases(),
            *_json_cases(), *_returning_cases(), *_coercion_cases()]


def required_names() -> frozenset[str]:
    return frozenset(SCALAR_SPECS) | NEW_FUNCTIONS | SPELLINGS | OPERATORS


# --------------------------------------------------------------------------- #
# Assertions
# --------------------------------------------------------------------------- #
def assert_cell(actual: Any, expected: Any, *, where: str) -> None:
    if isinstance(expected, Unspecified):
        return
    if expected is None:
        assert actual is None, f"{where}: expected NULL, got {actual!r}"
        return
    assert actual is not None, f"{where}: expected {expected!r}, got NULL"
    if isinstance(expected, NumText):
        assert isinstance(actual, str), f"{where}: expected TEXT, got {actual!r}"
        assert float(actual) == expected.value, f"{where}: expected {expected.value}, got {actual!r}"
    elif isinstance(expected, bool):
        assert actual in (True, False, 0, 1), f"{where}: expected a boolean, got {actual!r}"
        assert bool(actual) is expected, f"{where}: expected {expected}, got {actual!r}"
    elif isinstance(expected, (date, datetime)):
        got = as_temporal(actual)
        got_day = got.date() if isinstance(got, datetime) and not isinstance(expected, datetime) else got
        assert got_day == expected, f"{where}: expected {expected}, got {actual!r}"
    elif isinstance(expected, str):
        assert actual == expected, f"{where}: expected {expected!r}, got {actual!r}"
    elif isinstance(expected, int):
        assert not isinstance(actual, (str, bool)), f"{where}: expected a number, got {actual!r}"
        assert Decimal(str(actual)) == Decimal(expected), f"{where}: expected {expected}, got {actual!r}"
    else:
        assert not isinstance(actual, (str, bool)), f"{where}: expected a number, got {actual!r}"
        assert math.isclose(float(actual), float(expected), rel_tol=1e-9, abs_tol=1e-12), (
            f"{where}: expected {expected}, got {actual!r}")


def assert_case(data: list[dict[str, Any]], case: Case) -> None:
    if case.keeps is not None:
        got = {int(r[f"{case.model}.id"]) for r in data}
        assert got == set(case.keeps), f"{case.case_id}: kept {sorted(got)}, expected {sorted(case.keeps)}"
        return
    assert case.values is not None
    got = {int(r[f"{case.model}.id"]): r[f"{case.model}.v"] for r in data}
    assert set(got) == set(case.values), f"{case.case_id}: row ids {sorted(got)}"
    for rid, expected in case.values.items():
        assert_cell(got[rid], expected, where=f"{case.case_id} id={rid}")


def lib_query(*dims: Any, **kw: Any) -> SlayerQuery:
    """A row-level ``lib`` query: ``id`` plus ``dims`` (an expression string becomes the ``v`` dimension)."""
    dimensions: list[Any] = ["id"] + [{"expression": d, "name": "v"} if isinstance(d, str) else d for d in dims]
    return SlayerQuery.model_validate({"source_model": "lib", "distinct_dimension_values": False,
                                       "dimensions": dimensions, **kw})


async def emit_lib(query: SlayerQuery, *, dialect: str, models: Optional[list[SlayerModel]] = None) -> str:
    """Dry-run SQL for a ``lib`` query on ``dialect`` (asserts it parses there)."""
    return await emit(query, dialect=dialect, models=models or [lib_model()])


def lib_values(resp: SlayerResponse, name: str = "v") -> dict[int, Any]:
    return {int(r["lib.id"]): r[f"lib.{name}"] for r in resp.data}


def lib_ids(resp: SlayerResponse) -> set[int]:
    return {int(r["lib.id"]) for r in resp.data}


def patch_profile(monkeypatch: Any, *, version: Optional[tuple[int, int]] = None, fail: bool = False) -> list[int]:
    """Stub the server-profile probe; the returned list records each probe."""
    calls: list[int] = []

    async def fake(_self: Any) -> ServerProfile:
        calls.append(1)
        if fail:
            raise ConnectionError("probe failed")
        return ServerProfile(version=version)

    monkeypatch.setattr(SlayerSQLClient, "server_profile", fake)
    return calls


async def run_case(engine: SlayerQueryEngine, case: Case) -> None:
    resp = await engine.execute(case.query())
    try:
        assert_case(resp.data, case)
    except AssertionError as exc:
        raise AssertionError(f"{exc}\nSQL:\n{resp.sql}") from None


# --------------------------------------------------------------------------- #
# Seeding
# --------------------------------------------------------------------------- #
@contextlib.asynccontextmanager
async def lib_engine(backend: str, *, models: Optional[list[SlayerModel]] = None) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, [LIB]), models=models or [lib_model()],
    ) as (engine, _db):
        yield engine


SERVER_LIB_TYPES: dict[str, dict[str, str]] = {
    **SERVER_TYPES,
    "postgres": {**SERVER_TYPES["postgres"], "TIMESTAMP": "TIMESTAMP"},
    "mysql": {**SERVER_TYPES["mysql"], "TIMESTAMP": "DATETIME"},
    "snowflake": {"INT": "NUMBER(38,0)", "DOUBLE": "FLOAT", "TEXT": "TEXT", "TIMESTAMP": "TIMESTAMP_NTZ"},
    "bigquery": {"INT": "INT64", "DOUBLE": "FLOAT64", "TEXT": "STRING", "TIMESTAMP": "DATETIME"},
}
_TEMPORAL_PREFIX = {"trino": "TIMESTAMP", "bigquery": "DATETIME"}
_SUFFIX = {"clickhouse": " ENGINE = MergeTree ORDER BY tuple()"}


def server_literal(value: Any, *, kind: str, dialect: str) -> str:
    """``value`` as a SQL literal on ``dialect`` (backslash-escaping engines and N'' strings handled)."""
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return repr(value)
    text = str(value)
    if dialect in ("mysql", "clickhouse", "snowflake", "bigquery"):
        text = text.replace("\\", "\\\\")
    if dialect == "bigquery":
        text = text.replace("'", "\\'").replace("\n", "\\n")
    else:
        text = text.replace("'", "''")
    if kind == "TIMESTAMP" and dialect in _TEMPORAL_PREFIX:
        return f"{_TEMPORAL_PREFIX[dialect]} '{text}'"
    return f"N'{text}'" if dialect == "tsql" else f"'{text}'"


def server_table_statements(table: TableSpec, *, dialect: str, prefix: str = "") -> list[str]:
    """CREATE + one multi-row INSERT for ``table`` on a server ``dialect`` (``prefix`` qualifies the name)."""
    types = SERVER_LIB_TYPES[dialect]
    cols = ", ".join(f"{c} {types[k]}" for c, k in table.columns)
    kinds = [k for _, k in table.columns]
    rows = ", ".join(
        "(" + ", ".join(server_literal(v, kind=k, dialect=dialect) for v, k in zip(row, kinds)) + ")"
        for row in table.rows
    )
    name = f"{prefix}{table.name}"
    return [f"CREATE TABLE {name} ({cols}){_SUFFIX.get(dialect, '')}", f"INSERT INTO {name} VALUES {rows}"]


# --------------------------------------------------------------------------- #
# Native JSON columns (ingested, then read with json_value)
# --------------------------------------------------------------------------- #
DOCS_ROWS: list[tuple[int, Optional[str]]] = [
    (1, '{"a": 1, "s": "x"}'), (2, '{"a": "2", "s": null}'), (3, '{"a": "x", "s": {"t": 1}}'),
    (4, "{}"), (5, None),
]
DOCS_S = {1: "x", 2: None, 3: None, 4: None, 5: None}
DOCS_A_INT = {1: 1, 2: 2, 3: None, 4: None, 5: None}


async def check_native_json(engine: SlayerQueryEngine, *, model: str = "docs", column: str = "payload") -> None:
    """``json_value`` over an ingested native-JSON column: TEXT and ``RETURNING INT`` reads."""
    for expr, expected in (
        (f"json_value({column}, '$.s')", DOCS_S),
        (f"json_value({column}, '$.a' RETURNING INT)", DOCS_A_INT),
        (f"{column} ->> 's'", DOCS_S),
    ):
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": model, "distinct_dimension_values": False,
            "dimensions": ["id", {"expression": expr, "name": "v"}],
        }))
        got = {int(r[f"{model}.id"]): r[f"{model}.v"] for r in resp.data}
        assert set(got) == set(expected), (expr, resp.sql)
        for rid, value in expected.items():
            assert_cell(got[rid], value, where=f"{expr} id={rid}")


REGEX_NAMES = frozenset({"regexp_like", "regexp_extract", "regexp_replace", "~", "~*", "!~", "!~*"})


async def check_library(engine: SlayerQueryEngine, *, gated: frozenset[str] = frozenset(),
                        refusal: str = "", cases: Optional[list[Case]] = None) -> None:
    """Every case (default: ``library_cases()``) on a live engine; a case covering a ``gated`` name must be
    refused naming ``refusal``.

    Failures are collected so one run reports them all.
    """
    failures: list[str] = []
    for case in library_cases() if cases is None else cases:
        try:
            if gated & set(case.covers):
                problem = await _refusal_problem(engine, case, refusal=refusal)
            else:
                await run_case(engine, case)
                problem = None
        except Exception as exc:  # noqa: BLE001 — report every failing case, not the first
            problem = f"{type(exc).__name__}: {exc}"
        if problem:
            failures.append(f"{case.case_id}: {problem}")
    assert not failures, "\n\n".join(failures)


async def _refusal_problem(engine: SlayerQueryEngine, case: Case, *, refusal: str) -> str | None:
    try:
        await engine.execute(case.query())
    except SlayerError as exc:
        return None if refusal in str(exc) else f"refusal without {refusal!r}: {exc}"
    return f"expected a plan-time refusal naming {refusal}"


# --------------------------------------------------------------------------- #
# List columns: the ``contractors`` table and its list-function cases
# --------------------------------------------------------------------------- #
LIST_FUNCTIONS = frozenset({"contains", "contains_any", "contains_all", "length"})
NATIVE_LIST_DIALECTS = ("postgres", "duckdb", "clickhouse", "trino", "snowflake", "bigquery")
#: Dialects without native lists → the database name their refusal carries.
NO_LIST_DATABASES = {"sqlite": "SQLite", "mysql": "MySQL", "tsql": "SQL Server"}

#: ``(name, kind)``; ``T[]`` is a list of ``T``.
CONTRACTOR_COLUMNS: tuple[tuple[str, str], ...] = (
    ("id", "INT"), ("name", "TEXT"), ("team", "TEXT"), ("rate", "INT"), ("city", "TEXT"), ("pick", "TEXT"),
    ("skills", "TEXT[]"), ("required", "TEXT[]"), ("scores", "INT[]"),
)
CONTRACTORS: tuple[dict[str, Any], ...] = (
    {"id": 1, "name": "Ann", "team": "A", "rate": 10, "city": "Oslo", "pick": "sql",
     "skills": ["go", "sql", "go"], "required": ["rust", "sql"], "scores": [3, 9]},
    {"id": 2, "name": "Bob", "team": "A", "rate": 20, "city": None, "pick": None,
     "skills": ["sql", None], "required": ["go"], "scores": [1]},
    {"id": 3, "name": "Cid", "team": "B", "rate": 30, "city": "Rome", "pick": "go",
     "skills": [], "required": [], "scores": []},
    {"id": 4, "name": "Dee", "team": "B", "rate": 40, "city": "Oslo", "pick": "go",
     "skills": None, "required": ["go"], "scores": None},
    {"id": 5, "name": "Eve", "team": "B", "rate": 50, "city": "Oslo", "pick": "rust",
     "skills": ["go"], "required": None, "scores": [5, None]},
)
LIST_COLUMNS = tuple(c for c, k in CONTRACTOR_COLUMNS if k.endswith("[]"))


class ListStorage(BaseModel):
    """What a backend can store: some keep a NULL list as ``[]``, some keep no NULL element."""

    model_config = ConfigDict(frozen=True)
    null_list_is_empty: bool = False
    drops_null_elements: bool = False


LIST_STORAGE: dict[str, ListStorage] = {
    "bigquery": ListStorage(null_list_is_empty=True, drops_null_elements=True),
    "clickhouse": ListStorage(null_list_is_empty=True),
    "snowflake": ListStorage(drops_null_elements=True),
}


def stored_contractors(storage: ListStorage = ListStorage()) -> list[dict[str, Any]]:
    """``CONTRACTORS`` as ``storage`` holds them."""
    def store(value: Optional[list[Any]]) -> Optional[list[Any]]:
        if value is None:
            return [] if storage.null_list_is_empty else None
        return [e for e in value if e is not None] if storage.drops_null_elements else list(value)
    return [{k: store(v) if k in LIST_COLUMNS else v for k, v in row.items()} for row in CONTRACTORS]


def contractors_model(*, data_source: str = "test", sql_table: str = "contractors", keyed: bool = True) -> SlayerModel:
    scalars = {"INT": DataType.INT, "TEXT": DataType.TEXT}
    columns = [
        Column(name=c, type=DataType.ARRAY, element_type=scalars[k[:-2]]) if k.endswith("[]")
        else Column(name=c, type=scalars[k], primary_key=keyed and c == "id")
        for c, k in CONTRACTOR_COLUMNS
    ]
    return SlayerModel(name="contractors", sql_table=sql_table, data_source=data_source, columns=columns)


# The list functions' meaning: a list's elements are its non-NULL elements.
def elements(value: list[Any]) -> list[Any]:
    return [e for e in value if e is not None]


def list_contains(value: Optional[list[Any]], x: Any) -> Optional[bool]:
    return None if value is None or x is None else x in elements(value)


def list_contains_any(value: Optional[list[Any]], other: Optional[list[Any]]) -> Optional[bool]:
    if value is None or other is None:
        return None
    return bool(set(elements(value)) & set(elements(other)))


def list_contains_all(value: Optional[list[Any]], other: Optional[list[Any]]) -> Optional[bool]:
    if value is None or other is None:
        return None
    return set(elements(other)) <= set(elements(value))


def list_length(value: Optional[list[Any]]) -> Optional[int]:
    return None if value is None else len(elements(value))


def list_cases(storage: ListStorage = ListStorage()) -> list[Case]:
    """The list-function cases over ``contractors`` as ``storage`` holds it."""
    rows = {r["id"]: r for r in stored_contractors(storage)}

    def v(case_id: str, covers: Iterable[str], expr: str, fn: Callable[[dict[str, Any]], Any], **kw: Any) -> Case:
        return Case(model="contractors", case_id=f"list|{case_id}", covers=tuple(covers), expr=expr,
                    values={rid: fn(r) for rid, r in rows.items()}, **kw)

    def f(case_id: str, covers: Iterable[str], expr: str, fn: Callable[[dict[str, Any]], Any], **kw: Any) -> Case:
        return Case(model="contractors", case_id=f"list|{case_id}", covers=tuple(covers), expr=expr,
                    keeps=frozenset(rid for rid, r in rows.items() if fn(r) is True), **kw)

    def length_plus_one(r: dict[str, Any]) -> Optional[int]:
        n = list_length(r["skills"])
        return None if n is None else n + 1

    return [
        v("contains", ["contains"], "contains(skills, 'go')", lambda r: list_contains(r["skills"], "go")),
        v("contains-column-value", ["contains"], "contains(skills, pick)",
          lambda r: list_contains(r["skills"], r["pick"])),
        v("contains-numbers", ["contains"], "contains(scores, 9)", lambda r: list_contains(r["scores"], 9)),
        v("contains-text-literal-on-numbers", ["contains"], "contains(scores, '5')",
          lambda r: list_contains(r["scores"], 5)),
        f("contains-filter", ["contains"], "contains(skills, 'go')", lambda r: list_contains(r["skills"], "go")),
        f("not-contains", ["contains"], "NOT contains(skills, 'go')",
          lambda r: list_contains(r["skills"], "go") is False),
        f("contains-text-unchanged", ["contains"], "contains(team, 'A')", lambda r: "A" in r["team"]),
        v("contains_any", ["contains_any"], "contains_any(skills, ['go', 'sql'])",
          lambda r: list_contains_any(r["skills"], ["go", "sql"])),
        v("contains_any-numbers", ["contains_any"], "contains_any(scores, [1, 9])",
          lambda r: list_contains_any(r["scores"], [1, 9])),
        v("contains_any-list-column", ["contains_any"], "contains_any(skills, required)",
          lambda r: list_contains_any(r["skills"], r["required"])),
        v("contains_any-empty", ["contains_any"], "contains_any(skills, [])",
          lambda r: list_contains_any(r["skills"], [])),
        f("contains_any-variable", ["contains_any"], "contains_any(skills, {wanted})",
          lambda r: list_contains_any(r["skills"], ["rust", "go"]), variables={"wanted": ["rust", "go"]}),
        f("contains_any-empty-variable", ["contains_any"], "contains_any(skills, {wanted})",
          lambda r: list_contains_any(r["skills"], []), variables={"wanted": []}),
        v("contains_all", ["contains_all"], "contains_all(skills, ['go', 'sql'])",
          lambda r: list_contains_all(r["skills"], ["go", "sql"])),
        v("contains_all-null-element", ["contains_all"], "contains_all(skills, ['sql'])",
          lambda r: list_contains_all(r["skills"], ["sql"])),
        v("contains_all-list-column", ["contains_all"], "contains_all(skills, required)",
          lambda r: list_contains_all(r["skills"], r["required"])),
        v("contains_all-empty", ["contains_all"], "contains_all(skills, [])",
          lambda r: list_contains_all(r["skills"], [])),
        f("contains_all-variable", ["contains_all"], "contains_all(skills, {wanted})",
          lambda r: list_contains_all(r["skills"], ["go", "sql"]), variables={"wanted": ["go", "sql"]}),
        f("contains_all-empty-variable", ["contains_all"], "contains_all(skills, {wanted})",
          lambda r: list_contains_all(r["skills"], []), variables={"wanted": []}),
        v("length", ["length"], "length(skills)", lambda r: list_length(r["skills"])),
        v("length-null-elements", ["length"], "length(scores)", lambda r: list_length(r["scores"])),
        v("length-arithmetic", ["length", "+"], "length(skills) + 1", length_plus_one),
        v("length-text-unchanged", ["length"], "length(name)", lambda r: len(r["name"])),
        f("is-null", ["IS NULL"], "skills IS NULL", lambda r: r["skills"] is None),
        f("is-not-null", ["IS NULL"], "skills IS NOT NULL", lambda r: r["skills"] is not None),
    ]


def seed_contractors(backend: str, db_path: str, *, extra: Iterable[str] = ()) -> None:
    """``contractors`` on an in-process backend (SQLite holds lists as JSON text), then the ``extra`` statements."""
    names = ", ".join(c for c, _ in CONTRACTOR_COLUMNS)
    marks = ", ".join("?" for _ in CONTRACTOR_COLUMNS)
    if backend == "sqlite":
        cols = ", ".join(f"{c} {'INTEGER' if k == 'INT' else 'TEXT'}" for c, k in CONTRACTOR_COLUMNS)
        rows = [tuple(json.dumps(r[c]) if c in LIST_COLUMNS and r[c] is not None else r[c] for c, _ in
                      CONTRACTOR_COLUMNS) for r in CONTRACTORS]
        with transaction(db_path) as con:
            con.execute(f"CREATE TABLE contractors ({cols})")
            con.executemany(f"INSERT INTO contractors ({names}) VALUES ({marks})", rows)
            for stmt in extra:
                con.execute(stmt)
        return
    duckdb = pytest.importorskip("duckdb")
    types = {"INT": "INTEGER", "TEXT": "VARCHAR", "TEXT[]": "VARCHAR[]", "INT[]": "INTEGER[]"}
    cols = ", ".join(f"{c} {types[k]}" for c, k in CONTRACTOR_COLUMNS)
    with contextlib.closing(duckdb.connect(db_path)) as con:
        con.execute(f"CREATE TABLE contractors ({cols})")
        con.executemany(f"INSERT INTO contractors ({names}) VALUES ({marks})",
                        [tuple(r[c] for c, _ in CONTRACTOR_COLUMNS) for r in CONTRACTORS])
        for stmt in extra:
            con.execute(stmt)


@contextlib.asynccontextmanager
async def contractors_engine(backend: str = "duckdb", *, models: Optional[list[SlayerModel]] = None,
                             extra: Iterable[str] = ()) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_contractors(backend, p, extra=extra),
        models=models or [contractors_model()],
    ) as (engine, _db):
        yield engine


async def emit_contractors(query: SlayerQuery, *, dialect: str, models: Optional[list[SlayerModel]] = None) -> str:
    """Dry-run SQL for a ``contractors`` query on ``dialect`` (asserts it parses there)."""
    return await emit(query, dialect=dialect, models=models or [contractors_model()])


_SERVER_LIST_TYPES: dict[str, dict[str, str]] = {
    "postgres": {"INT": "INTEGER", "TEXT": "TEXT", "TEXT[]": "TEXT[]", "INT[]": "INTEGER[]"},
    "trino": {"INT": "INTEGER", "TEXT": "VARCHAR", "TEXT[]": "ARRAY(VARCHAR)", "INT[]": "ARRAY(INTEGER)"},
    "clickhouse": {"INT": "Int32", "TEXT": "Nullable(String)", "TEXT[]": "Array(Nullable(String))",
                   "INT[]": "Array(Nullable(Int32))"},
    "bigquery": {"INT": "INT64", "TEXT": "STRING", "TEXT[]": "ARRAY<STRING>", "INT[]": "ARRAY<INT64>"},
    # Untyped ARRAY: the element type is inferred at ingest.
    "snowflake": {"INT": "NUMBER(38,0)", "TEXT": "TEXT", "TEXT[]": "ARRAY", "INT[]": "ARRAY"},
}


def _server_list_literal(value: Optional[list[Any]], *, kind: str, dialect: str) -> str:
    if value is None:
        return "NULL"
    element = kind[:-2]
    items = ", ".join(server_literal(e, kind=element, dialect=dialect) for e in value)
    if dialect == "postgres":
        return f"ARRAY[{items}]::{_SERVER_LIST_TYPES['postgres'][kind]}"
    if dialect == "trino":
        return f"CAST(ARRAY[{items}] AS {_SERVER_LIST_TYPES['trino'][kind]})"
    if dialect == "bigquery":
        return f"{_SERVER_LIST_TYPES['bigquery'][kind]}[{items}]"
    if dialect == "snowflake":
        return f"ARRAY_CONSTRUCT({items})"
    return f"[{items}]"


def contractors_statements(*, dialect: str, prefix: str = "") -> list[str]:
    """CREATE + INSERT of ``contractors`` (as ``dialect`` stores it) on a server backend."""
    types = _SERVER_LIST_TYPES[dialect]
    cols = ", ".join(f"{c} {types[k]}" for c, k in CONTRACTOR_COLUMNS)

    def lit(value: Any, kind: str) -> str:
        if kind.endswith("[]"):
            return _server_list_literal(value, kind=kind, dialect=dialect)
        return server_literal(value, kind=kind, dialect=dialect)

    rows = [[lit(r[c], k) for c, k in CONTRACTOR_COLUMNS] for r in stored_contractors(LIST_STORAGE.get(dialect, ListStorage()))]
    name = f"{prefix}contractors"
    if dialect == "snowflake":  # ARRAY_CONSTRUCT is not allowed in VALUES
        body = " UNION ALL ".join("SELECT " + ", ".join(row) for row in rows)
        insert = f"INSERT INTO {name} {body}"
    else:
        insert = f"INSERT INTO {name} VALUES " + ", ".join("(" + ", ".join(row) + ")" for row in rows)
    return [f"CREATE TABLE {name} ({cols}){_SUFFIX.get(dialect, '')}", insert]
