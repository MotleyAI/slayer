"""The portable library's render path: each library call through its typed dialect hook."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Callable, List, Optional, Tuple

from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.json_path import JsonPath, parse_json_path
from slayer.core.keys import CastKey, ScalarCallKey
from slayer.core.like_pattern import parse_like
from slayer.core.regex_pattern import parse_regex, parse_replacement
from slayer.sql.dialects.base import (
    LIBRARY_FUNCTIONS,
    REGEX_LIBRARY,
    SqlDialect,
    any_null,
    case,
    is_extremum_fn,
    is_math_fn,
    is_text_fn,
    lit,
    literal_int,
    require_library,
)


def check_library_support(dialect: SqlDialect, keys: Iterable[object]) -> None:
    """Refuse a library call among ``keys`` that ``dialect`` does not translate, then a ``TRY_CAST`` (one the
    call itself wraps, as ``RETURNING`` does, names the call)."""
    keys = list(keys)
    for key in keys:
        if isinstance(key, ScalarCallKey) and key.name in LIBRARY_FUNCTIONS:
            require_library(dialect, key.name)
    if any(isinstance(key, CastKey) and key.try_ for key in keys):
        require_library(dialect, "try_cast")


def _text(node: Expression) -> str:
    if not (isinstance(node, exp.Literal) and node.is_string):
        raise RuntimeError(f"a library text argument reached render as {node.sql()!r}; the binder checks it.")
    return node.this


def _int(node: Expression) -> int:
    value = literal_int(node)
    if value is None:
        raise RuntimeError(f"a library integer argument reached render as {node.sql()!r}; the binder checks it.")
    return value


def json_input(value: Expression, *, dialect: SqlDialect) -> bool:
    """Whether ``json_value``'s rendered input is the engine's native JSON type (a cast to it)."""
    if not isinstance(value, exp.Cast):
        return False
    return value.to.sql(dialect=dialect.sqlglot_name).split("(")[0].upper() in dialect.json_db_types


def json_operands(
    key: ScalarCallKey, *, part: Callable[[Any], Expression], dialect: SqlDialect,
) -> Tuple[Expression, JsonPath, bool]:
    """``json_value``'s rendered input, its path, and whether the input is the engine's native JSON type."""
    require_library(dialect, "json_value")
    value = part(key.args[0])
    return value, parse_json_path(_text(part(key.args[1]))), json_input(value, dialect=dialect)


def render_library_call(*, name: str, args: List[Expression], dialect: SqlDialect) -> Optional[Expression]:
    """``name(args)`` lowered through its dialect hook; ``None`` for a non-library scalar."""
    if name not in LIBRARY_FUNCTIONS:
        return None
    require_library(dialect, name)
    if is_text_fn(name):
        return dialect.build_text_call(name, tuple(args))
    if is_math_fn(name):
        return dialect.build_math_call(name, tuple(args))
    if is_extremum_fn(name):
        return dialect.build_extremum(name, tuple(args))
    if name == "split_part":
        return _split_part(args, dialect=dialect)
    if name in ("like", "ilike"):
        pattern = args[1]
        written = parse_like(pattern.this) if isinstance(pattern, exp.Literal) and pattern.is_string else pattern
        return dialect.build_like(args[0], written, name == "ilike")
    if name == "json_value":
        return dialect.build_json_value(args[0], parse_json_path(_text(args[1])), json_input(args[0], dialect=dialect))
    if name in REGEX_LIBRARY:
        return _render_regex(name=name, args=args, dialect=dialect)
    raise RuntimeError(f"library function {name!r} has no render path.")


def _split_part(args: List[Expression], *, dialect: SqlDialect) -> Expression:
    """An empty delimiter leaves the whole value as the one field (Postgres), whatever the engine's own split."""
    value, delimiter, index = args[0], args[1], _int(args[2])
    whole = value.copy() if abs(index) == 1 else case((exp.Not(this=any_null(value)), lit("")))
    empty = exp.EQ(this=dialect.char_length(delimiter), expression=lit(0))
    return case((empty, whole), default=dialect.build_split_part(value, delimiter, index))


def _render_regex(*, name: str, args: List[Expression], dialect: SqlDialect) -> Expression:
    pattern = parse_regex(_text(args[1]))
    if name == "regexp_extract":
        return dialect.build_regexp_extract(args[0], pattern, _int(args[2]) if len(args) > 2 else 0, False)
    if name == "regexp_like":
        return dialect.build_regexp_like(args[0], pattern, len(args) > 2)
    replacement = parse_replacement(_text(args[2]), groups=pattern.groups)
    return dialect.build_regexp_replace(args[0], pattern, replacement, len(args) > 3)


__all__ = ["LIBRARY_FUNCTIONS", "check_library_support", "json_input", "json_operands", "render_library_call"]
