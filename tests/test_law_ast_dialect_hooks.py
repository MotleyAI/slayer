"""Law (sql.arc42.md §3.1): an AST-returning dialect hook takes only typed, non-str, non-callable operands."""

from __future__ import annotations

import collections.abc
import inspect
import types
import typing
from collections.abc import Callable
from typing import Literal, Optional, Union

import pytest
import sqlalchemy as sa
from sqlglot import exp

from slayer.core.enums import TimeGranularity
from slayer.core.models import DatasourceConfig
from slayer.sql.dialects import _ALL_DIALECTS, SqlDialect

_LOCALNS = {"DatasourceConfig": DatasourceConfig, "sa": sa}


def _returns_ast(hint: object) -> bool:
    return isinstance(hint, type) and issubclass(hint, exp.Expression)


def _forbidden(hint: object) -> bool:
    if hint is str or hint is collections.abc.Callable:
        return True
    origin = typing.get_origin(hint)
    if origin is collections.abc.Callable:
        return True
    if origin is Literal:
        return False
    if origin is Union or isinstance(hint, types.UnionType):
        return any(_forbidden(a) for a in typing.get_args(hint))
    return False


def _violations(cls: type) -> list[str]:
    out: list[str] = []
    for name, fn in inspect.getmembers(cls, inspect.isfunction):
        if not fn.__module__.startswith(("slayer.", "tests.")):
            continue
        hints = typing.get_type_hints(fn, localns=_LOCALNS)
        if not _returns_ast(hints.get("return")):
            continue
        for param in list(inspect.signature(fn).parameters)[1:]:
            if param not in hints:
                out.append(f"{cls.__name__}.{name}({param}): unannotated")
            elif _forbidden(hints[param]):
                out.append(f"{cls.__name__}.{name}({param}): {hints[param]!r}")
    return out


@pytest.mark.parametrize("cls", sorted({SqlDialect, *(type(d) for d in _ALL_DIALECTS)},
                                       key=lambda c: c.__name__))
def test_dialect_hooks_take_typed_ast_operands(cls: type) -> None:
    assert _violations(cls) == []


class _Bad(SqlDialect):
    def build_text(self, col: str) -> exp.Expression:
        return exp.column(col)

    def build_optional(self, col: Optional[str]) -> exp.Column:
        return exp.column(col or "x")

    def build_pipe(self, col: "exp.Expression | str") -> exp.Expression:
        return exp.column("x")

    def build_callback(self, parse: Callable[[str], exp.Expression]) -> exp.Expression:
        return parse("x")

    def build_untyped(self, col) -> exp.Expression:  # noqa: ANN001
        return col


class _Good(SqlDialect):
    def build_literal(self, agg: Literal["a", "b"], col: exp.Expression) -> exp.Expression:
        return col

    def build_enum(self, g: TimeGranularity, n: int) -> exp.Expression:
        return exp.Literal.number(n)

    def describe(self, name: str) -> str:
        return name


def test_law_flags_every_forbidden_shape() -> None:
    flagged = {v.split("(")[0].split(".")[1] for v in _violations(_Bad)}
    assert flagged >= {"build_text", "build_optional", "build_pipe", "build_callback",
                       "build_untyped"}


def test_law_accepts_closed_literals_enums_and_non_ast_methods() -> None:
    own = {"build_literal", "build_enum", "describe"}
    assert [v for v in _violations(_Good) if v.split("(")[0].split(".")[1] in own] == []
