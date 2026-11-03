"""Type probes: read-only statements bounded to the dialect's probe rows, checked on their AST before the one render."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, model_validator
from sqlglot import exp
from sqlglot.expressions.core import Expr

from slayer.sql.dialects.base import SqlDialect
from slayer.sql.render.parse import is_read_only_query


class TypeProbe(BaseModel, frozen=True):
    """A rendered type probe: the only statement the type door executes. Minted only by ``mint_type_probe``."""

    sql: str

    @model_validator(mode="before")
    @classmethod
    def _minted_only(cls, data: Any) -> Any:
        raise ValueError("a TypeProbe is minted from a checked AST by mint_type_probe")


def _row_bound(statement: Expr) -> Expr | None:
    limit = statement.args.get("limit") if isinstance(statement, exp.Select) else None
    if isinstance(limit, exp.Limit):
        return limit.expression
    return limit.args.get("count") if isinstance(limit, exp.Fetch) else None


def mint_type_probe(statement: Expr, *, dialect: SqlDialect, render: Callable[[Expr], str]) -> TypeProbe:
    """``statement``, proven read-only and bounded to ``dialect.type_probe_rows``, rendered once by ``render``."""
    bound = _row_bound(statement)
    rows = dialect.type_probe_rows
    if not (is_read_only_query(statement) and isinstance(bound, exp.Literal) and bound.is_int
            and int(bound.name) == rows):
        raise ValueError(f"a type probe must be a read-only query bounded to {rows} row(s)")
    return TypeProbe.model_construct(sql=render(statement))
