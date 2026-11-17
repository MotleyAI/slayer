"""Type probes: read-only statements bounded to the dialect's probe rows, checked on their AST before the one render."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from pydantic import ValidationError
from sqlglot import exp
from sqlglot.expressions.core import Expr

from slayer.sql.dialects.base import SqlDialect
from slayer.sql.render.parse import is_read_only_query


class TypeProbe(str):
    """A rendered type probe: the bounded probe SQL text, its checked AST carried privately. Minted only by
    ``mint_type_probe``; equal to and hashed as its text."""

    _sample: exp.Select | None
    _render: Callable[[Expr], str] | None

    def __new__(cls, *args: Any, **kwargs: Any) -> TypeProbe:
        raise ValidationError.from_exception_data(cls.__name__, [{
            "type": "value_error", "loc": (), "input": kwargs or args,
            "ctx": {"error": ValueError("a TypeProbe is minted from a checked AST by mint_type_probe")},
        }])

    def __setattr__(self, name: str, value: Any) -> None:
        raise ValidationError.from_exception_data(type(self).__name__, [
            {"type": "frozen_instance", "loc": (name,), "input": value},
        ])

    @property
    def sql(self) -> str:
        """The probe statement's text."""
        return str.__str__(self)

    def census_sql(self, columns: Sequence[str], *, dialect: SqlDialect) -> str | None:
        """The dialect's storage census of ``columns`` over a sample of the probed rows; ``None`` when its cursor
        types them."""
        select = dialect.result_census_select(columns)
        if select is None or self._sample is None or self._render is None:
            return None
        return self._render(exp.select(*select).from_(self._sample.subquery("_types")))


def _row_bound(statement: Expr) -> Expr | None:
    limit = statement.args.get("limit") if isinstance(statement, exp.Select) else None
    if isinstance(limit, exp.Limit):
        return limit.expression
    return limit.args.get("count") if isinstance(limit, exp.Fetch) else None


def mint_type_probe(statement: Expr, *, dialect: SqlDialect, render: Callable[[Expr], str]) -> TypeProbe:
    """``statement``, proven read-only and bounded to ``dialect.type_probe_rows``, rendered once by ``render``."""
    bound = _row_bound(statement)
    probe_rows = dialect.type_probe_rows
    if not (is_read_only_query(statement) and isinstance(bound, exp.Literal) and bound.is_int
            and int(bound.name) == probe_rows):
        raise ValueError(f"a type probe must be a read-only query bounded to {probe_rows} row(s)")
    probe = str.__new__(TypeProbe, render(statement))
    rows = dialect.result_census_rows
    sample = statement.limit(rows) if rows is not None and isinstance(statement, exp.Select) else None
    object.__setattr__(probe, "_sample", sample)
    object.__setattr__(probe, "_render", render if sample is not None else None)
    return probe
