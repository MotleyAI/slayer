"""The table columns a statement's result columns read as they are."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.expressions.core import Expr
from sqlglot.optimizer.scope import Scope, build_scope


class ColumnSource(BaseModel):
    """A result column that is a table's column unchanged; ``result`` and ``column`` ``None``: every column of the
    table, under its own name."""

    model_config = ConfigDict(frozen=True)

    table: str
    schema_name: Optional[str] = None
    result: Optional[str] = None
    column: Optional[str] = None


def column_sources(statement: Expr) -> list[ColumnSource]:
    """The plain column reads among ``statement``'s result columns, through subqueries and ``*``; a computed column,
    a set operation and an unqualified column of a join have none."""
    try:
        scope = build_scope(statement)
    except SqlglotError:
        return []
    return [] if scope is None else _scope_sources(scope)


def _scope_sources(scope: Scope) -> list[ColumnSource]:
    select = scope.expression
    if not isinstance(select, exp.Select):
        return []
    return [found for projection in select.expressions for found in _projection_sources(scope, projection)]


def _projection_sources(scope: Scope, projection: Expr) -> list[ColumnSource]:
    node = projection.unalias()
    if _is_star(node):
        qualifier = node.table if isinstance(node, exp.Column) else ""
        names = [qualifier] if qualifier else list(scope.selected_sources)
        return [found for name in names for found in _every_column(scope.sources.get(name))]
    if isinstance(node, exp.Column):
        return _one_column(_source(scope, node), column=node.name, result=projection.alias_or_name)
    return []


def _is_star(node: Expr) -> bool:
    return isinstance(node, exp.Star) or (isinstance(node, exp.Column) and isinstance(node.this, exp.Star))


def _source(scope: Scope, column: exp.Column) -> object:
    if column.table:
        return scope.sources.get(column.table)
    names = list(scope.selected_sources)
    return scope.sources.get(names[0]) if len(names) == 1 else None


def _table(table: exp.Table, *, result: Optional[str] = None, column: Optional[str] = None) -> list[ColumnSource]:
    if not table.name:
        return []  # a table function
    return [ColumnSource(table=table.name, schema_name=table.db or None, result=result, column=column)]


def _every_column(source: object) -> list[ColumnSource]:
    if isinstance(source, exp.Table):
        return _table(source)
    return _scope_sources(source) if isinstance(source, Scope) else []


def _one_column(source: object, *, column: str, result: str) -> list[ColumnSource]:
    if isinstance(source, exp.Table):
        return _table(source, result=result, column=column)
    if not isinstance(source, Scope):
        return []
    if source.outer_columns:
        return _listed_column(source, column=column, result=result)
    inner = _scope_sources(source)
    named = [i for i in inner if i.result is not None and i.result.lower() == column.lower()]
    if named:
        return [named[0].model_copy(update={"result": result})]
    # Under ``*`` the column is whichever table's it is: each is asked, and only its owner answers.
    return [i.model_copy(update={"result": result, "column": column}) for i in inner if i.result is None]


def _listed_column(source: Scope, *, column: str, result: str) -> list[ColumnSource]:
    """``column`` of a source with a column list: the inner projection at its position; none under ``*``."""
    names = [name.lower() for name in source.outer_columns]
    select = source.expression
    if column.lower() not in names or not isinstance(select, exp.Select):
        return []
    projections = select.expressions
    if any(_is_star(p.unalias()) for p in projections):
        return []
    position = names.index(column.lower())
    if position >= len(projections):
        return []
    found = _projection_sources(source, projections[position])
    return [f.model_copy(update={"result": result}) for f in found]
