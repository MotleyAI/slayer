"""Which logical column, if any, reads each physical column an importer's SQL names."""

from __future__ import annotations

from collections.abc import Callable, Iterable

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy
from sqlglot.errors import SqlglotError
from sqlglot.expressions.core import Expression

from slayer.core.models import Column, is_base_column_sql
from slayer.sql.column_expansion import root_scope_column_ids


class PhysicalIndex:
    """A model's columns plus the physical columns known to exist, matched in one dialect."""

    def __init__(self, columns: Iterable[Column], *, dialect: str | None, live: Iterable[str] = ()) -> None:
        self.dialect = dialect
        self._strategy = Dialect.get_or_raise(dialect).normalization_strategy
        self.columns = list(columns)
        # Physical spellings: exact name -> whether written unquoted.
        self._reads: dict[int, tuple[str, bool]] = {
            id(c): self._spelling(self._identifier(c)) for c in self.columns if c.is_base and c.filter is None}
        self._known: dict[str, bool] = dict.fromkeys(live, False)
        for name, unquoted in self._reads.values():
            self._known[name] = self._known.get(name, False) or unquoted

    def _identifier(self, col: Column) -> exp.Identifier:
        try:
            node = sqlglot.parse_one(col.sql, read=self.dialect) if col.sql is not None else None
        except SqlglotError:  # a reserved word such as `from`
            node = None
        if isinstance(node, exp.Column) and isinstance(node.this, exp.Identifier):
            return node.this
        return exp.to_identifier(col.physical_name)

    def _spelling(self, ident: exp.Identifier) -> tuple[str, bool]:
        """``(exact name, unquoted)``: an unquoted name folded per the dialect."""
        if ident.quoted or self._strategy in (NormalizationStrategy.CASE_SENSITIVE,
                                              NormalizationStrategy.CASE_INSENSITIVE):
            return ident.name, not ident.quoted
        return (ident.name.upper() if self._strategy is NormalizationStrategy.UPPERCASE else ident.name.lower()), True

    def _matches(self, ident: exp.Identifier, names: dict[str, bool]) -> list[str]:
        """The names ``ident`` denotes: quoted exactly, unquoted folded (an unquoted exact spelling wins)."""
        name, unquoted = self._spelling(ident)
        if not unquoted or self._strategy is not NormalizationStrategy.CASE_INSENSITIVE:
            return [n for n in names if n == name]
        return ([n for n, u in names.items() if u and n == name]
                or [n for n in names if n.lower() == name.lower()])

    def _targets(self, ident: exp.Identifier) -> list[str]:
        return self._matches(ident, self._known) or [self._spelling(ident)[0]]

    def _named(self, ident: exp.Identifier, col: Column) -> bool:
        return bool(self._matches(ident, {col.name: True}))

    def reader(self, ident: exp.Identifier) -> Column | None:
        """The unfiltered base column reading ``ident``'s physical column, a same-named one first."""
        targets = self._targets(ident)
        readers = [c for c in self.columns if id(c) in self._reads and self._reads[id(c)][0] in targets]
        return next((c for c in readers if self._named(ident, c)), readers[0] if readers else None)

    def problem(self, ident: exp.Identifier, *, owner: str | None = None) -> str | None:
        """Why a physical reference cannot be read with its meaning here, else ``None``."""
        if len(found := self._matches(ident, self._known)) > 1:
            return f"physical column {ident.name!r} is ambiguous between {sorted(found)}"
        targets = self._targets(ident)
        for col in self.columns:
            if not self._named(ident, col) or (id(col) in self._reads and self._reads[id(col)][0] in targets):
                continue
            if col.name == owner:
                return f"{owner!r} reads the physical column of its own name"
            return f"physical column {ident.name!r} is masked by the column {col.name!r}"
        return None

    def sql_problem(self, sql: str, *, owner: str | None = None, logical: frozenset[str] = frozenset()) -> str | None:
        """The first unreadable root-scope physical reference in ``sql``; names in ``logical`` are members."""
        try:
            tree = sqlglot.parse_one(sql, read=self.dialect)
        except SqlglotError as exc:
            tree = exc
        if is_base_column_sql(sql) and not isinstance(tree, exp.Column):
            return f"{sql.strip()!r} is a reserved word; quote it to read the physical column"
        if isinstance(tree, SqlglotError):
            return f"SQL does not parse: {tree}"
        for col in root_physical_columns(tree):
            if col.name not in logical and (found := self.problem(col.this, owner=owner)) is not None:
                return found
        return None


def unreadable_columns(
    columns: list[Column], *, dialect: str | None, live: Iterable[str] = (), logical: frozenset[str] = frozenset(),
    prepare: Callable[[str], str] = str,
) -> dict[str, str]:
    """Columns to drop, with why: self-named ones first (freeing their names), then those reading a masked column.

    ``prepare`` turns stored SQL into parseable SQL (placeholders rendered)."""
    live = list(live)

    def check(pool: list[Column]) -> dict[str, str]:
        index = PhysicalIndex(pool, dialect=dialect, live=live)
        found: dict[str, str] = {}
        for col in pool:
            for sql in (col.sql, col.filter):
                if sql is not None and col.name not in found:
                    if (problem := index.sql_problem(prepare(sql), owner=col.name, logical=logical)) is not None:
                        found[col.name] = problem
        return found

    self_named = {n: p for n, p in check(columns).items() if p.endswith("of its own name")}
    return {**self_named, **check([c for c in columns if c.name not in self_named])}


def root_physical_columns(tree: exp.Expr) -> list[exp.Column]:
    """Unqualified column references in ``tree``'s root scope."""
    if not isinstance(tree, Expression):
        return []
    root = root_scope_column_ids(parsed=tree)
    return [c for c in tree.find_all(exp.Column) if id(c) in root and not c.table and isinstance(c.this, exp.Identifier)]
