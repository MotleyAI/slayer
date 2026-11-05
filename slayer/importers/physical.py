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
from slayer.sql.dialects import SqlDialect
from slayer.sql.naming import as_sql_dialect, denoted_name


class UndecidableIdentifierCaseError(Exception):
    """An import without a datasource dialect whose physical SQL holds identifiers whose meaning depends on it."""

    def __init__(self, identifiers: Iterable[str]) -> None:
        self.identifiers = list(dict.fromkeys(identifiers))
        super().__init__(
            f"Cannot tell which database objects the unquoted names {', '.join(map(repr, self.identifiers))} name "
            f"without the datasource's dialect: configure the datasource (it need not be reachable), or quote the "
            f"names in the source.")


def physical_identifier(col: Column, *, dialect: str | None) -> exp.Identifier:
    """The identifier a base column's SQL reads (its name when it has none)."""
    try:
        node = sqlglot.parse_one(col.sql, read=dialect) if col.sql is not None else None
    except SqlglotError:  # a reserved word such as `from`
        node = None
    if isinstance(node, exp.Column) and isinstance(node.this, exp.Identifier):
        return node.this
    return exp.to_identifier(col.physical_name)


def live_exact_name(name: str, *, dialect: SqlDialect | None) -> str:
    """The exact name a live name in SQLAlchemy's spelling denotes (an upper-case Snowflake / Oracle name is
    reported lowercased, a lower-case one flagged quoted)."""
    if (dialect is not None and dialect.identifier_folding == "upper" and getattr(name, "quote", None) is not True
            and name == name.lower()):
        return name.upper()
    return str(name)


class PhysicalIndex:
    """A model's columns plus the physical columns known to exist, matched in one dialect."""

    def __init__(self, columns: Iterable[Column], *, dialect: str | None, live: Iterable[str] = ()) -> None:
        self.dialect = dialect
        self._sql_dialect = as_sql_dialect(dialect)
        folds = self._sql_dialect is not None and self._sql_dialect.identifier_folding != "none"
        self._insensitive = not folds and (
            Dialect.get_or_raise(dialect).normalization_strategy is NormalizationStrategy.CASE_INSENSITIVE)
        self.columns = list(columns)
        # Physical spellings: exact name -> whether written unquoted.
        self._reads: dict[int, tuple[str, bool]] = {
            id(c): self._spelling(physical_identifier(c, dialect=dialect))
            for c in self.columns if c.is_base and c.filter is None}
        self._known: dict[str, bool] = dict.fromkeys(
            (live_exact_name(n, dialect=self._sql_dialect) for n in live), False)
        for name, unquoted in self._reads.values():
            self._known[name] = self._known.get(name, False) or unquoted

    def _spelling(self, ident: exp.Identifier) -> tuple[str, bool]:
        """``(exact name, unquoted)`` as SLayer reads ``ident``."""
        return denoted_name(ident, dialect=self._sql_dialect), not ident.quoted

    def _matches(self, ident: exp.Identifier, names: dict[str, bool]) -> list[str]:
        """The exact names ``ident`` denotes; an unquoted one also matches as written (Snowflake's ``"foo"``)."""
        if self._insensitive and not ident.quoted:
            return ([n for n, u in names.items() if u and n == ident.name]
                    or [n for n in names if n.lower() == ident.name.lower()])
        candidates = {self._spelling(ident)[0], *([] if ident.quoted else [ident.name])}
        return [n for n in names if n in candidates]

    def live_names(self, ident: exp.Identifier) -> list[str]:
        """The exact live names ``ident`` denotes."""
        return self._matches(ident, self._known)

    def _targets(self, ident: exp.Identifier) -> list[str]:
        return self._matches(ident, self._known) or [self._spelling(ident)[0]]

    def _named(self, ident: exp.Identifier, col: Column) -> bool:
        return bool(self._matches(ident, {self._spelling(exp.to_identifier(col.name))[0]: True}))

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
