"""Which logical field, if any, reads each physical column an importer's SQL names."""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy
from sqlglot.errors import SqlglotError
from sqlglot.expressions.core import Expression

from slayer.core.field_expression import DialectEntry, ExpressionObject, unquoted_identifier
from slayer.core.models import DatasetField, is_base_field_sql
from slayer.core.names import name_taken
from slayer.sql.field_expansion import root_scope_field_ids
from slayer.sql.dialects import SqlDialect
from slayer.sql.naming import as_sql_dialect, denoted_name
from slayer.sql.root_scope import respelled_columns


class UndecidableIdentifierCaseError(Exception):
    """An import without a datasource dialect whose physical SQL holds identifiers whose meaning depends on it."""

    def __init__(self, identifiers: Iterable[str]) -> None:
        self.identifiers = list(dict.fromkeys(identifiers))
        super().__init__(
            f"Cannot tell which database objects the unquoted names {', '.join(map(repr, self.identifiers))} name "
            f"without the datasource's dialect: configure the datasource (it need not be reachable), or quote the "
            f"names in the source.")


def physical_identifier(col: DatasetField, *, dialect: str | None) -> exp.Identifier:
    """The identifier a base field's SQL reads (its name when it has none)."""
    text = col.definition_sql
    try:
        node = sqlglot.parse_one(text, read=dialect) if text is not None else None
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


def case_insensitive(dialect: str | None) -> bool:
    """Whether ``dialect`` matches unquoted names case-insensitively without folding them."""
    found = as_sql_dialect(dialect)
    if found is not None and found.identifier_folding != "none":
        return False
    return Dialect.get_or_raise(dialect).normalization_strategy is NormalizationStrategy.CASE_INSENSITIVE


class PhysicalIndex:
    """A dataset's fields plus the physical columns known to exist, matched in one dialect."""

    def __init__(self, fields: Iterable[DatasetField], *, dialect: str | None, live: Iterable[str] = ()) -> None:
        self.dialect = dialect
        self._sql_dialect = as_sql_dialect(dialect)
        self._insensitive = case_insensitive(dialect)
        self.fields = list(fields)
        # Physical spellings: exact name -> whether written unquoted.
        self._reads: dict[int, tuple[str, bool]] = {
            id(c): self._spelling(physical_identifier(c, dialect=dialect))
            for c in self.fields if c.is_base and c.filter is None}
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

    def _named(self, ident: exp.Identifier, col: DatasetField) -> bool:
        return bool(self._matches(ident, {self._spelling(exp.to_identifier(col.name))[0]: True}))

    def reader(self, ident: exp.Identifier) -> DatasetField | None:
        """The unfiltered base field reading ``ident``'s physical column, a same-named one first."""
        targets = self._targets(ident)
        readers = [c for c in self.fields if id(c) in self._reads and self._reads[id(c)][0] in targets]
        return next((c for c in readers if self._named(ident, c)), readers[0] if readers else None)

    def problem(self, ident: exp.Identifier) -> str | None:
        """Why a physical reference cannot be read with its meaning here (an ambiguous live name), else ``None``."""
        if len(found := self._matches(ident, self._known)) > 1:
            return f"physical column {ident.name!r} is ambiguous between {sorted(found)}"
        return None

    def taken(self, ident: exp.Identifier) -> bool:
        """Whether a field that does not read ``ident``'s physical column holds its name."""
        targets = self._targets(ident)
        named = set(self._matches(ident, {c.name: True for c in self.fields}))
        return any(c.name in named and not (id(c) in self._reads and self._reads[id(c)][0] in targets)
                   for c in self.fields)

    def sql_problem(self, sql: str, *, logical: frozenset[str] = frozenset()) -> str | None:
        """The first unreadable root-scope physical reference in ``sql``; names in ``logical`` are members."""
        try:
            tree = sqlglot.parse_one(sql, read=self.dialect)
        except SqlglotError as exc:
            tree = exc
        if is_base_field_sql(sql) and not isinstance(tree, exp.Column):
            return f"{sql.strip()!r} is a reserved word; quote it to read the physical column"
        if isinstance(tree, SqlglotError):
            return f"SQL does not parse: {tree}"
        for col in root_physical_columns(tree):
            if col.name not in logical and (found := self.problem(col.this)) is not None:
                return found
        return None


def unreadable_fields(
    fields: list[DatasetField], *, dialect: str | None, live: Iterable[str] = (), logical: frozenset[str] = frozenset(),
    prepare: Callable[[str], str] = str,
) -> dict[str, str]:
    """Fields to drop, with why: those whose SQL does not parse or reads an ambiguous physical column.

    ``prepare`` turns stored SQL into parseable SQL (placeholders rendered)."""
    index = PhysicalIndex(fields, dialect=dialect, live=live)
    found: dict[str, str] = {}
    for col in fields:
        for sql in (col.definition_sql, col.filter):
            if sql is not None and col.name not in found:
                if (problem := index.sql_problem(prepare(sql), logical=logical)) is not None:
                    found[col.name] = problem
    return found


def raw_name(column: str, *, taken: Collection[str]) -> str:
    """A hidden reader's name for ``column``: ``<column>_raw``, else ``<column>_raw_2``, … — the first not ``taken``
    in any case."""
    name, n = f"{column}_raw", 2
    while name_taken(name, taken):
        name, n = f"{column}_raw_{n}", n + 1
    return name


def finished_field(field: DatasetField, *, token: str | None) -> DatasetField:
    """``field``, its imported native text held as the shorthand, in its imported form: the shorthand for one column,
    else the entry for the datasource's dialect ``token``; with no dialect known, the deprecated ``sql`` until it is."""
    text = field.definition_sql
    if text is None or unquoted_identifier(text) is not None or not isinstance(field.expression, str):
        return field
    if token is not None:
        data: dict[str, Any] = field.model_dump(exclude={"expression", "sql"}, exclude_none=True)
        return DatasetField.model_validate(
            {**data, "expression": ExpressionObject(dialects=[DialectEntry(dialect=token, expression=text)])})
    out = field.model_copy(update={"expression": None})
    out.sql = text
    return out


def _masked_read_by_path(fields: list[DatasetField], *, dataset: str, dialect: str | None) -> list[DatasetField]:
    """``fields`` with each bare reference to a name another derived field holds read through a physical reader by
    path, so a deprecated ``sql``'s conversion (which reads such a name as that field) keeps it physical."""
    expanding = {c.name for c in fields if c.needs_expansion}
    out = list(fields)
    readers: dict[str, str] = {}

    def reader(name: str) -> str:
        if name not in readers:
            found = PhysicalIndex(out, dialect=dialect).reader(exp.to_identifier(name))
            if found is None:
                found = DatasetField(name=raw_name(name, taken={c.name for c in out}), expression=name, hidden=True)
                out.append(found)
            readers[name] = found.name
        return readers[name]

    for i, field in enumerate(fields):
        text, masked = field.definition_sql, expanding - {field.name}
        if text is None or not masked or unquoted_identifier(text) is not None:
            continue
        respelled = respelled_columns(
            text, respell=lambda name: f"{dataset}.{reader(name)}" if name in masked else None, dialect=dialect)
        if respelled != text:
            out[i] = field.model_copy(update={"expression": respelled})
    return out


def finished_fields(fields: list[DatasetField], *, dataset: str, token: str | None,
                    dialect: str | None) -> list[DatasetField]:
    """``fields`` of ``dataset`` in their imported form (``finished_field``)."""
    if token is None:
        fields = _masked_read_by_path(fields, dataset=dataset, dialect=dialect)
    return [finished_field(f, token=token) for f in fields]


def root_physical_columns(tree: exp.Expr) -> list[exp.Column]:
    """Unqualified column references in ``tree``'s root scope."""
    if not isinstance(tree, Expression):
        return []
    root = root_scope_field_ids(parsed=tree)
    return [c for c in tree.find_all(exp.Column) if id(c) in root and not c.table and isinstance(c.this, exp.Identifier)]
