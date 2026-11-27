"""Parse an OSI ``Dataset.source`` into its physical components.

``source`` is either a dotted physical identifier (``[catalog.]db.schema.table``,
optionally quoted per segment) or a raw SQL query. The identifier form is read
with the datasource dialect's identifier rules, table-last / schema-second-last /
database-the-rest; the query form is carried through to SLayer sql-mode.

The parsed ``database`` is currently dropped — every dataset binds to the
importer's ``--datasource``. ``resolve_datasource`` is the stubbed extension
point for future per-database routing.
"""

from __future__ import annotations

import re

from pydantic import BaseModel
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.errors import SqlglotError

from slayer.sql.dialects import SqlDialect

_SELECT_RE = re.compile(r"\bselect\b", re.IGNORECASE)


class ParsedSource(BaseModel):
    """A parsed OSI dataset source."""

    database: str | None = None
    schema_name: str | None = None
    schema_quoted: bool = False
    table: str | None = None
    table_quoted: bool = False
    query: str | None = None
    is_query: bool = False


def _outside_quotes(s: str, *, read: str | None) -> str:
    """``s`` with its quoted identifier spans (``read``'s identifier quotes) removed."""
    pairs = [q if isinstance(q, tuple) else (q, q) for q in Dialect.get_or_raise(read).tokenizer_class.IDENTIFIERS]
    quotes = dict(pairs)
    out: list[str] = []
    close: str | None = None
    i = 0
    while i < len(s):
        ch = s[i]
        if close is not None and ch == close:
            if s[i + 1:i + 2] == close:  # a doubled closing quote escapes it
                i += 1
            else:
                close = None
        elif close is None and ch in quotes:
            close = quotes[ch]
        elif close is None:
            out.append(ch)
        i += 1
    return "".join(out)


def _looks_like_query(s: str, *, read: str | None) -> bool:
    stripped = s.strip()
    # A quoted identifier segment like "My Select" or `Order Items` is not a query.
    unquoted = _outside_quotes(stripped, read=read)
    return stripped.startswith("(") or any(ch.isspace() for ch in unquoted) or bool(_SELECT_RE.search(unquoted))


def parse_source(source: str, *, dialect: SqlDialect | None = None) -> ParsedSource:
    """Parse an OSI ``Dataset.source`` string, an identifier read with ``dialect`` (default: Postgres)."""
    read = dialect.sqlglot_name if dialect else None
    if _looks_like_query(source, read=read):
        return ParsedSource(is_query=True, query=source.strip())
    try:
        parts = exp.to_table(source.strip(), dialect=read).parts
    except SqlglotError:
        parts = []
    if not parts:
        return ParsedSource(is_query=True, query=source.strip())

    table = parts[-1]
    schema = parts[-2] if len(parts) >= 2 else None
    database = ".".join(p.name for p in parts[:-2]) if len(parts) >= 3 else None
    return ParsedSource(database=database, schema_name=schema.name if schema else None,
                        schema_quoted=bool(schema and schema.args.get("quoted")), table=table.name,
                        table_quoted=bool(table.args.get("quoted")))


def _identifier_quotes(read: str | None) -> dict[str, str]:
    pairs = [q if isinstance(q, tuple) else (q, q) for q in Dialect.get_or_raise(read).tokenizer_class.IDENTIFIERS]
    return dict(pairs)


def _parts(name: str, *, read: str | None) -> list[tuple[str, bool]] | None:
    """``name``'s dot-separated parts as written, each with whether it is quoted; ``None`` when a quote is unclosed or
    a part is empty."""
    quotes = _identifier_quotes(read)
    parts: list[tuple[str, bool]] = []
    current: list[str] = []
    close: str | None = None
    i = 0
    while i < len(name):
        ch = name[i]
        if close is not None:
            current.append(ch)
            if ch == close and name[i + 1:i + 2] == close:
                current.append(close)
                i += 1
            elif ch == close:
                close = None
        elif ch in quotes and not current:
            close = quotes[ch]
            current.append(ch)
        elif ch == ".":
            parts.append(("".join(current), False))
            current = []
        else:
            current.append(ch)
        i += 1
    if close is not None:
        return None
    parts.append(("".join(current), False))
    out = [(p, bool(p) and p[0] in quotes and p[-1] == quotes[p[0]] and len(p) > 1) for p, _ in parts]
    return None if any(not p for p, _ in out) else out


def ossie_source(sql_table: str, *, dialect: SqlDialect | None = None) -> str:
    """A stored ``sql_table`` as an Ossie ``source`` naming the same table: each part SLayer's table-name rule reads
    exactly (an unquoted part holding an upper-case letter) quoted, every other part as written."""
    read = dialect.sqlglot_name if dialect else None
    stripped = sql_table.strip()
    parts = _parts(stripped, read=read)
    if parts is None:
        return stripped
    quote = next(iter(_identifier_quotes(read).items()))
    out = [p if quoted or not any(c.isupper() for c in p) else f"{quote[0]}{p.replace(quote[1], quote[1] * 2)}{quote[1]}"
           for p, quoted in parts]
    return ".".join(out)


def source_table(source: str, *, dialect: SqlDialect | None = None) -> str:
    """A table-form Ossie ``source`` read by Ossie's identifier rule, as the ``sql_table`` SLayer's table-name rule reads
    as the same table: an unquoted part folded as the database folds it, every other part as written."""
    stripped = source.strip()
    parts = _parts(stripped, read=dialect.sqlglot_name if dialect else None)
    if parts is None:
        return stripped
    fold = dialect.unquoted_identifier if dialect is not None else str.lower
    return ".".join(_exact(p) if quoted else p if not any(c.isupper() for c in p) else fold(p) for p, quoted in parts)


_PLAIN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _exact(part: str) -> str:
    """A quoted part as ``sql_table`` spells it: unquoted when SLayer reads that spelling exactly (a plain identifier
    holding an upper-case letter), else as written."""
    inner = part[1:-1]
    return inner if _PLAIN.fullmatch(inner) and any(c.isupper() for c in inner) else part


def resolve_datasource(database: str | None, default: str) -> str:
    """Map an OSI dataset's ``database`` to a SLayer datasource name.

    Stubbed extension point: for now the parsed database is dropped
    and every dataset binds to ``default`` (the importer's ``--datasource``). A
    future version can route different OSI databases to different SLayer
    datasources here without reworking the converter.
    """
    return default
