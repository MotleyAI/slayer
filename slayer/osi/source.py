"""Parse an OSI ``Dataset.source`` into its physical components (DEV-1643).

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
from sqlglot.errors import SqlglotError

from slayer.sql.dialects import SqlDialect

_SELECT_RE = re.compile(r"\bselect\b", re.IGNORECASE)


class ParsedSource(BaseModel):
    """A parsed OSI dataset source."""

    database: str | None = None
    schema_name: str | None = None
    table: str | None = None
    table_quoted: bool = False
    query: str | None = None
    is_query: bool = False


def _has_top_level_space(s: str) -> bool:
    """True if ``s`` contains whitespace outside of double-quoted spans."""
    in_quote = False
    for ch in s:
        if ch == '"':
            in_quote = not in_quote
        elif ch.isspace() and not in_quote:
            return True
    return False


def _looks_like_query(s: str) -> bool:
    stripped = s.strip()
    # Run the SELECT check on the text OUTSIDE double-quoted spans so a valid
    # quoted identifier segment like "My Select" isn't mistaken for a query.
    unquoted = re.sub(r'"[^"]*"', "", stripped)
    return (
        stripped.startswith("(")
        or _has_top_level_space(stripped)
        or bool(_SELECT_RE.search(unquoted))
    )


def parse_source(source: str, *, dialect: SqlDialect | None = None) -> ParsedSource:
    """Parse an OSI ``Dataset.source`` string, an identifier read with ``dialect`` (default: Postgres)."""
    if _looks_like_query(source):
        return ParsedSource(is_query=True, query=source.strip())
    try:
        parts = exp.to_table(source.strip(), dialect=dialect.sqlglot_name if dialect else None).parts
    except SqlglotError:
        parts = []
    if not parts:
        return ParsedSource(is_query=True, query=source.strip())

    table = parts[-1]
    schema_name = parts[-2].name if len(parts) >= 2 else None
    database = ".".join(p.name for p in parts[:-2]) if len(parts) >= 3 else None
    return ParsedSource(database=database, schema_name=schema_name, table=table.name,
                        table_quoted=bool(table.args.get("quoted")))


def resolve_datasource(database: str | None, default: str) -> str:
    """Map an OSI dataset's ``database`` to a SLayer datasource name.

    Stubbed extension point (DEV-1643): for now the parsed database is dropped
    and every dataset binds to ``default`` (the importer's ``--datasource``). A
    future version can route different OSI databases to different SLayer
    datasources here without reworking the converter.
    """
    return default
