"""Parse an OSI ``Dataset.source`` into its physical components (DEV-1643).

``source`` is either a dotted physical identifier (``[catalog.]db.schema.table``,
optionally double-quoted per segment) or a raw SQL query. The identifier form is
split table-last / schema-second-last / database-the-rest; the query form is
carried through to SLayer sql-mode.

The parsed ``database`` is currently dropped — every dataset binds to the
importer's ``--datasource``. ``resolve_datasource`` is the stubbed extension
point for future per-database routing.
"""

from __future__ import annotations

import re

from pydantic import BaseModel

_SELECT_RE = re.compile(r"\bselect\b", re.IGNORECASE)


class ParsedSource(BaseModel):
    """A parsed OSI dataset source."""

    database: str | None = None
    schema_name: str | None = None
    table: str | None = None
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
    return (
        stripped.startswith("(")
        or _has_top_level_space(stripped)
        or bool(_SELECT_RE.search(stripped))
    )


def _split_identifier(s: str) -> list[str]:
    """Split a dotted identifier on unquoted dots, stripping double-quotes."""
    parts: list[str] = []
    cur: list[str] = []
    in_quote = False
    for ch in s:
        if ch == '"':
            in_quote = not in_quote
            continue
        if ch == "." and not in_quote:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    parts.append("".join(cur))
    return parts


def parse_source(source: str) -> ParsedSource:
    """Parse an OSI ``Dataset.source`` string."""
    if _looks_like_query(source):
        return ParsedSource(is_query=True, query=source.strip())

    parts = [p for p in _split_identifier(source.strip()) if p != ""]
    if not parts:
        return ParsedSource(is_query=True, query=source.strip())

    table = parts[-1]
    schema_name = parts[-2] if len(parts) >= 2 else None
    database = ".".join(parts[:-2]) if len(parts) >= 3 else None
    return ParsedSource(database=database, schema_name=schema_name, table=table)


def resolve_datasource(database: str | None, default: str) -> str:
    """Map an OSI dataset's ``database`` to a SLayer datasource name.

    Stubbed extension point (DEV-1643): for now the parsed database is dropped
    and every dataset binds to ``default`` (the importer's ``--datasource``). A
    future version can route different OSI databases to different SLayer
    datasources here without reworking the converter.
    """
    return default
