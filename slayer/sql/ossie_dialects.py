"""Pick one SQL candidate per Ossie expression, read it, and render it for the target datasource."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import sqlglot
from pydantic import BaseModel
from sqlglot import exp
from sqlglot.errors import SqlglotError

from slayer.core.ossie_expression import OSIExpression
from slayer.sql.dialects import SQLGLOT_NAMES, ossie_token

__all__ = ["ANSI_SQL", "OSSIE_SQL", "PORTABLE_READER", "Candidate", "Selection", "expression_digest", "is_native",
           "ossie_token", "read", "reader_for", "render", "select_expression", "source_digest"]

OSSIE_SQL = "OSSIE_SQL_2026"
ANSI_SQL = "ANSI_SQL"
# Ossie's portable function set (IFF, ZEROIFNULL, DATEADD, STARTSWITH, ||) reads as Snowflake.
PORTABLE_READER = "snowflake"
# Each SLayer dialect's Ossie token: its sqlglot name, upper-cased.
_READERS: dict[str, str] = {name.upper(): name for name in SQLGLOT_NAMES}


class Candidate(BaseModel):
    """One expression text and the dialect token it is declared in."""

    dialect: str
    text: str


class Selection(BaseModel):
    """The chosen candidate (``None`` when nothing is readable), with info notes or the failure reason."""

    candidate: Candidate | None = None
    notes: list[str] = []
    reason: str | None = None


def _token(dialect: str) -> str:
    return dialect.strip().upper()


def reader_for(dialect: str) -> str | None:
    """The sqlglot read dialect for an Ossie dialect token, or ``None`` when it is not readable SQL."""
    token = _token(dialect)
    if token in (OSSIE_SQL, ANSI_SQL):
        return PORTABLE_READER
    return _READERS.get(token)


def _native(dialect: str, target: str) -> bool:
    return _token(dialect) not in (OSSIE_SQL, ANSI_SQL) and reader_for(dialect) == target


def _first(step: list[Candidate]) -> Selection:
    notes = [f"{len(step)} {step[0].dialect} candidates; using the first"] if len(step) > 1 else []
    return Selection(candidate=step[0], notes=notes)


def select_expression(expression: OSIExpression, *, override: str | None, target: str) -> Selection:
    """Apply the selection order: override, top-level ``sql``, OSSIE_SQL_2026, ANSI_SQL, target-native, other."""
    entries = [Candidate(dialect=d.dialect, text=d.expression) for d in expression.dialects]
    readable = [c for c in entries if reader_for(c.dialect) is not None]

    def matching(pred: Callable[[str], bool]) -> list[Candidate]:
        return [c for c in readable if pred(_token(c.dialect))]

    steps: list[list[Candidate]] = []
    if override:
        steps.append(matching(lambda t: t == _token(override)))
    if expression.sql:
        steps.append([Candidate(dialect=OSSIE_SQL, text=expression.sql)])
    steps += [
        matching(lambda t: t == OSSIE_SQL),
        matching(lambda t: t == ANSI_SQL),
        matching(lambda t: _native(t, target)),
    ]
    for step in steps:
        if step:
            return _first(step)
    if readable:
        selection = _first(readable)
        selection.notes.append(f"read from dialect {readable[0].dialect} (no portable or target-native form)")
        return selection
    if not entries:
        return Selection(reason="has no expression")
    found = ", ".join(dict.fromkeys(d.dialect for d in entries))
    return Selection(reason=f"has no readable SQL expression (dialects: {found})")


def read(candidate: Candidate) -> exp.Expr:
    """Parse ``candidate`` with its dialect's reader; raises ``SqlglotError``."""
    return sqlglot.parse_one(candidate.text, read=reader_for(candidate.dialect))


def is_native(candidate: Candidate, *, target: str) -> bool:
    """Whether ``candidate`` is written in the target's own dialect (kept verbatim)."""
    return _native(candidate.dialect, target)


def render(candidate: Candidate, tree: exp.Expr, *, target: str) -> str:
    """Target-native text verbatim, anything else rendered in the target dialect."""
    return candidate.text if is_native(candidate, target=target) else tree.sql(dialect=target)


def _normalized(text: str, reader: str | None) -> str:
    if reader is None:
        return text.strip()
    try:
        return sqlglot.parse_one(text, read=reader).sql(dialect=reader)
    except SqlglotError:
        return text.strip()


def expression_digest(expression: OSIExpression) -> list[Any]:
    """An Ossie expression with each text normalized through its own reader."""
    return [
        _normalized(expression.sql, PORTABLE_READER) if expression.sql else None,
        [[d.dialect.strip().upper(), _normalized(d.expression, reader_for(d.dialect))] for d in expression.dialects],
    ]


def source_digest(source: str, *, reader: str) -> str:
    return _normalized(source, reader)
