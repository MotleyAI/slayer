"""Ossie's expression object (``{sql, dialects}``) and which of its forms runs on a datasource."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from slayer.core.errors import FieldExpressionError
from slayer.core.refs import IDENTIFIER_RE

__all__ = [
    "OSSIE_SQL", "DialectEntry", "Expression", "ExpressionObject", "RunningForm", "bare_identifier", "dialect_entries",
    "dialect_tokens", "portable_text", "running_form", "stored_definition_text", "token", "unquoted_identifier",
]

OSSIE_SQL = "OSSIE_SQL_2026"


class DialectEntry(BaseModel):
    """One dialect's text of an expression."""

    model_config = ConfigDict(extra="forbid")

    dialect: str
    expression: str


class ExpressionObject(BaseModel):
    """Ossie's expression: portable ``sql``, per-dialect ``dialects`` entries, or both."""

    model_config = ConfigDict(extra="forbid")

    sql: str | None = None
    dialects: list[DialectEntry] | None = None


#: A definition as written: a string (shorthand for ``{sql: …}``) or the object.
Expression = str | ExpressionObject


class RunningForm(BaseModel):
    """The text a definition runs on a datasource; ``native`` when it is the datasource's own SQL."""

    text: str
    native: bool


def token(dialect: str) -> str:
    """A dialect token compared case-insensitively."""
    return dialect.strip().upper()


def unquoted_identifier(text: str) -> str | None:
    """The identifier ``text`` names, bare or double-quoted, else ``None``."""
    stripped = text.strip()
    if len(stripped) > 1 and stripped[0] == stripped[-1] == '"':
        stripped = stripped[1:-1].replace('""', '"')
    return stripped if IDENTIFIER_RE.match(stripped) else None


def bare_identifier(expression: Expression | None) -> str | None:
    """The identifier a shorthand or portable ``sql`` names alone, else ``None``."""
    text = portable_text(expression)
    return unquoted_identifier(text) if text is not None else None


def portable_text(expression: Expression | None) -> str | None:
    """The portable form: the shorthand, the object's ``sql``, else its first ``OSSIE_SQL_2026`` entry."""
    if expression is None or isinstance(expression, str):
        return expression
    if expression.sql:
        return expression.sql
    return next((e.expression for e in expression.dialects or [] if token(e.dialect) == OSSIE_SQL), None)


def stored_definition_text(field: dict) -> object:
    """A raw field dict's definition text: its deprecated ``sql``, else its expression's shorthand, portable ``sql`` or
    sole dialect text (``""`` when its dialect entries differ); ``None`` reads the source column of its name."""
    if field.get("sql") is not None:
        return field["sql"]
    expression = field.get("expression")
    if not isinstance(expression, dict):
        return expression
    if expression.get("sql") is not None:
        return expression["sql"]
    texts = {e.get("expression") for e in expression.get("dialects") or [] if isinstance(e, dict)}
    return texts.pop() if len(texts) == 1 else ""


def dialect_entries(expression: Expression | None, dialect: str) -> list[DialectEntry]:
    """The entries of ``expression`` for ``dialect``."""
    if not isinstance(expression, ExpressionObject):
        return []
    return [e for e in expression.dialects or [] if token(e.dialect) == token(dialect)]


def dialect_tokens(expression: Expression | None) -> list[str]:
    """The dialects ``expression`` carries entries for, as written."""
    if not isinstance(expression, ExpressionObject):
        return []
    return [e.dialect for e in expression.dialects or []]


def running_form(*, owner: str, expression: Expression, dialect: str) -> RunningForm:
    """The form ``expression`` runs on a datasource whose own dialect is ``dialect``: its entry, else portable."""
    entries = dialect_entries(expression, dialect) if token(dialect) != OSSIE_SQL else []
    if len(entries) > 1:
        raise FieldExpressionError(owner=owner, reason=f"carries {len(entries)} entries for dialect {token(dialect)}.",
                                   suggestion="Keep one.")
    if entries:
        return RunningForm(text=entries[0].expression, native=True)
    portable = portable_text(expression)
    if portable is not None:
        return RunningForm(text=portable, native=False)
    carried = ", ".join(dialect_tokens(expression)) or "none"
    raise FieldExpressionError(
        owner=owner, reason=f"has no form that runs on {token(dialect)} (dialects carried: {carried}).",
        suggestion=f"Add portable sql or a {token(dialect)} entry.",
    )
