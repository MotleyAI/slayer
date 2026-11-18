"""Exception fields for export, with SQL text removed unless ``SLAYER_OTEL_CAPTURE`` includes ``sql``.

SQLAlchemy puts the failed statement and its parameters into the error text (``[SQL: ...]``),
and SLayer's own errors can wrap that text. A driver message can still repeat a literal value,
for example ``invalid input syntax for type integer: "abc"``; only the statement is removed.
"""

from __future__ import annotations

import re
import traceback
from typing import Any

from sqlalchemy.exc import StatementError

SQL_PLACEHOLDER = "<sql>"
_PARAMETERS = re.compile(r"\[parameters: [^\n]*\]")


def exception_fields(error: BaseException, *, sql: bool) -> dict[str, Any]:
    """The OTel ``exception.*`` attributes; without ``sql``, every statement in the chain becomes ``<sql>``."""
    message = str(error)
    stacktrace = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    if not sql:
        message, stacktrace = _without_sql(message, error=error), _without_sql(stacktrace, error=error)
    return {
        "exception.type": type(error).__name__,
        "exception.message": message,
        "exception.stacktrace": stacktrace,
    }


def _without_sql(text: str, *, error: BaseException) -> str:
    for statement in sorted(_statements(error), key=len, reverse=True):
        text = text.replace(statement, SQL_PLACEHOLDER)
    return _PARAMETERS.sub(f"[parameters: {SQL_PLACEHOLDER}]", text)


def _statements(error: BaseException) -> set[str]:
    """The SQL statements of every SQLAlchemy error in the cause and context chain."""
    found: set[str] = set()
    seen: set[int] = set()
    pending: list[BaseException | None] = [error]
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, StatementError) and current.statement:
            found.add(str(current.statement))
        pending.extend([current.__cause__, current.__context__])
    return found
