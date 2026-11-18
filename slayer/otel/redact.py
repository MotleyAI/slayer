"""Exception fields for export: without ``SLAYER_OTEL_CAPTURE=sql``, no database error text leaves.

A database error's text can hold the SQL, an excerpt of it, or a value from the data, for
example ``[SQL: ...]``, DuckDB's ``LINE 3: WHERE ...`` or Postgres's ``DETAIL: Key (id)=(42)``.
Matching that text cannot be complete, so this module does not try. If the exception chain
holds a database error and ``sql`` is off, the fields keep only what cannot carry query text:
the exception classes, the SQLSTATE code if the driver gives one, and the stack frames
(file, line, function and source line), with no exception message. Any other exception
leaves in full. With ``sql``, every exception leaves in full.
"""

from __future__ import annotations

import re
import traceback
from typing import Any

from sqlalchemy.exc import StatementError

# PEP 249 base classes: a driver's query errors derive from them, also when SQLAlchemy does not wrap them.
_DBAPI_BASES = frozenset({"DatabaseError", "InterfaceError"})
_SQLSTATE = re.compile(r"[0-9A-Z]{5}")
_CAUSE = "\nThe above exception was the direct cause of the following exception:\n\n"
_CONTEXT = "\nDuring handling of the above exception, another exception occurred:\n\n"


def exception_fields(error: BaseException, *, sql: bool) -> dict[str, Any]:
    """The OTel ``exception.*`` attributes; without ``sql``, a database error leaves without its text."""
    database = None if sql else _database_error(error)
    if database is None:
        return {
            "exception.type": type(error).__name__,
            "exception.message": str(error),
            "exception.stacktrace": "".join(traceback.format_exception(type(error), error, error.__traceback__)),
        }
    return {
        "exception.type": type(error).__name__,
        "exception.message": _summary(database),
        "exception.stacktrace": _frames_only(error),
    }


def _database_error(error: BaseException) -> BaseException | None:
    """The first database error in the cause and context chain or in an exception group, if any."""
    seen: set[int] = set()
    pending: list[BaseException | None] = [error]
    while pending:
        current = pending.pop(0)
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if _is_database_error(current):
            return current
        if isinstance(current, BaseExceptionGroup):
            pending.extend(current.exceptions)
        pending.extend([current.__cause__, current.__context__])
    return None


def _is_database_error(error: BaseException) -> bool:
    return isinstance(error, StatementError) or any(cls.__name__ in _DBAPI_BASES for cls in type(error).__mro__)


def _summary(database: BaseException) -> str:
    """``database error (BinderException)``, plus the SQLSTATE code when the driver gives one."""
    driver = getattr(database, "orig", None) or database
    code = _sqlstate(driver)
    return f"database error ({type(driver).__name__}{f', SQLSTATE {code}' if code else ''})"


def _sqlstate(driver: BaseException) -> str | None:
    # psycopg has .sqlstate, psycopg2 .pgcode; only a five-character code is trusted to carry no data.
    for value in (getattr(driver, "sqlstate", None), getattr(driver, "pgcode", None)):
        if isinstance(value, str) and _SQLSTATE.fullmatch(value):
            return value
    return None


def _frames_only(error: BaseException) -> str:
    """The traceback as Python prints it, with each exception message left out; the classes stay."""
    chain = _chain(error)
    parts: list[str] = []
    for inner, outer in zip(chain, chain[1:] + [None], strict=True):
        parts.append("Traceback (most recent call last):\n")
        parts.extend(traceback.format_list(traceback.extract_tb(inner.__traceback__)))
        parts.append(f"{_qualified(inner)}\n")
        if isinstance(inner, BaseExceptionGroup):
            parts.append(f"  members: {', '.join(_qualified(member) for member in inner.exceptions)}\n")
        if outer is not None:
            parts.append(_CAUSE if outer.__cause__ is inner else _CONTEXT)
    return "".join(parts)


def _chain(error: BaseException) -> list[BaseException]:
    """The exceptions Python prints for ``error``, innermost first."""
    chain: list[BaseException] = []
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        chain.append(current)
        current = current.__cause__ or (None if current.__suppress_context__ else current.__context__)
    return chain[::-1]


def _qualified(error: BaseException) -> str:
    module = type(error).__module__
    name = type(error).__qualname__
    return name if module == "builtins" else f"{module}.{name}"
