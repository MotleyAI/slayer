"""OTLP export of SLayer's own log records.

By default a record leaves as its message template (``"pg facade: cannot translate %r: %s"``),
logger name, level and code location; its arguments, ``extra`` fields, exception message and
traceback stay in the process, and the exception is reported by class as ``error.type``. SLayer
logs every value as a lazy ``%`` argument, so the template never carries SQL, literals or
credentials. ``SLAYER_OTEL_CAPTURE=exceptions`` adds the exception message and stack trace. For a
database error, only its classes, SQLSTATE code and stack frames leave (see ``redact``).
``exceptions`` and ``sql`` together send the full record: the formatted message, the ``extra``
fields and the complete exception.

The handler writes to the OTel Logs API directly, so it needs neither the SDK's deprecated
``LoggingHandler`` nor ``opentelemetry-instrumentation-logging``.
"""

from __future__ import annotations

import contextlib
import logging
import os
import time
from collections.abc import Iterable
from typing import Any

from opentelemetry._logs import SeverityNumber
from opentelemetry.context import get_current
from opentelemetry.sdk._logs import LoggerProvider

from slayer.otel import redact

SLAYER_LOGGER = "slayer"
_previous_level: int | None = None

_RESERVED = frozenset(vars(logging.makeLogRecord({}))) | {"message", "asctime", "taskName"}
_SEVERITIES = [
    (logging.CRITICAL, SeverityNumber.FATAL, "FATAL"),
    (logging.ERROR, SeverityNumber.ERROR, "ERROR"),
    (logging.WARNING, SeverityNumber.WARN, "WARN"),
    (logging.INFO, SeverityNumber.INFO, "INFO"),
    (logging.DEBUG, SeverityNumber.DEBUG, "DEBUG"),
]


class TemplateLogHandler(logging.Handler):
    """Exports each record as its template, plus what ``capture`` allows; never raises."""

    def __init__(self, *, logger_provider: LoggerProvider, capture: Iterable[str] = ()) -> None:
        super().__init__()
        self.logger_provider = logger_provider
        self.capture = frozenset(capture)
        # Arguments and ``extra`` fields can carry SQL, so the full record needs both opt-ins.
        self.full = {"exceptions", "sql"} <= self.capture

    def emit(self, record: logging.LogRecord) -> None:
        # Fail-silent without logging: a log call from inside a log handler can recurse.
        with contextlib.suppress(Exception):
            severity_number, severity_text = _severity(record.levelno)
            self.logger_provider.get_logger(record.name).emit(
                timestamp=int(record.created * 1e9),
                observed_timestamp=time.time_ns(),
                context=get_current(),
                severity_number=severity_number,
                severity_text=severity_text,
                body=record.getMessage() if self.full else _template(record),
                attributes=_attributes(record=record, capture=self.capture),
            )


def attach(*, logger_provider: LoggerProvider, capture: Iterable[str] = ()) -> list[logging.Handler]:
    """Attach the handler to the ``slayer`` logger only; the host's other loggers stay untouched.

    The level follows Python logging; ``OTEL_PYTHON_LOG_LEVEL`` (the OTel Python name), if set,
    sets it for the ``slayer`` logger. Python prints to stderr through ``logging.lastResort``
    only while no handler exists on a record's path, so keep that output by adding it too.
    """
    global _previous_level
    logger = logging.getLogger(SLAYER_LOGGER)
    added: list[logging.Handler] = []
    if not logger.hasHandlers() and logging.lastResort is not None:
        added.append(logging.lastResort)
    added.append(TemplateLogHandler(logger_provider=logger_provider, capture=capture))
    level = logging.getLevelName(os.environ.get("OTEL_PYTHON_LOG_LEVEL", "").strip().upper())
    if isinstance(level, int):
        _previous_level = logger.level
        logger.setLevel(level)
    for handler in added:
        logger.addHandler(handler)
    return added


def detach(handlers: list[logging.Handler]) -> None:
    global _previous_level
    logger = logging.getLogger(SLAYER_LOGGER)
    for handler in handlers:
        logger.removeHandler(handler)
    if _previous_level is not None:
        logger.setLevel(_previous_level)
        _previous_level = None


def _severity(levelno: int) -> tuple[SeverityNumber, str]:
    for threshold, number, text in _SEVERITIES:
        if levelno >= threshold:
            return number, text
    return SeverityNumber.TRACE, "TRACE"


def _template(record: logging.LogRecord) -> str:
    return record.msg if isinstance(record.msg, str) else type(record.msg).__qualname__


def _attributes(*, record: logging.LogRecord, capture: frozenset[str]) -> dict[str, Any]:
    attributes: dict[str, Any] = {
        "code.file.path": record.pathname,
        "code.function.name": record.funcName,
        "code.line.number": record.lineno,
    }
    exc_type, exc_value, _ = record.exc_info or (None, None, None)
    if exc_type is not None:
        attributes["error.type"] = exc_type.__qualname__
    sql = "sql" in capture
    if sql and "exceptions" in capture:
        attributes.update({key: value for key, value in vars(record).items() if key not in _RESERVED})
    if exc_value is not None and "exceptions" in capture:
        attributes.update(redact.exception_fields(exc_value, sql=sql))
    return attributes
