"""OTLP export of SLayer's own log records.

By default a record leaves as its message template (``"pg facade: cannot translate %r: %s"``),
logger name, level and code location; its arguments, ``extra`` fields, exception message and
traceback stay in the process, and the exception is reported by class as ``error.type``. SLayer
logs every value as a lazy ``%`` argument, so the template never carries SQL, literals or
credentials. With ``SLAYER_OTEL_CAPTURE_CONTENT=on`` the full record leaves: the formatted
message, the ``extra`` fields, and the exception message and stack trace.

The handler writes to the OTel Logs API directly, so it needs neither the SDK's deprecated
``LoggingHandler`` nor ``opentelemetry-instrumentation-logging``.
"""

from __future__ import annotations

import contextlib
import logging
import os
import time
import traceback
from typing import Any

from opentelemetry._logs import SeverityNumber
from opentelemetry.context import get_current
from opentelemetry.sdk._logs import LoggerProvider

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
    """Exports each record as its template, or in full when ``full``; never raises."""

    def __init__(self, *, logger_provider: LoggerProvider, full: bool = False) -> None:
        super().__init__()
        self.logger_provider = logger_provider
        self.full = full

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
                attributes=_attributes(record=record, full=self.full),
            )


def attach(*, logger_provider: LoggerProvider, full: bool = False) -> list[logging.Handler]:
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
    added.append(TemplateLogHandler(logger_provider=logger_provider, full=full))
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


def _attributes(*, record: logging.LogRecord, full: bool) -> dict[str, Any]:
    attributes: dict[str, Any] = {
        "code.file.path": record.pathname,
        "code.function.name": record.funcName,
        "code.line.number": record.lineno,
    }
    exc_type, exc_value, exc_traceback = record.exc_info or (None, None, None)
    if exc_type is not None:
        attributes["error.type"] = exc_type.__qualname__
    if not full:
        return attributes
    attributes.update({key: value for key, value in vars(record).items() if key not in _RESERVED})
    if exc_type is not None:
        attributes["exception.type"] = exc_type.__name__
        attributes["exception.message"] = str(exc_value)
        attributes["exception.stacktrace"] = "".join(
            traceback.format_exception(exc_type, exc_value, exc_traceback),
        )
    return attributes
