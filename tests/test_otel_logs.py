"""Log export: SLayer's own records leave as templates by default, and the host's loggers stay untouched."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from unittest import mock

import pytest
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter, SimpleLogRecordProcessor
from pydantic import BaseModel, ConfigDict
from sqlalchemy.exc import OperationalError

from slayer.otel import logs
from tests._usage_reporting_helpers import SENT, assert_no_sentinel

_TEMPLATE = "pg facade: cannot translate %r: %s"


class _Exported(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    exporter: InMemoryLogRecordExporter
    handlers: list[logging.Handler]

    def bodies(self) -> list[str]:
        return [str(data.log_record.body) for data in self.exporter.get_finished_logs()]

    def attributes(self) -> list[dict]:
        return [dict(data.log_record.attributes or {}) for data in self.exporter.get_finished_logs()]

    def everything(self) -> str:
        return "\n".join(f"{body} {attributes}" for body, attributes in zip(self.bodies(), self.attributes(), strict=True))


def _attach(*, full: bool = False) -> _Exported:
    exporter = InMemoryLogRecordExporter()
    provider = LoggerProvider(shutdown_on_exit=False)
    provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    return _Exported(exporter=exporter, handlers=logs.attach(logger_provider=provider, full=full))


def _attached(monkeypatch: pytest.MonkeyPatch, *, full: bool) -> Iterator[_Exported]:
    # A developer's own OTEL_PYTHON_LOG_LEVEL must not filter out the test's warning.
    monkeypatch.setenv("OTEL_PYTHON_LOG_LEVEL", "WARNING")
    exported = _attach(full=full)
    try:
        yield exported
    finally:
        logs.detach(exported.handlers)


@pytest.fixture
def templates(monkeypatch: pytest.MonkeyPatch) -> Iterator[_Exported]:
    yield from _attached(monkeypatch, full=False)


@pytest.fixture
def full(monkeypatch: pytest.MonkeyPatch) -> Iterator[_Exported]:
    yield from _attached(monkeypatch, full=True)


def _fail_and_log(logger: logging.Logger) -> None:
    try:
        raise ValueError(f"bad literal {SENT}")
    except ValueError:
        logger.exception(_TEMPLATE, SENT, "detail")


# --- templates -----------------------------------------------------------------------


def test_a_record_leaves_as_its_template(templates: _Exported) -> None:
    logging.getLogger("slayer.pg_facade").warning(_TEMPLATE, SENT, "detail")
    assert templates.bodies() == [_TEMPLATE]


def test_an_exception_leaves_as_its_class_only(templates: _Exported) -> None:
    _fail_and_log(logging.getLogger("slayer.pg_facade"))
    assert (templates.attributes()[0]["error.type"], "exception.stacktrace" in templates.attributes()[0]) == (
        "ValueError", False,
    )


def test_no_argument_or_exception_text_leaves(templates: _Exported) -> None:
    _fail_and_log(logging.getLogger("slayer.pg_facade"))
    assert_no_sentinel(templates.everything())


_RETRY_TEMPLATE = "Transient DB error on attempt %d, retrying in %.1fs: %s | sql: %s"


def _log_retry(*, sql: str) -> None:
    try:
        raise OperationalError(sql, {}, Exception("server closed the connection"))
    except OperationalError as error:
        logging.getLogger("slayer.database.client").warning(
            _RETRY_TEMPLATE, 1, 0.5, error, sql, exc_info=True,
        )


def test_a_retry_warning_sends_no_sql_by_default(templates: _Exported) -> None:
    _log_retry(sql=f"SELECT * FROM orders WHERE note = '{SENT}'")
    assert templates.bodies() == [_RETRY_TEMPLATE]
    assert_no_sentinel(templates.everything())


def test_full_records_leave_with_content_capture(full: _Exported) -> None:
    _fail_and_log(logging.getLogger("slayer.pg_facade"))
    attributes = full.attributes()[0]
    assert (SENT in full.bodies()[0], SENT in attributes["exception.message"], bool(attributes["exception.stacktrace"])) == (
        True, True, True,
    )


def test_a_retry_warning_sends_its_sql_with_content_capture(full: _Exported) -> None:
    _log_retry(sql=f"SELECT * FROM orders WHERE note = '{SENT}'")
    assert SENT in full.bodies()[0]


# --- scope ---------------------------------------------------------------------------


def test_the_hosts_loggers_are_not_exported(templates: _Exported) -> None:
    logging.getLogger("uvicorn.error").warning("host message")
    logging.getLogger().warning("root message")
    assert templates.bodies() == []


def test_detach_removes_every_added_handler() -> None:
    before = list(logging.getLogger(logs.SLAYER_LOGGER).handlers)
    exported = _attach()
    logs.detach(exported.handlers)
    assert logging.getLogger(logs.SLAYER_LOGGER).handlers == before


def test_stderr_output_survives_when_slayer_had_no_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(logging.getLogger(logs.SLAYER_LOGGER), "propagate", False)
    exported = _attach()
    try:
        assert logging.lastResort in exported.handlers
    finally:
        logs.detach(exported.handlers)


def test_no_stderr_handler_is_added_when_a_handler_exists(templates: _Exported) -> None:
    assert logging.lastResort not in templates.handlers


# --- level ---------------------------------------------------------------------------


_LEVELS = [
    ("info", logging.INFO),
    ("DEBUG", logging.DEBUG),
    ("error", logging.ERROR),
]


@pytest.mark.parametrize(("raw", "level"), _LEVELS)
def test_otel_python_log_level_sets_the_slayer_logger_level(
    monkeypatch: pytest.MonkeyPatch, raw: str, level: int,
) -> None:
    monkeypatch.setenv("OTEL_PYTHON_LOG_LEVEL", raw)
    exported = _attach()
    try:
        assert logging.getLogger(logs.SLAYER_LOGGER).level == level
    finally:
        logs.detach(exported.handlers)


def test_detach_restores_the_previous_level(monkeypatch: pytest.MonkeyPatch) -> None:
    logger = logging.getLogger(logs.SLAYER_LOGGER)
    before = logger.level
    monkeypatch.setenv("OTEL_PYTHON_LOG_LEVEL", "debug")
    logs.detach(_attach().handlers)
    assert logger.level == before


def test_an_unknown_level_leaves_the_level_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    logger = logging.getLogger(logs.SLAYER_LOGGER)
    before = logger.level
    monkeypatch.setenv("OTEL_PYTHON_LOG_LEVEL", "loud")
    exported = _attach()
    try:
        assert logger.level == before
    finally:
        logs.detach(exported.handlers)


def test_a_failing_emit_never_reaches_the_caller(templates: _Exported) -> None:
    handler = next(h for h in templates.handlers if isinstance(h, logs.TemplateLogHandler))
    broken = mock.Mock(**{"emit.side_effect": RuntimeError("SDK bug")})
    with mock.patch.object(handler.logger_provider, "get_logger", return_value=broken):
        logging.getLogger("slayer.pg_facade").warning(_TEMPLATE, SENT, "detail")
    assert broken.emit.called
