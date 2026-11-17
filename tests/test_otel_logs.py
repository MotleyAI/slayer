"""Log export: SLayer's own records leave as templates by default, and the host's loggers stay untouched."""

from __future__ import annotations

import logging
from collections.abc import Iterator

import pytest
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter, SimpleLogRecordProcessor
from pydantic import BaseModel, ConfigDict

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


def _attach(*, full: bool) -> _Exported:
    exporter = InMemoryLogRecordExporter()
    provider = LoggerProvider(shutdown_on_exit=False)
    provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    return _Exported(exporter=exporter, handlers=logs.attach(logger_provider=provider, full=full))


@pytest.fixture
def templates() -> Iterator[_Exported]:
    exported = _attach(full=False)
    try:
        yield exported
    finally:
        logs.detach(exported.handlers)


@pytest.fixture
def full() -> Iterator[_Exported]:
    exported = _attach(full=True)
    try:
        yield exported
    finally:
        logs.detach(exported.handlers)


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


def test_full_records_leave_with_capture_exceptions(full: _Exported) -> None:
    _fail_and_log(logging.getLogger("slayer.pg_facade"))
    assert (SENT in full.bodies()[0], "exception.stacktrace" in full.attributes()[0]) == (True, True)


# --- scope ---------------------------------------------------------------------------


def test_the_hosts_loggers_are_not_exported(templates: _Exported) -> None:
    logging.getLogger("uvicorn.error").warning("host message")
    logging.getLogger().warning("root message")
    assert templates.bodies() == []


def test_detach_removes_every_added_handler() -> None:
    before = list(logging.getLogger(logs.SLAYER_LOGGER).handlers)
    exported = _attach(full=False)
    logs.detach(exported.handlers)
    assert logging.getLogger(logs.SLAYER_LOGGER).handlers == before


def test_stderr_output_survives_when_slayer_had_no_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(logging.getLogger(logs.SLAYER_LOGGER), "propagate", False)
    exported = _attach(full=False)
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
    exported = _attach(full=False)
    try:
        assert logging.getLogger(logs.SLAYER_LOGGER).level == level
    finally:
        logs.detach(exported.handlers)


def test_detach_restores_the_previous_level(monkeypatch: pytest.MonkeyPatch) -> None:
    logger = logging.getLogger(logs.SLAYER_LOGGER)
    before = logger.level
    monkeypatch.setenv("OTEL_PYTHON_LOG_LEVEL", "debug")
    logs.detach(_attach(full=False).handlers)
    assert logger.level == before


def test_an_unknown_level_leaves_the_level_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    logger = logging.getLogger(logs.SLAYER_LOGGER)
    before = logger.level
    monkeypatch.setenv("OTEL_PYTHON_LOG_LEVEL", "loud")
    exported = _attach(full=False)
    try:
        assert logger.level == before
    finally:
        logs.detach(exported.handlers)


def test_a_failing_export_never_reaches_the_caller(templates: _Exported, monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(*args: object, **kwargs: object) -> None:
        raise RuntimeError("exporter bug")

    monkeypatch.setattr(templates.exporter, "export", broken)
    logging.getLogger("slayer.pg_facade").warning(_TEMPLATE, SENT, "detail")
    assert templates.bodies() == []
