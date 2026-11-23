"""A Flight SQL statement that cannot be translated is logged server-side, as the Postgres facade logs it."""

from __future__ import annotations

import logging

import pytest

from slayer.flight import server
from slayer.flight.translator import TranslationError


def test_a_translation_failure_is_logged_with_the_error_as_an_argument(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger=server.logger.name):
        server._translation_error_to_flight(TranslationError("unknown table secret_table"))
    (record,) = caplog.records
    assert (record.levelno, "secret_table" in str(record.msg), "secret_table" in record.getMessage()) == (
        logging.WARNING, False, True,
    )
