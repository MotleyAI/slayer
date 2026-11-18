"""Content (SQL text, exception text) leaves only with ``SLAYER_OTEL_CAPTURE_CONTENT=on``."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator

import pytest
from sqlalchemy.exc import OperationalError

from slayer.core import stages
from tests._otel_helpers import OtelCapture
from tests._saved_query_refinement_fixtures import refine_engine
from tests._usage_reporting_helpers import SENT, assert_no_sentinel

_FILTERED = {"source_model": "orders", "measures": ["count(*)"], "filters": [f"region = '{SENT}'"]}
_MISSING = {"source_model": f"{SENT}_model", "measures": ["count(*)"]}


def _run(query: dict) -> None:
    async def main() -> None:
        async with refine_engine("sqlite") as (engine, _):
            await engine.execute(query)

    asyncio.run(main())


def _fail() -> None:
    with pytest.raises(Exception, match=SENT):
        _run(_MISSING)


@pytest.fixture
def capturing() -> Iterator[OtelCapture]:
    otel_capture = OtelCapture.build()
    otel_capture.instrument(capture_content=True)
    try:
        yield otel_capture
    finally:
        otel_capture.close()


# --- default: nothing sensitive leaves -----------------------------------------------


def test_sql_text_stays_in_the_process_by_default(otel_env: OtelCapture) -> None:
    _run(_FILTERED)
    assert ("db.query.text" in otel_env.attributes("slayer.db.execute"), SENT in otel_env.everything()) == (
        False, False,
    )


def test_exception_text_stays_in_the_process_by_default(otel_env: OtelCapture) -> None:
    _fail()
    events = [event.name for span in otel_env.spans() for event in span.events]
    assert ("exception" in events, otel_env.attributes("slayer.query")["error.type"]) == (False, "ModelNotFoundError")


def test_nothing_derived_from_a_failing_query_leaves(otel_env: OtelCapture) -> None:
    _fail()
    assert_no_sentinel(otel_env.everything())


def test_a_sensitive_key_is_dropped_from_events_too(otel_env: OtelCapture) -> None:
    with stages.stage(name="db.execute"):
        stages.current().add_event(name="db.retry", attributes={"db.query.text": f"select '{SENT}'", "attempt": 1})
    events = otel_env.span("slayer.db.execute").events
    assert ([event.name for event in events], "db.query.text" in dict(events[0].attributes or {})) == (
        ["slayer.db.retry"], False,
    )
    assert_no_sentinel(otel_env.everything())


# --- opt-in: SLAYER_OTEL_CAPTURE_CONTENT -----------------------------------------------


def test_content_capture_sends_the_query_text(capturing: OtelCapture) -> None:
    _run(_FILTERED)
    assert "select" in str(capturing.attributes("slayer.db.execute")["db.query.text"]).lower()


def test_content_capture_keeps_the_query_text_off_metrics(capturing: OtelCapture) -> None:
    _run(_FILTERED)
    assert {key for point in capturing.metrics() for key in point.attributes} & {"db.query.text"} == set()


def test_content_capture_records_the_exception(capturing: OtelCapture) -> None:
    _fail()
    events = capturing.span("slayer.query").events
    attributes = dict(events[0].attributes or {})
    assert ([event.name for event in events], SENT in str(attributes["exception.message"])) == (["exception"], True)
    assert attributes["exception.stacktrace"]


_FAILED_SQL = f"SELECT * FROM orders WHERE region = '{SENT}'"


def _raise_statement_error() -> None:
    raise OperationalError(_FAILED_SQL, {"region": "param-value"}, Exception("no such column"))


def test_content_capture_keeps_the_failed_statement_complete(capturing: OtelCapture) -> None:
    with pytest.raises(OperationalError), stages.stage(name="db.execute"):
        _raise_statement_error()
    message = str(dict(capturing.span("slayer.db.execute").events[0].attributes or {})["exception.message"])
    assert (_FAILED_SQL in message, "param-value" in message) == (True, True)


class _Unprintable(Exception):
    def __str__(self) -> str:
        raise RuntimeError("no text")


def _raise_unprintable() -> None:
    raise _Unprintable


def test_an_unprintable_exception_still_records_error_type_and_metrics(capturing: OtelCapture) -> None:
    with pytest.raises(_Unprintable), stages.stage(name="query"):
        _raise_unprintable()
    durations = capturing.metric("slayer.query.duration")
    assert (capturing.attributes("slayer.query")["error.type"], [p.attributes.get("error.type") for p in durations]) == (
        "_Unprintable", ["_Unprintable"],
    )
