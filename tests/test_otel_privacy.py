"""Sensitive data leaves only on the operator's opt-in: SQL with ``SLAYER_OTEL_CAPTURE=sql``, exception text with ``exceptions``."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator

import pytest

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
def capturing(request: pytest.FixtureRequest) -> Iterator[OtelCapture]:
    otel_capture = OtelCapture.build()
    otel_capture.instrument(capture=tuple(request.param))
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
        stages.current().add_event(name="db.retry", attributes={"db.query.text": f"select '{SENT}'"})
    assert_no_sentinel(otel_env.everything())


# --- opt-in --------------------------------------------------------------------------


@pytest.mark.parametrize("capturing", [("sql",)], indirect=True)
def test_capture_sql_sends_the_query_text(capturing: OtelCapture) -> None:
    _run(_FILTERED)
    assert "select" in str(capturing.attributes("slayer.db.execute")["db.query.text"]).lower()


@pytest.mark.parametrize("capturing", [("sql",)], indirect=True)
def test_capture_sql_keeps_the_query_text_off_metrics(capturing: OtelCapture) -> None:
    _run(_FILTERED)
    assert {key for point in capturing.metrics() for key in point.attributes} & {"db.query.text"} == set()


@pytest.mark.parametrize("capturing", [("exceptions",)], indirect=True)
def test_capture_exceptions_records_the_exception(capturing: OtelCapture) -> None:
    _fail()
    events = capturing.span("slayer.query").events
    assert ([event.name for event in events], SENT in str(dict(events[0].attributes or {}))) == (["exception"], True)


@pytest.mark.parametrize("capturing", [("exceptions",)], indirect=True)
def test_capture_exceptions_alone_keeps_sql_text_in_the_process(capturing: OtelCapture) -> None:
    _run(_FILTERED)
    assert "db.query.text" not in capturing.attributes("slayer.db.execute")
