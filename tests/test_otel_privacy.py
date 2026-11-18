"""Sensitive data leaves only on the operator's opt-in: SQL with ``SLAYER_OTEL_CAPTURE=sql``, exception text with ``exceptions``."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError, OperationalError

from slayer.core import stages
from slayer.otel import redact
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
        stages.current().add_event(name="db.retry", attributes={"db.query.text": f"select '{SENT}'", "attempt": 1})
    events = otel_env.span("slayer.db.execute").events
    assert ([event.name for event in events], "db.query.text" in dict(events[0].attributes or {})) == (
        ["slayer.db.retry"], False,
    )
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
    attributes = dict(events[0].attributes or {})
    assert ([event.name for event in events], SENT in str(attributes["exception.message"])) == (["exception"], True)
    assert attributes["exception.stacktrace"]


@pytest.mark.parametrize("capturing", [("exceptions",)], indirect=True)
def test_capture_exceptions_alone_keeps_sql_text_in_the_process(capturing: OtelCapture) -> None:
    _run(_FILTERED)
    assert "db.query.text" not in capturing.attributes("slayer.db.execute")


_FAILED_SQL = f"SELECT * FROM orders WHERE region = '{SENT}'"
_DUCKDB_SQL = f"SELECT x\nFROM (SELECT 1 AS x) AS t\nWHERE nope_column = '{SENT}'"


def _raise_wrapped_statement_error() -> None:
    """A failed statement, re-wrapped as SLayer's client does, with the SQL in the chain and the text."""
    try:
        raise OperationalError(_FAILED_SQL, {"region": "param-value"}, Exception("no such column"))
    except OperationalError as error:
        raise RuntimeError(f"query failed: {error}") from error


def _failing_statement() -> None:
    with pytest.raises(RuntimeError), stages.stage(name="db.execute"):
        _raise_wrapped_statement_error()


@pytest.mark.parametrize("capturing", [("exceptions",)], indirect=True)
def test_capture_exceptions_alone_removes_sql_from_the_exception(capturing: OtelCapture) -> None:
    _failing_statement()
    attributes = dict(capturing.span("slayer.db.execute").events[0].attributes or {})
    assert (attributes["exception.message"], "sqlalchemy.exc.OperationalError" in str(attributes["exception.stacktrace"])) == (
        "database error (Exception)", True,
    )
    assert_no_sentinel(capturing.everything())


@pytest.mark.parametrize("capturing", [("exceptions", "sql")], indirect=True)
def test_capture_exceptions_and_sql_keep_the_statement(capturing: OtelCapture) -> None:
    _failing_statement()
    attributes = dict(capturing.span("slayer.db.execute").events[0].attributes or {})
    assert (_FAILED_SQL in str(attributes["exception.message"]), "param-value" in str(attributes["exception.message"])) == (
        True, True,
    )


def _duckdb_bind_error() -> Exception:
    engine = sa.create_engine("duckdb:///:memory:")
    try:
        with engine.connect() as connection:
            connection.execute(sa.text(_DUCKDB_SQL))
    except DBAPIError as error:
        return error
    finally:
        engine.dispose()
    raise AssertionError("the statement did not fail")


@pytest.mark.parametrize("wrap", [False, True], ids=["plain", "exception-group"])
def test_redaction_removes_driver_excerpts_and_group_members(wrap: bool) -> None:
    error: Exception = _duckdb_bind_error()
    if wrap:
        try:
            raise ExceptionGroup("batch", [error])
        except ExceptionGroup as group:
            error = group
    fields = redact.exception_fields(error, sql=False)
    assert fields["exception.message"] == "database error (BinderException)"
    second_line = _DUCKDB_SQL.splitlines()[1]
    assert [(SENT in fields[key], second_line in fields[key]) for key in ("exception.message", "exception.stacktrace")] == [
        (False, False), (False, False),
    ]


class _PostgresStyleError(Exception):
    pgcode = "23505"


def test_redaction_keeps_the_sqlstate_and_drops_detail_values() -> None:
    try:
        raise OperationalError(_FAILED_SQL, {}, _PostgresStyleError(f"duplicate key\nDETAIL: Key (tenant)=({SENT})"))
    except OperationalError as error:
        fields = redact.exception_fields(error, sql=False)
    assert (fields["exception.message"], SENT in str(fields)) == ("database error (_PostgresStyleError, SQLSTATE 23505)", False)


def test_redaction_leaves_other_exceptions_in_full() -> None:
    try:
        raise ValueError(f"no model {SENT}")
    except ValueError as error:
        fields = redact.exception_fields(error, sql=False)
    assert SENT in fields["exception.message"]


def test_redaction_keeps_everything_with_sql() -> None:
    error = _duckdb_bind_error()
    assert _DUCKDB_SQL in redact.exception_fields(error, sql=True)["exception.message"]


class _Unprintable(Exception):
    def __str__(self) -> str:
        raise RuntimeError("no text")


def _raise_unprintable() -> None:
    raise _Unprintable


@pytest.mark.parametrize("capturing", [("exceptions",)], indirect=True)
def test_an_unprintable_exception_still_records_error_type_and_metrics(capturing: OtelCapture) -> None:
    with pytest.raises(_Unprintable), stages.stage(name="query"):
        _raise_unprintable()
    durations = capturing.metric("slayer.query.duration")
    assert (capturing.attributes("slayer.query")["error.type"], [p.attributes.get("error.type") for p in durations]) == (
        "_Unprintable", ["_Unprintable"],
    )
