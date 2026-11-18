"""core.probes: with no listener every entry point is inert; with one, it sees each probe open and close."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator

import pytest

from slayer.core import probes


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str | None]] = []
        self.ended: list[probes.Probe] = []

    def on_start(self, probe: probes.Probe) -> None:
        self.calls.append(("start", probe.name, None))

    def on_end(self, probe: probes.Probe, *, error: BaseException | None) -> None:
        self.calls.append(("end", probe.name, type(error).__name__ if error else None))
        self.ended.append(probe)


class _Broken:
    def on_start(self, probe: probes.Probe) -> None:
        raise RuntimeError("listener bug")

    def on_end(self, probe: probes.Probe, *, error: BaseException | None) -> None:
        raise RuntimeError("listener bug")


@pytest.fixture
def recorder() -> Iterator[_Recorder]:
    listener = _Recorder()
    probes.add_listener(listener)
    try:
        yield listener
    finally:
        probes.remove_listener(listener)


# --- no listener ---------------------------------------------------------------------


def test_without_listener_scope_yields_the_inactive_probe() -> None:
    with probes.scope(name="query") as opened:
        opened.set({"rows": 3})
        opened.add_event(name="db.retry")
    assert (opened.active, opened.attributes, opened.events) == (False, {}, [])


def test_without_listener_current_is_inactive_and_ignores_writes() -> None:
    probes.current().set({"rows": 3})
    assert (probes.current().active, probes.current().attributes) == (False, {})


def test_without_listener_the_probe_scope_is_shared() -> None:
    assert probes.scope(name="a") is probes.scope(name="b")


# --- lifecycle -----------------------------------------------------------------------


def test_listener_sees_start_then_end(recorder: _Recorder) -> None:
    with probes.scope(name="query"):
        pass
    assert recorder.calls == [("start", "query", None), ("end", "query", None)]


def test_nested_probes_link_to_their_parent(recorder: _Recorder) -> None:
    with probes.scope(name="query"), probes.scope(name="db.execute"):
        inner = probes.current()
    assert (inner.name, inner.parent.name if inner.parent else None) == ("db.execute", "query")


def test_current_returns_to_the_outer_probe_after_the_inner_one_closes(recorder: _Recorder) -> None:
    with probes.scope(name="query"):
        with probes.scope(name="db.execute"):
            pass
        assert probes.current().name == "query"
    assert probes.current().active is False


def test_writes_reach_the_probe_the_listener_ends(recorder: _Recorder) -> None:
    with probes.scope(name="query", attributes={"surface": "cli"}):
        probes.current().set({"rows": 3})
        probes.current().add_event(name="db.retry", attributes={"db.attempt": 2})
    ended = recorder.ended[0]
    assert (ended.attributes, [event.name for event in ended.events]) == ({"surface": "cli", "rows": 3}, ["db.retry"])


def test_duration_is_measured(recorder: _Recorder) -> None:
    with probes.scope(name="query"):
        pass
    assert recorder.ended[0].duration_s > 0


_NON_SCALARS = [None, ["a"], {"a": 1}, object()]


@pytest.mark.parametrize("value", _NON_SCALARS)
def test_non_scalar_attribute_values_are_dropped(recorder: _Recorder, value: object) -> None:
    with probes.scope(name="query", attributes={"kept": "yes", "dropped": value}):  # type: ignore[dict-item]
        pass
    assert recorder.ended[0].attributes == {"kept": "yes"}


# --- errors --------------------------------------------------------------------------


def test_an_error_reaches_the_listener_and_still_raises(recorder: _Recorder) -> None:
    with pytest.raises(ValueError, match="boom"), probes.scope(name="query"):
        raise ValueError("boom")
    assert recorder.calls[-1] == ("end", "query", "ValueError")


def test_a_failing_listener_never_reaches_the_host(recorder: _Recorder) -> None:
    broken = _Broken()
    probes.add_listener(broken)
    try:
        with probes.scope(name="query"):
            value = 42
    finally:
        probes.remove_listener(broken)
    assert (value, recorder.calls[-1]) == (42, ("end", "query", None))


class _RaisingHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        raise RuntimeError("handler bug")

    def handleError(self, record: logging.LogRecord) -> None:  # noqa: N802 - logging API name
        raise RuntimeError("handler bug")


def test_a_failing_debug_handler_never_reaches_the_host(recorder: _Recorder) -> None:
    logger = logging.getLogger("slayer.core.probes")
    handler, level = _RaisingHandler(), logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    probes.add_listener(broken := _Broken())
    try:
        with probes.scope(name="query"):
            value = 42
    finally:
        probes.remove_listener(broken)
        logger.removeHandler(handler)
        logger.setLevel(level)
    assert value == 42


def test_a_finished_probe_ignores_writes(recorder: _Recorder) -> None:
    with probes.scope(name="query") as finished:
        pass
    finished.add_event(name="db.retry")
    assert (finished.active, finished.events) == (False, [])


def test_a_scope_takes_its_parent_when_entered(recorder: _Recorder) -> None:
    later = probes.scope(name="inner")
    with probes.scope(name="outer") as outer, later as inner:
        pass
    assert inner.parent is outer


# --- registry ------------------------------------------------------------------------


def test_adding_a_listener_twice_registers_it_once(recorder: _Recorder) -> None:
    probes.add_listener(recorder)
    with probes.scope(name="query"):
        pass
    assert len(recorder.calls) == 2


def test_a_removed_listener_sees_nothing() -> None:
    listener = _Recorder()
    probes.add_listener(listener)
    probes.remove_listener(listener)
    with probes.scope(name="query"):
        pass
    assert listener.calls == []


def test_a_probe_ends_on_the_listeners_it_started_on() -> None:
    first, second = _Recorder(), _Recorder()
    probes.add_listener(first)
    try:
        with probes.scope(name="query"):
            probes.remove_listener(first)
            probes.add_listener(second)
    finally:
        probes.remove_listener(first)
        probes.remove_listener(second)
    assert (first.calls, second.calls) == ([("start", "query", None), ("end", "query", None)], [])


def test_a_scope_takes_its_listeners_when_entered() -> None:
    first, second = _Recorder(), _Recorder()
    probes.add_listener(first)
    try:
        later = probes.scope(name="query")
        probes.remove_listener(first)
        probes.add_listener(second)
        with later:
            pass
    finally:
        probes.remove_listener(first)
        probes.remove_listener(second)
    assert (first.calls, second.calls) == ([], [("start", "query", None), ("end", "query", None)])


class _EqualRecorder(_Recorder):
    def __eq__(self, other: object) -> bool:
        return isinstance(other, _EqualRecorder)

    __hash__ = object.__hash__


def test_two_equal_listeners_are_both_registered() -> None:
    first, second = _EqualRecorder(), _EqualRecorder()
    probes.add_listener(first)
    probes.add_listener(second)
    try:
        with probes.scope(name="query"):
            pass
    finally:
        probes.remove_listener(first)
        probes.remove_listener(second)
    assert (len(first.calls), len(second.calls)) == (2, 2)


# --- concurrency ---------------------------------------------------------------------


def test_concurrent_tasks_keep_their_own_current_probe(recorder: _Recorder) -> None:
    async def run(name: str) -> str | None:
        with probes.scope(name=name):
            await asyncio.sleep(0.01)
            with probes.scope(name="db.execute"):
                parent = probes.current().parent
                return parent.name if parent else None

    async def main() -> list[str | None]:
        return list(await asyncio.gather(run("query"), run("ingest")))

    assert asyncio.run(main()) == ["query", "ingest"]
