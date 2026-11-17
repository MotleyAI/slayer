"""core.stages: with no listener every entry point is inert; with one, it sees each stage open and close."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator

import pytest

from slayer.core import stages


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str | None]] = []
        self.ended: list[stages.Stage] = []

    def on_start(self, stage: stages.Stage) -> None:
        self.calls.append(("start", stage.name, None))

    def on_end(self, stage: stages.Stage, *, error: BaseException | None) -> None:
        self.calls.append(("end", stage.name, type(error).__name__ if error else None))
        self.ended.append(stage)


class _Broken:
    def on_start(self, stage: stages.Stage) -> None:
        raise RuntimeError("listener bug")

    def on_end(self, stage: stages.Stage, *, error: BaseException | None) -> None:
        raise RuntimeError("listener bug")


@pytest.fixture
def recorder() -> Iterator[_Recorder]:
    listener = _Recorder()
    stages.add_listener(listener)
    try:
        yield listener
    finally:
        stages.remove_listener(listener)


# --- no listener ---------------------------------------------------------------------


def test_without_listener_stage_yields_the_inactive_stage() -> None:
    with stages.stage(name="query") as opened:
        opened.set({"rows": 3})
        opened.add_event(name="db.retry")
    assert (opened.active, opened.attributes, opened.events) == (False, {}, [])


def test_without_listener_current_is_inactive_and_ignores_writes() -> None:
    stages.current().set({"rows": 3})
    assert (stages.current().active, stages.current().attributes) == (False, {})


def test_without_listener_the_stage_scope_is_shared() -> None:
    assert stages.stage(name="a") is stages.stage(name="b")


# --- lifecycle -----------------------------------------------------------------------


def test_listener_sees_start_then_end(recorder: _Recorder) -> None:
    with stages.stage(name="query"):
        pass
    assert recorder.calls == [("start", "query", None), ("end", "query", None)]


def test_nested_stages_link_to_their_parent(recorder: _Recorder) -> None:
    with stages.stage(name="query"), stages.stage(name="db.execute"):
        inner = stages.current()
    assert (inner.name, inner.parent.name if inner.parent else None) == ("db.execute", "query")


def test_current_returns_to_the_outer_stage_after_the_inner_one_closes(recorder: _Recorder) -> None:
    with stages.stage(name="query"):
        with stages.stage(name="db.execute"):
            pass
        assert stages.current().name == "query"
    assert stages.current().active is False


def test_writes_reach_the_stage_the_listener_ends(recorder: _Recorder) -> None:
    with stages.stage(name="query", attributes={"surface": "cli"}):
        stages.current().set({"rows": 3})
        stages.current().add_event(name="db.retry", attributes={"db.attempt": 2})
    ended = recorder.ended[0]
    assert (ended.attributes, [event.name for event in ended.events]) == ({"surface": "cli", "rows": 3}, ["db.retry"])


def test_duration_is_measured(recorder: _Recorder) -> None:
    with stages.stage(name="query"):
        pass
    assert recorder.ended[0].duration_s > 0


_NON_SCALARS = [None, ["a"], {"a": 1}, object()]


@pytest.mark.parametrize("value", _NON_SCALARS)
def test_non_scalar_attribute_values_are_dropped(recorder: _Recorder, value: object) -> None:
    with stages.stage(name="query", attributes={"kept": "yes", "dropped": value}):  # type: ignore[dict-item]
        pass
    assert recorder.ended[0].attributes == {"kept": "yes"}


# --- errors --------------------------------------------------------------------------


def test_an_error_reaches_the_listener_and_still_raises(recorder: _Recorder) -> None:
    with pytest.raises(ValueError, match="boom"), stages.stage(name="query"):
        raise ValueError("boom")
    assert recorder.calls[-1] == ("end", "query", "ValueError")


def test_a_failing_listener_never_reaches_the_host(recorder: _Recorder) -> None:
    broken = _Broken()
    stages.add_listener(broken)
    try:
        with stages.stage(name="query"):
            value = 42
    finally:
        stages.remove_listener(broken)
    assert (value, recorder.calls[-1]) == (42, ("end", "query", None))


# --- registry ------------------------------------------------------------------------


def test_adding_a_listener_twice_registers_it_once(recorder: _Recorder) -> None:
    stages.add_listener(recorder)
    with stages.stage(name="query"):
        pass
    assert len(recorder.calls) == 2


def test_a_removed_listener_sees_nothing() -> None:
    listener = _Recorder()
    stages.add_listener(listener)
    stages.remove_listener(listener)
    with stages.stage(name="query"):
        pass
    assert listener.calls == []


# --- concurrency ---------------------------------------------------------------------


def test_concurrent_tasks_keep_their_own_current_stage(recorder: _Recorder) -> None:
    async def run(name: str) -> str | None:
        with stages.stage(name=name):
            await asyncio.sleep(0.01)
            with stages.stage(name="db.execute"):
                parent = stages.current().parent
                return parent.name if parent else None

    async def main() -> list[str | None]:
        return list(await asyncio.gather(run("query"), run("ingest")))

    assert asyncio.run(main()) == ["query", "ingest"]
