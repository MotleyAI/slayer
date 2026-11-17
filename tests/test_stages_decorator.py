"""core.stages.staged: a decorated function behaves as before; the stage only adds what a listener reads."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Iterator

import pytest

from slayer.core import stages


class _Recorder:
    def __init__(self) -> None:
        self.ended: list[tuple[stages.Stage, BaseException | None]] = []

    def on_start(self, stage: stages.Stage) -> None:
        return None

    def on_end(self, stage: stages.Stage, *, error: BaseException | None) -> None:
        self.ended.append((stage, error))

    def last(self) -> stages.Stage:
        return self.ended[-1][0]


@pytest.fixture
def recorder() -> Iterator[_Recorder]:
    listener = _Recorder()
    stages.add_listener(listener)
    try:
        yield listener
    finally:
        stages.remove_listener(listener)


@stages.staged(
    name="db.execute",
    attributes={"surface": "cli"},
    params={"datasource.name": "datasource"},
    result=lambda rows: {"db.response.returned_rows": len(rows)},
)
def _sync_execute(sql: str, *, datasource: str, limit: int = 10) -> list[int]:
    return list(range(min(limit, 3)))


@stages.staged(name="query", params={"query.dry_run": "dry_run"}, result=lambda rows: {"rows": len(rows)})
async def _async_execute(*, dry_run: bool = False) -> list[int]:
    await asyncio.sleep(0)
    return [] if dry_run else [1, 2]


@stages.staged(name="query", extract=lambda bound: {"operation": bound.arguments["request"]["op"]})
def _extracting(request: dict) -> str:
    return request["op"]


@stages.staged(name="query", extract=lambda bound: {"operation": bound.arguments["missing"]})
def _bad_extract(value: int) -> int:
    return value


@stages.staged(name="query", result=lambda value: {"rows": len(value)})
def _bad_result(value: int) -> int:
    return value


@stages.staged(name="query")
def _failing() -> None:
    raise LookupError("no such model")


# --- unchanged behavior --------------------------------------------------------------


def test_sync_function_returns_its_value_without_listener() -> None:
    assert _sync_execute("select 1", datasource="shop") == [0, 1, 2]


def test_async_function_returns_its_value_without_listener() -> None:
    assert asyncio.run(_async_execute()) == [1, 2]


def test_wrapper_keeps_the_name_and_signature() -> None:
    assert (_sync_execute.__name__, list(inspect.signature(_sync_execute).parameters)) == (
        "_sync_execute", ["sql", "datasource", "limit"],
    )


def test_async_wrapper_stays_a_coroutine_function() -> None:
    assert inspect.iscoroutinefunction(_async_execute)


def test_an_error_still_raises_with_a_listener(recorder: _Recorder) -> None:
    with pytest.raises(LookupError, match="no such model"):
        _failing()
    assert isinstance(recorder.ended[-1][1], LookupError)


# --- attributes ----------------------------------------------------------------------


def test_params_fixed_attributes_and_result_reach_the_stage(recorder: _Recorder) -> None:
    _sync_execute("select 1", datasource="shop", limit=2)
    assert recorder.last().attributes == {"surface": "cli", "datasource.name": "shop", "db.response.returned_rows": 2}


def test_async_params_and_result_reach_the_stage(recorder: _Recorder) -> None:
    asyncio.run(_async_execute(dry_run=True))
    assert recorder.last().attributes == {"query.dry_run": True, "rows": 0}


def test_extract_reads_the_bound_arguments(recorder: _Recorder) -> None:
    _extracting({"op": "query"})
    assert recorder.last().attributes == {"operation": "query"}


def test_a_failing_extract_leaves_the_call_unchanged(recorder: _Recorder) -> None:
    assert (_bad_extract(5), recorder.last().attributes) == (5, {})


def test_a_failing_result_hook_leaves_the_value_unchanged(recorder: _Recorder) -> None:
    assert (_bad_result(5), recorder.last().attributes) == (5, {})


# --- decoration-time checks ----------------------------------------------------------


def test_an_unknown_parameter_fails_at_decoration() -> None:
    decorate = stages.staged(name="query", params={"rows": "no_such_param"})
    with pytest.raises(TypeError, match="no_such_param"):
        decorate(lambda value: value)


def _generator() -> Iterator[int]:
    yield 1


async def _async_generator():  # noqa: ANN202 - the async generator type is not the point here
    yield 1


@pytest.mark.parametrize("fn", [_generator, _async_generator])
def test_a_generator_is_refused(fn) -> None:
    decorate = stages.staged(name="query")
    with pytest.raises(TypeError, match="generator"):
        decorate(fn)
