"""core.probes.probe: a decorated function behaves as before; the probe only adds what a listener reads."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy.exc

from slayer.core import probes
from slayer.database import client as client_module
from slayer.database import engine_factory
from slayer.engine.query_engine import SlayerQueryEngine
from tests._compat_helpers import future_warnings, recorded
from tests._gateway_harness import orders_dataset, seed_shop, shop_datasource, shop_storage


class _Recorder:
    def __init__(self) -> None:
        self.ended: list[tuple[probes.Probe, BaseException | None]] = []

    def on_start(self, probe: probes.Probe) -> None:
        return None

    def on_end(self, probe: probes.Probe, *, error: BaseException | None) -> None:
        self.ended.append((probe, error))

    def last(self) -> probes.Probe:
        return self.ended[-1][0]


@pytest.fixture
def recorder() -> Iterator[_Recorder]:
    listener = _Recorder()
    probes.add_listener(listener)
    try:
        yield listener
    finally:
        probes.remove_listener(listener)


@probes.probe(
    name="db.execute",
    attributes={"surface": "cli"},
    params={"datasource.name": "datasource"},
    result=lambda rows: {"db.response.returned_rows": len(rows)},
)
def _sync_execute(sql: str, *, datasource: str, limit: int = 10) -> list[int]:
    return list(range(min(limit, 3)))


@probes.probe(name="query", params={"query.dry_run": "dry_run"}, result=lambda rows: {"rows": len(rows)})
async def _async_execute(*, dry_run: bool = False) -> list[int]:
    await asyncio.sleep(0)
    return [] if dry_run else [1, 2]


@probes.probe(name="query", extract=lambda bound: {"operation": bound.arguments["request"]["op"]})
def _extracting(request: dict) -> str:
    return request["op"]


@probes.probe(name="query", extract=lambda bound: {"operation": bound.arguments["missing"]})
def _bad_extract(value: int) -> int:
    return value


@probes.probe(name="query", result=lambda value: {"rows": len(value)})
def _bad_result(value: int) -> int:
    return value


@probes.probe(name="query")
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


_LEGACY_FILTER_QUERY = {
    "population": "orders", "metrics": [{"expression": "count(*)", "name": "n"}], "filters": ["status == 'paid'"],
}


@pytest.mark.parametrize("listening", [False, True])
async def test_a_warning_through_a_probed_query_names_the_callers_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, listening: bool,
) -> None:
    # The autouse reparse-law guard is test code, so it would be the first frame outside SLayer.
    probed = inspect.unwrap(SlayerQueryEngine.execute, stop=lambda fn: fn.__code__.co_filename == probes.__file__)
    assert probed.__code__.co_filename == probes.__file__
    monkeypatch.setattr(SlayerQueryEngine, "execute", probed)
    db = str(tmp_path / "shop.db")
    seed_shop(db)
    datasource = shop_datasource(db)
    engine = SlayerQueryEngine(
        storage=await shop_storage(str(tmp_path / "store"), datasource=datasource, datasets=[orders_dataset()]),
    )
    listener = _Recorder()
    if listening:
        probes.add_listener(listener)
    try:
        with recorded() as caught:
            line = sys._getframe().f_lineno + 1
            await engine.execute(_LEGACY_FILTER_QUERY)
    finally:
        probes.remove_listener(listener)
        engine_factory.invalidate_engine(datasource)
    assert {(w.filename, w.lineno) for w in future_warnings(caught)} == {(__file__, line)}


# --- attributes ----------------------------------------------------------------------


def test_params_fixed_attributes_and_result_reach_the_probe(recorder: _Recorder) -> None:
    _sync_execute("select 1", datasource="shop", limit=2)
    assert recorder.last().attributes == {"surface": "cli", "datasource.name": "shop", "db.response.returned_rows": 2}


def test_async_params_and_result_reach_the_probe(recorder: _Recorder) -> None:
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
    decorate = probes.probe(name="query", params={"rows": "no_such_param"})
    with pytest.raises(TypeError, match="no_such_param"):
        decorate(lambda value: value)


def _generator() -> Iterator[int]:
    yield 1


async def _async_generator():  # noqa: ANN202 - the async generator type is not the point here
    yield 1


@pytest.mark.parametrize("fn", [_generator, _async_generator])
def test_a_generator_is_refused(fn) -> None:
    decorate = probes.probe(name="query")
    with pytest.raises(TypeError, match="generator"):
        decorate(fn)


# --- retry events ----------------------------------------------------------------------


def _locked() -> sqlalchemy.exc.OperationalError:
    return sqlalchemy.exc.OperationalError("SELECT 1", {}, Exception("database is locked"))


async def _retried(*, monkeypatch: pytest.MonkeyPatch, sleep: Any) -> list[str]:
    calls = iter([_locked(), None])

    async def do_call() -> Any:
        failure = next(calls)
        if failure is not None:
            raise failure

    monkeypatch.setattr(client_module.asyncio, "sleep", sleep)
    with probes.scope(name="db.execute") as probe:
        with contextlib.suppress(asyncio.CancelledError):
            await client_module._retry_with_backoff(
                sql="SELECT 1", do_call=do_call, max_attempts=2, initial_delay=0, max_delay=0,
            )
    return [event.name for event in probe.events]


async def test_a_retry_is_recorded_when_the_next_attempt_starts(
    recorder: _Recorder, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def no_wait(_delay: float) -> None:
        return None

    assert await _retried(monkeypatch=monkeypatch, sleep=no_wait) == ["db.retry"]


async def test_a_retry_cancelled_during_backoff_is_not_recorded(
    recorder: _Recorder, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def cancelled(_delay: float) -> None:
        raise asyncio.CancelledError

    assert await _retried(monkeypatch=monkeypatch, sleep=cancelled) == []
