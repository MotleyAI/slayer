"""The cost of a ``@staged`` function: with no listener (OTel off), and with the OTel listener on.

Run with: poetry run pytest tests/perf/test_otel_bench.py --benchmark-only
"""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest

from slayer.core import stages
from tests._otel_helpers import OtelCapture


def _plain(value: int, *, name: str) -> int:
    return value


@stages.staged(name="db.execute", params={"datasource.name": "name"}, result=lambda value: {"rows": value})
def _decorated(value: int, *, name: str) -> int:
    return value


@pytest.fixture
def listening() -> Iterator[None]:
    capture = OtelCapture.build()
    capture.instrument()
    try:
        yield
    finally:
        capture.close()


_CALLS: dict[str, Callable[..., int]] = {"plain": _plain, "staged": _decorated}


@pytest.mark.benchmark(group="otel-off")
@pytest.mark.parametrize("call", list(_CALLS))
def test_otel_off(benchmark, call: str) -> None:
    assert benchmark(_CALLS[call], 3, name="shop") == 3


@pytest.mark.benchmark(group="otel-on")
def test_otel_on(benchmark, listening: None) -> None:
    assert benchmark(_decorated, 3, name="shop") == 3
