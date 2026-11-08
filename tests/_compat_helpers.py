"""Warning capture for the 2.0-deprecation tests."""

from __future__ import annotations

import contextlib
import sys
import warnings
from collections.abc import Generator


def next_line() -> int:
    """The caller's next line number: where the call under test sits."""
    return sys._getframe(1).f_lineno + 1


@contextlib.contextmanager
def recorded() -> Generator[list[warnings.WarningMessage]]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        yield caught


def future_warnings(caught: list[warnings.WarningMessage]) -> list[warnings.WarningMessage]:
    return [w for w in caught if issubclass(w.category, FutureWarning)]


def assert_one_deprecation(
    caught: list[warnings.WarningMessage],
    *,
    call: str,
    remedies: tuple[str, ...],
    filename: str,
    line: int | None = None,
) -> None:
    """Exactly one FutureWarning naming ``call``, one of ``remedies`` and SLayer 2.0, attributed to the caller."""
    future = future_warnings(caught)
    assert len(future) == 1, [str(w.message) for w in future]
    message = str(future[0].message)
    assert call in message, message
    assert any(remedy in message for remedy in remedies), message
    assert "SLayer 2.0" in message, message
    assert future[0].filename == filename
    if line is not None:
        assert future[0].lineno == line
