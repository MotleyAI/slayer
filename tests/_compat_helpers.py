"""Warning capture for the deprecation tests."""

from __future__ import annotations

import contextlib
import warnings
from collections.abc import Generator


@contextlib.contextmanager
def recorded() -> Generator[list[warnings.WarningMessage]]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        yield caught


def future_warnings(caught: list[warnings.WarningMessage]) -> list[warnings.WarningMessage]:
    return [w for w in caught if issubclass(w.category, FutureWarning)]
