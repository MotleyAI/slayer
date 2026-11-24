"""The one door for code scheduled for removal: a call to ``warn_deprecated``, ``raise_moved``, ``deprecated_input``,
``deprecated_spelling`` or ``scheduled_notice`` is the tag.

``tests/test_law_removal_tripwire.py`` scans for these calls and fails once the package's
major version reaches a call's ``removal``.
"""

from __future__ import annotations

import contextlib
import os
import sys
import warnings
from collections.abc import Generator, Iterable
from contextvars import ContextVar
from typing import NoReturn

import pydantic

from slayer.core import warnings as payloads  # a module reference: warnings reaches this module through vocabulary

# Frames under these directories are never the caller a warning names.
_INTERNAL_DIRS = (
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + os.sep,
    os.path.abspath(pydantic.__path__[0]) + os.sep,
)


def warn_deprecated(message: str, *, removal: str, stacklevel: int = 2) -> None:
    """Emit ``message`` as a ``FutureWarning`` (shown outside ``__main__``, unlike ``DeprecationWarning``).

    ``removal`` is the version dropping the deprecated form, as a string literal; ``stacklevel``
    counts from the caller, as in ``warnings.warn``.
    """
    del removal  # read statically by the tripwire
    warnings.warn(message, FutureWarning, stacklevel=stacklevel + 1)


class DeprecationFutureWarning(FutureWarning):
    """Carrier ``FutureWarning`` for a deprecation payload — one wording on both channels."""

    def __init__(self, payload: payloads.SlayerWarning) -> None:
        super().__init__(payload)
        self.payload = payload

    def __str__(self) -> str:
        return self.payload.human_message()


class DeprecatedInputFutureWarning(DeprecationFutureWarning):
    """Carrier for a ``DeprecatedInputWarning``."""

    payload: payloads.DeprecatedInputWarning

    def __init__(self, payload: payloads.DeprecatedInputWarning) -> None:
        super().__init__(payload)


class DeprecatedSpellingFutureWarning(DeprecationFutureWarning):
    """Carrier for a ``DeprecatedSpellingWarning``."""

    payload: payloads.DeprecatedSpellingWarning

    def __init__(self, payload: payloads.DeprecatedSpellingWarning) -> None:
        super().__init__(payload)


_DEPRECATION_PAYLOADS = (
    payloads.DeprecatedInputWarning, payloads.DeprecatedSpellingWarning, payloads.DateOnlyMidnightWarning,
    payloads.ChangedReferenceWarning,
)

# The texts the innermost ``deprecations_on_one_line`` block has shown or let through.
_shown: ContextVar[set[str] | None] = ContextVar("slayer_shown_deprecations", default=None)


def outside_stacklevel() -> int:
    """``stacklevel``, counted from this function's caller, of the first frame outside SLayer and pydantic."""
    frame = sys._getframe(1)
    level = 1
    while frame.f_back is not None and os.path.abspath(frame.f_code.co_filename).startswith(_INTERNAL_DIRS):
        frame = frame.f_back
        level += 1
    return level


def deprecated_input(old: str, new: str, *, removal: str, emit: bool = True) -> payloads.DeprecatedInputWarning:
    """Report converting the input form ``old`` to ``new``: one ``FutureWarning`` at the first line outside SLayer
    that supplied it (none when ``emit`` is off), and the payload. ``removal`` as in ``warn_deprecated``."""
    payload = payloads.DeprecatedInputWarning(old=old, new=new, removal=removal)
    if emit:
        warnings.warn(DeprecatedInputFutureWarning(payload), stacklevel=outside_stacklevel())
    return payload


def deprecated_spelling(
    spelling: str, written: str, canonical: str, *, removal: str,
) -> payloads.DeprecatedSpellingWarning:
    """Report reading the Mode-B ``spelling`` (as ``written``) for ``canonical``: one ``FutureWarning`` at the first
    line outside SLayer, and the payload. ``removal`` as in ``warn_deprecated``."""
    payload = payloads.DeprecatedSpellingWarning(
        spelling=spelling, written=written, canonical=canonical, removal=removal)
    warnings.warn(DeprecatedSpellingFutureWarning(payload), stacklevel=outside_stacklevel())
    return payload


def raise_moved(old: str, new: str, *, removal: str) -> NoReturn:
    """Raise an ``ImportError`` saying ``old`` moved to ``new`` in SLayer 2.0; ``removal`` as in ``warn_deprecated``."""
    del removal  # read statically by the tripwire
    raise ImportError(f"{old} moved to {new} in SLayer 2.0")


def scheduled_notice(message: str, *, removal: str) -> str:
    """``message``, an informational notice dropped in ``removal``; it emits nothing, its caller surfaces it."""
    del removal  # read statically by the tripwire
    return message


@contextlib.contextmanager
def deprecations_on_one_line() -> Generator[None]:
    """Within the block, show each deprecation once, as one ``Deprecated: …`` line on the current stderr."""
    seen: set[str] = set()
    token = _shown.set(seen)
    try:
        with warnings.catch_warnings():
            shown = warnings.showwarning

            def _show(message, category, filename, lineno, file=None, line=None) -> None:  # noqa: ANN001 — showwarning's signature
                if not issubclass(category, FutureWarning):
                    shown(message, category, filename, lineno, file, line)
                elif (text := str(message)) not in seen:
                    seen.add(text)
                    print(f"Deprecated: {text}", file=sys.stderr)

            warnings.showwarning = _show
            yield
    finally:
        _shown.reset(token)


def unshown(items: Iterable[str | payloads.SlayerWarning]) -> list[str]:
    """The texts of ``items`` the enclosing ``deprecations_on_one_line`` block has not shown; deprecation payloads
    returned count as shown."""
    seen = _shown.get()
    out: list[str] = []
    for item in items:
        text = item if isinstance(item, str) else item.human_message()
        if seen is None:
            out.append(text)
        elif text not in seen:
            out.append(text)
            if isinstance(item, _DEPRECATION_PAYLOADS):
                seen.add(text)
    return out
