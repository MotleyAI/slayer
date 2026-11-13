"""The one door for code scheduled for removal: a call to ``warn_deprecated``, ``raise_moved``, ``deprecated_input``
or ``scheduled_notice`` is the tag.

``tests/test_law_removal_tripwire.py`` scans for these calls and fails once the package's
major version reaches a call's ``removal``.
"""

import contextlib
import os
import sys
import warnings
from collections.abc import Generator
from typing import NoReturn

import pydantic

from slayer.core.warnings import DeprecatedInputWarning

# Frames under these directories never supply input; a deprecated input form is attributed past them.
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


class DeprecatedInputFutureWarning(FutureWarning):
    """Carrier ``FutureWarning`` for a ``DeprecatedInputWarning`` payload — one wording on both channels."""

    def __init__(self, payload: DeprecatedInputWarning) -> None:
        super().__init__(payload)
        self.payload = payload

    def __str__(self) -> str:
        return self.payload.human_message()


def _outside_caller_stacklevel() -> int:
    """``warnings.warn``'s ``stacklevel`` (from ``deprecated_input``) of the first frame outside SLayer and pydantic."""
    frame = sys._getframe(2)  # deprecated_input's caller
    level = 2
    while frame.f_back is not None and os.path.abspath(frame.f_code.co_filename).startswith(_INTERNAL_DIRS):
        frame = frame.f_back
        level += 1
    return level


def deprecated_input(old: str, new: str, *, removal: str) -> DeprecatedInputWarning:
    """Report converting the input form ``old`` to ``new``: one ``FutureWarning`` at the first line outside SLayer
    that supplied it, and the payload the caller keeps with the converted input. ``removal`` as in ``warn_deprecated``."""
    payload = DeprecatedInputWarning(old=old, new=new, removal=removal)
    warnings.warn(DeprecatedInputFutureWarning(payload), stacklevel=_outside_caller_stacklevel())
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
    """Within the block, show each deprecation as one ``Deprecated: …`` line on the current stderr."""
    with warnings.catch_warnings():
        shown = warnings.showwarning

        def _show(message, category, filename, lineno, file=None, line=None) -> None:  # noqa: ANN001 — showwarning's signature
            if issubclass(category, FutureWarning):
                print(f"Deprecated: {message}", file=sys.stderr)
            else:
                shown(message, category, filename, lineno, file, line)

        warnings.showwarning = _show
        yield
