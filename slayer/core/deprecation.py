"""The one door for deprecations scheduled for removal: a call to ``warn_deprecated`` or ``raise_moved`` is the tag.

``tests/test_law_removal_tripwire.py`` scans for these calls and fails once the package's
major version reaches a call's ``removal``.
"""

import contextlib
import sys
import warnings
from collections.abc import Generator
from typing import NoReturn

from pydantic import BaseModel


class WarningLocation(BaseModel):
    """An explicit source location for a warning whose caller is not on the stack."""

    filename: str
    lineno: int
    module: str


def warn_deprecated(
    message: str, *, removal: str, stacklevel: int = 2, location: WarningLocation | None = None,
) -> None:
    """Emit ``message`` as a ``FutureWarning`` (shown outside ``__main__``, unlike ``DeprecationWarning``).

    ``removal`` is the version dropping the deprecated form, as a string literal; ``stacklevel``
    counts from the caller, as in ``warnings.warn``.
    """
    del removal  # read statically by the tripwire
    if location is None:
        warnings.warn(message, FutureWarning, stacklevel=stacklevel + 1)
    else:
        warnings.warn_explicit(
            message, FutureWarning, filename=location.filename, lineno=location.lineno, module=location.module,
        )


def raise_moved(old: str, new: str, *, removal: str) -> NoReturn:
    """Raise an ``ImportError`` saying ``old`` moved to ``new`` in SLayer 2.0; ``removal`` as in ``warn_deprecated``."""
    del removal  # read statically by the tripwire
    raise ImportError(f"{old} moved to {new} in SLayer 2.0")


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
