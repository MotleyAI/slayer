"""Assert a failure is the typed colon-spelling refusal."""

from __future__ import annotations

from pydantic import ValidationError

from slayer.core.errors import ColonSpellingError


def colon_error(exc: BaseException) -> ColonSpellingError | None:
    """The ``ColonSpellingError`` behind ``exc``: itself, an explicit cause, or a pydantic validation context.

    ``__context__`` is not followed: an error raised while handling a swallowed colon error is not the refusal.
    """
    seen: set[int] = set()
    stack: list[BaseException] = [exc]
    while stack:
        cur = stack.pop()
        if id(cur) in seen:
            continue
        seen.add(id(cur))
        if isinstance(cur, ColonSpellingError):
            return cur
        if isinstance(cur, ValidationError):
            stack += [ctx["error"] for e in cur.errors()
                      if isinstance(ctx := e.get("ctx") or {}, dict) and isinstance(ctx.get("error"), BaseException)]
        if cur.__cause__ is not None:
            stack.append(cur.__cause__)
    return None


def assert_colon_refused(exc: BaseException, *, span: str, twin: str) -> None:
    """``exc`` is (or wraps) the colon-spelling error for ``span``, naming ``twin``."""
    err = colon_error(exc)
    assert err is not None, f"{type(exc).__name__}: {exc}"
    assert (err.span, err.twin) == (span, twin)
    assert twin in str(err)
