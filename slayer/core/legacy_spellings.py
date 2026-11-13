"""Deprecated Mode-B spellings: each keeps its meaning, warns once per query or save, and is stored as its twin."""

from __future__ import annotations

from collections.abc import Generator, Iterable
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Dict, List, Literal, Optional, Protocol, TypeVar

from pydantic import BaseModel, ConfigDict

from slayer.core.deprecation import warn_deprecated
from slayer.core.warnings import DeprecatedSpellingWarning

LegacyKind = Literal["==", "is None", "is not None", "bare None", "list after IN", "method-call"]


class LegacySpan(BaseModel):
    """A deprecated spelling at ``text[start:end]`` and its canonical ``twin``."""

    model_config = ConfigDict(frozen=True)

    kind: LegacyKind
    start: int
    end: int
    written: str
    twin: str


_sink: ContextVar[Optional[Dict[str, LegacySpan]]] = ContextVar("slayer_legacy_spellings", default=None)


@contextmanager
def collect_legacy_spellings() -> Generator[Dict[str, LegacySpan]]:
    """Inside the block, the first span of each deprecated spelling the reader meets is recorded."""
    found: Dict[str, LegacySpan] = {}
    token = _sink.set(found)
    try:
        yield found
    finally:
        _sink.reset(token)


def note_legacy_spellings(spans: Iterable[LegacySpan]) -> None:
    found = _sink.get()
    if found is not None:
        for span in spans:
            found.setdefault(span.kind, span)


def emit_deprecations(found: Dict[str, LegacySpan]) -> List[DeprecatedSpellingWarning]:
    """One ``FutureWarning`` and one payload per spelling recorded."""
    return [_emit(span) for span in found.values()]


def _emit(span: LegacySpan) -> DeprecatedSpellingWarning:
    if span.kind == "==":
        warn_deprecated("The comparison `==` is deprecated and removed in SLayer 3.0; write `=`.", removal="3.0",
                        stacklevel=4)
    elif span.kind == "is None":
        warn_deprecated("`is None` is deprecated and removed in SLayer 3.0; write `IS NULL`.", removal="3.0",
                        stacklevel=4)
    elif span.kind == "is not None":
        warn_deprecated("`is not None` is deprecated and removed in SLayer 3.0; write `IS NOT NULL`.", removal="3.0",
                        stacklevel=4)
    elif span.kind == "bare None":
        warn_deprecated("A bare None is deprecated and removed in SLayer 3.0; write `NULL`.", removal="3.0",
                        stacklevel=4)
    elif span.kind == "list after IN":
        warn_deprecated("A [ … ] list after IN is deprecated and removed in SLayer 3.0; write a parenthesised "
                        "list ( … ).", removal="3.0", stacklevel=4)
    else:
        warn_deprecated(f"The method-call form `{span.written}` is deprecated and removed in SLayer 3.0; write "
                        f"`{span.twin}`.", removal="3.0", stacklevel=4)
    return DeprecatedSpellingWarning(spelling=span.kind, written=span.written, canonical=span.twin)


class TextSpan(Protocol):
    """A span of text to replace by its ``twin``."""

    @property
    def start(self) -> int: ...

    @property
    def end(self) -> int: ...

    @property
    def twin(self) -> str: ...


_Span = TypeVar("_Span", bound=TextSpan)


def outermost(spans: Iterable[_Span]) -> List[_Span]:
    """The spans no other span contains, in text order."""
    ordered = sorted(spans, key=lambda s: (s.start, -s.end))
    out: List[_Span] = []
    for span in ordered:
        if out and span.start < out[-1].end:
            continue
        out.append(span)
    return out


def rewrite_spans(text: str, spans: Iterable[TextSpan]) -> str:
    """``text`` with each outermost span replaced by its twin, the rest byte-identical."""
    out: List[str] = []
    cursor = 0
    for span in outermost(spans):
        out.append(text[cursor:span.start] + span.twin)
        cursor = span.end
    return "".join(out) + text[cursor:]
