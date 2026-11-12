"""The one LIKE pattern language: ``%`` / ``_`` wildcards, ``\\`` escaping ``%``, ``_`` and itself, all else literal."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import BaseModel, ConfigDict

LIKE_SPECIALS = frozenset("%_\\")


class LikeWildcard(BaseModel):
    model_config = ConfigDict(frozen=True)

    #: ``any``: ``%`` (any run of characters); ``one``: ``_`` (exactly one).
    kind: Literal["any", "one"]


class LikePattern(BaseModel):
    """Literal text runs and wildcards, in order."""

    model_config = ConfigDict(frozen=True)

    parts: tuple[str | LikeWildcard, ...]


@lru_cache(maxsize=4096)
def parse_like(text: str) -> LikePattern:
    """``text`` read in the LIKE pattern language (every string is a valid pattern)."""
    parts: list = []
    literal: list = []
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "\\" and i + 1 < len(text) and text[i + 1] in LIKE_SPECIALS:
            literal.append(text[i + 1])
            i += 2
            continue
        if ch in "%_":
            if literal:
                parts.append("".join(literal))
                literal = []
            parts.append(LikeWildcard(kind="any" if ch == "%" else "one"))
        else:
            literal.append(ch)
        i += 1
    if literal:
        parts.append("".join(literal))
    return LikePattern(parts=tuple(parts))


__all__ = ["LIKE_SPECIALS", "LikePattern", "LikeWildcard", "parse_like"]
