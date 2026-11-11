"""The one name matcher: unquoted names fold case, quoted names match exactly, the exact spelling wins."""

from __future__ import annotations

from typing import Iterable, List, Optional, Sequence

from slayer.core.errors import AmbiguousReferenceError


def quoted_name(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def match_name(*, name: str, declared: Iterable[str], quoted: bool) -> Optional[str]:
    """The declared spelling ``name`` reaches: an exact match, else (unquoted) the unique case-insensitive one;
    several case-insensitive matches raise ``AmbiguousReferenceError``."""
    names = list(dict.fromkeys(declared))
    if name in names:
        return name
    if quoted:
        return None
    folded = name.casefold()
    return _unique(name=name, hits=[d for d in names if d.casefold() == folded])


def match_path(*, parts: Sequence[str], declared: Iterable[str], quoted: Sequence[bool]) -> Optional[str]:
    """``match_name`` for a dotted declared name, segment by segment: ``quoted[i]`` makes segment ``i`` exact."""
    names = list(dict.fromkeys(declared))
    text = ".".join(parts)
    if text in names:
        return text
    return _unique(name=text, hits=[d for d in names if _segments_match(parts=parts, declared=d.split("."), quoted=quoted)])


def _segments_match(*, parts: Sequence[str], declared: Sequence[str], quoted: Sequence[bool]) -> bool:
    return len(parts) == len(declared) and all(
        p == d or (not (i < len(quoted) and quoted[i]) and p.casefold() == d.casefold())
        for i, (p, d) in enumerate(zip(parts, declared))
    )


def _unique(*, name: str, hits: List[str]) -> Optional[str]:
    if len(hits) > 1:
        raise AmbiguousReferenceError(
            name, hits, suggestion="Quote the exact name: " + " or ".join(quoted_name(h) for h in sorted(hits)) + ".",
        )
    return hits[0] if hits else None


def folded_collisions(names: Iterable[str]) -> list[list[str]]:
    """Groups of distinct names equal up to case, each sorted."""
    groups: dict[str, list[str]] = {}
    for name in dict.fromkeys(names):
        groups.setdefault(name.casefold(), []).append(name)
    return [sorted(g) for g in groups.values() if len(g) > 1]
