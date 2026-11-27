"""The one name matcher: unquoted names fold case, quoted names match exactly then by Ossie's normalized form,
the exact spelling wins."""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Iterable, List, Optional, Sequence

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from slayer.core.errors import AmbiguousReferenceError


#: An unquoted name: letters, digits and underscores, not starting with a digit.
NAME_PATTERN = re.compile(r"^[a-zA-Z_]\w*$", re.ASCII)


def quoted_name(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def spelled_name(name: str) -> str:
    """``name`` as a Mode-B reference segment: bare when it is a plain name, else double-quoted."""
    return name if NAME_PATTERN.match(name) else quoted_name(name)


def qualified_reference(*segments: str) -> str:
    """The dotted reference of ``segments`` (a dataset first), each spelled as a segment."""
    return ".".join(spelled_name(s) for s in segments)


def normalized_identifier(name: str, *, quoted: bool) -> str:
    """Ossie Layer 1's normalized identifier: a regular one upper-cased, a quoted one exactly as written."""
    return name if quoted else name.upper()


@lru_cache(maxsize=4096)
def normalized_name(name: str) -> str:
    """Ossie's normalized form of a declared name (upstream ``declared_name``): upper-cased when readable unquoted
    as a reference's second part, else as written."""
    try:
        reference = sqlglot.parse_one(f"x.{name}", into=exp.Condition)
    except (SqlglotError, ValueError):
        return name
    parts = reference.parts if isinstance(reference, exp.Column) else []
    regular = (len(parts) == 2 and isinstance(parts[1], exp.Identifier) and not parts[1].quoted
               and parts[1].name == name)
    return name.upper() if regular else name


def match_name(*, name: str, declared: Iterable[str], quoted: bool, exact: bool = False) -> Optional[str]:
    """The declared spelling ``name`` reaches: an exact match, else (unless ``exact``) the unique one equal up to case
    (unquoted) or by normalized form (quoted); several such matches raise ``AmbiguousReferenceError``."""
    names = list(dict.fromkeys(declared))
    if name in names:
        return name
    if exact:
        return None
    if quoted:
        return _unique(name=name, hits=[d for d in names if normalized_name(d) == name])
    folded = name.casefold()
    return _unique(name=name, hits=[d for d in names if d.casefold() == folded])


def match_path(*, parts: Sequence[str], declared: Iterable[str], quoted: Sequence[bool]) -> Optional[str]:
    """``match_name`` for a dotted declared name, segment by segment: ``quoted[i]`` makes segment ``i`` exact, else
    normalized when no declared name matches with it exact."""
    names = list(dict.fromkeys(declared))
    text = ".".join(parts)
    if text in names:
        return text
    for normalized in (False, True):
        hits = [d for d in names
                if _segments_match(parts=parts, declared=d.split("."), quoted=quoted, normalized=normalized)]
        if hits:
            return _unique(name=text, hits=hits)
    return None


def _segment_matches(*, part: str, declared: str, quoted: bool, normalized: bool) -> bool:
    if part == declared:
        return True
    if quoted:
        return normalized and normalized_name(declared) == part
    return part.casefold() == declared.casefold()


def _segments_match(*, parts: Sequence[str], declared: Sequence[str], quoted: Sequence[bool],
                    normalized: bool) -> bool:
    return len(parts) == len(declared) and all(
        _segment_matches(part=p, declared=d, quoted=i < len(quoted) and quoted[i], normalized=normalized)
        for i, (p, d) in enumerate(zip(parts, declared))
    )


def _unique(*, name: str, hits: List[str]) -> Optional[str]:
    if len(hits) > 1:
        raise AmbiguousReferenceError(
            name, hits, suggestion="Quote the exact name: " + " or ".join(quoted_name(h) for h in sorted(hits)) + ".",
        )
    return hits[0] if hits else None


def name_taken(name: str, taken: Iterable[str]) -> bool:
    """Whether ``name`` equals one of ``taken`` in any case (a dataset's names are one case-insensitive namespace)."""
    folded = name.casefold()
    return any(t.casefold() == folded for t in taken)


def folded_collisions(names: Iterable[str]) -> list[list[str]]:
    """Groups of distinct names equal up to case, each sorted."""
    groups: dict[str, list[str]] = {}
    for name in dict.fromkeys(names):
        groups.setdefault(name.casefold(), []).append(name)
    return [sorted(g) for g in groups.values() if len(g) > 1]
